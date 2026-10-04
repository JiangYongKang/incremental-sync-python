"""并发可见性：提交进行中持续读取目标端，任何一次读取都必须是整组一致状态。

覆盖业务（单测日志可逐条核对 sync.applier 的 staged/committed/checkpoint 流水）：
- 跨表 + 多行事务（orders 两行、billing/inventory/shipments 各一行）在提交
  窗口期内，另一线程反复读取，只能看到“该事务之前”或“该事务之后”的完整状态，
  绝不能看到某张表已出现新行、其余表为空的半成品；
- 多个提交线程并发提交多个事务、多个读者线程持续读取，每次读到的事务集合
  必须跨表完全相同且为按 seq 的连续前缀（不跨事务错拼行）；
- 每次读取的数据与位点、被拒记录自洽：consistent_view 的 confirmed_seq 必须
  恰好覆盖可见的完整事务数。

这些断言是**不变式断言**（任意调度下成立），不依赖 sleep 或撞运气；唯一的
时间窗由 SyncApplier.before_visibility 钩子确定性撑开。
"""
import logging
import threading

from sync.models import ChangeRecord, Column, Op, TxnCommit

test_log = logging.getLogger("sync.applier")

TABLES = ("orders", "billing", "inventory", "shipments")
MEMBERS_PER_TXN = 5       # orders 两行 + 其余三表各一行
EVENTS_PER_TXN = MEMBERS_PER_TXN + 1  # 含提交标记


def _register(schemas):
    schemas.register("orders", "id", [
        Column("id", "int", nullable=False),
        Column("user_id", "int", nullable=False),
        Column("amount", "int", nullable=False),
    ])
    schemas.register("billing", "id", [
        Column("id", "int", nullable=False),
        Column("total", "int", nullable=False),
    ])
    schemas.register("inventory", "id", [
        Column("id", "int", nullable=False),
        Column("stock", "int", nullable=False),
    ])
    schemas.register("shipments", "id", [
        Column("id", "int", nullable=False),
        Column("addr", "str", nullable=False),
    ])


def _env(tmp_path):
    from helpers import make_env
    schemas, source, target, checkpoints, applier = make_env(tmp_path)
    _register(schemas)
    return schemas, source, target, checkpoints, applier


def _txn_events(g):
    """第 g 个跨表事务：orders 两行，billing/inventory/shipments 各一行。

    主键由事务序号 g 推导，便于断言“可见集合 = 连续完整事务前缀”。
    """
    base = g * EVENTS_PER_TXN + 1
    txn = f"T{g}"
    recs = [
        ChangeRecord(seq=base, table="orders", pk=1000 + g, op=Op.INSERT,
                     fields={"id": 1000 + g, "user_id": g, "amount": 10 * g},
                     schema_version=1, txn_id=txn),
        ChangeRecord(seq=base + 1, table="orders", pk=2000 + g, op=Op.INSERT,
                     fields={"id": 2000 + g, "user_id": g, "amount": 20 * g},
                     schema_version=1, txn_id=txn),
        ChangeRecord(seq=base + 2, table="billing", pk=1000 + g, op=Op.INSERT,
                     fields={"id": 1000 + g, "total": 30 * g},
                     schema_version=1, txn_id=txn),
        ChangeRecord(seq=base + 3, table="inventory", pk=1000 + g, op=Op.INSERT,
                     fields={"id": 1000 + g, "stock": 5 * g},
                     schema_version=1, txn_id=txn),
        ChangeRecord(seq=base + 4, table="shipments", pk=1000 + g, op=Op.INSERT,
                     fields={"id": 1000 + g, "addr": f"addr-{g}"},
                     schema_version=1, txn_id=txn),
    ]
    commit = TxnCommit(seq=base + 5, txn_id=txn,
                       members=tuple(range(base, base + 5)))
    return recs, commit


def _whole_txns(tables):
    """返回快照中“整组可见”的事务序号集合；任何事务半成品/跨表错拼立即失败。

    不变式：对每个事务 g，orders 的两行与 billing/inventory/shipments 的行
    要么全部在、要么全不在——绝不允许只看到其中一部分。允许读到若干完整事务
    的任意组合（乱序到达时已确认位点之前的完整前缀一定可见）。
    """
    sets = {t: set(tables.get(t, {})) for t in TABLES}
    present = set()
    # 以任一表出现过的事务号为候选全集
    candidates = set()
    for pk in sets["orders"]:
        v = int(pk)
        if v >= 2000:
            candidates.add(v - 2000)
        else:
            candidates.add(v - 1000)
    for t in ("billing", "inventory", "shipments"):
        candidates.update(int(pk) - 1000 for pk in sets[t])
    for g in candidates:
        flags = [
            str(1000 + g) in sets["orders"],
            str(2000 + g) in sets["orders"],
            str(1000 + g) in sets["billing"],
            str(1000 + g) in sets["inventory"],
            str(1000 + g) in sets["shipments"],
        ]
        assert all(flags) or not any(flags), (
            f"事务 T{g} 出现半成品/跨表错拼: orders/billing/inventory/"
            f"shipments 可见标志={flags}")
        if all(flags):
            present.add(g)
    return present


def test_reader_during_commit_never_sees_half_txn(tmp_path):
    """事务已写 journal、尚未整库发布的窗口内：读者只能读到提交前完整状态。"""
    _, _, target, checkpoints, applier = _env(tmp_path)
    n = 8
    window_reads = 0
    for i in range(n):
        recs, commit = _txn_events(i)
        entered = threading.Event()
        release = threading.Event()

        def hook(_i=i):
            test_log.info("visibility window open: T%d durable-but-invisible", _i)
            entered.set()
            assert release.wait(timeout=5), "提交窗口未被释放（死锁）"

        done = threading.Event()

        def commit_job():
            applier.before_visibility = hook
            try:
                applier.apply_batch([*recs, commit])
            finally:
                applier.before_visibility = None
                done.set()

        t = threading.Thread(target=commit_job, name=f"committer-T{i}")
        t.start()
        assert entered.wait(timeout=5), "提交线程未进入发布窗口"

        # 窗口内高频直读目标端（不经 applier 锁，真正与提交并发）：
        # 必须一直停在“前 i 个完整事务”的旧状态。
        for _ in range(300):
            _gen, tables = target.view()
            present = _whole_txns(tables)
            assert present == set(range(i)), (
                f"T{i} 提交窗口内读到半成品/越界状态: 可见={sorted(present)}, "
                f"期望 0..{i - 1}")
            window_reads += 1
        # 同样用 rows() 逐表拼读，模拟真实读者分表查询
        per_table = {t: target.rows(t) for t in TABLES}
        assert _whole_txns(per_table) == set(range(i))

        release.set()
        assert done.wait(timeout=5)
        t.join()

        # 发布后：整组（多行+多表）同时可见，位点恰好覆盖 i+1 个事务
        view = applier.consistent_view()
        present = _whole_txns(view.tables)
        assert present == set(range(i + 1))
        assert view.confirmed_seq == (i + 1) * EVENTS_PER_TXN
        assert view.dead == {}
        test_log.info("T%d visible atomically: confirmed=%d", i, view.confirmed_seq)

    assert checkpoints.load() == n * EVENTS_PER_TXN
    test_log.info("窗口内并发读取 %d 次，全部为整组一致状态", window_reads)


def test_concurrent_committers_and_readers_always_whole(tmp_path):
    """多线程并发提交多个事务 + 多读者持续读：每次读取都是若干完整事务的组合。"""
    _, _, target, checkpoints, applier = _env(tmp_path)
    total = 40
    committers = 4
    per = total // committers
    errors = []

    def committer(c):
        try:
            for g in range(c * per, (c + 1) * per):
                recs, commit = _txn_events(g)
                applier.apply_batch([*recs, commit])
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    checked = {"target": 0, "view": 0}
    stop = threading.Event()

    def _confirmed_count(seq):
        """位点 seq 恰好完整确认的事务数（每事务占 EVENTS_PER_TXN 个位点）。"""
        return seq // EVENTS_PER_TXN

    def reader(name):
        try:
            while not stop.is_set():
                # 直读目标端：每个可见事务必须整组（5 行）齐全，无半成品/错拼
                _gen, tables = target.view()
                present = _whole_txns(tables)
                checked["target"] += 1
                # 整组一致视图：位点确认的连续前缀必须全部整组可见，数据与位点自洽
                view = applier.consistent_view()
                present_v = _whole_txns(view.tables)
                confirmed_k = _confirmed_count(view.confirmed_seq)
                missing = set(range(confirmed_k)) - present_v
                assert not missing, (
                    f"位点已确认 {view.confirmed_seq}（{confirmed_k} 个事务），"
                    f"但缺少整组事务 {sorted(missing)}：数据/位点不自洽")
                assert view.dead == {}
                checked["view"] += 1
                if checked["view"] % 200 == 0:
                    test_log.info("[%s] whole txns=%d confirmed_seq=%d",
                                  name, len(present_v), view.confirmed_seq)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=committer, args=(c,),
                                name=f"committer-{c}") for c in range(committers)]
    readers = [threading.Thread(target=reader, args=(f"r{c}",),
                                name=f"reader-{c}") for c in range(2)]
    for t in readers:
        t.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    for t in readers:
        t.join()

    assert not errors, errors
    assert checkpoints.load() == total * EVENTS_PER_TXN
    final_view = applier.consistent_view()
    assert _whole_txns(final_view.tables) == set(range(total))
    assert final_view.confirmed_seq == total * EVENTS_PER_TXN
    test_log.info("并发提交 %d 事务；目标端直读校验 %d 次、一致视图校验 %d 次，"
                  "无一次半成品", total, checked["target"], checked["view"])
    # 必须真实发生了大量并发读取，而非读者没跑起来
    assert checked["target"] > 100 and checked["view"] > 100
