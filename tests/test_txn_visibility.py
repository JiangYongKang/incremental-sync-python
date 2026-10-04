"""并发读取下的事务原子可见性：提交进行中，读者只能看到整组事务。

覆盖业务：
- 跨表多行事务一边提交、另一边持续读目标端，每次读到的都是整组一致状态；
- 多线程并发提交多个事务 + 并发读，读到的只能是若干完整事务的组合；
- 事务整组拒绝时对读者完全不可见，重启后无残留、重投结论稳定。
"""
import logging
import random
import threading
import time

from helpers import make_env
from sync.models import ChangeRecord, Column, Op, TxnCommit
from sync.target import LocalTarget
from sync.checkpoint import CheckpointStore
from sync.schema_registry import SchemaRegistry
from sync.applier import SyncApplier

logger = logging.getLogger("test.txn_visibility")

TABLES = ("orders", "users")


def _register(schemas):
    schemas.register("orders", "id", [
        Column("id", "int", nullable=False),
        Column("tid", "int", nullable=False),
        Column("amount", "int", nullable=False),
    ])


def _env(tmp_path, op_hook=None):
    schemas, source, _, checkpoints, _ = make_env(tmp_path)
    _register(schemas)
    target = LocalTarget(tmp_path / "target.json", op_hook=op_hook)
    applier = SyncApplier(target, checkpoints, schemas, tmp_path / "applier_state.json")
    return schemas, source, target, checkpoints, applier


def _txn(i, base):
    """第 i 个跨表事务：orders 2 行 + users 2 行，行内带 tid 便于按事务核对。"""
    recs = [
        ChangeRecord(seq=base, table="orders", pk=1000 + i, op=Op.INSERT,
                     fields={"id": 1000 + i, "tid": i, "amount": 10 * i},
                     schema_version=1, txn_id=f"T{i}"),
        ChangeRecord(seq=base + 1, table="orders", pk=2000 + i, op=Op.INSERT,
                     fields={"id": 2000 + i, "tid": i, "amount": 10 * i + 1},
                     schema_version=1, txn_id=f"T{i}"),
        ChangeRecord(seq=base + 2, table="users", pk=1000 + i, op=Op.INSERT,
                     fields={"id": 1000 + i, "name": f"u{i}a"},
                     schema_version=1, txn_id=f"T{i}"),
        ChangeRecord(seq=base + 3, table="users", pk=2000 + i, op=Op.INSERT,
                     fields={"id": 2000 + i, "name": f"u{i}b"},
                     schema_version=1, txn_id=f"T{i}"),
    ]
    commit = TxnCommit(seq=base + 4, txn_id=f"T{i}", members=tuple(range(base, base + 4)))
    return recs, commit


def _check_snapshot(snap, n_txns):
    """校验一份跨表快照：每个事务要么 4 行全在、要么全不在。返回违例描述列表。"""
    problems = []
    for i in range(n_txns):
        o = [pk for pk in (str(1000 + i), str(2000 + i)) if pk in snap["orders"]]
        u = [pk for pk in (str(1000 + i), str(2000 + i)) if pk in snap["users"]]
        if not o and not u:
            continue  # 整组尚未生效：合法状态
        if len(o) != 2 or len(u) != 2:
            problems.append(
                f"T{i} 半成品: orders 有 {sorted(o)} users 有 {sorted(u)}")
        for pk in o:
            if snap["orders"][pk].get("tid") != i:
                problems.append(f"T{i} 行内容错拼: orders[{pk}]={snap['orders'][pk]}")
    return problems


def test_concurrent_reader_sees_only_whole_transactions(tmp_path, caplog):
    """跨表事务提交进行中，另一线程持续读：每次读到的都是整组一致状态。"""
    n_txns = 30
    # 行变更之间强制让出 1ms：把"事务生效窗口"拉大到毫秒级，
    # 读者必然在窗口内反复采样——若非原子生效必然被抓到，不靠撞运气
    _, _, target, checkpoints, applier = _env(
        tmp_path, op_hook=lambda: time.sleep(0.001))

    violations: list[str] = []
    samples = 0
    intermediate_seen = False  # 读到过"部分事务已生效"的推进中间态（证明确实在并发读）
    stop = threading.Event()

    def reader():
        nonlocal samples, intermediate_seen
        while not stop.is_set():
            snap = target.read_snapshot(TABLES)
            samples += 1
            done = sum(1 for i in range(n_txns) if str(1000 + i) in snap["orders"])
            if 0 < done < n_txns:
                intermediate_seen = True
            violations.extend(_check_snapshot(snap, n_txns))

    def writer():
        for i in range(n_txns):
            recs, commit = _txn(i, base=1 + i * 5)
            applier.apply_batch([*recs, commit])

    with caplog.at_level(logging.INFO, logger="test.txn_visibility"):
        rt = threading.Thread(target=reader)
        rt.start()
        writer()
        stop.set()
        rt.join()
        logger.info("并发可见性: 事务数=%d 每事务4行跨2表 读采样=%d 次, 中间态采样=%s, 违例=%d",
                    n_txns, samples, intermediate_seen, len(violations))

    assert not violations, "读到半成品事务:\n" + "\n".join(violations[:10])
    assert samples > n_txns  # 确认读者确实在提交过程中反复采样，不是空转
    assert intermediate_seen  # 确认读到过推进中的状态，测试不是"写完才读"
    assert checkpoints.load() == n_txns * 5
    snap = target.read_snapshot(TABLES)
    assert len(snap["orders"]) == n_txns * 2 and len(snap["users"]) == n_txns * 2


def test_concurrent_committers_and_reader(tmp_path, caplog):
    """多线程并发提交（含重复投递）+ 并发读：快照只能是若干完整事务的组合。"""
    n_txns = 12
    _, _, target, checkpoints, applier = _env(
        tmp_path, op_hook=lambda: time.sleep(0.001))
    batches = []
    for i in range(n_txns):
        recs, commit = _txn(i, base=1 + i * 5)
        batches.append([*recs, commit])

    violations: list[str] = []
    errors: list[Exception] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            violations.extend(_check_snapshot(target.read_snapshot(TABLES), n_txns))

    def writer(seed):
        try:
            for batch in random.Random(seed).sample(batches, len(batches)):
                applier.apply_batch(batch)  # 乱序 + 各线程整批重复投递
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    with caplog.at_level(logging.INFO, logger="test.txn_visibility"):
        threads = [threading.Thread(target=writer, args=(s,)) for s in range(4)]
        threads.append(threading.Thread(target=reader))
        for t in threads:
            t.start()
        for t in threads[:4]:
            t.join()
        stop.set()
        threads[4].join()
        logger.info("并发提交+并发读: 写入线程=4 事务数=%d 违例=%d 异常=%d",
                    n_txns, len(violations), len(errors))

    assert not errors
    assert not violations, "读到半成品事务:\n" + "\n".join(violations[:10])
    assert checkpoints.load() == n_txns * 5
    snap = target.read_snapshot(TABLES)
    assert len(snap["orders"]) == n_txns * 2 and len(snap["users"]) == n_txns * 2


def test_rejected_txn_never_visible_and_stable_after_restart(tmp_path, caplog):
    """整组拒绝的事务对读者不可见；重启后无残留，重投结论稳定、位点自洽。"""
    _, _, target, checkpoints, applier = _env(tmp_path)
    recs0, commit0 = _txn(0, 1)
    applier.apply_batch([*recs0, commit0])  # T0 正常生效，位点 5

    # T1：users 第二条成员缺必填字段 name -> 整事务拒绝
    bad = ChangeRecord(seq=9, table="users", pk=2001, op=Op.INSERT,
                       fields={"id": 2001}, schema_version=1, txn_id="T1")
    recs1, commit1 = _txn(1, 6)
    batch1 = [recs1[0], recs1[1], recs1[2], bad, commit1]

    violations: list[str] = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            snap = target.read_snapshot(TABLES)
            # T1 的任何成员行都绝不可见（无论拒绝前、拒绝后）
            leaked = [t for t in TABLES for pk in ("1001", "2001") if pk in snap[t]]
            if leaked:
                violations.append(f"被拒事务 T1 的行泄漏到: {leaked}")

    with caplog.at_level(logging.INFO, logger="test.txn_visibility"):
        rt = threading.Thread(target=reader)
        rt.start()
        r = applier.apply_batch(batch1)
        stop.set()
        rt.join()
        logger.info("拒绝不可见: 被拒事务=%s 原因=%s 位点=%d 泄漏=%d",
                    [t.txn_id for t in r.rejected_txns],
                    [rj.reason.value for t in r.rejected_txns for rj in t.reasons],
                    checkpoints.load(), len(violations))

    assert not violations
    assert [t.txn_id for t in r.rejected_txns] == ["T1"]
    assert checkpoints.load() == 10  # 被拒事务的位点已消费，与数据/死信自洽
    dead = applier.rejected_transactions()
    assert len(dead) == 1 and dead[0].txn_id == "T1"

    # 重启：目标端只剩 T0 的完整数据，无 T1 残留；死信与位点一起恢复
    schemas = SchemaRegistry()
    _register(schemas)
    schemas.register("users", "id", [
        Column("id", "int", nullable=False),
        Column("name", "str", nullable=False),
        Column("age", "int", nullable=True, default=0),
    ])
    target2 = LocalTarget(tmp_path / "target.json")
    checkpoints2 = CheckpointStore(tmp_path / "checkpoint.json")
    applier2 = SyncApplier(target2, checkpoints2, schemas, tmp_path / "applier_state.json")
    snap = target2.read_snapshot(TABLES)
    assert not _check_snapshot(snap, 2)
    assert "1001" not in snap["orders"] and "1001" not in snap["users"]
    assert checkpoints2.load() == 10
    assert [d.txn_id for d in applier2.rejected_transactions()] == ["T1"]

    # 重投同一事务：整组再次被拒（判重），不会写入半组
    r2 = applier2.apply_batch(batch1)
    assert not r2.applied and not r2.rejected_txns
    snap = target2.read_snapshot(TABLES)
    assert "1001" not in snap["orders"] and "1001" not in snap["users"]
    assert checkpoints2.load() == 10
