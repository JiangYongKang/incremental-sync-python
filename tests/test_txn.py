"""事务化同步：跨表整体生效、重复/乱序/并发、整组拒绝、崩溃续传。"""
import random
import threading

from helpers import make_tx_env, rec
from sync.checkpoint import CheckpointStore
from sync.models import Op, RejectReason
from sync.rejections import RejectionStore


def tx_changes():
    """两个跨表事务 + 一条非事务记录的基线流水（seq 1..6）。"""
    return [
        rec(1, 1, Op.INSERT, {"id": 1, "name": "alice"}, tx_id="tx1", tx_total=2),
        rec(2, 10, Op.INSERT, {"id": 10, "user_id": 1, "amount": 9.5},
            table="orders", tx_id="tx1", tx_total=2),
        rec(3, 2, Op.INSERT, {"id": 2, "name": "bob"}, tx_id="tx2", tx_total=3),
        rec(4, 1, Op.UPDATE, {"age": 31}, tx_id="tx2", tx_total=3),
        rec(5, 20, Op.INSERT, {"id": 20, "user_id": 2, "amount": 3.0},
            table="orders", tx_id="tx2", tx_total=3),
        rec(6, 3, Op.INSERT, {"id": 3, "name": "carol"}),  # 非事务记录
    ]


EXPECTED_USERS = {
    "1": {"id": 1, "name": "alice", "age": 31},
    "2": {"id": 2, "name": "bob", "age": 0},
    "3": {"id": 3, "name": "carol", "age": 0},
}
EXPECTED_ORDERS = {
    "10": {"id": 10, "user_id": 1, "amount": 9.5},
    "20": {"id": 20, "user_id": 2, "amount": 3.0},
}


def test_tx_atomic_visibility_no_partial_state(tmp_path):
    """事务未凑齐时目标端看不到任何中间态；凑齐后一次性整体可见。"""
    _, _, target, checkpoints, applier, _ = make_tx_env(tmp_path)
    r1 = applier.apply_batch(tx_changes()[:1])   # tx1 只到了一半
    assert not r1.applied and not r1.applied_txs
    assert target.rows("users") == {} and target.rows("orders") == {}
    assert checkpoints.load() == 0               # 位点不越过未凑齐的事务

    r2 = applier.apply_batch(tx_changes()[1:2])  # tx1 凑齐
    assert r2.applied_txs == ["tx1"]
    assert target.rows("users") == {"1": {"id": 1, "name": "alice", "age": 0}}
    assert target.rows("orders") == {"10": EXPECTED_ORDERS["10"]}
    assert checkpoints.load() == 2


def test_tx_atomic_visibility_concurrent_reader(tmp_path):
    """读者线程在事务生效过程中只能看到全部生效前或全部生效后。"""
    _, _, target, _, applier, _ = make_tx_env(tmp_path)
    started = threading.Event()
    observed = {}

    def reader():
        started.wait()
        observed["users"] = target.rows("users")
        observed["orders"] = target.rows("orders")

    t = threading.Thread(target=reader)
    t.start()

    orig_upsert = target.upsert

    def spy_upsert(table, pk, row):
        orig_upsert(table, pk, row)
        started.set()                # 第一行已写、第二行未写时唤醒读者
        threading.Event().wait(0.05)

    target.upsert = spy_upsert
    try:
        applier.apply_batch(tx_changes()[:2])    # tx1：users + orders 两行
    finally:
        target.upsert = orig_upsert
    t.join()
    # apply_atomic 持锁期间读者被阻塞，看到的必为整体生效后的状态
    assert observed["users"] == {"1": {"id": 1, "name": "alice", "age": 0}}
    assert observed["orders"] == {"10": EXPECTED_ORDERS["10"]}


def test_tx_duplicate_delivery_idempotent(tmp_path):
    _, _, target, checkpoints, applier, _ = make_tx_env(tmp_path)
    changes = tx_changes()
    r1 = applier.apply_batch(changes)
    r2 = applier.apply_batch(changes)            # 整批重复投递
    r3 = applier.apply_batch(changes[:2])        # 事务部分重复投递
    assert sorted(r1.applied_txs) == ["tx1", "tx2"]
    assert not r2.applied and not r2.applied_txs and len(r2.duplicates) == 6
    assert not r3.applied and len(r3.duplicates) == 2
    assert checkpoints.load() == 6
    assert target.rows("users") == EXPECTED_USERS
    assert target.rows("orders") == EXPECTED_ORDERS


def test_tx_out_of_order_split_batches_same_final_state(tmp_path):
    changes = tx_changes()
    _, _, target_a, _, applier_a, _ = make_tx_env(tmp_path / "a")
    applier_a.apply_batch(changes)               # 基线：按序只投递一次

    _, _, target_b, checkpoints_b, applier_b, _ = make_tx_env(tmp_path / "b")
    shuffled = changes[:]
    random.Random(7).shuffle(shuffled)
    for r in shuffled:                           # 乱序、逐条分批到达
        applier_b.apply_batch([r])
    applier_b.apply_batch(changes)               # 再整体重复一遍
    assert target_b.rows("users") == target_a.rows("users") == EXPECTED_USERS
    assert target_b.rows("orders") == target_a.rows("orders") == EXPECTED_ORDERS
    assert checkpoints_b.load() == 6


def test_tx_concurrent_commit_same_final_state(tmp_path):
    changes = tx_changes()
    _, _, target_a, _, applier_a, _ = make_tx_env(tmp_path / "a")
    applier_a.apply_batch(changes)

    _, _, target_b, checkpoints_b, applier_b, _ = make_tx_env(tmp_path / "b")
    errors = []

    def worker(batch):
        try:
            applier_b.apply_batch(batch)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    batches = [changes[3:], changes[:4], list(reversed(changes)), changes]
    threads = [threading.Thread(target=worker, args=(b,)) for b in batches]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert target_b.rows("users") == target_a.rows("users")
    assert target_b.rows("orders") == target_a.rows("orders")
    assert checkpoints_b.load() == 6


def test_tx_transient_block_retried_after_dependency_arrives(tmp_path):
    """tx_b 依赖 tx_a 创建的行：tx_b 先凑齐被暂时性阻塞，tx_a 生效后自动重试成功。"""
    _, _, target, checkpoints, applier, _ = make_tx_env(tmp_path)
    r1 = applier.apply_batch([
        rec(3, 1, Op.UPDATE, {"age": 31}, tx_id="tx_b", tx_total=2),
        rec(4, 1, Op.UPDATE, {"name": "alice2"}, tx_id="tx_b", tx_total=2),
    ])
    assert not r1.applied and not r1.applied_txs
    assert checkpoints.load() == 0               # 未消费，位点不前进
    assert target.rows("users") == {}

    r2 = applier.apply_batch([
        rec(1, 1, Op.INSERT, {"id": 1, "name": "alice"}, tx_id="tx_a", tx_total=2),
        rec(2, 10, Op.INSERT, {"id": 10, "user_id": 1, "amount": 1.0},
            table="orders", tx_id="tx_a", tx_total=2),
    ])
    assert sorted(r2.applied_txs) == ["tx_a", "tx_b"]
    assert checkpoints.load() == 4
    assert target.get("users", 1) == {"id": 1, "name": "alice2", "age": 31}


def test_tx_one_bad_record_rejects_whole_tx_and_is_retained(tmp_path):
    """组内一条非法 -> 整个事务拒绝、一条都不写入、完整留存、重投结论一致。"""
    _, _, target, checkpoints, applier, _ = make_tx_env(tmp_path)
    applier.apply_batch(tx_changes())            # 先推进到 seq 6
    bad_tx = [
        rec(7, 4, Op.INSERT, {"id": 4, "name": "dave"}, tx_id="tx3", tx_total=2),
        rec(8, 30, Op.INSERT, {"id": 30, "user_id": 4, "amount": "abc"},  # 类型非法
            table="orders", tx_id="tx3", tx_total=2),
    ]
    r = applier.apply_batch(bad_tx)
    assert len(r.rejected_txs) == 1
    txr = r.rejected_txs[0]
    assert txr.tx_id == "tx3" and txr.reason == RejectReason.TYPE_MISMATCH
    assert [rec_.seq for rec_ in txr.records] == [7, 8]   # 完整留存
    # 一条都不写入
    assert target.get("users", 4) is None
    assert target.rows("orders").get("30") is None
    assert checkpoints.load() == 8               # 死信计入已消费，位点越过

    # 重启后从磁盘仍可查看到被拒事务
    store = RejectionStore(tmp_path / "rejections.json")
    persisted = store.get("tx3")
    assert persisted is not None and persisted.reason == RejectReason.TYPE_MISMATCH
    assert [rec_.seq for rec_ in persisted.records] == [7, 8]

    # 同样内容重投：结论稳定（判重跳过，不重复留存、不重复拒绝）
    r2 = applier.apply_batch(bad_tx)
    assert not r2.rejected_txs and len(r2.duplicates) == 2
    assert len(store.list()) == 1
    # 后续正常事务继续推进
    r3 = applier.apply_batch([rec(9, 5, Op.INSERT, {"id": 5, "name": "erin"})])
    assert len(r3.applied) == 1 and checkpoints.load() == 9


def test_tx_pk_conflict_rejects_whole_tx(tmp_path):
    """事务内 insert 撞上已存在主键：整个事务（含合法记录）被拒绝。"""
    _, _, target, checkpoints, applier, _ = make_tx_env(tmp_path)
    applier.apply_batch([rec(1, 1, Op.INSERT, {"id": 1, "name": "alice"})])
    r = applier.apply_batch([
        rec(2, 1, Op.INSERT, {"id": 1, "name": "dup"}, tx_id="tx9", tx_total=2),   # 主键冲突
        rec(3, 9, Op.INSERT, {"id": 9, "name": "valid"}, tx_id="tx9", tx_total=2),
    ])
    assert [t.reason for t in r.rejected_txs] == [RejectReason.PK_CONFLICT]
    assert target.get("users", 9) is None        # 合法记录也不写入
    assert target.get("users", 1)["name"] == "alice"
    assert checkpoints.load() == 3


def test_tx_restart_with_incomplete_tx_buffered(tmp_path):
    """事务未凑齐时进程重启：缓冲不丢、目标端仍无中间态，凑齐后整体生效。"""
    _, _, target, checkpoints, applier, _ = make_tx_env(tmp_path)
    applier.apply_batch(tx_changes()[:1])        # tx1 只到一半就“宕机”
    assert checkpoints.load() == 0

    _, _, target2, checkpoints2, applier2, _ = make_tx_env(tmp_path)  # 重启
    assert target2.rows("users") == {} and target2.rows("orders") == {}
    r = applier2.apply_batch(tx_changes())       # 重投全量（含已缓冲的一半）
    assert r.applied_txs[:1] == ["tx1"]
    assert checkpoints2.load() == 6
    assert target2.rows("users") == EXPECTED_USERS
    assert target2.rows("orders") == EXPECTED_ORDERS


def test_tx_crash_after_data_flush_before_checkpoint_commit(tmp_path):
    """提交过程中出错（数据与状态已落盘、位点未提交）后重启续传：不多写不漏写。"""
    _, _, target, checkpoints, applier, _ = make_tx_env(tmp_path)
    changes = tx_changes()

    class FlakyCheckpoint(CheckpointStore):
        def __init__(self, path):
            super().__init__(path)
            self.fail_once = True

        def commit(self, seq, expected_prev):
            if self.fail_once:
                self.fail_once = False
                raise RuntimeError("模拟提交位点时进程被杀/写盘报错")
            return super().commit(seq, expected_prev)

    flaky = FlakyCheckpoint(tmp_path / "checkpoint.json")
    applier.checkpoints = flaky
    try:
        applier.apply_batch(changes)
        raise AssertionError("应当抛出注入的故障")
    except RuntimeError:
        pass
    # 数据已生效、位点未提交（位点绝不超过已生效数据之外的逆序也不允许）
    assert checkpoints.load() == 0

    # 重启：重建组件，重投全量
    _, _, target2, checkpoints2, applier2, _ = make_tx_env(tmp_path)
    assert checkpoints2.load() == 0
    r = applier2.apply_batch(changes)
    assert not r.applied and not r.applied_txs   # 全部被识别为已消费，不多写
    assert len(r.duplicates) == 6
    assert checkpoints2.load() == 6              # 位点补齐，不漏写
    assert target2.rows("users") == EXPECTED_USERS
    assert target2.rows("orders") == EXPECTED_ORDERS


def test_tx_records_survive_source_log_roundtrip(tmp_path):
    """事务标记随源端日志落盘/读回不丢失，管线可基于日志续跑事务。"""
    from sync.pipeline import run_once

    _, source, target, checkpoints, applier, _ = make_tx_env(tmp_path)
    for r in tx_changes():
        source.append(r)
    r1 = run_once(source, applier, batch_size=4)   # tx1 完整、tx2 只到一半
    assert r1.applied_txs == ["tx1"]
    assert checkpoints.load() == 2
    assert target.rows("orders") == {"10": EXPECTED_ORDERS["10"]}
    r2 = run_once(source, applier)                 # 续跑：tx2 凑齐整体生效
    assert r2.applied_txs == ["tx2"]
    assert checkpoints.load() == 6
    assert target.rows("users") == EXPECTED_USERS
    assert target.rows("orders") == EXPECTED_ORDERS
