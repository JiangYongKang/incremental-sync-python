"""事务化同步：跨表原子可见、重复投递、乱序分批、并发提交。"""
import random
import threading

from helpers import make_env
from sync.models import ChangeRecord, Column, Op, RejectReason
from sync.source import LocalSource


def _register_orders(schemas):
    schemas.register("orders", "id", [
        Column("id", "int", nullable=False),
        Column("user_id", "int", nullable=False),
        Column("amount", "int", nullable=False),
    ])
    schemas.register("billing", "id", [
        Column("id", "int", nullable=False),
        Column("total", "int", nullable=False),
    ])


def _env(tmp_path):
    schemas, source, target, checkpoints, applier = make_env(tmp_path)
    _register_orders(schemas)
    return schemas, source, target, checkpoints, applier


def _r(seq, table, pk, op, fields, txn=None, sv=1):
    return ChangeRecord(seq=seq, table=table, pk=pk, op=op, fields=fields,
                        schema_version=sv, txn_id=txn)


def test_cross_table_txn_commits_atomically(tmp_path):
    _, _, target, checkpoints, applier = _env(tmp_path)
    recs = [
        _r(1, "orders", 10, Op.INSERT, {"id": 10, "user_id": 1, "amount": 100}, "T1"),
        _r(2, "billing", 1, Op.INSERT, {"id": 1, "total": 100}, "T1"),
    ]
    # 只到成员、未到提交标记：暂存，目标端完全看不到
    r0 = applier.apply_batch(recs)
    assert r0.staged_txns == ["T1"]
    assert r0.committed_txns == []
    assert target.rows("orders") == {} and target.rows("billing") == {}
    assert checkpoints.load() == 0

    # 提交标记到达：两条一起生效，位点越过成员+提交标记(seq=3)
    from sync.models import TxnCommit
    r1 = applier.apply_batch([TxnCommit(seq=3, txn_id="T1", members=(1, 2))])
    assert r1.committed_txns == ["T1"]
    assert sorted(r.seq for r in r1.applied) == [1, 2]
    assert checkpoints.load() == 3
    assert target.rows("orders")["10"]["amount"] == 100
    assert target.rows("billing")["1"]["total"] == 100


def test_txn_duplicate_delivery_is_idempotent(tmp_path):
    _, _, target, checkpoints, applier = _env(tmp_path)
    from sync.models import TxnCommit
    recs = [
        _r(1, "orders", 10, Op.INSERT, {"id": 10, "user_id": 1, "amount": 100}, "T1"),
        _r(2, "billing", 1, Op.INSERT, {"id": 1, "total": 100}, "T1"),
    ]
    commit = TxnCommit(seq=3, txn_id="T1", members=(1, 2))
    applier.apply_batch([*recs, commit])
    # 整事务（含提交标记）重复投递
    r2 = applier.apply_batch([*recs, commit])
    assert not r2.applied and not r2.rejected_txns and not r2.committed_txns
    assert len(r2.duplicates) == 2  # 两条记录均判重
    # 只重复其中部分成员
    r3 = applier.apply_batch([recs[0]])
    assert not r3.applied and len(r3.duplicates) == 1
    assert checkpoints.load() == 3
    assert len(target.rows("orders")) == 1 and len(target.rows("billing")) == 1


def test_txn_split_out_of_order_batches(tmp_path):
    """事务成员乱序、分批、与提交标记穿插到达，最终与按序一次投递一致。"""
    _, _, target_a, _, applier_a = _env(tmp_path / "a")
    from sync.models import TxnCommit
    recs = [
        _r(1, "orders", 10, Op.INSERT, {"id": 10, "user_id": 1, "amount": 100}, "T1"),
        _r(2, "orders", 11, Op.INSERT, {"id": 11, "user_id": 2, "amount": 200}, "T1"),
        _r(3, "billing", 1, Op.INSERT, {"id": 1, "total": 300}, "T1"),
    ]
    commit = TxnCommit(seq=4, txn_id="T1", members=(1, 2, 3))
    applier_a.apply_batch([*recs, commit])

    _, _, target_b, cp_b, applier_b = _env(tmp_path / "b")
    # 先到提交标记 + 部分成员（缺 seq=2），不能生效
    r1 = applier_b.apply_batch([commit, recs[2]])
    assert r1.committed_txns == [] and r1.applied == []
    assert cp_b.load() == 0
    # 乱序补齐剩余成员
    applier_b.apply_batch([recs[0]])
    r3 = applier_b.apply_batch([recs[1]])
    assert r3.committed_txns == ["T1"]
    assert cp_b.load() == 4
    assert target_b.all_tables() == target_a.all_tables()


def test_concurrent_txn_submission_same_final_state(tmp_path):
    _, _, target_a, _, applier_a = _env(tmp_path / "a")
    from sync.models import TxnCommit
    txns = []
    seq = 1
    for i in range(6):
        members = (
            _r(seq, "orders", 100 + i, Op.INSERT,
               {"id": 100 + i, "user_id": i, "amount": 10 * i}, f"T{i}"),
            _r(seq + 1, "billing", 100 + i, Op.INSERT,
               {"id": 100 + i, "total": 10 * i}, f"T{i}"),
        )
        commit = TxnCommit(seq=seq + 2, txn_id=f"T{i}", members=(seq, seq + 1))
        txns.append((members, commit))
        seq += 3
    for members, commit in txns:
        applier_a.apply_batch([*members, commit])

    _, _, target_b, cp_b, applier_b = _env(tmp_path / "b")
    errors = []

    def worker(batch):
        try:
            applier_b.apply_batch(batch)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    # 4 个线程提交互相重叠、乱序、重复的事务批次
    batches = []
    for members, commit in txns:
        batches.append([*members, commit])
    batches.append([e for m, c in txns for e in (*m, c)])  # 全量重复
    batches.append([c for _, c in txns])                    # 只有提交标记
    batches.append([m for members, _ in txns for m in members])  # 只有成员
    threads = [threading.Thread(target=worker, args=(b,)) for b in batches]
    random.Random(7).shuffle(threads)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert target_b.all_tables() == target_a.all_tables()
    assert cp_b.load() == 18  # 6 事务 × (2 成员 + 1 提交)
