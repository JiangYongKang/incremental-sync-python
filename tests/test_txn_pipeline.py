"""端到端：源端事务流水 -> 管线 -> 目标端，含分批边界与重启续传。"""
from helpers import make_env
from sync.models import ChangeRecord, Column, Op
from sync.pipeline import run_once


def _setup(schemas):
    schemas.register("orders", "id", [
        Column("id", "int", nullable=False),
        Column("user_id", "int", nullable=False),
        Column("amount", "int", nullable=False),
    ])


def _r(table, pk, fields, txn):
    return ChangeRecord(seq=0, table=table, pk=pk, op=Op.INSERT,
                        fields=fields, schema_version=1, txn_id=txn)


def test_pipeline_txn_split_across_batches_and_restart(tmp_path):
    schemas, source, target, checkpoints, applier = make_env(tmp_path)
    _setup(schemas)
    # 事务 T1：成员 seq=1,2，提交标记 seq=3；事务 T2：seq=4,5，提交 seq=6
    source.append_txn("T1", [
        _r("orders", 10, {"id": 10, "user_id": 1, "amount": 100}, "T1"),
        _r("users", 10, {"id": 10, "name": "a"}, "T1"),
    ])
    source.append_txn("T2", [
        _r("orders", 11, {"id": 11, "user_id": 2, "amount": 200}, "T2"),
        _r("users", 11, {"id": 11, "name": "b"}, "T2"),
    ])
    # 读批边界切在事务内部：第一批只含 T1 的 2 条成员（没有提交标记），
    # 目标端看不到半成品；下一轮读到提交标记后整组生效。
    r1 = run_once(source, applier, batch_size=2)
    assert r1.committed_txns == [] and target.rows("orders") == {}
    assert checkpoints.load() == 0
    r2 = run_once(source, applier, batch_size=3)
    assert r2.committed_txns == ["T1"]
    assert checkpoints.load() == 3
    # “宕机”后重建并追平：表结构由 journal 快照恢复，无需重新注册
    schemas2, source2, target2, checkpoints2, applier2 = make_env(tmp_path)
    assert schemas2.current("orders").version == 1
    r3 = run_once(source2, applier2)  # 剩余 T2
    assert r3.committed_txns == ["T2"]
    assert checkpoints2.load() == 6
    assert target2.rows("orders") == {
        "10": {"id": 10, "user_id": 1, "amount": 100},
        "11": {"id": 11, "user_id": 2, "amount": 200},
    }
    # 空跑幂等
    r4 = run_once(source2, applier2)
    assert not r4.applied and not r4.duplicates and not r4.rejected
