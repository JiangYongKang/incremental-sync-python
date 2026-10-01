"""事务内任一成员非法：整事务拒绝、不留中间态、死信完整留存、重投稳定。"""
import json

from helpers import make_env
from sync.models import ChangeRecord, Column, Op, RejectReason, TxnCommit
from sync.source import record_to_dict


def _env(tmp_path):
    schemas, source, target, checkpoints, applier = make_env(tmp_path)
    schemas.register("orders", "id", [
        Column("id", "int", nullable=False),
        Column("user_id", "int", nullable=False),
        Column("amount", "int", nullable=True, default=0),
    ])
    schemas.register("billing", "id", [
        Column("id", "int", nullable=False),
        Column("total", "int", nullable=False),
    ])
    return schemas, source, target, checkpoints, applier


def _r(seq, table, pk, op, fields, txn="T1", sv=1):
    return ChangeRecord(seq=seq, table=table, pk=pk, op=op, fields=fields,
                        schema_version=sv, txn_id=txn)


def _commit(seq=4, txn="T1", members=(1, 2, 3)):
    return TxnCommit(seq=seq, txn_id=txn, members=members)


def test_one_bad_member_aborts_whole_txn(tmp_path):
    _, _, target, checkpoints, applier = _env(tmp_path)
    recs = [
        _r(1, "orders", 10, Op.INSERT, {"id": 10, "user_id": 1, "amount": 100}),
        _r(2, "billing", 1, Op.INSERT, {"id": 1, "total": 100}),
        _r(3, "orders", 11, Op.INSERT, {"id": 11}),  # 必填 user_id 缺失
    ]
    result = applier.apply_batch([*recs, _commit()])
    # 整事务拒绝，前两条也不得写入（无中间态）
    assert result.committed_txns == []
    assert result.applied == []
    assert [rt.primary_reason for rt in result.rejected_txns] == [RejectReason.MISSING_FIELD]
    assert target.rows("orders") == {} and target.rows("billing") == {}
    # 死信消费位点：成员 1,2,3 + 提交标记 4 全部越过
    assert checkpoints.load() == 4
    # 拒绝原因可区分（多个非法成员时全部上报）
    reasons = {rj.record.seq: rj.reason for rj in result.rejected_txns[0].reasons}
    assert reasons == {3: RejectReason.MISSING_FIELD}


def test_txn_pk_conflict_and_type_mismatch(tmp_path):
    _, _, target, _, applier = _env(tmp_path)
    # 先有一行 orders/10
    applier.apply_batch([
        _r(1, "orders", 10, Op.INSERT, {"id": 10, "user_id": 1, "amount": 1}, "T0"),
        _commit(2, "T0", (1,)),
    ])
    recs = [
        _r(3, "orders", 10, Op.INSERT, {"id": 10, "user_id": 9, "amount": 9}, "T1"),  # 主键冲突
        _r(4, "billing", 2, Op.INSERT, {"id": 2, "total": "x"}, "T1"),              # 类型不符
    ]
    result = applier.apply_batch([*recs, _commit(5, "T1", (3, 4))])
    txn = result.rejected_txns[0]
    assert target.rows("billing") == {}
    assert target.rows("orders") == {"10": {"id": 10, "user_id": 1, "amount": 1}}
    reasons = sorted((rj.reason for rj in txn.reasons), key=lambda x: x.value)
    assert reasons == [RejectReason.PK_CONFLICT, RejectReason.TYPE_MISMATCH]
    # 死信的 primary_reason 稳定（按成员 seq 排序后第一个）
    assert txn.primary_reason == RejectReason.PK_CONFLICT


def test_rejected_txn_is_retained_and_stable(tmp_path):
    _, _, _, checkpoints, applier = _env(tmp_path)
    recs = [
        _r(1, "orders", 10, Op.INSERT, {"id": 10, "user_id": 1, "amount": 100}),
        _r(2, "billing", 1, Op.INSERT, {"id": 1, "total": 100}),
        _r(3, "orders", 11, Op.INSERT, {"id": 11}),  # 非法
    ]
    commit = _commit()
    r1 = applier.apply_batch([*recs, commit])
    fp = r1.rejected_txns[0].fingerprint
    # 完整留存：查询接口与死信视图文件都能看到全部成员和原因
    dead = applier.rejected_transactions()
    assert len(dead) == 1
    assert {r.seq for r in dead[0].records} == {1, 2, 3}
    view_file = tmp_path / "dead_letters.json"
    view = json.loads(view_file.read_text(encoding="utf-8"))
    assert view[0]["txn_id"] == "T1"
    assert len(view[0]["records"]) == 3
    assert {r["seq"] for r in view[0]["records"]} == {1, 2, 3}
    # 同样内容重投：结论相同（指纹一致），不重复写死信、不生效
    r2 = applier.apply_batch([*recs, commit])
    assert not r2.rejected_txns and not r2.applied
    assert len(applier.rejected_transactions()) == 1
    assert applier.rejected_transactions()[0].fingerprint == fp
    assert checkpoints.load() == 4


def test_transient_pk_conflict_inside_txn_blocks_then_succeeds(tmp_path):
    """事务成员 update 了尚不存在的行（乱序）：暂时性拒绝，位点不动，补齐后成功。"""
    _, _, target, checkpoints, applier = _env(tmp_path)
    # T1：先 insert orders/20 再 update billing/1，但前置隐式 insert billing 未到
    recs = [
        _r(2, "orders", 20, Op.INSERT, {"id": 20, "user_id": 1, "amount": 5}, "T1"),
        _r(3, "billing", 1, Op.UPDATE, {"total": 50}, "T1"),
    ]
    r1 = applier.apply_batch([*recs, _commit(4, "T1", (2, 3))])
    assert [rj.reason for rj in r1.rejected] == [RejectReason.PK_CONFLICT]
    assert not r1.rejected_txns  # 暂时性，不进死信
    assert checkpoints.load() == 0
    assert target.rows("orders") == {}
    # 前序变更 seq=1 到达后整体重投，成功且无中间态遗留
    pre = ChangeRecord(seq=1, table="billing", pk=1, op=Op.INSERT,
                       fields={"id": 1, "total": 0}, schema_version=1)
    r2 = applier.apply_batch([pre, *recs, _commit(4, "T1", (2, 3))])
    assert r2.committed_txns == ["T1"]
    assert checkpoints.load() == 4
    assert target.rows("billing") == {"1": {"id": 1, "total": 50}}
