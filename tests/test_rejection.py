"""非法记录不得静默丢弃，拒绝原因可区分。"""
from helpers import make_env, rec
from sync.models import Op, RejectReason


def test_reject_reasons_are_distinguishable(tmp_path):
    _, _, target, _, applier = make_env(tmp_path)
    applier.apply_batch([rec(1, 1, Op.INSERT, {"id": 1, "name": "a"})])
    result = applier.apply_batch([
        rec(2, 1, Op.INSERT, {"id": 1, "name": "dup"}),      # 主键冲突
        rec(3, 2, Op.INSERT, {"id": 2}),                     # 必填字段 name 缺失
        rec(4, 3, Op.INSERT, {"id": 3, "name": 123}),        # name 类型不匹配
        rec(5, 4, Op.INSERT, {"id": 4, "name": "x"}, table="ghost"),  # 未注册表
        rec(6, 5, Op.INSERT, {"id": 5, "name": "y"}, schema_version=99),  # 结构版本不兼容
    ])
    reasons = {rj.record.seq: rj.reason for rj in result.rejected}
    assert reasons == {
        2: RejectReason.PK_CONFLICT,
        3: RejectReason.MISSING_FIELD,
        4: RejectReason.TYPE_MISMATCH,
        5: RejectReason.UNKNOWN_TABLE,
        6: RejectReason.SCHEMA_INCOMPATIBLE,
    }
    # 被拒绝的记录不写入目标端，也不静默丢失（均出现在 rejected 中）
    assert target.rows("users") == {"1": {"id": 1, "name": "a", "age": 0}}
    assert len(result.rejected) == 5


def test_terminal_rejection_is_deterministic_on_redelivery(tmp_path):
    """死信重投结论相同，且位点可越过死信继续前进。"""
    _, _, _, checkpoints, applier = make_env(tmp_path)
    r1 = applier.apply_batch([rec(1, 1, Op.INSERT, {"id": 1}),       # 缺 name
                              rec(2, 2, Op.INSERT, {"id": 2, "name": "b"})])
    assert [rj.reason for rj in r1.rejected] == [RejectReason.MISSING_FIELD]
    assert checkpoints.load() == 2  # 死信计入已消费，位点连续推进
    r2 = applier.apply_batch([rec(1, 1, Op.INSERT, {"id": 1})])  # 重投
    assert not r2.applied and not r2.rejected and len(r2.duplicates) == 1


def test_transient_pk_conflict_retryable(tmp_path):
    """update 先于 insert 到达：拒绝为暂时性，重投后成功，数据不丢。"""
    _, _, target, checkpoints, applier = make_env(tmp_path)
    r1 = applier.apply_batch([rec(2, 1, Op.UPDATE, {"name": "updated"})])
    assert [rj.reason for rj in r1.rejected] == [RejectReason.PK_CONFLICT]
    assert checkpoints.load() == 0  # 未消费，位点不前进
    r2 = applier.apply_batch([rec(1, 1, Op.INSERT, {"id": 1, "name": "orig"}),
                              rec(2, 1, Op.UPDATE, {"name": "updated"})])
    assert len(r2.applied) == 2
    assert checkpoints.load() == 2
    assert target.rows("users") == {"1": {"id": 1, "name": "updated", "age": 0}}
