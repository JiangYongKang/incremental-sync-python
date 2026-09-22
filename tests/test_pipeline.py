"""端到端：源端日志 -> 管线 -> 目标端，含重启续传与重复运行。"""
from helpers import make_env, rec, sample_changes
from sync.models import Op
from sync.pipeline import run_once


def test_pipeline_end_to_end_with_restart(tmp_path):
    schemas, source, target, checkpoints, applier = make_env(tmp_path)
    for r in sample_changes():
        source.append(r)

    r1 = run_once(source, applier, batch_size=3)   # 只同步一批后“宕机”
    assert r1.committed_seq == 3

    # 重启：重建组件，重复运行管线直到追平
    _, source2, target2, checkpoints2, applier2 = make_env(tmp_path)
    r2 = run_once(source2, applier2)               # 续传剩余
    r3 = run_once(source2, applier2)               # 空跑：幂等
    assert sorted(r.seq for r in r2.applied) == [4, 5]
    assert not r3.applied and not r3.duplicates and not r3.rejected
    assert checkpoints2.load() == 5
    assert target2.rows("users") == {"2": {"id": 2, "name": "bobby", "age": 0}}

    # 上游继续写入，管线继续从已确认位点推进
    source2.append(rec(6, 3, Op.INSERT, {"id": 3, "name": "carol"}))
    r4 = run_once(source2, applier2)
    assert [r.seq for r in r4.applied] == [6]
    assert checkpoints2.load() == 6
    assert target2.get("users", 3)["name"] == "carol"
