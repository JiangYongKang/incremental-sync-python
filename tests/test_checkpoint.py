"""位点与断点：重启续传、回退与跳段拒绝、重复消费不重复不丢失。"""
import pytest

from helpers import make_env, rec, sample_changes
from sync.applier import SyncApplier
from sync.checkpoint import CheckpointError, CheckpointStore
from sync.models import Op


def test_restart_resumes_from_confirmed(tmp_path):
    schemas, source, target, checkpoints, applier = make_env(tmp_path)
    changes = sample_changes()
    for r in changes:
        source.append(r)
    # 第一轮：只同步前 3 条后“进程退出”
    applier.apply_batch(source.read_from(0, limit=3))
    assert checkpoints.load() == 3

    # 模拟重启：同一目录重建全部组件
    _, _, target2, checkpoints2, applier2 = make_env(tmp_path)
    assert checkpoints2.load() == 3
    # 从已确认位点继续，且把已确认部分也重复投递一遍
    result = applier2.apply_batch(source.read_from(0))
    assert sorted(r.seq for r in result.applied) == [4, 5]
    assert sorted(r.seq for r in result.duplicates) == [1, 2, 3]
    assert checkpoints2.load() == 5
    assert target2.rows("users") == {"2": {"id": 2, "name": "bobby", "age": 0}}


def test_restart_with_unconfirmed_gap(tmp_path):
    """缺段时位点停在空洞前，重启后补齐空洞再继续。"""
    _, _, _, checkpoints, applier = make_env(tmp_path)
    applier.apply_batch([rec(1, 1, Op.INSERT, {"id": 1, "name": "a"}),
                         rec(3, 1, Op.UPDATE, {"name": "c"})])  # 缺 seq=2
    assert checkpoints.load() == 1  # 位点停在空洞前

    _, _, target2, checkpoints2, applier2 = make_env(tmp_path)  # 重启
    assert checkpoints2.load() == 1
    applier2.apply_batch([rec(2, 1, Op.UPDATE, {"name": "b"})])  # 补齐空洞
    assert checkpoints2.load() == 3
    # seq=3 在重启前已应用，重启后不得重复应用；最终值由最大 seq 决定
    assert target2.rows("users") == {"1": {"id": 1, "name": "c", "age": 0}}


def test_checkpoint_rollback_rejected(tmp_path):
    cp = CheckpointStore(tmp_path / "cp.json")
    cp.commit(5, expected_prev=0)
    with pytest.raises(CheckpointError, match="回退"):
        cp.commit(3, expected_prev=5)
    with pytest.raises(CheckpointError, match="回退"):
        cp.commit(5, expected_prev=5)  # 原地不动也算回退
    assert cp.load() == 5


def test_checkpoint_gap_rejected(tmp_path):
    cp = CheckpointStore(tmp_path / "cp.json")
    cp.commit(5, expected_prev=0)
    with pytest.raises(CheckpointError, match="跳段"):
        cp.commit(8, expected_prev=0)   # 声明的前驱与持久化位点不一致
    with pytest.raises(CheckpointError, match="跳段"):
        cp.commit(8, expected_prev=3)
    assert cp.load() == 5


def test_source_rejects_noncontiguous_append(tmp_path):
    _, source, _, _, _ = make_env(tmp_path)
    source.append(rec(1, 1, Op.INSERT, {"id": 1, "name": "a"}))
    with pytest.raises(ValueError, match="不连续"):
        source.append(rec(3, 2, Op.INSERT, {"id": 2, "name": "b"}))
