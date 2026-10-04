"""原子落盘与崩溃恢复：提交过程写盘失败/进程被杀后重启续传。"""
import json

from helpers import make_env
from sync.models import ChangeRecord, Column, Op, TxnCommit
from sync.target import LocalTarget
from sync.checkpoint import CheckpointStore
from sync.schema_registry import SchemaRegistry
from sync.applier import SyncApplier


def _env(tmp_path):
    schemas, source, target, checkpoints, applier = make_env(tmp_path)
    schemas.register("orders", "id", [
        Column("id", "int", nullable=False),
        Column("amount", "int", nullable=False),
    ])
    return schemas, source, target, checkpoints, applier


def _rebuild(tmp_path):
    """模拟重启：从同一目录重建全部组件。"""
    schemas = SchemaRegistry()
    schemas.register("orders", "id", [
        Column("id", "int", nullable=False),
        Column("amount", "int", nullable=False),
    ])
    target = LocalTarget(tmp_path / "target.json")
    checkpoints = CheckpointStore(tmp_path / "checkpoint.json")
    applier = SyncApplier(target, checkpoints, schemas, tmp_path / "applier_state.json")
    return schemas, target, checkpoints, applier


def _txn(i, base, txn=None):
    txn = txn or f"T{i}"
    recs = [
        ChangeRecord(seq=base, table="orders", pk=100 + i, op=Op.INSERT,
                     fields={"id": 100 + i, "amount": 10 * i}, schema_version=1,
                     txn_id=txn),
        ChangeRecord(seq=base + 1, table="users", pk=100 + i, op=Op.INSERT,
                     fields={"id": 100 + i, "name": f"u{i}"}, schema_version=1,
                     txn_id=txn),
    ]
    commit = TxnCommit(seq=base + 2, txn_id=txn, members=(base, base + 1))
    return recs, commit


def test_persist_failure_leaves_no_partial_state(tmp_path):
    """journal 写盘失败：事务整体未生效；重启读到旧状态，重投成功。"""
    _, _, target, checkpoints, applier = _env(tmp_path)
    recs0, commit0 = _txn(0, 1)
    applier.apply_batch([*recs0, commit0])  # T0 正常生效，位点 3
    assert checkpoints.load() == 3

    # 注入 journal 写盘失败（模拟磁盘错误/进程在提交中被杀）
    applier._journal._fail_after_bytes = 8
    recs1, commit1 = _txn(1, 4)
    try:
        applier.apply_batch([*recs1, commit1])
        assert False, "应当抛出写盘错误"
    except OSError:
        pass

    # 重启：位点绝不能超过已真正生效的数据（仍停在 3）
    _, target2, checkpoints2, applier2 = _rebuild(tmp_path)
    assert checkpoints2.load() == 3
    assert target2.rows("orders") == {"100": {"id": 100, "amount": 0}}
    assert "101" not in target2.rows("orders")
    # 无残缺 tmp 残留
    assert not (tmp_path / "journal.json.tmp").exists()

    # 从已确认位置重投 T1：不多写也不漏写，追平到位点 6
    r = applier2.apply_batch([*recs1, commit1])
    assert r.committed_txns == ["T1"]
    assert checkpoints2.load() == 6
    assert target2.rows("orders")["101"]["amount"] == 10
    assert target2.rows("users")["101"]["name"] == "u1"


def test_persist_failure_rolls_back_in_memory_atomically(tmp_path):
    """写盘失败在同一进程内整体回滚：目标端/位点/暂存全部回到批前，无半组可见。"""
    _, _, target, checkpoints, applier = _env(tmp_path)
    # 补注册第二张跨表表，构造跨表事务
    applier.schemas.register("billing", "id", [
        Column("id", "int", nullable=False),
        Column("total", "int", nullable=False),
    ])
    recs0 = [
        ChangeRecord(seq=1, table="orders", pk=100, op=Op.INSERT,
                     fields={"id": 100, "amount": 1}, schema_version=1, txn_id="T0"),
        ChangeRecord(seq=2, table="users", pk=100, op=Op.INSERT,
                     fields={"id": 100, "name": "u0"}, schema_version=1, txn_id="T0"),
    ]
    commit0 = TxnCommit(seq=3, txn_id="T0", members=(1, 2))
    applier.apply_batch([*recs0, commit0])
    assert checkpoints.load() == 3

    bad = [
        ChangeRecord(seq=4, table="orders", pk=101, op=Op.INSERT,
                     fields={"id": 101, "amount": 9}, schema_version=1, txn_id="T1"),
        ChangeRecord(seq=5, table="billing", pk=1, op=Op.INSERT,
                     fields={"id": 1, "total": 9}, schema_version=1, txn_id="T1"),
    ]
    commit1 = TxnCommit(seq=6, txn_id="T1", members=(4, 5))
    applier._journal._fail_after_bytes = 8
    try:
        applier.apply_batch([*bad, commit1])
        assert False, "应当抛出写盘错误"
    except OSError:
        pass

    # 失败后同一进程内：绝不能出现 orders 有行、billing 无行的半组状态
    assert "101" not in target.rows("orders")
    assert target.rows("billing") == {}
    # T0 旧状态完好
    assert target.rows("orders") == {"100": {"id": 100, "amount": 1}}
    assert checkpoints.load() == 3
    # 一致视图同样停在批前，且数据与位点自洽
    view = applier.consistent_view()
    assert view.confirmed_seq == 3
    assert set(view.tables.get("orders", {})) == {"100"}
    assert "billing" not in view.tables or view.tables["billing"] == {}

    # 恢复写盘后，不重启直接重投：整组正确生效
    applier._journal._fail_after_bytes = None
    r = applier.apply_batch([*bad, commit1])
    assert r.committed_txns == ["T1"]
    assert checkpoints.load() == 6
    assert target.rows("orders")["101"] == {"id": 101, "amount": 9}
    assert target.rows("billing")["1"] == {"id": 1, "total": 9}


def test_restart_mid_txn_resumes_and_commits_atomically(tmp_path):
    """成员已部分暂存（未到提交标记）时重启：续传后事务整体生效。"""
    _, _, target, checkpoints, applier = _env(tmp_path)
    recs0, commit0 = _txn(0, 1)
    applier.apply_batch([*recs0, commit0])
    recs1, commit1 = _txn(1, 4)
    applier.apply_batch([recs1[0]])  # T1 只到第一条成员
    assert target.rows("orders") == {"100": {"id": 100, "amount": 0}}
    assert checkpoints.load() == 3

    # 重启后补齐 T1 剩余成员 + 提交标记
    _, target2, checkpoints2, applier2 = _rebuild(tmp_path)
    r = applier2.apply_batch([recs1[1], commit1])
    assert r.committed_txns == ["T1"]
    assert checkpoints2.load() == 6
    assert target2.rows("orders")["101"] == {"id": 101, "amount": 10}
    assert target2.rows("users")["101"] == {"id": 101, "name": "u1", "age": 0}


def test_restart_recovers_dead_letters(tmp_path):
    """被拒事务在重启后仍可查询；其成员位点已消费，不会被重复处理。"""
    _, _, _, checkpoints, applier = _env(tmp_path)
    bad = ChangeRecord(seq=1, table="orders", pk=1, op=Op.INSERT,
                       fields={"id": 1}, schema_version=1, txn_id="BAD")  # amount 缺失
    commit = TxnCommit(seq=2, txn_id="BAD", members=(1,))
    applier.apply_batch([bad, commit])
    assert checkpoints.load() == 2

    _, target2, checkpoints2, applier2 = _rebuild(tmp_path)
    assert checkpoints2.load() == 2
    dead = applier2.rejected_transactions()
    assert len(dead) == 1 and dead[0].txn_id == "BAD"
    # 死信重投仍被判定为重复，结论稳定
    r = applier2.apply_batch([bad, commit])
    assert not r.applied and not r.rejected_txns
    assert target2.rows("orders") == {}


def test_restart_sidecar_files_lag_behind_journal(tmp_path):
    """journal 已生效但 checkpoint/target 旁路文件尚未重写即崩溃：以 journal 为准。"""
    _, _, target, checkpoints, applier = _env(tmp_path)
    recs0, commit0 = _txn(0, 1)
    applier.apply_batch([*recs0, commit0])
    assert checkpoints.load() == 3

    # 模拟“快照已原子落盘、旁路文件还没来得及重写”的崩溃窗口
    (tmp_path / "checkpoint.json").write_text(json.dumps({"confirmed_seq": 0}))
    LocalTarget(tmp_path / "lag_target.json")  # 仅占位
    (tmp_path / "target.json").write_text(json.dumps({}))

    _, target2, checkpoints2, applier2 = _rebuild(tmp_path)
    assert checkpoints2.load() == 3            # 位点从 journal 对齐回来
    assert target2.rows("orders") == {"100": {"id": 100, "amount": 0}}
    assert target2.rows("users") == {"100": {"id": 100, "name": "u0", "age": 0}}
    # 续传不多写不漏写
    recs1, commit1 = _txn(1, 4)
    r = applier2.apply_batch([*recs1, commit1])
    assert r.committed_txns == ["T1"] and checkpoints2.load() == 6
