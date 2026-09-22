"""测试公共构造：本地源端、目标端、位点、结构注册与应用器。"""
from pathlib import Path

from sync.applier import SyncApplier
from sync.checkpoint import CheckpointStore
from sync.models import ChangeRecord, Column, Op
from sync.schema_registry import SchemaRegistry
from sync.source import LocalSource
from sync.target import LocalTarget

USER_COLUMNS = [
    Column("id", "int", nullable=False),
    Column("name", "str", nullable=False),
    Column("age", "int", nullable=True, default=0),
]


def make_env(tmp_path: Path):
    schemas = SchemaRegistry()
    schemas.register("users", "id", USER_COLUMNS)
    source = LocalSource(tmp_path / "source.jsonl")
    target = LocalTarget(tmp_path / "target.json")
    checkpoints = CheckpointStore(tmp_path / "checkpoint.json")
    applier = SyncApplier(target, checkpoints, schemas, tmp_path / "applier_state.json")
    return schemas, source, target, checkpoints, applier


def rec(seq, pk, op, fields, table="users", schema_version=1):
    return ChangeRecord(seq=seq, table=table, pk=pk, op=op,
                        fields=fields, schema_version=schema_version)


def sample_changes():
    """5 条变更的基线流水。"""
    return [
        rec(1, 1, Op.INSERT, {"id": 1, "name": "alice", "age": 30}),
        rec(2, 2, Op.INSERT, {"id": 2, "name": "bob"}),
        rec(3, 1, Op.UPDATE, {"age": 31}),
        rec(4, 2, Op.UPDATE, {"name": "bobby"}),
        rec(5, 1, Op.DELETE, {}),
    ]
