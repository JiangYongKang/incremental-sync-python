"""上游表结构的兼容演进。"""
import pytest

from helpers import USER_COLUMNS, make_env, rec
from sync.models import Column, Op, RejectReason
from sync.schema_registry import SchemaIncompatibleError


def test_add_column_default_is_deterministic(tmp_path):
    schemas, _, target, _, applier = make_env(tmp_path)
    applier.apply_batch([rec(1, 1, Op.INSERT, {"id": 1, "name": "a"})])
    # 上游新增列，缺省值确定
    schemas.evolve("users", [*USER_COLUMNS, Column("email", "str", default="")])
    # 旧版本记录（无 email）：按缺省补齐
    applier.apply_batch([rec(2, 2, Op.INSERT, {"id": 2, "name": "b"}, schema_version=1)])
    # 新版本记录（带 email）
    applier.apply_batch([rec(3, 3, Op.INSERT, {"id": 3, "name": "c", "email": "c@x"},
                               schema_version=2)])
    rows = target.rows("users")
    assert rows["2"]["email"] == ""          # 缺省语义稳定
    assert rows["3"]["email"] == "c@x"
    # 旧行不被追溯改写（不静默回填）；读取方按当前结构自行补缺省
    old = target.get("users", 1)
    assert "email" not in old


def test_drop_or_rename_column_explicitly_rejected(tmp_path):
    schemas, _, _, _, _ = make_env(tmp_path)
    with pytest.raises(SchemaIncompatibleError, match="删除或改名"):
        schemas.evolve("users", [USER_COLUMNS[0], USER_COLUMNS[2]])  # 删掉 name
    with pytest.raises(SchemaIncompatibleError, match="删除或改名"):
        # 改名 = 旧列消失 + 新列出现
        schemas.evolve("users", [USER_COLUMNS[0], Column("full_name", "str"),
                                 USER_COLUMNS[2]])
    with pytest.raises(SchemaIncompatibleError, match="类型/可空性"):
        schemas.evolve("users", [Column("id", "str", nullable=False),
                                 *USER_COLUMNS[1:]])
    assert schemas.current("users").version == 1  # 不兼容变化未被静默接受


def test_record_with_unknown_field_rejected_not_misaligned(tmp_path):
    """记录带有当前结构中不存在的字段：显式拒绝，绝不静默错列写入。"""
    _, _, target, _, applier = make_env(tmp_path)
    result = applier.apply_batch([
        rec(1, 1, Op.INSERT, {"id": 1, "name": "a", "ghost_col": "x"})])
    assert [rj.reason for rj in result.rejected] == [RejectReason.SCHEMA_INCOMPATIBLE]
    assert target.rows("users") == {}  # 未写入，更未错列
