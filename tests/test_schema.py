"""表结构兼容演进规则测试。"""

from __future__ import annotations

import pytest

from sync.errors import SchemaIncompatibleError, SyncError
from sync.models import Column, TableSchema
from sync.schema_registry import SchemaRegistry, check_value


def v1() -> TableSchema:
    return TableSchema(
        "users", 1,
        (Column("id", "int", False),
         Column("name", "str", False),
         Column("age", "int", True)),
        ("id",),
    )


def test_add_nullable_column_is_compatible():
    st: dict = {"schemas": {}}
    r = SchemaRegistry()
    r.register_initial(st, v1())
    v2 = TableSchema("users", 2,
                     v1().columns + (Column("city", "str", True),), ("id",))
    added, defaults = r.register_change(st, v2)
    assert added == ["city"]
    assert defaults == {"city": None}
    assert r.get(st, "users", 2).column("city") is not None


def test_add_not_null_column_requires_default():
    st: dict = {"schemas": {}}
    r = SchemaRegistry()
    r.register_initial(st, v1())
    bad = TableSchema("users", 2,
                      v1().columns + (Column("tenant", "str", False),), ("id",))
    with pytest.raises(SchemaIncompatibleError, match="NOT NULL without a default"):
        r.register_change(st, bad)


def test_add_not_null_column_with_default_is_compatible():
    st: dict = {"schemas": {}}
    r = SchemaRegistry()
    r.register_initial(st, v1())
    v2 = TableSchema("users", 2,
                     v1().columns + (Column("level", "int", False, 1),), ("id",))
    added, defaults = r.register_change(st, v2)
    assert defaults == {"level": 1}


def test_drop_column_is_incompatible():
    st: dict = {"schemas": {}}
    r = SchemaRegistry()
    r.register_initial(st, v1())
    v2 = TableSchema("users", 2,
                     (Column("id", "int", False), Column("name", "str", False)),
                     ("id",))
    with pytest.raises(SchemaIncompatibleError, match="dropped or renamed"):
        r.register_change(st, v2)


def test_rename_looks_like_drop_plus_add_and_is_rejected():
    st: dict = {"schemas": {}}
    r = SchemaRegistry()
    r.register_initial(st, v1())
    # age -> years 在数据层面无法与"删 age、加 years"区分，必须拒绝
    cols = (Column("id", "int", False), Column("name", "str", False),
            Column("years", "int", True))
    with pytest.raises(SchemaIncompatibleError, match="dropped or renamed"):
        r.register_change(st, TableSchema("users", 2, cols, ("id",)))


def test_type_widening_int_to_float_ok_others_rejected():
    st: dict = {"schemas": {}}
    r = SchemaRegistry()
    r.register_initial(st, v1())
    wide = TableSchema(
        "users", 2,
        (Column("id", "int", False), Column("name", "str", False),
         Column("age", "float", True)),
        ("id",),
    )
    assert r.register_change(st, wide)[0] == []

    narrow = TableSchema(
        "users", 3,
        (Column("id", "str", False), Column("name", "str", False),
         Column("age", "float", True)),
        ("id",),
    )
    with pytest.raises(SchemaIncompatibleError, match="type narrowed"):
        r.register_change(st, narrow)


def test_primary_key_change_rejected():
    st: dict = {"schemas": {}}
    r = SchemaRegistry()
    r.register_initial(st, v1())
    v2 = TableSchema(
        "users", 2,
        v1().columns + (Column("uuid", "str", True),),
        ("id", "uuid"),
    )
    with pytest.raises(SchemaIncompatibleError, match="primary key changed"):
        r.register_change(st, v2)


def test_version_must_be_adjacent():
    st: dict = {"schemas": {}}
    r = SchemaRegistry()
    r.register_initial(st, v1())
    v3 = TableSchema("users", 3,
                     v1().columns + (Column("city", "str", True),), ("id",))
    with pytest.raises(SyncError, match="must be exactly 2"):
        r.register_change(st, v3)


def test_tighten_nullable_without_default_rejected():
    st: dict = {"schemas": {}}
    r = SchemaRegistry()
    r.register_initial(st, v1())
    v2 = TableSchema(
        "users", 2,
        (Column("id", "int", False), Column("name", "str", False),
         Column("age", "int", False)),
        ("id",),
    )
    with pytest.raises(SchemaIncompatibleError, match="tightened to NOT NULL"):
        r.register_change(st, v2)


def test_strict_type_checking_no_implicit_coercion():
    assert check_value(Column("x", "int", False), "1") is not None
    assert check_value(Column("x", "int", False), True) is not None  # bool 不算 int
    assert check_value(Column("x", "int", False), None) is not None
    assert check_value(Column("x", "int", True), None) is None
    assert check_value(Column("x", "float", True), 3) is None
    assert check_value(Column("x", "bool", True), 0) is not None
