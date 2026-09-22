"""表结构版本注册与兼容演进规则。

兼容规则（新结构相对当前结构）：

================================  ==========================================
变化                               判定
================================  ==========================================
新增列（含非空列，但必须带默认值）  兼容；旧记录读出时按列默认值稳定补齐
删除列                             **不兼容**（防止目标端静默错列/数据丢失）
列改名                             **不兼容**（在数据层面表现为"删列+加列"）
修改列类型                         仅允许 int -> float 的安全拓宽，其余不兼容
主键变化                           **不兼容**
把可空列收紧为非空且无默认值        **不兼容**（旧数据可能为 NULL）
修改列默认值 / 放宽为可空           兼容（默认值变化只影响之后补齐的旧行）
版本号不连续 / 非递增               **拒绝**（防止乱序登记造成版本错位）
================================  ==========================================

不兼容的结构变化不会被静默接受，而是抛出 :class:`SchemaIncompatibleError`，
整批提交被拒绝、位点不推进，等待上游显式处理（迁移或更正结构声明）。
"""

from __future__ import annotations

from typing import Any

from sync.errors import SchemaIncompatibleError, SyncError
from sync.models import Column, TableSchema

#: 允许的安全类型拓宽：源类型 -> 可接受的目标类型
_WIDENING: dict[str, set[str]] = {
    "int": {"int", "float"},
    "float": {"float"},
    "str": {"str"},
    "bool": {"bool"},
}

_PY_TYPES: dict[str, tuple[type, ...]] = {
    "int": (int,),       # bool 是 int 子类，下面显式排除
    "float": (int, float),
    "str": (str,),
    "bool": (bool,),
}


def check_value(column: Column, value: Any) -> str | None:
    """按列定义严格校验单个值。返回错误详情字符串，合法时返回 ``None``。

    刻意不做任何隐式转换（"1" 不会被当作 1，1 不会被当作 True），
    类型不匹配必须显式拒绝，避免静默写入错位数据。
    """

    if value is None:
        if column.nullable:
            return None
        return f"column {column.name!r} is NOT NULL but got null"
    expected = _PY_TYPES[column.type]
    if not isinstance(value, expected) or (column.type == "int" and isinstance(value, bool)):
        return (
            f"column {column.name!r} expects {column.type}, "
            f"got {type(value).__name__} ({value!r})"
        )
    return None


class SchemaRegistry:
    """在共享 state 上工作的结构注册表（无自身状态，便于单事务提交）。"""

    @staticmethod
    def _schemas(state: dict[str, Any]) -> dict[str, Any]:
        return state["schemas"]

    def register_initial(self, state: dict[str, Any], schema: TableSchema) -> None:
        """登记一张新表的首个结构版本（版本必须为 1）。"""

        self._validate_shape(schema)
        table_map = self._schemas(state).setdefault(schema.table, {})
        if table_map:
            raise SyncError(f"table {schema.table!r} already registered; "
                            "use register_change for new versions")
        if schema.version != 1:
            raise SyncError(f"initial schema version must be 1, got {schema.version}")
        table_map["1"] = schema.to_dict()

    def register_change(
        self, state: dict[str, Any], schema: TableSchema
    ) -> tuple[list[str], dict[str, Any]]:
        """登记一个新结构版本。

        返回 ``(added_columns, defaults)``：新增列名及其默认值，供存储层
        对存量行执行确定性的回填。不兼容时抛出异常，state 不做任何修改。
        """

        self._validate_shape(schema)
        table_map = self._schemas(state).get(schema.table)
        if not table_map:
            raise SyncError(f"table {schema.table!r} not registered")
        current_version = max(int(v) for v in table_map)
        if schema.version != current_version + 1:
            raise SyncError(
                f"schema version for {schema.table!r} must be exactly "
                f"{current_version + 1}, got {schema.version} (out-of-order registration?)"
            )

        old = TableSchema.from_dict(table_map[str(current_version)])
        added, defaults = self._check_compatible(old, schema)

        # 只有通过全部检查后才写入，保证失败时 state 无副作用
        table_map[str(schema.version)] = schema.to_dict()
        return added, defaults

    def get(self, state: dict[str, Any], table: str, version: int) -> TableSchema:
        try:
            return TableSchema.from_dict(self._schemas(state)[table][str(version)])
        except KeyError as exc:
            raise SyncError(
                f"unknown schema version: table={table!r} version={version}"
            ) from exc

    def latest(self, state: dict[str, Any], table: str) -> TableSchema | None:
        table_map = self._schemas(state).get(table)
        if not table_map:
            return None
        return TableSchema.from_dict(table_map[str(max(int(v) for v in table_map))])

    def known_tables(self, state: dict[str, Any]) -> list[str]:
        return sorted(self._schemas(state).keys())

    # ------------------------------------------------------------------ 内部
    @staticmethod
    def _validate_shape(schema: TableSchema) -> None:
        names = [c.name for c in schema.columns]
        if len(names) != len(set(names)):
            raise SyncError(f"duplicate column names in schema for {schema.table!r}")
        if not schema.primary_key:
            raise SyncError(f"table {schema.table!r} must declare a primary key")
        for pk in schema.primary_key:
            if pk not in names:
                raise SyncError(f"primary key column {pk!r} missing in schema")
        for c in schema.columns:
            if c.default is not None and check_value(c, c.default) is not None:
                raise SyncError(
                    f"invalid default {c.default!r} for column {c.name!r} "
                    f"(type {c.type})"
                )

    def _check_compatible(
        self, old: TableSchema, new: TableSchema
    ) -> tuple[list[str], dict[str, Any]]:
        problems: list[str] = []
        old_cols = {c.name: c for c in old.columns}
        new_cols = {c.name: c for c in new.columns}

        # 1) 主键不可变化
        if tuple(new.primary_key) != tuple(old.primary_key):
            problems.append(
                f"primary key changed: {old.primary_key} -> {new.primary_key}"
            )

        # 2) 删除列（含改名场景）不兼容
        dropped = [n for n in old_cols if n not in new_cols]
        if dropped:
            problems.append(f"columns dropped or renamed (would misalign writes): {dropped}")

        added = [n for n in new_cols if n not in old_cols]
        defaults: dict[str, Any] = {}

        for name, col in new_cols.items():
            old_col = old_cols.get(name)
            if old_col is None:
                # 3) 新增列：非空列必须给出默认值，否则旧行无法满足约束
                if not col.nullable and col.default is None:
                    problems.append(
                        f"new column {name!r} is NOT NULL without a default; "
                        "existing rows could not be backfilled deterministically"
                    )
                defaults[name] = col.default
                continue
            # 4) 类型只能安全拓宽（int -> float）
            if col.type not in _WIDENING[old_col.type]:
                problems.append(
                    f"column {name!r} type narrowed/changed: "
                    f"{old_col.type} -> {col.type}"
                )
            elif col.type == "float" and old_col.type == "int":
                # 存量 int 值天然是合法 float，无需转换
                pass
            # 5) 可空 -> 非空必须有默认值（用于回填旧行中的 NULL）
            if old_col.nullable and not col.nullable and col.default is None:
                problems.append(
                    f"column {name!r} tightened to NOT NULL without a default"
                )

        if problems:
            raise SchemaIncompatibleError(
                f"incompatible schema change for {new.table!r} "
                f"v{old.version} -> v{new.version}: " + "; ".join(problems)
            )
        return added, defaults
