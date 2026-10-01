"""表结构注册与兼容演进。"""
from __future__ import annotations

from .models import Column, TableSchema


def _column_to_dict(c: Column) -> dict:
    return {"name": c.name, "type": c.type, "nullable": c.nullable,
            "default": c.default}


def _column_from_dict(d: dict) -> Column:
    return Column(name=d["name"], type=d["type"], nullable=d["nullable"],
                  default=d.get("default"))


class SchemaIncompatibleError(Exception):
    """无法兼容的结构变化（删列、改名、改类型）。"""


class SchemaRegistry:
    """内存结构注册表。

    兼容规则：
    - 新增列：兼容。缺省语义确定且稳定——以列定义中的 default 为准
      （未显式给出时为 None），旧记录读到新列时按该缺省补齐。
    - 删除列、列改名（表现为旧列消失+新列出现）、列类型变更：不兼容，
      抛出 SchemaIncompatibleError，由调用方显式处理，绝不静默接受。
    """

    def __init__(self):
        self._schemas: dict[str, TableSchema] = {}

    def register(self, table: str, pk: str, columns: list[Column]) -> TableSchema:
        if table in self._schemas:
            raise ValueError(f"表已注册: {table}")
        if pk not in {c.name for c in columns}:
            raise ValueError(f"主键列 {pk} 不在列定义中")
        schema = TableSchema(table=table, version=1, pk=pk, columns=tuple(columns))
        self._schemas[table] = schema
        return schema

    def current(self, table: str) -> TableSchema | None:
        return self._schemas.get(table)

    def evolve(self, table: str, new_columns: list[Column]) -> TableSchema:
        """仅允许新增列（带确定缺省值）；删列/改名/改类型抛 SchemaIncompatibleError。"""
        old = self._schemas.get(table)
        if old is None:
            raise KeyError(f"表未注册: {table}")
        old_map = old.column_map()
        new_map = {c.name: c for c in new_columns}
        removed = set(old_map) - set(new_map)
        if removed:
            raise SchemaIncompatibleError(
                f"表 {table} 结构不兼容: 删除或改名了列 {sorted(removed)}"
            )
        for name, old_col in old_map.items():
            new_col = new_map[name]
            if new_col.type != old_col.type or new_col.nullable != old_col.nullable:
                raise SchemaIncompatibleError(
                    f"表 {table} 结构不兼容: 列 {name} 类型/可空性变化 "
                    f"({old_col.type},{old_col.nullable}) -> ({new_col.type},{new_col.nullable})"
                )
        schema = TableSchema(
            table=table, version=old.version + 1, pk=old.pk, columns=tuple(new_columns)
        )
        self._schemas[table] = schema
        return schema

    # ---- 快照与恢复（随原子提交日志一起持久化） ----

    def snapshot(self) -> dict[str, dict]:
        return {
            t: {"version": s.version, "pk": s.pk,
                "columns": [_column_to_dict(c) for c in s.columns]}
            for t, s in self._schemas.items()
        }

    def restore(self, snap: dict[str, dict]) -> None:
        """按快照重建注册表（进程重启后调用）。"""
        self._schemas = {}
        for table, data in snap.items():
            self._schemas[table] = TableSchema(
                table=table, version=data["version"], pk=data["pk"],
                columns=tuple(_column_from_dict(c) for c in data["columns"])
            )
