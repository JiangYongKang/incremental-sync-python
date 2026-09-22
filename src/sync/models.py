"""同步系统使用的数据类型：表结构、变更事件、应用结果、死信记录。

所有类型都是 JSON 可序列化的 dataclass，便于本地文件存储与测试断言。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal

from sync.errors import RejectReason

#: 支持的列数据类型。刻意保持最小集合，类型检查严格、不做隐式转换。
ColumnType = Literal["int", "float", "str", "bool"]


class OpType(str, Enum):
    """变更操作类型。"""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclass(frozen=True)
class Column:
    """列定义。

    ``nullable=False`` 且 ``default=None`` 表示该列必填；
    新增列（兼容演进）必须给出 ``default``，缺省语义由此唯一确定。
    """

    name: str
    type: ColumnType
    nullable: bool = True
    default: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "nullable": self.nullable,
            "default": self.default,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Column:
        return cls(name=d["name"], type=d["type"], nullable=d["nullable"], default=d["default"])


@dataclass(frozen=True)
class TableSchema:
    """某个版本的表结构。"""

    table: str
    version: int
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]

    def column(self, name: str) -> Column | None:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "version": self.version,
            "columns": [c.to_dict() for c in self.columns],
            "primary_key": list(self.primary_key),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TableSchema:
        return cls(
            table=d["table"],
            version=d["version"],
            columns=tuple(Column.from_dict(c) for c in d["columns"]),
            primary_key=tuple(d["primary_key"]),
        )


@dataclass(frozen=True)
class ChangeEvent:
    """一条数据变更记录（上游数据源产生，按 ``seq`` 严格递增）。

    - ``seq``：全流（非按表）严格连续递增的位点序列号 1,2,3,...
    - ``data``：insert 为整行；update 为要修改的列（部分列更新）；delete 可为空
    - ``schema_version``：该记录写入时对应的表结构版本
    """

    seq: int
    table: str
    op: OpType
    key: dict[str, Any]
    data: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1
    event_time: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "table": self.table,
            "op": self.op.value,
            "key": self.key,
            "data": self.data,
            "schema_version": self.schema_version,
            "event_time": self.event_time,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ChangeEvent:
        return cls(
            seq=d["seq"],
            table=d["table"],
            op=OpType(d["op"]),
            key=d["key"],
            data=d.get("data", {}),
            schema_version=d.get("schema_version", 1),
            event_time=d.get("event_time"),
        )


@dataclass(frozen=True)
class SchemaChangeEvent:
    """一次表结构变更声明。结构变更本身也占用一个位点，保证全序。"""

    seq: int
    table: str
    new_schema: TableSchema
    event_time: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "table": self.table,
            "kind": "schema_change",
            "new_schema": self.new_schema.to_dict(),
            "event_time": self.event_time,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SchemaChangeEvent:
        return cls(
            seq=d["seq"],
            table=d["table"],
            new_schema=TableSchema.from_dict(d["new_schema"]),
            event_time=d.get("event_time"),
        )


@dataclass(frozen=True)
class RejectedRecord:
    """被拒绝记录的死信件：保留原始事件与可区分的拒绝原因。"""

    seq: int
    table: str
    op: str
    reason: RejectReason
    detail: str
    event: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "table": self.table,
            "op": self.op,
            "reason": self.reason.value,
            "detail": self.detail,
            "event": self.event,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> RejectedRecord:
        return cls(
            seq=d["seq"],
            table=d["table"],
            op=d["op"],
            reason=RejectReason(d["reason"]),
            detail=d["detail"],
            event=d["event"],
        )


@dataclass
class CommitResult:
    """一个批次提交后的汇总结果。"""

    committed_seq: int
    """提交完成后的已确认位点。"""

    applied: list[int] = field(default_factory=list)
    """实际写入（含删除）生效的事件 seq。"""

    skipped_duplicates: list[int] = field(default_factory=list)
    """幂等跳过的重复事件 seq（结果与只投递一次一致）。"""

    rejected: list[RejectedRecord] = field(default_factory=list)
    """被拒绝进入死信的记录。"""

    schema_changes: list[int] = field(default_factory=list)
    """已登记的结构变更事件 seq。"""

    def to_dict(self) -> dict[str, Any]:
        return {
            "committed_seq": self.committed_seq,
            "applied": self.applied,
            "skipped_duplicates": self.skipped_duplicates,
            "rejected": [r.to_dict() for r in self.rejected],
            "schema_changes": self.schema_changes,
        }
