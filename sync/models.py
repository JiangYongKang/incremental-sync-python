"""核心数据模型：变更记录、位点、拒绝原因、应用结果。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class Op(enum.Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class RejectReason(enum.Enum):
    PK_CONFLICT = "pk_conflict"          # 主键冲突（insert 已存在 / update、delete 不存在）
    MISSING_FIELD = "missing_field"      # 必填字段缺失
    TYPE_MISMATCH = "type_mismatch"      # 字段类型与表结构不匹配
    UNKNOWN_TABLE = "unknown_table"      # 表未注册
    SCHEMA_INCOMPATIBLE = "schema_incompatible"  # 变更基于不兼容的结构版本


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # "int" | "float" | "str" | "bool"
    nullable: bool = True
    default: Any = None  # 新增列的缺省值（确定且稳定）


@dataclass(frozen=True)
class TableSchema:
    table: str
    version: int
    pk: str
    columns: tuple[Column, ...]

    def column_map(self) -> dict[str, Column]:
        return {c.name: c for c in self.columns}


@dataclass(frozen=True)
class ChangeRecord:
    seq: int                 # 全局单调递增位点
    table: str
    pk: Any
    op: Op
    fields: dict[str, Any]   # insert/update 的字段值；delete 为空
    schema_version: int
    tx_id: str | None = None    # 业务事务标记；None 表示单条变更（非事务）
    tx_total: int | None = None  # 该事务的记录总条数（上游在每条事务记录上声明）


@dataclass(frozen=True)
class Rejection:
    record: ChangeRecord
    reason: RejectReason
    detail: str


@dataclass(frozen=True)
class TxRejection:
    """整个事务被拒绝（组内任一记录非法则整体拒绝，结论确定且稳定）。"""
    tx_id: str
    reason: RejectReason
    detail: str
    records: tuple[ChangeRecord, ...]  # 事务内全部记录，完整留存


@dataclass
class ApplyResult:
    applied: list[ChangeRecord] = field(default_factory=list)
    duplicates: list[ChangeRecord] = field(default_factory=list)  # 重复投递被幂等跳过
    rejected: list[Rejection] = field(default_factory=list)
    committed_seq: int | None = None  # 本次确认的位点
    applied_txs: list[str] = field(default_factory=list)      # 本次整体生效的事务
    rejected_txs: list[TxRejection] = field(default_factory=list)  # 本次整体拒绝的事务
