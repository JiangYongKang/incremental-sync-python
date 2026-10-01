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
    txn_id: str | None = None  # 事务标记；None 表示单条隐式事务（旧语义）


@dataclass(frozen=True)
class TxnCommit:
    """事务提交事件：声明 txn_id 的全部成员 seq 已发齐。

    seq 为提交事件自身的位点（大于该事务所有成员 seq）；
    members 为事务内全部变更记录的 seq 列表（判定到齐/缺条的依据）。
    """
    seq: int
    txn_id: str
    members: tuple[int, ...]


# 同步流中的事件：变更记录或事务提交标记
SyncEvent = ChangeRecord | TxnCommit


@dataclass(frozen=True)
class Rejection:
    record: ChangeRecord
    reason: RejectReason
    detail: str


@dataclass(frozen=True)
class RejectedTransaction:
    """被整体拒绝的事务：完整留存，原因可区分、重投结论稳定。"""
    txn_id: str
    commit_seq: int
    records: tuple[ChangeRecord, ...]
    reasons: tuple[Rejection, ...]
    fingerprint: str  # 事务内容的稳定指纹（规范化 JSON 的 sha256）

    @property
    def primary_reason(self) -> RejectReason:
        return self.reasons[0].reason


@dataclass
class ApplyResult:
    applied: list[ChangeRecord] = field(default_factory=list)
    duplicates: list[ChangeRecord] = field(default_factory=list)  # 重复投递被幂等跳过
    rejected: list[Rejection] = field(default_factory=list)
    committed_seq: int | None = None  # 本次确认的位点
    committed_txns: list[str] = field(default_factory=list)   # 本次整体生效的事务
    rejected_txns: list[RejectedTransaction] = field(default_factory=list)  # 本次整体拒绝的事务
    staged_txns: list[str] = field(default_factory=list)      # 本次仅暂存、尚未到齐的事务
