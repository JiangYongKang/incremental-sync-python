"""同步过程中可区分的错误原因。

约定：
- 记录级问题（主键冲突、字段缺失、类型不匹配等）使用 :class:`RejectReason`
  枚举描述原因，记录被写入死信而不是静默丢弃，位点正常推进。
- 流级问题（位点回退、跳段、结构不兼容）使用异常抛出，整个批次被拒绝，
  目标端不发生任何部分写入。
"""

from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    """单条变更记录被拒绝的可区分原因。"""

    DUPLICATE_KEY = "duplicate_key"
    """INSERT 时目标端已存在相同主键的行。"""

    ROW_NOT_FOUND = "row_not_found"
    """UPDATE/DELETE 时目标端不存在对应主键的行。"""

    MISSING_FIELD = "missing_field"
    """非空且无默认值的必填字段在记录中缺失。"""

    TYPE_MISMATCH = "type_mismatch"
    """字段值的实际类型与表结构声明不一致。"""

    UNKNOWN_FIELD = "unknown_field"
    """记录携带了当前表结构中不存在的列。"""

    SCHEMA_VERSION_UNKNOWN = "schema_version_unknown"
    """记录引用了目标端未注册的表结构版本（含表不存在）。"""

    MALFORMED_RECORD = "malformed_record"
    """记录结构畸形：主键不完整/含多余主键分量、key 与 data 中主键冲突等。"""


class SyncError(Exception):
    """所有同步错误的基类。"""


class CheckpointError(SyncError):
    """位点不合法（回退或跳段）。批次整体拒绝，不推进位点。"""


class CheckpointRollbackError(CheckpointError):
    """首个事件的序列号小于等于已确认位点（位点回退）。"""


class CheckpointGapError(CheckpointError):
    """批次内部存在序列号空洞（跳段）。"""


class SchemaIncompatibleError(SyncError):
    """表结构发生了无法兼容的演进（删列/改名/收窄类型/主键变更）。"""


class StorageError(SyncError):
    """本地存储读写失败。"""
