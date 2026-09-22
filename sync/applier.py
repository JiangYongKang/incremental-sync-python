"""同步应用器：幂等应用变更、校验记录、推进位点。"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any

from .checkpoint import CheckpointStore
from .models import ApplyResult, ChangeRecord, Op, Rejection, RejectReason, TableSchema
from .schema_registry import SchemaRegistry
from .target import LocalTarget

logger = logging.getLogger("sync.applier")

_TYPE_MAP = {"int": int, "float": (int, float), "str": str, "bool": bool}


def _type_ok(value: Any, type_name: str) -> bool:
    if isinstance(value, bool):
        return type_name == "bool"  # bool 是 int 子类，需特判
    return isinstance(value, _TYPE_MAP[type_name])


class SyncApplier:
    """把变更记录应用到目标端。

    一致性语义：
    - 幂等：seq <= 已确认位点 或已在未确认集合中的记录，判为重复，跳过。
    - 乱序：按 (table, pk) 维护高水位，过期记录（seq <= 该键高水位）跳过，
      保证每个主键的最终值由最大 seq 的变更决定。
    - 位点：确认位点只推进到连续前缀，缺段时等待，绝不跳过空洞。
    - 状态（已确认位点、未确认 seq 集合、键高水位）持久化，重启后续传。
    """

    def __init__(self, target: LocalTarget, checkpoints: CheckpointStore,
                 schemas: SchemaRegistry, state_path: Path | None = None):
        self.target = target
        self.checkpoints = checkpoints
        self.schemas = schemas
        self._lock = threading.RLock()
        self._state_path = Path(state_path) if state_path else None
        self._pending: set[int] = set()          # 已应用但未确认的 seq
        self._pk_hwm: dict[str, dict[str, int]] = {}  # table -> pk -> 已应用最大 seq
        if self._state_path and self._state_path.exists():
            state = json.loads(self._state_path.read_text(encoding="utf-8"))
            self._pending = set(state["pending"])
            self._pk_hwm = {t: dict(m) for t, m in state["pk_hwm"].items()}

    # ---- 校验 ----

    def _validate(self, r: ChangeRecord, schema: TableSchema) -> Rejection | None:
        if r.schema_version > schema.version:
            return Rejection(r, RejectReason.SCHEMA_INCOMPATIBLE,
                             f"记录结构版本 v{r.schema_version} 高于当前 v{schema.version}")
        cols = schema.column_map()
        for name in r.fields:
            if name not in cols:
                return Rejection(r, RejectReason.SCHEMA_INCOMPATIBLE,
                                 f"字段 {name} 不在当前表结构 v{schema.version} 中")
        for name, value in r.fields.items():
            if value is None:
                if not cols[name].nullable:
                    return Rejection(r, RejectReason.MISSING_FIELD,
                                     f"非空字段 {name} 为 null")
                continue
            if not _type_ok(value, cols[name].type):
                return Rejection(r, RejectReason.TYPE_MISMATCH,
                                 f"字段 {name} 期望 {cols[name].type}, 实际 {type(value).__name__}")
        if r.op is Op.INSERT:
            for c in schema.columns:
                if not c.nullable and c.default is None and c.name not in r.fields:
                    return Rejection(r, RejectReason.MISSING_FIELD,
                                     f"必填字段 {c.name} 缺失")
        return None

    # ---- 单行应用 ----

    def _apply_one(self, r: ChangeRecord, schema: TableSchema) -> None:
        cols = schema.column_map()
        if r.op is Op.INSERT:
            row = {c.name: c.default for c in schema.columns}
            row.update(r.fields)
            row[schema.pk] = r.pk
            self.target.upsert(r.table, r.pk, row)
        elif r.op is Op.UPDATE:
            row = self.target.get(r.table, r.pk) or {}
            row.update(r.fields)
            self.target.upsert(r.table, r.pk, row)
        else:  # DELETE
            self.target.delete(r.table, r.pk)
        self._pk_hwm.setdefault(r.table, {})[str(r.pk)] = r.seq

    # ---- 批处理 ----

    def apply_batch(self, records: list[ChangeRecord]) -> ApplyResult:
        """应用一批变更（可重复、可乱序、可并发），结果与按序只投递一次一致。"""
        result = ApplyResult()
        with self._lock:
            confirmed = self.checkpoints.load()
            for r in sorted(records, key=lambda x: x.seq):
                if r.seq <= confirmed or r.seq in self._pending:
                    result.duplicates.append(r)
                    logger.info("dup skip seq=%d table=%s pk=%r", r.seq, r.table, r.pk)
                    continue
                hwm = self._pk_hwm.get(r.table, {}).get(str(r.pk), 0)
                if r.seq <= hwm:
                    # 过期记录：效果已被更大 seq 覆盖，视为已消费，位点可越过
                    result.duplicates.append(r)
                    self._pending.add(r.seq)
                    logger.info("stale skip seq=%d table=%s pk=%r hwm=%d",
                                r.seq, r.table, r.pk, hwm)
                    continue
                schema = self.schemas.current(r.table)
                rejection: Rejection | None = None
                if schema is None:
                    rejection = Rejection(r, RejectReason.UNKNOWN_TABLE,
                                          f"表 {r.table} 未注册")
                else:
                    rejection = self._validate(r, schema)
                if rejection is None:
                    exists = self.target.get(r.table, r.pk) is not None
                    if r.op is Op.INSERT and exists:
                        rejection = Rejection(r, RejectReason.PK_CONFLICT,
                                              f"insert 冲突: pk={r.pk!r} 已存在")
                    elif r.op is not Op.INSERT and not exists:
                        # 暂时性冲突：乱序下 insert 可能尚未到达。
                        # 拒绝但不消费，等待重投；位点停在该处而非跳过。
                        result.rejected.append(Rejection(
                            r, RejectReason.PK_CONFLICT,
                            f"{r.op.value} 失败: pk={r.pk!r} 不存在（可能乱序，待重投）"))
                        logger.info("reject(transient) seq=%d reason=pk_conflict pk=%r",
                                    r.seq, r.pk)
                        continue
                if rejection is not None:
                    # 确定性拒绝（死信）：同一记录重投结论相同，计入已消费，位点可越过
                    result.rejected.append(rejection)
                    self._pending.add(r.seq)
                    logger.info("reject seq=%d reason=%s detail=%s",
                                r.seq, rejection.reason.value, rejection.detail)
                    continue
                self._apply_one(r, schema)
                self._pending.add(r.seq)
                result.applied.append(r)
                logger.info("applied seq=%d table=%s pk=%r op=%s fields=%s",
                            r.seq, r.table, r.pk, r.op.value, r.fields)
            # 位点推进到连续前缀
            new_confirmed = confirmed
            while new_confirmed + 1 in self._pending:
                new_confirmed += 1
                self._pending.discard(new_confirmed)
            if new_confirmed > confirmed:
                self.checkpoints.commit(new_confirmed, expected_prev=confirmed)
                logger.info("checkpoint %d -> %d", confirmed, new_confirmed)
            result.committed_seq = self.checkpoints.load()
            self.target.flush()
            self._save_state()
        return result

    def _save_state(self) -> None:
        if not self._state_path:
            return
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
        tmp.write_text(json.dumps({"pending": sorted(self._pending),
                                   "pk_hwm": self._pk_hwm}), encoding="utf-8")
        tmp.replace(self._state_path)
