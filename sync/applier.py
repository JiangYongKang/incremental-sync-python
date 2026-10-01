"""同步应用器：幂等应用变更、事务化整体生效、校验拒绝、推进位点。"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any

from .checkpoint import CheckpointStore
from .models import (
    ApplyResult,
    ChangeRecord,
    Op,
    Rejection,
    RejectReason,
    TableSchema,
    TxRejection,
)
from .rejections import RejectionStore
from .schema_registry import SchemaRegistry
from .target import LocalTarget
from .txn import TxnBuffer

logger = logging.getLogger("sync.applier")

_TYPE_MAP = {"int": int, "float": (int, float), "str": str, "bool": bool}


def _type_ok(value: Any, type_name: str) -> bool:
    if isinstance(value, bool):
        return type_name == "bool"  # bool 是 int 子类，需特判
    return isinstance(value, _TYPE_MAP[type_name])


class SyncApplier:
    """把变更记录应用到目标端。

    一致性语义：
    - 幂等：seq <= 已确认位点、已在未确认集合或已缓冲在事务中的记录，
      判为重复，跳过。
    - 乱序：按 (table, pk) 维护高水位，过期记录（seq <= 该键高水位）跳过，
      保证每个主键的最终值由最大 seq 的变更决定。
    - 事务：同一 tx_id 的记录凑齐（已见条数达到 tx_total）后整体校验、整体
      原子生效；任一记录非法则整个事务拒绝并完整留存，绝不部分写入。
      未凑齐的事务只对缓冲可见，目标端读不到中间态。
    - 位点：确认位点只推进到连续前缀，缺段时等待，绝不跳过空洞。
    - 崩溃恢复：持久化顺序为 数据 -> 应用器状态（未确认集合、键高水位、
      事务缓冲）-> 位点，位点绝不超过已生效数据；重启后从已确认位点
      继续，重复投递被识别，既不多写也不漏写。
    """

    def __init__(self, target: LocalTarget, checkpoints: CheckpointStore,
                 schemas: SchemaRegistry, state_path: Path | None = None,
                 rejections: RejectionStore | None = None):
        self.target = target
        self.checkpoints = checkpoints
        self.schemas = schemas
        self._rejections = rejections
        self._lock = threading.RLock()
        self._state_path = Path(state_path) if state_path else None
        self._pending: set[int] = set()          # 已应用但未确认的 seq
        self._pk_hwm: dict[str, dict[str, int]] = {}  # table -> pk -> 已应用最大 seq
        self._tx_buf = TxnBuffer()               # 未凑齐/被阻塞的事务
        if self._state_path and self._state_path.exists():
            state = json.loads(self._state_path.read_text(encoding="utf-8"))
            self._pending = set(state["pending"])
            self._pk_hwm = {t: dict(m) for t, m in state["pk_hwm"].items()}
            self._tx_buf = TxnBuffer.from_dict(state.get("tx_buffer", {}))

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

    @staticmethod
    def _insert_row(r: ChangeRecord, schema: TableSchema) -> dict[str, Any]:
        row = {c.name: c.default for c in schema.columns}
        row.update(r.fields)
        row[schema.pk] = r.pk
        return row

    def _apply_one(self, r: ChangeRecord, schema: TableSchema) -> None:
        if r.op is Op.INSERT:
            self.target.upsert(r.table, r.pk, self._insert_row(r, schema))
        elif r.op is Op.UPDATE:
            row = self.target.get(r.table, r.pk) or {}
            row.update(r.fields)
            self.target.upsert(r.table, r.pk, row)
        else:  # DELETE
            self.target.delete(r.table, r.pk)
        self._pk_hwm.setdefault(r.table, {})[str(r.pk)] = r.seq

    # ---- 非事务记录 ----

    def _apply_single(self, r: ChangeRecord, result: ApplyResult) -> None:
        schema = self.schemas.current(r.table)
        rejection: Rejection | None = None
        if schema is None:
            rejection = Rejection(r, RejectReason.UNKNOWN_TABLE,
                                  f"表 {r.table} 未注册")
        else:
            rejection = self._validate(r, schema)
        if rejection is None:
            existing = self.target.get(r.table, r.pk)
            if r.op is Op.INSERT and existing is not None:
                if existing == self._insert_row(r, schema):
                    # 崩溃恢复窗口内的重复写入：内容一致，幂等跳过
                    self._pending.add(r.seq)
                    self._pk_hwm.setdefault(r.table, {})[str(r.pk)] = r.seq
                    result.duplicates.append(r)
                    logger.info("dup identical insert seq=%d table=%s pk=%r",
                                r.seq, r.table, r.pk)
                    return
                rejection = Rejection(r, RejectReason.PK_CONFLICT,
                                      f"insert 冲突: pk={r.pk!r} 已存在")
            elif r.op is not Op.INSERT and existing is None:
                # 暂时性冲突：乱序下 insert 可能尚未到达。
                # 拒绝但不消费，等待重投；位点停在该处而非跳过。
                result.rejected.append(Rejection(
                    r, RejectReason.PK_CONFLICT,
                    f"{r.op.value} 失败: pk={r.pk!r} 不存在（可能乱序，待重投）"))
                logger.info("reject(transient) seq=%d reason=pk_conflict pk=%r",
                            r.seq, r.pk)
                return
        if rejection is not None:
            # 确定性拒绝（死信）：同一记录重投结论相同，计入已消费，位点可越过
            result.rejected.append(rejection)
            self._pending.add(r.seq)
            logger.info("reject seq=%d reason=%s detail=%s",
                        r.seq, rejection.reason.value, rejection.detail)
            return
        self._apply_one(r, schema)
        self._pending.add(r.seq)
        result.applied.append(r)
        logger.info("applied seq=%d table=%s pk=%r op=%s fields=%s",
                    r.seq, r.table, r.pk, r.op.value, r.fields)

    # ---- 事务 ----

    def _reject_tx(self, tx_id: str, records: list[ChangeRecord],
                   first: Rejection, result: ApplyResult) -> None:
        """整体拒绝一个事务：完整留存、全部 seq 计入已消费（死信）。"""
        txr = TxRejection(tx_id=tx_id, reason=first.reason,
                          detail=first.detail, records=tuple(records))
        if self._rejections is not None:
            self._rejections.put(txr)
        for r in records:
            self._pending.add(r.seq)
        self._tx_buf.pop(tx_id)
        result.rejected_txs.append(txr)
        logger.info("tx rejected tx=%s reason=%s detail=%s seqs=%s",
                    tx_id, first.reason.value, first.detail,
                    [r.seq for r in records])

    def _try_apply_tx(self, tx_id: str, result: ApplyResult) -> bool:
        """尝试让一个已凑齐的事务整体生效。

        返回 True 表示已了结（整体生效或整体拒绝）；False 表示被
        暂时性主键冲突阻塞（依赖的前序变更未到），留在缓冲等待重试。
        """
        records = self._tx_buf.records_of(tx_id)
        # 应用时再次剔除过期记录（缓冲期间键高水位可能已推进）
        effective: list[ChangeRecord] = []
        for r in records:
            hwm = self._pk_hwm.get(r.table, {}).get(str(r.pk), 0)
            if r.seq <= hwm:
                self._pending.add(r.seq)
                logger.info("tx stale skip seq=%d tx=%s", r.seq, tx_id)
            else:
                effective.append(r)
        if not effective:
            self._tx_buf.pop(tx_id)
            logger.info("tx %s 全部记录已过期，直接消费", tx_id)
            return True
        # 1) 确定性校验：任一记录非法则整个事务拒绝
        schemas: dict[str, TableSchema] = {}
        for r in effective:
            schema = self.schemas.current(r.table)
            rej: Rejection | None = None
            if schema is None:
                rej = Rejection(r, RejectReason.UNKNOWN_TABLE, f"表 {r.table} 未注册")
            else:
                schemas[r.table] = schema
                rej = self._validate(r, schema)
            if rej is not None:
                self._reject_tx(tx_id, records, rej, result)
                return True
        # 2) 主键语义模拟：按 seq 顺序在 目标端+事务内效果 上演练
        overlay: dict[tuple[str, str], dict[str, Any] | None] = {}

        def sim_get(table: str, pk: Any) -> dict[str, Any] | None:
            key = (table, str(pk))
            return overlay[key] if key in overlay else self.target.get(table, pk)

        for r in effective:
            key = (r.table, str(r.pk))
            existing = sim_get(r.table, r.pk)
            if r.op is Op.INSERT:
                row = self._insert_row(r, schemas[r.table])
                if existing is not None:
                    if existing == row:
                        overlay[key] = row  # 崩溃恢复窗口的重复写入：幂等
                        continue
                    self._reject_tx(tx_id, records, Rejection(
                        r, RejectReason.PK_CONFLICT,
                        f"insert 冲突: pk={r.pk!r} 已存在"), result)
                    return True
                overlay[key] = row
            elif r.op is Op.UPDATE:
                if existing is None:
                    logger.info("tx %s blocked(transient): update pk=%r 不存在，待重投",
                                tx_id, r.pk)
                    return False
                row = dict(existing)
                row.update(r.fields)
                overlay[key] = row
            else:  # DELETE
                if existing is None:
                    logger.info("tx %s blocked(transient): delete pk=%r 不存在，待重投",
                                tx_id, r.pk)
                    return False
                overlay[key] = None
        # 3) 整体原子生效：同一把锁内写全部行并一次落盘
        self.target.apply_atomic(
            lambda t: [self._apply_one(r, schemas[r.table]) for r in effective]
        )
        for r in effective:
            self._pending.add(r.seq)
        self._tx_buf.pop(tx_id)
        result.applied.extend(effective)
        result.applied_txs.append(tx_id)
        logger.info("tx applied tx=%s seqs=%s", tx_id, [r.seq for r in effective])
        return True

    def _drain_ready_txs(self, result: ApplyResult) -> None:
        """按最小 seq 顺序处理所有已凑齐的事务，直到没有进展。"""
        progressed = True
        while progressed:
            progressed = False
            for tx_id in self._tx_buf.ready_tx_ids():
                if self._try_apply_tx(tx_id, result):
                    progressed = True

    # ---- 批处理 ----

    def apply_batch(self, records: list[ChangeRecord]) -> ApplyResult:
        """应用一批变更（可重复、可乱序、可并发），结果与按序只投递一次一致。"""
        result = ApplyResult()
        with self._lock:
            confirmed = self.checkpoints.load()
            self._drain_ready_txs(result)  # 重试之前被暂时性阻塞的事务
            buffered = self._tx_buf.buffered_seqs()
            for r in sorted(records, key=lambda x: x.seq):
                if r.seq <= confirmed or r.seq in self._pending or r.seq in buffered:
                    result.duplicates.append(r)
                    logger.info("dup skip seq=%d table=%s pk=%r", r.seq, r.table, r.pk)
                    continue
                hwm = self._pk_hwm.get(r.table, {}).get(str(r.pk), 0)
                if r.seq <= hwm:
                    # 过期记录：效果已被更大 seq 覆盖，视为已消费，位点可越过
                    result.duplicates.append(r)
                    self._pending.add(r.seq)
                    if r.tx_id is not None:
                        self._tx_buf.note_meta(r)
                    logger.info("stale skip seq=%d table=%s pk=%r hwm=%d",
                                r.seq, r.table, r.pk, hwm)
                    continue
                if r.tx_id is not None:
                    self._tx_buf.add(r)
                    buffered.add(r.seq)
                    logger.info("tx buffer seq=%d tx=%s table=%s pk=%r op=%s",
                                r.seq, r.tx_id, r.table, r.pk, r.op.value)
                    if self._tx_buf.is_complete(r.tx_id):
                        self._drain_ready_txs(result)
                    continue
                self._apply_single(r, result)
            self._drain_ready_txs(result)
            # 位点推进到连续前缀
            new_confirmed = confirmed
            while new_confirmed + 1 in self._pending:
                new_confirmed += 1
                self._pending.discard(new_confirmed)
            # 持久化顺序：数据 -> 应用器状态 -> 位点（位点绝不超过已生效数据）
            self.target.flush()
            self._save_state()
            if new_confirmed > confirmed:
                self.checkpoints.commit(new_confirmed, expected_prev=confirmed)
                logger.info("checkpoint %d -> %d", confirmed, new_confirmed)
            result.committed_seq = self.checkpoints.load()
        return result

    def _save_state(self) -> None:
        if not self._state_path:
            return
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
        tmp.write_text(json.dumps({"pending": sorted(self._pending),
                                   "pk_hwm": self._pk_hwm,
                                   "tx_buffer": self._tx_buf.to_dict()}),
                       encoding="utf-8")
        tmp.replace(self._state_path)
