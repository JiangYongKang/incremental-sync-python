"""同步应用器：事务化幂等应用、整组校验、位点推进与原子状态持久化。

一致性语义：
- 事务分组：变更记录携带 txn_id，同一 txn_id 的记录 + 提交标记（TxnCommit，
  含成员 seq 列表）构成一个事务单元；无 txn_id 的记录视为单条隐式事务。
- 原子可见：事务记录先进暂存区，成员到齐且整组校验通过后才一次性生效；
  任一成员非法则整事务拒绝（死信），目标端永远看不到半个事务。
  生效动作本身也是原子的：整组行变更（可跨表）在同一把目标端锁内一次
  写入（LocalTarget.apply_atomic），因此即使在事务生效过程中有并发读者，
  读到的也只能是”整组生效前”或”整组生效后”，绝不会是跨表各读一半的
  中间态；跨表一致读取须用 read_snapshot/all_tables（单次加锁）。
- 有序决议：单元按提交位点（隐式=记录 seq，显式=提交标记 seq）排序决议；
  前序单元未到齐或遇暂时性主键冲突时阻塞，绝不越过空洞或未决议单元。
- 幂等去重：seq <= 已确认位点、已消费未确认、死信事务重投均判重，跳过。
  最终结果与”按顺序只投递一次”一致，重复投递/乱序分批/并发提交均成立。
- 原子落盘：CommitLogger 以 tmp+replace 原子写入完整快照（数据、位点、
  结构、键水位、暂存、死信）；旁路文件（target/checkpoint/死信视图）在
  快照落盘后尽力重写，重启一律以快照为准恢复，位点不超过已生效数据。
  提交中途写盘报错或进程被杀：整组事务随旧快照一起回滚，重启续传后
  重投，要么整组生效、要么整组拒绝，绝不残留半组。
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
from pathlib import Path
from typing import Any

from .checkpoint import CheckpointStore
from .journal import CommitLogger
from .models import (ApplyResult, ChangeRecord, Op, RejectedTransaction, Rejection,
                     RejectReason, SyncEvent, TableSchema, TxnCommit)
from .schema_registry import SchemaRegistry
from .source import commit_to_dict, event_from_dict, record_to_dict
from .target import LocalTarget

logger = logging.getLogger("sync.applier")

_TYPE_MAP = {"int": int, "float": (int, float), "str": str, "bool": bool}


def _type_ok(value: Any, type_name: str) -> bool:
    if isinstance(value, bool):
        return type_name == "bool"  # bool 是 int 子类，需特判
    return isinstance(value, _TYPE_MAP[type_name])


def _fingerprint(txn_id: str, commit_seq: int,
                 records: list[ChangeRecord]) -> str:
    payload = json.dumps({
        "txn_id": txn_id,
        "commit_seq": commit_seq,
        "records": [record_to_dict(r) for r in sorted(records, key=lambda r: r.seq)],
    }, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class SyncApplier:
    def __init__(self, target: LocalTarget, checkpoints: CheckpointStore,
                 schemas: SchemaRegistry, state_path: Path | None = None,
                 journal_path: Path | None = None,
                 dead_letter_path: Path | None = None):
        self.target = target
        self.checkpoints = checkpoints
        self.schemas = schemas
        self._lock = threading.RLock()
        self._state_path = Path(state_path) if state_path else None
        base = (self._state_path or Path("sync_state.json")).parent
        self._journal_path = Path(journal_path) if journal_path else base / "journal.json"
        self._dead_path = Path(dead_letter_path) if dead_letter_path else base / "dead_letters.json"
        self._pending: set[int] = set()                 # 已消费但未确认的 seq
        self._pk_hwm: dict[str, dict[str, int]] = {}  # table -> pk -> 已生效最大记录 seq
        self._staging: dict[str, dict] = {}             # txn_id -> {records, commit}
        self._dead: dict[str, RejectedTransaction] = {}  # txn_id -> 死信
        self._journal = CommitLogger(self._journal_path, self._snapshot_state)

        # 崩溃恢复：journal 是权威状态；无 journal 时退回旧式 state 文件
        snap = self._journal.recover()
        if snap is not None:
            self._restore_state(snap)
        elif self._state_path and self._state_path.exists():
            state = json.loads(self._state_path.read_text(encoding="utf-8"))
            self._pending = set(state["pending"])
            self._pk_hwm = {t: dict(m) for t, m in state["pk_hwm"].items()}

    # ================= 快照与恢复 =================

    def _snapshot_state(self) -> dict[str, Any]:
        return {
            "version": 1,
            "confirmed_seq": self.checkpoints.load(),
            "target": self.target.all_tables(),
            "pending": sorted(self._pending),
            "pk_hwm": self._pk_hwm,
            "staging": {
                txn: {"records": [record_to_dict(r) for r in d["records"]],
                      "commit": commit_to_dict(d["commit"]) if d["commit"] else None}
                for txn, d in self._staging.items()
            },
            "dead": {
                txn: {"txn_id": d.txn_id, "commit_seq": d.commit_seq,
                      "records": [record_to_dict(r) for r in d.records],
                      "reasons": [[r.record.seq, r.reason.value, r.detail]
                                  for r in d.reasons],
                      "fingerprint": d.fingerprint}
                for txn, d in self._dead.items()
            },
            "schemas": self.schemas.snapshot(),
        }

    def _restore_state(self, snap: dict[str, Any]) -> None:
        # 先重写旁路文件，再刷新各组件内存，保证它们与快照一致
        self.checkpoints.rewrite(snap["confirmed_seq"])
        self.target.replace_all(snap["target"])
        self.schemas.restore(snap.get("schemas", {}))
        self._pending = set(snap.get("pending", []))
        self._pk_hwm = {t: dict(m) for t, m in snap.get("pk_hwm", snap.get("unit_hwm", {})).items()}
        self._staging = {}
        for txn, d in snap.get("staging", {}).items():
            self._staging[txn] = {
                "records": [event_from_dict(x) for x in d["records"]],
                "commit": event_from_dict(d["commit"]) if d["commit"] else None,
            }
        self._dead = {}
        for txn, d in snap.get("dead", {}).items():
            records = tuple(event_from_dict(x) for x in d["records"])
            reasons = tuple(
                Rejection(next(r for r in records if r.seq == seq),
                          RejectReason(reason), detail)
                for seq, reason, detail in d["reasons"]
            )
            self._dead[txn] = RejectedTransaction(
                txn_id=d["txn_id"], commit_seq=d["commit_seq"], records=records,
                reasons=reasons, fingerprint=d["fingerprint"])
        logger.info("journal recover confirmed=%d pending=%d staged=%d dead=%d",
                    snap["confirmed_seq"], len(self._pending),
                    len(self._staging), len(self._dead))

    def _persist_atomic(self, new_confirmed: int) -> None:
        """数据/位点/结构/暂存/死信一起原子生效。"""
        # 1) 位点内存视图先行（快照内容随之反映新值）
        self.checkpoints.stage(new_confirmed)
        # 2) 权威快照原子落盘（失败则旧文件保持完好，异常向上抛）
        self._journal.persist()
        # 2) 旁路文件尽力重写；若在这之间崩溃，重启仍以快照为准重放
        self.checkpoints.rewrite(new_confirmed)
        self.target.flush()
        self._write_dead_view()
        if self._state_path:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
            tmp.write_text(json.dumps(
                {"pending": sorted(self._pending), "pk_hwm": self._pk_hwm}),
                encoding="utf-8")
            tmp.replace(self._state_path)

    def _write_dead_view(self) -> None:
        view = [{
            "txn_id": d.txn_id,
            "commit_seq": d.commit_seq,
            "primary_reason": d.primary_reason.value,
            "fingerprint": d.fingerprint,
            "reasons": [{"seq": r.record.seq, "reason": r.reason.value,
                         "detail": r.detail} for r in d.reasons],
            "records": [record_to_dict(r) for r in d.records],
        } for d in sorted(self._dead.values(), key=lambda x: x.commit_seq)]
        tmp = self._dead_path.with_suffix(self._dead_path.suffix + ".tmp")
        tmp.write_text(json.dumps(view, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._dead_path)

    # ================= 校验 =================

    def _validate_fields(self, r: ChangeRecord,
                         schema: TableSchema) -> Rejection | None:
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

    def _validate_unit(self, records: list[ChangeRecord]
                       ) -> tuple[list[Rejection], Rejection | None]:
        """整组校验。返回（确定性拒绝列表，暂时性冲突拒绝）。"""
        deterministic: list[Rejection] = []
        # 阶段一：与行状态无关的结构/字段校验，逐成员收集（全部要报）
        validated: list[tuple[ChangeRecord, TableSchema]] = []
        for r in records:
            schema = self.schemas.current(r.table)
            if schema is None:
                deterministic.append(Rejection(r, RejectReason.UNKNOWN_TABLE,
                                               f"表 {r.table} 未注册"))
                continue
            rej = self._validate_fields(r, schema)
            if rej is not None:
                deterministic.append(rej)
            else:
                validated.append((r, schema))
        # 阶段二：在目标端当前状态上按成员 seq 顺序做主键存在性沙盘推演。
        # 即使阶段一已有确定性错误也继续收集 insert 主键冲突，保证原因完整可区分。
        scratch: dict[str, set[str]] = {}
        for r, _schema in sorted(validated, key=lambda x: x[0].seq):
            key = str(r.pk)
            exists = key in scratch.setdefault(r.table, set()) or \
                self.target.get(r.table, r.pk) is not None
            if r.op is Op.INSERT:
                if exists:
                    deterministic.append(Rejection(
                        r, RejectReason.PK_CONFLICT,
                        f"insert 冲突: pk={r.pk!r} 已存在"))
                    continue
                scratch[r.table].add(key)
            elif r.op is Op.UPDATE:
                if not exists:
                    # 暂时性：前序单元可能尚未到齐（乱序），等待重投
                    transient = Rejection(
                        r, RejectReason.PK_CONFLICT,
                        f"update 失败: pk={r.pk!r} 不存在（可能乱序，待重投）")
                    return deterministic, (None if deterministic else transient)
            else:  # DELETE
                if not exists:
                    transient = Rejection(
                        r, RejectReason.PK_CONFLICT,
                        f"delete 失败: pk={r.pk!r} 不存在（可能乱序，待重投）")
                    return deterministic, (None if deterministic else transient)
                scratch[r.table].discard(key)
        return deterministic, None

    # ================= 单事务生效 =================

    def _apply_unit(self, records: list[ChangeRecord], unit_seq: int) -> None:
        """整组事务原子生效：先算好全部行变更，再一次加锁落进目标端。

        目标端锁在整组写入期间不释放，任何并发读者（read_snapshot /
        all_tables / rows）要么看到整组生效前、要么看到整组生效后，
        绝不会看到跨表事务只生效了一半的中间态。
        """
        # 阶段一：在应用器锁内（无并发写者）算出整组行变更
        ops: list[tuple[str, str, Any, dict[str, Any] | None]] = []
        for r, schema in [(r, self.schemas.current(r.table))
                          for r in sorted(records, key=lambda x: x.seq)]:
            if r.op is Op.INSERT:
                row = {c.name: c.default for c in schema.columns}
                row.update(r.fields)
                row[schema.pk] = r.pk
                ops.append(("upsert", r.table, r.pk, row))
            elif r.op is Op.UPDATE:
                row = self.target.get(r.table, r.pk) or {}
                row.update(r.fields)
                ops.append(("upsert", r.table, r.pk, row))
            else:
                ops.append(("delete", r.table, r.pk, None))
        # 阶段二：同一把目标端锁内一次性生效（原子可见边界）
        self.target.apply_atomic(ops)
        for r in records:
            self._pk_hwm.setdefault(r.table, {})[str(r.pk)] = r.seq

    # ================= 批处理入口 =================

    def apply_batch(self, events: list[SyncEvent]) -> ApplyResult:
        """应用一批事件（可重复、可乱序分批、可并发），结果与按序只投递一次一致。"""
        result = ApplyResult()
        with self._lock:
            confirmed = self.checkpoints.load()

            # ---- 1. 摄入事件：去重，显式事务进暂存，隐式事务入待决议 ----
            implicit: list[ChangeRecord] = []
            seen_staged: set[str] = set()
            intake_dirty = False  # 有新的事务成员/提交标记进入暂存（需落盘以跨重启）
            for ev in sorted(events, key=lambda e: e.seq):
                if isinstance(ev, TxnCommit):
                    if ev.seq <= confirmed or ev.seq in self._pending:
                        logger.info("dup skip commit seq=%d txn=%s", ev.seq, ev.txn_id)
                        continue
                    slot = self._staging.setdefault(
                        ev.txn_id, {"records": [], "commit": None})
                    if slot["commit"] is None:
                        slot["commit"] = ev
                        seen_staged.add(ev.txn_id)
                        intake_dirty = True
                    else:
                        logger.info("dup skip commit seq=%d txn=%s", ev.seq, ev.txn_id)
                    continue
                r = ev
                if r.seq <= confirmed or r.seq in self._pending:
                    result.duplicates.append(r)
                    logger.info("dup skip seq=%d table=%s pk=%r", r.seq, r.table, r.pk)
                    continue
                if r.txn_id is None:
                    implicit.append(r)
                    continue
                slot = self._staging.setdefault(
                    r.txn_id, {"records": [], "commit": None})
                if any(x.seq == r.seq for x in slot["records"]):
                    result.duplicates.append(r)
                    logger.info("dup skip seq=%d txn=%s", r.seq, r.txn_id)
                else:
                    slot["records"].append(r)
                    seen_staged.add(r.txn_id)
                    intake_dirty = True

            # ---- 2. 组装候选单元（按提交位点排序） ----
            candidates: list[tuple[int, str, list[ChangeRecord], bool, int | None]] = []
            for r in implicit:
                candidates.append((r.seq, f"<implicit:{r.seq}>", [r], True, None))
            for txn_id, slot in self._staging.items():
                commit: TxnCommit | None = slot["commit"]
                if commit is None:
                    if txn_id in seen_staged:
                        result.staged_txns.append(txn_id)
                        logger.info("txn staged txn=%s records=%d (await commit)",
                                    txn_id, len(slot["records"]))
                    continue
                members = {r.seq: r for r in slot["records"]}
                missing = [s for s in commit.members if s not in members]
                extra = [s for s in members if s not in commit.members]
                if missing or extra:
                    if txn_id in seen_staged:
                        result.staged_txns.append(txn_id)
                        logger.info("txn staged txn=%s missing=%s extra=%s (await members)",
                                    txn_id, missing, extra)
                    continue
                ordered = [members[s] for s in sorted(commit.members)]
                candidates.append((commit.seq, txn_id, ordered, False, commit.seq))
            candidates.sort(key=lambda c: c[0])

            # ---- 3. 有序决议：未到齐/暂时性冲突即阻塞其后所有单元 ----
            resolved_any = False
            assembled: set[str] = set()
            for unit_seq, key, records, is_implicit, commit_seq in candidates:
                txn_id = None if is_implicit else key
                # 过期成员：其效果已被同一主键更大 seq 的生效变更覆盖（乱序补洞
                # 或重启后续投），判重消费；整组若全部过期则直接跳过
                live: list[ChangeRecord] = []
                for r in records:
                    hwm = self._pk_hwm.get(r.table, {}).get(str(r.pk), 0)
                    if r.seq <= hwm:
                        result.duplicates.append(r)
                        self._pending.add(r.seq)
                        logger.info("stale skip seq=%d table=%s pk=%r hwm=%d",
                                    r.seq, r.table, r.pk, hwm)
                    else:
                        live.append(r)
                if not live:
                    if commit_seq is not None:
                        self._pending.add(commit_seq)
                    assembled.add(key)
                    resolved_any = True
                    continue
                records = live
                if txn_id is not None and txn_id in self._dead:
                    # 死信事务重投：结论稳定，判重消费
                    for r in records:
                        result.duplicates.append(r)
                    dead = self._dead[txn_id]
                    if _fingerprint(txn_id, commit_seq, records) != dead.fingerprint:
                        logger.warning("txn_id=%s 重投内容与死信不一致，维持原拒绝结论",
                                       txn_id)
                    for s in (*[r.seq for r in records], commit_seq):
                        self._pending.add(s)
                    assembled.add(key)
                    logger.info("dup skip dead txn=%s reason=%s",
                                txn_id, dead.primary_reason.value)
                    continue
                rejections, transient = self._validate_unit(records)
                if transient is not None:
                    # 暂时性冲突：不消费，位点停住，等待前序变更到达后重投
                    result.rejected.append(transient)
                    logger.info("reject(transient) txn=%s reason=pk_conflict pk=%r",
                                txn_id, transient.record.pk)
                    break
                if rejections:
                    # 整事务确定性拒绝：全部成员不得写入，死信完整留存
                    cseq = commit_seq if commit_seq is not None else records[0].seq
                    tkey = txn_id or f"<implicit:{records[0].seq}>"
                    # 拒绝原因按成员 seq 排序，保证主原因稳定、可复现
                    rejections = sorted(rejections, key=lambda rj: rj.record.seq)
                    rt = RejectedTransaction(
                        txn_id=tkey, commit_seq=cseq, records=tuple(records),
                        reasons=tuple(rejections),
                        fingerprint=_fingerprint(tkey, cseq, records))
                    self._dead[rt.txn_id] = rt
                    result.rejected.extend(rejections)
                    result.rejected_txns.append(rt)
                    consumed = [r.seq for r in records] + ([commit_seq] if commit_seq else [])
                    for s in consumed:
                        self._pending.add(s)
                    assembled.add(key)
                    resolved_any = True
                    logger.info("txn rejected txn=%s reasons=%s",
                                rt.txn_id, [rj.reason.value for rj in rejections])
                    continue
                # 整组生效
                self._apply_unit(records, unit_seq)
                for r in records:
                    self._pending.add(r.seq)
                    result.applied.append(r)
                    logger.info("applied seq=%d txn=%s table=%s pk=%r op=%s fields=%s",
                                r.seq, txn_id, r.table, r.pk, r.op.value, r.fields)
                if commit_seq is not None:
                    self._pending.add(commit_seq)
                if txn_id is not None:
                    result.committed_txns.append(txn_id)
                assembled.add(key)
                resolved_any = True
                logger.info("txn committed txn=%s members=%d", txn_id, len(records))

            for key in assembled:
                self._staging.pop(key, None)

            # ---- 4. 位点推进到连续前缀，并原子持久化 ----
            new_confirmed = confirmed
            while new_confirmed + 1 in self._pending:
                new_confirmed += 1
                self._pending.discard(new_confirmed)
            if resolved_any or intake_dirty or new_confirmed > confirmed:
                try:
                    self._persist_atomic(new_confirmed)
                except BaseException:
                    # 落盘失败（写盘报错等）：内存状态整体回滚到最后一份已确认
                    # 快照，保证数据/位点/暂存/死信始终互相自洽——本批事件视为
                    # 未消费，重投后要么整组生效、要么整组拒绝，绝不残留半组。
                    snap = self._journal.recover()
                    if snap is not None:
                        self._restore_state(snap)
                        logger.warning("persist failed, rolled back to confirmed=%d",
                                       snap["confirmed_seq"])
                    raise
                if new_confirmed > confirmed:
                    logger.info("checkpoint %d -> %d", confirmed, new_confirmed)
            result.committed_seq = self.checkpoints.load()
        return result

    # ================= 查询 =================

    def rejected_transactions(self) -> list[RejectedTransaction]:
        with self._lock:
            return sorted(self._dead.values(), key=lambda d: d.commit_seq)
