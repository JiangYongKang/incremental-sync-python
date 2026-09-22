""":synopsis: 增量同步引擎：位点校验 -> 逐事件校验与应用 -> 原子提交。

每个 :meth:`SyncEngine.submit` 的执行模型：

1. 在存储锁内完成全部工作，使并发批次天然串行化（先到者推进位点，
   后来者按位点规则被明确拒绝）；
2. 先做批次级位点校验（回退/跳段 -> 抛异常，整批拒绝、零副作用）；
3. 再按 seq 顺序逐个处理数据事件与结构变更事件：
   - 数据事件：按它声明的 ``schema_version`` 取结构，做严格的主键、
     字段存在性与类型校验；非法记录进入死信（原因可区分），不静默丢弃；
   - 结构变更：走兼容规则；不兼容则整批拒绝；兼容时对存量行做
     确定性回填（新列=声明的默认值）；
4. 所有变更（数据 + 死信 + 新结构 + 新位点）在同一个原子快照中落盘。

因此：重复投递 / 乱序到达 / 并发提交，目标端最终状态都与"按序且只投递
一次"完全一致；非法记录不阻塞流，但每条都留有可区分原因。
"""

from __future__ import annotations

import logging
from typing import Any

from sync.checkpoint import CheckpointManager
from sync.errors import (
    RejectReason,
    SchemaIncompatibleError,
    SyncError,
)
from sync.models import (
    ChangeEvent,
    CommitResult,
    OpType,
    RejectedRecord,
    SchemaChangeEvent,
    TableSchema,
)
from sync.schema_registry import SchemaRegistry, check_value
from sync.storage import LocalStore

logger = logging.getLogger("sync.engine")

#: 进入引擎的事件可以是这两种
AnyEvent = ChangeEvent | SchemaChangeEvent


class SyncEngine:
    """编排位点、结构、行数据与死信的同步引擎。"""

    def __init__(self, store: LocalStore) -> None:
        self.store = store
        self.registry = SchemaRegistry()
        self.checkpoints = CheckpointManager()

    # ================================================================ 对外 API
    @staticmethod
    def _dedup_same_seq(
        batch: list[AnyEvent],
    ) -> tuple[list[AnyEvent], list[int]]:
        """去除同一批次内重复 seq 的副本。

        - 同 seq 且内容完全相同（典型的单条记录重投）：保留一份，其余计入
          ``skipped_duplicates``，最终效果与只投递一次一致；
        - 同 seq 但内容不同：流本身自相矛盾，无法判定哪条为准，直接拒绝整批。
        """

        by_seq: dict[int, AnyEvent] = {}
        skipped: list[int] = []
        for ev in batch:
            prior = by_seq.get(ev.seq)
            if prior is None:
                by_seq[ev.seq] = ev
            elif prior.to_dict() == ev.to_dict():
                skipped.append(ev.seq)
            else:
                raise SyncError(
                    f"conflicting payloads for the same seq={ev.seq} within one "
                    f"batch; refusing to guess the winner"
                )
        return list(by_seq.values()), sorted(skipped)

    @property
    def committed_seq(self) -> int:
        return self.store.get_checkpoint()

    def state_dict(self) -> dict[str, Any]:
        """返回当前状态快照（测试/演示用，只读）。"""

        return self.store.snapshot()

    def register_table(self, schema: TableSchema) -> None:
        """登记一张新表（建表动作，不占数据位点，须在写入前完成）。"""

        def mutate(state: dict[str, Any]) -> None:
            self.registry.register_initial(state, schema)

        self.store.commit(mutate)
        logger.info("[DDL] register table=%s v%s pk=%s cols=%s",
                    schema.table, schema.version, schema.primary_key,
                    schema.column_names)

    def submit(self, batch: list[AnyEvent]) -> CommitResult:
        """提交一个事件批次。

        批次可乱序传入，引擎按 seq 处理。批次必须恰好接续当前位点
        （回退/跳段会抛异常，整批拒绝、无副作用）。

        同一个引擎实例上的并发 submit 由存储锁串行化；不同进程对同一
        文件并发写不在本模拟范围内（原子 replace 保证文件不损坏，
        但跨进程互斥需外部文件锁，属于文档说明的边界条件）。
        """

        if not batch:
            return CommitResult(committed_seq=self.committed_seq)

        logger.info("---- batch received: size=%d seqs=%s",
                    len(batch), sorted(e.seq for e in batch))

        # 所有读写在同一把锁 + 同一个原子提交内完成
        with self.store.lock:
            snapshot = self.store.snapshot()
            unique_events, same_seq_skips = self._dedup_same_seq(batch)
            ordered = self.checkpoints.validate_batch(snapshot, unique_events)

            result = CommitResult(committed_seq=self.checkpoints.get(snapshot))
            result.skipped_duplicates = same_seq_skips

            def mutate(state: dict[str, Any]) -> None:
                applied: list[int] = []
                rejected: list[RejectedRecord] = []
                ddl: list[int] = []

                for ev in ordered:
                    if isinstance(ev, SchemaChangeEvent):
                        self._apply_schema_change(state, ev, ddl)
                    else:
                        self._apply_data_event(
                            state, ev, applied, rejected
                        )

                new_seq = ordered[-1].seq
                self.checkpoints.advance(state, new_seq)
                result.committed_seq = new_seq
                result.applied = applied
                result.rejected = rejected
                result.schema_changes = ddl

                if rejected:
                    self.store.append_dead_letters_locked(rejected)

            self.store.commit(mutate)

        for ev in ordered:
            logger.info("[event] seq=%d kind=%s table=%s%s",
                        ev.seq,
                        "ddl" if isinstance(ev, SchemaChangeEvent) else ev.op.value,
                        ev.table,
                        "" if isinstance(ev, SchemaChangeEvent)
                        else f" key={ev.key} data={ev.data} sv={ev.schema_version}")
        logger.info(
            "---- batch committed: checkpoint=%s applied=%s skipped_dup=%s "
            "rejected=%s ddl=%s",
            result.committed_seq, result.applied, result.skipped_duplicates,
            [(r.seq, r.reason.value) for r in result.rejected],
            result.schema_changes,
        )
        return result

    def get_row(self, table: str, **key: Any) -> dict[str, Any] | None:
        return self.store.get_row(table, key)

    def all_rows(self, table: str) -> list[dict[str, Any]]:
        return self.store.all_rows(table)

    def dead_letters(self) -> list[dict[str, Any]]:
        return self.store.dead_letters()

    def latest_schema(self, table: str) -> TableSchema | None:
        with self.store.lock:
            return self.registry.latest(self.store.snapshot(), table)

    # ================================================================ 结构事件
    def _apply_schema_change(
        self,
        state: dict[str, Any],
        ev: SchemaChangeEvent,
        ddl: list[int],
    ) -> None:
        if ev.new_schema.version == 1:
            raise SyncError(
                f"schema change at seq={ev.seq} cannot use version 1; "
                "use register_table for the initial version"
            )
        added, defaults = self.registry.register_change(state, ev.new_schema)

        # 对存量行执行确定性回填：新列 = 结构声明的默认值。
        # 只回填旧行中确实缺失的列，不覆盖任何已存在的值。
        bucket = state["tables"].setdefault(ev.table, {})
        for row in bucket.values():
            for name in added:
                if name not in row:
                    row[name] = defaults[name]

        ddl.append(ev.seq)
        logger.info("[DDL] seq=%d table=%s upgraded v%s, added=%s defaults=%s",
                    ev.seq, ev.table, ev.new_schema.version, added, defaults)

    # ================================================================ 数据事件
    def _apply_data_event(
        self,
        state: dict[str, Any],
        ev: ChangeEvent,
        applied: list[int],
        rejected: list[RejectedRecord],
    ) -> None:
        schema = self.registry.latest(state, ev.table)
        if schema is None:
            self._reject(rejected, ev, RejectReason.SCHEMA_VERSION_UNKNOWN,
                         f"table {ev.table!r} is not registered")
            return

        # 事件允许引用当前表任意已注册版本；版本未注册则拒绝，
        # 防止按错误版本解读列（防错列/错位写入）。
        try:
            event_schema = self.registry.get(state, ev.table, ev.schema_version)
        except SyncError:
            self._reject(rejected, ev, RejectReason.SCHEMA_VERSION_UNKNOWN,
                         f"schema version {ev.schema_version} not registered "
                         f"for table {ev.table!r} (latest=v{schema.version})")
            return

        key_error = self._validate_key(event_schema, ev)
        if key_error:
            self._reject(rejected, ev, RejectReason.MALFORMED_RECORD, key_error)
            return

        bucket = state["tables"].setdefault(ev.table, {})
        store_key = self.store.encode_key(ev.key)
        existing = bucket.get(store_key)

        if ev.op is OpType.INSERT:
            if existing is not None:
                self._reject(rejected, ev, RejectReason.DUPLICATE_KEY,
                             f"row with pk {ev.key} already exists")
                return
            row, problems = self._build_insert_row(event_schema, schema, ev)
            if problems:
                first_reason = problems[0][0]
                self._reject(rejected, ev, first_reason,
                             "; ".join(p[1] for p in problems))
                return
            bucket[store_key] = row
            applied.append(ev.seq)

        elif ev.op is OpType.UPDATE:
            if existing is None:
                self._reject(rejected, ev, RejectReason.ROW_NOT_FOUND,
                             f"no row with pk {ev.key} to update")
                return
            updated, reason, detail = self._build_update_row(event_schema, schema,
                                                             existing, ev)
            if detail:
                self._reject(rejected, ev, reason, detail)
                return
            bucket[store_key] = updated
            applied.append(ev.seq)
        elif ev.op is OpType.DELETE:
            if existing is None:
                self._reject(rejected, ev, RejectReason.ROW_NOT_FOUND,
                             f"no row with pk {ev.key} to delete")
                return
            del bucket[store_key]
            applied.append(ev.seq)

        else:  # pragma: no cover - 枚举已穷尽
            self._reject(rejected, ev, RejectReason.MALFORMED_RECORD,
                         f"unknown op {ev.op!r}")

    # ---------------------------------------------------------- 行构造/校验
    def _validate_key(self, schema: TableSchema, ev: ChangeEvent) -> str | None:
        if set(ev.key.keys()) != set(schema.primary_key):
            return (f"primary key mismatch: event key columns "
                    f"{sorted(ev.key)} != schema pk {list(schema.primary_key)}")
        for pk in schema.primary_key:
            col = schema.column(pk)
            assert col is not None
            err = check_value(col, ev.key[pk])
            if err:
                return f"invalid primary key value: {err}"
        # data 里若也带了主键，必须与 key 一致，否则记录自相矛盾
        for pk in schema.primary_key:
            if pk in ev.data and ev.data[pk] != ev.key[pk]:
                return (f"primary key conflict between key={ev.key[pk]!r} "
                        f"and data={ev.data[pk]!r}")
        return None

    def _build_insert_row(
        self,
        event_schema: TableSchema,
        latest: TableSchema,
        ev: ChangeEvent,
    ) -> tuple[dict[str, Any], list[tuple[RejectReason, str]]]:
        """按事件版本校验输入，按最新结构投影存储。返回 (行, 问题列表)。"""

        problems: list[tuple[RejectReason, str]] = []
        incoming = dict(ev.data)
        for pk in latest.primary_key:
            incoming.setdefault(pk, ev.key[pk])

        # 1) 拒绝事件版本中不存在的列（防止错位/错列写入）
        event_columns = {c.name: c for c in event_schema.columns}
        for name in incoming:
            if name not in event_columns:
                problems.append(
                    (RejectReason.UNKNOWN_FIELD,
                     f"unknown field {name!r} for schema "
                     f"v{event_schema.version} of {ev.table!r}")
                )

        # 2) 按事件版本逐列做存在性与严格类型校验
        typed: dict[str, Any] = {}
        problem_cols: set[str] = set()
        for col in event_schema.columns:
            if col.name in incoming:
                err = check_value(col, incoming[col.name])
                if err:
                    problems.append((RejectReason.TYPE_MISMATCH, err))
                    problem_cols.add(col.name)
                else:
                    typed[col.name] = incoming[col.name]
            elif col.name in latest.primary_key:
                # 主键缺失（正常不会发生，key 已合并，双保险）
                problems.append((RejectReason.MISSING_FIELD,
                                 f"missing primary key field {col.name!r}"))
                problem_cols.add(col.name)
            elif col.default is not None or col.nullable:
                typed[col.name] = col.default
            else:
                problems.append((RejectReason.MISSING_FIELD,
                                 f"required field {col.name!r} missing "
                                 f"(NOT NULL without default)"))
                problem_cols.add(col.name)

        # 3) 投影到最新结构：新版本新增列按默认值稳定补齐
        row: dict[str, Any] = {}
        for col in latest.columns:
            if col.name in typed:
                row[col.name] = typed[col.name]
            elif col.default is not None or col.nullable:
                row[col.name] = col.default
            elif col.name not in problem_cols:
                # 理论不可达：非空新列在 DDL 阶段已强制默认值
                problems.append((RejectReason.MISSING_FIELD,
                                 f"cannot fill new required column {col.name!r}"))
        return row, problems

    def _build_update_row(
        self,
        event_schema: TableSchema,
        latest: TableSchema,
        existing: dict[str, Any],
        ev: ChangeEvent,
    ) -> tuple[dict[str, Any], RejectReason | None, str | None]:
        """部分列更新：只改 data 中出现的列，其余列原样保留。

        成功返回 ``(新行, None, None)``；拒绝返回 ``({}, 原因, 详情)``。
        """

        event_columns = {c.name: c for c in event_schema.columns}
        for name in ev.data:
            if name in latest.primary_key:
                continue  # 主键冲突已在 _validate_key 处理
            if name not in event_columns:
                return {}, RejectReason.UNKNOWN_FIELD, (
                    f"unknown field {name!r} for schema v{event_schema.version} "
                    f"of {ev.table!r}")
            col = event_columns[name]
            err = check_value(col, ev.data[name])
            if err:
                return {}, RejectReason.TYPE_MISMATCH, err

        updated = dict(existing)
        for name, value in ev.data.items():
            if name in latest.primary_key:
                continue
            updated[name] = value
        return updated, None, None

    # ---------------------------------------------------------- 杂项
    @staticmethod
    def _reject(
        rejected: list[RejectedRecord],
        ev: ChangeEvent,
        reason: RejectReason,
        detail: str,
    ) -> None:
        rejected.append(RejectedRecord(
            seq=ev.seq, table=ev.table, op=ev.op.value,
            reason=reason, detail=detail, event=ev.to_dict(),
        ))
        logger.warning("[reject] seq=%d table=%s op=%s reason=%s detail=%s",
                       ev.seq, ev.table, ev.op.value, reason.value, detail)
