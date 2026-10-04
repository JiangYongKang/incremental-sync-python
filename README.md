# incremental-sync-python

本地可验证的增量数据同步：用本地文件模拟上游数据源与目标端，不依赖任何外部
服务、真实凭据或网络资源。在**重复投递、乱序到达、并发提交、进程重启、表结构
演进**下，目标端最终状态可预期且可复现。现支持**按业务事务分组的事务化同步**：
同一事务标记下的多条跨表变更作为一个整体生效或整体拒绝。

## 组件

| 模块 | 职责 |
| --- | --- |
| `sync/models.py` | 变更 `ChangeRecord`、事务提交标记 `TxnCommit`、表结构 `TableSchema`、拒绝原因 `RejectReason`、被拒事务 `RejectedTransaction` |
| `sync/source.py` | 上游模拟：JSONL 事件日志（变更记录 + 事务提交标记），seq 从 1 严格递增、无空洞；`append_txn` 整事务写入 |
| `sync/target.py` | 目标端模拟：本地 JSON 行存储，`{table: {pk: row}}`；`apply_atomic` 整组行变更单次加锁原子生效，`read_snapshot`/`all_tables` 单次加锁跨表一致读 |
| `sync/checkpoint.py` | 位点持久化；回退与跳段抛 `CheckpointError`；内存视图与原子快照对齐 |
| `sync/schema_registry.py` | 表结构注册、兼容演进、快照与恢复；不兼容变化抛 `SchemaIncompatibleError` |
| `sync/journal.py` | **原子提交日志**：tmp + `os.replace` 写入完整状态快照，崩溃后以最后一份完整快照为准恢复 |
| `sync/applier.py` | 核心：事务暂存、整组校验、原子生效/整体拒绝、幂等去重、乱序归并、位点推进、原子持久化 |
| `sync/pipeline.py` | `run_once`：从已确认位点读一批事件并应用，重复调用安全 |
| `main.py` | FastAPI 演示接口（状态落在 `./data/`，纯本地） |

## 事务同步语义

**事务单元**：变更记录携带 `txn_id`；同一 `txn_id` 的全部成员记录 + 一个
`TxnCommit(seq, txn_id, members)` 提交标记构成一个事务。提交标记自带一个位点
（大于全部成员），并声明成员 seq 列表——这是判定"事务是否到齐"的唯一依据。
无 `txn_id` 的记录视为**单条隐式事务**，行为与旧版完全一致。

**原子可见性范围**：事务成员到达后先进**暂存区**（不触达目标端），直到
`TxnCommit` 到达且 `members` 全部到齐，才把该事务作为整体决议。任何时刻从
目标端读取，一个事务要么完全没生效，要么全部生效，**绝不会看到半个事务的
中间状态**；跨表事务同样如此。未到齐（成员缺失/分批到达/提交标记先到）时
目标端与位点均不动，暂存内容也持久化，可跨重启补齐。

**并发读取下的保证**：事务生效动作本身是原子的——整组行变更（可跨多表、
多行）由 `LocalTarget.apply_atomic` 在同一把锁内一次性写入，锁不释放读者
就进不来。因此即使在事务正在写入、且有并发读者不断读取时，每次读到的也
只能是"整组生效前"或"整组生效后"，或若干完整事务的组合，绝不会是某个
事务拼凑出的半成品、也不会把不同事务的行错拼在一起。需要跨表一致视图时
必须用 `read_snapshot(tables)` / `all_tables()`（单次加锁一次读全）；逐表
分别调用 `rows()` 不在保证范围内（两次调用之间可能插入其他事务的提交）。
HTTP 演示接口 `/sync/status` 即采用一致快照读取。

**有序决议**：事务按提交位点排序依次决议（隐式=记录 seq，显式=提交标记
seq）。前序事务未到齐，或出现暂时性主键冲突时，其后的事务一并等待，绝不越过
空洞或未决议事务；最终结果与"按 seq 顺序只投递一次"一致。重复投递（整事务
或部分成员重发）、乱序分批到达、多线程并发重叠提交，均得到同一结果。

**整体拒绝**：事务内只要有一条成员非法（主键冲突、必填字段缺失、类型不符、
表未注册、结构版本不符等），整个事务拒绝——所有成员（包括原本合法的）一律不
写入。拒绝原因逐条收集并按成员 seq 排序，**可区分、且对同样内容重投稳定**
（主原因 = seq 最小的非法成员；死信附带规范化内容指纹 sha256）。被拒事务
**完整留存**：内存可通过 `applier.rejected_transactions()` 查询，同时落到
`data/dead_letters.json`（含全部原始记录、逐条原因、指纹），不会丢失。死信
事务计入已消费（位点可越过）；同内容重投判定为重复，结论不变。

**暂时性 vs 确定性失败**：`update/delete` 目标行不存在可能是乱序导致（前序
insert 尚未到达），属**暂时性 PK_CONFLICT**：事务不消费、位点停住、不进
死信，等前序变更到达后整体重投即可成功。结构/字段/`insert` 主键冲突等属
**确定性**失败，整事务死信。

## 原子落盘与崩溃恢复

数据、位点、表结构、键高水位、事务暂存区、被拒事务由 `sync/journal.py` 的
**单一权威快照**绑定：每个决议边界先更新位点内存视图，再以 tmp 文件 +
`os.replace`（+ fsync）原子写入快照；快照落盘后才尽力重写旁路文件
（`target.json` / `checkpoint.json` / `dead_letters.json`）。因此：

- 进程在任何时刻被杀、或写盘报错（tmp 残缺会被忽略），重启时一律以最后一份
  完整快照为准重放：目标端数据、已确认位点、结构、暂存、死信一起恢复。
  **事务在生效过程中途出错不会残留半组**——快照要么整组包含这套事务、要么
  完全不包含，重启续传重投后要么整组正确生效、要么整组被拒。
- **提交过程写盘报错时的内存回滚**：`apply_batch` 在原子落盘失败时会捕获异常，
  把内存中的数据、位点、暂存区、死信整体回滚到最后一份已确认快照（同进程内
  读到的数据、位点与被拒记录始终互相自洽），再把异常抛给调用方；本批事件视为
  未消费，故障恢复后同进程重投即可整组生效。
- **已确认位点绝不会超过已经真正生效的数据**：从已确认位置续跑既不多写也不
  漏写；已生效/已死信部分的重投被识别为重复。
- 位点回退与跳段照旧抛 `CheckpointError`；源端非连续 append 照旧 `ValueError`。
- 暂存区也在快照中：事务分批到达即使跨越重启，剩余成员与提交标记到达后仍能
  整组生效。

## 结构兼容规则

- **新增列：兼容**。缺省语义确定且稳定——以列定义的 `default` 为准（未给为
  `None`）；旧版本记录应用时按缺省补齐新列；已存在的旧行**不被追溯改写**
  （不静默回填），读取方按当前结构自行补缺省。
- **删列、改名（= 旧列消失 + 新列出现）、改类型/可空性：不兼容**。
  `SchemaRegistry.evolve` 抛 `SchemaIncompatibleError`，结构版本不前进，
  绝不静默接受。
- 记录携带当前结构中不存在的字段 → 按 `SCHEMA_INCOMPATIBLE` 拒绝，
  不发生错列/错位写入。

## 边界条件

- 位点回退（`seq <= 已确认`）与跳段（声明的前驱与持久化位点不一致）一律
  `CheckpointError`，进程重启后不会从中间状态继续。
- 源端 append 非连续 seq 直接 `ValueError`，保证“位点之后继续读”良定义。
- 永久空洞（某 seq 的变更或事务提交标记永远不到达）会使位点停在空洞前——
  这是有意的 at-least-once 语义，需要运维介入，而不是静默跳过。
- 提交标记声明的 `members` 与实际到齐的成员不一致时，事务保持暂存等待补齐，
  绝不按部分成员提交。
- 主键在目标端统一按 `str` 规范化比较；同一主键的最终值由最大 seq 的生效
  变更决定（过期记录在乱序补洞/重启续投时跳过，与旧版语义一致）。

## 拒绝原因速查

| 原因 | 含义 | 事务级处理 |
| --- | --- | --- |
| `PK_CONFLICT` | insert 已存在主键（确定性）；update/delete 目标不存在（暂时性，可能乱序） | 前者整事务死信；后者整事务等待重投、位点不前进 |
| `MISSING_FIELD` | 必填（非空且无缺省）字段缺失或为 null | 整事务死信 |
| `TYPE_MISMATCH` | 字段类型与表结构不符（`bool` 与 `int` 严格区分） | 整事务死信 |
| `UNKNOWN_TABLE` | 表未注册 | 整事务死信 |
| `SCHEMA_INCOMPATIBLE` | 结构版本高于当前，或字段不在当前结构中 | 整事务死信 |

## 本地验证

```bash
uv run pytest tests/ -q        # 全部测试（日志含位点、事务与变更流水）
uv run pytest tests/ -q -s     # 实时打印 staged/applied/reject/commit/checkpoint 日志
uv run pytest tests/test_txn_visibility.py -q   # 并发读取原子可见性（可反复运行，稳定通过）
uv run pytest tests/test_txn_recovery.py -q     # 写盘失败/进程被杀后的失败与恢复边界
```

测试覆盖（共 34 个用例，原有用例无回归）：

- 旧语义：重复投递、乱序到达、多线程并发提交、重启续传、位点回退/跳段拒绝、
  五类拒绝原因、新增列缺省、删列/改名/改类型拒绝、端到端管线。
- 事务新增（`tests/test_transaction.py`）：跨表事务原子生效、事务重复投递
  幂等、成员乱序分批到达、多线程并发提交互相重叠/重复。
- 事务拒绝（`tests/test_txn_rejection.py`）：组内一条非法整事务回滚无中间态、
  多种原因逐条收集、死信完整留存（接口 + `dead_letters.json`）、重投指纹稳定、
  暂时性主键冲突阻塞后补齐成功。
- 崩溃恢复（`tests/test_txn_recovery.py`）：journal 写盘失败不留半成品、
  写盘失败后内存状态整体回滚且同进程重投成功、事务半批暂存跨重启、死信跨
  重启、旁路文件落后于 journal 时以 journal 对齐。
- 并发可见性（`tests/test_txn_visibility.py`）：跨表事务一边提交一边用另一
  线程持续读目标端，每次读到的都是整组一致状态（行变更之间注入毫秒级让出
  点，读者必然在生效窗口内反复采样，不靠撞运气）；多线程并发提交 + 并发读
  时快照只能是若干完整事务的组合；被拒事务对读者完全不可见、重启无残留、
  重投结论稳定。
- 事务管线（`tests/test_txn_pipeline.py`）：源端 `append_txn`，读批边界切在
  事务内部、重启后续传追平、空跑幂等。

日志关键字：`txn staged`（仅暂存未到齐）、`txn committed`（整组生效）、
`txn rejected`（整组拒绝）、`journal persisted/recover`（原子落盘/恢复）、
`stale/dup skip`（过期/重复跳过）、`checkpoint a -> b`（位点推进）。

事务流水的本地写法：

```python
from sync.source import LocalSource
source = LocalSource("data/source.jsonl")
# 自动为成员与提交标记分配连续 seq：成员 1,2，提交标记 3
source.append_txn("order-42", [
    ChangeRecord(seq=0, table="orders", pk=10, op=Op.INSERT,
                 fields={"id": 10, "amount": 100}, schema_version=1),
    ChangeRecord(seq=0, table="billing", pk=1, op=Op.INSERT,
                 fields={"id": 1, "total": 100}, schema_version=1),
])
```

HTTP 演示（可选）：

```bash
uv run uvicorn main:app --port 8000
curl -X POST localhost:8000/sync/run   # 从已确认位点同步一批，幂等
curl localhost:8000/sync/status        # 查看位点、目标端数据与被拒事务
```
