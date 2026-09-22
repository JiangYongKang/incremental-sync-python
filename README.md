# incremental-sync-python

本地可验证的增量数据同步：用本地文件模拟上游数据源与目标端，不依赖任何外部
服务、真实凭据或网络资源。在**重复投递、乱序到达、并发提交、进程重启、表结构
演进**下，目标端最终状态可预期且可复现。

## 组件

| 模块 | 职责 |
| --- | --- |
| `sync/models.py` | 变更记录 `ChangeRecord`、表结构 `TableSchema`、拒绝原因 `RejectReason` |
| `sync/source.py` | 上游模拟：JSONL 变更日志，seq 从 1 严格递增、无空洞（乱序 append 直接报错） |
| `sync/target.py` | 目标端模拟：本地 JSON 行存储，`{table: {pk: row}}`，原子写盘 |
| `sync/checkpoint.py` | 位点持久化；回退与跳段抛 `CheckpointError` |
| `sync/schema_registry.py` | 表结构注册与兼容演进；不兼容变化抛 `SchemaIncompatibleError` |
| `sync/applier.py` | 核心：幂等应用、乱序归并、校验拒绝、位点推进、状态持久化 |
| `sync/pipeline.py` | `run_once`：从已确认位点读一批并应用，重复调用安全 |
| `main.py` | FastAPI 演示接口（状态落在 `./data/`，纯本地） |

## 同步语义

**位点（seq）**：源端为每条变更分配从 1 开始严格递增、无空洞的序号。
已确认位点 = 目标端已连续应用的最大 seq。

**最终一致性保证**：同一批变更无论被重复投递、乱序到达还是多线程并发提交，
目标端最终状态与“按 seq 顺序只投递一次”完全一致。机制：

- **幂等去重**：`seq <= 已确认位点` 或已在未确认集合中的记录判为重复，跳过。
- **乱序归并**：按 `(table, pk)` 维护高水位（已应用的最大 seq），过期记录跳过，
  保证每个主键的最终值由最大 seq 的变更决定。
- **位点连续推进**：确认位点只推进到连续前缀；存在空洞时停在空洞前等待补齐，
  绝不跳过空洞从中间状态继续。
- **崩溃恢复**：已确认位点、未确认 seq 集合、键高水位全部持久化，重启后从已
  确认位点继续；已确认部分的重复投递被识别为重复，不重复应用、不丢失。

**拒绝语义（不静默丢弃）**：非法记录进入 `ApplyResult.rejected`，原因可区分：

| 原因 | 含义 | 是否消费 |
| --- | --- | --- |
| `PK_CONFLICT` | insert 已存在的主键（确定性，死信）；或 update/delete 不存在的主键（**暂时性**，可能乱序，等待重投，位点不前进） | 前者消费，后者不消费 |
| `MISSING_FIELD` | 必填（非空且无缺省）字段缺失或为 null | 消费（死信） |
| `TYPE_MISMATCH` | 字段类型与表结构不符（`bool` 与 `int` 严格区分） | 消费（死信） |
| `UNKNOWN_TABLE` | 表未注册 | 消费（死信） |
| `SCHEMA_INCOMPATIBLE` | 记录结构版本高于当前，或字段不在当前结构中 | 消费（死信） |

死信类拒绝是确定性的（同记录重投结论相同），因此计入已消费、位点可越过；
暂时性主键冲突不消费，位点停在该处，待缺失的前序变更到达后重投成功。

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
- 永久空洞（某 seq 的变更永远不到达）会使位点停在空洞前——这是有意的
  at-least-once 语义，需要运维介入，而不是静默跳过。
- 主键在目标端统一按 `str` 规范化比较。

## 本地验证

```bash
uv run pytest tests/ -q        # 全部测试（日志含位点与变更流水）
uv run pytest tests/ -q -s     # 实时打印 applied/dup/reject/checkpoint 日志
```

测试覆盖：重复投递、乱序到达、并发提交（多线程重叠批次）、重启续传、
位点回退/跳段拒绝、五类拒绝原因区分、新增列缺省语义、删列/改名/改类型拒绝、
端到端管线（`tests/test_*.py`）。

HTTP 演示（可选）：

```bash
uv run uvicorn main:app --port 8000
curl -X POST localhost:8000/sync/run   # 从已确认位点同步一批，幂等
curl localhost:8000/sync/status        # 查看位点与目标端数据
```
