# 本地增量数据同步（incremental-sync-python-bootstrap-a）

一个**零外部依赖、本地可完整验证**的增量数据同步实现：用本地 JSON 文件
同时模拟上游变更流的消费端与目标端数据库，不依赖任何外部服务、真实凭据或
不可访问的资源。

它保证：**重复投递、乱序到达、并发提交、进程重启、表结构演进**下，
目标端的最终状态与"按序、只投递一次"完全一致，且所有失败都有可区分、
可复现的原因。

---

## 1. 快速开始

```bash
uv sync                       # 安装依赖（Python >= 3.14）
uv run pytest -s              # 运行全部单测（-s 可实时看到位点/变更流水）
uv run python main.py         # 端到端演示：乱序/重复/非法记录/结构演进/重启
uv run python main.py --keep  # 演示后保留 ./demo-state/state.json 供检查
```

状态文件是一个人类可读的 JSON，包含 4 个部分：

| 键            | 含义                                               |
| ------------- | -------------------------------------------------- |
| `tables`      | 目标端行数据（按表名 -> 主键 JSON -> 行）          |
| `dead_letters`| 被拒绝记录（含原始事件 + 可区分拒绝原因）          |
| `schemas`     | 每张表全部已注册的结构版本                         |
| `checkpoint`  | 已确认位点（单调连续整数；0 表示尚无提交）         |

四者在**同一个原子快照**中提交（临时文件 + `os.replace`），因此崩溃后
重启只会看到"提交前"或"提交后"，不会看到数据已写但位点没动的中间态。

---

## 2. 核心同步语义

### 2.1 事件与位点

- 事件分为两类：`ChangeEvent`（insert/update/delete）与
  `SchemaChangeEvent`（结构变更）。**结构变更也占用一个位点**，从而数据与
  DDL 处于同一个全序序列中。
- 每条事件带一个全流严格连续递增的 `seq`（从 1 开始）。
- 事件带 `schema_version`，声明它是按哪一版表结构写出的。
- `checkpoint` 表示"所有 `seq <= checkpoint` 的事件都已随数据原子落盘"。

### 2.2 批次提交规则（先校验，后写入）

`SyncEngine.submit(batch)`：

1. 批次可**乱序传入**，引擎按 `seq` 排序后处理；
2. **位点校验**（不满足则抛异常，整批拒绝、目标端零副作用）：
   - 首个 `seq < checkpoint + 1` → `CheckpointRollbackError`（位点回退/整批重投）
   - 首个 `seq > checkpoint + 1` → `CheckpointGapError`（跳段）
   - 批次内部 `seq` 不连续 → `CheckpointGapError`（空洞）
3. 逐条校验并应用；数据行、死信、新结构、新位点在**一个原子快照**中落盘。

### 2.3 重复投递（at-least-once）的结果与只投递一次一致

| 重复形态 | 处理 |
| --- | --- |
| 已提交的整个批次再次投递 | 抛 `CheckpointRollbackError` 明确拒绝，状态不变 |
| 同一批次内出现相同 `seq` 且内容相同 | 幂等去重，仅应用一次，计入 `skipped_duplicates` |
| 同一批次内相同 `seq` 但内容不同 | 抛 `SyncError` 拒绝整批（无法判定以谁为准，不猜） |
| 重启后上游从旧位点重放 | 读盘得到已确认位点，同样按回退拒绝，再从断点续传 |

重复消费不会造成目标端数据重复或丢失；并发提交由引擎内的互斥锁串行化，
先到者推进位点，后来者得到明确的回退拒绝，可读取最新位点后重试。

### 2.4 非法记录：不静默丢弃，进死信并给出可区分原因

记录级问题**不阻塞流**（位点照常推进），但记录被原样写入 `dead_letters`，
原因枚举如下（`sync.errors.RejectReason`）：

| 原因 | 触发条件 |
| --- | --- |
| `duplicate_key` | insert 时目标端已存在相同主键 |
| `row_not_found` | update/delete 时目标端不存在该主键 |
| `missing_field` | NOT NULL 且无默认值的列在 insert 中缺失 |
| `type_mismatch` | 值类型与列声明不符（**严格校验，无隐式转换**） |
| `unknown_field` | 记录携带了该结构版本中不存在的列（防错列/错位写入） |
| `schema_version_unknown` | 引用了未注册的结构版本，或表尚未注册 |
| `malformed_record` | 主键缺列/多列、`key` 与 `data` 中主键值冲突 |

类型校验刻意严格：`"1"` 不会被当作 `1`，`True` 不会被当作 `1`，
`0` 不会被当作 `False`。

### 2.5 insert / update / delete 的精确定义

- **insert**：必须提供全部 NOT NULL 且无默认值的列；可空列/带默认值列
  缺失时按列定义补齐；行最终按"最新结构"投影存储（新版本新增列补默认值）。
- **update**：部分列更新，只修改 `data` 中出现的非主键列，其余列原样保留；
  主键不可修改（改主键的记录按 `malformed_record` 拒绝）。
- **delete**：按主键删除；删除不存在的行进死信（`row_not_found`），
  从而重复删除可复现、可区分，不会被静默当成成功。

---

## 3. 表结构兼容演进规则

结构变更通过 `SchemaChangeEvent` 提交，必须**版本号严格 +1**（乱序登记
结构同样被拒绝）。兼容性判定（见 `sync/schema_registry.py`）：

| 变化 | 判定 | 说明 |
| --- | --- | --- |
| 新增可空列 | 兼容 | 旧行读/写时该列补 `null` |
| 新增带默认值的 NOT NULL 列 | 兼容 | DDL 提交时对**存量行一次性确定性回填**该默认值 |
| 新增 NOT NULL 且无默认值列 | **不兼容** | 存量行无法确定取值，拒绝 |
| 删除列 | **不兼容** | 防止目标端静默丢失数据 |
| 列改名 | **不兼容** | 数据层面等价于"删列+加列"，无法安全识别，显式拒绝 |
| 类型 `int -> float` | 兼容 | 唯一允许的安全拓宽；存量 int 是合法 float |
| 其他类型变化（含 `float -> int`） | **不兼容** | 防止收窄/错位写入 |
| 主键变化 | **不兼容** | |
| 可空列收紧为 NOT NULL（无默认值） | **不兼容** | 旧数据可能为 NULL |
| 修改列默认值 / 放宽为可空 | 兼容 | 仅影响之后新补齐的取值 |

新增列的缺省语义是**确定且稳定**的：默认值来自结构声明本身，存储行始终
投影到最新结构，任何路径（旧版本事件写入、DDL 回填、update 保留未涉及列）
对同一列得到的缺省值都相同。

不兼容变更抛出 `SchemaIncompatibleError`，**整批拒绝（同批后续合法数据
也不写入），位点不推进**，等待上游显式处理，绝不静默接受。

---

## 4. 边界条件与明确取舍

1. **进程崩溃**：原子快照保证数据/位点/结构/死信要么全生效、要么全不生效；
   重启后从已确认位点继续。
2. **进程内并发**：`SyncEngine.submit` 在存储互斥锁内完成"校验+写入+落盘"，
   并发批次天然串行化。
3. **多进程同时写同一状态文件**：不在本模拟的保证范围内。`os.replace`
   保证文件不会损坏，但跨进程互斥需要外部文件锁（真实部署中由数据库事务
   或分布式锁提供）。
4. **位点只增不减**：任何试图把位点写小的操作都抛 `CheckpointRollbackError`。
5. **空批次**：直接返回当前结果，不推进位点。
6. **建表（`register_table`）不占位点**：它是写入前的初始化动作；版本 1
   只能由建表产生，结构变更事件从版本 2 开始。
7. **死信不自动重放**：被拒绝记录需要人工/上游修正后以新的事件重新投递，
   位点不会因为等待修正而停住，避免一条坏记录阻塞整条流。
8. **规模**：本地 JSON 全量快照面向演示与单测，不是高性能存储；接口层
   `LocalStore` 可被替换为真实数据库实现（同样要求"数据+位点"同事务）。

---

## 5. 代码结构

```
src/sync/
  errors.py            # 拒绝原因枚举 + 流级异常（回退/跳段/结构不兼容）
  models.py            # Column/TableSchema/ChangeEvent/SchemaChangeEvent/
                       #   RejectedRecord/CommitResult
  storage.py           # LocalStore：线程安全、原子落盘的本地 JSON 存储
  schema_registry.py   # 结构版本注册、严格类型校验、兼容性判定
  checkpoint.py        # 位点校验：回退/跳段/批次内空洞
  engine.py            # SyncEngine：编排校验、应用、死信、原子提交与流水日志
tests/                 # 37 个单测（位点/结构/存储/引擎全场景）
main.py                # 端到端本地演示
```

流水日志（pytest `-s` 或 main.py 运行时可见）示例：

```
[INFO] sync.engine: ---- batch received: size=2 seqs=[2, 1]
[INFO] sync.engine: [event] seq=1 kind=insert table=orders key={'id': 1} ...
[WARNING] sync.engine: [reject] seq=3 table=orders op=insert reason=duplicate_key ...
[INFO] sync.engine: ---- batch committed: checkpoint=3 applied=[1, 2] rejected=[(3, 'duplicate_key')]
```

## 6. 测试覆盖

- `tests/test_checkpoint.py`：正常接续、乱序排序、位点回退、跳段、空洞；
- `tests/test_schema.py`：加列/删列/改名/类型拓宽收窄/主键变化/版本跳号/严格类型；
- `tests/test_storage.py`：提交失败原子回滚、重启加载、死信往返；
- `tests/test_engine.py`：重复投递、乱序收敛、**多线程并发提交与失败续传**、
  七类拒绝原因可区分、重启续传、跳段拒绝、加列回填、旧版本事件写入、
  不兼容结构整批拒绝、日志包含位点与流水。
