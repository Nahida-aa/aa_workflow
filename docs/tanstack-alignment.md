# 与 TanStack Workflow 的关系与分歧

本文档记录 aa-workflow（`workflow-core`）与
[TanStack Workflow](https://tanstack.com/workflow) 的对照结论，包括我们采纳了什么、
拒绝了什么、以及为什么。任何复读这几轮的讨论都可以回到这里。

## 我们采纳了什么（命名对齐）

仅为降低 TS/Rust 双语言切换的心智负担，API 术语与 TanStack 对齐：

| aa-workflow | TanStack |
| ----------- | -------- |
| `StepSpec` / `StepState` / `StepStatus` / `StepAttempt` | `StepContext` / `RunState` 派生 / — / `StepAttempt` |
| `RunEvent` 变体 `STEP_*`；serde tag `type` | `WorkflowEvent` 的 `type: 'STEP_FINISHED'` 等 |
| `RunStore` trait（CAS append + subscribe） | `RunStore` interface |
| `RunState` 信封 | `RunState`（runId/status/workflowId/input/output/error/...） |
| `run_workflow` | `runWorkflow` |
| `step_id` | `stepId` |

对齐后，持久化事件 JSON 与 TS 侧日志同构：`{"type":"STEP_FINISHED","stepId":...}`。
大小写（snake vs camel）保留各语言惯例，概念等价即可。

## 我们拒绝了什么（执行模型）

TanStack 的执行模型是 **handler 代码重放**：handler 每次从头跑一遍代码，
引擎靠 `findCheckpoint` 按 `stepId` 短路已成功的 step（`STEP_FINISHED/STEP_FAILED` 命中即
返回缓存结果或 rethrow，且 `STEP_FAILED` 只会 rethrow，**不会自动重跑才失败过的 step**）。
重放决定确定性，副作用只能通过 `ctx.step` 表达。

aa-workflow 保留 **显式 `needs` DAG + 调度器**。理由与差异：

1. **并行无关**。TanStack 的 step 同样可以并行（handler 里 `Promise.all`），
   并行表达力两模型等同。我们选显式图不是因为"pipeline 是线性的"。
2. **`continue_from`（无视成功、从 X 起重跑后缀）TanStack 官方没有，这是硬分歧点**。
   重放短路是无条件的，没有任何 reset/force/truncate 语义。见下文"用户层面有办法吗"。
3. **`target_step` / `continue_from` / `max_concurrency` / `progress()`**
   是本地扩展（TanStack 的提前终止是 handler 内 early `return`，不是引擎参数）。

### 用户层面有办法做 continue_from 吗？

有，但都是 hack，且代价在自身上：

- **换 stepId**（官方语境下的"改法"）：新版本 handler 里把 `separate` 改名
  `separate-v2`，旧 checkpoint 不再命中 → 真重跑。代价：下游 stepId 仍需同步改，
  否则读到旧产物（半新半旧）；日志堆积死 checkpoint，派生态歧义；本质上在"身份造假"。
- **换 runId 重开日志**：这是 restart，不是 continue_from。
- **在自定义 `RunStore` 里删/截断 checkpoint**：`appendEvent/getEvents/getState`
  之外自己加方法，引擎不会管。这等于把 aa-workflow 已经做的事情再实现一遍，
  且 TanStack 的托管存储（Durable Objects 等）根本不允许删。

结论：TanStack 引擎刻意没有一等公民的 continue_from；能做过，但要造假身份、
或自己实现 truncate。这印证了我们的取舍——continue_from 不依赖任何特定引擎形状，
它活在 **store/log 层**（truncate 后缀 + 重放），图引擎和 handler 引擎都能承载它。

### 这算"设计死板"吗？

不是蠢，是价值轴不同。append-only 不可变日志换来：崩溃恢复、多节点 CAS 一致、
"重放绝不重执行"的副作用安全。这是 **Temporal / durable-execution** 档位的设计，
面向无人值守长跑、断网续跑、审计日志——那个世界里"就地重跑后缀"罕有，
惯例就是 new run / new version。

TanStack 比 Temporal 已更宽松：Temporal 甚至要求重放时步序与历史逐一对齐
（确定性更强），TanStack 至少容忍 stepId 变化。

但它的死板恰好压在 pipeline 的正轴上：本地文件管道的"从某步重来"是一等 UX
（LocalDub 的 continue_from / target_step），durability 对它是免费赠品
（产物本来就在磁盘）。所以**领域不同频**：TanStack 把确定性当宗教，
pipeline 把它当负担。

## 为什么不重写为 handler 重放

评估过把执行内核换成 TanStack 的 handler 重放模型。结论：零净收益，纯叠加风险：

- handler 模型要复现 continue_from，必须自己写 truncate（TanStack 不给）——
  与现图引擎做的事一模一样；
- 二者能力等价：并行（Promise.all / fan-out）、target_step（early return / 参数）、
  continue_from（自建 truncate）；唯一区别是代码表达形态；
- 保留图 + store 层 truncate，等于在确定性轴之外白拿"像 TanStack"的命名养料。

因此：**命名对齐做，执行模型不换**。若未来有"重放跳过失败段"
（TanStack `recover` 语义）的需求，那是独立功能，不构成换模型理由。