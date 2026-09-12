# 与 TanStack Workflow 的关系与分歧

本文档记录 aa-workflow（`workflow-core`）与
[TanStack Workflow](https://tanstack.com/workflow) 的对照结论，包括我们采纳了什么、
拒绝了什么、以及为什么。任何复读这几轮的讨论都可以回到这里。

## 结论速览

第一轮（`118fcbe`）：命名对齐，执行模型保留显式 `needs` DAG + 调度器。
第二轮（`2beb027`，最终）：**执行模型也换成 TanStack 的 handler 代码重放**
（代码即 DAG），`needs` 图和调度器删除。continue_from / target_step 继续保留为
本地一等公民，以 store 层截断 / `StepHalt` 哨兵承载。

## 我们采纳了什么

### 命名对齐（两轮一致，仅为降低 TS/Rust 双语言切换心智负担）

| aa-workflow | TanStack |
| ----------- | -------- |
| `Workflow` / `WorkflowCtx` / `StepCtx` / `StepState` / `StepStatus` / `StepAttempt` | `Workflow` / handler ctx / `StepContext` / 派生 / — / `StepAttempt` |
| `RunEvent` 变体 `STEP_*`；serde tag `type` | `WorkflowEvent` 的 `type: 'STEP_FINISHED'` 等 |
| `RunStore` trait（CAS append + subscribe） | `RunStore` interface |
| `RunState` 信封 | `RunState`（runId/status/workflowId/input/output/error/...） |
| `run_workflow` / `run_workflow_sync` | `runWorkflow` |
| `step_id` | `stepId` |

对齐后，持久化事件 JSON 与 TS 侧日志同构：`{"type":"STEP_FINISHED","stepId":...}`。
大小写（snake vs camel）保留各语言惯例，概念等价即可。

### 执行模型（第二轮才采纳）：handler 代码重放

与 TanStack 一致：handler 每次从头跑一遍代码，引擎靠 checkpoint 按 `stepId`
短路已成功的 step（`STEP_FINISHED` 命中即返回缓存结果、不再调用 `run`；
`STEP_FAILED` 只会 rethrow，**不会自动重跑才失败过的 step**）。重放决定确定性，
副作用只能通过 `ctx.step` 表达；并行就是 handler 里 `tokio::try_join!`（TanStack
的 `Promise.all`）。`StepSpec` / `StepContext` 因此删除，只剩 `ctx.step(...).await`。

事件分两层：

- **observability-only（不落盘，走 publisher）**：`RunStarted`、`StepStarted`、`StepProgress`；
- **checkpoint（落盘，CAS append）**：`StepFinished`、`StepFailed`、`RunFinished`、`RunErrored`。

## 为什么第二轮把执行模型换成 handler 重放

第一轮拒绝的三条理由逐条重估：

1. **"handler 模型要复现 continue_from，得自己写 truncate（TanStack 不给）"**
   ——结论本身没错：continue_from 确实活在 store/log 层（截断后缀 + 重放），
   handler 引擎和显式图引擎都能承载它。但换 handler 后"自己写 truncate"并没有更贵：
   引擎只多一个 `truncate_runs` 的 store 方法，换来的是调度器整段删除。
2. **"二者能力等价，只是代码表达形态区别"** ——等价对，但形态即 API 面。
   `try_join!` 内联并行比 `needs` 图更贴 TanStack 生态，且确定性不变
   （重放短路依赖的一直是事件日志，与表达形态无关）。
3. **"保留图 + store 层 truncate 白拿命名养料"** ——命名养料其实不用靠图拿；
   图带来的却是调度器/~900 行内核 + 测试面，收益没有兑现。

换内核后的净收益：

- 删掉 scheduler（执显式 DAG 拓扑 + `max_concurrency`），`DrvInner` 只剩
  `log_len` 单调 CAS + `lives`（重放短路缓存）；
- 重放短路自带 **resume**：同 run_id 重跑自动跳过已成功 step；
- continue_from 变成纯 store 语义，图/非图无关。

## 保留了分歧（本地扩展 / 一等公民）

1. **`continue_from`**：TanStack 官方没有（重放短路无条件，无 reset/force 语义）。
   我们把它做成 `truncate_runs(run_id, step_id)`：事件日志裁到该 step 最新的
   终态 checkpoint（含），重放时前缀短路、后缀从零重跑。LocalDub 的
   `continue_pipeline` / engine 都由此承载。
2. **`target_step`**：TanStack 用 handler 内 early `return`；我们用引擎参数 +
   `StepHalt` 哨兵，命中即停、handler 展开（`?` 传播）后 run `Finished`。
3. **资源门**：`resource` key 用容量-1 的 async `Semaphore`，同 key 串行
   （等待而不是"并发守恒"）。对齐"同一把锁"的直觉，先到先跑。
4. **`up_to_date`（make 式 freshness）**：成功 checkpoint 存在但 make-check 报
   stale 时仍重新执行——TanStack 没有这个钩子，是续跑定语的本地扩展。
5. **signals / pause**：TanStack 用 `ctx.approve` + `__timer`（sleep）内建；
   我们按同一语义落地：`ctx.approve(key, reason)` / `ctx.sleep(key, dur)` 在
   handler 内挂起，引擎持久化 `STEP_PAUSED` checkpoint（`due_at` 供 timer
   host），外部 `signal_run(run_id, step_id, payload)` 追加 `STEP_RESUME`
   唤醒（approvals），sleep 到期由引擎自 deliver。等待方是轮询式
   （25ms 重读日志），因为 LocalDub `FsRunStore` 无 `subscribe`；重放时
   已交付的 `STEP_RESUME` 从日志短路，挂起点幂等（绝无重复 append）。

## 换了模型仍没变的硬设计

- append-only 日志 + 单写者 CAS（`DrvInner.log_len` 单调推进，冲突重基）：
  崩溃恢复、多节点一致的根基，不随执行模型动摇。
- 失败即终局：`STEP_FAILED` rethrow；重试靠 `continue_from` / 新 run。
  这正好是 TanStack 的语义，也是 Temporal 档位的惯例——dur 面向无人值守长跑，
  "就地重跑失败段"罕有。
- `continue_from` 的截断是"删"而非"追加遮盖"，呼应"新 run 惯例"，但本地
  pipeline（产物在磁盘）接受就地重跑作为一等 UX。