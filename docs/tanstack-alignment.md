# 与 TanStack Workflow 的关系与分歧

本文档记录 aa-workflow（`aa-workflow-core`）与
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
| `WorkflowEvent` 变体 `STEP_*`；serde tag `type` | `WorkflowEvent` 的 `type: 'STEP_FINISHED'` 等 |
| `RunStore` trait（CAS append + subscribe） | `RunStore` interface |
| `RunState` 信封 | `RunState`（runId/status/workflowId/input/output/error/...） |
| `run_workflow` / `run_workflow_sync` | `runWorkflow` |
| `step_id` | `stepId` |

对齐后事件**类型名**与 TS 一致（`type` 标签 + `SCREAMING_SNAKE` 取值），所以
grep 日志、对照 TS 源码读事件序列时是直接对应的。

字段名各守语言惯例，不做逐字段对齐：

```jsonc
// Rust 侧序列化（snake_case）
{"type":"STEP_FINISHED","ts":1,"run_id":"r1","step_id":"a",
 "attempts":[{"attempt":1,"started_at":1,"finished_at":2}]}
// TanStack（camelCase）
{"type":"STEP_FINISHED","ts":1,"runId":"r1","stepId":"a",
 "attempts":[{"attempt":1,"startedAt":1,"finishedAt":2}]}
```

### 为什么「落盘 JSON 与 TS 逐字段一致」不是需求

写第一版对齐文档时（118fcbe）顺手把「持久化事件 JSON 与 TS 侧日志同构」当成
了一个需求。它错在**前提**：core 根本没有「落盘格式」这个东西。

- 存成什么文件名、什么布局、什么序列化格式，全是 **store 实现者**的事。
  `events.jsonl` / `run.json` 是示例层 `FileRunStore`（`publish = false`）的
  内部选择，core 里没有一行代码知道它们存在。换 Postgres、S3、或自己写的
  store，事件长什么样由那个 store 决定。
- 所以「跨语言读日志」要面对的是**某个 store 实现**的格式，不是本库的契约。
  就算真有跨语言需求，对齐也是那个 store 作者的事，不是这里的 API 设计。
- aa-workflow 是要**替代** TS 引擎承载 LocalDub（纯 Rust），不是与它互操作。

`RunStore` 契约真正覆盖的只有：方法签名与传参语义（`run_id`、
`expected_next_index`）、事件 `type` 取值（`fold_step_states` 要按它派状态）、
以及 `Conflict` / `Io` 的错误语义。**不覆盖**：字段名 casing、文件布局、
序列化格式。

结论：文档原先那句「同构」是错的，已纠正。事件类型名对齐保留（有用，是 grep
日志和对照源码时看的东西）；其余不动。

### 执行模型（第二轮才采纳）：handler 代码重放

与 TanStack 一致：handler 每次从头跑一遍代码，引擎靠 checkpoint 按 `stepId`
短路已成功的 step（`STEP_FINISHED` 命中即返回缓存结果、不再调用 `run`；
`STEP_FAILED` 只会 rethrow，**不会自动重跑才失败过的 step**）。重放决定确定性，
副作用只能通过 `ctx.step` 表达；并行就是 handler 里 `tokio::try_join!`（TanStack
的 `Promise.all`）。`StepSpec` / `StepContext` 因此删除，只剩 `ctx.step(...).await`。

事件分两层：

- **observability-only（不进日志，走 publisher）**：`RunStarted`、`StepStarted`、`StepProgress`；
- **checkpoint（进日志，CAS append）**：`StepFinished`、`StepFailed`、`RunFinished`、`RunErrored`。

「进日志」不等于「持久化」：日志怎么存由 [`RunStore`](packages/aa-workflow-core/src/run_store/mod.rs)
实现决定，`InMemoryStore` 进程一退就没了。

## 为什么第二轮把执行模型换成 handler 重放

第一轮拒绝的三条理由逐条重估：

1. **"handler 模型要复现 continue_from，得自己写 truncate（TanStack 不给）"**
   ——结论本身没错：continue_from 确实活在 store/log 层（截断后缀 + 重放），
   handler 引擎和显式图引擎都能承载它。但换 handler 后"自己写 truncate"并没有更贵：
   引擎只多一个 `truncate_log_at_step` 的 store 方法，换来的是调度器整段删除。
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
   我们把它做成 `truncate_log_at_step(run_id, step_id)`：事件日志裁到该 step 最新的
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
   handler 内挂起，引擎持久化 `STEP_PAUSED` checkpoint（`deadline` 是**绝对
   时间戳**，供 timer 驱动器索引），外部 `signal_run(run_id, step_id, payload)`
   追加 `STEP_RESUME` 唤醒；重放时已交付的 `STEP_RESUME` 从日志短路，挂起点
   幂等（绝无重复 append）。
   **挂起即返回**：写到挂起点就抛 `WorkflowParked`，drive 以 `Paused` 收尾，
   不阻塞——与上游 `throw new WorkflowPaused()` / `if (engine.paused) return`
   同形。唤醒一律来自外部（runtime 的 `sweep` / `deliver_signal`）。
   见 `docs/runtime-design.md` D3。

## 驱动入口：`RunWorkflowOptions` 的字段对照

上游 `runWorkflow` 收一个结构体（`engine/run-workflow.ts:34-72` 的
`RunWorkflowOptions`），`workflow` / `runStore` 必填。我们照做（2026-09-18 起；
此前是 4 个位置参数，易传错位）：

```rust
RunWorkflowOptions::new(workflow, run_store)   // 必填两项由 new() 强制
    .input(..).run_id(..).continue_from(..).target_step(..)
    .deadline(..).min_yield_remaining(..).yield_resume_at(..)
    .publisher(Some(..))
```

逐字段对照（截至 2026-09-18）：

| 上游 `RunWorkflowOptions` | 本仓 | 说明 |
| --- | --- | --- |
| `workflow` / `runStore` | ✅ `workflow` / `run_store` | 必填，`new()` 强制 |
| `input` / `runId` | ✅ 同名 | |
| `deadline` / `minYieldRemainingMs` / `yieldResumeAt` | ✅ 同名 | |
| `publish` | ✅ `publisher` | 位置从参数移进结构体 |
| `signalDelivery` / `approval` | — | 形态差异：上游「带投递再跑一次」是一个调用；本仓先 `signal_run` / `signal_event` 落盘（`STEP_RESUME`），再单独 drive（D3） |
| `recover` | ❌ 暂无 | 「认领过期 lease 后重放中断的 run」。本仓有 `claim_stale_runs`（认领）但没有对应的恢复入口——**真缺口**，补它要连着 runtime 闭环 |
| `attach` | ❌ 暂无 | 只读订阅已有 run（不驱动）。本仓 `subscribe` 只能 tail 事件，拿不到 RunState 快照 |
| `signal`（AbortSignal） | ❌ 暂无 | 本仓有 `cancel_run`（写 Aborted 终局），不是 drive 参数 |
| `threadId` / `outputSink` / `telemetry` | ❌ 暂无 | 无 thread 概念 / 无 OTel 集成 |
| — | ➕ `continue_from` / `target_step` | **本地扩展**（见上节 1、2） |

名字差异：旧版本仓叫 `RunOptions`，2026-09-18 起对齐为 `RunWorkflowOptions`。
另注：上游把这个接口定义在 `engine/run-workflow.ts` 而非 `types.ts`（与 `RunStore`
的放法不一致），对照时容易漏。

## 换了模型仍没变的硬设计

- append-only 日志 + 单写者 CAS（`DrvInner.log_len` 单调推进，冲突重基）：
  崩溃恢复、多节点一致的根基，不随执行模型动摇。
- 失败即终局：`STEP_FAILED` rethrow；重试靠 `continue_from` / 新 run。
  这正好是 TanStack 的语义，也是 Temporal 档位的惯例——dur 面向无人值守长跑，
  "就地重跑失败段"罕有。
- `continue_from` 的截断是"删"而非"追加遮盖"，呼应"新 run 惯例"，但本地
  pipeline（产物在磁盘）接受就地重跑作为一等 UX。

## 上游的演化史：为什么 runtime 层是后来才有的

对照我们在 [README 的已知边界](../README.md) 里承认的缺口（无 lease / 无 sweep /
无 schedules / 无 timer 索引），值得看一眼上游是怎么长出这一层的——它解释了
「core 稳定后再补 runtime」是个正常节奏，而不是我们落后了。

调查方法：`git log --diff-filter=A -- packages/<name>/` 追每个包的诞生 commit。

### 时间线（41 个 commit，2026-05-20 ~ 07-21）

| 阶段 | 时间 | 内容 |
| ---- | ---- | ---- |
| A. 从 ai-orchestration 抽出 | 05-20 ~ 05-22 | `965a8a5` 抽出核心 → `4f64b9c` 闭包引擎重写 → `c98f260` 发 0.0.1 |
| B. **runtime 诞生** | 05-24 ~ 05-28 | `f3f6381` 补发布面 → `5d05fa8` runtime + adapters + 全套 docs |
| C. adapters 扩张 | 05-29 ~ 05-30 | Cloudflare/Railway adapter、store schema、schema primitives |
| D. 收尾加固 | 07-20 ~ 07-21 | runtime deadline + observability、lease 恢复加固 |

**最值得注意的是 `4f64b9c`**（05-20，带 `!` 的破坏性变更）：
`rewrite engine around closure handler + ctx-as-arg + middleware`——
「代码即 DAG」这套内核在**项目出生当天**就定下来了，不是后来演化出来的。
我们 [第二轮采纳 handler 重放](#为什么第二轮把执行模型换成-handler-重放) 对齐的
正是这一刻的设计。

### runtime 是 `5d05fa8`（2026-05-28），距 core 0.0.1 六天

它不是一个包单独出生，那一个 commit 同时加了：

```
packages/aa-workflow-runtime/                    runtime 本体（lease / sweep / timer / schedule）
packages/workflow-vercel/                     host adapter ×4
packages/workflow-netlify/
packages/workflow-cloudflare/                 （次日 ba9cc31）
packages/workflow-railway/
packages/workflow-store-drizzle-postgres/     store adapter
docs/api/runtime.md + host-adapters.md        runtime 与 adapter 的 API 文档
docs/guide/runtime-model.md + deployment.md   执行模型与部署指南
examples/deployment-pocs/                     各平台部署示例
```

动机在 core 自己的文档里写着：core「intentionally not a scheduler, queue,
database adapter, or deployment adapter」（`docs/guide/runtime-model.md:33`），
把「谁来跑、什么时候跑、崩溃了谁接手」留空；runtime 就是来补执行所有权层的。

**对我们的启示**：上游也是先让 core 稳定、再补这一层，间隔 6 天且是一次性大礼包。
我们缺的那块在结构上是合理的阶段性缺位，不是设计缺陷——但补的时候应该照这个
切分来（runtime 是独立包，不下沉进 core）。

### 文档节奏与我们有别

上游的 docs 常**先于**实现：`30e46ba docs(research): add SCHEDULING.md —
cron landscape + future package shape`（05-21）写的是「未来的包形态」，
7 天后 `5d05fa8` 才把 runtime 真做出来。先写清要什么，再动手。

我们的 `docs/tanstack-alignment.md` 相反，是**事后**记录决策；README 更晚。
这不是错，但补 runtime 层时值得借上游的节奏：先落一份「要什么」，
再写代码。

（另一个容易误解的点：本仓 `examples/` 在初始 commit 就有了，但那是 TanStack
library template 的脚手架产物——React/Solid 样板应用，与 workflow 无关。
真正装 workflow 内容的 examples 从 `5d05fa8` 才出现。）

### 参考上游时看哪边：guide 是权威，examples 是过时样例

`docs/guide/` 与 `examples/deployment-pocs/` 的代码示例**写法不一致**，而且
新的那个是 guide。改动前务必以 guide 为准。

时间线（关键：examples 写出来时，guide 用的那套写法已存在 8 天）：

| 内容 | commit | 日期 |
| ---- | ------ | ---- |
| schema 机制（`z.object` / `StandardSchemaV1` / 推断契约） | `4f64b9c` | 05-20 |
| `inference.test.ts` 锁死推断契约 | `6577262` | 05-20 |
| `examples/deployment-pocs/` | `5d05fa8` | 05-28 |
| `docs/guide/index.md` 最后更新 | `87340c8` | 07-20 |

写法对照：

| | examples（旧） | guide（新） |
| ---- | -------------- | ----------- |
| 输入 | `ctx.input as FulfillmentInput` 手动断言 | `input: z.object({...})` + `ctx.input.orderId` 直接属性访问 |
| state / output | 不声明 | `state: z.object(...)` / `output: z.object(...)` |
| 时钟 | 裸用 `Date.now()` | `const now = await ctx.now()` |
| middleware | 无 | `.middleware([requireUser, traced])` |

**examples 里那个 `Date.now()` 是确定性违规**（本仓 `primitives.md:111` 把它列为
footgun），而同一个仓库的 `inference.test.ts` 头部注释正以「AI can write this
with zero annotations」为卖点——examples 恰好是它反对的写法。

权威性依据：`inference.test.ts` 用 `expectTypeOf` 把推断契约**锁在编译期**
（注释原话：any future engine change breaks inference flow → these tests fail at
compile time）。guide 的写法受这个契约保护，examples 的 `as` 断言是绕开契约。

**对我们的影响**：guide 的 `input: z.object(...)` + `ctx.input.field` 对应我们
已做的 `.input::<In>()` + `ctx.input`，方向一致。但 TS 有一套我们**没有**的能力——
从 handler 返回值反推 output 类型（`WorkflowOutput<typeof order>`），以及「先用
`as` 断言糊过去」的逃生舱。Rust 没有 `infer`，所以：

- 类型标注必须手写（这就是 `RunState<In, Out>` + `into_typed` 存在的原因）；
- 那个 `expectTypeOf` 契约我们无法照搬，只能用手写标注 + 测试代偿。

结论：**位置参考 examples（示例层该放什么），代码写法参考 guide。**

### 但 examples 内部也不齐：只有 `cloudflare-d1/` 用了 runtime

上面那批 examples 内部还有一层分裂。`examples/deployment-pocs/` 下 6 个目录，
**只有 `cloudflare-d1/` 引用了 `@tanstack/aa-workflow-runtime`**：

| 目录 | 依赖 | 诞生 |
| ---- | ---- | ---- |
| `cloudflare/` / `netlify/` / `vercel/` / `api/` / `shared/` | 只有 `@tanstack/aa-workflow-core@^0.0.2` | 05-28 |
| **`cloudflare-d1/`** | core + **runtime** + `workflow-cloudflare` + `store-cloudflare-d1`（全 `workspace:*`） | **05-30** |

两个可读的信号：

1. **`cloudflare-d1/` 是后补的**（晚两天，`602cdec`），与那批 05-28 的示例不是
   一次写的。
2. **依赖版本暴露视角差异**：老的写死 `^0.0.2`（发布包消费者视角），
   `cloudflare-d1` 用 `workspace:*`（仓库内开发视角）。

所以 05-28 那批是**半成品**：core 裸跑 + 手写 HTTP 端点，**没有 sweep、没有
lease、没有 timer 投递**——也就是没有 runtime 层。而 `docs/guide/index.md`
（07-20）里的示例全是 `defineWorkflowRuntime` + `runtime.sweep()`。

**这比上一节的结论更严重**：guide 与 examples 的差距不只是写法新旧，是
**架构层次不同**——examples 演示的是没有 runtime 的用法。

**实际参考价值排序**：

1. `docs/guide/` —— 权威，代表当前架构
2. `examples/deployment-pocs/cloudflare-d1/` —— 唯一用了完整架构的示例，且带
   `migrations/0000_workflow_store.sql`，能看到 store 契约在真实数据库里的形状
3. 其余四个平台目录 —— 演示的用法已不推荐

顺带说明：本仓 `examples/shared/` 是我们自己的示例层，对应的是**位置**，不是
上面这批的写法。我们还没有 runtime 层，所以现阶段与 05-28 那批的处境类似——
这也是为什么 README 的「已知边界」要显式列出缺的那几项，而不是含糊带过。