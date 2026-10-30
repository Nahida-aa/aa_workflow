# 崩溃恢复（TS 版本实现）

> 基线：`learn_ls/workflow` `taren/durable-recovery` 分支 HEAD = `0971b4c`
> （TanStack PR #19「Add durable signals, retries, and compensation」源分支）。
> 本文件回答一个问题：**一个跑了一半就崩了的 run，TS 引擎/宿主怎么把它捞回来继续跑**。

> **关于代码片段的约定**：本文每个代码块都从真实测试裁剪而来（裁剪 = 删掉
> 与主题无关的断言和脚手架，**不改写语义**）。每个块下方的「对端测试」给出
> `文件:行` + 测试名，点击即可跳去核对——**片段与测试不一致时，以测试为准**，
> 并回来修文档。路径相对 `learn_ls/workflow`；跑法：
>
> ```bash
> cd /home/aa/repos/lib_ls/learn_ls/workflow
> node node_modules/.bin/vitest run packages/workflow-core   # 128 全绿
> # 跑 workflow-runtime 的测试前先构建 core（跨包引 dist 产物）：
> #   cd packages/workflow-core && node ../../node_modules/.bin/tsdown
> ```
>
> ⚠️ **必须用 `node` 跑，不要用 `bun` 跑 vitest**：bun 跑 vitest 时 zod v4 的
> `import * as z … export { z }` 在 interop 层丢名字（namespace 里 236 个键都在，
> 唯独没有 `z`），症状是 36 个测试假红（`z.object is undefined`）。这是 runner
> 问题，不是代码问题——用 bun 的 `bun test` 则反过来丢 2 个 `vi.*` 测试。
> 完整记录见 `workflow` 仓 `docs/internal/test-runner.md`。


## 0. 一句话总览

**崩溃恢复的本质只有一件：日志可重放 + 已 checkpoint 的 step 按 `stepId` 短路**
（耐久性来自重放，不来自挂起）。进程死了，日志还在 store 里；任何人拿同一个
runId 再驱动一次，已完成的 step 不重跑、未完成的接着跑。

在此之上，**用 runtime 时**多一套自动化四件套：

1. **lease**：每个 drive 给 run 发一个 lease token（`{ owner, expiresAt }`），写 store 时 fence（过期即拒写）。
2. **claim**：sweep 把「lease 过期 / 卡在 queued」的 run **原子认领**给自己（CAS 抢 lease 所有权），拿到所有权才驱动。
3. **replay**：drive 入口 = 重放持久化日志（这本身就是 core 的能力，runtime 只是调用它）。
4. **宿主 sweep**：引擎**不自己活着**——宿主 cron / scheduled function 定期调 `runtime.sweep()`，sweep 把到期 timer + 过期 lease 的 run + 待 materialize 的 schedule 捞出来驱动。

> ⚠️ **lease / claim / sweep 都不在 `workflow-core` 里**（core 只有注释、遥测字段、
> 可选守卫钩子三处「被动知晓」，见 §1 末）。纯 core 路径没有 lease——宿主自己记
> 「哪些 run 要恢复」。四件套是 runtime 栈，不是崩溃恢复的定义。

> 关键设计：**「挂起即返回」+ 日志可重放**（见 `core-runtime-map.md` §4 不变量 #5 / #1）。

> **注意区分「引擎」与「运行时」**：
> - **崩溃恢复的实质（重放 + checkpoint 短路）在 `workflow-core`**，宿主**可以只用 core** 实现：
>   自己记录哪些 run 该恢复、自己决定 delivery，直接调 `runWorkflow({ recover: true })`。
> - **`workflow-runtime` 提供的是「自动化」便利**：`sweep()` 自动捞 stale lease + 自动注入
>   `__timer` / cron。你**可以不用 runtime**，自己实现 claim + 驱动。
> - 两种写法的对照见 **§6（仅 core）** 与 **§7（用 runtime）**。

---

## 1. 崩溃发生在哪、靠什么兜住

先答与路径无关的那半：**崩溃可能发生在一次 drive 的任何一步**——step 闭包执行到一半、两次 append 之间、事件发布时，任何地方。兜住它的不是「引擎优雅退出」，而是两件**与 runtime 无关**的事实：

1. **日志已经落盘**：已 append 的事件在 store 里，进程死了不丢（in-memory store 除外，它随进程丢）；
2. **重放幂等**：已 checkpoint 的 step 重放时短路不重跑，未 checkpoint 的保守重跑（§4）。

引擎自身不需要「感知崩溃」——它连「对端死了」都无法知道。真正的路径分野在
**「谁再来驱动一次」**：runtime 用 lease + sweep 自动发现（下图），纯 core 由宿主
自己记账（§6）。

### runtime 路径：一次 drive 的生命周期

`driveClaimedRun`（`runtime-driver.ts:746`）：

```
resolveLeaseMs → claimRun（原子 CAS 认领）
  → startLeaseHeartbeat（lease/3 周期续租）
  → runWorkflow(recover?: ...)      ← 引擎驱动
  → syncTimerFromRunState（停在 __timer 的 deadline 登记成 timer 记录）
  → releaseRunLease（finally 语义）
```

在这条路径上，兜住崩溃的具体机制是 **lease 会过期**：

- 引擎正常跑时，心跳每 `leaseMs/3` 续一次租约（`startLeaseHeartbeat`，`:1200`）；
- 进程崩了 → 心跳停 → `lease.expiresAt` 到点后无人续 → 这个 run 变成 **stale lease**；
- 下一轮宿主 sweep 调 `claimStaleRuns`：把 lease 过期的 run 交给新的 owner 重新驱动。

所以**用 runtime 时**，「崩溃恢复」的自动化 = **lease 过期 + 下一轮 sweep 重新认领
+ 重放日志续跑**。

> 📎 **证据（core 无 lease）**：lease 是 runtime + execution store 的概念，
> `workflow-core` 里没有它——core 只有三处「被动知晓」：`run-workflow.ts:45` 的
> recover 注释（「宿主 reclaim 过期 lease 之后用这个」）、`telemetry.ts:17` 的
> 可选透传字段 `leaseOwner`、`types.ts:641` 的可选宿主守卫钩子。**没有 lease
> 状态、没有过期、没有认领。**

---

## 2. lease / claim：谁抢到谁跑

store 契约（`types.ts:353`）暴露 `claimStaleRuns`：

```ts
interface ClaimStaleRunsArgs {
  now: number
  limit: number
  leaseOwner: LeaseOwner   // 形如 `sweep:{now}`
  leaseMs: number
}
// 返回 RunClaim[] = { run, lease }
```

> 对端测试：`packages/workflow-runtime/tests/contracts/workflow-execution-store.contract.ts:110`
> "claims stale running leases"（同文件 `:74` "claims, blocks, and reclaims run leases"
> 覆盖认领/阻塞/抢占全路径）。该契约套件被 in-memory / D1 / Postgres 三个 adapter
> 共用——**store 的 lease 语义以它为准**。

`in-memory-store.ts:497` 的参考实现逻辑（其他 store 照此语义）：

- 扫所有 run，挑出满足「**lease 过期**（`run.lease.expiresAt <= now`）**或卡在 `queued`**
  **或有待投递信号 `pendingSignal`** **或缺 timer `missingTimer`**」的；
- 逐个 `updateRun` 写入**新的 lease**（CAS：`claimed` = 写入成功才认领成功，
  避免两个 sweep 抢到同一个 run）；
- 认领成功的 run 进入 claims 返回。

> 「原子认领 + lease 语义」是本仓 store adapter 清单里的检查项（`AGENTS.md`
> 「Adapter 实现清单」第 4 条：原子认领与 lease 语义）。

---

## 3. sweep：宿主的「推一把」

`runtime.sweep()`（`runtime-driver.ts:464` 起）三段循环：

| 循环 | 捞什么 | 调用 |
| --- | --- | --- |
| **recovered**（`:469`） | stale lease / 卡 queued / 待投递的 run | `claimStaleRuns({ limit: 1, leaseOwner: 'sweep:{now}' })`，逐个 `driveClaimedRun(recover: true)` |
| **scheduled**（`:545`） | 到期的 schedule bucket（cron/every） | `claimDueScheduleBuckets` → materialize 成新 run |
| **timers**（`:610`） | 到期的 `__timer` | `claimDueTimers` → `deliverSignal(name:'__timer')` 唤醒 sleep |

每段都有 **bounded 上限**（`maxRecoveredRuns` / `maxScheduledRuns` / `maxTimers`，
默认 `DEFAULT_SWEEP_LIMIT`）+ deadline + 背压（`shouldStopForDeadline`）。宿主只负责
**定期 wake-up tick**，真正「能不能捞到」是 store 的活。

> sweep 是 **runtime 路径**崩溃恢复的主通道：**用 runtime 时宿主必须定期跑 sweep**（上游
> `durable-recovery.md` 升级指南第 4 条「Keep regular sweeps enabled, including
> maxRecoveredRuns and maxTimers」）。in-memory store 数据随进程丢，但它的 sweep
> 语义与 D1/Postgres 一致，测试时用 in-memory 验证行为。
>
> 但 sweep 不是唯一选择——**不用 runtime、纯 core** 也能做崩溃恢复：宿主自己记录
> 「哪些 run 要恢复 + 每次恢复的 delivery」，崩溃后逐个 `runWorkflow({ recover: true, ... })`。
> 区别只是「谁来负责发现该恢复哪个 run」：runtime 的 `claimStaleRuns` 自动扫 lease，
> 纯 core 模式这步要宿主自己维护（见 §6 仅 core / §7 用 runtime 的对照）。

---

## 4. replay：drive 入口把「继续」变成「重放 + 续跑」

认领到 run 后，`driveClaimedRun` 调 `runWorkflow({ recover: true, ... })`
（`run-workflow.ts` 四驱动模式之一，`core-runtime-map.md` §3）。recover 模式做的事：

1. 从 store `getEvents(runId)` 拿全量日志；
2. `fold_step_states` 折叠出每个 step 的 `StepState`（已 checkpoint 的 step = Success）；
3. 重放 handler：**已 checkpoint 的 step 直接取日志结果、闭包不执行**（短路）；
   未 checkpoint 的 step 真实执行；
4. 若 run 停在 `__timer`（`waitingFor.signalName === '__timer'` 且 deadline 已到），
   sweep 自动注入 `signalDelivery: { name: '__timer', signalId: 'timer:{runId}:{stepId}:{deadline}' }`
   （`runtime-driver.ts:528`）把 sleep 唤醒。

> 所以「崩溃后能不能恢复」= 「日志是否持久化 + step 是否已 checkpoint」。
> **未 checkpoint 的 step 会重跑**——这是设计，不是 bug：引擎不知道那个 step
> 的副作用有没有发生（闭包可能跑了一半崩了），只能保守重跑。

---

## 5. 4 种「崩溃后状态」× 恢复路径（runtime 路径速查）

前两列（崩在什么状态）与路径无关；**后两列是 runtime 的答案**——纯 core 路径的
状态分类完全相同，只是「谁发现该恢复」换成宿主自己（§6）。

| run 崩在 | lease | 下一轮 sweep 做什么 | 结果 |
| --- | --- | --- | --- |
| step 跑一半（无 checkpoint） | 过期 | 认领 → replay，该 step **重跑** | 续跑到 checkpoint |
| step 已 checkpoint、还没跑下一步 | 过期 | 认领 → replay，已 checkpoint 短路，续跑下一步 | 无损 |
| 停在 `SIGNAL_AWAITED(__timer)` | 过期 | 认领 → replay → 注入 `__timer`（若 deadline 到） | 唤醒 sleep |
| 停在 `SIGNAL_AWAITED(外部信号)` | 过期 | 认领 → replay → 若 inbox 有 `pendingSignal` 则 `consumeBufferedSignal` 消费；没有就继续 parked 等宿主 `deliverSignal` | 恢复或继续等 |

> `bufferSignals: true` 时，早到的信号进 inbox（`consumeBufferedSignal` +
> `appendEvents` 原子消费），崩溃后 sweep 认领能接着消费；`false`（默认）时早到信号
> 直接返回 `not-waiting`，要宿主自己存 + 重投。

---

## 6. 仅 core：宿主自己驱动恢复（不用 runtime）

`workflow-core` 自带崩溃恢复的**实质**——重放 + checkpoint 短路 + `recover: true`
驱动模式。宿主可以完全不引 `workflow-runtime`，自己负责「发现该恢复哪个 run」：

```ts
import { runWorkflow, inMemoryRunStore } from '@tanstack/workflow-core'
import { collect } from './test-utils'   // = 把 AsyncIterable 抽干的助手（tests/test-utils.ts:5）

const store = inMemoryRunStore()   // 生产换 D1 / Postgres RunStore
const workflow = createWorkflow({ id: 'order', ... }).handler((ctx) => { /* ... */ })

// —— 正常驱动（start）——
// runWorkflow 返回 async generator：不 drain 就不会执行，所以测试里统一 collect()
await collect(runWorkflow({ workflow, runStore: store, runId: 'o-1', input: {...} }))
// 进程在这里崩了；日志已落 store（D1/Postgres），in-memory 则丢

// —— 新进程起来，宿主自己知道「o-1 还卡在 __timer / 外部信号」——
// 崩溃恢复 = 重新调 runWorkflow，带 runId + 该 run 当前挂起点需要的 delivery：
await collect(runWorkflow({
  workflow,
  runStore: store,
  runId: 'o-1',
  recover: true,                                    // recovery 驱动模式（run-workflow.ts:169）
  // 卡在 __timer：
  signalDelivery: { name: '__timer', signalId: 'timer:o-1:step:deadline', payload: undefined },
  // 卡在外部信号：
  // signalDelivery: { name: 'payment-received', signalId: 'pay-1', payload: { paymentId: '456' } },
  // 卡在审批：
  // approval: { approvalId: 'appr-1', approved: true },
}))
```

> 对端测试：`packages/workflow-core/tests/engine.durability.test.ts:23`
> "recovers an interrupted running execution from checkpoints"（`recover: true` +
> 同 runId 重驱，断言「已 checkpoint 的 step 零重跑」）；`:62` "does not re-execute
> step fns on replay" 是同一机制的裸形式。注意 `collect` 是**测试脚手架**，
> 生产宿主等价物 = 「把返回的 async iterable 消费完」（否则 generator 不推进）。

要点：

- **core 不知道「哪些 run 该被恢复」**——它只接受「给我这个 runId + 它的 delivery，我重放续跑」。
  「哪个 run 卡住了、卡在哪、delivery 是什么」是**宿主的账**（宿主自己存 run 状态 / 挂起点）。
- `recover: true` 只是告诉引擎「这是 recovery 驱动」（sweep 与 deliverSignal 未等待时的
  推一把都走它，`core-runtime-map.md` §3）。实际重放逻辑 = 从 store 拿日志 + 短路。
- **没有租约、没有自动 sweep、没有 timer 自动注入**——这些都靠宿主自己实现。
  sleep 的 `__timer` 要宿主 cron 到期后**手动**构造 `signalDelivery` 投进去。

> 适合：宿主已有调度框架（平台帮你 sweep）、或短命单进程「崩了直接开新 run 重跑」
> 的场景。核心语义够用，不想要 runtime 的自动化，就停在这。

---

## 7. 用 runtime：自动 claim + 自动 sweep（完整闭环）

`workflow-runtime` 在 core 之上加了「自动发现 + 自动驱动」：

```ts
import { defineWorkflowRuntime } from '@tanstack/workflow-runtime'

// 宿主（Cloudflare Cron / Railway / Netlify / Vercel scheduler 都行）
const runtime = defineWorkflowRuntime({ store, workflows, bufferSignals: true })

// 正常驱动
await runtime.startRun({ workflowId: 'order', runId: 'o-1', input: {...} })

// ... 进程崩了 ...

// 宿主定期（比如每 30s 一次）：
await runtime.sweep({ maxRecoveredRuns: 25, maxTimers: 25, maxDurationMs: 55_000 })
//   内部自动：
//   1) claimStaleRuns 捞 lease 过期的 run → driveClaimedRun(recover:true) 重放续跑
//   2) claimDueTimers 把到期 __timer 自动 deliverSignal 唤醒 sleep
//   3) claimDueScheduleBuckets 把到期 cron 物化成新 run
```

> 对端测试：`packages/workflow-runtime/tests/runtime-driver.test.ts:701`
> "sweeps due timers and resumes sleeping workflows"（第 2 条）、`:734` "sweeps due
> schedule buckets into deterministic runs"（第 3 条）、`:661` "delivers a signal
> and resumes a paused workflow"（信号恢复）。三段合起来的端到端在
> `packages/workflow-runtime/tests/examples.durable-recovery.test.ts:5`
> "fulfills an order with an early payment and a durable shipping retry"
> （早到信号 + 耐久重试 + sweep 恢复的完整订单流）。

runtime 比「仅 core」多做了三件事：

| 多做的事 | 机制 |
| --- | --- |
| **自动发现**哪些 run 该恢复 | `claimStaleRuns`（`runtime-driver.ts:469`）扫 lease 过期 + queued + pendingSignal + missingTimer |
| **自动认领**（谁抢到谁跑） | lease CAS（`claimRun`，`runtime-driver.ts:796`）+ 心跳续租（`startLeaseHeartbeat`，`:837`） |
| **自动唤醒 timer** | `syncTimerFromRunState`（`:941`）把停在 `__timer` 的 deadline 登记成 timer 记录，sweep 到期自动 `deliverSignal` |

**宿主责任清单**（引擎不管的部分）：

- 定期调 `sweep()`（频率要 ≤ lease 时长，保证崩溃的 run 在 lease 过期后能被下一轮捞到）；
- 用 **D1 / Postgres** store（in-memory 数据随进程丢，只用于测试）；
- 早到信号：`bufferSignals: true`（否则宿主自己存 + 重投）；
- 外部副作用幂等（`StepContext.id` 当 idempotency key）；
- run 保留策略（inbox 条目无自动过期）。

> 两种写法**底层都是同一套**：runtime 的 sweep 内部还是调 core 的 `runWorkflow({ recover: true })`
> 做重放（`driveClaimedRun` → `runWorkflow`）。区别只在「发现 + 认领 + timer 注入」这三件自动化
> 由谁做。

---

## 8. 不变量（违反会怎样）

| 不变量 | 违反后果 |
| --- | --- |
| 日志 append-only + CAS 全序（`appendEvent` index 必须 = 当前长度） | 重放顺序不稳定，两次重放分叉 |
| 已 checkpoint 的 step 无条件短路（闭包不执行） | 重放重跑副作用 → 钱扣两次 / 消息发两遍 |
| lease 过期才允许别的 owner 认领（CAS 闸） | 两个 worker 同时驱动一个 run → 日志冲突 |
| sweep bounded（上限 + deadline） | 一个慢 run 卡死整批 sweep → 其他 run 饿死 |
| 「发现该恢复哪个 run」有人负责（runtime sweep 或宿主自己） | 崩溃的 run 永远不被捞回 |

---

## 9. 代码位置索引（0971b4c）

| 主题 | 文件 : 行 |
| --- | --- |
| `runWorkflow` 四驱动模式（含 `recover:true`） | `packages/workflow-core/src/engine/run-workflow.ts:34/:189/:260/:371`（start/resume/attach），`recover` 走 `:169` 分流 |
| `runWorkflow` generator + 事件队列 | `run-workflow.ts:82` |
| **仅 core 恢复**：`recover:true` + `signalDelivery`/`approval` 直接 `runWorkflow` | `run-workflow.ts:169`（分流）+ `RunWorkflowOptions`（`:34`） |
| `driveClaimedRun` 骨架（认领→心跳→驱动→timer→释放） | `packages/workflow-runtime/src/runtime-driver.ts:746` |
| `sweep`（recovered/scheduled/timers 三段） | `runtime-driver.ts:464/:545/:610` |
| `claimStaleRuns` 契约 + 参考实现 | `runtime/src/types.ts:353` / `in-memory-store.ts:497` |
| `startLeaseHeartbeat`（lease/3 续租） | `runtime-driver.ts:1200` |
| `syncTimerFromRunState`（__timer 登记） | `runtime-driver.ts:941` |
| `bufferSignals` 升级指南 | `docs/guide/durable-recovery.md`（本基线新增） |

## 10. 测试位置索引（0971b4c）

上面每个论断的「对端测试」。路径相对 `learn_ls/workflow`，跑法见顶部约定
（`node node_modules/.bin/vitest run <文件>`）。

| 主题 | 测试文件 : 行（测试名） |
| --- | --- |
| **仅 core 崩溃恢复**（recover + checkpoint 短路） | `packages/workflow-core/tests/engine.durability.test.ts:23` "recovers an interrupted running execution from checkpoints" |
| 重放不重跑 step 闭包 | 同文件 `:62` "does not re-execute step fns on replay" |
| 挂起/恢复的 payload 过墙 | `packages/workflow-core/tests/engine.smoke.test.ts:70` "pauses on approval — stream ends without RUN_FINISHED" |
| lease 认领 / 阻塞 / 抢占 | `packages/workflow-runtime/tests/contracts/workflow-execution-store.contract.ts:74` "claims, blocks, and reclaims run leases" |
| stale lease 认领（`claimStaleRuns` 本体） | 同文件 `:110` "claims stale running leases" |
| 信号投递恢复挂起的 run | `packages/workflow-runtime/tests/runtime-driver.test.ts:661` "delivers a signal and resumes a paused workflow" |
| sweep 唤醒到期 timer | 同文件 `:701` "sweeps due timers and resumes sleeping workflows" |
| sweep 物化到期 schedule | 同文件 `:734` "sweeps due schedule buckets into deterministic runs" |
| **端到端**：早到信号 + 耐久重试 + sweep 恢复 | `packages/workflow-runtime/tests/examples.durable-recovery.test.ts:5` "fulfills an order with an early payment and a durable shipping retry" |
| `collect`（抽干 async iterable 的助手） | `packages/workflow-core/tests/test-utils.ts:5` |
