# 崩溃恢复（TS 版本实现）

> 基线：`learn_ls/workflow` `taren/durable-recovery` 分支 HEAD = `0971b4c`
> （TanStack PR #19「Add durable signals, retries, and compensation」源分支）。
> 本文件回答一个问题：**一个跑了一半就崩了的 run，TS 引擎/宿主怎么把它捞回来继续跑**。

## 0. 一句话总览

崩溃恢复 = **租约（lease）+ 认领闸（claim）+ 重放（replay）+ 宿主定期 sweep** 四件套：

1. **lease**：每个 drive 给 run 发一个 lease token（`{ owner, expiresAt }`），写 store 时 fence（过期即拒写）。
2. **claim**：sweep 把「lease 过期 / 卡在 queued」的 run **原子认领**给自己（CAS 抢 lease 所有权），拿到所有权才驱动。
3. **replay**：drive 入口 = 重放持久化日志；已 checkpoint 的 step 按 `stepId` 短路不重跑（耐久性来自重放，不来自挂起）。
4. **宿主 sweep**：引擎**不自己活着**——宿主 cron / scheduled function 定期调 `runtime.sweep()`，sweep 把到期 timer + 过期 lease 的 run + 待 materialize 的 schedule 捞出来驱动。

> 关键设计：**「挂起即返回」+ 日志可重放**（见 `core-runtime-map.md` §4 不变量 #5 / #1）。
> 所以崩溃 = 进程死了，但日志还在 store 里；新进程 sweep 捞到 → 认领 → 重放 → 续跑。

---

## 1. 崩溃发生在哪、靠什么兜住

一次 drive 的完整生命周期（`driveClaimedRun`，`runtime-driver.ts:746`）：

```
resolveLeaseMs → claimRun（原子 CAS 认领）
  → startLeaseHeartbeat（lease/3 周期续租）
  → runWorkflow(recover?: ...)      ← 引擎驱动
  → syncTimerFromRunState（停在 __timer 的 deadline 登记成 timer 记录）
  → releaseRunLease（finally 语义）
```

崩溃可能发生在 `runWorkflow` 的任何一步。兜住它的不是「引擎优雅退出」，而是 **lease 会过期**：

- 引擎正常跑时，心跳每 `leaseMs/3` 续一次租约（`startLeaseHeartbeat`，`:1200`）；
- 进程崩了 → 心跳停 → `lease.expiresAt` 到点后无人续 → 这个 run 变成 **stale lease**；
- 下一轮宿主 sweep 调 `claimStaleRuns`：把 lease 过期的 run 交给新的 owner 重新驱动。

所以「崩溃恢复」本质上 = **lease 过期 + 下一轮 sweep 重新认领 + 重放日志续跑**。引擎自身不需要「感知崩溃」。

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

> sweep 是崩溃恢复的主通道：**宿主必须定期跑 sweep**（上游 `durable-recovery.md`
> 升级指南第 4 条「Keep regular sweeps enabled, including maxRecoveredRuns and
> maxTimers」）。in-memory store 数据随进程丢，但它的 sweep 语义与 D1/Postgres 一致，
> 测试时用 in-memory 验证行为。

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

## 5. 4 种「崩溃后状态」× 恢复路径（速查）

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

## 6. 宿主视角：完整崩溃恢复闭环

```ts
// 宿主（Cloudflare Cron / Railway / Netlify / Vercel scheduler 都行）
const runtime = defineWorkflowRuntime({ store, workflows, bufferSignals: true })

// 正常驱动
await runtime.startRun({ workflowId: 'order', runId: 'o-1', input: {...} })

// ... 进程崩了 ...

// 宿主定期（比如每 30s 一次）：
await runtime.sweep({ maxRecoveredRuns: 25, maxTimers: 25, maxDurationMs: 55_000 })
//   内部：claimStaleRuns 捞过期 lease 的 run → driveClaimedRun(recover:true) 重放续跑
//         + claimDueTimers / claimDueScheduleBuckets 把到期 timer / cron 也驱动了
```

**宿主责任清单**（引擎不管的部分）：

- 定期调 `sweep()`（频率要 ≤ lease 时长，保证崩溃的 run 在 lease 过期后能被下一轮捞到）；
- 用 **D1 / Postgres** store（in-memory 数据随进程丢，只用于测试）；
- 早到信号：`bufferSignals: true`（否则宿主自己存 + 重投）；
- 外部副作用幂等（`StepContext.id` 当 idempotency key）；
- run 保留策略（inbox 条目无自动过期）。

---

## 7. 不变量（违反会怎样）

| 不变量 | 违反后果 |
| --- | --- |
| 日志 append-only + CAS 全序（`appendEvent` index 必须 = 当前长度） | 重放顺序不稳定，两次重放分叉 |
| 已 checkpoint 的 step 无条件短路（闭包不执行） | 重放重跑副作用 → 钱扣两次 / 消息发两遍 |
| lease 过期才允许别的 owner 认领（CAS 闸） | 两个 worker 同时驱动一个 run → 日志冲突 |
| sweep bounded（上限 + deadline） | 一个慢 run 卡死整批 sweep → 其他 run 饿死 |
| 宿主定期 sweep | 崩溃的 run 永远不被捞回 |

---

## 8. 代码位置索引（0971b4c）

| 主题 | 文件 : 行 |
| --- | --- |
| `runWorkflow` 四驱动模式（含 `recover:true`） | `packages/workflow-core/src/engine/run-workflow.ts:34/:189/:260/:371`（start/resume/attach），`recover` 走 `:169` 分流 |
| `runWorkflow` generator + 事件队列 | `run-workflow.ts:82` |
| `driveClaimedRun` 骨架（认领→心跳→驱动→timer→释放） | `packages/workflow-runtime/src/runtime-driver.ts:746` |
| `sweep`（recovered/scheduled/timers 三段） | `runtime-driver.ts:464/:545/:610` |
| `claimStaleRuns` 契约 + 参考实现 | `runtime/src/types.ts:353` / `in-memory-store.ts:497` |
| `startLeaseHeartbeat`（lease/3 续租） | `runtime-driver.ts:1200` |
| `syncTimerFromRunState`（__timer 登记） | `runtime-driver.ts:941` |
| `bufferSignals` 升级指南 | `docs/guide/durable-recovery.md`（本基线新增） |
