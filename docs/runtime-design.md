# workflow-runtime 设计意图

> **状态**：D1 已定（supertrait，不设中间层）。D2 / D4 有倾向待确认。
> **结论：runtime 层必需**——判据是通用形态（serverless / 多 worker），而不是
> 单个应用。LocalDub 现有的 queue 是「常驻进程」形态的专用解决，与通用层不是
> 替代关系。详见 D5 及其两个附录。

> 本文只写**意图与取舍**，不写 API 签名——签名会随设计变化，写在意图之前
> 只会造成文档与实现不一致。

## 为什么要先写这份文档

上游 TanStack 在实现 runtime 之前，先写了一篇
`docs/research/SCHEDULING.md — cron landscape + future package shape`
（`30e46ba`，2026-05-21），**7 天后**才由 `5d05fa8` 实现 runtime。先写清
「要什么」再动手。

我们的处境比上游更需要在动手前定清楚，因为下面几个决策**会反向影响 core**
（core 刚理顺：59 个引擎级测试、契约清晰），不能边写边定。

## 起点：core 刻意不做的事

`packages/workflow-core` 现在的边界（与上游 core 一致）：

- replay 引擎 + 耐久原语 + `RunStore` 契约
- **不做**：调度、队列、worker 协调、timer 索引、schedule

上游 core 的原话（`docs/guide/runtime-model.md:33`）：

> The core engine is intentionally not a scheduler, queue, database adapter, or
> deployment adapter.

runtime 就是来补这块执行所有权层。上游 runtime 的职责（`runtime-model.md:60`）：

- `startRun` / `deliverSignal` / `deliverApproval` / `sweep`
- **不要求某个进程永久拥有一个 run**：每次调用 claim 一个 run、驱到下一个
  pause 或终态、释放 lease、返回
- 依赖 `WorkflowExecutionStore`——它比 core 的 `RunStore` **更宽**，含
  timers / schedules / 原子 claim / lease / 陈旧 run 恢复 / list 与 timeline

---

## 决策点

### D1. `WorkflowExecutionStore` 是新 trait，还是扩 `RunStore`？—— **已定：supertrait，不设中间层**

#### 上游的实际结构

上游有**两个**名字指同一套东西，这点容易看漏：

```ts
// workflow-core/src/types.ts:599
export interface RunStore { getRunState; setRunState; deleteRun; appendEvent; getEvents; subscribe? }

// workflow-runtime/src/types.ts:290
export interface WorkflowRunStoreAdapterStore { loadRunState; saveRunState; deleteRun; appendEvents; readEvents; subscribeEvents? }

// workflow-runtime/src/types.ts:305
export type WorkflowRunStoreAdapter = RunStore          // ← 一行别名，与上面那个 interface 无继承关系

// workflow-runtime/src/types.ts:307
export interface WorkflowExecutionStore extends WorkflowRunStoreAdapterStore { /* +19 个方法 */ }
```

两个 interface **语义等价、仅命名不同**：

| core `RunStore` | runtime `WorkflowRunStoreAdapterStore` | 差异 |
| --- | --- | --- |
| `getRunState(runId)` | `loadRunState(runId)` | 仅名字 |
| `setRunState(runId, state)` | `saveRunState(args)` | 名字 + 参数打包 |
| `deleteRun(runId, reason)` | `deleteRun(runId, reason)` | ✅ 相同 |
| `appendEvent(runId, idx, event)` | `appendEvents(args)` | 名字 + 参数打包 |
| `getEvents(runId)` | `readEvents(args)` | 名字 + 参数打包 |
| `subscribe?(...)` | `subscribeEvents?` | 名字 |

（连我们已实现的 `DeleteReason` 参数都跟它一致——不是拍脑袋加的。）

#### 我们的选择

```rust
// workflow-runtime
pub trait WorkflowExecutionStore: RunStore { /* +19 个方法 */ }
```

**不设 `WorkflowRunStoreAdapterStore` 的对应物。** 理由：

1. **那层重复是 TS 的产物**。`export type X = Y` 零成本，所以上游能一行抹平。
   Rust 的 trait 别名要 `#![feature(trait_alias)]`（至今 unstable）；
   用 `trait A: B {}` 则是新 trait——**每个实现者都得多写一行空 `impl`**，
   为纯名字差异付实现成本，不划算。
2. **`RunStore` 已经是我们的 core 契约面**，被测试与文档锚定
   （`DeleteReason` / `into_typed` / `RunState.error` 都挂它上面）。改名成
   runtime 的词汇会让 core 朝上层倾斜。
3. **supertrait 在 Rust 里是直接的**：`RunStore` 就是那 6 项基础层，
   `WorkflowExecutionStore` 直接 `: RunStore` 扩展，不需要中间层。

#### 命名不对称，以及为何保留

`RunStore`（无前缀）vs `WorkflowExecutionStore`（有前缀）在 Rust 里略不对称。
**保留这个不对称**——它准确反映层级：`RunStore` 是通用 run store 契约，
`WorkflowExecutionStore` 是 workflow 专属扩展。为了「看起来对称」去改
`RunStore` 的名字，收益只是观感。

**这个对应关系已写进 `RunStore` 的文档注释**（`run_store/mod.rs`），说明上游
那两个名字、我们为何只有一个。

#### 实现清单（上游 `ExecutionStore` 的 19 个方法，五组）

| 组 | 方法 |
| --- | --- |
| run 生命周期 | `createRun` / `loadRun` / `loadExecution` / `markRunPaused` / `markRunFinished` / `markRunErrored` |
| lease | `claimRun` / `heartbeatRunLease` / `releaseRunLease` / `claimStaleRuns` |
| timer | `scheduleTimer` / `claimDueTimers` |
| 投递 | `deliverSignal` / `deliverApproval` |
| schedule | `upsertSchedule` / `claimDueScheduleBuckets` / `markScheduleBucketStarted` |
| 查询 | `listRuns` / `getRunTimeline` |

注意两点：

- **`heartbeatRunLease` 的存在说明 lease 要续租**，不是一次 claim 就完事
  （上游 `runtime-model.md:156`：runtime renews every third of `leaseMs`）。
- **`markRunPaused` / `markRunFinished` / `markRunErrored` 是独立方法**，
  不是「改 `RunState` 再 `save`」——上游把 run 状态转移也建模成了 store 的
  原子操作。这意味着 `ExecutionStore` 比「`RunStore` 加几个查询」要重。

### D2. lease 放 store 还是放 runtime？

上游把 lease 交给 store（`leaseOwner` / `leaseMs`，runtime 负责续租）。

| 选项 | 代价 | 倾向 |
| --- | --- | --- |
| **A. lease 在 store（原子 claim 由 store 保证）** | 每个 store 实现要正确处理并发 claim；`FileRunStore` 这种单机实现要伪造成「总能 claim 成功」 | ✅ |
| B. lease 在 runtime（进程内锁） | 多进程无效——runtime 的整个存在意义就是跨进程，进程内的锁解决不了 | ❌ |

**倾向 A**。**但注意**：这一条**不影响 core**——只要 lease 不塞进 `RunStore`
（见 D1），core 完全无感。

**为什么必需**（而不是「待定」）：serverless 形态下每次调用都是新进程，
两个 worker 可能同时 drive 同一个 run——`cloudflare-d1/src/worker.ts` 里
`startRun({ leaseOwner: 'http:start' })` / `deliverSignal({ leaseOwner:
'http:payment' })` 就是为此。LocalDub 用进程内 `INFLIGHT` 解决同一个问题，但
那只在「单进程常驻」下有效。

**单机 store 的退化**：`InMemoryStore` / `FileRunStore` 这类实现可以让 `claim`
永远成功（无并发场景），但**接口必须存在**——否则 serverless 形态没法接入。

### D3. timer 投递：谁来唤醒 sleep？

现状：`exec_pause` 在挂起点自轮询（`engine/mod.rs` 的 `RESUME_POLL_MS = 25ms`），
到期后引擎自己 `signal_run` 投递。

| 选项 | 代价 | 倾向 |
| --- | --- | --- |
| **A. runtime 的 sweep 认领到期 timer 并投递** | core 要提供「查询哪些 run 在等、deadline 是什么」的入口；或 runtime 自己扫 `RunState.waiting_for` | ✅ 与上游一致 |
| B. 保持 core 自轮询 | 每个挂起的 run 占一个存活的任务 + 25ms 轮询；进程一退，timer 就没人投递（除非有人重放） | ⚠️ 见下 |
| C. core 提供 `subscribe` 等待 | 已有 `RunStore::subscribe`，但 `FileRunStore` 之外未必有 | — |

现在其实是 **B**。它能工作是因为「drive 期间进程活着」；一旦把 run 交给运行时
边界（进程退出），**没有东西会来投递 timer**——这正是 runtime 存在的理由。

**但这个切换有代价**（已实测，见下）：从 B 到 A 意味着 `exec_pause` 不再自己等，
而是「写 `STEP_PAUSED` → 返回」。这是**行为变更**。

### D3 附：影响面实测

依赖「同一次 drive 内自动 resume」的测试共 **7 个**：

| 位置 | 测试 |
| --- | --- |
| `engine/mod.rs:1555` | `sleep_pauses_then_auto_resumes` —— 断言 `task.await` 在整个 sleep 期间不返回，**且引擎自己 append 了 `StepResume`**（`:1612-1620`） |
| `engine/mod.rs:1808` | `sleep_until_past_resolves_immediately` |
| `engine/mod.rs:1841` | `sleep_until_schedules_timer` |
| `examples/.../workflows.rs:1068` | `invoice_double_sleep_auto_resumes` |
| `examples/.../workflows.rs:1184` | `refund_approved_disburses_after_timer` |
| `examples/.../workflows.rs:1614` | `event_gate_emit_wait_then_sleep_until` |
| `examples/.../runtime.rs:265` | `invoice_double_sleep_survives_restart`（跨进程重启，本身就不依赖单次 drive） |

**结论**：切换到 A 是**行为变更但非破坏性**——没有测试断言「引擎绝不会自己
投递 timer」，只是它们**顺带**依赖了这个行为。改写方式是把
`task.await` 换成「等 pause → 手动投递 timer → 再 drive」，与
`approval` 类测试的写法一致（那类测试已经是这个模式）。

**但这会损失一个能力**：现在「一次 `run_workflow` 调用就能跑完含 sleep 的
workflow」是成立的，切到 A 之后就需要外部驱动器。如果 LocalDub 的调用方
（`packages/cli/run-task.ts`）依赖这个便利，得先确认。

**可能的折中**：core 保留自投递（B），runtime 的 sweep 作为**可选的**
外部驱动器（A）——两者不冲突，`exec_pause` 的行为不变，runtime 只是多了一条
「不依赖进程存活」的路径。这样零测试改动。**这一条值得优先考虑。**

**但 fold 的适用边界要想清楚**：B 只在「进程活着」时有效。serverless 下进程
随时退出（`cloudflare-d1` 的 `scheduled` handler 每次都是新 isolate），所以
**B 必须能退化成 A**——即「挂起点由外部 sweep 投递」这条路径必须走得通，不能
只有自轮询。折中的正确形态是：**两条路径都支持，由调用方形态决定用哪条**。

### D4. sweep 的边界怎么定？

上游：`sweep({ maxRecoveredRuns, maxScheduledRuns, maxTimers, maxDurationMs })`，
边界是为了「塞进一次 host 执行」（serverless 函数超时）。

本地常驻进程没有 host 超时概念——但 serverless 形态有
（`cloudflare-d1` 用 `maxDurationMs: 25_000` / `maxTimers: 25`）。

| 选项 | 代价 | 倾向 |
| --- | --- | --- |
| **A. 保留有界 sweep（`max_*` + `maxDurationMs`），常驻形态传大值** | 多几个参数，但接口与上游同构，两种形态都能覆盖 | ✅ |
| B. 无界 sweep（跑到没有活干为止） | 简单，但 serverless 形态直接不可用 | ❌ |

**倾向 A**。边界是**接口形状**的一部分——只有它能同时覆盖常驻与 serverless
两种形态；省掉它就把 serverless 排除了。

### D5. 我们真的需要哪些能力？

上游 runtime 有 8 项能力（`runtime-model.md:75`）：幂等创建 / 事件日志 /
run state / timers / signal 与 approval 投递 / schedules / 原子 claim 与 lease /
陈旧 run 恢复 / list 与 timeline。

**判据是通用形态，不是单个应用。** 这个库不是只为 LocalDub 服务的——目标包括
把上游的 `examples/deployment-pocs/cloudflare-d1` 作为 Rust 侧对标例子实现。
所以判据是「Rust 的 serverless / 多 worker 形态需不需要」，而不是「LocalDub
需不需要」。

#### 形态 A：常驻进程（LocalDub 现在这样）

LocalDub 已有等价实现，见下节。这个形态下 lease / sweep 边界 / timer 投递
都不是刚需。

#### 形态 B：serverless / 无进程常驻（cloudflare-d1 这样）

这个形态下**每一项都是刚需**。以 `cloudflare-d1/src/worker.ts` 为证：

| 上游 runtime 能力 | serverless 形态下为何必需 | 例证 |
| --- | --- | --- |
| **有界 sweep** | 一次 host 执行有超时，必须切分 | `maxDurationMs: 25_000` / `maxTimers: 25` |
| **lease** | 每次调用都是新进程，必须防止两个 worker 同时跑同一个 run | `leaseOwner: 'http:start'` / `'http:payment'` |
| **timer 投递** | **没有进程常驻，timer 必须由外部认领** | `readyAt` + `ctx.sleepUntil`，靠 `scheduled` handler 唤醒 |
| **list / timeline API** | 无进程内状态可查，全部走 store | `runtime.store.listRuns` / `getRunTimeline` |
| **host adapter** | 把平台入口（`scheduled()` / cron）接到 `sweep()` | `createCloudflareWorkflowScheduledHandler` |
| **幂等投递** | webhook 会重试 | `signalId` |

**关键**：serverless 下**没有常驻进程**，所以 `ctx.sleep` 那种「挂起后等引擎
自己投递」的 B 方案根本不可行——这正是 D3 里那个问题的实例。

**结论：要做 `cloudflare-d1` 这类例子，runtime 层（lease / sweep / timer 投递 /
list）全部必需，D2 与 D4 不能作废。**

### D5 附一：LocalDub 已自有 queue，但那是形态 A 的解法

`packages/server/src/feat/workflows/queue/`（642 行）在**形态 A** 下是完整的
等价实现：

| 上游 runtime 能力 | LocalDub 已有 | 实现 |
| --- | --- | --- |
| sweep（有界后台单元） | ✅ 等价 | `run_worker()` 常驻循环（`queue/mod.rs:328`） |
| append-only event log | ✅ | `events.ndjson`，crash 靠重放 `[consumed_offset, EOF)` |
| run state | ✅ | `checkpoint.json`，原子重写，正常路径零重放 |
| lease | ✅（**进程内**） | `INFLIGHT: Mutex<HashSet<String>>`（`workflows/mod.rs:29`） |
| execution store | ✅ | `data/queue/` |
| run status | ✅ | Queued / Running / Done / Failed |
| `startRun` | ✅ | `enqueue` + `wait_next()` |
| schedules / cron | ❌ 不需要 | 任务由 CLI/桌面显式 `enqueue` |
| timer 投递 | ❌ 不需要 | `execute_entry` 在 `spawn_blocking` 里跑同步 pipeline，整条一次跑完 |

**但这是形态 A 的专用实现，不是通用解**：

- `INFLIGHT` 是**进程内** `HashSet`——它明确假设 worker 单进程常驻，在
  serverless 下完全失效（两个 isolate 各有一份内存）。
- `run_worker` 是**常驻循环**而非有界单元，`maxDurationMs` 对它无意义。
- `execute_entry` 跑**同步** pipeline——`ctx.sleep` 的「挂起-返回-以后再来」
  模式对它不适用。

**所以两者不是替代关系，是两种形态各要一套。** 我们要做的是提供一个能同时
覆盖两种形态的通用层，而 LocalDub 现有的 queue 是形态 A 的既有实现（是否迁移
到通用层是另一个决定，不是前提）。

### D5 附二：LocalDub 真正缺的是并行编排（与 runtime 无关）

这一条独立于 runtime 的判断。`get_steps`（`steps/utils/steps.rs:134`）返回
**扁平串行列表**，`DUB_SF_OCR_STEPS`：

```
separate → separate_after → sf_ocr_pre → sf_ocr → sf_ocr_fix
  → translate → split_audio → tts → mix_audio → mix_video
```

其中 `separate` / `separate_after`（语音分离）与 `sf_ocr_pre` / `sf_ocr`
（关键帧 OCR）**没有数据依赖**，可以并行。`asr_ocr` 序列里 `separate*` 与
`asr` / `asr_ocr*` 同理。

这是 core 的 `tokio::try_join!` 能解决的问题（`fulfillment-saga` 示例就是），
**与 runtime 层无关，可以独立推进**。

### D6. core 的 25ms 轮询要不要顺带修？

现状：`exec_pause` 每 25ms `store.get_events()` 全量重读 + `find_resume` 线性扫，
是 O(n²)。这是 README「已知边界」里记的唯一性能硬伤。

| 选项 | 代价 | 倾向 |
| --- | --- | --- |
| **A. 先不动，等 runtime 定了再说** | 硬伤留着 | ✅ 见下 |
| B. 现在换成 `subscribe` | `subscribe` 返回阻塞 `mpsc::Receiver`，async 上下文里要 `spawn_blocking`；且 store 可以不给（`None`） | ⚠️ |

**倾向 A 暂时不动**，但要在 D3 的讨论里一并考虑——因为「谁来唤醒」这个问题的
答案，会决定轮询要留还是要改。

---

## 明确不做的事

- **不做托管控制面**：上游 `comparison.md:49` 把「Managed control plane
  included」列为 Intentional non-goal。
- **不下沉进 core**：D1 的 A 选项就是这个意思。
- **不做上游那套 serverless host adapter（至少不是一开始）**：Netlify / Vercel /
  Cloudflare 的 `scheduled()` 薄壳是为 JS serverless 平台写的，Rust 侧没有对等
  入口。但**「无进程常驻」这个形态本身是必须支持的**（这是 runtime 存在的理由），
  所以：
  - runtime 层要能表达「有界 sweep + lease + timer 投递」，这样才可能被任意
    外部驱动器调用；
  - 对标 `cloudflare-d1` 时，Rust 侧的对应物是**一个最小 HTTP server 或 CLI
    子命令**去调 `sweep()`，而不是平台专属 adapter。
  - `cloudflare-d1` 值得我们抄的是**它的架构与 store 契约用法**（`listRuns` /
    `getRunTimeline` / `leaseOwner` / `maxDurationMs`），不是它的 Wrangler 配置。

---

## 下一步（按顺序）

1. ~~查 D3 的影响面~~ —— **已实测**，见 D3 附。
2. ~~查 D5~~ —— **已调查**，见 D5：runtime 层必需（判据是通用形态而非单个应用）。
3. **拍板 D2 / D4**（D1 已定）：
   - D2（lease 位置）—— 倾向 store，前提是不塞进 `RunStore`
   - D4（sweep 边界）—— 倾向保留 `max_*` / `maxDurationMs`
4. **并行编排（D5 附二）可以独立推进**：它只依赖 core 的 `try_join!`，与
   runtime 层的决策无关。如果想让 LocalDub 先有收益，这条可以并行开工。

**次序**：D1 已定，`WorkflowExecutionStore: RunStore` 的骨架可以立起来了。
D2 / D4 是在此之上的细节，可在实现 lease / sweep 时再敲定。
