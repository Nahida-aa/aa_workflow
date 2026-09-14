# workflow-runtime 设计意图

> **状态**：D1 已定（supertrait，不设中间层）。**D3 已定且已落地**（core 不自轮询，
> 挂起即返回）。D2 / D4 有倾向待确认。
>
> **结论：runtime 层必需**——判据是**「有没有常驻的驱动者」**（不是「有没有
> 持久存储」，两者正交；见 D5）。LocalDub 有常驻 worker，属形态 A，**只用
> `workflow-core` 是正确取舍**；但要支持形态 B（单次执行有时长上限的托管
> 进程 / serverless），lease / sweep / timer 投递全部必需。详见 D5 及其两个
> 附录。
>
> 实现进度：runtime 层已全部移植（存储契约 / in-memory 实现 / driver /
> materializer / 规格构造器）。

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

### D1. `WorkflowExecutionStore` 的父契约是什么？—— **已定：`ExecutionStore: WorkflowRunStoreAdapterStore`，不写适配器**

#### 上游的实际结构（三个名字，容易看错）

```ts
// workflow-core/src/types.ts:599
export interface RunStore {                       // ① core 的契约
  getRunState; setRunState; deleteRun
  appendEvent(runId, idx, event)                  //    单条
  getEvents(runId) -> WorkflowEvent[]             //    裸数组
  subscribe?
}

// workflow-runtime/src/types.ts:290
export interface WorkflowRunStoreAdapterStore {   // ② runtime 的存储基础
  loadRunState; saveRunState; deleteRun
  appendEvents({ runId, expectedNextIndex, events }) -> { nextIndex }   // 批量
  readEvents({ runId }) -> StoredWorkflowEvent[]                        // 带索引信封
  subscribeEvents?
}

// workflow-runtime/src/types.ts:305
export type WorkflowRunStoreAdapter = RunStore    // ③ 别名，与 ② 无继承关系

// workflow-runtime/src/types.ts:307
export interface WorkflowExecutionStore extends WorkflowRunStoreAdapterStore { /* +19 */ }
```

**①②不是同一个东西。** 差别在事件读写这一对：

| ① core `RunStore` | ② `WorkflowRunStoreAdapterStore` | 实际差别 |
| --- | --- | --- |
| `getRunState` | `loadRunState` | 仅命名 |
| `setRunState` | `saveRunState(args)` | 命名 + 参数打包 |
| `deleteRun(runId, reason)` | `deleteRun(runId, reason)` | ✅ 完全相同 |
| `appendEvent`（单条） | `appendEvents`（**批量**，返回 `nextIndex`） | **语义差别** |
| `getEvents` → `WorkflowEvent[]` | `readEvents` → `StoredWorkflowEvent[]`（**带 `eventIndex` / `createdAt` / `eventType` / `stepId`**） | **语义差别** |
| `subscribe?` | `subscribeEvents?` | 同签名 |

`StoredWorkflowEvent`（`types.ts:50`）：

```ts
{ runId, eventIndex, eventType, stepId?, event, createdAt }
```

#### 那为什么会有两个？—— 迁移的桥

关键证据是 `run-store-adapter.ts` 的 `createRunStoreAdapter`：

```ts
// runtime-driver.ts:723
const runStore = createRunStoreAdapter(config.store, telemetry)
```

runtime 持有一个 `WorkflowExecutionStore`（新世界），却要把它**降格**成
`RunStore`（旧世界）才能喂给 core 的 `runWorkflow`。适配器做的就是这件事：
`loadRunState` ← `getRunState`、`appendEvents({events:[e]})` ← `appendEvent`、
`readEvents(...).map(e => e.event)` ← `getEvents`（**丢弃索引信封**）。
③ 那行别名的含义正是「适配器的**输出**必须是 `RunStore`」。

**所以 ① 与 ② 是迁移前后两代形状在共存**，适配器是过渡层。

#### 那两处「语义差别」谁真正需要？—— 没人

这是决定我们要不要 ② 的关键。查 driver 的实际调用（`runtime-driver.ts`）：

```
releaseRunLease ×2  loadRunState ×2  deliverSignal ×2  createRun ×2
scheduleTimer  markScheduleBucketStarted  loadRun  heartbeatRunLease
deliverApproval  claimStaleRuns  claimRun  claimDueTimers  claimDueScheduleBuckets
```

**driver 完全不碰 `appendEvents` / `readEvents`**：

- `eventIndex` 只有 store 内部实现与 `subscribe` 回调用它
- 批量 append 的**唯一调用点**是适配器（塞单元素数组）
- `createdAt` 只用于 store 内部排序 / 游标

也就是说：那两处语义差别是 `ExecutionStore` 作为**独立存储契约**自己长出来的
（游标分页、订阅索引），**不是 driver 逼出来的**。

#### 我们的选择（**已修正**：② 要立）

```rust
// workflow-runtime/src/run_store_adapter.rs
pub trait WorkflowRunStoreAdapterStore: Send + Sync { /* 6 个方法，照 ② 逐字对齐 */ }

// workflow-runtime/src/lib.rs
pub trait WorkflowExecutionStore: WorkflowRunStoreAdapterStore { /* 扩展方法，待定 */ }
```

**立 ②，不立 ③（那个真别名），也不写那个转换函数。** 三条理由：

1. **② 是 `ExecutionStore` 的依赖**。`WorkflowExecutionStore extends
   WorkflowRunStoreAdapterStore` 意味着扩展层的所有事件相关方法（
   `get_run_timeline` 返回 `RunTimeline { run, events: StoredWorkflowEvent[] }`）
   都建立在 ② 的形状上。**不先立 ②，`ExecutionStore` 的签名就没有落脚点**——
   这是最初把 ② 当成「重复」时完全忽略的一点。
2. **事件索引与游标是跨进程 / DB store 的基础**。`InMemoryStore` 里
   `event_index` 等于数组下标，看不出价值；但 Postgres / D1 store 要靠它分页
   与按位查询。先立形状，实现可以晚。
3. **③ 与那个转换函数确实不需要**。③ 是 TS 的 `type` 别名，Rust 里无廉价对应
   （`trait_alias` 仍 unstable），且它的唯一用途是标注「适配器输出类型」；
   适配器的职责是「② → ①」降格——我们不做迁移，没有两代形状，所以不需要这个
   转换函数。

（注：我曾用「driver 不用那两处新语义」论证 ② 不需要——**那个论据是错的**，
它只回答了「driver 需不需要」，而问题是「`ExecutionStore` 需不需要」。见上面
理由 1。）

**但「不需要转换函数」不等于「适配是免费的」——这一点初版写错了。**

实测（编译期验证）：把 `&dyn WorkflowExecutionStore` 传给要 `&dyn RunStore`
的函数：

```
error[E0308]: mismatched types
   expected trait `RunStore`, found trait `WorkflowExecutionStore`
```

原因：`WorkflowExecutionStore: WorkflowRunStoreAdapterStore` 这条继承链**与
`RunStore` 毫无关系**。supertrait 只保证「`WorkflowRunStoreAdapterStore` 是
父 trait」，不保证任何 `RunStore` 关系。

**实际后果**：想让同一个类型既能被 runtime 驱动、又能喂给 core 的
`run_workflow`（入参是 `Arc<dyn RunStore>`），必须**两套都实现**：

- `RunStore` 的 6 个方法（`get_run_state` / `append_event` / …）
- `WorkflowRunStoreAdapterStore` 的 6 个方法（`load_run_state` / `append_events` / …）
- 加上扩展层

两套是**平行的**，没有自动转换。

**修正：我们提供转换函数（与上游一致）。** 让每个实现者手写两遍 6 个方法纯属
重复，且有漂移风险（改了一套忘另一套 → runtime 与 core 看到的状态不一致）。
所以本 crate 提供：

```rust
pub fn run_store_adapter(store: Arc<dyn WorkflowExecutionStore>) -> Arc<dyn RunStore>;
```

- **实现者只写一套**（`WorkflowRunStoreAdapterStore` 的 6 个 + 扩展层）
- core 的 `run_workflow` 要 `Arc<dyn RunStore>`，适配器转给它
- **无损**：适配器丢弃的只有 `from_index` 游标与索引信封，而 `RunStore` 本就
  没有这两样（读的是裸事件数组）——是投影到更小的面，不是信息损失

两个实现细节值得记：

- **CAS 冲突必须原样透出**：适配器把 `anyhow::Error` downcast 回
  `StoreError::Conflict`，否则 core 的 rebase-retry 循环会把它当普通 IO 错误
  处理（已由 `cas_conflict_survives_adaptation` 测试钉住）。
- **两个方法如实报不支持**：`truncate_runs`（`continue_from` 用）与 `subscribe`
  在基础层没有对应物——上游的 `createRunStoreAdapter` 同样没实现 `truncateRuns`。
  适配器选择**报错**而非静默 no-op。

代价：每次调用多一次 `dyn` 间接 + 参数转换。本地低频率调用可忽略。

**实证**：`e2e_tests::core_runs_against_adapted_store` 用一个只实现了
`WorkflowExecutionStore` 的 store 真正跑通了一次 core 的 `run_workflow`。

#### 附：为什么适配必须是具体类型（`RunStoreAdapter`）

自然的想法是「给 `dyn WorkflowExecutionStore` 直接 `impl RunStore`，省掉那个
struct」。**实测这条路走不通**（三个探针，均已清理）：

```rust
impl RunStore for dyn WorkflowExecutionStore { /* 6 个方法 */ }          // ✅ 编译通过
fn coerce(x: Arc<dyn WorkflowExecutionStore>) -> Arc<dyn RunStore> { x } // ❌ E0308
fn coerce_ref(x: &dyn WorkflowExecutionStore) -> &dyn RunStore { x }     // ❌ E0308
RunStore::get_events(x, "r1")                                            // ✅ 编译通过
```

三点结论：

1. `impl LocalTrait for dyn LocalTrait` **合法**（孤儿规则允许）——容易误判为
   不合法；
2. 但它**不产生 coercion**：trait 对象的类型转换只认 **supertrait 关系**
   （`trait B: A` 时 `dyn B → dyn A`），手动 `impl` 不参与；
3. 它唯一的作用是让方法能被 UFCS 显式调用，而 `run_workflow` 要的是
   `Arc<dyn RunStore>` 这个**类型**——「能调方法」≠「能当参数传」。

所以转换只能发生在 `Arc::new` 那一刻（把实现了 `RunStore` 的具体类型装进
`dyn`），这正是 trait 对象转换的合法路径。

#### 附：这一层不是 Rust 特有的开销

曾误以为「Rust 因为显式写了适配器，开销比 TS 大」。**不成立**——上游同样手写
转发（`createRunStoreAdapter` 里逐方法包装），两边结构完全对应：

| 环节 | TS | Rust |
| ---- | -- | ---- |
| 转换函数作者 | 库 | 库 |
| 实现者负担 | 一套方法 | 一套方法 |
| 转换时刻 | 一次对象分配 | 一次 `Arc` 分配 |
| 每调用跳转 | 2 层（core → adapter → store） | 2 层（同） |
| 参数包装 | `{ events: [event] }` | `vec![event.clone()]` |

**打平。** 真要论差异，TS 的结构化类型意味着「方法名写对就通过」，Rust 的
`impl` 强制实现全部 6 个——那是编译期保证的收益，不是开销。

（详细版本写在 `run_store_adapter.rs` 的模块文档里，与实现同处。）

#### 命名：逐字沿用上游，尽管拗口

`RunStore`（core，无前缀）vs `WorkflowRunStoreAdapterStore` /
`WorkflowExecutionStore`（runtime，有前缀）。

`WorkflowRunStoreAdapterStore` 这个名字读起来是「Store 的 Adapter 的 Store」，
而且 `Adapter` 那半截**在我们这里没有对应物**（我们没有适配器）。曾经考虑
简化为 `RunStoreAdapterStore`，被否——本项目一贯的命名策略是对齐上游
（`RunStore` / `RunState` / `WorkflowEvent` / `WorkflowExecutionStore` 全是
照搬）。**名字难读是一次性成本，对不上上游是持续成本**——每次读上游代码都要
在脑子里做映射。理由写进了 trait 自己的文档注释。

对应关系写进了 `RunStore` 的文档注释（`run_store/mod.rs`）与
`WorkflowRunStoreAdapterStore` 的模块文档（`run_store_adapter.rs`）。

#### 风险：将来做 DB store 时要改 trait

索引与分页在「append-only 数组」下是免费的，但在 **Postgres / D1 这类后端**上
会变成刚需（游标分页、按 index 查询）。

**接受这个风险**，理由：现在还没有第二个 store 实现，为假想的第三个实现去设计
索引形状是过度设计。等真做 DB store 时再加——那时改 trait **有具体依据**，
是合理改动。

#### 实现清单（上游 `ExecutionStore` 的扩展方法，五组）

| 组 | 方法 |
| --- | --- |
| run 生命周期 | `createRun` / `loadRun` / `loadExecution` / `markRunPaused` / `markRunFinished` / `markRunErrored` |
| lease | `claimRun` / `heartbeatRunLease` / `releaseRunLease` / `claimStaleRuns` |
| timer | `scheduleTimer` / `claimDueTimers` |
| 投递 | `deliverSignal` / `deliverApproval` |
| schedule | `upsertSchedule` / `claimDueScheduleBuckets` / `markScheduleBucketStarted` |
| 查询 | `listRuns` / `getRunTimeline` |

注意两点：

- **`heartbeatRunLease` 说明 lease 要续租**，不是一次 claim 就完事
  （上游 `runtime-model.md:156`：runtime renews every third of `leaseMs`）。
- **`markRunPaused` / `markRunFinished` / `markRunErrored` 是独立方法**，不是
  「改 `RunState` 再 save」——上游把 run 状态转移也建模成了 store 的原子操作。

#### 附：本次判断失误的记录

初版 D1 把 `WorkflowRunStoreAdapter`（③，真别名）与
`WorkflowRunStoreAdapterStore`（②，独立 interface）**当成同一个东西**——两个
名字只差一个 `Store` 后缀。据此推出「② 是 `RunStore` 的重复」，还做了一张
「六项逐项对应」的表。

那个表**有证据的外观但配对是错的**，所以「逐项对应」的结论也是假的。事实是
② 在事件读写上有真实的语义差别（见上表）。

教训：**名字前缀相同、只差后缀时，必须逐字核对，不能按「看起来一样」处理**。
比「没查就下结论」更危险的是「查了但配对错了」——它带着证据的外观。

**第二次修正在此之上**：我随后用「driver 不用那两处新语义」论证「② 不需要」，
**又一次错**——忽略了 `ExecutionStore extends ②` 这层依赖。driver 不用 ≠ 
`ExecutionStore` 不需要：扩展层的查询方法返回的就是 `StoredWorkflowEvent`。

**所以 D1 的最终结论是「立 ②，不立 ③」**，见上文「我们的选择（已修正）」。

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

### D3. timer 投递：谁来唤醒 sleep？—— **已定：core 不自轮询，挂起即返回（对齐上游）**

#### 实测：上游的行为（2026-09-14，在 `learn_ls/workflow` 实跑）

探针（`packages/workflow-core/tests/probe.suspend.test.ts`，已清理）：

| 挂起点 | 耗时 | 事件流 | 返回后 `RunState` |
| --- | --- | --- | --- |
| `ctx.approve` | **1 ms** | `RUN_STARTED` → `APPROVAL_REQUESTED` | `status: "paused"` + `pendingApproval` |
| `ctx.sleep(60s)` | **0 ms** | `RUN_STARTED` → `SIGNAL_AWAITED`（含 `deadline`） | `status: "paused"` |

**上游是「写到挂起点就返回」**。看源码就能看到机制（`run-workflow.ts`）：

```ts
// engineWaitForEvent（sleep / wait_for_event 走这条）：
await emitAndAppend(... { type: 'SIGNAL_AWAITED', stepId, name, deadline })  // 写日志
if (persisted) await runStore.setRunState(runId, { ...persisted, status: 'paused', waitingFor })
engine.paused = true
throw new WorkflowPaused()          // ← 用异常把控制权弹回 drive
```

`drive` 的 catch 里专门认这个标志（`run-workflow.ts:529`）：

```ts
if (engine.paused) {
  // The primitive that paused already wrote the pause state — status,
  // waitingFor / pendingApproval — directly to the store.
  return                            // ← 不写终态事件、不写终态 status
}
```

所以「挂起」不是一个 await 点，而是**一条以 `paused` 收尾的退出路径**——
drive 正常结束、进程可以走了。

`sleep` 与 `approve` **是同一机制**——都等一个信号（`sleep` 等 `__timer`，
`approve` 等审批），都由**外部**投递。`SIGNAL_AWAITED` 事件里带 `deadline`，
供 timer 索引使用。上游自己唯一产生 `__timer` 信号的地方是
`yieldForDeadlineIfNeeded`（预算耗尽的主动让出），而且它也是**写事件 + 弹异常**，
不是自己等自己——「到期投递」这件事上游整体交给外部。

#### 我们现在的行为（偏离）

`exec_pause` 是**阻塞轮询**（`RESUME_POLL_MS = 25ms` 自旋）：

| | 上游 | 我们（改前） |
| --- | --- | --- |
| `approve` 挂起 | 1ms 返回 `paused` | **阻塞等外部 `signal_run`** |
| `sleep(60s)` | 0ms 返回 `paused` + `deadline` | **自轮询 60 秒**，同一次 drive 内恢复 |
| 挂起期间需要进程活着吗 | **不需要** | **需要** |

**这不只是「实现细节不同」，是核心语义上的能力差异**：我们的挂起依赖进程存活，
上游的挂起不需要。上游因此可以把 `sleep` 挂几天的 run 交给存储，自己退出。

#### 结论（**已落地**）

改 `exec_pause`：**写挂起 checkpoint（`StepPaused`）后立即返回**，不再自轮询。

对齐上游的 `throw new WorkflowPaused()` 形态——新增内部哨兵
`WorkflowParked`（与 `StepHalt` 并列），由 drive 的 `match handler_result` 收尾：

```rust
Err(e) if e.downcast_ref::<WorkflowParked>().is_some() => {
    return Ok(RunOutcome { run_id, status: RunStatus::Paused, output: None, error: None });
}
```

**挂起不是终态**：`RunStatus::Paused` 下**不 append** `RunFinished` / `RunErrored`
（照上游 `if (engine.paused) return`）；`RunOutcome.error` 保持 `None`。
`RunOutcome`/`RunState` 的 `status` 就是 `Paused`，外部据此决定是投递信号还是
留给 sweep。这也让 `RunStatus::Paused` 从「投影状态」变成**真实的 drive 终态**。

这同时解释了为什么 runtime 的 `sweep` 是**必需**而非可选——core 不再自投递，
就必须有外部驱动器。

落地时**顺带发现并修掉的四处**（不在最初的评估里，都是删掉轮询后暴露的）：

| 项 | 问题 | 修法 |
| --- | --- | --- |
| **deadline 语义** | `exec_pause` 收的是 `dur: Duration`，`sleep_until(过去)` 被 clamp 成 `sleep(0)`，`due_at` 变成 `now`——**原始时间戳丢了**，sweep 无法索引 | `exec_pause` 改收 `deadline: Option<i64>`（**绝对**时间戳，原样存）。`sleep(ms)` 按上游改写为 `sleep_until(now+ms)`（`run-workflow.ts:992`），`yield_` 同理传 `yield_resume_at` |
| **wait 投影的清除点** | 原 `clear_run_wait` 在「恢复的那一刻」清；挂起即返回后没有「那一刻」了 | 移到**下次 drive 的前导**（`run_workflow`），对齐上游 `run-workflow.ts:336-342`。顺带清 `pending_approval` |
| **cancel 不再被观察** | 原检查在轮询循环里 | 移到 `exec_pause` 入口（挂起是最后一个能观察 flag 的边界）；并把 `Aborted` 做成**锁存终态**——drive 前导直接返回 `Aborted`，不再把 run 复活成 `Running`（对齐上游 `resumeRun` 对 aborted 的处理，`run-workflow.ts:275-282`） |
| **runtime driver 的 deliver** | 原来 deliver 拿到 `NotClaimable`（挂起的 drive 还攥着 lease）；现在挂起即释放，deliver 自己 claim + 驱动到 `Completed` | 测试断言随之改成 `Completed` |

连带影响（均已落地）：

| 影响 | 内容 |
| --- | --- |
| core | `exec_pause` 去掉轮询循环与 `RESUME_POLL_MS`；新增 `WorkflowParked` 哨兵；`clear_run_wait` 删除 |
| `run_workflow` | 新增 `Paused` 收尾分支（不 append 终态）；drive 前导清 wait 投影 + 锁存 `Aborted` |
| `RunStatus::Paused` | 从「投影状态」变成**真实的 drive 终态** |
| **上一版的「折中方案」作废** | 「core 保留自投递 + runtime 作为可选驱动器」不成立——那正是偏离本身 |
| 测试 | 实际改了 **21 个**（不是评估时估的 7 个——approve 类也依赖了阻塞行为）。写法统一为「drive → 断言 `Paused` → 投递 → 再 drive」 |
| `yield_` | 走 `exec_pause`，所以 `should_yield` 路径也变成挂起即返回——这正是上游的语义（让出预算后由 host 重新唤起） |
| **LocalDub** | `run_workflow_engine` 的 `match outcome.status` **必须**处理 `Paused`（`engine.rs:282` 现在掉进 `other =>` 报「unexpected engine status」）——**这是一处破坏性变更**，LocalDub 侧未同步修（按「LocalDub 只是普通使用者」处理） |

**测试耗时顺带暴跌**：examples 包从 66.77s → 0.52s。原来是真在等 sleep。

#### D3 附一：影响面实测（改前做的，事后看偏保守）

当时统计「依赖同一次 drive 内自动 resume」的测试为 **7 个**：

| 位置 | 测试 |
| --- | --- |
| `engine/mod.rs` | `sleep_pauses_then_auto_resumes` |
| `engine/mod.rs` | `sleep_until_past_resolves_immediately` |
| `engine/mod.rs` | `sleep_until_schedules_timer` |
| `examples/.../workflows.rs` | `invoice_double_sleep_auto_resumes` |
| `examples/.../workflows.rs` | `refund_approved_disburses_after_timer` |
| `examples/.../workflows.rs` | `event_gate_emit_wait_then_sleep_until` |
| `examples/.../runtime.rs` | `invoice_double_sleep_survives_restart` |

**漏的是「阻塞式 approve」那一类**：它们在评估时被当作「已经是正确写法」（因为
测试用的是 `spawn` + `wait_until` + 外部 `signal_run`），但那个写法的前提正是
「drive 会一直阻塞」，所以也得改。教训：**别按「测试怎么写」分类，要按「依赖了
哪个行为」分类**。

#### D3 附二：曾经的错误结论（留档）

本决策曾两次被记错，都是**没查上游就下结论**：

1. 记成「折中方案：core 保留自投递，runtime 作为**可选的**外部驱动器，两者不冲突」
   ——实际上上游根本没有自投递这条路，自投递本身就是偏离。
2. 把「core 自轮询」说成能用、只是效率差 —— 实际上它**改变了语义**：
   挂起依赖进程存活，`approve` 会永久阻塞（而非返回 `paused`）。

教训与前几次一致：**移植时行为必须逐条对照上游实跑**，不能凭「功能上像是等价的」
推断。这次是用户要求「看看上游 / 可以对 TS 测试」，实跑后 5 分钟就得到了相反的答案。

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

#### 先澄清两个正交的维度（容易混淆）

| 维度 | 问题 | 是否决定 runtime 要不要 |
| ---- | ---- | ---------------------- |
| **有没有持久存储** | 数据跨进程重启还在吗？ | ❌ 不决定。两种形态**都需要**（runtime 的 `ExecutionStore` 也是持久层） |
| **有没有常驻的驱动者** | 挂起后**谁**去把它捡起来接着跑？ | ✅ **这才是判据** |

LocalDub 是「有持久存储 + 有常驻驱动者」——`FsRunStore` 保证数据不丢，
`packages/server/` 的 `run_worker` 常驻循环保证有人接着跑。两者都满足，
所以不需要 runtime 的 sweep / lease。

**关键短语是「常驻」= 进程能无限期活着并轮询**，不是「有持久化」。

#### 形态 A：常驻驱动者（本地自有进程 / 本地服务器 / 传统服务器）

一个能长期活着的进程一直在跑（`run_worker` 那类循环），随时能把挂起的 run
接着跑。LocalDub 属此类。

这个形态下 **lease / 有界 sweep / timer 投递都不是刚需**——因为「谁接着跑」
这个问题已经被常驻循环回答了。LocalDub 已有等价实现，见下节。

#### 形态 B：单次执行有时长上限的托管进程（serverless）

serverless 的限制不是「没有进程」，而是**单个进程有硬性执行时长上限**
（Vercel 函数超时、Cloudflare `maxDurationMs`、Netlify Scheduled Function 等），
所以**它不可能常驻**，也就不可能有「一直等在那儿捡活」的循环。

每次调用都是**一次性短命进程**，只做一件有界的事然后退出。

这个形态下**每一项都是刚需**。以 `cloudflare-d1/src/worker.ts` 为证：

| 上游 runtime 能力 | 形态 B 下为何必需 | 例证 |
| --- | --- | --- |
| **有界 sweep** | 一次 host 执行有超时，必须切成有界单元 | `maxDurationMs: 25_000` / `maxTimers: 25` |
| **lease** | 每次调用都是新进程，必须防止两个 worker 同时跑同一个 run | `leaseOwner: 'http:start'` / `'http:payment'` |
| **timer 投递** | **没有常驻进程，timer 必须由外部唤起者认领** | `readyAt` + `ctx.sleepUntil`，靠 `scheduled` handler 唤醒 |
| **list / timeline API** | 无进程内状态可查，全部走 store | `runtime.store.listRuns` / `getRunTimeline` |
| **host adapter** | 把平台入口（`scheduled()` / cron）接到 `sweep()` | `createCloudflareWorkflowScheduledHandler` |
| **幂等投递** | webhook 会重试 | `signalId` |

**关键**：形态 B 下没有常驻进程，所以 `ctx.sleep` 那种「挂起后等引擎自己
投递」的方案根本不可行——每个挂起的 run 都得等下一次外部调用**把唤醒条件
查出来再投递**。这正是 D3 里那个问题的实例，也是 `sweep` 存在的理由。

**结论：要做 `cloudflare-d1` 这类例子，runtime 层（lease / sweep / timer 投递 /
list）全部必需，D2 与 D4 不能作废。**

#### 对 LocalDub 的意义

它**属于形态 A**，所以「只用 `workflow-core`、不接 runtime」是**正确取舍**，
不是「可控性不足」：

- 上游 `quick-start.md` 的「a workflow that does one thing」正是形态 A 的最小
  场景——一次 `runWorkflow` 跑完，core 足够
- LocalDub 更进一步：它有常驻 worker，连「挂起后谁恢复」都自己解决了
- runtime 层对它没有增量价值（除非哪天它要做成「跑一次就退」的 CLI 形态，
  那才会掉进形态 B）

**反过来**：如果只做 core 但**没有**常驻驱动者（例如「CLI 跑一次就退」），
挂起的 run 就没人捡——那时 core 就不够了，必须有外部 cron 调 `sweep`。

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

### D6. core 的 25ms 轮询要不要顺带修？—— **已被 D3 吸收：代码已删除**

原问题：`exec_pause` 每 25ms `store.get_events()` 全量重读 + `find_resume`
线性扫，是 O(n²)，README「已知边界」里记的唯一性能硬伤。当时纠结的是
「换成 `subscribe` 还是先不动」。

**D3 落地后这个问题不存在了**：`exec_pause` 不再等，轮询循环整段删除，
`RESUME_POLL_MS` 常量随之消失。不是「换个更快的等待方式」，是「不再需要等待」。

顺带的收益：`subscribe` 那条路也不用走了（它在 async 上下文里要
`spawn_blocking`，且 store 可以不给）。

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
3. ~~拍板 D3~~ → ~~动手改 core~~ —— **已完成**（见 D3 结论）。`exec_pause` 不再等，
   `WorkflowParked` 哨兵就位，21 个测试改写，145 个测试全绿。
   - **LocalDub 侧未同步**：`engine.rs:282` 的 `other =>` 仍需加 `Paused`。这是一处
     破坏性的接口变更——LocalDub 下次升级依赖时会踩到。
4. **拍板 D2 / D4**（D1 已定）：
   - D2（lease 位置）—— 倾向 store，前提是不塞进 `RunStore`
   - D4（sweep 边界）—— 倾向保留 `max_*` / `maxDurationMs`
5. **并行编排（D5 附二）可以独立推进**：它只依赖 core 的 `try_join!`，与
   runtime 层的决策无关。如果想让 LocalDub 先有收益，这条可以并行开工。

**次序**：D3 已落地（它反向决定了 core 的形状），接下来是 D2 / D4 的细节。**

### 顺带暴露的一个设计缺口

`WorkflowExecutionStore` 目前没有「查所有到期的 timer」这种入口，而 D3 之后
`deadline` 是**绝对时间戳**（见 D3 结论的影响表）——这正是 sweep 索引需要的形状。
实现 runtime 的 timer 认领时，`waiting_for.deadline` 加上按时间排序的查询就是
`claim_due_timers` 的雏形。这一点在 D4 里一并定。
