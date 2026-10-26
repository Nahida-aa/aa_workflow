# core-runtime-map：闭卷重画的对照答案

> **用法**：定期（建议两周）**不看代码**白纸重画三样东西——①文件地图 ②事件三分类表
> ③四入口/driveClaimedRun 骨架——然后翻到本页核对。画不出来的地方就是「以为懂了
> 其实没懂」的机制；对不上的条目说明上游变了，回源码核实后更新本页。
> 行号基于 mirror base `b928717`（2026-10 快照），上游 diff 后按 §8 雷达更新。

主线一句话：**把「执行到哪了」记成 append-only 事件日志，把「继续执行」变成
「重放日志 + 从断点接着跑」**。handler 是普通 async 函数，每次 drive 从头重跑，
已 checkpoint 的原语按 stepId 查日志短路——耐久性来自重放，不来自挂起。

---

## 1. 文件地图（行数为 2026-10 快照）

### workflow-core（3515 行）

| 文件 | 行数 | 职责 | Rust 对应 |
| --- | --- | --- | --- |
| `engine/run-workflow.ts` | 1742 | 心脏：runWorkflow generator、四驱动模式分流、driveHandler、全部原语实现（engineStep/engineApprove/engineSleep/…）、reexecuteFrom 导入、middleware 组合、yield/deadline | `engine/run_workflow.rs` + `engine/mod.rs` |
| `types.ts` | 721 | WorkflowEvent 联合 + 三分类注释（`:45-70`）、StepOptions/RetryOptions、RunState（`:540`）、BaseCtx、waitingFor（`:551`） | `event.rs` + `run_store/mod.rs` + `define/mod.rs` |
| `define/define-workflow.ts` | 206 | `createWorkflow(config)` 声明期入口（唯一公共构造门） | `define/define_workflow.rs` |
| `telemetry.ts` | 182 | OTel span（`tanstack.workflow` 前缀）；记录 payload 默认不进属性 | 无（Rust 未做） |
| `run-store/in-memory.ts` | 117 | core 侧测试 store（旧 6 方法契约） | `run_store/in_memory.rs` |
| `engine/state-diff.ts` | 113 | `Operation`/diff——STATE_DELTA 的 delta 生成 | `engine/state_diff.rs` |
| `registry/select-version.ts` | 105 | 版本路由：run 持久的 version 匹配 current/previous，**不匹配不回退** | `registry/select_version.rs` |
| `server/parse-request.ts` + `handle-webhook.ts` | 84+68 | HTTP 入口：webhook/signalDelivery/approval → 投递 | 无（宿主自接） |
| `middleware/create-middleware.ts` | 57 | `createMiddleware().server(fn)` ctx 扩展链 | `middleware/` |
| `index.ts` / `result.ts` | 96+19 | 桶导出 / WorkflowPaused 哨兵 | `lib.rs` |

### workflow-runtime（3128 行）

| 文件 | 行数 | 职责 | Rust 对应 |
| --- | --- | --- | --- |
| `runtime-driver.ts` | 1351 | 四入口 + driveClaimedRun 骨架（`:746`）+ 心跳（`:1200`）+ syncTimerFromRunState（`:941`）+ classifyRun | `runtime_driver.rs` |
| `in-memory-store.ts` | 774 | 契约参考实现（队列+记录+索引三合一） | `in_memory_store.rs` |
| `types.ts` | 514 | WorkflowExecutionStore 契约（19 方法）+ 四态投递结果（`:423`） | `types.rs`（23 方法） |
| `schedule-materializer.ts` | 272 | cron/every 声明 → 队列播种（schedule → run） | `schedule_materializer.rs` |
| `run-store-adapter.ts` | 89 | 新契约 → core 旧契约降格（`createRunStoreAdapter`） | `run_store_adapter.rs` |
| `define-runtime.ts` + `index.ts` | 46+82 | `defineWorkflowRuntime(config)` / 桶导出 | 同名 |

## 2. WorkflowEvent 三分类（`types.ts:45-70` 官方注释）

| 类 | 成员 | 落盘？ | replay 用它做什么 |
| --- | --- | --- | --- |
| **Checkpoint**（replay 短路的依据，`CheckpointEvent` 提取类型 `:214`） | STEP_FINISHED / STEP_FAILED / SIGNAL_RESOLVED / APPROVAL_RESOLVED / NOW_RECORDED / UUID_RECORDED / RUN_FINISHED / RUN_ERRORED / STEP_ATTEMPT_STARTED / STEP_RETRY_SCHEDULED / COMPENSATION_REGISTERED / COMPENSATION_STARTED | ✅ `emitAndAppend`（`:1503`，CAS 追加） | 按 stepId 短路已完成的原语 |
| **Coordination**（定位挂起点） | SIGNAL_AWAITED / APPROVAL_REQUESTED | ✅ | 让宿主/resume 找到 pending wait；`waitingFor` 投影的来源 |
| **Observability**（replay 忽略） | RUN_STARTED / STEP_STARTED / STATE_DELTA / CUSTOM | ❌ 只 `emit`（`:91` → AsyncIterable / publish） | 纯观测；要不要持久化是宿主策略 |

关键细节：

- **`NOW_RECORDED` / `UUID_RECORDED`**：确定性的外部性补丁——先掷骰子再记录，
  重放读记录不重新生成（`engineNow :1270` / `engineUuid :1299`）。
- **`meta` 槽**：宿主元数据 + `importedFrom` 溯源（reexecuteFrom 导入的事件）；
  **`audience`**：引擎忽略，投影层过滤 internal/client/admin 视图用。
- **RUN_STARTED 不落盘**（测试原话 "the stream prepends the unpersisted
  RUN_STARTED"）——importedFrom 的 index 以**持久化日志**为基准，不受它影响。
- **TS 形态差异**：TS 没有独立的事件类型表达挂起——sleep/yield 统一走
  `engineWaitForEvent('__timer')`（`engineSleepUntil :1159`），挂起 =
  SIGNAL_AWAITED（`__timer` 通道）+ `waitingFor.kind: 'signal'|'approval'`。
  Rust 侧是 `STEP_PAUSED` / `STEP_RESUME` 一对 checkpoint + `waiting_for`
  投影——**形态不同，语义同构**（泊车写 checkpoint、恢复找 StepResume 短路）。

## 3. 四驱动模式 × 四入口

### core：runWorkflow 的四种驱动模式（`run-workflow.ts:34/:189/:260/:371/:189`）

```
input                          → start（新 run）
runId                          → resume（重放接续）
runId + recover: true          → recovery（崩溃捞回；sweep 与 deliverSignal 未等待时的推一把 `:256` 都走它）
runId + attach: true           → attach（只读回放，不执行 handler）
```

### runtime：四入口 → driveClaimedRun 骨架（`runtime-driver.ts:746`）

```
startRun        幂等 createRun（已存在且非 Queued → 不重复驱动）
deliverSignal   四态投递：NotFound / Duplicate / NotWaiting / Delivered（types.ts:423）
deliverApproval 同款四态
sweep           recover 的家：捞 stale lease 的 run + 驱动到期 timer
                    ↓ 全部汇入
driveClaimedRun:  resolveLeaseMs → claimRun（原子 CAS 认领 :796）
                  → startLeaseHeartbeat（lease/3 周期续租 :837）
                  → runWorkflow(recover: true, yieldResumeAt: now+1)
                  → syncTimerFromRunState（停在 __timer 的 deadline 登记成 timer 记录 :881）
                  → releaseRunLease（:902，finally 语义）
```

**store = 队列 + 系统记录 + 索引 三合一**：Queued 认领闸（谁抢到谁跑）、
RunState 投影（resume/审计）、timer/schedule 索引（sweep 的捞取面）。

## 4. 不变量清单（每条：出处 + 违反会怎样）

1. **replay 短路是无条件的**——已 checkpoint 的 step 按 stepId 直接返回记录结果，
   闭包不执行。违反（加任何「重新执行」的豁免）= 同一日志两次重放分叉。
   本仓曾有的 `up_to_date` 是唯一的豁免点，已删（aa-workflow a82bcc4）。
2. **同一条日志两次重放得到同一个 state**——state 由 `initialize(input)` 重建 +
   重放短路恢复，绝不持久化；并行分支对 state 的写入没有确定的先后 → 不允许
   （写入位置必须词法决定，aa-workflow AGENTS.md 有完整论证）。
3. **日志 append-only + CAS 全序**——`appendEvent(runId, index, event)` 的 index
   必须等于当前长度；两个并发写入者靠 CAS 裁决（`:1650/:1699` 的重试环）。
   违反 = 重放顺序不稳定。
4. **落盘与扇出是两条线**——checkpoint 类才 append；observability 类只 emit。
   混淆 = 日志膨胀 / replay 语义漂移。
5. **挂起即返回**——泊车 = 写 checkpoint 后立刻结束本次 drive（WorkflowPaused
   哨兵，`result.ts`）；宿主续驱靠重新驱动。违反（在挂起点阻塞）= 占死 worker。
6. **importedFrom 的 index 以源 run 的持久化日志为基准**——RUN_STARTED 不占下标。
   违反 = 溯源错位（aa-workflow 057a0c6 的测试钉死）。
7. **run 已存在且非 Queued 时 startRun 不重复驱动**——幂等边界；继续（recover/
   续驱）走 deliverSignal（或 recover 驱动），不是再 start 一次。
8. **终态不可原地翻转**——errored/finished 的 run 没有「解除终态继续跑」；
   retry 在 step 内（durable retry 是「延迟终态」），修复后重开 =
   reexecuteFrom 开新 run 播种。要原地翻案需要 INVALIDATED 原语（策略 C，未实现）。
9. **版本路由不回退**——run 持久化的 version 匹配不上 current/previous 时报错
   结束，绝不路由进新版代码（select-version.ts 的注释原话）。
10. **core 只区分「哪些要持久化」，介质不在词汇里**——磁盘与否、连接池、租约
    都是 runtime/store 层的事。core 的函数签名里不应该出现这些词。
