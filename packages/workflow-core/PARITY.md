# BaseCtx API 对等矩阵（aa-workflow Rust 端口 ↔ TanStack workflow-core）

对照真源：`learn_ls/workflow/packages/workflow-core/src/types.ts`（BaseCtx /
StepContext / StepRuntimeContext / WorkflowRuntimeContext / RunWorkflowOptions.runtime）。
本矩阵只覆盖 **ctx 层 API 对等**；引擎内部（checkpoint 日志、replay、RunState 信封）
的对等由各自文档描述，不在本表。

图例：✅ 完全对等（含语义） · ◐ 对等但有命名/签名差异 · ◯ 部分实现 · ✖ 未实现

> **挂起语义（已对齐）**：`yield_` / `sleep` / `sleep_until` / `wait_for_event` /
> `approve` 都是**写到挂起点就抛内部哨兵返回**，drive 以 `Paused` 收尾
> （`WorkflowParked`，对应上游的 `throw new WorkflowPaused()` +
> `if (engine.paused) return`）。**不阻塞等待**——唤醒由外部投递。
> 详见 [`docs/runtime-design.md`](../../docs/runtime-design.md) D3。
>
> 表内 `deadline` 一律是**绝对** UTC ms 时间戳（上游同形：`sleep(ms)` 定义在
> `sleepUntil(Date.now()+ms)` 上，`run-workflow.ts:992`）。

## ctx（handler 参数）

| TanStack BaseCtx | Rust（`Ctx<In, St, Ext>` 即 `BaseCtx<In, St, Ext>`；`WorkflowCtx = Ctx<Value, Value, ()>`） | 状态 | 备注 |
| ---------------- | ------------------------------------------------ | ---- | ---- |
| `runId: string` | `run_id: String`（字段） | ✅ | 公开字段，与 TS `runId` 同名同构 |
| `input: TInput` | `input: In`（字段；未声明 schema 时 `Value`） | ✅ | 与 TS 同名同构；借用即 `&In` |
| `state: TState` | `state: St`（字段；`St` 由 config `.state::<St>()` 限定） | ◐ | **实现细节不同**：TS 是 engine 共享对象的 in-place 引用（`baseCtx.state === engine.state`），Rust 是 per-drive 工作副本字段，durable 边界（step/approve/sleep/wait_for_event/yield）flush 回引擎镜像。镜像本身 per-invocation、每次 initialize 重建，故 flush 不参与持久性（与 TS 一致）；差异见差异清单 #7 |
| `TExtensions`（middleware 积累的 context） | `ext: Ext`（字段；`Ext` 由 `builder.middleware::<PExt>()` 限定，无 middleware 时 `()`） | ◐ | TS 是 `BaseCtx<TInput,TState> & TExtensions` 交集；Rust 拍平成**单一** Ext 字段（差异 #9）。`produce` 产出 → 每 drive 反序列化进 `ctx.ext` |
| `signal: AbortSignal`（run 级） | `is_cancelled()` | ◐ | 语义等价：取消信号可被动查询。差异：(1) 无法注册 `signal.addEventListener`（Rust 无该机制，需轮询）；(2) 引擎在 step 边界才检查——**step 闭包内的 `await` 无法被中断**，与 JS AbortSignal 同粒度但 Rust 侧无法「中止 promise」，闭包必须协作式自检（见下） |
| `runtime: WorkflowRuntimeContext` | `deadline() / time_remaining() / should_yield() / yield_()` | ◐ | TS 是嵌套对象；Rust **拍平为 ctx 顶层方法**。`yield` 是 Rust 保留字 → 命名 `yield_` |
| `runtime.deadline?: number` | `deadline() -> Option<i64>` | ✅ | 无 deadline 时 TS 为 `undefined`，Rust 为 `None` |
| `runtime.timeRemaining()` | `time_remaining() -> u64` | ✅ | 无 deadline：TS `Infinity`，Rust `u64::MAX`（外部可见均为「无限」，值不同） |
| `runtime.shouldYield(minRemainingMs?)` | `should_yield()` | ◐ | TS 支持按调用传 `minRemainingMs` 覆盖；Rust 只有 RunWorkflowOptions 级默认值，不支持逐调用覆盖 |
| `runtime.yield(options?)` | `yield_()` | ◐ | 行为对等：park 在 `"__timer"` 直到 `yieldResumeAt`（缺省 now+1ms），**写 checkpoint 即返回**，由外部 timer 投递放行。差异：TS 可传 `id`/`reason`；Rust 自动派生 `__yield-{n}` key，无 reason |
| `step(id, fn, options?)` | `step(id, f)` / `step_with(id, f, StepOptions)` | ✅ | |
| `sleep(ms, options?)` | `sleep(key, ms)` | ◐ | TS `id` 可选；Rust key **必填**（确定性/可重入之需） |
| `sleepUntil(timestamp, options?)` | `sleep_until(key, ts_ms)` | ◐ | 同上；过去时间戳立即放行，二者一致 |
| `waitForEvent(name, options?)` | `wait_for_event(key, name)` | ◐ | TS `id` 可选；Rust key 必填（同名事件多次 wait 需区分 checkpoint） |
| `approve({id,title,description})` | `approve(key, title)` | ◐ | 对象参数 → 位置参数；TS 有 `description`，Rust 暂无（后续补 `Ok`/`Err` 语义已具备） |
| `now()` | `now() -> Result<i64>` | ◐ | 返回值锁定为**确定性 checkpoint**（`NowRecorded`），跨 resume 一致 —— 语义强于 TS 的 per-run engine counter；仅在 store 失败时 Err |
| `uuid()` | `uuid() -> Result<String>` | ◐ | 同上；TS 为 Promise 纯净值，Rust 包 `Result`（PARITY delta，仅 store 失败时 Err） |
| `emit(name, value)` | `emit(name, value: Value)` | ✅ | 不进日志、不参与 replay（WorkflowEvent::Custom 仅投递 publisher） |

## step 闭包参数（StepContext）

| TanStack StepContext | Rust（StepCtx） | 状态 | 备注 |
| -------------------- | --------------- | ---- | ---- |
| `id: string` | `StepCtx.id: String` | ✅ | 确定性 step ID，跨 retry 与 replay 稳定；`up_to_date` 内部 probe 用 `attempt: 0` 构造 |
| `attempt: number` | `StepCtx.attempt: usize` | ✅ | 1-based；`0` 仅见于 `up_to_date` make-check 的内部 probe |
| `input: TInput` | `input()` | ✅ | TS StepContext 无此字段，Rust 额外提供 |
| `runtime: StepRuntimeContext` | `deadline()/time_remaining()/should_yield()` | ◐ | 同 ctx 的 runtime 拍平 |
| `signal: AbortSignal`（attempt 级，step timeout / run abort） | `StepCtx::is_cancelled()` | ◐ | 协作式轮询谓词（差异 #2）：Rust 无法中止 in-flight future，长 step 必须自定节奏自检 |
| `progress(value)` | `progress(value: f64)` | ✅ | TS StepContext 无此字段？——见下注 |

注：`progress` 在 TS 侧属于 `StepOptions.onProgress` 回调而非 StepContext；Rust
以 `StepCtx::progress()` 推送，属 API 形状差异（上报渠道不同，行为均为 0..1 进度）。

## RunWorkflowOptions / engine 层入口对等

| TanStack | Rust | 备注 |
| -------- | ---- | ---- |
| `runId` | `RunWorkflowOptions::run_id()` | |
| `input` | `RunWorkflowOptions::new(input)` | |
| `targetStep` | `target_step()` | |
| `continueFrom` | `continue_from()` | |
| `runtime.deadline` | `deadline()` | |
| `runtime.minYieldRemainingMs` | `min_yield_remaining()`（缺省 1000） | |
| `runtime.yieldResumeAt` | `yield_resume_at()` | |
| `AbortController().cancel()` | `cancel_run(store, run_id)` | run 以 `Aborted` 终局；终态幂等；未知 run → `RunNotFound` |
| `signalEvent(runId, event, payload)` | `signal_event(store, run_id, name, payload)` | 扫 `StepPaused` 按 `signal_name` 定位投递；无监听者 → `WorkflowError::SignalLost` |
| 事件名 → 信号映射 | `"__approval"` / `"__timer"` / 用户事件名 | `approve`/`sleep`/`yield` 内部固定通道，用户事件名自定 |

## 已知待补 / 差异清单

1. **`yield` 命名**：Rust 保留字 → `yield_`（引用方需注意）。
2. **run 级 `signal`**：仅 `is_cancelled()` 谓词；无中止回调注册。实现细节：
   取消由 `cancel_run` 将 RunState 置 `Aborted`，引擎在 step 入口 / attempt
   重试前 / `exec_pause` 入口轮询拾取 → run 以 `Aborted` + `RunErrored
   "workflow aborted"` 终局（对齐 TanStack code `'aborted'`）。**`Aborted` 是
   锁存终态**：后续 drive 直接返回 `Aborted`，不把 run 复活（对齐上游 `resumeRun`
   对 aborted 的处理）。挂起期间 cancel 只翻状态，等下次 drive 才被观察——挂起时
   本来就没有进程在跑。
3. **step 级 `attempt` / `signal`**：`StepCtx.attempt` 与 `StepCtx::is_cancelled()`
   已暴露（见 StepContext 矩阵）；仍缺的是「可被中断的 await」——只能协作式自检。
4. **`shouldYield` 逐调用 `minRemainingMs`**：未支持（仅 RunWorkflowOptions 级）。
5. **`now()/uuid()` 包 `Result`**：设计上仅 store 失败时 Err；正常路径与 TS 等价。
6. **`approve.description`**：未实现（位置参数缺该项）。
7. **`state` 写传播（快照 vs in-place 共享）**：TS 中 `ctx.state` 是引擎共享对象
   （`baseCtx.state === engine.state`，L480），step 闭包捕获 `ctx` 后 in-place
   mutation 立即可见；Rust 中 `ctx.state` 是字段工作副本，`ctx.clone()` 产生
   **独立快照**——并行 step 各自持快照互不可见（driver 位点的 mutation 在
   durable 边界 flush 回镜像）。单 handler 串行流（driver 位点读写）语义等价；
   是否并发写下依赖克隆分裂，写入只在本 drive 可见。已由
   `state_parallel_steps_snapshot_then_driver_flush` 测试固化。
8. **引擎句柄不可见**：`BaseCtx` 持有 `#[doc(hidden)] pub(crate) engine:
   Arc<EngineRuntime>`——对外 API 恰为 TS 接口成员（runId/input/state + 原语
   方法），句柄如 TS 一样只在闭包作用域（此处为字段私有）持有。
9. **middleware Ext 交集 → 单一字段**：TS 支持多 middleware 的类型交集
   `{...ctx, ...m1Ext, ...m2Ext}`；Rust `ctx.ext: Ext` 为单一类型，
   最后一个 `.middleware::<PExt>()` 决定 `Ext`；多个 ext 的 host 负责在
   PExt 结构体内组合。差异见
   `define/define_workflow.rs` `WorkflowBuilder::middleware`。
10. **schema 即 serde 类型**：TS 的 `inputSchema`/`stateSchema`/`outputSchema`
    是运行时 zod schema；Rust 用 serde `DeserializeOwned` 类型代替（`.input::<T>()` /
    `.state::<T>()` / `.output::<T>()`），解析失败 = 类型错误，语义与 zod
    `.safeParse` 一致，但无 schema 实例对象（差异 #10）。
11. **`previousVersions` + `selectWorkflowVersion` 内建**：resume 时引擎自动
    按 `RunState.workflow_version` 在 `[current, ...previous_versions]` 中
    路由到正确的 handler（`select_workflow_version`，在 `registry/`
    目录，对齐上游 `registry/select-version.ts` 的文件组织），工作流定义用
    `.previous_versions(vec![v1])` 声明。`defaultStepRetry` 由
    `CreateWorkflowConfig::default_step_retry()` 或
    `Workflow::default_step_retry()` 设置（步骤级 `StepOptions::retry`
    优先）。

    **匹配不上的语义（与上游对齐，2026-09-14 修正）**：版本化 run 的
    `workflow_version` 既不是当前版本、也不在 `previous_versions` 里时，
    **报错而非回退**——回退会把 v1 的 run 路由进当前版本的代码，
    是确定性违规（上游 `select-version.ts` 注释原话）。错误码
    `RunErrorCode::WorkflowVersionMismatch`（对齐上游
    `workflow_version_mismatch`）。无版本的老 run（版本机制引入前）仍回退
    到当前定义——它跑的本就是无版本代码，是语义正确的兼容路径。

    与上游的签名差异：上游是 `selectWorkflowVersion(versions[], runId, store)`
    （显式数组 + 读 store），我们是 `select_workflow_version(workflow,
    persisted_version)`（从 `.previous_versions` 取候选 + 版本由调用方传入，
    因为 `run_workflow` 已经读过 `RunState`）。

## `createWorkflow` / `WorkflowBuilder` / `WorkflowDefinition` 对等矩阵

对照真源：`define-workflow.ts`（`WorkflowBuilder` L84-133、`createWorkflow`
L198）+ `types.ts`（`Ctx<TIn, TState, TExt>` L386）。

| TanStack | Rust（`WorkflowBuilder<In, Out, St, Ext>`） | 状态 | 备注 |
| -------- | ------------------------------------------------ | ---- | ---- |
| `createWorkflow({ id, ... })` | `create_workflow(CreateWorkflowConfig::new(id).input::<In>()...)` | ◐ | config `.initialize()` 重写 `initialize`；TS config 用 `zod object`；Rust 用 serde `DeserializeOwned` 类型 |
| `config.inputSchema` | `.input::<In>()` | ◐ | 同 #10（serde 类型） |
| `config.stateSchema` | `.state::<St>()` | ◐ | 同 #10 |
| `config.outputSchema` | `.output::<Out>()` | ◐ | 同 #10 |
| `config.description` | `.description()` | ✅ | |
| `config.version` | `.version()` | ✅ | |
| `config.initialize` | `.initialize()` | ✅ | |
| `config.defaultStepRetry` | `.default_step_retry(RetryPolicy::new(...))` | ✅ | |
| `builder.middleware(md)` | `.middleware::<PExt>(Middleware { produce, wrap })` | ◐ | Ext 为单一字段（差异 #9） |
| `builder.previousVersions(v)` | `.previous_versions(Vec<Workflow>)` | ✅ | TS 传 `Array<{..., workflow}>`; Rust 传已构建的 `Workflow` 或 `WorkflowDefinition.into_workflow()` |
| `builder.handler(\|ctx\| ...)` | `.handler(\|ctx: BaseCtx<In, St, Ext>\| async move { ... })` | ✅ | AOut 从返回值推断；输出用 `output_validator` 校验 |
| `WorkflowDefinition.deref` | `impl Deref<Target = Workflow>` for `WorkflowDefinition<...>` | ✅ | |
| `WorkflowDefinition.intoWorkflow()` | `.into_workflow()` | ✅ | |
| `middleware produce` → `ctx.ext` | `Middleware::produce` → `ctx.ext: Ext` | ✅ | 无 produce → `Ext: Default` |
| `middleware wrap(ctx, next)` | `Middleware::wrap(ctx, BoxFuture)` → `BoxFuture` | ✅ | 从外向内组合；注册序 = 从外向内（最外先注册） |
| `selectWorkflowVersion(persisted, registry)` | `select_workflow_version(workflow, persisted_version)` | ✅ | `.previous_versions` 替代 registry map；持久化版本 = `RunState.workflow_version` |
| `run.id` (handler) | `ctx.run_id` | ✅ | |
| 输出存储 | handler 返回 → `serde_json::to_value(out)` → `output_validator` 校验 → `WorkflowEvent::RunFinished` → `RunOutcome.output` | ✅ | |

## 验证覆盖

- `cargo test -p workflow-core`：59 个引擎级测试覆盖 Phase 1-5（named wait /
  sleep_until 过去/定时 / emit 不进日志 / now·uuid 确定性 / cancel_run
  三态 + 重打可恢复 / runtime budget / yield park+replay / **middleware
  produce + wrap 注册序 / `defaultStepRetry` 兜底 + 覆盖 / select_workflow_version
  路由 / resume 按持久化版本路由**）。
- `cargo test -p workflow-examples-shared`：28 个共享 workflow 的 e2e
  （含 `event_gate`：emit → wait_for_event → sleep_until 链路；含
  `typed_workflow_keeps_static_types` 确认 builder 泛型推断）。
- clippy：基线 8 个 pre-existing warning，新代码零新增。