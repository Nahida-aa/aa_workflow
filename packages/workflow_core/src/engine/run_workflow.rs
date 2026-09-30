//! 单次 invocation 的驱动入口：把 workflow **跑起来 / 续跑** 的顶层编排。
//!
//! 引擎本体（`EngineRuntime` 驱动态、`exec_step` / `exec_pause` / `signal_run`）
//! 在 [`super`](crate::engine) 中；这里只负责一次 `run_workflow` 调用内部：
//! 从 store 构造 run state、算 per-invocation state、造 `EngineRuntime`、跑 handler、
//! 收尾写终态事件与 RunState 信封。每次调用（start / resume）都独立走完整条路径。

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use crate::define::{AnyWorkflowDefinition, WorkflowDefinition};
use crate::engine::{DEFAULT_MIN_YIELD_REMAINING_MS, EngineRuntime, Fanout, StepHalt, WorkflowCancelled, WorkflowParked, now_ms};
use crate::error::{RunError, RunErrorCode, WorkflowError};
use crate::event::{RunStatus, StepStatus, WorkflowEvent, fold_step_states};
use crate::resource::Gate;
use crate::run_store::{RunState, RunStore};

/// `run_workflow` / `run_workflow_sync` 的入参（对齐上游 `RunWorkflowOptions`，
/// 见 `engine/run-workflow.ts:34-72`）。
///
/// # 为什么把 `workflow` / `run_store` 也收进来
///
/// 之前是 4 个位置参数 `run_workflow(&wf, store, &opts, publisher)`——容易传错位。
/// 上游把**全部**入参放在一个结构体里，`workflow` / `runStore` 是**必填字段**。
/// 这里照做：必填项由 [`RunWorkflowOptions::new`] 强制（构造完就一定齐了），
/// 可选项走 builder 链。
///
/// # 与上游的字段差异
///
/// | 上游 `RunWorkflowOptions` | 本结构 | 说明 |
/// | --- | --- | --- |
/// | `workflow` / `runStore` | ✅ `workflow` / `run_store` | 必填 |
/// | `input` / `runId` / `deadline` / `minYieldRemainingMs` / `yieldResumeAt` | ✅ | 同名同义 |
/// | `publish` | ✅ `publisher` | 位置从参数移进结构体 |
/// | `signalDelivery` / `approval` | — | 我们走 `signal_run` / `signal_event` 先落盘再 drive（D3 的形态差异） |
/// | `recover` / `attach` / `signal` / `threadId` / `outputSink` / `telemetry` | — | **暂无**；未做，不是不做 |
/// | — | ➕ `continue_from` / `target_step` | **本地扩展**（上游连这两个概念都没有） |
///
/// # 为什么字段类型是 `AnyWorkflowDefinition`（newtype）而不是裸 `WorkflowDefinition`
///
/// 上游这个字段是 `AnyWorkflowDefinition`，而它**本身就是别名**
/// `WorkflowDefinition<any, any, any>`（`types.ts:466`）——TS 的 `any` 双向可赋值，
/// 一个类型同时充当「带类型的声明」和「擦除的运行站点」。Rust 做不到：
/// `WorkflowDefinition<ChargeInput, Draft>` 与 `WorkflowDefinition<Value, Value>`
/// 是两个互不 coercion 的类型，所以擦除态必须是**独立的类型**，也就是这个 newtype。
/// 它的名字沿用上游，于是本字段与上游一字不差。
///
/// newtype 内部持 `Arc<WorkflowDefinition>`：`previous_versions` 是 `Vec`、另有
/// 若干 `Arc`，按值持有会让每次 run 都 deep-copy 一遍；`Arc` 让 clone 变引用计数自增，
/// 也避免给结构体引入生命周期参数（那会让 builder 链很难写）。
/// 事件回调的**已擦除**类型：返回 future 而非直接调用，所以宿主可以给异步实现
/// （对齐上游 `publish?: (runId, event) => void | Promise<void>`）。
///
/// **按值收 `WorkflowEvent`**，不是 `&WorkflowEvent`。这不是口味问题：
/// `publish()` 走队列时本来就要克隆一份（调用点只有 `&self`），drain task 已经
/// 持有一份完整副本。传引用就得让返回的 future 借用它，于是被迫用
/// `BoxFuture<'static, _>` + HRTB 兜（`for<'a> Fn(&'a E) -> BoxFuture<'a, ()>`
/// 在 `dyn` 上是噩梦），代价是 async block **不能借用事件**。
/// 按值传则一次克隆都不浪费、生命周期问题直接消失：
///
/// ```ignore
/// .async_publisher(|ev| async move { sink.send(ev).await })   // 借用不了，但也不需要
/// ```
pub type Publisher =
    Arc<dyn Fn(WorkflowEvent) -> crate::define::BoxFuture<'static, ()> + Send + Sync>;

pub struct RunWorkflowOptions {
    /// 要驱动的 workflow。**必填**（由 [`Self::new`] 保证）。
    ///
    /// 用上游的名字，让本字段与 TanStack 的 `RunWorkflowOptions.workflow` 一字不差。
    /// `AnyWorkflowDefinition` = 擦除态的 [`WorkflowDefinition`]（上游是
    /// `WorkflowDefinition<any,any,any>` 的别名；Rust 没有 `any` 的双向可赋值，
    /// 所以擦除态是独立类型）。内部持 `Arc`，clone 只加引用计数。
    pub workflow: AnyWorkflowDefinition,
    /// 事件日志 / run 元数据的落盘位置。**必填**（由 [`Self::new`] 保证）。
    pub run_store: Arc<dyn RunStore>,

    /// 复用该 run_id ⇒ resume（成功 step 短路、失败 rethrow）。
    pub run_id: Option<String>,
    /// run 输入。默认 `Value::Null`。
    pub input: serde_json::Value,
    /// 命中即停（本地扩展；上游用 handler 内 early `return`）。
    pub target_step: Option<String>,
    /// 从该 step 的最新终态 checkpoint 处截断后重跑后缀（**本地扩展**）。
    pub continue_from: Option<String>,
    /// 本次 drive 的绝对 UTC ms 预算（上游 `deadline`）。设了之后
    /// `time_remaining()` / `should_yield()` / `ctx.yield_()` 才生效；
    /// 每次 resume 都可以给一个新的。
    pub deadline: Option<i64>,
    /// 剩余预算低于此值时 `should_yield()` 翻真（上游 `minYieldRemainingMs`，默认 1000）。
    pub min_yield_remaining_ms: Option<u64>,
    /// `ctx.yield_()` 的重新唤醒时刻（上游 `yieldResumeAt`；默认每次调用「now+1ms」）。
    pub yield_resume_at: Option<i64>,
    /// 每个事件都会回调（上游 `publish`）——host 可以接到 Redis / Durable Streams
    /// 之类的扇出通道，让别的节点能 tail 这个 run。
    ///
    /// # 这是「可观测」与「耐久」的分界
    ///
    /// 引擎只把**事实**落盘（`append`）：`STEP_FINISHED` / `STEP_FAILED` /
    /// `STEP_PAUSED` / `NOW_RECORDED` / `UUID_RECORDED`。其余事件——`STEP_PROGRESS`、
    /// `STEP_STARTED`、`CUSTOM`、`STATE_DELTA`——**只**走到这里，**从不**写盘
    /// （`EngineRuntime::publish` 没有任何落盘路径）。
    ///
    /// 「要不要把 progress 落盘」是**宿主的策略**，引擎不替所有人决定：devtools
    /// 不需要，产品 UI 可能需要。所以要留就自己在这里写：
    ///
    /// ```ignore
    /// .publisher(Some(Arc::new(move |e: WorkflowEvent| {
    ///     if let WorkflowEvent::StepProgress { step_id, value, .. } = e {
    ///         my_db.insert_progress(run_id, step_id, *value);
    ///     }
    /// })))
    /// ```
    ///
    /// ## publisher 不拖慢引擎（对齐上游的 async generator）
    ///
    /// 投递是「同步调用点 + 独立 drain task」两段：调用点只做一次
    /// `UnboundedSender::send`（非阻塞），真正 await publisher 的是 drain task。
    ///
    /// 上游是同一个形状：`runWorkflow` 是个 `async function*`，它在
    /// `queue.shift()` 之后 `await publish` 再 `yield`，而**执行在另一个 task**
    /// 里继续往那个 queue 推（`run-workflow.ts:85-134`）。所以「publisher 慢」
    /// 在两边都只拖慢**消费端吞吐**，不拖慢**引擎进度**。
    ///
    /// 队列是**无界**的（上游就是个裸数组），所以慢 publisher 涨内存而不是卡住
    /// run；要背压就在宿主自己的 publisher 里做。
    ///
    /// **「返回前终态已投递」是守住的旧语义**：收尾发 shutdown 并 join drain，
    /// 所以 `RUN_FINISHED` / `RUN_ERRORED` / `STEP_PAUSED` 一定在
    /// `run_workflow` 返回前送达（Paused 早退路径也排空）。代价是慢 publisher 会
    /// 延迟**返回**，但执行早已结束。
    ///
    /// 死锁风险与改造前相同：publisher 若 `await` 依赖本次 run 完成的东西，仍会
    /// 挂——旧语义下内联调用时也会挂。不是回归。
    ///
    /// ## 坑 1：进程内回调 = 有丢失窗口
    ///
    /// 崩溃时最后一批事件就没了。所以「自己落盘」得到的是**被观测到的那部分**
    /// 耐久，不是「全部」耐久。拿它当审计日志会得到一份有洞的审计日志——
    /// 审计要耐久就别走这里，该让引擎 append。
    ///
    /// ## 坑 2：publisher panic 会被吞掉（与上游一致，刻意如此）
    ///
    /// 宿主 publisher 里的 panic **不会**掀掉你的 run —— `publish()` 用
    /// `catch_unwind` 兜住。上游同形（*"A misbehaving publisher must not break
    /// the run — swallow and continue."*，`run-workflow.ts:128-134`）：宿主代码
    /// 不该有能力损毁已经 append 了 checkpoint 的耐久状态。
    ///
    /// 代价是**静默** —— 本 crate 没有日志依赖，所以拿不到「publisher 炸了」这
    /// 条信息。要诊断就在**你自己的 publisher 内部** catch + 记日志，日志策略和
    /// 依赖都留在宿主那侧。
    /// 可选的事件回调（上游 `publish`）。多数宿主用同步的 [`Self::publisher`]；
    /// 需要 `await` 落盘/发网络的用 [`Self::async_publisher`]。
    pub publisher: Option<Publisher>,
}

impl RunWorkflowOptions {
    /// **必填项在这里**：`workflow` + `run_store`。构造完这两个就一定齐了。
    /// `workflow` 收 `impl Into<AnyWorkflowDefinition>`，所以三种写法都直接可用：
    /// 擦除态 `WorkflowDefinition`、带类型的 `WorkflowDefinition<TInput, …>`
    /// （`create_workflow` 的产物），以及它们的 `Arc`。
    pub fn new(
        workflow: impl Into<AnyWorkflowDefinition>,
        run_store: Arc<dyn RunStore>,
    ) -> Self {
        Self {
            workflow: workflow.into(),
            run_store,
            run_id: None,
            input: serde_json::Value::Null,
            target_step: None,
            continue_from: None,
            deadline: None,
            min_yield_remaining_ms: None,
            yield_resume_at: None,
            publisher: None,
        }
    }

    /// run 输入。
    pub fn input(mut self, v: serde_json::Value) -> Self {
        self.input = v;
        self
    }

    /// 复用该 run_id ⇒ resume。
    pub fn run_id(mut self, v: impl Into<String>) -> Self {
        self.run_id = Some(v.into());
        self
    }

    /// 命中即停。
    pub fn target_step(mut self, v: impl Into<String>) -> Self {
        self.target_step = Some(v.into());
        self
    }

    /// 从该 step 截断后重跑后缀。
    pub fn continue_from(mut self, v: impl Into<String>) -> Self {
        self.continue_from = Some(v.into());
        self
    }

    /// 设置本次 drive 的绝对 UTC ms 预算。
    pub fn deadline(mut self, v: i64) -> Self {
        self.deadline = Some(v);
        self
    }

    /// 剩余预算低于此值时允许让出（上游 `minYieldRemainingMs`）。
    pub fn min_yield_remaining(mut self, v: u64) -> Self {
        self.min_yield_remaining_ms = Some(v);
        self
    }

    /// `ctx.yield_()` 的重新唤醒时刻。
    pub fn yield_resume_at(mut self, v: i64) -> Self {
        self.yield_resume_at = Some(v);
        self
    }

    /// 事件回调（上游 `publish`）。收 `Option`，便于直接对接旧的四参数签名。
    ///
    /// 同步版：内部包装成一个**立即完成**的 future，实际投递发生在 drain task
    /// 上（见字段文档「publisher 不拖慢引擎」）。要真正 `await` 请用
    /// [`Self::async_publisher`]。
    ///
    /// 事件**按值**传入，与 `async_publisher` 同一套所有权语义（只有投递时机
    /// 不同），这样两条路径的心智模型是一致的：`ev` 归你，随便 move 进
    /// `async move`、随便丢给线程、随便 `join()`。
    pub fn publisher(mut self, v: Option<Arc<dyn Fn(WorkflowEvent) + Send + Sync>>) -> Self {
        self.publisher = v.map(|f| {
            Arc::new(move |ev: WorkflowEvent| -> crate::define::BoxFuture<'static, ()> {
                let out = f(ev);
                Box::pin(async move { out })
            }) as Publisher
        });
        self
    }

    /// 异步事件回调（上游 `publish` 的 `Promise<void>` 那一支）。
    ///
    /// 与 [`Self::publisher`] 的差别只是**允许 `await`**：投递在 drain task 上
    /// 串行进行，但引擎执行不受它阻塞。要落盘/发网络而不想卡住引擎，就用这个。
    ///
    /// 事件**按值**传入，所以 async block 可以直接 `async move` 整个事件 ——
    /// 不需要「先取值再进 async」那套：
    ///
    /// ```ignore
    /// .async_publisher(|ev| async move { sink.send(ev).await })
    /// ```
    ///
    /// 想留一份自己用就克隆（事件不大，且 `publish()` 已经为了入队克隆过一次，
    /// 这里不会再多一次引擎侧克隆）。
    pub fn async_publisher<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(WorkflowEvent) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        self.publisher = Some(Arc::new(move |ev: WorkflowEvent| {
            Box::pin(f(ev)) as crate::define::BoxFuture<'static, ()>
        }));
        self
    }

    /// 清掉事件回调。
    pub fn no_publisher(mut self) -> Self {
        self.publisher = None;
        self
    }
}

#[derive(Debug)]
pub struct RunOutcome {
    pub run_id: String,
    pub status: RunStatus,
    pub output: Option<serde_json::Value>,
    /// 与 [`RunState::error`](crate::run_store::RunState) 是**同一个类型**——
    /// 一次失败的 run 在「返回值」和「持久化信封」两处描述一致，不要只改一边。
    ///
    /// `RunError` 实现了 [`Display`](std::fmt::Display)（转发 `message`），
    /// 所以 `warn!("{e}")` / `e.to_string()` 照常可用；想要错误名用 `.name`。
    pub error: Option<RunError>,
}

/// Resolves which workflow definition drives a run.
///
/// 真正的实现在 [`crate::registry::select_workflow_version`]（对齐上游
/// `registry/select-version.ts` 的文件组织）；此处仅再导出。
pub use crate::registry::select_workflow_version;

/// Runs (or resumes) a workflow by driving its async handler.
///
/// Inputs:
/// - `continue_from`: the store truncates everything at `step_id`'s latest
///   terminal checkpoint (inclusive) before re-running the handler, so the
///   prefix short-circuits and the suffix re-executes from scratch.
/// - `target_step`: once that step succeeds the engine raises [`StepHalt`],
///   the handler unwinds (users should propagate with `?`), and the run ends
///   `Finished` with no output.
///
/// On resume, `ctx.step` short-circuits succeeded checkpoints (cached result,
/// `run` not re-executed) and rethrows failed ones. Multiplex step results
/// however you like — the log is the only source of truth.
pub async fn run_workflow(
    opts: &RunWorkflowOptions,
) -> Result<RunOutcome, WorkflowError> {
    let workflow: &WorkflowDefinition = &opts.workflow;
    let store = Arc::clone(&opts.run_store);
    let publisher = opts.publisher.clone();

    let run_id = opts
        .run_id
        .clone()
        .unwrap_or_else(|| format!("run_{}", now_ms()));
    let ts = now_ms();

    // continue_from lives at the store layer: cut the log at the step's latest
    // terminal checkpoint so the replayed handler re-runs that suffix.
    if let Some(cf) = &opts.continue_from {
        store.truncate_log_at_step(&run_id, cf)?;
    }

    // A run that was cancelled while parked must not be resurrected by a drive:
    // `cancel_run` flipped `status` to `Aborted` and nothing is watching a
    // parked run, so this is the first place it can be observed. Report it
    // instead of resetting the run to `Running`.
    //
    // (`Finished` / `Errored` are likewise terminal — re-driving those replays
    // from the log, which is how `resumed_run_replays_from_log_without_rewaiting`
    // asserts replay-safety; only `Aborted` is latched here, because a cancelled
    // run's handler has unfinished business and must not run again.)
    if let Some(st) = store.get_run_state(&run_id)?
        && st.status == RunStatus::Aborted
    {
        return Ok(RunOutcome {
            run_id,
            status: RunStatus::Aborted,
            output: None,
            error: st.error,
        });
    }

    let run_state = match store.get_run_state(&run_id)? {
        Some(mut st) => {
            st.status = RunStatus::Running;
            st.error = None;
            // 清除上一次 drive 留下的等待投影（上游 `run-workflow.ts:336-342`
            // 在 resume 前导里做同一件事）。挂起由 `exec_pause` 写、
            // **在下次 drive 开头清**——不是在被唤醒的那一刻清，因为挂起后
            // 进程已经退出，没有「那一刻」。
            st.waiting_for = None;
            st.pending_approval = None;
            st.updated_at = ts;
            st
        }
        None => RunState {
            run_id: run_id.clone(),
            workflow_id: workflow.id.clone(),
            workflow_version: workflow.version.clone(),
            status: RunStatus::Running,
            input: opts.input.clone(),
            output: None,
            error: None,
            waiting_for: None,
            pending_approval: None,
            awaiting: vec![],
            created_at: ts,
            updated_at: ts,
        },
    };
    store.set_run_state(&run_id, &run_state)?;

    // Version routing: resume against the definition whose `version` the run
    // persisted (workflow or one of its `previous_versions`). 版本化 run 匹配
    // 不上时**不回退**——那会把 v1 的 run 路由进当前版本的代码，是确定性违规
    // （上游 `select-version.ts` 注释原话）。这里以终局错误结束。
    let persisted_version = run_state
        .workflow_version
        .as_deref()
        .or(workflow.version.as_deref());
    let Some(active) = select_workflow_version(workflow, persisted_version) else {
        return Err(WorkflowError::Validation(format!(
            "workflow version mismatch: run `{run_id}` was started under version \
             {persisted_version:?}, which is not the current version nor in \
             `previous_versions`"
        )));
    };

    // Per-invocation state: re-derived from `initialize(input)` on every
    // start and resume (mirrors TanStack, where state is rebuilt from
    // `initialize({ input })` and never persisted). The handler input the
    // workflow sees is `opts.input` (see `EngineRuntime.input`), so initialize
    // shares that source for consistency.
    let state = match (active.initialize)(&opts.input) {
        Ok(s) => s,
        Err(e) => return init_failed(&store, run_state, &run_id, &e, publisher.as_ref()).await,
    };
    if let Some(validate) = &active.state_validator
        && let Err(e) = validate(&state)
    {
        return init_failed(&store, run_state, &run_id, &e, publisher.as_ref()).await;
    }

    let events = store.get_events(&run_id)?;
    let lives = fold_step_states(&events);
    let log_len = events.len();

    let target_reached = match &opts.target_step {
        Some(t) => lives
            .get(t)
            .map(|s| s.status == StepStatus::Success)
            .unwrap_or(false),
        None => false,
    };

    let state_mirror: Arc<Mutex<serde_json::Value>> = Arc::new(Mutex::new(state.clone()));

    // 事件扇出：同步调用点只 `send`（非阻塞），drain task 独立 await publisher。
    // 上游等价物是 `runWorkflow` 这个 async generator：它在 `queue.shift()` 之后
    // `await publish`，而执行在**另一个** task 里往 queue 推 —— 所以慢 publisher
    // 拖慢的是消费端吞吐，不是引擎进度（`run-workflow.ts:85-134`）。
    let (publish_tx, drain) = match opts.publisher.clone() {
        None => (None, None),
        Some(publisher) => {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Fanout>();
            let drain = tokio::spawn(async move {
                // 串行 await，保证投递顺序 = 产生顺序（无界队列不会重排）。
                while let Some(msg) = rx.recv().await {
                    let ev = match msg {
                        Fanout::Event(ev) => ev,
                        // 显式收尾信号：保证「run_workflow 返回前最后一条事件
                        // （RUN_FINISHED / RUN_ERRORED）也已投递」。
                        Fanout::Shutdown => break,
                    };
                    // 逐个兜住：宿主 panic 不得掀掉 run（上游同形，见 publish 文档）。
                    // future 在构造时才 panic，所以只能连 future 一起 catch。
                    // 按值移交：drain task 独占这份事件，publisher 也独占它，
                    // 所以全程只有 publish() 那一次克隆。
                    let fut = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        publisher(ev)
                    }));
                    if let Ok(fut) = fut {
                        // `await` 期间的 panic 由 task 级隔离兜住（见收尾 join）。
                        let _ = fut.await;
                    }
                }
            });
            (Some(tx), Some(drain))
        }
    };

    let inner = Arc::new(EngineRuntime {
        run_id: run_id.clone(),
        input: opts.input.clone(),
        state_mirror,
        prev_state_snapshot: Mutex::new(state),
        store: store.clone(),
        gate: Arc::new(Gate::new()),
        log_len: AtomicUsize::new(log_len),
        lives: Mutex::new(lives),
        target_step: opts.target_step.clone(),
        target_reached: AtomicBool::new(target_reached),
        publish_tx: publish_tx.clone(),
        now_counter: AtomicUsize::new(0),
        uuid_counter: AtomicUsize::new(0),
        deadline: opts.deadline,
        min_yield_remaining_ms: opts
            .min_yield_remaining_ms
            .unwrap_or(DEFAULT_MIN_YIELD_REMAINING_MS),
        yield_resume_at: opts.yield_resume_at,
        yield_counter: AtomicUsize::new(0),
        default_step_retry: active.default_step_retry.clone(),
    });
    inner.publish(&WorkflowEvent::RunStarted {
        ts,
        run_id: run_id.clone(),
    });
    eprintln!("[trace] run_started published");

    let ctx = crate::define::WorkflowCtx::untyped(inner.clone());
    eprintln!("[trace] ctx constructed");
    let handler_result = (active.handler)(ctx).await;
    // 尾段 state delta：ctx 在 handler 内 drop 时已把最终 state 同步进
    // mirror，这里统一 diff 发射（对齐上游 handler 返回 / catch 处的
    // flushStateDelta）。
    inner.emit_state_delta();

    // `failure` 是结构化的（`RunState.error`、`RUN_ERRORED`、`RunOutcome.error`
    // 三处同类型）。
    //
    // `WorkflowParked` 走**单独一条路**：它不是失败，也不是正常结束——run
    // 停在挂起点，等外部驱动器投递。`exec_pause` 已经把 checkpoint 与 RunState
    // 投影写好了，这里**不 append 终态事件、不覆盖 RunState**，直接以 `Paused`
    // 返回（对齐上游 `if (engine.paused) return`，`run-workflow.ts:529`）。
    let (status, output, failure) = match handler_result {
        Ok(output) => (RunStatus::Finished, Some(output), None),
        Err(e) if e.downcast_ref::<StepHalt>().is_some() => (RunStatus::Finished, None, None),
        Err(e) if e.downcast_ref::<WorkflowParked>().is_some() => {
            finish_fanout(publish_tx.as_ref(), drain).await;
            return Ok(RunOutcome {
                run_id,
                status: RunStatus::Paused,
                output: None,
                error: None,
            });
        }
        Err(e) if e.downcast_ref::<WorkflowCancelled>().is_some() => (
            RunStatus::Aborted,
            None,
            Some((RunError::cancelled(), RunErrorCode::Aborted)),
        ),
        Err(e) => (
            RunStatus::Errored,
            None,
            Some((RunError::from_anyhow(&e), RunErrorCode::Error)),
        ),
    };

    let terminal = match &failure {
        Some((err, code)) => WorkflowEvent::RunErrored {
            ts: now_ms(),
            run_id: run_id.clone(),
            error: err.clone(),
            code: *code,
        },
        None => WorkflowEvent::RunFinished {
            ts: now_ms(),
            run_id: run_id.clone(),
            output: output.clone(),
        },
    };
    if let Err(e) = inner.append(&terminal) {
        finish_fanout(publish_tx.as_ref(), drain).await;
        return Err(e);
    }
    inner.publish(&terminal);

    let mut st = run_state;
    st.status = status;
    st.output = output.clone();
    st.error = failure.as_ref().map(|(e, _)| e.clone());
    st.updated_at = now_ms();
    store.set_run_state(&run_id, &st)?;

    finish_fanout(publish_tx.as_ref(), drain).await;

    Ok(RunOutcome {
        run_id,
        status,
        output,
        error: failure.map(|(e, _)| e),
    })
}

/// 收尾事件扇出：发 shutdown 信号并等 drain task 把队列排空。
///
/// **必须在每条 return 路径上调用**（Paused 早退、正常收尾、以及 append 失败
/// 的 `?` 传播）——否则最后一条事件（`RUN_FINISHED` / `RUN_ERRORED`）可能还
/// 在队列里，`run_workflow` 就返回了，宿主会漏掉终态。
///
/// 注意这会让「慢 publisher」延迟 **run_workflow 的返回**，但不会延迟引擎执行
/// （执行早已结束）。这和改造前一致：原先 publisher 是内联同步调用，返回前必然
/// 已投递完毕——语义是守住的，没有静默削弱成 fire-and-forget。
///
/// 死锁风险与改造前相同：publisher 若 `await` 依赖本次 run 完成的东西，仍会挂。
/// 那在旧语义下也会挂（内联调用时挂），所以不是回归。
async fn finish_fanout(
    tx: Option<&tokio::sync::mpsc::UnboundedSender<Fanout>>,
    drain: Option<tokio::task::JoinHandle<()>>,
) {
    if let Some(tx) = tx {
        let _ = tx.send(Fanout::Shutdown);
    }
    if let Some(drain) = drain {
        let _ = drain.await;
    }
}

/// Persists a run that failed during pre-handler initialization (state
/// `initialize` returning an error, or the `state_schema` shape check
/// rejecting the built state) and returns the errored outcome. Counterpart of
/// TanStack zod `.safeParse` failing validation: the run is recorded as
/// failed rather than left dangling.
///
/// **只 publish，不 append**：什么都没跑，不该在日志里留下半条记录——TanStack
/// 同理，他们的 validation 失败走 `emit(...)`（只进内存队列），不是
/// `emitAndAppend`（`run-workflow.ts:215`）。
async fn init_failed(
    store: &Arc<dyn RunStore>,
    mut run_state: RunState,
    run_id: &str,
    err: &anyhow::Error,
    publisher: Option<&Publisher>,
) -> Result<RunOutcome, WorkflowError> {
    let run_err = RunError::from_anyhow(err);
    run_state.status = RunStatus::Errored;
    run_state.error = Some(run_err.clone());
    run_state.updated_at = now_ms();
    store.set_run_state(run_id, &run_state)?;
    if let Some(publish) = publisher {
        // 这条路径在 drain task 建立**之前**返回（handler 都没跑起来），所以直接
        // await 即可——没有引擎执行会被拖慢。panic 仍然吞掉，与 publish 同策略。
        let ev = WorkflowEvent::RunErrored {
            ts: now_ms(),
            run_id: run_id.to_string(),
            error: run_err.clone(),
            code: RunErrorCode::Validation,
        };
        if let Ok(fut) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| publish(ev)))
        {
            let _ = fut.await;
        }
    }
    Ok(RunOutcome {
        run_id: run_id.to_string(),
        status: RunStatus::Errored,
        output: None,
        error: Some(run_err),
    })
}

/// Sync convenience over a local multi-thread runtime for callers that are
/// not async themselves (e.g. LocalDub's CLI entrypoint).
pub fn run_workflow_sync(opts: &RunWorkflowOptions) -> Result<RunOutcome, WorkflowError> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .map_err(|e| WorkflowError::Internal(format!("tokio runtime: {e}")))?;
    rt.block_on(run_workflow(opts))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::define::{BaseCtx, CreateWorkflowConfig, StepCtx, WorkflowDefinition, WorkflowCtx, create_workflow};
    use crate::engine::testkit::TestLog;
    use crate::run_store::InMemoryStore;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(serde::Deserialize, serde::Serialize)]
    struct StrictState {
        n: i64,
    }

    /// 未显式给 `run_id` 时引擎生成 `run_<now_ms>`，并照常写 RunState 信封。
    #[tokio::test]
    async fn run_id_defaults_to_generated_when_absent() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("gen-id").handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", |_sc: StepCtx| async move { Ok(json!({ "ok": true })) })
                .await
        });
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({ "x": 1 })),
        )
        .await
        .unwrap();
        assert!(
            out.run_id.starts_with("run_"),
            "缺省 run_id 由引擎生成，实际为 {:?}",
            out.run_id
        );
        let st = store
            .get_run_state(&out.run_id)
            .unwrap()
            .expect("RunState 应已写入");
        assert_eq!(st.status, RunStatus::Finished);
        assert_eq!(st.input, json!({ "x": 1 }), "input 落在 RunState 信封上");
    }

    /// `initialize` 失败 → `init_failed`：run 记 Errored，且**不 append 任何事件**
    /// （什么都没跑）。对齐 TanStack zod `.safeParse` 失败的语义。
    #[tokio::test]
    async fn initialize_failure_errors_run_without_events() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("bad-init")
            .initialize(|_| Err(anyhow::anyhow!("cannot build state")))
            .handler(|ctx: WorkflowCtx| async move {
                ctx.step("never", |_sc: StepCtx| async move {
                    Ok(serde_json::Value::Null)
                })
                .await
            });
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("bad-init:r"),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.unwrap().message.contains("cannot build state"));
        assert!(
            store.get_events("bad-init:r").unwrap().is_empty(),
            "init 失败不应留下任何事件"
        );
        let st = store.get_run_state("bad-init:r").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Errored);
        assert!(st.output.is_none());
    }

    /// `state_schema` 形状不匹配走同一条 `init_failed` 路径。
    #[tokio::test]
    async fn state_shape_mismatch_errors_run_without_events() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = create_workflow(
            CreateWorkflowConfig::new("bad-state")
                .input::<serde_json::Value>()
                .initialize(|_| Ok(json!({ "n": "not-a-number" })))
                .state::<StrictState>(),
        )
        .handler(|ctx: BaseCtx<serde_json::Value, StrictState>| async move {
            Ok(json!({ "n": ctx.state.n }))
        });
        let wf: AnyWorkflowDefinition = wf.into();
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("bad-state:r"),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(
            out.error.unwrap().message.contains("invalid type"),
            "错误应来自 state 的 serde 形状校验"
        );
        assert!(
            store.get_events("bad-state:r").unwrap().is_empty(),
            "state 校验失败同样不落事件"
        );
        let st = store.get_run_state("bad-state:r").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Errored);
    }

    /// `RUN_ERRORED` 带机器可读的 `code`（TanStack `RUN_ERRORED.code`），
    /// host 该用它分支，而不是去匹配 `error.message`。
    #[tokio::test]
    async fn handler_failure_errored_with_error_code() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("bad").handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", |_sc: StepCtx| async move { anyhow::bail!("boom") })
                .await
        });
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("code:err"),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);

        let (err, code) = store
            .get_events("code:err")
            .unwrap()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, code, .. } => Some((error.clone(), *code)),
                _ => None,
            })
            .expect("终局事件应为 RunErrored");
        assert_eq!(code, RunErrorCode::Error);
        assert_eq!(code.as_str(), "error");
        assert_eq!(err.message, "boom");
    }

    #[tokio::test]
    async fn cancelled_run_errored_with_aborted_code() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("c").handler(|_ctx: WorkflowCtx| async move {
            Err(crate::engine::WorkflowCancelled.into())
        });
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("code:abort"),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Aborted);

        let (err, code) = store
            .get_events("code:abort")
            .unwrap()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, code, .. } => Some((error.clone(), *code)),
                _ => None,
            })
            .expect("终局事件应为 RunErrored");
        assert_eq!(code, RunErrorCode::Aborted);
        assert_eq!(err.name, "Aborted");
    }

    /// `initialize` 失败：publish `validation_error`，但**不进事件日志**（对齐
    /// TanStack 的 `emit` 而非 `emitAndAppend`）。
    #[tokio::test]
    async fn init_failure_publishes_validation_code_without_appending() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let seen: Arc<Mutex<Vec<WorkflowEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let publisher: Arc<dyn Fn(WorkflowEvent) + Send + Sync> = {
            let seen = seen.clone();
            Arc::new(move |ev| seen.lock().unwrap().push(ev))
        };
        let wf = WorkflowDefinition::new("bad-init")
            .initialize(|_| Err(anyhow::anyhow!("nope")))
            .handler(|_ctx: WorkflowCtx| async move { Ok(json!({ "unreachable": true })) });
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("code:validation")
            .publisher(Some(publisher)),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(
            store.get_events("code:validation").unwrap().is_empty(),
            "什么都没跑，不该留下事件"
        );

        let (err, code) = seen
            .lock()
            .unwrap()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, code, .. } => Some((error.clone(), *code)),
                _ => None,
            })
            .expect("应 publish RunErrored");
        assert_eq!(code, RunErrorCode::Validation);
        assert_eq!(code.as_str(), "validation_error");
        assert_eq!(err.message, "nope");
    }

    /// 一次失败的 run，在四处描述必须是**同一个** `RunError`：
    /// `RunOutcome.error`（返回值）、`RunState.error`（信封）、
    /// `RUN_ERRORED.error`（日志）、`StepAttempt.error`（step 级）。
    /// 任何一处退回字符串都是退步，这个测试钉住它。
    ///
    /// 顺带验证 `RunError` 的 `Display` 转发 `message`——所以改成结构化之后
    /// 调用方 `warn!("{e}")` 的写法不受影响。
    #[tokio::test]
    async fn one_failure_is_one_run_error_everywhere() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("one-error").handler(|ctx: WorkflowCtx| async move {
            ctx.step(
                "a",
                |_sc: StepCtx| async move { anyhow::bail!("same failure") },
            )
            .await
        });
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(json!({}))
            .run_id("one:r"),
        )
        .await
        .unwrap();

        let from_outcome = out.error.expect("RunOutcome.error");
        let from_state = store
            .get_run_state("one:r")
            .unwrap()
            .unwrap()
            .error
            .expect("RunState.error");

        let events = store.get_events("one:r").unwrap();
        let from_event = events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, .. } => Some(error.clone()),
                _ => None,
            })
            .expect("RUN_ERRORED.error");
        let from_attempt = events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::StepFailed { attempts, .. } => {
                    attempts.first().and_then(|a| a.error.clone())
                }
                _ => None,
            })
            .expect("StepAttempt.error");

        assert_eq!(
            from_outcome, from_state,
            "RunOutcome 与 RunState 应同类型同值"
        );
        assert_eq!(from_outcome, from_event, "与 RUN_ERRORED 应同类型同值");
        assert_eq!(from_outcome, from_attempt, "与 StepAttempt 应同类型同值");
        assert_eq!(from_outcome.message, "same failure");

        // Display 转发 message —— 调用方 `format!("{e}")` 不受影响。
        assert_eq!(from_outcome.to_string(), from_outcome.message);
    }

    /// 版本化 run 的 `workflow_version` 匹配不上 → 终局错误，**不回退**。
    /// 回退会把旧版 run 路由进新版代码（确定性违规，见 registry 模块文档）。
    #[tokio::test]
    async fn version_mismatch_errors_instead_of_falling_back() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());

        // 先以 v1 起一个 run（挂起在 approve，保持非终态以便后续 resume）。
        let v1 = WorkflowDefinition::new("ver")
            .version("v1")
            .handler(|ctx: WorkflowCtx| async move {
                ctx.approve("gate", "hold").await?;
                Ok(serde_json::Value::Null)
            });
        let task = tokio::spawn({
            let store = store.clone();
            let v1 = v1.clone();
            async move {
                run_workflow(
                    &RunWorkflowOptions::new(Arc::new(v1.clone()), store)
                        .input(serde_json::json!({}))
                    .run_id("ver:mismatch"),
                )
                .await
            }
        });
        // 等挂起（`RunState` 信封此时已记下 `workflow_version = "v1"`）。
        for _ in 0..500 {
            if let Some(st) = store.get_run_state("ver:mismatch").unwrap()
                && st.status == RunStatus::Paused
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        task.abort();
        let _ = task.await;

        // v2 **不带** v1 作 previous_versions → resume 应报错而非回退到 v2。
        let v2 = WorkflowDefinition::new("ver")
            .version("v2")
            .handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::Value::Null) });
        let err = run_workflow(
            &RunWorkflowOptions::new(Arc::new(v2.clone()), store.clone())
                .input(serde_json::json!({}))
                .run_id("ver:mismatch"),
        )
        .await
        .expect_err("版本不匹配应报错");
        assert!(
            matches!(err, WorkflowError::Validation(ref m) if m.contains("version mismatch")),
            "应为版本不匹配错误，实际 {err:?}"
        );
    }

    #[tokio::test]
    async fn failed_is_terminal_until_continue_from() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let fail = Arc::new(AtomicBool::new(true));
        let wf = WorkflowDefinition::new("w").handler({
            let log = log.clone();
            let fail = fail.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                let fail = fail.clone();
                async move {
                    for id in ["a", "b"] {
                        let (log, fail) = (log.clone(), fail.clone());
                        let id = id.to_string();
                        let id_c = id.clone();
                        ctx.step(&id, move |_sc: StepCtx| {
                            let (log, fail, id) = (log.clone(), fail.clone(), id_c.clone());
                            async move {
                                log.lock().unwrap().note_start(&id);
                                let res = if id == "b" && fail.swap(false, Ordering::SeqCst) {
                                    Err(anyhow::anyhow!("boom"))
                                } else {
                                    Ok(serde_json::Value::Null)
                                };
                                log.lock().unwrap().note_finish(&id);
                                res
                            }
                        })
                        .await?;
                    }
                    Ok(serde_json::Value::Null)
                }
            }
        });

        // first run: b fails, run errors
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.unwrap().message.contains("boom"));
        assert_eq!(log.lock().unwrap().runs["b"], 1);
        let run_id = out.run_id.clone();

        // plain resume: failed checkpoint rethrows → still errored, no rerun
        let again = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id(run_id.clone()),
        )
        .await
        .unwrap();
        assert_eq!(again.status, RunStatus::Errored);
        assert_eq!(
            log.lock().unwrap().runs["b"],
            1,
            "no re-execution on plain resume"
        );

        // continue_from "b": truncate b's checkpoint + suffix, replay reruns b
        let resumed = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id(run_id)
            .continue_from("b"),
        )
        .await
        .unwrap();
        assert_eq!(resumed.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1, "prefix before continue_from untouched");
        assert_eq!(l.runs["b"], 2, "suffix reran via continue_from");
    }

    #[tokio::test]
    async fn continue_from_resets_downstream() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = WorkflowDefinition::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                async move {
                    for id in ["a", "b", "c"] {
                        let log = log.clone();
                        let id = id.to_string();
                        let id_c = id.clone();
                        ctx.step(&id, move |_sc: StepCtx| {
                            let (log, id) = (log.clone(), id_c.clone());
                            async move {
                                log.lock().unwrap().note_start(&id);
                                log.lock().unwrap().note_finish(&id);
                                Ok(serde_json::Value::Null)
                            }
                        })
                        .await?;
                    }
                    Ok(serde_json::Value::Null)
                }
            }
        });
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .await
        .unwrap();
        let second = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id(out.run_id)
            .continue_from("b"),
        )
        .await
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 2);
        assert_eq!(l.runs["c"], 2);
    }

    #[tokio::test]
    async fn target_step_stops_downstream() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = WorkflowDefinition::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                async move {
                    for id in ["a", "b", "c"] {
                        let log = log.clone();
                        let id = id.to_string();
                        let id_c = id.clone();
                        ctx.step(&id, move |_sc: StepCtx| {
                            let (log, id) = (log.clone(), id_c.clone());
                            async move {
                                log.lock().unwrap().note_start(&id);
                                log.lock().unwrap().note_finish(&id);
                                Ok(serde_json::Value::Null)
                            }
                        })
                        .await?;
                    }
                    Ok(serde_json::Value::Null)
                }
            }
        });
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({}))
            .target_step("b"),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
        assert_eq!(
            l.runs.get("c").copied().unwrap_or(0),
            0,
            "downstream never ran"
        );
    }

    #[tokio::test]
    async fn handler_output_is_run_output() {
        let store = Arc::new(InMemoryStore::new());
        let wf = WorkflowDefinition::new("w").handler(|ctx: WorkflowCtx| async move {
            let v = ctx
                .step("a", move |_sc: StepCtx| async move {
                    Ok(serde_json::json!({"x": 1}))
                })
                .await?;
            Ok(serde_json::json!({ "out": v }))
        });
        let out = run_workflow(
            &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "out": { "x": 1 } })));
        let st = store.get_run_state(&out.run_id).unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Finished);
        assert_eq!(st.output, Some(serde_json::json!({ "out": { "x": 1 } })));
    }

    /// resume 按持久化 `workflow_version` 路由到 previous version 的 handler；
    /// 全新 run 用当前版本。
    #[tokio::test]
    async fn resume_routes_by_persisted_workflow_version() {
        let store = Arc::new(InMemoryStore::new());
        let v1: AnyWorkflowDefinition = create_workflow(
            CreateWorkflowConfig::new("ver-wf")
                .version("v1")
                .input::<serde_json::Value>(),
        )
        .handler(|ctx: BaseCtx<serde_json::Value>| async move {
            ctx.step("s", |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "ver": "v1" }))
            })
            .await
        })
        .into();

        let out1 = run_workflow(
            &RunWorkflowOptions::new(Arc::new(v1.clone()), store.clone())
                .input(serde_json::json!({}))
            .run_id("ver:r"),
        )
        .await
        .unwrap();
        assert_eq!(out1.output, Some(serde_json::json!({ "ver": "v1" })));

        let v2: AnyWorkflowDefinition = create_workflow(
            CreateWorkflowConfig::new("ver-wf")
                .version("v2")
                .input::<serde_json::Value>(),
        )
        .previous_versions(vec![v1])
        .handler(|ctx: BaseCtx<serde_json::Value>| async move {
            ctx.step("s", |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "ver": "v2" }))
            })
            .await
        })
        .into();

        let out2 = run_workflow(
            &RunWorkflowOptions::new(v2.clone(), store.clone())
                .input(serde_json::json!({}))
                .run_id("ver:r"),
        )
        .await
        .unwrap();
        assert_eq!(
            out2.output,
            Some(serde_json::json!({ "ver": "v1" })),
            "resume 路由到持久化的 previous version"
        );

        let out3 = run_workflow(
            &RunWorkflowOptions::new(v2.clone(), store.clone())
                .input(serde_json::json!({}))
                .run_id("ver:r2"),
        )
        .await
        .unwrap();
        assert_eq!(
            out3.output,
            Some(serde_json::json!({ "ver": "v2" })),
            "全新 run 用当前版本"
        );
    }
}
