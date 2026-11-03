//! WorkflowDefinition 定义层。按职责分文件（对齐 TanStack `src/define/` 的划分）：
//!
//! - 本文件：handler **运行时**能看到的东西 —— [`BaseCtx`]（`ctx`）、
//!   [`StepCtx`]、[`StepOptions`] / [`RetryPolicy`] / [`Backoff`]，以及 workflow
//!   本体 [`WorkflowDefinition`]。
//! - `define_workflow` 子模块：**声明**一个 workflow 的入口 ——
//!   [`CreateWorkflowConfig`] / [`WorkflowBuilder`] / [`create_workflow`] /
//!   [`WorkflowDefinition`]。
//!
//! [`Middleware`] 与其扩展类型在 [`crate::middleware`]（TS 侧同为独立目录）。

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::engine::EngineRuntime;
use crate::event::WorkflowEvent;
use crate::middleware::Middleware;

mod define_workflow;
pub mod ext;
mod state_handle;
pub use define_workflow::{CreateWorkflowConfig, WorkflowBuilder, create_workflow};
pub use ext::{DefExt, ExtBag, ExtNamespace, merge_ext_value};
pub use state_handle::StateHandle;

/// Boxed async step-returning future. Steps are spawned inside the engine's
/// driver; the async world is the default (mirrors `Promise.all` in JS).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The workflow's orchestrating closure: plain async control flow over
/// `ctx.step` / `try_join!`. Decisions (branching, which steps run) are made
/// by this code — the graph is an emergent, per-run artifact, never declared
/// up front.
pub type WorkflowHandler =
    Arc<dyn Fn(WorkflowCtx) -> BoxFuture<'static, anyhow::Result<serde_json::Value>> + Send + Sync>;

/// Per-invocation view handed step closures. Mirrors TanStack's `StepContext`
/// member-for-member: `id` / `attempt` / `runtime` helpers, with the per-attempt
/// `signal` exposed as a cooperative [`is_cancelled`](Self::is_cancelled) poll.
#[derive(Clone)]
pub struct StepCtx {
    pub(crate) inner: Arc<EngineRuntime>,
    /// Deterministic step ID. Stable across retries *and* replays of the same
    /// run — the idempotency-key candidate for external systems (`id`).
    pub id: String,
    /// Current attempt number (1-indexed).
    pub attempt: usize,
}

impl StepCtx {
    pub fn run_id(&self) -> &str {
        &self.inner.run_id
    }
    pub fn input(&self) -> &serde_json::Value {
        &self.inner.input
    }
    pub fn progress(&self, value: f64) {
        self.inner.publish_progress(&self.id, value);
    }
    /// Whether this run was cancelled via [`cancel_run`](crate::engine::cancel_run).
    /// Cooperative poll counterpart to TanStack's per-attempt `signal` — Rust
    /// cannot interrupt an in-flight future, so long steps must self-check on
    /// their own cadence (same granularity as `BaseCtx::is_cancelled`).
    pub fn is_cancelled(&self) -> bool {
        self.inner
            .store
            .get_run_state(&self.inner.run_id)
            .ok()
            .flatten()
            .map(|st| st.status == crate::event::RunStatus::Aborted)
            .unwrap_or(false)
    }
    /// Absolute UTC ms runtime budget for this drive (TanStack
    /// `runtime.deadline`); `None` when unbudgeted.
    pub fn deadline(&self) -> Option<i64> {
        self.inner.deadline
    }
    /// Ms of runtime budget left (`u64::MAX` when no deadline).
    pub fn time_remaining(&self) -> u64 {
        match self.inner.deadline {
            Some(d) => i64::saturating_sub(d, crate::engine::now_ms()).max(0) as u64,
            None => u64::MAX,
        }
    }
    /// True once the budget is nearly exhausted (`time_remaining() <
    /// min_yield_remaining_ms`); the step should `?` fast or orchestrate
    /// yielding via `WorkflowCtx::yield_`.
    pub fn should_yield(&self) -> bool {
        self.time_remaining() < self.inner.min_yield_remaining_ms
    }
}

/// The handler's argument — mirrors TanStack's `BaseCtx<TInput, TState>`
/// interface member-for-member. Public surface is exactly [`Self::run_id`]
/// (`runId`), [`Self::input`] (`input`) and [`Self::state`] (`state`), plus
/// the durable primitives as methods (`step` / `sleep` / `sleep_until` /
/// `wait_for_event` / `approve` / `now` / `uuid` / `emit`), a
/// [`is_cancelled`](Self::is_cancelled) `signal` and the runtime budget
/// helpers ([`deadline`](Self::deadline) / [`time_remaining`](Self::time_remaining)
/// / [`should_yield`](Self::should_yield) / [`yield_`](Self::yield_)).
///
/// The engine handle is *not* part of the public API: TanStack holds it in JS
/// closure scope (invisible on `ctx`), so here it lives in a
/// `#[doc(hidden)]` crate-private field — reachable but never nameable by
/// callers.
///
/// State semantics: `state` is the handler's working copy, seeded from the
/// engine's live image at drive start and flushed back to it before every
/// durable primitive. `ctx.input` / `ctx.state` are plain owned fields —
/// clone (or move) values as needed across `await` points.
#[derive(Clone)]
pub struct BaseCtx<TInput = serde_json::Value, TState = serde_json::Value> {
    /// **Run 级参数**（`RunWorkflowOptions.meta`，本仓有意扩展）——中间件的
    /// wrap 前置与策略钩子读取；handler 不消费、不落 `RunState`。与 `input`
    /// 的分工：input 是业务数据（不可变、handler 消费），meta 是策略参数
    /// （每次 drive 可换、中间件消费）。
    /// `runId: string`
    pub run_id: String,
    /// `input: TInput` — frozen run input (typed or `Value`)
    pub input: TInput,
    /// `state: TState` — 共享可变的 typed state（对齐 TS 的
    /// `ctx.state === engine.state`）；见 `state_handle` 模块文档。
    pub state: StateHandle<TState>,
    /// `TExtensions` — 中间件写进 ctx 的扩展袋。
    ///
    /// 上游是类型交集（`ctx.user` 是字段）；Rust 里是**按命名空间存取**
    /// （`ctx.ext.user()`）——使用者不需要定义任何合并类型。见 [`ExtBag`]。
    pub ext: ExtBag,
    /// **Run 级参数**（`RunWorkflowOptions.meta`，本仓有意扩展）——中间件的
    /// wrap 前置与策略钩子读取；handler 不消费、不落 `RunState`。与 `input`
    /// 的分工：input 是业务数据（不可变、handler 消费），meta 是策略参数
    /// （每次 drive 可换、中间件消费）。
    #[doc(hidden)]
    pub meta: Option<serde_json::Value>,
    /// Engine-side runtime, mirror of TanStack's closure-captured `engine`.
    /// Not part of the public API.
    #[doc(hidden)]
    pub(crate) engine: Arc<EngineRuntime>,
}

/// The full ctx type —— 对齐上游 `Ctx<TInput, TState>`。
///
/// 上游的第三个泛型 `TExtensions` 在这里不存在：扩展走 [`ExtBag`] 的命名空间获取，
/// 不做类型交集（见 `docs/decisions/0015`）。
pub type Ctx<TInput = serde_json::Value, TState = serde_json::Value> =
    BaseCtx<TInput, TState>;

/// Default (untyped) ctx: `Ctx<Value, Value>`.
pub type WorkflowCtx = Ctx<serde_json::Value, serde_json::Value>;

impl BaseCtx<serde_json::Value, serde_json::Value> {
    /// Engine-facing drive-start construction: freeze the run input and
    /// snapshot the current state image.
    pub(crate) fn untyped(engine: Arc<EngineRuntime>) -> Self {
        let mirror = engine.state_mirror.clone();
        let state_value = mirror.lock().expect("state mirror lock poisoned").clone();
        let state = StateHandle::new(
            state_value,
            Arc::clone(&mirror),
            Arc::new(|v: &serde_json::Value| v.clone()),
        );
        Self {
            run_id: engine.run_id.clone(),
            input: engine.input.clone(),
            meta: engine.meta.clone(),
            state,
            ext: {
                let raw = engine.ext_extra.lock().expect("ext lock").clone();
                ExtBag::from_value(raw)
            },
            engine,
        }
    }
}

impl<TInput, TState> BaseCtx<TInput, TState> {
    /// Write the handler's state changes into the engine's mirror and emit a
    /// `STATE_DELTA` if anything changed. Called at every durable boundary.
    pub(crate) fn flush_state(&self) -> anyhow::Result<()>
    where
        TState: serde::Serialize,
    {
        eprintln!("[trace] flush_state enter");
        self.state.sync();
        self.engine.emit_state_delta();
        Ok(())
    }

    /// Runs `run` durably under `step_id` and resolves to `T` — the closure's own
    /// return type, the way upstream's `step<T>` behaves.
    ///
    /// On replay (resume) a previously succeeded step short-circuits to its
    /// cached result *without* calling `run` again; a previously failed step
    /// rethrows the stored error. The cached result is deserialized back into `T`,
    /// so a recorded shape that no longer matches `T` fails loudly here instead
    /// of silently yielding `null`.
    ///
    /// On a fresh run the closure's `T` is returned untouched — the `Value` that
    /// goes into `STEP_FINISHED` is derived from it, never round-tripped back.
    /// Only the log is `serde_json::Value`; the handler keeps its types.
    ///
    /// The step closure is async so that concurrent steps compose via
    /// `tokio::try_join!` — parallel durable execution, no custom primitive.
    ///
    /// # Bound on `T`
    ///
    /// `T` must survive a JSON round-trip only for the *replay* path (the log is
    /// the sole source of truth there). A type that serializes to something it
    /// can't deserialize from — a `f64` field that may be `NaN`, a `HashMap`
    /// keyed by a type with a custom `Deserialize` — will therefore work on the
    /// first drive and fail on resume. That asymmetry is inherent to storing the
    /// result as JSON, not something this signature can paper over.
    /// Phase-1 补偿登记（undo 轴，对齐 #19 `ctx.compensate`）：登记先于受
    /// 保护效果；内存登记 + 重放重建（handler 重跑确定性重登记，回调不
    /// 序列化）。未捕获失败时引擎逆序执行已登记补偿；名单由使用者提供，
    /// 不校验封闭性（"unsafe" 语义：名单外下游的处置由调用者负责）。
    pub fn compensate(
        &self,
        id: impl Into<String>,
        undo: impl Fn(StepCtx) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>,
            > + Send
            + Sync
            + 'static,
        retry: RetryPolicy,
    ) -> anyhow::Result<()> {
        let id = id.into();
        if id.starts_with("__compensation:") || id.starts_with("__retry:") {
            return Err(anyhow::anyhow!(
                "compensation id \"{id}\" uses the reserved __compensation:/__retry: prefix"
            ));
        }
        // 重复 id 直接报错（对齐上游 `Duplicate compensation id "x"`）。否则同一
        // 补偿会进名单两次，失败时逆序 undo 把它执行两遍——且第二次的撤销对象
        // 可能已经被第一次消掉了。
        {
            let mut list = self
                .engine
                .compensations
                .lock()
                .expect("compensations lock");
            if list.iter().any(|c| c.id == id) {
                return Err(anyhow::anyhow!("Duplicate compensation id \"{id}\"."));
            }
            list.push(crate::engine::Compensation {
                id: id.clone(),
                undo: std::sync::Arc::new(undo),
                retry,
            });
        }
        // 登记落成耐久事件（对齐上游 `COMPENSATION_REGISTERED`）：handler 重放时
        // 这一步不会重复写日志，崩溃后也从日志看得见"登记过什么"。
        self.engine.append_compensation_registered(&id)?;
        Ok(())
    }

    pub async fn step<T, F, Fut>(&self, step_id: &str, run: F) -> anyhow::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
        F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = anyhow::Result<T>> + Send + 'static,
        TState: serde::Serialize,
    {
        self.step_with(step_id, StepOptions::default(), run).await
    }

    /// Durable approval wait: pauses the run until [`signal_run`](crate::engine::signal_run)
    /// delivers a decision for `key`. `reason` is persisted in the `SignalAwaited`
    /// checkpoint for approvers. Resolves to the signal's payload on success;
    /// on replay the already-recorded `SignalResolved` is served from the log.
    pub async fn approve(
        &self,
        key: impl Into<String>,
        reason: impl AsRef<str>,
    ) -> anyhow::Result<serde_json::Value>
    where
        TState: serde::Serialize,
    {
        self.approve_with(key, reason, ApproveOptions::default()).await
    }

    /// [`Self::approve`] + [`ApproveOptions`]（`meta` / payload `schema`）。
    pub async fn approve_with(
        &self,
        key: impl Into<String>,
        reason: impl AsRef<str>,
        opts: ApproveOptions,
    ) -> anyhow::Result<serde_json::Value>
    where
        TState: serde::Serialize,
    {
        self.flush_state()?;
        crate::engine::exec_pause_with(
            &self.engine,
            &key.into(),
            "__approval",
            reason.as_ref(),
            opts.deadline,
            opts.meta,
            opts.schema.map(|v| v.validator),
        )
        .await
    }

    /// Durable sleep: pauses the run until `dur` elapses. `key` is the
    /// deterministic pause identity. An external timer (the runtime's sweep,
    /// indexing on `RunState.waiting_for.deadline`) delivers the resume via
    /// [`signal_run`](crate::engine::signal_run) once the deadline passes; on
    /// replay, a previously delivered resume short-circuits immediately.
    pub async fn sleep(
        &self,
        key: impl Into<String>,
        dur: std::time::Duration,
    ) -> anyhow::Result<serde_json::Value>
    where
        TState: serde::Serialize,
    {
        // 与上游同构：`sleep(ms)` 就是 `sleepUntil(now + ms)`
        // （`run-workflow.ts:992`）。deadline 一律**绝对时间戳**。
        let due = crate::engine::now_ms() + dur.as_millis() as i64;
        self.sleep_until(key, due).await
    }

    /// Durable absolute-time wait: pauses until wall-clock `ts_ms` (equivalent
    /// to TanStack's `sleepUntil`). The timestamp is stored **verbatim** —
    /// a timestamp already in the past parks with an expired deadline, which a
    /// timer sweep claims on its next pass. `key` is the deterministic pause
    /// identity.
    pub async fn sleep_until(
        &self,
        key: impl Into<String>,
        ts_ms: i64,
    ) -> anyhow::Result<serde_json::Value>
    where
        TState: serde::Serialize,
    {
        self.flush_state()?;
        crate::engine::exec_pause(&self.engine, &key.into(), "__timer", "sleep", Some(ts_ms)).await
    }

    /// Durable named wait: pauses the run until [`signal_event`](crate::engine::signal_event)
    /// delivers a payload for `event_name`. `key` is the deterministic pause
    /// identity (must not collide with step ids); `event_name` is the channel
    /// a host signals on (`RunState.waiting_for.signal_name`). On replay a
    /// previously delivered resume short-circuits from the log.
    pub async fn wait_for_event(
        &self,
        key: impl Into<String>,
        event_name: impl AsRef<str>,
    ) -> anyhow::Result<serde_json::Value>
    where
        TState: serde::Serialize,
    {
        self.wait_for_event_with(key, event_name, WaitForEventOptions::default())
            .await
    }

    /// [`Self::wait_for_event`] + [`WaitForEventOptions`]（`deadline` / `meta` /
    /// payload `schema`）。对齐 TanStack `WaitForEventOptions`（`types.ts:268-279`）。
    pub async fn wait_for_event_with(
        &self,
        key: impl Into<String>,
        event_name: impl AsRef<str>,
        opts: WaitForEventOptions,
    ) -> anyhow::Result<serde_json::Value>
    where
        TState: serde::Serialize,
    {
        let name = event_name.as_ref();
        self.flush_state()?;
        crate::engine::exec_pause_with(
            &self.engine,
            &key.into(),
            name,
            "event",
            opts.deadline,
            opts.meta,
            opts.schema.map(|v| v.validator),
        )
        .await
    }

    /// Emit an observability event to the publisher. Never appended to the
    /// log, so it is outside replay — `fold_step_states` and resume ignore it
    /// (mirrors TanStack's `emit` / `CUSTOM`).
    pub fn emit(&self, name: impl AsRef<str>, value: serde_json::Value) {
        self.engine.publish(&WorkflowEvent::Custom {
            ts: crate::engine::now_ms(),
            run_id: self.run_id.clone(),
            name: name.as_ref().to_string(),
            value,
        });
    }

    /// Deterministic wall-clock (TanStack `ctx.now`): records the call's
    /// timestamp as a checkpoint; replay serves the recorded value, so a run
    /// sees the same clock across resumes. Returns `Err` only on store failure.
    pub fn now(&self) -> anyhow::Result<i64> {
        crate::engine::exec_now(&self.engine)
    }

    /// Deterministic id (TanStack `ctx.uuid`): records a generated UUIDv4 as a
    /// checkpoint; replay serves the recorded id, so the same value is seen
    /// across resumes. Returns `Err` only on store failure.
    pub fn uuid(&self) -> anyhow::Result<String> {
        crate::engine::exec_uuid(&self.engine)
    }

    /// Whether this run was cancelled via [`cancel_run`](crate::engine::cancel_run).
    /// Polled at step boundaries only — the engine cannot interrupt a step's
    /// in-flight `await` (same granularity as JS `AbortSignal`: the closure
    /// must check cooperatively).
    pub fn is_cancelled(&self) -> bool {
        self.engine
            .store
            .get_run_state(&self.run_id)
            .ok()
            .flatten()
            .map(|st| st.status == crate::event::RunStatus::Aborted)
            .unwrap_or(false)
    }

    /// Absolute UTC ms runtime budget for this drive (TanStack `deadline`);
    /// `None` when the host set no budget.
    pub fn deadline(&self) -> Option<i64> {
        self.engine.deadline
    }

    /// Ms of runtime budget left (`u64::MAX` when no deadline).
    pub fn time_remaining(&self) -> u64 {
        match self.engine.deadline {
            Some(d) => i64::saturating_sub(d, crate::engine::now_ms()).max(0) as u64,
            None => u64::MAX,
        }
    }

    /// True once the budget is nearly exhausted: `time_remaining() <
    /// min_yield_remaining_ms` (default 1000ms).
    pub fn should_yield(&self) -> bool {
        self.time_remaining() < self.engine.min_yield_remaining_ms
    }

    /// Cooperative hand-back of the runtime budget (TanStack `yield`): durably
    /// parks the run on a `"__timer"` wait until [`RunWorkflowOptions`](crate::engine::RunWorkflowOptions) 的 `yield_resume_at`
    /// (or now+1ms), so a host can re-invoke with a freshly extended deadline.
    /// Deterministic id `__yield-{n}` (per-invocation counter), replay-safe.
    pub async fn yield_(&self) -> anyhow::Result<serde_json::Value>
    where
        TState: serde::Serialize,
    {
        self.flush_state()?;
        let k = self
            .engine
            .yield_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let step_id = format!("__yield-{k}");
        let target = self
            .engine
            .yield_resume_at
            .unwrap_or_else(|| crate::engine::now_ms() + 1);
        crate::engine::exec_pause(&self.engine, &step_id, "__timer", "yield", Some(target)).await
    }

    /// [`step`](Self::step) with per-step options (retry policy, timeout,
    /// resource gate). Same typing: resolves to the
    /// closure's own `T`.
    pub async fn step_with<T, F, Fut>(
        &self,
        step_id: &str,
        opts: StepOptions,
        run: F,
    ) -> anyhow::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
        F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = anyhow::Result<T>> + Send + 'static,
        TState: serde::Serialize,
    {
        self.flush_state()?;
        crate::engine::exec_step(&self.engine, step_id, &opts, run).await
    }
}

/// Fixed | exponential | custom backoff. `attempt` is 1-based: after attempt
/// #N fails, we wait `delay_ms(N)` before attempt N+1 (exponential: base * 2^(N-1)).
#[derive(Clone)]
pub enum Backoff {
    Fixed { base_ms: u64 },
    Exponential { base_ms: u64 },
    Custom(Arc<dyn Fn(usize) -> u64 + Send + Sync>),
}

impl Backoff {
    pub fn delay_ms(&self, attempt: usize) -> u64 {
        match self {
            Backoff::Fixed { base_ms } => *base_ms,
            Backoff::Exponential { base_ms } => {
                let n = (attempt as u32).saturating_sub(1);
                base_ms.saturating_mul(1u64 << n.min(62))
            }
            Backoff::Custom(f) => f(attempt),
        }
    }
}

#[derive(Clone)]
pub struct RetryPolicy {
    /// Total attempts, including the first. 1 == no retry.
    pub max_attempts: usize,
    pub backoff: Backoff,
    /// 耐久重试（#19 `retry.durable`）：attempt/失败/下次截止时间持久化，
    /// 重启后经 sweep 续跑。false = 内存级重试（现状）。
    pub durable: bool,
}

impl RetryPolicy {
    pub fn new(max_attempts: usize, backoff: Backoff) -> Self {
        Self {
            max_attempts,
            backoff,
            durable: false,
        }
    }
}

/// Per-step configuration. `retry`/`timeout` match upstream `StepOptions`
/// (upstream passes them as inline `step(id, fn, opts)`); `retry.durable`
/// maps to upstream `StepRetryOptions.durable`. `resource` is a local
/// extension. Everything here is orthogonal to the durable log.
#[derive(Clone, Default)]
pub struct StepOptions {
    pub retry: Option<RetryPolicy>,
    pub timeout: Option<Duration>,
    /// Resource key this step contends on (e.g. `"gpu:0"`). Steps with the
    /// same key are serialized (capacity-1 gate, sibling of a mutex).
    pub resource: Option<String>,
}

impl StepOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = Some(retry);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }
}

/// Rebuilds the per-invocation state from frozen input. Mirrors TanStack's
/// `initialize({ input })`: state is NOT persisted — it's re-derived on every
/// invocation (start and resume) by re-running `initialize` + the handler.
type InitializeFn =
    Arc<dyn Fn(&serde_json::Value) -> anyhow::Result<serde_json::Value> + Send + Sync>;

/// Shape-check for the initial state (installed by `WorkflowDefinition::state_schema::<T>()`).
type StateValidatorFn = Arc<dyn Fn(&serde_json::Value) -> anyhow::Result<()> + Send + Sync>;

/// 恢复 payload 的形状校验器（对齐 TanStack `WaitForEventOptions.schema`，
/// `types.ts:276-277`）。**代码而非数据**：不落进 `SignalAwaited` checkpoint，
/// replay 时同一个 `wait_for_event_with(..)` 调用点重新装上同一个校验器。
pub type PayloadValidator = Arc<dyn Fn(&serde_json::Value) -> anyhow::Result<()> + Send + Sync>;

/// 一个具名的 payload 形状声明。由 [`PayloadSchema::of::<T>`] 构造。
///
/// 上游用 `StandardSchemaV1`（zod/valibot 通用接口）；Rust 侧没有那种运行时
/// schema 生态，最接近的既有物是 `serde` 的 `Deserialize`（和
/// `WorkflowDefinition::state_schema::<T>()` 同一个约定：`T` 就是 schema）。
#[derive(Clone)]
pub struct PayloadSchema {
    validator: PayloadValidator,
}

impl PayloadSchema {
    /// 用 `T` 作 schema：`T: Deserialize` 决定接受的 payload 形状。
    pub fn of<T: serde::de::DeserializeOwned + Send + Sync + 'static>() -> Self {
        Self {
            validator: Arc::new(|v| {
                serde_json::from_value::<T>(v.clone())?;
                Ok(())
            }),
        }
    }
}

impl std::fmt::Debug for PayloadSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PayloadSchema(<opaque>)")
    }
}

/// [`BaseCtx::wait_for_event_with`] 的选项（对齐 TanStack
/// `WaitForEventOptions`，`types.ts:268-279`）。
#[derive(Default, Clone, Debug)]
pub struct WaitForEventOptions {
    /// 绝对 UTC ms 唤醒期限。落进 `SignalAwaited.due_at` 和
    /// `RunState.waiting_for.deadline`，供 host 建时间索引的 worker job。
    pub deadline: Option<i64>,
    /// 自由元数据，落进 `SignalAwaited.meta` 与 `RunState` 的三处 await 投影。
    pub meta: Option<serde_json::Value>,
    /// 恢复时对投递 payload 的形状校验（`schema`）。校验失败 → run 报错。
    pub schema: Option<PayloadSchema>,
}

/// [`BaseCtx::approve_with`] 的选项（对齐 TanStack `ApproveOptions` +
/// `RunAwaitable` 的 approval 变体，`types.ts:307-312` / `:521-528`）。
#[derive(Default, Clone, Debug)]
pub struct ApproveOptions {
    /// 绝对 UTC ms 期限；`None` = 等人，无时间上限。
    pub deadline: Option<i64>,
    /// 自由元数据（如审批单链接、金额、影响面），落进 checkpoint + 投影。
    pub meta: Option<serde_json::Value>,
    /// 恢复时对审批决定 payload 的形状校验。
    pub schema: Option<PayloadSchema>,
}

/// A declared workflow: id, optional version, the async handler, plus the
/// builder-derived extras (`description`, `default_step_retry`, `middlewares`,
/// `previous_versions`, `output_validator`) that TanStack carries on the
/// workflow object.
///
/// Mirrors TanStack's `WorkflowDefinition<TInput, TOutput, TState>`
/// (`types.ts:442-460`) member-for-member — **including the generics**, which is
/// the whole point: upstream's erased alias (`AnyWorkflowDefinition`) and its
/// typed form are the same type, and here they are too, just spelled out.
///
/// Every type parameter is defaulted to the erased form, so a **bare
/// `WorkflowDefinition` is the type-erased workflow** — what the engine and
/// every run site see. See [`AnyWorkflowDefinition`] for the run-site spelling.
pub struct WorkflowDefinition<
    TInput = serde_json::Value,
    TOutput = serde_json::Value,
    TState = serde_json::Value,
> {
    pub id: String,
    pub version: Option<String>,
    pub description: Option<String>,
    /// Fallback retry policy for steps that declare none
    /// (TanStack `defaultStepRetry`). Per-step [`StepOptions::retry`] wins.
    pub default_step_retry: Option<RetryPolicy>,
    /// Middleware list, in the order TanStack's `composeMiddlewares` chains
    /// them (first = outermost wrapping of the typed handler).
    pub middlewares: Vec<Middleware>,
    /// Older versions of the same workflow. Resume routes by the persisted
    /// `workflow_version` to the matching entry (see
    /// [`select_workflow_version`](crate::engine::select_workflow_version)).
    pub previous_versions: Vec<AnyWorkflowDefinition>,
    pub handler: WorkflowHandler,
    pub initialize: InitializeFn,
    /// Shape-check for the run input (`config.inputSchema` counterpart,
    /// installed by `CreateWorkflowConfig::input::<T>()`). Checked by the
    /// engine **before** `initialize` and the handler, so bad input fails the
    /// run with [`RunErrorCode::Validation`] instead of surfacing later as a
    /// generic handler error.
    pub input_validator: Option<StateValidatorFn>,
    pub state_validator: Option<StateValidatorFn>,
    /// Shape-check for the handler's `TOutput` value (config `output` schema).
    pub output_validator: Option<StateValidatorFn>,
    _input: PhantomData<TInput>,
    _output: PhantomData<TOutput>,
    _state: PhantomData<TState>,
}

// `#[derive(Clone)]` would demand `TInput: Clone + TOutput: Clone + …` for what
// is only `PhantomData`. The type parameters carry no data, so `Clone` holds
// unconditionally — a definition stays cloneable whatever the authoring site
// declared.
impl<TInput, TOutput, TState> Clone for WorkflowDefinition<TInput, TOutput, TState> {
    fn clone(&self) -> Self {
        WorkflowDefinition {
            id: self.id.clone(),
            version: self.version.clone(),
            description: self.description.clone(),
            default_step_retry: self.default_step_retry.clone(),
            middlewares: self.middlewares.clone(),
            previous_versions: self.previous_versions.clone(),
            handler: Arc::clone(&self.handler),
            initialize: Arc::clone(&self.initialize),
            input_validator: self.input_validator.clone(),
            state_validator: self.state_validator.clone(),
            output_validator: self.output_validator.clone(),
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
        }
    }
}

/// The erased workflow, and the type every run site takes — TanStack's
/// `AnyWorkflowDefinition` (`types.ts:466`).
///
/// # Why this is a newtype and not an alias
///
/// Upstream that name **is** an alias: `WorkflowDefinition<any, any, any>`.
/// TypeScript's `any` is bidirectionally assignable, so a single type serves
/// both the typed declaration site and the erased run site. Rust has no such
/// assignability — `WorkflowDefinition<ChargeInput, Draft>` is a *different
/// type* from `WorkflowDefinition<Value, Value>` and neither coerces to the
/// other — so the erased form has to be a type of its own. That is this.
///
/// Conversion is explicit, lossless, and free (both directions go through an
/// `Arc`, so no deep clone):
///
/// ```
/// use aa_workflow_core::{AnyWorkflowDefinition, CreateWorkflowConfig, create_workflow};
///
/// // the erased form — what a run site takes
/// let typed = create_workflow(CreateWorkflowConfig::new("charge"))
///     .handler(|_ctx| async { Ok(()) });
/// let erased: AnyWorkflowDefinition = typed.into();
/// assert_eq!(erased.id, "charge");
/// assert_eq!(erased.version, None);
/// ```
///
/// # Why it holds an `Arc`
///
/// `WorkflowDefinition` owns `Vec`s and several `Arc`s, so a by-value clone on
/// every run would deep-copy the middleware and previous-version lists. `Arc`
/// makes clone a refcount bump and keeps the struct lifetime-parameter-free
/// (lifetimes would make the builder chain very unpleasant to write).
#[derive(Clone)]
pub struct AnyWorkflowDefinition(Arc<WorkflowDefinition>);

impl AnyWorkflowDefinition {
    /// The shared field carrier. Cloning this is a refcount bump, so a host
    /// that already holds an `Arc<AnyWorkflowDefinition>` can hand it to a run
    /// site without copying the middleware / previous-version lists.
    fn shared(&self) -> Arc<WorkflowDefinition> {
        Arc::clone(&self.0)
    }
}

impl std::ops::Deref for AnyWorkflowDefinition {
    type Target = WorkflowDefinition;

    fn deref(&self) -> &WorkflowDefinition {
        &self.0
    }
}

impl<TInput, TOutput, TState> From<WorkflowDefinition<TInput, TOutput, TState>>
    for AnyWorkflowDefinition
{
    fn from(w: WorkflowDefinition<TInput, TOutput, TState>) -> Self {
        Self(Arc::new(w.erase()))
    }
}

impl<TInput, TOutput, TState> From<Arc<WorkflowDefinition<TInput, TOutput, TState>>>
    for AnyWorkflowDefinition
{
    fn from(w: Arc<WorkflowDefinition<TInput, TOutput, TState>>) -> Self {
        // Unique refcount → move the fields out, no clone at all. Shared →
        // fall back to a clone (only PhantomData actually differs).
        let inner = Arc::try_unwrap(w).unwrap_or_else(|shared| (*shared).clone());
        Self(Arc::new(inner.erase()))
    }
}

impl From<Arc<AnyWorkflowDefinition>> for AnyWorkflowDefinition {
    fn from(w: Arc<AnyWorkflowDefinition>) -> Self {
        Self(Arc::clone(&w.0))
    }
}

impl<TInput, TOutput, TState> WorkflowDefinition<TInput, TOutput, TState> {
    /// Drop the type parameters, keeping every real field. The parameters are
    /// `PhantomData` only — the handler, validator and middleware types are
    /// already erased (`WorkflowHandler` etc.) — so this is a field move, and
    /// the only thing erased is the *authoring-site* type information.
    fn erase(self) -> WorkflowDefinition {
        WorkflowDefinition {
            id: self.id,
            version: self.version,
            description: self.description,
            default_step_retry: self.default_step_retry,
            middlewares: self.middlewares,
            previous_versions: self.previous_versions,
            handler: self.handler,
            initialize: self.initialize,
            input_validator: self.input_validator,
            state_validator: self.state_validator,
            output_validator: self.output_validator,
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
        }
    }
}

impl WorkflowDefinition {
    /// The erased workflow: no `TInput`/`TOutput`/`TState`/`TCtxExt`.
    ///
    /// **Crate-internal only**（引擎测试 / registry 内部用）：公共声明期的唯一
    /// 入口是 [`create_workflow`](super::create_workflow)(...).handler(...)——
    /// 它在声明点钉死类型参数，并要求 config（input schema / version 等）。
    /// 擦除态直造会绕过这些声明期设施，不对外提供。
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
            description: None,
            default_step_retry: None,
            middlewares: Vec::new(),
            previous_versions: Vec::new(),
            handler: Arc::new(|_ctx: WorkflowCtx| Box::pin(async { Ok(serde_json::Value::Null) })),
            initialize: Arc::new(|_| Ok(serde_json::Value::Object(Default::default()))),
            input_validator: None,
            state_validator: None,
            output_validator: None,
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
        }
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Fallback retry policy for steps that declare no
    /// [`StepOptions::retry`](StepOptions::retry).
    pub fn default_step_retry(mut self, retry: RetryPolicy) -> Self {
        self.default_step_retry = Some(retry);
        self
    }

    /// Same as
    /// [`WorkflowBuilder::previous_versions`](WorkflowBuilder::previous_versions) —
    /// the direct-construction variant for engine-facing code.
    pub fn previous_versions(
        mut self,
        v: impl IntoIterator<Item = impl Into<AnyWorkflowDefinition>>,
    ) -> Self {
        self.previous_versions = v.into_iter().map(Into::into).collect();
        self
    }

    /// Declare the initial per-invocation state, derived from the frozen run
    /// input (TanStack `initialize`). Called on every invocation — start and
    /// resume — and the handler re-runs from scratch, so mutations written
    /// *outside* step closures re-execute deterministically.
    pub fn initialize(
        mut self,
        f: impl Fn(&serde_json::Value) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static,
    ) -> Self {
        self.initialize = Arc::new(f);
        self
    }

    /// Declare a typed state (serde `Deserialize` type = schema). The initial
    /// state built by [`initialize`](Self::initialize) is shape-checked against
    /// `T` on every invocation; a mismatch errors the run (zod `.safeParse`
    /// counterpart for `stateSchema`). Typed access at runtime is the `ctx.state`
    /// field.
    pub fn state_schema<T: serde::de::DeserializeOwned + Send + Sync + 'static>(mut self) -> Self {
        self.state_validator = Some(Arc::new(|v| {
            serde_json::from_value::<T>(v.clone())?;
            Ok(())
        }));
        self
    }

    /// Installs the orchestrating closure. Branching/parallelism/order are
    /// plain async code; only durable side effects go through `ctx.step`.
    pub fn handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(WorkflowCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<serde_json::Value>> + Send + 'static,
    {
        self.handler = Arc::new(move |ctx| Box::pin(handler(ctx)));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{RunWorkflowOptions, run_workflow};
    use crate::event::RunStatus;
    use crate::middleware::{
        Middleware, MiddlewareServerFn, MiddlewareServerFnArgs, StepAction, StepEvent,
        StepInterceptor,
    };
    use crate::run_store::{InMemoryStore, RunStore};
    use serde_json::Value;
    use std::sync::{Arc, Mutex};

    // ── 两个「外部开发者」定义的扩展命名空间 ──────────────────

    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Clone)]
    pub struct UserExt {
        user: String,
    }
    pub struct UserNs;
    impl ExtNamespace for UserNs {
        const KEY: &'static str = "user";
        type Value = UserExt;
    }
    crate::impl_ext_accessors!(UserNs => UserAccess, user, try_user);
    #[allow(dead_code)]
    fn _uses_try_user(b: &ExtBag) { let _ = b.try_user(); }

    /// 单独命名的 `MiddlewareServerFn` 能直接交给 `server()`（不必再套一层闭包），
    /// 并且扩展按命名空间被 handler 读到。
    #[tokio::test]
    async fn middleware_accepts_a_named_server_fn() {
        let store = Arc::new(InMemoryStore::new());
        let ext: MiddlewareServerFn = Arc::new(|mut sc: MiddlewareServerFnArgs| {
            Box::pin(async move { sc.next(Some(DefExt::<UserNs>::new(UserExt { user: "bob".into() }).into_value())).await })
        });
        let wf = create_workflow(CreateWorkflowConfig::new("mw-fn").input::<Value>())
            .middleware([Middleware { server: ext }])
            .handler(|ctx: WorkflowCtx| async move {
                Ok(serde_json::json!({ "user": ctx.ext.user().user }))
            });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store).input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "user": "bob" })));
    }

    /// `next(ext)` 写入的扩展被 handler 读到 —— 对齐 TS 的 `next({ context })`。
    /// 没写扩展时 `ctx.ext` 是空袋（`is_empty()`），读取会 panic（与 TS 里
    /// 「声明了 ctx.user 却没挂中间件」一致）。
    #[tokio::test]
    async fn middleware_next_ext_is_readable_by_handler() {
        let store = Arc::new(InMemoryStore::new());
        let wf = create_workflow(CreateWorkflowConfig::new("mw-ext").input::<Value>())
            .middleware([Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
                Box::pin(async move {
                    sc.next(Some(
                        DefExt::<UserNs>::new(UserExt { user: "alice".into() }).into_value(),
                    ))
                    .await
                })
            })])
            .handler(|ctx: WorkflowCtx| async move {
                // 第二个中间件不写扩展 → 空袋
                Ok(serde_json::json!({ "user": ctx.ext.user().user }))
            });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store).input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "user": "alice" })));
    }

    /// 多个 server 的 `next(ext)` 全部合并、后者覆盖同名键 —— 对齐上游
    /// composeMiddlewares 的 Object.assign 累加。
    #[tokio::test]
    async fn middleware_next_ext_merges_in_registration_order() {
        #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Clone)]
        struct Bundle {
            user: String,
            trace: String,
            shared: String,
        }
        struct BundleNs;
        impl ExtNamespace for BundleNs {
            const KEY: &'static str = "bundle";
            type Value = Bundle;
        }

        let m_user = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
            Box::pin(async move {
                sc.next(Some(
                    DefExt::<BundleNs>::new(Bundle {
                        user: "alice".into(),
                        trace: String::new(),
                        shared: "from-user".into(),
                    })
                    .into_value(),
                ))
                .await
            })
        });
        let m_trace = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
            Box::pin(async move {
                sc.next(Some(
                    DefExt::<BundleNs>::new(Bundle {
                        user: String::new(),
                        trace: "t-1".into(),
                        shared: "from-trace".into(),
                    })
                    .into_value(),
                ))
                .await
            })
        });
        let wf = create_workflow(CreateWorkflowConfig::new("mw-merge").input::<Value>())
            .middleware([m_user, m_trace])
            .handler(|ctx: WorkflowCtx| async move {
                let b = ctx.ext.get::<BundleNs>();
                Ok(serde_json::json!({ "shared": b.shared }))
            });
        let store = Arc::new(InMemoryStore::new());
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store).input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(
            out.output,
            Some(serde_json::json!({ "shared": "from-trace" })),
            "后注册的 next(ext) 覆盖同名键"
        );
    }

    /// server 洋葱包裹按注册序最外层：before-in → before-out → handler →
    /// after-out → after-in（TanStack `composeMiddlewares` 同义）。
    #[tokio::test]
    async fn middleware_server_composes_in_registration_order() {
        fn make_server(name: &'static str, order: &Arc<Mutex<Vec<String>>>) -> Middleware {
            let order = order.clone();
            Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
                let order = order.clone();
                Box::pin(async move {
                    order.lock().unwrap().push(format!("before-{name}"));
                    let r = sc.next(None).await;
                    order.lock().unwrap().push(format!("after-{name}"));
                    r
                })
            })
        }

        let order = Arc::new(Mutex::new(Vec::new()));
        let m_in = make_server("in", &order);
        let m_out = make_server("out", &order);
        let wf = create_workflow(CreateWorkflowConfig::new("mw-order").input::<Value>())
            .middleware([m_in, m_out])
            .handler(|ctx: BaseCtx<Value>| async move {
                ctx.step("inner", |_sc: StepCtx| async move {
                    Ok(serde_json::json!({ "ok": true }))
                })
                .await
            });
        let store = Arc::new(InMemoryStore::new());
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store).input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(
            *order.lock().unwrap(),
            vec![
                "before-in".to_string(),
                "before-out".to_string(),
                "after-out".to_string(),
                "after-in".to_string(),
            ],
            "先注册的 middleware 在最外"
        );
    }

    /// **step 拦截器**（本仓有意扩展）：server 里 `set_step_interceptor`，
    /// 对真实执行的 step 作出 Skip/Park/透传决定。
    #[tokio::test]
    async fn step_interceptor_can_skip_and_park() {
        let store = Arc::new(InMemoryStore::new());
        let ran = Arc::new(Mutex::new(Vec::new()));
        let ran_mw = ran.clone();

        let wf = create_workflow(CreateWorkflowConfig::new("mw-interceptor").input::<Value>())
            .middleware([Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
                let intercept: StepInterceptor = Arc::new(|ev: &StepEvent<'_>| {
                    match ev.step_id {
                        "b" => Some(StepAction::Skip {
                            result: serde_json::json!("B-CACHED"),
                        }),
                        "c" => Some(StepAction::Park {
                            signal_name: "__test-park".into(),
                            meta: None,
                        }),
                        _ => None,
                    }
                });
                sc.set_step_interceptor(intercept);
                Box::pin(async move { sc.next(None).await })
            })])
            .handler({
                let ran = ran_mw.clone();
                move |ctx: WorkflowCtx| {
                    let ran = ran.clone();
                    async move {
                        for id in ["a", "b", "c", "d"] {
                            let ran = ran.clone();
                            let sid = id.to_string();
                            ctx.step(id, move |_sc: StepCtx| {
                                let ran = ran.clone();
                                let sid = sid.clone();
                                async move {
                                    ran.lock().unwrap().push(sid.clone());
                                    Ok(serde_json::json!(sid))
                                }
                            })
                            .await?;
                        }
                        Ok(serde_json::json!({ "ok": true }))
                    }
                }
            });

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store.clone())
                .input(serde_json::json!({}))
                .run_id("mi:1"),
        )
        .outcome()
        .await
        .unwrap();

        assert_eq!(
            ran.lock().unwrap().clone(),
            vec!["a".to_string()],
            "b skip 不执行；c 在入口 Park，c/d 都不执行"
        );
        assert_ne!(format!("{:?}", out.status), "Finished");

        let events = store.get_events("mi:1").unwrap();
        assert!(
            events.iter().any(|e| matches!(
                e,
                crate::event::WorkflowEvent::StepFinished { step_id, result: Some(r), .. }
                    if step_id == "b" && r == &serde_json::json!("B-CACHED")
            )),
            "Skip 的结果应作为 b 的 checkpoint 落盘"
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                crate::event::WorkflowEvent::SignalAwaited { name, .. } if name == "__test-park"
            )),
            "Park 应落一条 __test-park 的 SignalAwaited"
        );
    }

    /// `next` 对齐上游 `next(opts)`：第二次调用报错（上游用 `advanced` 标志，
    /// 这里用 `Option::take`），见 `run-workflow.ts:1502-1513`。
    #[tokio::test]
    async fn middleware_next_is_at_most_once() {
        let store = Arc::new(InMemoryStore::new());
        let wf = create_workflow(CreateWorkflowConfig::new("mw-next-once").input::<Value>())
            .middleware([Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
                Box::pin(async move {
                    let first = sc.next(None).await?;
                    assert!(sc.next(None).await.is_err(), "第二次 next 必须报错");
                    Ok(first)
                })
            })])
            .handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::json!({ "ok": true })) });

        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), store).input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
    }

    /// 对齐上游 `ReadonlyArray<AnyMiddleware>`：不同来源的中间件放进同一个数组，
    /// 各自的扩展**互不干扰**（不需要合并类型）。
    #[tokio::test]
    async fn any_middleware_array_mixes_distinct_sources() {
        #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Clone)]
        struct TraceExt {
            trace: String,
        }
        struct TraceNs;
        impl ExtNamespace for TraceNs {
            const KEY: &'static str = "trace";
            type Value = TraceExt;
        }

        let user_mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
            Box::pin(async move {
                sc.next(Some(
                    DefExt::<UserNs>::new(UserExt { user: "alice".into() }).into_value(),
                ))
                .await
            })
        });
        let trace_mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
            Box::pin(async move {
                sc.next(Some(
                    DefExt::<TraceNs>::new(TraceExt { trace: "t-1".into() }).into_value(),
                ))
                .await
            })
        });

        let wf = create_workflow(CreateWorkflowConfig::new("mw-mixed").input::<Value>())
            .middleware(vec![user_mw, trace_mw])
            .handler(|ctx: WorkflowCtx| async move {
                Ok(serde_json::json!({
                    "user": ctx.ext.user().user,
                    "trace": ctx.ext.get::<TraceNs>().trace,
                }))
            });
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new()))
                .input(serde_json::json!({})),
        )
        .outcome()
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(
            out.output,
            Some(serde_json::json!({ "user": "alice", "trace": "t-1" }))
        );
    }
}
