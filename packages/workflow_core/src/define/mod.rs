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
use crate::event::{StepState, WorkflowEvent};
use crate::middleware::Middleware;

mod define_workflow;
mod state_handle;
pub use define_workflow::{CreateWorkflowConfig, WorkflowBuilder, create_workflow};
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
    /// Current attempt number (1-indexed). 0 only for internal probes (e.g.
    /// the `up_to_date` make-check).
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
pub struct BaseCtx<TInput = serde_json::Value, TState = serde_json::Value, TCtxExt = ()> {
    /// `runId: string`
    pub run_id: String,
    /// `input: TInput` — frozen run input (typed or `Value`)
    pub input: TInput,
    /// `state: TState` — 共享可变的 typed state（对齐 TS 的
    /// `ctx.state === engine.state`）；见 `state_handle` 模块文档。
    pub state: StateHandle<TState>,
    /// `TExtensions` — ctx extension bundle, the `{...context}` accumulated by
    /// middleware. `()` (the default) when no middleware declares one. Built by
    /// the middleware's `produce` and re-deserialized on every drive, so it is
    /// deterministic across resume.
    pub ext: TCtxExt,
    /// Engine-side runtime, mirror of TanStack's closure-captured `engine`.
    /// Not part of the public API.
    #[doc(hidden)]
    pub(crate) engine: Arc<EngineRuntime>,
}

/// The full ctx type. Generic `TCtxExt` matches TS's `Ctx<TInput, TState, TCtxExt>`,
/// where `TCtxExt` defaults to `{}` (our `()`).
pub type Ctx<TInput = serde_json::Value, TState = serde_json::Value, TCtxExt = ()> =
    BaseCtx<TInput, TState, TCtxExt>;

/// Default (untyped) ctx: `Ctx<Value, Value, ()>`.
pub type WorkflowCtx = Ctx<serde_json::Value, serde_json::Value, ()>;

impl BaseCtx<serde_json::Value, serde_json::Value, ()> {
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
            state,
            ext: (),
            engine,
        }
    }
}

impl<TInput, TState, TCtxExt> BaseCtx<TInput, TState, TCtxExt> {
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
    /// delivers a decision for `key`. `reason` is persisted in the `StepPaused`
    /// checkpoint for approvers. Resolves to the signal's payload on success;
    /// on replay the already-recorded `StepResume` is served from the log.
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
    /// resource gate, `up_to_date` make-check). Same typing: resolves to the
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
}

impl RetryPolicy {
    pub fn new(max_attempts: usize, backoff: Backoff) -> Self {
        Self {
            max_attempts,
            backoff,
        }
    }
}

/// Per-step configuration. `retry`/`timeout`/`resource`/`up_to_date` are
/// per-call-site options (v2), whereas TanStack passes them as inline
/// `step(id, fn, opts)`. Everything here is orthogonal to the durable log.
#[derive(Clone, Default)]
pub struct StepOptions {
    pub label: Option<String>,
    pub retry: Option<RetryPolicy>,
    pub timeout: Option<Duration>,
    /// Resource key this step contends on (e.g. `"gpu:0"`). Steps with the
    /// same key are serialized (capacity-1 gate, sibling of a mutex).
    pub resource: Option<String>,
    /// Make-style freshness check against the *derived* step state. `Ok(true)`
    /// while a successful checkpoint exists ⇒ the step short-circuits; if it
    /// reports stale a step still re-executes on resume. Defaults to the
    /// durable checkpoint only.
    pub up_to_date: Option<Arc<dyn Fn(&StepCtx, &StepState) -> bool + Send + Sync>>,
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

    pub fn up_to_date(
        mut self,
        up_to_date: impl Fn(&StepCtx, &StepState) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.up_to_date = Some(Arc::new(up_to_date));
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
/// `types.ts:276-277`）。**代码而非数据**：不落进 `StepPaused` checkpoint，
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
    /// 绝对 UTC ms 唤醒期限。落进 `StepPaused.due_at` 和
    /// `RunState.waiting_for.deadline`，供 host 建时间索引的 worker job。
    pub deadline: Option<i64>,
    /// 自由元数据，落进 `StepPaused.meta` 与 `RunState` 的三处 await 投影。
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
    TCtxExt = (),
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
    pub state_validator: Option<StateValidatorFn>,
    /// Shape-check for the handler's `TOutput` value (config `output` schema).
    pub output_validator: Option<StateValidatorFn>,
    _input: PhantomData<TInput>,
    _output: PhantomData<TOutput>,
    _state: PhantomData<TState>,
    _ext: PhantomData<TCtxExt>,
}

// `#[derive(Clone)]` would demand `TInput: Clone + TOutput: Clone + …` for what
// is only `PhantomData`. The type parameters carry no data, so `Clone` holds
// unconditionally — a definition stays cloneable whatever the authoring site
// declared.
impl<TInput, TOutput, TState, TCtxExt> Clone
    for WorkflowDefinition<TInput, TOutput, TState, TCtxExt>
{
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
            state_validator: self.state_validator.clone(),
            output_validator: self.output_validator.clone(),
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
            _ext: PhantomData,
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
/// use aa_workflow_core::{AnyWorkflowDefinition, CreateWorkflowConfig, WorkflowDefinition, create_workflow};
///
/// // the erased form — what a run site takes
/// let erased: AnyWorkflowDefinition = WorkflowDefinition::new("charge").into();
/// assert_eq!(erased.id, "charge");
///
/// // a typed declaration site erases just as cheaply
/// let typed = create_workflow(CreateWorkflowConfig::new("charge"))
///     .handler(|_ctx| async { Ok(()) });
/// let erased: AnyWorkflowDefinition = typed.into();
/// assert_eq!(erased.id, "charge");
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

impl<TInput, TOutput, TState, TCtxExt> From<WorkflowDefinition<TInput, TOutput, TState, TCtxExt>>
    for AnyWorkflowDefinition
{
    fn from(w: WorkflowDefinition<TInput, TOutput, TState, TCtxExt>) -> Self {
        Self(Arc::new(w.erase()))
    }
}

impl<TInput, TOutput, TState, TCtxExt> From<Arc<WorkflowDefinition<TInput, TOutput, TState, TCtxExt>>>
    for AnyWorkflowDefinition
{
    fn from(w: Arc<WorkflowDefinition<TInput, TOutput, TState, TCtxExt>>) -> Self {
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

impl<TInput, TOutput, TState, TCtxExt> WorkflowDefinition<TInput, TOutput, TState, TCtxExt> {
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
            state_validator: self.state_validator,
            output_validator: self.output_validator,
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
            _ext: PhantomData,
        }
    }
}

impl WorkflowDefinition {
    /// The erased workflow: no `TInput`/`TOutput`/`TState`/`TCtxExt`. This is
    /// the engine-facing constructor (it replaces the old standalone `Workflow`
    /// type); a *typed* definition comes from
    /// [`create_workflow`](super::create_workflow)(...).handler(...), which
    /// pins the parameters from the declaration site.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
            description: None,
            default_step_retry: None,
            middlewares: Vec::new(),
            previous_versions: Vec::new(),
            handler: Arc::new(|_ctx: WorkflowCtx| Box::pin(async { Ok(serde_json::Value::Null) })),
            initialize: Arc::new(|_| Ok(serde_json::Value::Object(Default::default()))),
            state_validator: None,
            output_validator: None,
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
            _ext: PhantomData,
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
    use crate::run_store::InMemoryStore;
    use std::sync::{Arc, Mutex};

    #[derive(serde::Deserialize, serde::Serialize, Default, Debug, PartialEq, Eq)]
    struct UserExt {
        user: String,
    }

    /// `middleware.produce` 的输出 deserialize 进 handler 的 `ctx.ext`
    /// （`WorkflowBuilder::middleware::<PExt>` 决定 `TCtxExt` 类型）；无 produce 时
    /// 用 [`Default`]。对齐 TanStack `defineMiddleware` 的 context 注入。
    #[tokio::test]
    async fn middleware_produce_builds_typed_ext() {
        let store = Arc::new(InMemoryStore::new());
        let wf = create_workflow(CreateWorkflowConfig::new("mw-ext").input::<serde_json::Value>())
            .middleware::<UserExt>(
                Middleware::new()
                    .produce(|_ctx| Ok(serde_json::json!({ "user": "alice", "ignored": true }))),
            )
            .handler(
                |ctx: BaseCtx<serde_json::Value, serde_json::Value, UserExt>| async move {
                    Ok(serde_json::json!({ "user": ctx.ext.user }))
                },
            );
        let out = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(
            out.output,
            Some(serde_json::json!({ "user": "alice" })),
            "middleware produce 注入 ctx.ext"
        );

        // 无 produce → TCtxExt 取 Default()。
        let store2 = Arc::new(InMemoryStore::new());
        let wf2 = create_workflow(
            CreateWorkflowConfig::new("mw-ext-default").input::<serde_json::Value>(),
        )
        .middleware::<UserExt>(Middleware::new())
        .handler(
            |ctx: BaseCtx<serde_json::Value, serde_json::Value, UserExt>| async move {
                Ok(serde_json::json!({ "user": ctx.ext.user }))
            },
        );
        let out2 = run_workflow(
            RunWorkflowOptions::new(Arc::new(wf2.clone()), store2)
                .input(serde_json::json!({})),
        )
        .outcome().await
        .unwrap();
        assert_eq!(out2.output, Some(serde_json::json!({ "user": "" })));
    }

    /// `wrap` 按注册序最外层包裹（TanStack `composeMiddlewares` 语义：先注册的
    /// 在最外）：before-in → before-out → handler → after-out → after-in。
    #[tokio::test]
    async fn middleware_wrap_composes_in_registration_order() {
        fn make_wrap(
            name: &'static str,
            order: &Arc<Mutex<Vec<String>>>,
        ) -> impl Fn(
            WorkflowCtx,
            BoxFuture<'static, anyhow::Result<serde_json::Value>>,
        ) -> BoxFuture<'static, anyhow::Result<serde_json::Value>>
        + Send
        + Sync
        + 'static {
            let order = order.clone();
            move |_ctx, next| {
                let order = order.clone();
                let name = name;
                Box::pin(async move {
                    order.lock().unwrap().push(format!("before-{name}"));
                    let r = next.await;
                    order.lock().unwrap().push(format!("after-{name}"));
                    r
                })
            }
        }

        let order = Arc::new(Mutex::new(Vec::new()));
        let m_in = Middleware::new().wrap(make_wrap("in", &order));
        let m_out = Middleware::new().wrap(make_wrap("out", &order));
        let wf =
            create_workflow(CreateWorkflowConfig::new("mw-order").input::<serde_json::Value>())
                .middleware::<()>(m_in)
                .middleware::<()>(m_out)
                .handler(|ctx: BaseCtx<serde_json::Value>| async move {
                    ctx.step("inner", |_sc: StepCtx| async move {
                        Ok(serde_json::json!({ "ok": true }))
                    })
                    .await
                });
        let store = Arc::new(InMemoryStore::new());
        let out = run_workflow(
            RunWorkflowOptions::new(wf.clone(), store)
                .input(serde_json::json!({})),
        )
        .outcome().await
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
}
