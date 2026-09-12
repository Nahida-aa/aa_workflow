use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::engine::EngineRuntime;
use crate::event::{RunEvent, StepState};

/// Boxed async step-returning future. Steps are spawned inside the engine's
/// driver; the async world is the default (mirrors `Promise.all` in JS).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The workflow's orchestrating closure: plain async control flow over
/// `ctx.step` / `try_join!`. Decisions (branching, which steps run) are made
/// by this code — the graph is an emergent, per-run artifact, never declared
/// up front.
pub type WorkflowHandler =
    Arc<dyn Fn(WorkflowCtx) -> BoxFuture<'static, anyhow::Result<serde_json::Value>> + Send + Sync>;

/// Per-invocation view handed step closures. Mirrors TanStack's `StepContext`.
#[derive(Clone)]
pub struct StepCtx {
    pub(crate) inner: Arc<EngineRuntime>,
    pub step_id: String,
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
        self.inner.publish_progress(&self.step_id, value);
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
pub struct BaseCtx<In = serde_json::Value, St = serde_json::Value> {
    /// `runId: string`
    pub run_id: String,
    /// `input: TInput` — frozen run input (typed or `Value`)
    pub input: In,
    /// `state: TState` — working state copy, flushed at durable boundaries
    pub state: St,
    /// Engine-side runtime, mirror of TanStack's closure-captured `engine`.
    /// Not part of the public API.
    #[doc(hidden)]
    pub(crate) engine: Arc<EngineRuntime>,
}

/// Default (untyped) ctx: `BaseCtx<Value, Value>`.
pub type WorkflowCtx = BaseCtx<serde_json::Value, serde_json::Value>;

/// Typed-input ctx: `BaseCtx<In, Value>`. For a typed `state: St` as well,
/// write `BaseCtx<In, St>` directly (via [`TypedWorkflowBuilder::state_schema`]).
pub type TypedCtx<In> = BaseCtx<In, serde_json::Value>;

impl BaseCtx<serde_json::Value, serde_json::Value> {
    /// Engine-facing drive-start construction: freeze the run input and
    /// snapshot the current state image.
    pub(crate) fn untyped(engine: Arc<EngineRuntime>) -> Self {
        let state = engine
            .state
            .read()
            .expect("workflow state lock poisoned")
            .clone();
        Self {
            run_id: engine.run_id.clone(),
            input: engine.input.clone(),
            state,
            engine,
        }
    }
}

impl<In, St> BaseCtx<In, St> {
    /// Write [`Self::state`] back into the engine's live image. Called before
    /// every durable primitive; the image is what pause snapshots and the
    /// next drive read. Cheap for the common `Value`/small-struct case.
    pub(crate) fn flush_state(&self) -> anyhow::Result<()>
    where
        St: serde::Serialize,
    {
        let v = serde_json::to_value(&self.state)?;
        *self.engine.state.write().expect("workflow state lock poisoned") = v;
        Ok(())
    }

    /// Runs `run` durably under `step_id`. On replay (resume) a previously
    /// succeeded step short-circuits to its cached result *without* calling
    /// `run` again; a previously failed step rethrows the stored error.
    ///
    /// The step closure is async so that concurrent steps compose via
    /// `tokio::try_join!` — parallel durable execution, no custom primitive.
    pub async fn step<F, Fut>(
        &self,
        step_id: &str,
        run: F,
    ) -> anyhow::Result<serde_json::Value>
    where
        F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = anyhow::Result<serde_json::Value>> + Send + 'static,
        St: serde::Serialize,
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
        St: serde::Serialize,
    {
        self.flush_state()?;
        crate::engine::exec_pause(&self.engine, &key.into(), "__approval", reason.as_ref(), None).await
    }

    /// Durable sleep: pauses the run until `dur` elapses. `key` is the
    /// deterministic pause identity; the engine auto-delivers the resume via
    /// [`signal_run`](crate::engine::signal_run) when the timer fires. On replay
    /// a previously delivered resume short-circuits immediately.
    pub async fn sleep(
        &self,
        key: impl Into<String>,
        dur: std::time::Duration,
    ) -> anyhow::Result<serde_json::Value>
    where
        St: serde::Serialize,
    {
        self.flush_state()?;
        crate::engine::exec_pause(&self.engine, &key.into(), "__timer", "sleep", Some(dur)).await
    }

    /// Durable absolute-time wait: pauses until wall-clock `ts_ms` (equivalent
    /// to TanStack's `sleepUntil`). A timestamp in the past resolves
    /// immediately. `key` is the deterministic pause identity.
    pub async fn sleep_until(
        &self,
        key: impl Into<String>,
        ts_ms: i64,
    ) -> anyhow::Result<serde_json::Value>
    where
        St: serde::Serialize,
    {
        let rem =
            Duration::from_millis(i64::saturating_sub(ts_ms, crate::engine::now_ms()).max(0) as u64);
        self.sleep(key, rem).await
    }

    /// Durable named wait: pauses the run until [`signal_event`](crate::engine::signal_event)
    /// delivers a payload for `event_name`. `key` is the deterministic pause
    /// identity (must not collide with step ids); `event_name` is the channel
    /// a host signals on (`run.json` `waiting_for.signal_name`). On replay a
    /// previously delivered resume short-circuits from the log.
    pub async fn wait_for_event(
        &self,
        key: impl Into<String>,
        event_name: impl AsRef<str>,
    ) -> anyhow::Result<serde_json::Value>
    where
        St: serde::Serialize,
    {
        let name = event_name.as_ref();
        self.flush_state()?;
        crate::engine::exec_pause(&self.engine, &key.into(), name, "event", None).await
    }

    /// Emit an observability event to the publisher. Never appended to the
    /// log, so it is outside replay — `fold_step_states` and resume ignore it
    /// (mirrors TanStack's `emit` / `CUSTOM`).
    pub fn emit(&self, name: impl AsRef<str>, value: serde_json::Value) {
        self.engine.publish(&RunEvent::Custom {
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
    /// parks the run on a `"__timer"` wait until [`RunOptions::yield_resume_at`]
    /// (or now+1ms), so a host can re-invoke with a freshly extended deadline.
    /// Deterministic id `__yield-{n}` (per-invocation counter), replay-safe.
    pub async fn yield_(&self) -> anyhow::Result<serde_json::Value>
    where
        St: serde::Serialize,
    {
        self.flush_state()?;
        let k = self.engine.yield_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let step_id = format!("__yield-{k}");
        let target = self
            .engine
            .yield_resume_at
            .unwrap_or_else(|| crate::engine::now_ms() + 1);
        let dur = std::time::Duration::from_millis(
            i64::saturating_sub(target, crate::engine::now_ms()).max(0) as u64,
        );
        crate::engine::exec_pause(&self.engine, &step_id, "__timer", "yield", Some(dur)).await
    }

    /// [`step`](Self::step) with per-step options (retry policy, timeout,
    /// resource gate, `up_to_date` make-check).
    pub async fn step_with<F, Fut>(
        &self,
        step_id: &str,
        opts: StepOptions,
        run: F,
    ) -> anyhow::Result<serde_json::Value>
    where
        F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = anyhow::Result<serde_json::Value>> + Send + 'static,
        St: serde::Serialize,
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
        Self { max_attempts, backoff }
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

/// Shape-check for the initial state (installed by `Workflow::state_schema::<T>()`).
type StateValidatorFn = Arc<dyn Fn(&serde_json::Value) -> anyhow::Result<()> + Send + Sync>;

/// A declared workflow: just an id, optional version, and the async handler.
#[derive(Clone)]
pub struct Workflow {
    pub id: String,
    pub version: Option<String>,
    pub handler: WorkflowHandler,
    pub initialize: InitializeFn,
    pub state_validator: Option<StateValidatorFn>,
}

impl Workflow {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
            handler: Arc::new(|_ctx: WorkflowCtx| Box::pin(async { Ok(serde_json::Value::Null) })),
            initialize: Arc::new(|_| Ok(serde_json::Value::Object(Default::default()))),
            state_validator: None,
        }
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
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

    /// Declare a typed input (serde `Deserialize` type = schema). Missing or
    /// mistyped fields fail the run on its first resume with a field-path
    /// error — the Rust counterpart of zod's `inputSchema` `.safeParse`.
    /// Returns a [`TypedWorkflowBuilder`] whose handler receives
    /// [`TypedCtx<In>`] with `ctx.input` already deserialized.
    pub fn input_schema<In>(self) -> TypedWorkflowBuilder<In, serde_json::Value>
    where
        In: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        TypedWorkflowBuilder {
            id: self.id,
            version: self.version,
            initialize: self.initialize,
            state_validator: self.state_validator,
            parse: Arc::new(|v| serde_json::from_value(v.clone()).map_err(anyhow::Error::from)),
            _state: PhantomData,
        }
    }
}

/// Parses frozen run input into the workflow's typed input.
type InputParser<In> = Arc<dyn Fn(&serde_json::Value) -> anyhow::Result<In> + Send + Sync>;

/// Chainable builder returned by [`Workflow::input_schema`]. `In` is the typed
/// run input (`ctx.input`), `St` the typed state (`ctx.state`) — both mirrored
/// on the [`BaseCtx<In, St>`](BaseCtx) the handler receives. The handler's
/// output type `Out` is inferred from the closure's return value — no output
/// schema declaration needed, mirroring TanStack's handler return type
/// inference (their `output` schema only *constrains*, it never declares).
pub struct TypedWorkflowBuilder<In, St = serde_json::Value> {
    id: String,
    version: Option<String>,
    initialize: InitializeFn,
    state_validator: Option<StateValidatorFn>,
    parse: InputParser<In>,
    _state: PhantomData<St>,
}

impl<In, St> TypedWorkflowBuilder<In, St>
where
    In: serde::de::DeserializeOwned + Send + Sync + 'static,
{
    /// Replace the default serde parse with a custom one (e.g.
    /// `serde_path_to_error` for friendlier field-path messages).
    pub fn with_parser(
        mut self,
        parse: impl Fn(&serde_json::Value) -> anyhow::Result<In> + Send + Sync + 'static,
    ) -> Self {
        self.parse = Arc::new(parse);
        self
    }

    /// Declare the initial per-invocation state (see [`Workflow::initialize`]).
    pub fn initialize(
        mut self,
        f: impl Fn(&serde_json::Value) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static,
    ) -> Self {
        self.initialize = Arc::new(f);
        self
    }

    /// Declare a typed state: re-types the builder so the handler's ctx is
    /// `BaseCtx<In, NewSt>` and `ctx.state` is `NewSt`. The built state is
    /// shape-checked against `NewSt` on every invocation (zod `.safeParse`
    /// counterpart for `stateSchema`).
    pub fn state_schema<NewSt>(self) -> TypedWorkflowBuilder<In, NewSt>
    where
        NewSt: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        TypedWorkflowBuilder {
            id: self.id,
            version: self.version,
            initialize: self.initialize,
            state_validator: Some(Arc::new(|v| {
                serde_json::from_value::<NewSt>(v.clone())?;
                Ok(())
            })),
            parse: self.parse,
            _state: PhantomData,
        }
    }

    /// Finalize with a typed handler. `Out` is inferred from the return value;
    /// the engine stores it serialized as JSON, so on resume the handler
    /// re-runs from scratch with the re-parsed input and state (same as
    /// TanStack).
    pub fn handler<F, Fut, Out>(self, handler: F) -> TypedWorkflow<In, Out>
    where
        Out: serde::Serialize + Send + Sync + 'static,
        St: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static,
        F: Fn(BaseCtx<In, St>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<Out>> + Send + 'static,
    {
        let TypedWorkflowBuilder {
            id,
            version,
            initialize,
            state_validator,
            parse,
            _state,
        } = self;
        let parse = Arc::new(parse);
        let handler = Arc::new(handler);
        let engine_handler: WorkflowHandler = Arc::new(move |ctx: WorkflowCtx| {
            let parse = Arc::clone(&parse);
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let input = parse(&ctx.input)?;
                let state: St = serde_json::from_value(ctx.state.clone())?;
                let typed = BaseCtx {
                    run_id: ctx.run_id,
                    input,
                    state,
                    engine: ctx.engine.clone(),
                };
                let out = handler(typed).await?;
                Ok(serde_json::to_value(out)?)
            })
        });
        TypedWorkflow {
            base: Workflow {
                id,
                version,
                handler: engine_handler,
                initialize,
                state_validator,
            },
            _input: PhantomData,
            _output: PhantomData,
        }
    }
}

/// A workflow whose `In`/`Out` are statically known at the declaration site.
/// `Deref<Target = Workflow>` lets it be handed to
/// [`crate::engine::run_workflow`] / the registry directly; explicit erasure is
/// [`into_workflow`](Self::into_workflow).
#[derive(Clone)]
pub struct TypedWorkflow<In, Out> {
    base: Workflow,
    _input: PhantomData<In>,
    _output: PhantomData<Out>,
}

impl<In, Out> TypedWorkflow<In, Out> {
    /// Type-erase back to the engine's [`Workflow`] view.
    pub fn into_workflow(self) -> Workflow {
        self.base
    }
}

impl<In, Out> std::ops::Deref for TypedWorkflow<In, Out> {
    type Target = Workflow;

    fn deref(&self) -> &Workflow {
        &self.base
    }
}