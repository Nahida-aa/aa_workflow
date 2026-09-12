use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::engine::DrvInner;
use crate::event::StepState;

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
    pub(crate) inner: Arc<DrvInner>,
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
}

/// Anything the handler needs to run steps durably. Held for the whole
/// handler execution; steps read their own context through [`StepCtx`].
#[derive(Clone)]
pub struct WorkflowCtx {
    pub(crate) inner: Arc<DrvInner>,
}

impl WorkflowCtx {
    pub fn run_id(&self) -> &str {
        &self.inner.run_id
    }

    /// Returns the frozen run input.
    pub fn input(&self) -> &serde_json::Value {
        &self.inner.input
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
    ) -> anyhow::Result<serde_json::Value> {
        crate::engine::exec_pause(&self.inner, &key.into(), reason.as_ref(), None).await
    }

    /// Durable sleep: pauses the run until `dur` elapses. `key` is the
    /// deterministic pause identity; the engine auto-delivers the resume via
    /// [`signal_run`](crate::engine::signal_run) when the timer fires. On replay
    /// a previously delivered resume short-circuits immediately.
    pub async fn sleep(
        &self,
        key: impl Into<String>,
        dur: std::time::Duration,
    ) -> anyhow::Result<serde_json::Value> {
        crate::engine::exec_pause(&self.inner, &key.into(), "sleep", Some(dur)).await
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
    {
        crate::engine::exec_step(&self.inner, step_id, &opts, run).await
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

/// A declared workflow: just an id, optional version, and the async handler.
#[derive(Clone)]
pub struct Workflow {
    pub id: String,
    pub version: Option<String>,
    pub handler: WorkflowHandler,
}

impl Workflow {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
            handler: Arc::new(|_ctx: WorkflowCtx| Box::pin(async { Ok(serde_json::Value::Null) })),
        }
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
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
    /// [`TypedCtx`] with `ctx.input()` already deserialized.
    pub fn input_schema<In>(self) -> TypedWorkflowBuilder<In>
    where
        In: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        TypedWorkflowBuilder {
            id: self.id,
            version: self.version,
            parse: Arc::new(|v| serde_json::from_value(v.clone()).map_err(anyhow::Error::from)),
        }
    }
}

/// Parses frozen run input into the workflow's typed input.
type InputParser<In> = Arc<dyn Fn(&serde_json::Value) -> anyhow::Result<In> + Send + Sync>;

/// Typed view handed to a typed handler. Re-parses the frozen run input on
/// every resume (deterministic: input JSON never mutates); the parsed value is
/// cached in an `Arc` so the handler and closures it spawns share it.
#[derive(Clone)]
pub struct TypedCtx<In> {
    inner: WorkflowCtx,
    input: Arc<In>,
}

impl<In> TypedCtx<In> {
    pub fn run_id(&self) -> &str {
        self.inner.run_id()
    }

    /// The typed, validated run input.
    pub fn input(&self) -> &In {
        &self.input
    }

    pub async fn step<F, Fut>(
        &self,
        step_id: &str,
        run: F,
    ) -> anyhow::Result<serde_json::Value>
    where
        F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = anyhow::Result<serde_json::Value>> + Send + 'static,
    {
        self.inner.step(step_id, run).await
    }

    pub async fn step_with<F, Fut>(
        &self,
        step_id: &str,
        opts: StepOptions,
        run: F,
    ) -> anyhow::Result<serde_json::Value>
    where
        F: FnOnce(StepCtx) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = anyhow::Result<serde_json::Value>> + Send + 'static,
    {
        self.inner.step_with(step_id, opts, run).await
    }

    pub async fn approve(
        &self,
        key: impl Into<String>,
        reason: impl AsRef<str>,
    ) -> anyhow::Result<serde_json::Value> {
        self.inner.approve(key, reason).await
    }

    pub async fn sleep(
        &self,
        key: impl Into<String>,
        dur: std::time::Duration,
    ) -> anyhow::Result<serde_json::Value> {
        self.inner.sleep(key, dur).await
    }
}

/// Chainable builder returned by [`Workflow::input_schema`]. The handler's
/// output type `Out` is inferred from the closure's return value — no output
/// schema declaration needed, mirroring TanStack's handler return type
/// inference (their `output` schema only *constrains*, it never declares).
pub struct TypedWorkflowBuilder<In> {
    id: String,
    version: Option<String>,
    parse: InputParser<In>,
}

impl<In: Send + Sync + 'static> TypedWorkflowBuilder<In> {
    /// Replace the default serde parse with a custom one (e.g.
    /// `serde_path_to_error` for friendlier field-path messages).
    pub fn with_parser(
        mut self,
        parse: impl Fn(&serde_json::Value) -> anyhow::Result<In> + Send + Sync + 'static,
    ) -> Self {
        self.parse = Arc::new(parse);
        self
    }

    /// Finalize with a typed handler. `Out` is inferred from the return value;
    /// the engine stores it serialized as JSON, so on resume the handler
    /// re-runs from scratch with the re-parsed input (same as TanStack).
    pub fn handler<F, Fut, Out>(self, handler: F) -> TypedWorkflow<In, Out>
    where
        Out: serde::Serialize + Send + Sync + 'static,
        F: Fn(TypedCtx<In>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<Out>> + Send + 'static,
    {
        let TypedWorkflowBuilder { id, version, parse } = self;
        let parse = Arc::new(parse);
        let handler = Arc::new(handler);
        let engine_handler: WorkflowHandler = Arc::new(move |ctx: WorkflowCtx| {
            let parse = Arc::clone(&parse);
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let input = parse(ctx.input())?;
                let out = handler(TypedCtx {
                    inner: ctx,
                    input: Arc::new(input),
                })
                .await?;
                Ok(serde_json::to_value(out)?)
            })
        });
        TypedWorkflow {
            base: Workflow {
                id,
                version,
                handler: engine_handler,
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