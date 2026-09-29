//! Workflow 的 authoring 层：怎么**声明**一个 workflow。
//!
//! 对齐 TanStack `define/define-workflow.ts`：`CreateWorkflowConfig`
//! （`createWorkflow(options)`）、`WorkflowBuilder`、`createWorkflow`，外加
//! Rust 特有的 [`WorkflowDefinition`]（把 TInput/TOutput/TState/TCtxExt 静态带在类型上）。
//!
//! 与 [`define`](super) 的分界：`super` 装的是 handler **运行时**能拿到的东西
//! （`BaseCtx` / `StepCtx` / `StepOptions` / `RetryPolicy`）与 workflow 本体
//! [`Workflow`](super::Workflow)；这里装的是把它们拼起来的声明式入口。

use std::marker::PhantomData;
use std::sync::Arc;

use super::{
    BaseCtx, InitializeFn, RetryPolicy, StateValidatorFn, Workflow, WorkflowCtx, WorkflowHandler,
};
use crate::middleware::Middleware;

/// The declaration config consumed by [`create_workflow`]. Mirrors TanStack's
/// `createWorkflow(options)`; only `id` is required (`initialize` defaults to
/// `{}`). `input`/`state`/`output` re-type the config and install the matching
/// serde shape-checks (zod `.safeParse` counterparts); `version` is the string
/// this workflow's runs persist for version routing; `default_step_retry` is
/// the step retry fallback; `description` is metadata.
pub struct CreateWorkflowConfig<TInput = (), TOutput = (), TState = serde_json::Value> {
    pub id: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub initialize: InitializeFn,
    pub state_validator: Option<StateValidatorFn>,
    pub output_validator: Option<StateValidatorFn>,
    pub default_step_retry: Option<RetryPolicy>,
    pub handler: WorkflowHandler,
    pub _input: PhantomData<TInput>,
    pub _output: PhantomData<TOutput>,
    pub _state: PhantomData<TState>,
}

impl CreateWorkflowConfig<(), (), serde_json::Value> {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            description: None,
            version: None,
            initialize: Arc::new(|_| Ok(serde_json::Value::Object(Default::default()))),
            state_validator: None,
            output_validator: None,
            default_step_retry: None,
            handler: Arc::new(|_ctx: WorkflowCtx| Box::pin(async { Ok(serde_json::Value::Null) })),
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
        }
    }
}

impl<TInput, TOutput, TState> CreateWorkflowConfig<TInput, TOutput, TState> {
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// `initialize({ input })` — rebuild the per-invocation state on every
    /// start and resume (state is never persisted, see
    /// [`Workflow::initialize`](Workflow::initialize)).
    pub fn initialize(
        mut self,
        f: impl Fn(&serde_json::Value) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static,
    ) -> Self {
        self.initialize = Arc::new(f);
        self
    }

    /// Fallback retry for steps that declare no retry of their own
    /// (see [`StepOptions`](super::StepOptions)).
    pub fn default_step_retry(mut self, retry: RetryPolicy) -> Self {
        self.default_step_retry = Some(retry);
        self
    }

    /// Declare the input schema as a serde type: re-types the config to
    /// `CreateWorkflowConfig<NewIn, TOutput, TState>` so `create_workflow` builds a
    /// [`WorkflowBuilder<NewIn, TOutput, TState>`](WorkflowBuilder). The handler's
    /// `ctx.input` is `NewIn`. Missing/mistyped input fields error the run
    /// (zod `inputSchema` `.safeParse` counterpart).
    pub fn input<NewIn>(self) -> CreateWorkflowConfig<NewIn, TOutput, TState>
    where
        NewIn: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        CreateWorkflowConfig {
            id: self.id,
            description: self.description,
            version: self.version,
            initialize: self.initialize,
            state_validator: self.state_validator,
            output_validator: self.output_validator,
            default_step_retry: self.default_step_retry,
            handler: self.handler,
            _input: PhantomData,
            _output: self._output,
            _state: self._state,
        }
    }

    /// Declare the state schema as a serde type: the state built by
    /// [`initialize`](Self::initialize) is shape-checked against `NewSt` on
    /// every invocation, and the handler's `ctx.state` is `NewSt`.
    pub fn state<NewSt>(self) -> CreateWorkflowConfig<TInput, TOutput, NewSt>
    where
        NewSt: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        CreateWorkflowConfig {
            id: self.id,
            description: self.description,
            version: self.version,
            initialize: self.initialize,
            state_validator: Some(Arc::new(|v| {
                serde_json::from_value::<NewSt>(v.clone())?;
                Ok(())
            })),
            output_validator: self.output_validator,
            default_step_retry: self.default_step_retry,
            handler: self.handler,
            _input: self._input,
            _output: self._output,
            _state: PhantomData,
        }
    }

    /// Declare the output schema as a serde type: re-types the config to
    /// `CreateWorkflowConfig<_, NewOut, _>`; the handler's `TOutput` is validated
    /// against `NewOut` (zod `.safeParse`) before the run finishes. Inferred
    /// from the handler closure otherwise, exactly like TanStack — the schema
    /// constrains but never declares.
    pub fn output<NewOut>(self) -> CreateWorkflowConfig<TInput, NewOut, TState>
    where
        NewOut: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        CreateWorkflowConfig {
            id: self.id,
            description: self.description,
            version: self.version,
            initialize: self.initialize,
            state_validator: self.state_validator,
            output_validator: Some(Arc::new(|v| {
                serde_json::from_value::<NewOut>(v.clone())?;
                Ok(())
            })),
            default_step_retry: self.default_step_retry,
            handler: self.handler,
            _input: self._input,
            _output: PhantomData,
            _state: self._state,
        }
    }
}

/// Define a workflow. Returns a builder chain:
///
/// ```rust,ignore
/// let onboard = create_workflow(
///     CreateWorkflowConfig::new("onboard").input::<OnboardInput>(),
/// )
/// .middleware(traced)
/// .handler(|ctx| async move {
///     let profile = ctx.step("load", |_| async { load_profile(&ctx.input.user_id) }).await?;
///     ctx.sleep("cooloff", std::time::Duration::from_secs(60)).await?;
///     let decision = ctx.approve("continue", "Continue?").await?;
///     Ok(serde_json::json!({ "ok": decision["approved"] }))
/// });
/// ```
///
/// The handler's `ctx` argument (a [`BaseCtx`]) carries everything: the
/// typed input, state, durable primitives ([`step`](BaseCtx::step),
/// [`sleep`](BaseCtx::sleep), [`approve`](BaseCtx::approve), ...), and any
/// fields added by registered middleware (`ctx.ext`). Helpers should accept
/// a typed `BaseCtx<...>` argument to compose cleanly.
///
/// `TInput`/`TOutput`/`TState` are pinned by the config's
/// [`input`](CreateWorkflowConfig::input) / [`output`](CreateWorkflowConfig::output)
/// / [`state`](CreateWorkflowConfig::state) methods; the handler's output type
/// is inferred from the closure return value.
pub fn create_workflow<TInput, TOutput, TState>(
    config: CreateWorkflowConfig<TInput, TOutput, TState>,
) -> WorkflowBuilder<TInput, TOutput, TState, ()> {
    WorkflowBuilder {
        config,
        middlewares: Vec::new(),
        previous: Vec::new(),
        _ext: PhantomData,
    }
}

/// The typed builder — `TInput`/`TOutput`/`TState` mirror the workflow's schema
/// at build time, `TCtxExt` is the ctx extension type declared by the last
/// [`middleware`](Self::middleware) call (`()` when none).
pub struct WorkflowBuilder<TInput, TOutput, TState, TCtxExt = ()> {
    config: CreateWorkflowConfig<TInput, TOutput, TState>,
    middlewares: Vec<Middleware>,
    previous: Vec<Workflow>,
    _ext: PhantomData<TCtxExt>,
}

impl<TInput, TOutput, TState, TCtxExt> WorkflowBuilder<TInput, TOutput, TState, TCtxExt> {
    /// Attach a runtime middleware and re-key `TCtxExt` to `PExt` — the type of
    /// `ctx.ext` the handler reads (`produce`'s JSON output is deserialized
    /// into it; [`Default`] is used when the middleware has no `produce`).
    /// TanStack's intersection of extension types collapses to this single
    /// bundle; extra `wrap`s still compose in registration order.
    pub fn middleware<PExt>(
        mut self,
        m: Middleware,
    ) -> WorkflowBuilder<TInput, TOutput, TState, PExt> {
        self.middlewares.push(m);
        WorkflowBuilder {
            config: self.config,
            middlewares: self.middlewares,
            previous: self.previous,
            _ext: PhantomData,
        }
    }

    /// Older versions of the same workflow to route resumed runs to (see
    /// [`select_workflow_version`](crate::engine::select_workflow_version)).
    pub fn previous_versions(mut self, v: Vec<Workflow>) -> Self {
        self.previous = v;
        self
    }

    /// Finalize with the orchestrating closure. `AOut` is inferred from the
    /// handler's return value; the engine stores it serialized as JSON, so on
    /// resume the handler re-runs from scratch with re-parsed input/state and a
    /// freshly `produce`d extension (same as TanStack).
    pub fn handler<F, Fut, AOut>(
        self,
        handler: F,
    ) -> WorkflowDefinition<TInput, AOut, TState, TCtxExt>
    where
        TInput: serde::de::DeserializeOwned + Send + Sync + 'static,
        TState: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static,
        TCtxExt: serde::de::DeserializeOwned + Default + Send + Sync + 'static,
        AOut: serde::Serialize + Send + Sync + 'static,
        F: Fn(BaseCtx<TInput, TState, TCtxExt>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<AOut>> + Send + 'static,
    {
        let CreateWorkflowConfig {
            id,
            description,
            version,
            initialize,
            state_validator,
            output_validator,
            default_step_retry,
            handler: _,
            _input: _,
            _output: _,
            _state: _,
        } = self.config;
        let handler = Arc::new(handler);
        let middlewares = self.middlewares.clone();
        let engine_middlewares = self.middlewares.clone();
        let workflow_middlewares = self.middlewares.clone();
        let validator = output_validator.clone();
        let engine_validator = validator.clone();
        let mut engine_handler: WorkflowHandler = Arc::new(move |ctx: WorkflowCtx| {
            let handler = Arc::clone(&handler);
            let produce_mw = engine_middlewares.clone();
            let output_validator = engine_validator.clone();
            Box::pin(async move {
                let input = serde_json::from_value(ctx.input.clone())?;
                let state = serde_json::from_value(ctx.state.snapshot())?;
                let ext: TCtxExt = produce_mw
                    .iter()
                    .filter_map(|m| m.produce.clone())
                    .next_back()
                    .map(|p| -> anyhow::Result<TCtxExt> { Ok(serde_json::from_value(p(&ctx)?)?) })
                    .transpose()?
                    .unwrap_or_default();
                let typed = BaseCtx {
                    run_id: ctx.run_id,
                    input,
                    state: crate::define::StateHandle::new(
                        state,
                        ctx.state.mirror(),
                        Arc::new(|st: &TState| {
                            serde_json::to_value(st).unwrap_or(serde_json::Value::Null)
                        }),
                    ),
                    ext,
                    engine: ctx.engine.clone(),
                };
                let out = handler(typed).await?;
                let value = serde_json::to_value(out)?;
                if let Some(validate) = &output_validator {
                    validate(&value)?;
                }
                Ok(value)
            })
        });
        // Fold wraps outermost-first, matching TanStack `composeMiddlewares`
        // where the first-listed middleware is outermost.
        for m in middlewares.iter().rev() {
            if let Some(wrap) = &m.wrap {
                let wrap = wrap.clone();
                let inner = Arc::clone(&engine_handler);
                engine_handler = Arc::new(move |ctx: WorkflowCtx| {
                    let wrap = wrap.clone();
                    let inner = inner.clone();
                    Box::pin(async move {
                        let next = inner(ctx.clone());
                        wrap(ctx, next).await
                    })
                });
            }
        }
        WorkflowDefinition {
            base: Workflow {
                id,
                version,
                description,
                default_step_retry,
                middlewares: workflow_middlewares,
                previous_versions: self.previous,
                handler: engine_handler,
                initialize,
                state_validator,
                output_validator: validator,
            },
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
            _ext: PhantomData,
        }
    }
}

/// A workflow whose `TInput`/`TOutput`/`TState`/`TCtxExt` are statically known at the
/// declaration site. `Deref<Target = Workflow>` lets it be handed to
/// [`crate::engine::run_workflow`] / the registry directly; explicit erasure is
/// [`into_workflow`](Self::into_workflow).
#[derive(Clone)]
pub struct WorkflowDefinition<TInput, TOutput, TState = serde_json::Value, TCtxExt = ()> {
    base: Workflow,
    _input: PhantomData<TInput>,
    _output: PhantomData<TOutput>,
    _state: PhantomData<TState>,
    _ext: PhantomData<TCtxExt>,
}

impl<TInput, TOutput, TState, TCtxExt> WorkflowDefinition<TInput, TOutput, TState, TCtxExt> {
    /// Type-erase back to the engine's [`Workflow`] view.
    pub fn into_workflow(self) -> Workflow {
        self.base
    }
}

impl<TInput, TOutput, TState, TCtxExt> std::ops::Deref
    for WorkflowDefinition<TInput, TOutput, TState, TCtxExt>
{
    type Target = Workflow;

    fn deref(&self) -> &Workflow {
        &self.base
    }
}
