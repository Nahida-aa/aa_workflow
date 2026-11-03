//! WorkflowDefinition 的 authoring 层：怎么**声明**一个 workflow。
//!
//! 对齐 TanStack `define/define-workflow.ts`：`CreateWorkflowConfig`
//! （`createWorkflow(options)`）、`WorkflowBuilder`、`createWorkflow`。
//!
//! 与 [`define`](super) 的分界：`super` 装的是 handler **运行时**能拿到的东西
//! （`BaseCtx` / `StepCtx` / `StepOptions` / `RetryPolicy`）与 workflow 本体
//! [`WorkflowDefinition`](super::WorkflowDefinition)；这里装的是把它们拼起来的
//! 声明式入口。`createWorkflow(...).handler(...)` 产出的就是
//! `WorkflowDefinition<TInput, TOutput, TState>`。

use std::marker::PhantomData;
use std::sync::Arc;

use super::{AnyWorkflowDefinition, BaseCtx, InitializeFn, RetryPolicy, StateValidatorFn, WorkflowCtx, WorkflowDefinition, WorkflowHandler};
use crate::middleware::{AnyMiddleware, Middleware};

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
    pub input_validator: Option<StateValidatorFn>,
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
            input_validator: None,
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
    /// [`WorkflowDefinition::initialize`](WorkflowDefinition::initialize)).
    ///
    /// typed：闭包吃 `&TInput`、产 `TState`（对齐上游——`TState` 由
    /// `.state::<T>()` 声明、编译期钉死）。内部 serde 桥接到运行时的擦除
    /// 表示（`WorkflowDefinition` 持 `Fn(&Value) -> Result<Value>`）；input 反序列化
    /// 失败 / state 序列化失败都算 run 错误。
    pub fn initialize(
        mut self,
        f: impl Fn(&TInput) -> anyhow::Result<TState> + Send + Sync + 'static,
    ) -> Self
    where
        TInput: serde::de::DeserializeOwned,
        TState: serde::Serialize,
    {
        self.initialize = Arc::new(move |input: &serde_json::Value| {
            let typed_input: TInput = serde_json::from_value(input.clone())?;
            let typed_state = f(&typed_input)?;
            Ok(serde_json::to_value(typed_state)?)
        });
        self
    }

    /// Declare the state's *shape* for validation **without** re-typing the
    /// handler's view of `ctx.state`（保持 `Value`）——`.state::<T>()` 会把
    /// `TState` 重键为 `T`（handler 拿到强类型 state），而这里只安装同一份
    /// serde 校验（initialize 产物 / 重放重建的 state 必须能反序列化回
    /// `T`，否则 run 在声明点就报 validation）。handler 想以强类型读写
    /// state 时用 [`Self::state`]，想保持擦除态只做形状把关时用本方法。
    pub fn state_schema<T: serde::de::DeserializeOwned + Send + Sync + 'static>(
        mut self,
    ) -> Self {
        self.state_validator = Some(Arc::new(|v| {
            serde_json::from_value::<T>(v.clone())?;
            Ok(())
        }));
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
            input_validator: Some(Arc::new(|v| {
                serde_json::from_value::<NewIn>(v.clone())?;
                Ok(())
            })),
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
            input_validator: self.input_validator,
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
            input_validator: self.input_validator,
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
) -> WorkflowBuilder<TInput, TOutput, TState> {
    WorkflowBuilder {
        config,
        middlewares: Vec::new(),
        previous: Vec::new(),
    }
}

/// The typed builder — `TInput`/`TOutput`/`TState` mirror the workflow's schema
/// at build time.
///
/// 没有 `TCtxExt`：扩展走 [`ExtBag`](crate::define::ExtBag) 的命名空间获取，
/// 不参与类型层（见 `docs/decisions/0015`）。
pub struct WorkflowBuilder<TInput, TOutput, TState> {
    config: CreateWorkflowConfig<TInput, TOutput, TState>,
    middlewares: Vec<Middleware>,
    previous: Vec<AnyWorkflowDefinition>,
}

impl<TInput, TOutput, TState> WorkflowBuilder<TInput, TOutput, TState> {
    /// 注册中间件数组 —— 对齐上游
    /// `middleware(middlewares: ReadonlyArray<AnyMiddleware>)`
    /// （`types.ts:494`、`define-workflow.ts:149`）。
    ///
    /// ```ignore
    /// let wf = create_workflow(cfg)
    ///     .middleware([agent_middleware(), auth()])   // 不同来源也能混装
    ///     .handler(|ctx| async move {
    ///         let auth = ctx.ext.auth();              // 按命名空间取
    ///         ...
    ///     });
    /// ```
    ///
    /// 与上游一致的地方：可多次调用叠加、元素可混装、顺序即注册顺序。
    ///
    /// 差异只有一处：**不需要写扩展类型**——上游靠 `TCtxExt & AccumulateExtensions`
    /// 在编译期求交集（handler 的 `ctx` 多出字段）；这里扩展在运行期按命名空间
    /// 存取（[`ExtBag`](crate::define::ExtBag)），所以没有「合并后的类型」这回事，
    /// 使用者也不必自己定义 `Combined`。
    pub fn middleware(
        mut self,
        middlewares: impl IntoIterator<Item = AnyMiddleware>,
    ) -> Self {
        // 对齐上游 `.middleware([...])`：收一个数组、可叠加。
        for m in middlewares {
            self.middlewares.push(m);
        }
        self
    }

    /// Older versions of the same workflow to route resumed runs to (see
    /// [`select_workflow_version`](crate::engine::select_workflow_version)).
    pub fn previous_versions(
        mut self,
        v: impl IntoIterator<Item = impl Into<AnyWorkflowDefinition>>,
    ) -> Self {
        self.previous = v.into_iter().map(Into::into).collect();
        self
    }

    /// Finalize with the orchestrating closure. `AOut` is inferred from the
    /// handler's return value; the engine stores it serialized as JSON, so on
    /// resume the handler re-runs from scratch with re-parsed input/state and a
    /// freshly `produce`d extension (same as TanStack).
    pub fn handler<F, Fut, AOut>(
        self,
        handler: F,
    ) -> WorkflowDefinition<TInput, AOut, TState>
    where
        TInput: serde::de::DeserializeOwned + Send + Sync + 'static,
        TState: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static,
        AOut: serde::Serialize + Send + Sync + 'static,
        F: Fn(BaseCtx<TInput, TState>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<AOut>> + Send + 'static,
    {
        let CreateWorkflowConfig {
            id,
            description,
            version,
            initialize,
            input_validator,
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
        let workflow_middlewares = self.middlewares.clone();
        let validator = output_validator.clone();
        // 中间件折叠（对齐 TS `composeMiddlewares` 的洋葱模型）：
        // 最内层 = typed ctx + handler；每个 middleware 的 `server` 包住内层。
        // server 经 `MiddlewareServerFnArgs` 拿到完整 ctx，可 `next(ext)` /
        // `set_step_interceptor` / `take_over`。
        let innermost: WorkflowHandler = Arc::new(move |ctx: WorkflowCtx| {
            let handler = Arc::clone(&handler);
            let output_validator = output_validator.clone();
            Box::pin(async move {
                let input = serde_json::from_value(ctx.input.clone())?;
                let state = serde_json::from_value(ctx.state.snapshot())?;
                // 各中间件 server 里经 `next(ext)` 合并进 `engine.ext_extra`
                // （擦除层 Value；Object.assign 同义）。handler 侧按命名空间取。
                let ext = crate::define::ExtBag::from_value(
                    ctx.engine.ext_extra.lock().expect("ext lock").clone(),
                );
                let typed = BaseCtx {
                    run_id: ctx.run_id,
                    input,
                    meta: ctx.meta.clone(),
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
        // server 链：注册顺序 = 外层先执行（TanStack `composeMiddlewares` 同义）。
        // 每层把 `MiddlewareServerFnArgs` 交给 middleware 的 server；`next` 闭包推进到内层。
        let mut engine_handler: WorkflowHandler = innermost;
        for m in middlewares.iter().rev() {
            let server = m.server.clone();
            let inner = std::sync::Arc::clone(&engine_handler);
            engine_handler = Arc::new(move |ctx: WorkflowCtx| {
                let server = server.clone();
                let inner = inner.clone();
                Box::pin(async move {
                    let sc = crate::middleware::MiddlewareServerFnArgs {
                        ctx: ctx.clone(),
                        erased: ctx.clone(),
                        pipeline: Some(std::sync::Arc::new(
                            move |ext: Option<serde_json::Value>| {
                                let inner = inner.clone();
                                let ctx = ctx.clone();
                                // `next(ext)` 一次调用做两件事，位置与上游
                                // `run-workflow.ts:1502` 一一对应：
                                //   ① Object.assign(ctx, opts.context)
                                //   ② compose(index + 1)
                                if let Some(ext) = ext {
                                    ctx.engine.merge_ext(ext);
                                }
                                Box::pin(async move { inner(ctx).await })
                            },
                        )),
                    };
                    server(sc).await
                })
            });
        }
        WorkflowDefinition {
            id,
            version,
            description,
            default_step_retry,
            middlewares: workflow_middlewares,
            previous_versions: self.previous,
            handler: engine_handler,
            initialize,
            input_validator,
            state_validator,
            output_validator: validator,
            _input: PhantomData,
            _output: PhantomData,
            _state: PhantomData,
        }
    }
}
