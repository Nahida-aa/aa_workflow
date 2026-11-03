//! Middleware：对齐 TanStack `createMiddleware`（`middleware/create-middleware.ts`）。
//!
//! # 形状
//!
//! TS 只有一个 `server` 函数：`async ({ ctx, next }) => { ...; next({ context }) }`。
//! Rust 是**同一个形状**：
//!
//! ```ignore
//! let mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
//!     Box::pin(async move {
//!         sc.set_step_interceptor(intercept);                              // ① 拦截 step
//!         sc.next(Some(DefExt::<AuthNs>::new(auth).into_value())).await    // ② 扩展 + 推进
//!     })
//! });
//! ```
//!
//! # 类型对照
//!
//! | TS | Rust |
//! | --- | --- |
//! | `Middleware<TCtxIn, TExtension>` | [`Middleware`]（无泛型） |
//! | `MiddlewareServerFn<TCtxIn, TExtension>` | [`MiddlewareServerFn`] |
//! | `({ ctx, next }) => …` | [`MiddlewareServerFnArgs`]（`ctx` 字段 + `next` 方法） |
//! | `next({ context })` | [`MiddlewareServerFnArgs::next`]（一次调用：合并 + 推进） |
//! | `ctx.step = wrapped` | [`MiddlewareServerFnArgs::set_step_interceptor`] |
//! | `AnyMiddleware = Middleware<any, any>` | [`AnyMiddleware`]（别名，可混装） |
//!
//! # 为什么没有 `TCtxIn` / `TExtension` 两个泛型
//!
//! 上游的两个泛型都服务于**类型交集**：`TCtxIn` 让中间件读到前序扩展，
//! `TExtension` 让 handler 的 ctx 多出字段。
//!
//! Rust 里两者都不需要了：
//!
//! - 读扩展走 `ctx.ext.<ns>()`（[`ExtNamespace`](crate::define::ExtNamespace)），
//!   不需要把 ctx 窄化成「已带前序扩展的 ctx」；
//! - 写扩展走 `next(Some({ KEY: value }))`，handler 侧按命名空间取，
//!   **不需要任何合并类型**——使用者不必定义 `Combined`。
//!
//! 详细论证见 `docs/decisions/0015`。
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use crate::define::{BoxFuture, WorkflowCtx};

/// 中间件 `server` 函数的签名（对齐上游 `MiddlewareServerFn`）。
pub type MiddlewareServerFn =
    Arc<dyn Fn(MiddlewareServerFnArgs) -> BoxFuture<'static, Result<Value>> + Send + Sync>;

/// 后续管线：吃一个扩展对象、返回 future（对齐上游 `next(opts)`）。
///
/// `Some`/`take()` 承载「至多一次」，对齐上游的 `advanced` 标志。
pub type NextFn = Arc<dyn Fn(Option<Value>) -> BoxFuture<'static, Result<Value>> + Send + Sync>;

/// `MiddlewareServerFn` 的入参对象 —— 上游那个**匿名**的函数参数
/// （`types.ts:458` 的 `(args: { ctx: TCtxIn; next: ... }) => ...`）。
///
/// `ctx` 是字段、`next` 是方法（借 `&mut self` 才能保证「至多一次」）。
pub struct MiddlewareServerFnArgs {
    /// 完整的 workflow ctx（`input` / `state` / `ext` / `meta`）。
    pub ctx: WorkflowCtx,
    /// 擦除层的完整 ctx（clone 廉价：内部全是 `Arc`）。
    pub(crate) erased: WorkflowCtx,
    /// 上游 `next` 参数的**实现**（不是 `next` 本身）。
    pub(crate) pipeline: Option<NextFn>,
}

impl MiddlewareServerFnArgs {
    /// 受控覆盖 `ctx.step`：拦截器对**本次 drive 内每个真实执行的 step** 作出决定。
    ///
    /// 对齐 TS 的 `ctx.step = wrapped`——那边是未承诺的实现细节，这边是受控 API。
    ///
    /// - [`StepAction::Skip`]：不执行闭包，把给定结果作为 `STEP_FINISHED` 落盘；
    /// - [`StepAction::Park`]：在本 step 边界泊车（后续 step 不执行）；
    /// - `None`：正常执行。
    pub fn set_step_interceptor(&self, f: StepInterceptor) {
        self.erased.engine.set_step_interceptor(f);
    }

    /// 推进到内层中间件与 handler —— 对齐上游 `next(opts)`（`run-workflow.ts:1502`）。
    ///
    /// 一次调用做两件事：`Object.assign(ctx, ext)` 与 `compose(index + 1)`。
    ///
    /// ```ignore
    /// let out = sc.next(Some(json!({ "user": "u1" }))).await?;   // TS: await next({ context: { user } })
    /// let out = sc.next(None).await?;                            // 不写扩展，直接推进
    /// ```
    ///
    /// # 为什么参数是 `Option<Value>`
    ///
    /// TS 的 `next` 参数必填（只能写 `next({ context: {} })`）。Rust 里 `None`
    /// 表示「这次不贡献扩展」，等价于传空对象，且不会往 `ext_extra` 塞空袋。
    ///
    /// # Errors
    ///
    /// 第二次调用报错（对齐 `next() must be called at most once per invocation`）。
    pub fn next(&mut self, ext: impl Into<Option<Value>>) -> BoxFuture<'static, Result<Value>> {
        let Some(next) = self.pipeline.take() else {
            return Box::pin(async move {
                anyhow::bail!("middleware.next() must be called at most once per invocation")
            });
        };
        next(ext.into())
    }

    /// 完全接管：跳过后续管线，直接给 output。
    ///
    /// Rust 特有（TS 丢弃 `m.server()` 的返回值）——「中间件就是 handler」时用，
    /// 例如 DAG 中间件。
    pub fn take_over(&mut self) {
        self.pipeline = None;
    }

    /// 擦除层的完整 ctx。
    pub fn erased(&self) -> &WorkflowCtx {
        &self.erased
    }
}

/// step 拦截器：对每个将要真实执行的 step 作出决定。
///
/// 时机：`exec_step` 入口、重放短路**之后**。槽位是**单值**：多个中间件都装时
/// 后者覆盖前者。
pub type StepInterceptor = Arc<dyn Fn(&StepEvent<'_>) -> Option<StepAction> + Send + Sync>;

/// 拦截器看到的数据。
#[derive(Debug)]
pub struct StepEvent<'a> {
    pub run_id: &'a str,
    pub step_id: &'a str,
    /// Run 级参数（`RunWorkflowOptions.meta`，策略输入）。
    pub run_meta: Option<&'a Value>,
}

/// 拦截器的决定。
#[derive(Debug, Clone)]
pub enum StepAction {
    /// 不执行 step 闭包，把给定结果作为本 step 的 `STEP_FINISHED` 落盘（redo 复用）。
    Skip { result: Value },
    /// 在本 step 边界泊车：`SIGNAL_AWAITED { name, meta }` 落盘，后续 step 不执行。
    Park {
        signal_name: String,
        meta: Option<Value>,
    },
}

/// 中间件（对齐 TS `Middleware`：一个 server 函数）。
///
/// 不含泛型：扩展走 `next(Some({ KEY: value }))` 写入、`ctx.ext.<ns>()` 读取，
/// 所以「中间件类型 → handler ctx 类型」这条编译期通道在 Rust 里不需要
/// （见模块文档「为什么没有两个泛型」）。
pub struct Middleware {
    pub server: MiddlewareServerFn,
}

impl Clone for Middleware {
    fn clone(&self) -> Self {
        Self {
            server: self.server.clone(),
        }
    }
}

impl Default for Middleware {
    /// 空中间件：**直接透传**（对齐 TS「不设 server 则什么也不做」）。
    fn default() -> Self {
        Self {
            server: Arc::new(|mut sc: MiddlewareServerFnArgs| {
                Box::pin(async move { sc.next(None).await })
            }),
        }
    }
}

impl Middleware {
    /// 空中间件：**直接透传**。
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the server function（对齐 TS 的 `createMiddleware().server(fn)`）。
    ///
    /// ```ignore
    /// let mw = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
    ///     Box::pin(async move { sc.next(None).await })
    /// });
    /// ```
    pub fn server<F>(mut self, f: F) -> Self
    where
        F: Fn(MiddlewareServerFnArgs) -> BoxFuture<'static, Result<Value>> + Send + Sync + 'static,
    {
        self.server = Arc::new(f);
        self
    }
}

/// 类型已擦除的中间件 —— 对齐上游 `AnyMiddleware = Middleware<any, any>`。
///
/// 因为 [`Middleware`] 已经没有类型参数，这就是它本身；保留别名是为了让
/// `Vec<AnyMiddleware>` / `[AnyMiddleware; N]` 的调用点读起来与上游一致。
pub type AnyMiddleware = Middleware;
