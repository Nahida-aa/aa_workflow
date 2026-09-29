//! Middleware：包在 handler 外面的一层，对齐 TanStack `createMiddleware`
//! （`middleware/create-middleware.ts`）。
//!
//! 两个正交能力：
//! - [`Middleware::produce`] —— 每 drive 产出一个 `ctx.ext`（typed ctx 扩展）；
//! - [`Middleware::wrap`] —— 围绕 handler future 的洋葱式包裹，先注册的在最外层。
//!
//! 与 TanStack 的差异见 [`WorkflowBuilder::middleware`](crate::define::WorkflowBuilder::middleware)(crate::define::WorkflowBuilder::middleware)：
//! 他们的多 middleware context 是类型交集，我们拍平成单一 `TCtxExt` 字段。

use std::sync::Arc;

use crate::define::{BoxFuture, WorkflowCtx};

/// Runtime middleware (erased): a [`wrap`](Self::wrap) around the handler
/// future chain plus a [`produce`](Self::produce) hook that builds the typed
/// ctx extension (`ctx.ext`). Mirrors TanStack's `defineMiddleware`: `wrap`
/// composes around `next`, and the produced context is what the handler reads
/// off `ctx` (their `{ ...context }` accumulation, collapsed to a single
/// extension value — see [`WorkflowBuilder::middleware`](crate::define::WorkflowBuilder::middleware)).
#[derive(Clone)]
pub struct Middleware {
    /// Builds the handler's `ctx.ext` from the erased drive ctx. Runs on every
    /// drive before the handler; its JSON output is deserialized into the
    /// builder's `TCtxExt` type (the last middleware with a `produce` wins).
    pub produce: Option<CtxProducer>,
    /// Around-wrapper on the handler future: `next` is the rest of the pipeline
    /// (inner middlewares, then the typed handler). The first-listed middleware
    /// is outermost, like TanStack's `composeMiddlewares`.
    pub wrap: Option<CtxWrapper>,
}

/// Erased ctx-extension producer: `&WorkflowCtx` → JSON ext value.
pub type CtxProducer = Arc<dyn Fn(&WorkflowCtx) -> anyhow::Result<serde_json::Value> + Send + Sync>;

/// Erased around-wrapper: `(ctx, next)` → wrapped handler future.
pub type CtxWrapper = Arc<
    dyn Fn(
            WorkflowCtx,
            BoxFuture<'static, anyhow::Result<serde_json::Value>>,
        ) -> BoxFuture<'static, anyhow::Result<serde_json::Value>>
        + Send
        + Sync,
>;

impl Middleware {
    pub fn new() -> Self {
        Self {
            produce: None,
            wrap: None,
        }
    }

    /// Set the ctx-extension producer.
    pub fn produce<F>(mut self, f: F) -> Self
    where
        F: Fn(&WorkflowCtx) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static,
    {
        self.produce = Some(Arc::new(f));
        self
    }

    /// Set the around-wrapper.
    pub fn wrap<F>(mut self, f: F) -> Self
    where
        F: Fn(
                WorkflowCtx,
                BoxFuture<'static, anyhow::Result<serde_json::Value>>,
            ) -> BoxFuture<'static, anyhow::Result<serde_json::Value>>
            + Send
            + Sync
            + 'static,
    {
        self.wrap = Some(Arc::new(f));
        self
    }
}

impl Default for Middleware {
    fn default() -> Self {
        Self::new()
    }
}
