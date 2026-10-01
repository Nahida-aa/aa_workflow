---
id: CtxWrapper
title: CtxWrapper
---

# Type Alias: CtxWrapper

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:42`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L42)

Erased around-wrapper: `(ctx, next)` → wrapped handler future.

## Definition

```rust
pub type CtxWrapper = Arc<dyn Fn(WorkflowCtx, BoxFuture<'static, Result<Value>>) -> BoxFuture<'static, Result<Value>> + Send + Sync>
```

