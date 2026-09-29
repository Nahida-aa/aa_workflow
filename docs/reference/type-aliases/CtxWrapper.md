---
id: CtxWrapper
title: CtxWrapper
---

# Type Alias: CtxWrapper

Defined in: `packages/workflow-core/src/middleware/create_middleware.rs:37`

Erased around-wrapper: `(ctx, next)` → wrapped handler future.

## Definition

```rust
pub type CtxWrapper = Arc<dyn Fn(WorkflowCtx, BoxFuture<'static, Result<Value>>) -> BoxFuture<'static, Result<Value>> + Send + Sync>
```

