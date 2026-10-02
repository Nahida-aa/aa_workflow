---
id: CtxProducer
title: CtxProducer
---

# Type Alias: CtxProducer

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:49`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L49)

Erased ctx-extension producer: `&WorkflowCtx` → JSON ext value.

## Definition

```rust
pub type CtxProducer = Arc<dyn Fn(&WorkflowCtx) -> Result<Value> + Send + Sync>
```

