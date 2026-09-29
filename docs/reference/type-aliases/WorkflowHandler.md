---
id: WorkflowHandler
title: WorkflowHandler
---

# Type Alias: WorkflowHandler

Defined in: [`packages/workflow-core/src/define/mod.rs:36`](../../../packages/workflow-core/src/define/mod.rs#L36)

The workflow's orchestrating closure: plain async control flow over
`ctx.step` / `try_join!`. Decisions (branching, which steps run) are made
by this code — the graph is an emergent, per-run artifact, never declared
up front.

## Definition

```rust
pub type WorkflowHandler = Arc<dyn Fn(WorkflowCtx) -> BoxFuture<'static, Result<Value>> + Send + Sync>
```

