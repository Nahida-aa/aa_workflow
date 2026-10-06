---
id: UndoFuture
title: UndoFuture
---

# Type Alias: UndoFuture

Defined in: [`packages/workflow_core/src/engine/mod.rs:130`](../../../packages/workflow_core/src/engine/mod.rs#L130)

## Definition

```rust
pub type UndoFuture = Pin<Box<dyn Future + Send>>
```

