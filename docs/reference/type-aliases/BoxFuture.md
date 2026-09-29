---
id: BoxFuture
title: BoxFuture
---

# Type Alias: BoxFuture

Defined in: `packages/workflow-core/src/define/mod.rs:30`

Boxed async step-returning future. Steps are spawned inside the engine's
driver; the async world is the default (mirrors `Promise.all` in JS).

## Definition

```rust
pub type BoxFuture = Pin<Box<dyn Future + Send>>
```

