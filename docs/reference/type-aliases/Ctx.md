---
id: Ctx
title: Ctx
---

# Type Alias: Ctx

Defined in: [`packages/workflow-core/src/define/mod.rs:136`](../../../packages/workflow-core/src/define/mod.rs#L136)

The full ctx type. Generic `TExt` matches TS's `Ctx<TInput, TState, TExt>`,
where `TExt` defaults to `{}` (our `()`).

## Definition

```rust
pub type Ctx = BaseCtx<TInput, TState, TExt>
```

