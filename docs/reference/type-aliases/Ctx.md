---
id: Ctx
title: Ctx
---

# Type Alias: Ctx

Defined in: [`packages/workflow_core/src/define/mod.rs:135`](../../../packages/workflow_core/src/define/mod.rs#L135)

The full ctx type. Generic `TCtxExt` matches TS's `Ctx<TInput, TState, TCtxExt>`,
where `TCtxExt` defaults to `{}` (our `()`).

## Definition

```rust
pub type Ctx = BaseCtx<TInput, TState, TCtxExt>
```

