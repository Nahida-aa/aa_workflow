---
id: Middleware
title: Middleware
---

# Struct: Middleware

Defined in: `packages/workflow-core/src/middleware/create_middleware.rs:22`

Runtime middleware (erased): a [`wrap`](Self::wrap) around the handler
future chain plus a [`produce`](Self::produce) hook that builds the typed
ctx extension (`ctx.ext`). Mirrors TanStack's `defineMiddleware`: `wrap`
composes around `next`, and the produced context is what the handler reads
off `ctx` (their `{ ...context }` accumulation, collapsed to a single
extension value — see [`WorkflowBuilder::middleware`](crate::define::WorkflowBuilder::middleware)).

## Fields

### `produce`

```rust
produce: Option<CtxProducer>
```

Builds the handler's `ctx.ext` from the erased drive ctx. Runs on every
drive before the handler; its JSON output is deserialized into the
builder's `Ext` type (the last middleware with a `produce` wins).

### `wrap`

```rust
wrap: Option<CtxWrapper>
```

Around-wrapper on the handler future: `next` is the rest of the pipeline
(inner middlewares, then the typed handler). The first-listed middleware
is outermost, like TanStack's `composeMiddlewares`.

## Implementations

### `new`

```rust
pub fn new() -> Self
```

Defined in: `packages/workflow-core/src/middleware/create_middleware.rs:47`

### `produce`

```rust
pub fn produce<F>(self, f: F) -> Self
```

Set the ctx-extension producer.

Defined in: `packages/workflow-core/src/middleware/create_middleware.rs:55`

### `wrap`

```rust
pub fn wrap<F>(self, f: F) -> Self
```

Set the around-wrapper.

Defined in: `packages/workflow-core/src/middleware/create_middleware.rs:64`

## Trait Implementations

- `impl Borrow for Middleware`
- `impl BorrowMut for Middleware`
- `impl CloneToUninit for Middleware`
- `impl Into for Middleware`
- `impl From for Middleware`
- `impl TryInto for Middleware`
- `impl TryFrom for Middleware`
- `impl Any for Middleware`
- `impl ToOwned for Middleware`
- `impl Clone for Middleware`
- `impl Default for Middleware`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

