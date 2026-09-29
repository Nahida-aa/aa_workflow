---
id: Middleware
title: Middleware
---

# Struct: Middleware

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:22`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L22)

Runtime middleware (erased): a [`wrap`](Middleware.md) around the handler
future chain plus a [`produce`](Middleware.md) hook that builds the typed
ctx extension (`ctx.ext`). Mirrors TanStack's `defineMiddleware`: `wrap`
composes around `next`, and the produced context is what the handler reads
off `ctx` (their `{ ...context }` accumulation, collapsed to a single
extension value — see [`WorkflowBuilder::middleware`](WorkflowBuilder.md)).

## Fields

### produce

```rust
produce: Option<CtxProducer>
```

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:26`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L26)

Builds the handler's `ctx.ext` from the erased drive ctx. Runs on every
drive before the handler; its JSON output is deserialized into the
builder's `TCtxExt` type (the last middleware with a `produce` wins).


***

### wrap

```rust
wrap: Option<CtxWrapper>
```

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:30`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L30)

Around-wrapper on the handler future: `next` is the rest of the pipeline
(inner middlewares, then the typed handler). The first-listed middleware
is outermost, like TanStack's `composeMiddlewares`.

## Implementations

### new()

```rust
pub fn new() -> Self
```

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:47`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L47)

#### Returns

`Self`


***

### produce()

```rust
pub fn produce<F>(self, f: F) -> Self
where
    F: Fn(&WorkflowCtx) -> Result<Value> + Send + Sync + 'static
```

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:55`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L55)

Set the ctx-extension producer.

#### Parameters

##### f

`F`

#### Returns

`Self`


***

### wrap()

```rust
pub fn wrap<F>(self, f: F) -> Self
where
    F: Fn(WorkflowCtx, BoxFuture<'static, Result<Value>>) -> BoxFuture<'static, Result<Value>> + Send + Sync + 'static
```

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:64`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L64)

Set the around-wrapper.

#### Parameters

##### f

`F`

#### Returns

`Self`

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

