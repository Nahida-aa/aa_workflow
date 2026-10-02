---
id: Middleware
title: Middleware
---

# Struct: Middleware

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:34`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L34)

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

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:41`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L41)

Builds part of the handler's `ctx.ext` from the erased drive ctx. All
`produce` hooks run on every drive, in registration order, and their
JSON outputs shallow-merge into one object (later keys win, matching
TanStack's `Object.assign` accumulation); only object outputs
participate. The merged object is deserialized into the builder's
`TCtxExt` type; `Default` applies when nothing produced an object.


***

### wrap

```rust
wrap: Option<CtxWrapper>
```

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:45`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L45)

Around-wrapper on the handler future: `next` is the rest of the pipeline
(inner middlewares, then the typed handler). The first-listed middleware
is outermost, like TanStack's `composeMiddlewares`.

## Implementations

### new()

```rust
pub fn new() -> Self
```

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:62`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L62)

#### Returns

`Self`


***

### produce()

```rust
pub fn produce<F>(self, f: F) -> Self
where
    F: Fn(&WorkflowCtx) -> Result<Value> + Send + Sync + 'static
```

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:70`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L70)

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

Defined in: [`packages/workflow_core/src/middleware/create_middleware.rs:79`](../../../packages/workflow_core/src/middleware/create_middleware.rs#L79)

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

