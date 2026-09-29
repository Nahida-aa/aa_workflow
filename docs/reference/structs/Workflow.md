---
id: Workflow
title: Workflow
---

# Struct: Workflow

Defined in: [`packages/workflow-core/src/define/mod.rs:466`](../../../packages/workflow-core/src/define/mod.rs#L466)

A declared workflow: id, optional version, the async handler, plus the
builder-derived extras (`description`, `default_step_retry`, `middlewares`,
`previous_versions`, `output_validator`) that TanStack carries on the
workflow object.

## Fields

### id

```rust
id: String
```

Defined in: [`packages/workflow-core/src/define/mod.rs:467`](../../../packages/workflow-core/src/define/mod.rs#L467)


***

### version

```rust
version: Option<String>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:468`](../../../packages/workflow-core/src/define/mod.rs#L468)


***

### description

```rust
description: Option<String>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:469`](../../../packages/workflow-core/src/define/mod.rs#L469)


***

### default_step_retry

```rust
default_step_retry: Option<RetryPolicy>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:472`](../../../packages/workflow-core/src/define/mod.rs#L472)

Fallback retry policy for steps that declare none
(TanStack `defaultStepRetry`). Per-step [`StepOptions::retry`](StepOptions.md) wins.


***

### middlewares

```rust
middlewares: Vec<Middleware>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:475`](../../../packages/workflow-core/src/define/mod.rs#L475)

Middleware list, in the order TanStack's `composeMiddlewares` chains
them (first = outermost wrapping of the typed handler).


***

### previous_versions

```rust
previous_versions: Vec<Workflow>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:479`](../../../packages/workflow-core/src/define/mod.rs#L479)

Older versions of the same workflow. Resume routes by the persisted
`workflow_version` to the matching entry (see
[`select_workflow_version`](../functions/select_workflow_version.md)).


***

### handler

```rust
handler: WorkflowHandler
```

Defined in: [`packages/workflow-core/src/define/mod.rs:480`](../../../packages/workflow-core/src/define/mod.rs#L480)


***

### initialize

```rust
initialize: Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:481`](../../../packages/workflow-core/src/define/mod.rs#L481)


***

### state_validator

```rust
state_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:482`](../../../packages/workflow-core/src/define/mod.rs#L482)


***

### output_validator

```rust
output_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Defined in: [`packages/workflow-core/src/define/mod.rs:484`](../../../packages/workflow-core/src/define/mod.rs#L484)

Shape-check for the handler's `Out` value (config `output` schema).

## Implementations

### new()

```rust
pub fn new<impl Into<String>: Into>(id: impl ?) -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:488`](../../../packages/workflow-core/src/define/mod.rs#L488)

#### Parameters

##### id

`impl ?`

#### Returns

`Self`


***

### version()

```rust
pub fn version<impl Into<String>: Into>(self, version: impl ?) -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:503`](../../../packages/workflow-core/src/define/mod.rs#L503)

#### Parameters

##### version

`impl ?`

#### Returns

`Self`


***

### description()

```rust
pub fn description<impl Into<String>: Into>(self, description: impl ?) -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:508`](../../../packages/workflow-core/src/define/mod.rs#L508)

#### Parameters

##### description

`impl ?`

#### Returns

`Self`


***

### default_step_retry()

```rust
pub fn default_step_retry(self, retry: RetryPolicy) -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:515`](../../../packages/workflow-core/src/define/mod.rs#L515)

Fallback retry policy for steps that declare no
[`StepOptions::retry`](StepOptions.md).

#### Parameters

##### retry

[`RetryPolicy`](RetryPolicy.md)

#### Returns

`Self`


***

### previous_versions()

```rust
pub fn previous_versions(self, v: Vec<Workflow>) -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:523`](../../../packages/workflow-core/src/define/mod.rs#L523)

Same as
[`WorkflowBuilder::previous_versions`](WorkflowBuilder.md) —
the erased-`Workflow` variant for engine-facing construction.

#### Parameters

##### v

`Vec<Workflow>`

#### Returns

`Self`


***

### initialize()

```rust
pub fn initialize<impl Fn(&serde_json::Value) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static: Fn(&Value) -> Result<Value> + Send + Sync + 'static>(self, f: impl ? + ? + ? + ?) -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:532`](../../../packages/workflow-core/src/define/mod.rs#L532)

Declare the initial per-invocation state, derived from the frozen run
input (TanStack `initialize`). Called on every invocation — start and
resume — and the handler re-runs from scratch, so mutations written
*outside* step closures re-execute deterministically.

#### Parameters

##### f

`impl ? + ? + ? + ?`

#### Returns

`Self`


***

### state_schema()

```rust
pub fn state_schema<T: DeserializeOwned + Send + Sync + 'static>(self) -> Self
```

Defined in: [`packages/workflow-core/src/define/mod.rs:545`](../../../packages/workflow-core/src/define/mod.rs#L545)

Declare a typed state (serde `Deserialize` type = schema). The initial
state built by [`initialize`](Workflow.md) is shape-checked against
`T` on every invocation; a mismatch errors the run (zod `.safeParse`
counterpart for `stateSchema`). Typed access at runtime is the `ctx.state`
field.

#### Returns

`Self`


***

### handler()

```rust
pub fn handler<F, Fut>(self, handler: F) -> Self
where
    F: Fn(WorkflowCtx) -> Fut + Send + Sync + 'static,
    Fut: Future + Send + 'static
```

Defined in: [`packages/workflow-core/src/define/mod.rs:555`](../../../packages/workflow-core/src/define/mod.rs#L555)

Installs the orchestrating closure. Branching/parallelism/order are
plain async code; only durable side effects go through `ctx.step`.

#### Parameters

##### handler

`F`

#### Returns

`Self`

## Trait Implementations

- `impl Borrow for Workflow`
- `impl BorrowMut for Workflow`
- `impl CloneToUninit for Workflow`
- `impl Into for Workflow`
- `impl From for Workflow`
- `impl TryInto for Workflow`
- `impl TryFrom for Workflow`
- `impl Any for Workflow`
- `impl ToOwned for Workflow`
- `impl Clone for Workflow`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

