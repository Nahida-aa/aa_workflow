---
id: Workflow
title: Workflow
---

# Struct: Workflow

Defined in: `packages/workflow-core/src/define/mod.rs:466`

A declared workflow: id, optional version, the async handler, plus the
builder-derived extras (`description`, `default_step_retry`, `middlewares`,
`previous_versions`, `output_validator`) that TanStack carries on the
workflow object.

## Fields

### `id`

```rust
id: String
```

### `version`

```rust
version: Option<String>
```

### `description`

```rust
description: Option<String>
```

### `default_step_retry`

```rust
default_step_retry: Option<RetryPolicy>
```

Fallback retry policy for steps that declare none
(TanStack `defaultStepRetry`). Per-step [`StepOptions::retry`] wins.

### `middlewares`

```rust
middlewares: Vec<Middleware>
```

Middleware list, in the order TanStack's `composeMiddlewares` chains
them (first = outermost wrapping of the typed handler).

### `previous_versions`

```rust
previous_versions: Vec<Workflow>
```

Older versions of the same workflow. Resume routes by the persisted
`workflow_version` to the matching entry (see
[`select_workflow_version`](crate::engine::select_workflow_version)).

### `handler`

```rust
handler: WorkflowHandler
```

### `initialize`

```rust
initialize: Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>
```

### `state_validator`

```rust
state_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

### `output_validator`

```rust
output_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Shape-check for the handler's `Out` value (config `output` schema).

## Implementations

### `new`

```rust
pub fn new<impl Into<String>>(id: impl ?) -> Self
```

Defined in: `packages/workflow-core/src/define/mod.rs:488`

### `version`

```rust
pub fn version<impl Into<String>>(self, version: impl ?) -> Self
```

Defined in: `packages/workflow-core/src/define/mod.rs:503`

### `description`

```rust
pub fn description<impl Into<String>>(self, description: impl ?) -> Self
```

Defined in: `packages/workflow-core/src/define/mod.rs:508`

### `default_step_retry`

```rust
pub fn default_step_retry(self, retry: RetryPolicy) -> Self
```

Fallback retry policy for steps that declare no
[`StepOptions::retry`](StepOptions::retry).

Defined in: `packages/workflow-core/src/define/mod.rs:515`

### `previous_versions`

```rust
pub fn previous_versions(self, v: Vec<Workflow>) -> Self
```

Same as
[`WorkflowBuilder::previous_versions`](WorkflowBuilder::previous_versions) —
the erased-`Workflow` variant for engine-facing construction.

Defined in: `packages/workflow-core/src/define/mod.rs:523`

### `initialize`

```rust
pub fn initialize<impl Fn(&serde_json::Value) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static>(self, f: impl ? + ? + ? + ?) -> Self
```

Declare the initial per-invocation state, derived from the frozen run
input (TanStack `initialize`). Called on every invocation — start and
resume — and the handler re-runs from scratch, so mutations written
*outside* step closures re-execute deterministically.

Defined in: `packages/workflow-core/src/define/mod.rs:532`

### `state_schema`

```rust
pub fn state_schema<T>(self) -> Self
```

Declare a typed state (serde `Deserialize` type = schema). The initial
state built by [`initialize`](Self::initialize) is shape-checked against
`T` on every invocation; a mismatch errors the run (zod `.safeParse`
counterpart for `stateSchema`). Typed access at runtime is the `ctx.state`
field.

Defined in: `packages/workflow-core/src/define/mod.rs:545`

### `handler`

```rust
pub fn handler<F, Fut>(self, handler: F) -> Self
```

Installs the orchestrating closure. Branching/parallelism/order are
plain async code; only durable side effects go through `ctx.step`.

Defined in: `packages/workflow-core/src/define/mod.rs:555`

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

