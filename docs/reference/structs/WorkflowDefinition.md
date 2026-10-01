---
id: WorkflowDefinition
title: WorkflowDefinition
---

# Struct: WorkflowDefinition

Defined in: [`packages/workflow_core/src/define/mod.rs:592`](../../../packages/workflow_core/src/define/mod.rs#L592)

A declared workflow: id, optional version, the async handler, plus the
builder-derived extras (`description`, `default_step_retry`, `middlewares`,
`previous_versions`, `output_validator`) that TanStack carries on the
workflow object.

Mirrors TanStack's `WorkflowDefinition<TInput, TOutput, TState>`
(`types.ts:442-460`) member-for-member — **including the generics**, which is
the whole point: upstream's erased alias (`AnyWorkflowDefinition`) and its
typed form are the same type, and here they are too, just spelled out.

Every type parameter is defaulted to the erased form, so a **bare
`WorkflowDefinition` is the type-erased workflow** — what the engine and
every run site see. See [`AnyWorkflowDefinition`](AnyWorkflowDefinition.md) for the run-site spelling.

## Fields

### id

```rust
id: String
```

Defined in: [`packages/workflow_core/src/define/mod.rs:598`](../../../packages/workflow_core/src/define/mod.rs#L598)


***

### version

```rust
version: Option<String>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:599`](../../../packages/workflow_core/src/define/mod.rs#L599)


***

### description

```rust
description: Option<String>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:600`](../../../packages/workflow_core/src/define/mod.rs#L600)


***

### default_step_retry

```rust
default_step_retry: Option<RetryPolicy>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:603`](../../../packages/workflow_core/src/define/mod.rs#L603)

Fallback retry policy for steps that declare none
(TanStack `defaultStepRetry`). Per-step [`StepOptions::retry`](StepOptions.md) wins.


***

### middlewares

```rust
middlewares: Vec<Middleware>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:606`](../../../packages/workflow_core/src/define/mod.rs#L606)

Middleware list, in the order TanStack's `composeMiddlewares` chains
them (first = outermost wrapping of the typed handler).


***

### previous_versions

```rust
previous_versions: Vec<AnyWorkflowDefinition>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:610`](../../../packages/workflow_core/src/define/mod.rs#L610)

Older versions of the same workflow. Resume routes by the persisted
`workflow_version` to the matching entry (see
[`select_workflow_version`](../functions/select_workflow_version.md)).


***

### handler

```rust
handler: WorkflowHandler
```

Defined in: [`packages/workflow_core/src/define/mod.rs:611`](../../../packages/workflow_core/src/define/mod.rs#L611)


***

### initialize

```rust
initialize: Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:612`](../../../packages/workflow_core/src/define/mod.rs#L612)


***

### input_validator

```rust
input_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:618`](../../../packages/workflow_core/src/define/mod.rs#L618)

Shape-check for the run input (`config.inputSchema` counterpart,
installed by `CreateWorkflowConfig::input::<T>()`). Checked by the
engine **before** `initialize` and the handler, so bad input fails the
run with [`RunErrorCode::Validation`] instead of surfacing later as a
generic handler error.


***

### state_validator

```rust
state_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:619`](../../../packages/workflow_core/src/define/mod.rs#L619)


***

### output_validator

```rust
output_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:621`](../../../packages/workflow_core/src/define/mod.rs#L621)

Shape-check for the handler's `TOutput` value (config `output` schema).

_（存在非公开字段）_

## Implementations

### new()

```rust
pub fn new<impl Into<String>: Into>(id: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:768`](../../../packages/workflow_core/src/define/mod.rs#L768)

The erased workflow: no `TInput`/`TOutput`/`TState`/`TCtxExt`. This is
the engine-facing constructor (it replaces the old standalone `Workflow`
type); a *typed* definition comes from
[`create_workflow`](../functions/create_workflow.md)(...).handler(...), which
pins the parameters from the declaration site.

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

Defined in: [`packages/workflow_core/src/define/mod.rs:788`](../../../packages/workflow_core/src/define/mod.rs#L788)

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

Defined in: [`packages/workflow_core/src/define/mod.rs:793`](../../../packages/workflow_core/src/define/mod.rs#L793)

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

Defined in: [`packages/workflow_core/src/define/mod.rs:800`](../../../packages/workflow_core/src/define/mod.rs#L800)

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
pub fn previous_versions<impl Into<AnyWorkflowDefinition>: Into, impl IntoIterator<Item = impl Into<AnyWorkflowDefinition>>: IntoIterator>(self, v: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:808`](../../../packages/workflow_core/src/define/mod.rs#L808)

Same as
[`WorkflowBuilder::previous_versions`](WorkflowBuilder.md) —
the direct-construction variant for engine-facing code.

#### Parameters

##### v

`impl ?`

#### Returns

`Self`


***

### initialize()

```rust
pub fn initialize<impl Fn(&serde_json::Value) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static: Fn(&Value) -> Result<Value> + Send + Sync + 'static>(self, f: impl ? + ? + ? + ?) -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:820`](../../../packages/workflow_core/src/define/mod.rs#L820)

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

Defined in: [`packages/workflow_core/src/define/mod.rs:833`](../../../packages/workflow_core/src/define/mod.rs#L833)

Declare a typed state (serde `Deserialize` type = schema). The initial
state built by [`initialize`](WorkflowDefinition.md) is shape-checked against
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

Defined in: [`packages/workflow_core/src/define/mod.rs:843`](../../../packages/workflow_core/src/define/mod.rs#L843)

Installs the orchestrating closure. Branching/parallelism/order are
plain async code; only durable side effects go through `ctx.step`.

#### Parameters

##### handler

`F`

#### Returns

`Self`

## Trait Implementations

- `impl Borrow for WorkflowDefinition`
- `impl BorrowMut for WorkflowDefinition`
- `impl CloneToUninit for WorkflowDefinition`
- `impl Into for WorkflowDefinition`
- `impl From for WorkflowDefinition`
- `impl TryInto for WorkflowDefinition`
- `impl TryFrom for WorkflowDefinition`
- `impl Any for WorkflowDefinition`
- `impl ToOwned for WorkflowDefinition`
- `impl Clone for WorkflowDefinition`
- `impl From for WorkflowDefinition`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

