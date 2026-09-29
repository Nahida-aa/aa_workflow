---
id: CreateWorkflowConfig
title: CreateWorkflowConfig
---

# Struct: CreateWorkflowConfig

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:25`](../../../packages/workflow_core/src/define/define_workflow.rs#L25)

The declaration config consumed by [`create_workflow`](../functions/create_workflow.md). Mirrors TanStack's
`createWorkflow(options)`; only `id` is required (`initialize` defaults to
`{}`). `input`/`state`/`output` re-type the config and install the matching
serde shape-checks (zod `.safeParse` counterparts); `version` is the string
this workflow's runs persist for version routing; `default_step_retry` is
the step retry fallback; `description` is metadata.

## Fields

### id

```rust
id: String
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:26`](../../../packages/workflow_core/src/define/define_workflow.rs#L26)


***

### description

```rust
description: Option<String>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:27`](../../../packages/workflow_core/src/define/define_workflow.rs#L27)


***

### version

```rust
version: Option<String>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:28`](../../../packages/workflow_core/src/define/define_workflow.rs#L28)


***

### initialize

```rust
initialize: Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:29`](../../../packages/workflow_core/src/define/define_workflow.rs#L29)


***

### state_validator

```rust
state_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:30`](../../../packages/workflow_core/src/define/define_workflow.rs#L30)


***

### output_validator

```rust
output_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:31`](../../../packages/workflow_core/src/define/define_workflow.rs#L31)


***

### default_step_retry

```rust
default_step_retry: Option<RetryPolicy>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:32`](../../../packages/workflow_core/src/define/define_workflow.rs#L32)


***

### handler

```rust
handler: WorkflowHandler
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:33`](../../../packages/workflow_core/src/define/define_workflow.rs#L33)


***

### _input

```rust
_input: PhantomData<TInput>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:34`](../../../packages/workflow_core/src/define/define_workflow.rs#L34)


***

### _output

```rust
_output: PhantomData<TOutput>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:35`](../../../packages/workflow_core/src/define/define_workflow.rs#L35)


***

### _state

```rust
_state: PhantomData<TState>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:36`](../../../packages/workflow_core/src/define/define_workflow.rs#L36)

## Implementations

### new()

```rust
pub fn new<impl Into<String>: Into>(id: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:40`](../../../packages/workflow_core/src/define/define_workflow.rs#L40)

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:58`](../../../packages/workflow_core/src/define/define_workflow.rs#L58)

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:63`](../../../packages/workflow_core/src/define/define_workflow.rs#L63)

#### Parameters

##### description

`impl ?`

#### Returns

`Self`


***

### initialize()

```rust
pub fn initialize<impl Fn(&serde_json::Value) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static: Fn(&Value) -> Result<Value> + Send + Sync + 'static>(self, f: impl ? + ? + ? + ?) -> Self
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:71`](../../../packages/workflow_core/src/define/define_workflow.rs#L71)

`initialize({ input })` — rebuild the per-invocation state on every
start and resume (state is never persisted, see
[`Workflow::initialize`](Workflow.md)).

#### Parameters

##### f

`impl ? + ? + ? + ?`

#### Returns

`Self`


***

### default_step_retry()

```rust
pub fn default_step_retry(self, retry: RetryPolicy) -> Self
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:81`](../../../packages/workflow_core/src/define/define_workflow.rs#L81)

Fallback retry for steps that declare no retry of their own
(see [`StepOptions`](StepOptions.md)).

#### Parameters

##### retry

[`RetryPolicy`](RetryPolicy.md)

#### Returns

`Self`


***

### input()

```rust
pub fn input<NewIn>(self) -> CreateWorkflowConfig<NewIn, TOutput, TState>
where
    NewIn: DeserializeOwned + Send + Sync + 'static
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:91`](../../../packages/workflow_core/src/define/define_workflow.rs#L91)

Declare the input schema as a serde type: re-types the config to
`CreateWorkflowConfig<NewIn, TOutput, TState>` so `create_workflow` builds a
[`WorkflowBuilder<NewIn, TOutput, TState>`](WorkflowBuilder.md). The handler's
`ctx.input` is `NewIn`. Missing/mistyped input fields error the run
(zod `inputSchema` `.safeParse` counterpart).

#### Returns

[`CreateWorkflowConfig<NewIn, TOutput, TState>`](CreateWorkflowConfig.md)


***

### state()

```rust
pub fn state<NewSt>(self) -> CreateWorkflowConfig<TInput, TOutput, NewSt>
where
    NewSt: DeserializeOwned + Send + Sync + 'static
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:113`](../../../packages/workflow_core/src/define/define_workflow.rs#L113)

Declare the state schema as a serde type: the state built by
[`initialize`](CreateWorkflowConfig.md) is shape-checked against `NewSt` on
every invocation, and the handler's `ctx.state` is `NewSt`.

#### Returns

[`CreateWorkflowConfig<TInput, TOutput, NewSt>`](CreateWorkflowConfig.md)


***

### output()

```rust
pub fn output<NewOut>(self) -> CreateWorkflowConfig<TInput, NewOut, TState>
where
    NewOut: DeserializeOwned + Send + Sync + 'static
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:140`](../../../packages/workflow_core/src/define/define_workflow.rs#L140)

Declare the output schema as a serde type: re-types the config to
`CreateWorkflowConfig<_, NewOut, _>`; the handler's `TOutput` is validated
against `NewOut` (zod `.safeParse`) before the run finishes. Inferred
from the handler closure otherwise, exactly like TanStack — the schema
constrains but never declares.

#### Returns

[`CreateWorkflowConfig<TInput, NewOut, TState>`](CreateWorkflowConfig.md)

## Trait Implementations

- `impl Borrow for CreateWorkflowConfig`
- `impl BorrowMut for CreateWorkflowConfig`
- `impl Into for CreateWorkflowConfig`
- `impl From for CreateWorkflowConfig`
- `impl TryInto for CreateWorkflowConfig`
- `impl TryFrom for CreateWorkflowConfig`
- `impl Any for CreateWorkflowConfig`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

