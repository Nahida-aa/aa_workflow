---
id: CreateWorkflowConfig
title: CreateWorkflowConfig
---

# Struct: CreateWorkflowConfig

Defined in: `packages/workflow-core/src/define/define_workflow.rs:25`

The declaration config consumed by [`create_workflow`]. Mirrors TanStack's
`createWorkflow(options)`; only `id` is required (`initialize` defaults to
`{}`). `input`/`state`/`output` re-type the config and install the matching
serde shape-checks (zod `.safeParse` counterparts); `version` is the string
this workflow's runs persist for version routing; `default_step_retry` is
the step retry fallback; `description` is metadata.

## Fields

### `id`

```rust
id: String
```

### `description`

```rust
description: Option<String>
```

### `version`

```rust
version: Option<String>
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

### `default_step_retry`

```rust
default_step_retry: Option<RetryPolicy>
```

### `handler`

```rust
handler: WorkflowHandler
```

### `_input`

```rust
_input: PhantomData<TInput>
```

### `_output`

```rust
_output: PhantomData<TOutput>
```

### `_state`

```rust
_state: PhantomData<TState>
```

## Implementations

### `new`

```rust
pub fn new<impl Into<String>>(id: impl ?) -> Self
```

Defined in: `packages/workflow-core/src/define/define_workflow.rs:40`

### `version`

```rust
pub fn version<impl Into<String>>(self, version: impl ?) -> Self
```

Defined in: `packages/workflow-core/src/define/define_workflow.rs:58`

### `description`

```rust
pub fn description<impl Into<String>>(self, description: impl ?) -> Self
```

Defined in: `packages/workflow-core/src/define/define_workflow.rs:63`

### `initialize`

```rust
pub fn initialize<impl Fn(&serde_json::Value) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static>(self, f: impl ? + ? + ? + ?) -> Self
```

`initialize({ input })` — rebuild the per-invocation state on every
start and resume (state is never persisted, see
[`Workflow::initialize`](Workflow::initialize)).

Defined in: `packages/workflow-core/src/define/define_workflow.rs:71`

### `default_step_retry`

```rust
pub fn default_step_retry(self, retry: RetryPolicy) -> Self
```

Fallback retry for steps that declare no retry of their own
(see [`StepOptions`](super::StepOptions)).

Defined in: `packages/workflow-core/src/define/define_workflow.rs:81`

### `input`

```rust
pub fn input<NewIn>(self) -> CreateWorkflowConfig<NewIn, TOutput, TState>
```

Declare the input schema as a serde type: re-types the config to
`CreateWorkflowConfig<In, _, _>` so `create_workflow` builds a
[`WorkflowBuilder<In, _, _>`](WorkflowBuilder). The handler's
`ctx.input` is `In`. Missing/mistyped input fields error the run
(zod `inputSchema` `.safeParse` counterpart).

Defined in: `packages/workflow-core/src/define/define_workflow.rs:91`

### `state`

```rust
pub fn state<NewSt>(self) -> CreateWorkflowConfig<TInput, TOutput, NewSt>
```

Declare the state schema as a serde type: the state built by
[`initialize`](Self::initialize) is shape-checked against `NewSt` on
every invocation, and the handler's `ctx.state` is `NewSt`.

Defined in: `packages/workflow-core/src/define/define_workflow.rs:113`

### `output`

```rust
pub fn output<NewOut>(self) -> CreateWorkflowConfig<TInput, NewOut, TState>
```

Declare the output schema as a serde type: re-types the config to
`CreateWorkflowConfig<_, NewOut, _>`; the handler's `Out` is validated
against `NewOut` (zod `.safeParse`) before the run finishes. Inferred
from the handler closure otherwise, exactly like TanStack — the schema
constrains but never declares.

Defined in: `packages/workflow-core/src/define/define_workflow.rs:140`

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

