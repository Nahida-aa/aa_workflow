---
id: CreateWorkflowConfig
title: CreateWorkflowConfig
---

# Struct: CreateWorkflowConfig

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:24`](../../../packages/workflow_core/src/define/define_workflow.rs#L24)

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:25`](../../../packages/workflow_core/src/define/define_workflow.rs#L25)


***

### description

```rust
description: Option<String>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:26`](../../../packages/workflow_core/src/define/define_workflow.rs#L26)


***

### version

```rust
version: Option<String>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:27`](../../../packages/workflow_core/src/define/define_workflow.rs#L27)


***

### initialize

```rust
initialize: Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:28`](../../../packages/workflow_core/src/define/define_workflow.rs#L28)


***

### state_validator

```rust
state_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:29`](../../../packages/workflow_core/src/define/define_workflow.rs#L29)


***

### output_validator

```rust
output_validator: Option<Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:30`](../../../packages/workflow_core/src/define/define_workflow.rs#L30)


***

### default_step_retry

```rust
default_step_retry: Option<RetryPolicy>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:31`](../../../packages/workflow_core/src/define/define_workflow.rs#L31)


***

### handler

```rust
handler: WorkflowHandler
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:32`](../../../packages/workflow_core/src/define/define_workflow.rs#L32)


***

### _input

```rust
_input: PhantomData<TInput>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:33`](../../../packages/workflow_core/src/define/define_workflow.rs#L33)


***

### _output

```rust
_output: PhantomData<TOutput>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:34`](../../../packages/workflow_core/src/define/define_workflow.rs#L34)


***

### _state

```rust
_state: PhantomData<TState>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:35`](../../../packages/workflow_core/src/define/define_workflow.rs#L35)

## Implementations

### new()

```rust
pub fn new<impl Into<String>: Into>(id: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:39`](../../../packages/workflow_core/src/define/define_workflow.rs#L39)

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:57`](../../../packages/workflow_core/src/define/define_workflow.rs#L57)

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:62`](../../../packages/workflow_core/src/define/define_workflow.rs#L62)

#### Parameters

##### description

`impl ?`

#### Returns

`Self`


***

### initialize()

```rust
pub fn initialize<impl Fn(&TInput) -> anyhow::Result<TState> + Send + Sync + 'static: Fn(&TInput) -> Result<TState> + Send + Sync + 'static>(self, f: impl ? + ? + ? + ?) -> Self
where
    TInput: DeserializeOwned,
    TState: Serialize
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:75`](../../../packages/workflow_core/src/define/define_workflow.rs#L75)

`initialize({ input })` — rebuild the per-invocation state on every
start and resume (state is never persisted, see
[`WorkflowDefinition::initialize`](WorkflowDefinition.md)).

typed：闭包吃 `&TInput`、产 `TState`（对齐上游——`TState` 由
`.state::<T>()` 声明、编译期钉死）。内部 serde 桥接到运行时的擦除
表示（`WorkflowDefinition` 持 `Fn(&Value) -> Result<Value>`）；input 反序列化
失败 / state 序列化失败都算 run 错误。

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:93`](../../../packages/workflow_core/src/define/define_workflow.rs#L93)

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:103`](../../../packages/workflow_core/src/define/define_workflow.rs#L103)

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:125`](../../../packages/workflow_core/src/define/define_workflow.rs#L125)

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

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:152`](../../../packages/workflow_core/src/define/define_workflow.rs#L152)

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

