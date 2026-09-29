---
id: create_workflow
title: create_workflow
---

# Function: create_workflow()

Defined in: [`packages/workflow-core/src/define/define_workflow.rs:167`](../../../packages/workflow-core/src/define/define_workflow.rs#L167)

Build a typed workflow the TanStack way: declaration config → builder →
`handler()`. `In`/`Out`/`St` come from the config's
[`input`](../structs/CreateWorkflowConfig.md)/[`output`](../structs/CreateWorkflowConfig.md)/[`state`](../structs/CreateWorkflowConfig.md) re-types; the
handler's output type is inferred from the closure return value.

```rust
pub fn create_workflow<TInput, TOutput, TState>(config: CreateWorkflowConfig<TInput, TOutput, TState>) -> WorkflowBuilder<TInput, TOutput, TState, ()>
```

## Type Parameters

### TInput

`TInput`

### TOutput

`TOutput`

### TState

`TState`

## Parameters

### config

[`CreateWorkflowConfig<TInput, TOutput, TState>`](../structs/CreateWorkflowConfig.md)

## Returns

[`WorkflowBuilder<TInput, TOutput, TState, ()>`](../structs/WorkflowBuilder.md)

