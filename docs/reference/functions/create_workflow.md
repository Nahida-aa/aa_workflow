---
id: create_workflow
title: create_workflow
---

# Function: create_workflow

Defined in: `packages/workflow-core/src/define/define_workflow.rs:167`

Build a typed workflow the TanStack way: declaration config → builder →
`handler()`. `In`/`Out`/`St` come from the config's
[`input`](CreateWorkflowConfig::input)/[`output`](CreateWorkflowConfig::output)/[`state`](CreateWorkflowConfig::state) re-types; the
handler's output type is inferred from the closure return value.

## Signature

```rust
pub fn create_workflow<TInput, TOutput, TState>(config: CreateWorkflowConfig<TInput, TOutput, TState>) -> WorkflowBuilder<TInput, TOutput, TState, ()>
```

