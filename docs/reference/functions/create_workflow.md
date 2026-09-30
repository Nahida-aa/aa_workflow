---
id: create_workflow
title: create_workflow
---

# Function: create_workflow()

```rust
pub fn create_workflow<TInput, TOutput, TState>(config: CreateWorkflowConfig<TInput, TOutput, TState>) -> WorkflowBuilder<TInput, TOutput, TState, ()>
```

Defined in: [`packages/workflow_core/src/define/define_workflow.rs:200`](../../../packages/workflow_core/src/define/define_workflow.rs#L200)

Define a workflow. Returns a builder chain:

```rust,ignore
let onboard = create_workflow(
    CreateWorkflowConfig::new("onboard").input::<OnboardInput>(),
)
.middleware(traced)
.handler(|ctx| async move {
    let profile = ctx.step("load", |_| async { load_profile(&ctx.input.user_id) }).await?;
    ctx.sleep("cooloff", std::time::Duration::from_secs(60)).await?;
    let decision = ctx.approve("continue", "Continue?").await?;
    Ok(serde_json::json!({ "ok": decision["approved"] }))
});
```

The handler's `ctx` argument (a [`BaseCtx`](../structs/BaseCtx.md)) carries everything: the
typed input, state, durable primitives ([`step`](../structs/BaseCtx.md),
[`sleep`](../structs/BaseCtx.md), [`approve`](../structs/BaseCtx.md), ...), and any
fields added by registered middleware (`ctx.ext`). Helpers should accept
a typed `BaseCtx<...>` argument to compose cleanly.

`TInput`/`TOutput`/`TState` are pinned by the config's
[`input`](../structs/CreateWorkflowConfig.md) / [`output`](../structs/CreateWorkflowConfig.md)
/ [`state`](../structs/CreateWorkflowConfig.md) methods; the handler's output type
is inferred from the closure return value.

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

