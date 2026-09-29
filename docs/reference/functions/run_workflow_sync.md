---
id: run_workflow_sync
title: run_workflow_sync
---

# Function: run_workflow_sync()

```rust
pub fn run_workflow_sync(opts: &RunWorkflowOptions) -> Result<RunOutcome, WorkflowError>
```

Defined in: [`packages/workflow-core/src/engine/run_workflow.rs:424`](../../../packages/workflow-core/src/engine/run_workflow.rs#L424)

Sync convenience over a local multi-thread runtime for callers that are
not async themselves (e.g. LocalDub's CLI entrypoint).

## Parameters

### opts

`&RunWorkflowOptions`

## Returns

`Result<RunOutcome, WorkflowError>`

