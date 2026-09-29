---
id: run_workflow_sync
title: run_workflow_sync
---

# Function: run_workflow_sync

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:424`

Sync convenience over a local multi-thread runtime for callers that are
not async themselves (e.g. LocalDub's CLI entrypoint).

## Signature

```rust
pub fn run_workflow_sync(opts: &RunWorkflowOptions) -> Result<RunOutcome, WorkflowError>
```

