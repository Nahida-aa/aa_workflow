---
id: run_workflow
title: run_workflow
---

# Function: run_workflow()

```rust
pub async fn run_workflow(opts: &RunWorkflowOptions) -> Result<RunOutcome, WorkflowError>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:176`](../../../packages/workflow_core/src/engine/run_workflow.rs#L176)

Runs (or resumes) a workflow by driving its async handler.

Inputs:
- `continue_from`: the store truncates everything at `step_id`'s latest
  terminal checkpoint (inclusive) before re-running the handler, so the
  prefix short-circuits and the suffix re-executes from scratch.
- `target_step`: once that step succeeds the engine raises [`StepHalt`](../structs/StepHalt.md),
  the handler unwinds (users should propagate with `?`), and the run ends
  `Finished` with no output.

On resume, `ctx.step` short-circuits succeeded checkpoints (cached result,
`run` not re-executed) and rethrows failed ones. Multiplex step results
however you like — the log is the only source of truth.

## Parameters

### opts

`&RunWorkflowOptions`

## Returns

`Result<RunOutcome, WorkflowError>`

