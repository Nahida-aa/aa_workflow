---
id: run_workflow
title: run_workflow
---

# Function: run_workflow()

```rust
pub fn run_workflow(opts: RunWorkflowOptions) -> RunEventStream
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:386`](../../../packages/workflow_core/src/engine/run_workflow.rs#L386)

Runs (or resumes) a workflow by driving its async handler.

Inputs:
- `target_step`: once that step succeeds the engine raises [`StepHalt`](../structs/StepHalt.md),
  the handler unwinds (users should propagate with `?`), and the run ends
  `Finished` with no output.

On resume, `ctx.step` short-circuits succeeded checkpoints (cached result,
`run` not re-executed) and rethrows failed ones. Multiplex step results
however you like — the log is the only source of truth.

## Parameters

### opts

[`RunWorkflowOptions`](../structs/RunWorkflowOptions.md)

## Returns

`RunEventStream`

