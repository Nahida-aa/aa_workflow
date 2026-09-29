---
id: fold_step_states
title: fold_step_states
---

# Function: fold_step_states

Defined in: `packages/workflow-core/src/event.rs:248`

Replays an event log and folds it into per-step states.

This is the resume mechanism: the log's terminal checkpoints
(`StepFinished`/`StepFailed`) fully determine which steps still need to
run. No application code is re-executed here.

## Signature

```rust
pub fn fold_step_states(events: &[WorkflowEvent]) -> HashMap<String, StepState>
```

