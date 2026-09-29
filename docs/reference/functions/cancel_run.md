---
id: cancel_run
title: cancel_run
---

# Function: cancel_run()

Defined in: [`packages/workflow-core/src/engine/mod.rs:99`](../../../packages/workflow-core/src/engine/mod.rs#L99)

Cancels a live or parked run: flips its state to terminal `Aborted`. The
engine spots it on its next step/attempt boundary and returns
[`WorkflowCancelled`](../structs/WorkflowCancelled.md), so the drive ends `Aborted` rather than `Errored`.
A **parked** run is not running at all, so nothing is watching: the flag is
only observed when a driver re-invokes `run_workflow`, which then ends the
run `Aborted` at its first boundary. A second cancel on an already-terminal
run is a no-op `Ok(())`.

```rust
pub fn cancel_run(store: &dyn RunStore, run_id: &str) -> Result<(), WorkflowError>
```

## Parameters

### store

`&dyn RunStore`

### run_id

`&str`

## Returns

`Result<(), WorkflowError>`

