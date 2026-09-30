---
id: signal_event
title: signal_event
---

# Function: signal_event()

```rust
pub fn signal_event(store: &dyn RunStore, run_id: &str, event_name: &str, payload: Value) -> Result<(), WorkflowError>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:868`](../../../packages/workflow_core/src/engine/mod.rs#L868)

Appends a `StepResume` for the run currently parked waiting for the event
`event_name` — the external side of
[`WorkflowCtx::wait_for_event`](../structs/BaseCtx.md).
Locates the paused step via its `signal_name` (the channel, not the pause
key), so a host needs only the event name. Fails with
[`WorkflowError::SignalLost`](../enums/WorkflowError.md) when nothing is parked on that name.

## Parameters

### store

`&dyn RunStore`

### run_id

`&str`

### event_name

`&str`

### payload

`Value`

## Returns

`Result<(), WorkflowError>`

