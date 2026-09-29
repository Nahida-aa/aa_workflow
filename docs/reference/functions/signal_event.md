---
id: signal_event
title: signal_event
---

# Function: signal_event

Defined in: `packages/workflow-core/src/engine/mod.rs:704`

Appends a `StepResume` for the run currently parked waiting for the event
`event_name` — the external side of
[`WorkflowCtx::wait_for_event`](crate::define::WorkflowCtx::wait_for_event).
Locates the paused step via its `signal_name` (the channel, not the pause
key), so a host needs only the event name. Fails with
[`WorkflowError::SignalLost`] when nothing is parked on that name.

## Signature

```rust
pub fn signal_event(store: &dyn RunStore, run_id: &str, event_name: &str, payload: Value) -> Result<(), WorkflowError>
```

