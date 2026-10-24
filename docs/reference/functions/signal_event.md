---
id: signal_event
title: signal_event
---

# Function: signal_event()

```rust
pub fn signal_event(store: &dyn RunStore, run_id: &str, event_name: &str, signal_id: Option<&str>, payload: Value, meta: Option<Value>) -> Result<(), WorkflowError>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:1219`](../../../packages/workflow_core/src/engine/mod.rs#L1219)

Appends a `SignalResolved` for the run currently parked waiting for the
event `event_name` — the external side of
[`WorkflowCtx::wait_for_event`](../structs/BaseCtx.md).
Locates the paused step via its channel `name` (not the pause key), so a
host needs only the event name. Fails with
[`WorkflowError::SignalLost`](../enums/WorkflowError.md) when nothing is parked on that name.

`signal_id` is the host-supplied idempotency token (upstream's rule: same
id at the same step is a no-op, a different one is a lost race). It is
recorded on the event **and** deduplicated by the store.

## Parameters

### store

`&dyn RunStore`

### run_id

`&str`

### event_name

`&str`

### signal_id

`Option<&str>`

### payload

`Value`

### meta

`Option<Value>`

## Returns

`Result<(), WorkflowError>`

