---
id: signal_run
title: signal_run
---

# Function: signal_run()

```rust
pub fn signal_run(store: &dyn RunStore, run_id: &str, step_id: &str, payload: Value) -> Result<(), WorkflowError>
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:676`](../../../packages/workflow-core/src/engine/mod.rs#L676)

Appends a `StepResume` for a parked run — the external side of
[`WorkflowCtx::approve`](../structs/BaseCtx.md). Safe to call
any time: nothing is parked *watching* the log (the drive already returned),
so this takes effect on the **next** `run_workflow` invocation, which
resolves the wait from the log and never re-appends its `StepPaused`.
Delivering the resume is the caller's job — that is the runtime driver's
whole reason to exist (see `aa-workflow-runtime`'s `deliver_signal`). The first
append per `(run_id, step_id)` wins; duplicates drop via the store's CAS
append. `payload` is what `approve` returns on the resumed drive.

## Parameters

### store

`&dyn RunStore`

### run_id

`&str`

### step_id

`&str`

### payload

`Value`

## Returns

`Result<(), WorkflowError>`

