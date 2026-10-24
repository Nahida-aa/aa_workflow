---
id: signal_run
title: signal_run
---

# Function: signal_run()

```rust
pub fn signal_run(store: &dyn RunStore, run_id: &str, step_id: &str, payload: Value) -> Result<(), WorkflowError>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:1073`](../../../packages/workflow_core/src/engine/mod.rs#L1073)

Appends a `SignalResolved` for a parked run — the external side of
[`WorkflowCtx::approve`](../structs/BaseCtx.md). Safe to call
any time: nothing is parked *watching* the log (the drive already returned),
so this takes effect on the **next** `run_workflow` invocation, which
resolves the wait from the log and never re-appends its `SignalAwaited`.
Delivering the resume is the caller's job — that is the runtime driver's
whole reason to exist (see `aa_workflow_runtime`'s `deliver_signal`). The first
append per `(run_id, step_id)` wins; duplicates drop via the store's CAS
append. `payload` is what `approve` returns on the resumed drive.
Appends a `SignalResolved` for `step_id` — the step_id-targeted form of
[`signal_event`](signal_event.md).

**过闸**：只在该 step 确实泊在信号等待上时才落盘；否则报
[`WorkflowError::SignalLost`](../enums/WorkflowError.md)。对齐上游默认语义（没在等就 `not-waiting`，
不静默写日志）。「早到信号」由 runtime 的可选收件箱承接
（`buffer_signal` + `buffer_signals` 开关），**不是**靠这里无条件 append。

上游的幂等令牌不在这条路上：`signal_id` 记为 `None`，去重由 store 负责
（`signal_deliveries`）。要把它记进日志，走带名的 [`signal_event`](signal_event.md)。

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

