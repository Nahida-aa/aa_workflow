---
id: exec_pause
title: exec_pause
---

# Function: exec_pause

Defined in: `packages/workflow-core/src/engine/mod.rs:529`

Durable wait implemented by [`WorkflowCtx::approve`](crate::define::WorkflowCtx::approve)
/ [`WorkflowCtx::sleep`](crate::define::WorkflowCtx::sleep) /
[`WorkflowCtx::wait_for_event`](crate::define::WorkflowCtx::wait_for_event).
`step_id` is the pause key == signal id; it must not collide with any step
id. `signal_name` is the channel the run parks on (`"__approval"` for
approvals, `"__timer"` for sleeps, a user event name for named waits).
`deadline` is an **absolute** UTC ms timestamp, stored verbatim (it is what
a timer sweep indexes on) — `None` for waits with no time bound.

Replay fast path: a `StepResume` already in the log resolves immediately,
so a crashed-paused run re-waits exactly once and never re-appends its
`StepPaused`. Otherwise the pause checkpoint is persisted (idempotently)
and the run state flips to `Paused` + `waiting_for` / `pending_approval`.

**Then it parks**: the function returns [`WorkflowParked`] immediately — it
does NOT block. The drive ends `Paused`, the process is free to exit, and an
external driver is what delivers the wake-up (a `StepResume` via
[`signal_run`] / [`signal_event`], or a timer sweep noticing `deadline`).
Mirrors TanStack, where the awaited primitive throws `WorkflowPaused` and
the drive returns (`run-workflow.ts:970` / `:529`).

## Signature

```rust
pub async fn exec_pause(inner: &Arc<EngineRuntime>, step_id: &str, signal_name: &str, reason: &str, deadline: Option<i64>) -> Result<Value>
```

