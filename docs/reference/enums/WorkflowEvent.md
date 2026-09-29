---
id: WorkflowEvent
title: WorkflowEvent
---

# Enum: WorkflowEvent

Defined in: `packages/workflow-core/src/event.rs:53`

Append-only persisted events. Each event is immutable; the log is the
engine's source of truth and doubling as the resume checkpoint.

## Variants

### `RunStarted`

```rust
{ .. }
```

Observability only (not persisted): marks the start of a run.

### `RunFinished`

```rust
{ .. }
```

Checkpoint (persisted): final state of a completed run.

### `RunErrored`

```rust
{ .. }
```

Checkpoint (persisted): the run errored (a step failed terminally).

### `StepStarted`

```rust
{ .. }
```

Observability only (not persisted): a step began executing.

### `StepFinished`

```rust
{ .. }
```

Checkpoint (persisted): a step finished successfully; the run may be
resumed from here without re-execution.

### `StepFailed`

```rust
{ .. }
```

Checkpoint (persisted): a step failed terminally.

### `StepPaused`

```rust
{ .. }
```

Checkpoint (persisted): the run parked at a durable wait point
(`ctx.approve` / `ctx.sleep` / `ctx.sleep_until` / `ctx.wait_for_event`).
`step_id` is the deterministic pause key (== the signal id
[`signal_run`](crate::engine::signal_run) / [`signal_event`](crate::engine::signal_event)
targets); `signal_name` is the channel the run is parked on
(`"__timer"` for sleeps, `"__approval"` for approvals, a user event
name for named waits). `due_at` is `Some` (wall-clock ms) for sleep-like
waits so a timer host could auto-deliver, `None` for waits that need an
external decision.

### `StepResume`

```rust
{ .. }
```

Checkpoint (persisted): a signal was delivered for a paused run
(see [`signal_run`](crate::engine::signal_run)). Resolves the pending
`StepPaused` with the same `step_id`; `payload` is what the
`approve`/`sleep` call returns.

### `StepProgress`

```rust
{ .. }
```

Observability only (not persisted): 0.0..=1.0 progress signal.

### `Custom`

```rust
{ .. }
```

Observability only (not persisted): `ctx.emit` fan-out. Reaches only
the publisher, never the log — so it never becomes part of replay
(mirrors TanStack's `CUSTOM` event).

### `NowRecorded`

```rust
{ .. }
```

Checkpoint (persisted): `ctx.now()` recorded a wall-clock timestamp.
On replay the cached value is served so a run sees the same clock
across resumes (TanStack `NOW_RECORDED`).

### `UuidRecorded`

```rust
{ .. }
```

Checkpoint (persisted): `ctx.uuid()` recorded a generated id. On
replay the same id is served (TanStack `UUID_RECORDED`).

### `StateDelta`

```rust
{ .. }
```

Observability only (not persisted): RFC 6902 JSON Patch describing
`ctx.state` changes since the last durable boundary (TanStack
`STATE_DELTA`).

**emit-only** 的理由（上游注释原话）：state 由日志重放推导，持久化
delta 会在每次 invocation 重放时重复 append，或者需要重放时跳过。
delta 语义见 [`crate::engine::state_diff`]。

## Implementations

### `step_id`

```rust
pub fn step_id(&self) -> Option<&str>
```

Defined in: `packages/workflow-core/src/event.rs:169`

### `ts`

```rust
pub fn ts(&self) -> i64
```

事件发生时间（每个变体都带 `ts`，对齐 TS 侧 `event.ts`）。

Defined in: `packages/workflow-core/src/event.rs:188`

### `type_name`

```rust
pub fn type_name(&self) -> &'static str
```

对齐 TS 侧 `event.type`）。

Defined in: `packages/workflow-core/src/event.rs:207`

## Trait Implementations

- `impl Borrow for WorkflowEvent`
- `impl BorrowMut for WorkflowEvent`
- `impl CloneToUninit for WorkflowEvent`
- `impl Into for WorkflowEvent`
- `impl From for WorkflowEvent`
- `impl TryInto for WorkflowEvent`
- `impl TryFrom for WorkflowEvent`
- `impl Any for WorkflowEvent`
- `impl ToOwned for WorkflowEvent`
- `impl DeserializeOwned for WorkflowEvent`
- `impl Debug for WorkflowEvent`
- `impl Clone for WorkflowEvent`
- `impl Serialize for WorkflowEvent`
- `impl Deserialize for WorkflowEvent`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

