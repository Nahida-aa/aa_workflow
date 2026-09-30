---
id: WorkflowEvent
title: WorkflowEvent
---

# Enum: WorkflowEvent

Defined in: [`packages/workflow_core/src/event.rs:53`](../../../packages/workflow_core/src/event.rs#L53)

Append-only persisted events. Each event is immutable; the log is the
engine's source of truth and doubling as the resume checkpoint.

## Variants

### RunStarted

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:55`](../../../packages/workflow_core/src/event.rs#L55)

Observability only (not persisted): marks the start of a run.


***

### RunFinished

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:57`](../../../packages/workflow_core/src/event.rs#L57)

Checkpoint (persisted): final state of a completed run.


***

### RunErrored

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:63`](../../../packages/workflow_core/src/event.rs#L63)

Checkpoint (persisted): the run errored (a step failed terminally).


***

### StepStarted

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:72`](../../../packages/workflow_core/src/event.rs#L72)

Observability only (not persisted): a step began executing.


***

### StepFinished

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:79`](../../../packages/workflow_core/src/event.rs#L79)

Checkpoint (persisted): a step finished successfully; the run may be
resumed from here without re-execution.


***

### StepFailed

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:87`](../../../packages/workflow_core/src/event.rs#L87)

Checkpoint (persisted): a step failed terminally.


***

### StepPaused

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:103`](../../../packages/workflow_core/src/event.rs#L103)

Checkpoint (persisted): the run parked at a durable wait point
(`ctx.approve` / `ctx.sleep` / `ctx.sleep_until` / `ctx.wait_for_event`).
`step_id` is the deterministic pause key (== the signal id
[`signal_run`](../functions/signal_run.md) / [`signal_event`](../functions/signal_event.md)
targets); `signal_name` is the channel the run is parked on
(`"__timer"` for sleeps, `"__approval"` for approvals, a user event
name for named waits). `due_at` is `Some` (wall-clock ms) for sleep-like
waits so a timer host could auto-deliver, `None` for waits that need an
external decision.


***

### StepResume

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:117`](../../../packages/workflow_core/src/event.rs#L117)

Checkpoint (persisted): a signal was delivered for a paused run
(see [`signal_run`](../functions/signal_run.md)). Resolves the pending
`StepPaused` with the same `step_id`; `payload` is what the
`approve`/`sleep` call returns.


***

### StepProgress

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:124`](../../../packages/workflow_core/src/event.rs#L124)

Observability only (not persisted): 0.0..=1.0 progress signal.


***

### Custom

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:133`](../../../packages/workflow_core/src/event.rs#L133)

Observability only (not persisted): `ctx.emit` fan-out. Reaches only
the publisher, never the log — so it never becomes part of replay
(mirrors TanStack's `CUSTOM` event).


***

### NowRecorded

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:142`](../../../packages/workflow_core/src/event.rs#L142)

Checkpoint (persisted): `ctx.now()` recorded a wall-clock timestamp.
On replay the cached value is served so a run sees the same clock
across resumes (TanStack `NOW_RECORDED`).


***

### UuidRecorded

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:150`](../../../packages/workflow_core/src/event.rs#L150)

Checkpoint (persisted): `ctx.uuid()` recorded a generated id. On
replay the same id is served (TanStack `UUID_RECORDED`).


***

### StateDelta

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:163`](../../../packages/workflow_core/src/event.rs#L163)

Observability only (not persisted): RFC 6902 JSON Patch describing
`ctx.state` changes since the last durable boundary (TanStack
`STATE_DELTA`).

**emit-only** 的理由（上游注释原话）：state 由日志重放推导，持久化
delta 会在每次 invocation 重放时重复 append，或者需要重放时跳过。
delta 语义见 `crate::engine::state_diff`。

## Implementations

### step_id()

```rust
pub fn step_id(&self) -> Option<&str>
```

Defined in: [`packages/workflow_core/src/event.rs:171`](../../../packages/workflow_core/src/event.rs#L171)

#### Returns

`Option<&str>`


***

### ts()

```rust
pub fn ts(&self) -> i64
```

Defined in: [`packages/workflow_core/src/event.rs:190`](../../../packages/workflow_core/src/event.rs#L190)

事件发生时间（每个变体都带 `ts`，对齐 TS 侧 `event.ts`）。

#### Returns

`i64`


***

### type_name()

```rust
pub fn type_name(&self) -> &'static str
```

Defined in: [`packages/workflow_core/src/event.rs:209`](../../../packages/workflow_core/src/event.rs#L209)

对齐 TS 侧 `event.type`）。

#### Returns

`&'static str`

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

