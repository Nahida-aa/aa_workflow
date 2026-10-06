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

Defined in: [`packages/workflow_core/src/event.rs:65`](../../../packages/workflow_core/src/event.rs#L65)

Checkpoint (persisted): final state of a completed run.


***

### RunErrored

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:71`](../../../packages/workflow_core/src/event.rs#L71)

Checkpoint (persisted): the run errored (a step failed terminally).


***

### StepStarted

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:80`](../../../packages/workflow_core/src/event.rs#L80)

Observability only (not persisted): a step began executing.


***

### StepFinished

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:87`](../../../packages/workflow_core/src/event.rs#L87)

Checkpoint (persisted): a step finished successfully; the run may be
resumed from here without re-execution.


***

### StepFailed

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:95`](../../../packages/workflow_core/src/event.rs#L95)

Checkpoint (persisted): a step failed terminally.


***

### StepPaused

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:111`](../../../packages/workflow_core/src/event.rs#L111)

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

Defined in: [`packages/workflow_core/src/event.rs:125`](../../../packages/workflow_core/src/event.rs#L125)

Checkpoint (persisted): a signal was delivered for a paused run
(see [`signal_run`](../functions/signal_run.md)). Resolves the pending
`StepPaused` with the same `step_id`; `payload` is what the
`approve`/`sleep` call returns.


***

### StepProgress

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:132`](../../../packages/workflow_core/src/event.rs#L132)

Observability only (not persisted): 0.0..=1.0 progress signal.


***

### Custom

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:141`](../../../packages/workflow_core/src/event.rs#L141)

Observability only (not persisted): `ctx.emit` fan-out. Reaches only
the publisher, never the log — so it never becomes part of replay
(mirrors TanStack's `CUSTOM` event).


***

### NowRecorded

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:150`](../../../packages/workflow_core/src/event.rs#L150)

Checkpoint (persisted): `ctx.now()` recorded a wall-clock timestamp.
On replay the cached value is served so a run sees the same clock
across resumes (TanStack `NOW_RECORDED`).


***

### UuidRecorded

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:158`](../../../packages/workflow_core/src/event.rs#L158)

Checkpoint (persisted): `ctx.uuid()` recorded a generated id. On
replay the same id is served (TanStack `UUID_RECORDED`).


***

### StateDelta

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:171`](../../../packages/workflow_core/src/event.rs#L171)

Observability only (not persisted): RFC 6902 JSON Patch describing
`ctx.state` changes since the last durable boundary (TanStack
`STATE_DELTA`).

**emit-only** 的理由（上游注释原话）：state 由日志重放推导，持久化
delta 会在每次 invocation 重放时重复 append，或者需要重放时跳过。
delta 语义见 `crate::engine::state_diff`。


***

### CompensationStarted

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:179`](../../../packages/workflow_core/src/event.rs#L179)

Checkpoint (persisted): 未捕获失败触发了补偿——原始失败随 `error`
持久化，已登记的撤销 handler 将逆序执行。崩溃后恢复的驱动据此得知
"补偿进行中"。


***

### CompensationCompleted

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:186`](../../../packages/workflow_core/src/event.rs#L186)

Checkpoint (persisted): 一个已登记的撤销 handler 成功完成——重启后
该补偿不再重复执行（幂等键 = 补偿 id）。


***

### CompensationFailed

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:193`](../../../packages/workflow_core/src/event.rs#L193)

Checkpoint (persisted): 一个撤销 handler 耗尽重试——补偿链就地停止
（后续登记的补偿保持未执行，留给人工处置）。


***

### StepRetryScheduled

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:203`](../../../packages/workflow_core/src/event.rs#L203)

Checkpoint (persisted)：耐久重试（#19 `retry.durable`）——步骤失败后
登记下次重试的绝对截止时间，run 挂起（`__timer` + deadline），到期的
sweep/重驱以此恢复。重放规则：本事件后该步骤视同未完成（attempts 随
事件携带，耗尽前的 STEP_FAILED 不落日志）。

## Implementations

### step_id()

```rust
pub fn step_id(&self) -> Option<&str>
```

Defined in: [`packages/workflow_core/src/event.rs:217`](../../../packages/workflow_core/src/event.rs#L217)

#### Returns

`Option<&str>`


***

### ts()

```rust
pub fn ts(&self) -> i64
```

Defined in: [`packages/workflow_core/src/event.rs:240`](../../../packages/workflow_core/src/event.rs#L240)

事件发生时间（每个变体都带 `ts`，对齐 TS 侧 `event.ts`）。

#### Returns

`i64`


***

### type_name()

```rust
pub fn type_name(&self) -> &'static str
```

Defined in: [`packages/workflow_core/src/event.rs:263`](../../../packages/workflow_core/src/event.rs#L263)

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

