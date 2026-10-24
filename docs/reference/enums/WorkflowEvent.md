---
id: WorkflowEvent
title: WorkflowEvent
---

# Enum: WorkflowEvent

Defined in: [`packages/workflow_core/src/event.rs:72`](../../../packages/workflow_core/src/event.rs#L72)

Append-only persisted events. Each event is immutable; the log is the
engine's source of truth and doubling as the resume checkpoint.

## Variants

### RunStarted

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:74`](../../../packages/workflow_core/src/event.rs#L74)

Observability only (not persisted): marks the start of a run.


***

### RunFinished

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:84`](../../../packages/workflow_core/src/event.rs#L84)

Checkpoint (persisted): final state of a completed run.


***

### RunErrored

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:90`](../../../packages/workflow_core/src/event.rs#L90)

Checkpoint (persisted): the run errored (a step failed terminally).


***

### StepStarted

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:99`](../../../packages/workflow_core/src/event.rs#L99)

Observability only (not persisted): a step began executing.


***

### StepFinished

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:106`](../../../packages/workflow_core/src/event.rs#L106)

Checkpoint (persisted): a step finished successfully; the run may be
resumed from here without re-execution.


***

### StepFailed

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:118`](../../../packages/workflow_core/src/event.rs#L118)

Checkpoint (persisted): a step failed terminally.


***

### SignalAwaited

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:132`](../../../packages/workflow_core/src/event.rs#L132)

Checkpoint (persisted): the run parked on a **named signal** wait
(`ctx.wait_for_event` / `ctx.sleep*`). Aligns with upstream
`SIGNAL_AWAITED` (`types.ts`, 字段逐字对应)。

`name` is the channel the run is parked on — `"__timer"` for sleeps,
a user event name for named waits. `deadline` is `Some` (wall-clock
ms) for sleep-like waits so a timer host could auto-deliver.


***

### ApprovalRequested

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:147`](../../../packages/workflow_core/src/event.rs#L147)

Checkpoint (persisted): the run parked on an **approval** wait
(`ctx.approve`). Aligns with upstream `APPROVAL_REQUESTED`.

`approval_id` is the deterministic pause key (== the approval id the
host delivers a decision for); `title` is what approvers see.


***

### SignalResolved

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:166`](../../../packages/workflow_core/src/event.rs#L166)

Checkpoint (persisted): a signal resolved the matching
[`SignalAwaited`](WorkflowEvent.md). Aligns with upstream
`SIGNAL_RESOLVED`.

`signal_id` is the host-supplied idempotency token — upstream's rule:
same `signal_id` at the same `step_id` is a no-op (idempotent retry),
a different one is a lost race. `payload` is what the
`wait_for_event` call returns.


***

### ApprovalResolved

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:184`](../../../packages/workflow_core/src/event.rs#L184)

Checkpoint (persisted): an approval decision resolved the matching
[`ApprovalRequested`](WorkflowEvent.md). Aligns with
upstream `APPROVAL_RESOLVED`.

上游这里**没有** payload 字段——审批是结构化决定（`approved` +
`feedback`），任意 payload 只属于 [`SignalResolved`]。见
`docs/tanstack-alignment.md` 的事件分类对照。


***

### StepProgress

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:196`](../../../packages/workflow_core/src/event.rs#L196)

Observability only (not persisted): 0.0..=1.0 progress signal.


***

### Custom

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:205`](../../../packages/workflow_core/src/event.rs#L205)

Observability only (not persisted): `ctx.emit` fan-out. Reaches only
the publisher, never the log — so it never becomes part of replay
(mirrors TanStack's `CUSTOM` event).


***

### NowRecorded

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:214`](../../../packages/workflow_core/src/event.rs#L214)

Checkpoint (persisted): `ctx.now()` recorded a wall-clock timestamp.
On replay the cached value is served so a run sees the same clock
across resumes (TanStack `NOW_RECORDED`).


***

### UuidRecorded

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:222`](../../../packages/workflow_core/src/event.rs#L222)

Checkpoint (persisted): `ctx.uuid()` recorded a generated id. On
replay the same id is served (TanStack `UUID_RECORDED`).


***

### StateDelta

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:235`](../../../packages/workflow_core/src/event.rs#L235)

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

Defined in: [`packages/workflow_core/src/event.rs:243`](../../../packages/workflow_core/src/event.rs#L243)

Checkpoint (persisted): 未捕获失败触发了补偿——原始失败随 `error`
持久化，已登记的撤销 handler 将逆序执行。崩溃后恢复的驱动据此得知
"补偿进行中"。


***

### CompensationCompleted

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:250`](../../../packages/workflow_core/src/event.rs#L250)

Checkpoint (persisted): 一个已登记的撤销 handler 成功完成——重启后
该补偿不再重复执行（幂等键 = 补偿 id）。


***

### CompensationFailed

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:257`](../../../packages/workflow_core/src/event.rs#L257)

Checkpoint (persisted): 一个撤销 handler 耗尽重试——补偿链就地停止
（后续登记的补偿保持未执行，留给人工处置）。


***

### StepRetryScheduled

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/event.rs:267`](../../../packages/workflow_core/src/event.rs#L267)

Checkpoint (persisted)：耐久重试（#19 `retry.durable`）——步骤失败后
登记下次重试的绝对截止时间，run 挂起（`__timer` + deadline），到期的
sweep/重驱以此恢复。重放规则：本事件后该步骤视同未完成（attempts 随
事件携带，耗尽前的 STEP_FAILED 不落日志）。

## Implementations

### step_id()

```rust
pub fn step_id(&self) -> Option<&str>
```

Defined in: [`packages/workflow_core/src/event.rs:281`](../../../packages/workflow_core/src/event.rs#L281)

#### Returns

`Option<&str>`


***

### ts()

```rust
pub fn ts(&self) -> i64
```

Defined in: [`packages/workflow_core/src/event.rs:306`](../../../packages/workflow_core/src/event.rs#L306)

事件发生时间（每个变体都带 `ts`，对齐 TS 侧 `event.ts`）。

#### Returns

`i64`


***

### type_name()

```rust
pub fn type_name(&self) -> &'static str
```

Defined in: [`packages/workflow_core/src/event.rs:331`](../../../packages/workflow_core/src/event.rs#L331)

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

