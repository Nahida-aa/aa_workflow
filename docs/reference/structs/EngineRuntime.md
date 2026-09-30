---
id: EngineRuntime
title: EngineRuntime
---

# Struct: EngineRuntime

Defined in: [`packages/workflow_core/src/engine/mod.rs:126`](../../../packages/workflow_core/src/engine/mod.rs#L126)

Shared driver state handed to every step (and to the `<WorkflowCtx>`).
This is the code-as-DAG substrate: the "graph" is just this state plus the
handler's control flow, discovered as the handler runs.

事件扇出通道直接传 `WorkflowEvent`：收尾**靠关通道**（`run_workflow` 返回的
[`RunEventStream`](crate::RunEventStream) 靠 `poll_recv` 见到 `None` 结束），
不需要显式的 shutdown 哨兵——`EngineRuntime` 随引擎 future 一起 drop，
它是最后一个 sender。

## Fields

### run_id

```rust
run_id: String
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:127`](../../../packages/workflow_core/src/engine/mod.rs#L127)


***

### input

```rust
input: Value
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:128`](../../../packages/workflow_core/src/engine/mod.rs#L128)


***

### state_mirror

```rust
state_mirror: Arc<Mutex<Value>>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:137`](../../../packages/workflow_core/src/engine/mod.rs#L137)

Per-invocation state, rebuilt from `initialize(input)` on every start
and resume (see `define::WorkflowDefinition::initialize`). Guarded by a
`std::sync::RwLock`; it is the live image that `BaseCtx::state` (the
handler's field working copy) snapshots at drive start and flushes back
to before every durable primitive. Never persisted.
Engine-side serialized view of the workflow state, **shared with the
handler's `StateHandle`**（handle 在变更语句与 drop 时同步进这里）。
`STATE_DELTA` 的 diff 读它；不含 `prev_state_snapshot` 的历史。


***

### prev_state_snapshot

```rust
prev_state_snapshot: Mutex<Value>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:141`](../../../packages/workflow_core/src/engine/mod.rs#L141)

State as of the last flush — the "before" side of the
[`WorkflowEvent::StateDelta`](../enums/WorkflowEvent.md) diff. Reset at drive start (upstream
`prevStateSnapshot: snapshotState(state)`).


***

### store

```rust
store: Arc<dyn RunStore>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:142`](../../../packages/workflow_core/src/engine/mod.rs#L142)


***

### gate

```rust
gate: Arc<Gate>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:143`](../../../packages/workflow_core/src/engine/mod.rs#L143)


***

### target_step

```rust
target_step: Option<String>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:149`](../../../packages/workflow_core/src/engine/mod.rs#L149)


***

### deadline

```rust
deadline: Option<i64>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:157`](../../../packages/workflow_core/src/engine/mod.rs#L157)

Runtime budget (TanStack `deadline`): absolute UTC ms for this drive.


***

### min_yield_remaining_ms

```rust
min_yield_remaining_ms: u64
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:159`](../../../packages/workflow_core/src/engine/mod.rs#L159)

Headroom threshold for `should_yield()` (TanStack `minYieldRemainingMs`).


***

### yield_resume_at

```rust
yield_resume_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:161`](../../../packages/workflow_core/src/engine/mod.rs#L161)

Absolute ms at which `ctx.yield_()` re-wakes (TanStack `yieldResumeAt`).

_（存在非公开字段）_

## Implementations

### publish_progress()

```rust
pub fn publish_progress(&self, step_id: &str, value: f64)
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:256`](../../../packages/workflow_core/src/engine/mod.rs#L256)

#### Parameters

##### step_id

`&str`

##### value

`f64`

## Trait Implementations

- `impl Borrow for EngineRuntime`
- `impl BorrowMut for EngineRuntime`
- `impl Into for EngineRuntime`
- `impl From for EngineRuntime`
- `impl TryInto for EngineRuntime`
- `impl TryFrom for EngineRuntime`
- `impl Any for EngineRuntime`

## Auto Trait Implementations

`Send` `Sync` `Unpin` `UnsafeUnpin`

