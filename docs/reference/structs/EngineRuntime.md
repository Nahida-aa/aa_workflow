---
id: EngineRuntime
title: EngineRuntime
---

# Struct: EngineRuntime

Defined in: [`packages/workflow-core/src/engine/mod.rs:121`](../../../packages/workflow-core/src/engine/mod.rs#L121)

Shared driver state handed to every step (and to the `<WorkflowCtx>`).
This is the code-as-DAG substrate: the "graph" is just this state plus the
handler's control flow, discovered as the handler runs.

## Fields

### run_id

```rust
run_id: String
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:122`](../../../packages/workflow-core/src/engine/mod.rs#L122)


***

### input

```rust
input: Value
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:123`](../../../packages/workflow-core/src/engine/mod.rs#L123)


***

### state_mirror

```rust
state_mirror: Arc<Mutex<Value>>
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:132`](../../../packages/workflow-core/src/engine/mod.rs#L132)

Per-invocation state, rebuilt from `initialize(input)` on every start
and resume (see `define::Workflow::initialize`). Guarded by a
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

Defined in: [`packages/workflow-core/src/engine/mod.rs:136`](../../../packages/workflow-core/src/engine/mod.rs#L136)

State as of the last flush — the "before" side of the
[`WorkflowEvent::StateDelta`](../enums/WorkflowEvent.md) diff. Reset at drive start (upstream
`prevStateSnapshot: snapshotState(state)`).


***

### store

```rust
store: Arc<dyn RunStore>
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:137`](../../../packages/workflow-core/src/engine/mod.rs#L137)


***

### gate

```rust
gate: Arc<Gate>
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:138`](../../../packages/workflow-core/src/engine/mod.rs#L138)


***

### target_step

```rust
target_step: Option<String>
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:144`](../../../packages/workflow-core/src/engine/mod.rs#L144)


***

### deadline

```rust
deadline: Option<i64>
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:152`](../../../packages/workflow-core/src/engine/mod.rs#L152)

Runtime budget (TanStack `deadline`): absolute UTC ms for this drive.


***

### min_yield_remaining_ms

```rust
min_yield_remaining_ms: u64
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:154`](../../../packages/workflow-core/src/engine/mod.rs#L154)

Headroom threshold for `should_yield()` (TanStack `minYieldRemainingMs`).


***

### yield_resume_at

```rust
yield_resume_at: Option<i64>
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:156`](../../../packages/workflow-core/src/engine/mod.rs#L156)

Absolute ms at which `ctx.yield_()` re-wakes (TanStack `yieldResumeAt`).


***

### publisher

```rust
publisher: Option<Arc<dyn Fn(&WorkflowEvent) + Send + Sync>>
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:159`](../../../packages/workflow-core/src/engine/mod.rs#L159)

_（存在非公开字段）_

## Implementations

### publish_progress()

```rust
pub fn publish_progress(&self, step_id: &str, value: f64)
```

Defined in: [`packages/workflow-core/src/engine/mod.rs:225`](../../../packages/workflow-core/src/engine/mod.rs#L225)

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

