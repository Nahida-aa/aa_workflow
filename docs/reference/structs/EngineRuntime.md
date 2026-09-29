---
id: EngineRuntime
title: EngineRuntime
---

# Struct: EngineRuntime

Defined in: `packages/workflow-core/src/engine/mod.rs:121`

Shared driver state handed to every step (and to the `<WorkflowCtx>`).
This is the code-as-DAG substrate: the "graph" is just this state plus the
handler's control flow, discovered as the handler runs.

## Fields

### `run_id`

```rust
run_id: String
```

### `input`

```rust
input: Value
```

### `state_mirror`

```rust
state_mirror: Arc<Mutex<Value>>
```

Per-invocation state, rebuilt from `initialize(input)` on every start
and resume (see `define::Workflow::initialize`). Guarded by a
`std::sync::RwLock`; it is the live image that `BaseCtx::state` (the
handler's field working copy) snapshots at drive start and flushes back
to before every durable primitive. Never persisted.
Engine-side serialized view of the workflow state, **shared with the
handler's `StateHandle`**（handle 在变更语句与 drop 时同步进这里）。
`STATE_DELTA` 的 diff 读它；不含 `prev_state_snapshot` 的历史。

### `prev_state_snapshot`

```rust
prev_state_snapshot: Mutex<Value>
```

State as of the last flush — the "before" side of the
[`WorkflowEvent::StateDelta`] diff. Reset at drive start (upstream
`prevStateSnapshot: snapshotState(state)`).

### `store`

```rust
store: Arc<dyn RunStore>
```

### `gate`

```rust
gate: Arc<Gate>
```

### `target_step`

```rust
target_step: Option<String>
```

### `deadline`

```rust
deadline: Option<i64>
```

Runtime budget (TanStack `deadline`): absolute UTC ms for this drive.

### `min_yield_remaining_ms`

```rust
min_yield_remaining_ms: u64
```

Headroom threshold for `should_yield()` (TanStack `minYieldRemainingMs`).

### `yield_resume_at`

```rust
yield_resume_at: Option<i64>
```

Absolute ms at which `ctx.yield_()` re-wakes (TanStack `yieldResumeAt`).

### `publisher`

```rust
publisher: Option<Arc<dyn Fn(&WorkflowEvent) + Send + Sync>>
```

_（存在非公开字段）_

## Implementations

### `publish_progress`

```rust
pub fn publish_progress(&self, step_id: &str, value: f64)
```

Defined in: `packages/workflow-core/src/engine/mod.rs:225`

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

