---
id: EngineRuntime
title: EngineRuntime
---

# Struct: EngineRuntime

Defined in: [`packages/workflow_core/src/engine/mod.rs:236`](../../../packages/workflow_core/src/engine/mod.rs#L236)

## Fields

### run_id

```rust
run_id: String
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:237`](../../../packages/workflow_core/src/engine/mod.rs#L237)


***

### input

```rust
input: Value
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:238`](../../../packages/workflow_core/src/engine/mod.rs#L238)


***

### state_mirror

```rust
state_mirror: Arc<Mutex<Value>>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:247`](../../../packages/workflow_core/src/engine/mod.rs#L247)

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

Defined in: [`packages/workflow_core/src/engine/mod.rs:251`](../../../packages/workflow_core/src/engine/mod.rs#L251)

State as of the last flush — the "before" side of the
[`WorkflowEvent::StateDelta`](../enums/WorkflowEvent.md) diff. Reset at drive start (upstream
`prevStateSnapshot: snapshotState(state)`).


***

### store

```rust
store: Arc<dyn RunStore>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:252`](../../../packages/workflow_core/src/engine/mod.rs#L252)


***

### gate

```rust
gate: Arc<Gate>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:253`](../../../packages/workflow_core/src/engine/mod.rs#L253)


***

### compensations

```rust
compensations: Mutex<Vec<Compensation>>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:261`](../../../packages/workflow_core/src/engine/mod.rs#L261)

Phase-1 补偿登记（undo 轴）：handler 内 `ctx.compensate` 按代码确定性
重登记；失败路径取走并逆序执行（见 `run_compensations`）。


***

### pending_retries

```rust
pending_retries: Mutex<HashMap<String, PendingRetry>>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:264`](../../../packages/workflow_core/src/engine/mod.rs#L264)

F2-lite 耐久重试：驱动启动时从日志折叠的待重试状态（按 step）。
空表 = 无待重试。exec_step 据此泊车（未到期）或续跑（到期）。


***

### interim_goals

```rust
interim_goals: Option<Vec<String>>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:268`](../../../packages/workflow_core/src/engine/mod.rs#L268)

F2-lite 临时目标（INTERIM_GOAL）：名单内步骤全部 checkpoint 后经保留
信号 `__interim-goal` 泊车（run → paused，宿主以 recover 续驱扩展边界）。
转移检查防重挂；未到达的 goal 永不触发（best-effort fence）。


***

### deadline

```rust
deadline: Option<i64>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:278`](../../../packages/workflow_core/src/engine/mod.rs#L278)

Runtime budget (TanStack `deadline`): absolute UTC ms for this drive.


***

### min_yield_remaining_ms

```rust
min_yield_remaining_ms: u64
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:280`](../../../packages/workflow_core/src/engine/mod.rs#L280)

Headroom threshold for `should_yield()` (TanStack `minYieldRemainingMs`).


***

### yield_resume_at

```rust
yield_resume_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:282`](../../../packages/workflow_core/src/engine/mod.rs#L282)

Absolute ms at which `ctx.yield_()` re-wakes (TanStack `yieldResumeAt`).

_（存在非公开字段）_

## Implementations

### publish_progress()

```rust
pub fn publish_progress(&self, step_id: &str, value: f64)
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:377`](../../../packages/workflow_core/src/engine/mod.rs#L377)

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

