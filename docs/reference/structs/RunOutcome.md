---
id: RunOutcome
title: RunOutcome
---

# Struct: RunOutcome

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:145`](../../../packages/workflow_core/src/engine/run_workflow.rs#L145)

## Fields

### run_id

```rust
run_id: String
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:146`](../../../packages/workflow_core/src/engine/run_workflow.rs#L146)


***

### status

```rust
status: RunStatus
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:147`](../../../packages/workflow_core/src/engine/run_workflow.rs#L147)


***

### output

```rust
output: Option<Value>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:148`](../../../packages/workflow_core/src/engine/run_workflow.rs#L148)


***

### error

```rust
error: Option<RunError>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:154`](../../../packages/workflow_core/src/engine/run_workflow.rs#L154)

与 [`RunState::error`](RunState.md) 是**同一个类型**——
一次失败的 run 在「返回值」和「持久化信封」两处描述一致，不要只改一边。

`RunError` 实现了 `Display`（转发 `message`），
所以 `warn!("{e}")` / `e.to_string()` 照常可用；想要错误名用 `.name`。

## Trait Implementations

- `impl Borrow for RunOutcome`
- `impl BorrowMut for RunOutcome`
- `impl Into for RunOutcome`
- `impl From for RunOutcome`
- `impl TryInto for RunOutcome`
- `impl TryFrom for RunOutcome`
- `impl Any for RunOutcome`
- `impl Debug for RunOutcome`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

