---
id: RunOutcome
title: RunOutcome
---

# Struct: RunOutcome

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:145`

## Fields

### `run_id`

```rust
run_id: String
```

### `status`

```rust
status: RunStatus
```

### `output`

```rust
output: Option<Value>
```

### `error`

```rust
error: Option<RunError>
```

与 [`RunState::error`](crate::run_store::RunState) 是**同一个类型**——
一次失败的 run 在「返回值」和「持久化信封」两处描述一致，不要只改一边。

`RunError` 实现了 [`Display`](std::fmt::Display)（转发 `message`），
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

