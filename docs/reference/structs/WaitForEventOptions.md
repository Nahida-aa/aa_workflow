---
id: WaitForEventOptions
title: WaitForEventOptions
---

# Struct: WaitForEventOptions

Defined in: [`packages/workflow_core/src/define/mod.rs:556`](../../../packages/workflow_core/src/define/mod.rs#L556)

[`BaseCtx::wait_for_event_with`](BaseCtx.md) 的选项（对齐 TanStack
`WaitForEventOptions`，`types.ts:268-279`）。

## Fields

### deadline

```rust
deadline: Option<i64>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:559`](../../../packages/workflow_core/src/define/mod.rs#L559)

绝对 UTC ms 唤醒期限。落进 `StepPaused.due_at` 和
`RunState.waiting_for.deadline`，供 host 建时间索引的 worker job。


***

### meta

```rust
meta: Option<Value>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:561`](../../../packages/workflow_core/src/define/mod.rs#L561)

自由元数据，落进 `StepPaused.meta` 与 `RunState` 的三处 await 投影。


***

### schema

```rust
schema: Option<PayloadSchema>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:563`](../../../packages/workflow_core/src/define/mod.rs#L563)

恢复时对投递 payload 的形状校验（`schema`）。校验失败 → run 报错。

## Trait Implementations

- `impl Borrow for WaitForEventOptions`
- `impl BorrowMut for WaitForEventOptions`
- `impl CloneToUninit for WaitForEventOptions`
- `impl Into for WaitForEventOptions`
- `impl From for WaitForEventOptions`
- `impl TryInto for WaitForEventOptions`
- `impl TryFrom for WaitForEventOptions`
- `impl Any for WaitForEventOptions`
- `impl ToOwned for WaitForEventOptions`
- `impl Default for WaitForEventOptions`
- `impl Clone for WaitForEventOptions`
- `impl Debug for WaitForEventOptions`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

