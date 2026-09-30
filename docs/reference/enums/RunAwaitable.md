---
id: RunAwaitable
title: RunAwaitable
---

# Enum: RunAwaitable

Defined in: [`packages/workflow_core/src/run_store/mod.rs:64`](../../../packages/workflow_core/src/run_store/mod.rs#L64)

一个挂起中的等待（对齐 TanStack `RunAwaitable`，`types.ts:517-529`）。

这是 `RunState.awaiting` 的元素类型，也是**规范形**（canonical）：数组 +
`type` 判别式。上游 `run-workflow.ts:950` / `:1059` 挂起时**同时**写
`awaiting` 和两个镜像字段 `waitingFor` / `pendingApproval`；后者是给「只关心
当前这一个等待」的观察者看的便利视图，本类型才是完整表达。

为什么是数组而当前只有一个元素：上游 `types.ts:548-551` 明说
“Current engine versions only create one awaitable at a time, but the
persisted shape can represent future fan-out/race primitives **without
replacing the run schema**”。本仓同样一次 drive 恒定一个 awaitable
（`exec_pause` 直接 `Err(WorkflowParked)`，第一个 park 就短路，见
`docs/tanstack-alignment.md`「实测：为什么『每次 drive 一个 awaitable』成立」），
但把形状对齐成数组，将来上 fan-out / race 原语时只加元素、不改 schema。

## Variants

### Signal

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:65`](../../../packages/workflow_core/src/run_store/mod.rs#L65)


***

### Approval

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:74`](../../../packages/workflow_core/src/run_store/mod.rs#L74)

## Trait Implementations

- `impl Borrow for RunAwaitable`
- `impl BorrowMut for RunAwaitable`
- `impl CloneToUninit for RunAwaitable`
- `impl Into for RunAwaitable`
- `impl From for RunAwaitable`
- `impl TryInto for RunAwaitable`
- `impl TryFrom for RunAwaitable`
- `impl Any for RunAwaitable`
- `impl ToOwned for RunAwaitable`
- `impl DeserializeOwned for RunAwaitable`
- `impl Debug for RunAwaitable`
- `impl Clone for RunAwaitable`
- `impl StructuralPartialEq for RunAwaitable`
- `impl PartialEq for RunAwaitable`
- `impl Serialize for RunAwaitable`
- `impl Deserialize for RunAwaitable`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

