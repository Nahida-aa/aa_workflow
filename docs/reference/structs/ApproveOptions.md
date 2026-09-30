---
id: ApproveOptions
title: ApproveOptions
---

# Struct: ApproveOptions

Defined in: [`packages/workflow_core/src/define/mod.rs:569`](../../../packages/workflow_core/src/define/mod.rs#L569)

[`BaseCtx::approve_with`](BaseCtx.md) 的选项（对齐 TanStack `ApproveOptions` +
`RunAwaitable` 的 approval 变体，`types.ts:307-312` / `:521-528`）。

## Fields

### deadline

```rust
deadline: Option<i64>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:571`](../../../packages/workflow_core/src/define/mod.rs#L571)

绝对 UTC ms 期限；`None` = 等人，无时间上限。


***

### meta

```rust
meta: Option<Value>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:573`](../../../packages/workflow_core/src/define/mod.rs#L573)

自由元数据（如审批单链接、金额、影响面），落进 checkpoint + 投影。


***

### schema

```rust
schema: Option<PayloadSchema>
```

Defined in: [`packages/workflow_core/src/define/mod.rs:575`](../../../packages/workflow_core/src/define/mod.rs#L575)

恢复时对审批决定 payload 的形状校验。

## Trait Implementations

- `impl Borrow for ApproveOptions`
- `impl BorrowMut for ApproveOptions`
- `impl CloneToUninit for ApproveOptions`
- `impl Into for ApproveOptions`
- `impl From for ApproveOptions`
- `impl TryInto for ApproveOptions`
- `impl TryFrom for ApproveOptions`
- `impl Any for ApproveOptions`
- `impl ToOwned for ApproveOptions`
- `impl Default for ApproveOptions`
- `impl Clone for ApproveOptions`
- `impl Debug for ApproveOptions`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

