---
id: PendingApproval
title: PendingApproval
---

# Struct: PendingApproval

Defined in: [`packages/workflow_core/src/run_store/mod.rs:32`](../../../packages/workflow_core/src/run_store/mod.rs#L32)

挂起中的审批（对齐 TanStack `RunState.pendingApproval`）。我们的
`approve` 用 key 作 `approval_id`；`title` 即挂起时给的理由。

## Fields

### step_id

```rust
step_id: Option<String>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:34`](../../../packages/workflow_core/src/run_store/mod.rs#L34)


***

### approval_id

```rust
approval_id: String
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:35`](../../../packages/workflow_core/src/run_store/mod.rs#L35)


***

### title

```rust
title: String
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:36`](../../../packages/workflow_core/src/run_store/mod.rs#L36)


***

### description

```rust
description: Option<String>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:37`](../../../packages/workflow_core/src/run_store/mod.rs#L37)

## Trait Implementations

- `impl Borrow for PendingApproval`
- `impl BorrowMut for PendingApproval`
- `impl CloneToUninit for PendingApproval`
- `impl Into for PendingApproval`
- `impl From for PendingApproval`
- `impl TryInto for PendingApproval`
- `impl TryFrom for PendingApproval`
- `impl Any for PendingApproval`
- `impl ToOwned for PendingApproval`
- `impl DeserializeOwned for PendingApproval`
- `impl Debug for PendingApproval`
- `impl Clone for PendingApproval`
- `impl Serialize for PendingApproval`
- `impl Deserialize for PendingApproval`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

