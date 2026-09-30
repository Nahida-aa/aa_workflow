---
id: DeleteReason
title: DeleteReason
---

# Enum: DeleteReason

Defined in: [`packages/workflow_core/src/run_store/mod.rs:215`](../../../packages/workflow_core/src/run_store/mod.rs#L215)

删除 run 的原因（对齐 TanStack `DeleteReason`，`types.ts:576`）。Store
可据此决定保留策略：比如终态归档 vs 中途废弃走不同的清理路径。

## Variants

### Finished

Defined in: [`packages/workflow_core/src/run_store/mod.rs:216`](../../../packages/workflow_core/src/run_store/mod.rs#L216)


***

### Errored

Defined in: [`packages/workflow_core/src/run_store/mod.rs:217`](../../../packages/workflow_core/src/run_store/mod.rs#L217)


***

### Aborted

Defined in: [`packages/workflow_core/src/run_store/mod.rs:218`](../../../packages/workflow_core/src/run_store/mod.rs#L218)

## Implementations

### as_str()

```rust
pub fn as_str(&self) -> &'static str
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:222`](../../../packages/workflow_core/src/run_store/mod.rs#L222)

#### Returns

`&'static str`

## Trait Implementations

- `impl Borrow for DeleteReason`
- `impl BorrowMut for DeleteReason`
- `impl CloneToUninit for DeleteReason`
- `impl Into for DeleteReason`
- `impl From for DeleteReason`
- `impl TryInto for DeleteReason`
- `impl TryFrom for DeleteReason`
- `impl Any for DeleteReason`
- `impl ToOwned for DeleteReason`
- `impl DeserializeOwned for DeleteReason`
- `impl Debug for DeleteReason`
- `impl Clone for DeleteReason`
- `impl Copy for DeleteReason`
- `impl StructuralPartialEq for DeleteReason`
- `impl PartialEq for DeleteReason`
- `impl Eq for DeleteReason`
- `impl Serialize for DeleteReason`
- `impl Deserialize for DeleteReason`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

