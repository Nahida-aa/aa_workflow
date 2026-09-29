---
id: StoreError
title: StoreError
---

# Enum: StoreError

Defined in: [`packages/workflow_core/src/error.rs:7`](../../../packages/workflow_core/src/error.rs#L7)

Errors produced by a [`crate::run_store::RunStore`](../traits/RunStore.md) implementation.

## Variants

### Conflict

```rust
{ .. }
```

Defined in: [`packages/workflow_core/src/error.rs:9`](../../../packages/workflow_core/src/error.rs#L9)

`append_event` was called with a stale expected index (CAS violation).


***

### Io

```rust
(String)
```

Defined in: [`packages/workflow_core/src/error.rs:15`](../../../packages/workflow_core/src/error.rs#L15)

Backing-store I/O or serialization failure.

## Trait Implementations

- `impl Borrow for StoreError`
- `impl BorrowMut for StoreError`
- `impl CloneToUninit for StoreError`
- `impl Into for StoreError`
- `impl From for StoreError`
- `impl TryInto for StoreError`
- `impl TryFrom for StoreError`
- `impl Any for StoreError`
- `impl ToOwned for StoreError`
- `impl ToString for StoreError`
- `impl Debug for StoreError`
- `impl Clone for StoreError`
- `impl StructuralPartialEq for StoreError`
- `impl PartialEq for StoreError`
- `impl Eq for StoreError`
- `impl Display for StoreError`
- `impl Error for StoreError`
- `impl From for StoreError`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

