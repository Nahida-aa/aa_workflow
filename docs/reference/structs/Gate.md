---
id: Gate
title: Gate
---

# Struct: Gate

Defined in: `packages/workflow-core/src/resource.rs:20`

Per-key async semaphore, used to serialize steps that share a physical
resource (halving VRAM pressure in LocalDub). `acquire` waits rather than
failing, because in the code-as-DAG model the handler has nowhere to defer
a contended step — it must either proceed or block.

_（存在非公开字段）_

## Implementations

### `new`

```rust
pub fn new() -> Self
```

Defined in: `packages/workflow-core/src/resource.rs:25`

### `acquire`

```rust
pub async fn acquire(self: &Arc<Self>, key: &ResourceKey) -> GateGuard
```

Async acquire of one unit for `key`, waiting until the previous holder
releases. The returned [`GateGuard`] (one permit) is released on drop,
before the step's checkpoint is appended.

Defined in: `packages/workflow-core/src/resource.rs:41`

## Trait Implementations

- `impl Borrow for Gate`
- `impl BorrowMut for Gate`
- `impl Into for Gate`
- `impl From for Gate`
- `impl TryInto for Gate`
- `impl TryFrom for Gate`
- `impl Any for Gate`
- `impl Default for Gate`

## Auto Trait Implementations

`RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

