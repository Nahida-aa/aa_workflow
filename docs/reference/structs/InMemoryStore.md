---
id: InMemoryStore
title: InMemoryStore
---

# Struct: InMemoryStore

Defined in: [`packages/workflow-core/src/run_store/in_memory.rs:28`](../../../packages/workflow-core/src/run_store/in_memory.rs#L28)

In-memory [`RunStore`](../traits/RunStore.md) for phase 0. Provides the full contract surface
(CAS append + subscribe) so the engine can be exercised with property
tests before a real filesystem store is written.

_（存在非公开字段）_

## Implementations

### new()

```rust
pub fn new() -> Self
```

Defined in: [`packages/workflow-core/src/run_store/in_memory.rs:33`](../../../packages/workflow-core/src/run_store/in_memory.rs#L33)

#### Returns

`Self`

## Trait Implementations

- `impl Borrow for InMemoryStore`
- `impl BorrowMut for InMemoryStore`
- `impl Into for InMemoryStore`
- `impl From for InMemoryStore`
- `impl TryInto for InMemoryStore`
- `impl TryFrom for InMemoryStore`
- `impl Any for InMemoryStore`
- `impl Default for InMemoryStore`
- `impl RunStore for InMemoryStore`

## Auto Trait Implementations

`RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

