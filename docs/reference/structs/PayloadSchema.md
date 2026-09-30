---
id: PayloadSchema
title: PayloadSchema
---

# Struct: PayloadSchema

Defined in: [`packages/workflow_core/src/define/mod.rs:531`](../../../packages/workflow_core/src/define/mod.rs#L531)

一个具名的 payload 形状声明。由 [`PayloadSchema::of::<T>`](PayloadSchema.md) 构造。

上游用 `StandardSchemaV1`（zod/valibot 通用接口）；Rust 侧没有那种运行时
schema 生态，最接近的既有物是 `serde` 的 `Deserialize`（和
`WorkflowDefinition::state_schema::<T>()` 同一个约定：`T` 就是 schema）。

_（存在非公开字段）_

## Implementations

### of()

```rust
pub fn of<T: DeserializeOwned + Send + Sync + 'static>() -> Self
```

Defined in: [`packages/workflow_core/src/define/mod.rs:537`](../../../packages/workflow_core/src/define/mod.rs#L537)

用 `T` 作 schema：`T: Deserialize` 决定接受的 payload 形状。

#### Returns

`Self`

## Trait Implementations

- `impl Borrow for PayloadSchema`
- `impl BorrowMut for PayloadSchema`
- `impl CloneToUninit for PayloadSchema`
- `impl Into for PayloadSchema`
- `impl From for PayloadSchema`
- `impl TryInto for PayloadSchema`
- `impl TryFrom for PayloadSchema`
- `impl Any for PayloadSchema`
- `impl ToOwned for PayloadSchema`
- `impl Clone for PayloadSchema`
- `impl Debug for PayloadSchema`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

