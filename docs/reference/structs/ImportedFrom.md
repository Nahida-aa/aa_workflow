---
id: ImportedFrom
title: ImportedFrom
---

# Struct: ImportedFrom

Defined in: [`packages/workflow_core/src/event.rs:63`](../../../packages/workflow_core/src/event.rs#L63)

一次 checkpoint 导入的出处：源 run 与该事件在**源 run 持久化日志**里的
下标（`RUN_STARTED` 不落盘、不占下标——与上游同约定）。

## Fields

### run_id

```rust
run_id: String
```

Defined in: [`packages/workflow_core/src/event.rs:64`](../../../packages/workflow_core/src/event.rs#L64)


***

### index

```rust
index: usize
```

Defined in: [`packages/workflow_core/src/event.rs:65`](../../../packages/workflow_core/src/event.rs#L65)

## Trait Implementations

- `impl Borrow for ImportedFrom`
- `impl BorrowMut for ImportedFrom`
- `impl CloneToUninit for ImportedFrom`
- `impl Into for ImportedFrom`
- `impl From for ImportedFrom`
- `impl TryInto for ImportedFrom`
- `impl TryFrom for ImportedFrom`
- `impl Any for ImportedFrom`
- `impl ToOwned for ImportedFrom`
- `impl DeserializeOwned for ImportedFrom`
- `impl Debug for ImportedFrom`
- `impl Clone for ImportedFrom`
- `impl StructuralPartialEq for ImportedFrom`
- `impl PartialEq for ImportedFrom`
- `impl Serialize for ImportedFrom`
- `impl Deserialize for ImportedFrom`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

