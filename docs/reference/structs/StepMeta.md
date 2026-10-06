---
id: StepMeta
title: StepMeta
---

# Struct: StepMeta

Defined in: [`packages/workflow_core/src/event.rs:54`](../../../packages/workflow_core/src/event.rs#L54)

`STEP_FINISHED` 的可选元数据槽（对齐上游事件上的 `meta`）。目前只承载
一种事实：checkpoint 从源 run 导入（`reexecute_from` 的 INVALIDATE 侧）。
引擎自己产生的 checkpoint 恒为 `None`——序列化时整个省略，旧日志反序
列化为 `None`，日志形状前后兼容。

## Fields

### imported_from

```rust
imported_from: Option<ImportedFrom>
```

Defined in: [`packages/workflow_core/src/event.rs:57`](../../../packages/workflow_core/src/event.rs#L57)

导入溯源（上游 `meta.importedFrom`）。

## Trait Implementations

- `impl Borrow for StepMeta`
- `impl BorrowMut for StepMeta`
- `impl CloneToUninit for StepMeta`
- `impl Into for StepMeta`
- `impl From for StepMeta`
- `impl TryInto for StepMeta`
- `impl TryFrom for StepMeta`
- `impl Any for StepMeta`
- `impl ToOwned for StepMeta`
- `impl DeserializeOwned for StepMeta`
- `impl Debug for StepMeta`
- `impl Clone for StepMeta`
- `impl Default for StepMeta`
- `impl StructuralPartialEq for StepMeta`
- `impl PartialEq for StepMeta`
- `impl Serialize for StepMeta`
- `impl Deserialize for StepMeta`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

