---
id: StateHandle
title: StateHandle
---

# Struct: StateHandle

Defined in: [`packages/workflow_core/src/define/state_handle.rs:55`](../../../packages/workflow_core/src/define/state_handle.rs#L55)

共享可变的 typed workflow state（对齐 TS 的 `ctx.state === engine.state`）。

**`Clone` = 快照**（复制 `TState`，共享 mirror），不是 live 共享——并行 step
通过 clone 各自持快照互不可见，避免竞写破坏重放确定性。

_（存在非公开字段）_

## Implementations

### new()

```rust
pub fn new(st: TState, mirror: Arc<Mutex<Value>>, to_value: Arc<dyn Fn(&TState) -> Value + Send + Sync>) -> Self
```

Defined in: [`packages/workflow_core/src/define/state_handle.rs:84`](../../../packages/workflow_core/src/define/state_handle.rs#L84)

构造：`to_value` 捕获 `TState: Serialize` 的序列化行为（在调用点确定），
guard / drop 的同步无需 trait bound。

#### Parameters

##### st

`TState`

##### mirror

`Arc<Mutex<Value>>`

##### to_value

`Arc<dyn Fn(&TState) -> Value + Send + Sync>`

#### Returns

`Self`


***

### snapshot()

```rust
pub fn snapshot(&self) -> Value
```

Defined in: [`packages/workflow_core/src/define/state_handle.rs:117`](../../../packages/workflow_core/src/define/state_handle.rs#L117)

当前 state 的序列化快照（构造 `EngineRuntime` / schema 校验用）。

#### Returns

`Value`

## Trait Implementations

- `impl Borrow for StateHandle`
- `impl BorrowMut for StateHandle`
- `impl CloneToUninit for StateHandle`
- `impl Into for StateHandle`
- `impl From for StateHandle`
- `impl TryInto for StateHandle`
- `impl TryFrom for StateHandle`
- `impl Receiver for StateHandle`
- `impl Any for StateHandle`
- `impl ToOwned for StateHandle`
- `impl Clone for StateHandle`
- `impl Deref for StateHandle`
- `impl DerefMut for StateHandle`
- `impl Drop for StateHandle`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

