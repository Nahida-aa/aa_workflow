---
id: WaitForState
title: WaitForState
---

# Struct: WaitForState

Defined in: [`packages/workflow_core/src/run_store/mod.rs:22`](../../../packages/workflow_core/src/run_store/mod.rs#L22)

挂起等待中的外部信号（对齐 TanStack `RunState.waitingFor`）。sleep 的
`due_at` 就是这里的 `deadline` —— host 可用它做时间索引的唤醒调度。

`step_id` 在 TS 侧是可选的（`types.ts:555`）：signal 也可以只按
`signal_name` 投递，不必绑定某个 step。

## Fields

### step_id

```rust
step_id: Option<String>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:24`](../../../packages/workflow_core/src/run_store/mod.rs#L24)


***

### signal_name

```rust
signal_name: String
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:25`](../../../packages/workflow_core/src/run_store/mod.rs#L25)


***

### deadline

```rust
deadline: Option<i64>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:26`](../../../packages/workflow_core/src/run_store/mod.rs#L26)


***

### meta

```rust
meta: Option<Value>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:30`](../../../packages/workflow_core/src/run_store/mod.rs#L30)

自由元数据（对齐 TanStack `RunState.waitingFor.meta`，
`types.ts:558`），host / UI 拿去渲染，不参与引擎判定。

## Trait Implementations

- `impl Borrow for WaitForState`
- `impl BorrowMut for WaitForState`
- `impl CloneToUninit for WaitForState`
- `impl Into for WaitForState`
- `impl From for WaitForState`
- `impl TryInto for WaitForState`
- `impl TryFrom for WaitForState`
- `impl Any for WaitForState`
- `impl ToOwned for WaitForState`
- `impl DeserializeOwned for WaitForState`
- `impl Debug for WaitForState`
- `impl Clone for WaitForState`
- `impl Serialize for WaitForState`
- `impl Deserialize for WaitForState`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

