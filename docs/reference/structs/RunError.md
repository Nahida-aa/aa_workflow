---
id: RunError
title: RunError
---

# Struct: RunError

Defined in: `packages/workflow-core/src/error.rs:54`

跨进程序列化的错误（对齐 TanStack `SerializedError`，`types.ts:16`）。

「序列化」不等于「持久化」：它可能进 `RunState.error`，也可能只随事件投给
publisher。存不存、存哪儿，是 [`RunStore`](crate::run_store::RunStore)
实现者的事。

两处刻意不对等：
- **没有 `stack`**。JS 的 `Error.stack` 在 Rust 无对应物：`anyhow` 的
  backtrace 需要 nightly，`std::backtrace` 虽稳定但要 `RUST_BACKTRACE=1`
  才有内容，且跨 `await` 边界捕获到的都是运行时内部帧，对排查 workflow
  逻辑没有帮助。
- **`name` 常常只是 `"Error"`**。JS 的 `Error.name` 是错误类名；Rust 的
  `anyhow::Error` 是类型擦除的，拿不到类名。只有引擎自身的
  [`WorkflowError`] 变体与
  [`WorkflowCancelled`](crate::engine::WorkflowCancelled) 能给出有意义的名字。

## Fields

### `name`

```rust
name: String
```

### `message`

```rust
message: String
```

## Implementations

### `cancelled`

```rust
pub fn cancelled() -> Self
```

引擎主动中止（`cancel_run`）产生的错误。

Defined in: `packages/workflow-core/src/error.rs:61`

### `from_anyhow`

```rust
pub fn from_anyhow(e: &Error) -> Self
```

从 `anyhow::Error` 提取：能 downcast 到引擎错误类型时给出变体名，
否则退化为 `"Error"`（见类型文档）。

Defined in: `packages/workflow-core/src/error.rs:71`

## Trait Implementations

- `impl Borrow for RunError`
- `impl BorrowMut for RunError`
- `impl CloneToUninit for RunError`
- `impl Into for RunError`
- `impl From for RunError`
- `impl TryInto for RunError`
- `impl TryFrom for RunError`
- `impl Any for RunError`
- `impl ToOwned for RunError`
- `impl ToString for RunError`
- `impl DeserializeOwned for RunError`
- `impl Debug for RunError`
- `impl Clone for RunError`
- `impl StructuralPartialEq for RunError`
- `impl PartialEq for RunError`
- `impl Eq for RunError`
- `impl Serialize for RunError`
- `impl Deserialize for RunError`
- `impl Display for RunError`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

