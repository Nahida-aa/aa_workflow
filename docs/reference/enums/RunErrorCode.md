---
id: RunErrorCode
title: RunErrorCode
---

# Enum: RunErrorCode

Defined in: [`packages/workflow_core/src/error.rs:109`](../../../packages/workflow_core/src/error.rs#L109)

终局错误码（对齐 TanStack `RUN_ERRORED.code`，`types.ts:91`）。

TS 全集是 `error | aborted | validation_error | run_lost | signal_lost |
approval_lost | workflow_version_mismatch`。这里只留引擎真能产出的三个：

- `run_lost` / `signal_lost` / `approval_lost` 对应他们「投递丢失」时把
  run 打 errored 的路径，我们的 `signal_run` / `signal_event` 是把错误
  **返回给调用方**，不往日志里写终态事件，没有发射点。
- `workflow_version_mismatch` 我们是**回退到当前版本**而不是报错
  （`select_workflow_version`），所以也产不出。

## Variants

### Error

Defined in: [`packages/workflow_core/src/error.rs:111`](../../../packages/workflow_core/src/error.rs#L111)

handler 抛错 / step 失败。


***

### Aborted

Defined in: [`packages/workflow_core/src/error.rs:113`](../../../packages/workflow_core/src/error.rs#L113)

被 `cancel_run` 中止。


***

### Validation

Defined in: [`packages/workflow_core/src/error.rs:115`](../../../packages/workflow_core/src/error.rs#L115)

`initialize` 失败或 state 形状校验不过（对应他们 zod `.safeParse` 失败）。


***

### WorkflowVersionMismatch

Defined in: [`packages/workflow_core/src/error.rs:119`](../../../packages/workflow_core/src/error.rs#L119)

版本化 run 的 `workflow_version` 既不是当前版本、也不在
`previous_versions` 里（对齐上游 `workflow_version_mismatch`
错误码）。**不回退**——那会把旧版 run 路由进新版代码，是确定性违规。

## Implementations

### as_str()

```rust
pub fn as_str(&self) -> &'static str
```

Defined in: [`packages/workflow_core/src/error.rs:123`](../../../packages/workflow_core/src/error.rs#L123)

#### Returns

`&'static str`

## Trait Implementations

- `impl Borrow for RunErrorCode`
- `impl BorrowMut for RunErrorCode`
- `impl CloneToUninit for RunErrorCode`
- `impl Into for RunErrorCode`
- `impl From for RunErrorCode`
- `impl TryInto for RunErrorCode`
- `impl TryFrom for RunErrorCode`
- `impl Any for RunErrorCode`
- `impl ToOwned for RunErrorCode`
- `impl ToString for RunErrorCode`
- `impl DeserializeOwned for RunErrorCode`
- `impl Debug for RunErrorCode`
- `impl Clone for RunErrorCode`
- `impl Copy for RunErrorCode`
- `impl StructuralPartialEq for RunErrorCode`
- `impl PartialEq for RunErrorCode`
- `impl Eq for RunErrorCode`
- `impl Serialize for RunErrorCode`
- `impl Deserialize for RunErrorCode`
- `impl Display for RunErrorCode`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

