---
id: RunState
title: RunState
---

# Struct: RunState

Defined in: `packages/workflow-core/src/run_store/mod.rs:86`

Minimal, durable metadata for a run. The heavy state lives in the event
log; this is just the envelope the launcher needs to locate runs.
`waiting_for` / `pending_approval` 是挂起态的一等投影（派生自事件日志，
恢复时清除）——观察者无需扫日志就能告诉 run 在等什么。

## 谁写、谁存

**内容由引擎写，介质由 store 定**。引擎在每次 drive 的收尾、以及挂起时
（[`crate::engine`] 的 pause 投影）调用 [`RunStore::set_run_state`] 更新信封；
store 只负责把它放哪儿——内存 map、JSON 文件、数据库行都行。TanStack 同样
如此：`run-workflow.ts` 有 7 处调 `setRunState`，挂起投影那处的注释就是
“Persist waitingFor on the run state so out-of-process workers can
discover the pending wake”。

注意别把 `RunState` 和某个具体 store 的文件布局混为一谈：本 crate 不假定
介质，也没有任何代码知道 `run.json` 这种文件名（那是示例层 `FileRunStore`
的事）。

⚠️ **`RunState` 不是 `ctx.state`，两者毫无关系**（仅名字相似，极易混淆）。

| | `RunState`（本类型） | `ctx.state: TState` |
| --- | --- | --- |
| 是什么 | run 的**持久化元数据信封** | workflow 的**业务状态** |
| 谁消费 | **store**（路由 / 恢复 / 审计） | **handler**（`ctx.state.count += 1`） |
| 怎么声明 | 固定结构 | `.state::<T>()` + `.initialize(...)` |
| 存哪 | **持久化**（本类型就是被存的那个） | **不持久化**，每次 resume 重建 |

**本类型里刻意不放 `ctx.state`。** 上游 `types.ts:534-536` 原话：
“Persisted run metadata. **State is intentionally NOT stored here** — it is
reconstructed from `initialize(input)` + log replay on every resume.”

由此推得三条，别搞反：

- store 的表里**不该有业务 state 的列**（`workflow_runs` /
  `workflow_run_states` 存的都是 `RunState` 这一族）；
- `ctx.state` 的变更走 `STATE_DELTA`，那是 **emit-only、不进日志、不参与
  replay** 的可观测事件——**不是** `ctx.state` 的持久化形态。`ctx.state`
  在整条链上没有任何持久化形态；
- `ctx.state` 靠 replay 重建，所以它依赖 handler 的确定性：step 闭包内的
  修改会随闭包短路而丢失，并行分支经 `ctx.clone()` 各持快照互不可见。
  跨分支传数据要走 **step 的返回值**（durable 结果），不要走 state。

`In` / `Out` 默认擦除为 [`serde_json::Value`]，因为 [`RunStore`] 的契约面
必须能装下任意 workflow 的 input/output（store 是 `dyn`，无法带泛型）。
想要具体类型的调用方用 [`RunState::into_typed`] 窄化。

## Fields

### `run_id`

```rust
run_id: String
```

### `workflow_id`

```rust
workflow_id: String
```

### `workflow_version`

```rust
workflow_version: Option<String>
```

### `status`

```rust
status: RunStatus
```

### `input`

```rust
input: In
```

### `output`

```rust
output: Option<Out>
```

### `error`

```rust
error: Option<RunError>
```

### `waiting_for`

```rust
waiting_for: Option<WaitForState>
```

挂起等待外部 signal / sleep 到期（sleep 有 deadline）。

### `pending_approval`

```rust
pending_approval: Option<PendingApproval>
```

挂起等待审批。

### `created_at`

```rust
created_at: i64
```

### `updated_at`

```rust
updated_at: i64
```

## Implementations

### `into_typed`

```rust
pub fn into_typed<In, Out>(self) -> Result<RunState<In, Out>>
```

Defined in: `packages/workflow-core/src/run_store/mod.rs:112`

## Trait Implementations

- `impl Borrow for RunState`
- `impl BorrowMut for RunState`
- `impl CloneToUninit for RunState`
- `impl Into for RunState`
- `impl From for RunState`
- `impl TryInto for RunState`
- `impl TryFrom for RunState`
- `impl Any for RunState`
- `impl ToOwned for RunState`
- `impl DeserializeOwned for RunState`
- `impl Debug for RunState`
- `impl Clone for RunState`
- `impl Serialize for RunState`
- `impl Deserialize for RunState`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

