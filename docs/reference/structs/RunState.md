---
id: RunState
title: RunState
---

# Struct: RunState

Defined in: [`packages/workflow_core/src/run_store/mod.rs:138`](../../../packages/workflow_core/src/run_store/mod.rs#L138)

Minimal, durable metadata for a run. The heavy state lives in the event
log; this is just the envelope the launcher needs to locate runs.
`waiting_for` / `pending_approval` 是挂起态的一等投影（派生自事件日志，
恢复时清除）——观察者无需扫日志就能告诉 run 在等什么。

## 谁写、谁存

**内容由引擎写，介质由 store 定**。引擎在每次 drive 的收尾、以及挂起时
（`crate::engine` 的 pause 投影）调用 [`RunStore::set_run_state`](../traits/RunStore.md) 更新信封；
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

`TInput` / `TOutput` 默认擦除为 `serde_json::Value`，因为 [`RunStore`](../traits/RunStore.md) 的契约面
必须能装下任意 workflow 的 input/output（store 是 `dyn`，无法带泛型）。
想要具体类型的调用方用 [`RunState::into_typed`](RunState.md) 窄化。

## Fields

### run_id

```rust
run_id: String
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:139`](../../../packages/workflow_core/src/run_store/mod.rs#L139)


***

### workflow_id

```rust
workflow_id: String
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:140`](../../../packages/workflow_core/src/run_store/mod.rs#L140)


***

### workflow_version

```rust
workflow_version: Option<String>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:141`](../../../packages/workflow_core/src/run_store/mod.rs#L141)


***

### status

```rust
status: RunStatus
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:142`](../../../packages/workflow_core/src/run_store/mod.rs#L142)


***

### input

```rust
input: TInput
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:143`](../../../packages/workflow_core/src/run_store/mod.rs#L143)


***

### output

```rust
output: Option<TOutput>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:152`](../../../packages/workflow_core/src/run_store/mod.rs#L152)

`Some(Value::Null)` 与 `None` 必须能区分——handler 返回 `null` 是合法的
（比如 `Ok(ctx.step(..).await?)` 而 step 的结果是 null）。

serde 默认会把 `Some(Value::Null)` 序列化成 `null`、再把 `null` 读回成
`None`，两者在 JSON 层撞成一个值。`default` + `skip_serializing_if` 让
「没有」表现为**键缺失**，`de_some` 让「键存在（哪怕是 null）」表现为
`Some`——用缺键来编码 `None`，两种状态就不撞了。


***

### error

```rust
error: Option<RunError>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:153`](../../../packages/workflow_core/src/run_store/mod.rs#L153)


***

### awaiting

```rust
awaiting: Vec<RunAwaitable>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:158`](../../../packages/workflow_core/src/run_store/mod.rs#L158)

挂起等待中的**全部** awaitable（对齐 TanStack `RunState.awaiting`，
`types.ts:552`）。这是**规范形**；下面两个字段是它的**镜像**，同一份
信息的两种看法。当前恒定 ≤1 个元素（见 [`RunAwaitable`](../enums/RunAwaitable.md) 的说明）。


***

### waiting_for

```rust
waiting_for: Option<WaitForState>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:161`](../../../packages/workflow_core/src/run_store/mod.rs#L161)

挂起等待外部 signal / sleep 到期（sleep 有 deadline）。


***

### pending_approval

```rust
pending_approval: Option<PendingApproval>
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:164`](../../../packages/workflow_core/src/run_store/mod.rs#L164)

挂起等待审批。


***

### created_at

```rust
created_at: i64
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:165`](../../../packages/workflow_core/src/run_store/mod.rs#L165)


***

### updated_at

```rust
updated_at: i64
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:166`](../../../packages/workflow_core/src/run_store/mod.rs#L166)

## Implementations

### into_typed()

```rust
pub fn into_typed<TInput, TOutput>(self) -> Result<RunState<TInput, TOutput>>
where
    TInput: DeserializeOwned,
    TOutput: DeserializeOwned
```

Defined in: [`packages/workflow_core/src/run_store/mod.rs:189`](../../../packages/workflow_core/src/run_store/mod.rs#L189)

#### Returns

`Result<RunState<TInput, TOutput>>`

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

