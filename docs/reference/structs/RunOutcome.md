---
id: RunOutcome
title: RunOutcome
---

# Struct: RunOutcome

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:304`](../../../packages/workflow_core/src/engine/run_workflow.rs#L304)

## Fields

### run_id

```rust
run_id: String
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:305`](../../../packages/workflow_core/src/engine/run_workflow.rs#L305)


***

### status

```rust
status: RunStatus
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:306`](../../../packages/workflow_core/src/engine/run_workflow.rs#L306)


***

### output

```rust
output: Option<Value>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:307`](../../../packages/workflow_core/src/engine/run_workflow.rs#L307)


***

### error

```rust
error: Option<RunError>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:313`](../../../packages/workflow_core/src/engine/run_workflow.rs#L313)

与 [`RunState::error`](RunState.md) 是**同一个类型**——
一次失败的 run 在「返回值」和「持久化信封」两处描述一致，不要只改一边。

`RunError` 实现了 `Display`（转发 `message`），
所以 `warn!("{e}")` / `e.to_string()` 照常可用；想要错误名用 `.name`。

## Implementations

### from_run_state()

```rust
pub fn from_run_state(state: RunState) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:340`](../../../packages/workflow_core/src/engine/run_workflow.rs#L340)

从 **store 里的 [`RunState`](RunState.md)** 读出 run 结果，而不是从事件流末事件推。

# 为什么不能看事件

上游把 iterable 定义成「本次 drive **append 到日志**的事件」，不是状态
通道（`engine/run-workflow.ts:76-84`：*"an `AsyncIterable` of every event
the engine appends to the run's log […] the log IS the transport"*）。
runtime 因此把两者分开：`collectWorkflowEvents` 允许 `includeEvents:
false`（`events: []`）和 `maxEvents` 截断，而 `classifyRun` **只看
store**——`events: []` 时 `kind` 依然正确，所以状态不可能由事件推出
（`runtime-driver.ts:968-977`、`:1179-1214`、`types.ts:449-457`）。
我们的 `RunEventStream::outcome` 就是这套顺序的直译：drain 完再读
`RunState`。

# 幂等重放为什么让「看末事件」必然出错

重 drive 一个已挂起的 run 时，primitive 短路、**一条新日志都不写**
（上游：*"primitives short-circuit via `findCheckpoint` lookup in
history"*，commit `4f64b9c`；本仓对应 `exec_pause_with` 的
`already_paused` 门），流里只剩 `RUN_STARTED`——凭末事件分不出「挂起」
和「结束」。`Paused` 只能从 `RunState.status` 拿。

完整决策记录见 `docs/tanstack-alignment.md` 的「决策（2026-09-30）」。

#### Parameters

##### state

[`RunState`](RunState.md)

#### Returns

`Self`


***

### try_from_run_state()

```rust
pub fn try_from_run_state(state: Option<RunState>) -> Result<Self, WorkflowError>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:350`](../../../packages/workflow_core/src/engine/run_workflow.rs#L350)

[`Self::from_run_state`](RunOutcome.md) 的 `Result` 版——`RunState` 读不出来才算错。

#### Parameters

##### state

`Option<RunState>`

#### Returns

`Result<Self, WorkflowError>`

## Trait Implementations

- `impl Borrow for RunOutcome`
- `impl BorrowMut for RunOutcome`
- `impl Into for RunOutcome`
- `impl From for RunOutcome`
- `impl TryInto for RunOutcome`
- `impl TryFrom for RunOutcome`
- `impl Any for RunOutcome`
- `impl Debug for RunOutcome`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

