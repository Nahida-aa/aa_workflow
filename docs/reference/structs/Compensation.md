---
id: Compensation
title: Compensation
---

# Struct: Compensation

Defined in: [`packages/workflow_core/src/engine/mod.rs:121`](../../../packages/workflow_core/src/engine/mod.rs#L121)

Shared driver state handed to every step (and to the `<WorkflowCtx>`).
This is the code-as-DAG substrate: the "graph" is just this state plus the
handler's control flow, discovered as the handler runs.

事件扇出通道直接传 `WorkflowEvent`：收尾**靠关通道**（`run_workflow` 返回的
[`RunEventStream`](crate::RunEventStream) 靠 `poll_recv` 见到 `None` 结束），
不需要显式的 shutdown 哨兵——`EngineRuntime` 随引擎 future 一起 drop，
它是最后一个 sender。
Phase-1 补偿（undo 轴）：一次登记的撤销动作。登记存于
`EngineRuntime::compensations`（内存，handler 重放时按代码确定性重建，
回调不序列化——对齐 #19 "callbacks are loaded from code"）；执行在失败
路径（逆序、逐个耐久 checkpoint，见 `run_compensations`）。

## Fields

### id

```rust
id: String
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:123`](../../../packages/workflow_core/src/engine/mod.rs#L123)

唯一补偿 id（幂等键的组成部分；`__compensation:` 前缀保留）。


***

### undo

```rust
undo: Arc<dyn Fn(StepCtx) -> UndoFuture + Send + Sync>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:125`](../../../packages/workflow_core/src/engine/mod.rs#L125)

撤销动作：引擎在执行时构造 [`StepCtx`](StepCtx.md) 注入（id = 幂等键）。


***

### retry

```rust
retry: RetryPolicy
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:127`](../../../packages/workflow_core/src/engine/mod.rs#L127)

补偿自身的重试预算（默认 1 次 = 不重试）。

## Trait Implementations

- `impl Borrow for Compensation`
- `impl BorrowMut for Compensation`
- `impl Into for Compensation`
- `impl From for Compensation`
- `impl TryInto for Compensation`
- `impl TryFrom for Compensation`
- `impl Any for Compensation`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

