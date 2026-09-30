---
id: RunId
title: RunId
---

# Type Alias: RunId

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:45`](../../../packages/workflow_core/src/engine/run_workflow.rs#L45)

事件回调的**已擦除**类型：返回 future 而非直接调用，所以宿主可以给异步实现。
参数与上游 `publish?: (runId, event) => void | Promise<void>`
（`run-workflow.ts:41`）**逐个对应**，顺序也一致。

**`run_id` 是补上的那个参数。** 之前回调只收事件，宿主想按 run 归集就得从
`event.run_id()` 里反解——而 `WorkflowEvent::Custom` 不带 run_id（自定义事件
是宿主的，引擎无从填），那条路径上宿主根本拿不到 run 身份。上游把 runId 当
独立参数传就是这个原因。

**事件按值传 `WorkflowEvent`**，不是 `&WorkflowEvent`。这不是口味问题：
`publish()` 走队列时本来就要克隆一份（调用点只有 `&self`），drain task 已经
持有一份完整副本。传引用就得让返回的 future 借用它，于是被迫用
`BoxFuture<'static, _>` + HRTB 兜（`for<'a> Fn(&'a E) -> BoxFuture<'a, ()>`
在 `dyn` 上是噩梦），代价是 async block **不能借用事件**。
按值传则一次克隆都不浪费、生命周期问题直接消失。
`run_id` 相反**按引用**：它 `&'static`（spawn 时克隆的那份），且真要在 async
block 里长期持有，按值只会多一次 clone。

```ignore
.async_publish(|run_id, ev| async move { sink.send(run_id, ev).await })
```
run 标识（上游 core 写 `string`，这里给个具名，和 runtime 的 `RunId` 同义）。

只用在**公开入口**上——`PublisherFn` 的回调契约、`RunWorkflowOptions.run_id`。
事件与 store 结构体里那几百处仍写 `String`：它们是 serde 面对的形状，
上游也一律 `string`，改名的文档收益抵不上 290 处 churn。

## Definition

```rust
pub type RunId = String
```

