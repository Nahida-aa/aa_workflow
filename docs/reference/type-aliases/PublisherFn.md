---
id: PublisherFn
title: PublisherFn
---

# Type Alias: PublisherFn

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:50`](../../../packages/workflow_core/src/engine/run_workflow.rs#L50)

事件 fan-out 回调。**按引用**收 run id（上游按值，但 TS 传引用不拷贝，
Rust 按值就得多 clone 一次）；事件按值收，因为 core 每次都要 clone 出去
（`RunEventStream` 要在 poll 里重放同一份）。

## Definition

```rust
pub type PublisherFn = Arc<dyn Fn(&RunId, WorkflowEvent) -> BoxFuture<'static, ()> + Send + Sync>
```

