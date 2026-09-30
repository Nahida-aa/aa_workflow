---
id: exec_pause_with
title: exec_pause_with
---

# Function: exec_pause_with()

```rust
pub async fn exec_pause_with(inner: &Arc<EngineRuntime>, step_id: &str, signal_name: &str, reason: &str, deadline: Option<i64>, meta: Option<Value>, validator: Option<PayloadValidator>) -> Result<Value>
```

Defined in: [`packages/workflow_core/src/engine/mod.rs:630`](../../../packages/workflow_core/src/engine/mod.rs#L630)

[`exec_pause`](exec_pause.md) 的完整形态：额外带 `meta`（落进 `StepPaused` checkpoint 与
`RunState` 的三处投影）和 `validator`（恢复时校验 payload 形状）。

`meta` 落进 checkpoint 是有意的：观察者只读 `RunState` 就能渲染，不必回放
日志（对齐 `DurableOperationOptions.meta`「copied into the operation's log
event」，`types.ts:29-31`）。

`validator` **不落盘** —— 它是代码而非数据，replay 时同一个
`wait_for_event_with(..)` 调用点会重新装上同一个校验器。

## Parameters

### inner

`&Arc<EngineRuntime>`

### step_id

`&str`

### signal_name

`&str`

### reason

`&str`

### deadline

`Option<i64>`

### meta

`Option<Value>`

### validator

`Option<PayloadValidator>`

## Returns

`Result<Value>`

