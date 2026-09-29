---
id: RunWorkflowOptions
title: RunWorkflowOptions
---

# Struct: RunWorkflowOptions

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:48`

`run_workflow` / `run_workflow_sync` 的入参（对齐上游 `RunWorkflowOptions`，
见 `engine/run-workflow.ts:34-72`）。

# 为什么把 `workflow` / `run_store` 也收进来

之前是 4 个位置参数 `run_workflow(&wf, store, &opts, publisher)`——容易传错位。
上游把**全部**入参放在一个结构体里，`workflow` / `runStore` 是**必填字段**。
这里照做：必填项由 [`RunWorkflowOptions::new`] 强制（构造完就一定齐了），
可选项走 builder 链。

# 与上游的字段差异

| 上游 `RunWorkflowOptions` | 本结构 | 说明 |
| --- | --- | --- |
| `workflow` / `runStore` | ✅ `workflow` / `run_store` | 必填 |
| `input` / `runId` / `deadline` / `minYieldRemainingMs` / `yieldResumeAt` | ✅ | 同名同义 |
| `publish` | ✅ `publisher` | 位置从参数移进结构体 |
| `signalDelivery` / `approval` | — | 我们走 `signal_run` / `signal_event` 先落盘再 drive（D3 的形态差异） |
| `recover` / `attach` / `signal` / `threadId` / `outputSink` / `telemetry` | — | **暂无**；未做，不是不做 |
| — | ➕ `continue_from` / `target_step` | **本地扩展**（上游连这两个概念都没有） |

# 为什么 `workflow` 是 `Arc<Workflow>` 而不是 `Workflow`

`Workflow` 内含 `Vec<Workflow>`（`previous_versions`）与若干 `Arc`，
直接持有所有权会让每次 builder 调用都要 clone 一遍。`Arc` 让 clone 变成
引用计数自增，同时避免给结构体引入生命周期参数（那会让 builder 链很难写）。

## Fields

### `workflow`

```rust
workflow: Arc<Workflow>
```

要驱动的 workflow。**必填**（由 [`Self::new`] 保证）。

### `run_store`

```rust
run_store: Arc<dyn RunStore>
```

事件日志 / run 元数据的落盘位置。**必填**（由 [`Self::new`] 保证）。

### `run_id`

```rust
run_id: Option<String>
```

复用该 run_id ⇒ resume（成功 step 短路、失败 rethrow）。

### `input`

```rust
input: Value
```

run 输入。默认 `Value::Null`。

### `target_step`

```rust
target_step: Option<String>
```

命中即停（本地扩展；上游用 handler 内 early `return`）。

### `continue_from`

```rust
continue_from: Option<String>
```

从该 step 的最新终态 checkpoint 处截断后重跑后缀（**本地扩展**）。

### `deadline`

```rust
deadline: Option<i64>
```

本次 drive 的绝对 UTC ms 预算（上游 `deadline`）。设了之后
`time_remaining()` / `should_yield()` / `ctx.yield_()` 才生效；
每次 resume 都可以给一个新的。

### `min_yield_remaining_ms`

```rust
min_yield_remaining_ms: Option<u64>
```

剩余预算低于此值时 `should_yield()` 翻真（上游 `minYieldRemainingMs`，默认 1000）。

### `yield_resume_at`

```rust
yield_resume_at: Option<i64>
```

`ctx.yield_()` 的重新唤醒时刻（上游 `yieldResumeAt`；默认每次调用「now+1ms」）。

### `publisher`

```rust
publisher: Option<Arc<dyn Fn(&WorkflowEvent) + Send + Sync>>
```

每个事件都会回调（上游 `publish`）——host 可以接到 Redis / Durable Streams
之类的扇出通道，让别的节点能 tail 这个 run。

## Implementations

### `new`

```rust
pub fn new(workflow: Arc<Workflow>, run_store: Arc<dyn RunStore>) -> Self
```

**必填项在这里**：`workflow` + `run_store`。构造完这两个就一定齐了。

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:77`

### `input`

```rust
pub fn input(self, v: Value) -> Self
```

run 输入。

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:93`

### `run_id`

```rust
pub fn run_id<impl Into<String>>(self, v: impl ?) -> Self
```

复用该 run_id ⇒ resume。

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:99`

### `target_step`

```rust
pub fn target_step<impl Into<String>>(self, v: impl ?) -> Self
```

命中即停。

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:105`

### `continue_from`

```rust
pub fn continue_from<impl Into<String>>(self, v: impl ?) -> Self
```

从该 step 截断后重跑后缀。

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:111`

### `deadline`

```rust
pub fn deadline(self, v: i64) -> Self
```

设置本次 drive 的绝对 UTC ms 预算。

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:117`

### `min_yield_remaining`

```rust
pub fn min_yield_remaining(self, v: u64) -> Self
```

剩余预算低于此值时允许让出（上游 `minYieldRemainingMs`）。

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:123`

### `yield_resume_at`

```rust
pub fn yield_resume_at(self, v: i64) -> Self
```

`ctx.yield_()` 的重新唤醒时刻。

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:129`

### `publisher`

```rust
pub fn publisher(self, v: Option<Arc<dyn Fn(&WorkflowEvent) + Send + Sync>>) -> Self
```

事件回调（上游 `publish`）。收 `Option`，便于直接对接旧的四参数签名。

Defined in: `packages/workflow-core/src/engine/run_workflow.rs:135`

## Trait Implementations

- `impl Borrow for RunWorkflowOptions`
- `impl BorrowMut for RunWorkflowOptions`
- `impl Into for RunWorkflowOptions`
- `impl From for RunWorkflowOptions`
- `impl TryInto for RunWorkflowOptions`
- `impl TryFrom for RunWorkflowOptions`
- `impl Any for RunWorkflowOptions`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

