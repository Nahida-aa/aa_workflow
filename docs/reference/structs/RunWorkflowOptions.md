---
id: RunWorkflowOptions
title: RunWorkflowOptions
---

# Struct: RunWorkflowOptions

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:77`](../../../packages/workflow_core/src/engine/run_workflow.rs#L77)

`run_workflow` / `run_workflow_sync` 的全部入参（对齐上游
`runWorkflow(options)` 的**按值**收法，见 `engine/run-workflow.ts:34-72`）。

# 为什么把 `workflow` / `run_store` 也收进来

之前是 4 个位置参数 `run_workflow(&wf, store, &opts, publisher)`——容易传错位。
上游把**全部**入参放在一个结构体里，`workflow` / `runStore` 是**必填字段**。
这里照做：必填项由 [`RunWorkflowOptions::new`](RunWorkflowOptions.md) 强制（构造完就一定齐了），
可选项走 builder 链。

# 与上游的字段差异

| 上游 `RunWorkflowOptions` | 本结构 | 说明 |
| --- | --- | --- |
| `workflow` / `runStore` | ✅ `workflow` / `run_store` | 必填 |
| `input` / `runId` / `deadline` / `minYieldRemainingMs` / `yieldResumeAt` | ✅ | 同名同义 |
| `publish` | ✅ `publish` | 位置从参数移进结构体 |
| `signalDelivery` / `approval` | — | 我们走 `signal_run` / `signal_event` 先落盘再 drive（D3 的形态差异） |
| `recover` / `attach` / `signal` / `threadId` / `outputSink` / `telemetry` | — | **暂无**；未做，不是不做 |
| — | ➕ `continue_from` / `target_step` | **本地扩展**（上游连这两个概念都没有） |

# 为什么字段类型是 `AnyWorkflowDefinition`（newtype）而不是裸 `WorkflowDefinition`

上游这个字段是 `AnyWorkflowDefinition`，而它**本身就是别名**
`WorkflowDefinition<any, any, any>`（`types.ts:466`）——TS 的 `any` 双向可赋值，
一个类型同时充当「带类型的声明」和「擦除的运行站点」。Rust 做不到：
`WorkflowDefinition<ChargeInput, Draft>` 与 `WorkflowDefinition<Value, Value>`
是两个互不 coercion 的类型，所以擦除态必须是**独立的类型**，也就是这个 newtype。
它的名字沿用上游，于是本字段与上游一字不差。

newtype 内部持 `Arc<WorkflowDefinition>`：`previous_versions` 是 `Vec`、另有
若干 `Arc`，按值持有会让每次 run 都 deep-copy 一遍；`Arc` 让 clone 变引用计数自增，
也避免给结构体引入生命周期参数（那会让 builder 链很难写）。

## Fields

### workflow

```rust
workflow: AnyWorkflowDefinition
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:84`](../../../packages/workflow_core/src/engine/run_workflow.rs#L84)

要驱动的 workflow。**必填**（由 [`Self::new`](RunWorkflowOptions.md) 保证）。

用上游的名字，让本字段与 TanStack 的 `RunWorkflowOptions.workflow` 一字不差。
`AnyWorkflowDefinition` = 擦除态的 [`WorkflowDefinition`](WorkflowDefinition.md)（上游是
`WorkflowDefinition<any,any,any>` 的别名；Rust 没有 `any` 的双向可赋值，
所以擦除态是独立类型）。内部持 `Arc`，clone 只加引用计数。


***

### run_store

```rust
run_store: Arc<dyn RunStore>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:86`](../../../packages/workflow_core/src/engine/run_workflow.rs#L86)

事件日志 / run 元数据的落盘位置。**必填**（由 [`Self::new`](RunWorkflowOptions.md) 保证）。


***

### run_id

```rust
run_id: Option<String>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:89`](../../../packages/workflow_core/src/engine/run_workflow.rs#L89)

复用该 run_id ⇒ resume（成功 step 短路、失败 rethrow）。


***

### input

```rust
input: Value
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:91`](../../../packages/workflow_core/src/engine/run_workflow.rs#L91)

run 输入。默认 `Value::Null`。


***

### target_step

```rust
target_step: Option<String>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:93`](../../../packages/workflow_core/src/engine/run_workflow.rs#L93)

命中即停（本地扩展；上游用 handler 内 early `return`）。


***

### continue_from

```rust
continue_from: Option<String>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:95`](../../../packages/workflow_core/src/engine/run_workflow.rs#L95)

从该 step 的最新终态 checkpoint 处截断后重跑后缀（**本地扩展**）。


***

### deadline

```rust
deadline: Option<i64>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:99`](../../../packages/workflow_core/src/engine/run_workflow.rs#L99)

本次 drive 的绝对 UTC ms 预算（上游 `deadline`）。设了之后
`time_remaining()` / `should_yield()` / `ctx.yield_()` 才生效；
每次 resume 都可以给一个新的。


***

### min_yield_remaining_ms

```rust
min_yield_remaining_ms: Option<u64>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:101`](../../../packages/workflow_core/src/engine/run_workflow.rs#L101)

剩余预算低于此值时 `should_yield()` 翻真（上游 `minYieldRemainingMs`，默认 1000）。


***

### yield_resume_at

```rust
yield_resume_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:103`](../../../packages/workflow_core/src/engine/run_workflow.rs#L103)

`ctx.yield_()` 的重新唤醒时刻（上游 `yieldResumeAt`；默认每次调用「now+1ms」）。


***

### publish

```rust
publish: Option<PublisherFn>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:164`](../../../packages/workflow_core/src/engine/run_workflow.rs#L164)

每个事件都会回调（上游 `publish`）——host 可以接到 Redis / Durable Streams
之类的扇出通道，让别的节点能 tail 这个 run。

# 这是「可观测」与「耐久」的分界

引擎只把**事实**落盘（`append`）：`STEP_FINISHED` / `STEP_FAILED` /
`STEP_PAUSED` / `NOW_RECORDED` / `UUID_RECORDED`。其余事件——`STEP_PROGRESS`、
`STEP_STARTED`、`CUSTOM`、`STATE_DELTA`——**只**走到这里，**从不**写盘
（`EngineRuntime::publish` 没有任何落盘路径）。

「要不要把 progress 落盘」是**宿主的策略**，引擎不替所有人决定：devtools
不需要，产品 UI 可能需要。所以要留就自己在这里写：

```ignore
.publish(Some(Arc::new(move |run_id: &str, e: WorkflowEvent| {
    if let WorkflowEvent::StepProgress { step_id, value, .. } = e {
        my_db.insert_progress(run_id, step_id, *value);
    }
})))
```

## publish 不拖慢引擎（对齐上游的 async generator）

投递是「同步调用点 + 独立 drain task」两段：调用点只做一次
`UnboundedSender::send`（非阻塞），真正 await publish 的是 drain task。

上游是同一个形状：`runWorkflow` 是个 `async function*`，它在
`queue.shift()` 之后 `await publish` 再 `yield`，而**执行在另一个 task**
里继续往那个 queue 推（`run-workflow.ts:85-134`）。所以「publish 慢」
在两边都只拖慢**消费端吞吐**，不拖慢**引擎进度**。

队列是**无界**的（上游就是个裸数组），所以慢 publish 涨内存而不是卡住
run；要背压就在宿主自己的 publish 里做。

**「返回前终态已投递」是守住的旧语义**：收尾发 shutdown 并 join drain，
所以 `RUN_FINISHED` / `RUN_ERRORED` / `STEP_PAUSED` 一定在
`run_workflow` 返回前送达（Paused 早退路径也排空）。代价是慢 publish 会
延迟**返回**，但执行早已结束。

死锁风险与改造前相同：publish 若 `await` 依赖本次 run 完成的东西，仍会
挂——旧语义下内联调用时也会挂。不是回归。

## 坑 1：进程内回调 = 有丢失窗口

崩溃时最后一批事件就没了。所以「自己落盘」得到的是**被观测到的那部分**
耐久，不是「全部」耐久。拿它当审计日志会得到一份有洞的审计日志——
审计要耐久就别走这里，该让引擎 append。

## 坑 2：publish panic 会被吞掉（与上游一致，刻意如此）

宿主 publish 里的 panic **不会**掀掉你的 run —— `publish()` 用
`catch_unwind` 兜住。上游同形（*"A misbehaving publish must not break
the run — swallow and continue."*，`run-workflow.ts:128-134`）：宿主代码
不该有能力损毁已经 append 了 checkpoint 的耐久状态。

代价是**静默** —— 本 crate 没有日志依赖，所以拿不到「publish 炸了」这
条信息。要诊断就在**你自己的 publish 内部** catch + 记日志，日志策略和
依赖都留在宿主那侧。
可选的事件回调（上游 `publish`）。多数宿主用同步的 [`Self::publish`](RunWorkflowOptions.md)；
需要 `await` 落盘/发网络的用 [`Self::async_publish`](RunWorkflowOptions.md)。

## Implementations

### new()

```rust
pub fn new<impl Into<AnyWorkflowDefinition>: Into>(workflow: impl ?, run_store: Arc<dyn RunStore>) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:172`](../../../packages/workflow_core/src/engine/run_workflow.rs#L172)

**必填项在这里**：`workflow` + `run_store`。构造完这两个就一定齐了。
`workflow` 收 `impl Into<AnyWorkflowDefinition>`，所以三种写法都直接可用：
擦除态 `WorkflowDefinition`、带类型的 `WorkflowDefinition<TInput, …>`
（`create_workflow` 的产物），以及它们的 `Arc`。

#### Parameters

##### workflow

`impl ?`

##### run_store

`Arc<dyn RunStore>`

#### Returns

`Self`


***

### input()

```rust
pub fn input(self, v: Value) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:191`](../../../packages/workflow_core/src/engine/run_workflow.rs#L191)

run 输入。

#### Parameters

##### v

`Value`

#### Returns

`Self`


***

### run_id()

```rust
pub fn run_id<impl Into<String>: Into>(self, v: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:197`](../../../packages/workflow_core/src/engine/run_workflow.rs#L197)

复用该 run_id ⇒ resume。

#### Parameters

##### v

`impl ?`

#### Returns

`Self`


***

### target_step()

```rust
pub fn target_step<impl Into<String>: Into>(self, v: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:203`](../../../packages/workflow_core/src/engine/run_workflow.rs#L203)

命中即停。

#### Parameters

##### v

`impl ?`

#### Returns

`Self`


***

### continue_from()

```rust
pub fn continue_from<impl Into<String>: Into>(self, v: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:209`](../../../packages/workflow_core/src/engine/run_workflow.rs#L209)

从该 step 截断后重跑后缀。

#### Parameters

##### v

`impl ?`

#### Returns

`Self`


***

### deadline()

```rust
pub fn deadline(self, v: i64) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:215`](../../../packages/workflow_core/src/engine/run_workflow.rs#L215)

设置本次 drive 的绝对 UTC ms 预算。

#### Parameters

##### v

`i64`

#### Returns

`Self`


***

### min_yield_remaining()

```rust
pub fn min_yield_remaining(self, v: u64) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:221`](../../../packages/workflow_core/src/engine/run_workflow.rs#L221)

剩余预算低于此值时允许让出（上游 `minYieldRemainingMs`）。

#### Parameters

##### v

`u64`

#### Returns

`Self`


***

### yield_resume_at()

```rust
pub fn yield_resume_at(self, v: i64) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:227`](../../../packages/workflow_core/src/engine/run_workflow.rs#L227)

`ctx.yield_()` 的重新唤醒时刻。

#### Parameters

##### v

`i64`

#### Returns

`Self`


***

### publish()

```rust
pub fn publish(self, v: Option<Arc<dyn Fn(&str, WorkflowEvent) + Send + Sync>>) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:241`](../../../packages/workflow_core/src/engine/run_workflow.rs#L241)

事件回调（上游 `publish`）。收 `Option`，便于直接对接旧的四参数签名。

同步版：内部包装成一个**立即完成**的 future，实际投递发生在 drain task
上（见字段文档「publish 不拖慢引擎」）。要真正 `await` 请用
[`Self::async_publish`](RunWorkflowOptions.md)。

事件**按值**传入，与 `async_publish` 同一套所有权语义（只有投递时机
不同），这样两条路径的心智模型是一致的：`ev` 归你，随便 move 进
`async move`、随便丢给线程、随便 `join()`。

#### Parameters

##### v

`Option<Arc<dyn Fn(&str, WorkflowEvent) + Send + Sync>>`

#### Returns

`Self`


***

### async_publish()

```rust
pub fn async_publish<F, Fut>(self, f: F) -> Self
where
    F: Fn(&str, WorkflowEvent) -> Fut + Send + Sync + 'static,
    Fut: Future + Send + 'static
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:265`](../../../packages/workflow_core/src/engine/run_workflow.rs#L265)

异步事件回调（上游 `publish` 的 `Promise<void>` 那一支）。

与 [`Self::publish`](RunWorkflowOptions.md) 的差别只是**允许 `await`**：投递在 drain task 上
串行进行，但引擎执行不受它阻塞。要落盘/发网络而不想卡住引擎，就用这个。

事件**按值**传入，所以 async block 可以直接 `async move` 整个事件 ——
不需要「先取值再进 async」那套：

```ignore
.async_publish(|run_id, ev| async move { sink.send(run_id, ev).await })
```

想留一份自己用就克隆（事件不大，且 `publish()` 已经为了入队克隆过一次，
这里不会再多一次引擎侧克隆）。

#### Parameters

##### f

`F`

#### Returns

`Self`


***

### no_publish()

```rust
pub fn no_publish(self) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:277`](../../../packages/workflow_core/src/engine/run_workflow.rs#L277)

清掉事件回调。

#### Returns

`Self`

## Trait Implementations

- `impl Borrow for RunWorkflowOptions`
- `impl BorrowMut for RunWorkflowOptions`
- `impl CloneToUninit for RunWorkflowOptions`
- `impl Into for RunWorkflowOptions`
- `impl From for RunWorkflowOptions`
- `impl TryInto for RunWorkflowOptions`
- `impl TryFrom for RunWorkflowOptions`
- `impl Any for RunWorkflowOptions`
- `impl ToOwned for RunWorkflowOptions`
- `impl Clone for RunWorkflowOptions`

## Auto Trait Implementations

`Freeze` `Send` `Sync` `Unpin` `UnsafeUnpin`

