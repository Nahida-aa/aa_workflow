---
id: RunWorkflowOptions
title: RunWorkflowOptions
---

# Struct: RunWorkflowOptions

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:121`](../../../packages/workflow_core/src/engine/run_workflow.rs#L121)

## Fields

### workflow

```rust
workflow: AnyWorkflowDefinition
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:128`](../../../packages/workflow_core/src/engine/run_workflow.rs#L128)

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

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:130`](../../../packages/workflow_core/src/engine/run_workflow.rs#L130)

事件日志 / run 元数据的落盘位置。**必填**（由 [`Self::new`](RunWorkflowOptions.md) 保证）。


***

### run_id

```rust
run_id: Option<RunId>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:133`](../../../packages/workflow_core/src/engine/run_workflow.rs#L133)

复用该 run_id ⇒ resume（成功 step 短路、失败 rethrow）。


***

### input

```rust
input: Value
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:135`](../../../packages/workflow_core/src/engine/run_workflow.rs#L135)

run 输入。默认 `Value::Null`。


***

### interim_goals

```rust
interim_goals: Option<Vec<String>>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:137`](../../../packages/workflow_core/src/engine/run_workflow.rs#L137)

命中即停（本地扩展；上游用 handler 内 early `return`）。


***

### reexecute_from

```rust
reexecute_from: Option<ReexecuteFrom>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:142`](../../../packages/workflow_core/src/engine/run_workflow.rs#L142)

re-execute 计划（INVALIDATE 侧，`reexecuteFrom`，f2-lite 自有）。只在**全新
run 的 start 路径**生效：resume 驱动忽略它（重放短路已覆盖旧
checkpoint，重复导入会双写）；`source_run_id == run_id`、或源 run
不存在时报错（`run_lost`）。语义见 [`ReexecuteFrom`](ReexecuteFrom.md)。


***

### attach

```rust
attach: bool
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:154`](../../../packages/workflow_core/src/engine/run_workflow.rs#L154)

只读回放一个已存在的 run（上游 `attach`，`run-workflow.ts:48` / `:371`）。

不碰 handler、不写日志、不改 `RunState`——把日志里已有的事件原样重放
一遍，让调用方拿到和"从头 drive 一次"同样形状的事件包。run 不存在时报
`run_lost`（上游 `attachRun` 的第一条分支）。

**只认显式**：不照抄上游 `startRun` 的隐式幂等 redirect
（`run-workflow.ts:194-200`），因为我们没有 `signalDelivery` 标志位去区分
start 和 resume——照抄会把每次 resume 都变成 attach；另外重驱动已完成的 run
在本仓是**故意的**（`replay_shape_mismatch_fails_instead_of_yielding_null`
靠它做日志形状漂移自检）。理由详见 `attach_decision` 的文档。


***

### deadline

```rust
deadline: Option<i64>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:158`](../../../packages/workflow_core/src/engine/run_workflow.rs#L158)

本次 drive 的绝对 UTC ms 预算（上游 `deadline`）。设了之后
`time_remaining()` / `should_yield()` / `ctx.yield_()` 才生效；
每次 resume 都可以给一个新的。


***

### min_yield_remaining_ms

```rust
min_yield_remaining_ms: Option<u64>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:160`](../../../packages/workflow_core/src/engine/run_workflow.rs#L160)

剩余预算低于此值时 `should_yield()` 翻真（上游 `minYieldRemainingMs`，默认 1000）。


***

### yield_resume_at

```rust
yield_resume_at: Option<i64>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:162`](../../../packages/workflow_core/src/engine/run_workflow.rs#L162)

`ctx.yield_()` 的重新唤醒时刻（上游 `yieldResumeAt`；默认每次调用「now+1ms」）。


***

### thread_id

```rust
thread_id: Option<String>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:167`](../../../packages/workflow_core/src/engine/run_workflow.rs#L167)

客户端关联用的线程标识（上游 `threadId`，`run-workflow.ts:52`）。

只写进 `RUN_STARTED` 事件，**不落 store**、不参与 resume 判定。上游
把它当纯客户端关联（"Thread ID for client-side correlation"）。


***

### publish

```rust
publish: Option<PublisherFn>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:228`](../../../packages/workflow_core/src/engine/run_workflow.rs#L228)

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

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:236`](../../../packages/workflow_core/src/engine/run_workflow.rs#L236)

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

### thread_id()

```rust
pub fn thread_id(self, v: Option<String>) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:257`](../../../packages/workflow_core/src/engine/run_workflow.rs#L257)

客户端关联标识（上游 `threadId`），只写进 `RUN_STARTED`。

#### Parameters

##### v

`Option<String>`

#### Returns

`Self`


***

### input()

```rust
pub fn input(self, v: Value) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:263`](../../../packages/workflow_core/src/engine/run_workflow.rs#L263)

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

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:269`](../../../packages/workflow_core/src/engine/run_workflow.rs#L269)

复用该 run_id ⇒ resume。

#### Parameters

##### v

`impl ?`

#### Returns

`Self`


***

### interim_goals()

```rust
pub fn interim_goals(self, v: Vec<String>) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:275`](../../../packages/workflow_core/src/engine/run_workflow.rs#L275)

命中即停。

#### Parameters

##### v

`Vec<String>`

#### Returns

`Self`


***

### reexecute_from()

```rust
pub fn reexecute_from(self, rf: ReexecuteFrom) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:282`](../../../packages/workflow_core/src/engine/run_workflow.rs#L282)

re-execute 计划（INVALIDATE 侧）：新 run 起步时导入源 run 幸存的
checkpoint。语义见 [`ReexecuteFrom`](ReexecuteFrom.md)；只在全新 run 的 start 路径生效。

#### Parameters

##### rf

[`ReexecuteFrom`](ReexecuteFrom.md)

#### Returns

`Self`


***

### attach()

```rust
pub fn attach(self) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:288`](../../../packages/workflow_core/src/engine/run_workflow.rs#L288)

只读回放已存在的 run（上游 `attach`）。见 [`RunWorkflowOptions::attach`](RunWorkflowOptions.md)。

#### Returns

`Self`


***

### deadline()

```rust
pub fn deadline(self, v: i64) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:294`](../../../packages/workflow_core/src/engine/run_workflow.rs#L294)

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

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:300`](../../../packages/workflow_core/src/engine/run_workflow.rs#L300)

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

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:306`](../../../packages/workflow_core/src/engine/run_workflow.rs#L306)

`ctx.yield_()` 的重新唤醒时刻。

#### Parameters

##### v

`i64`

#### Returns

`Self`


***

### publish()

```rust
pub fn publish(self, v: Option<Arc<dyn Fn(&RunId, WorkflowEvent) + Send + Sync>>) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:320`](../../../packages/workflow_core/src/engine/run_workflow.rs#L320)

事件回调（上游 `publish`）。收 `Option`，便于直接对接旧的四参数签名。

同步版：内部包装成一个**立即完成**的 future，实际投递发生在 drain task
上（见字段文档「publish 不拖慢引擎」）。要真正 `await` 请用
[`Self::async_publish`](RunWorkflowOptions.md)。

事件**按值**传入，与 `async_publish` 同一套所有权语义（只有投递时机
不同），这样两条路径的心智模型是一致的：`ev` 归你，随便 move 进
`async move`、随便丢给线程、随便 `join()`。

#### Parameters

##### v

`Option<Arc<dyn Fn(&RunId, WorkflowEvent) + Send + Sync>>`

#### Returns

`Self`


***

### async_publish()

```rust
pub fn async_publish<F, Fut>(self, f: F) -> Self
where
    F: Fn(&RunId, WorkflowEvent) -> Fut + Send + Sync + 'static,
    Fut: Future + Send + 'static
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:344`](../../../packages/workflow_core/src/engine/run_workflow.rs#L344)

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

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:356`](../../../packages/workflow_core/src/engine/run_workflow.rs#L356)

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

