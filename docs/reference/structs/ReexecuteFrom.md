---
id: ReexecuteFrom
title: ReexecuteFrom
---

# Struct: ReexecuteFrom

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:98`](../../../packages/workflow_core/src/engine/run_workflow.rs#L98)

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
| `attach` | ✅ `attach` | 只读回放，见 [`RunWorkflowOptions::attach`](RunWorkflowOptions.md) |
| `recover` / `signal` / `threadId` / `outputSink` / `telemetry` | — | **暂无**；未做，不是不做 |
| — | ➕ `interimGoals` | **本地扩展**（对齐 TS f2-lite 泊车语义） |

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
re-execute 计划（INVALIDATE 侧，`reexecuteFrom`——**f2-lite 自有，上游未采纳**；
引用自 `f2-lite` 分支 `run-workflow.ts:71-84`。以**全新 run** 启动，把源 run 幸存
的 checkpoint 导入本 run 的日志——每条 stepId 不在 `Self::unsafe_invalidate`
名单里的 `STEP_FINISHED`（失败的 step 不导入、按设计重跑）。溯源写进事件的
`meta.imported_from`。

引擎没有图：无法校验名单对依赖方向封闭——**保留被失效 step 的消费者是
调用者的不变量**（`unsafe` 之名由此而来：b 消费 a 的结果而你只失效 a，
导入的 b 会拿着 a 的旧结果短路）。上层（DAG 编排）负责算出闭包名单，
引擎只透传名单本身。

## Fields

### source_run_id

```rust
source_run_id: String
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:100`](../../../packages/workflow_core/src/engine/run_workflow.rs#L100)

checkpoint 的来源 run。


***

### unsafe_invalidate

```rust
unsafe_invalidate: Vec<String>
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:103`](../../../packages/workflow_core/src/engine/run_workflow.rs#L103)

强制重跑的 stepId 名单（引擎透传、不校验封闭性）。空 = 全部导入、
零重跑（对齐上游 `unsafeInvalidate?` 缺省）。

## Implementations

### new()

```rust
pub fn new<impl Into<String>: Into>(source_run_id: impl ?) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:107`](../../../packages/workflow_core/src/engine/run_workflow.rs#L107)

#### Parameters

##### source_run_id

`impl ?`

#### Returns

`Self`


***

### with_invalidate()

```rust
pub fn with_invalidate(self, steps: Vec<String>) -> Self
```

Defined in: [`packages/workflow_core/src/engine/run_workflow.rs:114`](../../../packages/workflow_core/src/engine/run_workflow.rs#L114)

#### Parameters

##### steps

`Vec<String>`

#### Returns

`Self`

## Trait Implementations

- `impl Borrow for ReexecuteFrom`
- `impl BorrowMut for ReexecuteFrom`
- `impl CloneToUninit for ReexecuteFrom`
- `impl Into for ReexecuteFrom`
- `impl From for ReexecuteFrom`
- `impl TryInto for ReexecuteFrom`
- `impl TryFrom for ReexecuteFrom`
- `impl Any for ReexecuteFrom`
- `impl ToOwned for ReexecuteFrom`
- `impl Debug for ReexecuteFrom`
- `impl Clone for ReexecuteFrom`

## Auto Trait Implementations

`Freeze` `RefUnwindSafe` `Send` `Sync` `Unpin` `UnsafeUnpin` `UnwindSafe`

