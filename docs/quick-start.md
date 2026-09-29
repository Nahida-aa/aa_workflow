---
id: quick-start
title: Quick Start
---

# Quick Start

`aa_workflow_core` 的最小上手配方（recipe 形态对齐 TanStack Workflow 的
[quick-start](../../../learn_ls/workflow/docs/quick-start.md)，代码是本仓的 Rust 对刻）。
每段都可独立成立；想按顺序读完可运行的版本，见 `examples/guide`。

## 安装

本仓不上 crates.io，用 path 依赖：

```toml
[dependencies]
aa_workflow_core = { path = "../aa-workflow/packages/workflow_core" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
anyhow = "1"
```

## Recipe: 跑一个 workflow

```rust
use std::sync::Arc;
use aa_workflow_core::{
    CreateWorkflowConfig, InMemoryStore, RunStatus, RunStore, RunWorkflowOptions, StepCtx,
    Workflow, WorkflowDefinition, create_workflow, run_workflow, signal_event, signal_run,
    // 下面各节还会用到：Middleware / StepOptions / RetryPolicy / Backoff
};

/// input schema：serde 类型就是 schema（Rust 版 zod `input`）。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ChargeInput { amount: i64, user_id: String }

fn charge_workflow() -> WorkflowDefinition<ChargeInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("charge").input::<ChargeInput>())
        // 闭包参数类型**可以推断**（`Fn(BaseCtx<TInput, TState, TExt>) -> Fut`
        // 这个约束会把签名推下去），不必写 `|ctx: BaseCtx<ChargeInput>|`。
        .handler(|ctx| async move {
            let amount = ctx.input.amount;      // 强类型字段，不是 .get() 链
            ctx.step("stripe-charge", |sc: StepCtx| async move {
                // sc.id 是确定性 step id —— 拿它当外部系统的幂等键
                Ok(serde_json::json!({ "chargeId": format!("ch_{}", sc.id), "amount": amount }))
            })
            .await
        })
}

let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
let outcome = run_workflow(
    &RunWorkflowOptions::new(Arc::new(charge_workflow().into_workflow()), store)
        .input(serde_json::json!({ "amount": 4200, "userId": "cus_123" })),
).await?;

// RunOutcome { run_id, status, output, error }
```

> **对应测试**（本 recipe 的回归在哪）：
> - **本文件专属**:`packages/workflow_core/tests/quickstart.rs` —— 三条:
>   typed input 成功路径、缺字段拒绝（并断言不产生 `StepFinished`）、
>   `sc.id` 跨 resume 稳定（钉住「拿它当幂等键」这条承诺）。
> - 基础形态（untyped 入口 + 一个 step + 输出即 run 输出）:
>   `packages/workflow_core/src/engine/run_workflow.rs` 的 `handler_output_is_run_output`。
> - typed input 的拒绝分支另有 examples 版:`examples/shared/src/workflows.rs` 的
>   `typed_input_rejects_missing_field`;`examples/guide` 覆盖 typed input 的成功路径
>   （用 `FulfillmentInput` 驱动 `fulfillment_workflow()` 并断言端到端结果）。

要点：

- **`workflow` 与 `run_store` 是必填项**，由 `RunWorkflowOptions::new` 强制；
  其余（`input` / `run_id` / `deadline` …）走 builder 链。
- **`.input::<T>()` 就是「有 schema」**：每次 drive 都做
  `from_value::<TInput>`,**缺字段 / 类型错 → run 直接 `Errored`**。这是它相对
  「裸 JSON」的主要收益。
- 不要 schema 就用 `Workflow::new("id")` 入口 —— 下面「审批挂起」一节就是，
  此时 `ctx.input` 是 `serde_json::Value`（且 `Workflow::new(...).handler(..)` 直接返回
  `Workflow`,不需要 `.into_workflow()`;`create_workflow(..)` 返回 `WorkflowDefinition`)。

### `serde_json::Value` 不是 TS 的 `any`

- Rust **没有 `any`**。`Value` 是一个**具体类型**（`enum`:Null / Bool / Number / String /
  Array / Object）——编译器照样检查你对它的用法。它表示的是「运行时形状不固定」，不是「不检查」。

用 `Value` 的代价（相对具体类型）:

- 取值要走 `Option` 访问器（`as_str()` / `as_i64()` / `.get("x")`)，编译器帮你挡住"忘处理缺失"，但代码更啰嗦。
- **丢掉 schema 校验**：没有 `.input::<T>()`,就不会有"缺字段即报错"。

所以：有明确入参形状就定义 struct 并 `.input::<T>()`;只有真正动态的 payload 才留 `Value`。

## Recipe: 挂起等人工审批

与上游的一个关键差异：**挂起即返回**。`ctx.approve` 写完 checkpoint 立刻抛
`WorkflowParked`，drive 以 `Paused` 收尾，进程可以退出；唤醒一律来自外部。

```rust
// 没有 input schema 时用 `Workflow::new`（返回 Workflow，不用 .into_workflow()）
let wf = Workflow::new("order").handler(|ctx| async move {
    let decision = ctx.approve("large-order", "金额超限，需要人工放行").await?;
    if decision.get("approved").and_then(|v| v.as_bool()) != Some(true) {
        return Ok(serde_json::json!({ "status": "rejected" }));
    }
    Ok(serde_json::json!({ "status": "approved" }))
});

// 第一次 drive：停在审批点。
let out = run_workflow(
    &RunWorkflowOptions::new(Arc::new(wf.clone()), store.clone())
        .input(serde_json::json!({ "amount": 1500 }))
        .run_id("order-1"),
).await?;
assert_eq!(out.status, RunStatus::Paused);

// 外部投递审批决定，再 drive 一次：前缀 checkpoint 短路，直接从唤醒点继续。
signal_run(store.as_ref(), "order-1", "large-order",
           serde_json::json!({ "approved": true }))?;
let out = run_workflow(
    &RunWorkflowOptions::new(Arc::new(wf.clone()), store)
        .input(serde_json::json!({ "amount": 1500 }))
        .run_id("order-1"),
).await?;
assert_eq!(out.status, RunStatus::Finished);
```

## Recipe: 等外部事件

```rust
let payment = ctx.wait_for_event("payment", "payment-received").await?;
// payment: serde_json::Value —— 投递方用 signal_event 带进来的 payload
```

投递方：

```rust
signal_event(store.as_ref(), "checkout-1", "payment-received",
             serde_json::json!({ "amount": 4200, "reference": "pi_1" }))?;
```

注意两个键的区别：`wait_for_event` 的第一个参数是 **pause key**（确定性重放用），
第二个是**事件名**（投递时按名匹配）。定时等待用 `ctx.sleep(key, dur)` /
`ctx.sleep_until(key, ts)`（引擎记 `__timer`，由 runtime 的 sweep 或 host 投递唤醒）。

## Recipe: typed state

`.state::<T>()` 声明契约，`.initialize` 每次 start / resume 从输入重建
（**不落盘**，靠 replay 重建），handler 里 `ctx.state` 直接是强类型字段。

```rust
#[derive(serde::Deserialize, serde::Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CounterState { pub total: i64 }

let wf = create_workflow(
    CreateWorkflowConfig::new("counter")
        .input::<serde_json::Value>()
        .state::<CounterState>()
        .initialize(|_input| Ok(serde_json::json!({ "total": 0 }))),
)
// 写 state 需要 `|mut ctx|`（DerefMut 要可变借用）；参数类型照旧可推断。
.handler(|mut ctx| async move {
    let v = ctx.step("compute", |_sc| async { Ok(serde_json::json!(42)) }).await?;
    ctx.state.total = v.as_i64().unwrap_or(0);   // 串行位置，写 state 安全
    Ok(serde_json::json!({ "total": ctx.state.total }))
});
```

⚠️ 两条写 state 的红线（详见 `AGENTS.md`）：

- **只在串行位置写**——`try_join!` 分支内写 state 会被快照分裂吞掉（Rust）或竞写（TS）；
  并行分支要汇合数据，走 **step 的返回值**。
- **step 闭包内的修改 replay 时会丢**（闭包被短路跳过）。要持久的东西放进 step
  返回值，不放 state。

## Recipe: 并行

并行没有专门原语——就是 `tokio::try_join!`。step 闭包是 async 的，所以并发
durable step 直接组合。这是「代码即 DAG」的内核：**并行 = `try_join!`，
顺序 = 词法 `.await`，分支 = `if`**。

```rust
let ctx_a = ctx.clone();           // BaseCtx 可 clone；各分支持独立 state 快照
let ctx_b = ctx.clone();
let (pdf, charged) = tokio::try_join!(
    async move {
        ctx_a.step("gen-pdf", |_sc| async { Ok(serde_json::json!({ "pdf": "a.pdf" })) }).await
    },
    async move {
        ctx_b.step_with("charge",
            StepOptions::new().retry(RetryPolicy::new(3, Backoff::Fixed { base_ms: 1 })),
            |_sc| async { payment_gateway::charge()?; Ok(serde_json::json!({ "ok": true })) },
        ).await
    },
)?;
if ctx.input.expedited {            // 分支 = 普通 if
    ctx.step("notify", |_sc| async { Ok(serde_json::Value::Null) }).await?;
}
```

同 key 串行用 `.resource(key)`（容量-1 资源门）；make 式新鲜度检查用
`.up_to_date(|| ..)`。

## Recipe: middleware 扩展 ctx

```rust
#[derive(serde::Deserialize, serde::Serialize, Default)]
struct UserExt { user: String }

let wf = create_workflow(CreateWorkflowConfig::new("send-receipt").input::<serde_json::Value>())
    .middleware::<UserExt>(
        Middleware::new().produce(|_ctx| Ok(serde_json::json!({ "user": "alice" }))),
    )
    .handler(|ctx| async move {
        // ctx.ext.user 现在是 typed 的
        ctx.step("email", |_sc| async move {
            Ok(serde_json::json!({ "to": ctx.ext.user }))
        }).await?;
        Ok(serde_json::Value::Null)
    });
```

与上游的差异：`Ext` 是**单一字段**（最后一个 `.middleware::<PExt>()` 决定），
不是 TS 的类型交集。

## Recipe: 跨版本 resume

```rust
// 存量 run 跑在 v1；新代码是 v2。引擎按持久化的 workflowVersion 路由。
let v2 = create_workflow(CreateWorkflowConfig::new("pipeline").version("v2"))
    .previous_versions(vec![v1])          // v1 的代码保持可达
    .handler(|_ctx: WorkflowCtx| async move { /* v2 逻辑 */ });
```

resume 时引擎读 run 里的 `workflowVersion`，在 `[当前] + previous_versions` 里
选定义（`select_workflow_version`）；匹配不上不回退。

## Recipe: 失败重试——`continue_from`（本地扩展）

**失败即终局**：`STEP_FAILED` 只会 rethrow，不自动重跑。重试靠 `continue_from`
或新开 run。**TanStack 没有这个能力**，是本仓的一等公民扩展：

```rust
let outcome = run_workflow(
    &RunWorkflowOptions::new(Arc::new(wf.clone()), store)
        .input(input)
        .run_id("run-1")
        .continue_from("charge"),   // 截断 charge 的终态 checkpoint 及后缀
).await?;
// 重放：前缀短路，charge 起的后缀从零重跑
```

命中即停用 `.target_step("gen-pdf")`（同为本地扩展）。
机制详见 `AGENTS.md` 的「日志是短路索引」。

## Recipe: 换 store

store 是可插拔的，workflow 代码不动。本仓三个实现跑**同一份契约套件**：

| store | 契约 | 并发边界 |
| --- | --- | --- |
| `InMemoryStore`（core） | `RunStore`（core 直驱） | 单进程 |
| `FileExecutionStore`（`examples/store_file`） | `WorkflowExecutionStore` | 单进程（文档写明） |
| `workflow-store-sqlx-postgres` | `WorkflowExecutionStore` | **多 worker**（Postgres 行锁） |

写新 store 实现 `WorkflowExecutionStore`（**不是** `RunStore`），然后跑
`aa_workflow_runtime::store_contract::run_store_contract`。规则见 `AGENTS.md`。

> 注意 `run_workflow` 吃的是 core 的 `Arc<dyn RunStore>`（旧形状）；手上有
> `WorkflowExecutionStore` 时用 `create_run_store_adapter` 降格。

## 与上游 quick-start 的差异速览

- `step` 闭包返回 `serde_json::Value`（上游是泛型 `T`）；typed 读取走 `ctx.state`
  或事后反序列化。
- `run_workflow` **不是** async generator——返回 `RunOutcome`，事件回调走
  `.publisher(Some(..))`（对应上游 `publish`）。
- 多了 `continue_from` / `target_step`；少了 `recover` / `attach` / `signal` /
  `threadId` / `outputSink` / `telemetry`（对照表见 `docs/tanstack-alignment.md`）。
- 挂起即返回：所有 pause 类原语写完 checkpoint 就结束本次 drive。

## Where next

- `examples/guide` — guide 的可运行移植（唯一跑通 core + runtime 端到端的地方）
- `docs/runtime-design.md` — runtime 层决策记录（lease / sweep / timer / schedule）
- `docs/tanstack-alignment.md` — 对齐与分歧的完整论证
- `AGENTS.md` — store adapter 规则、`RunState` ≠ `ctx.state`、state 写入位置准则
- `examples/store_file` / `packages/workflow_store_sqlx_postgres` — 两个 store 参考
