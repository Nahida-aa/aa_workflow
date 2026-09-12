# aa-workflow

Rust 版耐久执行（durable execution）引擎，对标
[TanStack Workflow](https://tanstack.com/workflow) 的 **core engine** 层：
headless replay 引擎 + `RunStore` 契约，无调度器、无 host adapter、无托管控制面。

一句话内核：**代码即 DAG** —— 没有声明式图，编排就是普通 async 代码。

```rust
分支  = if / else
并行  = tokio::try_join!
顺序  = 词法 .await
耐久  = ctx.step(...).await   // 唯一会被 checkpoint 的副作用
```

DAG 不是预先声明的，是 handler 跑出来的 per-run 涌现产物。每次 drive 从头重放
handler，引擎按 `step_id` 短路已成功的 checkpoint。

> **状态**：实验阶段（edition 2024）。尚未接回 LocalDub，core 里只有 phase-0 的
> `InMemoryStore`，没有可执行 binary —— 示例通过 `cargo test` 驱动。
>
> 对标范围是 TanStack 的 core engine，**不含**它的 `@tanstack/workflow-runtime`
> 层（lease / sweep / timer 索引 / schedules）——见[已知边界](#已知边界)。

---

## 快速开始

```bash
cargo test -p workflow-core              # 引擎级测试
cargo test -p workflow-examples-shared   # 示例 workflow e2e
cargo clippy --workspace --all-targets
```

## 定义一个 workflow

输入用 serde 类型当 schema（Rust 版 zod `inputSchema`），handler 拿到的
`ctx.input` 直接就是 `In`，没有 `.get(...)` 链。

```rust
use workflow_core::{Backoff, BaseCtx, CreateWorkflowConfig, RetryPolicy,
                    StepCtx, StepOptions, WorkflowDefinition, create_workflow};

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SagaInput { order_id: String, #[serde(default)] expedited: bool }

pub fn fulfillment_saga() -> WorkflowDefinition<SagaInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("fulfillment-saga").input::<SagaInput>())
        .handler(|ctx: BaseCtx<SagaInput>| async move {
            let order_id = ctx.input.order_id.clone();

            // 并行 = 普通 try_join!；charge 带重试
            let (pdf, charged) = tokio::try_join!(
                {
                    let id = order_id.clone();
                    ctx.step("gen-pdf", move |_sc: StepCtx| {
                        let id = id.clone();
                        async move { Ok(serde_json::json!({ "pdf": format!("{id}.pdf") })) }
                    })
                },
                {
                    let id = order_id.clone();
                    ctx.step_with(
                        "charge",
                        StepOptions::new().retry(RetryPolicy::new(3, Backoff::Fixed { base_ms: 1 })),
                        move |_sc: StepCtx| {
                            let id = id.clone();
                            async move {
                                payment_gateway::charge(&id)?;   // 副作用
                                Ok(serde_json::json!({ "charged": true }))
                            }
                        },
                    )
                },
            )?;

            if ctx.input.expedited {                 // 分支 = 普通 if
                ctx.step("notify", move |_sc: StepCtx| async move {
                    Ok(serde_json::json!({ "notified": true }))
                }).await?;
            }

            Ok(serde_json::json!({ "orderId": order_id, "pdf": pdf, "charge": charged }))
        })
}
```

`StepOptions` 还支持 `.timeout()`、`.resource(key)`（同 key 串行）、
`.up_to_date(|| ...)`（make 式 freshness 检查）。

## 驱动

```rust
let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
let outcome = run_workflow(
    &workflow,                    // &Workflow
    store.clone(),
    &RunOptions::new(json!({ "orderId": "A-1" }))
        .run_id("run_1")          // 复用 run_id ⇒ resume
        .continue_from("charge")  // 从该 step 截断后重跑后缀
        .target_step("gen-pdf")   // 命中即停
        .deadline(now_ms + 30_000),
    Some(tracing_publisher()),    // 事件回调
).await?;
```

`RunOutcome { run_id, status, output, error }`。

引擎入口：

| 函数 | 作用 |
| --- | --- |
| `run_workflow(wf, store, opts, publisher)` | 跑 / 续跑（每次调用都完整重放 handler） |
| `run_workflow_sync(...)` | 内部建 tokio runtime 的同步壳 |
| `signal_run(store, run_id, step_id, payload)` | 按 pause key 投递信号（唤醒 `approve`） |
| `signal_event(store, run_id, name, payload)` | 按事件名投递；无监听者 → `SignalLost` |
| `cancel_run(store, run_id)` | run 以 `Aborted` 终局；终态幂等 |
| `select_workflow_version(wf, persisted)` | 按持久化版本路由到旧 handler |

## 耐久原语

`ctx` 上所有会产生 checkpoint 的原语。持久到哪儿由 `RunStore` 实现决定——内存、
本地文件、SQL 数据库、云 KV 都可能（TanStack 侧就有 drizzle-postgres 与
Cloudflare D1 的 store adapter），引擎不假定介质，也不假定一定有磁盘。它保证的
只是「进事件日志、参与 replay」。
key **必填**（确定性 / 可重入所需，与 TanStack 的可选 `id` 不同）。

| 方法 | 语义 |
| --- | --- |
| `step(id, f)` / `step_with(id, opts, f)` | 唯一带 checkpoint 的副作用 |
| `sleep(key, dur)` / `sleep_until(key, ts)` | 耐久定时器，到期由引擎自投递 |
| `wait_for_event(key, name)` | 等命名外部事件 |
| `approve(key, title)` | 人工审批；`signal_run` 投递后返回 payload |
| `now()` / `uuid()` | 确定性值，首次记录后 replay 读回 |
| `yield_()` | runtime budget 内的协作让出（`yield` 是保留字） |
| `emit(name, value)` | **只投 publisher，不进日志、不参与 replay** |

非耐久：`is_cancelled()`、`deadline()`、`time_remaining()`、`should_yield()`、
`StepCtx::progress(0.0..=1.0)`。

## 确定性契约

和 TanStack 一样，handler **必须**在每次 replay 时以相同顺序触达相同的原语。
这不是可选项，而是 replay 模型的直接后果：

```rust
// 违反确定性：
let t = SystemTime::now();        // 用 ctx.now()
let id = Uuid::new_v4();          // 用 ctx.uuid()
if fetch_flag().await? { ... }    // 把 fetch 包进 ctx.step()

// 安全：
let t = ctx.now()?;
let id = ctx.uuid()?;
let flag = ctx.step("flag", fetch_flag).await?;
if flag { ... }
```

引擎**不检测也不强制**这一点（TanStack 同样只是把它写成 footgun）。写错了的表现是
replay 漂移或 checkpoint 错配，不会报错。

state 变更会在 replay 上重跑，之所以能确定性重放，是因为它只依赖 replay 出来的
step 结果。

## 事件日志

单一真相源是 **append-only 事件日志**，`serde(tag = "type")` 命名与 TS 侧同构
（`{"type":"STEP_FINISHED","stepId":...}`）。

- **checkpoint（进日志，参与 replay）**：`StepFinished` / `StepFailed` / `StepPaused` / `StepResume`
  / `RunFinished` / `RunErrored` / `NowRecorded` / `UuidRecorded`
- **observability（只投 publisher）**：`RunStarted` / `StepStarted` / `StepProgress` / `Custom`

per-step 状态是**派生投影**，从不独立存储：`fold_step_states(events) -> HashMap<String, StepState>`
（`Pending`/`Running`/`Success`/`Failed`/`Paused`）。

`run.json` 把挂起态做成一等投影，观察者不扫日志就知道 run 在等什么：
`waiting_for: {step_id, signal_name, deadline}` / `pending_approval: {step_id, approval_id, title}`。

**失败即终局**：`StepFailed` 只会 rethrow，不会自动重跑。重试靠 `continue_from`
（store 层截断到该 step 最新终态 checkpoint，含）或新开 run。

## 实现自己的 store

```rust
pub trait RunStore: Send + Sync {
    // 1. metadata
    fn get_run_state(&self, run_id: &str) -> Result<Option<RunState>, StoreError>;
    fn set_run_state(&self, run_id: &str, state: &RunState) -> Result<(), StoreError>;
    fn delete_run(&self, run_id: &str) -> Result<(), StoreError>;
    // 2. append-only log
    fn append_event(&self, run_id: &str, expected_next_index: usize,
                    event: &RunEvent) -> Result<(), StoreError>;   // MUST CAS
    fn get_events(&self, run_id: &str) -> Result<Vec<RunEvent>, StoreError>;
    fn truncate_runs(&self, run_id: &str, step_id: &str) -> Result<(), StoreError>;
    fn subscribe(&self, run_id: &str) -> Option<Receiver<RunEvent>> { None }
}
```

`append_event` 的 `expected_next_index` 是乐观并发护栏：长度不符**必须**返回
`StoreError::Conflict`，引擎侧会 rebase + retry（`engine/mod.rs:140`）。做不到截断的
store 必须报 `StoreError::Io`，不许静默 no-op。

内置 `InMemoryStore`（phase-0，含 subscribe fan-out）；示例层另有以文件为介质的
`FileRunStore`（`run.json` + `events.jsonl`，`publish = false`）。两者都只是
`RunStore` 的一种实现，换成数据库后端不需要动引擎。

`RunState` 的 `input` / `output` 默认擦除成 `Value`（store 是 `dyn`，装不下泛型），
要具体类型就 `state.into_typed::<In, Out>()?` —— 对应 TanStack 的
`RunState<TInput, TOutput>`，但他们是 cast（不校验），我们真的反序列化（会 `Err`）。

## 与 TanStack 的关系

采纳其命名与 store/event 契约；执行模型在**第二轮**才换成 handler 重放（第一版是
显式 `needs` 图 + 调度器，已删除）。完整论证见 [`docs/tanstack-alignment.md`](docs/tanstack-alignment.md)，
逐 API 对等矩阵见 [`packages/workflow-core/PARITY.md`](packages/workflow-core/PARITY.md)。

**不是差异的地方**（容易误读）：

- **确定性契约两边相同** —— TanStack 同样要求 handler 在 replay 上以相同顺序触达
  相同原语（`docs/concepts/replay-and-resume.md:39`）。见上节。
- **两边都不需要常驻 server** —— TanStack 的 core engine 刻意不做调度器 / host
  adapter，部署靠 serverless cron handler 调 `runtime.sweep()`；需要常驻控制面的是
  Temporal，不是 TanStack。

真正的差异（都是刻意的）：

- `state` 是 **per-drive 工作副本**，不是 TS 的 in-place 共享引用。`ctx.clone()`
  产生独立快照，并行 step 各自持快照互不可见，只在 durable 边界 flush 回镜像。
- run 级取消只有 `is_cancelled()` 谓词，**step 闭包内的 `await` 无法被中断**，
  闭包必须协作式自检。
- middleware 的 Ext 是**单一字段**而非 TS 的类型交集，最后一个
  `.middleware::<PExt>()` 决定 `Ext`。
- schema 即 serde 类型，没有 zod schema 实例对象。
- 本地一等公民（TS 没有）：`continue_from`、`target_step`、`resource` 门、`up_to_date`。
- 挂起等待是**轮询式**（25ms 重读日志），因为下游 `FsRunStore` 无 `subscribe`。

## 仓库结构

```
packages/workflow-core/     引擎本体
  src/define.rs             Workflow / WorkflowBuilder / BaseCtx / StepOptions
  src/engine/mod.rs         EngineRuntime、exec_step / exec_pause、signal_*
  src/engine/run_workflow.rs  单次 drive 的顶层编排、RunOptions
  src/event.rs              RunEvent / RunStatus / fold_step_states
  src/run_store/mod.rs      RunStore trait + RunState 信封
  src/run_store/in_memory.rs  InMemoryStore
  src/resource.rs           容量-1 的资源门
examples/shared/            host 无关示例层（10 个 workflow + FileRunStore + drive 薄壳）
docs/tanstack-alignment.md  对齐决策记录（含推翻第一轮的论证）
```

示例层强约束：只依赖 `workflow-core` + tokio，**不依赖 LocalDub**。

## 已知边界

对标范围止于 TanStack 的 core engine。**它对等的 `@tanstack/workflow-runtime`
层我们还没有**，即以下能力全部缺失：

- **多 worker 协调**：只有 CAS 原语，没有 lease / 心跳 / 抢占 / 陈旧 run 恢复。
  两个 worker 同时 drive 同一 run_id 会同时重放（日志不会坏，但不保证只有一个人在跑）。
- **sweep**：没有统一的后台单元做崩溃恢复扫描、到期定时器投递、调度桶启动。
  sleep 目前靠引擎自己在挂起点轮询自投递。
- **schedules / cron**：没有 schedule 定义与分桶。
- **timer 索引**：`waiting_for.deadline` 有投影，但没有按时间索引的唤醒面，host
  只能轮询。

其他缺口：

- **没有生产级 store**：core 只有 `InMemoryStore`；`FileRunStore` 的 `append_event`
  是全量读 + 全量重写（O(n²)），且 `Mutex` 只在单进程内有效。
- **性能是平方级**：25ms 轮询 × 每次全量反序列化日志 × 全量线性扫。长 run 需注意。
- **确定性契约未强制**：引擎不检测 handler 的非确定性写法（TanStack 同样不检测）。
- **无 observability 集成**：`publisher` 是裸 `Arc<dyn Fn(&RunEvent)>`，core 不依赖
  tracing，接入要自己搭桥。
- `subscribe` 返回 `std::sync::mpsc::Receiver`（阻塞通道），async 上下文里要用
  `try_recv()`。
