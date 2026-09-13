//! TanStack Workflow [guide/index.md] 的 Rust 可运行移植。章节顺序与 guide
//! 一致，从上往下读就是执行顺序。
//!
//! 覆盖 guide 的 §1（定义 workflow）、§2（创建 runtime）、§5（启动与恢复）；
//! §6（sweep）演示空扫——注意我们 core 的 `ctx.sleep` 在挂起点自轮询（同一次
//! drive 内自动恢复），sweep 的 timer 投递只在「drive 已死」的 serverless 形态
//! 才参与（见 `docs/runtime-design.md` 的 D3）。
//!
//! guide 的 §3（OpenTelemetry）与 §4（Postgres）不在此例：前者 core 无 OTel
//! 集成，后者属于 store adapter（上游的 drizzle-postgres 对应物还没做）。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use workflow_core::{CreateWorkflowConfig, WorkflowDefinition, create_workflow};
use workflow_runtime::{
    InMemoryExecutionStore, WorkflowExecutionStore, WorkflowRegistration, WorkflowRuntime,
    WorkflowRuntimeConfig, WorkflowRuntimeDeliverSignalArgs, WorkflowRuntimeStartRunArgs,
    define_workflow_runtime,
};

// ============================================================
// §1 Define a workflow
// ============================================================
//
// guide 用 zod 声明 input/output；Rust 版用 serde 类型当 schema
// （`.input::<In>()`），handler 的 `ctx.input` 直接是强类型字段访问。

/// 对齐 guide 的 `FulfillmentInput { orderId, delayMs }`。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct FulfillmentInput {
    order_id: String,
    /// 支付就绪前的等待时长（ms）——对应 guide 的 `delayMs`。
    delay_ms: i64,
}

/// 对齐 guide 的 fulfillmentWorkflow：
/// reserve-inventory → sleepUntil → waitForEvent → ship-order。
fn fulfillment_workflow() -> WorkflowDefinition<FulfillmentInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("fulfillment").input::<FulfillmentInput>()).handler(
        |ctx: workflow_core::BaseCtx<FulfillmentInput>| async move {
            let order_id = ctx.input.order_id.clone();
            let reserve_order_id = order_id.clone();
            let ship_order_id = order_id.clone();
            let delay_ms = ctx.input.delay_ms;

            // 副作用走 ctx.step；`step.id` 是幂等键（对齐 guide 的
            // `idempotencyKey: stepCtx.id`）。
            let _reservation = ctx
                .step(
                    "reserve-inventory",
                    move |step: workflow_core::StepCtx| async move {
                        let order_id = reserve_order_id;
                        // 真实场景这里调库存服务。
                        Ok(serde_json::json!({
                            "reservationId": format!("res_{order_id}"),
                            "idempotencyKey": step.id,
                        }))
                    },
                )
                .await?;

            // 耐久等待：挂起 → 持久化 → 恢复后从这里继续。
            let now = ctx.now()?;
            ctx.sleep_until("ready-gate", now + delay_ms).await?;

            let payment = ctx.wait_for_event("payment", "payment-received").await?;
            let payment_id = payment
                .get("paymentId")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            ctx.step("ship-order", move |step: workflow_core::StepCtx| {
                let order_id = ship_order_id.clone();
                async move {
                    Ok(serde_json::json!({
                        "shipmentId": format!("ship_{order_id}"),
                        "idempotencyKey": step.id,
                    }))
                }
            })
            .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "shipped": true,
                "paymentId": payment_id,
            }))
        },
    )
}

// ============================================================
// §2 Create a runtime
// ============================================================
//
// guide 用 `defineWorkflowRuntime({ store, workflows })`；Rust 版
// `define_workflow_runtime`。in-memory store 仅供测试与演示（进程一退数据
// 全没）；生产换 DB 后端实现同一套契约即可，workflow 代码不动。

fn define_runtime() -> (WorkflowRuntime, Arc<InMemoryExecutionStore>) {
    let mem: Arc<InMemoryExecutionStore> = Arc::new(InMemoryExecutionStore::default());
    let store: Arc<dyn WorkflowExecutionStore> = mem.clone();

    let mut workflows = HashMap::new();
    workflows.insert(
        "fulfillment".to_string(),
        WorkflowRegistration {
            workflow: fulfillment_workflow().into_workflow(),
            previous_versions: vec![],
            version_override: None,
        },
    );

    (
        define_workflow_runtime(WorkflowRuntimeConfig::new(store, workflows)),
        mem,
    )
}

// ============================================================
// §5 Start and resume runs
// ============================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (runtime, store) = define_runtime();
    let runtime = Arc::new(runtime);

    let run_id = "fulfillment:order-42";
    println!("── start_run（第一次调用）：驱动到下一个挂起点");
    let rt = Arc::clone(&runtime);
    let drive = tokio::spawn(async move {
        rt.start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "fulfillment".into(),
            run_id: run_id.into(),
            input: serde_json::json!({ "orderId": "order-42", "delayMs": 100 }),
            ..Default::default()
        })
        .await
    });

    // 等 run 停在 `waitForEvent('payment-received')`。
    //
    // 注意：run 会先在 sleepUntil 挂起（等 readyAt）——core 自轮询 100ms 后
    // 自动恢复，继续走到 waitForEvent 才真正需要外部信号。所以这里等的不是
    // 「Paused 状态」（sleep 也是 Paused），而是具体的等待信号名。
    wait_for_signal(&store, run_id, "payment-received").await;
    let paused = store.load_run(run_id)?.expect("run 存在");
    println!(
        "── run 挂起：status = {:?}，等信号 {:?}（RunState 投影，无需扫日志）",
        paused.status,
        paused.waiting_for.as_ref().map(|w| &w.signal_name)
    );

    // 支付 webhook 到达：用稳定的 signalId 投递（重试幂等）。
    println!("── deliver_signal（第二次调用 = webhook）：投递 payment-received");
    let delivered = runtime
        .deliver_signal(WorkflowRuntimeDeliverSignalArgs {
            run_id: run_id.into(),
            signal_id: "stripe:evt:1".into(),
            name: "payment-received".into(),
            payload: serde_json::json!({ "paymentId": "pay_1" }),
            ..Default::default()
        })
        .await?;

    // 注意：挂起中的 drive 仍在自轮询并持有 lease，所以这里的「尝试认领」
    // 得到 NotClaimable——恢复由那个存活的 drive 完成（25ms 内拾取
    // StepResume）。serverless 形态（drive 已死）下认领会成功、由本次调用
    // 驱动完成。
    println!(
        "── deliver 返回 {:?}（恢复已交给存活的 drive）",
        delivered.kind
    );

    // 等第一次调用的 drive 跑完。
    let finished = tokio::spawn(drive).await.unwrap()??;
    println!(
        "── 完成：kind = {:?}，output = {}",
        finished.kind,
        finished
            .run
            .as_ref()
            .and_then(|r| r.output.clone())
            .map(|o| o.to_string())
            .unwrap_or_default()
    );

    // ============================================================
    // §6 Wake timers and schedules
    // ============================================================
    //
    // sweep 的职责：恢复陈旧 run + 投递到期 timer + 启动到期 schedule 桶。
    // 本例单进程且 drive 存活，timer 由 core 自轮询消化，sweep 自然为空——
    // 它在 serverless 形态（drive 已死）下才承担投递。见 runtime-design.md。

    let sweep = runtime.sweep(Default::default()).await?;
    println!(
        "── sweep：recovered = {}，scheduled = {}，timers = {}，remaining_may_exist = {}",
        sweep.recovered.len(),
        sweep.scheduled.len(),
        sweep.timers.len(),
        sweep.remaining_may_exist
    );

    Ok(())
}

async fn wait_for_signal(store: &InMemoryExecutionStore, run_id: &str, signal: &str) {
    for _ in 0..500 {
        if let Some(run) = store.load_run(run_id).unwrap()
            && run
                .waiting_for
                .as_ref()
                .map(|w| w.signal_name == signal)
                .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run {run_id} 迟迟未开始等信号 {signal}");
}
