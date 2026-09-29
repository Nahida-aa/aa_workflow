//! TanStack Workflow [guide/index.md] 的 Rust 可运行移植。章节顺序与 guide
//! 一致，从上往下读就是执行顺序。
//!
//! 覆盖 guide 的 §1（定义 workflow）、§2（创建 runtime）、§5（启动与恢复）、
//! §6（sweep）。§3（OpenTelemetry）与 §4（Postgres）不在此例：前者 core 无 OTel
//! 集成，后者属于 store adapter（上游的 drizzle-postgres 对应物还没做）。
//!
//! # 与 guide 的一处关键差异：挂起是「写完就返回」
//!
//! 上游 core 跑到挂起点（`sleep` / `waitForEvent` / `approve`）时，写 checkpoint
//! 后**立刻返回** `paused`，进程可以退出——唤醒一律来自外部。
//!
//! 本仓早期版本是「在挂起点自轮询等待」，**那是语义偏离**，已在
//! [`docs/runtime-design.md`] D3 修正为与上游一致。所以本文件里的每一步都是
//! 「drive 到挂起点 → 外部投递 → 再 drive」，这也正是 runtime 层存在的理由。
//!
//! 例子的**断言**在 `#[cfg(test)] mod guide_tests` 里（`cargo test` 会跑）；
//! `main` 只是把同一批步骤打印出来，方便 `cargo run` 肉眼跟进。
//!
//! [`docs/runtime-design.md`]: ../../docs/runtime-design.md

use std::collections::HashMap;
use std::sync::Arc;

use aa_workflow_core::{CreateWorkflowConfig, WorkflowDefinition, create_workflow};
use aa_workflow_runtime::{
    InMemoryExecutionStore, RunResultKind, WorkflowExecutionStatus, WorkflowExecutionStore,
    WorkflowRegistration, WorkflowRuntimeConfig, WorkflowRuntimeDefinition,
    WorkflowRuntimeDeliverSignalArgs, WorkflowRuntimeStartRunArgs, define_workflow_runtime,
};

// ============================================================
// §1 Define a workflow
// ============================================================
//
// guide 用 zod 声明 input/output；Rust 版用 serde 类型当 schema
// （`.input::<TInput>()`），handler 的 `ctx.input` 直接是强类型字段访问。

/// 对齐 guide 的 `FulfillmentInput { orderId, delayMs }`。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FulfillmentInput {
    pub order_id: String,
    /// 支付就绪前的等待时长（ms）——对应 guide 的 `delayMs`。
    pub delay_ms: i64,
}

/// 对齐 guide 的 fulfillmentWorkflow：
/// reserve-inventory → sleepUntil → waitForEvent → ship-order。
///
/// 这条链正好覆盖三类挂起：**定时器**（`sleep_until`）与**外部信号**
/// （`wait_for_event`）——前者由 sweep 认领投递，后者由 webhook 投递。
pub fn fulfillment_workflow() -> WorkflowDefinition<FulfillmentInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("fulfillment").input::<FulfillmentInput>()).handler(
        |ctx: aa_workflow_core::BaseCtx<FulfillmentInput>| async move {
            let order_id = ctx.input.order_id.clone();
            let reserve_order_id = order_id.clone();
            let ship_order_id = order_id.clone();
            let delay_ms = ctx.input.delay_ms;

            // 副作用走 ctx.step；`step.id` 是幂等键（对齐 guide 的
            // `idempotencyKey: stepCtx.id`）。
            let _reservation = ctx
                .step(
                    "reserve-inventory",
                    move |step: aa_workflow_core::StepCtx| async move {
                        let order_id = reserve_order_id;
                        // 真实场景这里调库存服务。
                        Ok(serde_json::json!({
                            "reservationId": format!("res_{order_id}"),
                            "idempotencyKey": step.id,
                        }))
                    },
                )
                .await?;

            // 耐久等待（定时器）：挂起 → 落 checkpoint → 进程可以退出。
            // 唤醒由外部 timer 投递完成（见 §6 的 sweep）。
            let now = ctx.now()?;
            ctx.sleep_until("ready-gate", now + delay_ms).await?;

            // 耐久等待（外部信号）：等支付 webhook。
            let payment = ctx.wait_for_event("payment", "payment-received").await?;
            let payment_id = payment
                .get("paymentId")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            ctx.step("ship-order", move |step: aa_workflow_core::StepCtx| {
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

/// 双柄：`mem` 供示例直读内部，`rt` 是给调用方的 runtime。
pub struct Fixture {
    pub rt: Arc<WorkflowRuntimeDefinition>,
    pub mem: Arc<InMemoryExecutionStore>,
}

pub fn define_runtime() -> Fixture {
    let mem: Arc<InMemoryExecutionStore> = Arc::new(InMemoryExecutionStore::default());
    let store: Arc<dyn WorkflowExecutionStore> = mem.clone();

    let mut workflows = HashMap::new();
    workflows.insert(
        "fulfillment".to_string(),
        WorkflowRegistration {
            load: Arc::new(|| fulfillment_workflow().into_workflow()),
            previous_versions: HashMap::new(),
            version_override: None,
            schedules: vec![],
        },
    );

    Fixture {
        rt: Arc::new(define_workflow_runtime(WorkflowRuntimeConfig::new(
            store, workflows,
        ))),
        mem,
    }
}

// ============================================================
// §5 Start and resume runs
// ============================================================

/// 走到第一个挂起点：`start_run` 在 `ready-gate` 上挂起并**立即返回**
/// `Paused`（不等那 `delay_ms`）。
pub async fn step_start_and_pause(fx: &Fixture, run_id: &str, delay_ms: i64) -> StartOutcome {
    let started = fx
        .rt
        .start_run(WorkflowRuntimeStartRunArgs {
            workflow_id: "fulfillment".into(),
            run_id: run_id.into(),
            input: serde_json::json!({ "orderId": "order-42", "delayMs": delay_ms }),
            ..Default::default()
        })
        .await
        .expect("start_run 不应失败");

    let run = fx
        .mem
        .load_run(run_id)
        .expect("load_run 不应失败")
        .expect("run 应已存在");

    StartOutcome {
        kind: started.kind,
        status: run.status,
        waiting_signal: run.waiting_for.map(|w| w.signal_name),
        wake_at: None,
    }
}

pub struct StartOutcome {
    pub kind: RunResultKind,
    pub status: WorkflowExecutionStatus,
    pub waiting_signal: Option<String>,
    pub wake_at: Option<i64>,
}

// ============================================================
// §6 Wake timers and schedules
// ============================================================
//
// sweep 的三件事：恢复陈旧 run + **投递到期 timer** + 启动到期 schedule 桶。
//
// 关键：core 不自投递 timer（D3），所以挂了 `sleep` 的 run 必须靠 sweep 唤醒。
// 「走到睡眠 → sweep 认领 → 再 drive」是**必需**的一环，不是可选优化。

/// 驱动到 `wait_for_event('payment-received')` 为止——中间那个 `sleep_until`
/// 由 sweep 负责跨过去。
pub async fn drive_to_payment_wait(
    fx: &Fixture,
    run_id: &str,
    delay_ms: i64,
) -> StartOutcome {
    let mut outcome = step_start_and_pause(fx, run_id, delay_ms).await;

    // 循环：挂在 `__timer` 上就等 deadline 到点 → sweep 认领投递 → 再看。
    // 上限只是防止写错时死循环。
    for _ in 0..8 {
        if outcome.waiting_signal.as_deref() != Some("__timer") {
            return outcome;
        }
        let run = fx.mem.load_run(run_id).unwrap().unwrap();
        let deadline = run
            .waiting_for
            .as_ref()
            .and_then(|w| w.deadline)
            .expect("__timer 挂起应带 deadline");

        // 等到点（示例里 delay 很小，通常会立刻到）。
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        if deadline > now {
            tokio::time::sleep(std::time::Duration::from_millis((deadline - now) as u64)).await;
        }

        let sweep = fx.rt.sweep(Default::default()).await.expect("sweep 不应失败");
        if sweep.timers.is_empty() {
            // 没有到期的 timer，交给调用方判断（正常不该发生）。
            return outcome;
        }

        let run = fx.mem.load_run(run_id).unwrap().unwrap();
        outcome = StartOutcome {
            kind: if run.status == WorkflowExecutionStatus::Paused {
                RunResultKind::Paused
            } else {
                RunResultKind::Completed
            },
            status: run.status,
            waiting_signal: run.waiting_for.map(|w| w.signal_name),
            wake_at: None,
        };
    }
    outcome
}

/// 支付 webhook 到达：投递信号。**这次投递会自己认领并驱动到完成**——挂起时
/// core 已返回、lease 已释放，所以不存在「还活着的 drive」需要等。
pub async fn step_deliver_payment(
    fx: &Fixture,
    run_id: &str,
) -> aa_workflow_runtime::RunResult {
    fx.rt
        .deliver_signal(WorkflowRuntimeDeliverSignalArgs {
            run_id: run_id.into(),
            // 稳定的 signalId → 重试幂等（webhook 常会重投）。
            signal_id: "stripe:evt:1".into(),
            name: "payment-received".into(),
            payload: serde_json::json!({ "paymentId": "pay_1" }),
            ..Default::default()
        })
        .await
        .expect("deliver_signal 不应失败")
}

// ============================================================
// main：把上面几步跑一遍并打印（断言在 tests 里）
// ============================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let fx = define_runtime();
    let run_id = "fulfillment:order-42";

    println!("── start_run：驱动到第一个挂起点就返回（不阻塞）");
    let paused = drive_to_payment_wait(&fx, run_id, 100).await;
    println!(
        "── 挂起于 {:?}：status = {:?}，等 {:?}",
        paused.kind, paused.status, paused.waiting_signal
    );

    println!("── deliver_signal（= webhook）：投递 payment-received");
    let delivered = step_deliver_payment(&fx, run_id).await;
    println!(
        "── deliver 返回 {:?}（挂起时 lease 已释放，所以这次投递自己认领并驱动完成）",
        delivered.kind
    );
    println!(
        "── output = {}",
        delivered
            .run
            .as_ref()
            .and_then(|r| r.output.clone())
            .map(|o| o.to_string())
            .unwrap_or_default()
    );

    let sweep = fx.rt.sweep(Default::default()).await?;
    println!(
        "── sweep（已无事可做）：recovered = {}，scheduled = {}，timers = {}",
        sweep.recovered.len(),
        sweep.scheduled.len(),
        sweep.timers.len()
    );

    Ok(())
}

// ============================================================
// 断言：与 main 走同一条路径，但把每步钉死
// ============================================================

#[cfg(test)]
mod guide_tests {
    use super::*;
    use aa_workflow_runtime::WorkflowExecutionStatus;

    /// guide §5+§6 的完整链路：定时器挂起 → sweep 唤醒 → 信号挂起 → 投递完成。
    ///
    /// 这条链路是 `examples/shared` 覆盖不到的——那里只有 core（无 runtime），
    /// 而「谁登记 timer、谁认领它」正是 runtime 的职责。
    #[tokio::test]
    async fn guide_end_to_end_timer_then_signal() {
        let fx = define_runtime();
        let run_id = "fulfillment:order-42";

        // ① start_run 走到定时器挂起点，**立即**返回 Paused。
        let started = step_start_and_pause(&fx, run_id, 100).await;
        assert_eq!(
            started.kind,
            RunResultKind::Paused,
            "start_run 应停在第一个挂起点"
        );
        assert_eq!(started.status, WorkflowExecutionStatus::Paused);
        assert_eq!(
            started.waiting_signal.as_deref(),
            Some("__timer"),
            "第一个挂起点是 sleep_until 的定时器"
        );

        // ② sweep 认领到期 timer 并投递 → run 越过 sleep_until，停在信号上。
        let at_signal = drive_to_payment_wait(&fx, run_id, 100).await;
        assert_eq!(
            at_signal.waiting_signal.as_deref(),
            Some("payment-received"),
            "跨过定时器后应停在外部信号上（这靠 sweep，不靠 core 自投递）"
        );
        assert_eq!(at_signal.status, WorkflowExecutionStatus::Paused);

        // ③ 投递支付信号 → 这一次调用自己认领 + 驱动到完成。
        let delivered = step_deliver_payment(&fx, run_id).await;
        assert_eq!(
            delivered.kind,
            RunResultKind::Completed,
            "挂起时 lease 已释放，deliver 应自己认领并跑完"
        );
        assert_eq!(
            delivered.run.as_ref().and_then(|r| r.output.clone()),
            Some(serde_json::json!({
                "orderId": "order-42",
                "shipped": true,
                "paymentId": "pay_1",
            }))
        );

        // ④ 完事之后 sweep 应该是空的——没有陈旧 run、没有到期 timer。
        let sweep = fx.rt.sweep(Default::default()).await.unwrap();
        assert_eq!(sweep.recovered.len(), 0, "没有崩溃的 run");
        assert_eq!(sweep.timers.len(), 0, "已完成 run 不该留下到期 timer");
    }

    /// webhook 重投（同 `signalId`）应该是幂等 no-op，不会把 run 再跑一遍。
    #[tokio::test]
    async fn guide_redelivered_webhook_is_idempotent() {
        let fx = define_runtime();
        let run_id = "fulfillment:order-7";

        drive_to_payment_wait(&fx, run_id, 10).await;
        let first = step_deliver_payment(&fx, run_id).await;
        assert_eq!(first.kind, RunResultKind::Completed);

        // 同 signalId 再投一次。
        let again = step_deliver_payment(&fx, run_id).await;
        assert_eq!(
            again.kind,
            RunResultKind::Duplicate,
            "同 signalId 重投应识别为重复（webhook 重试是常态）"
        );
        // ship-order 只跑过一次。
        let events = fx
            .mem
            .load_execution(run_id)
            .unwrap()
            .expect("run 存在")
            .events;
        let shipped = events
            .iter()
            .filter(|e| {
                matches!(
                    e.event,
                    aa_workflow_core::WorkflowEvent::StepFinished { ref step_id, .. }
                        if step_id == "ship-order"
                )
            })
            .count();
        assert_eq!(shipped, 1, "重投不该让副作用跑第二遍");
    }

    /// `reserve-inventory` 的 checkpoint 在恢复时被重放短路——只落一条记录。
    #[tokio::test]
    async fn guide_completed_step_is_replayed_from_log() {
        let fx = define_runtime();
        let run_id = "fulfillment:order-9";

        drive_to_payment_wait(&fx, run_id, 10).await;
        step_deliver_payment(&fx, run_id).await;

        let events = fx
            .mem
            .load_execution(run_id)
            .unwrap()
            .expect("run 存在")
            .events;
        let reserved = events
            .iter()
            .filter(|e| {
                matches!(
                    e.event,
                    aa_workflow_core::WorkflowEvent::StepFinished { ref step_id, .. }
                        if step_id == "reserve-inventory"
                )
            })
            .count();
        assert_eq!(
            reserved, 1,
            "三次 drive 之后，已完成的 step 仍只应有一条 checkpoint"
        );
    }
}
