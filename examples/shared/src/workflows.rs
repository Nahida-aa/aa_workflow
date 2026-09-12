//! 示例 workflow 定义（handler 形态，代码即 DAG）。
//!
//! 输入用 `.with_input::<T>()` 声明（Rust 版 zod `inputSchema`：serde
//! Deserialize 即运行时校验，缺字段/错类型在首次恢复时报 schema 错误）；
//! handler 收到 [`TypedCtx<T>`]，`ctx.input()` 直接是 `&T`。输出类型从
//! handler 返回值自动推断（对齐 TanStack 的返回值推断，无 output 声明）。
//!
//! - [`fulfillment_saga`]（id `fulfillment-saga`）— 并行（`tokio::try_join!`）+ retry
//!   + 分支：演示 authoring 面；扣款等副作用**不注入**，走模块级 [`payment_gateway`]
//!     服务（对齐官方 pocs：workflow 无依赖参数，effect 写死在 handler 内）。
//!
//! - [`fulfillment`]（id `fulfillment`）— 对齐官方 `fulfillmentWorkflow` / wf-demo：
//!   `reserve-inventory` → `readyAt` 定时闸门（sleep）→ `payment-received` 事件等待 → `ship-order`。
//! - [`approval_review`] — 人工审批：`ctx.approve` 持久化一个等待点，
//!   外部用 [`workflow_core::signal_run`] 交付决定后继续（signals 语义载体）。
//! - [`approval_order`]（id `approval-order`）— 对齐 wf-demo：金额阈值决定是否人工
//!   审批，拒绝分支；`email_digest` 是 resume / `continue_from` 的行为载体。
//! - [`invoice`]（id `invoice`）— 对齐 wf-demo：连续两个 `sleep`（双定时器，引擎自动唤醒）。
//! - [`compliance`]（id `compliance`）— 对齐 wf-demo：连续两次事件等待后归档。
//! - [`refund`]（id `refund`）— 对齐 wf-demo 混合链：事件 → 定时闸门 → 审批 → 步骤。

use std::time::Duration;
use workflow_core::{
    Backoff, RetryPolicy, StepCtx, StepOptions, TypedCtx, TypedWorkflow, Workflow, WorkflowCtx,
};

// ========================================================================
// 输入类型 = workflow 的「schema」（Rust 版 zod `inputSchema`）：serde
// Deserialize 即运行时校验，`.with_input::<In>()` 后 handler 拿到
// `TypedCtx<In>`，`ctx.input()` 直接是 `&In`，没有 `.get(...)` 链。
// ========================================================================

/// 履约 saga 输入。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FulfillmentSagaInput {
    pub order_id: String,
    #[serde(default)]
    pub expedited: bool,
}

/// 邮件 digest 输入。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EmailDigestInput {
    pub days: i64,
}

/// 履约流输入（对齐 wf-demo `FulfillmentInput`）。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FulfillmentInput {
    pub order_id: String,
    pub ready_at: i64,
}

/// 审批流输入。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalOrderInput {
    pub order_id: String,
    pub amount: i64,
    #[serde(default = "default_currency")]
    pub currency: String,
}

/// 发票输入。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvoiceInput {
    pub order_id: String,
    #[serde(default = "default_t1")]
    pub t1: u64,
    #[serde(default = "default_t2")]
    pub t2: u64,
}

/// 合规归档输入。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ComplianceInput {
    pub subject_id: String,
}

/// 退款输入。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefundInput {
    pub order_id: String,
    pub amount: i64,
    #[serde(default = "default_currency")]
    pub currency: String,
    #[serde(default)]
    pub refund_at: Option<i64>,
}

fn default_currency() -> String {
    "USD".to_string()
}

fn default_t1() -> u64 {
    1_000
}

fn default_t2() -> u64 {
    3_000
}

/// 模拟"外部扣款"的模块级服务（对齐官方 pocs：workflow 不注入依赖，
/// 副作用就写成模块作用域里的服务函数）。用可配置的全局状态模拟支付网关
/// 的瞬时故障，好让 retry / `continue_from` 测试可控。
pub mod payment_gateway {
    use std::sync::{LazyLock, Mutex};

    #[derive(Default)]
    struct State {
        attempts: usize,
        fail_first: usize,
        fail_always: bool,
    }

    static STATE: LazyLock<Mutex<State>> = LazyLock::new(Mutex::default);

    /// 扣款一次：前 `fail_first` 次（或 `fail_always` 置位时一直）返回瞬时
    /// 错误，由 workflow 步骤的 retry 策略吸收。
    pub fn charge(order_id: &str) -> anyhow::Result<()> {
        let mut st = STATE.lock().expect("payment gateway poisoned");
        st.attempts += 1;
        if st.fail_always || st.attempts <= st.fail_first {
            return Err(anyhow::anyhow!(
                "payment gateway down (attempt {})",
                st.attempts
            ));
        }
        tracing::info!(target: "examples", "charged {order_id}");
        Ok(())
    }

    /// 已尝试次数（含被 retry 吸收的失败）。
    pub fn attempts() -> usize {
        STATE.lock().expect("payment gateway poisoned").attempts
    }

    /// 让前 `n` 次扣款失败（测试用）。
    pub fn set_fail_first(n: usize) {
        STATE.lock().expect("payment gateway poisoned").fail_first = n;
    }

    /// 一直失败（测试用）。
    pub fn set_fail_always(yes: bool) {
        STATE.lock().expect("payment gateway poisoned").fail_always = yes;
    }

    /// 复位全局服务状态（测试用）。
    pub fn reset() {
        *STATE.lock().expect("payment gateway poisoned") = State::default();
    }
}

/// 履约 saga：`gen-pdf` 与 `charge` 并行（`try_join!`），扣款带重试
/// （瞬时失败经 [`payment_gateway`] 模拟）；`input.expedited` 为真时
/// 追加 `notify`（分支 = 普通 `if`）。输入 `orderId` 必填，缺失即
/// run 首次恢复时报 schema 错误（相当于 zod `.safeParse` 失败）。
pub fn fulfillment_saga() -> TypedWorkflow<FulfillmentSagaInput, serde_json::Value> {
    Workflow::new("fulfillment-saga")
        .with_input::<FulfillmentSagaInput>()
        .handler(|ctx: TypedCtx<FulfillmentSagaInput>| async move {
            let input = ctx.input();
            let order_id = input.order_id.clone();
            let expedited = input.expedited;

            let (pdf, charged) = tokio::try_join!(
                {
                    let order_id = order_id.clone();
                    ctx.step("gen-pdf", move |_sc: StepCtx| {
                        let order_id = order_id.clone();
                        async move {
                            tracing::info!(target: "examples", "render pdf for {order_id}");
                            Ok(serde_json::json!({ "pdf": format!("{order_id}.pdf") }))
                        }
                    })
                },
                {
                    let order_id = order_id.clone();
                    ctx.step_with(
                        "charge",
                        StepOptions::new().retry(RetryPolicy::new(
                            3,
                            Backoff::Fixed { base_ms: 1 },
                        )),
                        move |_sc: StepCtx| {
                            let order_id = order_id.clone();
                            async move {
                                payment_gateway::charge(&order_id)?;
                                Ok(serde_json::json!({ "charged": true, "orderId": order_id }))
                            }
                        },
                    )
                },
            )?;

            if expedited {
                let order_id = order_id.clone();
                ctx.step("notify", move |_sc: StepCtx| {
                    let order_id = order_id.clone();
                    async move {
                        tracing::info!(target: "examples", "expedited notify for {order_id}");
                        Ok(serde_json::json!({ "notified": true }))
                    }
                })
                .await?;
            }

            Ok(serde_json::json!({
                "orderId": order_id,
                "pdf": pdf,
                "charge": charged,
            }))
        })
}

/// 邮件 digest：`scan-events` → `render` → `send` 三步链。
/// 本身无并行/分支，专门用来演示 resume 与 `continue_from` 的检查点行为。
pub fn email_digest() -> TypedWorkflow<EmailDigestInput, serde_json::Value> {
    Workflow::new("email-digest")
        .with_input::<EmailDigestInput>()
        .handler(move |ctx: TypedCtx<EmailDigestInput>| async move {
            let days = ctx.input().days;

            let scanned = ctx
                .step("scan-events", move |_sc: StepCtx| {
                    async move {
                        tracing::info!(target: "examples", "scan events for {days} days");
                        Ok(serde_json::json!({ "count": days * 3 }))
                    }
                })
                .await?;

            let rendered = ctx
                .step("render", move |_sc: StepCtx| {
                    let scanned = scanned.clone();
                    async move {
                        tracing::info!(target: "examples", "render digest");
                        Ok(serde_json::json!({
                            "body": format!("{} events last {} days", scanned["count"], days),
                        }))
                    }
                })
                .await?;

            let sent = ctx
                .step("send", move |_sc: StepCtx| {
                    let rendered = rendered.clone();
                    async move {
                        tracing::info!(target: "examples", "send digest email");
                        Ok(serde_json::json!({ "to": "ops@example.com", "body": rendered["body"] }))
                    }
                })
                .await?;

            Ok(serde_json::json!({ "days": days, "sent": sent }))
        })
}

/// 人工审批流：`draft` → `review`（`ctx.approve` 挂起，等外部决定）→ `publish`。
/// 审批决定（signal 的 payload）就是 `approve` 的返回值，最后作为 run 输出。
pub fn approval_review() -> Workflow {
    Workflow::new("approval-review").handler(|ctx: WorkflowCtx| {
        async move {
            ctx.step("draft", move |_sc: StepCtx| {
                async move {
                    tracing::info!(target: "examples", "draft the proposal");
                    Ok(serde_json::json!({ "draft": true }))
                }
            })
            .await?;

            let decision = ctx
                .approve("review", "这份提案是否放行发布？")
                .await?;
            let decision_out = decision.clone();

            ctx.step("publish", move |_sc: StepCtx| {
                let decision = decision.clone();
                async move {
                    tracing::info!(target: "examples", "publish with decision");
                    Ok(decision.clone())
                }
            })
            .await?;

            Ok(decision_out)
        }
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// 履约流（对齐官方 `fulfillmentWorkflow` / wf-demo `fulfillment`）：
/// `reserve-inventory` → 若 `readyAt` 在未来先 `sleep` 闸门 → 等
/// `payment-received` 事件 → `ship-order`。
pub fn fulfillment() -> TypedWorkflow<FulfillmentInput, serde_json::Value> {
    Workflow::new("fulfillment")
        .with_input::<FulfillmentInput>()
        .handler(|ctx: TypedCtx<FulfillmentInput>| async move {
            let input = ctx.input();
            let order_id = input.order_id.clone();
            let ready_at = input.ready_at;

            let order_id_reserve = order_id.clone();
            let order_id_ship = order_id.clone();

            let reservation = ctx
                .step("reserve-inventory", move |_sc: StepCtx| {
                    let order_id = order_id_reserve;
                    async move {
                        tracing::info!(target: "examples", "reserve inventory for {order_id}");
                        Ok(serde_json::json!({
                            "reservationId": format!("res_{order_id}"),
                            "reservedAt": now_ms(),
                        }))
                    }
                })
                .await?;

            if ready_at > now_ms() {
                ctx.sleep(
                    "ready",
                    Duration::from_millis((ready_at - now_ms()) as u64),
                )
                .await?;
            }

            let payment = ctx
                .approve("payment", "waiting for payment-received event")
                .await?;

            let shipment = ctx
                .step("ship-order", move |_sc: StepCtx| {
                    let order_id = order_id_ship;
                    async move {
                        tracing::info!(target: "examples", "ship {order_id}");
                        Ok(serde_json::json!({
                            "shipmentId": format!("ship_{order_id}"),
                            "shippedAt": now_ms(),
                        }))
                    }
                })
                .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "reservation": reservation,
                "payment": payment,
                "shipment": shipment,
            }))
        })
}

const APPROVAL_THRESHOLD: i64 = 1_000;

/// 审批流（对齐 wf-demo `approval-order`）：`check-order` → 金额超过阈值才
/// `approve`（拒绝走 `notify-rejected` 分支）→ `charge-payment`。
pub fn approval_order() -> TypedWorkflow<ApprovalOrderInput, serde_json::Value> {
    Workflow::new("approval-order")
        .with_input::<ApprovalOrderInput>()
        .handler(|ctx: TypedCtx<ApprovalOrderInput>| async move {
            let input = ctx.input();
            let order_id = input.order_id.clone();
            let amount = input.amount;
            let currency = input.currency.clone();

            let order_id_check = order_id.clone();
            let currency_check = currency.clone();

            ctx.step("check-order", move |_sc: StepCtx| {
                let order_id = order_id_check;
                let currency = currency_check;
                async move {
                    Ok(serde_json::json!({ "orderId": order_id, "currency": currency }))
                }
            })
            .await?;

            if amount > APPROVAL_THRESHOLD {
                let decision = ctx
                    .approve("review", "amount exceeds no-approval threshold")
                    .await?;
                let approved = decision
                    .get("ok")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if !approved {
                    let reason = decision
                        .get("feedback")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                        .unwrap_or_else(|| "declined".to_string());
                    ctx.step("notify-rejected", move |_sc: StepCtx| async move {
                        Ok(serde_json::json!({ "notifiedAt": now_ms() }))
                    })
                    .await?;
                    return Ok(serde_json::json!({
                        "orderId": order_id,
                        "status": "rejected",
                        "amount": amount,
                        "currency": currency,
                        "reason": reason,
                    }));
                }
            }

            let order_id_charge = order_id.clone();
            ctx.step("charge-payment", move |_sc: StepCtx| {
                let order_id = order_id_charge;
                async move {
                    tracing::info!(target: "examples", "charge {order_id}");
                    payment_gateway::charge(&order_id)?;
                    Ok(serde_json::json!({ "chargedAt": now_ms() }))
                }
            })
            .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "status": "approved",
                "amount": amount,
                "currency": currency,
            }))
        })
}

/// 发票（对齐 wf-demo `invoice`）：`prepare-invoice` → 连续两个 `sleep`
/// （双定时器等待，引擎内自动唤醒）→ `settle-invoice`。
pub fn invoice() -> TypedWorkflow<InvoiceInput, serde_json::Value> {
    Workflow::new("invoice")
        .with_input::<InvoiceInput>()
        .handler(|ctx: TypedCtx<InvoiceInput>| async move {
            let input = ctx.input();
            let order_id = input.order_id.clone();
            let t1 = input.t1;
            let t2 = input.t2;

            let order_id_prepare = order_id.clone();
            let order_id_settle = order_id.clone();

            ctx.step("prepare-invoice", move |_sc: StepCtx| {
                let order_id = order_id_prepare;
                async move {
                    Ok(serde_json::json!({ "orderId": order_id, "preparedAt": now_ms() }))
                }
            })
            .await?;

            ctx.sleep("settle-due-1", Duration::from_millis(t1)).await?;
            ctx.sleep("settle-due-2", Duration::from_millis(t2)).await?;

            ctx.step("settle-invoice", move |_sc: StepCtx| {
                let order_id = order_id_settle;
                async move {
                    Ok(serde_json::json!({ "orderId": order_id, "settledAt": now_ms() }))
                }
            })
            .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "settled": true,
                "settledAt": now_ms(),
            }))
        })
}

/// 合规归档（对齐 wf-demo `compliance`）：连续两次事件等待
/// （`legal-review-done` → `compliance-signed`）→ `archive-record`。
pub fn compliance() -> TypedWorkflow<ComplianceInput, serde_json::Value> {
    Workflow::new("compliance")
        .with_input::<ComplianceInput>()
        .handler(|ctx: TypedCtx<ComplianceInput>| async move {
            let subject_id = ctx.input().subject_id.clone();

            ctx.approve("legal-review", "waiting for legal-review-done")
                .await?;
            ctx.approve("compliance-sign", "waiting for compliance-signed")
                .await?;

            let subject_id_archive = subject_id.clone();
            let record = ctx
                .step("archive-record", move |_sc: StepCtx| {
                    let subject_id = subject_id_archive;
                    async move {
                        Ok(serde_json::json!({
                            "subjectId": subject_id,
                            "archivedAt": now_ms(),
                        }))
                    }
                })
                .await?;

            Ok(serde_json::json!({
                "subjectId": subject_id,
                "archived": true,
                "record": record,
            }))
        })
}

/// 退款（对齐 wf-demo `refund`）混合链：`chargeback-filed` 事件 → 定时闸门 → 审批
/// （拒绝走 `declined`，无 disburse step）→ `disburse-refund`。
pub fn refund() -> TypedWorkflow<RefundInput, serde_json::Value> {
    Workflow::new("refund")
        .with_input::<RefundInput>()
        .handler(|ctx: TypedCtx<RefundInput>| async move {
            let input = ctx.input();
            let order_id = input.order_id.clone();
            let amount = input.amount;
            let currency = input.currency.clone();
            let refund_at = input.refund_at;

            ctx.approve("chargeback", "waiting for chargeback-filed")
                .await?;

            if let Some(ts) = refund_at
                && ts > now_ms()
            {
                ctx.sleep("refund-wait", Duration::from_millis((ts - now_ms()) as u64))
                    .await?;
            }

            let decision = ctx
                .approve("refund-policy", "refund needs manager approval")
                .await?;
            let approved = decision
                .get("ok")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !approved {
                return Ok(serde_json::json!({
                    "orderId": order_id,
                    "status": "declined",
                    "amount": amount,
                    "currency": currency,
                }));
            }

            let order_id_disburse = order_id.clone();
            ctx.step("disburse-refund", move |_sc: StepCtx| {
                let order_id = order_id_disburse;
                async move {
                    tracing::info!(target: "examples", "disburse refund for {order_id}");
                    payment_gateway::charge(&order_id)?;
                    Ok(serde_json::json!({ "disbursedAt": now_ms() }))
                }
            })
            .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "status": "refunded",
                "amount": amount,
                "currency": currency,
            }))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, LazyLock};
    use std::time::Duration;
    use workflow_core::{
        run_workflow, signal_run, InMemoryStore, RunEvent, RunOptions, RunStatus, RunStore,
    };

    /// 序列化共享全局网关状态的测试（tokio 各 test 默认并行跑）。
    static GATEWAY_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    fn finished_count(events: &[RunEvent], step: &str) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, RunEvent::StepFinished { step_id, .. } if step_id == step))
            .count()
    }

    fn sf_ts(events: &[RunEvent], step: &str) -> i64 {
        events
            .iter()
            .find_map(|e| match e {
                RunEvent::StepFinished { step_id, ts, .. } if step_id == step => Some(*ts),
                _ => None,
            })
            .expect("应有 StepFinished")
    }

    /// 轮询直到 run 在 `step_id` 挂起（外部 signal 投递前的等待点）。
    async fn wait_paused<S: RunStore + ?Sized>(
        store: &Arc<S>,
        run_id: &str,
        step_id: &str,
    ) {
        for _ in 0..2000 {
            let evs = store.get_events(run_id).unwrap();
            if evs.iter().any(|e| {
                matches!(e, RunEvent::StepPaused { step_id: id, .. } if id == step_id)
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("run {run_id} never paused at {step_id}");
    }

    #[tokio::test]
    async fn fulfillment_parallel_retry_then_success() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        payment_gateway::set_fail_first(2);

        let store = Arc::new(InMemoryStore::new());
        let wf = fulfillment_saga();

        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({ "orderId": "o-1", "expedited": true })),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);

        // charge 重试 3 次成功（前 2 次失败）
        assert_eq!(payment_gateway::attempts(), 3);
        let events = store.get_events(&out.run_id).unwrap();
        let charge_fin = events
            .iter()
            .find_map(|e| match e {
                RunEvent::StepFinished { step_id, attempts, .. } if step_id == "charge" => {
                    Some(attempts)
                }
                _ => None,
            })
            .expect("charge 应 StepFinished");
        assert_eq!(charge_fin.len(), 3, "切开看的 attempts 应为 3 次");
        // try_join! 并行：gen-pdf 与 charge 都只跑一次；分支 notify 也执行
        assert_eq!(finished_count(&events, "gen-pdf"), 1);
        assert_eq!(finished_count(&events, "charge"), 1);
        assert_eq!(finished_count(&events, "notify"), 1);
    }

    #[tokio::test]
    async fn fulfillment_failure_terminal_until_continue_from() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        payment_gateway::set_fail_always(true);

        let store = Arc::new(InMemoryStore::new());
        let wf = fulfillment_saga();

        // run 1: charge 在 3 次重试后仍失败 → run Errored
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({ "orderId": "o-1" })),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.as_deref().unwrap().contains("payment gateway down"));
        assert_eq!(payment_gateway::attempts(), 3);
        let run_id = out.run_id.clone();
        let events_1 = store.get_events(&run_id).unwrap();
        let pdf_ts_1 = sf_ts(&events_1, "gen-pdf");

        // 普通 resume: 失败 checkpoint rethrow, 不再执行 charge
        let again = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({ "orderId": "o-1" })).run_id(run_id.clone()),
            None,
        )
        .await
        .unwrap();
        assert_eq!(again.status, RunStatus::Errored);
        assert_eq!(payment_gateway::attempts(), 3, "plain resume 不重试已失败 step");

        // continue_from charge: 截断 charge 的 StepFailed + 后缀, 前缀短路、后缀重跑
        payment_gateway::set_fail_always(false);
        let resumed = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({ "orderId": "o-1" }))
                .run_id(run_id)
                .continue_from("charge"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(resumed.status, RunStatus::Finished);
        assert_eq!(payment_gateway::attempts(), 4, "续跑只再跑一次 charge");

        let events_2 = store.get_events(&resumed.run_id).unwrap();
        assert_eq!(finished_count(&events_2, "gen-pdf"), 1, "前缀 gen-pdf 不重跑");
        assert_eq!(finished_count(&events_2, "charge"), 1, "truncate 后 charge 重记一条");
        assert_eq!(
            sf_ts(&events_2, "gen-pdf"),
            pdf_ts_1,
            "gen-pdf 的 checkpoint 未被触碰"
        );
    }

    #[tokio::test]
    async fn email_digest_resume_and_continue_from() {
        let store = Arc::new(InMemoryStore::new());
        let wf = email_digest();

        let input = serde_json::json!({ "days": 7 });
        let out = run_workflow(&wf, store.clone(), &RunOptions::new(input.clone()), None)
            .await
            .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let run_id = out.run_id.clone();
        let events_1 = store.get_events(&run_id).unwrap();
        let scan_ts_1 = sf_ts(&events_1, "scan-events");
        assert_eq!(finished_count(&events_1, "scan-events"), 1);

        // 同 run_id resume：全部短路，不产生新 StepFinished
        let again = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(input.clone()).run_id(run_id.clone()),
            None,
        )
        .await
        .unwrap();
        assert_eq!(again.status, RunStatus::Finished);
        let events_2 = store.get_events(&run_id).unwrap();
        assert_eq!(sf_ts(&events_2, "scan-events"), scan_ts_1);
        assert_eq!(finished_count(&events_2, "scan-events"), 1);
        assert_eq!(finished_count(&events_2, "render"), 1);
        assert_eq!(finished_count(&events_2, "send"), 1);

        // continue_from render: render + send 重跑, scan-events 不重跑
        let cont = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(input).run_id(run_id).continue_from("render"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(cont.status, RunStatus::Finished);
        let events_3 = store.get_events(&cont.run_id).unwrap();
        assert_eq!(sf_ts(&events_3, "scan-events"), scan_ts_1, "scan-events 不重跑");
        assert_eq!(finished_count(&events_3, "render"), 1, "render 重记一条新终态");
        assert_eq!(finished_count(&events_3, "send"), 1);
    }

    #[tokio::test]
    async fn approval_pauses_until_signal_then_publishes() {
        let store = Arc::new(InMemoryStore::new());
        let wf = approval_review();

        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({})).run_id("approve:r"),
                None,
            )
            .await
        });

        for _ in 0..2000 {
            let evs = store.get_events("approve:r").unwrap();
            if evs
                .iter()
                .any(|e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "review"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            store.get_run_state("approve:r").unwrap().unwrap().status,
            RunStatus::Paused,
            "挂起时 run 状态应为 Paused"
        );

        signal_run(store.as_ref(), "approve:r", "review", serde_json::json!({ "ok": true })).unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "ok": true })));

        let evs = store.get_events("approve:r").unwrap();
        assert_eq!(finished_count(&evs, "draft"), 1);
        assert_eq!(finished_count(&evs, "publish"), 1);
        assert_eq!(
            evs.iter()
                .filter(|e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "review"))
                .count(),
            1
        );
        assert_eq!(
            evs.iter()
                .filter(|e| matches!(e, RunEvent::StepResume { step_id, .. } if step_id == "review"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn fulfillment_sleep_gate_then_wait_event() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store = Arc::new(InMemoryStore::new());
        let wf = fulfillment();
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({
                    "orderId": "o-1",
                    "readyAt": now_ms() + 10,
                }))
                .run_id("fulfill:p"),
                None,
            )
            .await
        });

        wait_paused(&store, "fulfill:p", "payment").await;
        signal_run(store.as_ref(), "fulfill:p", "payment", serde_json::json!({ "ok": true }))
            .unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);

        let events = store.get_events("fulfill:p").unwrap();
        assert_eq!(finished_count(&events, "reserve-inventory"), 1);
        assert_eq!(finished_count(&events, "ship-order"), 1);
        let ready_sleep = events
            .iter()
            .filter(|e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "ready"))
            .count();
        assert_eq!(ready_sleep, 1, "readyAt 定时闸门睡过一次");
    }

    #[tokio::test]
    async fn approval_order_below_threshold_completes_immediately() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store = Arc::new(InMemoryStore::new());
        let out = run_workflow(
            &approval_order(),
            store.clone(),
            &RunOptions::new(serde_json::json!({ "orderId": "o-1", "amount": 500 })),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(
            out.output,
            Some(serde_json::json!({
                "orderId": "o-1", "status": "approved", "amount": 500, "currency": "USD"
            }))
        );
        assert_eq!(
            finished_count(&store.get_events(&out.run_id).unwrap(), "charge-payment"),
            1
        );
        let events = store.get_events(&out.run_id).unwrap();
        assert!(
            !events.iter().any(|e| matches!(e, RunEvent::StepPaused { .. })),
            "未超阈值不应有暂停"
        );
    }

    #[tokio::test]
    async fn approval_order_above_threshold_await_decision() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store = Arc::new(InMemoryStore::new());
        let store2 = store.clone();
        let wf = approval_order();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({ "orderId": "o-2", "amount": 5000 }))
                    .run_id("approve:o"),
                None,
            )
            .await
        });

        wait_paused(&store, "approve:o", "review").await;
        signal_run(
            store.as_ref(),
            "approve:o",
            "review",
            serde_json::json!({ "ok": false, "feedback": "budget overrun" }),
        )
        .unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output.clone().unwrap()["status"], "rejected");
        assert_eq!(
            out.output.clone().unwrap()["reason"],
            "budget overrun"
        );
        let events = store.get_events("approve:o").unwrap();
        assert_eq!(finished_count(&events, "notify-rejected"), 1);
        assert_eq!(finished_count(&events, "charge-payment"), 0);
    }

    #[tokio::test]
    async fn invoice_double_sleep_auto_resumes() {
        let store = Arc::new(InMemoryStore::new());
        let out = run_workflow(
            &invoice(),
            store.clone(),
            &RunOptions::new(serde_json::json!({ "orderId": "i-1", "t1": 10, "t2": 25 })),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output.unwrap()["settled"], true);

        let events = store.get_events(&out.run_id).unwrap();
        assert_eq!(finished_count(&events, "settle-invoice"), 1);
        for step in ["settle-due-1", "settle-due-2"] {
            let paused = events
                .iter()
                .filter(|e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == step))
                .count();
            let resumed = events
                .iter()
                .filter(|e| matches!(e, RunEvent::StepResume { step_id, .. } if step_id == step))
                .count();
            assert_eq!(paused, 1, "{step} 暂停一次");
            assert_eq!(resumed, 1, "{step} 自动恢复一次");
        }
    }

    #[tokio::test]
    async fn compliance_two_sequential_events_then_archive() {
        let store = Arc::new(InMemoryStore::new());
        let store2 = store.clone();
        let wf = compliance();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({ "subjectId": "s-1" })).run_id("comp:r"),
                None,
            )
            .await
        });

        wait_paused(&store, "comp:r", "legal-review").await;
        signal_run(store.as_ref(), "comp:r", "legal-review", serde_json::json!({ "ok": true }))
            .unwrap();
        wait_paused(&store, "comp:r", "compliance-sign").await;
        signal_run(
            store.as_ref(),
            "comp:r",
            "compliance-sign",
            serde_json::json!({ "ok": true }),
        )
        .unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output.unwrap()["archived"], true);

        let events = store.get_events("comp:r").unwrap();
        assert_eq!(finished_count(&events, "archive-record"), 1);
    }

    #[tokio::test]
    async fn refund_declined_on_approval() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store = Arc::new(InMemoryStore::new());
        let store2 = store.clone();
        let wf = refund();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({ "orderId": "r-1", "amount": 200 }))
                    .run_id("refund:d"),
                None,
            )
            .await
        });

        wait_paused(&store, "refund:d", "chargeback").await;
        signal_run(store.as_ref(), "refund:d", "chargeback", serde_json::json!({ "ok": true }))
            .unwrap();
        wait_paused(&store, "refund:d", "refund-policy").await;
        signal_run(
            store.as_ref(),
            "refund:d",
            "refund-policy",
            serde_json::json!({ "ok": false }),
        )
        .unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output.clone().unwrap()["status"], "declined");
        let events = store.get_events("refund:d").unwrap();
        assert_eq!(finished_count(&events, "disburse-refund"), 0);
    }

    #[tokio::test]
    async fn refund_approved_disburses_after_timer() {
        let _guard = GATEWAY_TEST_LOCK.lock().await;
        payment_gateway::reset();
        let store = Arc::new(InMemoryStore::new());
        let store2 = store.clone();
        let wf = refund();
        let wf2 = wf.clone();
        let refund_at = now_ms() + 500;
        let task = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({
                    "orderId": "r-2",
                    "amount": 200,
                    "refundAt": refund_at,
                }))
                .run_id("refund:q"),
                None,
            )
            .await
        });

        wait_paused(&store, "refund:q", "chargeback").await;
        signal_run(store.as_ref(), "refund:q", "chargeback", serde_json::json!({ "ok": true }))
            .unwrap();
        wait_paused(&store, "refund:q", "refund-policy").await;
        signal_run(
            store.as_ref(),
            "refund:q",
            "refund-policy",
            serde_json::json!({ "ok": true }),
        )
        .unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output.clone().unwrap()["status"], "refunded");

        let events = store.get_events("refund:q").unwrap();
        assert_eq!(finished_count(&events, "disburse-refund"), 1);
        let timer_sleep = events
            .iter()
            .filter(|e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "refund-wait"))
            .count();
        assert_eq!(timer_sleep, 1, "定时闸门睡过一次");
    }

    /// `.with_input` 保留双向静态类型：In 显式声明，Out 从 handler 返回类型
    /// 自动推断（对齐 TanStack 的返回值推断，无需 output schema 声明）。
    #[test]
    fn typed_workflow_keeps_static_types() {
        let _saga: TypedWorkflow<FulfillmentSagaInput, serde_json::Value> = fulfillment_saga();
        let _wf: TypedWorkflow<FulfillmentInput, serde_json::Value> = fulfillment();
    }

    /// zod 语义：缺失必填字段 → 首次恢复即 schema 错误，run 直接 Errored。
    #[tokio::test]
    async fn typed_input_rejects_missing_field() {
        let store = Arc::new(InMemoryStore::new());
        let store2 = store.clone();
        let wf = fulfillment().into_workflow();
        let wf2 = wf.clone();
        let out = run_workflow(
            &wf2,
            store2,
            &RunOptions::new(serde_json::json!({ "orderId": "o-1" })).run_id("fulfill:bad"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored, "缺 readyAt 应 schema 报错");
        let msg = out.error.unwrap();
        assert!(
            msg.contains("ready_at") || msg.contains("missing field"),
            "错误应指向字段路径: {msg}"
        );
    }
}