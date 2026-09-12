//! 示例 workflow 定义（handler 形态，代码即 DAG）。
//!
//! 输入用 `create_workflow(CreateWorkflowConfig::new(id).input::<T>())` 声明
//! （Rust 版 zod `inputSchema`：serde Deserialize 即运行时校验，缺字段/错类型
//! 在首次恢复时报 schema 错误）；handler 收到 `BaseCtx<In, St, Ext>`，
//! `ctx.input` 字段直接是 `In`（借用即 `&In`）。
//! 输出类型从 handler 返回值自动推断（对齐 TanStack 的返回值推断，无 output 声明）。
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
//! - [`state_demo`]（id `state-demo`）— per-invocation state（对齐 TanStack
//!   `initialize` + `stateSchema`）：`.state::<T>()` 声明 typed state，
//!   `initialize(|input| ...)` 每次 start/resume 重建 state（**不落盘**），handler
//!   直接读写 `ctx.state` 字段（typed 时为 `T`，untyped 时为 `Value`）。step
//!   闭包**外**的 mutation 随重建重放，闭包**内**的被 replay 短路丢失
//!   （与 TanStack 0.0.4 一致）。

use std::time::Duration;
use workflow_core::{
    Backoff, BaseCtx, CreateWorkflowConfig, RetryPolicy, StepCtx, StepOptions, Workflow,
    WorkflowCtx, WorkflowDefinition, create_workflow,
};

// ========================================================================
// 输入类型 = workflow 的「schema」（Rust 版 zod `inputSchema`）：serde
// Deserialize 即运行时校验，`create_workflow(config.input::<In>())` 后 handler
// 拿到 `BaseCtx<In, ...>`，`ctx.input` 直接是 `In`，没有 `.get(...)` 链。
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
pub fn fulfillment_saga() -> WorkflowDefinition<FulfillmentSagaInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("fulfillment-saga").input::<FulfillmentSagaInput>())
        .handler(|ctx: BaseCtx<FulfillmentSagaInput>| async move {
            let input = &ctx.input;
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
                        StepOptions::new()
                            .retry(RetryPolicy::new(3, Backoff::Fixed { base_ms: 1 })),
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
pub fn email_digest() -> WorkflowDefinition<EmailDigestInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("email-digest").input::<EmailDigestInput>()).handler(
        move |ctx: BaseCtx<EmailDigestInput>| async move {
            let days = ctx.input.days;

            let scanned = ctx
                .step("scan-events", move |_sc: StepCtx| async move {
                    tracing::info!(target: "examples", "scan events for {days} days");
                    Ok(serde_json::json!({ "count": days * 3 }))
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
        },
    )
}

/// 人工审批流：`draft` → `review`（`ctx.approve` 挂起，等外部决定）→ `publish`。
/// 审批决定（signal 的 payload）就是 `approve` 的返回值，最后作为 run 输出。
pub fn approval_review() -> Workflow {
    Workflow::new("approval-review").handler(|ctx: WorkflowCtx| async move {
        ctx.step("draft", move |_sc: StepCtx| async move {
            tracing::info!(target: "examples", "draft the proposal");
            Ok(serde_json::json!({ "draft": true }))
        })
        .await?;

        let decision = ctx.approve("review", "这份提案是否放行发布？").await?;
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
pub fn fulfillment() -> WorkflowDefinition<FulfillmentInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("fulfillment").input::<FulfillmentInput>()).handler(
        |ctx: BaseCtx<FulfillmentInput>| async move {
            let input = &ctx.input;
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
                ctx.sleep("ready", Duration::from_millis((ready_at - now_ms()) as u64))
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
        },
    )
}

const APPROVAL_THRESHOLD: i64 = 1_000;

/// 审批流（对齐 wf-demo `approval-order`）：`check-order` → 金额超过阈值才
/// `approve`（拒绝走 `notify-rejected` 分支）→ `charge-payment`。
pub fn approval_order() -> WorkflowDefinition<ApprovalOrderInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("approval-order").input::<ApprovalOrderInput>())
        .handler(|ctx: BaseCtx<ApprovalOrderInput>| async move {
            let input = &ctx.input;
            let order_id = input.order_id.clone();
            let amount = input.amount;
            let currency = input.currency.clone();

            let order_id_check = order_id.clone();
            let currency_check = currency.clone();

            ctx.step("check-order", move |_sc: StepCtx| {
                let order_id = order_id_check;
                let currency = currency_check;
                async move { Ok(serde_json::json!({ "orderId": order_id, "currency": currency })) }
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
pub fn invoice() -> WorkflowDefinition<InvoiceInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("invoice").input::<InvoiceInput>()).handler(
        |ctx: BaseCtx<InvoiceInput>| async move {
            let input = &ctx.input;
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
                async move { Ok(serde_json::json!({ "orderId": order_id, "settledAt": now_ms() })) }
            })
            .await?;

            Ok(serde_json::json!({
                "orderId": order_id,
                "settled": true,
                "settledAt": now_ms(),
            }))
        },
    )
}

/// 合规归档（对齐 wf-demo `compliance`）：连续两次事件等待
/// （`legal-review-done` → `compliance-signed`）→ `archive-record`。
pub fn compliance() -> WorkflowDefinition<ComplianceInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("compliance").input::<ComplianceInput>()).handler(
        |ctx: BaseCtx<ComplianceInput>| async move {
            let subject_id = ctx.input.subject_id.clone();

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
        },
    )
}

/// 退款（对齐 wf-demo `refund`）混合链：`chargeback-filed` 事件 → 定时闸门 → 审批
/// （拒绝走 `declined`，无 disburse step）→ `disburse-refund`。
pub fn refund() -> WorkflowDefinition<RefundInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("refund").input::<RefundInput>()).handler(
        |ctx: BaseCtx<RefundInput>| async move {
            let input = &ctx.input;
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
        },
    )
}

/// 状态机 demo 输入。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateDemoInput {
    pub order_id: String,
    pub amount: i64,
}

/// typed state 的形状（Rust 版 zod `stateSchema`）：`.state::<T>()`
/// 声明契约，`initialize` 每次调用从输入重建，handler 的 `ctx.state` 就是
/// typed 字段 `CounterState`，直接读写。
#[derive(serde::Deserialize, serde::Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CounterState {
    pub total: i64,
    pub settle_events: Vec<String>,
}

/// per-invocation state（对齐 TanStack `initialize`）：state **不落盘**，每次
/// start/resume 都从 `initialize(input)` 重建，然后 handler 从头重跑。因此
/// step 闭包**之外**的 mutation 天然可重放：gate 之后对 state 的修改在
/// resume 时会再次执行，最终结果与一次跑到底完全一致。
pub fn state_demo() -> WorkflowDefinition<StateDemoInput, serde_json::Value, CounterState> {
    create_workflow(
        CreateWorkflowConfig::new("state-demo")
            .input::<StateDemoInput>()
            .state::<CounterState>()
            .initialize(|input| {
                let amount = input["amount"].as_i64().unwrap_or(0);
                Ok(serde_json::json!({
                    "total": amount,
                    "settleEvents": serde_json::Value::Array(vec![]),
                }))
            }),
    )
    .handler(
        |mut ctx: BaseCtx<StateDemoInput, CounterState>| async move {
            let order_id = ctx.input.order_id.clone();
            let amount = ctx.input.amount;

            // step 闭包之外的 mutation —— resume 时随 initialize 重建+重跑。
            ctx.state.total += 1;

            let order_id_prepare = order_id.clone();
            let prepared = ctx
                .step("prepare", move |_sc: StepCtx| {
                    let order_id = order_id_prepare;
                    async move {
                        Ok(serde_json::json!({ "orderId": order_id, "preparedAt": now_ms() }))
                    }
                })
                .await?;

            // 人工放行 gate：跨调用边界，验证 resume 后 state 重建+重放。
            ctx.approve("manual-release", "gate pending human release")
                .await?;

            ctx.state.settle_events.push("released".into());
            ctx.state.total += amount;

            Ok(serde_json::json!({
                "orderId": order_id,
                "total": ctx.state.total,
                "settleEvents": ctx.state.settle_events,
                "preparedAt": prepared["preparedAt"],
                "settled": true,
            }))
        },
    )
}

/// 事件闸门 demo（对齐 TanStack `waitForEvent` / `sleepUntil` / `emit`）：
/// 1. `ctx.emit` 广播可观测事件（不进日志，publisher 可见）；
/// 2. `ctx.wait_for_event` 按事件名挂起，外部 `signal_event` 按名投递；
/// 3. `ctx.sleep_until` 等绝对时间点（predictedAt 靠后成立，否则立即放行）。
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventGateInput {
    pub market: String,
    pub predicted_at: i64,
}

pub fn event_gate() -> WorkflowDefinition<EventGateInput, serde_json::Value> {
    create_workflow(CreateWorkflowConfig::new("event-gate").input::<EventGateInput>()).handler(
        |ctx: BaseCtx<EventGateInput>| async move {
            let market = ctx.input.market.clone();
            let predicted_at = ctx.input.predicted_at;

            ctx.emit(
                "gate-opened",
                serde_json::json!({ "market": market, "predictedAt": predicted_at }),
            );

            // 按名挂起：host 只认识 "price-settled" 事件，不需要知道 pause key。
            let settlement = ctx.wait_for_event("price-wait", "price-settled").await?;

            // 绝对时间闸门：预测时间未到则睡到那一刻。
            ctx.sleep_until("settle-wait", predicted_at).await?;

            let market_finalize = market.clone();
            ctx.step("finalize", move |_sc: StepCtx| {
                let market = market_finalize;
                async move { Ok(serde_json::json!({ "market": market })) }
            })
            .await?;

            Ok(serde_json::json!({
                "market": market,
                "settlement": settlement,
                "finalized": true,
            }))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, LazyLock};
    use std::time::Duration;
    use workflow_core::{
        InMemoryStore, RunEvent, RunOptions, RunStatus, RunStore, run_workflow, signal_event,
        signal_run,
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
    async fn wait_paused<S: RunStore + ?Sized>(store: &Arc<S>, run_id: &str, step_id: &str) {
        for _ in 0..2000 {
            let evs = store.get_events(run_id).unwrap();
            if evs
                .iter()
                .any(|e| matches!(e, RunEvent::StepPaused { step_id: id, .. } if id == step_id))
            {
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
                RunEvent::StepFinished {
                    step_id, attempts, ..
                } if step_id == "charge" => Some(attempts),
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
        assert!(
            out.error
                .as_deref()
                .unwrap()
                .contains("payment gateway down")
        );
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
        assert_eq!(
            payment_gateway::attempts(),
            3,
            "plain resume 不重试已失败 step"
        );

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
        assert_eq!(
            finished_count(&events_2, "gen-pdf"),
            1,
            "前缀 gen-pdf 不重跑"
        );
        assert_eq!(
            finished_count(&events_2, "charge"),
            1,
            "truncate 后 charge 重记一条"
        );
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
            &RunOptions::new(input)
                .run_id(run_id)
                .continue_from("render"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(cont.status, RunStatus::Finished);
        let events_3 = store.get_events(&cont.run_id).unwrap();
        assert_eq!(
            sf_ts(&events_3, "scan-events"),
            scan_ts_1,
            "scan-events 不重跑"
        );
        assert_eq!(
            finished_count(&events_3, "render"),
            1,
            "render 重记一条新终态"
        );
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

        signal_run(
            store.as_ref(),
            "approve:r",
            "review",
            serde_json::json!({ "ok": true }),
        )
        .unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "ok": true })));

        let evs = store.get_events("approve:r").unwrap();
        assert_eq!(finished_count(&evs, "draft"), 1);
        assert_eq!(finished_count(&evs, "publish"), 1);
        assert_eq!(
            evs.iter()
                .filter(
                    |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "review")
                )
                .count(),
            1
        );
        assert_eq!(
            evs.iter()
                .filter(
                    |e| matches!(e, RunEvent::StepResume { step_id, .. } if step_id == "review")
                )
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
        signal_run(
            store.as_ref(),
            "fulfill:p",
            "payment",
            serde_json::json!({ "ok": true }),
        )
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
            !events
                .iter()
                .any(|e| matches!(e, RunEvent::StepPaused { .. })),
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
        assert_eq!(out.output.clone().unwrap()["reason"], "budget overrun");
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
        signal_run(
            store.as_ref(),
            "comp:r",
            "legal-review",
            serde_json::json!({ "ok": true }),
        )
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
        signal_run(
            store.as_ref(),
            "refund:d",
            "chargeback",
            serde_json::json!({ "ok": true }),
        )
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
        signal_run(
            store.as_ref(),
            "refund:q",
            "chargeback",
            serde_json::json!({ "ok": true }),
        )
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
            .filter(
                |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "refund-wait"),
            )
            .count();
        assert_eq!(timer_sleep, 1, "定时闸门睡过一次");
    }

    /// 静态类型保留：`create_workflow(config.input::<In>())` 的 In 显式声明，
    /// Out 从 handler 返回类型自动推断（对齐 TanStack 的返回值推断，无需
    /// output schema 声明）。
    #[test]
    fn typed_workflow_keeps_static_types() {
        let _saga: WorkflowDefinition<FulfillmentSagaInput, serde_json::Value> = fulfillment_saga();
        let _wf: WorkflowDefinition<FulfillmentInput, serde_json::Value> = fulfillment();
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

    /// zod `stateSchema` 的 Rust 版：initialize 产物形状不符 `state_schema::<T>()`
    /// → 每次调用（start/resume）都把 run 标 Errored（与 inputSchema 语义一致）。
    #[tokio::test]
    async fn state_validation_rejects_init_shape() {
        let store = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("state-bad-shape")
            .state_schema::<CounterState>()
            .initialize(|_input| Ok(serde_json::json!({ "bogus": 1 })))
            .handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::json!({})) });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            out.status,
            RunStatus::Errored,
            "initialize 产物不符 state_schema 应报错"
        );
        let err = out.error.unwrap();
        assert!(
            err.contains("missing field") || err.contains("total") || err.contains("settle_events"),
            "错误应指向 state 形状: {err}"
        );
        let st = store.get_run_state(&out.run_id).unwrap().unwrap();
        assert_eq!(
            st.status,
            RunStatus::Errored,
            "run state 也应持久化为 Errored"
        );
    }

    /// `state::<St>()` 让 builder 重命名泛型：handler 拿到
    /// `BaseCtx<In, St>`，`ctx.state` 是 typed 字段直接读写；durable 边界
    /// （`step`）把工作副本 flush 回引擎镜像。
    #[tokio::test]
    async fn state_typed_field_roundtrip() {
        let store = Arc::new(InMemoryStore::new());
        let wf = create_workflow(
            CreateWorkflowConfig::new("state-roundtrip")
                .input::<serde_json::Value>()
                .state::<CounterState>()
                .initialize(|_input| {
                    Ok(serde_json::json!({ "total": 7, "settleEvents": ["seed"] }))
                }),
        )
        .handler(
            |mut ctx: BaseCtx<serde_json::Value, CounterState>| async move {
                assert_eq!(
                    (ctx.state.total, ctx.state.settle_events.len()),
                    (7, 1),
                    "initialize 已注入"
                );
                ctx.state.total = 99;
                ctx.state.settle_events.push("mutated".into());
                // durable 边界：flush 执行（镜像写回，per-invocation 语义）。
                ctx.step("flush-check", |_sc: StepCtx| async move {
                    Ok(serde_json::json!({ "flushed": true }))
                })
                .await?;
                Ok(serde_json::json!(ctx.state.total))
            },
        );
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!(99)));
    }

    /// state 每次调用（start/resume）都从 initialize(input) 重建、step 闭包之外的
    /// mutation 重放——所以崩溃重启（abort → 重跑）后的输出与一次跑完完全一致。
    /// total = amount(initialize) + 1(前置) + amount(gate 后) = 2*amount + 1。
    #[tokio::test]
    async fn state_rebuilt_each_invocation() {
        let inp = || serde_json::json!({ "orderId": "s-1", "amount": 10 });

        // run A：单任务内 pause + resume（gate 由外部信号放行），不重建 state。
        let signal_gate = |store: &Arc<InMemoryStore>, run_id: &str| {
            let store = store.clone();
            let run_id = run_id.to_string();
            async move {
                wait_paused(&store, &run_id, "manual-release").await;
                signal_run(
                    store.as_ref(),
                    &run_id,
                    "manual-release",
                    serde_json::json!({ "ok": true }),
                )
                .unwrap();
            }
        };
        let store_a = Arc::new(InMemoryStore::new());
        let store_a2 = store_a.clone();
        let s1 = signal_gate(&store_a, "state:burst");
        let burst = tokio::spawn(async move {
            run_workflow(
                &state_demo().into_workflow(),
                store_a2,
                &RunOptions::new(inp()).run_id("state:burst"),
                None,
            )
            .await
        });
        s1.await;
        let out_burst = burst.await.unwrap().unwrap();
        assert_eq!(out_burst.status, RunStatus::Finished);

        // run B：先挂起在 gate 后 abort（模拟崩溃，state 不落盘），signal 后重跑
        // 同 run_id → 引擎从头重建 state + 重放 handler，最终输出应与 A 一致。
        let store_b = Arc::new(InMemoryStore::new());
        let store_b2 = store_b.clone();
        let wf = state_demo().into_workflow();
        let wf2 = wf.clone();
        let task_b = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store_b2,
                &RunOptions::new(inp()).run_id("state:gated"),
                None,
            )
            .await
        });
        wait_paused(&store_b, "state:gated", "manual-release").await;
        let paused_b = store_b.get_run_state("state:gated").unwrap().unwrap();
        assert_eq!(paused_b.status, RunStatus::Paused, "gate 应把 run 挂起");
        task_b.abort();
        let _ = task_b.await;
        signal_run(
            store_b.as_ref(),
            "state:gated",
            "manual-release",
            serde_json::json!({ "ok": true }),
        )
        .unwrap();
        let out_b = run_workflow(
            &wf,
            store_b.clone(),
            &RunOptions::new(inp()).run_id("state:gated"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out_b.status, RunStatus::Finished);

        // 跳过 preparedAt（每次取 now_ms）比较其余全部字段。
        let project = |o: &serde_json::Value| {
            serde_json::json!({
                "orderId": o["orderId"],
                "total": o["total"],
                "settleEvents": o["settleEvents"],
                "settled": o["settled"],
            })
        };
        let expected = serde_json::json!({
            "orderId": "s-1",
            "total": 21,
            "settleEvents": ["released"],
            "settled": true,
        });
        assert_eq!(
            project(&out_burst.output.unwrap()),
            expected,
            "一次跑完的输出"
        );
        assert_eq!(
            project(&out_b.output.unwrap()),
            expected,
            "崩溃重启后输出应一致（state 重建+重放）"
        );
    }

    /// 并行 step（`tokio::try_join!`）各自持 `ctx.clone()` 时的 state 快照：
    /// 互不共享、不持锁跨 `.await`（与 TanStack 的 in-place 共享对象不同——
    /// 克隆即分裂，见 PARITY.md）。driver 位点的 mutation 在 durable 边界
    /// `approve` 时 flush 回引擎镜像。
    #[tokio::test]
    async fn state_parallel_steps_snapshot_then_driver_flush() {
        let store = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("state-concurrency")
            .state_schema::<CounterState>()
            .initialize(|_| Ok(serde_json::json!({ "total": 0, "settleEvents": [] })))
            .handler(|mut ctx: WorkflowCtx| async move {
                // clone 发生在 step 创建时 = state 快照；闭包内看到的都与 driver
                // 无关，3 个并发等待互不锁（state 是字段，不经过 RwLock）。
                let qa = ctx.clone();
                let fa = ctx.step("a", move |_sc: StepCtx| {
                    let qa = qa;
                    async move { Ok(qa.state["total"].clone()) }
                });
                let qb = ctx.clone();
                let fb = ctx.step("b", move |_sc: StepCtx| {
                    let qb = qb;
                    async move { Ok(qb.state["total"].clone()) }
                });
                let qc = ctx.clone();
                let fc = ctx.step("c", move |_sc: StepCtx| {
                    let qc = qc;
                    async move { Ok(qc.state["total"].clone()) }
                });
                let (ra, rb, rc): (serde_json::Value, serde_json::Value, serde_json::Value) =
                    tokio::try_join!(fa, fb, fc)?;
                assert_eq!(
                    (ra, rb, rc),
                    (
                        serde_json::json!(0),
                        serde_json::json!(0),
                        serde_json::json!(0)
                    ),
                    "快照分裂：各看 clone 时 total=0"
                );

                // driver 位点 mutation → durable 边界 flush 回引擎镜像。
                ctx.state["total"] = serde_json::json!(1);
                ctx.approve("flush-gate", "driver mutation flushed here")
                    .await?;
                Ok(serde_json::json!({ "total": ctx.state["total"] }))
            });
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({})).run_id("state:flush"),
                None,
            )
            .await
        });
        wait_paused(&store, "state:flush", "flush-gate").await;
        signal_run(
            store.as_ref(),
            "state:flush",
            "flush-gate",
            serde_json::json!({ "ok": true }),
        )
        .unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "total": 1 })));
    }

    /// TanStack 0.0.4 同款 sharp edge：重启重放（crash → re-run handler）时，
    /// 已被 checkpoint 记录的 step 闭包**不会**再次调用，因此闭包内对 state 的
    /// 修改丢失——只有 step 闭包**之外**的 mutation 才是可重放的。
    #[tokio::test]
    async fn state_step_closure_mutation_lost_on_resume() {
        let store = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("state-sharp-edge")
            .state_schema::<CounterState>()
            .initialize(|input| {
                Ok(serde_json::json!({
                    "total": input["amount"].as_i64().unwrap_or(0),
                    "settleEvents": [],
                }))
            })
            .handler(|mut ctx: WorkflowCtx| async move {
                // mutation 在 step 闭包内：重放时整个闭包被跳过 → 修改丢失；
                // 且闭包持 clone 快照，首个 drive 内也不写 driver 的字段。
                let inner = ctx.clone();
                ctx.step("bump-inside", move |_sc: StepCtx| {
                    let mut ctx = inner;
                    async move {
                        let mut st: CounterState = serde_json::from_value(ctx.state.clone())?;
                        st.total += 1;
                        ctx.state = serde_json::to_value(st)?;
                        let bumped = ctx.state["total"].clone();
                        Ok(serde_json::json!({ "bumped": true, "total": bumped }))
                    }
                })
                .await?;

                ctx.approve("gate", "sharp edge gate").await?;

                // step 闭包之外的重放 mutation：重跑后仍会执行。
                let mut st: CounterState = serde_json::from_value(ctx.state.clone())?;
                st.settle_events.push("outside".into());
                ctx.state = serde_json::to_value(st)?;

                let final_state: CounterState = serde_json::from_value(ctx.state.clone())?;
                Ok(serde_json::json!({
                    "total": final_state.total,
                    "settleEvents": final_state.settle_events,
                }))
            });

        // 进程 A：跑到 gate 挂起后"崩溃"（abort，不写终态）。
        let store2 = Arc::clone(&store);
        let wf2 = wf.clone();
        let task_a = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({ "amount": 5 })).run_id("state:edge"),
                None,
            )
            .await
        });
        wait_paused(&store, "state:edge", "gate").await;
        task_a.abort();
        let _ = task_a.await;

        // 进程 B：同 run_id 重跑 → state 从 initialize 重建为 5，`bump-inside`
        // 已被 checkpoint 记录 → 闭包跳过（+1 丢失），只剩闭包之外的 mutation。
        signal_run(
            store.as_ref(),
            "state:edge",
            "gate",
            serde_json::json!({ "ok": true }),
        )
        .unwrap();
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({ "amount": 5 })).run_id("state:edge"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let o = out.output.unwrap();
        assert_eq!(
            o["total"].as_i64().unwrap(),
            5,
            "bump-inside 的 +1 在重启重放时丢失（闭包被短路），只保留 initialize 的 base"
        );
        assert_eq!(
            o["settleEvents"],
            serde_json::json!(["outside"]),
            "闭包之外的 mutation 重放"
        );
    }

    /// Typed ctx 的完整基元组合：emit（publisher 可见、日志无）→ wait_for_event
    /// （按名挂起 + signal_event 按名投递）→ sleep_until（绝对时间点）。
    #[tokio::test]
    async fn event_gate_emit_wait_then_sleep_until() {
        let store = Arc::new(InMemoryStore::new());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        let wf = event_gate().into_workflow();
        let wf2 = wf.clone();
        let store2 = store.clone();
        let task = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({
                    "market": "btc",
                    "predictedAt": 0, // 过去 → sleep_until 立即放行
                }))
                .run_id("event:gate"),
                Some(Arc::new(move |e: &RunEvent| {
                    sink.lock().unwrap().push(e.clone())
                })),
            )
            .await
        });
        wait_paused(&store, "event:gate", "price-wait").await;

        // 挂起投影：waiting_for.signal_name = 事件通道（≠ pause key）。
        let st = store.get_run_state("event:gate").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        let w = st.waiting_for.as_ref().unwrap();
        assert_eq!(w.step_id, "price-wait");
        assert_eq!(w.signal_name, "price-settled");
        assert!(st.pending_approval.is_none());

        signal_event(
            store.as_ref(),
            "event:gate",
            "price-settled",
            serde_json::json!({ "px": 62000 }),
        )
        .unwrap();
        let out = task.await.unwrap().unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(
            out.output.clone().unwrap()["settlement"],
            serde_json::json!({ "px": 62000 })
        );
        assert_eq!(
            out.output.clone().unwrap()["finalized"],
            serde_json::json!(true)
        );

        // emit 进 publisher 但不进日志。
        assert!(seen.lock().unwrap().iter().any(|e| matches!(
            e,
            RunEvent::Custom { name, .. } if name == "gate-opened"
        )));
        assert!(
            store
                .get_events("event:gate")
                .unwrap()
                .iter()
                .all(|e| !matches!(e, RunEvent::Custom { .. }))
        );
    }

    /// wait_for_event 后崩溃 → 同 run_id 重跑：StepResume 已在日志，按名信号
    /// 重放即可完成，不重新挂起。
    #[tokio::test]
    async fn event_gate_named_signal_replays_after_crash() {
        let store = Arc::new(InMemoryStore::new());
        let wf = event_gate().into_workflow();
        let store2 = store.clone();
        let wf2 = wf.clone();
        let task_a = tokio::spawn(async move {
            run_workflow(
                &wf2,
                store2,
                &RunOptions::new(serde_json::json!({
                    "market": "eth",
                    "predictedAt": 0, // 过去 → sleep_until 重放时立即放行
                }))
                .run_id("event:crash"),
                None,
            )
            .await
        });
        wait_paused(&store, "event:crash", "price-wait").await;
        task_a.abort();
        let _ = task_a.await;

        signal_event(
            store.as_ref(),
            "event:crash",
            "price-settled",
            serde_json::json!({ "px": 3000 }),
        )
        .unwrap();
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({
                "market": "eth",
                "predictedAt": 0,
            }))
            .run_id("event:crash"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(
            out.output.clone().unwrap()["settlement"],
            serde_json::json!({ "px": 3000 })
        );
        assert_eq!(
            store
                .get_events("event:crash")
                .unwrap()
                .iter()
                .filter(
                    |e| matches!(e, RunEvent::StepPaused { step_id, .. } if step_id == "price-wait")
                )
                .count(),
            1,
            "重跑不重 append pause checkpoint"
        );
    }
}
