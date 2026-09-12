//! 示例 workflow 定义（handler 形态，代码即 DAG）。
//!
//! - [`fulfillment_saga`] — 并行（`tokio::try_join!`）+ retry（脆弱外部支付）+ 分支：
//!   演示 authoring 面；扣款等副作用**不注入**，走模块级 [`payment_gateway`] 服务
//!   （对齐官方 pocs：workflow 无依赖参数，effect 写死在 handler 内）。
//! - [`email_digest`] — 简单三步链：resume / `continue_from` 的行为载体。
//! - [`approval_review`] — 人工审批：`ctx.approve` 持久化一个等待点，
//!   外部用 [`workflow_core::signal_run`] 交付决定后继续（signals 语义载体）。

use workflow_core::{
    Backoff, RetryPolicy, StepCtx, StepOptions, Workflow, WorkflowCtx,
};

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
/// 追加 `notify`（分支 = 普通 `if`）。
pub fn fulfillment_saga() -> Workflow {
    Workflow::new("fulfillment").handler(|ctx: WorkflowCtx| {
        async move {
            let order_id = ctx
                .input()
                .get("orderId")
                .and_then(|v| v.as_str())
                .map(String::from)
                .unwrap_or_else(|| "order-1".to_string());
            let expedited = ctx
                .input()
                .get("expedited")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

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
        }
    })
}

/// 邮件 digest：`scan-events` → `render` → `send` 三步链。
/// 本身无并行/分支，专门用来演示 resume 与 `continue_from` 的检查点行为。
pub fn email_digest() -> Workflow {
    Workflow::new("email-digest").handler(move |ctx: WorkflowCtx| {
        async move {
            let days = ctx
                .input()
                .get("days")
                .and_then(|v| v.as_u64())
                .unwrap_or(7) as i64;

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
        }
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
}