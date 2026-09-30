//! `docs/quick-start.md` 首个 recipe（「跑一个 workflow」）在**核心仓库**的回归。
//!
//! 之前这块只由 examples 承担（untyped 的 happy path 在 `run_workflow.rs` 单测里，
//! typed input 的拒绝分支在 `examples/shared`），核心仓库没有目录级集成测试。
//! 本文件补上：typed input 的成功路径、缺字段拒绝、以及 `StepCtx::id` 的稳定性
//! （recipe 里说「拿 sc.id 当外部系统幂等键」——这个承诺得有测试钉住）。

use std::sync::Arc;

use aa_workflow_core::{CreateWorkflowConfig, InMemoryStore, RunStatus, RunStore, RunWorkflowOptions, StepCtx, WorkflowDefinition, create_workflow, run_workflow};

#[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ChargeInput {
    amount: i64,
    user_id: String,
}

/// recipe 里的 workflow：`ctx.input` 是强类型字段，step 用 `sc.id` 当幂等键。
fn charge_workflow() -> WorkflowDefinition<ChargeInput, serde_json::Value> {
    create_workflow(
        CreateWorkflowConfig::new("quickstart-charge").input::<ChargeInput>(),
    )
    .handler(|ctx| async move {
        let amount = ctx.input.amount;
        let user_id = ctx.input.user_id.clone();
        ctx.step("stripe-charge", move |sc: StepCtx| async move {
            Ok(serde_json::json!({
                "chargeId": format!("ch_{}", sc.id),
                "amount": amount,
                "userId": user_id,
            }))
        })
        .await
    })
}

#[tokio::test]
async fn typed_input_is_deserialized_and_reaches_the_handler() {
    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
    let outcome = run_workflow(
        RunWorkflowOptions::new(charge_workflow(), store)
            .input(serde_json::json!({ "amount": 4200, "userId": "cus_123" })),
    )
    .outcome().await
    .unwrap();

    assert_eq!(outcome.status, RunStatus::Finished);
    assert_eq!(
        outcome.output,
        Some(serde_json::json!({
            // sc.id = 确定性 step id → 可作幂等键
            "chargeId": "ch_stripe-charge",
            "amount": 4200,
            "userId": "cus_123",
        })),
        "handler 应拿到强类型 input，step 结果即 run output"
    );
}

/// `.input::<T>()` 的**成功路径的对照面**：缺字段 → run Errored，错误指向字段路径。
///
/// 机制在 `define/define_workflow.rs`（每次 drive 都 `from_value::<TInput>`）。
#[tokio::test]
async fn typed_input_rejects_missing_field() {
    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
    let outcome = run_workflow(
        RunWorkflowOptions::new(charge_workflow(), store.clone())
            .input(serde_json::json!({ "amount": 4200 })), // 缺 userId
    )
    .outcome().await
    .unwrap();

    assert_eq!(outcome.status, RunStatus::Errored);
    let msg = outcome.error.unwrap().message;
    assert!(
        msg.contains("user_id") || msg.contains("missing field"),
        "错误应指向字段路径，实际：{msg}"
    );

    // 校验失败不该留下半个 run：没有 STEP_* 事件。
    let events = store.get_events(&outcome.run_id).unwrap();
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, aa_workflow_core::WorkflowEvent::StepFinished { .. })),
        "schema 校验失败不应产生 StepFinished"
    );
}

/// input 形状不对 ⇒ `RunErrorCode::Validation`（上游 `validation_error`），
/// **不是**通用的 `Error`。
///
/// 这条单独钉，是因为它和「能不能发现」无关——serde 一直都拦得住（上一个测试
/// 就证了）。真正的差别是**错误码**：上游把「你给的 input 不合法」和「数据库
/// 挂了」分成两个码（`run-workflow.ts:219`），宿主的正确反应完全不同——前者是
/// 调用方的 400，不该重试；后者是我们自己的事。我们曾经两者都报 `error`。
///
/// 顺带钉住时机：校验发生在 `initialize` / handler **之前**（上游
/// `run-workflow.ts:209-213` 把两者放同一个 guarded block，先 input 后 state），
/// 所以不合法的 input 不会先把 workflow 跑起来。
#[tokio::test]
async fn bad_input_is_a_validation_error_not_a_generic_error() {
    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
    let events = run_workflow(
        RunWorkflowOptions::new(charge_workflow(), store.clone())
            .input(serde_json::json!({ "amount": 4200 })) // 缺 userId
            .run_id("typed-input-validation"),
    )
    .collect()
    .await;

    let code = events
        .iter()
        .find_map(|e| match e {
            aa_workflow_core::WorkflowEvent::RunErrored { code, .. } => Some(*code),
            _ => None,
        })
        .expect("形状不对的 input 必须以 RUN_ERRORED 收场");
    assert_eq!(
        code,
        aa_workflow_core::RunErrorCode::Validation,
        "input 形状失败要报 validation，对齐上游 validation_error"
    );
    // 时机：handler 一步都没跑。
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, aa_workflow_core::WorkflowEvent::StepStarted { .. })),
        "input 校验在 handler 之前，不该有任何 step 被启动"
    );
}

/// `sc.id` 是**确定性**的——resume 时 step 走缓存结果，chargeId 必须一致。
///
/// 这是 recipe 里「拿它当外部系统幂等键」这条承诺的回归：sc.id 若漂移，
/// 重放就会算出不同的幂等键，外部系统可能重复扣款。
#[tokio::test]
async fn step_ctx_id_is_stable_across_resume() {
    let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
    let wf = charge_workflow();
    let input = serde_json::json!({ "amount": 4200, "userId": "cus_123" });

    let first = run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(input.clone())
            .run_id("charge-1"),
    )
    .outcome().await
    .unwrap();
    assert_eq!(first.status, RunStatus::Finished);

    // 同 run_id 再 drive：已成功 step 短路，输出（含 chargeId）应逐字相同。
    let second = run_workflow(
        RunWorkflowOptions::new(wf.clone(), store.clone())
            .input(input)
            .run_id("charge-1"),
    )
    .outcome().await
    .unwrap();

    assert_eq!(second.status, RunStatus::Finished);
    assert_eq!(
        second.output, first.output,
        "resume 应短路到缓存结果，sc.id 不能漂移"
    );

    // 幂等键确实来自 sc.id，而不是每次新生成。
    let charge_id = |o: &aa_workflow_core::RunOutcome| {
        o.output
            .as_ref()
            .and_then(|v| v.get("chargeId"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    };
    assert_eq!(charge_id(&first), "ch_stripe-charge");
    assert_eq!(charge_id(&second), charge_id(&first));
}
