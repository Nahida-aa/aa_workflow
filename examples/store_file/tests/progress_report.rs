//! [`progress_report`] 的测试：`StepCtx::progress` **即时但不耐久**。
//!
//! workflow 定义在 `src/progress_report.rs`。
//!
//! 本文件要钉住的核心是一条**对比**，不是「progress 能用」：
//!
//! | 事件 | publisher 看到 | 事件日志里有 |
//! | --- | --- | --- |
//! | `STEP_PROGRESS` × N | ✅ | ❌ |
//! | `STEP_FINISHED` | ✅ | ✅ |
//!
//! 差在 `StepProgress` 的定义上——`/// Observability only (not persisted)`，
//! `publish()` 只调回调、从不 `append`。这不是遗漏：step 内进度在语义上就**不该**
//! 耐久（崩了就整个 step 重来，50% 无处安放），与 `docs/concepts/ctx-state.md`
//! 里「state 不是结果通道」是同一条道理。

use std::sync::{Arc, Mutex};

use aa_workflow_core::{RunStatus, RunWorkflowOptions, WorkflowEvent, run_workflow};
use aa_workflow_runtime::run_store_adapter::{WorkflowExecutionStore, create_run_store_adapter};
use aa_workflow_runtime::types::ReadEventsArgs;
use example_store_file::{FileExecutionStore, progress_report};

const ROUNDS: u32 = 5;

fn store_of(tag: &str) -> (Arc<dyn WorkflowExecutionStore>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "wf_store_file_progress_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("建临时目录");
    (Arc::new(FileExecutionStore::new(&dir)), dir)
}

/// 驱动一次，publisher 收到的全部事件 + store 里落盘的全部事件。
async fn drive(
    tag: &str,
    run_id: &str,
) -> (Vec<WorkflowEvent>, Vec<aa_workflow_runtime::types::StoredWorkflowEvent>) {
    let (store, dir) = store_of(tag);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let out = run_workflow(
        &RunWorkflowOptions::new(progress_report(), create_run_store_adapter(store.clone()))
            .input(serde_json::json!({ "rounds": ROUNDS, "tickMs": 5 }))
            .run_id(run_id)
            .publish(Some(Arc::new(move |_run_id: &str, e: WorkflowEvent| {
                sink.lock().unwrap().push(e.clone())
            }))),
    )
    .outcome().await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);
    assert_eq!(out.output.unwrap()["slow"]["rounds"], ROUNDS);

    let logged = store
        .read_events(ReadEventsArgs {
            run_id: run_id.to_string(),
            from_index: None,
        })
        .expect("读事件");
    let _ = std::fs::remove_dir_all(dir);
    (seen.lock().unwrap().clone(), logged)
}

fn progress_values(events: &[WorkflowEvent]) -> Vec<f64> {
    events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::StepProgress { step_id, value, .. } if step_id == "slow" => Some(*value),
            _ => None,
        })
        .collect()
}

fn logged_types(events: &[aa_workflow_runtime::types::StoredWorkflowEvent]) -> Vec<String> {
    events.iter().map(|e| e.event_type.clone()).collect()
}

/// publisher 在 step **执行过程中**就收到了 5 条 progress——不是等 step 结束
/// 一次性补发的。
#[tokio::test]
async fn publisher_receives_incremental_progress() {
    let (seen, _) = drive("live", "p1").await;
    assert_eq!(
        progress_values(&seen),
        vec![0.2, 0.4, 0.6, 0.8, 1.0],
        "应逐次收到 5 条递增的 progress"
    );

    // 顺序也重要：progress 必须夹在 Started 和 Finished 之间，
    // 否则就退化成「结束时回放一遍历史」，实时性是假的。
    let types: Vec<&str> = seen.iter().map(|e| e.type_name()).collect();
    let started = types.iter().position(|t| *t == "STEP_STARTED").expect("有 Started");
    let finished = types.iter().position(|t| *t == "STEP_FINISHED").expect("有 Finished");
    let first_p = types.iter().position(|t| *t == "STEP_PROGRESS").expect("有 Progress");
    let last_p = types.iter().rposition(|t| *t == "STEP_PROGRESS").expect("有 Progress");
    assert!(
        started < first_p && last_p < finished,
        "progress 应夹在 Started({started}) 与 Finished({finished}) 之间，实际序列 {types:?}"
    );
}

/// **关键断言：事件日志里一条 `STEP_PROGRESS` 都没有。**
///
/// 这条如果哪天红了，说明「不持久化」这个性质被破坏了 —— 而它是有意的：
/// progress 是观测信号，不是耐久事实。崩在 50% 时它本来就该跟着一起消失。
#[tokio::test]
async fn progress_is_never_persisted() {
    let (_, logged) = drive("log", "p2").await;
    let types = logged_types(&logged);
    assert!(
        !types.iter().any(|t| t == "STEP_PROGRESS"),
        "STEP_PROGRESS 不得落进事件日志，实际日志类型：{types:?}"
    );

    // 对照：同一个 step 的 Finished 确实在日志里。
    assert!(
        types.iter().any(|t| t == "STEP_FINISHED"),
        "STEP_FINISHED 应当落盘，实际：{types:?}"
    );
    // 两个 step 的 Finished 都在。
    let finished: Vec<&String> = types.iter().filter(|t| *t == "STEP_FINISHED").collect();
    assert_eq!(finished.len(), 2, "slow + phase 各一条 Finished");
}

/// 不调 `progress` 的 step：publisher 看到 Started/Finished 两态，日志里**只有
/// Finished**（`STEP_STARTED` 也是 emit-only）。
///
/// 顺带钉住一件事：**「step 内实时可见」只有主动报才有**——不报的话，外部能看到的
/// 粒度就只到「进入过」和「结束了」，中间发生什么都不留痕。
#[tokio::test]
async fn a_step_without_progress_leaves_only_finished_in_the_log() {
    let (seen, logged) = drive("quiet", "p3").await;

    let publisher_saw: Vec<&str> = seen
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::StepStarted { step_id, .. } | WorkflowEvent::StepFinished { step_id, .. } if step_id == "phase"))
        .map(|e| e.type_name())
        .collect();
    assert_eq!(
        publisher_saw,
        vec!["STEP_STARTED", "STEP_FINISHED"],
        "publisher 侧应看到两态"
    );

    let in_log: Vec<String> = logged
        .iter()
        .filter(|e| e.step_id.as_deref() == Some("phase"))
        .map(|e| e.event_type.clone())
        .collect();
    assert_eq!(
        in_log,
        vec!["STEP_FINISHED"],
        "STEP_STARTED 是 emit-only，落盘里应只有 Finished"
    );
}

/// resume 不会「接着上次的累计」——闭包整个被跳过，产物由日志恢复。
///
/// 这是「progress 不耐久」的必然结果，也是不把它当结果通道的理由。
#[tokio::test]
async fn resume_does_not_continue_the_progress_counter() {
    let dir = std::env::temp_dir().join(format!(
        "wf_store_file_progress_resume_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("建临时目录");
    let store: Arc<dyn WorkflowExecutionStore> = Arc::new(FileExecutionStore::new(&dir));
    let wf = progress_report();
    let input = serde_json::json!({ "rounds": ROUNDS, "tickMs": 5 });

    let first = run_workflow(
        &RunWorkflowOptions::new(wf.clone(), create_run_store_adapter(store.clone()))
            .input(input.clone())
            .run_id("r"),
    )
    .outcome().await
    .unwrap();
    assert_eq!(first.output.unwrap()["slow"]["reported"], ROUNDS);

    // 同 run_id 再跑一次：闭包被短路，reported 仍是日志里那个值，
    // 而不是「再报 5 次 = 10」。
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let second = run_workflow(
        &RunWorkflowOptions::new(wf, create_run_store_adapter(store))
            .input(input)
            .run_id("r")
            .publish(Some(Arc::new(move |_run_id: &str, e: WorkflowEvent| {
                sink.lock().unwrap().push(e.clone())
            }))),
    )
    .outcome().await
    .unwrap();
    assert_eq!(second.status, RunStatus::Finished);
    assert_eq!(
        second.output.unwrap()["slow"]["reported"],
        ROUNDS,
        "resume 应原样恢复日志里的值，不接着累加"
    );
    assert_eq!(
        progress_values(&seen.lock().unwrap()),
        Vec::<f64>::new(),
        "resume 时闭包被跳过，一条 progress 都不该重发"
    );

    let _ = std::fs::remove_dir_all(dir);
}
