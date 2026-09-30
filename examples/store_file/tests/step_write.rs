//! [`step_write`] 的测试：三个 step 各自在「闭包内」与「handler 体外」读写
//! `ctx.state` 的可见性对照。
//!
//! workflow 定义在 `src/step_write.rs`（单循环、三个 step、刻意最小）。
//!
//! 重放侧的丢失（闭包被短路）在
//! `examples/shared/src/workflows.rs::state_step_closure_mutation_lost_on_resume`
//! 有覆盖；本文件只钉住**同一次 drive 内**的可见性。

use std::sync::Arc;

use aa_workflow_core::{RunStatus, RunWorkflowOptions, WorkflowEvent, run_workflow};
use aa_workflow_runtime::run_store_adapter::{WorkflowExecutionStore, create_run_store_adapter};
use aa_workflow_runtime::types::ReadEventsArgs;
use example_store_file::{FileExecutionStore, step_write};

fn core_store(tag: &str) -> (Arc<dyn WorkflowExecutionStore>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "wf_store_file_step_write_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("建临时目录");
    let store: Arc<dyn WorkflowExecutionStore> = Arc::new(FileExecutionStore::new(&dir));
    (store, dir)
}

async fn run(tag: &str) -> serde_json::Value {
    let (store, dir) = core_store(tag);
    let core = create_run_store_adapter(store);
    let out = run_workflow(
        &RunWorkflowOptions::new(step_write(), core)
            .input(serde_json::json!({ "base": 7 })),
    )
    .outcome().await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);
    let o = out.output.unwrap();
    let _ = std::fs::remove_dir_all(dir);
    o
}

/// **`Arc<Mutex<_>>`（用户自己共享）**：闭包内的写，下一个 step 读得到 —— 链通。
///
/// 这就是「TS 闭包按引用捕获」在 Rust 里的对应物：`Arc` 是引用语义，闭包捕获它
/// 等于捕获同一个值。所以用户要的「步骤内写、下一步读到」**在 Rust 里做得到**，
/// 只是不能靠 `StateHandle::clone`。
///
/// 代价（不在本测试断言，但要知道）：它不是 `ctx.state`——没有 `STATE_DELTA`、
/// 不过 state schema，且 resume 时闭包被短路则不重演。
#[tokio::test]
async fn arc_shares_across_the_three_steps() {
    let o = run("arc").await;
    assert_eq!(
        o["arcReads"],
        serde_json::json!([0, 1, 2]),
        "第 i 个 step 应读到上一个闭包经 Arc 写的 i"
    );
    assert_eq!(o["arcWrote"], serde_json::json!([1, 2, 3]));
    assert_eq!(o["arcAfter"], serde_json::json!([1, 2, 3]));
}

/// **对照组**：handler 体外写的值，下一个 step 读得到 —— 链是通的。
///
/// `ctx.state` 是 driver 手里的活句柄（`DerefMut` 就地改，`state_handle.rs:130`），
/// 没有副本、没有快照，所以步骤间天然共享。
#[tokio::test]
async fn body_writes_chain_across_the_three_steps() {
    let o = run("body").await;
    assert_eq!(
        o["bodyReads"],
        serde_json::json!([0, 1, 2]),
        "第 i 个 step 应读到 i"
    );
    assert_eq!(o["finalBody"], 3, "三次体外写都留下了");
}

/// **目标**：闭包内的写**不往下传** —— 三个 step 各自从 0 开始，链是断的。
///
/// 根因在 `StateHandle::clone`（`state_handle.rs:62`）：它复制 `TState`、只共享
/// mirror。闭包捕获的克隆体有自己的 `st`（`:57`），`DerefMut` 改的是那一份
/// （`:130`）；driver 原句柄的 `Deref` 读自己的 `st`（`:126`）——从 clone 那一刻
/// 起就分家了。driver 侧永远是 `initialize` 播种的值。
#[tokio::test]
async fn closure_writes_do_not_chain_across_the_three_steps() {
    let o = run("closure").await;

    // 每个闭包都从 0 读起 —— 上一个闭包写的 1 从未落到 driver 上。
    assert_eq!(
        o["closureSawClosure"],
        serde_json::json!([0, 0, 0]),
        "闭包内读 via_closure 应恒为 0：链断了，每步都重新开始"
    );
    // 写本身是成功的（对它自己而言）。
    assert_eq!(o["closureWrote"], serde_json::json!([1, 1, 1]));
    // 闭包返回后 driver 读，全空。
    assert_eq!(
        o["driverAfterClosure"],
        serde_json::json!([0, 0, 0]),
        "driver 句柄应从未看到闭包内的写"
    );
    assert_eq!(o["finalClosure"], 0, "最终值仍是播种的 0");
}

/// 排除误读：闭包**能**读到体外写的最新值，所以断的只是「写回去」那一半。
///
/// `sawBody` 是 `[1,2,3]`——s1 的闭包看到 1，正是 s1 自己那步体外写产出的值。
/// 说明 clone 精确取在「本步写之后」，没有滞后一拍。若它读到过期的值，结论就会
/// 变成「闭包整体看到过期快照」（另一回事，接近 clone 时机问题）。而同一批闭包里
/// `sawClosure` 恒为 0 —— 差别只在写有没有出口。
#[tokio::test]
async fn closure_reads_the_latest_body_value_so_only_the_write_is_lost() {
    let o = run("timing").await;
    assert_eq!(
        o["closureSawBody"],
        serde_json::json!([1, 2, 3]),
        "闭包应读到本步体外写之后的最新值"
    );
    // 同一批闭包里，via_body 读得到、via_closure 读不到 —— 差别只在写。
    assert_eq!(o["closureSawBody"], serde_json::json!([1, 2, 3]));
    assert_eq!(o["closureSawClosure"], serde_json::json!([0, 0, 0]));
}

/// `initialize` 播种的值不受影响——闭包的写不是「覆盖」，是「没发生」。
#[tokio::test]
async fn initialize_seeded_base_is_untouched() {
    let o = run("base").await;
    assert_eq!(o["base"], 7, "base 应仍是 initialize 播种的 7");
}

/// 正确通道：step 的**返回值**把三个闭包算出的数据都带了出来，且落进日志。
///
/// 这是闭包内唯一能让结果离开的路径——进事件日志、参与重放。顺带验了
/// 「durable」不是修辞：返回值确实在 `StepFinished.result` 里。
#[tokio::test]
async fn step_return_values_reach_the_log_for_all_three_steps() {
    let (store, dir) = core_store("logged");
    let core = create_run_store_adapter(store.clone());
    let out = run_workflow(
        &RunWorkflowOptions::new(step_write(), core)
            .input(serde_json::json!({ "base": 7 }))
            .run_id("step-write-logged".to_string()),
    )
    .outcome().await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);

    let events = store
        .read_events(ReadEventsArgs {
            run_id: out.run_id,
            from_index: None,
        })
        .unwrap();
    let logged_of = |id: &str| {
        events
            .iter()
            .find(|e| e.event_type == "STEP_FINISHED" && e.step_id.as_deref() == Some(id))
            .map(|e| match &e.event {
                WorkflowEvent::StepFinished { result, .. } => result.as_ref().unwrap().clone(),
                other => panic!("应是 StepFinished，实际 {other:?}"),
            })
            .unwrap_or_else(|| panic!("{id} 应有 StepFinished"))
    };

    assert_eq!(logged_of("s1")["wrote"], 1, "s1 的返回值进了日志");
    assert_eq!(logged_of("s2")["wrote"], 1, "s2 的返回值进了日志");
    assert_eq!(logged_of("s3")["wrote"], 1, "s3 的返回值进了日志");
    // 三个闭包各自 sawClosure 都是 0 —— 断链这件事也一起落进日志了。
    assert_eq!(logged_of("s1")["sawClosure"], 0);
    assert_eq!(logged_of("s2")["sawClosure"], 0);
    assert_eq!(logged_of("s3")["sawClosure"], 0);

    let _ = std::fs::remove_dir_all(dir);
}

/// step 正常完成并落终态——「写不到」不是失败，是**静默**无效。
///
/// 这条最需要钉住：没有报错、没有告警，run 照样 `Finished`，三次闭包写全部丢失。
#[tokio::test]
async fn steps_still_succeed_despite_every_closure_write_being_dropped() {
    let o = run("silent").await;
    assert_eq!(o["finalClosure"], 0, "三次闭包写全部蒸发");
    assert_eq!(
        o["finalBody"], 3,
        "同时体外写是好的 —— 静默失效只针对闭包内的写"
    );
    // Arc 那条同时也是好的：说明问题在 StateHandle::clone，不在「闭包内写」本身。
    assert_eq!(o["arcAfter"], serde_json::json!([1, 2, 3]));
}
