//! publisher 的两条语义（B 半边 + A 半边）。
//!
//! 改造前 `publish()` 是**内联同步**调用，于是：
//!
//! - 宿主 publisher 慢 ⇒ 引擎每一步都被拖住
//! - 宿主只能同步 I/O，想 `await` 也写不了
//!
//! 改造后对齐上游 `runWorkflow` 的 async generator 架构（`run-workflow.ts:85-134`）：
//! `queue.shift()` → `await publish` → `yield`，而**执行在另一个 task** 里往
//! queue 推。所以两件事同时成立：
//!
//! - 慢 publisher 拖慢的是**消费端吞吐**，不是**引擎进度**
//! - 宿主可以 `await`（A 半边）
//!
//! 而「返回前最后一条事件已投递」是**守住**的旧语义，没有静默退化成
//! fire-and-forget —— 收尾会发 shutdown 并 join drain task。

use std::sync::Arc;
use std::time::{Duration, Instant};

use aa_workflow_core::{
    BaseCtx, CreateWorkflowConfig, RunStatus, RunWorkflowOptions, StepCtx, WorkflowEvent,
    create_workflow, run_workflow,
};
use aa_workflow_runtime::run_store_adapter::{WorkflowExecutionStore, create_run_store_adapter};
use example_store_file::FileExecutionStore;

fn store_of(tag: &str) -> Arc<dyn WorkflowExecutionStore> {
    let dir = std::env::temp_dir().join(format!(
        "wf_pub_async_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("建临时目录");
    Arc::new(FileExecutionStore::new(&dir))
}

/// 三个 step 的 workflow，返回 step id 序列。
fn three_steps() -> Arc<aa_workflow_core::Workflow> {
    Arc::new(
        create_workflow(CreateWorkflowConfig::new("t").input::<serde_json::Value>())
            .handler(|ctx: BaseCtx<serde_json::Value>| async move {
                let mut done = Vec::new();
                for id in ["a", "b", "c"] {
                    done.push(
                        ctx.step(id, move |_sc: StepCtx| async move { Ok(serde_json::Value::Null) })
                            .await?,
                    );
                }
                Ok(serde_json::json!(done))
            })
            .into_workflow(),
    )
}

/// **B 半边：慢 publisher 不阻塞引擎执行。**
///
/// publisher 每个事件睡 150ms。改造前 3 个 step 至少 6 个事件 ⇒ handler 自身
/// 就要 900ms+；改造后 handler 应当几乎立刻跑完，睡眠全部发生在收尾 drain。
#[tokio::test]
async fn slow_publisher_does_not_stall_the_engine() {
    let store = store_of("slow");
    let handler_elapsed = Arc::new(std::sync::Mutex::new(Duration::ZERO));
    let slot = handler_elapsed.clone();
    let sink_elapsed = sink_elapsed();
    let sink_slot = sink_elapsed.clone();

    let wf = Arc::new(
        create_workflow(CreateWorkflowConfig::new("t").input::<serde_json::Value>())
            .handler(move |ctx: BaseCtx<serde_json::Value>| {
                let slot = slot.clone();
                async move {
                    let t0 = Instant::now();
                    let mut done = Vec::new();
                    for id in ["a", "b", "c"] {
                        done.push(
                            ctx.step(id, move |_sc: StepCtx| async move {
                                Ok(serde_json::Value::Null)
                            })
                            .await?,
                        );
                    }
                    *slot.lock().unwrap() = t0.elapsed();
                    Ok(serde_json::json!(done))
                }
            })
            .into_workflow(),
    );

    let out = run_workflow(
        &RunWorkflowOptions::new(wf, create_run_store_adapter(store))
            .input(serde_json::json!({}))
            .run_id("r")
            .async_publisher(move |_e| {
                let slot = sink_slot.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    slot.lock().unwrap().push(Instant::now());
                }
            }),
    )
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);

    let handler = *handler_elapsed.lock().unwrap();
    assert!(
        handler < Duration::from_millis(300),
        "handler 自身跑了 {handler:?}——publisher 的 150ms 睡眠被算进了引擎路径，\
         说明 publish 还在内联 await。事件数 {} 个，内联的话至少 900ms。",
        sink_elapsed.lock().unwrap().len(),
    );
    assert!(
        !sink_elapsed.lock().unwrap().is_empty(),
        "publisher 应当被调用过"
    );
}

fn sink_elapsed() -> Arc<std::sync::Mutex<Vec<Instant>>> {
    Arc::new(std::sync::Mutex::new(Vec::new()))
}

/// **A 半边：async publisher 真的能 await。**
///
/// 改造前 `publisher` 收 `Fn(&WorkflowEvent)`，宿主想 `tokio::time::sleep().await`
/// 只能自己 `block_on`，那会把 executor 卡住。
#[tokio::test]
async fn async_publisher_can_await_without_blocking_the_runtime() {
    let store = store_of("await");
    // 用 `current_thread` runtime：如果 publisher 靠 block_in_place 硬等，
    // 同一个线程上的 sleep 就没法推进 → 测试会超时。
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let sink = seen.clone();

    let out = run_workflow(
        &RunWorkflowOptions::new(three_steps(), create_run_store_adapter(store))
            .input(serde_json::json!({}))
            .run_id("r")
            .async_publisher(move |e| {
                let sink = sink.clone();
                async move {
                    // 真 await：让出执行权。事件按值进来，整条 move 进 async。
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    sink.lock().unwrap().push(e.type_name().to_string());
                }
            }),
    )
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);
    assert!(!seen.lock().unwrap().is_empty(), "async publisher 应被调用");
}

/// **顺序保证：投递顺序 == 产生顺序。**
///
/// drain task 逐个 await，无界队列不重排，所以订阅者看到的序列必须是确定的。
#[tokio::test]
async fn delivery_order_matches_production_order() {
    let store = store_of("order");
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let sink = seen.clone();

    run_workflow(
        &RunWorkflowOptions::new(three_steps(), create_run_store_adapter(store))
            .input(serde_json::json!({}))
            .run_id("r")
            .async_publisher(move |e| {
                // 按值传入 ⇒ 可以直接 `async move` 整个事件，不需要先取值。
                let sink = sink.clone();
                async move {
                    // 抖动一下：若实现有并发投递，顺序就会乱
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    if let WorkflowEvent::StepFinished { step_id, .. } = &e {
                        sink.lock().unwrap().push(step_id.clone());
                    }
                }
            }),
    )
    .await
    .unwrap();

    let got = seen.lock().unwrap().clone();
    assert_eq!(got, vec!["a", "b", "c"], "step 的投递顺序必须等于执行顺序");
}

/// **回归护栏：返回前终态事件已投递。**
///
/// 收尾 `finish_fanout` 发 shutdown + join drain 就是为了这条。没有它，
/// `RUN_FINISHED` 可能还躺在队列里，宿主就永远等不到 run 结束的通知。
#[tokio::test]
async fn terminal_event_is_delivered_before_run_workflow_returns() {
    let store = store_of("term");
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let sink = seen.clone();

    let out = run_workflow(
        &RunWorkflowOptions::new(three_steps(), create_run_store_adapter(store))
            .input(serde_json::json!({}))
            .run_id("r")
            .async_publisher(move |e| {
                let sink = sink.clone();
                async move {
                    // 每个事件都睡一会儿，让「还没排空就返回」变得容易暴露
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    sink.lock().unwrap().push(e.type_name().to_string());
                }
            }),
    )
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);

    let got = seen.lock().unwrap().clone();
    assert_eq!(
        got.last().map(String::as_str),
        Some("RUN_FINISHED"),
        "最后一条必须是 RUN_FINISHED，实际尾部：{:?}",
        &got[got.len().saturating_sub(4)..],
    );
}

/// Paused 早退路径也得排空 drain——它比正常收尾早 return，最容易漏。
#[tokio::test]
async fn paused_path_also_drains_the_queue() {
    let store = store_of("paused");
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let sink = seen.clone();

    let wf = Arc::new(
        create_workflow(CreateWorkflowConfig::new("p").input::<serde_json::Value>())
            .handler(|ctx: BaseCtx<serde_json::Value>| async move {
                ctx.step(
                    "gate",
                    move |_sc: StepCtx| async move { Ok(serde_json::Value::Null) },
                )
                .await?;
                ctx.approve("need-ok", "等外部批准").await?;
                Ok(serde_json::Value::Null)
            })
            .into_workflow(),
    );

    let out = run_workflow(
        &RunWorkflowOptions::new(wf, create_run_store_adapter(store))
            .input(serde_json::json!({}))
            .run_id("r")
            .async_publisher(move |e| {
                let sink = sink.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    sink.lock().unwrap().push(e.type_name().to_string());
                }
            }),
    )
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Paused);

    let got = seen.lock().unwrap().clone();
    assert_eq!(
        got.last().map(String::as_str),
        Some("STEP_PAUSED"),
        "Paused 早退也必须排空 drain，终态事件应是 STEP_PAUSED，实际：{got:?}",
    );
}

/// **按值语义的意义**：事件能整条 move 进 async block，不需要「先取值再进
/// async」那套（`&WorkflowEvent` + `BoxFuture<'static, _>` 的组合做不到这点）。
///
/// 顺带钉住「引擎侧只克隆一次」：publisher 拿到的是**独占所有权**，所以它可以
/// 把事件 move 进 `tokio::spawn` —— 若是 `&`，这里就得自己再克隆。
#[tokio::test]
async fn event_moves_into_the_future_with_no_extra_clone() {
    let store = store_of("move");
    let done = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let sink = done.clone();

    run_workflow(
        &RunWorkflowOptions::new(three_steps(), create_run_store_adapter(store))
            .input(serde_json::json!({}))
            .run_id("r")
            .async_publisher(move |ev| {
                let sink = sink.clone();
                async move {
                    // 整条 move 进独立 task：只有按值传才做得到
                    tokio::spawn(async move {
                        sink.lock().unwrap().push(ev.type_name().to_string());
                    })
                    .await
                    .expect("spawn 成功");
                }
            }),
    )
    .await
    .unwrap();

    let got = done.lock().unwrap().clone();
    assert_eq!(got.last().map(String::as_str), Some("RUN_FINISHED"), "{got:?}");
    assert!(
        got.iter().filter(|t| *t == "STEP_FINISHED").count() == 3,
        "三个 step 的 Finished 都应送达：{got:?}"
    );
}
