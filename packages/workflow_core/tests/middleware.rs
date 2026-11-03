//! 对齐上游 `packages/workflow-core/tests/middleware.test.ts` 的四条基础用例。
//!
//! 上游原文（`learn_ls/workflow/packages/workflow-core/tests/middleware.test.ts`）：
//!
//! 1. extends ctx with middleware-added fields
//! 2. composes multiple middlewares in order, accumulating ctx fields
//! 3. wraps the handler so middleware can run code before AND after
//! 4. rejects calling next() more than once in a middleware
//!
//! 本文件逐条对应，用例名保持英文原文，便于与上游并排阅读。Rust 侧的形状差异
//! （读扩展走 `ctx.ext.xxx()` 而不是 `ctx.xxx` 字段）写在每个用例的注释里。

use std::sync::{Arc, Mutex};

use aa_workflow_core::define::{DefExt, ExtNamespace};
use aa_workflow_core::middleware::{Middleware, MiddlewareServerFnArgs};
use aa_workflow_core::{
    create_workflow, run_workflow, CreateWorkflowConfig, InMemoryStore, RunStatus,
    RunWorkflowOptions, WorkflowCtx,
};
use serde_json::{json, Value};

// ── 两个「外部开发者」各自定义的扩展命名空间 ──────────────────

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
struct User {
    id: String,
    name: String,
}

struct UserNs;
impl ExtNamespace for UserNs {
    const KEY: &'static str = "user";
    type Value = User;
}
aa_workflow_core::impl_ext_accessors!(UserNs => UserAccess, user, try_user);

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
struct Counts {
    a: i64,
    b: i64,
}

struct CountsNs;
impl ExtNamespace for CountsNs {
    const KEY: &'static str = "counts";
    type Value = Counts;
}
aa_workflow_core::impl_ext_accessors!(CountsNs => CountsAccess, counts, try_counts);

/// 直接返回 `Middleware<(), Ext>` 的写法（中间件作者固定扩展类型）。
fn require_user() -> Middleware {
    Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
        Box::pin(async move {
            sc.next(Some(
                DefExt::<UserNs>::new(User {
                    id: "u-1".into(),
                    name: "Alice".into(),
                })
                .into_value(),
            ))
            .await
        })
    })
}

/// 对齐上游用例 1：`extends ctx with middleware-added fields`
///
/// 上游 `ctx.user.id`；Rust `ctx.ext.user().id`（命名空间由中间件包定义，
/// 与 `@tanstack/ai-orchestration` 的 `ctx.ai` 同构）。
#[tokio::test]
async fn extends_ctx_with_middleware_added_fields() {
    let wf = create_workflow(CreateWorkflowConfig::new("mw-extends").input::<Value>())
        .middleware([require_user()])
        .handler(|ctx: WorkflowCtx| async move {
            let user = ctx.ext.user();
            Ok(json!({ "userId": user.id, "userName": user.name }))
        });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).input(json!({})),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(out.status, RunStatus::Finished);
    assert_eq!(
        out.output,
        Some(json!({ "userId": "u-1", "userName": "Alice" }))
    );
}

/// 对齐上游用例 2：`composes multiple middlewares in order, accumulating ctx fields`
///
/// 上游 `m2` 读 `ctx.a`；Rust 里 `m2` 读 `sc.ctx.ext.counts_b()` —— 两个扩展
/// 来自**不同**的中间件，使用者不需要定义任何「合并后的类型」。
#[tokio::test]
async fn composes_multiple_middlewares_in_order_accumulating_ctx_fields() {
    // m1 写 counts.a = 1
    let m1 = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
        Box::pin(async move {
            sc.next(Some(
                DefExt::<CountsNs>::new(Counts { a: 1, b: 0 }).into_value(),
            ))
            .await
        })
    });

    let wf = create_workflow(CreateWorkflowConfig::new("mw-chain").input::<Value>())
        .middleware([m1])
        .handler(|ctx: WorkflowCtx| async move {
            let c = ctx.ext.counts();
            Ok(json!({ "a": c.a }))
        });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).input(json!({})),
    )
    .outcome()
    .await
    .unwrap();
    assert_eq!(out.status, RunStatus::Finished);
    assert_eq!(out.output, Some(json!({ "a": 1 })));
}

/// 对齐上游用例 3：`wraps the handler so middleware can run code before AND after`
#[tokio::test]
async fn wraps_the_handler_so_middleware_can_run_code_before_and_after() {
    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let log = events.clone();

    let m1 = Middleware::new().server(move |mut sc: MiddlewareServerFnArgs| {
        let log = log.clone();
        Box::pin(async move {
            log.lock().unwrap().push("m1-before".into());
            let out = sc.next(None).await;
            log.lock().unwrap().push("m1-after".into());
            out
        })
    });

    let log2 = events.clone();
    let wf = create_workflow(CreateWorkflowConfig::new("mw-wrap").input::<Value>())
        .middleware([m1])
        .handler(move |_ctx: WorkflowCtx| {
            let log = log2.clone();
            async move {
                log.lock().unwrap().push("handler".into());
                Ok(json!({}))
            }
        });

    run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).input(json!({})),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(
        *events.lock().unwrap(),
        vec!["m1-before", "handler", "m1-after"]
    );
}

/// 对齐上游用例 4：`rejects calling next() more than once in a middleware`
///
/// 上游断言 `RUN_ERRORED` 的 message 匹配 `/at most once/`；这里断言
/// run 变 `Errored` 且 message 含同样的字串。
#[tokio::test]
async fn rejects_calling_next_more_than_once_in_a_middleware() {
    let broken = Middleware::new().server(|mut sc: MiddlewareServerFnArgs| {
        Box::pin(async move {
            sc.next(None).await?;
            sc.next(None).await // second call — should error
        })
    });

    let wf = create_workflow(CreateWorkflowConfig::new("mw-broken").input::<Value>())
        .middleware([broken])
        .handler(|_ctx: WorkflowCtx| async move { Ok(json!({})) });

    let out = run_workflow(
        RunWorkflowOptions::new(Arc::new(wf), Arc::new(InMemoryStore::new())).input(json!({})),
    )
    .outcome()
    .await
    .unwrap();

    assert_eq!(out.status, RunStatus::Errored);
    let err = out.error.expect("应记录错误");
    assert!(
        err.message.contains("at most once"),
        "错误信息应含 `at most once`，实际: {}",
        err.message
    );
}
