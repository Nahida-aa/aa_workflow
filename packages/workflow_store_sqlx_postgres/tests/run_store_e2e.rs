//! core 侧端到端：`SqlxPostgresStore` → `create_run_store_adapter` → `run_workflow`。
//!
//! 契约套件（`tests/contract.rs`）验的是 runtime 面的 19 个方法。这里补的是
//! **core 面**：`run_workflow` 的入参是 `Arc<dyn RunStore>`（旧形状），要经
//! `create_run_store_adapter` 降格。这条链跑通，才说明这个 store 真能驱动一个
//! workflow——包括并发 step 的 CAS 追加（走真库的 `for update` 行锁，而不是内存
//! 的 Mutex）。
//!
//! 环境同 `tests/contract.rs`：`DATABASE_URL`，缺省 `postgres:///workflow_test`。

use std::sync::Arc;

use aa_workflow_store_sqlx_postgres::SqlxPostgresStore;
use sqlx::postgres::PgPoolOptions;
use aa_workflow_core::{RunStatus, RunWorkflowOptions, create_workflow, run_workflow};
use aa_workflow_runtime::run_store_adapter::create_run_store_adapter;

fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres:///workflow_test".to_string())
}

fn url_with_schema(url: &str, schema: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}options=-csearch_path%3D{schema}")
}

/// 一个带并行 step 的 workflow（`try_join!` 会触发并发 `append_event`）。
fn parallel_workflow() -> aa_workflow_core::WorkflowDefinition<serde_json::Value, serde_json::Value>
{
    create_workflow(
        aa_workflow_core::CreateWorkflowConfig::new("store-e2e")
            .input::<serde_json::Value>(),
    )
    .handler(
        |ctx: aa_workflow_core::BaseCtx<serde_json::Value>| async move {
            let a = ctx.clone();
            let b = ctx.clone();
            let (ra, rb) = tokio::try_join!(
                async move {
                    a.step("left", |_sc| async move {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        Ok(serde_json::json!({ "side": "left" }))
                    })
                    .await
                },
                async move {
                    b.step("right", |_sc| async move {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        Ok(serde_json::json!({ "side": "right" }))
                    })
                    .await
                },
            )?;
            Ok(serde_json::json!({ "left": ra, "right": rb }))
        },
    )
}

#[test]
fn run_workflow_against_postgres_store() {
    let url = database_url();

    // **全程一个 multi-thread runtime**。
    //
    // 这是关键：sqlx 连接池是 runtime 绑定的——池在哪个 runtime 上建，就要用哪个
    // runtime 驱动它的连接。之前踩的 `pool timed out while waiting for an open
    // connection`，根因就是「池建在 A runtime、却在 B runtime 上被使用」。
    //
    // 用 multi-thread（worker ≥2）还因为 store 的同步桥走 `block_in_place`：
    // 它会占用一个 worker，而连接池回收连接要靠 runtime 继续驱动后台任务。
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // 先探连通性（连不上就 skip）。
    if rt.block_on(PgPoolOptions::new().max_connections(1).connect(&url)).is_err() {
        eprintln!("跳过 e2e 测试：连不上 Postgres ({url})");
        return;
    }

    let schema = format!("e2e_{}", uuid::Uuid::new_v4().simple());

    // 建 schema + 建表 + 建 store 的池，全在这一个 runtime 上。
    let pool = rt.block_on(async {
        let boot = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("连 Postgres");
        sqlx::query(&format!("create schema if not exists \"{schema}\""))
            .execute(&boot)
            .await
            .expect("建 schema");
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(&url_with_schema(&url, &schema))
            .await
            .expect("连 schema");
        aa_workflow_store_sqlx_postgres::migrations::ensure_schema(&pool)
            .await
            .expect("ensure_schema");
        pool
    });
    let store = SqlxPostgresStore::new(pool.clone());

    // 跑 workflow：同一个 runtime 驱动。
    let wf = parallel_workflow();
    let core_store = create_run_store_adapter(Arc::new(store));
    let outcome = rt
        .block_on(async {
            run_workflow(
                &RunWorkflowOptions::new(Arc::new(wf.clone().into_workflow()), core_store)
                    .input(serde_json::json!({ "n": 1 })),
            )
            .await
        })
        .expect("run_workflow");

    assert_eq!(outcome.status, RunStatus::Finished, "workflow 应跑完");

    // 事件确实落到了 Postgres。
    let events: i64 = rt
        .block_on(sqlx::query_scalar("select count(*) from workflow_events").fetch_one(&pool))
        .expect("查事件数");
    assert!(events > 0, "事件应已落库，实际 {events} 条");

    // 两个并行 step 都记了终态。
    for step in ["left", "right"] {
        let n: i64 = rt
            .block_on(
                sqlx::query_scalar(
                    "select count(*) from workflow_events where step_id = $1 and event_type = 'STEP_FINISHED'",
                )
                .bind(step)
                .fetch_one(&pool),
            )
            .expect("查 step 事件");
        assert_eq!(n, 1, "{step} 应有且仅有 1 条 STEP_FINISHED");
    }

    // 清场：先关掉指向该 schema 的池（否则它的连接还挂在 schema 上，
    // `drop schema` 会失败/阻塞），再用未限定的连接 drop。
    let _ = rt.block_on(async {
        pool.close().await;
        let boot = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .ok()?;
        sqlx::query(&format!("drop schema if exists \"{schema}\" cascade"))
            .execute(&boot)
            .await
            .ok()?;
        Some(())
    });
}
