//! 共享契约套件的运行入口。
//!
//! 直接调 `aa_workflow_runtime::store_contract::run_store_contract`——**不另写一套**。
//! 这正是该套件的设计意图（`store_contract.rs` 模块文档）：
//!
//! > 一份可执行规格，N 个实现共用。任何 store 想宣告自己兼容，跑
//! > [`run_store_contract`] 就行。
//!
//! # 环境
//!
//! 需要真实 Postgres。连接串取 `DATABASE_URL`，缺省 `postgres:///workflow_test`
//! （unix socket + 当前用户）。连不上就 skip（打印提示），不让 CI 硬红。
//!
//! # 隔离与 runtime 处理
//!
//! 契约套件是**同步**的（`fn(&StoreFactory)`），而 `SqlxPostgresStore` 内部要
//! `block_on`。所以整段跑在 `spawn_blocking` 出来的独立线程上，并在那里建一个
//! runtime，**全程复用**（建池 / ensure_schema / 每个用例的 store 都指向它）。
//! 复用的原因不只是省事：临时 runtime 在异步上下文里被 drop 会 panic
//! （tokio 的限制），集中建一个、在同步线程里 drop 才安全。
//!
//! 隔离用**每条用例一个独立 schema**，跑完 drop。

use std::sync::Arc;

use aa_workflow_store_sqlx_postgres::SqlxPostgresStore;
use sqlx::postgres::PgPoolOptions;
use aa_workflow_runtime::run_store_adapter::WorkflowExecutionStore;
use aa_workflow_runtime::store_contract::run_store_contract;

fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres:///workflow_test".to_string())
}

/// 把 `search_path` 塞进连接串的 `options`。
///
/// 比 `after_connect` 回调可靠：那是每连接建好后执行一次 SQL，而 sqlx 池会
/// 复用连接、也可能在某些路径上绕过；URL 的 `options` 由 libpq/postgres 在
/// **连接建立时**应用，池里每条连接都生效。
fn url_with_schema(url: &str, schema: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}options=-csearch_path%3D{schema}")
}

#[tokio::test]
async fn sqlx_postgres_satisfies_execution_store_contract() {
    // 先探一次连通性，连不上就 skip（在 tokio 上下文里探，探测用的池随后 drop
    // 在这个 async 块内——sqlx 的池可以在异步上下文里 drop，只有 runtime 不行）。
    let url = database_url();
    if PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .is_err()
    {
        eprintln!("跳过契约测试：连不上 Postgres ({url})");
        return;
    }

    let handle = tokio::task::spawn_blocking(move || run_contract_sync(&url));
    let outcome = handle.await.expect("契约测试线程 panic");
    if let Err(msg) = outcome {
        panic!("{msg}");
    }
}

/// 在同步线程里建 runtime，跑完整套契约。
///
/// runtime 在函数末尾 drop——此时已在 tokio 上下文之外，安全。
fn run_contract_sync(url: &str) -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("建 runtime 失败: {e}"))?;
    let rt = Arc::new(rt);

    // 每个用例的 schema 记下来，跑完统一清掉（不留垃圾）。
    let created: Arc<std::sync::Mutex<Vec<String>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
        let created = Arc::clone(&created);
        let rt = Arc::clone(&rt);
        move || {
            run_store_contract("sqlx-postgres", || {
                let case_schema = format!("test_{}", uuid::Uuid::new_v4().simple());
                created.lock().unwrap().push(case_schema.clone());
                // 建用例 schema + 建表，然后连到它。全部用共享的 rt 驱动。
                let pool = rt.block_on(async {
                    let boot = PgPoolOptions::new()
                        .max_connections(1)
                        .connect(url)
                        .await
                        .expect("连 Postgres（建 schema）");
                    sqlx::query(&format!("create schema if not exists \"{case_schema}\""))
                        .execute(&boot)
                        .await
                        .expect("建用例 schema");
                    let pool = PgPoolOptions::new()
                        .max_connections(4)
                        .connect(&url_with_schema(url, &case_schema))
                        .await
                        .expect("连 Postgres（用例 schema）");
                    aa_workflow_store_sqlx_postgres::migrations::ensure_schema(&pool)
                        .await
                        .expect("用例 ensure_schema");
                    pool
                });
                Arc::new(SqlxPostgresStore::new(pool))
                    as Arc<dyn WorkflowExecutionStore>
            });
        }
    }));

    // 清场：drop 所有用例 schema。
    let schemas: Vec<String> = created.lock().unwrap().clone();
    if !schemas.is_empty() {
        let _ = rt.block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(url)
                .await
                .ok()?;
            for s in &schemas {
                let _ = sqlx::query(&format!("drop schema if exists \"{s}\" cascade"))
                    .execute(&pool)
                    .await;
            }
            Some(())
        });
    }

    match result {
        Ok(()) => Ok(()),
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "<非字符串 panic>".to_string());
            Err(msg)
        }
    }
}
