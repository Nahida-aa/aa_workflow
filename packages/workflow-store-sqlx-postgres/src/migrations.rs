//! Schema 迁移：package-owned SQL 的加载与执行。
//!
//! 对齐上游 `SCHEMA_MIGRATIONS.md` 的体例——**迁移由包自己拥有**，应用侧
//! 应该执行包里带的 SQL 产物，而不是把 `workflow_*` 表抄进自己的 schema。
//!
//! ## `ensure_schema` 的定位（照抄上游 `docs/api/store-adapters.md`）
//!
//! > Use this from tests, local demos, or an explicit admin bootstrap script,
//! > not from every request or sweep. Runtime and host adapters assume the
//! > schema exists. Production deploys should prefer the package-owned SQL
//! > migration artifact.
//!
//! 即：**测试 / 本地 demo / 显式 bootstrap 脚本**可用；**不要在 request
//! handler、sweep、cron tick 里调**。生产走
//! `psql "$DATABASE_URL" -f migrations/0000_workflow_store.sql`。

/// 初始迁移的 SQL 源文本（编译期嵌入，保证与 `migrations/` 下的产物一致）。
pub const INITIAL_MIGRATION_SQL: &str =
    include_str!("../migrations/0000_workflow_store.sql");

/// 本次发布包含的全部迁移，按执行顺序。
pub const MIGRATIONS: &[Migration] = &[Migration {
    id: "0000_workflow_store",
    sql: INITIAL_MIGRATION_SQL,
}];

/// 一条迁移。
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    pub id: &'static str,
    pub sql: &'static str,
}

/// 迁移记录表里的一行（`workflow_schema_migrations`）。
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AppliedMigration {
    pub migration_id: String,
    pub package_name: String,
    pub package_version: Option<String>,
    pub applied_at: i64,
}

/// 在 `pool` 上建表（幂等）。见模块文档关于调用时机的说明。
///
/// 所有语句都是 `if not exists`，可安全重复调用。
pub async fn ensure_schema(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    for m in MIGRATIONS {
        sqlx::raw_sql(m.sql).execute(pool).await?;
    }
    Ok(())
}

/// 读已应用的迁移记录（供测试断言迁移被正确登记）。
pub async fn applied_migrations(
    pool: &sqlx::PgPool,
) -> anyhow::Result<Vec<AppliedMigration>> {
    let rows = sqlx::query_as::<_, AppliedMigration>(
        "select migration_id, package_name, package_version, applied_at \
         from workflow_schema_migrations order by migration_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
