# Postgres Store Schema Migrations

`aa_workflow_store_sqlx_postgres` owns the durable Workflow store schema.
Applications should apply package-owned migrations instead of copying
`workflow_*` tables into their own schema.

体例对齐上游 `@tanstack/workflow-store-drizzle-postgres` 的
[`SCHEMA_MIGRATIONS.md`](../../../learn_ls/workflow/packages/workflow-store-drizzle-postgres/SCHEMA_MIGRATIONS.md)：
表名 / 列名逐字沿用，两边可对照。

## When to Version

改到以下任一处就要发变更：

- `migrations/` 下的 SQL 迁移文件
- `src/migrations.rs` 的迁移列表与 helper
- `src/store.rs` 里依赖表 / 列 / 索引 / 锁形状的代码

向后兼容的**加列 / 加索引**用 patch；需要应用侧配合升级（改列类型、删列、
改语义）用 minor 或 major。

## Adding a Migration

1. 在 `migrations/` 下加下一个编号的 SQL，例如 `0001_add_retention_indexes.sql`。
2. SQL **尽量幂等**：
   - `create table if not exists`
   - `create index if not exists`
   - 先加后删，别一上来就 destructive
3. 在迁移里往 `workflow_schema_migrations` 插一行：

   ```sql
   insert into "workflow_schema_migrations" (
     migration_id, package_name, package_version, applied_at
   )
   values (
     '0001_add_retention_indexes',
     'aa_workflow_store_sqlx_postgres',
     null,
     (extract(epoch from now()) * 1000)::bigint
   )
   on conflict (migration_id) do nothing;
   ```

4. 把新迁移加进 `src/migrations.rs` 的 `MIGRATIONS` 数组（顺序即执行顺序）。
   `INITIAL_MIGRATION_SQL` 用 `include_str!` 嵌入，保证与 SQL 产物一致。

## Compatibility Rules

- runtime 与 host adapter **假定 schema 已存在**。
- 生产部署应先应用 package-owned SQL 迁移，再滚动新版本 adapter。
- `ensure_schema()` 只用于**测试 / 本地 demo / 显式 bootstrap 脚本**。
  **不要在 request handler、scheduled sweep、cron tick 里调**——它是建表 DDL，
  每次请求跑一遍没有意义且会拖慢热路径。

## Verification

```bash
# 需要真实 Postgres（见 tests/*.rs 的 DATABASE_URL 约定）
createdb -T template0 --lc-collate=C --lc-ctype=C workflow_test   # 首次
cargo test -p aa_workflow_store_sqlx_postgres
```

测试跑在**每条用例一个独立 schema**（`test_<uuid>` / `e2e_<uuid>`）上，
跑完 drop——不会污染 `public`，也不会互相干扰。

## 与上游 schema 的差异

**目前无差异**：`migrations/0000_workflow_store.sql` 逐列对齐上游
`workflow-store-drizzle-postgres/migrations/0000_workflow_store.sql`
（9 张表：`workflow_schema_migrations` / `workflow_runs` / `workflow_run_states` /
`workflow_event_locks` / `workflow_events` / `workflow_timers` /
`workflow_signal_deliveries` / `workflow_schedules` / `workflow_schedule_buckets`）。

上游 SQL 里有几条 `alter table ... add column if not exists`——那是给**已存在的
旧库**补列用的（该迁移在早期版本后又加过 `awaiting`）。我们的初始迁移直接把这些
列写进 `create table`，新库一次到位；**若要对接已有上游库**，需要补上那些 alter。
