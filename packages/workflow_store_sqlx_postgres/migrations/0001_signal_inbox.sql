-- 耐久信号收件箱（早到信号暂存）。
--
-- 逐字沿用上游 `@tanstack/workflow-store-drizzle-postgres/migrations/0001_signal_inbox.sql`
-- （见 ../../../learn_ls/workflow/packages/workflow-store-drizzle-postgres/migrations/0001_signal_inbox.sql）：
-- 表名 / 列名 / 索引名与上游一致，两边可对照。仅 `package_name` 换成本包。
--
-- 为什么需要这张表：上游的等待点是**按下标定位**消费信号的
-- （`consumeBufferedSignal` 带 `expectedNextIndex`，日志长度不符就抛
-- `LogConflictError`），所以「run 还没走到等待点」的早到信号无处安放，只能落在
-- 这张边表里。本仓 core 的 `find_resume` 是全日志扫描、与下标无关，所以收件箱
-- 对我们是**新增能力**（去重 + 开关），不是必需路径——没有它时行为回落到既有
-- 的日志型收件箱。
create table if not exists "workflow_signal_inbox" (
  sequence bigserial primary key,
  run_id text not null,
  signal_id text not null,
  name text not null,
  step_id text,
  delivery jsonb not null,
  created_at bigint not null,
  unique (run_id, signal_id)
);

create index if not exists "workflow_signal_inbox_pending_idx"
  on "workflow_signal_inbox" (run_id, name, sequence);

-- 与上游的一处**有意差异**：上游这里还有一条
--   create index "workflow_events_signal_id_idx"
--     on workflow_events (run_id, (event->>'signalId'))
--     where event_type = 'SIGNAL_RESOLVED';
-- 我们没有建，两个理由叠加：
--   1. 字段大小写：`WorkflowEvent` 的 `rename_all` 只作用于**类型 tag**
--      （`#[serde(tag="type", rename_all="SCREAMING_SNAKE_CASE")]`），字段名未改，
--      序列化出来是 `signal_id`，`event->>'signalId'` 恒为 NULL；
--   2. 查询用不到：投递去重走 `workflow_signal_deliveries`，没有 SQL 在
--      `workflow_events` 上按 signalId 查。
-- 注意事件分类**已**与上游对齐（有 `SIGNAL_RESOLVED`），所以这不是"缺事件类型"，
-- 而是上面两个具体原因。差异同样记在 SCHEMA_MIGRATIONS.md。

insert into "workflow_schema_migrations" (migration_id, package_name, package_version, applied_at)
  values ('0001_signal_inbox', 'aa_workflow_store_sqlx_postgres', null, cast(extract(epoch from clock_timestamp()) * 1000 as bigint))
  on conflict (migration_id) do nothing;
