-- 0000_workflow_store — aa-workflow 的 Postgres 持久化 schema。
--
-- 逐列对齐上游 `@tanstack/workflow-store-drizzle-postgres` 的
-- `migrations/0000_workflow_store.sql`（见 docs/api/store-adapters.md 的
-- "Default tables" 表）。表名 / 列名保持上游原样，不改成 Rust 侧命名，
-- 这样两边可以对照、也便于将来共享数据。
--
-- 所有语句幂等（`if not exists`），可重复执行。

create table if not exists "workflow_schema_migrations" (
  migration_id text primary key,
  package_name text not null,
  package_version text,
  applied_at bigint not null
);

-- runtime 的 run 执行记录（对齐 WorkflowExecution：含 lease / wake_at）。
create table if not exists "workflow_runs" (
  run_id text primary key,
  workflow_id text not null,
  workflow_version text,
  status text not null,
  input jsonb not null,
  output jsonb,
  error jsonb,
  awaiting jsonb,
  waiting_for jsonb,
  pending_approval jsonb,
  wake_at bigint,
  lease_owner text,
  lease_expires_at bigint,
  created_at bigint not null,
  updated_at bigint not null
);

create index if not exists "workflow_runs_status_idx"
  on "workflow_runs" (status, updated_at);

create index if not exists "workflow_runs_lease_idx"
  on "workflow_runs" (status, lease_expires_at);

-- core 语义的 RunState 信封（对齐 `WorkflowRunStoreAdapterStore` 的
-- load/save_run_state）。与 workflow_runs 分开：前者是 runtime 的执行记录，
-- 后者是 core 的状态信封，两个接口面各自演进。
create table if not exists "workflow_run_states" (
  run_id text primary key,
  workflow_id text not null,
  workflow_version text,
  status text not null,
  input jsonb not null,
  output jsonb,
  error jsonb,
  awaiting jsonb,
  waiting_for jsonb,
  pending_approval jsonb,
  created_at bigint not null,
  updated_at bigint not null
);

-- 每个 run 一行锁：`append_events` 在事务里 `select ... for update` 它，
-- 把同一 run 的并发 append 串行化。这是 CAS 护栏的载体。
create table if not exists "workflow_event_locks" (
  run_id text primary key,
  created_at bigint not null
);

-- append-only 事件日志。`event_index` 从 0 连续，CAS 校验的就是它的长度。
-- `event_type` / `step_id` 冗余出来供索引，不必解析 jsonb 体。
create table if not exists "workflow_events" (
  run_id text not null,
  event_index integer not null,
  event_type text not null,
  step_id text,
  event jsonb not null,
  created_at bigint not null,
  primary key (run_id, event_index)
);

create index if not exists "workflow_events_type_idx"
  on "workflow_events" (run_id, event_type);

-- 到期 timer 索引（sleep 的唤醒）。认领时挂 lease 而非删除，
-- 由投递成功后的 deliver_signal 删除。
create table if not exists "workflow_timers" (
  run_id text not null,
  signal_id text not null,
  workflow_id text not null,
  workflow_version text,
  wake_at bigint not null,
  lease_owner text,
  lease_expires_at bigint,
  primary key (run_id, signal_id)
);

create index if not exists "workflow_timers_due_idx"
  on "workflow_timers" (wake_at, lease_expires_at);

-- signal / approval 投递的幂等键：同 (run_id, signal_id) 重复投递识别为
-- Duplicate。
create table if not exists "workflow_signal_deliveries" (
  run_id text not null,
  signal_id text not null,
  created_at bigint not null,
  primary key (run_id, signal_id)
);

-- schedule 定义（cron / every）。`next_fire_at` 由 host 算好写入。
create table if not exists "workflow_schedules" (
  schedule_id text primary key,
  workflow_id text not null,
  workflow_version text,
  schedule jsonb not null,
  overlap_policy text not null,
  input jsonb,
  next_fire_at bigint,
  enabled boolean not null,
  updated_at bigint not null
);

create index if not exists "workflow_schedules_due_idx"
  on "workflow_schedules" (enabled, next_fire_at);

-- schedule 的已物化时间桶：一个桶对应一次将要/已经触发的 run。
-- 桶的 run_id 由 {workflowId}:{scheduleId}:{bucketId} 推导，天然幂等。
create table if not exists "workflow_schedule_buckets" (
  schedule_id text not null,
  bucket_id text not null,
  workflow_id text not null,
  workflow_version text,
  run_id text not null,
  fire_at bigint not null,
  input jsonb,
  overlap_policy text not null,
  status text not null,
  lease_owner text,
  lease_expires_at bigint,
  started_at bigint,
  primary key (schedule_id, bucket_id)
);

create index if not exists "workflow_schedule_buckets_lease_idx"
  on "workflow_schedule_buckets" (status, fire_at, lease_expires_at);

insert into "workflow_schema_migrations" (
  migration_id,
  package_name,
  package_version,
  applied_at
)
values (
  '0000_workflow_store',
  'aa-workflow-store-sqlx-postgres',
  null,
  (extract(epoch from now()) * 1000)::bigint
)
on conflict (migration_id) do nothing;
