//! `SqlxPostgresStore`：Postgres 上的 [`WorkflowExecutionStore`] 实现。
//!
//! 语义逐方法对齐 `workflow-runtime` 的 `InMemoryExecutionStore`
//! （`in_memory_store.rs`），SQL 逐方法对照上游 TS 的
//! `workflow-store-drizzle-postgres/src/store.ts`。
//!
//! # 与内存实现的唯一结构差异：CAS 靠**行锁**而非**内存锁**
//!
//! `append_events` 的「当前日志长度 == expected_next_index」这条护栏，在内存
//! 里靠 `Mutex` 串行化同一 run 的 append；在 Postgres 里靠
//! `workflow_event_locks` 那一行：事务内先 `insert ... on conflict do nothing`
//! 保证锁行存在，再 `select ... for update` 把它锁住——**同一 run 的并发
//! append 由此排队**，然后才读 `count(*)` 比对。这是整个 store 的立身之本，
//! 也是「并发 step 不会写坏日志」的保证（见 `AGENTS.md` 的 adapter 清单）。

use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use workflow_core::{DeleteReason, RunState, StoreError, WaitForState};
use workflow_runtime::run_store_adapter::{WorkflowExecutionStore, WorkflowRunStoreAdapterStore};
use workflow_runtime::types::*;

/// Postgres 上的 durable 执行存储。
///
/// # 同步 trait + async sqlx 的桥
///
/// [`WorkflowExecutionStore`] 的 19 个方法**都是同步的**（对照
/// `workflow-runtime/src/run_store_adapter.rs:124/230`）。这不是疏漏：同步签名让
/// 契约套件（`store_contract.rs`）与 core 的 `RunStore`（同样同步）都能直接调用，
/// 不需要先建 runtime。上游 TS 侧是 `async`——那是 JS 的语言特性，不是契约差异。
///
/// 而 sqlx 是 async 的，所以内部要把 future 驱动到完成。见 [`Self::block`] 的两条
/// 路径与它们各自的前提。
///
/// 克隆廉价（内部是连接池的 `Arc`）。
#[derive(Clone)]
pub struct SqlxPostgresStore {
    pool: PgPool,
}

impl SqlxPostgresStore {
    /// 构造：**不**自建 runtime。
    ///
    /// 同步桥 [`Self::block`] 优先用调用方所处的 runtime（`block_in_place`）；
    /// 只有在**没有** runtime 时（`#[test]` / CLI）才临时建一个。
    ///
    /// 这样也避免了「在异步上下文里 drop runtime」——`new` 不再持有 runtime，
    /// 就不会在 drop 时恐慌。
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 连接 `database_url` 并建池。
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(database_url)
            .await?;
        Ok(Self::new(pool))
    }

    /// 底层连接池。
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// 把任意 future 驱动到完成（本 store 的同步/异步桥）。
    ///
    /// # 两条路径，取决于调用方在不在 runtime 里
    ///
    /// 1. **在 runtime 里** → `block_in_place` + 当前 handle。
    ///    这是 async 引擎同步调 store 的常态（core 的 `run_workflow` 是 async，
    ///    内部却直接同步调 `RunStore`）。**要求调用方 runtime 是 multi-thread**
    ///    —— `block_in_place` 在 current-thread runtime 上会 panic。
    /// 2. **不在 runtime 里**（`#[test]`、CLI）→ 用自建的专用 runtime。
    ///
    /// # 两条都用不了的组合（实测结论）
    ///
    /// - 只用「自建 runtime + `block_on`」：在 async 引擎里撞
    ///   *Cannot start a runtime from within a runtime*（store 的 `block_on`
    ///   必然发生在某个 runtime 上下文内）。
    /// - 只用 `block_in_place`：没有 runtime 时无处可 block。
    ///
    /// ⚠️ `block_in_place` 会占用一个 worker 线程，而 sqlx 连接池回收连接依赖
    /// runtime 继续驱动后台任务。所以调用方 runtime 的 worker 数要 **≥2**，
    /// 否则会自己把自己卡住（症状是 `pool timed out while waiting for an
    /// open connection`）。
    fn block<F: std::future::Future>(&self, fut: F) -> F::Output {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return tokio::task::block_in_place(|| handle.block_on(fut));
        }
        // 没有 ambient runtime（`#[test]` / CLI）：临时建一个。建完即 drop，
        // 此时本来就不在异步上下文里，drop 是安全的。
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("建临时 runtime 失败")
            .block_on(fut)
    }

    /// 在阻塞线程池里跑一个使用本 store 的闭包，供 async 上下文调用。
    ///
    /// ```ignore
    /// let store = SqlxPostgresStore::new(pool);
    /// let n = store.blocking(move |s| s.list_runs(args)).await?;
    /// ```
    pub async fn blocking<T, F>(&self, f: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(SqlxPostgresStore) -> anyhow::Result<T> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || f(store)).await?
    }

    /// 建表（幂等）。见 [`crate::migrations::ensure_schema`] 关于调用时机的说明。
    pub async fn ensure_schema(&self) -> anyhow::Result<()> {
        crate::migrations::ensure_schema(&self.pool).await
    }
}

// ============================================================
// 行 → 类型的映射
// ============================================================

/// `WorkflowExecutionStatus` ↔ 数据库里的 `status` 文本。
///
/// 直接用 serde 的 snake_case 表示（`queued` / `running` / …），与上游 TS
/// 写入的字符串一致。
fn status_to_str(s: WorkflowExecutionStatus) -> &'static str {
    match s {
        WorkflowExecutionStatus::Queued => "queued",
        WorkflowExecutionStatus::Running => "running",
        WorkflowExecutionStatus::Paused => "paused",
        WorkflowExecutionStatus::Finished => "finished",
        WorkflowExecutionStatus::Errored => "errored",
        WorkflowExecutionStatus::Aborted => "aborted",
    }
}

fn status_from_str(s: &str) -> anyhow::Result<WorkflowExecutionStatus> {
    Ok(match s {
        "queued" => WorkflowExecutionStatus::Queued,
        "running" => WorkflowExecutionStatus::Running,
        "paused" => WorkflowExecutionStatus::Paused,
        "finished" => WorkflowExecutionStatus::Finished,
        "errored" => WorkflowExecutionStatus::Errored,
        "aborted" => WorkflowExecutionStatus::Aborted,
        other => anyhow::bail!("未知的 run status: {other}"),
    })
}

/// `RunStatus`（core）↔ 数据库文本（存 `workflow_run_states.status`）。
fn run_status_to_str(s: workflow_core::RunStatus) -> &'static str {
    match s {
        workflow_core::RunStatus::Running => "running",
        workflow_core::RunStatus::Paused => "paused",
        workflow_core::RunStatus::Finished => "finished",
        workflow_core::RunStatus::Errored => "errored",
        workflow_core::RunStatus::Aborted => "aborted",
    }
}

fn run_status_from_str(s: &str) -> anyhow::Result<workflow_core::RunStatus> {
    Ok(match s {
        "running" => workflow_core::RunStatus::Running,
        "paused" => workflow_core::RunStatus::Paused,
        "finished" => workflow_core::RunStatus::Finished,
        "errored" => workflow_core::RunStatus::Errored,
        "aborted" => workflow_core::RunStatus::Aborted,
        other => anyhow::bail!("未知的 RunStatus: {other}"),
    })
}

fn decode_json(v: serde_json::Value) -> serde_json::Value {
    v
}

/// jsonb 列里可能是个 JSON `null`（而不是 SQL NULL），统一收敛成 `None`。
fn decode_opt_json_nullable(v: Option<serde_json::Value>) -> Option<serde_json::Value> {
    match v {
        Some(serde_json::Value::Null) | None => None,
        other => other,
    }
}

/// 把 `WorkflowExecution` 从一行里读出来。
fn run_from_row(row: &sqlx::postgres::PgRow) -> anyhow::Result<WorkflowExecution> {
    let status: String = row.try_get("status")?;
    let lease_owner: Option<String> = row.try_get("lease_owner")?;
    let lease_expires_at: Option<i64> = row.try_get("lease_expires_at")?;
    let lease = match (lease_owner, lease_expires_at) {
        (Some(owner), Some(expires_at)) => Some(WorkflowLease { owner, expires_at }),
        _ => None,
    };
    let waiting_for: Option<serde_json::Value> = row.try_get("waiting_for")?;
    let pending_approval: Option<serde_json::Value> = row.try_get("pending_approval")?;
    let error: Option<serde_json::Value> = row.try_get("error")?;
    Ok(WorkflowExecution {
        run_id: row.try_get("run_id")?,
        workflow_id: row.try_get("workflow_id")?,
        workflow_version: row.try_get("workflow_version")?,
        status: status_from_str(&status)?,
        input: decode_json(row.try_get("input")?),
        output: decode_opt_json_nullable(row.try_get("output")?),
        error: error
            .and_then(|v| serde_json::from_value(v).ok())
            .filter(|_| true),
        waiting_for: waiting_for.and_then(|v| serde_json::from_value(v).ok()),
        pending_approval: pending_approval.and_then(|v| serde_json::from_value(v).ok()),
        wake_at: row.try_get("wake_at")?,
        lease,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// 把 `RunState` 从一行里读出来。
fn run_state_from_row(row: &sqlx::postgres::PgRow) -> anyhow::Result<RunState> {
    let status: String = row.try_get("status")?;
    let error: Option<serde_json::Value> = row.try_get("error")?;
    let waiting_for: Option<serde_json::Value> = row.try_get("waiting_for")?;
    let pending_approval: Option<serde_json::Value> = row.try_get("pending_approval")?;
    Ok(RunState {
        run_id: row.try_get("run_id")?,
        workflow_id: row.try_get("workflow_id")?,
        workflow_version: row.try_get("workflow_version")?,
        status: run_status_from_str(&status)?,
        input: decode_json(row.try_get("input")?),
        output: decode_opt_json_nullable(row.try_get("output")?),
        error: error.and_then(|v| serde_json::from_value(v).ok()),
        waiting_for: waiting_for.and_then(|v| serde_json::from_value(v).ok()),
        pending_approval: pending_approval.and_then(|v| serde_json::from_value(v).ok()),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// 把一行事件读成 `StoredWorkflowEvent`（对齐 `store_event` 的信封）。
fn stored_event_from_row(row: &sqlx::postgres::PgRow) -> anyhow::Result<StoredWorkflowEvent> {
    let event_json: serde_json::Value = row.try_get("event")?;
    let index: i32 = row.try_get("event_index")?;
    Ok(StoredWorkflowEvent {
        run_id: row.try_get("run_id")?,
        event_index: index as u64,
        event_type: row.try_get("event_type")?,
        step_id: row.try_get("step_id")?,
        event: serde_json::from_value(event_json)?,
        created_at: row.try_get("created_at")?,
    })
}

/// lease 是否可认领：无 lease / 是自己的 / 已过期（对齐 `can_claim`）。
fn can_claim(existing: Option<&WorkflowLease>, owner: &LeaseOwner, now: i64) -> bool {
    match existing {
        None => true,
        Some(l) => l.owner == *owner || l.expires_at <= now,
    }
}

fn new_lease(owner: LeaseOwner, lease_ms: i64, now: i64) -> WorkflowLease {
    WorkflowLease {
        owner,
        expires_at: now + lease_ms,
    }
}

/// run 是否在等这个信号（对齐 `is_run_waiting_for_signal`）。
fn is_run_waiting_for_signal(run: &WorkflowExecution, delivery: &SignalDelivery) -> bool {
    match &run.waiting_for {
        Some(w) => {
            w.signal_name == delivery.name
                && (delivery.step_id.is_none()
                    || w.step_id.is_none()
                    || w.step_id == delivery.step_id)
        }
        None => false,
    }
}

fn is_run_waiting_for_approval(run: &WorkflowExecution, approval: &ApprovalResult) -> bool {
    run.pending_approval
        .as_ref()
        .map(|p| p.approval_id == approval.approval_id)
        .unwrap_or(false)
}

fn to_run_summary(run: &WorkflowExecution) -> RunSummary {
    RunSummary {
        run_id: run.run_id.clone(),
        workflow_id: run.workflow_id.clone(),
        workflow_version: run.workflow_version.clone(),
        status: run.status,
        waiting_for: run.waiting_for.clone(),
        pending_approval: run.pending_approval.clone(),
        wake_at: run.wake_at,
        created_at: run.created_at,
        updated_at: run.updated_at,
    }
}

impl SqlxPostgresStore {
    async fn load_run_row(
        &self,
        executor: impl sqlx::PgExecutor<'_>,
        run_id: &str,
    ) -> anyhow::Result<Option<WorkflowExecution>> {
        let row = sqlx::query(
            "select run_id, workflow_id, workflow_version, status, input, output, error, \
                    waiting_for, pending_approval, wake_at, lease_owner, lease_expires_at, \
                    created_at, updated_at \
             from workflow_runs where run_id = $1",
        )
        .bind(run_id)
        .fetch_optional(executor)
        .await?;
        row.as_ref().map(run_from_row).transpose()
    }

    /// 读一行 `workflow_runs` 并加行锁（`for update`），供 claim / mark 等
    /// 读-改-写路径使用。
    async fn lock_run_row(
        tx: &mut Transaction<'_, Postgres>,
        run_id: &str,
    ) -> anyhow::Result<Option<WorkflowExecution>> {
        let row = sqlx::query(
            "select run_id, workflow_id, workflow_version, status, input, output, error, \
                    waiting_for, pending_approval, wake_at, lease_owner, lease_expires_at, \
                    created_at, updated_at \
             from workflow_runs where run_id = $1 for update",
        )
        .bind(run_id)
        .fetch_optional(&mut **tx)
        .await?;
        row.as_ref().map(run_from_row).transpose()
    }
}

// ============================================================
// 基础面：WorkflowRunStoreAdapterStore
// ============================================================

impl WorkflowRunStoreAdapterStore for SqlxPostgresStore {
    fn load_run_state(&self, run_id: &str) -> anyhow::Result<Option<RunState>> {
        self.block(async move {
        let row = sqlx::query(
            "select run_id, workflow_id, workflow_version, status, input, output, error, \
                    waiting_for, pending_approval, created_at, updated_at \
             from workflow_run_states where run_id = $1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(run_state_from_row).transpose()
        })
    }

    fn save_run_state(&self, args: SaveRunStateArgs) -> anyhow::Result<()> {
        self.block(async move {
        let state = &args.state;
        let input = serde_json::to_value(&state.input)?;
        let output = serde_json::to_value(state.output.as_ref())?;
        let error = serde_json::to_value(state.error.as_ref())?;
        let waiting_for = serde_json::to_value(state.waiting_for.as_ref())?;
        let pending_approval = serde_json::to_value(state.pending_approval.as_ref())?;

        let mut tx = self.pool.begin().await?;

        let exists: Option<String> =
            sqlx::query_scalar("select run_id from workflow_runs where run_id = $1 for update")
                .bind(&state.run_id)
                .fetch_optional(&mut *tx)
                .await?;
        let lease: Option<WorkflowLease> = match &exists {
            Some(_) => {
                let row = sqlx::query(
                    "select lease_owner, lease_expires_at from workflow_runs where run_id = $1",
                )
                .bind(&state.run_id)
                .fetch_one(&mut *tx)
                .await?;
                let owner: Option<String> = row.try_get("lease_owner")?;
                let expires: Option<i64> = row.try_get("lease_expires_at")?;
                match (owner, expires) {
                    (Some(owner), Some(expires_at)) => {
                        Some(WorkflowLease { owner, expires_at })
                    }
                    _ => None,
                }
            }
            None => None,
        };

        sqlx::query(
            "insert into workflow_run_states (run_id, workflow_id, workflow_version, status, \
                input, output, error, waiting_for, pending_approval, created_at, updated_at) \
             values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
             on conflict (run_id) do update set \
                workflow_id = excluded.workflow_id, \
                workflow_version = excluded.workflow_version, \
                status = excluded.status, \
                input = excluded.input, \
                output = excluded.output, \
                error = excluded.error, \
                waiting_for = excluded.waiting_for, \
                pending_approval = excluded.pending_approval, \
                created_at = excluded.created_at, \
                updated_at = excluded.updated_at",
        )
        .bind(&state.run_id)
        .bind(&state.workflow_id)
        .bind(&state.workflow_version)
        .bind(run_status_to_str(state.status))
        .bind(input)
        .bind(output)
        .bind(error)
        .bind(waiting_for)
        .bind(pending_approval)
        .bind(state.created_at)
        .bind(state.updated_at)
        .execute(&mut *tx)
        .await?;

        // 同步投影到 workflow_runs（对齐 `execution_from_run_state`）：
        // wake_at 从 `__timer` 等待推导，lease 原样保留。
        let wake_at = state
            .waiting_for
            .as_ref()
            .filter(|w| w.signal_name == "__timer")
            .and_then(|w| w.deadline);
        let exec_status: WorkflowExecutionStatus = state.status.into();
        sqlx::query(
            "insert into workflow_runs (run_id, workflow_id, workflow_version, status, input, \
                output, error, waiting_for, pending_approval, wake_at, lease_owner, \
                lease_expires_at, created_at, updated_at) \
             values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) \
             on conflict (run_id) do update set \
                workflow_id = excluded.workflow_id, \
                workflow_version = excluded.workflow_version, \
                status = excluded.status, \
                input = excluded.input, \
                output = excluded.output, \
                error = excluded.error, \
                waiting_for = excluded.waiting_for, \
                pending_approval = excluded.pending_approval, \
                wake_at = excluded.wake_at, \
                updated_at = excluded.updated_at",
        )
        .bind(&state.run_id)
        .bind(&state.workflow_id)
        .bind(&state.workflow_version)
        .bind(status_to_str(exec_status))
        .bind(serde_json::to_value(&state.input)?)
        .bind(serde_json::to_value(state.output.as_ref())?)
        .bind(serde_json::to_value(state.error.as_ref())?)
        .bind(serde_json::to_value(state.waiting_for.as_ref())?)
        .bind(serde_json::to_value(state.pending_approval.as_ref())?)
        .bind(wake_at)
        .bind(lease.as_ref().map(|l| l.owner.clone()))
        .bind(lease.as_ref().map(|l| l.expires_at))
        .bind(state.created_at)
        .bind(state.updated_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
        })
    }

    fn delete_run(&self, run_id: &str, _reason: DeleteReason) -> anyhow::Result<()> {
        self.block(async move {
        let mut tx = self.pool.begin().await?;
        for table in [
            "workflow_run_states",
            "workflow_event_locks",
            "workflow_signal_deliveries",
            "workflow_timers",
            "workflow_events",
            "workflow_runs",
        ] {
            sqlx::query(&format!("delete from {table} where run_id = $1"))
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
        })
    }

    fn append_events(&self, args: AppendEventsArgs) -> anyhow::Result<AppendEventsResult> {
        self.block(async move {
        let mut tx = self.pool.begin().await?;

        // 1) 保证锁行存在（幂等）。
        sqlx::query(
            "insert into workflow_event_locks (run_id, created_at) values ($1, $2) \
             on conflict (run_id) do nothing",
        )
        .bind(&args.run_id)
        .bind(now_ms())
        .execute(&mut *tx)
        .await?;

        // 2) 锁住它——同一 run 的并发 append 在此排队（内存实现的 Mutex 对等物）。
        sqlx::query("select run_id from workflow_event_locks where run_id = $1 for update")
            .bind(&args.run_id)
            .fetch_one(&mut *tx)
            .await?;

        // 3) CAS 校验。
        let count: i64 =
            sqlx::query_scalar("select count(*) from workflow_events where run_id = $1")
                .bind(&args.run_id)
                .fetch_one(&mut *tx)
                .await?;
        let actual = count as u64;
        if actual != args.expected_next_index {
            // 事务随 drop 回滚。
            return Err(anyhow::Error::new(StoreError::Conflict {
                run_id: args.run_id.clone(),
                expected: args.expected_next_index as usize,
                actual: actual as usize,
            }));
        }

        // 4) 逐条写入（索引连续）。
        let mut next_index = args.expected_next_index;
        for event in &args.events {
            let event_type = event.type_name().to_string();
            let step_id = event.step_id().map(str::to_string);
            sqlx::query(
                "insert into workflow_events (run_id, event_index, event_type, step_id, event, \
                    created_at) values ($1,$2,$3,$4,$5,$6)",
            )
            .bind(&args.run_id)
            .bind(next_index as i32)
            .bind(event_type)
            .bind(step_id)
            .bind(serde_json::to_value(event)?)
            .bind(event.ts())
            .execute(&mut *tx)
            .await?;
            next_index += 1;
        }

        tx.commit().await?;
        Ok(AppendEventsResult { next_index })
        })
    }

    fn read_events(
        &self,
        args: ReadEventsArgs,
    ) -> anyhow::Result<Vec<StoredWorkflowEvent>> {
        self.block(async move {
        let from = args.from_index.unwrap_or(0) as i32;
        let rows = sqlx::query(
            "select run_id, event_index, event_type, step_id, event, created_at \
             from workflow_events where run_id = $1 and event_index >= $2 \
             order by event_index asc",
        )
        .bind(&args.run_id)
        .bind(from)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(stored_event_from_row).collect()
        })
    }

    // subscribe_events：sqlx 无内建 pub/sub，用 trait 的默认实现（返回 None），
    // 调用方退化为轮询 read_events。与上游 drizzle 版一致（它也没有 subscribeEvents）。
}

/// 当前毫秒时间戳（对齐上游 `Date.now()`）。
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 占位：`Arc<dyn WorkflowExecutionStore>` 的便捷构造。
pub fn sqlx_postgres_store(pool: PgPool) -> Arc<dyn WorkflowExecutionStore> {
    Arc::new(SqlxPostgresStore::new(pool))
}

// ============================================================
// 扩展面：WorkflowExecutionStore
// ============================================================

impl WorkflowExecutionStore for SqlxPostgresStore {
    fn create_run(&self, args: CreateRunArgs) -> anyhow::Result<CreateRunResult> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            let inserted = sqlx::query(
                "insert into workflow_runs (run_id, workflow_id, workflow_version, status, input, \
                    created_at, updated_at) \
                 values ($1,$2,$3,'queued',$4,$5,$6) \
                 on conflict (run_id) do nothing \
                 returning *",
            )
            .bind(&args.run_id)
            .bind(&args.workflow_id)
            .bind(&args.workflow_version)
            .bind(serde_json::to_value(&args.input)?)
            .bind(args.now)
            .bind(args.now)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = inserted {
                let run = run_from_row(&row)?;
                tx.commit().await?;
                return Ok(CreateRunResult::Created { run });
            }

            // 已存在：幂等返回（不再报错）。
            let existing = Self::lock_run_row(&mut tx, &args.run_id).await?;
            tx.commit().await?;
            match existing {
                Some(run) => Ok(CreateRunResult::Existing { run }),
                None => anyhow::bail!("run {} 既未插入也未读出", args.run_id),
            }
        })
    }

    fn load_run(&self, run_id: &str) -> anyhow::Result<Option<WorkflowExecution>> {
        self.block(async move {
            self.load_run_row(&self.pool, run_id).await
        })
    }

    fn load_execution(&self, run_id: &str) -> anyhow::Result<Option<LoadedExecution>> {
        // 这两步都是同步方法（各自 block），**不要**放进 async 块里——否则会嵌套 block_on。
        let Some(run) = self.load_run(run_id)? else {
            return Ok(None);
        };
        let events = self.read_events(ReadEventsArgs {
            run_id: run_id.to_string(),
            from_index: None,
        })?;
        Ok(Some(LoadedExecution { run, events }))
    }

    fn mark_run_paused(&self, args: MarkRunPausedArgs) -> anyhow::Result<()> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "update workflow_runs set status = 'paused', waiting_for = $2, \
                    pending_approval = $3, wake_at = $4, lease_owner = null, \
                    lease_expires_at = null, updated_at = $5 where run_id = $1",
            )
            .bind(&args.run_id)
            .bind(serde_json::to_value(args.waiting_for.as_ref())?)
            .bind(serde_json::to_value(args.pending_approval.as_ref())?)
            .bind(args.wake_at)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn mark_run_finished(&self, args: MarkRunFinishedArgs) -> anyhow::Result<()> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "update workflow_runs set status = 'finished', output = $2, waiting_for = null, \
                    pending_approval = null, wake_at = null, lease_owner = null, \
                    lease_expires_at = null, updated_at = $3 where run_id = $1",
            )
            .bind(&args.run_id)
            .bind(serde_json::to_value(&args.output)?)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn mark_run_errored(&self, args: MarkRunErroredArgs) -> anyhow::Result<()> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "update workflow_runs set status = 'errored', error = $2, waiting_for = null, \
                    pending_approval = null, wake_at = null, lease_owner = null, \
                    lease_expires_at = null, updated_at = $3 where run_id = $1",
            )
            .bind(&args.run_id)
            .bind(serde_json::to_value(&args.error)?)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn claim_run(&self, args: ClaimRunArgs) -> anyhow::Result<ClaimRunResult> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            let Some(existing) = Self::lock_run_row(&mut tx, &args.run_id).await? else {
                tx.commit().await?;
                return Ok(ClaimRunResult::NotFound);
            };
            if existing.status.is_terminal()
                || !can_claim(existing.lease.as_ref(), &args.lease_owner, args.now)
            {
                tx.commit().await?;
                return Ok(ClaimRunResult::NotClaimable { run: existing });
            }
            let lease = new_lease(args.lease_owner, args.lease_ms, args.now);
            sqlx::query(
                "update workflow_runs set status = 'running', lease_owner = $2, \
                    lease_expires_at = $3, updated_at = $4 where run_id = $1",
            )
            .bind(&args.run_id)
            .bind(&lease.owner)
            .bind(lease.expires_at)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            let row = sqlx::query(
                "select run_id, workflow_id, workflow_version, status, input, output, error, \
                        waiting_for, pending_approval, wake_at, lease_owner, lease_expires_at, \
                        created_at, updated_at from workflow_runs where run_id = $1",
            )
            .bind(&args.run_id)
            .fetch_one(&mut *tx)
            .await?;
            let run = run_from_row(&row)?;
            tx.commit().await?;
            Ok(ClaimRunResult::Claimed { run })
        })
    }

    fn heartbeat_run_lease(&self, args: HeartbeatRunLeaseArgs) -> anyhow::Result<()> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            // 只有持有者能续租（对齐内存实现：owner 不等则什么都不做）。
            sqlx::query(
                "update workflow_runs set lease_expires_at = $3, updated_at = $4 \
                 where run_id = $1 and lease_owner = $2",
            )
            .bind(&args.run_id)
            .bind(&args.lease_owner)
            .bind(args.now + args.lease_ms)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn release_run_lease(&self, args: ReleaseRunLeaseArgs) -> anyhow::Result<()> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "update workflow_runs set lease_owner = null, lease_expires_at = null \
                 where run_id = $1 and lease_owner = $2",
            )
            .bind(&args.run_id)
            .bind(&args.lease_owner)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn claim_stale_runs(
        &self,
        args: ClaimStaleRunsArgs,
    ) -> anyhow::Result<Vec<RunClaim>> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            // 只扫 Running 且 lease 已过期（对齐内存实现）。
            let rows = sqlx::query(
                "select run_id, workflow_id, workflow_version, status, input, output, error, \
                        waiting_for, pending_approval, wake_at, lease_owner, lease_expires_at, \
                        created_at, updated_at \
                 from workflow_runs \
                 where status = 'running' and lease_expires_at is not null and lease_expires_at <= $1 \
                 order by updated_at asc \
                 limit $2 \
                 for update skip locked",
            )
            .bind(args.now)
            .bind(args.limit as i64)
            .fetch_all(&mut *tx)
            .await?;

            let mut claims = Vec::new();
            for row in rows {
                let run_id: String = row.try_get("run_id")?;
                let lease = new_lease(args.lease_owner.clone(), args.lease_ms, args.now);
                sqlx::query(
                    "update workflow_runs set lease_owner = $2, lease_expires_at = $3, \
                        updated_at = $4 where run_id = $1",
                )
                .bind(&run_id)
                .bind(&lease.owner)
                .bind(lease.expires_at)
                .bind(args.now)
                .execute(&mut *tx)
                .await?;
                let updated = sqlx::query(
                    "select run_id, workflow_id, workflow_version, status, input, output, error, \
                            waiting_for, pending_approval, wake_at, lease_owner, lease_expires_at, \
                            created_at, updated_at from workflow_runs where run_id = $1",
                )
                .bind(&run_id)
                .fetch_one(&mut *tx)
                .await?;
                claims.push(RunClaim {
                    run: run_from_row(&updated)?,
                    lease,
                });
            }
            tx.commit().await?;
            Ok(claims)
        })
    }

    fn schedule_timer(&self, args: ScheduleTimerArgs) -> anyhow::Result<()> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "insert into workflow_timers (run_id, signal_id, workflow_id, workflow_version, \
                    wake_at, lease_owner, lease_expires_at) values ($1,$2,$3,$4,$5,null,null) \
                 on conflict (run_id, signal_id) do update set \
                    workflow_id = excluded.workflow_id, \
                    workflow_version = excluded.workflow_version, \
                    wake_at = excluded.wake_at, \
                    lease_owner = null, \
                    lease_expires_at = null",
            )
            .bind(&args.run_id)
            .bind(&args.signal_id)
            .bind(&args.workflow_id)
            .bind(&args.workflow_version)
            .bind(args.wake_at)
            .execute(&mut *tx)
            .await?;
            sqlx::query("update workflow_runs set wake_at = $2, updated_at = $3 where run_id = $1")
                .bind(&args.run_id)
                .bind(args.wake_at)
                .bind(args.now)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    /// 认领 = 挂 lease，**不删**。删除发生在 `deliver_signal`。
    fn claim_due_timers(
        &self,
        args: ClaimDueTimersArgs,
    ) -> anyhow::Result<Vec<TimerWakeup>> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            let rows = sqlx::query(
                "select run_id, signal_id, workflow_id, workflow_version, wake_at, lease_owner, \
                        lease_expires_at \
                 from workflow_timers \
                 where wake_at <= $1 \
                   and (lease_expires_at is null or lease_owner = $2 or lease_expires_at <= $1) \
                 order by wake_at asc \
                 limit $3 \
                 for update skip locked",
            )
            .bind(args.now)
            .bind(&args.lease_owner)
            .bind(args.limit as i64)
            .fetch_all(&mut *tx)
            .await?;

            let mut due = Vec::new();
            for row in rows {
                let run_id: String = row.try_get("run_id")?;
                let signal_id: String = row.try_get("signal_id")?;
                sqlx::query(
                    "update workflow_timers set lease_owner = $3, lease_expires_at = $4 \
                     where run_id = $1 and signal_id = $2",
                )
                .bind(&run_id)
                .bind(&signal_id)
                .bind(&args.lease_owner)
                .bind(args.now + args.lease_ms)
                .execute(&mut *tx)
                .await?;
                due.push(TimerWakeup {
                    run_id,
                    signal_id,
                    workflow_id: row.try_get("workflow_id")?,
                    workflow_version: row.try_get("workflow_version")?,
                    wake_at: row.try_get("wake_at")?,
                });
            }
            tx.commit().await?;
            Ok(due)
        })
    }

    fn deliver_signal(
        &self,
        args: DeliverSignalArgs,
    ) -> anyhow::Result<DeliverSignalResult> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            let Some(run) = Self::lock_run_row(&mut tx, &args.run_id).await? else {
                tx.commit().await?;
                return Ok(DeliverSignalResult::NotFound);
            };

            let already: Option<String> = sqlx::query_scalar(
                "select signal_id from workflow_signal_deliveries \
                 where run_id = $1 and signal_id = $2",
            )
            .bind(&args.run_id)
            .bind(&args.delivery.signal_id)
            .fetch_optional(&mut *tx)
            .await?;
            if already.is_some() {
                tx.commit().await?;
                return Ok(DeliverSignalResult::Duplicate { run });
            }
            if !is_run_waiting_for_signal(&run, &args.delivery) {
                tx.commit().await?;
                return Ok(DeliverSignalResult::NotWaiting { run });
            }

            sqlx::query(
                "insert into workflow_signal_deliveries (run_id, signal_id, created_at) \
                 values ($1,$2,$3) on conflict do nothing",
            )
            .bind(&args.run_id)
            .bind(&args.delivery.signal_id)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            // 信号已投，对应 timer 不再需要。
            sqlx::query("delete from workflow_timers where run_id = $1 and signal_id = $2")
                .bind(&args.run_id)
                .bind(&args.delivery.signal_id)
                .execute(&mut *tx)
                .await?;
            // 回到 Queued：待认领再驱一次处理 payload。
            sqlx::query(
                "update workflow_runs set status = 'queued', waiting_for = null, \
                    pending_approval = null, wake_at = null, updated_at = $2 where run_id = $1",
            )
            .bind(&args.run_id)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            let row = sqlx::query(
                "select run_id, workflow_id, workflow_version, status, input, output, error, \
                        waiting_for, pending_approval, wake_at, lease_owner, lease_expires_at, \
                        created_at, updated_at from workflow_runs where run_id = $1",
            )
            .bind(&args.run_id)
            .fetch_one(&mut *tx)
            .await?;
            let run = run_from_row(&row)?;
            tx.commit().await?;
            Ok(DeliverSignalResult::Delivered { run })
        })
    }

    fn deliver_approval(
        &self,
        args: DeliverApprovalArgs,
    ) -> anyhow::Result<DeliverApprovalResult> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            let Some(run) = Self::lock_run_row(&mut tx, &args.run_id).await? else {
                tx.commit().await?;
                return Ok(DeliverApprovalResult::NotFound);
            };

            // approval 的幂等键与 signal 共用一张表，用 `approval:` 前缀区分
            // （对齐内存实现的 key 构造）。
            let key = format!("approval:{}", args.approval.approval_id);
            let already: Option<String> = sqlx::query_scalar(
                "select signal_id from workflow_signal_deliveries \
                 where run_id = $1 and signal_id = $2",
            )
            .bind(&args.run_id)
            .bind(&key)
            .fetch_optional(&mut *tx)
            .await?;
            if already.is_some() {
                tx.commit().await?;
                return Ok(DeliverApprovalResult::Duplicate { run });
            }
            if !is_run_waiting_for_approval(&run, &args.approval) {
                tx.commit().await?;
                return Ok(DeliverApprovalResult::NotWaiting { run });
            }

            sqlx::query(
                "insert into workflow_signal_deliveries (run_id, signal_id, created_at) \
                 values ($1,$2,$3) on conflict do nothing",
            )
            .bind(&args.run_id)
            .bind(&key)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "update workflow_runs set status = 'queued', waiting_for = null, \
                    pending_approval = null, wake_at = null, updated_at = $2 where run_id = $1",
            )
            .bind(&args.run_id)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            let row = sqlx::query(
                "select run_id, workflow_id, workflow_version, status, input, output, error, \
                        waiting_for, pending_approval, wake_at, lease_owner, lease_expires_at, \
                        created_at, updated_at from workflow_runs where run_id = $1",
            )
            .bind(&args.run_id)
            .fetch_one(&mut *tx)
            .await?;
            let run = run_from_row(&row)?;
            tx.commit().await?;
            Ok(DeliverApprovalResult::Delivered { run })
        })
    }

    fn upsert_schedule(&self, args: UpsertScheduleArgs) -> anyhow::Result<()> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "insert into workflow_schedules (schedule_id, workflow_id, workflow_version, \
                    schedule, overlap_policy, input, next_fire_at, enabled, updated_at) \
                 values ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
                 on conflict (schedule_id) do update set \
                    workflow_id = excluded.workflow_id, \
                    workflow_version = excluded.workflow_version, \
                    schedule = excluded.schedule, \
                    overlap_policy = excluded.overlap_policy, \
                    input = excluded.input, \
                    next_fire_at = excluded.next_fire_at, \
                    enabled = excluded.enabled, \
                    updated_at = excluded.updated_at",
            )
            .bind(&args.schedule_id)
            .bind(&args.workflow_id)
            .bind(&args.workflow_version)
            .bind(serde_json::to_value(&args.schedule)?)
            .bind(overlap_policy_to_str(args.overlap_policy))
            .bind(serde_json::to_value(args.input.as_ref())?)
            .bind(args.next_fire_at)
            .bind(args.enabled)
            .bind(args.now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    /// 桶按 `next_fire_at` 生成 `bucket_id`（同一天重复 fire 由桶状态去重）。
    fn claim_due_schedule_buckets(
        &self,
        args: ClaimDueScheduleBucketsArgs,
    ) -> anyhow::Result<Vec<ScheduleBucket>> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            let rows = sqlx::query(
                "select schedule_id, workflow_id, workflow_version, schedule, overlap_policy, \
                        input, next_fire_at \
                 from workflow_schedules \
                 where enabled = true and next_fire_at is not null and next_fire_at <= $1 \
                 order by next_fire_at asc \
                 limit $2",
            )
            .bind(args.now)
            .bind(args.limit as i64)
            .fetch_all(&mut *tx)
            .await?;

            let mut due = Vec::new();
            for row in rows {
                let schedule_id: String = row.try_get("schedule_id")?;
                let workflow_id: String = row.try_get("workflow_id")?;
                let workflow_version: Option<String> = row.try_get("workflow_version")?;
                let fire_at: i64 = row.try_get("next_fire_at")?;
                let bucket_id = fire_at.to_string();
                let input: Option<serde_json::Value> = row.try_get("input")?;
                let overlap_policy: String = row.try_get("overlap_policy")?;
                let run_id = format!("{workflow_id}:{schedule_id}:{bucket_id}");

                // 已 started 或仍被别人持有 lease 的桶跳过。
                let existing = sqlx::query(
                    "select status, lease_owner, lease_expires_at from workflow_schedule_buckets \
                     where schedule_id = $1 and bucket_id = $2 for update",
                )
                .bind(&schedule_id)
                .bind(&bucket_id)
                .fetch_optional(&mut *tx)
                .await?;
                if let Some(b) = &existing {
                    let status: String = b.try_get("status")?;
                    if status == "started" {
                        continue;
                    }
                    let owner: Option<String> = b.try_get("lease_owner")?;
                    let expires: Option<i64> = b.try_get("lease_expires_at")?;
                    let lease = match (owner, expires) {
                        (Some(owner), Some(expires_at)) => Some(WorkflowLease { owner, expires_at }),
                        _ => None,
                    };
                    if !can_claim(lease.as_ref(), &args.lease_owner, args.now) {
                        continue;
                    }
                }

                sqlx::query(
                    "insert into workflow_schedule_buckets (schedule_id, bucket_id, workflow_id, \
                        workflow_version, run_id, fire_at, input, overlap_policy, status, \
                        lease_owner, lease_expires_at) \
                     values ($1,$2,$3,$4,$5,$6,$7,$8,'claimed',$9,$10) \
                     on conflict (schedule_id, bucket_id) do update set \
                        run_id = excluded.run_id, \
                        status = 'claimed', \
                        lease_owner = excluded.lease_owner, \
                        lease_expires_at = excluded.lease_expires_at",
                )
                .bind(&schedule_id)
                .bind(&bucket_id)
                .bind(&workflow_id)
                .bind(&workflow_version)
                .bind(&run_id)
                .bind(fire_at)
                .bind(&input)
                .bind(&overlap_policy)
                .bind(&args.lease_owner)
                .bind(args.now + args.lease_ms)
                .execute(&mut *tx)
                .await?;

                due.push(ScheduleBucket {
                    schedule_id,
                    bucket_id,
                    workflow_id,
                    workflow_version,
                    run_id,
                    fire_at,
                    input,
                    overlap_policy: overlap_policy_from_str(&overlap_policy)?,
                });
            }
            tx.commit().await?;
            Ok(due)
        })
    }

    fn mark_schedule_bucket_started(
        &self,
        args: MarkScheduleBucketStartedArgs,
    ) -> anyhow::Result<()> {
        self.block(async move {
            let mut tx = self.pool.begin().await?;
            sqlx::query(
                "update workflow_schedule_buckets set run_id = $3, status = 'started' \
                 where schedule_id = $1 and bucket_id = $2",
            )
            .bind(&args.schedule_id)
            .bind(&args.bucket_id)
            .bind(&args.run_id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    fn list_runs(&self, args: ListRunsArgs) -> anyhow::Result<Vec<RunSummary>> {
        self.block(async move {
            // cursor 是不透明字符串：这里用 `updated_at` 做键集分页（与排序键一致）。
            let cursor: Option<i64> = args.cursor.as_deref().and_then(|c| c.parse().ok());
            let rows = sqlx::query(
                "select run_id, workflow_id, workflow_version, status, input, output, error, \
                        waiting_for, pending_approval, wake_at, lease_owner, lease_expires_at, \
                        created_at, updated_at \
                 from workflow_runs \
                 where ($1::text is null or workflow_id = $1) \
                   and ($2::text is null or status = $2) \
                   and ($3::bigint is null or updated_at < $3) \
                 order by updated_at desc \
                 limit $4",
            )
            .bind(args.workflow_id.as_deref())
            .bind(args.status.map(status_to_str))
            .bind(cursor)
            .bind(args.limit as i64)
            .fetch_all(&self.pool)
            .await?;
            rows.iter()
                .map(|r| run_from_row(r).map(|run| to_run_summary(&run)))
                .collect()
        })
    }

    fn get_run_timeline(&self, run_id: &str) -> anyhow::Result<Option<RunTimeline>> {
        // 同 load_execution：走同步方法，避免嵌套 block_on。
        let Some(run) = self.load_run(run_id)? else {
            return Ok(None);
        };
        let events = self.read_events(ReadEventsArgs {
            run_id: run_id.to_string(),
            from_index: None,
        })?;
        Ok(Some(RunTimeline { run, events }))
    }
}

/// `WorkflowOverlapPolicy` ↔ 数据库文本（用 serde 的 kebab-case 表示）。
fn overlap_policy_to_str(p: WorkflowOverlapPolicy) -> &'static str {
    match p {
        WorkflowOverlapPolicy::Skip => "skip",
        WorkflowOverlapPolicy::Allow => "allow",
        WorkflowOverlapPolicy::BufferOne => "buffer-one",
        WorkflowOverlapPolicy::CancelPrevious => "cancel-previous",
        WorkflowOverlapPolicy::TerminatePrevious => "terminate-previous",
    }
}

fn overlap_policy_from_str(s: &str) -> anyhow::Result<WorkflowOverlapPolicy> {
    Ok(match s {
        "skip" => WorkflowOverlapPolicy::Skip,
        "allow" => WorkflowOverlapPolicy::Allow,
        "buffer-one" => WorkflowOverlapPolicy::BufferOne,
        "cancel-previous" => WorkflowOverlapPolicy::CancelPrevious,
        "terminate-previous" => WorkflowOverlapPolicy::TerminatePrevious,
        other => anyhow::bail!("未知的 overlap policy: {other}"),
    })
}

// 让 `WaitForState` 参与 JSON 序列化（它是 core 的类型，这里仅用于文档完整性）。
#[allow(dead_code)]
fn _assert_wait_for_serializes(w: &WaitForState) -> anyhow::Result<serde_json::Value> {
    Ok(serde_json::to_value(w)?)
}
