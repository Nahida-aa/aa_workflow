//! Workflow 的**执行所有权层**：lease、sweep、timer、schedule。
//!
//! 对齐 TanStack `@tanstack/workflow-runtime`。定位见
//! `docs/runtime-design.md`（决策记录）与 core 的 `run_store` 模块文档
//! （两个 store 契约的对应关系）。
//!
//! # 为什么 core 里没有这一层
//!
//! core 刻意只做 replay + 耐久原语 + 一个「存住就行」的 [`RunStore`](aa_workflow_core::RunStore) 契约
//! （上游原话：*The core engine is intentionally not a scheduler, queue,
//! database adapter, or deployment adapter.*）。「谁来跑、什么时候跑、崩溃了
//! 谁接手」是这一层的职责。
//!
//! # 两种部署形态，同一套抽象
//!
//! | 形态 | 代表 | lease | 有界 sweep | timer 投递 |
//! | ---- | ---- | ----- | ---------- | ---------- |
//! | 常驻进程 | LocalDub 的 server queue | 进程内锁够用 | 不必需 | 引擎自轮询够用 |
//! | 无进程常驻 | serverless / 多 worker | **必需** | **必需** | **必需**（无人常驻） |
//!
//! 抽象按**较强的那一侧**设计：有界 sweep 与 lease 即使常驻形态用不上，
//! 也是接口形状的一部分——省掉它们等于把 serverless 形态排除在外。
//!
//! # 当前状态
//!
//! 契约（[`WorkflowRunStoreAdapterStore`] / [`WorkflowExecutionStore`]）、
//! in-memory 实现、driver（`start_run` / `deliver_signal` / `deliver_approval` /
//! `sweep`）、schedule materializer 均已就位。
//!
//! **尚缺的是第二个 store 实现**——目前只有 [`InMemoryExecutionStore`]，
//! 而它不实现任何真实的并发语义（`claim` 永远成功）。所以 lease / sweep /
//! timer 这些路径虽然在 [`store_contract`] 的规格下通过了，但**没有在真会冲突的
//! 存储上验证过**。D2（lease 位置）等的就是这一步。
//!
//! # 层次
//!
//! ```text
//! aa_workflow_core::RunStore          引擎 replay 用（6 个方法）
//!   ↕ 形状相近但不同（见 run_store_adapter 模块文档）
//! WorkflowRunStoreAdapterStore     本 crate 的存储基础（6 个方法）
//!   └── WorkflowExecutionStore     扩展：lease / timer / schedule / 查询
//! ```
//!
//! 后两个 trait 都在 [`run_store_adapter`] 里（与上游把两个 interface 放在同一个
//! `types.ts` 一致），本模块只做声明与再导出。
//!
//! 新 store 实现要宣告自己兼容，跑 [`store_contract::run_store_contract`]。

pub mod define_runtime;
pub mod in_memory_store;
pub mod run_store_adapter;
pub mod runtime_driver;
pub mod schedule_materializer;
pub mod store_contract;
pub mod types;

pub use define_runtime::{cron, every};
pub use in_memory_store::{InMemoryExecutionStore, in_memory_execution_store};
pub use run_store_adapter::{
    WorkflowExecutionStore, WorkflowRunStoreAdapterStore, create_run_store_adapter,
};
pub use runtime_driver::{
    DEFAULT_LEASE_MS, DEFAULT_MIN_YIELD_REMAINING_MS, DEFAULT_SWEEP_LIMIT, RunResult,
    RunResultKind, SweepSummary, WorkflowRegistration, WorkflowRegistry,
    WorkflowRuntimeConfig, WorkflowRuntimeDefinition, WorkflowRuntimeDeliverApprovalArgs,
    WorkflowRuntimeDeliverSignalArgs, WorkflowRuntimeStartRunArgs, WorkflowRuntimeSweepArgs,
    WorkflowRuntimeSweepResult, define_workflow_runtime,
};
pub use schedule_materializer::{
    DEFAULT_CRON_LOOKBACK_MS, MaterializeWorkflowSchedulesOptions, MaterializedWorkflowSchedule,
    materialize_workflow_schedules,
};
pub use types::{
    AppendEventsArgs, AppendEventsResult, ClaimDueTimersArgs, ClaimRunArgs, ClaimRunResult,
    ClaimStaleRunsArgs, CreateRunArgs, CreateRunResult, HeartbeatRunLeaseArgs, LeaseOwner,
    LoadedExecution, MarkRunErroredArgs, MarkRunFinishedArgs, MarkRunPausedArgs, ReadEventsArgs,
    ReleaseRunLeaseArgs, RunClaim, RunId, RunSummary, RunTimeline, SaveRunStateArgs,
    ScheduleTimerArgs, StoredWorkflowEvent, TimerWakeup, WorkflowExecution,
    WorkflowExecutionStatus, WorkflowId, WorkflowLease, WorkflowScheduleDefinition,
    WorkflowVersion,
};
