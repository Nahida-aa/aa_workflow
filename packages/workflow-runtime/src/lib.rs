//! Workflow 的**执行所有权层**：lease、sweep、timer、schedule。
//!
//! 对齐 TanStack `@tanstack/workflow-runtime`。定位见
//! `docs/runtime-design.md`（决策记录）与 core 的 `run_store` 模块文档
//! （两个 store 契约的对应关系）。
//!
//! # 为什么 core 里没有这一层
//!
//! core 刻意只做 replay + 耐久原语 + 一个「存住就行」的 [`RunStore`](workflow_core::RunStore) 契约
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
//! 基础契约（[`WorkflowRunStoreAdapterStore`]）与类型已立，[`WorkflowExecutionStore`]
//! 的扩展方法**尚未定义**——它们依赖 `docs/runtime-design.md` 的 D2（lease
//! 位置）与 D4（sweep 边界）敲定。
//!
//! # 层次
//!
//! ```text
//! workflow_core::RunStore          引擎 replay 用（6 个方法）
//!   ↕ 形状相近但不同（见 run_store_adapter 模块文档）
//! WorkflowRunStoreAdapterStore     本 crate 的存储基础（6 个方法）
//!   └── WorkflowExecutionStore     扩展：lease / timer / schedule / 查询
//! ```
//!
//! 后两个 trait 都在 [`run_store_adapter`] 里（与上游把两个 interface 放在同一个
//! `types.ts` 一致），本模块只做声明与再导出。

pub mod run_store_adapter;
pub mod types;

pub use run_store_adapter::{
    RunStoreAdapter, WorkflowExecutionStore, WorkflowRunStoreAdapterStore, create_run_store_adapter,
};
pub use types::{
    AppendEventsArgs, AppendEventsResult, LeaseOwner, LoadedExecution, ReadEventsArgs, RunId,
    RunSummary, RunTimeline, SaveRunStateArgs, StoredWorkflowEvent, WorkflowExecution, WorkflowId,
    WorkflowLease, WorkflowVersion,
};
