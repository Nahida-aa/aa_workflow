//! 文件落盘的 durable 执行存储，实现 **新契约** `WorkflowExecutionStore`。
//!
//! # 为什么需要这个 crate（起因是我们自己踩的坑）
//!
//! `examples/shared` 里的 `FileRunStore` 实现的是 **core 的旧契约 `RunStore`**。
//! 那让示例层看起来在示范「写 store adapter = 实现 `RunStore`」——而上游
//! `docs/api/store-adapters.md` 开篇的规定恰好相反：
//!
//! > Store adapters implement `WorkflowExecutionStore` from
//! > `@tanstack/workflow-runtime`.
//!
//! 我们**被这个示例误导过**（写 `AGENTS.md` 时一度把「实现 `RunStore`」当成 store
//! adapter 的做法）。示例层不能只有旧契约的 store——所以这个 crate 提供**新契约**
//! 的落盘实现示范。
//!
//! `examples/shared` 保留不动：它对齐 TanStack `examples/deployment-pocs/shared`
//! 的位置，有对账价值。两者分工是**契约**，不是新旧：
//!
//! | crate | 契约 | 用途 |
//! | --- | --- | --- |
//! | `examples/shared`（`FileRunStore`） | `aa_workflow_core::RunStore`（旧） | 与上游示例对账 |
//! | 本 crate（`FileExecutionStore`） | `WorkflowExecutionStore`（新） | store adapter 的参考实现 |
//! | `packages/workflow_store_sqlx_postgres` | `WorkflowExecutionStore`（新） | 生产级 / 多 worker |
//!
//! # 与 `-sqlx-postgres` 的分工：单进程 vs 多 worker
//!
//! 两者实现**同一套契约**，都跑同一份契约套件（`store_contract.rs`）。差别在并发：
//!
//! - 本 crate 用**进程内 `Mutex`** 串行化读-改-写，因此只保证**单进程正确**。
//!   文件系统没有 `select ... for update skip locked` 那样的原子原语，
//!   真做跨进程 lease 要引入文件锁（flock），而它在 NFS 等场景不可靠。
//! - **多 worker / serverless 场景请用
//!   [`workflow-store-sqlx-postgres`](../../packages/workflow_store_sqlx_postgres)**，
//!   它用 Postgres 的行锁做 CAS 与 lease。
//!
//! 这不是「简化版」的托词：契约套件本来就用**显式时间戳**模拟竞争而非真并发
//! （见其模块文档），所以这里的 lease / 四态投递语义是**完整的**，只是并发边界
//! 止于单进程。
//!
//! # 落盘布局
//!
//! ```text
//! <base>/<run_id>/run.json         core 的 RunState 信封（对应 workflow_run_states）
//! <base>/<run_id>/exec.json        runtime 的 WorkflowExecution（含 lease / wake_at）
//! <base>/<run_id>/events.jsonl     append-only 事件日志（CAS 追加）
//! <base>/<run_id>/timers.json      到期的 sleep 唤醒索引
//! <base>/<run_id>/deliveries.json  signal / approval 的幂等投递记录
//! ```
//!
//! `run.json` 与 `exec.json` 分开存，对应 Postgres 版那两张像似的表
//! （`workflow_run_states` / `workflow_runs`）——**lease 字段只在后者**，
//! 因为那是 runtime 的「执行所有权」概念，core 的 `RunState` 里没有。
//!
//! # 目录
//!
//! - [`file_store`] — `FileExecutionStore` 本体（新契约的落盘实现）
//! - [`mod@dub_sf_ocr`] — 一个可复用的示例 workflow：LocalDub `dub_sf_ocr` 的
//!   **两分支并行**形状。放在 `src/` 而非测试里，是为了别人也能
//!   `use example_store_file::dub_sf_ocr` 拿去驱动自己的 store。

pub mod dub_sf_ocr;
pub mod file_store;

pub use dub_sf_ocr::{DubSfOcrInput, DubSfOcrState, dub_sf_ocr};
pub use file_store::FileExecutionStore;
