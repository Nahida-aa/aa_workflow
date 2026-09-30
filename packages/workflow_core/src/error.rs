use std::fmt;

use serde::{Deserialize, Serialize};

/// Errors produced by a [`crate::run_store::RunStore`] implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// `append_event` was called with a stale expected index (CAS violation).
    Conflict {
        run_id: String,
        expected: usize,
        actual: usize,
    },
    /// Backing-store I/O or serialization failure.
    Io(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Conflict {
                run_id,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "append CAS conflict for run {run_id}: expected index {expected}, found {actual}"
                )
            }
            StoreError::Io(msg) => write!(f, "store io error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// 跨进程序列化的错误（对齐 TanStack `SerializedError`，`types.ts:16`）。
///
/// 「序列化」不等于「持久化」：它可能进 `RunState.error`，也可能只随事件投给
/// publisher。存不存、存哪儿，是 [`RunStore`](crate::run_store::RunStore)
/// 实现者的事。
///
/// 两处刻意不对等：
/// - **没有 `stack`**。JS 的 `Error.stack` 在 Rust 无对应物：`anyhow` 的
///   backtrace 需要 nightly，`std::backtrace` 虽稳定但要 `RUST_BACKTRACE=1`
///   才有内容，且跨 `await` 边界捕获到的都是运行时内部帧，对排查 workflow
///   逻辑没有帮助。
/// - **`name` 常常只是 `"Error"`**。JS 的 `Error.name` 是错误类名；Rust 的
///   `anyhow::Error` 是类型擦除的，拿不到类名。只有引擎自身的
///   [`WorkflowError`] 变体与
///   [`WorkflowCancelled`](crate::engine::WorkflowCancelled) 能给出有意义的名字。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunError {
    pub name: String,
    pub message: String,
}

impl RunError {
    /// 引擎主动中止（`cancel_run`）产生的错误。
    pub fn cancelled() -> Self {
        use crate::engine::WorkflowCancelled;
        Self {
            name: "Aborted".into(),
            message: WorkflowCancelled.to_string(),
        }
    }

    /// 从 `anyhow::Error` 提取：能 downcast 到引擎错误类型时给出变体名，
    /// 否则退化为 `"Error"`（见类型文档）。
    pub fn from_anyhow(e: &anyhow::Error) -> Self {
        use WorkflowError;
        let name = if e
            .downcast_ref::<crate::engine::WorkflowCancelled>()
            .is_some()
        {
            "Aborted"
        } else {
            match e.downcast_ref::<WorkflowError>() {
                Some(WorkflowError::Validation(_)) => "Validation",
                Some(WorkflowError::Step { .. }) => "Step",
                Some(WorkflowError::Finalize(_)) => "Finalize",
                Some(WorkflowError::Store(_)) => "Store",
                Some(WorkflowError::RunNotFound(_)) => "RunNotFound",
                Some(WorkflowError::SignalLost(_)) => "SignalLost",
                Some(WorkflowError::Internal(_)) => "Internal",
                None => "Error",
            }
        };
        Self {
            name: name.into(),
            message: e.to_string(),
        }
    }
}

/// 终局错误码（对齐 TanStack `RUN_ERRORED.code`，`types.ts:91`）。
///
/// TS 全集是 `error | aborted | validation_error | run_lost | signal_lost |
/// approval_lost | workflow_version_mismatch`。这里只留引擎真能产出的三个：
///
/// - `run_lost` / `signal_lost` / `approval_lost` 对应他们「投递丢失」时把
///   run 打 errored 的路径，我们的 `signal_run` / `signal_event` 是把错误
///   **返回给调用方**，不往日志里写终态事件，没有发射点。
/// - `workflow_version_mismatch` 我们是**回退到当前版本**而不是报错
///   （`select_workflow_version`），所以也产不出。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunErrorCode {
    /// handler 抛错 / step 失败。
    Error,
    /// 被 `cancel_run` 中止。
    Aborted,
    /// `initialize` 失败或 state 形状校验不过（对应他们 zod `.safeParse` 失败）。
    Validation,
    /// 版本化 run 的 `workflow_version` 既不是当前版本、也不在
    /// `previous_versions` 里（对齐上游 `workflow_version_mismatch`
    /// 错误码）。**不回退**——那会把旧版 run 路由进新版代码，是确定性违规。
    WorkflowVersionMismatch,
    /// `attach` 指向的 run 在 store 里不存在（对齐上游 `run_lost`）。
    RunLost,
}

impl RunErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunErrorCode::Error => "error",
            RunErrorCode::Aborted => "aborted",
            RunErrorCode::Validation => "validation_error",
            RunErrorCode::WorkflowVersionMismatch => "workflow_version_mismatch",
            RunErrorCode::RunLost => "run_lost",
        }
    }
}

impl std::fmt::Display for RunErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Errors produced by the engine.
#[derive(Debug)]
pub enum WorkflowError {
    /// The workflow graph is invalid (duplicate ids, unknown `needs`, cycle).
    Validation(String),
    /// A workflow step failed after exhausting its retry policy.
    Step { step_id: String, error: String },
    /// The finalize step failed.
    Finalize(String),
    /// Transport/persistence error while talking to the store.
    Store(StoreError),
    /// No `RunState` exists for the requested run id.
    RunNotFound(String),
    /// A delivered signal could not be matched to a parked wait point (from
    /// [`crate::engine::signal_event`]: nothing was paused on that event name).
    SignalLost(String),
    /// An internal invariant was violated (deadlock guard, channel closed).
    Internal(String),
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkflowError::Validation(msg) => write!(f, "workflow validation failed: {msg}"),
            WorkflowError::Step { step_id, error } => {
                write!(f, "step `{step_id}` failed: {error}")
            }
            WorkflowError::Finalize(msg) => write!(f, "finalize failed: {msg}"),
            WorkflowError::Store(e) => write!(f, "store error: {e}"),
            WorkflowError::RunNotFound(id) => write!(f, "run `{id}` not found"),
            WorkflowError::SignalLost(msg) => write!(f, "signal lost: {msg}"),
            WorkflowError::Internal(msg) => write!(f, "engine internal error: {msg}"),
        }
    }
}

impl std::error::Error for WorkflowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WorkflowError::Store(e) => Some(e),
            _ => None,
        }
    }
}

impl From<StoreError> for WorkflowError {
    fn from(e: StoreError) -> Self {
        WorkflowError::Store(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_error_names_engine_error_variants() {
        let e = RunError::from_anyhow(&anyhow::Error::from(WorkflowError::SignalLost(
            "nobody waiting".into(),
        )));
        assert_eq!(e.name, "SignalLost");
        assert_eq!(e.message, "signal lost: nobody waiting");
    }

    #[test]
    fn run_error_falls_back_to_error_for_opaque_anyhow() {
        // anyhow::Error 是类型擦除的，拿不到类名——如实退化，不编造。
        let e = RunError::from_anyhow(&anyhow::anyhow!("boom"));
        assert_eq!(e.name, "Error");
        assert_eq!(e.message, "boom");
    }

    #[test]
    fn run_error_cancelled_names_aborted() {
        let e = RunError::cancelled();
        assert_eq!(e.name, "Aborted");
        assert_eq!(e.message, "workflow aborted");
    }

    #[test]
    fn run_error_serializes_without_stack() {
        let v = serde_json::to_value(RunError::cancelled()).unwrap();
        assert_eq!(v["name"], "Aborted");
        assert!(v.get("stack").is_none(), "没有 stack 字段（见类型文档）");
    }
}
