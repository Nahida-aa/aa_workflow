//! 单次 invocation 的驱动入口：把 workflow **跑起来 / 续跑** 的顶层编排。
//!
//! 引擎本体（`EngineRuntime` 驱动态、`exec_step` / `exec_pause` / `signal_run`）
//! 在 [`super`](crate::engine) 中；这里只负责一次 `run_workflow` 调用内部：
//! 从 store 构造 run state、算 per-invocation state、造 `EngineRuntime`、跑 handler、
//! 收尾写终态事件与 RunState 信封。每次调用（start / resume）都独立走完整条路径。

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use crate::define::Workflow;
use crate::engine::{
    DEFAULT_MIN_YIELD_REMAINING_MS, EngineRuntime, StepHalt, WorkflowCancelled, now_ms,
};
use crate::error::{RunError, RunErrorCode, WorkflowError};
use crate::event::{RunStatus, StepStatus, WorkflowEvent, fold_step_states};
use crate::resource::Gate;
use crate::run_store::{RunState, RunStore};

/// Per-invocation options. `run_id`, `continue_from` and `target_step` are
/// invocation options, NOT persisted — persistent state lives in the event
/// log. `max_concurrency` is gone: parallelism is explicit handler code
/// (`tokio::try_join!`).
#[derive(Default)]
pub struct RunOptions {
    pub run_id: Option<String>,
    pub input: serde_json::Value,
    pub target_step: Option<String>,
    pub continue_from: Option<String>,
    /// Absolute UTC ms budget for this drive (TanStack `deadline`). When set,
    /// `time_remaining()` / `should_yield()` (ctx + step) and `ctx.yield_()`
    /// become active; a fresh budget can be supplied on every resume.
    pub deadline: Option<i64>,
    /// `should_yield()` flips true when fewer than this many ms remain
    /// (TanStack `minYieldRemainingMs`, default 1000).
    pub min_yield_remaining_ms: Option<u64>,
    /// Absolute ms at which `ctx.yield_()` re-wakes (TanStack `yieldResumeAt`;
    /// defaults to "now+1ms" per call).
    pub yield_resume_at: Option<i64>,
}

impl RunOptions {
    pub fn new(input: serde_json::Value) -> Self {
        Self {
            run_id: None,
            input,
            target_step: None,
            continue_from: None,
            deadline: None,
            min_yield_remaining_ms: None,
            yield_resume_at: None,
        }
    }

    pub fn run_id(mut self, v: impl Into<String>) -> Self {
        self.run_id = Some(v.into());
        self
    }

    pub fn target_step(mut self, v: impl Into<String>) -> Self {
        self.target_step = Some(v.into());
        self
    }

    pub fn continue_from(mut self, v: impl Into<String>) -> Self {
        self.continue_from = Some(v.into());
        self
    }

    /// Set the absolute UTC ms runtime budget for this drive.
    pub fn deadline(mut self, v: i64) -> Self {
        self.deadline = Some(v);
        self
    }

    /// Set when `should_yield()` turns true (ms of headroom left).
    pub fn min_yield_remaining(mut self, v: u64) -> Self {
        self.min_yield_remaining_ms = Some(v);
        self
    }

    /// Set the absolute re-wake timestamp for `ctx.yield_()`.
    pub fn yield_resume_at(mut self, v: i64) -> Self {
        self.yield_resume_at = Some(v);
        self
    }
}

#[derive(Debug)]
pub struct RunOutcome {
    pub run_id: String,
    pub status: RunStatus,
    pub output: Option<serde_json::Value>,
    /// 与 [`RunState::error`](crate::run_store::RunState) 是**同一个类型**——
    /// 一次失败的 run 在「返回值」和「持久化信封」两处描述一致，不要只改一边。
    ///
    /// `RunError` 实现了 [`Display`](std::fmt::Display)（转发 `message`），
    /// 所以 `warn!("{e}")` / `e.to_string()` 照常可用；想要错误名用 `.name`。
    pub error: Option<RunError>,
}

/// Resolves which workflow definition drives a run.
///
/// 真正的实现在 [`crate::registry::select_workflow_version`]（对齐上游
/// `registry/select-version.ts` 的文件组织）；此处仅再导出。
pub use crate::registry::select_workflow_version;

/// Runs (or resumes) a workflow by driving its async handler.
///
/// Inputs:
/// - `continue_from`: the store truncates everything at `step_id`'s latest
///   terminal checkpoint (inclusive) before re-running the handler, so the
///   prefix short-circuits and the suffix re-executes from scratch.
/// - `target_step`: once that step succeeds the engine raises [`StepHalt`],
///   the handler unwinds (users should propagate with `?`), and the run ends
///   `Finished` with no output.
///
/// On resume, `ctx.step` short-circuits succeeded checkpoints (cached result,
/// `run` not re-executed) and rethrows failed ones. Multiplex step results
/// however you like — the log is the only source of truth.
pub async fn run_workflow(
    workflow: &Workflow,
    store: Arc<dyn RunStore>,
    opts: &RunOptions,
    publisher: Option<Arc<dyn Fn(&WorkflowEvent) + Send + Sync>>,
) -> Result<RunOutcome, WorkflowError> {
    let run_id = opts
        .run_id
        .clone()
        .unwrap_or_else(|| format!("run_{}", now_ms()));
    let ts = now_ms();

    // continue_from lives at the store layer: cut the log at the step's latest
    // terminal checkpoint so the replayed handler re-runs that suffix.
    if let Some(cf) = &opts.continue_from {
        store.truncate_runs(&run_id, cf)?;
    }

    let run_state = match store.get_run_state(&run_id)? {
        Some(mut st) => {
            st.status = RunStatus::Running;
            st.error = None;
            st.updated_at = ts;
            st
        }
        None => RunState {
            run_id: run_id.clone(),
            workflow_id: workflow.id.clone(),
            workflow_version: workflow.version.clone(),
            status: RunStatus::Running,
            input: opts.input.clone(),
            output: None,
            error: None,
            waiting_for: None,
            pending_approval: None,
            created_at: ts,
            updated_at: ts,
        },
    };
    store.set_run_state(&run_id, &run_state)?;

    // Version routing: resume against the definition whose `version` the run
    // persisted (workflow or one of its `previous_versions`). 版本化 run 匹配
    // 不上时**不回退**——那会把 v1 的 run 路由进当前版本的代码，是确定性违规
    // （上游 `select-version.ts` 注释原话）。这里以终局错误结束。
    let persisted_version = run_state
        .workflow_version
        .as_deref()
        .or(workflow.version.as_deref());
    let Some(active) = select_workflow_version(workflow, persisted_version) else {
        return Err(WorkflowError::Validation(format!(
            "workflow version mismatch: run `{run_id}` was started under version \
             {persisted_version:?}, which is not the current version nor in \
             `previous_versions`"
        )));
    };

    // Per-invocation state: re-derived from `initialize(input)` on every
    // start and resume (mirrors TanStack, where state is rebuilt from
    // `initialize({ input })` and never persisted). The handler input the
    // workflow sees is `opts.input` (see `EngineRuntime.input`), so initialize
    // shares that source for consistency.
    let state = match (active.initialize)(&opts.input) {
        Ok(s) => s,
        Err(e) => return init_failed(&store, run_state, &run_id, &e, publisher.as_ref()),
    };
    if let Some(validate) = &active.state_validator
        && let Err(e) = validate(&state)
    {
        return init_failed(&store, run_state, &run_id, &e, publisher.as_ref());
    }

    let events = store.get_events(&run_id)?;
    let lives = fold_step_states(&events);
    let log_len = events.len();

    let target_reached = match &opts.target_step {
        Some(t) => lives
            .get(t)
            .map(|s| s.status == StepStatus::Success)
            .unwrap_or(false),
        None => false,
    };

    let state_mirror: Arc<Mutex<serde_json::Value>> = Arc::new(Mutex::new(state.clone()));
    let inner = Arc::new(EngineRuntime {
        run_id: run_id.clone(),
        input: opts.input.clone(),
        state_mirror,
        prev_state_snapshot: Mutex::new(state),
        store: store.clone(),
        gate: Arc::new(Gate::new()),
        log_len: AtomicUsize::new(log_len),
        lives: Mutex::new(lives),
        target_step: opts.target_step.clone(),
        target_reached: AtomicBool::new(target_reached),
        publisher,
        now_counter: AtomicUsize::new(0),
        uuid_counter: AtomicUsize::new(0),
        deadline: opts.deadline,
        min_yield_remaining_ms: opts
            .min_yield_remaining_ms
            .unwrap_or(DEFAULT_MIN_YIELD_REMAINING_MS),
        yield_resume_at: opts.yield_resume_at,
        yield_counter: AtomicUsize::new(0),
        default_step_retry: active.default_step_retry.clone(),
    });
    inner.publish(&WorkflowEvent::RunStarted {
        ts,
        run_id: run_id.clone(),
    });
    eprintln!("[trace] run_started published");

    let ctx = crate::define::WorkflowCtx::untyped(inner.clone());
    eprintln!("[trace] ctx constructed");
    let handler_result = (active.handler)(ctx).await;
    // 尾段 state delta：ctx 在 handler 内 drop 时已把最终 state 同步进
    // mirror，这里统一 diff 发射（对齐上游 handler 返回 / catch 处的
    // flushStateDelta）。
    inner.emit_state_delta();

    // `failure` 是结构化的（`RunState.error`、`RUN_ERRORED`、`RunOutcome.error`
    // 三处同类型）。
    let (status, output, failure) = match handler_result {
        Ok(output) => (RunStatus::Finished, Some(output), None),
        Err(e) if e.downcast_ref::<StepHalt>().is_some() => (RunStatus::Finished, None, None),
        Err(e) if e.downcast_ref::<WorkflowCancelled>().is_some() => (
            RunStatus::Aborted,
            None,
            Some((RunError::cancelled(), RunErrorCode::Aborted)),
        ),
        Err(e) => (
            RunStatus::Errored,
            None,
            Some((RunError::from_anyhow(&e), RunErrorCode::Error)),
        ),
    };

    let terminal = match &failure {
        Some((err, code)) => WorkflowEvent::RunErrored {
            ts: now_ms(),
            run_id: run_id.clone(),
            error: err.clone(),
            code: *code,
        },
        None => WorkflowEvent::RunFinished {
            ts: now_ms(),
            run_id: run_id.clone(),
            output: output.clone(),
        },
    };
    inner.append(&terminal)?;
    inner.publish(&terminal);

    let mut st = run_state;
    st.status = status;
    st.output = output.clone();
    st.error = failure.as_ref().map(|(e, _)| e.clone());
    st.updated_at = now_ms();
    store.set_run_state(&run_id, &st)?;

    Ok(RunOutcome {
        run_id,
        status,
        output,
        error: failure.map(|(e, _)| e),
    })
}

/// Persists a run that failed during pre-handler initialization (state
/// `initialize` returning an error, or the `state_schema` shape check
/// rejecting the built state) and returns the errored outcome. Counterpart of
/// TanStack zod `.safeParse` failing validation: the run is recorded as
/// failed rather than left dangling.
///
/// **只 publish，不 append**：什么都没跑，不该在日志里留下半条记录——TanStack
/// 同理，他们的 validation 失败走 `emit(...)`（只进内存队列），不是
/// `emitAndAppend`（`run-workflow.ts:215`）。
fn init_failed(
    store: &Arc<dyn RunStore>,
    mut run_state: RunState,
    run_id: &str,
    err: &anyhow::Error,
    publisher: Option<&Arc<dyn Fn(&WorkflowEvent) + Send + Sync>>,
) -> Result<RunOutcome, WorkflowError> {
    let run_err = RunError::from_anyhow(err);
    run_state.status = RunStatus::Errored;
    run_state.error = Some(run_err.clone());
    run_state.updated_at = now_ms();
    store.set_run_state(run_id, &run_state)?;
    if let Some(publish) = publisher {
        publish(&WorkflowEvent::RunErrored {
            ts: now_ms(),
            run_id: run_id.to_string(),
            error: run_err.clone(),
            code: RunErrorCode::Validation,
        });
    }
    Ok(RunOutcome {
        run_id: run_id.to_string(),
        status: RunStatus::Errored,
        output: None,
        error: Some(run_err),
    })
}

/// Sync convenience over a local multi-thread runtime for callers that are
/// not async themselves (e.g. LocalDub's CLI entrypoint).
pub fn run_workflow_sync(
    workflow: &Workflow,
    store: Arc<dyn RunStore>,
    opts: &RunOptions,
    publisher: Option<Arc<dyn Fn(&WorkflowEvent) + Send + Sync>>,
) -> Result<RunOutcome, WorkflowError> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .map_err(|e| WorkflowError::Internal(format!("tokio runtime: {e}")))?;
    rt.block_on(run_workflow(workflow, store, opts, publisher))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::define::{
        BaseCtx, CreateWorkflowConfig, StepCtx, Workflow, WorkflowCtx, create_workflow,
    };
    use crate::engine::testkit::TestLog;
    use crate::run_store::InMemoryStore;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(serde::Deserialize, serde::Serialize)]
    struct StrictState {
        n: i64,
    }

    /// 未显式给 `run_id` 时引擎生成 `run_<now_ms>`，并照常写 RunState 信封。
    #[tokio::test]
    async fn run_id_defaults_to_generated_when_absent() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("gen-id").handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", |_sc: StepCtx| async move { Ok(json!({ "ok": true })) })
                .await
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(json!({ "x": 1 })),
            None,
        )
        .await
        .unwrap();
        assert!(
            out.run_id.starts_with("run_"),
            "缺省 run_id 由引擎生成，实际为 {:?}",
            out.run_id
        );
        let st = store
            .get_run_state(&out.run_id)
            .unwrap()
            .expect("RunState 应已写入");
        assert_eq!(st.status, RunStatus::Finished);
        assert_eq!(st.input, json!({ "x": 1 }), "input 落在 RunState 信封上");
    }

    /// `initialize` 失败 → `init_failed`：run 记 Errored，且**不 append 任何事件**
    /// （什么都没跑）。对齐 TanStack zod `.safeParse` 失败的语义。
    #[tokio::test]
    async fn initialize_failure_errors_run_without_events() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("bad-init")
            .initialize(|_| Err(anyhow::anyhow!("cannot build state")))
            .handler(|ctx: WorkflowCtx| async move {
                ctx.step("never", |_sc: StepCtx| async move {
                    Ok(serde_json::Value::Null)
                })
                .await
            });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(json!({})).run_id("bad-init:r"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.unwrap().message.contains("cannot build state"));
        assert!(
            store.get_events("bad-init:r").unwrap().is_empty(),
            "init 失败不应留下任何事件"
        );
        let st = store.get_run_state("bad-init:r").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Errored);
        assert!(st.output.is_none());
    }

    /// `state_schema` 形状不匹配走同一条 `init_failed` 路径。
    #[tokio::test]
    async fn state_shape_mismatch_errors_run_without_events() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = create_workflow(
            CreateWorkflowConfig::new("bad-state")
                .input::<serde_json::Value>()
                .initialize(|_| Ok(json!({ "n": "not-a-number" })))
                .state::<StrictState>(),
        )
        .handler(|ctx: BaseCtx<serde_json::Value, StrictState>| async move {
            Ok(json!({ "n": ctx.state.n }))
        });
        let wf: Workflow = wf.into_workflow();
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(json!({})).run_id("bad-state:r"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(
            out.error.unwrap().message.contains("invalid type"),
            "错误应来自 state 的 serde 形状校验"
        );
        assert!(
            store.get_events("bad-state:r").unwrap().is_empty(),
            "state 校验失败同样不落事件"
        );
        let st = store.get_run_state("bad-state:r").unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Errored);
    }

    /// `RUN_ERRORED` 带机器可读的 `code`（TanStack `RUN_ERRORED.code`），
    /// host 该用它分支，而不是去匹配 `error.message`。
    #[tokio::test]
    async fn handler_failure_errored_with_error_code() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("bad").handler(|ctx: WorkflowCtx| async move {
            ctx.step("a", |_sc: StepCtx| async move { anyhow::bail!("boom") })
                .await
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(json!({})).run_id("code:err"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);

        let (err, code) = store
            .get_events("code:err")
            .unwrap()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, code, .. } => Some((error.clone(), *code)),
                _ => None,
            })
            .expect("终局事件应为 RunErrored");
        assert_eq!(code, RunErrorCode::Error);
        assert_eq!(code.as_str(), "error");
        assert_eq!(err.message, "boom");
    }

    #[tokio::test]
    async fn cancelled_run_errored_with_aborted_code() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("c").handler(|_ctx: WorkflowCtx| async move {
            Err(crate::engine::WorkflowCancelled.into())
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(json!({})).run_id("code:abort"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Aborted);

        let (err, code) = store
            .get_events("code:abort")
            .unwrap()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, code, .. } => Some((error.clone(), *code)),
                _ => None,
            })
            .expect("终局事件应为 RunErrored");
        assert_eq!(code, RunErrorCode::Aborted);
        assert_eq!(err.name, "Aborted");
    }

    /// `initialize` 失败：publish `validation_error`，但**不进事件日志**（对齐
    /// TanStack 的 `emit` 而非 `emitAndAppend`）。
    #[tokio::test]
    async fn init_failure_publishes_validation_code_without_appending() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let seen: Arc<Mutex<Vec<WorkflowEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let publisher: Arc<dyn Fn(&WorkflowEvent) + Send + Sync> = {
            let seen = seen.clone();
            Arc::new(move |ev| seen.lock().unwrap().push(ev.clone()))
        };
        let wf = Workflow::new("bad-init")
            .initialize(|_| Err(anyhow::anyhow!("nope")))
            .handler(|_ctx: WorkflowCtx| async move { Ok(json!({ "unreachable": true })) });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(json!({})).run_id("code:validation"),
            Some(publisher),
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(
            store.get_events("code:validation").unwrap().is_empty(),
            "什么都没跑，不该留下事件"
        );

        let (err, code) = seen
            .lock()
            .unwrap()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, code, .. } => Some((error.clone(), *code)),
                _ => None,
            })
            .expect("应 publish RunErrored");
        assert_eq!(code, RunErrorCode::Validation);
        assert_eq!(code.as_str(), "validation_error");
        assert_eq!(err.message, "nope");
    }

    /// 一次失败的 run，在四处描述必须是**同一个** `RunError`：
    /// `RunOutcome.error`（返回值）、`RunState.error`（信封）、
    /// `RUN_ERRORED.error`（日志）、`StepAttempt.error`（step 级）。
    /// 任何一处退回字符串都是退步，这个测试钉住它。
    ///
    /// 顺带验证 `RunError` 的 `Display` 转发 `message`——所以改成结构化之后
    /// 调用方 `warn!("{e}")` 的写法不受影响。
    #[tokio::test]
    async fn one_failure_is_one_run_error_everywhere() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("one-error").handler(|ctx: WorkflowCtx| async move {
            ctx.step(
                "a",
                |_sc: StepCtx| async move { anyhow::bail!("same failure") },
            )
            .await
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(json!({})).run_id("one:r"),
            None,
        )
        .await
        .unwrap();

        let from_outcome = out.error.expect("RunOutcome.error");
        let from_state = store
            .get_run_state("one:r")
            .unwrap()
            .unwrap()
            .error
            .expect("RunState.error");

        let events = store.get_events("one:r").unwrap();
        let from_event = events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::RunErrored { error, .. } => Some(error.clone()),
                _ => None,
            })
            .expect("RUN_ERRORED.error");
        let from_attempt = events
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::StepFailed { attempts, .. } => {
                    attempts.first().and_then(|a| a.error.clone())
                }
                _ => None,
            })
            .expect("StepAttempt.error");

        assert_eq!(
            from_outcome, from_state,
            "RunOutcome 与 RunState 应同类型同值"
        );
        assert_eq!(from_outcome, from_event, "与 RUN_ERRORED 应同类型同值");
        assert_eq!(from_outcome, from_attempt, "与 StepAttempt 应同类型同值");
        assert_eq!(from_outcome.message, "same failure");

        // Display 转发 message —— 调用方 `format!("{e}")` 不受影响。
        assert_eq!(from_outcome.to_string(), from_outcome.message);
    }

    /// 版本化 run 的 `workflow_version` 匹配不上 → 终局错误，**不回退**。
    /// 回退会把旧版 run 路由进新版代码（确定性违规，见 registry 模块文档）。
    #[tokio::test]
    async fn version_mismatch_errors_instead_of_falling_back() {
        let store: Arc<dyn RunStore> = Arc::new(InMemoryStore::new());

        // 先以 v1 起一个 run（挂起在 approve，保持非终态以便后续 resume）。
        let v1 = Workflow::new("ver")
            .version("v1")
            .handler(|ctx: WorkflowCtx| async move {
                ctx.approve("gate", "hold").await?;
                Ok(serde_json::Value::Null)
            });
        let task = tokio::spawn({
            let store = store.clone();
            let v1 = v1.clone();
            async move {
                run_workflow(
                    &v1,
                    store,
                    &RunOptions::new(serde_json::json!({})).run_id("ver:mismatch"),
                    None,
                )
                .await
            }
        });
        // 等挂起（`RunState` 信封此时已记下 `workflow_version = "v1"`）。
        for _ in 0..500 {
            if let Some(st) = store.get_run_state("ver:mismatch").unwrap()
                && st.status == RunStatus::Paused
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        task.abort();
        let _ = task.await;

        // v2 **不带** v1 作 previous_versions → resume 应报错而非回退到 v2。
        let v2 = Workflow::new("ver")
            .version("v2")
            .handler(|_ctx: WorkflowCtx| async move { Ok(serde_json::Value::Null) });
        let err = run_workflow(
            &v2,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id("ver:mismatch"),
            None,
        )
        .await
        .expect_err("版本不匹配应报错");
        assert!(
            matches!(err, WorkflowError::Validation(ref m) if m.contains("version mismatch")),
            "应为版本不匹配错误，实际 {err:?}"
        );
    }

    #[tokio::test]
    async fn failed_is_terminal_until_continue_from() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let fail = Arc::new(AtomicBool::new(true));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            let fail = fail.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                let fail = fail.clone();
                async move {
                    for id in ["a", "b"] {
                        let (log, fail) = (log.clone(), fail.clone());
                        let id = id.to_string();
                        let id_c = id.clone();
                        ctx.step(&id, move |_sc: StepCtx| {
                            let (log, fail, id) = (log.clone(), fail.clone(), id_c.clone());
                            async move {
                                log.lock().unwrap().note_start(&id);
                                let res = if id == "b" && fail.swap(false, Ordering::SeqCst) {
                                    Err(anyhow::anyhow!("boom"))
                                } else {
                                    Ok(serde_json::Value::Null)
                                };
                                log.lock().unwrap().note_finish(&id);
                                res
                            }
                        })
                        .await?;
                    }
                    Ok(serde_json::Value::Null)
                }
            }
        });

        // first run: b fails, run errors
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Errored);
        assert!(out.error.unwrap().message.contains("boom"));
        assert_eq!(log.lock().unwrap().runs["b"], 1);
        let run_id = out.run_id.clone();

        // plain resume: failed checkpoint rethrows → still errored, no rerun
        let again = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id(run_id.clone()),
            None,
        )
        .await
        .unwrap();
        assert_eq!(again.status, RunStatus::Errored);
        assert_eq!(
            log.lock().unwrap().runs["b"],
            1,
            "no re-execution on plain resume"
        );

        // continue_from "b": truncate b's checkpoint + suffix, replay reruns b
        let resumed = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({}))
                .run_id(run_id)
                .continue_from("b"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(resumed.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1, "prefix before continue_from untouched");
        assert_eq!(l.runs["b"], 2, "suffix reran via continue_from");
    }

    #[tokio::test]
    async fn continue_from_resets_downstream() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                async move {
                    for id in ["a", "b", "c"] {
                        let log = log.clone();
                        let id = id.to_string();
                        let id_c = id.clone();
                        ctx.step(&id, move |_sc: StepCtx| {
                            let (log, id) = (log.clone(), id_c.clone());
                            async move {
                                log.lock().unwrap().note_start(&id);
                                log.lock().unwrap().note_finish(&id);
                                Ok(serde_json::Value::Null)
                            }
                        })
                        .await?;
                    }
                    Ok(serde_json::Value::Null)
                }
            }
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})),
            None,
        )
        .await
        .unwrap();
        let second = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({}))
                .run_id(out.run_id)
                .continue_from("b"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(second.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 2);
        assert_eq!(l.runs["c"], 2);
    }

    #[tokio::test]
    async fn target_step_stops_downstream() {
        let store = Arc::new(InMemoryStore::new());
        let log = Arc::new(Mutex::new(TestLog::default()));
        let wf = Workflow::new("w").handler({
            let log = log.clone();
            move |ctx: WorkflowCtx| {
                let log = log.clone();
                async move {
                    for id in ["a", "b", "c"] {
                        let log = log.clone();
                        let id = id.to_string();
                        let id_c = id.clone();
                        ctx.step(&id, move |_sc: StepCtx| {
                            let (log, id) = (log.clone(), id_c.clone());
                            async move {
                                log.lock().unwrap().note_start(&id);
                                log.lock().unwrap().note_finish(&id);
                                Ok(serde_json::Value::Null)
                            }
                        })
                        .await?;
                    }
                    Ok(serde_json::Value::Null)
                }
            }
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).target_step("b"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        let l = log.lock().unwrap();
        assert_eq!(l.runs["a"], 1);
        assert_eq!(l.runs["b"], 1);
        assert_eq!(
            l.runs.get("c").copied().unwrap_or(0),
            0,
            "downstream never ran"
        );
    }

    #[tokio::test]
    async fn handler_output_is_run_output() {
        let store = Arc::new(InMemoryStore::new());
        let wf = Workflow::new("w").handler(|ctx: WorkflowCtx| async move {
            let v = ctx
                .step("a", move |_sc: StepCtx| async move {
                    Ok(serde_json::json!({"x": 1}))
                })
                .await?;
            Ok(serde_json::json!({ "out": v }))
        });
        let out = run_workflow(
            &wf,
            store.clone(),
            &RunOptions::new(serde_json::json!({})),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status, RunStatus::Finished);
        assert_eq!(out.output, Some(serde_json::json!({ "out": { "x": 1 } })));
        let st = store.get_run_state(&out.run_id).unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Finished);
        assert_eq!(st.output, Some(serde_json::json!({ "out": { "x": 1 } })));
    }

    /// resume 按持久化 `workflow_version` 路由到 previous version 的 handler；
    /// 全新 run 用当前版本。
    #[tokio::test]
    async fn resume_routes_by_persisted_workflow_version() {
        let store = Arc::new(InMemoryStore::new());
        let v1 = create_workflow(
            CreateWorkflowConfig::new("ver-wf")
                .version("v1")
                .input::<serde_json::Value>(),
        )
        .handler(|ctx: BaseCtx<serde_json::Value>| async move {
            ctx.step("s", |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "ver": "v1" }))
            })
            .await
        })
        .into_workflow();

        let out1 = run_workflow(
            &v1,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id("ver:r"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(out1.output, Some(serde_json::json!({ "ver": "v1" })));

        let v2 = create_workflow(
            CreateWorkflowConfig::new("ver-wf")
                .version("v2")
                .input::<serde_json::Value>(),
        )
        .previous_versions(vec![v1])
        .handler(|ctx: BaseCtx<serde_json::Value>| async move {
            ctx.step("s", |_sc: StepCtx| async move {
                Ok(serde_json::json!({ "ver": "v2" }))
            })
            .await
        });

        let out2 = run_workflow(
            &v2,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id("ver:r"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            out2.output,
            Some(serde_json::json!({ "ver": "v1" })),
            "resume 路由到持久化的 previous version"
        );

        let out3 = run_workflow(
            &v2,
            store.clone(),
            &RunOptions::new(serde_json::json!({})).run_id("ver:r2"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            out3.output,
            Some(serde_json::json!({ "ver": "v2" })),
            "全新 run 用当前版本"
        );
    }
}
