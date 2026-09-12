
//! 单次 invocation 的驱动入口：把 workflow **跑起来 / 续跑** 的顶层编排。
//!
//! 引擎本体（`DrvInner` 驱动态、`exec_step` / `exec_pause` / `signal_run`）
//! 在 [`super`](crate::engine) 中；这里只负责一次 `run_workflow` 调用内部：
//! 从 store 构造 run state、算 per-invocation state、造 `DrvInner`、跑 handler、
//! 收尾写终态事件与 run.json。每次调用（start / resume）都独立走完整条路径。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Mutex;

use crate::define::Workflow;
use crate::engine::{now_ms, DrvInner, StepHalt, WorkflowCancelled};
use crate::error::WorkflowError;
use crate::event::{RunEvent, RunStatus, StepStatus, fold_step_states};
use crate::resource::Gate;
use crate::store::{RunState, RunStore};

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
}

impl RunOptions {
    pub fn new(input: serde_json::Value) -> Self {
        Self {
            run_id: None,
            input,
            target_step: None,
            continue_from: None,
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
}

#[derive(Debug)]
pub struct RunOutcome {
    pub run_id: String,
    pub status: RunStatus,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
}

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
    publisher: Option<Arc<dyn Fn(&RunEvent) + Send + Sync>>,
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

    // Per-invocation state: re-derived from `initialize(input)` on every
    // start and resume (mirrors TanStack, where state is rebuilt from
    // `initialize({ input })` and never persisted). The handler input the
    // workflow sees is `opts.input` (see `DrvInner.input`), so initialize
    // shares that source for consistency.
    let state = match (workflow.initialize)(&opts.input) {
        Ok(s) => s,
        Err(e) => return init_failed(&store, run_state, &run_id, &e),
    };
    if let Some(validate) = &workflow.state_validator
        && let Err(e) = validate(&state)
    {
        return init_failed(&store, run_state, &run_id, &e);
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

    let inner = Arc::new(DrvInner {
        run_id: run_id.clone(),
        input: opts.input.clone(),
        state: Arc::new(std::sync::RwLock::new(state)),
        store: store.clone(),
        gate: Arc::new(Gate::new()),
        log_len: AtomicUsize::new(log_len),
        lives: Mutex::new(lives),
        target_step: opts.target_step.clone(),
        target_reached: AtomicBool::new(target_reached),
        publisher,
        now_counter: AtomicUsize::new(0),
        uuid_counter: AtomicUsize::new(0),
    });
    inner.publish(&RunEvent::RunStarted { ts, run_id: run_id.clone() });

    let ctx = crate::define::WorkflowCtx { inner: inner.clone() };
    let handler_result = (workflow.handler)(ctx).await;

    let (status, output, error) = match handler_result {
        Ok(output) => (RunStatus::Finished, Some(output), None),
        Err(e) if e.downcast_ref::<StepHalt>().is_some() => {
            (RunStatus::Finished, None, None)
        }
        Err(e) if e.downcast_ref::<WorkflowCancelled>().is_some() => {
            (RunStatus::Aborted, None, Some(WorkflowCancelled.to_string()))
        }
        Err(e) => {
            let msg = e.to_string();
            (RunStatus::Errored, None, Some(msg))
        }
    };

    let terminal = match &error {
        Some(e) => RunEvent::RunErrored {
            ts: now_ms(),
            run_id: run_id.clone(),
            error: e.clone(),
        },
        None => RunEvent::RunFinished {
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
    st.error = error.clone();
    st.updated_at = now_ms();
    store.set_run_state(&run_id, &st)?;

    Ok(RunOutcome {
        run_id,
        status,
        output,
        error,
    })
}

/// Persists a run that failed during pre-handler initialization (state
/// `initialize` returning an error, or the `state_schema` shape check
/// rejecting the built state) and returns the errored outcome. Counterpart of
/// TanStack zod `.safeParse` failing validation: the run is recorded as
/// failed rather than left dangling. No events are appended — nothing has run.
fn init_failed(
    store: &Arc<dyn RunStore>,
    mut run_state: RunState,
    run_id: &str,
    err: &anyhow::Error,
) -> Result<RunOutcome, WorkflowError> {
    let msg = err.to_string();
    run_state.status = RunStatus::Errored;
    run_state.error = Some(msg.clone());
    run_state.updated_at = now_ms();
    store.set_run_state(run_id, &run_state)?;
    Ok(RunOutcome {
        run_id: run_id.to_string(),
        status: RunStatus::Errored,
        output: None,
        error: Some(msg),
    })
}

/// Sync convenience over a local multi-thread runtime for callers that are
/// not async themselves (e.g. LocalDub's CLI entrypoint).
pub fn run_workflow_sync(
    workflow: &Workflow,
    store: Arc<dyn RunStore>,
    opts: &RunOptions,
    publisher: Option<Arc<dyn Fn(&RunEvent) + Send + Sync>>,
) -> Result<RunOutcome, WorkflowError> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .map_err(|e| WorkflowError::Internal(format!("tokio runtime: {e}")))?;
    rt.block_on(run_workflow(workflow, store, opts, publisher))
}
