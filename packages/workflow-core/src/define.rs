use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::error::WorkflowError;
use crate::event::StepState;
use crate::scheduler::StepContext;

/// The per-step execution function. Steps write artifacts to disk and read
/// from the frozen input / other steps' outputs they depend on; the engine
/// only cares about success/failure.
pub type StepRunFn = Arc<dyn Fn(StepContext) -> anyhow::Result<Option<serde_json::Value>> + Send + Sync>;
/// Per-run filter; `None` means "always enabled". This is the LocalDub
/// `get_steps`-style selection carrier.
pub type StepEnabledFn = Arc<dyn Fn(&StepContext) -> bool + Send + Sync>;
/// Maps a run to the [`crate::resource::ResourceKey`] it holds while running.
pub type StepResourceFn = Arc<dyn Fn(&StepContext) -> String + Send + Sync>;
/// Make-style freshness check: `Ok(true)` means "already up to date, skip
/// re-running". Mirrors LocalDub's mtime / pipeline-fingerprint staleness.
pub type StepUpToDateFn = Arc<dyn Fn(&StepContext, &StepState) -> bool + Send + Sync>;

/// Fixed | exponential | custom backoff. `attempt` is 1-based: after attempt
/// #N fails, we wait `delay_ms(N)` before attempt N+1 (exponential: base * 2^(N-1)).
#[derive(Clone)]
pub enum Backoff {
    Fixed { base_ms: u64 },
    Exponential { base_ms: u64 },
    Custom(Arc<dyn Fn(usize) -> u64 + Send + Sync>),
}

impl Backoff {
    pub fn delay_ms(&self, attempt: usize) -> u64 {
        match self {
            Backoff::Fixed { base_ms } => *base_ms,
            Backoff::Exponential { base_ms } => {
                let n = (attempt as u32).saturating_sub(1);
                base_ms.saturating_mul(1u64 << n.min(62))
            }
            Backoff::Custom(f) => f(attempt),
        }
    }
}

#[derive(Clone)]
pub struct RetryPolicy {
    /// Total attempts, including the first. 1 == no retry.
    pub max_attempts: usize,
    pub backoff: Backoff,
}

impl RetryPolicy {
    pub fn new(max_attempts: usize, backoff: Backoff) -> Self {
        Self { max_attempts, backoff }
    }
}

/// A single step in the workflow graph. Edges are the explicit `needs` list;
/// if a `needs` step is filtered out by `enabled`, that edge is ignored (so a
/// conditional dependency like LocalDub's asr↔separate_after just vanishes).
#[derive(Clone)]
pub struct StepSpec {
    pub id: String,
    pub label: Option<String>,
    pub needs: Vec<String>,
    pub enabled: Option<StepEnabledFn>,
    pub resource: Option<StepResourceFn>,
    pub retry: Option<RetryPolicy>,
    pub timeout: Option<Duration>,
    pub up_to_date: Option<StepUpToDateFn>,
    pub run: StepRunFn,
}

impl StepSpec {
    pub fn new(id: impl Into<String>, run: StepRunFn) -> Self {
        Self {
            id: id.into(),
            label: None,
            needs: Vec::new(),
            enabled: None,
            resource: None,
            retry: None,
            timeout: None,
            up_to_date: None,
            run,
        }
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn needs(mut self, needs: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.needs = needs.into_iter().map(Into::into).collect();
        self
    }

    pub fn enabled(mut self, enabled: impl Fn(&StepContext) -> bool + Send + Sync + 'static) -> Self {
        self.enabled = Some(Arc::new(enabled));
        self
    }

    pub fn resource(mut self, resource: impl Fn(&StepContext) -> String + Send + Sync + 'static) -> Self {
        self.resource = Some(Arc::new(resource));
        self
    }

    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = Some(retry);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn up_to_date(
        mut self,
        up_to_date: impl Fn(&StepContext, &StepState) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.up_to_date = Some(Arc::new(up_to_date));
        self
    }
}

/// A declared workflow: an immutable graph of [`StepSpec`]s plus an optional
/// finalize step that derives the run output from the completed step states.
#[derive(Clone)]
pub struct Workflow {
    pub id: String,
    pub version: Option<String>,
    pub steps: Vec<StepSpec>,
    pub finalize: Option<Arc<dyn Fn(&HashMap<String, StepState>) -> anyhow::Result<Option<serde_json::Value>> + Send + Sync>>,
    validated: bool,
}

impl Workflow {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
            steps: Vec::new(),
            finalize: None,
            validated: false,
        }
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn step(mut self, spec: StepSpec) -> Self {
        self.steps.push(spec);
        self
    }

    pub fn finalize_with(
        mut self,
        f: impl Fn(&HashMap<String, StepState>) -> anyhow::Result<Option<serde_json::Value>> + Send + Sync + 'static,
    ) -> Self {
        self.finalize = Some(Arc::new(f));
        self
    }

    /// Structural validation: unique ids, all `needs` reference known steps,
    /// and the full graph is acyclic. Called at `run` time (and idempotent,
    /// so safe to call defensively).
    pub fn validate(&mut self) -> Result<(), WorkflowError> {
        if self.validated {
            return Ok(());
        }
        let mut ids: HashSet<&str> = HashSet::new();
        for step in &self.steps {
            if !ids.insert(step.id.as_str()) {
                return Err(WorkflowError::Validation(format!(
                    "duplicate step id `{}`",
                    step.id
                )));
            }
        }
        for step in &self.steps {
            for need in &step.needs {
                if !ids.contains(need.as_str()) {
                    return Err(WorkflowError::Validation(format!(
                        "step `{}` needs unknown step `{}`",
                        step.id, need
                    )));
                }
            }
        }
        // Cycle detection via Kahn's algorithm over all edges.
        let mut in_degree: HashMap<&str, usize> = ids.iter().map(|id| (*id, 0usize)).collect();
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
        for step in &self.steps {
            for need in &step.needs {
                in_degree.entry(step.id.as_str()).and_modify(|d| *d += 1);
                dependents.entry(need.as_str()).or_default().push(step.id.as_str());
            }
        }
        let mut queue: Vec<&str> = ids
            .iter()
            .filter(|id| in_degree[*id] == 0)
            .copied()
            .collect();
        let mut visited = 0usize;
        while let Some(id) = queue.pop() {
            visited += 1;
            if let Some(deps) = dependents.get(id) {
                for d in deps {
                    let deg = in_degree.entry(d).or_insert(0);
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push(d);
                    }
                }
            }
        }
        if visited != ids.len() {
            let cyclic: Vec<&str> = ids
                .iter()
                .filter(|id| in_degree[*id] > 0)
                .copied()
                .collect();
            return Err(WorkflowError::Validation(format!(
                "cycle detected involving steps: {}",
                cyclic.join(", ")
            )));
        }
        self.validated = true;
        Ok(())
    }
}