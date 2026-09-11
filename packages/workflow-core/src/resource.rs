use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A named resource that nodes contend on (e.g. `"gpu:0"`, `"cpu"`,
/// `"cloud"`, or `format!("gpu:{}", device_id)` derived from input params).
pub type ResourceKey = String;

#[derive(Default)]
struct GateInner {
    /// How many in-flight holders each key currently has.
    in_use: HashMap<ResourceKey, usize>,
}

/// Capacity-1 semaphore per resource key, used to serialize nodes that share
/// a physical resource (halving VRAM pressure in LocalDub).
///
/// `try_acquire` never blocks: the scheduler makes the dispatch decision
/// synchronously, so busy resources just leave the node queued while other
/// ready nodes are considered (avoiding head-of-line blocking).
#[derive(Default)]
pub struct Gate {
    inner: Mutex<GateInner>,
}

impl Gate {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(GateInner {
                in_use: HashMap::new(),
            }),
        }
    }

    pub fn try_acquire(self: &Arc<Self>, key: &ResourceKey) -> Option<GateGuard> {
        let mut inner = self.inner.lock().ok()?;
        let count = inner.in_use.entry(key.clone()).or_insert(0);
        if *count >= 1 {
            return None;
        }
        *count += 1;
        Some(GateGuard {
            gate: self.clone(),
            key: key.clone(),
        })
    }
}

/// Holds one unit of a resource for the lifetime of a running node. Released
/// on drop (before the worker signals completion, so dependents can start as
/// soon as the node finishes).
pub struct GateGuard {
    gate: Arc<Gate>,
    key: ResourceKey,
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.gate.inner.lock() {
            if let Some(count) = inner.in_use.get_mut(&self.key) {
                *count -= 1;
                if *count == 0 {
                    inner.in_use.remove(&self.key);
                }
            }
        }
    }
}