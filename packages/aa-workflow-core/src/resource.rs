use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A named resource that steps contend on (e.g. `"gpu:0"`, `"cpu"`, `"cloud"`).
pub type ResourceKey = String;

#[derive(Default)]
struct GateInner {
    /// Per-key capacity-1 semaphore.
    semaphores: HashMap<ResourceKey, Arc<Semaphore>>,
}

/// Per-key async semaphore, used to serialize steps that share a physical
/// resource (halving VRAM pressure in LocalDub). `acquire` waits rather than
/// failing, because in the code-as-DAG model the handler has nowhere to defer
/// a contended step — it must either proceed or block.
#[derive(Default)]
pub struct Gate {
    inner: Mutex<GateInner>,
}

impl Gate {
    pub fn new() -> Self {
        Self::default()
    }

    fn semaphore(&self, key: &ResourceKey) -> Arc<Semaphore> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.semaphores.get(key).cloned().unwrap_or_else(|| {
            let sem = Arc::new(Semaphore::new(1));
            inner.semaphores.insert(key.clone(), sem.clone());
            sem
        })
    }

    /// Async acquire of one unit for `key`, waiting until the previous holder
    /// releases. The returned [`GateGuard`] (one permit) is released on drop,
    /// before the step's checkpoint is appended.
    pub async fn acquire(self: &Arc<Self>, key: &ResourceKey) -> GateGuard {
        let sem = self.semaphore(key);
        let permit = sem
            .acquire_owned()
            .await
            .unwrap_or_else(|_| unreachable!("resource semaphore is never closed"));
        GateGuard { _permit: permit }
    }
}

/// Holds one unit of a resource until dropped.
pub struct GateGuard {
    _permit: OwnedSemaphorePermit,
}
