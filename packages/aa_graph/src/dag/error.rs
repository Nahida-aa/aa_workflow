// ============================================================
// 错误
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagError {
    CycleDetected,
    NodeNotFound,
    SelfLoop,
}

impl std::fmt::Display for DagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagError::CycleDetected => write!(f, "cycle detected in DAG"),
            DagError::NodeNotFound => write!(f, "node not found"),
            DagError::SelfLoop => write!(f, "self-loop is not allowed in DAG"),
        }
    }
}

impl std::error::Error for DagError {}
