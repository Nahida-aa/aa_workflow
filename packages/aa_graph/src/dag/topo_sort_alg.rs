// ============================================================
// 拓扑排序（Kahn 算法）
// ============================================================

use std::collections::VecDeque;

use crate::dag::{error::DagError, idx::IndexType};

pub(crate) fn topo_sort<Ix: IndexType, E>(
    v: usize,
    edges: &[(Ix, Ix, E)],
) -> Result<Vec<Ix>, DagError> {
    let mut in_degree = vec![0usize; v];
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); v];

    for (from, to, _) in edges {
        adj[from.to_usize()].push(to.to_usize());
        in_degree[to.to_usize()] += 1;
    }

    let mut queue: VecDeque<usize> = VecDeque::new();
    for i in 0..v {
        if in_degree[i] == 0 {
            queue.push_back(i);
        }
    }

    let mut order: Vec<Ix> = Vec::with_capacity(v);
    while let Some(u) = queue.pop_front() {
        order.push(Ix::from_usize(u));
        for &n in &adj[u] {
            in_degree[n] -= 1;
            if in_degree[n] == 0 {
                queue.push_back(n);
            }
        }
    }

    if order.len() != v {
        return Err(DagError::CycleDetected);
    }
    Ok(order)
}
