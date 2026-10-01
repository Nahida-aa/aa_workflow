// ============================================================
// 构建器
// ============================================================

use std::marker::PhantomData;

use crate::dag::{base::Dag, csr::{Csr, RevCsr}, edge::EdgeStore, error::DagError, idx::IndexType,  static_dag::StaticDag, topo_sort_alg::topo_sort};

pub struct DagBuilder<N, E = (), Ix = u32> {
   pub(crate) nodes: Vec<N>,
   pub(crate) edges: Vec<(Ix, Ix, E)>,
   pub build_reverse: bool,
}

impl<N, E, Ix: IndexType> DagBuilder<N, E, Ix> {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            build_reverse: false,
        }
    }

    /// 开启反向 CSR 构建（用于 ancestors / sub_dag） /
    /// 开启后 `ancestors` / `sub_dag` 无需临时构建反向邻接表； /
    /// 代价是 `Dag` 额外占用 `V × size_of::<Ix>() + E × size_of::<Ix>()` 字节。
     pub fn with_reverse(mut self, on: bool) -> Self {
         self.build_reverse = on;
         self
     }

    /// 添加节点，返回 NodeId
    pub fn add_node(&mut self, data: N) -> Ix {
        let id = Ix::from_usize(self.nodes.len());
        self.nodes.push(data);
        id
    }

    /// 添加有向边。
    ///
    /// - 立即拒绝自环（`SelfLoop`）；
    /// - 其他成环情况在 `freeze()` 时统一检测（`CycleDetected`）。
    pub fn add_edge(&mut self, from: Ix, to: Ix, data: E) -> Result<(), DagError> {
        if from.to_usize() >= self.nodes.len() || to.to_usize() >= self.nodes.len() {
            return Err(DagError::NodeNotFound);
        }
        if from == to {
            return Err(DagError::SelfLoop);
        }
        self.edges.push((from, to, data));
        Ok(())
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// 冻结，按拓扑序重排节点。
     /// `Dag` 最轻，不保留原 ID 映射。
     /// 节点 ID 就是拓扑序位置，不等于 `add_node` 顺序。
     pub fn freeze(self) -> Result<StaticDag<N, E, Ix>, DagError> {
         self.freeze_inner().map(|(dag, _)| dag)
     }

     /// 冻结，按拓扑序重排节点，并返回 `new_id -> old_id` 映射。
     /// 映射由用户保存，`Dag` 不存。
     pub fn freeze_with_map(
         self,
     ) -> Result<(StaticDag<N, E, Ix>, Vec<Ix>), DagError> {
         self.freeze_inner()
     }
    fn freeze_inner(mut self) -> Result<(StaticDag<N, E, Ix>, Vec<Ix>), DagError> {
        let v = self.nodes.len();

        // ---------- 1. 拓扑排序，顺便检测环 ----------
        // topo_order[k] = 原图中第 k 个拓扑位置的节点 ID
        let topo_order = topo_sort(v, &self.edges)?;

        // ---------- 2. old_id -> new_id ----------
        let mut old_to_new: Vec<Ix> = vec![Ix::from_usize(0); v];
        for (new_id, &old_id) in topo_order.iter().enumerate() {
            old_to_new[old_id.to_usize()] = Ix::from_usize(new_id);
        }

        // ---------- 3. 节点 in-place 重排 ----------
        // 目标：new_nodes[k] = old_nodes[topo_order[k]]
        // 用循环置换，只多一个 Vec<bool>
        {
            let nodes = &mut self.nodes;
            let mut visited = vec![false; v];
            for i in 0..v {
                if visited[i] {
                    continue;
                }
                let mut j = i;
                loop {
                    visited[j] = true;
                    let src = topo_order[j].to_usize();
                    if visited[src] {
                        break;
                    }
                    nodes.swap(j, src);
                    j = src;
                }
            }
        }

        // ---------- 4. 重排边的 from/to ----------
        for (from, to, _) in self.edges.iter_mut() {
            *from = old_to_new[from.to_usize()];
            *to = old_to_new[to.to_usize()];
        }
        //  5. 按 from 排序（关键！）
        self.edges.sort_by_key(|(from, _, _)| from.to_usize());
        // 6. 构建正向 CSR
        let csr = Csr::from_sorted_edges(v, self.edges.drain(..));

        // 7. 构建反向 CSR（如果需要）
        let rev = if self.build_reverse {
            Some(RevCsr::from_csr(&csr, v))
        } else {
            None
        };


        let dag = Dag {
            nodes: std::mem::take(&mut self.nodes).into_boxed_slice(),
            csr,
            rev,
            _cap: PhantomData,
        };

        Ok((dag, topo_order))
    }
}

impl<N, E, Ix: IndexType> Default for DagBuilder<N, E, Ix> {
    fn default() -> Self {
        Self::new()
    }
}
