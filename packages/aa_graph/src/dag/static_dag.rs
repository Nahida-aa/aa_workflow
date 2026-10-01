use std::{ marker::PhantomData};

use crate::dag::{base::Dag, cap::Static, csr::{Csr, RevCsr}, edge::EdgeStore, idx::IndexType};

/// 静态 DAG 的对外别名
pub type StaticDag<N = (), E = (), Ix = u32> = Dag<Static, N, E, Ix>;

// ============================================================
// 静态 DAG 的只读 API
// ============================================================

impl<N, E, Ix: IndexType> Dag<Static, N, E, Ix> {
    #[inline]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    pub fn edge_count(&self) -> usize {
        self.csr.edge_count()
    }

    /// 获取节点数据（越界会 panic）
    #[inline]
    pub fn node(&self, id: Ix) -> &N {
        &self.nodes[id.to_usize()]
    }

    /// 遍历所有节点
    pub fn nodes(&self) -> impl Iterator<Item = (Ix, &N)> + '_ {
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (Ix::from_usize(i), n))
    }

    #[inline]
    pub fn contains_node(&self, id: Ix) -> bool {
        id.to_usize() < self.nodes.len()
    }

    /// 是否已构建反向 CSR
    #[inline]
    pub fn has_reverse(&self) -> bool {
        self.rev.is_some()
    }

    /// 出边目标节点迭代器（零分配）
    ///
    /// - successor: 后继
    /// - successors: 从一条边出发可达的节点集合
    #[inline]
    pub fn successors(&self, id: Ix) -> impl Iterator<Item = Ix> + '_ {
        self.csr.successors(id.to_usize()).iter().copied()
    }

    /// 出度: 从一个节点出发的边的数量(从节点出发相邻的节点数)
    #[inline]
    pub fn out_degree(&self, id: Ix) -> usize {
        self.csr.out_degree(id.to_usize())
    }

    /// 出边目标与边数据的迭代器
    pub fn successors_with_data(
        &self,
        id: Ix,
    ) -> impl Iterator<Item = (Ix, Option<&E>)> + '_ {
       self.csr.edges_with_data_of(id.to_usize())
    }
    // ---------- 反向遍历 ----------

    /// 入边迭代器。若未构建反向 CSR，返回 None。
    pub fn predecessors(&self, id: Ix) -> Option<impl Iterator<Item = Ix> + '_> {
        let rev = self.rev.as_ref()?;
        Some(rev.predecessors_of(id.to_usize()).iter().copied())
    }

    /// 入度。若未构建反向 CSR，返回 None。
    pub fn in_degree(&self, id: Ix) -> Option<usize> {
        self.rev.as_ref().map(|r| r.in_degree(id.to_usize()))
    }

    // ---------- 拓扑序 ----------
    // #[inline]
    // pub fn topo_order(&self) -> &[Ix] {
    //     &self.topo_order
    // }

    // ---------- 可达性 ----------

    /// 从 `from` 中所有节点开始，标记所有可达节点（含起点）。
    ///
    /// 前提：节点按拓扑序编号（`freeze()` 保证）。
    /// `visited` 长度必须等于 `node_count()`。
    pub(crate) fn mark_descendants(&self, from: &[Ix], visited: &mut [bool]) {
        let n = self.node_count();
        debug_assert_eq!(visited.len(), n);

        // 标记起点，同时找到最小位置，缩小扫描范围
        let mut min_pos = n;
        for &f in from {
            let i = f.to_usize();
            if i < n {
                visited[i] = true;
                if i < min_pos {
                    min_pos = i;
                }
            }
        }

        // 按拓扑序传播：ID 小的先处理，保证后继被标记时能被继续传播
        for u in min_pos..n {
            if visited[u] {
                for v in self.successors(Ix::from_usize(u)) {
                    visited[v.to_usize()] = true;
                }
            }
        }
    }

    /// 从 `from` 出发的所有可达节点（不含 `from` 自身）
    /// 返回按 ID 升序排列。
    pub fn descendants(&self, from: Ix) -> Vec<Ix> {
        let n = self.node_count();
        let mut visited = vec![false; n];
        self.mark_descendants(std::slice::from_ref(&from), &mut visited);

        let f = from.to_usize();
        (0..n)
            .filter(|&i| visited[i] && i != f)
            .map(Ix::from_usize)
            .collect()
    }
    /// 从 `from` 集合出发可达的所有节点，不含这些起点自身。
    pub fn descendants_from(&self, from: &[Ix]) -> Vec<Ix> {
        let n = self.node_count();
        let mut visited = vec![false; n];
        self.mark_descendants(from, &mut visited);

        let mut excluded = vec![false; n];
        for &f in from {
            let i = f.to_usize();
            if i < n {
                excluded[i] = true;
            }
        }

        (0..n)
            .filter(|&i| visited[i] && !excluded[i])
            .map(Ix::from_usize)
            .collect()
    }
    /// 从 `to` 中所有节点开始，沿入边反向遍历，
    /// 标记所有可达节点（含终点）。
    ///
    /// 前提：节点按拓扑序编号（`freeze()` 保证）。
    /// `visited` 长度必须等于 `node_count()`，调用者负责清空。
    pub(crate) fn mark_ancestors(&self, to: &[Ix], visited: &mut [bool]) {
        let n = self.node_count();
        debug_assert_eq!(visited.len(), n);

        // 标记终点，同时找到最大位置，缩小反向扫描范围
        let mut max_pos = 0usize;
        let mut any = false;
        for &t in to {
            let i = t.to_usize();
            if i < n {
                visited[i] = true;
                if !any || i > max_pos {
                    max_pos = i;
                    any = true;
                }
            }
        }
        if !any {
            return;
        }

        if self.has_reverse() {
            // 从 max_pos 反向扫到 0，靠拓扑序保证前驱已被标记
            for u in (0..=max_pos).rev() {
                if visited[u] {
                    for pred in self.predecessors(Ix::from_usize(u)).unwrap() {
                        visited[pred.to_usize()] = true;
                    }
                }
            }
        } else {
            // 临时构建反向 CSR，复用 RevCsr::from_csr
            let rev = RevCsr::from_csr(&self.csr, n);
            for u in (0..=max_pos).rev() {
                if visited[u] {
                    for pred in rev.predecessors_of(u) {
                        visited[pred.to_usize()] = true;
                    }
                }
            }
        }
    }
    /// 能到达 `to` 的所有节点，不含 `to` 自身。
    /// 返回按 ID 升序排列。
    pub fn ancestors(&self, to: Ix) -> Vec<Ix> {
        let n = self.node_count();
        let mut visited = vec![false; n];
        self.mark_ancestors(std::slice::from_ref(&to), &mut visited);

        let t = to.to_usize();
        (0..n)
            .filter(|&i| visited[i] && i != t)
            .map(Ix::from_usize)
            .collect()
    }
    pub fn ancestors_to(&self, to: &[Ix]) -> Vec<Ix> {
        let n = self.node_count();
        let mut visited = vec![false; n];
        self.mark_ancestors(to, &mut visited);

        let mut excluded = vec![false; n];
        for &t in to {
            let i = t.to_usize();
            if i < n {
                excluded[i] = true;
            }
        }

        (0..n)
            .filter(|&i| visited[i] && !excluded[i])
            .map(Ix::from_usize)
            .collect()
    }
    // ---------- 子 DAG 提取 ----------

    /// 计算子图节点对应的原图 ID 列表
    pub fn sub_dag_node_ids(&self, from: &[Ix], to: &[Ix]) -> Vec<Ix> {
        let n = self.node_count();
         let mut reach_from = vec![false; n];
         let mut reach_to = vec![false; n];

         self.mark_descendants(from, &mut reach_from);
         self.mark_ancestors(to, &mut reach_to);


        (0..n)
            .filter(|&i| reach_from[i] && reach_to[i])
            .map(Ix::from_usize)
            .collect()
    }

    pub fn sub_dag(&self, from: &[Ix], to: &[Ix]) -> StaticDag<N, E, Ix>
    where
        N: Clone,
        E: Clone,
    {
        let ids = self.sub_dag_node_ids(from, to);
        self.sub_dag_from_ids(&ids)
    }

    pub fn sub_dag_with_ids(
        &self,
        from: &[Ix],
        to: &[Ix],
    ) -> (StaticDag<N, E, Ix>, Vec<Ix>)
    where
        N: Clone,
        E: Clone,
    {
        let ids = self.sub_dag_node_ids(from, to);
        let sub = self.sub_dag_from_ids(&ids);
        (sub, ids)
    }

    /// 内部：已有原图 ID 列表时构建子图
    fn sub_dag_from_ids(&self, ids: &[Ix]) -> StaticDag<N, E, Ix>
    where
        N: Clone,
        E: Clone,
    {
        let new_count = ids.len();
        let n = self.nodes.len();

        // 原图 ID -> 新图 ID，只用于内部边遍历
        let mut old_to_new: Vec<Option<Ix>> = vec![None; n];
        for (new_id, &old_id) in ids.iter().enumerate() {
            old_to_new[old_id.to_usize()] = Some(Ix::from_usize(new_id));
        }

        // 收集保留边：(new_from, new_to, old_edge_id)
        // ids 升序 + 原图拓扑序 ⇒ new_edges 已按 new_from 排序
        let mut new_edges: Vec<(Ix, Ix, Ix)> = Vec::new();
        for (new_from, &old_from) in ids.iter().enumerate() {
            let u_new = Ix::from_usize(new_from);
            for (old_edge_id, tgt) in self.csr.edges_of(old_from.to_usize()) {
                if let Some(v_new) = old_to_new[tgt.to_usize()] {
                    new_edges.push((u_new, v_new, old_edge_id));
                }
            }
        }


        // 新 offsets
        let mut new_offsets: Vec<Ix> = vec![Ix::from_usize(0); new_count + 1];
        for &(f, _, _) in &new_edges {
            let i = f.to_usize() + 1;
            new_offsets[i] = Ix::from_usize(new_offsets[i].to_usize() + 1);
        }
        for i in 1..=new_count {
            new_offsets[i] = Ix::from_usize(
                new_offsets[i].to_usize() + new_offsets[i - 1].to_usize(),
            );
        }

        // 填 targets / edge_data
        let mut new_targets: Vec<Ix> = Vec::with_capacity(new_edges.len());
        let mut new_edge_data: Vec<E> = Vec::with_capacity(new_edges.len());
        for &(_, v_new, old_edge_id) in &new_edges {
            new_targets.push(v_new);
            if let Some(d) = self.csr.edge_data_at(old_edge_id.to_usize()) {
                new_edge_data.push(d.clone());
            }
        }
        // 节点数据：按 ids 顺序取
        let new_nodes: Vec<N> = ids
            .iter()
            .map(|&old| self.nodes[old.to_usize()].clone())
            .collect();

        Dag {
            nodes: new_nodes.into_boxed_slice(),
            csr: Csr {
                offsets: new_offsets.into_boxed_slice(),
                targets: new_targets.into_boxed_slice(),
                edge_data: EdgeStore::from_vec(new_edge_data),
            },
            rev: None,
            _cap: PhantomData,
        }

    }

}
