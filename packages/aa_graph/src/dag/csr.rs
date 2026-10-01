// aa_graph/src/dag/scr.rs
use crate::dag::{edge::EdgeStore, idx::IndexType};

/// 正向 CSR：offsets + targets + 可选的边数据
pub struct Csr<E, Ix> {
    pub(crate)  offsets: Box<[Ix]>,
    pub(crate)  targets: Box<[Ix]>,
    pub(crate)  edge_data: EdgeStore<E>,
}

impl<E, Ix: IndexType> Csr<E, Ix> {
    // ---------- 计数：usize ----------
    #[inline]
    pub fn out_degree(&self, i: usize) -> usize {
        self.offsets[i + 1].to_usize() - self.offsets[i].to_usize()
    }
    #[inline]
    pub fn edge_count(&self) -> usize {
        self.targets.len()
    }

    /// 按全局边下标取目标节点
    #[inline]
    pub fn target_at(&self, i: usize) -> Ix {
        self.targets[i]
    }
    /// 节点 id 的所有后继节点
    #[inline]
    pub fn targets_of(&self, i: usize) -> &[Ix] {
        let start = self.offsets[i].to_usize();
        let end = self.offsets[i + 1].to_usize();
        &self.targets[start..end]
    }
    /// 按全局边下标取边数据
    #[inline]
    pub fn edge_data_at(&self, i: usize) -> Option<&E> {
        self.edge_data.get(i)
    }

    /// 遍历节点 i 的所有出边，返回 (边ID, 目标节点ID)
    #[inline]
    pub fn edges_of(&self, i: usize) -> impl Iterator<Item = (Ix, Ix)> + '_ {
        let start = self.offsets[i].to_usize();
        let end = self.offsets[i + 1].to_usize();
        self.targets[start..end].iter().copied()
            .enumerate()
            .map(move |(local, t)| (Ix::from_usize(start + local), t))
    }

    /// 边下标区间，供 sub_dag 等需要旧边索引的场景 \
    /// 节点 i 的出边在 targets 中的全局下标区间 [start, end)
    #[inline]
    pub fn range_of(&self, i: usize) -> (usize, usize) {
        (self.offsets[i].to_usize(), self.offsets[i + 1].to_usize())
    }

    /// 从已按 from 排序的边构建 CSR。
    pub(crate) fn from_sorted_edges<It>(v: usize, edges: It) -> Self
    where
        It: IntoIterator<Item = (Ix, Ix, E)>,
    {
        let edges: Vec<_> = edges.into_iter().collect();
        let e = edges.len();

        let mut offsets = vec![Ix::from_usize(0); v + 1];
        for (from, _, _) in &edges {
            let i = from.to_usize() + 1;
            offsets[i] = Ix::from_usize(offsets[i].to_usize() + 1);
        }
        for i in 1..=v {
            offsets[i] = Ix::from_usize(
                offsets[i].to_usize() + offsets[i - 1].to_usize(),
            );
        }

        let mut targets = Vec::with_capacity(e);
        let mut edge_data = Vec::with_capacity(e);
        for (_, to, data) in edges {
            targets.push(to);
            edge_data.push(data);
        }

        Csr {
            offsets: offsets.into_boxed_slice(),
            targets: targets.into_boxed_slice(),
            edge_data: EdgeStore::from_vec(edge_data),
        }
    }
}

/// 反向 CSR：只有 offsets + targets
pub struct RevCsr<Ix> {
    pub(crate)  offsets: Box<[Ix]>,
    pub(crate)  targets: Box<[Ix]>,
}

impl<Ix: IndexType> RevCsr<Ix> {
    #[inline]
    pub fn in_degree(&self, i: usize) -> usize {
        self.offsets[i + 1].to_usize() - self.offsets[i].to_usize()
    }

    #[inline]
    pub fn predecessors_of(&self, i: usize) -> &[Ix] {
        let start = self.offsets[i].to_usize();
        let end = self.offsets[i + 1].to_usize();
        &self.targets[start..end]
    }
    /// 从正向 CSR 构建反向 CSR。
       ///
       /// 遍历所有边一次，统计入度、前缀和、填 targets。
       pub(crate) fn from_csr<E>(csr: &Csr<E, Ix>, v: usize) -> Self {
           let e = csr.edge_count();

           // 1. 统计入度
           let mut offsets = vec![Ix::from_usize(0); v + 1];
           for u in 0..v {
               for succ in csr.targets_of(u) {
                   let i = succ.to_usize() + 1;
                   offsets[i] = Ix::from_usize(offsets[i].to_usize() + 1);
               }
           }

           // 2. 前缀和
           for i in 1..=v {
               offsets[i] = Ix::from_usize(
                   offsets[i].to_usize() + offsets[i - 1].to_usize(),
               );
           }

           // 3. 填 targets
           let mut targets = vec![Ix::from_usize(0); e];
           let mut cursor: Vec<usize> =
               offsets[..v].iter().map(|x| x.to_usize()).collect();
           for u in 0..v {
               for succ in csr.targets_of(u) {
                   let t = succ.to_usize();
                   targets[cursor[t]] = Ix::from_usize(u);
                   cursor[t] += 1;
               }
           }

           RevCsr {
               offsets: offsets.into_boxed_slice(),
               targets: targets.into_boxed_slice(),
           }
       }
}
