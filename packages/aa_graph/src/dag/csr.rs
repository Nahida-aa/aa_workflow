// aa_graph/src/dag/csr.rs
use crate::dag::{edge::EdgeStore, idx::IndexType};

/// 正向 CSR（Compressed Sparse Row，压缩稀疏行）。
///
/// CSR 用两个数组紧凑地存储所有边：
///
/// - `offsets`：长度 `V + 1`，`offsets[i]` 是节点 `i` 的出边在 `targets` 中的起点；
///   `offsets[i + 1] - offsets[i]` 就是节点 `i` 的出度。
/// - `targets`：长度 `E`，按源节点顺序连续存放所有边的目标节点。
/// - `edge_data`：长度 `E`，与 `targets` 一一对应的边数据；`E = ()` 时零开销。
///
/// 相比邻接表，CSR 没有指针、没有堆分配，邻居连续存储，缓存友好。
/// 代价是构建后不可增删边——增删需要重建整张表。
///
/// # 示例
///
/// ```text
/// 0 → 1, 2            0 -> 1
/// 1 → 2       or      0 -> 2
/// 2 → 3               1 -> 2
///                     2 -> 3
/// ```
///
/// 对应的 CSR：
///
/// ```text
/// offsets = [0, 2, 3, 4, 4]   // 长度 V+1 = 5
/// targets = [1, 2, 2, 3]      // 长度 E = 4
/// ```
///
/// 节点 `i` 的后继是 `targets[offsets[i]..offsets[i+1]]`：
///
/// | 节点 | 区间 | 后继 |
/// |---|---|---|
/// | 0 | `targets[0..2]` | `[1, 2]` |
/// | 1 | `targets[2..3]` | `[2]` |
/// | 2 | `targets[3..4]` | `[3]` |
/// | 3 | `targets[4..4]` | `[]` |
///
/// # 方法分类
///
/// ## 标准 CSR 操作
///
/// - [`out_degree`](Self::out_degree)：节点出度 = `offsets[i+1] - offsets[i]`
/// - [`successors`](Self::successors)：节点的所有后继 = `targets[offsets[i]..offsets[i+1]]`
/// - [`edge_count`](Self::edge_count)：边总数 = `targets.len()`
///
/// ## 带边数据的扩展
///
/// - [`edge_target`](Self::edge_target)：按边位置取目标节点
/// - [`edge_data_of`](Self::edge_data_of)：按边位置取边数据
/// - [`edges_of`](Self::edges_of)：遍历节点的出边，产出 `(边 ID, 目标节点)`
/// - [`edges_with_data_of`](Self::edges_with_data_of)：遍历节点的出边，产出 `(目标节点, 边数据)`
///
/// # 位置 vs ID
///
/// 所有方法接收的 `n` / `e` 是 **CSR 内部位置**（`usize`），不是图元素 ID。
/// 图元素 ID 由上层 `Dag` 用 `Ix` 表示，调用时通过 `to_usize()` 转换。
pub struct Csr<E, Ix> {
    pub(crate)  offsets: Box<[Ix]>,
    pub(crate)  targets: Box<[Ix]>,
    pub(crate)  edge_data: EdgeStore<E>,
}

impl<E, Ix: IndexType> Csr<E, Ix> {
    // ---------- 计数：usize ----------
    #[inline]
    pub fn edge_count(&self) -> usize {
        self.targets.len()
    }
    // 出度: 节点 i 的出边数量
    #[inline]
    pub fn out_degree(&self, n_i: usize) -> usize {
        self.offsets[n_i + 1].to_usize() - self.offsets[n_i].to_usize()
    }
    /// 节点 id 的后继节点
    #[inline]
    pub fn successors(&self, n_i: usize) -> &[Ix] {
        let start = self.offsets[n_i].to_usize();
        let end = self.offsets[n_i + 1].to_usize();
        &self.targets[start..end]
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
    /// 遍历节点 i 的出边，返回 (目标节点 ID, 边数据)
    #[inline]
    pub fn edges_with_data_of(&self, i: usize)
        -> impl Iterator<Item = (Ix, Option<&E>)> + '_
    {
        let start = self.offsets[i].to_usize();
        let end = self.offsets[i + 1].to_usize();
        let edge_data = &self.edge_data;
        self.targets[start..end].iter().copied().enumerate()
            .map(move |(local, t)| (t, edge_data.get(start + local)))
    }

    /// 边 ix 的目标
    #[inline]
    pub fn edge_target(&self, e_i: usize) -> Ix {
        self.targets[e_i]
    }
    /// 按全局边下标取边数据
    #[inline]
    pub fn edge_data_at(&self, i: usize) -> Option<&E> {
        self.edge_data.get(i)
    }

    /// 边下标区间，供 sub_dag 等需要旧边索引的场景 \
    /// 节点 i 的出边在 targets 中的全局下标区间 [start, end)
    #[inline]
    pub fn range_of(&self, i: usize) -> (usize, usize) {
        (self.offsets[i].to_usize(), self.offsets[i + 1].to_usize())
    }

    /// 从已按 from 排序的边构建 CSR。
    pub(crate) fn from_sorted_edges<It>(node_count: usize, edges: It) -> Self
    where
        It: IntoIterator<Item = (Ix, Ix, E)>,
    {
        let edges: Vec<_> = edges.into_iter().collect();
        let e = edges.len();

        let mut offsets = vec![Ix::from_usize(0); node_count + 1];
        for (from, _, _) in &edges {
            let i = from.to_usize() + 1;
            offsets[i] = Ix::from_usize(offsets[i].to_usize() + 1);
        }
        for i in 1..=node_count {
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
               for succ in csr.successors(u) {
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
               for succ in csr.successors(u) {
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
