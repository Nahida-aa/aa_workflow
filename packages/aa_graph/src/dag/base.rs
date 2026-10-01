// dag/base.rs

use std::marker::PhantomData;

use crate::dag::csr::{Csr, RevCsr};


// ============================================================
// 核心类型
// ============================================================

/// 通用 DAG 类型。
///
/// - `Cap`：能力，第一个泛型参数
/// - `N`：节点数据
/// - `E`：边数据，默认 `()`
/// - `Ix`：索引类型，默认 `u32`
pub struct Dag<Cap, N = (), E = (), Ix = u32> {
   pub nodes: Box<[N]>,
   pub csr: Csr<E, Ix>,
   pub rev: Option<RevCsr<Ix>>,
   pub(crate) _cap: PhantomData<Cap>,
}
