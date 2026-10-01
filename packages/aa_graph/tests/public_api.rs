// packages/aa_graph/tests/public_api.rs
//! 公开 API 集成测试。
//!
//! 只使用 crate 对外暴露的类型和方法。
//! 假设 crate 路径为 `aa_graph::dag`。

use aa_graph::dag::{DagBuilder, DagError, StaticDag};

// ============================================================
// 辅助
// ============================================================

/// 链 A → B → C。返回 (dag, map)，map[new_id] = old_id。
fn build_chain() -> (StaticDag<&'static str>, Vec<u32>) {
    let mut b = DagBuilder::<&'static str>::new();
    let a = b.add_node("A");
    let b_ = b.add_node("B");
    let c = b.add_node("C");
    b.add_edge(a, b_, ()).unwrap();
    b.add_edge(b_, c, ()).unwrap();
    b.freeze_with_map().unwrap()
}

/// 菱形 A → B → D, A → C → D。
fn build_diamond() -> (StaticDag<&'static str>, Vec<u32>) {
    let mut b = DagBuilder::<&'static str>::new();
    let a = b.add_node("A");
    let b_ = b.add_node("B");
    let c = b.add_node("C");
    let d = b.add_node("D");
    b.add_edge(a, b_, ()).unwrap();
    b.add_edge(a, c, ()).unwrap();
    b.add_edge(b_, d, ()).unwrap();
    b.add_edge(c, d, ()).unwrap();
    b.freeze_with_map().unwrap()
}

/// 原 ID -> 新 ID。
fn new_id_of(map: &[u32], old_id: u32) -> u32 {
    map.iter().position(|&x| x == old_id).unwrap() as u32
}

// ============================================================
// 构建
// ============================================================

#[test]
fn freeze_without_map() {
    let mut b = DagBuilder::<i32>::new();
    let a = b.add_node(0);
    let b_ = b.add_node(1);
    b.add_edge(a, b_, ()).unwrap();
    let dag: StaticDag<i32> = b.freeze().unwrap();
    assert_eq!(dag.node_count(), 2);
    assert_eq!(dag.edge_count(), 1);
}

#[test]
fn build_simple_chain() {
    let (dag, map) = build_chain();
    assert_eq!(dag.node_count(), 3);
    assert_eq!(dag.edge_count(), 2);

    let a = new_id_of(&map, 0);
    let b = new_id_of(&map, 1);
    let c = new_id_of(&map, 2);

    assert_eq!(dag.node(a), &"A");
    assert_eq!(dag.node(b), &"B");
    assert_eq!(dag.node(c), &"C");

    // 链的拓扑序唯一，A < B < C
    assert!(a < b);
    assert!(b < c);
}

// ============================================================
// 错误
// ============================================================

#[test]
fn detect_cycle() {
    let mut b = DagBuilder::<i32>::new();
    let a = b.add_node(0);
    let b_ = b.add_node(1);
    let c = b.add_node(2);
    b.add_edge(a, b_, ()).unwrap();
    b.add_edge(b_, c, ()).unwrap();
    b.add_edge(c, a, ()).unwrap();
    assert!(matches!(b.freeze(), Err(DagError::CycleDetected)));
}

#[test]
fn self_loop_rejected() {
    let mut b = DagBuilder::<i32>::new();
    let a = b.add_node(0);
    assert!(matches!(b.add_edge(a, a, ()), Err(DagError::SelfLoop)));
}

#[test]
fn node_not_found() {
    let mut b = DagBuilder::<i32>::new();
    b.add_node(0);
    assert!(matches!(
        b.add_edge(0, 99, ()),
        Err(DagError::NodeNotFound)
    ));
}

// ============================================================
// 拓扑序不变式
// ============================================================

#[test]
fn topological_invariant_chain() {
    let (dag, _) = build_chain();
    for (u, _) in dag.nodes() {
        for v in dag.successors(u) {
            assert!(u < v, "edge {} -> {} violates topo order", u, v);
        }
    }
}

#[test]
fn topological_invariant_diamond() {
    let (dag, _) = build_diamond();
    for (u, _) in dag.nodes() {
        for v in dag.successors(u) {
            assert!(u < v, "edge {} -> {} violates topo order", u, v);
        }
    }
}

// ============================================================
// 基本查询
// ============================================================

#[test]
fn nodes_and_contains() {
    let (dag, _) = build_chain();
    let all: Vec<_> = dag.nodes().collect();
    assert_eq!(all.len(), 3);

    assert!(dag.contains_node(0));
    assert!(dag.contains_node(2));
    assert!(!dag.contains_node(3));
}

#[test]
fn successors_and_out_degree() {
    let (dag, map) = build_chain();
    let a = new_id_of(&map, 0);
    let b = new_id_of(&map, 1);
    let c = new_id_of(&map, 2);

    assert_eq!(dag.successors(a).collect::<Vec<_>>(), vec![b]);
    assert_eq!(dag.successors(b).collect::<Vec<_>>(), vec![c]);
    assert!(dag.successors(c).next().is_none());

    assert_eq!(dag.out_degree(a), 1);
    assert_eq!(dag.out_degree(b), 1);
    assert_eq!(dag.out_degree(c), 0);
}

// ============================================================
// descendants / ancestors
// ============================================================

#[test]
fn descendants_of_chain() {
    let (dag, map) = build_chain();
    let a = new_id_of(&map, 0);
    let b = new_id_of(&map, 1);
    let c = new_id_of(&map, 2);

    assert_eq!(dag.descendants(a), vec![b, c]);
    assert_eq!(dag.descendants(b), vec![c]);
    assert!(dag.descendants(c).is_empty());
}

#[test]
fn ancestors_of_chain() {
    let (dag, map) = build_chain();
    let a = new_id_of(&map, 0);
    let b = new_id_of(&map, 1);
    let c = new_id_of(&map, 2);

    assert_eq!(dag.ancestors(c), vec![a, b]);
    assert_eq!(dag.ancestors(b), vec![a]);
    assert!(dag.ancestors(a).is_empty());
}

#[test]
fn descendants_of_diamond() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let b = new_id_of(&map, 1);
    let c = new_id_of(&map, 2);
    let d = new_id_of(&map, 3);

    let desc_a = dag.descendants(a);
    assert_eq!(desc_a.len(), 3);
    assert!(desc_a.contains(&b));
    assert!(desc_a.contains(&c));
    assert!(desc_a.contains(&d));
    assert!(!desc_a.contains(&a));

    let desc_b = dag.descendants(b);
    assert_eq!(desc_b, vec![d]);

    assert!(dag.descendants(d).is_empty());
}

#[test]
fn ancestors_of_diamond() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let b = new_id_of(&map, 1);
    let c = new_id_of(&map, 2);
    let d = new_id_of(&map, 3);

    let anc_d = dag.ancestors(d);
    assert_eq!(anc_d.len(), 3);
    assert!(anc_d.contains(&a));
    assert!(anc_d.contains(&b));
    assert!(anc_d.contains(&c));
    assert!(!anc_d.contains(&d));

    let anc_b = dag.ancestors(b);
    assert_eq!(anc_b, vec![a]);

    assert!(dag.ancestors(a).is_empty());
}

#[test]
fn multi_start_descendants() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let b = new_id_of(&map, 1);
    let c = new_id_of(&map, 2);
    let d = new_id_of(&map, 3);

    let desc = dag.descendants_from(&[a]);
    assert_eq!(desc.len(), 3);
    assert!(!desc.contains(&a));

    let desc_bc = dag.descendants_from(&[b, c]);
    assert_eq!(desc_bc, vec![d]);
}

#[test]
fn multi_target_ancestors() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let b = new_id_of(&map, 1);
    let c = new_id_of(&map, 2);
    let d = new_id_of(&map, 3);

    let anc = dag.ancestors_to(&[d]);
    assert_eq!(anc.len(), 3);
    assert!(!anc.contains(&d));

    let anc_bc = dag.ancestors_to(&[b, c]);
    assert_eq!(anc_bc, vec![a]);
}

// ============================================================
// 反向 CSR
// ============================================================

#[test]
fn reverse_csr_predecessors() {
    let mut b = DagBuilder::<&'static str>::new().with_reverse(true);
    let a = b.add_node("A");
    let b_ = b.add_node("B");
    let c = b.add_node("C");
    b.add_edge(a, b_, ()).unwrap();
    b.add_edge(b_, c, ()).unwrap();
    let (dag, map) = b.freeze_with_map().unwrap();

    assert!(dag.has_reverse());

    let a = new_id_of(&map, a);
    let b = new_id_of(&map, b_);
    let c = new_id_of(&map, c);

    assert_eq!(dag.predecessors(c).unwrap().collect::<Vec<_>>(), vec![b]);
    assert!(dag.predecessors(a).unwrap().next().is_none());

    assert_eq!(dag.in_degree(a), Some(0));
    assert_eq!(dag.in_degree(b), Some(1));
    assert_eq!(dag.in_degree(c), Some(1));
}

#[test]
fn without_reverse_no_predecessors() {
    let (dag, _) = build_chain();
    assert!(!dag.has_reverse());
    assert!(dag.predecessors(0).is_none());
    assert!(dag.in_degree(0).is_none());
}

#[test]
fn ancestors_works_without_reverse_csr() {
    let (dag, map) = build_chain();
    assert!(!dag.has_reverse());

    let a = new_id_of(&map, 0);
    let c = new_id_of(&map, 2);

    let anc = dag.ancestors(c);
    assert!(anc.contains(&a));
    assert_eq!(anc.len(), 2);
}

// ============================================================
// 子 DAG
// ============================================================

#[test]
fn sub_dag_full_diamond() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let d = new_id_of(&map, 3);

    let sub = dag.sub_dag(&[a], &[d]);
    assert_eq!(sub.node_count(), 4);
    assert_eq!(sub.edge_count(), 4);
}

#[test]
fn sub_dag_partial_path() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let b = new_id_of(&map, 1);

    // 只取 A → B
    let sub = dag.sub_dag(&[a], &[b]);
    assert_eq!(sub.node_count(), 2);
    assert_eq!(sub.edge_count(), 1);
}

#[test]
fn sub_dag_empty_when_unreachable() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let d = new_id_of(&map, 3);

    // D 到 A 反方向，为空
    let sub = dag.sub_dag(&[d], &[a]);
    assert_eq!(sub.node_count(), 0);
    assert_eq!(sub.edge_count(), 0);
}

#[test]
fn sub_dag_node_ids_sorted() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let d = new_id_of(&map, 3);

    let ids = dag.sub_dag_node_ids(&[a], &[d]);
    assert_eq!(ids.len(), 4);
    // 按升序
    assert!(ids.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn sub_dag_with_ids_roundtrip() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let d = new_id_of(&map, 3);

    let (sub, sub_ids) = dag.sub_dag_with_ids(&[a], &[d]);
    assert_eq!(sub.node_count(), 4);
    assert_eq!(sub_ids.len(), 4);

    // 子图节点 k 对应原图 sub_ids[k]
    for (k, &old) in sub_ids.iter().enumerate() {
        assert_eq!(sub.node(k as u32), dag.node(old));
    }
}

#[test]
fn sub_dag_topological_invariant() {
    let (dag, map) = build_diamond();
    let a = new_id_of(&map, 0);
    let d = new_id_of(&map, 3);

    let sub = dag.sub_dag(&[a], &[d]);
    for (u, _) in sub.nodes() {
        for v in sub.successors(u) {
            assert!(u < v);
        }
    }
}

#[test]
fn sub_dag_chain() {
    let (dag, map) = build_chain();
    let a = new_id_of(&map, 0);
    let c = new_id_of(&map, 2);

    let sub = dag.sub_dag(&[a], &[c]);
    assert_eq!(sub.node_count(), 3);
    assert_eq!(sub.edge_count(), 2);

    // 子图也是链
    let ids = dag.sub_dag_node_ids(&[a], &[c]);
    assert_eq!(ids.len(), 3);
}

// ============================================================
// 边数据
// ============================================================

#[test]
fn edge_data_with_weight() {
    let mut b: DagBuilder<&'static str, u32> = DagBuilder::new();
    let a = b.add_node("A");
    let b_ = b.add_node("B");
    b.add_edge(a, b_, 42).unwrap();

    let (dag, _) = b.freeze_with_map().unwrap();
    assert_eq!(dag.node_count(), 2);
    assert_eq!(dag.edge_count(), 1);
}

// ============================================================
// 无反向 CSR 时 sub_dag 的降级路径
// ============================================================

#[test]
fn sub_dag_without_reverse_csr() {
    let (dag, map) = build_diamond();
    assert!(!dag.has_reverse());

    let a = new_id_of(&map, 0);
    let d = new_id_of(&map, 3);

    // 走降级路径
    let sub = dag.sub_dag(&[a], &[d]);
    assert_eq!(sub.node_count(), 4);
    assert_eq!(sub.edge_count(), 4);
}
