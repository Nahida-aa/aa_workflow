//! # aa_workflow_dag
//!
//! 结构化编排层（对齐 TS 侧 `workflow-dag` 包的定位）：**形状声明
//! （aa_graph）→ 闭包展开 → 引擎名单透传**。core 不知道任何 DAG——本
//! crate 是「DAG 术语」唯一合法的驻地：把使用者声明的形状图和源 run 日志
//! 折算成引擎能理解的失效名单，再交给 `run_workflow` 的 `reseed_run`。
//!
//! 分层契约（对齐 TS 侧已验证的分工）：
//!
//! | 层 | 收什么 | 计算 |
//! | --- | --- | --- |
//! | core `reseed_run` | 展开后的名单 | 无（名单透传，不校验封闭性） |
//! | 本 crate | `from`（种子）；缺省 = 源日志未成功节点 | `种子 ∪ descendants_from(种子)` |
//!
//! demo 测试见 `tests/dag_recovery_demo.rs`（五个场景：四个引擎原语 +
//! 形状图合龙）。

use aa_graph::dag::{builder::DagBuilder, error::DagError, static_dag::StaticDag};
use aa_workflow_core::{ReseedRun, WorkflowEvent};
use std::collections::{BTreeSet, HashMap};

/// 形状/名单计算的错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapeError {
    /// 同一个 step id 声明了两次。
    DuplicateStep(String),
    /// 依赖或 `from` 引用了形状里不存在的 step。
    UnknownStep(String),
    /// 图构造失败（自环 / 成环）。
    Graph(DagError),
}

impl std::fmt::Display for ShapeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShapeError::DuplicateStep(id) => write!(f, "duplicate step id `{id}`"),
            ShapeError::UnknownStep(id) => write!(f, "unknown step id `{id}`"),
            ShapeError::Graph(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ShapeError {}

impl From<DagError> for ShapeError {
    fn from(e: DagError) -> Self {
        ShapeError::Graph(e)
    }
}

/// 步骤依赖形状（静态 DAG）。节点 = step id，边 = 依赖方向（前置 → 后继）。
///
/// 形状只是**名单计算的依据**，不参与执行调度——handler 里的步骤怎么跑
/// （串行 / `try_join!` / 条件分支）是 handler 的事，引擎看不见这张图，
/// 本 crate 也不会把图交给引擎。
pub struct DagShape {
    dag: StaticDag<String>,
    /// step id → 拓扑重排后的节点下标（`freeze` 会重排，映射在冻结后重建）。
    index: HashMap<String, u32>,
}

impl DagShape {
    /// 声明形状：`(step id, 依赖的前置 step ids)`。依赖必须已在 `steps`
    /// 里声明过（先声明后引用）；自环立即拒绝，成环在冻结时检测。
    pub fn build(steps: &[(&str, &[&str])]) -> Result<Self, ShapeError> {
        let mut b = DagBuilder::<String>::new();
        let mut tmp: HashMap<&str, u32> = HashMap::with_capacity(steps.len());
        for (id, _) in steps {
            if tmp.contains_key(id) {
                return Err(ShapeError::DuplicateStep(id.to_string()));
            }
            let ix = b.add_node(id.to_string());
            tmp.insert(id, ix);
        }
        for (id, deps) in steps {
            for dep in *deps {
                let Some(&di) = tmp.get(dep) else {
                    return Err(ShapeError::UnknownStep(dep.to_string()));
                };
                let &si = tmp.get(id).expect("本循环的 id 一定已登记");
                b.add_edge(di, si, ())?;
            }
        }
        let dag = b.freeze()?;
        let mut index = HashMap::with_capacity(steps.len());
        for (ix, name) in dag.nodes() {
            index.insert(name.clone(), ix);
        }
        Ok(Self { dag, index })
    }

    pub fn contains(&self, id: &str) -> bool {
        self.index.contains_key(id)
    }

    /// redo 失效名单：`from`（种子）∪ `descendants_from(from)`。
    ///
    /// 种子**自含**——aa_graph 的 `descendants_from` 不含起点，而 redo 语义
    /// 里种子自己必须重跑。返回值按字典序（确定性，方便断言与比对）。
    /// 引擎拿到的永远是这份展开后的名单；封闭性由本crate按声明的形状保证，
    /// 形状与 handler 实际步骤不符时后果仍是调用者的（名单透传的 unsafe
    /// 契约在引擎侧不变）。
    pub fn invalidation_set(&self, from: &[&str]) -> Result<Vec<String>, ShapeError> {
        let mut ixs = Vec::with_capacity(from.len());
        for id in from {
            let Some(&ix) = self.index.get(*id) else {
                return Err(ShapeError::UnknownStep(id.to_string()));
            };
            ixs.push(ix);
        }
        let mut set: BTreeSet<String> = from.iter().map(|s| s.to_string()).collect();
        for ix in self.dag.descendants_from(&ixs) {
            set.insert(self.dag.node(ix).clone());
        }
        Ok(set.into_iter().collect())
    }

    /// redo 计划：把 `from` 折算成引擎的 [`ReseedRun`]。
    ///
    /// - `from = Some(seeds)`：种子即失效起点，展开为 `种子 ∪ 后继闭包`；
    /// - `from = None`：缺省 = 源日志里**未成功**的节点（没有
    ///   `STEP_FINISHED` 的形状节点——失败、缺席都算），即「哪里没跑通
    ///   就从哪里重跑」。
    ///
    /// `source_log` 传源 run 的持久化事件（`store.get_events(source_run_id)`）；
    /// 导入的 checkpoint 也带 `STEP_FINISHED`，所以 redo 源同样适用。
    pub fn reseed_plan(
        &self,
        source_run_id: impl Into<String>,
        source_log: &[WorkflowEvent],
        from: Option<Vec<String>>,
    ) -> Result<ReseedRun, ShapeError> {
        let seeds = match from {
            Some(f) => f,
            None => {
                let done: BTreeSet<&str> = source_log
                    .iter()
                    .filter_map(|e| match e {
                        WorkflowEvent::StepFinished { step_id, .. } => Some(step_id.as_str()),
                        _ => None,
                    })
                    .collect();
                self.dag
                    .nodes()
                    .map(|(_, name)| name.clone())
                    .filter(|id| !done.contains(id.as_str()))
                    .collect()
            }
        };
        let seeds: Vec<&str> = seeds.iter().map(String::as_str).collect();
        let invalidate = self.invalidation_set(&seeds)?;
        Ok(ReseedRun::new(source_run_id).with_invalidate(invalidate))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step_finished(step_id: &str) -> WorkflowEvent {
        WorkflowEvent::StepFinished {
            ts: 0,
            run_id: "src".into(),
            step_id: step_id.into(),
            result: None,
            attempts: vec![],
            meta: None,
        }
    }

    /// 菱形：a → (b, c) → d
    fn diamond() -> DagShape {
        DagShape::build(&[
            ("a", &[]),
            ("b", &["a"]),
            ("c", &["a"]),
            ("d", &["b", "c"]),
        ])
        .unwrap()
    }

    #[test]
    fn invalidation_set_is_seed_union_descendants() {
        let shape = diamond();
        // 种子自含 + 后继闭包；无关分支 c 不进名单
        assert_eq!(shape.invalidation_set(&["b"]).unwrap(), vec!["b", "d"]);
        // 多种子
        assert_eq!(
            shape.invalidation_set(&["a", "b"]).unwrap(),
            vec!["a", "b", "c", "d"]
        );
    }

    #[test]
    fn unknown_step_is_rejected() {
        let shape = diamond();
        assert_eq!(
            shape.invalidation_set(&["nope"]),
            Err(ShapeError::UnknownStep("nope".into()))
        );
    }

    #[test]
    fn cycles_and_duplicates_are_rejected() {
        assert!(matches!(
            DagShape::build(&[("a", &["b"]), ("b", &["a"])]),
            Err(ShapeError::Graph(DagError::CycleDetected))
        ));
        assert!(matches!(
            DagShape::build(&[("a", &[]), ("a", &[])]),
            Err(ShapeError::DuplicateStep(id)) if id == "a"
        ));
        assert!(matches!(
            DagShape::build(&[("a", &["ghost"])]),
            Err(ShapeError::UnknownStep(id)) if id == "ghost"
        ));
    }

    #[test]
    fn default_from_is_unsuccessful_nodes() {
        let shape = diamond();
        // 源 run：a、c 成功（b 失败 → d 没跑到）→ 缺省种子 {b, d}
        let log = vec![step_finished("a"), step_finished("c")];
        let plan = shape
            .reseed_plan("src-run", &log, None)
            .unwrap();
        assert_eq!(plan.source_run_id, "src-run");
        assert_eq!(plan.unsafe_invalidate, vec!["b", "d"]);
    }

    #[test]
    fn explicit_from_overrides_default() {
        let shape = diamond();
        let log = vec![step_finished("a"), step_finished("c")];
        let plan = shape
            .reseed_plan("src-run", &log, Some(vec!["b".into()]))
            .unwrap();
        assert_eq!(plan.unsafe_invalidate, vec!["b", "d"]);
    }
}
