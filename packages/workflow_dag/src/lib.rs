//! # aa_workflow_dag
//!
//! DAG 层（对齐 TS 侧 `workflow-dag` 包的定位）：宿主侧编排的驻地——
//! 名单透传原语的组合示例、结构化运行器、（将来的）分段/失效编排。
//!
//! 当前内容：`tests/dag_recovery_demo.rs` —— 三个已实现引擎原语的 demo 测试
//! （interimGoals 泊车/续驱、compensate 逆序撤销、durable retry 泊车/续跑），
//! 全部只用 `aa_workflow_core` 的公开 API（图中性，core 不知道任何 DAG）。
