//! Runtime 的声明式入口与 schedule 规格构造器。
//!
//! 对齐 TanStack `aa-workflow-runtime/src/define-runtime.ts`。
//!
//! # 内容
//!
//! - [`define_aa_workflow_runtime`]：注册 workflow + 持有 store，返回可驱动的
//!   runtime。**实现在 `runtime_driver`**（那边是 driver 主体），此处只做
//!   再导出——上游的 `define-runtime.ts` 也只是 `createRuntimeDriver` 的薄壳。
//! - [`cron`] / [`every`]：构造 [`WorkflowScheduleSpec`]，对应 guide 里的
//!   `every.minutes(15)` / `cron('*/5 * * * *')`。
//!
//! # 构造器的现状（说清楚免得误用）
//!
//! 这两个构造器**当前没有运行时消费者**：产出 `WorkflowScheduleSpec` 之后，
//! 谁来解析 cron 表达式、算下一次触发时间、生成 schedule 桶——那是上游
//! `schedule-materializer.ts`（272 行）的职责，**尚未移植**。
//!
//! 现在的实际用法是：宿主自己算好 `next_fire_at`，用
//! [`upsert_schedule`](crate::WorkflowExecutionStore::upsert_schedule) 登记，
//! 然后 sweep 的 `claim_due_schedule_buckets` 会认领到期的桶。也就是说
//! [`WorkflowScheduleSpec`] 目前只是**声明性元数据**，spec → `next_fire_at`
//! 的换算还在宿主手里。

pub use crate::runtime_driver::define_aa_workflow_runtime;

use crate::types::WorkflowScheduleSpec;

/// 构造一个 cron 规格（对齐上游 `cron(expression, { timezone })`）。
///
/// 注意：**表达式本身不被解析**（见模块文档）——跨宿主传的是声明，实际触发
/// 时间由宿主换算成 `next_fire_at`。
pub fn cron(expression: impl Into<String>, timezone: Option<String>) -> WorkflowScheduleSpec {
    WorkflowScheduleSpec::Cron {
        expression: expression.into(),
        timezone,
    }
}

/// 固定间隔规格构造器（对齐上游 `every` 对象）。
///
/// ```ignore
/// every::minutes(15)   // → Interval { every_ms: 900_000, timezone: None }
/// every::seconds(30)
/// ```
///
/// 上游是带四个方法的对象字面量；Rust 用零尺寸类型 + 关联函数——同样的调用
/// 形状。命名故意保持小写以对齐上游的 `every.minutes()` 读法（Rust 惯例是
/// CamelCase，此处是刻意的命名对齐取舍）。
#[allow(non_camel_case_types)]
pub struct every;

impl every {
    /// 对齐上游 `every.milliseconds`。
    pub fn milliseconds(every_ms: i64) -> WorkflowScheduleSpec {
        WorkflowScheduleSpec::Interval {
            every_ms,
            timezone: None,
        }
    }

    /// 对齐上游 `every.seconds`。
    pub fn seconds(seconds: i64) -> WorkflowScheduleSpec {
        Self::milliseconds(seconds * 1000)
    }

    /// 对齐上游 `every.minutes`。
    pub fn minutes(minutes: i64) -> WorkflowScheduleSpec {
        Self::milliseconds(minutes * 60 * 1000)
    }

    /// 对齐上游 `every.hours`。
    pub fn hours(hours: i64) -> WorkflowScheduleSpec {
        Self::milliseconds(hours * 60 * 60 * 1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_builds_interval_specs() {
        assert_eq!(
            every::milliseconds(500),
            WorkflowScheduleSpec::Interval {
                every_ms: 500,
                timezone: None
            }
        );
        assert_eq!(
            every::seconds(30),
            WorkflowScheduleSpec::Interval {
                every_ms: 30_000,
                timezone: None
            }
        );
        assert_eq!(
            every::minutes(15),
            WorkflowScheduleSpec::Interval {
                every_ms: 900_000,
                timezone: None
            },
            "对齐 guide 的 every.minutes(15)"
        );
        assert_eq!(
            every::hours(2),
            WorkflowScheduleSpec::Interval {
                every_ms: 7_200_000,
                timezone: None
            }
        );
    }

    #[test]
    fn cron_builds_cron_spec() {
        assert_eq!(
            cron("*/5 * * * *", None),
            WorkflowScheduleSpec::Cron {
                expression: "*/5 * * * *".into(),
                timezone: None
            }
        );
        assert_eq!(
            cron("0 9 * * MON", Some("Asia/Shanghai".into())),
            WorkflowScheduleSpec::Cron {
                expression: "0 9 * * MON".into(),
                timezone: Some("Asia/Shanghai".into())
            }
        );
    }

    /// spec 的 wire 形状与上游同构（`kind` 判别式 + camelCase 字段）。
    #[test]
    fn spec_serializes_with_kind_discriminant() {
        assert_eq!(
            serde_json::to_value(every::minutes(15)).unwrap(),
            serde_json::json!({ "kind": "interval", "everyMs": 900_000 })
        );
        assert_eq!(
            serde_json::to_value(cron("*/5 * * * *", None)).unwrap(),
            serde_json::json!({ "kind": "cron", "expression": "*/5 * * * *" })
        );
    }
}
