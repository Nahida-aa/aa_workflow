//! 按持久化版本为 run 选 workflow 定义。
//!
//! 对齐 TanStack `workflow-core/src/registry/select-version.ts`。
//!
//! # 解析顺序（与上游一致）
//!
//! 1. **精确匹配**：`workflow_version` 与候选的 `version` 相等。
//! 2. **无版本 run 的兼容回退**：持久化里**没有** `workflow_version` 时
//!    （版本机制引入前的老 run），回退到当前定义。
//! 3. **其余情况返回 `None`**——由调用方决定报错还是用别的兜底。
//!
//! # 为什么版本化 run 匹配不上时**不能**回退
//!
//! 上游注释原话：*falling through to the unversioned default for a versioned
//! run would route a v1 run into v-undefined code, which is a **determinism
//! violation***。
//!
//! 理由：版本化 run 的 handler 代码已经变了，用当前版本重放会产生与原始 run
//! 不一致的副作用（step 集合、顺序、幂等键都可能不同）。静默回退比报错危险，
//! 所以这里返回 `None`，让 [`run_workflow`](crate::engine::run_workflow) 把它
//! 变成 `WorkflowVersionMismatch` 终局错误。
//!
//! 唯一的例外是情况 2：老 run 本来就没记版本，它跑的就是「无版本代码」，
//! 回退是语义正确的、不是违规。

use crate::define::WorkflowDefinition;

/// 在 `[current] + current.previous_versions` 中按持久化版本选定义。
///
/// 返回 `None` = 版本化 run 在候选里找不到对应定义（由调用方报错）。
pub fn select_workflow_version<'a>(
    workflow: &'a WorkflowDefinition,
    persisted: Option<&str>,
) -> Option<&'a WorkflowDefinition> {
    match persisted {
        // 情况 1：精确匹配（当前版本自身也在候选里）。
        Some(v) => {
            if workflow.version.as_deref() == Some(v) {
                return Some(workflow);
            }
            workflow
                .previous_versions
                .iter()
                .map(|w| &**w)
                .find(|w| w.version.as_deref() == Some(v))
        }
        // 情况 2：无持久化版本（老 run）→ 当前定义。
        None => Some(workflow),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match_in_current_and_previous() {
        let v1 = WorkflowDefinition::new("wf").version("v1");
        let v2 = WorkflowDefinition::new("wf")
            .version("v2")
            .previous_versions(vec![v1]);

        assert_eq!(
            select_workflow_version(&v2, Some("v1")).map(|w| w.version.clone()),
            Some(Some("v1".into())),
            "命中 previous_versions"
        );
        assert_eq!(
            select_workflow_version(&v2, Some("v2")).map(|w| w.version.clone()),
            Some(Some("v2".into())),
            "命中当前版本自身"
        );
    }

    /// 版本化 run 匹配不上 → `None`（**不回退**，对齐上游的确定性要求）。
    #[test]
    fn versioned_run_without_match_returns_none() {
        let v1 = WorkflowDefinition::new("wf").version("v1");
        let v2 = WorkflowDefinition::new("wf")
            .version("v2")
            .previous_versions(vec![v1]);
        assert!(
            select_workflow_version(&v2, Some("v9")).is_none(),
            "未知版本不得静默回退到当前版本（确定性违规）"
        );
    }

    /// 无持久化版本（老 run）→ 当前定义。
    #[test]
    fn unversioned_run_falls_back_to_current() {
        let v2 = WorkflowDefinition::new("wf").version("v2");
        assert_eq!(
            select_workflow_version(&v2, None).map(|w| w.version.clone()),
            Some(Some("v2".into()))
        );
    }
}
