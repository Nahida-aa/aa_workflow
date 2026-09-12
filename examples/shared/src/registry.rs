//! 示例 workflow 注册表（对齐 wf-demo 的 `WORKFLOWS` map 位置）：按 id 查示例。

use workflow_core::Workflow;

use crate::workflows::{
    approval_order, approval_review, compliance, email_digest, fulfillment, fulfillment_saga,
    invoice, refund,
};

/// 全部注册示例 workflow。
pub fn all() -> Vec<Workflow> {
    vec![
        fulfillment(),
        fulfillment_saga(),
        approval_review(),
        approval_order(),
        email_digest(),
        invoice(),
        compliance(),
        refund(),
    ]
}

/// 按 id 取 workflow（找不到返回 `None`）。
pub fn get(id: &str) -> Option<Workflow> {
    all().into_iter().find(|w| w.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_covers_wf_demo_scenarios() {
        let wfs = all();
        let ids: Vec<&str> = wfs.iter().map(|w| w.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "fulfillment",
                "fulfillment-saga",
                "approval-review",
                "approval-order",
                "email-digest",
                "invoice",
                "compliance",
                "refund",
            ]
        );
        for id in [
            "fulfillment",
            "approval-order",
            "invoice",
            "compliance",
            "refund",
        ] {
            assert!(get(id).is_some(), "{id} 应在注册表");
        }
    }
}