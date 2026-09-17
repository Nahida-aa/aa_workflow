//! 示例 workflow 注册表（对齐 wf-demo 的 `WORKFLOWS` map 位置）：按 id 查示例。

use workflow_core::Workflow;

use crate::workflows::{
    approval_order, approval_review, compliance, dub_sf_ocr, email_digest, event_gate, fulfillment,
    fulfillment_saga, invoice, refund, state_demo,
};

/// 全部注册示例 workflow（类型擦除：进注册表的都是引擎 `Workflow`）。
pub fn all() -> Vec<Workflow> {
    vec![
        fulfillment().into_workflow(),
        fulfillment_saga().into_workflow(),
        approval_review(),
        approval_order().into_workflow(),
        email_digest().into_workflow(),
        invoice().into_workflow(),
        compliance().into_workflow(),
        refund().into_workflow(),
        state_demo().into_workflow(),
        event_gate().into_workflow(),
        dub_sf_ocr().into_workflow(),
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
                "state-demo",
                "event-gate",
                "dub-sf-ocr",
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
