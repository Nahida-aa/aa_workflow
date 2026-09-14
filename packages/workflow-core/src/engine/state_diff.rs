//! Minimal JSON Patch (RFC 6902) helpers for workflow state observability.
//!
//! 对齐 TanStack `engine/state-diff.ts`（113 行）。只发引擎需要的三种 op
//! （replace / add / remove）——move / copy / test 刻意省略：前向 diff 永远
//! 不会产生它们，RFC 也允许生产者使用任意子集。
//!
//! 用途：[`crate::event::WorkflowEvent::StateDelta`] 的 delta 载荷。
//! **emit-only，不落盘**——state 由日志重放推导，持久化 delta 会在每次
//! invocation 重放时重复 append（上游注释原话）。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 一个 JSON Patch 操作（对齐上游 `Operation`）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Operation {
    Replace { path: String, value: Value },
    Add { path: String, value: Value },
    Remove { path: String },
}

/// 给 state 拍快照，供之后 diff（对齐上游 `snapshotState`）。
///
/// TS 版要 `structuredClone` 防引用共享；Rust 的 `Value` 是拥有的值，
/// `.clone()` 即等价。
pub fn snapshot_state(state: &Value) -> Value {
    state.clone()
}

/// 产出 `prev` → `next` 的 RFC 6902 JSON Patch；无变化返回空数组。
///
/// 递归 diff 对象与数组；数组长度不同时发一个整体 `replace` 而非 splice 式
/// ops（线格式更简单，对状态观测足够——上游注释原话）。
pub fn diff_state(prev: &Value, next: &Value) -> Vec<Operation> {
    diff(prev, next, String::new())
}

fn diff(prev: &Value, next: &Value, path: String) -> Vec<Operation> {
    if prev == next {
        return vec![];
    }

    let prev_is_obj = prev.is_object() || prev.is_array();
    let next_is_obj = next.is_object() || next.is_array();
    // 一方是标量（或 null），或容器类型不同（object vs array）——整体 replace。
    // （TS 版用 `Array.isArray(prev) !== Array.isArray(next)` 判定；Rust 侧
    // `Value` 的 object/array 区分等价。）
    let container_kind = |v: &Value| match v {
        Value::Array(_) => 1,
        Value::Object(_) => 2,
        _ => 0,
    };
    if !prev_is_obj || !next_is_obj || container_kind(prev) != container_kind(next) {
        return vec![Operation::Replace {
            path: path.clone(),
            value: next.clone(),
        }];
    }

    if let (Value::Array(prev_arr), Value::Array(next_arr)) = (prev, next) {
        // 长度不同 → 整个数组 replace；同长 → 逐元素 diff。
        if prev_arr.len() != next_arr.len() {
            return vec![Operation::Replace {
                path: path.clone(),
                value: next.clone(),
            }];
        }
        let mut ops = Vec::new();
        for (i, (p, n)) in prev_arr.iter().zip(next_arr.iter()).enumerate() {
            ops.extend(diff(p, n, format!("{path}/{i}")));
        }
        return ops;
    }

    // 两侧都是 object。
    let (Value::Object(prev_obj), Value::Object(next_obj)) = (prev, next) else {
        unreachable!("上面已保证两侧都是容器");
    };
    let mut ops = Vec::new();
    let mut all_keys: Vec<&String> = prev_obj.keys().chain(next_obj.keys()).collect();
    all_keys.sort();
    all_keys.dedup();

    for key in all_keys {
        let sub_path = format!("{path}/{}", escape_json_pointer(key));
        match (prev_obj.get(key), next_obj.get(key)) {
            (Some(p), Some(n)) => ops.extend(diff(p, n, sub_path)),
            (None, Some(n)) => ops.push(Operation::Add {
                path: sub_path,
                value: n.clone(),
            }),
            (Some(_), None) => ops.push(Operation::Remove { path: sub_path }),
            (None, None) => unreachable!("key 来自两侧 keys 的并集"),
        }
    }
    ops
}

/// 按 RFC 6901 转义 JSON Pointer 段中的 `~` 与 `/`（对齐上游 `escapeJsonPointer`）。
fn escape_json_pointer(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn identical_values_diff_empty() {
        let a = json!({ "x": 1, "y": [1, 2] });
        assert!(diff_state(&a, &a).is_empty());
    }

    #[test]
    fn primitive_change_replaces_at_root() {
        let ops = diff_state(&json!(1), &json!(2));
        assert_eq!(
            ops,
            vec![Operation::Replace {
                path: String::new(),
                value: json!(2)
            }]
        );
    }

    #[test]
    fn nested_replace_add_remove() {
        let prev = json!({ "a": 1, "b": { "c": 2 }, "gone": 3 });
        let next = json!({ "a": 1, "b": { "c": 3 }, "new": 4 });
        let ops = diff_state(&prev, &next);
        assert_eq!(
            ops,
            vec![
                Operation::Replace {
                    path: "/b/c".into(),
                    value: json!(3)
                },
                Operation::Remove {
                    path: "/gone".into()
                },
                Operation::Add {
                    path: "/new".into(),
                    value: json!(4)
                },
            ]
        );
    }

    /// 数组长度不同 → 整体 replace（不做 splice 式 ops，上游注释原话）。
    #[test]
    fn array_length_mismatch_replaces_whole_array() {
        let ops = diff_state(&json!([1, 2, 3]), &json!([1, 2]));
        assert_eq!(
            ops,
            vec![Operation::Replace {
                path: String::new(),
                value: json!([1, 2])
            }]
        );
    }

    /// 同长数组逐元素 diff。
    #[test]
    fn same_length_arrays_diff_elementwise() {
        let ops = diff_state(&json!([1, 2]), &json!([1, 3]));
        assert_eq!(
            ops,
            vec![Operation::Replace {
                path: "/1".into(),
                value: json!(3)
            }]
        );
    }

    /// 对象 vs 数组（容器类型不同）→ 在当前路径整体 replace。
    #[test]
    fn object_to_array_replaces() {
        // 根级别：object → array，path 为根空串。
        let ops = diff_state(&json!({ "a": 1 }), &json!([1]));
        assert_eq!(
            ops,
            vec![Operation::Replace {
                path: String::new(),
                value: json!([1])
            }]
        );
        // 嵌套：字段值的容器类型变化，path 指向该字段。
        let ops = diff_state(&json!({ "a": { "x": 1 } }), &json!({ "a": [1] }));
        assert_eq!(
            ops,
            vec![Operation::Replace {
                path: "/a".into(),
                value: json!([1])
            }]
        );
    }

    /// RFC 6901 转义：key 里的 `~` → `~0`、`/` → `~1`。
    #[test]
    fn json_pointer_escaping() {
        let prev = json!({});
        let next = json!({ "a/b": 1, "c~d": 2 });
        let ops = diff_state(&prev, &next);
        let paths: Vec<&str> = ops
            .iter()
            .map(|o| match o {
                Operation::Add { path, .. } => path.as_str(),
                _ => panic!("应全是 add"),
            })
            .collect();
        assert!(paths.contains(&"/a~1b"), "实际 {paths:?}");
        assert!(paths.contains(&"/c~0d"), "实际 {paths:?}");
    }

    /// 根路径的整体 replace 用空字符串（对齐上游 `path || ''`）。
    #[test]
    fn root_path_is_empty_string() {
        let ops = diff_state(&json!({ "a": {} }), &json!({ "a": [] }));
        match &ops[0] {
            Operation::Replace { path, .. } => assert_eq!(path, "/a"),
            _ => panic!(),
        }
    }

    /// 序列化形状符合 RFC 6902 wire 格式（`op` / `path` / `value`）。
    #[test]
    fn operation_serializes_as_json_patch() {
        let v = serde_json::to_value(&Operation::Add {
            path: "/x".into(),
            value: json!(1),
        })
        .unwrap();
        assert_eq!(v, json!({ "op": "add", "path": "/x", "value": 1 }));
        let v = serde_json::to_value(&Operation::Remove { path: "/x".into() }).unwrap();
        assert_eq!(v, json!({ "op": "remove", "path": "/x" }));
    }
}
