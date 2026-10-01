// ============================================================
// 能力标记
// ============================================================

/// 静态能力：构建完成后不可修改
pub struct Static;
/// 增量能力：只能添加节点/边（预留）
pub struct Incremental;
/// 减量能力：只能删除节点/边（预留）
pub struct Decremental;
/// 全动态能力：可增可删（预留）
pub struct Dynamic;

pub trait Capability {}
impl Capability for Static {}
impl Capability for Incremental {}
impl Capability for Decremental {}
impl Capability for Dynamic {}
