// ============================================================
// 边数据存储：E 为零大小类型时零开销
// ============================================================

pub enum EdgeStore<E> {
    Empty,
    Data(Box<[E]>),
}

impl<E> EdgeStore<E> {
   pub(crate) fn from_vec(v: Vec<E>) -> Self {
        if std::mem::size_of::<E>() == 0 {
            Self::Empty
        } else {
            Self::Data(v.into_boxed_slice())
        }
    }

    #[inline]
    pub(crate) fn get(&self, idx: usize) -> Option<&E> {
        match self {
            EdgeStore::Empty => None,
            EdgeStore::Data(d) => d.get(idx),
        }
    }
}
