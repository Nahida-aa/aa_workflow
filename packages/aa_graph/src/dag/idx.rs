use std::hash::Hash;

pub trait IndexType:
    Copy + Eq + Ord + Hash + std::fmt::Debug + Send + Sync + 'static
{
    fn from_usize(x: usize) -> Self;
    fn to_usize(self) -> usize;
}

impl IndexType for u32 {
    #[inline]
    fn from_usize(x: usize) -> Self { x as u32 }
    #[inline]
    fn to_usize(self) -> usize { self as usize }
}

impl IndexType for u64 {
    #[inline]
    fn from_usize(x: usize) -> Self { x as u64 }
    #[inline]
    fn to_usize(self) -> usize { self as usize }
}

impl IndexType for usize {
    #[inline]
    fn from_usize(x: usize) -> Self { x }
    #[inline]
    fn to_usize(self) -> usize { self }
}
