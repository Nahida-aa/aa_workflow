// use crate::dag::idx::IndexType;

// pub(crate) fn build_reverse_csr<Ix: IndexType, E>(
//     v: usize,
//     e: usize,
//     edges: &[(Ix, Ix, E)],
// ) -> (Vec<Ix>, Vec<Ix>) {
//     let mut in_offsets = vec![Ix::from_usize(0); v + 1];
//     for (_, to, _) in edges {
//         let i = to.to_usize() + 1;
//         in_offsets[i] = Ix::from_usize(in_offsets[i].to_usize() + 1);
//     }
//     for i in 1..=v {
//         in_offsets[i] = Ix::from_usize(
//             in_offsets[i].to_usize() + in_offsets[i - 1].to_usize(),
//         );
//     }

//     let mut in_targets = vec![Ix::from_usize(0); e];
//     let mut cursor: Vec<usize> =
//         in_offsets[..v].iter().map(|x| x.to_usize()).collect();
//     for (from, to, _) in edges {
//         let t = to.to_usize();
//         in_targets[cursor[t]] = *from;
//         cursor[t] += 1;
//     }
//     (in_offsets, in_targets)
// }
