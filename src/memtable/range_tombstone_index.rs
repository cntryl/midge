//! Range tombstones fragmented into disjoint key intervals for point lookups
//! (#921).
//!
//! A point read needs the newest range tombstone visible at its snapshot that
//! covers one key. Scanning every tombstone costs `O(T)` per source; this index
//! splits the tombstones at every start and end bound into disjoint fragments,
//! each carrying the sequences of the tombstones that cover it, so a lookup is
//! a binary search over fragments plus a binary search over sequences.
//!
//! Heavily nested tombstones would make the fragments repeat the same sequences
//! many times over. The index refuses to build past a bounded multiple of the
//! tombstone count so its memory stays `O(T)`; callers then fall back to the
//! linear scan.

use crate::types::RangeTombstone;

/// Half-open key interval and the sequences of every tombstone covering it,
/// newest first.
struct Fragment {
    start: Vec<u8>,
    end: Vec<u8>,
    seqs: Vec<u64>,
}

pub(crate) struct FragmentedRangeTombstones {
    fragments: Vec<Fragment>,
}

impl FragmentedRangeTombstones {
    /// Fragments beyond this many sequence entries per tombstone mean the
    /// tombstones nest so deeply that the index would cost more memory than
    /// the linear scan saves in time.
    const MAX_SEQ_ENTRIES_PER_TOMBSTONE: usize = 8;
    /// Small tombstone sets may fragment freely.
    const MIN_SEQ_ENTRY_ALLOWANCE: usize = 4096;

    /// Build the index, or `None` when the tombstones nest too deeply for the
    /// memory bound.
    pub(crate) fn build(tombstones: &[RangeTombstone]) -> Option<Self> {
        let mut by_start: Vec<&RangeTombstone> = tombstones
            .iter()
            .filter(|tombstone| tombstone.start < tombstone.end)
            .collect();
        by_start.sort_by(|left, right| left.start.cmp(&right.start));

        let mut bounds: Vec<&[u8]> = Vec::with_capacity(by_start.len() * 2);
        for tombstone in &by_start {
            bounds.push(&tombstone.start);
            bounds.push(&tombstone.end);
        }
        bounds.sort_unstable();
        bounds.dedup();

        let allowance = (by_start.len() * Self::MAX_SEQ_ENTRIES_PER_TOMBSTONE)
            .max(Self::MIN_SEQ_ENTRY_ALLOWANCE);
        let mut seq_entries = 0usize;
        let mut fragments = Vec::new();
        let mut next = 0usize;
        // Tombstones whose start is at or before the fragment and whose end is
        // past it. Ends are bounds, so an active tombstone covers the whole
        // fragment.
        let mut active: Vec<(&[u8], u64)> = Vec::new();
        for window in bounds.windows(2) {
            let (lo, hi) = (window[0], window[1]);
            while let Some(tombstone) = by_start.get(next).filter(|t| t.start.as_slice() <= lo) {
                active.push((&tombstone.end, tombstone.seq));
                next += 1;
            }
            active.retain(|(end, _)| *end > lo);
            if active.is_empty() {
                continue;
            }
            seq_entries += active.len();
            if seq_entries > allowance {
                return None;
            }
            let mut seqs: Vec<u64> = active.iter().map(|(_, seq)| *seq).collect();
            seqs.sort_unstable_by(|left, right| right.cmp(left));
            fragments.push(Fragment {
                start: lo.to_vec(),
                end: hi.to_vec(),
                seqs,
            });
        }
        Some(Self { fragments })
    }

    /// The highest sequence visible at `snapshot_seq` among the tombstones
    /// covering `key`.
    pub(crate) fn max_covering_seq(&self, key: &[u8], snapshot_seq: u64) -> Option<u64> {
        let index = self
            .fragments
            .partition_point(|fragment| fragment.start.as_slice() <= key);
        let fragment = &self.fragments[index.checked_sub(1)?];
        if key >= fragment.end.as_slice() {
            return None;
        }
        let first_visible = fragment
            .seqs
            .partition_point(|&seq| snapshot_seq != u64::MAX && seq > snapshot_seq);
        fragment.seqs.get(first_visible).copied()
    }

    /// Heap bytes held by the index, for the owner's accounting.
    pub(crate) fn memory_usage(&self) -> usize {
        self.fragments.iter().fold(
            self.fragments
                .len()
                .saturating_mul(std::mem::size_of::<Fragment>()),
            |total, fragment| {
                total
                    .saturating_add(fragment.start.capacity())
                    .saturating_add(fragment.end.capacity())
                    .saturating_add(fragment.seqs.capacity().saturating_mul(8))
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn fragment_count(&self) -> usize {
        self.fragments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::FragmentedRangeTombstones;
    use crate::memtable::max_covering_seq;
    use crate::types::RangeTombstone;

    fn tombstone(start: u8, end: u8, seq: u64) -> RangeTombstone {
        RangeTombstone::new(vec![start], vec![end], seq)
    }

    #[test]
    fn should_return_newest_visible_cover_given_nested_and_adjacent_ranges_when_queried() {
        // Arrange: [a,m)@10 nests [c,f)@20; [m,z)@5 is adjacent.
        let tombstones = [
            tombstone(b'a', b'm', 10),
            tombstone(b'c', b'f', 20),
            tombstone(b'm', b'z', 5),
        ];
        let index = FragmentedRangeTombstones::build(&tombstones).expect("bounded index");

        // Act / Assert
        assert_eq!(index.max_covering_seq(b"d", u64::MAX), Some(20));
        assert_eq!(index.max_covering_seq(b"d", 15), Some(10));
        assert_eq!(index.max_covering_seq(b"d", 9), None);
        assert_eq!(index.max_covering_seq(b"b", u64::MAX), Some(10));
        assert_eq!(index.max_covering_seq(b"m", u64::MAX), Some(5));
        assert_eq!(index.max_covering_seq(b"z", u64::MAX), None);
        assert_eq!(index.max_covering_seq(b"0", u64::MAX), None);
        assert_eq!(index.fragment_count(), 4);
    }

    #[test]
    fn should_refuse_to_build_given_deeply_nested_ranges_when_memory_bound_is_exceeded() {
        // Arrange: 5,000 staggered ranges each overlapping thousands of others.
        let tombstones: Vec<RangeTombstone> = (0..5_000_u64)
            .map(|i| {
                RangeTombstone::new(
                    format!("{i:08}").into_bytes(),
                    format!("{:08}", i + 4_000).into_bytes(),
                    i + 1,
                )
            })
            .collect();

        // Act
        let index = FragmentedRangeTombstones::build(&tombstones);

        // Assert
        assert!(index.is_none());
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(256))]
        #[test]
        fn should_match_linear_scan_given_random_ranges_when_queried(
            specs in proptest::collection::vec((0_u8..16, 0_u8..16, 1_u64..8), 0..12),
            key in 0_u8..16,
            snapshot in proptest::option::of(1_u64..8),
        ) {
            // Arrange: inverted and empty ranges are included; they cover nothing.
            let tombstones: Vec<RangeTombstone> = specs
                .iter()
                .map(|&(start, end, seq)| tombstone(start, end, seq))
                .collect();
            let snapshot_seq = snapshot.unwrap_or(u64::MAX);
            let expected = max_covering_seq(&tombstones, &[key], snapshot_seq);

            // Act
            let index = FragmentedRangeTombstones::build(&tombstones).expect("small sets always build");

            // Assert
            proptest::prop_assert_eq!(index.max_covering_seq(&[key], snapshot_seq), expected);
        }
    }
}
