//! Streaming merging iterator for efficient range scans across multiple sources.
//!
//! World-class merging iterator following LSM best practices:
//! - Internal key ordering: (user_key ASC/DESC, seq DESC, value before tombstone, source priority)
//! - Single-key dedup via tracking last emitted/processed user_key
//! - Tombstones mask older values
//! - Snapshot visibility support
//! - Heap maintains one entry per source; advance only producing source
//! - Correct forward and reverse iteration without materializing the full result

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

//
// ─────────────────────────────────────────────────────────────────────────────
//   Heap Entry
// ─────────────────────────────────────────────────────────────────────────────
//

#[derive(Debug, Clone)]
struct HeapEntry {
    key: Bytes,
    value: Option<Bytes>, // None = tombstone
    seq: u64,
    source_id: usize,
    source_priority: u8, // lower = newer (mem=0, imm=1, L0=2, L1+=3)
    reverse: bool,
}

impl Eq for HeapEntry {}
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq && self.source_id == other.source_id
    }
}

impl Ord for HeapEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap = max-heap. We return "greater means more desirable".

        // 1. User key ordering (ASC vs DESC).
        let key_ord = if self.reverse {
            self.key.cmp(&other.key) // reverse = larger key wins
        } else {
            other.key.cmp(&self.key) // forward = smaller key wins
        };
        if key_ord != Ordering::Equal {
            return key_ord;
        }

        // 2. seq DESC (newest version first).
        if self.seq != other.seq {
            return self.seq.cmp(&other.seq);
        }

        // 3. Values before tombstones.
        let self_val = self.value.is_some();
        let other_val = other.value.is_some();
        if self_val != other_val {
            return if self_val {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }

        // 4. Source priority (lower = newer).
        if self.source_priority != other.source_priority {
            return other.source_priority.cmp(&self.source_priority);
        }

        // 5. Stable tie-breaker.
        other.source_id.cmp(&self.source_id)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

//
// ─────────────────────────────────────────────────────────────────────────────
//   Iterator Source Trait
// ─────────────────────────────────────────────────────────────────────────────
//

pub trait IteratorSource {
    fn next(&mut self) -> Option<(Bytes, Option<Bytes>, u64)>;

    // mem=0, imm=1, L0=2, L1+=3 (default low signal)
    fn priority(&self) -> u8 {
        10
    }
}

//
// ─────────────────────────────────────────────────────────────────────────────
//   VecSource (for testing)
// ─────────────────────────────────────────────────────────────────────────────
//

pub struct VecSource {
    items: Vec<Option<(Bytes, Option<Bytes>, u64)>>,
    pos: usize,
    #[allow(dead_code)]
    reverse: bool,
    priority: u8,
}

impl VecSource {
    pub fn new(mut items: Vec<(Bytes, Option<Bytes>, u64)>) -> Self {
        items.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            items: items.into_iter().map(Some).collect(),
            pos: 0,
            reverse: false,
            priority: 3,
        }
    }

    pub fn new_reverse(mut items: Vec<(Bytes, Option<Bytes>, u64)>) -> Self {
        items.sort_by(|a, b| b.0.cmp(&a.0));
        Self {
            items: items.into_iter().map(Some).collect(),
            pos: 0,
            reverse: true,
            priority: 3,
        }
    }

    pub fn with_priority(mut items: Vec<(Bytes, Option<Bytes>, u64)>, priority: u8) -> Self {
        items.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            items: items.into_iter().map(Some).collect(),
            pos: 0,
            reverse: false,
            priority,
        }
    }
}

impl IteratorSource for VecSource {
    #[inline]
    fn next(&mut self) -> Option<(Bytes, Option<Bytes>, u64)> {
        if self.pos >= self.items.len() {
            return None;
        }
        let out = self.items[self.pos].take();
        self.pos += 1;
        out
    }

    fn priority(&self) -> u8 {
        self.priority
    }
}

//
// ─────────────────────────────────────────────────────────────────────────────
//   Merging Iterator
// ─────────────────────────────────────────────────────────────────────────────
//

pub struct MergingIterator {
    heap: BinaryHeap<HeapEntry>,
    sources: Vec<Box<dyn IteratorSource>>,
    last_key: Option<Bytes>,
    limit: Option<usize>,
    emitted: usize,
    reverse: bool,
    snapshot_seq: Option<u64>,
}

impl MergingIterator {
    pub fn new(sources: Vec<Box<dyn IteratorSource>>, limit: Option<usize>) -> Self {
        Self::with_reverse_and_snapshot(sources, limit, false, None)
    }

    pub fn with_reverse(
        sources: Vec<Box<dyn IteratorSource>>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Self {
        Self::with_reverse_and_snapshot(sources, limit, reverse, None)
    }

    pub fn with_reverse_and_snapshot(
        mut sources: Vec<Box<dyn IteratorSource>>,
        limit: Option<usize>,
        reverse: bool,
        snapshot_seq: Option<u64>,
    ) -> Self {
        let mut heap = BinaryHeap::with_capacity(sources.len());

        for (id, src) in sources.iter_mut().enumerate() {
            if let Some((k, v, s)) = src.next() {
                heap.push(HeapEntry {
                    key: k,
                    value: v,
                    seq: s,
                    source_id: id,
                    source_priority: src.priority(),
                    reverse,
                });
            }
        }

        Self {
            heap,
            sources,
            last_key: None,
            limit,
            emitted: 0,
            reverse,
            snapshot_seq,
        }
    }
}

impl Iterator for MergingIterator {
    type Item = (Bytes, Bytes);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.heap.pop() {
            // Limit
            if let Some(limit) = self.limit {
                if self.emitted >= limit {
                    return None;
                }
            }

            // Advance producing source
            if let Some((k, v, s)) = self.sources[entry.source_id].next() {
                self.heap.push(HeapEntry {
                    key: k,
                    value: v,
                    seq: s,
                    source_id: entry.source_id,
                    source_priority: self.sources[entry.source_id].priority(),
                    reverse: self.reverse,
                });
            }

            // Snapshot visibility (skip versions > snapshot, but do not finalize key yet)
            if let Some(snap) = self.snapshot_seq {
                if entry.seq > snap {
                    continue;
                }
            }

            // Dedup: if we already processed this user_key, skip this entry.
            if let Some(last) = &self.last_key {
                if *last == entry.key {
                    continue;
                }
            }

            // First visible version for this key: remember the key whether value or tombstone.
            self.last_key = Some(entry.key.clone());

            // Tombstone → skip but block older versions.
            let Some(value) = entry.value else {
                continue;
            };

            // Emit value
            self.emitted += 1;
            return Some((entry.key, value));
        }
        None
    }
}

//
// ─────────────────────────────────────────────────────────────────────────────
//   Tests
// ─────────────────────────────────────────────────────────────────────────────
//

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn kv(key: &str, val: &str, seq: u64) -> (Bytes, Option<Bytes>, u64) {
        (
            Bytes::copy_from_slice(key.as_bytes()),
            Some(Bytes::copy_from_slice(val.as_bytes())),
            seq,
        )
    }

    #[test]
    fn should_merge_two_sources_with_deduplication() {
        // Arrange
        let s0 = VecSource::new(vec![kv("k1", "v1_new", 2)]);
        let s1 = VecSource::new(vec![kv("k1", "v1_old", 1), kv("k2", "v2", 1)]);

        // Act
        let it = MergingIterator::new(vec![Box::new(s0), Box::new(s1)], None);
        let res: Vec<_> = it.collect();

        // Assert
        assert_eq!(
            res,
            vec![
                (Bytes::from("k1"), Bytes::from("v1_new")),
                (Bytes::from("k2"), Bytes::from("v2")),
            ]
        );
    }

    #[test]
    fn should_mask_older_value_given_tombstone_when_merging() {
        // Arrange
        let s0 = VecSource::new(vec![(Bytes::from("k1"), None, 2)]);
        let s1 = VecSource::new(vec![kv("k1", "v1", 1)]);

        // Act
        let it = MergingIterator::new(vec![Box::new(s0), Box::new(s1)], None);
        let res: Vec<_> = it.collect();

        // Assert
        assert!(res.is_empty());
    }

    #[test]
    fn should_see_older_value_given_snapshot_before_tombstone_when_reading() {
        // Arrange
        let s0 = VecSource::new(vec![(Bytes::from("k1"), None, 200)]);
        let s1 = VecSource::new(vec![kv("k1", "v1", 100)]);

        // Act
        let it = MergingIterator::with_reverse_and_snapshot(
            vec![Box::new(s0), Box::new(s1)],
            None,
            false,
            Some(150),
        );
        let res: Vec<_> = it.collect();

        // Assert
        assert_eq!(res, vec![(Bytes::from("k1"), Bytes::from("v1"))]);
    }

    #[test]
    fn should_respect_limit() {
        // Arrange
        let s = VecSource::new(vec![
            kv("k1", "v1", 1),
            kv("k2", "v2", 1),
            kv("k3", "v3", 1),
            kv("k4", "v4", 1),
        ]);

        // Act
        let it = MergingIterator::new(vec![Box::new(s)], Some(2));
        let res: Vec<_> = it.collect();

        // Assert
        assert_eq!(res.len(), 2);
    }

    #[test]
    fn should_merge_sorted_across_sources() {
        // Arrange
        let s0 = VecSource::new(vec![kv("a", "a0", 3), kv("c", "c0", 3)]);
        let s1 = VecSource::new(vec![kv("b", "b1", 2), kv("d", "d1", 2)]);

        // Act
        let it = MergingIterator::new(vec![Box::new(s0), Box::new(s1)], None);
        let keys: Vec<_> = it.map(|(k, _)| k).collect();

        // Assert
        assert_eq!(
            keys,
            vec!["a", "b", "c", "d"]
                .into_iter()
                .map(Bytes::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn should_iterate_in_reverse_given_reverse_flag_when_merging() {
        // Arrange
        let s = VecSource::new_reverse(vec![
            kv("k1", "v1", 1),
            kv("k2", "v2", 1),
            kv("k3", "v3", 1),
        ]);

        // Act
        let it = MergingIterator::with_reverse(vec![Box::new(s)], None, true);
        let keys: Vec<_> = it.map(|(k, _)| k).collect();

        // Assert
        assert_eq!(
            keys,
            vec!["k3", "k2", "k1"]
                .into_iter()
                .map(Bytes::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn should_deduplicate_given_reverse_iteration_when_merging() {
        // Arrange
        let s0 = VecSource::new_reverse(vec![(Bytes::from("k3"), Some(Bytes::from("v3_new")), 2)]);
        let s1 = VecSource::new_reverse(vec![
            (Bytes::from("k3"), Some(Bytes::from("v3_old")), 1),
            (Bytes::from("k2"), Some(Bytes::from("v2")), 1),
            (Bytes::from("k1"), Some(Bytes::from("v1")), 1),
        ]);

        // Act
        let it = MergingIterator::with_reverse(vec![Box::new(s0), Box::new(s1)], None, true);
        let res: Vec<_> = it.collect();

        // Assert
        assert_eq!(res[0], (Bytes::from("k3"), Bytes::from("v3_new")));
        assert_eq!(res[1].0, Bytes::from("k2"));
        assert_eq!(res[2].0, Bytes::from("k1"));
    }

    #[test]
    fn should_return_empty_given_empty_sources_when_merging() {
        // Arrange
        // (no setup needed)

        // Act
        let it = MergingIterator::new(vec![Box::new(VecSource::new(vec![]))], None);
        let res: Vec<_> = it.collect();

        // Assert
        assert!(res.is_empty());
    }

    #[test]
    fn should_respect_limit_given_tombstones_when_merging() {
        // Arrange
        let s = VecSource::new(vec![
            kv("k1", "v1", 1),
            (Bytes::from("k2"), None, 2),
            kv("k3", "v3", 3),
            (Bytes::from("k4"), None, 4),
            kv("k5", "v5", 5),
        ]);

        // Act
        let it = MergingIterator::new(vec![Box::new(s)], Some(2));
        let res: Vec<_> = it.collect();

        // Assert
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].0, Bytes::from("k1"));
        assert_eq!(res[1].0, Bytes::from("k3"));
    }
}
