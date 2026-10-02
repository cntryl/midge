//! Sweep-line coverage of half-open key ranges.
//!
//! Scans visit keys in one direction. A range only matters from the first key
//! it can cover until the last, so it waits in a `pending` heap until the scan
//! reaches it, then lives in an `active` max-heap ordered by sequence until
//! the scan passes it. Each range is pushed and popped at most once per heap
//! and each query inspects one active head, so a scan of `K` keys over `T`
//! ranges costs `O((K + T) log T)` instead of `O(K * T)`.
//!
//! Ranges may be inserted lazily while the scan runs, but a range must be
//! inserted before the first key it covers is queried.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A range waiting for the scan to reach it.
struct Pending {
    /// Forward scans activate at `start`; reverse scans at `end`.
    activation: Vec<u8>,
    /// Forward scans retire at `end`; reverse scans at `start`.
    retirement: Vec<u8>,
    seq: u64,
    forward: bool,
}

impl PartialEq for Pending {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Pending {}
impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Pending {
    /// Max-heap order that surfaces the range the scan reaches first.
    fn cmp(&self, other: &Self) -> Ordering {
        let order = self.activation.cmp(&other.activation);
        if self.forward {
            order.reverse()
        } else {
            order
        }
    }
}

/// A range covering the current key, ordered by sequence.
struct Active {
    seq: u64,
    retirement: Vec<u8>,
}

impl PartialEq for Active {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq
    }
}
impl Eq for Active {}
impl PartialOrd for Active {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Active {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seq.cmp(&other.seq)
    }
}

/// Answers "highest sequence among ranges covering this key" for keys
/// queried in monotone scan order.
pub(crate) struct RangeCoverSweep {
    reverse: bool,
    pending: BinaryHeap<Pending>,
    active: BinaryHeap<Active>,
    position: Option<Vec<u8>>,
    work: u64,
}

impl RangeCoverSweep {
    pub(crate) fn new(reverse: bool) -> Self {
        Self {
            reverse,
            pending: BinaryHeap::new(),
            active: BinaryHeap::new(),
            position: None,
            work: 0,
        }
    }

    /// Deterministic count of heap operations and queries performed.
    #[cfg(test)]
    pub(crate) fn work(&self) -> u64 {
        self.work
    }

    /// Whether a range whose coverage ends at `retirement` is behind a scan
    /// positioned at `key`.
    fn retired(&self, retirement: &[u8], key: &[u8]) -> bool {
        if self.reverse {
            retirement > key
        } else {
            retirement <= key
        }
    }

    /// Whether a range first covering keys at `activation` has been reached
    /// by a scan positioned at `key`.
    fn reached(&self, activation: &[u8], key: &[u8]) -> bool {
        if self.reverse {
            activation > key
        } else {
            activation <= key
        }
    }

    /// Adds the half-open range `[start, end)` at `seq`.
    pub(crate) fn insert(&mut self, start: Vec<u8>, end: Vec<u8>, seq: u64) {
        if start >= end {
            return;
        }
        let (activation, retirement) = if self.reverse {
            (end, start)
        } else {
            (start, end)
        };
        if let Some(position) = &self.position {
            if self.reached(&activation, position) {
                if !self.retired(&retirement, position) {
                    self.work += 1;
                    self.active.push(Active { seq, retirement });
                }
                return;
            }
        }
        self.work += 1;
        self.pending.push(Pending {
            activation,
            retirement,
            seq,
            forward: !self.reverse,
        });
    }

    /// Highest sequence among ranges covering `key`. Keys must be queried in
    /// the scan's direction (non-decreasing forward, non-increasing reverse).
    pub(crate) fn max_cover_seq(&mut self, key: &[u8]) -> Option<u64> {
        self.work += 1;
        while self
            .pending
            .peek()
            .is_some_and(|head| self.reached(&head.activation, key))
        {
            self.work += 1;
            let range = self.pending.pop().expect("peeked");
            if !self.retired(&range.retirement, key) {
                self.work += 1;
                self.active.push(Active {
                    seq: range.seq,
                    retirement: range.retirement,
                });
            }
        }
        while self
            .active
            .peek()
            .is_some_and(|head| self.retired(&head.retirement, key))
        {
            self.work += 1;
            self.active.pop();
        }
        self.position = Some(key.to_vec());
        self.active.peek().map(|head| head.seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Range = (Vec<u8>, Vec<u8>, u64);

    fn oracle(ranges: &[Range], key: &[u8]) -> Option<u64> {
        ranges
            .iter()
            .filter(|(start, end, _)| start.as_slice() <= key && key < end.as_slice())
            .map(|(_, _, seq)| *seq)
            .max()
    }

    /// Small deterministic generator so failures reproduce.
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % bound
        }
    }

    fn key(n: u64) -> Vec<u8> {
        format!("{n:03}").into_bytes()
    }

    #[test]
    fn should_match_brute_force_when_ranges_nest_overlap_and_tie_in_both_directions() {
        for seed in 0..200 {
            for reverse in [false, true] {
                // Arrange
                let mut rng = Lcg(seed);
                let ranges: Vec<Range> = (0..24)
                    .map(|_| {
                        let a = rng.next(40);
                        let b = rng.next(40);
                        (key(a.min(b)), key(a.max(b) + 1), rng.next(6))
                    })
                    .collect();
                let mut sweep = RangeCoverSweep::new(reverse);
                for (start, end, seq) in &ranges {
                    sweep.insert(start.clone(), end.clone(), *seq);
                }
                let mut keys: Vec<u64> = (0..45).filter(|_| rng.next(3) != 0).collect();
                if reverse {
                    keys.reverse();
                }

                // Act / Assert
                for k in keys {
                    assert_eq!(
                        sweep.max_cover_seq(&key(k)),
                        oracle(&ranges, &key(k)),
                        "seed {seed} reverse {reverse} key {k}"
                    );
                }
            }
        }
    }

    #[test]
    fn should_cover_keys_when_ranges_are_inserted_lazily_during_the_sweep() {
        for reverse in [false, true] {
            // Arrange: each range is revealed just before the first key it
            // covers, as lazily opened files reveal their tombstones.
            let ranges: Vec<Range> = vec![(key(10), key(20), 3), (key(12), key(30), 5)];
            let mut sweep = RangeCoverSweep::new(reverse);
            let keys: Vec<u64> = if reverse {
                vec![25, 15, 11, 5]
            } else {
                vec![5, 11, 15, 25]
            };

            // Act
            let mut seen = Vec::new();
            let mut revealed = vec![false; ranges.len()];
            for k in &keys {
                for (index, (start, end, seq)) in ranges.iter().enumerate() {
                    let covers = start.as_slice() <= key(*k).as_slice() && key(*k) < *end;
                    if covers && !revealed[index] {
                        revealed[index] = true;
                        sweep.insert(start.clone(), end.clone(), *seq);
                    }
                }
                seen.push(sweep.max_cover_seq(&key(*k)));
            }

            // Assert
            let expected: Vec<Option<u64>> =
                keys.iter().map(|k| oracle(&ranges, &key(*k))).collect();
            assert_eq!(seen, expected, "reverse {reverse}");
        }
    }
}
