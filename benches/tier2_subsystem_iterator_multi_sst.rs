//! Tier 2 — Iterator Traversal Across Multiple SSTs
//!
//! Measures merge cost across multiple synthetic SST iterators.

use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};
use std::collections::BinaryHeap;

const KEYS_PER_SST: usize = 1000;
const MERGE_REPEATS: usize = 1024;

#[derive(Clone)]
struct MockSst {
    id: u64,
    keys: Vec<Bytes>,
}

impl MockSst {
    fn new(id: u64, start: usize, count: usize) -> Self {
        let keys = (start..(start + count))
            .map(|i| Bytes::from(format!("key:{i:010}")))
            .collect();

        Self { id, keys }
    }

    fn iter_from(&self, idx: usize) -> SstIterator<'_> {
        SstIterator {
            sst_id: self.id,
            keys: &self.keys,
            current: idx,
        }
    }
}

struct SstIterator<'a> {
    sst_id: u64,
    keys: &'a [Bytes],
    current: usize,
}

impl SstIterator<'_> {
    fn next(&mut self) -> Option<(Bytes, u64)> {
        if self.current < self.keys.len() {
            let key = self.keys[self.current].clone();
            let sst_id = self.sst_id;
            self.current += 1;
            Some((key, sst_id))
        } else {
            None
        }
    }
}

#[derive(Eq, PartialEq)]
struct HeapEntry {
    key: Bytes,
    sst_id: u64,
    iter_idx: u32,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.sst_id.cmp(&self.sst_id))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct MergeIterator<'a> {
    iterators: Vec<SstIterator<'a>>,
    heap: BinaryHeap<HeapEntry>,
    keys_compared: usize,
}

impl<'a> MergeIterator<'a> {
    fn new(mut iterators: Vec<SstIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, iter) in iterators.iter_mut().enumerate() {
            if let Some((key, sst_id)) = iter.next() {
                heap.push(HeapEntry {
                    key,
                    sst_id,
                    iter_idx: u32::try_from(idx).expect("iterator index fits in u32"),
                });
            }
        }

        Self {
            iterators,
            heap,
            keys_compared: 0,
        }
    }

    fn next(&mut self) -> Option<(Bytes, u64)> {
        if let Some(entry) = self.heap.pop() {
            self.keys_compared += 1;

            let iter = &mut self.iterators[entry.iter_idx as usize];
            if let Some((key, sst_id)) = iter.next() {
                self.heap.push(HeapEntry {
                    key,
                    sst_id,
                    iter_idx: entry.iter_idx,
                });
            }

            Some((entry.key, entry.sst_id))
        } else {
            None
        }
    }

    const fn keys_compared(&self) -> usize {
        self.keys_compared
    }
}

fn disjoint_ssts(num_ssts: usize) -> Vec<MockSst> {
    (0..num_ssts)
        .map(|i| MockSst::new(i as u64, i * KEYS_PER_SST, KEYS_PER_SST))
        .collect()
}

fn overlapping_ssts(num_ssts: usize) -> Vec<MockSst> {
    (0..num_ssts)
        .map(|i| MockSst::new(i as u64, 0, KEYS_PER_SST))
        .collect()
}

fn partial_overlap_ssts(num_ssts: usize) -> Vec<MockSst> {
    (0..num_ssts)
        .map(|i| {
            let start = i * KEYS_PER_SST / 2;
            MockSst::new(i as u64, start, KEYS_PER_SST)
        })
        .collect()
}

fn run_disjoint(ctx: &mut StressContext, scenario: &'static str, num_ssts: usize) {
    let ssts = disjoint_ssts(num_ssts);
    ctx.parameter("pattern", "disjoint");
    ctx.parameter("sst_count", num_ssts);
    ctx.parameter("keys_per_sst", KEYS_PER_SST);
    ctx.parameter("merge_repeats", MERGE_REPEATS);

    let _completed = ctx.measure_batch(
        scenario,
        (num_ssts * KEYS_PER_SST * MERGE_REPEATS) as u64,
        || {
            let mut total_count = 0usize;
            let mut total_compared = 0usize;
            for _ in 0..MERGE_REPEATS {
                let iterators: Vec<SstIterator> = ssts.iter().map(|sst| sst.iter_from(0)).collect();
                let mut merge = MergeIterator::new(iterators);
                let mut count = 0usize;
                while merge.next().is_some() {
                    count += 1;
                }
                total_count += count;
                total_compared += merge.keys_compared();
            }
            black_box((total_count, total_compared));
        },
    );
}

fn run_deduping_merge(
    ctx: &mut StressContext,
    scenario: &'static str,
    pattern: &'static str,
    ssts: &[MockSst],
) {
    let num_ssts = ssts.len();
    ctx.parameter("pattern", pattern);
    ctx.parameter("sst_count", num_ssts);
    ctx.parameter("keys_per_sst", KEYS_PER_SST);
    ctx.parameter("merge_repeats", MERGE_REPEATS);

    let _completed = ctx.measure_batch(
        scenario,
        (num_ssts * KEYS_PER_SST * MERGE_REPEATS) as u64,
        || {
            let mut total_count = 0usize;
            let mut total_compared = 0usize;
            for _ in 0..MERGE_REPEATS {
                let iterators: Vec<SstIterator> = ssts.iter().map(|sst| sst.iter_from(0)).collect();
                let mut merge = MergeIterator::new(iterators);
                let mut count = 0usize;
                let mut prev_key: Option<Bytes> = None;

                while let Some((key, _sst_id)) = merge.next() {
                    if prev_key.as_ref() != Some(&key) {
                        count += 1;
                        prev_key = Some(key);
                    }
                }

                total_count += count;
                total_compared += merge.keys_compared();
            }
            black_box((total_count, total_compared));
        },
    );
}

#[stress(
    tier = 2,
    metadata(component = "iterator_multi_sst", scenario = "disjoint_2")
)]
fn disjoint_2(ctx: &mut StressContext) {
    run_disjoint(ctx, "disjoint_2", 2);
}

#[stress(
    tier = 2,
    metadata(component = "iterator_multi_sst", scenario = "disjoint_3")
)]
fn disjoint_3(ctx: &mut StressContext) {
    run_disjoint(ctx, "disjoint_3", 3);
}

#[stress(
    tier = 2,
    metadata(component = "iterator_multi_sst", scenario = "disjoint_5")
)]
fn disjoint_5(ctx: &mut StressContext) {
    run_disjoint(ctx, "disjoint_5", 5);
}

#[stress(
    tier = 2,
    metadata(component = "iterator_multi_sst", scenario = "overlapping_2")
)]
fn overlapping_2(ctx: &mut StressContext) {
    run_deduping_merge(ctx, "overlapping_2", "overlapping", &overlapping_ssts(2));
}

#[stress(
    tier = 2,
    metadata(component = "iterator_multi_sst", scenario = "overlapping_3")
)]
fn overlapping_3(ctx: &mut StressContext) {
    run_deduping_merge(ctx, "overlapping_3", "overlapping", &overlapping_ssts(3));
}

#[stress(
    tier = 2,
    metadata(component = "iterator_multi_sst", scenario = "overlapping_5")
)]
fn overlapping_5(ctx: &mut StressContext) {
    run_deduping_merge(ctx, "overlapping_5", "overlapping", &overlapping_ssts(5));
}

#[stress(
    tier = 2,
    metadata(component = "iterator_multi_sst", scenario = "partial_2")
)]
fn partial_2(ctx: &mut StressContext) {
    run_deduping_merge(ctx, "partial_2", "partial_50pct", &partial_overlap_ssts(2));
}

#[stress(
    tier = 2,
    metadata(component = "iterator_multi_sst", scenario = "partial_3")
)]
fn partial_3(ctx: &mut StressContext) {
    run_deduping_merge(ctx, "partial_3", "partial_50pct", &partial_overlap_ssts(3));
}

#[stress(
    tier = 2,
    metadata(component = "iterator_multi_sst", scenario = "partial_5")
)]
fn partial_5(ctx: &mut StressContext) {
    run_deduping_merge(ctx, "partial_5", "partial_50pct", &partial_overlap_ssts(5));
}

stress_main!();
