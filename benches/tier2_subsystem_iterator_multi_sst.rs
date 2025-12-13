//! Tier 2 — Iterator Traversal Across Multiple SSTs
//!
//! **Target Runtime:** 4-7 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! **Purpose**: Measures merge cost when iterator must traverse multiple SST files.
//! Validates that multi-SST iteration has acceptable overhead from key comparisons.
//!
//! **Tier-2 Compliance**:
//! - Subsystem interaction: Iterator → Multiple SSTs → Merge logic
//! - System metrics: Keys compared, SSTs accessed, merge overhead
//! - Realistic patterns: 2-5 SSTs with overlapping/disjoint key ranges

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::collections::BinaryHeap;
use std::hint::black_box;

// ─── Test Configuration ──────────────────────────────────────────────────────

const KEYS_PER_SST: usize = 1000;

/// Represents an SST with sorted keys
#[derive(Clone)]
struct MockSst {
    id: u64,
    keys: Vec<Bytes>,
}

impl MockSst {
    /// Create SST with keys in range [start, start+count)
    fn new(id: u64, start: usize, count: usize) -> Self {
        let keys = (start..(start + count))
            .map(|i| Bytes::from(format!("key:{:010}", i)))
            .collect();

        Self { id, keys }
    }

    /// Create iterator starting at index
    fn iter_from(&self, idx: usize) -> SstIterator {
        SstIterator {
            sst_id: self.id,
            keys: &self.keys,
            current: idx,
        }
    }
}

/// Iterator over single SST keys
struct SstIterator<'a> {
    sst_id: u64,
    keys: &'a [Bytes],
    current: usize,
}

impl<'a> SstIterator<'a> {
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

    fn peek(&self) -> Option<&Bytes> {
        self.keys.get(self.current)
    }
}

/// Entry in merge heap (reverse order for min-heap)
#[derive(Eq, PartialEq)]
struct HeapEntry {
    key: Bytes,
    sst_id: u64,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap
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

/// Merge iterator that combines multiple SST iterators
struct MergeIterator<'a> {
    iterators: Vec<SstIterator<'a>>,
    heap: BinaryHeap<HeapEntry>,
    keys_compared: usize,
}

impl<'a> MergeIterator<'a> {
    fn new(mut iterators: Vec<SstIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        // Initialize heap with first key from each iterator
        for (idx, iter) in iterators.iter_mut().enumerate() {
            if let Some((key, sst_id)) = iter.next() {
                heap.push(HeapEntry { key, sst_id });
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

            // Find the iterator that produced this key and advance it
            for iter in &mut self.iterators {
                if iter.sst_id == entry.sst_id {
                    if let Some((key, sst_id)) = iter.next() {
                        self.heap.push(HeapEntry { key, sst_id });
                    }
                    break;
                }
            }

            Some((entry.key, entry.sst_id))
        } else {
            None
        }
    }

    fn keys_compared(&self) -> usize {
        self.keys_compared
    }
}

// ─── Disjoint SSTs (No Overlap) ──────────────────────────────────────────────

/// Benchmark merging 2-5 SSTs with disjoint key ranges (no overlap)
fn bench_iterator_disjoint_ssts(c: &mut Criterion) {
    for &num_ssts in &[2, 3, 5] {
        let mut group = c.benchmark_group(format!("iterator_multi_sst_disjoint_{}_ssts", num_ssts));
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Elements((num_ssts * KEYS_PER_SST) as u64));

        group.bench_function("sequential_merge", |b| {
            // Create non-overlapping SSTs
            let ssts: Vec<MockSst> = (0..num_ssts)
                .map(|i| MockSst::new(i as u64, i * KEYS_PER_SST, KEYS_PER_SST))
                .collect();

            b.iter(|| {
                let iterators: Vec<SstIterator> = ssts.iter().map(|sst| sst.iter_from(0)).collect();
                let mut merge = MergeIterator::new(iterators);

                let mut count = 0;
                while merge.next().is_some() {
                    count += 1;
                }

                black_box((count, merge.keys_compared()))
            })
        });

        group.finish();
    }
}

// ─── Overlapping SSTs (Full Overlap) ─────────────────────────────────────────

/// Benchmark merging 2-5 SSTs with fully overlapping key ranges
fn bench_iterator_overlapping_ssts(c: &mut Criterion) {
    for &num_ssts in &[2, 3, 5] {
        let mut group =
            c.benchmark_group(format!("iterator_multi_sst_overlapping_{}_ssts", num_ssts));
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Elements((num_ssts * KEYS_PER_SST) as u64));

        group.bench_function("sequential_merge", |b| {
            // Create fully overlapping SSTs (same key range, different versions)
            let ssts: Vec<MockSst> = (0..num_ssts)
                .map(|i| MockSst::new(i as u64, 0, KEYS_PER_SST))
                .collect();

            b.iter(|| {
                let iterators: Vec<SstIterator> = ssts.iter().map(|sst| sst.iter_from(0)).collect();
                let mut merge = MergeIterator::new(iterators);

                let mut count = 0;
                let mut prev_key: Option<Bytes> = None;

                while let Some((key, _sst_id)) = merge.next() {
                    // Deduplicate same keys from different SSTs (latest wins)
                    if prev_key.as_ref() != Some(&key) {
                        count += 1;
                        prev_key = Some(key);
                    }
                }

                black_box((count, merge.keys_compared()))
            })
        });

        group.finish();
    }
}

// ─── Partially Overlapping SSTs ──────────────────────────────────────────────

/// Benchmark merging SSTs with 50% overlap (realistic LSM scenario)
fn bench_iterator_partial_overlap_ssts(c: &mut Criterion) {
    for &num_ssts in &[2, 3, 5] {
        let mut group = c.benchmark_group(format!("iterator_multi_sst_partial_{}_ssts", num_ssts));
        group.sampling_mode(SamplingMode::Flat);
        group.throughput(Throughput::Elements((num_ssts * KEYS_PER_SST) as u64));

        group.bench_function("50pct_overlap", |b| {
            // Create SSTs with 50% overlap
            let ssts: Vec<MockSst> = (0..num_ssts)
                .map(|i| {
                    let start = i * KEYS_PER_SST / 2;
                    MockSst::new(i as u64, start, KEYS_PER_SST)
                })
                .collect();

            b.iter(|| {
                let iterators: Vec<SstIterator> = ssts.iter().map(|sst| sst.iter_from(0)).collect();
                let mut merge = MergeIterator::new(iterators);

                let mut count = 0;
                let mut prev_key: Option<Bytes> = None;

                while let Some((key, _sst_id)) = merge.next() {
                    // Deduplicate overlapping keys
                    if prev_key.as_ref() != Some(&key) {
                        count += 1;
                        prev_key = Some(key);
                    }
                }

                black_box((count, merge.keys_compared()))
            })
        });

        group.finish();
    }
}

// ─── Comparison Benchmark ────────────────────────────────────────────────────

/// Direct comparison: 2 SSTs vs 5 SSTs with different overlap patterns
fn bench_iterator_multi_sst_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterator_multi_sst_comparison");
    group.sampling_mode(SamplingMode::Flat);

    for &(pattern, num_ssts) in &[
        ("disjoint_2ssts", 2),
        ("disjoint_5ssts", 5),
        ("overlapping_2ssts", 2),
        ("overlapping_5ssts", 5),
    ] {
        group.throughput(Throughput::Elements((num_ssts * KEYS_PER_SST) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(pattern),
            &(pattern, num_ssts),
            |b, &(pattern, num_ssts)| {
                if pattern.starts_with("disjoint") {
                    let ssts: Vec<MockSst> = (0..num_ssts)
                        .map(|i| MockSst::new(i as u64, i * KEYS_PER_SST, KEYS_PER_SST))
                        .collect();

                    b.iter(|| {
                        let iterators: Vec<SstIterator> =
                            ssts.iter().map(|sst| sst.iter_from(0)).collect();
                        let mut merge = MergeIterator::new(iterators);

                        let mut count = 0;
                        while merge.next().is_some() {
                            count += 1;
                        }

                        black_box((count, merge.keys_compared()))
                    })
                } else {
                    let ssts: Vec<MockSst> = (0..num_ssts)
                        .map(|i| MockSst::new(i as u64, 0, KEYS_PER_SST))
                        .collect();

                    b.iter(|| {
                        let iterators: Vec<SstIterator> =
                            ssts.iter().map(|sst| sst.iter_from(0)).collect();
                        let mut merge = MergeIterator::new(iterators);

                        let mut count = 0;
                        let mut prev_key: Option<Bytes> = None;

                        while let Some((key, _sst_id)) = merge.next() {
                            if prev_key.as_ref() != Some(&key) {
                                count += 1;
                                prev_key = Some(key);
                            }
                        }

                        black_box((count, merge.keys_compared()))
                    })
                }
            },
        );
    }

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier2_subsystem_iterator_multi_sst;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets =
        bench_iterator_disjoint_ssts,
        bench_iterator_overlapping_ssts,
        bench_iterator_partial_overlap_ssts,
        bench_iterator_multi_sst_comparison
}
criterion_main!(tier2_subsystem_iterator_multi_sst);
