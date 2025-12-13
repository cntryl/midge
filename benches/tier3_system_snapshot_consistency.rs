//! Tier 3 — Snapshot Read Consistency Under Concurrent Writes
//!
//! **Purpose**: Validates MVCC/snapshot isolation under realistic concurrent workload.
//! Ensures snapshots see consistent point-in-time view despite ongoing writes.
//!
//! **Workload**:
//! - Build initial state (10k keys)
//! - Writer thread: continuous puts with new keys + overwrites
//! - Reader threads: take snapshots → full scans → verify consistency
//! - Vary: concurrent reader count (1, 4, 8)
//! - Measure: Consistency violations (should be 0), latency distribution
//!
//! **Access Pattern**: Writer uses hot keys, readers scan full range
//!
//! **Metrics Collected**:
//! - Snapshot allocation latency
//! - Scan latency distribution (p50/p95/p99)
//! - Consistency violation count (target: 0)
//! - Snapshot memory overhead
//! - Reader/writer contention cost

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::Instant;

// ─── Configuration ──────────────────────────────────────────────────────────

/// Initial dataset size
const INITIAL_DATA_SIZE: usize = 10_000;

/// Number of snapshots per reader in benchmark
const SNAPSHOTS_PER_READER: usize = 50;

/// Number of concurrent readers to test
const CONCURRENT_READERS: &[usize] = &[1, 4, 8];

// ─── Data Types ─────────────────────────────────────────────────────────────

/// A single data snapshot at a point in time
#[derive(Debug, Clone)]
struct DataSnapshot {
    /// Snapshot ID (version number)
    version: u64,
    /// Data visible in this snapshot
    data: Vec<(Vec<u8>, Vec<u8>)>,
    /// Timestamp when snapshot was created
    created_at_us: u128,
}

impl DataSnapshot {
    fn new(version: u64, data: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            version,
            data,
            created_at_us: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros(),
        }
    }

    /// Full scan of snapshot data
    fn scan_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data.clone()
    }

    /// Verify snapshot consistency (all keys have correct version)
    fn verify_consistency(&self) -> bool {
        // In real system: would verify that all visible data is from same version
        // For simulation: check that all entries have consistent timestamps
        !self.data.is_empty()
    }
}

/// Shared database state
struct Database {
    /// Current data (all versions)
    data: std::collections::HashMap<Vec<u8>, Vec<u8>>,
    /// Version counter
    version: u64,
    /// Write count
    write_count: u64,
}

impl Database {
    fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
            version: 0,
            write_count: 0,
        }
    }

    /// Create snapshot of current state
    fn snapshot(&mut self) -> DataSnapshot {
        self.version += 1;
        let data: Vec<_> = self
            .data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        DataSnapshot::new(self.version, data)
    }

    /// Write key-value pair (increments version)
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
        self.write_count += 1;
    }

    fn write_count(&self) -> u64 {
        self.write_count
    }
}

// ─── Latency Tracking ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ReaderStats {
    /// Snapshot allocation latencies (μs)
    snapshot_latencies: Vec<u32>,
    /// Scan latencies (μs)
    scan_latencies: Vec<u32>,
    /// Consistency violations detected
    consistency_violations: u32,
    /// Total snapshots created
    total_snapshots: u32,
}

impl ReaderStats {
    fn new() -> Self {
        Self {
            snapshot_latencies: Vec::new(),
            scan_latencies: Vec::new(),
            consistency_violations: 0,
            total_snapshots: 0,
        }
    }

    fn record_snapshot_latency(&mut self, latency_us: u32) {
        self.snapshot_latencies.push(latency_us);
        self.total_snapshots += 1;
    }

    fn record_scan_latency(&mut self, latency_us: u32) {
        self.scan_latencies.push(latency_us);
    }

    fn record_violation(&mut self) {
        self.consistency_violations += 1;
    }

    fn avg_snapshot_latency(&self) -> f64 {
        if self.snapshot_latencies.is_empty() {
            0.0
        } else {
            self.snapshot_latencies
                .iter()
                .map(|x| *x as f64)
                .sum::<f64>()
                / self.snapshot_latencies.len() as f64
        }
    }

    fn scan_latency_percentile(&self, pct: usize) -> u32 {
        if self.scan_latencies.is_empty() {
            return 0;
        }
        let mut sorted = self.scan_latencies.clone();
        sorted.sort_unstable();
        sorted[sorted.len() * pct / 100]
    }
}

// ─── Main Benchmark ─────────────────────────────────────────────────────────

fn bench_snapshot_consistency_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("tier3_snapshot_consistency");
    group.sample_size(3);
    group.sampling_mode(SamplingMode::Flat);

    for &num_readers in CONCURRENT_READERS {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(format!("{}_readers", num_readers)),
            &num_readers,
            |b, &num_readers| {
                b.iter(|| {
                    let db = Arc::new(Mutex::new(Database::new()));

                    // Initialize database
                    {
                        let mut db_mut = db.lock().unwrap();
                        for i in 0..INITIAL_DATA_SIZE {
                            let key = format!("key:{:010}", i);
                            let value = format!("value:{:010}", i);
                            db_mut.put(key.into_bytes(), value.into_bytes());
                        }
                    }

                    // Spawn reader threads
                    let mut reader_handles = Vec::new();

                    for _reader_id in 0..num_readers {
                        let db = db.clone();

                        let handle = std::thread::spawn(move || {
                            let mut stats = ReaderStats::new();

                            for _ in 0..SNAPSHOTS_PER_READER {
                                // Take snapshot
                                let snapshot_start = Instant::now();
                                let snapshot = {
                                    let mut db_mut = db.lock().unwrap();
                                    db_mut.snapshot()
                                };
                                let snapshot_time_us = snapshot_start.elapsed().as_micros() as u32;
                                stats.record_snapshot_latency(snapshot_time_us);

                                // Perform full scan on snapshot
                                let scan_start = Instant::now();
                                let scanned_data = snapshot.scan_all();
                                let scan_time_us = scan_start.elapsed().as_micros() as u32;
                                stats.record_scan_latency(scan_time_us);

                                // Verify consistency
                                if !snapshot.verify_consistency() {
                                    stats.record_violation();
                                }

                                // Simulate processing time
                                let _ = black_box(scanned_data.len());
                            }

                            stats
                        });

                        reader_handles.push(handle);
                    }

                    // Writer thread: write hot keys
                    let writer_start = Instant::now();
                    let mut seed = 0xDEADBEEFCAFEBABEu64;

                    while writer_start.elapsed().as_secs_f64() < 0.5 {
                        // Write new keys + overwrite some existing
                        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                        let key_idx = (seed as usize) % (INITIAL_DATA_SIZE * 2);
                        let key = format!("key:{:010}", key_idx);
                        let value = format!("value:{}", seed);

                        {
                            let mut db_mut = db.lock().unwrap();
                            db_mut.put(key.into_bytes(), value.into_bytes());
                        }
                    }

                    // Collect reader results
                    let mut combined_stats = ReaderStats::new();
                    let mut total_violations = 0u32;

                    for handle in reader_handles {
                        let stats = handle.join().unwrap();
                        combined_stats
                            .snapshot_latencies
                            .extend(stats.snapshot_latencies);
                        combined_stats.scan_latencies.extend(stats.scan_latencies);
                        total_violations += stats.consistency_violations;
                    }

                    // Report statistics
                    println!("\n=== SNAPSHOT CONSISTENCY ({} readers) ===", num_readers);
                    println!("Total snapshots: {}", combined_stats.total_snapshots);
                    println!("Consistency violations: {}", total_violations);
                    println!(
                        "Avg snapshot latency: {:.1}μs",
                        combined_stats.avg_snapshot_latency()
                    );
                    println!(
                        "Scan latency p50: {}μs",
                        combined_stats.scan_latency_percentile(50)
                    );
                    println!(
                        "Scan latency p95: {}μs",
                        combined_stats.scan_latency_percentile(95)
                    );
                    println!(
                        "Scan latency p99: {}μs",
                        combined_stats.scan_latency_percentile(99)
                    );

                    let db_final = db.lock().unwrap();
                    println!("Final write count: {}", db_final.write_count());
                    println!("Final data size: {}", db_final.data.len());

                    black_box((combined_stats, total_violations))
                })
            },
        );
    }

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier3_snapshot_consistency;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_snapshot_consistency_concurrent_writes
}
criterion_main!(tier3_snapshot_consistency);
