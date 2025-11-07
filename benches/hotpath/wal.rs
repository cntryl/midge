//! Tier 1 — Hot Path WAL Benchmarks
//!
//! **Target Runtime:** < 3 seconds total
//! **Run Frequency:** Every PR (CI gate)
//!
//! ## Coverage
//!
//! This consolidated benchmark suite covers all aspects of WAL performance:
//!
//! 1. **Encoding/Decoding** - TLV format serialization performance
//! 2. **Fast Path Optimizations** - Specialized encoding functions
//! 3. **Parallel Encoding** - Multi-threaded batch encoding
//! 4. **Append Throughput** - Write performance across sync modes
//! 5. **I/O Performance** - Sequential throughput and sync latency
//! 6. **Raw I/O Baseline** - Pre-encoded writes (kernel-level baseline)
//! 7. **Platform Optimizations** - io_uring vs fallback comparison
//!
//! ## Design Goals
//!
//! - **Implementation-agnostic**: Benchmarks should work across fs::Wal, mem::WalMem, etc.
//! - **Realistic workloads**: Mix of operation types and sizes
//! - **Optimization guidance**: Clear baseline vs optimized comparisons
//! - **Durability modes**: NoSync, EveryWrite, GroupCommit coverage
//! - **Layer isolation**: Separate encoding, I/O, and platform concerns

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;
use hdrhistogram::Histogram;
use std::time::Instant;

use midge::fs::{io as fs_io, write_vectored};
use midge::wal::encode_pipeline::{EncodeConfig, WalEncoder};
use midge::wal::encoding::{decode, encode, encode_delete, encode_put_simple};
use midge::wal::{WalOpKind, WalRecord, WalSyncMode, WalWriter};
use std::hint::black_box;
use tempfile::tempdir;

// ============================================================================
// SECTION 1: ENCODING BENCHMARKS
// ============================================================================

/// Benchmark WAL record encoding (TLV format) with different sizes and operation types.
///
/// This measures the core serialization cost, which affects:
/// - Write path latency (encoding before append)
/// - Memory allocation patterns
/// - CPU efficiency during batch writes
fn bench_wal_encode_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_encode");

    let test_cases = vec![
        // Small records (typical cache keys)
        (
            "small_put",
            WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
                1,
            ),
        ),
        // Medium records (typical database rows)
        (
            "medium_put",
            WalRecord::new(
                WalOpKind::Put,
                Bytes::copy_from_slice(&[0u8; 64]),
                Some(Bytes::copy_from_slice(&[0u8; 256])),
                1,
            ),
        ),
        // Large records (documents, BLOBs)
        (
            "large_put",
            WalRecord::new(
                WalOpKind::Put,
                Bytes::copy_from_slice(&[0u8; 256]),
                Some(Bytes::copy_from_slice(&[0u8; 4096])),
                1,
            ),
        ),
        // Delete operations (no value)
        (
            "delete",
            WalRecord::new(
                WalOpKind::Delete,
                Bytes::from_static(b"deleted_key"),
                None,
                1,
            ),
        ),
        // Range delete (with range_end)
        ("range_delete", {
            let mut rec =
                WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"start_key"), None, 1);
            rec.range_end = Some(Bytes::from_static(b"end_key"));
            rec
        }),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, record) in test_cases {
        group.bench_function(name, |b| {
            b.iter(|| black_box(encode(&record).unwrap()));
        });
    }

    group.finish();
}

/// Benchmark WAL record decoding (TLV format) with different sizes.
///
/// This measures the cost of deserializing records during:
/// - WAL replay (crash recovery)
/// - Read operations from WAL
/// - Compaction reading from WAL
fn bench_wal_decode_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_decode");

    let test_cases = vec![
        (
            "small_put",
            encode(&WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
                1,
            ))
            .unwrap(),
        ),
        (
            "medium_put",
            encode(&WalRecord::new(
                WalOpKind::Put,
                Bytes::copy_from_slice(&[0u8; 64]),
                Some(Bytes::copy_from_slice(&[0u8; 256])),
                1,
            ))
            .unwrap(),
        ),
        (
            "large_put",
            encode(&WalRecord::new(
                WalOpKind::Put,
                Bytes::copy_from_slice(&[0u8; 256]),
                Some(Bytes::copy_from_slice(&[0u8; 4096])),
                1,
            ))
            .unwrap(),
        ),
        (
            "delete",
            encode(&WalRecord::new(
                WalOpKind::Delete,
                Bytes::from_static(b"deleted_key"),
                None,
                1,
            ))
            .unwrap(),
        ),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, encoded) in test_cases {
        group.bench_function(name, |b| {
            b.iter(|| black_box(decode(&encoded).unwrap()));
        });
    }

    group.finish();
}

/// Benchmark full encode -> decode round-trip.
///
/// This simulates the complete cycle:
/// 1. Write path: encode record
/// 2. Read/recovery path: decode record
///
/// Useful for understanding total serialization overhead.
fn bench_wal_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_roundtrip");

    let test_cases = vec![
        (
            "small",
            WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
                1,
            ),
        ),
        (
            "medium",
            WalRecord::new(
                WalOpKind::Put,
                Bytes::copy_from_slice(&[0u8; 64]),
                Some(Bytes::copy_from_slice(&[0u8; 256])),
                1,
            ),
        ),
        (
            "large",
            WalRecord::new(
                WalOpKind::Put,
                Bytes::copy_from_slice(&[0u8; 256]),
                Some(Bytes::copy_from_slice(&[0u8; 4096])),
                1,
            ),
        ),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, record) in test_cases {
        group.bench_function(name, |b| {
            b.iter(|| {
                let encoded = encode(&record).unwrap();
                black_box(decode(&encoded).unwrap())
            });
        });
    }

    group.finish();
}

// ============================================================================
// SECTION 2: FAST PATH OPTIMIZATIONS
// ============================================================================

/// Benchmark specialized encode_delete fast path vs normal encode.
///
/// The fast path should:
/// - Avoid allocation overhead
/// - Skip unnecessary field setup
/// - Use stack allocation where possible
///
/// **Optimization Target:** Fast path should be 2-3x faster than normal encode.
fn bench_wal_encode_delete_fast_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_encode_delete");

    let cf_id = 0;
    let seq = 1;
    let key = b"deleted_key";

    // Baseline: using normal encode() with a delete record
    let delete_record = WalRecord::new(WalOpKind::Delete, Bytes::from_static(key), None, seq);

    group.throughput(Throughput::Elements(1));

    group.bench_function("normal_encode", |b| {
        b.iter(|| black_box(encode(&delete_record).unwrap()));
    });

    group.bench_function("fast_path", |b| {
        b.iter(|| black_box(encode_delete(cf_id, seq, key)));
    });

    group.finish();
}

/// Benchmark specialized encode_put_simple fast path vs normal encode.
///
/// The fast path should:
/// - Pre-calculate buffer sizes
/// - Minimize allocations
/// - Use optimized TLV writing
///
/// **Optimization Target:** Fast path should be 2-3x faster than normal encode.
fn bench_wal_encode_put_fast_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_encode_put");

    let test_cases = vec![
        ("tiny", b"k" as &[u8], b"v" as &[u8]),
        ("small", b"test_key", b"test_value"),
        ("medium", &[0u8; 32], &[0u8; 128]),
        ("large", &[0u8; 128], &[0u8; 1024]),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, key, value) in test_cases {
        let cf_id = 0;
        let seq = 1;

        // Baseline: normal encode
        let put_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
            seq,
        );

        group.bench_function(format!("{}_normal", name), |b| {
            b.iter(|| black_box(encode(&put_record).unwrap()));
        });

        group.bench_function(format!("{}_fast", name), |b| {
            b.iter(|| black_box(encode_put_simple(cf_id, seq, key, value)));
        });
    }

    group.finish();
}

// ============================================================================
// SECTION 3: PARALLEL ENCODING
// ============================================================================

/// Benchmark sequential vs parallel batch encoding for different batch sizes.
///
/// This measures the effectiveness of parallel encoding, showing:
/// - Where parallelism starts to pay off (batch size threshold)
/// - Scalability with number of threads
/// - Overhead of thread coordination
///
/// **Key Insight:** Small batches (<50) may be faster sequential due to overhead.
fn bench_wal_batch_encode_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_parallel_encode");

    // Test different batch sizes to identify parallelism threshold
    for batch_size in [10, 50, 100, 500, 1000] {
        // Create test batch with realistic 1KB values
        let records: Vec<WalRecord> = (0..batch_size)
            .map(|i| {
                WalRecord::new(
                    WalOpKind::Put,
                    Bytes::from(format!("key{:08}", i)),
                    Some(Bytes::from(vec![0u8; 1024])),
                    i as u64,
                )
            })
            .collect();

        group.throughput(Throughput::Elements(batch_size as u64));

        // Sequential baseline
        let encoder_seq = WalEncoder::with_config(EncodeConfig {
            parallelism: 1,
            max_body_len: u32::MAX as usize,
            parallel_threshold_bytes: 1,
        })
        .unwrap();

        group.bench_with_input(
            BenchmarkId::new("sequential", batch_size),
            &records,
            |b, recs| {
                b.iter(|| encoder_seq.encode_batch(recs).unwrap());
            },
        );

        // Parallel with 2 threads
        let encoder_par2 = WalEncoder::with_config(EncodeConfig {
            parallelism: 2,
            max_body_len: u32::MAX as usize,
            parallel_threshold_bytes: 1,
        })
        .unwrap();

        group.bench_with_input(
            BenchmarkId::new("parallel_2", batch_size),
            &records,
            |b, recs| {
                b.iter(|| encoder_par2.encode_batch(recs).unwrap());
            },
        );

        // Parallel with 4 threads
        let encoder_par4 = WalEncoder::with_config(EncodeConfig {
            parallelism: 4,
            max_body_len: u32::MAX as usize,
            parallel_threshold_bytes: 1,
        })
        .unwrap();

        group.bench_with_input(
            BenchmarkId::new("parallel_4", batch_size),
            &records,
            |b, recs| {
                b.iter(|| encoder_par4.encode_batch(recs).unwrap());
            },
        );
    }

    group.finish();
}

/// Benchmark realistic mixed workload with varied record sizes.
///
/// Simulates production workload:
/// - 80% small records (256 bytes)
/// - 10% medium records (1KB)
/// - 10% large records (4KB)
///
/// This tests how parallel encoding handles non-uniform work distribution.
fn bench_wal_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_mixed_workload");

    // Create realistic mixed workload
    let records: Vec<WalRecord> = (0..200)
        .map(|i| {
            let value_size = match i % 10 {
                9 => 4096, // 10% large (4KB)
                8 => 1024, // 10% medium (1KB)
                _ => 256,  // 80% small (256B)
            };
            WalRecord::new(
                WalOpKind::Put,
                Bytes::from(format!("key{:08}", i)),
                Some(Bytes::from(vec![0u8; value_size])),
                i as u64,
            )
        })
        .collect();

    group.throughput(Throughput::Elements(200));

    // Sequential
    let encoder_seq = WalEncoder::with_config(EncodeConfig {
        parallelism: 1,
        max_body_len: u32::MAX as usize,
        parallel_threshold_bytes: 1,
    })
    .unwrap();

    group.bench_function("sequential", |b| {
        b.iter(|| encoder_seq.encode_batch(&records).unwrap());
    });

    // Parallel (auto-detect physical cores)
    let encoder_par = WalEncoder::with_defaults().unwrap();

    group.bench_function("parallel_auto", |b| {
        b.iter(|| encoder_par.encode_batch(&records).unwrap());
    });

    group.finish();
}

// ============================================================================
// SECTION 4: APPEND THROUGHPUT
// ============================================================================

/// Benchmark WAL append throughput for individual record appends.
///
/// This measures the performance of the most common write pattern:
/// - Individual append_record() or append_op() calls
/// - Different sync modes (durability vs performance tradeoff)
/// - Scaling behavior with batch sizes
///
/// **Performance Expectations:**
/// - NoSync: 500K+ ops/sec (limited by encoding + I/O buffering)
/// - EveryWrite: 1K-10K ops/sec (limited by fsync)
/// - GroupCommit: 50K-200K ops/sec (amortized fsync across batch)
///
/// **Optimization Opportunities:**
/// - Reduce encoding overhead (fast paths)
/// - Minimize syscall overhead
/// - Optimize buffer management
fn bench_wal_append_individual(c: &mut Criterion) {
    let sync_modes = [
        ("nosync", WalSyncMode::NoSync),
        ("everywrite", WalSyncMode::EveryWrite),
        ("groupcommit", WalSyncMode::GroupCommit),
    ];

    for (mode_name, sync_mode) in &sync_modes {
        let mut group = c.benchmark_group(format!("hotpath_wal_append_individual_{}", mode_name));

        // Vary batch size to understand scaling
        for batch_size in &[1, 10, 50, 100, 500, 1000] {
            group.throughput(Throughput::Elements(*batch_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(batch_size),
                batch_size,
                |b, &size| {
                    let tmp = tempdir().expect("tempdir");
                    let dir = tmp.path();

                    let mut writer =
                        midge::wal::fs::Wal::open_with_mode(dir, *sync_mode).expect("open WAL");

                    // Pre-create records with realistic 18-byte values
                    let records: Vec<WalRecord> = (0..size)
                        .map(|i| {
                            WalRecord::new(
                                WalOpKind::Put,
                                Bytes::copy_from_slice(format!("key{:08}", i).as_bytes()),
                                Some(Bytes::copy_from_slice(b"value_data_payload")),
                                i as u64,
                            )
                        })
                        .collect();

                    b.iter(|| {
                        // Fixed total operations per iteration to keep benchmark stable
                        // Reduce for durable modes to keep runtime reasonable
                        let total_ops: usize = if *sync_mode == WalSyncMode::NoSync {
                            1_000
                        } else {
                            100 // Much smaller for fsync modes
                        };
                        let rounds = total_ops.div_ceil(size);
                        for _ in 0..rounds {
                            for record in &records {
                                writer.append(record).expect("append");
                            }
                        }
                    })
                },
            );
        }

        group.finish();
    }
}

/// Benchmark WAL batch append throughput.
///
/// This measures the optimized batch write path:
/// - append_batch() with multiple records
/// - Amortized encoding and I/O costs
/// - Sync mode impact on batch performance
///
/// **Performance Expectations:**
/// - NoSync: 1M+ ops/sec (parallel encoding + buffered I/O)
/// - EveryWrite: 10K-50K ops/sec (one fsync per batch)
/// - GroupCommit: 200K-500K ops/sec (optimal sync batching)
///
/// **Optimization Opportunities:**
/// - Parallel encoding for large batches
/// - Vectored I/O (writev)
/// - Pre-allocation of buffers
fn bench_wal_append_batch(c: &mut Criterion) {
    let sync_modes = [
        ("nosync", WalSyncMode::NoSync),
        ("everywrite", WalSyncMode::EveryWrite),
        ("groupcommit", WalSyncMode::GroupCommit),
    ];

    for (mode_name, sync_mode) in &sync_modes {
        let mut group = c.benchmark_group(format!("hotpath_wal_append_batch_{}", mode_name));

        for batch_size in &[1, 10, 50, 100, 500, 1000] {
            group.throughput(Throughput::Elements(*batch_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(batch_size),
                batch_size,
                |b, &size| {
                    let tmp = tempdir().expect("tempdir");
                    let dir = tmp.path();

                    let writer =
                        midge::wal::fs::Wal::open_with_mode(dir, *sync_mode).expect("open WAL");

                    // Pre-create records
                    let records: Vec<WalRecord> = (0..size)
                        .map(|i| {
                            WalRecord::new(
                                WalOpKind::Put,
                                Bytes::copy_from_slice(format!("key{:08}", i).as_bytes()),
                                Some(Bytes::copy_from_slice(b"value_data_payload")),
                                i as u64,
                            )
                        })
                        .collect();

                    b.iter(|| {
                        // Fixed total elements per iteration
                        // Reduce for durable modes to keep runtime reasonable
                        let total_ops: usize = if *sync_mode == WalSyncMode::NoSync {
                            5_000
                        } else {
                            500 // Much smaller for fsync modes
                        };
                        let rounds = total_ops.div_ceil(size);
                        for _ in 0..rounds {
                            writer.append_batch(&records).expect("append_batch");
                        }
                    })
                },
            );
        }

        group.finish();
    }
}

// ============================================================================
// SECTION 5: I/O PERFORMANCE
// ============================================================================
//
// Benchmarks that focus on I/O throughput and latency at the WAL writer level.
// These measure the cost of writing to disk with different sync modes.

/// Sequential append throughput using NoSync mode (buffered writes)
///
/// This benchmark measures pure append throughput without fsync overhead,
/// showing the maximum write rate the WAL can sustain when durability is
/// handled asynchronously (e.g., via group commit or background sync).
fn bench_wal_io_seq_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_io_seq_throughput");

    // Test different record sizes to show scaling
    for (name, value_size) in &[
        ("small", 64usize),
        ("medium", 512usize),
        ("large", 4096usize),
    ] {
        group.throughput(Throughput::Bytes(*value_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), value_size, |b, &size| {
            let tmp = tempdir().expect("tempdir");
            let dir = tmp.path();

            let mut writer =
                midge::wal::fs::Wal::open_with_mode(dir, WalSyncMode::NoSync).expect("open WAL");

            // Pre-create a single record template
            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"key_template"),
                Some(Bytes::from(vec![0u8; size])),
                1,
            );

            b.iter(|| {
                // Perform a burst of appends per iteration to reduce overhead
                for _ in 0..1000_usize {
                    writer.append(&record).expect("append");
                }
            })
        });
    }

    group.finish();
}

/// Measure append + sync latency (EveryWrite).
///
/// This benchmark measures the critical path latency when durability is required
/// immediately (fsync on every write). It collects a latency histogram and prints
/// quantiles (p50/p95/p99) to help identify outliers and variance.
///
/// Typical results:
/// - p50: 0.5-2ms (SSD)
/// - p99: 5-10ms (depends on OS page cache, storage type)
fn bench_wal_io_append_sync_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_io_append_sync_latency");

    group.bench_function("append_sync_p99", |b| {
        b.iter_custom(|iters| {
            let tmp = tempdir().expect("tempdir");
            let dir = tmp.path();

            let mut writer = midge::wal::fs::Wal::open_with_mode(dir, WalSyncMode::EveryWrite)
                .expect("open WAL");

            // Use a compact payload to emphasize sync cost
            let record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"k"),
                Some(Bytes::from(vec![0u8; 128])),
                1,
            );

            let mut hist = Histogram::<u64>::new(3).expect("histogram");
            let start = Instant::now();
            for _ in 0..iters {
                let t0 = Instant::now();
                writer.append(&record).expect("append");
                writer.sync().expect("sync");
                let us = t0.elapsed().as_micros() as u64;
                let _ = hist.record(us);
            }

            let total = start.elapsed();
            // Print quantiles to stderr so CI logs capture them. Gate via BENCH_PRINT
            // so local runs remain quiet unless BENCH_PRINT is set in the environment.
            if std::env::var("BENCH_PRINT").is_ok() {
                eprintln!(
                    "WAL append+sync latency (us) p50={} p95={} p99={} max={}",
                    hist.value_at_quantile(0.50),
                    hist.value_at_quantile(0.95),
                    hist.value_at_quantile(0.99),
                    hist.max()
                );
            }

            total
        })
    });

    group.finish();
}

// ============================================================================
// SECTION 6: RAW I/O BASELINE
// ============================================================================
//
// Benchmarks using pre-encoded WAL fragments to isolate raw kernel I/O cost,
// eliminating encoding and buffering overhead from the measurement.

/// Benchmark raw I/O using pre-encoded WAL fragments
///
/// This establishes a baseline for I/O performance by pre-encoding the WAL record
/// and measuring only the write_vectored syscall cost. Useful for:
/// - Identifying encoding vs I/O overhead
/// - Platform comparison (different filesystems, storage devices)
/// - Validating that optimizations aren't just moving work around
fn bench_wal_io_preencoded(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_io_preencoded");

    group.throughput(Throughput::Bytes(1024));

    group.bench_function("preencoded_append_nosync", |b| {
        b.iter(|| {
            let tmp = tempdir().expect("tempdir");
            let path = tmp.path().join("raw_wal_test.wal");
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("open file");

            let encoder = WalEncoder::with_defaults().expect("encoder");
            let rec = WalRecord::new(
                WalOpKind::Put,
                Bytes::from_static(b"k"),
                Some(Bytes::from(vec![0u8; 1024])),
                1,
            );
            let frag = encoder.encode_one(&rec).expect("encode");

            // Write header + body using our fs abstraction
            write_vectored(&mut file, &[&frag.header, &frag.body]).expect("write_vectored");
        })
    });

    group.finish();
}

// ============================================================================
// SECTION 7: PLATFORM OPTIMIZATIONS
// ============================================================================
//
// Benchmarks that compare platform-specific I/O implementations (io_uring vs fallback)
// to validate that platform-specific code paths provide actual benefits.

/// Compare fallback writev vs io_uring-backed writev (when enabled)
///
/// This benchmark compares the standard writev fallback implementation against
/// the io_uring path (when compiled with the `io_uring` feature). On systems
/// without io_uring support, both paths will be identical.
///
/// Expected results (Linux with io_uring):
/// - Fallback: ~2-5μs per write
/// - io_uring: ~1-3μs per write (lower syscall overhead)
fn bench_wal_io_uring_compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_wal_io_uring_compare");

    for &size in &[1024usize, 4096usize, 65536usize] {
        group.throughput(Throughput::Bytes(size as u64));

        // Fallback writev implementation
        group.bench_with_input(BenchmarkId::new("fallback", size), &size, |b, &s| {
            b.iter(|| {
                let tmp = tempdir().expect("tempdir");
                let path = tmp.path().join("raw_wal_cmp.wal");
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .expect("open");

                let encoder = WalEncoder::with_defaults().expect("encoder");
                let rec = WalRecord::new(
                    WalOpKind::Put,
                    Bytes::from_static(b"k"),
                    Some(Bytes::from(vec![0u8; s])),
                    1,
                );
                let frag = encoder.encode_one(&rec).expect("encode");

                // Call the internal fallback via the fs::io module
                fs_io::write_vectored_fallback(&mut file, &[&frag.header, &frag.body])
                    .expect("fallback write");
            })
        });

        // io_uring dispatch (will use io_uring if feature enabled, otherwise same as fallback)
        group.bench_with_input(BenchmarkId::new("dispatch", size), &size, |b, &s| {
            b.iter(|| {
                let tmp = tempdir().expect("tempdir");
                let path = tmp.path().join("raw_wal_cmp.wal");
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .expect("open");

                let encoder = WalEncoder::with_defaults().expect("encoder");
                let rec = WalRecord::new(
                    WalOpKind::Put,
                    Bytes::from_static(b"k"),
                    Some(Bytes::from(vec![0u8; s])),
                    1,
                );
                let frag = encoder.encode_one(&rec).expect("encode");

                write_vectored(&mut file, &[&frag.header, &frag.body]).expect("write");
            })
        });
    }

    group.finish();
}

// ============================================================================
// CRITERION CONFIGURATION
// ============================================================================

criterion_group! {
    name = hotpath_wal;
    config = criterion_config();
    targets =
        // Section 1: Encoding
        bench_wal_encode_record,
        bench_wal_decode_record,
        bench_wal_roundtrip,
        // Section 2: Fast Paths
        bench_wal_encode_delete_fast_path,
        bench_wal_encode_put_fast_path,
        // Section 3: Parallel Encoding
        bench_wal_batch_encode_comparison,
        bench_wal_mixed_workload,
        // Section 4: Append Throughput
        bench_wal_append_individual,
        bench_wal_append_batch,
        // Section 5: I/O Performance
        bench_wal_io_seq_throughput,
        bench_wal_io_append_sync_latency,
        // Section 6: Raw I/O Baseline
        bench_wal_io_preencoded,
        // Section 7: Platform Optimizations
        bench_wal_io_uring_compare
}
criterion_main!(hotpath_wal);
