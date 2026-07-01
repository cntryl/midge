//! Helpers for Tier-4 (YCSB) stress workloads.
//!
//! These helpers are intentionally deterministic and "boring": Tier-4 aims to
//! measure steady-state behavior over time (load → warm-up → measured).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::SystemTime;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use crate::engine::api;
use crate::{ColumnFamilyHandle, Engine, MidgeEngine, MidgeError, MidgeResult};

use super::config::MidgeOptions;

pub const KEY_SIZE: usize = 16;
pub const DEFAULT_VALUE_SIZE: usize = 128;

pub const TIER4_MEMTABLE_SIZE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default)]
pub struct MultiClientRunStats {
    pub operations: u64,
    pub latency_p50_us: u64,
    pub latency_p95_us: u64,
    pub latency_p99_us: u64,
    pub latency_max_us: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RuntimePerfSnapshot {
    pub write_stalls_total: u64,
    pub write_stalls_memory_total: u64,
    pub write_stalls_compaction_total: u64,
    pub write_stalls_cloud_total: u64,
    pub write_stalls_no_space_total: u64,
    pub wal_append_count: u64,
    pub wal_fsync_count: u64,
    pub wal_append_ns_total: u64,
    pub wal_fsync_ns_total: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cloud_async_wal_segments_sealed: u64,
    pub cloud_async_wal_uploads_started: u64,
    pub cloud_async_wal_uploads_completed: u64,
    pub cloud_async_wal_uploads_failed: u64,
    pub cloud_async_wal_seal_latency_us: u64,
    pub cloud_async_wal_upload_latency_us: u64,
    pub cloud_async_wal_ack_latency_us: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RuntimePerfReport {
    pub end_pending_cloud_uploads: usize,
    pub end_wal_local_durable_seq: u64,
    pub end_wal_cloud_durable_seq: u64,
    pub end_hybrid_max_local_bytes: u64,
    pub end_hybrid_total_committed_bytes: u64,
    pub end_hybrid_free_bytes: u64,
    pub end_hybrid_usage_percent: u32,
    pub end_hybrid_pending_evictions: usize,
    pub write_stalls_total: u64,
    pub write_stalls_memory_total: u64,
    pub write_stalls_compaction_total: u64,
    pub write_stalls_cloud_total: u64,
    pub write_stalls_no_space_total: u64,
    pub wal_append_count: u64,
    pub wal_fsync_count: u64,
    pub wal_append_ns_total: u64,
    pub wal_fsync_ns_total: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cloud_async_wal_segments_sealed: u64,
    pub cloud_async_wal_uploads_started: u64,
    pub cloud_async_wal_uploads_completed: u64,
    pub cloud_async_wal_uploads_failed: u64,
    pub cloud_async_wal_seal_latency_us: u64,
    pub cloud_async_wal_upload_latency_us: u64,
    pub cloud_async_wal_ack_latency_us: u64,
}

impl RuntimePerfReport {
    #[must_use]
    pub fn tags(&self) -> Vec<(&'static str, u64)> {
        let mut tags = vec![
            ("write_stalls", self.write_stalls_total),
            ("write_stalls_memory", self.write_stalls_memory_total),
            (
                "write_stalls_compaction",
                self.write_stalls_compaction_total,
            ),
            ("write_stalls_cloud", self.write_stalls_cloud_total),
            ("write_stalls_no_space", self.write_stalls_no_space_total),
            ("wal_append_count", self.wal_append_count),
            ("wal_fsync_count", self.wal_fsync_count),
            (
                "avg_wal_append_us",
                average_u64(self.wal_append_ns_total, self.wal_append_count * 1_000),
            ),
            (
                "avg_wal_sync_us",
                average_u64(self.wal_fsync_ns_total, self.wal_fsync_count * 1_000),
            ),
            ("cache_hits", self.cache_hits),
            ("cache_misses", self.cache_misses),
            (
                "cache_hit_ratio_ppm",
                ratio_ppm(self.cache_hits, self.cache_misses),
            ),
            (
                "cloud_async_wal_segments_sealed",
                self.cloud_async_wal_segments_sealed,
            ),
            (
                "cloud_async_wal_uploads_started",
                self.cloud_async_wal_uploads_started,
            ),
            (
                "cloud_async_wal_uploads_completed",
                self.cloud_async_wal_uploads_completed,
            ),
            (
                "cloud_async_wal_uploads_failed",
                self.cloud_async_wal_uploads_failed,
            ),
            (
                "avg_cloud_async_wal_seal_us",
                average_u64(
                    self.cloud_async_wal_seal_latency_us,
                    self.cloud_async_wal_segments_sealed,
                ),
            ),
            (
                "avg_cloud_async_wal_upload_us",
                average_u64(
                    self.cloud_async_wal_upload_latency_us,
                    self.cloud_async_wal_uploads_completed,
                ),
            ),
            (
                "avg_cloud_async_wal_ack_us",
                average_u64(
                    self.cloud_async_wal_ack_latency_us,
                    self.cloud_async_wal_uploads_completed,
                ),
            ),
            (
                "pending_cloud_uploads_end",
                usize_to_u64(self.end_pending_cloud_uploads),
            ),
            ("wal_local_durable_seq_end", self.end_wal_local_durable_seq),
            ("wal_cloud_durable_seq_end", self.end_wal_cloud_durable_seq),
            (
                "wal_cloud_durable_lag_end",
                self.end_wal_local_durable_seq
                    .saturating_sub(self.end_wal_cloud_durable_seq),
            ),
            ("hybrid_max_local_bytes", self.end_hybrid_max_local_bytes),
            (
                "hybrid_total_committed_bytes",
                self.end_hybrid_total_committed_bytes,
            ),
            ("hybrid_free_bytes", self.end_hybrid_free_bytes),
            (
                "hybrid_usage_percent",
                u64::from(self.end_hybrid_usage_percent),
            ),
            (
                "hybrid_pending_evictions",
                usize_to_u64(self.end_hybrid_pending_evictions),
            ),
        ];
        tags.retain(|(name, value)| should_emit_runtime_perf_tag(name, *value));
        tags
    }
}

fn should_emit_runtime_perf_tag(name: &str, value: u64) -> bool {
    value > 0
        || matches!(
            name,
            "pending_cloud_uploads_end"
                | "wal_cloud_durable_lag_end"
                | "hybrid_max_local_bytes"
                | "hybrid_total_committed_bytes"
                | "hybrid_free_bytes"
                | "hybrid_usage_percent"
                | "hybrid_pending_evictions"
        )
}

impl MultiClientRunStats {
    #[must_use]
    pub fn latency_tags(&self) -> [(&'static str, u64); 4] {
        [
            ("latency_p50_us", self.latency_p50_us),
            ("latency_p95_us", self.latency_p95_us),
            ("latency_p99_us", self.latency_p99_us),
            ("latency_max_us", self.latency_max_us),
        ]
    }
}

struct ClientRunStats {
    operations: u64,
    latency_us: Histogram<u64>,
}

impl ClientRunStats {
    fn empty() -> Self {
        Self {
            operations: 0,
            latency_us: Histogram::<u64>::new(3).expect("create client latency histogram"),
        }
    }

    fn record_latency(&mut self, elapsed: Duration) {
        self.latency_us
            .record(duration_to_micros(elapsed))
            .expect("record client latency");
    }
}

#[derive(Clone, Copy)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

#[must_use]
pub fn configured_initial_keys(default: usize) -> usize {
    std::env::var("MIDGE_YCSB_INITIAL_KEYS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[must_use]
pub fn configured_value_size() -> usize {
    std::env::var("MIDGE_YCSB_VALUE_SIZE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_VALUE_SIZE)
}

#[must_use]
pub fn logical_entry_size_bytes() -> usize {
    KEY_SIZE + configured_value_size()
}

#[must_use]
pub fn logical_dataset_bytes(initial_keys: usize) -> u64 {
    initial_keys as u64 * logical_entry_size_bytes() as u64
}

#[must_use]
pub fn make_key(id: u64) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[..8].copy_from_slice(&id.to_be_bytes());
    k
}

#[must_use]
pub fn make_value(fill: u8) -> Vec<u8> {
    vec![fill; configured_value_size()]
}

#[must_use]
/// # Panics
/// Panics if the engine cannot be opened with the derived Tier-4 options.
pub fn open_tier4_engine(mut opts: MidgeOptions) -> Engine {
    // Tier-4 workloads should exercise the full system shape.
    opts.enable_compaction = true;
    // Avoid tiny testkit memtables causing constant flush.
    opts.memtable_size = std::env::var("MIDGE_BENCH_MEMTABLE_SIZE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(TIER4_MEMTABLE_SIZE_BYTES);
    opts.memory_budget = std::env::var("MIDGE_BENCH_MEMORY_BUDGET_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0);

    Engine::open_with_options(&opts).expect("open tier4 engine")
}

/// Capture a benchmark runtime performance counter snapshot.
///
/// # Panics
///
/// Panics if runtime metrics cannot be captured.
pub fn capture_runtime_perf_snapshot(engine: &Engine) -> RuntimePerfSnapshot {
    let metrics = engine
        .get_runtime_metrics()
        .expect("capture runtime performance snapshot");
    RuntimePerfSnapshot {
        write_stalls_total: metrics.write_stalls_total,
        write_stalls_memory_total: metrics.write_stalls_memory_total,
        write_stalls_compaction_total: metrics.write_stalls_compaction_total,
        write_stalls_cloud_total: metrics.write_stalls_cloud_total,
        write_stalls_no_space_total: metrics.write_stalls_no_space_total,
        wal_append_count: metrics.wal_append_count,
        wal_fsync_count: metrics.wal_fsync_count,
        wal_append_ns_total: metrics.wal_append_ns_total,
        wal_fsync_ns_total: metrics.wal_fsync_ns_total,
        cache_hits: metrics.cache_hits,
        cache_misses: metrics.cache_misses,
        cloud_async_wal_segments_sealed: metrics.cloud_async_wal_segments_sealed,
        cloud_async_wal_uploads_started: metrics.cloud_async_wal_uploads_started,
        cloud_async_wal_uploads_completed: metrics.cloud_async_wal_uploads_completed,
        cloud_async_wal_uploads_failed: metrics.cloud_async_wal_uploads_failed,
        cloud_async_wal_seal_latency_us: metrics.cloud_async_wal_seal_latency_us,
        cloud_async_wal_upload_latency_us: metrics.cloud_async_wal_upload_latency_us,
        cloud_async_wal_ack_latency_us: metrics.cloud_async_wal_ack_latency_us,
    }
}

/// Build a benchmark runtime performance report from a prior snapshot.
///
/// # Panics
///
/// Panics if runtime metrics cannot be captured.
pub fn runtime_perf_report(engine: &Engine, start: RuntimePerfSnapshot) -> RuntimePerfReport {
    let end = engine
        .get_runtime_metrics()
        .expect("capture runtime performance report");
    RuntimePerfReport {
        end_pending_cloud_uploads: end.pending_cloud_uploads,
        end_wal_local_durable_seq: end.wal_local_durable_seq,
        end_wal_cloud_durable_seq: end.wal_cloud_durable_seq,
        end_hybrid_max_local_bytes: end.hybrid_max_local_bytes,
        end_hybrid_total_committed_bytes: end.hybrid_total_committed_bytes,
        end_hybrid_free_bytes: end.hybrid_free_bytes,
        end_hybrid_usage_percent: end.hybrid_usage_percent,
        end_hybrid_pending_evictions: end.hybrid_pending_evictions,
        write_stalls_total: end
            .write_stalls_total
            .saturating_sub(start.write_stalls_total),
        write_stalls_memory_total: end
            .write_stalls_memory_total
            .saturating_sub(start.write_stalls_memory_total),
        write_stalls_compaction_total: end
            .write_stalls_compaction_total
            .saturating_sub(start.write_stalls_compaction_total),
        write_stalls_cloud_total: end
            .write_stalls_cloud_total
            .saturating_sub(start.write_stalls_cloud_total),
        write_stalls_no_space_total: end
            .write_stalls_no_space_total
            .saturating_sub(start.write_stalls_no_space_total),
        wal_append_count: end.wal_append_count.saturating_sub(start.wal_append_count),
        wal_fsync_count: end.wal_fsync_count.saturating_sub(start.wal_fsync_count),
        wal_append_ns_total: end
            .wal_append_ns_total
            .saturating_sub(start.wal_append_ns_total),
        wal_fsync_ns_total: end
            .wal_fsync_ns_total
            .saturating_sub(start.wal_fsync_ns_total),
        cache_hits: end.cache_hits.saturating_sub(start.cache_hits),
        cache_misses: end.cache_misses.saturating_sub(start.cache_misses),
        cloud_async_wal_segments_sealed: end
            .cloud_async_wal_segments_sealed
            .saturating_sub(start.cloud_async_wal_segments_sealed),
        cloud_async_wal_uploads_started: end
            .cloud_async_wal_uploads_started
            .saturating_sub(start.cloud_async_wal_uploads_started),
        cloud_async_wal_uploads_completed: end
            .cloud_async_wal_uploads_completed
            .saturating_sub(start.cloud_async_wal_uploads_completed),
        cloud_async_wal_uploads_failed: end
            .cloud_async_wal_uploads_failed
            .saturating_sub(start.cloud_async_wal_uploads_failed),
        cloud_async_wal_seal_latency_us: end
            .cloud_async_wal_seal_latency_us
            .saturating_sub(start.cloud_async_wal_seal_latency_us),
        cloud_async_wal_upload_latency_us: end
            .cloud_async_wal_upload_latency_us
            .saturating_sub(start.cloud_async_wal_upload_latency_us),
        cloud_async_wal_ack_latency_us: end
            .cloud_async_wal_ack_latency_us
            .saturating_sub(start.cloud_async_wal_ack_latency_us),
    }
}

/// # Panics
/// Panics if transaction creation, writes, commits, or the final flush fail
/// during deterministic dataset loading.
pub fn load_initial_dataset(engine: &Engine, cf: &ColumnFamilyHandle, initial_keys: usize) {
    // Load is not measured; optimize aggressively to keep Tier-4 runs practical.
    // Use WriteOptions::best_effort() for fastest loading of initial dataset:
    //
    // SAFETY: No durability is needed during load because:
    // - Load phase is not measured (setup phase only)
    // - On engine crash, re-running load_initial_dataset reloads the data
    // - Measured workload uses buffered() for proper durability
    // - flush_cf() at end ensures data reaches storage before warm-up begins
    //
    // This skip of WAL commits speeds up 100k key loads by 3-5x compared to buffered().
    //
    // Use larger batches to amortize commit overhead during load.

    // Optional trace: set MIDGE_TRACE_YCSB=1 to print progress during load.
    let trace = std::env::var_os("MIDGE_TRACE_YCSB").is_some();

    // Hard-coded sensible defaults (no env lookups):
    // - Batch size: increased from 20k to 50k to amortize commit overhead.
    // - Threads: use available CPU cores, but never exceed the number of keys.
    let batch_ops: usize = DEFAULT_BATCH_OPS;

    let threads: usize = std::cmp::min(initial_keys.max(1), num_cpus::get().max(1));

    let cf_id = cf.id();
    if trace {
        eprintln!(
            "[midge][ycsb] starting load: initial_keys={initial_keys} batch_ops={batch_ops} threads={threads}"
        );
    }

    if threads <= 1 {
        // Single-threaded (original behavior) but with configurable batch size.
        let mut tx = engine
            .begin_tx(cf_id, api::TransactionMode::ReadWrite)
            .expect("begin_tx failed");
        let mut count = 0;

        for i in 0..usize_to_u64(initial_keys) {
            let k = make_key(i);
            let v = make_value(fill_byte(i));

            tx.put(k.to_vec(), v, None).expect("put failed");
            count += 1;

            if count >= batch_ops {
                tx.commit(api::WriteOptions::best_effort())
                    .expect("commit failed");
                if trace {
                    eprintln!("[midge][ycsb] loaded {} keys", i + 1);
                }
                tx = engine
                    .begin_tx(cf_id, api::TransactionMode::ReadWrite)
                    .expect("begin_tx failed");
                count = 0;
            }
        }

        if count > 0 {
            tx.commit(api::WriteOptions::best_effort())
                .expect("commit failed");
            if trace {
                eprintln!("[midge][ycsb] loaded {initial_keys} keys (final)");
            }
        }
    } else {
        // Parallel load: split keyspace into contiguous ranges per worker and
        // let each thread drive its own transactions. Use a scoped thread
        // pool so we can borrow &Engine safely.
        let per_worker = initial_keys.div_ceil(threads); // ceil div

        thread::scope(|s| {
            for worker in 0..threads {
                let start = worker * per_worker;
                let end = ((worker + 1) * per_worker).min(initial_keys);

                if start >= end {
                    continue;
                }

                s.spawn(move || {
                    let mut tx = engine
                        .begin_tx(cf_id, api::TransactionMode::ReadWrite)
                        .expect("begin_tx failed");
                    let mut count = 0usize;
                    for i in start..end {
                        let i = usize_to_u64(i);
                        let k = make_key(i);
                        let v = make_value(fill_byte(i));
                        tx.put(k.to_vec(), v, None).expect("put failed");
                        count += 1;
                        if count >= batch_ops {
                            tx.commit(api::WriteOptions::best_effort())
                                .expect("commit failed");
                            if trace {
                                eprintln!(
                                    "[midge][ycsb] worker={} loaded {}..{}",
                                    worker,
                                    start,
                                    i + 1
                                );
                            }
                            tx = engine
                                .begin_tx(cf_id, api::TransactionMode::ReadWrite)
                                .expect("begin_tx failed");
                            count = 0;
                        }
                    }
                    if count > 0 {
                        tx.commit(api::WriteOptions::best_effort())
                            .expect("commit failed");
                        if trace {
                            eprintln!(
                                "[midge][ycsb] worker={worker} loaded {start}..{end} (final)"
                            );
                        }
                    }
                });
            }
        });
    }

    engine.flush_cf(cf).expect("load phase flush");
    if trace {
        eprintln!("[midge][ycsb] load complete");
    }
}

/// Run a duration-based loop, returning `(operations, bytes)`.
///
/// The `step` closure executes one logical workload operation and returns the
/// number of bytes logically touched by that operation.
pub fn run_for_duration<F>(duration: Duration, mut step: F) -> (u64, u64)
where
    F: FnMut(u64) -> u64,
{
    let deadline = Instant::now() + duration;

    let mut ops: u64 = 0;
    let mut bytes: u64 = 0;

    loop {
        // Reduce the cost of time checks in tight loops.
        if ops.trailing_zeros() >= 8 && Instant::now() >= deadline {
            break;
        }

        bytes = bytes.wrapping_add(step(ops));
        ops = ops.wrapping_add(1);
    }

    (ops, bytes)
}

fn splitmix64(mut x: u64) -> u64 {
    // Deterministic, fast mixing function.
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministically derive a pseudo-random u64 from `(seed, client_id, op_index, draw_index)`.
///
/// Use this to feed Zipfian generators without introducing true randomness.
#[must_use]
pub fn deterministic_u64(seed: u64, client_id: usize, op_index: u64, draw_index: u64) -> u64 {
    let base = seed
        ^ (usize_to_u64(client_id).wrapping_mul(0xD6E8_FEB8_6659_FD93))
        ^ op_index
        ^ draw_index.rotate_left(17);
    splitmix64(base)
}

/// Retry an operation on `MidgeError::WriteStall` by waiting for the engine to
/// signal that backpressure has cleared.
///
/// This is designed for Tier-4 stress workloads:
/// - No sleeps
/// - No panics on expected backpressure
/// - Cancellation-aware via the shared `stop` flag
///
/// # Errors
/// Returns any non-`WriteStall` engine error from `op`, or any error returned
/// while waiting for backpressure to clear.
pub fn retry_write_stall<F>(
    engine: &MidgeEngine,
    cf_id: crate::engine::ColumnFamilyId,
    stop: &AtomicBool,
    mut op: F,
) -> MidgeResult<()>
where
    F: FnMut() -> MidgeResult<()>,
{
    loop {
        if stop.load(Ordering::Acquire) {
            // Stress harness is ending; don't block shutdown.
            return Ok(());
        }

        match op() {
            Ok(()) => return Ok(()),
            Err(MidgeError::WriteStall(_)) => {
                // Block waiting for stall to clear, but use a timeout so we can
                // observe stop and avoid hanging on pathological stalls.
                while !stop.load(Ordering::Acquire) {
                    let cleared =
                        engine.wait_for_write_stall_clear(cf_id, Duration::from_millis(50))?;
                    if cleared {
                        break;
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }
}

/// Run `clients` independent client loops concurrently for `duration` and return total ops.
///
/// Core contract:
/// - One shared engine instance (passed by `Arc`)
/// - Each client runs a tight loop with no sleeps/pacing
/// - The only shared state between clients is the engine and the stop flag
///
/// # Panics
/// Panics if the expected benchmark column family does not exist, the optional
/// watchdog detects a stall, or a client thread panics before reporting its
/// completed operation count.
pub fn run_multi_client_for_duration<MakeClient, Step>(
    engine: &Arc<MidgeEngine>,
    clients: usize,
    duration: Duration,
    make_client: MakeClient,
) -> u64
where
    MakeClient: Fn(usize, Arc<AtomicBool>) -> Step,
    Step: FnMut(&MidgeEngine, &ColumnFamilyHandle, u64) + Send + 'static,
{
    run_multi_client_for_duration_with_stats(engine, clients, duration, make_client).operations
}

/// Run concurrent client loops and return both throughput and tail-latency signal.
///
/// The latency histogram records one sample per completed logical operation.
///
/// # Panics
/// Panics under the same conditions as [`run_multi_client_for_duration`].
pub fn run_multi_client_for_duration_with_stats<MakeClient, Step>(
    engine: &Arc<MidgeEngine>,
    clients: usize,
    duration: Duration,
    make_client: MakeClient,
) -> MultiClientRunStats
where
    MakeClient: Fn(usize, Arc<AtomicBool>) -> Step,
    Step: FnMut(&MidgeEngine, &ColumnFamilyHandle, u64) + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut handles = Vec::with_capacity(clients);

    // Optional watchdog to detect stalls. Enable by setting MIDGE_YCSB_WATCHDOG=1.
    let watchdog_enabled = std::env::var_os("MIDGE_YCSB_WATCHDOG").is_some();
    let watchdog_secs: u64 = std::env::var("MIDGE_YCSB_WATCHDOG_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    // Shared last-op timestamp (milliseconds since UNIX_EPOCH).
    let last_op_ts = Arc::new(AtomicU64::new(millis_since_epoch()));

    for client_id in 0..clients {
        let engine = Arc::clone(engine);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let mut client_step = make_client(client_id, Arc::clone(&stop));
        let last_op_ts = Arc::clone(&last_op_ts);

        // Get the CF (it was created in load phase)
        // Note: The actual CF name ("cf1", "data", etc.) varies by benchmark,
        // so we try "cf1" first (YCSB convention), then fall back to "data"
        let cf = engine
            .get_column_family("cf1")
            .or_else(|| engine.get_column_family("data"))
            .expect("CF should exist (tried 'cf1' and 'data')");

        handles.push(thread::spawn(move || {
            // Start all clients together to reduce launch skew.
            barrier.wait();

            // Stagger thread starts to enable write grouping: threads with
            // overlapping commits will be batched together automatically.
            // Without this, all threads hit their first commit simultaneously,
            // preventing the write group coordinator from batching.
            if client_id > 0 {
                std::thread::sleep(Duration::from_micros(usize_to_u64(client_id) * 50));
            }

            let mut stats = ClientRunStats::empty();
            let mut op_index: u64 = 0;
            // Optional slow-op threshold (enable with MIDGE_YCSB_SLOW_OP_MS)
            let slow_op_ms = std::env::var("MIDGE_YCSB_SLOW_OP_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok());

            while !stop.load(Ordering::Acquire) {
                let start = Instant::now();
                client_step(engine.as_ref(), &cf, op_index);
                let elapsed = start.elapsed();

                // Update heartbeat timestamp after each logical operation.
                let now_ms = millis_since_epoch();
                last_op_ts.store(now_ms, Ordering::Release);

                if let Some(threshold) = slow_op_ms {
                    let el_ms = u128_to_u64(elapsed.as_millis());
                    if el_ms >= threshold {
                        eprintln!(
                            "[midge][ycsb][slow_op] client={client_id} op_index={op_index} elapsed_ms={el_ms} threshold_ms={threshold}"
                        );
                    }
                }

                stats.record_latency(elapsed);
                stats.operations = stats.operations.wrapping_add(1);
                op_index = op_index.wrapping_add(1);
            }
            stats
        }));
    }

    // Release all clients at the same time, then start the measurement window.
    barrier.wait();

    // Start watchdog thread if enabled. It will panic with diagnostics when a stall is detected.
    let watchdog_handle = if watchdog_enabled {
        let stop = Arc::clone(&stop);
        let last_op_ts = Arc::clone(&last_op_ts);
        Some(thread::spawn(move || {
            let poll_interval = std::time::Duration::from_secs(1);
            loop {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let now_ms = millis_since_epoch();
                let last_ms = last_op_ts.load(Ordering::Acquire);
                let elapsed = now_ms.saturating_sub(last_ms);
                if elapsed >= watchdog_secs.saturating_mul(1000) {
                    eprintln!(
                        "[midge][ycsb][watchdog] No progress for {elapsed} ms (threshold {watchdog_secs}s). Panicking to capture diagnostics."
                    );
                    // Suggest the user run with RUST_BACKTRACE=1 for stack traces.
                    panic!("YCSB watchdog detected stall: elapsed={elapsed} ms");
                }
                thread::sleep(poll_interval);
            }
        }))
    } else {
        None
    };

    thread::sleep(duration);
    stop.store(true, Ordering::Release);

    if let Some(h) = watchdog_handle {
        let _ = h.join();
    }

    let mut total_ops: u64 = 0;
    let mut latency_us = Histogram::<u64>::new(3).expect("create aggregate latency histogram");
    for h in handles {
        let result = h.join().unwrap_or_else(|_| ClientRunStats::empty());
        total_ops = total_ops.wrapping_add(result.operations);
        latency_us
            .add(&result.latency_us)
            .expect("merge compatible latency histograms");
    }

    if total_ops == 0 {
        return MultiClientRunStats::default();
    }

    MultiClientRunStats {
        operations: total_ops,
        latency_p50_us: latency_us.value_at_percentile(50.0),
        latency_p95_us: latency_us.value_at_percentile(95.0),
        latency_p99_us: latency_us.value_at_percentile(99.0),
        latency_max_us: latency_us.max(),
    }
}

const DEFAULT_BATCH_OPS: usize = 50_000;

fn fill_byte(value: u64) -> u8 {
    u8::try_from(value % 251).unwrap_or(0)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn duration_to_micros(value: Duration) -> u64 {
    u64::try_from(value.as_micros().max(1)).unwrap_or(u64::MAX)
}

fn average_u64(total: u64, count: u64) -> u64 {
    total.checked_div(count).unwrap_or(0)
}

fn ratio_ppm(hits: u64, misses: u64) -> u64 {
    let total = hits.saturating_add(misses);
    hits.saturating_mul(1_000_000)
        .checked_div(total)
        .unwrap_or(0)
}

fn u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn millis_since_epoch() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| u128_to_u64(duration.as_millis()))
}
