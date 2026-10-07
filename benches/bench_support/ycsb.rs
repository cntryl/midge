//! Helpers for Tier-4 (YCSB) stress workloads.
//!
//! These helpers are intentionally deterministic and "boring": Tier-4 aims to
//! measure steady-state behavior over time (load → warm-up → measured).

#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::SystemTime;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use cntryl_midge::{
    ColumnFamilyHandle, Engine, MidgeError, MidgeResult, TransactionMode, WriteOptions,
};
use cntryl_stress::{ObservationDirection, ObservationUnit, StressContext};

use super::config::{MidgeOptions, StorageMode};

pub const KEY_SIZE: usize = 16;
pub const DEFAULT_VALUE_SIZE: usize = 128;

#[path = "ycsb/inventory.rs"]
pub mod inventory;

pub const TIER4_MEMTABLE_SIZE_BYTES: usize = 4 * 1024 * 1024;
pub const TIER4_MEMORY_MEMTABLE_SIZE_BYTES: usize = 512 * 1024 * 1024;

const EXPORTED_LATENCY_QUANTILES: usize = 256;
const RUNTIME_METRICS_RETRY_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, Default)]
pub struct MultiClientRunStats {
    pub operations: u64,
    pub latency_p50_us: u64,
    pub latency_p95_us: u64,
    pub latency_p99_us: u64,
    pub latency_max_us: u64,
    latency_quantiles_us: Vec<u64>,
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
    pub read_only_begin_tx_count: u64,
    pub read_only_snapshot_cache_hits: u64,
    pub read_only_snapshot_cache_misses: u64,
    pub snapshot_register_count: u64,
    pub snapshot_unregister_count: u64,
    pub sst_reader_cache_hits: u64,
    pub sst_reader_cache_misses: u64,
    pub sst_block_cache_hits: u64,
    pub sst_block_cache_misses: u64,
    pub candidate_sst_files_checked: u64,
    pub candidate_blocks_checked: u64,
    pub data_blocks_read: u64,
    pub bloom_rejects: u64,
    pub range_tombstone_scans: u64,
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
    pub read_only_begin_tx_count: u64,
    pub read_only_snapshot_cache_hits: u64,
    pub read_only_snapshot_cache_misses: u64,
    pub snapshot_register_count: u64,
    pub snapshot_unregister_count: u64,
    pub sst_reader_cache_hits: u64,
    pub sst_reader_cache_misses: u64,
    pub sst_block_cache_hits: u64,
    pub sst_block_cache_misses: u64,
    pub candidate_sst_files_checked: u64,
    pub candidate_blocks_checked: u64,
    pub data_blocks_read: u64,
    pub bloom_rejects: u64,
    pub range_tombstone_scans: u64,
}

impl RuntimePerfReport {
    #[must_use]
    pub fn diagnostic_observations(&self) -> Vec<(&'static str, f64, ObservationUnit)> {
        self.tags()
            .into_iter()
            .filter_map(|(name, value)| match name {
                "write_stalls"
                | "wal_append_count"
                | "cache_hits"
                | "cache_misses"
                | "candidate_sst_files_checked"
                | "data_blocks_read"
                | "cloud_async_wal_uploads_completed"
                | "cloud_async_wal_uploads_failed" => {
                    Some((name, observation_f64(value), ObservationUnit::Count))
                }
                "cache_hit_ratio_ppm" => {
                    let hits = observation_f64(self.cache_hits);
                    let misses = observation_f64(self.cache_misses);
                    let ratio = if hits + misses == 0.0 {
                        0.0
                    } else {
                        hits / (hits + misses)
                    };
                    Some(("cache_hit_ratio", ratio, ObservationUnit::Ratio))
                }
                _ => None,
            })
            .collect()
    }

    #[must_use]
    pub fn tags(&self) -> Vec<(&'static str, u64)> {
        let mut tags = Vec::with_capacity(64);
        self.push_write_wal_cache_tags(&mut tags);
        self.push_cloud_wal_tags(&mut tags);
        self.push_read_snapshot_tags(&mut tags);
        self.push_sst_read_tags(&mut tags);
        self.push_storage_state_tags(&mut tags);
        tags
    }

    fn push_write_wal_cache_tags(&self, tags: &mut Vec<(&'static str, u64)>) {
        tags.extend([
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
                average_u64(
                    self.wal_append_ns_total,
                    self.wal_append_count.saturating_mul(1_000),
                ),
            ),
            (
                "avg_wal_sync_us",
                average_u64(
                    self.wal_fsync_ns_total,
                    self.wal_fsync_count.saturating_mul(1_000),
                ),
            ),
            ("cache_hits", self.cache_hits),
            ("cache_misses", self.cache_misses),
            (
                "cache_hit_ratio_ppm",
                ratio_ppm(self.cache_hits, self.cache_misses),
            ),
        ]);
    }

    fn push_cloud_wal_tags(&self, tags: &mut Vec<(&'static str, u64)>) {
        tags.extend([
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
        ]);
    }

    fn push_read_snapshot_tags(&self, tags: &mut Vec<(&'static str, u64)>) {
        tags.extend([
            ("read_only_begin_tx_count", self.read_only_begin_tx_count),
            (
                "read_only_snapshot_cache_hits",
                self.read_only_snapshot_cache_hits,
            ),
            (
                "read_only_snapshot_cache_misses",
                self.read_only_snapshot_cache_misses,
            ),
            (
                "read_only_snapshot_cache_hit_ratio_ppm",
                ratio_ppm(
                    self.read_only_snapshot_cache_hits,
                    self.read_only_snapshot_cache_misses,
                ),
            ),
            ("snapshot_register_count", self.snapshot_register_count),
            ("snapshot_unregister_count", self.snapshot_unregister_count),
        ]);
    }

    fn push_sst_read_tags(&self, tags: &mut Vec<(&'static str, u64)>) {
        tags.extend([
            ("sst_reader_cache_hits", self.sst_reader_cache_hits),
            ("sst_reader_cache_misses", self.sst_reader_cache_misses),
            (
                "sst_reader_cache_hit_ratio_ppm",
                ratio_ppm(self.sst_reader_cache_hits, self.sst_reader_cache_misses),
            ),
            ("sst_block_cache_hits", self.sst_block_cache_hits),
            ("sst_block_cache_misses", self.sst_block_cache_misses),
            (
                "sst_block_cache_hit_ratio_ppm",
                ratio_ppm(self.sst_block_cache_hits, self.sst_block_cache_misses),
            ),
            (
                "candidate_sst_files_checked",
                self.candidate_sst_files_checked,
            ),
            ("candidate_blocks_checked", self.candidate_blocks_checked),
            (
                "avg_candidate_ssts_per_read",
                average_u64(
                    self.candidate_sst_files_checked,
                    self.read_only_begin_tx_count,
                ),
            ),
            (
                "avg_candidate_blocks_per_read",
                average_u64(self.candidate_blocks_checked, self.read_only_begin_tx_count),
            ),
            ("data_blocks_read", self.data_blocks_read),
            ("bloom_rejects", self.bloom_rejects),
            ("range_tombstone_scans", self.range_tombstone_scans),
        ]);
    }

    fn push_storage_state_tags(&self, tags: &mut Vec<(&'static str, u64)>) {
        tags.extend([
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
        ]);
    }
}

fn observation_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).expect("upper counter bits fit in u32");
    let low = u32::try_from(value & u64::from(u32::MAX)).expect("lower counter bits fit in u32");
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

impl MultiClientRunStats {
    /// Attach a bounded, deterministic approximation of the aggregate
    /// operation-latency distribution to the latest stress measurement.
    pub fn record_latencies(&self, ctx: &mut StressContext) {
        for latency_us in &self.latency_quantiles_us {
            ctx.record_latency(Duration::from_micros(*latency_us));
        }
    }
}

fn latency_quantiles_us(histogram: &Histogram<u64>) -> Vec<u64> {
    if histogram.is_empty() {
        return Vec::new();
    }

    (0..EXPORTED_LATENCY_QUANTILES)
        .map(|index| {
            let numerator = u32::try_from(index).expect("latency quantile index fits in u32");
            let denominator = u32::try_from(EXPORTED_LATENCY_QUANTILES - 1)
                .expect("latency quantile count fits in u32");
            let quantile = f64::from(numerator) / f64::from(denominator);
            histogram.value_at_quantile(quantile)
        })
        .collect()
}

pub fn configure_workload_parameters(
    ctx: &mut StressContext,
    profile: &str,
    clients: usize,
    measured: Duration,
) {
    ctx.parameter("storage_profile", profile);
    ctx.parameter("clients", clients);
    ctx.parameter("measured_secs", measured.as_secs());
    ctx.parameter(
        "latency_observation_source",
        "aggregate_histogram_256_quantiles",
    );
}

/// Attach measured-window runtime diagnostics and cloud-upload failures to the
/// latest YCSB measurement.
pub fn record_runtime_report(ctx: &mut StressContext, report: &RuntimePerfReport) {
    for (name, value, unit) in report.diagnostic_observations() {
        ctx.record_observation(name, value, unit, ObservationDirection::Informational);
    }
    record_runtime_correctness(ctx, report);
}

pub fn record_runtime_correctness(ctx: &mut StressContext, report: &RuntimePerfReport) {
    let _ = ctx
        .correctness()
        .failures(report.cloud_async_wal_uploads_failed);
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

/// Join every client before reporting a failure, so a partial set of
/// operation counts can never be mistaken for a successful benchmark run.
fn collect_client_runs(handles: Vec<thread::JoinHandle<ClientRunStats>>) -> (u64, Histogram<u64>) {
    let mut total_ops = 0_u64;
    let mut latency_us = Histogram::<u64>::new(3).expect("create aggregate latency histogram");
    let mut first_failure = None;
    for handle in handles {
        match handle.join() {
            Ok(result) => {
                total_ops = total_ops.wrapping_add(result.operations);
                latency_us
                    .add(&result.latency_us)
                    .expect("merge compatible latency histograms");
            }
            Err(failure) => {
                if first_failure.is_none() {
                    first_failure = Some(failure);
                }
            }
        }
    }
    if let Some(failure) = first_failure {
        std::panic::resume_unwind(failure);
    }
    (total_ops, latency_us)
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
    let default_memtable_size = if matches!(opts.storage_mode, StorageMode::Memory) {
        TIER4_MEMORY_MEMTABLE_SIZE_BYTES
    } else {
        TIER4_MEMTABLE_SIZE_BYTES
    };
    // Avoid tiny testkit memtables causing constant flush.
    opts.memtable_size = std::env::var("MIDGE_BENCH_MEMTABLE_SIZE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_memtable_size);
    opts.memory_budget = std::env::var("MIDGE_BENCH_MEMORY_BUDGET_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0);

    Engine::open(opts.to_open_options()).expect("open tier4 engine")
}

/// Capture a benchmark runtime performance counter snapshot.
///
/// # Panics
///
/// Panics if runtime metrics cannot be captured.
pub fn capture_runtime_perf_snapshot(engine: &Engine) -> RuntimePerfSnapshot {
    let metrics = retry_transient_runtime_request(|| engine.metrics().get_runtime_metrics())
        .expect("capture runtime performance snapshot");
    let read_path = engine.read_path_diagnostics_snapshot_for_benchmarks();
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
        read_only_begin_tx_count: read_path.read_only_begin_tx_count,
        read_only_snapshot_cache_hits: read_path.read_only_snapshot_cache_hits,
        read_only_snapshot_cache_misses: read_path.read_only_snapshot_cache_misses,
        snapshot_register_count: read_path.snapshot_register_count,
        snapshot_unregister_count: read_path.snapshot_unregister_count,
        sst_reader_cache_hits: read_path.sst_reader_cache_hits,
        sst_reader_cache_misses: read_path.sst_reader_cache_misses,
        sst_block_cache_hits: read_path.sst_block_cache_hits,
        sst_block_cache_misses: read_path.sst_block_cache_misses,
        candidate_sst_files_checked: read_path.candidate_sst_files_checked,
        candidate_blocks_checked: read_path.candidate_blocks_checked,
        data_blocks_read: read_path.data_blocks_read,
        bloom_rejects: read_path.bloom_rejects,
        range_tombstone_scans: read_path.range_tombstone_scans,
    }
}

/// Build a benchmark runtime performance report from a prior snapshot.
///
/// # Panics
///
/// Panics if runtime metrics cannot be captured.
pub fn runtime_perf_report(engine: &Engine, start: RuntimePerfSnapshot) -> RuntimePerfReport {
    let end = retry_transient_runtime_request(|| engine.metrics().get_runtime_metrics())
        .expect("capture runtime performance report");
    let read_path = engine.read_path_diagnostics_snapshot_for_benchmarks();
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
        read_only_begin_tx_count: read_path
            .read_only_begin_tx_count
            .saturating_sub(start.read_only_begin_tx_count),
        read_only_snapshot_cache_hits: read_path
            .read_only_snapshot_cache_hits
            .saturating_sub(start.read_only_snapshot_cache_hits),
        read_only_snapshot_cache_misses: read_path
            .read_only_snapshot_cache_misses
            .saturating_sub(start.read_only_snapshot_cache_misses),
        snapshot_register_count: read_path
            .snapshot_register_count
            .saturating_sub(start.snapshot_register_count),
        snapshot_unregister_count: read_path
            .snapshot_unregister_count
            .saturating_sub(start.snapshot_unregister_count),
        sst_reader_cache_hits: read_path
            .sst_reader_cache_hits
            .saturating_sub(start.sst_reader_cache_hits),
        sst_reader_cache_misses: read_path
            .sst_reader_cache_misses
            .saturating_sub(start.sst_reader_cache_misses),
        sst_block_cache_hits: read_path
            .sst_block_cache_hits
            .saturating_sub(start.sst_block_cache_hits),
        sst_block_cache_misses: read_path
            .sst_block_cache_misses
            .saturating_sub(start.sst_block_cache_misses),
        candidate_sst_files_checked: read_path
            .candidate_sst_files_checked
            .saturating_sub(start.candidate_sst_files_checked),
        candidate_blocks_checked: read_path
            .candidate_blocks_checked
            .saturating_sub(start.candidate_blocks_checked),
        data_blocks_read: read_path
            .data_blocks_read
            .saturating_sub(start.data_blocks_read),
        bloom_rejects: read_path.bloom_rejects.saturating_sub(start.bloom_rejects),
        range_tombstone_scans: read_path
            .range_tombstone_scans
            .saturating_sub(start.range_tombstone_scans),
    }
}

fn retry_transient_runtime_request<T>(
    mut request: impl FnMut() -> MidgeResult<T>,
) -> MidgeResult<T> {
    let deadline = Instant::now() + RUNTIME_METRICS_RETRY_TIMEOUT;
    loop {
        match request() {
            Err(MidgeError::WriteStall(_)) if Instant::now() < deadline => {
                // Metrics share the bounded runtime request queue with normal
                // operations. Let the runtime drain a saturated queue without
                // treating an observation-only request as a workload failure.
                thread::yield_now();
            }
            result => return result,
        }
    }
}

/// # Panics
/// Panics if transaction creation, writes, commits, or the final flush fail
/// during deterministic dataset loading.
pub fn load_initial_dataset(engine: &Engine, cf: &ColumnFamilyHandle, initial_keys: usize) {
    let workers = initial_keys.max(1).min(num_cpus::get().max(1));
    load_initial_dataset_with_workers(engine, cf, initial_keys, workers, configured_value_size())
        .expect("load initial dataset");
}

/// Load the same setup dataset with an explicit worker count and value size.
///
/// The benchmark wrapper supplies its configured values. Tests can exercise
/// the actual parallel loader without changing process-wide environment state.
/// Loading remains outside the measured window, uses best-effort commits and
/// finishes with the same real flush before warm-up.
///
/// # Errors
/// Returns a transaction or flush error after joining every started worker.
///
/// # Panics
/// Propagates an unexpected worker panic after scoped worker cleanup.
pub fn load_initial_dataset_with_workers(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    initial_keys: usize,
    workers: usize,
    value_size: usize,
) -> MidgeResult<()> {
    let workers = initial_keys.max(1).min(workers.max(1));
    let per_worker = initial_keys.div_ceil(workers);
    let local_bytes = retry_transient_runtime_request(|| engine.metrics().get_runtime_metrics())?
        .hybrid_max_local_bytes;
    let batch_ops = preload_batch_ops(local_bytes, value_size);
    let trace = std::env::var_os("MIDGE_TRACE_YCSB").is_some();
    if trace {
        eprintln!(
            "[midge][ycsb] starting load: initial_keys={initial_keys} batch_ops={batch_ops} threads={workers}"
        );
    }
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let start = worker * per_worker;
            let end = ((worker + 1) * per_worker).min(initial_keys);
            if start < end {
                handles.push(scope.spawn(move || {
                    load_initial_range(engine, cf.id(), start..end, value_size, worker, batch_ops)
                }));
            }
        }
        let mut result = Ok(());
        for handle in handles {
            let worker_result = handle.join().expect("dataset load worker panicked");
            if result.is_ok() {
                result = worker_result;
            }
        }
        result
    })?;
    retry_preload_write_stall(
        || engine.flush_cf(cf),
        |timeout| engine.wait_for_write_stall_clear(cf.id(), timeout),
        Duration::from_secs(30),
    )?;
    if trace {
        eprintln!("[midge][ycsb] load complete");
    }
    Ok(())
}

fn preload_batch_ops(local_bytes: u64, value_size: usize) -> usize {
    use cntryl_midge::__internal::memtable::bench::point_flush_staging_bytes;

    if local_bytes == 0 {
        return DEFAULT_BATCH_OPS;
    }
    // Cloud admission reserves the other half for compaction staging.
    let window = local_bytes / 2;
    // Let the actual engine report an indivisible point's resource error.
    if point_flush_staging_bytes(1, KEY_SIZE, value_size) > window {
        return 1;
    }
    let mut low = 1;
    let mut high = DEFAULT_BATCH_OPS;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if point_flush_staging_bytes(middle, KEY_SIZE, value_size) <= window {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    low
}

fn load_initial_range(
    engine: &Engine,
    cf_id: u32,
    keys: std::ops::Range<usize>,
    value_size: usize,
    worker: usize,
    batch_ops: usize,
) -> MidgeResult<()> {
    let trace = std::env::var_os("MIDGE_TRACE_YCSB").is_some();
    let mut start = keys.start;
    while start < keys.end {
        let end = start.saturating_add(batch_ops).min(keys.end);
        retry_preload_write_stall(
            || {
                let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
                for index in start..end {
                    let id = usize_to_u64(index);
                    tx.put(make_key(id).to_vec(), vec![fill_byte(id); value_size], None)?;
                }
                tx.commit(WriteOptions::best_effort())
            },
            |timeout| engine.wait_for_write_stall_clear(cf_id, timeout),
            Duration::from_secs(30),
        )?;
        if trace {
            eprintln!("[midge][ycsb] worker={worker} loaded {start}..{end}");
        }
        start = end;
    }
    Ok(())
}

/// Retry only rejected setup batches, with one original write-stall retry budget.
///
/// # Errors
/// Returns non-`WriteStall` errors unchanged, or `Timeout` when retries exhaust
/// the original budget. A cleared stall still requires a successful commit.
pub fn retry_preload_write_stall<O, W>(
    mut operation: O,
    mut wait_for_clear: W,
    timeout: Duration,
) -> MidgeResult<()>
where
    O: FnMut() -> MidgeResult<()>,
    W: FnMut(Duration) -> MidgeResult<bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        match operation() {
            Ok(()) => return Ok(()),
            Err(MidgeError::WriteStall(_)) => {}
            Err(error) => return Err(error),
        }
        loop {
            let remaining = preload_retry_remaining(deadline)?;
            match wait_for_clear(remaining.min(Duration::from_millis(50))) {
                Ok(true) => break,
                Ok(false) | Err(MidgeError::WriteStall(_)) => {}
                Err(error) => return Err(error),
            }
        }
        preload_retry_remaining(deadline)?;
    }
}

fn preload_retry_remaining(deadline: Instant) -> MidgeResult<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(MidgeError::Timeout(
            "YCSB preload write-stall retry budget exhausted".into(),
        ))
    } else {
        Ok(remaining)
    }
}

/// Flush a completed setup or warm-up phase, waiting through ordinary
/// immutable-queue backpressure.
///
/// # Errors
/// Returns any non-`WriteStall` engine error, or an error encountered while
/// waiting for the column family's write stall to clear.
pub fn flush_after_phase(engine: &Engine, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
    let stop = AtomicBool::new(false);
    retry_write_stall(engine, cf.id(), &stop, || engine.flush_cf(cf))
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
    engine: &Engine,
    cf_id: cntryl_midge::ColumnFamilyId,
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
                    match engine.wait_for_write_stall_clear(cf_id, Duration::from_millis(50)) {
                        Ok(true) => break,
                        Ok(false) | Err(MidgeError::WriteStall(_)) => {}
                        Err(e) => return Err(e),
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }
}

/// Retry write stalls and report whether the logical operation completed.
/// Cancellation before success returns `false`; an actual success remains
/// completed when the stop signal arrives before the callback returns.
///
/// # Errors
/// Returns any non-`WriteStall` engine error from `op`, or any error returned
/// while waiting for backpressure to clear.
pub fn retry_write_stall_observed<F>(
    engine: &Engine,
    cf_id: cntryl_midge::ColumnFamilyId,
    stop: &AtomicBool,
    mut op: F,
) -> MidgeResult<bool>
where
    F: FnMut() -> MidgeResult<()>,
{
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(false);
        }

        match op() {
            Ok(()) => return Ok(true),
            Err(MidgeError::WriteStall(_)) => {
                while !stop.load(Ordering::Acquire) {
                    match engine.wait_for_write_stall_clear(cf_id, Duration::from_millis(50)) {
                        Ok(true) => break,
                        Ok(false) | Err(MidgeError::WriteStall(_)) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
            Err(error) => return Err(error),
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
    engine: &Arc<Engine>,
    clients: usize,
    duration: Duration,
    make_client: MakeClient,
) -> u64
where
    MakeClient: Fn(usize, Arc<AtomicBool>) -> Step,
    Step: FnMut(&Engine, &ColumnFamilyHandle, u64) + Send + 'static,
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
    engine: &Arc<Engine>,
    clients: usize,
    duration: Duration,
    make_client: MakeClient,
) -> MultiClientRunStats
where
    MakeClient: Fn(usize, Arc<AtomicBool>) -> Step,
    Step: FnMut(&Engine, &ColumnFamilyHandle, u64) + Send + 'static,
{
    run_multi_client_for_duration_observed_with_stats(
        engine,
        clients,
        duration,
        |client_id, stop| {
            let mut step = make_client(client_id, stop);
            move |engine, cf, op_index| {
                step(engine, cf, op_index);
                true
            }
        },
    )
}

/// Run concurrent clients and meter callbacks that report actual completion.
/// A `false` result cancels that client without recording a completion or latency.
///
/// # Panics
/// Panics under the same conditions as [`run_multi_client_for_duration`].
pub fn run_multi_client_for_duration_observed_with_stats<MakeClient, Step>(
    engine: &Arc<Engine>,
    clients: usize,
    duration: Duration,
    make_client: MakeClient,
) -> MultiClientRunStats
where
    MakeClient: Fn(usize, Arc<AtomicBool>) -> Step,
    Step: FnMut(&Engine, &ColumnFamilyHandle, u64) -> bool + Send + 'static,
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
        let client_step = make_client(client_id, Arc::clone(&stop));
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

            // Stagger starts so commits overlap the runtime drain window and
            // can share a physical WAL append.
            if client_id > 0 {
                std::thread::sleep(Duration::from_micros(usize_to_u64(client_id) * 50));
            }

            run_observed_client_loop(
                engine.as_ref(),
                &cf,
                stop.as_ref(),
                last_op_ts.as_ref(),
                client_id,
                client_step,
            )
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

    let watchdog_failure = watchdog_handle.and_then(|handle| handle.join().err());
    let (total_ops, latency_us) = collect_client_runs(handles);
    if let Some(failure) = watchdog_failure {
        std::panic::resume_unwind(failure);
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
        latency_quantiles_us: latency_quantiles_us(&latency_us),
    }
}

fn run_observed_client_loop<Step>(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    stop: &AtomicBool,
    last_op_ts: &AtomicU64,
    client_id: usize,
    mut client_step: Step,
) -> ClientRunStats
where
    Step: FnMut(&Engine, &ColumnFamilyHandle, u64) -> bool,
{
    let mut stats = ClientRunStats::empty();
    let mut op_index: u64 = 0;
    // Optional slow-op threshold (enable with MIDGE_YCSB_SLOW_OP_MS)
    let slow_op_ms = std::env::var("MIDGE_YCSB_SLOW_OP_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());

    while !stop.load(Ordering::Acquire) {
        let start = Instant::now();
        let completed = client_step(engine, cf, op_index);
        if !completed {
            break;
        }
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
}

/// Run concurrent client loops for a fixed number of operations per client.
///
/// # Panics
/// Panics if the expected benchmark column family does not exist, or if a
/// client thread panics before reporting its completed operation count.
#[must_use]
pub fn run_multi_client_for_operations_with_stats<MakeClient, Step>(
    engine: &Arc<Engine>,
    clients: usize,
    operations_per_client: u64,
    make_client: MakeClient,
) -> (MultiClientRunStats, Duration)
where
    MakeClient: Fn(usize, Arc<AtomicBool>) -> Step,
    Step: FnMut(&Engine, &ColumnFamilyHandle, u64) + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut handles = Vec::with_capacity(clients);

    for client_id in 0..clients {
        let engine = Arc::clone(engine);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let mut client_step = make_client(client_id, Arc::clone(&stop));

        let cf = engine
            .get_column_family("cf1")
            .or_else(|| engine.get_column_family("data"))
            .expect("CF should exist (tried 'cf1' and 'data')");

        handles.push(thread::spawn(move || {
            barrier.wait();
            if client_id > 0 {
                thread::sleep(Duration::from_micros(usize_to_u64(client_id) * 50));
            }

            let mut stats = ClientRunStats::empty();
            for op_index in 0..operations_per_client {
                let start = Instant::now();
                client_step(engine.as_ref(), &cf, op_index);
                stats.record_latency(start.elapsed());
                stats.operations = stats.operations.wrapping_add(1);
            }
            stats
        }));
    }

    barrier.wait();
    let started_at = Instant::now();

    let (total_ops, latency_us) = collect_client_runs(handles);
    stop.store(true, Ordering::Release);
    let elapsed = started_at.elapsed();

    if total_ops == 0 {
        return (MultiClientRunStats::default(), elapsed);
    }

    (
        MultiClientRunStats {
            operations: total_ops,
            latency_p50_us: latency_us.value_at_percentile(50.0),
            latency_p95_us: latency_us.value_at_percentile(95.0),
            latency_p99_us: latency_us.value_at_percentile(99.0),
            latency_max_us: latency_us.max(),
            latency_quantiles_us: latency_quantiles_us(&latency_us),
        },
        elapsed,
    )
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

#[cfg(test)]
#[path = "ycsb/cancelled_completion_tests.rs"]
mod cancelled_completion_tests;
