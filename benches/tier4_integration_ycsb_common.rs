//! Common utilities for YCSB-style benchmarks
//!
//! - Deterministic key/value generation
//! - No heap allocs or RNG in hot loops
//! - Correct Zipfian distribution (YCSB-style)
//! - Precomputed keys/values via OnceLock
//! - Batched load helpers
//! - FS + Cloud engine variants with configurable latency

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, WriteBatch};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::sync::OnceLock;
use tempfile::TempDir;

// ============================================================================
// Workload Constants
// ============================================================================

#[allow(dead_code)]
pub const OPS_PER_ITER: usize = 1_000; // Fewer iterations; prefer larger dataset over many iterations
#[allow(dead_code)]
// Increase the record count so total dataset size >> block cache (default block cache ~128MB)
// RECORD_COUNT * VALUE_SIZE should be significantly larger than cache to force I/O pressure.
pub const RECORD_COUNT: usize = 200_000; // ~200MB dataset at VALUE_SIZE=1_000
pub const VALUE_SIZE: usize = 1_000;
pub const BATCH_SIZE: usize = 1024; // Larger batches for efficient bulk load

// Duration for steady-state YCSB workloads (seconds). Used as canonical default.
#[allow(dead_code)]
pub const WORKLOAD_DURATION_SECS: u64 = 60;

// Load statistics reported separately from RUN phase. We expose a simple struct and a
// measurement helper so benches can report LOAD throughput/latency independently.
#[allow(dead_code)]
pub struct LoadStats {
    pub records: usize,
    pub duration_secs: f64,
    pub throughput_rps: f64,
    pub mean_latency_us: f64,
}

#[allow(dead_code)]
pub fn load_full_dataset_with_stats(engine: &MidgeEngine) -> LoadStats {
    let start = std::time::Instant::now();
    let keys = PREGEN_KEYS.get().expect("call init_ycsb_globals()");
    let vals = PREGEN_VALUES.get().expect("call init_ycsb_globals()");

    load_data_batched(engine, keys, vals, BATCH_SIZE);

    let duration = start.elapsed();
    let duration_secs = duration.as_secs_f64();
    let records = keys.len();
    let throughput = records as f64 / duration_secs;
    let mean_latency_us = (duration_secs * 1_000_000.0) / (records as f64);

    LoadStats {
        records,
        duration_secs,
        throughput_rps: throughput,
        mean_latency_us,
    }
}

#[allow(dead_code)]
pub fn load_full_dataset(engine: &MidgeEngine) {
    // Deprecated for measurement-aware benches; keep wrapper for compatibility and
    // print a brief LOAD summary for visibility.
    let stats = load_full_dataset_with_stats(engine);
    eprintln!(
        "LOAD STATS: records={} duration_s={:.3} throughput_rec_s={:.0} mean_latency_us={:.3}",
        stats.records, stats.duration_secs, stats.throughput_rps, stats.mean_latency_us
    );
}

#[allow(dead_code)]
pub const THREAD_COUNTS: [usize; 3] = [1, 2, 8];

// ============================================================================
// Key / Value Generation
// ============================================================================

pub fn generate_key(id: usize) -> Bytes {
    // YCSB-style keys: "user000000000123"
    Bytes::from(format!("user{:012}", id))
}

pub fn pregen_values(count: usize, seed: u64) -> Vec<Bytes> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            let mut buf = vec![0u8; VALUE_SIZE];
            rng.fill_bytes(&mut buf);
            Bytes::from(buf)
        })
        .collect()
}

// ---------- PRECOMPUTED KEYS AND VALUES (OnceLock) -------------------------

#[allow(dead_code)]
pub static PREGEN_KEYS: OnceLock<Vec<Bytes>> = OnceLock::new();
#[allow(dead_code)]
pub static PREGEN_VALUES: OnceLock<Vec<Bytes>> = OnceLock::new();

#[allow(dead_code)]
pub fn init_pregen() {
    PREGEN_KEYS.get_or_init(|| (0..RECORD_COUNT).map(generate_key).collect());
    PREGEN_VALUES.get_or_init(|| pregen_values(RECORD_COUNT, 0xD1CE_F00D_CAFE_F00D));
}

// ============================================================================
// Zipfian Distribution
// ============================================================================

#[allow(dead_code)]
pub struct ZipfianGenerator {
    items: usize,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    eta: f64,
}

impl ZipfianGenerator {
    pub fn new(items: usize, theta: f64) -> Self {
        let zeta_n = Self::zeta(items, theta);
        let zeta_2 = Self::zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / items as f64).powf(1.0 - theta)) / (1.0 - (zeta_2 / zeta_n));

        Self {
            items,
            theta,
            zeta_n,
            alpha,
            eta,
        }
    }

    fn zeta(n: usize, theta: f64) -> f64 {
        (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
    }

    #[allow(dead_code)]
    pub fn next(&self, rng: &mut StdRng) -> usize {
        // Deterministic [0,1) from u64, no FP RNG in hot loop
        let u: f64 = {
            let r = rng.next_u64();
            (r as f64) / 18446744073709551616.0 // 2^64
        };

        let uz = u * self.zeta_n;

        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5_f64.powf(self.theta) {
            return 1;
        }

        let v = self.eta * u - (self.eta - 1.0);
        let idx = (self.items as f64 * v.powf(self.alpha)) as usize;

        idx % self.items
    }
}

// ---- Global Zipf (OnceLock) ----------------------------------------------

#[allow(dead_code)]
pub static ZIPF_DEFAULT: OnceLock<ZipfianGenerator> = OnceLock::new();

#[allow(dead_code)]
pub fn init_zipf() {
    ZIPF_DEFAULT.get_or_init(|| ZipfianGenerator::new(RECORD_COUNT, 0.99));
}

/// Convenience for YCSB benches: call once at top of `bench_*`.
#[allow(dead_code)]
pub fn init_ycsb_globals() {
    init_pregen();
    init_zipf();
}

#[allow(dead_code)]
pub fn make_thread_rng(thread_id: usize, workload_seed: u64) -> StdRng {
    StdRng::seed_from_u64(workload_seed ^ ((thread_id as u64) << 32))
}

// ============================================================================
// Batched Load
// ============================================================================

// Load wrapper left intentionally minimal here; public API (load_full_dataset)
// is already implemented higher in the file and prints a brief summary for
// compatibility with existing benches.
/// Generic batched loader: no RNG, no allocations in hot loop.
pub fn load_data_batched(
    engine: &MidgeEngine,
    keys: &[Bytes],
    values: &[Bytes],
    batch_size: usize,
) {
    let cf = engine.default_column_family();
    let cf_id = cf.id();

    let mut batch = WriteBatch::new();
    for i in 0..keys.len() {
        batch.put_cf(cf_id, keys[i].clone(), values[i].clone());
        if batch.len() >= batch_size {
            engine.write_batch(&batch).unwrap();
            batch.clear();
        }
    }

    if !batch.is_empty() {
        engine.write_batch(&batch).unwrap();
    }

    let _ = engine.flush();
}

// ============================================================================
// Scan Range Helpers (Workload E)
// ============================================================================

#[allow(dead_code)]
pub fn pregen_scan_ranges(count: usize, scan_len: usize) -> Vec<(Bytes, Bytes)> {
    let keys = PREGEN_KEYS.get().expect("call init_ycsb_globals()");
    (0..count)
        .map(|i| {
            let start = keys[i].clone();
            let end_idx = (i + scan_len).min(RECORD_COUNT - 1);
            let end = keys[end_idx].clone();
            (start, end)
        })
        .collect()
}

// ============================================================================
// Engine Setup Variants (FS)
// ============================================================================

#[allow(dead_code)]
pub fn setup_engine_fs_nosync() -> (MidgeEngine, TempDir) {
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };
    (MidgeEngine::open(opts).unwrap(), dir)
}

#[allow(dead_code)]
pub fn setup_engine_fs_sync() -> (MidgeEngine, TempDir) {
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: true,
        ..Default::default()
    };
    (MidgeEngine::open(opts).unwrap(), dir)
}

// ============================================================================
// Engine Setup Variants (Cloud-backed)
// ============================================================================

/// Setup cloud-backed engine with realistic latency simulation.
#[allow(dead_code)]
pub fn setup_engine_cloud_nosync() -> (MidgeEngine, TempDir) {
    let dir = TempDir::new().unwrap();

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };

    (MidgeEngine::open(opts).unwrap(), dir)
}

#[allow(dead_code)]
pub fn setup_engine_cloud_sync() -> (MidgeEngine, TempDir) {
    let dir = TempDir::new().unwrap();

    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: true,
        ..Default::default()
    };

    (MidgeEngine::open(opts).unwrap(), dir)
}
