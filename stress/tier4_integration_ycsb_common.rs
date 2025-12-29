//! Shared support code for the Tier-4 YCSB benchmarks under `stress/`.
//!
//! These benchmarks are Criterion-based but rely on a small shared harness:
//! - deterministic key/value pools
//! - a Zipf-like skewed key selector
//! - engine setup helpers for different storage scenarios

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, WriteBatch};
use once_cell::sync::OnceCell;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::path::PathBuf;
use tempfile::TempDir;

pub const RECORD_COUNT: usize = 50_000;
pub const OPS_PER_ITER: usize = 50_000;
pub const BATCH_SIZE: usize = 128;
pub const WORKLOAD_DURATION_SECS: u64 = 60;

pub const THREAD_COUNTS: &[usize] = &[1, 4, 8];

pub static PREGEN_KEYS: OnceCell<Vec<Bytes>> = OnceCell::new();
pub static PREGEN_VALUES: OnceCell<Vec<Bytes>> = OnceCell::new();
pub static ZIPF_DEFAULT: OnceCell<ZipfGenerator> = OnceCell::new();

pub fn init_ycsb_globals() {
    PREGEN_KEYS.get_or_init(|| {
        (0..RECORD_COUNT)
            .map(|i| Bytes::from(format!("user{:010}", i)))
            .collect()
    });

    PREGEN_VALUES.get_or_init(|| {
        (0..RECORD_COUNT)
            .map(|i| {
                let mut buf = vec![0u8; 100];
                buf[..8].copy_from_slice(&(i as u64).to_be_bytes());
                Bytes::from(buf)
            })
            .collect()
    });

    ZIPF_DEFAULT.get_or_init(|| ZipfGenerator::new(RECORD_COUNT, 0.99));
}

pub fn make_thread_rng(thread_id: usize, base_seed: u64) -> StdRng {
    let mixed = base_seed ^ ((thread_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    StdRng::seed_from_u64(mixed)
}

pub fn pregen_scan_ranges(range_count: usize, scan_len: usize) -> Vec<(Bytes, Bytes)> {
    init_ycsb_globals();

    let keys = PREGEN_KEYS.get().expect("init_ycsb_globals");
    let mut rng = StdRng::seed_from_u64(0x1234_5678);

    let mut out = Vec::with_capacity(range_count);
    for _ in 0..range_count {
        let start = (rng.next_u64() as usize) % keys.len();
        let end = (start + scan_len).min(keys.len() - 1);
        out.push((keys[start].clone(), keys[end].clone()));
    }
    out
}

fn open_with_temp_opts(mut opts: MidgeOptions, tmp: &TempDir, wal_sync: bool) -> MidgeEngine {
    opts.wal_sync = wal_sync;

    // Force temp paths so benches are isolated.
    opts.storage_mode = match opts.storage_mode {
        StorageMode::LocalDisk { .. } => StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        StorageMode::CloudBacked { .. } => StorageMode::CloudBacked {
            local_cache_path: tmp.path().to_path_buf(),
        },
        StorageMode::Memory => StorageMode::Memory,
    };

    MidgeEngine::open_with_options(opts).expect("open engine")
}

pub fn setup_engine_fs_nosync() -> (MidgeEngine, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.enable_compaction = true;
    let engine = open_with_temp_opts(opts, &tmp, false);
    (engine, tmp)
}

pub fn setup_engine_fs_sync() -> (MidgeEngine, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.enable_compaction = true;
    let engine = open_with_temp_opts(opts, &tmp, true);
    (engine, tmp)
}

pub fn setup_engine_cloud_nosync() -> (MidgeEngine, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    opts.enable_compaction = true;
    let engine = open_with_temp_opts(opts, &tmp, false);
    (engine, tmp)
}

pub fn setup_engine_cloud_sync() -> (MidgeEngine, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    opts.enable_compaction = true;
    let engine = open_with_temp_opts(opts, &tmp, true);
    (engine, tmp)
}

pub fn load_full_dataset(engine: &MidgeEngine) {
    init_ycsb_globals();
    let keys = PREGEN_KEYS.get().expect("init_ycsb_globals");
    let values = PREGEN_VALUES.get().expect("init_ycsb_globals");

    let cf_list = engine.list_column_families().unwrap_or_default();

    // Load into every CF so reads are meaningful regardless of CF selection.
    for cf in &cf_list {
        let cf_id = cf.id();
        let mut batch = WriteBatch::new();

        for i in 0..RECORD_COUNT {
            batch.put_cf(cf_id, keys[i].clone(), values[i].clone());
            if batch.len() >= BATCH_SIZE {
                engine.write_batch(&batch).expect("write_batch");
                batch.clear();
            }
        }

        if !batch.is_empty() {
            engine.write_batch(&batch).expect("write_batch");
        }
    }

    let _ = engine.flush();
}

// ============================================================================
// Zipf-like generator
// ============================================================================

#[derive(Clone)]
pub struct ZipfGenerator {
    cdf: Vec<f64>,
}

impl ZipfGenerator {
    pub fn new(n: usize, theta: f64) -> Self {
        assert!(n > 0);
        assert!(theta >= 0.0);

        // Build CDF for P(k) ∝ 1/(k+1)^theta.
        let mut weights = Vec::with_capacity(n);
        let mut sum = 0.0f64;
        for k in 0..n {
            let w = 1.0 / ((k as f64) + 1.0).powf(theta);
            sum += w;
            weights.push(sum);
        }
        for w in &mut weights {
            *w /= sum;
        }

        Self { cdf: weights }
    }

    pub fn next<R: RngCore>(&self, rng: &mut R) -> usize {
        let u = (rng.next_u64() as f64) / (u64::MAX as f64);
        match self.cdf.binary_search_by(|p| p.partial_cmp(&u).unwrap()) {
            Ok(i) => i,
            Err(i) => i.min(self.cdf.len() - 1),
        }
    }
}
