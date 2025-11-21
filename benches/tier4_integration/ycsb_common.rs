//! Common utilities for YCSB benchmarks

use bytes::Bytes;
use cntryl_midge::cloud::mock::MockCloudBackend;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

// ============================================================================
// Configuration Constants
// ============================================================================

#[allow(dead_code)]
pub const OPS_PER_ITER: usize = 5_000;

#[allow(dead_code)]
pub const RECORD_COUNT: usize = 25_000;

#[allow(dead_code)]
pub const BATCH_SIZE: usize = 100; // Batch writes for realistic throughput

#[allow(dead_code)]
pub const THREAD_COUNTS: [usize; 3] = [1_usize, 2, 8];

// ============================================================================
// Zipfian Distribution
// ============================================================================

/// Zipfian distribution for realistic skewed access patterns
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
        let eta = (1.0 - ((2.0 / items as f64).powf(1.0 - theta))) / (1.0 - zeta_2 / zeta_n);

        Self {
            items,
            theta,
            zeta_n,
            alpha,
            eta,
        }
    }

    fn zeta(n: usize, theta: f64) -> f64 {
        let mut sum = 0.0;
        for i in 1..=n {
            sum += 1.0 / (i as f64).powf(theta);
        }
        sum
    }

    pub fn next(&self, rng: &mut StdRng) -> usize {
        let u: f64 = rng.random();
        let uz = u * self.zeta_n;

        if uz < 1.0 {
            return 0;
        }

        if uz < 1.0 + 0.5_f64.powf(self.theta) {
            return 1;
        }

        ((self.items as f64) * ((self.eta * u - self.eta + 1.0).powf(self.alpha))) as usize
            % self.items
    }
}

// ============================================================================
// Data Generation
// ============================================================================

pub fn generate_key(id: usize) -> Bytes {
    Bytes::from(format!("user{:012}", id))
}

pub fn generate_value(id: usize, seed: u64) -> Bytes {
    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(id as u64));
    let data: Vec<u8> = (0..1000).map(|_| rng.random::<u8>()).collect();
    Bytes::from(data)
}

#[allow(dead_code)]
pub fn load_data(engine: &MidgeEngine, record_count: usize) {
    let cf = engine.default_column_family();
    for i in 0..record_count {
        let key = generate_key(i);
        let value = generate_value(i, 42);
        engine
            .put(&cf, &key, &value)
            .expect("failed to insert record during load");
    }
    let _ = engine.flush();
}

// ============================================================================
// Engine Setup
// ============================================================================

#[allow(dead_code)]
pub fn setup_engine_fs_nosync() -> (MidgeEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    (engine, temp_dir)
}

#[allow(dead_code)]
pub fn setup_engine_fs_sync() -> (MidgeEngine, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: true,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    (engine, temp_dir)
}

#[allow(dead_code)]
pub fn setup_engine_cloud_nosync() -> (MidgeEngine, Arc<MockCloudBackend>) {
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(MockCloudBackend::new().with_latency(Duration::from_millis(1)));
    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: temp_dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: Default::default(),
            local_wal_sync: false,
            wal_batch_size: 1024 * 1024, // 1MB batches
            sst_cache_capacity: 10,
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    (engine, backend)
}

#[allow(dead_code)]
pub fn setup_engine_cloud_sync() -> (MidgeEngine, Arc<MockCloudBackend>) {
    let temp_dir = TempDir::new().unwrap();
    let backend = Arc::new(MockCloudBackend::new().with_latency(Duration::from_millis(1)));
    let opts = MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: temp_dir.path().to_path_buf(),
            cloud_backend: backend.clone(),
            storage_context: Default::default(),
            local_wal_sync: true,
            wal_batch_size: 1024 * 1024, // 1MB batches
            sst_cache_capacity: 10,
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: true,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    (engine, backend)
}
