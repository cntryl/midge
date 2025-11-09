//! YCSB Smoke Test - Quick validation of all workloads
//!
//! This runs abbreviated versions of YCSB workloads A, B, and C to validate
//! the benchmark infrastructure works correctly without the full 30-second runs.

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use tempfile::TempDir;

/// Zipfian distribution for realistic skewed access patterns
struct ZipfianGenerator {
    items: usize,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    eta: f64,
}

impl ZipfianGenerator {
    fn new(items: usize, theta: f64) -> Self {
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

    fn next(&self, rng: &mut StdRng) -> usize {
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

fn generate_key(id: usize) -> Bytes {
    Bytes::from(format!("user{:012}", id))
}

fn generate_value(id: usize, seed: u64) -> Bytes {
    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(id as u64));
    let data: Vec<u8> = (0..1000).map(|_| rng.random()).collect();
    Bytes::from(data)
}

fn load_data(engine: &MidgeEngine, record_count: usize) {
    let cf = engine.default_column_family();
    for i in 0..record_count {
        let key = generate_key(i);
        let value = generate_value(i, 42);
        engine.put(&cf, &key, &value).expect("Failed to insert record");
    }
    let _ = engine.flush();
}

#[test]
fn should_run_ycsb_workload_a_smoke_test() {
    // Arrange
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).expect("Failed to open engine");
    let record_count = 1000; // Small dataset for smoke test
    load_data(&engine, record_count);

    // Act
    let cf = engine.default_column_family();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);

    for _ in 0..100 {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);

        if rng.random_bool(0.5) {
            let _ = black_box(engine.get(&cf, &key));
        } else {
            let value = generate_value(key_id, rng.random());
            let _ = engine.put(&cf, &key, &value);
        }
    }

    // Assert
    let key = generate_key(0);
    assert!(
        engine.get(&cf, &key).unwrap().is_some(),
        "Data should be readable"
    );

    println!("✅ Workload A smoke test passed (50% R / 50% W)");
}

#[test]
fn should_run_ycsb_workload_b_smoke_test() {
    // Arrange
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).expect("Failed to open engine");
    let record_count = 1000;
    load_data(&engine, record_count);

    // Act
    let cf = engine.default_column_family();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);

    for _ in 0..100 {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);

        if rng.random_bool(0.95) {
            let _ = black_box(engine.get(&cf, &key));
        } else {
            let value = generate_value(key_id, rng.random());
            let _ = engine.put(&cf, &key, &value);
        }
    }

    // Assert
    let key = generate_key(0);
    assert!(
        engine.get(&cf, &key).unwrap().is_some(),
        "Data should be readable"
    );

    println!("✅ Workload B smoke test passed (95% R / 5% W)");
}

#[test]
fn should_run_ycsb_workload_c_smoke_test() {
    // Arrange
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).expect("Failed to open engine");
    let record_count = 1000;
    load_data(&engine, record_count);

    // Act
    let cf = engine.default_column_family();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);

    for _ in 0..100 {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);
        let _ = black_box(engine.get(&cf, &key));
    }

    // Assert
    let key = generate_key(0);
    assert!(
        engine.get(&cf, &key).unwrap().is_some(),
        "Data should be readable"
    );

    println!("✅ Workload C smoke test passed (100% R)");
}
