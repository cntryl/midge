//! YCSB Workload A — 50% Read / 50% Update (duration-bounded)
//!
//! Behavior (implementation notes):
//! - Deterministic pre-generated keys & values
//! - Zipfian key selection (theta=0.99)
//! - 50% reads, 50% updates
//! - Batched writes (BATCH_SIZE)
//! - Runs for the provided wall-clock duration

use std::sync::OnceLock;
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::thread;

use bytes::Bytes;
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use cntryl_midge::{MidgeEngine, WriteBatch};

// Workload constants (mirrors bench defaults)
const RECORD_COUNT: usize = 200_000;
const VALUE_SIZE: usize = 1_000;
const BATCH_SIZE: usize = 1024;
const CF_COUNTS: &[usize] = &[1, 4, 16];

// Precomputed keys & values
static PREGEN_KEYS: OnceLock<Vec<Bytes>> = OnceLock::new();
static PREGEN_VALUES: OnceLock<Vec<Bytes>> = OnceLock::new();

fn init_pregen() {
    PREGEN_KEYS.get_or_init(|| (0..RECORD_COUNT).map(|i| Bytes::from(format!("user{:012}", i))).collect());
    PREGEN_VALUES.get_or_init(|| {
        let mut rng = StdRng::seed_from_u64(0xD1CE_F00D_CAFE_F00D);
        (0..RECORD_COUNT)
            .map(|_| {
                let mut v = vec![0u8; VALUE_SIZE];
                rng.fill_bytes(&mut v);
                Bytes::from(v)
            })
            .collect()
    });
}

// Zipfian generator (portable copy from bench)
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
        let eta = (1.0 - (2.0 / items as f64).powf(1.0 - theta)) / (1.0 - (zeta_2 / zeta_n));
        Self { items, theta, zeta_n, alpha, eta }
    }

    fn zeta(n: usize, theta: f64) -> f64 {
        (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
    }

    fn next(&self, rng: &mut StdRng) -> usize {
        let r = rng.next_u64();
        let u: f64 = (r as f64) / 18446744073709551616.0; // 2^64
        let uz = u * self.zeta_n;

        if uz < 1.0 { return 0; }
        if uz < 1.0 + 0.5_f64.powf(self.theta) { return 1; }

        let v = self.eta * u - (self.eta - 1.0);
        let idx = (self.items as f64 * v.powf(self.alpha)) as usize;
        idx % self.items
    }
}

fn init_zipf() -> ZipfianGenerator {
    ZipfianGenerator::new(RECORD_COUNT, 0.99)
}

fn make_thread_rng(thread_id: usize, workload_seed: u64) -> StdRng {
    StdRng::seed_from_u64(workload_seed ^ ((thread_id as u64) << 32))
}

#[derive(Clone, Debug)]
struct LatencyStats {
    p50: u64,
    p99: u64,
    p99_9: u64,
}

fn run_thread_workload(
    engine: Arc<MidgeEngine>,
    duration: Duration,
    thread_id: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine.list_column_families().unwrap_or_default();
    let mut rng = make_thread_rng(thread_id, 0xCAFEBABE);

    let zipf = init_zipf();
    let keys = PREGEN_KEYS.get().expect("pregen not initialized");
    let values = PREGEN_VALUES.get().expect("pregen not initialized");

    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut batch = WriteBatch::new();

    let start_run = Instant::now();

    while start_run.elapsed() < duration {
        let key_id = zipf.next(&mut rng);
        let key = &keys[key_id];
        let cf = &cf_list[rng.gen_range(0..cf_count)];
        let cf_id = cf.id();

        let start = Instant::now();

        if rng.next_u32() & 1 == 0 {
            let _ = engine.get(cf, key);
        } else {
            let value = &values[key_id];
            batch.put_cf(cf_id, key.clone(), value.clone());

            if batch.len() >= BATCH_SIZE {
                engine.write_batch(&batch).unwrap();
                batch.clear();
            }
        }

        let elapsed = start.elapsed().as_micros() as u64;
        let _ = hist.record(elapsed.max(1));
    }

    if !batch.is_empty() {
        engine.write_batch(&batch).unwrap();
    }

    LatencyStats {
        p50: hist.value_at_percentile(50.0),
        p99: hist.value_at_percentile(99.0),
        p99_9: hist.value_at_percentile(99.9),
    }
}

fn load_full_dataset(engine: &MidgeEngine) {
    let keys = PREGEN_KEYS.get().expect("pregen not initialized");
    let vals = PREGEN_VALUES.get().expect("pregen not initialized");

    let cf = engine.default_column_family();
    let cf_id = cf.id();

    let mut batch = WriteBatch::new();
    for i in 0..keys.len() {
        batch.put_cf(cf_id, keys[i].clone(), vals[i].clone());
        if batch.len() >= BATCH_SIZE {
            engine.write_batch(&batch).unwrap();
            batch.clear();
        }
    }

    if !batch.is_empty() {
        engine.write_batch(&batch).unwrap();
    }

    let _ = engine.flush();
}

/// Run Workload A with the given engine and duration.
///
/// Behavior: For each CF count in `CF_COUNTS` we create additional CFs to match
/// the desired count, load the full dataset, then run the workload (single or
/// multi-threaded based on STRESS_THREADS env). Results (p50/p99/p99.9) are
/// printed per-run for visibility.
pub fn run(engine: Arc<MidgeEngine>, duration: Duration) {
    init_pregen();

    // threads selectable via env var, default 1
    let threads: usize = std::env::var("STRESS_THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);

    for &cf_count in CF_COUNTS {
        // create CFs to reach cf_count
        for i in 1..cf_count {
            let _ = engine.create_column_family(&format!("cf{cf_count}_{i}"));
        }

        // Load dataset
        eprintln!("loading dataset for cf_count={}...", cf_count);
        load_full_dataset(engine.as_ref());

        if threads == 1 {
            eprintln!("running single-threaded for {:?}...", duration);
            let stats = run_thread_workload(Arc::clone(&engine), duration, 0, cf_count);
            eprintln!("RESULT cf{}: p50={}us p99={}us p99.9={}us", cf_count, stats.p50, stats.p99, stats.p99_9);
        } else {
            eprintln!("running concurrently with {} threads for {:?}...", threads, duration);
            let handles: Vec<_> = (0..threads)
                .map(|tid| {
                    let engine = Arc::clone(&engine);
                    thread::spawn(move || run_thread_workload(engine, duration, tid, cf_count))
                })
                .collect();

            for (i, h) in handles.into_iter().enumerate() {
                let s = h.join().unwrap();
                eprintln!("RESULT cf{} thread{}: p50={}us p99={}us p99.9={}us", cf_count, i, s.p50, s.p99, s.p99_9);
            }
        }
    }
}
