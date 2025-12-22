//! YCSB Workload A — 50% Read / 50% Update (duration-bounded)
//!
//! Behavior (implementation notes):
//! - Deterministic pre-generated keys & values
//! - Zipfian key selection (theta=0.99)
//! - 50% reads, 50% updates
//! - Batched writes (BATCH_SIZE)
//! - Runs for the provided wall-clock duration

use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

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
    PREGEN_KEYS.get_or_init(|| {
        (0..RECORD_COUNT)
            .map(|i| Bytes::from(format!("user{:012}", i)))
            .collect()
    });
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

use super::common::ZipfianGenerator;

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

    let zipf = ZipfianGenerator::new(RECORD_COUNT, 0.99);
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

pub fn load_full_dataset(engine: &MidgeEngine, verbose: bool) {
    // Default convenience: load into the default column family
    let cf = engine.default_column_family();
    load_full_dataset_into_cf(engine, cf, verbose);
}

/// Load the entire dataset into a specific column family handle.
pub fn load_full_dataset_into_cf(
    engine: &MidgeEngine,
    cf: &cntryl_midge::engine::ColumnFamilyHandle,
    verbose: bool,
) {
    let keys = PREGEN_KEYS.get().expect("pregen not initialized");
    let vals = PREGEN_VALUES.get().expect("pregen not initialized");

    // Warmup hook: ensure a few small WAL writes + sync before heavy batches (helps on Windows/AV)
    // This is intentionally lightweight and idempotent.
    fn warmup_once(engine: &MidgeEngine) {
        let _ = engine.sync();
    }

    warmup_once(engine);

    let cf_id = cf.id();

    let mut batch = WriteBatch::new();
    let mut written: usize = 0;
    let report_interval = 10_000_usize;

    let load_start = Instant::now();
    let mut last_report = Instant::now();
    let mut batch_write_count: usize = 0;
    let mut total_batch_write_ns: u128 = 0;

    if verbose {
        eprintln!(
            "begin load into cf_id={:?}: {} records, batch_size={}",
            cf_id,
            keys.len(),
            BATCH_SIZE
        );
    }

    for i in 0..keys.len() {
        batch.put_cf(cf_id, keys[i].clone(), vals[i].clone());
        if batch.len() >= BATCH_SIZE {
            let bw_start = Instant::now();
            engine.write_batch(&batch).unwrap();
            let bw_dur = bw_start.elapsed();

            // Diagnostics
            let bw_ms = bw_dur.as_secs_f64() * 1000.0;
            if verbose && bw_ms > 50.0 {
                eprintln!(
                    "warning: slow batch write at record {} took {:.3} ms",
                    written + BATCH_SIZE,
                    bw_ms
                );
            }

            total_batch_write_ns += bw_dur.as_nanos();
            batch_write_count += 1;

            batch.clear();
            written += BATCH_SIZE;

            // Periodic time-based progress (every ~5s) or record-based progress
            if verbose {
                let now = Instant::now();
                if now.duration_since(last_report) > Duration::from_secs(5)
                    || written.is_multiple_of(report_interval)
                {
                    let elapsed = now.duration_since(load_start);
                    let rec_per_s = written as f64 / elapsed.as_secs_f64();
                    let avg_batch_ms = if batch_write_count > 0 {
                        (total_batch_write_ns as f64 / batch_write_count as f64) / 1_000_000.0
                    } else {
                        0.0
                    };
                    eprintln!("progress: loaded {} / {} records ({:.2}%) — {:.0} rec/s avg_batch_write={:.3}ms ({} batches)",
                        written, keys.len(), written as f64 * 100.0 / keys.len() as f64, rec_per_s, avg_batch_ms, batch_write_count);
                    last_report = now;
                }
            }
        }
    }

    if !batch.is_empty() {
        let remaining = batch.len();
        let bw_start = Instant::now();
        engine.write_batch(&batch).unwrap();
        let bw_dur = bw_start.elapsed();
        let bw_ms = bw_dur.as_secs_f64() * 1000.0;
        if verbose && bw_ms > 50.0 {
            eprintln!(
                "warning: slow batch write at final record took {:.3} ms",
                bw_ms
            );
        }
        total_batch_write_ns += bw_dur.as_nanos();
        batch_write_count += 1;
        written += remaining;
    }

    let total_elapsed = load_start.elapsed();
    let rec_per_s = if total_elapsed.as_secs_f64() > 0.0 {
        written as f64 / total_elapsed.as_secs_f64()
    } else {
        0.0
    };
    let avg_batch_ms = if batch_write_count > 0 {
        (total_batch_write_ns as f64 / batch_write_count as f64) / 1_000_000.0
    } else {
        0.0
    };

    if verbose {
        eprintln!("loaded {} / {} records (100%) in {:.3}s — {:.0} rec/s avg_batch_write={:.3}ms ({} batches)",
            written, keys.len(), total_elapsed.as_secs_f64(), rec_per_s, avg_batch_ms, batch_write_count);
    }

    let _ = engine.flush();
}

/// Prepare the engine for a single, once-per-engine load. Creates CFs up to the
/// largest CF count used by the workload and loads the full dataset into each CF once.
pub fn prepare_for_load(engine: &MidgeEngine, verbose: bool) {
    let max_cf = *CF_COUNTS.iter().max().unwrap_or(&1);

    // Create additional CFs if needed (CF 0 exists by default)
    for i in 1..max_cf {
        let name = format!("cf{}_{}", max_cf, i);
        let _ = engine.create_column_family(&name);
    }

    // Load dataset into every CF id in [0, max_cf)
    let cf_list = engine.list_column_families().unwrap_or_default();
    for cf in cf_list.iter().filter(|c| c.id().as_u32() < max_cf as u32) {
        if verbose {
            eprintln!(
                "loading dataset into column family '{}' (id={:?})",
                cf.name(),
                cf.id()
            );
        }
        load_full_dataset_into_cf(engine, cf, verbose);
    }
}

/// Probe batch write latency by performing `count` sample batches and flushing.
/// Useful to diagnose first-write / WAL/FS behavior before the full load.
pub fn warmup_wal(engine: &MidgeEngine, verbose: bool) {
    init_pregen();
    let keys = PREGEN_KEYS.get().expect("pregen not initialized");
    let vals = PREGEN_VALUES.get().expect("pregen not initialized");

    let cf = engine.default_column_family();
    let cf_id = cf.id();

    eprintln!("warming up WAL/filesystem with small batches...");

    for i in 0..4 {
        let mut batch = WriteBatch::new();
        for j in 0..64 {
            let idx = (i * 64 + j) % keys.len();
            batch.put_cf(cf_id, keys[idx].clone(), vals[idx].clone());
        }
        let start = Instant::now();
        engine.write_batch(&batch).unwrap();
        let dur = start.elapsed();
        let ms = dur.as_secs_f64() * 1000.0;
        if verbose {
            eprintln!("warmup batch {} took {:.3} ms", i + 1, ms);
        }
    }

    // Force WAL sync to ensure files are created and any FS overhead is paid now.
    let start = Instant::now();
    let _ = engine.sync();
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    eprintln!("warmup wal sync took {:.3} ms", ms);
}

pub fn probe_batch_writes(engine: &MidgeEngine, verbose: bool, count: usize) {
    init_pregen();
    let keys = PREGEN_KEYS.get().expect("pregen not initialized");
    let vals = PREGEN_VALUES.get().expect("pregen not initialized");

    let cf = engine.default_column_family();
    let cf_id = cf.id();

    eprintln!(
        "probing {} batch writes (batch_size={})...",
        count, BATCH_SIZE
    );

    for probe in 0..count {
        let mut batch = WriteBatch::new();
        // use deterministic slice of keys for probe
        for i in 0..BATCH_SIZE {
            let idx = (probe * BATCH_SIZE + i) % keys.len();
            batch.put_cf(cf_id, keys[idx].clone(), vals[idx].clone());
        }
        let start = Instant::now();
        engine.write_batch(&batch).unwrap();
        let dur = start.elapsed();
        let ms = dur.as_secs_f64() * 1000.0;
        if verbose {
            eprintln!("probe {} batch_write took {:.3} ms", probe + 1, ms);
        } else {
            eprintln!("probe {}: {:.3} ms", probe + 1, ms);
        }
    }

    // flush to ensure durability effects are observed
    let start = Instant::now();
    let _ = engine.flush();
    let flush_ms = start.elapsed().as_secs_f64() * 1000.0;
    eprintln!("probe flush took {:.3} ms", flush_ms);
}

/// Run Workload A with the given engine and duration.
///
/// Behavior: For each CF count in `CF_COUNTS` we create additional CFs to match
/// the desired count, load the full dataset, then run the workload (single or
/// multi-threaded based on STRESS_THREADS env). Results (p50/p99/p99.9) are
/// printed per-run for visibility.
pub fn run(engine: Arc<MidgeEngine>, duration: Duration, skip_load: bool, verbose: bool) {
    init_pregen();

    // threads selectable via env var, default 1
    let threads: usize = std::env::var("STRESS_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    for &cf_count in CF_COUNTS {
        // create CFs to reach cf_count
        for i in 1..cf_count {
            let _ = engine.create_column_family(&format!("cf{cf_count}_{i}"));
        }

        // Load dataset (skip if requested)
        if skip_load {
            eprintln!("skipping load phase for cf_count={}", cf_count);
        } else {
            eprintln!("loading dataset for cf_count={}...", cf_count);
            load_full_dataset(engine.as_ref(), verbose);
        }

        if threads == 1 {
            eprintln!("running single-threaded for {:?}...", duration);
            let stats = run_thread_workload(Arc::clone(&engine), duration, 0, cf_count);
            eprintln!(
                "RESULT cf{}: p50={}us p99={}us p99.9={}us",
                cf_count, stats.p50, stats.p99, stats.p99_9
            );
        } else {
            eprintln!(
                "running concurrently with {} threads for {:?}...",
                threads, duration
            );
            let handles: Vec<_> = (0..threads)
                .map(|tid| {
                    let engine = Arc::clone(&engine);
                    thread::spawn(move || run_thread_workload(engine, duration, tid, cf_count))
                })
                .collect();

            for (i, h) in handles.into_iter().enumerate() {
                let s = h.join().unwrap();
                eprintln!(
                    "RESULT cf{} thread{}: p50={}us p99={}us p99.9={}us",
                    cf_count, i, s.p50, s.p99, s.p99_9
                );
            }
        }
    }
}
