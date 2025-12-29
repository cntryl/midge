//! Stress repro for the Tier-3 durability-mode "sludge" without Criterion.
//!
//! This uses the `cntryl-stress` harness so we can time phases in isolation and
//! vary ack/durability knobs deterministically.

use cntryl_midge::wal::BatchConfig;
use cntryl_midge::{AckPolicy, MidgeEngine, StorageMode};
use cntryl_stress::{stress_test, StressContext};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const RECORDS: usize = 1_000;
const OPS: usize = 500;
const VALUE_SIZE: usize = 64;
const BATCH_SIZE: usize = 100;

fn make_key(i: usize) -> Vec<u8> {
    // Matches benches/tier3_system_bench_common.rs ("key_" + 10 digits).
    let mut key = vec![0u8; 14];
    key[..4].copy_from_slice(b"key_");
    let mut n = i;
    for j in (4..14).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    key
}

fn make_value_fixed(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

fn load_data_put(engine: &MidgeEngine, keys: &[Vec<u8>], values: &[Vec<u8>]) {
    let cf = engine.default_column_family();
    for (i, key) in keys.iter().enumerate() {
        let val_idx = i % values.len();
        engine.put(cf, key.as_slice(), values[val_idx].as_slice()).unwrap();
    }
}

fn load_data_write_batch(
    engine: &MidgeEngine,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    batch_size: usize,
) {
    let cf = engine.default_column_family();
    for chunk in keys.chunks(batch_size) {
        let mut batch = cntryl_midge::WriteBatch::new();
        for (i, key) in chunk.iter().enumerate() {
            let val_idx = i % values.len();
            batch.put_owned_cf(cf.id(), key.clone(), values[val_idx].clone());
        }
        engine.write_batch(&batch).unwrap();
    }
}

fn run_mixed_workload(
    engine: &MidgeEngine,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    operations: usize,
) {
    let cf = engine.default_column_family();
    for i in 0..operations {
        let key_idx = i % keys.len();
        if i % 2 == 0 {
            let _ = engine.get(cf, keys[key_idx].as_slice());
        } else {
            let val_idx = i % values.len();
            let _ = engine.put(cf, keys[key_idx].as_slice(), values[val_idx].as_slice());
        }
    }
}

fn run_case(ctx: &mut StressContext, ack_policy: AckPolicy, wal_sync: bool, batch_ms: u64) {
    ctx.set_elements(OPS as u64);

    // Unique path per run.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = std::env::temp_dir().join(format!(
        "midge_durability_sludge_{}_{}_ack_{:?}_sync_{}_batch_{}ms",
        std::process::id(),
        now,
        ack_policy,
        wal_sync,
        batch_ms
    ));

    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: db_path.clone(),
    };
    opts.enable_compaction = false;
    opts.memtable_size = 8 * 1024 * 1024;
    opts.ack_policy = ack_policy;
    opts.wal_sync = wal_sync;
    opts.wal_batch_config = Some(BatchConfig {
        max_delay_ms: batch_ms,
        max_bytes: 64 * 1024,
    });

    let keys: Vec<Vec<u8>> = (0..RECORDS).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..OPS).map(|_| make_value_fixed(VALUE_SIZE)).collect();

    let open_start = Instant::now();
    let engine = MidgeEngine::open_with_options(opts).unwrap();
    let open_d = open_start.elapsed();

    let load_start = Instant::now();
    // Load with batches to avoid setup dominating when ack policy is durable.
    load_data_write_batch(&engine, &keys, &values, BATCH_SIZE);
    let load_d = load_start.elapsed();

    // Time the workload with the stress harness, but also measure wall time locally.
    // (The harness API is intentionally minimal and does not necessarily return durations.)
    let work_start = Instant::now();
    ctx.measure_ref(&engine, |e: &MidgeEngine| {
        run_mixed_workload(e, &keys, &values, OPS);
    });
    let work_d = work_start.elapsed();

    let drop_start = Instant::now();
    drop(engine);
    let drop_d = drop_start.elapsed();

    eprintln!(
        "[stress] case ack={:?} wal_sync={} batch={}ms | open={:.3}s load={:.3}s work={:.3}s drop={:.3}s",
        ack_policy,
        wal_sync,
        batch_ms,
        open_d.as_secs_f64(),
        load_d.as_secs_f64(),
        work_d.as_secs_f64(),
        drop_d.as_secs_f64(),
    );

    let _ = std::fs::remove_dir_all(db_path);
}

// Focused cases that demonstrate the root cause:
// - AfterLocalDurable + Batched with large max_delay adds ~max_delay latency per put for single-threaded callers.
// - Immediate ack removes that caller-visible wait.

#[stress_test]
fn durability_sludge_after_local_batched_100ms(ctx: &mut StressContext) {
    run_case(ctx, AckPolicy::AfterLocalDurable, false, 100);
}

#[stress_test]
fn durability_sludge_after_local_batched_1ms(ctx: &mut StressContext) {
    run_case(ctx, AckPolicy::AfterLocalDurable, false, 1);
}

#[stress_test]
fn durability_sludge_immediate_batched_100ms(ctx: &mut StressContext) {
    run_case(ctx, AckPolicy::Immediate, false, 100);
}

#[stress_test]
fn durability_sludge_after_local_strict(ctx: &mut StressContext) {
    // Strict implies per-write fsync; batch window is irrelevant but kept for consistent config.
    run_case(ctx, AckPolicy::AfterLocalDurable, true, 100);
}
