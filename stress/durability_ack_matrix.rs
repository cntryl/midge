use cntryl_midge::wal::BatchConfig;
use cntryl_midge::{
    AckPolicy, Durability, Goal, MidgeEngine, MidgeOptions, OpenOptions, StorageMode,
    WorkloadProfile,
};
use cntryl_stress::{stress_test, StressContext};
use std::time::{SystemTime, UNIX_EPOCH};

const NUM_ENTRIES: usize = 10;
const VALUE_SIZE: usize = 128;

fn run_durability_case(ctx: &mut StressContext, mut opts: MidgeOptions) {
    // Pre-build fixed-size binary keys/values for this case
    let mut keys: Vec<[u8; 16]> = Vec::with_capacity(NUM_ENTRIES);
    let mut values: Vec<Vec<u8>> = Vec::with_capacity(NUM_ENTRIES);
    for i in 0..NUM_ENTRIES {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&(i as u64).to_le_bytes());
        keys.push(k);
        values.push(vec![(i % 256) as u8; VALUE_SIZE]);
    }

    ctx.set_elements(NUM_ENTRIES as u64);

    // Derive a readable batch label for the path
    let batch_label = opts
        .wal_batch_config
        .as_ref()
        .map(|b| b.max_delay_ms.to_string())
        .unwrap_or_else(|| "none".to_string());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = std::env::temp_dir().join(format!(
        "midge_put_matrix_{}_{}_{}",
        std::process::id(),
        now,
        batch_label
    ));

    // Ensure storage path is local for these stress runs
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: db_path.clone(),
    };

    // IMPORTANT: open_with_options() is the path that actually honors MidgeOptions.
    let engine = MidgeEngine::open_with_options(opts).unwrap();

    ctx.measure_ref(&engine, |e: &MidgeEngine| {
        let cf = e.default_column_family();
        for (k, v) in keys.iter().zip(values.iter()) {
            e.put(&cf, &k[..], v).unwrap();
        }
    });

    drop(engine);
    let _ = std::fs::remove_dir_all(db_path);
}

// Per-case durability matrix (one test function per row)
//
// ACK POLICY           WAL SYNC     BATCH
// ------------------------------------------------
// Immediate            false        10ms
// Immediate            false        100ms
// AfterLocalDurable    false        10ms
// AfterLocalDurable    false        100ms
// AfterLocalDurable    true         10ms
// AfterLocalDurable    true         100ms
//
// This generates a separate `#[stress_test]` for each case so the harness
// produces one result per matrix row (no ambiguous single aggregate result).

macro_rules! define_durability_case {
    ($func:ident, $name:expr, $ack:expr, $wal_sync:expr, $batch_ms:expr) => {
        #[stress_test]
        fn $func(ctx: &mut StressContext) {
            let mut opts = cntryl_midge::testkit::opts_for_mode("local");
            opts.wal_sync = $wal_sync;
            opts.ack_policy = $ack;
            opts.enable_compaction = false;
            opts.wal_batch_config = $batch_ms.map(|ms| BatchConfig { max_delay_ms: ms, max_bytes: 64 * 1024 });

            // If the stress harness adds named subcases in the future, the
            // `$name` value can be forwarded to it for better output.
            run_durability_case(ctx, opts);
        }
    };
}

define_durability_case!(durability_immediate_async_10ms, "immediate_async_10ms", AckPolicy::Immediate, false, Some(10));
define_durability_case!(durability_immediate_async_100ms, "immediate_async_100ms", AckPolicy::Immediate, false, Some(100));
define_durability_case!(durability_after_local_async_10ms, "after_local_async_10ms", AckPolicy::AfterLocalDurable, false, Some(10));
define_durability_case!(durability_after_local_async_100ms, "after_local_async_100ms", AckPolicy::AfterLocalDurable, false, Some(100));
define_durability_case!(durability_after_local_strict_10ms, "after_local_strict_10ms", AckPolicy::AfterLocalDurable, true, Some(10));
define_durability_case!(durability_after_local_strict_100ms, "after_local_strict_100ms", AckPolicy::AfterLocalDurable, true, Some(100));
