use cntryl_midge::wal::BatchConfig;
use cntryl_midge::{
    AckPolicy,
    MidgeEngine,
    MidgeOptions,
    OpenOptions,
    Goal,
    Durability,
    WorkloadProfile,
    StorageMode,
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
    opts.storage_mode = StorageMode::LocalDisk { db_path: db_path.clone() };

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

#[stress_test]
fn durability_ack_batched_after_local_durable_10ms(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    opts.ack_policy = AckPolicy::AfterLocalDurable;
    opts.wal_batch_config = Some(BatchConfig { max_delay_ms: 10, max_bytes: 64 * 1024 });
    opts.enable_compaction = false;
    run_durability_case(ctx, opts);
}

#[stress_test]
fn durability_ack_batched_after_local_durable_100ms(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    opts.ack_policy = AckPolicy::AfterLocalDurable;
    opts.wal_batch_config = Some(BatchConfig { max_delay_ms: 100, max_bytes: 64 * 1024 });
    opts.enable_compaction = false;
    run_durability_case(ctx, opts);
}

#[stress_test]
fn durability_ack_batched_immediate_10ms(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    opts.ack_policy = AckPolicy::Immediate;
    opts.wal_batch_config = Some(BatchConfig { max_delay_ms: 10, max_bytes: 64 * 1024 });
    opts.enable_compaction = false;
    run_durability_case(ctx, opts);
}

#[stress_test]
fn durability_ack_batched_immediate_100ms(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    opts.ack_policy = AckPolicy::Immediate;
    opts.wal_batch_config = Some(BatchConfig { max_delay_ms: 100, max_bytes: 64 * 1024 });
    opts.enable_compaction = false;
    run_durability_case(ctx, opts);
}

#[stress_test]
fn durability_ack_strict_after_local_durable_10ms(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = true;
    opts.ack_policy = AckPolicy::AfterLocalDurable;
    opts.wal_batch_config = Some(BatchConfig { max_delay_ms: 10, max_bytes: 64 * 1024 });
    opts.enable_compaction = false;
    run_durability_case(ctx, opts);
}

#[stress_test]
fn durability_ack_strict_after_local_durable_100ms(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = true;
    opts.ack_policy = AckPolicy::AfterLocalDurable;
    opts.wal_batch_config = Some(BatchConfig { max_delay_ms: 100, max_bytes: 64 * 1024 });
    opts.enable_compaction = false;
    run_durability_case(ctx, opts);
}
