use cntryl_midge::{
    Durability, Goal, MidgeEngine, MidgeOptions, OpenOptions, StorageMode, WorkloadProfile,
};
use cntryl_stress::{stress_test, StressContext};

const VALUE_SIZE: usize = 128;

fn open_engine_for_readmostly() -> (MidgeEngine, std::path::PathBuf) {
    open_engine_with_config(None, None)
}

fn open_engine_with_config(batch_cfg: Option<cntryl_midge::wal::BatchConfig>, memtable_size: Option<usize>) -> (MidgeEngine, std::path::PathBuf) {
    let built = OpenOptions::new()
        .goal(Goal::Latency)
        .durability(Durability::Steady)
        .workload(WorkloadProfile::ReadMostly)
        .build();

    let mut opts = MidgeOptions::default();
    opts.wal_sync = built.wal_sync_on_write();
    opts.ack_policy = match built.durability {
        Durability::Strict => cntryl_midge::AckPolicy::AfterLocalDurable,
        Durability::Steady => cntryl_midge::AckPolicy::Immediate,
        Durability::CloudPersisted => cntryl_midge::AckPolicy::Immediate,
    };

    // Apply optional overrides for experiments
    opts.wal_batch_config = batch_cfg;
    opts.enable_compaction = false;
    opts.memtable_size = memtable_size.unwrap_or_else(|| built.memtable_size_limit());

    let db_dir = std::env::temp_dir().join(format!(
        "midge_put_scale_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: db_dir.clone(),
    };

    let engine = MidgeEngine::open_with_options(opts).unwrap();
    (engine, db_dir)
}

fn run_puts(ctx: &mut StressContext, n: usize) {
    run_puts_with_cfg(ctx, n, None, None)
}

fn run_puts_with_cfg(ctx: &mut StressContext, n: usize, batch_cfg: Option<cntryl_midge::wal::BatchConfig>, memtable_size: Option<usize>) {
    ctx.set_elements(n as u64);

    // Ensure telemetry is enabled for these investigation runs so WAL metrics are recorded
    let _ = cntryl_midge::telemetry::Telemetry::init(cntryl_midge::telemetry::TelemetryConfig::default().with_enabled(true));

    let (engine, dir) = open_engine_with_config(batch_cfg, memtable_size);

    ctx.measure_ref(&engine, |e: &MidgeEngine| {
        let cf = e.default_column_family();
        let val = vec![0u8; VALUE_SIZE];
        for i in 0..n {
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&(i as u64).to_le_bytes());
            e.put(&cf, &key, &val).unwrap();
        }
    });

    // Emit a single summary log line for this stress run
    if let Some(t) = cntryl_midge::telemetry::Telemetry::global() {
        let snap = t.metrics().snapshot();
        // Try to get runtime configuration (best effort)
        if let Ok(cfg) = engine.get_runtime_config() {
            eprintln!(
                "stress_summary n={} wal_policy={:?} batch_delay_ms={} batch_bytes={} wal_append_count={} wal_flush_count={} wal_fsync_count={} wal_append_ns_total={} wal_fsync_ns_total={}",
                n,
                cfg.wal_durability_policy,
                cfg.wal_batch_config.max_delay_ms,
                cfg.wal_batch_config.max_bytes,
                snap.wal_append_count,
                snap.wal_flush_count,
                snap.wal_fsync_count,
                snap.wal_append_ns_total,
                snap.wal_fsync_ns_total,
            );
            // Also write a small summary file to temp for external collection
            let _ = std::fs::write(
                std::env::temp_dir().join(format!("midge_stress_summary_n{}_pid{}.log", n, std::process::id())),
                format!(
                    "n={} wal_policy={:?} batch_delay_ms={} batch_bytes={} wal_append_count={} wal_flush_count={} wal_fsync_count={} wal_append_ns_total={} wal_fsync_ns_total={}\n",
                    n,
                    cfg.wal_durability_policy,
                    cfg.wal_batch_config.max_delay_ms,
                    cfg.wal_batch_config.max_bytes,
                    snap.wal_append_count,
                    snap.wal_flush_count,
                    snap.wal_fsync_count,
                    snap.wal_append_ns_total,
                    snap.wal_fsync_ns_total,
                ),
            );
        } else {
            eprintln!(
                "stress_summary n={} wal_append_count={} wal_flush_count={} wal_fsync_count={} wal_append_ns_total={} wal_fsync_ns_total={} (no runtime config)",
                n,
                snap.wal_append_count,
                snap.wal_flush_count,
                snap.wal_fsync_count,
                snap.wal_append_ns_total,
                snap.wal_fsync_ns_total,
            );
            let _ = std::fs::write(
                std::env::temp_dir().join(format!("midge_stress_summary_n{}_pid{}.log", n, std::process::id())),
                format!(
                    "n={} wal_append_count={} wal_flush_count={} wal_fsync_count={} wal_append_ns_total={} wal_fsync_ns_total={} (no runtime config)\n",
                    n,
                    snap.wal_append_count,
                    snap.wal_flush_count,
                    snap.wal_fsync_count,
                    snap.wal_append_ns_total,
                    snap.wal_fsync_ns_total,
                ),
            );
        }
    }

    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
}

#[stress_test]
fn put_10(ctx: &mut StressContext) {
    run_puts(ctx, 10);
}

#[stress_test]
fn put_100(ctx: &mut StressContext) {
    run_puts(ctx, 100);
}

#[stress_test]
fn put_1000(ctx: &mut StressContext) {
    run_puts(ctx, 1000);
}

#[stress_test]
fn put_1000_tuned(ctx: &mut StressContext) {
    // Larger memtable + longer batch delay to amortize fsyncs
    let batch = cntryl_midge::wal::BatchConfig { max_delay_ms: 500, max_bytes: 256 * 1024 };
    run_puts_with_cfg(ctx, 1000, Some(batch), Some(64 * 1024 * 1024));
}

#[stress_test]
fn put_1000_batch_only(ctx: &mut StressContext) {
    let batch = cntryl_midge::wal::BatchConfig { max_delay_ms: 500, max_bytes: 256 * 1024 };
    run_puts_with_cfg(ctx, 1000, Some(batch), None);
}

#[stress_test]
fn put_1000_mem_only(ctx: &mut StressContext) {
    run_puts_with_cfg(ctx, 1000, None, Some(64 * 1024 * 1024));
}

// Temporarily added: force WAL sync = false to compare throughput against default
#[stress_test]
fn put_1000_force_no_wal_sync(ctx: &mut StressContext) {
    ctx.set_elements(1000);

    // Build options like open_engine_with_config but force wal_sync = false
    let built = OpenOptions::new()
        .goal(Goal::Latency)
        .durability(Durability::Steady)
        .workload(WorkloadProfile::ReadMostly)
        .build();

    let mut opts = MidgeOptions::default();
    opts.wal_sync = false; // force no per-write fsync
    opts.ack_policy = match built.durability {
        Durability::Strict => cntryl_midge::AckPolicy::AfterLocalDurable,
        Durability::Steady => cntryl_midge::AckPolicy::Immediate,
        Durability::CloudPersisted => cntryl_midge::AckPolicy::Immediate,
    };

    opts.wal_batch_config = None;
    opts.enable_compaction = false;
    opts.memtable_size = built.memtable_size_limit();

    let db_dir = std::env::temp_dir().join(format!(
        "midge_put_scale_force_no_wal_sync_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    opts.storage_mode = StorageMode::LocalDisk { db_path: db_dir.clone() };

    let engine = MidgeEngine::open_with_options(opts).unwrap();

    ctx.measure_ref(&engine, |e: &MidgeEngine| {
        let cf = e.default_column_family();
        let val = vec![0u8; VALUE_SIZE];
        for i in 0..1000 {
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&(i as u64).to_le_bytes());
            e.put(&cf, &key, &val).unwrap();
        }
    });

    // Emit a summary
    if let Some(t) = cntryl_midge::telemetry::Telemetry::global() {
        let snap = t.metrics().snapshot();
        if let Ok(cfg) = engine.get_runtime_config() {
            eprintln!(
                "stress_summary n=1000 wal_policy={:?} batch_delay_ms={} batch_bytes={} wal_append_count={} wal_flush_count={} wal_fsync_count={} wal_append_ns_total={} wal_fsync_ns_total={} (forced_no_wal_sync)",
                cfg.wal_durability_policy,
                cfg.wal_batch_config.max_delay_ms,
                cfg.wal_batch_config.max_bytes,
                snap.wal_append_count,
                snap.wal_flush_count,
                snap.wal_fsync_count,
                snap.wal_append_ns_total,
                snap.wal_fsync_ns_total,
            );
            let _ = std::fs::write(
                std::env::temp_dir().join(format!("midge_stress_summary_n{}_pid{}.log", 1000, std::process::id())),
                format!(
                    "n=1000 wal_policy={:?} batch_delay_ms={} batch_bytes={} wal_append_count={} wal_flush_count={} wal_fsync_count={} wal_append_ns_total={} wal_fsync_ns_total={} (forced_no_wal_sync)\n",
                    cfg.wal_durability_policy,
                    cfg.wal_batch_config.max_delay_ms,
                    cfg.wal_batch_config.max_bytes,
                    snap.wal_append_count,
                    snap.wal_flush_count,
                    snap.wal_fsync_count,
                    snap.wal_append_ns_total,
                    snap.wal_fsync_ns_total,
                ),
            );
        }
    }

    drop(engine);
    let _ = std::fs::remove_dir_all(db_dir);
}

#[stress_test]
fn put_10000(ctx: &mut StressContext) {
    run_puts(ctx, 10000);
}
