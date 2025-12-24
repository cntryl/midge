use cntryl_midge::wal::BatchConfig;
use cntryl_midge::{AckPolicy, MidgeEngine, MidgeOptions, StorageMode};
use cntryl_stress::{stress_test, StressContext};
use std::time::{SystemTime, UNIX_EPOCH};

#[stress_test]
fn durability_ack_matrix(ctx: &mut StressContext) {
    let num_entries = 10;
    let value_size = 128;

    // Pre-build fixed-size binary keys
    let mut keys: Vec<[u8; 16]> = Vec::with_capacity(num_entries);
    let mut values: Vec<Vec<u8>> = Vec::with_capacity(num_entries);
    for i in 0..num_entries {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&(i as u64).to_le_bytes());
        keys.push(k);
        values.push(vec![(i % 256) as u8; value_size]);
    }

    let batch_10ms = BatchConfig {
        max_delay_ms: 10,
        max_bytes: 64 * 1024,
    };
    let batch_100ms = BatchConfig {
        max_delay_ms: 100,
        max_bytes: 64 * 1024,
    };

    let cases: Vec<(&str, bool, AckPolicy, BatchConfig)> = vec![
        (
            "Batched + AfterLocalDurable (delay=10ms)",
            false,
            AckPolicy::AfterLocalDurable,
            batch_10ms,
        ),
        (
            "Batched + AfterLocalDurable (delay=100ms)",
            false,
            AckPolicy::AfterLocalDurable,
            batch_100ms,
        ),
        (
            "Batched + Immediate (delay=10ms)",
            false,
            AckPolicy::Immediate,
            batch_10ms,
        ),
        (
            "Batched + Immediate (delay=100ms)",
            false,
            AckPolicy::Immediate,
            batch_100ms,
        ),
        (
            "Strict + AfterLocalDurable (delay=10ms; should be ignored)",
            true,
            AckPolicy::AfterLocalDurable,
            batch_10ms,
        ),
        (
            "Strict + AfterLocalDurable (delay=100ms; should be ignored)",
            true,
            AckPolicy::AfterLocalDurable,
            batch_100ms,
        ),
    ];

    ctx.set_elements((num_entries * cases.len()) as u64);

    for (case_name, wal_sync, ack_policy, wal_batch_config) in cases {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path = std::env::temp_dir().join(format!(
            "midge_put_matrix_{}_{}_{}",
            std::process::id(),
            now,
            wal_batch_config.max_delay_ms
        ));

        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            wal_sync,
            ack_policy,
            wal_batch_config: Some(wal_batch_config),
            enable_compaction: false,
            ..Default::default()
        };

        // IMPORTANT: open_with_options() is the path that actually honors MidgeOptions.
        let engine = MidgeEngine::open_with_options(opts).unwrap();

        let cfg = engine.get_runtime_config().unwrap();
        println!(
            "[matrix] {} => wal_policy={:?} batch_delay_ms={} batch_max_bytes={}",
            case_name,
            cfg.wal_durability_policy,
            cfg.wal_batch_config.max_delay_ms,
            cfg.wal_batch_config.max_bytes
        );

        ctx.measure_ref(&engine, |e: &MidgeEngine| {
            use std::time::Instant;
            let cf = e.default_column_family();
            let mut durs: Vec<u128> = Vec::with_capacity(keys.len());
            for (k, v) in keys.iter().zip(values.iter()) {
                let t = Instant::now();
                e.put(&cf, &k[..], v).unwrap();
                durs.push(t.elapsed().as_nanos() as u128);
            }
            print_stats(&format!("[matrix] {}", case_name), &durs);
        });

        drop(engine);
        let _ = std::fs::remove_dir_all(db_path);
    }
}

// Minimal stats printer
fn print_stats(name: &str, durs_ns: &[u128]) {
    if durs_ns.is_empty() {
        println!("{}: no samples", name);
        return;
    }
    let count = durs_ns.len();
    let total_ns: u128 = durs_ns.iter().copied().sum();
    let mean_ns = total_ns / count as u128;
    let min_ns = *durs_ns.iter().min().unwrap();
    let max_ns = *durs_ns.iter().max().unwrap();
    let mut sorted = durs_ns.to_vec();
    sorted.sort_unstable();
    let p50 = sorted[count / 2];
    let p95 = sorted[(count * 95) / 100];
    println!("{}: samples={} total_ms={:.2} mean_us={:.2} min_us={:.2} p50_us={:.2} p95_us={:.2} max_ms={:.2}",
        name,
        count,
        total_ns as f64 / 1_000_000.0,
        mean_ns as f64 / 1_000.0,
        min_ns as f64 / 1_000.0,
        p50 as f64 / 1_000.0,
        p95 as f64 / 1_000.0,
        max_ns as f64 / 1_000_000.0,
    );
}
