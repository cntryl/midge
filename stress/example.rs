use cntryl_stress::{stress_test, StressContext};
use std::time::{SystemTime, UNIX_EPOCH};
use cntryl_midge::{AckPolicy, MidgeEngine, MidgeOptions, StorageMode};

#[stress_test]
fn put_1k_entries_no_wal(ctx: &mut StressContext) {
    let num_entries = 1_000;
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

    ctx.set_elements(num_entries as u64);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = std::env::temp_dir()
        .join(format!("midge_put_no_wal_{}_{}", std::process::id(), now));

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: db_path.clone() },
        // Durability mechanism (local): Batched group commit.
        wal_sync: false,
        // Caller-visible acknowledgment policy: return as soon as the write is accepted.
        // Durability still happens later according to the durability mechanism.
        ack_policy: AckPolicy::Immediate,
        enable_compaction: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();

    ctx.measure_ref(&engine, |e: &MidgeEngine| {
        use std::time::Instant;
        let cf = e.default_column_family();
        let mut durs: Vec<u128> = Vec::with_capacity(keys.len());
        for (k, v) in keys.iter().zip(values.iter()) {
            let t = Instant::now();
            e.put(&cf, &k[..], v).unwrap();
            durs.push(t.elapsed().as_nanos() as u128);
        }
        print_stats(
            "put_1k_entries (durability=Batched, ack=Immediate)",
            &durs,
        );
    });

    drop(engine);
    let _ = std::fs::remove_dir_all(db_path);
}

// Run the same workload with wal_sync enabled to measure fsync cost
#[stress_test]
fn put_1k_entries_with_wal_sync(ctx: &mut StressContext) {
    let num_entries = 1_000;
    let value_size = 128;

    let mut keys: Vec<[u8; 16]> = Vec::with_capacity(num_entries);
    let mut values: Vec<Vec<u8>> = Vec::with_capacity(num_entries);

    for i in 0..num_entries {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&(i as u64).to_le_bytes());
        keys.push(k);
        values.push(vec![(i % 256) as u8; value_size]);
    }

    ctx.set_elements(num_entries as u64);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = std::env::temp_dir()
        .join(format!("midge_put_wal_sync_{}_{}", std::process::id(), now));

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: db_path.clone() },
        // Durability mechanism (local): Strict fsync per write.
        wal_sync: true,
        // Caller-visible acknowledgment policy: wait for local durability.
        ack_policy: AckPolicy::AfterLocalDurable,
        enable_compaction: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();

    ctx.measure_ref(&engine, |e: &MidgeEngine| {
        use std::time::Instant;
        let cf = e.default_column_family();
        let mut durs: Vec<u128> = Vec::with_capacity(keys.len());
        for (k, v) in keys.iter().zip(values.iter()) {
            let t = Instant::now();
            e.put(&cf, &k[..], v).unwrap();
            durs.push(t.elapsed().as_nanos() as u128);
        }
        print_stats(
            "put_1k_entries (durability=Strict, ack=AfterLocalDurable)",
            &durs,
        );
    });

    drop(engine);
    let _ = std::fs::remove_dir_all(db_path);
}

// Batched writes using WriteBatch to see speedup from batching
#[stress_test]
fn put_1k_entries_batched(ctx: &mut StressContext) {
    use cntryl_midge::engine::api::WriteBatch;

    let num_entries = 1_000;
    let value_size = 128;

    let mut keys: Vec<[u8; 16]> = Vec::with_capacity(num_entries);
    let mut values: Vec<Vec<u8>> = Vec::with_capacity(num_entries);

    for i in 0..num_entries {
        let mut k = [0u8; 16];
        k[..8].copy_from_slice(&(i as u64).to_le_bytes());
        keys.push(k);
        values.push(vec![(i % 256) as u8; value_size]);
    }

    ctx.set_elements(num_entries as u64);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = std::env::temp_dir()
        .join(format!("midge_put_batched_{}_{}", std::process::id(), now));

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: db_path.clone() },
        // Durability mechanism (local): Batched group commit.
        wal_sync: false,
        // For write batching, wait for local durability so the batch timing reflects
        // durable commits (still amortized across many ops).
        ack_policy: AckPolicy::AfterLocalDurable,
        enable_compaction: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();

    ctx.measure_ref(&engine, |e: &MidgeEngine| {
        use std::time::Instant;
        let batch_size = 64usize;
        let mut batch_durs: Vec<u128> = Vec::new();
        let mut i = 0usize;
        while i < keys.len() {
            let mut batch = WriteBatch::new();
            let start = Instant::now();
            let end = std::cmp::min(i + batch_size, keys.len());
            for j in i..end {
                batch.put_owned(keys[j].to_vec(), values[j].clone());
            }
            e.write_batch(&batch).unwrap();
            batch_durs.push(start.elapsed().as_nanos() as u128);
            i = end;
        }
        print_stats(
            "write_batch 1k entries (durability=Batched, ack=AfterLocalDurable, batch=64)",
            &batch_durs,
        );
    });

    drop(engine);
    let _ = std::fs::remove_dir_all(db_path);
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
    let p50 = sorted[count/2];
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
