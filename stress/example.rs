use cntryl_stress::{stress_test, StressContext};
use std::time::{SystemTime, UNIX_EPOCH};

#[stress_test]
fn put_1k_entries_no_wal(ctx: &mut StressContext) {
    let num_entries = 1_000;
    let key_size = 16;
    let value_size = 128;

    let mut keys = Vec::with_capacity(num_entries);
    let mut values = Vec::with_capacity(num_entries);
    for i in 0..num_entries {
        keys.push(format!("k{:0>14}", i).into_bytes());
        values.push(vec![(i % 256) as u8; value_size]);
    }

    ctx.set_elements(num_entries as u64);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path =
        std::env::temp_dir().join(format!("midge_put_no_wal_{}_{}", std::process::id(), now));

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        wal_sync: false,
        enable_compaction: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();

    ctx.measure_ref(&engine, |e| {
        let cf = e.default_column_family();
        for (k, v) in keys.iter().zip(values.iter()) {
            e.put(&cf, k, v).unwrap();
        }
    });

    drop(engine);
    let _ = std::fs::remove_dir_all(db_path);
}
