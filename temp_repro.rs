use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

fn main() {
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // 1000 appends to same key
    for i in 0..1000 {
        eng.put(&cf, b\"hot_key\", format!(\"append{}\", i).as_bytes())
            .unwrap();
    }
    eng.flush().expect(\"flush should succeed\");
    
    let value = eng.get(&cf, b\"hot_key\").unwrap();
    assert_eq!(value.as_deref(), Some(b\"append999\".as_ref()));
    println!(\"SUCCESS: 1000 appends worked!\");
}
