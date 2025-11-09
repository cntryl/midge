// Minimal test to debug WAL replay issue

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::tempdir;

#[test]
fn should_replay_wal_after_engine_restart() {
    let dir = tempdir().unwrap();

    println!("=== First open ===");
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).unwrap();

        println!("Writing key1");
        eng.put(Bytes::from("key1"), Bytes::from("value1")).unwrap();

        println!("Closing engine");
    }

    println!("\n=== Checking WAL files ===");
    for entry in std::fs::read_dir(dir.path().join("wal")).unwrap() {
        let entry = entry.unwrap();
        println!(
            "WAL file: {:?}, size: {}",
            entry.path(),
            entry.metadata().unwrap().len()
        );
    }

    println!("\n=== Second open ===");
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).unwrap();

        println!("Reading key1");
        let result = eng.get(b"key1").unwrap();
        println!("Result: {:?}", result);

        assert_eq!(result, Some(Bytes::from("value1")), "Data should persist!");
    }
}
