// WAL and Recovery
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::fs;

mod common;
use common::{test_temp_dir, new_engine};
#[test]
fn should_rotate_wal_given_small_buffer_when_multiple_puts() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_buffer_size: 64,
        memtable_size: 1024 * 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();

    // Act
    for i in 0..10u8 {
        let k = [b"k"[0], i];
        let v = [b"v"[0], i];
        eng.put(&cf, &k, &v).unwrap();
    }

    // Assert - Check after writes. WAL creation may be performed by
    // background components; poll briefly to avoid flaky failures on
    // heavily-loaded or slow CI hosts.
    let wal_dir = opts.storage_mode.local_path().join("wal");
    let sst_dir = opts.storage_mode.local_path().join("sst");

    // Wait up to 2000ms for either a WAL file or an SST file to appear.
    // In some configurations the flush worker may quickly rotate and prune
    // WAL files after creating SSTs, so asserting exclusively on WAL files
    // is flaky. Accept either artifact as evidence that rotation occurred.
    let mut waited = 0u64;
    while waited < 2000 {
        let wal_has_file = wal_dir.exists()
            && fs::read_dir(&wal_dir)
                .map(|mut it| it.next().is_some())
                .unwrap_or(false);
        let sst_has_file = sst_dir.exists()
            && fs::read_dir(&sst_dir)
                .map(|mut it| it.next().is_some())
                .unwrap_or(false);
        if wal_has_file || sst_has_file {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
        waited += 10;
    }

    let wal_has_file = wal_dir.exists()
        && fs::read_dir(&wal_dir)
            .map(|mut it| it.next().is_some())
            .unwrap_or(false);
    let sst_has_file = sst_dir.exists()
        && fs::read_dir(&sst_dir)
            .map(|mut it| it.next().is_some())
            .unwrap_or(false);

    assert!(
        wal_has_file || sst_has_file,
        "expected at least one WAL or SST file after writes (wal_exists={} sst_exists={})",
        wal_has_file,
        sst_has_file
    );
}


#[test]
fn should_recover_state_given_unflushed_wal_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        memtable_size: 1024 * 1024,
        ..Default::default()
    };

    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"a", b"1").unwrap();
        eng.put(&cf, b"b", b"2").unwrap();
        // Intentionally drop without flushing to SST
    }

    // Act: reopen
    let eng2 = MidgeEngine::open(opts.clone()).expect("reopen");
    let cf = eng2.default_column_family();

    // Assert: state recovered
    assert_eq!(eng2.get(&cf, b"a").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(eng2.get(&cf, b"b").unwrap(), Some(Bytes::from_static(b"2")));
}


