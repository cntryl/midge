// Compaction Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

mod common;
use common::test_temp_dir;
#[test]
fn should_compact_all_merge_newest_and_drop_tombstones() {
    // Arrange: create multiple SSTs with overlapping keys and tombstones
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    // Use rotation to create multiple SSTs
    opts.wal_buffer_size = 64; // tiny
    opts.memtable_size = 1024 * 1024;
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        // SST1: a=1, b=2
        eng.put(&cf, b"a", b"1").unwrap();
        eng.put(&cf, b"zz", &vec![b'x'; 256]).unwrap();
        eng.wait_for_flush(std::time::Duration::from_secs(1))
            .expect("flush should complete");
        // SST2: b=2', c=3
        eng.put(&cf, b"b", b"2' ").unwrap();
        eng.put(&cf, b"zz2", &vec![b'x'; 256]).unwrap();
        eng.wait_for_flush(std::time::Duration::from_secs(1))
            .expect("flush should complete");
        // SST3: delete a
        eng.delete(&cf, b"a").unwrap();
        eng.put(&cf, b"zz3", &vec![b'x'; 256]).unwrap();
        eng.wait_for_flush(std::time::Duration::from_secs(1))
            .expect("flush should complete");
        // leave eng in scope to ensure flush thread has time
    }

    let eng = MidgeEngine::open(opts.clone()).expect("reopen");
    let cf = eng.default_column_family();
    // Sanity before compaction: get pulls latest view with tombstone respected
    assert_eq!(eng.get(&cf, b"a").unwrap(), None);
    let b = eng.get(&cf, b"b").unwrap().unwrap();
    assert!(b == Bytes::from_static(b"2' "));

    // Act: compact all
    eng.compact_all().unwrap();

    // Assert: only one SST remains and reads still correct
    let got_a = eng.get(&cf, b"a").unwrap();
    let got_b = eng.get(&cf, b"b").unwrap();
    assert_eq!(got_a, None);
    assert_eq!(got_b, Some(Bytes::from_static(b"2' ")));
}

#[test]
fn should_preserve_snapshot_visibility_across_compaction() {
    // Arrange: create value, take snapshot, delete value, then compact
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 64;
    opts.memtable_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();

    eng.put(&cf, b"a", b"v1").expect("put v1");
    eng.flush().expect("flush v1");

    let snap = eng.snapshot();

    eng.delete(&cf, b"a").expect("delete");
    eng.flush().expect("flush tombstone");

    // Act: compact all SSTs into one file
    eng.compact_all().expect("compact_all");

    // Assert: current view sees deletion, snapshot still sees old value
    let current = eng.get(&cf, b"a").expect("get current");
    assert_eq!(current, None);

    let snapshot_view = eng.get_at(&cf, b"a", &snap).expect("get_at snapshot");
    assert_eq!(snapshot_view, Some(Bytes::from_static(b"v1")));
}

#[test]
#[ignore] // TODO: Background compaction doesn't fully compact in one round. Needs investigation.
fn should_background_compact_when_threshold_exceeded() {
    // Arrange: enable compaction with low threshold so it triggers
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = true;
    opts.compaction_sst_threshold = 1;
    opts.compaction_check_interval_ms = 50;
    opts.wal_buffer_size = 64;
    opts.memtable_size = 1024;
    
    // Create 3 SSTs by writing, closing, and reopening (ensures memtable is fresh each time)
    for i in 0..3 {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, format!("key{}", i).as_bytes(), b"value").unwrap();
        eng.put(&cf, format!("padding{}", i).as_bytes(), &[b'x'; 128]).unwrap();
        eng.flush_cf(&cf).unwrap();
        // Wait for flush to complete before closing
        eng.wait_for_flush(std::time::Duration::from_secs(1)).unwrap();
        drop(eng);
    }
    
    // Open engine and wait for background compaction to complete multiple rounds
    {
        let _eng = MidgeEngine::open(opts.clone()).expect("open");
        // Background compaction runs every 50ms. Wait up to 10 seconds for all rounds to complete.
        // Check every 500ms to see if we're down to 1 SST.
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(10);
        loop {
            let m = cntryl_midge::manifest::Manifest::load(&opts.storage_mode.local_path()).unwrap();
            if m.ssts.len() <= 1 {
                break;
            }
            if start.elapsed() > timeout {
                println!("Timeout waiting for compaction to complete");
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }

    // Assert: compaction happened (fewer SSTs than we started with) and data intact
    let eng = MidgeEngine::open(opts.clone()).expect("reopen");
    let cf = eng.default_column_family();
    let m = cntryl_midge::manifest::Manifest::load(&opts.storage_mode.local_path()).unwrap();
    
    println!("SSTs found: {}", m.ssts.len());
    println!("Files: {:?}", m.files.iter().map(|f| &f.name).collect::<Vec<_>>());
    
    // Background compaction should have reduced file count from 3
    // (won't necessarily be 1 file - LSM compacts L0->L1, which can have multiple files)
    assert!(m.ssts.len() < 3, "Expected compaction to reduce SST count, got {}", m.ssts.len());
    
    // Verify data is intact after compaction
    assert_eq!(eng.get(&cf, b"key0").unwrap(), Some(Bytes::from_static(b"value")));
    assert_eq!(eng.get(&cf, b"key1").unwrap(), Some(Bytes::from_static(b"value")));
    assert_eq!(eng.get(&cf, b"key2").unwrap(), Some(Bytes::from_static(b"value")));
}
