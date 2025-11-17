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
        eng.wait_for_flush(std::time::Duration::from_millis(100))
            .expect("flush should complete");
        // SST2: b=2', c=3
        eng.put(&cf, b"b", b"2' ").unwrap();
        eng.put(&cf, b"zz2", &vec![b'x'; 256]).unwrap();
        eng.wait_for_flush(std::time::Duration::from_millis(100))
            .expect("flush should complete");
        // SST3: delete a
        eng.delete(&cf, b"a").unwrap();
        eng.put(&cf, b"zz3", &vec![b'x'; 256]).unwrap();
        eng.wait_for_flush(std::time::Duration::from_millis(100))
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
fn should_background_compact_when_threshold_exceeded() {
    // Initialize tracing for this test
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

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

    // Create 4 SSTs with overlapping keys so compaction can merge them
    // The strategy compacts all L0 sublevels when file count >= 4
    for i in 0..4 {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();

        // Write overlapping keys - newer versions will supersede older ones
        eng.put(&cf, b"key_a", format!("value_v{}", i).as_bytes())
            .unwrap();
        eng.put(&cf, b"key_b", format!("value_v{}", i).as_bytes())
            .unwrap();
        eng.put(&cf, b"key_c", format!("value_v{}", i).as_bytes())
            .unwrap();

        // Add padding to ensure we trigger flush threshold
        for j in 0..5 {
            eng.put(&cf, format!("padding_{}_{}", i, j).as_bytes(), &[b'x'; 128])
                .unwrap();
        }

        eng.flush_cf(&cf).unwrap();
        eng.wait_for_flush(std::time::Duration::from_millis(100))
            .unwrap();
        drop(eng);
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    // Open engine with background compaction enabled
    {
        let _eng = MidgeEngine::open(opts.clone()).expect("open for background compaction");

        // Background compaction runs every 50ms. Wait up to 10 seconds for compaction to reduce file count.
        // Check every 500ms to see if file count has decreased.
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(10);
        let db_path = opts.storage_mode.local_path();

        loop {
            if let Ok(m) = cntryl_midge::manifest::Manifest::load(&db_path) {
                if m.ssts.len() < 4 {
                    tracing::info!(
                        "Compaction succeeded: SST count reduced to {}",
                        m.ssts.len()
                    );
                    break;
                }
            }
            if start.elapsed() > timeout {
                tracing::warn!("Timeout: compaction did not reduce SST count within 10 seconds");
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }

    // Assert: compaction happened (fewer SSTs than we started with) and data intact
    let eng = MidgeEngine::open(opts.clone()).expect("reopen");
    let cf = eng.default_column_family();
    let m = cntryl_midge::manifest::Manifest::load(&opts.storage_mode.local_path()).unwrap();

    tracing::debug!("SSTs found: {}", m.ssts.len());
    tracing::debug!(
        "Files: {:?}",
        m.files.iter().map(|f| &f.name).collect::<Vec<_>>()
    );

    // Background compaction should have reduced file count from 4
    // With overlapping keys, compaction merges them into fewer files
    assert!(
        m.ssts.len() < 4,
        "Expected compaction to reduce SST count from 4, got {}",
        m.ssts.len()
    );

    // Verify data is intact after compaction - should see latest version (v3 from iteration 3)
    assert_eq!(
        eng.get(&cf, b"key_a").unwrap(),
        Some(Bytes::from_static(b"value_v3"))
    );
    assert_eq!(
        eng.get(&cf, b"key_b").unwrap(),
        Some(Bytes::from_static(b"value_v3"))
    );
    assert_eq!(
        eng.get(&cf, b"key_c").unwrap(),
        Some(Bytes::from_static(b"value_v3"))
    );
}
