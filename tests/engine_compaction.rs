// Compaction Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::test_hooks::TestHooks;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

mod common;
use common::test_helpers::TEST_GATE_TIMEOUT;
use common::test_temp_dir;
#[test]
fn should_compact_all_merge_newest_drop_tombstones() {
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
        eng.flush().expect("flush should complete");
        // SST2: b=2', c=3
        eng.put(&cf, b"b", b"2' ").unwrap();
        eng.put(&cf, b"zz2", &vec![b'x'; 256]).unwrap();
        eng.flush().expect("flush should complete");
        // SST3: delete a
        eng.delete(&cf, b"a").unwrap();
        eng.put(&cf, b"zz3", &vec![b'x'; 256]).unwrap();
        eng.flush().expect("flush should complete");
        // leave eng in scope to ensure flush thread has time
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
    let hooks = TestHooks::new();
    opts.test_hooks = Some(hooks.clone());
    let hooks = TestHooks::new();
    opts.test_hooks = Some(hooks.clone());
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

    // Arrange: Disable compaction during setup to prevent race conditions
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false; // Disable during setup
    opts.wal_buffer_size = 64;
    opts.memtable_size = 1024;
    let hooks = TestHooks::new();
    opts.test_hooks = Some(hooks.clone());

    // Create 4 SSTs with overlapping keys so compaction can merge them
    // The strategy compacts all L0 sublevels when file count >= 4
    let mut manifest_updates = 0u64;
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
        eng.flush().expect("flush should complete");
        drop(eng);
        // Wait briefly until the SST/manifest shows the expected files for this iteration
        // Deterministically assert the manifest was updated at least once this iteration
        assert!(
            hooks.wait_for_manifest_update(manifest_updates, TEST_GATE_TIMEOUT),
            "Expected manifest update after flush; prior_updates={}, new_updates={}  iteration={}",
            manifest_updates,
            hooks.manifest_update_count(),
            i
        );
        manifest_updates = hooks.manifest_update_count();
    }

    // Verify all 4 SST files were created before starting background compaction
    {
        let manifest_before =
            cntryl_midge::manifest::Manifest::load(&opts.storage_mode.local_path())
                .expect("load manifest");
        assert_eq!(
            manifest_before.ssts.len(),
            4,
            "Expected 4 SST files before compaction"
        );
    }

    // Now enable compaction for the final engine instance
    opts.enable_compaction = true;
    opts.compaction_sst_threshold = 1;
    opts.compaction_check_interval_ms = 50;

    // Act - Open engine with background compaction enabled
    {
        let _eng = MidgeEngine::open(opts.clone()).expect("open for background compaction");

        // Background compaction runs every 50ms. Wait up to 10 seconds for compaction to run.
        // Use engine's wait_for_compaction helper instead of manual polling.
        if _eng.wait_for_compaction(TEST_GATE_TIMEOUT).is_err() {
            tracing::warn!(
                "Timeout: compaction did not complete within 10 seconds (manifest may not be reduced)"
            );
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
