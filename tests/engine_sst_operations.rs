// SST Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};
use cntryl_midge::test_hooks::{TestHooks, FlushGatePoint};

mod common;
use common::test_temp_dir;
#[test]
fn should_read_from_sst_after_reopen_when_memtable_has_no_key() {
    // Arrange: write a couple keys, then force WAL rotation to flush memtable -> SST
    let dir = test_temp_dir();
    let mut opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        wal_buffer_size: 64,
        memtable_size: 1024 * 1024,
        ..Default::default()
    };
    let hooks = TestHooks::new();
    opts.test_hooks = Some(hooks.clone());
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"a", b"1").unwrap();
        eng.put(&cf, b"b", b"2").unwrap();
        // Next put should rotate WAL due to tiny buffer; choose a larger value to be safe
        let big = vec![b'v'; 128];
        let gate = hooks.install_flush_gate(FlushGatePoint::BeforeManifestUpdate);
        // Use a write batch to predictably exceed WAL buffer size and force rotation
        let mut batch = cntryl_midge::WriteBatch::new();
        batch.put(cf.id(), Bytes::from("zz0"), Bytes::from(big.clone()));
        batch.put(cf.id(), Bytes::from("zz1"), Bytes::from(big.clone()));
        eng.write_batch(&batch).expect("write_batch");
        // Let the WAL rotation/background flush worker process the job and reach
        // the gate; do NOT call `eng.flush()` here, which performs a synchronous
        // foreground flush and bypasses the background worker (thus skipping the gate).
        assert!(gate.wait_until_blocked(std::time::Duration::from_secs(5)), "flush did not reach gate");
        gate.release();
        // Wait for the background flush to complete deterministically.
        eng.wait_for_flush(std::time::Duration::from_secs(5)).expect("flush should complete");
        // Verify initial SST contains expected values
        assert_eq!(eng.get(&cf, b"a").unwrap(), Some(Bytes::from_static(b"1")));
        assert_eq!(eng.get(&cf, b"b").unwrap(), Some(Bytes::from_static(b"2")));
    }

    // Act: reopen engine (memtable will only have post-rotation tail; 'a' should be in SST)
    let eng2 = MidgeEngine::open(opts.clone()).expect("reopen");

    // Assert: engine.get should fall back to SST when not found in memtable
    let cf2 = eng2.default_column_family();
    let got_a = eng2.get(&cf2, b"a").expect("get a from sst");
    let got_b = eng2.get(&cf2, b"b").expect("get b from sst");
    assert_eq!(got_a, Some(Bytes::from_static(b"1")));
    assert_eq!(got_b, Some(Bytes::from_static(b"2")));
}

#[test]
fn should_respect_tombstone_from_sst_when_point_lookup() {
    // Arrange: write k->v, rotate/flush, then delete and rotate/flush, so SST set has a tombstone
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 64; // force rotation
    opts.memtable_size = 1024 * 1024;
    let hooks = TestHooks::new();
    opts.test_hooks = Some(hooks.clone());
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"k", b"v1").unwrap();
        // Sanity: value visible in memtable before flush
        assert_eq!(eng.get(&cf, b"k").unwrap(), Some(Bytes::from_static(b"v1")));
        // rotate to flush first version
        let big = vec![b'v'; 128];
        let gate = hooks.install_flush_gate(FlushGatePoint::BeforeManifestUpdate);
        let mut batch = cntryl_midge::WriteBatch::new();
        batch.put(cf.id(), Bytes::from("zz3"), Bytes::from(big.clone()));
        batch.put(cf.id(), Bytes::from("zz4"), Bytes::from(big.clone()));
        eng.write_batch(&batch).expect("write_batch");
        assert!(gate.wait_until_blocked(std::time::Duration::from_secs(5)), "flush did not reach gate");
        gate.release();
        eng.wait_for_flush(std::time::Duration::from_secs(5)).expect("flush should complete");
        // Ensure a manifest update happened (SST persisted)
        assert!(hooks.manifest_update_count() > 0, "expected a manifest update after flush");
        // Verify initial SST contains v1
        let get_k = eng.get(&cf, b"k").unwrap();
        if get_k.is_none() {
            // Debug: manifest & sst contents when value not found in get
            let manifest = cntryl_midge::manifest::Manifest::load(&opts.storage_mode.local_path()).unwrap();
            println!("manifest ssts: {:?}", manifest.ssts);
            for s in manifest.ssts.iter() {
                let sst_path = opts
                    .storage_mode
                    .local_path()
                    .join("sst")
                    .join(s);
                if sst_path.exists() {
                    let sst = cntryl_midge::sst::fs::SstFile::open(&sst_path).unwrap();
                    let rows = cntryl_midge::sst::SstStateReader::scan_range_state(&sst, None, None).unwrap();
                    println!("sst {} rows: {:?}", s, rows);
                }
            }
        }
        assert_eq!(get_k, Some(Bytes::from_static(b"v1")));
        // delete and flush tombstone synchronously to ensure it persists
        eng.delete(&cf, b"k").unwrap();
        eng.flush().expect("flush tombstone");
    }

    // Act: reopen
    let eng2 = MidgeEngine::open(opts.clone()).expect("reopen");

    // Assert: engine should not resurrect deleted key; get returns None
    let cf2 = eng2.default_column_family();
    let got = eng2.get(&cf2, b"k").expect("get");
    assert_eq!(got, None);
}

#[test]
fn should_merge_memtable_ssts_with_last_write_wins_on_scan() {
    // Arrange: seed SST with a,b,c; then in memtable update b, delete c, add d.
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 64; // force rotation-based flush
    opts.memtable_size = 1024 * 1024; // avoid size-based flush
                                      // Phase 1: open with tiny WAL to force rotation and flush SST
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf1 = eng.default_column_family();
        eng.put(&cf1, b"a", b"1").unwrap();
        eng.put(&cf1, b"b", b"2").unwrap();
        eng.put(&cf1, b"c", b"3").unwrap();
        let big = vec![b'v'; 256];
        eng.put(&cf1, b"zz", big.as_slice()).unwrap();
        // Force and deterministically wait for flush
        eng.flush().expect("flush should complete");
    }
    // Phase 2: reopen with large WAL so overlay remains in memtable
    opts.wal_buffer_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("reopen");
    let cf = eng.default_column_family();
    eng.put(&cf, b"b", b"2'").unwrap();
    eng.delete(&cf, b"c").unwrap();
    eng.put(&cf, b"d", b"4").unwrap();

    // Act: scan full range
    let rows = eng
        .scan(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
        )
        .expect("scan");

    // Assert: a from SST; b updated in memtable; c deleted by memtable; d from memtable
    assert_eq!(
        rows,
        vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
            (Bytes::from_static(b"b"), Bytes::from_static(b"2'")),
            (Bytes::from_static(b"d"), Bytes::from_static(b"4")),
        ]
    );
}

#[test]
fn should_scan_by_prefix_limit_across_sst_memtable() {
    // Arrange: seed SST with a, ab, ac; then add ad in memtable
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 1024 * 1024;
    opts.memtable_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();

    eng.put(&cf, b"a", b"1").unwrap();
    eng.put(&cf, b"ab", b"2").unwrap();
    eng.put(&cf, b"ac", b"3").unwrap();
    eng.flush().unwrap(); // persist above to SST
                          // Now add a memtable overlay
    eng.put(&cf, b"ad", b"4").unwrap();

    // Act: prefix "a" should include a, ab, ac from SST and ad from memtable
    let rows = eng
        .scan(&cf, Query::new().prefix(Bytes::from_static(b"a")))
        .expect("scan");

    // Assert: full prefix returns 4 keys sorted
    let expected_full = vec![
        (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
        (Bytes::from_static(b"ab"), Bytes::from_static(b"2")),
        (Bytes::from_static(b"ac"), Bytes::from_static(b"3")),
        (Bytes::from_static(b"ad"), Bytes::from_static(b"4")),
    ];
    assert_eq!(rows, expected_full);
}

#[test]
fn should_scan_by_prefix_limit_across_sst_memtable_limited() {
    // Arrange: seed SST with a, ab, ac; then add ad in memtable (same setup as previous test)
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 1024 * 1024;
    opts.memtable_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();

    eng.put(&cf, b"a", b"1").unwrap();
    eng.put(&cf, b"ab", b"2").unwrap();
    eng.put(&cf, b"ac", b"3").unwrap();
    eng.flush().unwrap(); // persist above to SST
    eng.put(&cf, b"ad", b"4").unwrap();

    // Act: limited prefix scan (limit 3)
    let rows_limited = eng
        .scan(&cf, Query::new().prefix(Bytes::from_static(b"a")).limit(3))
        .expect("scan");

    // Assert: limited returns first 3 keys
    assert_eq!(rows_limited.len(), 3);
    assert_eq!(rows_limited[0].0, Bytes::from_static(b"a"));
    assert_eq!(rows_limited[1].0, Bytes::from_static(b"ab"));
    assert_eq!(rows_limited[2].0, Bytes::from_static(b"ac"));
}

#[test]
fn should_return_sst_value_at_snapshot_when_memtable_has_newer() {
    // Arrange: write k->v1, flush to SST, snapshot, then write k->v2 in memtable
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    // Large wal buffer to avoid rotation; we'll use explicit flush
    opts.wal_buffer_size = 1024 * 1024;
    opts.memtable_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();

    eng.put(&cf, b"k", b"v1").unwrap();
    // Flush so v1 is persisted to SST
    eng.flush().unwrap();
    let snap = eng.snapshot();
    let manifest = cntryl_midge::manifest::Manifest::load(&opts.storage_mode.local_path()).unwrap();
    tracing::debug!("manifest ssts after flush: {:?}", manifest.ssts);
    let sst_path = opts
        .storage_mode
        .local_path()
        .join("sst")
        .join(&manifest.ssts[0]);
    let sst = cntryl_midge::sst::fs::SstFile::open(&sst_path).unwrap();
    let rows = cntryl_midge::sst::SstStateReader::scan_range_state(&sst, None, None).unwrap();
    tracing::debug!("sst rows: {:?}", rows);
    tracing::debug!("snapshot seq={} ", snap.seq);
    // Newer write stays in memtable with higher seq
    eng.put(&cf, b"k", b"v2").unwrap();

    // Act: get_at and full get
    let at = eng.get_at(&cf, b"k", &snap).unwrap();
    let full = eng.get(&cf, b"k").unwrap();

    // Assert: snapshot sees v1 from SST; latest sees v2 from memtable
    assert_eq!(at, Some(Bytes::from_static(b"v1")));
    assert_eq!(full, Some(Bytes::from_static(b"v2")));

    // Act: scan_at separately to verify snapshot-scoped scan behavior
    let rows_at = eng
        .scan_at(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
            &snap,
        )
        .unwrap();

    // Assert: scan_at returns the snapshot value
    assert_eq!(
        rows_at,
        vec![(Bytes::from_static(b"k"), Bytes::from_static(b"v1"))]
    );
}

#[test]
fn should_scan_reverse_respects_tombstones() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Write and delete keys
    eng.put(&cf, b"k1", b"v1").expect("put");
    eng.put(&cf, b"k2", b"v2").expect("put");
    eng.put(&cf, b"k3", b"v3").expect("put");
    eng.delete(&cf, b"k2").expect("delete");

    // Act: Reverse scan
    let results = eng.scan(&cf, Query::new().reverse()).expect("scan");

    // Assert: k2 should be masked by tombstone
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, Bytes::from_static(b"k3"));
    assert_eq!(results[0].1, Bytes::from_static(b"v3"));
    assert_eq!(results[1].0, Bytes::from_static(b"k1"));
    assert_eq!(results[1].1, Bytes::from_static(b"v1"));
}

// ============================================================================
// Insert-if-not-exists and CAS tests (consolidated from tests/insert.rs)
// ============================================================================
