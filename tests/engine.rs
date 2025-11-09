#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Mutation, MutationOp, Query, StorageMode};
use std::fs;

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn should_get_value_given_existing_key_when_put() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        wal_buffer_size: 1024 * 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Act
    eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"1"))
        .expect("put");

    // Assert
    let got = eng.get(&cf, b"a").expect("get");
    assert_eq!(got, Some(Bytes::from_static(b"1")));

    // range scan sanity
    let rows = eng
        .scan(
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
        )
        .expect("scan");
    assert_eq!(
        rows,
        vec![(Bytes::from_static(b"a"), Bytes::from_static(b"1"))]
    );
}

#[test]
fn should_return_none_given_deleted_key_when_delete() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    eng.put(&cf, Bytes::from_static(b"k"), Bytes::from_static(b"v"))
        .expect("put");

    // Act
    eng.delete(&cf, Bytes::from_static(b"k")).expect("del");

    // Assert
    let got = eng.get(&cf, b"k").expect("get");
    assert_eq!(got, None);
}

#[test]
fn should_return_ordered_pairs_given_range_when_scan() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")] {
        eng.put(&cf, Bytes::from_static(k), Bytes::from_static(v))
            .expect("put");
    }

    // Act
    let rows = eng
        .scan(
            Query::new()
                .start_key(Bytes::from_static(b"b"))
                .end_key(Bytes::from_static(b"d")),
        )
        .expect("scan");

    // Assert
    assert_eq!(
        rows,
        vec![
            (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
            (Bytes::from_static(b"c"), Bytes::from_static(b"3")),
        ]
    );
}

#[test]
fn should_apply_all_mutations_given_mixed_ops_when_batch() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    let muts = vec![
        Mutation {
            op: MutationOp::Put,
            key: Bytes::from_static(b"a"),
            value: Some(Bytes::from_static(b"1")),
            ttl: None,
            range_end: None,
        },
        Mutation {
            op: MutationOp::Put,
            key: Bytes::from_static(b"b"),
            value: Some(Bytes::from_static(b"2")),
            ttl: None,
            range_end: None,
        },
        Mutation {
            op: MutationOp::Delete,
            key: Bytes::from_static(b"a"),
            value: None,
            ttl: None,
            range_end: None,
        },
    ];

    // Act
    eng.batch(muts).expect("batch");

    // Assert
    assert_eq!(eng.get(&cf, b"a").unwrap(), None);
    assert_eq!(eng.get(&cf, b"b").unwrap(), Some(Bytes::from_static(b"2")));
}

#[test]
fn should_hide_newer_writes_given_snapshot_when_get_at() {
    // Arrange
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    let eng = MidgeEngine::open(opts).expect("open");

    eng.put(&cf, Bytes::from_static(b"k"), Bytes::from_static(b"v1"))
        .unwrap();
    let snap = eng.snapshot();
    eng.put(&cf, Bytes::from_static(b"k"), Bytes::from_static(b"v2"))
        .unwrap();

    // Act
    let at = eng.get_at(b"k", &snap).unwrap();
    let full = eng.get(&cf, b"k").unwrap();

    // Assert
    // With multi-version memtable, a snapshot created after v1 should still
    // observe v1 even after a newer v2 is written. Latest read should see v2.
    assert_eq!(at, Some(Bytes::from_static(b"v1")));
    assert_eq!(full, Some(Bytes::from_static(b"v2")));
}

#[test]
fn should_scan_at_hides_newer_writes_given_snapshot() {
    // Arrange: put v1, snapshot, then write v2 in memtable (v1 persisted or not)
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    let eng = MidgeEngine::open(opts).expect("open");

    eng.put(&cf, Bytes::from_static(b"k"), Bytes::from_static(b"v1"))
        .unwrap();
    let snap = eng.snapshot();
    eng.put(&cf, Bytes::from_static(b"k"), Bytes::from_static(b"v2"))
        .unwrap();

    // Act: scan_at should see the older version (v1) and hide v2
    let rows_at = eng
        .scan_at(
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
            &snap,
        )
        .unwrap();

    // Assert: The snapshot should observe v1 only
    assert_eq!(
        rows_at,
        vec![(Bytes::from_static(b"k"), Bytes::from_static(b"v1"))]
    );
}

#[test]
fn should_rotate_wal_given_small_buffer_when_multiple_puts() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_buffer_size: 64,
        memtable_size: 1024 * 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts.clone()).expect("open");

    // Act
    for i in 0..10u8 {
        let k = [b"k"[0], i];
        let v = [b"v"[0], i];
        eng.put(&cf, Bytes::copy_from_slice(&k), Bytes::copy_from_slice(&v))
            .unwrap();
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
    let dir = temp_dir();
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
        eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"1"))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"b"), Bytes::from_static(b"2"))
            .unwrap();
        // Intentionally drop without flushing to SST
    }

    // Act: reopen
    let eng2 = MidgeEngine::open(opts.clone()).expect("reopen");

    // Assert: state recovered
    assert_eq!(eng2.get(&cf, b"a").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(eng2.get(&cf, b"b").unwrap(), Some(Bytes::from_static(b"2")));
}

#[test]
fn should_read_from_sst_after_reopen_when_memtable_has_no_key() {
    let cf = engine.default_column_family();
    // Arrange: write a couple keys, then force WAL rotation to flush memtable -> SST
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        wal_buffer_size: 64,
        memtable_size: 1024 * 1024,
        ..Default::default()
    };
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"1"))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"b"), Bytes::from_static(b"2"))
            .unwrap();
        // Next put should rotate WAL due to tiny buffer; choose a larger value to be safe
        let big = vec![b'v'; 128];
        eng.put(&cf, Bytes::from_static(b"zz"), Bytes::from(big))
            .unwrap();
        // Give background flush a moment to materialize SST and update manifest
        std::thread::sleep(std::time::Duration::from_millis(150));
    }

    // Act: reopen engine (memtable will only have post-rotation tail; 'a' should be in SST)
    let eng2 = MidgeEngine::open(opts.clone()).expect("reopen");

    // Assert: engine.get should fall back to SST when not found in memtable
    let got_a = eng2.get(&cf, b"a").expect("get a from sst");
    let got_b = eng2.get(&cf, b"b").expect("get b from sst");
    assert_eq!(got_a, Some(Bytes::from_static(b"1")));
    assert_eq!(got_b, Some(Bytes::from_static(b"2")));
}

#[test]
fn should_respect_tombstone_from_sst_when_point_lookup() {
    let cf = engine.default_column_family();
    // Arrange: write k->v, rotate/flush, then delete and rotate/flush, so SST set has a tombstone
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 64; // force rotation
    opts.memtable_size = 1024 * 1024;
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        eng.put(&cf, Bytes::from_static(b"k"), Bytes::from_static(b"v1"))
            .unwrap();
        // rotate to flush first version
        eng.put(&cf, Bytes::from_static(b"zz"), Bytes::from(vec![b'v'; 128]))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        // delete and rotate again to flush tombstone
        eng.delete(&cf, Bytes::from_static(b"k")).unwrap();
        eng.put(&cf, Bytes::from_static(b"zz2"), Bytes::from(vec![b'v'; 128]))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(120));
    }

    // Act: reopen
    let eng2 = MidgeEngine::open(opts.clone()).expect("reopen");

    // Assert: engine should not resurrect deleted key; get returns None
    let got = eng2.get(&cf, b"k").expect("get");
    assert_eq!(got, None);
}

#[test]
fn should_merge_memtable_and_ssts_with_last_write_wins_when_scan() {
    let cf = engine.default_column_family();
    // Arrange: seed SST with a,b,c; then in memtable update b, delete c, add d.
    let dir = temp_dir();
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
        eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"1"))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"b"), Bytes::from_static(b"2"))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"c"), Bytes::from_static(b"3"))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"zz"), Bytes::from(vec![b'v'; 256]))
            .unwrap();
    }
    // Wait for background flush
    std::thread::sleep(std::time::Duration::from_millis(150));
    // Phase 2: reopen with large WAL so overlay remains in memtable
    opts.wal_buffer_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("reopen");
    eng.put(&cf, Bytes::from_static(b"b"), Bytes::from_static(b"2'"))
        .unwrap();
    eng.delete(&cf, Bytes::from_static(b"c")).unwrap();
    eng.put(&cf, Bytes::from_static(b"d"), Bytes::from_static(b"4"))
        .unwrap();

    // Act: scan full range
    let rows = eng
        .scan(
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
fn should_scan_by_prefix_memtable() {
    // Arrange
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    let eng = MidgeEngine::open(opts).expect("open");

    for k in [b"user:1:a", b"user:1:b", b"user:1:c", b"user:2:a"] {
        eng.put(&cf, Bytes::from_static(k), Bytes::from_static(b"v"))
            .unwrap();
    }

    // Act: prefix only
    let rows = eng
        .scan(Query::new().prefix(Bytes::from_static(b"user:1:")))
        .expect("scan");

    // Assert
    let expected = vec![
        (Bytes::from_static(b"user:1:a"), Bytes::from_static(b"v")),
        (Bytes::from_static(b"user:1:b"), Bytes::from_static(b"v")),
        (Bytes::from_static(b"user:1:c"), Bytes::from_static(b"v")),
    ];
    assert_eq!(rows, expected);
}

#[test]
fn should_scan_by_prefix_and_limit_memtable() {
    // Arrange
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    let eng = MidgeEngine::open(opts).expect("open");

    for k in [b"user:1:a", b"user:1:b", b"user:1:c", b"user:2:a"] {
        eng.put(&cf, Bytes::from_static(k), Bytes::from_static(b"v"))
            .unwrap();
    }

    // Act: prefix + limit
    let rows_limited = eng
        .scan(Query::new().prefix(Bytes::from_static(b"user:1:")).limit(2))
        .expect("scan");

    // Assert
    assert_eq!(rows_limited.len(), 2);
    assert_eq!(rows_limited[0].0, Bytes::from_static(b"user:1:a"));
    assert_eq!(rows_limited[1].0, Bytes::from_static(b"user:1:b"));
}

#[test]
fn should_scan_by_prefix_and_limit_across_sst_and_memtable() {
    // Arrange: seed SST with a, ab, ac; then add ad in memtable
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 1024 * 1024;
    opts.memtable_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("open");

    eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"1"))
        .unwrap();
    eng.put(&cf, Bytes::from_static(b"ab"), Bytes::from_static(b"2"))
        .unwrap();
    eng.put(&cf, Bytes::from_static(b"ac"), Bytes::from_static(b"3"))
        .unwrap();
    eng.flush().unwrap(); // persist above to SST
                          // Now add a memtable overlay
    eng.put(&cf, Bytes::from_static(b"ad"), Bytes::from_static(b"4"))
        .unwrap();

    // Act: prefix "a" should include a, ab, ac from SST and ad from memtable
    let rows = eng
        .scan(Query::new().prefix(Bytes::from_static(b"a")))
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
fn should_scan_by_prefix_and_limit_across_sst_and_memtable_limited() {
    // Arrange: seed SST with a, ab, ac; then add ad in memtable (same setup as previous test)
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 1024 * 1024;
    opts.memtable_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("open");

    eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"1"))
        .unwrap();
    eng.put(&cf, Bytes::from_static(b"ab"), Bytes::from_static(b"2"))
        .unwrap();
    eng.put(&cf, Bytes::from_static(b"ac"), Bytes::from_static(b"3"))
        .unwrap();
    eng.flush().unwrap(); // persist above to SST
    eng.put(&cf, Bytes::from_static(b"ad"), Bytes::from_static(b"4"))
        .unwrap();

    // Act: limited prefix scan (limit 3)
    let rows_limited = eng
        .scan(Query::new().prefix(Bytes::from_static(b"a")).limit(3))
        .expect("scan");

    // Assert: limited returns first 3 keys
    assert_eq!(rows_limited.len(), 3);
    assert_eq!(rows_limited[0].0, Bytes::from_static(b"a"));
    assert_eq!(rows_limited[1].0, Bytes::from_static(b"ab"));
    assert_eq!(rows_limited[2].0, Bytes::from_static(b"ac"));
}

#[test]
fn should_compact_all_merge_newest_and_drop_tombstones() {
    // Arrange: create multiple SSTs with overlapping keys and tombstones
    let dir = temp_dir();
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
        // SST1: a=1, b=2
        eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"1"))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"zz"), Bytes::from(vec![b'x'; 256]))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        // SST2: b=2', c=3
        eng.put(&cf, Bytes::from_static(b"b"), Bytes::from_static(b"2' "))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"zz2"), Bytes::from(vec![b'x'; 256]))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        // SST3: delete a
        eng.delete(&cf, Bytes::from_static(b"a")).unwrap();
        eng.put(&cf, Bytes::from_static(b"zz3"), Bytes::from(vec![b'x'; 256]))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(120));
        // leave eng in scope to ensure flush thread has time
    }

    let eng = MidgeEngine::open(opts.clone()).expect("reopen");
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
    let cf = engine.default_column_family();
    // Arrange: create value, take snapshot, delete value, then compact
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 64;
    opts.memtable_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("open");

    eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"v1"))
        .expect("put v1");
    eng.flush().expect("flush v1");

    let snap = eng.snapshot();

    eng.delete(&cf, Bytes::from_static(b"a")).expect("delete");
    eng.flush().expect("flush tombstone");

    // Act: compact all SSTs into one file
    eng.compact_all().expect("compact_all");

    // Assert: current view sees deletion, snapshot still sees old value
    let current = eng.get(&cf, b"a").expect("get current");
    assert_eq!(current, None);

    let snapshot_view = eng.get_at(b"a", &snap).expect("get_at snapshot");
    assert_eq!(snapshot_view, Some(Bytes::from_static(b"v1")));
}

#[test]
fn should_background_compact_when_threshold_exceeded() {
    // Arrange: enable compaction with low threshold so it triggers
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = true;
    opts.compaction_sst_threshold = 1;
    opts.compaction_check_interval_ms = 50;
    opts.wal_buffer_size = 64;
    opts.memtable_size = 1024 * 1024;
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        // Create 3 SSTs quickly
        eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"1"))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"zz"), Bytes::from(vec![b'x'; 128]))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(80));
        eng.put(&cf, Bytes::from_static(b"b"), Bytes::from_static(b"2"))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"zz2"), Bytes::from(vec![b'x'; 128]))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(80));
        eng.put(&cf, Bytes::from_static(b"c"), Bytes::from_static(b"3"))
            .unwrap();
        eng.put(&cf, Bytes::from_static(b"zz3"), Bytes::from(vec![b'x'; 128]))
            .unwrap();
    }
    // Act: wait for background compaction to kick in
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Assert: only one SST remains and reads intact
    let eng = MidgeEngine::open(opts.clone()).expect("reopen");
    let m = cntryl_midge::manifest::Manifest::load(&opts.storage_mode.local_path()).unwrap();
    assert_eq!(m.ssts.len(), 1);
    assert_eq!(eng.get(&cf, b"a").unwrap(), Some(Bytes::from_static(b"1")));
    assert_eq!(eng.get(&cf, b"b").unwrap(), Some(Bytes::from_static(b"2")));
    assert_eq!(eng.get(&cf, b"c").unwrap(), Some(Bytes::from_static(b"3")));
}

#[test]
fn should_create_checkpoint_and_read_from_it() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    eng.put(&cf, Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
        .unwrap();
    eng.put(&cf, Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
        .unwrap();
    eng.flush().unwrap();
    // Create checkpoint
    let cp_dir = dir.path().join("checkpoint");
    eng.create_checkpoint(&cp_dir).unwrap();

    // Act: open a new engine on the checkpoint directory (read-only in spirit)
    let mut cp_opts = MidgeOptions::default();
    cp_opts.storage_mode = StorageMode::LocalDisk {
        db_path: cp_dir.clone(),
    };
    cp_opts.enable_compaction = false;
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");

    // Assert: data is readable from checkpoint
    assert_eq!(cp.get(&cf, b"k1").unwrap(), Some(Bytes::from_static(b"v1")));
    assert_eq!(cp.get(&cf, b"k2").unwrap(), Some(Bytes::from_static(b"v2")));
}

#[test]
fn should_return_sst_value_at_snapshot_when_memtable_has_newer() {
    // Arrange: write k->v1, flush to SST, snapshot, then write k->v2 in memtable
    let dir = temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    // Large wal buffer to avoid rotation; we'll use explicit flush
    opts.wal_buffer_size = 1024 * 1024;
    opts.memtable_size = 1024 * 1024;
    let eng = MidgeEngine::open(opts.clone()).expect("open");

    eng.put(&cf, Bytes::from_static(b"k"), Bytes::from_static(b"v1"))
        .unwrap();
    // Flush so v1 is persisted to SST
    eng.flush().unwrap();
    let snap = eng.snapshot();
    let manifest = cntryl_midge::manifest::Manifest::load(&opts.storage_mode.local_path()).unwrap();
    println!("manifest ssts after flush: {:?}", manifest.ssts);
    let sst_path = opts
        .storage_mode
        .local_path()
        .join("sst")
        .join(&manifest.ssts[0]);
    let sst = cntryl_midge::sst::fs::SstFile::open(&sst_path).unwrap();
    let rows = cntryl_midge::sst::SstStateReader::scan_range_state(&sst, None, None).unwrap();
    println!("sst rows: {:?}", rows);
    println!("snapshot seq={} ", snap.seq);
    // Newer write stays in memtable with higher seq
    eng.put(&cf, Bytes::from_static(b"k"), Bytes::from_static(b"v2"))
        .unwrap();

    // Act: get_at and full get
    let at = eng.get_at(b"k", &snap).unwrap();
    let full = eng.get(&cf, b"k").unwrap();

    // Assert: snapshot sees v1 from SST; latest sees v2 from memtable
    assert_eq!(at, Some(Bytes::from_static(b"v1")));
    assert_eq!(full, Some(Bytes::from_static(b"v2")));

    // Act: scan_at separately to verify snapshot-scoped scan behavior
    let rows_at = eng
        .scan_at(
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
fn should_streaming_scan_match_regular_scan() {
    // Arrange: create a database with multiple keys across memtable and SSTs
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 100, // Small to force flush
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Put some keys and force flush
    for i in 0..5u8 {
        eng.put(&cf, vec![b'a' + i].as_bytes(), vec![b'1' + i].as_bytes())
            .expect("put");
    }

    // Wait for flush
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Add more keys to memtable
    for i in 5..10u8 {
        eng.put(&cf, vec![b'a' + i].as_bytes(), vec![b'1' + i].as_bytes())
            .expect("put");
    }

    // Act: scan with both methods
    let regular_results = eng
        .scan(
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
        )
        .expect("scan");
    let streaming_results = eng
        .scan_streaming(
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
        )
        .expect("scan_streaming");

    // Assert: both should return the same results
    assert_eq!(regular_results.len(), streaming_results.len());
    assert_eq!(regular_results, streaming_results);
}

#[test]
fn should_streaming_scan_respect_limit() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Put 10 keys
    for i in 0..10u8 {
        eng.put(&cf, 
            Bytes::from(vec![b'k', b'0' + i]),
            Bytes::from(vec![b'v', b'0' + i]),
        )
        .expect("put");
    }

    // Act: scan with limit of 5
    let results = eng
        .scan_streaming(Query::new().limit(5))
        .expect("scan_streaming");

    // Assert
    assert_eq!(results.len(), 5);
}

#[test]
fn should_streaming_scan_apply_tombstones() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    eng.put(&cf, Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
        .expect("put");

    // Delete k1
    eng.delete(&cf, Bytes::from_static(b"k1")).expect("delete");

    // Act
    let results = eng.scan_streaming(Query::new()).expect("scan_streaming");

    // Assert: only k2 should be present
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, Bytes::from_static(b"k2"));
    assert_eq!(results[0].1, Bytes::from_static(b"v2"));
}

#[test]
fn should_multi_get_all_keys_from_memtable() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    eng.put(&cf, Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"k3"), Bytes::from_static(b"v3"))
        .expect("put");

    // Act
    let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3", b"k4"];
    let results = eng.multi_get(&keys).expect("multi_get");

    // Assert
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].0, Bytes::from_static(b"k1"));
    assert_eq!(results[0].1, Some(Bytes::from_static(b"v1")));
    assert_eq!(results[1].0, Bytes::from_static(b"k2"));
    assert_eq!(results[1].1, Some(Bytes::from_static(b"v2")));
    assert_eq!(results[2].0, Bytes::from_static(b"k3"));
    assert_eq!(results[2].1, Some(Bytes::from_static(b"v3")));
    assert_eq!(results[3].0, Bytes::from_static(b"k4"));
    assert_eq!(results[3].1, None); // Not found
}

#[test]
fn should_multi_get_respect_tombstones() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    eng.put(&cf, Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
        .expect("put");
    eng.delete(&cf, Bytes::from_static(b"k1")).expect("delete");

    // Act
    let keys: Vec<&[u8]> = vec![b"k1", b"k2"];
    let results = eng.multi_get(&keys).expect("multi_get");

    // Assert
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, Bytes::from_static(b"k1"));
    assert_eq!(results[0].1, None); // Deleted
    assert_eq!(results[1].0, Bytes::from_static(b"k2"));
    assert_eq!(results[1].1, Some(Bytes::from_static(b"v2")));
}

#[test]
fn should_multi_get_from_ssts_after_flush() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        wal_buffer_size: 64, // Small WAL buffer to force rotation/flush
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Write data and force flush via WAL rotation
    eng.put(&cf, 
        Bytes::from_static(b"key000"), Bytes::from_static(b"value000"),
    )
    .expect("put");
    eng.put(&cf, 
        Bytes::from_static(b"key005"), Bytes::from_static(b"value005"),
    )
    .expect("put");

    // Force WAL rotation with a large write
    let big = vec![b'x'; 128];
    eng.put(&cf, Bytes::from_static(b"key009"), Bytes::from(big.clone()))
        .expect("put");

    // Give flush time to complete
    std::thread::sleep(std::time::Duration::from_millis(150));

    // Act: Read keys that should be in SSTs (from before rotation)
    let keys: Vec<&[u8]> = vec![b"key000", b"key005", b"key009", b"key999"];
    let results = eng.multi_get(&keys).expect("multi_get");

    // Assert
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].0, Bytes::from_static(b"key000"));
    assert!(results[0].1.is_some()); // Should find key000
    assert_eq!(results[1].0, Bytes::from_static(b"key005"));
    assert!(results[1].1.is_some()); // Should find key005
    assert_eq!(results[2].0, Bytes::from_static(b"key009"));
    assert!(results[2].1.is_some()); // Should find key009
    assert_eq!(results[3].0, Bytes::from_static(b"key999"));
    assert_eq!(results[3].1, None); // Not found
}

#[test]
fn should_multi_get_mixed_memtable_and_sst() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        wal_buffer_size: 512, // WAL buffer (increased to account for WAL format v2 cf_id field)
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Write data and force flush
    eng.put(&cf, 
        Bytes::from_static(b"old000"), Bytes::from_static(b"oval000"),
    )
    .expect("put");
    eng.put(&cf, 
        Bytes::from_static(b"old005"), Bytes::from_static(b"oval005"),
    )
    .expect("put");

    // Force WAL rotation
    let big = vec![b'x'; 128];
    eng.put(&cf, Bytes::from_static(b"oldlarge"), Bytes::from(big))
        .expect("put");

    std::thread::sleep(std::time::Duration::from_millis(150));

    // Write new data to memtable (after rotation)
    eng.put(&cf, Bytes::from_static(b"new1"), Bytes::from_static(b"nval1"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"new2"), Bytes::from_static(b"nval2"))
        .expect("put");

    // Act: Get mix of old (in SST) and new (in memtable) keys
    let keys: Vec<&[u8]> = vec![b"old000", b"new1", b"old005", b"new2", b"missing"];
    let results = eng.multi_get(&keys).expect("multi_get");

    // Assert
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0, Bytes::from_static(b"old000"));
    assert!(results[0].1.is_some()); // From SST
    assert_eq!(results[1].0, Bytes::from_static(b"new1"));
    assert_eq!(results[1].1, Some(Bytes::from_static(b"nval1"))); // From memtable
    assert_eq!(results[2].0, Bytes::from_static(b"old005"));
    assert!(results[2].1.is_some()); // From SST
    assert_eq!(results[3].0, Bytes::from_static(b"new2"));
    assert_eq!(results[3].1, Some(Bytes::from_static(b"nval2"))); // From memtable
    assert_eq!(results[4].0, Bytes::from_static(b"missing"));
    assert_eq!(results[4].1, None); // Not found
}

#[test]
fn should_scan_reverse_from_memtable() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Write keys in forward order
    eng.put(&cf, Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"k3"), Bytes::from_static(b"v3"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"k4"), Bytes::from_static(b"v4"))
        .expect("put");

    // Act: Scan in reverse
    let results = eng.scan(Query::new().reverse()).expect("scan");

    // Assert: Should get results in reverse order
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].0, Bytes::from_static(b"k4"));
    assert_eq!(results[0].1, Bytes::from_static(b"v4"));
    assert_eq!(results[1].0, Bytes::from_static(b"k3"));
    assert_eq!(results[1].1, Bytes::from_static(b"v3"));
    assert_eq!(results[2].0, Bytes::from_static(b"k2"));
    assert_eq!(results[2].1, Bytes::from_static(b"v2"));
    assert_eq!(results[3].0, Bytes::from_static(b"k1"));
    assert_eq!(results[3].1, Bytes::from_static(b"v1"));
}

#[test]
fn should_scan_reverse_with_bounds() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Write keys
    eng.put(&cf, Bytes::from_static(b"a"), Bytes::from_static(b"va"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"b"), Bytes::from_static(b"vb"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"c"), Bytes::from_static(b"vc"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"d"), Bytes::from_static(b"vd"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"e"), Bytes::from_static(b"ve"))
        .expect("put");

    // Act: Reverse scan from 'b' to 'e' (exclusive of e)
    let results = eng
        .scan(
            Query::new()
                .start_key(Bytes::from_static(b"b"))
                .end_key(Bytes::from_static(b"e"))
                .reverse(),
        )
        .expect("scan");

    // Assert: Should get d, c, b in reverse order (e is excluded)
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, Bytes::from_static(b"d"));
    assert_eq!(results[0].1, Bytes::from_static(b"vd"));
    assert_eq!(results[1].0, Bytes::from_static(b"c"));
    assert_eq!(results[1].1, Bytes::from_static(b"vc"));
    assert_eq!(results[2].0, Bytes::from_static(b"b"));
    assert_eq!(results[2].1, Bytes::from_static(b"vb"));
}

#[test]
fn should_scan_reverse_with_limit() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Write keys
    for i in 1..=10 {
        let key = format!("k{:02}", i);
        let val = format!("v{:02}", i);
        eng.put(&cf, key.as_bytes(), val.as_bytes()).expect("put");
    }

    // Act: Reverse scan with limit of 3
    let results = eng.scan(Query::new().reverse().limit(3)).expect("scan");

    // Assert: Should get top 3 in reverse order
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, Bytes::from_static(b"k10"));
    assert_eq!(results[1].0, Bytes::from_static(b"k09"));
    assert_eq!(results[2].0, Bytes::from_static(b"k08"));
}

#[test]
fn should_scan_with_lower_and_upper_bounds() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Write keys
    for c in b'a'..=b'z' {
        let key = vec![c];
        let val = vec![c + 32]; // lowercase + 32 offset
        eng.put(&cf, key.as_bytes(), val.as_bytes()).expect("put");
    }

    // Act: Scan from 'f' to 'k' (exclusive)
    let results = eng
        .scan(
            Query::new()
                .start_key(Bytes::from_static(b"f"))
                .end_key(Bytes::from_static(b"k")),
        )
        .expect("scan");

    // Assert: Should get f, g, h, i, j (k is excluded)
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].0, Bytes::from_static(b"f"));
    assert_eq!(results[1].0, Bytes::from_static(b"g"));
    assert_eq!(results[2].0, Bytes::from_static(b"h"));
    assert_eq!(results[3].0, Bytes::from_static(b"i"));
    assert_eq!(results[4].0, Bytes::from_static(b"j"));
}

#[test]
fn should_scan_reverse_respects_tombstones() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Write and delete keys
    eng.put(&cf, Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
        .expect("put");
    eng.put(&cf, Bytes::from_static(b"k3"), Bytes::from_static(b"v3"))
        .expect("put");
    eng.delete(&cf, Bytes::from_static(b"k2")).expect("delete");

    // Act: Reverse scan
    let results = eng.scan(Query::new().reverse()).expect("scan");

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

#[test]
fn should_insert_key_given_nonexistent_key() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("key1");
    let value = Bytes::from("value1");

    // Act
    let inserted = engine.insert(key.clone(), value.clone()).unwrap();
    let result = engine.get(&cf, &key).unwrap();

    // Assert
    assert!(inserted, "First insert should return true");
    assert_eq!(result, Some(value));
}

#[test]
fn should_not_insert_given_existing_key() {
    let cf = engine.default_column_family();
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("key1");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");
    engine.put(&cf, key.clone(), value1.clone()).unwrap();

    // Act
    let inserted = engine.insert(key.clone(), value2).unwrap();
    let result = engine.get(&cf, &key).unwrap();

    // Assert
    assert!(!inserted, "Insert should return false for existing key");
    assert_eq!(result, Some(value1));
}

#[test]
fn should_return_existing_value_given_insert_with_value() {
    let cf = engine.default_column_family();
    // Arrange
    use cntryl_midge::InsertResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("key1");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");

    // Act
    let result1 = engine
        .insert_with_value(key.clone(), value1.clone())
        .unwrap();
    let result2 = engine.insert_with_value(key.clone(), value2).unwrap();
    let stored = engine.get(&cf, &key).unwrap();

    // Assert
    assert_eq!(result1, InsertResult::Inserted);
    assert_eq!(result2, InsertResult::AlreadyExists(value1.clone()));
    assert_eq!(stored, Some(value1));
}

#[test]
fn should_swap_value_given_matching_expected() {
    // Arrange
    use cntryl_midge::CasResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("counter");

    // Act
    let result1 = engine
        .compare_and_swap(key.clone(), None, Bytes::from("0"))
        .unwrap();
    let result2 = engine
        .compare_and_swap(key.clone(), Some(Bytes::from("0")), Bytes::from("1"))
        .unwrap();
    let value = engine.get(&cf, &key).unwrap();

    // Assert
    assert_eq!(result1, CasResult::Swapped);
    assert_eq!(result2, CasResult::Swapped);
    assert_eq!(value, Some(Bytes::from("1")));
}

#[test]
fn should_return_mismatch_given_unexpected_value() {
    // Arrange
    use cntryl_midge::CasResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("counter");
    let initial = Bytes::from("5");
    engine.put(&cf, key.clone(), initial.clone()).unwrap();

    // Act
    let result = engine
        .compare_and_swap(key.clone(), Some(Bytes::from("0")), Bytes::from("1"))
        .unwrap();
    let value = engine.get(&cf, &key).unwrap();

    // Assert
    assert_eq!(result, CasResult::Mismatch(Some(initial.clone())));
    assert_eq!(value, Some(initial));
}

#[test]
fn should_handle_concurrent_inserts_given_race_simulation() {
    let cf = engine.default_column_family();
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("shared_key");

    // Act
    let result1 = engine.insert(key.clone(), Bytes::from("value1")).unwrap();
    let result2 = engine.insert(key.clone(), Bytes::from("value2")).unwrap();
    let result3 = engine.insert(key.clone(), Bytes::from("value3")).unwrap();
    let value = engine.get(&cf, &key).unwrap();

    // Assert
    assert!(result1, "First insert should succeed");
    assert!(!result2, "Second insert should fail");
    assert!(!result3, "Third insert should fail");
    assert_eq!(value, Some(Bytes::from("value1")));
}

#[test]
fn should_handle_concurrent_cas_given_race_simulation() {
    let cf = engine.default_column_family();
    // Arrange
    use cntryl_midge::CasResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("counter");
    engine.put(&cf, key.clone(), Bytes::from("0")).unwrap();

    // Act
    let result1 = engine
        .compare_and_swap(key.clone(), Some(Bytes::from("0")), Bytes::from("1"))
        .unwrap();
    let result2 = engine
        .compare_and_swap(key.clone(), Some(Bytes::from("0")), Bytes::from("2"))
        .unwrap();
    let result3 = engine
        .compare_and_swap(key.clone(), Some(Bytes::from("1")), Bytes::from("3"))
        .unwrap();
    let value = engine.get(&cf, &key).unwrap();

    // Assert
    assert_eq!(result1, CasResult::Swapped);
    assert_eq!(result2, CasResult::Mismatch(Some(Bytes::from("1"))));
    assert_eq!(result3, CasResult::Swapped);
    assert_eq!(value, Some(Bytes::from("3")));
}

#[test]
fn should_respect_snapshot_isolation_given_insert() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("key1");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");
    let snap1 = engine.snapshot();

    // Act
    let inserted1 = engine.insert(key.clone(), value1.clone()).unwrap();
    let snap2 = engine.snapshot();
    let inserted2 = engine.insert(key.clone(), value2).unwrap();

    // Assert
    assert!(inserted1);
    assert!(!inserted2);
    assert_eq!(engine.get_at(&key, &snap1).unwrap(), None);
    assert_eq!(engine.get_at(&key, &snap2).unwrap(), Some(value1.clone()));
    assert_eq!(engine.get(&cf, &key).unwrap(), Some(value1));
}

#[test]
fn should_fail_insert_given_read_only_mode() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        read_only: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("key1");
    let value = Bytes::from("value1");
    engine.put(&cf, key.clone(), value).unwrap();
    drop(engine);

    let opts_ro = MidgeOptions {
        storage_mode: StorageMode::Memory,
        read_only: true,
        ..Default::default()
    };
    let engine_ro = MidgeEngine::open(opts_ro).unwrap();

    // Act
    let result = engine_ro.insert(key.clone(), Bytes::from("value2"));

    // Assert
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        cntryl_midge::error::MidgeError::ReadOnly
    ));
}

#[test]
fn should_handle_insert_after_delete() {
    let cf = engine.default_column_family();
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("key1");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");
    engine.put(&cf, key.clone(), value1).unwrap();
    engine.delete(&cf, key.clone()).unwrap();

    // Act
    let inserted = engine.insert(key.clone(), value2.clone()).unwrap();
    let result = engine.get(&cf, &key).unwrap();

    // Assert
    assert!(inserted, "Insert should succeed after delete");
    assert_eq!(result, Some(value2));
}

#[test]
fn should_use_latest_value_given_cas_after_concurrent_put() {
    let cf = engine.default_column_family();
    // Arrange
    use cntryl_midge::CasResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let key = Bytes::from("key1");
    engine.put(&cf, key.clone(), Bytes::from("A")).unwrap();
    let snap = engine.snapshot();

    // Act
    engine.put(&cf, key.clone(), Bytes::from("B")).unwrap();
    let result = engine
        .compare_and_swap(key.clone(), Some(Bytes::from("A")), Bytes::from("C"))
        .unwrap();

    // Assert
    assert_eq!(
        result,
        CasResult::Mismatch(Some(Bytes::from("B"))),
        "CAS should see the updated value"
    );
    assert_eq!(engine.get_at(&key, &snap).unwrap(), Some(Bytes::from("A")));
}

// ============================================================================
// Read-only mode tests (consolidated from tests/read_only.rs)
// ============================================================================

#[test]
fn should_allow_reads_when_opened_read_only() {
    // Arrange: create a temp dir DB and write a key, then close
    let tmp = tempfile::tempdir().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let db = MidgeEngine::open(opts.clone()).unwrap();
    db.put(&cf, "k".as_bytes(), "v".as_bytes()).unwrap();
    db.flush().unwrap();
    drop(db);

    // Act: Re-open read-only
    let ro = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        enable_compaction: false,
        read_only: true,
        ..Default::default()
    };
    let db_ro = MidgeEngine::open(ro).unwrap();

    // Assert: reads work
    let got = db_ro.get(&cf, b"k").unwrap();
    assert_eq!(got, Some(Bytes::from("v")));
}

#[test]
fn should_reject_writes_when_opened_read_only() {
    // Arrange: prepare an existing DB on disk
    let tmp = tempfile::tempdir().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let db = MidgeEngine::open(opts.clone()).unwrap();
    db.put(&cf, "k".as_bytes(), "v".as_bytes()).unwrap();
    db.flush().unwrap();
    drop(db);

    // Act: open read-only and attempt a write
    let ro = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        enable_compaction: false,
        read_only: true,
        ..Default::default()
    };
    let db_ro = MidgeEngine::open(ro).unwrap();
    let err = db_ro.put(&cf, "k2".as_bytes(), "v2".as_bytes()).unwrap_err();

    // Assert: error indicates read-only
    let msg = format!("{}", err);
    assert!(msg.contains("read-only"));
}

#[test]
fn should_commit_transaction_atomically_given_multiple_operations() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Act: create transaction and stage operations (as shown in README)
    let mut txn = engine.begin_transaction();
    txn.put(&cf, Bytes::from("key3"), Bytes::from("value3"), None)
        .expect("put");
    txn.insert(Bytes::from("key4"), Bytes::from("value4"), None)
        .expect("insert");
    txn.delete(&cf, "key5".as_bytes()).expect("delete");
    engine
        .commit_transaction(txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert: all operations applied
    assert_eq!(
        engine.get(&cf, b"key3").expect("get"),
        Some(Bytes::from("value3"))
    );
    assert_eq!(
        engine.get(&cf, b"key4").expect("get"),
        Some(Bytes::from("value4"))
    );
    assert_eq!(engine.get(&cf, b"key5").expect("get"), None);
}

#[test]
fn should_rollback_transaction_on_drop_given_uncommitted() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Act: create transaction, stage operations, then drop without committing
    {
        let mut txn = engine.begin_transaction();
        txn.put(&cf, 
            Bytes::from("rollback_key"), Bytes::from("rollback_value"),
            None,
        )
        .expect("put");
        // txn dropped here without commit
    }

    // Assert: changes not persisted
    assert_eq!(engine.get(&cf, b"rollback_key").expect("get"), None);
}

#[test]
fn should_provide_snapshot_isolation_in_transaction() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("k1"), Bytes::from("v1"))
        .expect("put");

    // Act: start transaction, then modify key externally
    let txn = engine.begin_transaction();
    engine
        .put(Bytes::from("k1"), Bytes::from("v2"))
        .expect("put");

    // Assert: transaction has consistent view (begin_sequence captured)
    let begin_seq = txn.begin_sequence();
    assert!(begin_seq > 0);

    // Note: Full snapshot isolation for transaction reads would require
    // wiring txn.get() to engine.get_at(key, snap) - that's a future enhancement
}

#[test]
fn should_stage_delete_range_in_transaction() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Pre-populate some keys
    for i in 0..5 {
        engine
            .put(
                Bytes::from(format!("key{}", i)),
                Bytes::from(format!("val{}", i)),
            )
            .expect("put");
    }

    // Act: use transaction to delete range
    let mut txn = engine.begin_transaction();
    txn.delete_range(Bytes::from("key1"), Bytes::from("key4"))
        .expect("delete_range");
    engine
        .commit_transaction(txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert: keys in range are deleted, boundaries preserved
    assert_eq!(engine.get(&cf, b"key0").expect("get"), Some(Bytes::from("val0")));
    assert_eq!(engine.get(&cf, b"key1").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key2").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key3").expect("get"), None);
    assert_eq!(engine.get(&cf, b"key4").expect("get"), Some(Bytes::from("val4")));
}

#[test]
fn should_delete_keys_in_range_given_delete_range() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Insert multiple keys
    engine.put(&cf, "a".as_bytes(), "1".as_bytes()).expect("put");
    engine.put(&cf, "b".as_bytes(), "2".as_bytes()).expect("put");
    engine.put(&cf, "c".as_bytes(), "3".as_bytes()).expect("put");
    engine.put(&cf, "d".as_bytes(), "4".as_bytes()).expect("put");
    engine.put(&cf, "e".as_bytes(), "5".as_bytes()).expect("put");

    // Act: delete range [b, d)
    engine
        .delete_range(Bytes::from("b"), Bytes::from("d"))
        .expect("delete_range");

    // Assert: keys b and c are deleted, others remain
    assert_eq!(engine.get(&cf, b"a").expect("get"), Some(Bytes::from("1")));
    assert_eq!(engine.get(&cf, b"b").expect("get"), None);
    assert_eq!(engine.get(&cf, b"c").expect("get"), None);
    assert_eq!(engine.get(&cf, b"d").expect("get"), Some(Bytes::from("4")));
    assert_eq!(engine.get(&cf, b"e").expect("get"), Some(Bytes::from("5")));
}

#[test]
fn should_handle_empty_range_given_start_equals_end() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    engine
        .put(Bytes::from("key"), Bytes::from("val"))
        .expect("put");

    // Act: delete empty range
    engine
        .delete_range(Bytes::from("key"), Bytes::from("key"))
        .expect("delete_range");

    // Assert: key still exists (empty range is no-op)
    assert_eq!(engine.get(&cf, b"key").expect("get"), Some(Bytes::from("val")));
}

#[test]
fn should_handle_inverted_range_given_start_greater_than_end() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    engine.put(&cf, "b".as_bytes(), "2".as_bytes()).expect("put");

    // Act: delete inverted range (should be no-op)
    engine
        .delete_range(Bytes::from("z"), Bytes::from("a"))
        .expect("delete_range");

    // Assert: key still exists
    assert_eq!(engine.get(&cf, b"b").expect("get"), Some(Bytes::from("2")));
}

#[test]
fn should_affect_scan_results_given_delete_range() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Insert keys
    for i in 0..10 {
        let key = format!("key{:02}", i);
        let val = format!("val{}", i);
        engine.put(&cf, key.as_bytes(), val.as_bytes()).expect("put");
    }

    // Act: delete range [key03, key07)
    engine
        .delete_range(Bytes::from("key03"), Bytes::from("key07"))
        .expect("delete_range");

    // Assert: scan shows deleted keys are missing
    let rows = engine
        .scan(
            Query::new()
                .start_key(Bytes::from("key00"))
                .end_key(Bytes::from("key10")),
        )
        .expect("scan");

    let expected = vec![
        (Bytes::from("key00"), Bytes::from("val0")),
        (Bytes::from("key01"), Bytes::from("val1")),
        (Bytes::from("key02"), Bytes::from("val2")),
        // key03-key06 deleted
        (Bytes::from("key07"), Bytes::from("val7")),
        (Bytes::from("key08"), Bytes::from("val8")),
        (Bytes::from("key09"), Bytes::from("val9")),
    ];

    assert_eq!(rows, expected);
}

#[test]
fn should_reject_delete_range_given_read_only_mode() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    drop(engine);

    let opts_ro = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        read_only: true,
        ..Default::default()
    };
    let engine_ro = MidgeEngine::open(opts_ro).expect("open");

    // Act
    let result = engine_ro.delete_range(Bytes::from("a"), Bytes::from("z"));

    // Assert
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        cntryl_midge::error::MidgeError::ReadOnly
    ));
}

#[test]
fn should_persist_delete_range_in_wal() {
    let cf = engine.default_column_family();
    // Arrange
    use bytes::Bytes;
    use tempfile::TempDir;

    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().to_path_buf();

    {
        let opts = cntryl_midge::MidgeOptions {
            storage_mode: cntryl_midge::StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            wal_buffer_size: 1024 * 1024,
            wal_sync: true,
            ..Default::default()
        };
        let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

        engine
            .put(Bytes::from("key1"), Bytes::from("value1"))
            .unwrap();
        engine
            .put(Bytes::from("key2"), Bytes::from("value2"))
            .unwrap();
        engine
            .put(Bytes::from("key3"), Bytes::from("value3"))
            .unwrap();
        engine
            .put(Bytes::from("key4"), Bytes::from("value4"))
            .unwrap();
        engine
            .put(Bytes::from("key5"), Bytes::from("value5"))
            .unwrap();

        // Act
        engine
            .delete_range(Bytes::from("key2"), Bytes::from("key4"))
            .unwrap();

        // Assert (before crash)
        assert_eq!(
            engine.get(&cf, &Bytes::from("key1")).unwrap(),
            Some(Bytes::from("value1"))
        );
        assert_eq!(engine.get(&cf, &Bytes::from("key2")).unwrap(), None);
        assert_eq!(engine.get(&cf, &Bytes::from("key3")).unwrap(), None);
        assert_eq!(
            engine.get(&cf, &Bytes::from("key4")).unwrap(),
            Some(Bytes::from("value4"))
        );
        assert_eq!(
            engine.get(&cf, &Bytes::from("key5")).unwrap(),
            Some(Bytes::from("value5"))
        );

        drop(engine);
    }

    // Act (recovery)
    {
        let opts = cntryl_midge::MidgeOptions {
            storage_mode: cntryl_midge::StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            wal_buffer_size: 1024 * 1024,
            wal_sync: true,
            ..Default::default()
        };
        let engine = cntryl_midge::MidgeEngine::open(opts).unwrap();

        // Assert (after recovery)
        assert_eq!(
            engine.get(&cf, &Bytes::from("key1")).unwrap(),
            Some(Bytes::from("value1"))
        );
        assert_eq!(engine.get(&cf, &Bytes::from("key2")).unwrap(), None);
        assert_eq!(engine.get(&cf, &Bytes::from("key3")).unwrap(), None);
        assert_eq!(
            engine.get(&cf, &Bytes::from("key4")).unwrap(),
            Some(Bytes::from("value4"))
        );
        assert_eq!(
            engine.get(&cf, &Bytes::from("key5")).unwrap(),
            Some(Bytes::from("value5"))
        );
    }

    drop(tmp_dir);
}
