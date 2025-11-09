// Scan Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};

mod common;
use common::{test_temp_dir, new_engine};
#[test]
fn should_return_ordered_pairs_given_range_when_scan() {
    let cf = engine.default_column_family();
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")] {
        eng.put(&cf, Bytes::from_static(k), Bytes::from_static(v))
            .expect("put");
    }

    // Act
    let rows = eng
        .scan(&cf, Query::new()
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
fn should_scan_by_prefix_memtable() {
    // Arrange
    let dir = test_temp_dir();
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
        .scan(&cf, Query::new().prefix(Bytes::from_static(b"user:1:")))
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
    let dir = test_temp_dir();
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
        .scan(&cf, Query::new().prefix(Bytes::from_static(b"user:1:")).limit(2))
        .expect("scan");

    // Assert
    assert_eq!(rows_limited.len(), 2);
    assert_eq!(rows_limited[0].0, Bytes::from_static(b"user:1:a"));
    assert_eq!(rows_limited[1].0, Bytes::from_static(b"user:1:b"));
}


#[test]
fn should_scan_reverse_from_memtable() {
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

    // Write keys in forward order
    eng.put(&cf, b"k1", b"v1")
        .expect("put");
    eng.put(&cf, b"k2", b"v2")
        .expect("put");
    eng.put(&cf, b"k3", b"v3")
        .expect("put");
    eng.put(&cf, b"k4", b"v4")
        .expect("put");

    // Act: Scan in reverse
    let results = eng.scan(&cf, Query::new().reverse()).expect("scan");

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
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");

    // Write keys
    eng.put(&cf, b"a", b"va")
        .expect("put");
    eng.put(&cf, b"b", b"vb")
        .expect("put");
    eng.put(&cf, b"c", b"vc")
        .expect("put");
    eng.put(&cf, b"d", b"vd")
        .expect("put");
    eng.put(&cf, b"e", b"ve")
        .expect("put");

    // Act: Reverse scan from 'b' to 'e' (exclusive of e)
    let results = eng
        .scan(&cf, Query::new()
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
    let dir = test_temp_dir();
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
    let results = eng.scan(&cf, Query::new().reverse().limit(3)).expect("scan");

    // Assert: Should get top 3 in reverse order
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, Bytes::from_static(b"k10"));
    assert_eq!(results[1].0, Bytes::from_static(b"k09"));
    assert_eq!(results[2].0, Bytes::from_static(b"k08"));
}


#[test]
fn should_scan_with_lower_and_upper_bounds() {
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

    // Write keys
    for c in b'a'..=b'z' {
        let key = vec![c];
        let val = vec![c + 32]; // lowercase + 32 offset
        eng.put(&cf, key.as_bytes(), val.as_bytes()).expect("put");
    }

    // Act: Scan from 'f' to 'k' (exclusive)
    let results = eng
        .scan(&cf, Query::new()
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


