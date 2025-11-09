// Delete Range Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};

mod common;
use common::{new_engine, test_temp_dir};
#[test]
fn should_delete_keys_in_range_given_delete_range() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Insert multiple keys
    engine
        .put(&cf, "a".as_bytes(), "1".as_bytes())
        .expect("put");
    engine
        .put(&cf, "b".as_bytes(), "2".as_bytes())
        .expect("put");
    engine
        .put(&cf, "c".as_bytes(), "3".as_bytes())
        .expect("put");
    engine
        .put(&cf, "d".as_bytes(), "4".as_bytes())
        .expect("put");
    engine
        .put(&cf, "e".as_bytes(), "5".as_bytes())
        .expect("put");

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
fn should_affect_scan_results_given_delete_range() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Insert keys
    for i in 0..10 {
        let key = format!("key{:02}", i);
        let val = format!("val{}", i);
        engine
            .put(&cf, key.as_bytes(), val.as_bytes())
            .expect("put");
    }

    // Act: delete range [key03, key07)
    engine
        .delete_range(Bytes::from("key03"), Bytes::from("key07"))
        .expect("delete_range");

    // Assert: scan shows deleted keys are missing
    let rows = engine
        .scan(
            &cf,
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
    let dir = test_temp_dir();
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
