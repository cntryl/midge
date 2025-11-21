#[test]
fn should_insert_new_key_given_absent_key() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));
    let cf = adapter.default_column_family();

    // Act
    adapter.insert(cf, b"ins_key", b"ins_val").expect("insert");

    // Assert
    let got = adapter.get(cf, b"ins_key").expect("get");
    assert_eq!(got.as_deref(), Some(b"ins_val".as_ref()));
}

#[test]
fn should_fail_insert_given_existing_key_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));
    let cf = adapter.default_column_family();
    adapter.put(cf, b"existingkey", b"value1").expect("put");

    // Act
    let result = adapter.insert(cf, b"existingkey", b"value2");

    // Assert
    assert!(result.is_err(), "Insert should fail for existing key");
}
// KvStore Adapter Integration Tests
//
// Tests for the KvStoreAdapter trait implementation that allows external
// integrations to use Midge through the generic KvStore interface.

use bytes::Bytes;
use cntryl_midge::api::kv_store::{BatchOperation, KvStore};
use cntryl_midge::api::WriteOptions;
use cntryl_midge::core::engine::KvStoreAdapter;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::sync::Arc;

mod common;
use common::test_temp_dir;
use std::sync::Arc as StdArc;
use std::thread;

#[test]
fn should_support_kvstore_trait_operations() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();

    // Act
    adapter.put(cf, b"key1", b"value1").expect("put");
    let result = adapter.get(cf, b"key1").expect("get");

    // Assert
    assert_eq!(result.as_deref(), Some(b"value1".as_ref()));
}

#[test]
fn should_insert_new_key_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();

    // Act
    let result = adapter.insert(cf, b"newkey", b"newvalue");

    // Assert
    assert!(result.is_ok(), "Insert should succeed for new key");
    let value = adapter.get(cf, b"newkey").expect("get");
    assert_eq!(value, Some(Bytes::from_static(b"newvalue")));
}

#[test]
fn should_delete_key_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();
    adapter.put(cf, b"key1", b"value1").expect("put");

    // Act
    adapter.delete(cf, b"key1").expect("delete");

    // Assert
    let result = adapter.get(cf, b"delkey").expect("get");
    assert_eq!(result, None);
}

#[test]
fn should_scan_range_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();
    adapter.put(cf, b"key1", b"value1").expect("put");
    adapter.put(cf, b"key2", b"value2").expect("put");
    adapter.put(cf, b"key3", b"value3").expect("put");

    // Act
    let results = adapter.scan(cf, b"key1", b"key3").expect("scan");

    // Assert
    assert_eq!(results.len(), 2, "Should return keys in range [key1, key3)");
    assert_eq!(results[0].0, Bytes::from_static(b"key1"));
    assert_eq!(results[1].0, Bytes::from_static(b"key2"));
}

#[test]
fn should_delete_range_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();
    adapter.put(cf, b"range1", b"value1").expect("put");
    adapter.put(cf, b"range2", b"value2").expect("put");
    adapter.put(cf, b"range3", b"value3").expect("put");
    adapter.put(cf, b"range4", b"value4").expect("put");

    // Act
    adapter
        .delete_range(cf, b"range2", b"range4")
        .expect("delete_range");

    // Assert
    assert_eq!(
        adapter.get(cf, b"range1").expect("get"),
        Some(Bytes::from_static(b"value1"))
    );
    assert_eq!(adapter.get(cf, b"range2").expect("get"), None);
    assert_eq!(adapter.get(cf, b"range3").expect("get"), None);
    assert_eq!(
        adapter.get(cf, b"range4").expect("get"),
        Some(Bytes::from_static(b"value4"))
    );
}

#[test]
fn should_perform_cas_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();
    adapter.put(cf, b"caskey", b"oldvalue").expect("put");

    // Act - CAS with correct expected value
    let success = adapter
        .compare_and_swap(cf, b"caskey", Some(b"oldvalue"), b"newvalue")
        .expect("cas");

    // Assert
    assert!(success, "CAS should succeed with matching expected value");
    let result = adapter.get(cf, b"caskey").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"newvalue")));
}

#[test]
fn should_fail_cas_with_mismatched_value() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();
    adapter.put(cf, b"caskey", b"actualvalue").expect("put");

    // Act - CAS with wrong expected value
    let success = adapter
        .compare_and_swap(cf, b"caskey", Some(b"wrongvalue"), b"newvalue")
        .expect("cas");

    // Assert
    assert!(!success, "CAS should fail with mismatched expected value");
    let result = adapter.get(cf, b"caskey").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"actualvalue")));
}

#[test]
fn should_execute_batch_operations_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();

    let operations = vec![
        BatchOperation::Put {
            key: b"batch1".to_vec(),
            value: b"value1".to_vec(),
        },
        BatchOperation::Put {
            key: b"batch2".to_vec(),
            value: b"value2".to_vec(),
        },
        BatchOperation::Delete {
            key: b"batch1".to_vec(),
        },
    ];

    // Act
    adapter.batch(cf, operations).expect("batch");

    // Assert
    assert_eq!(adapter.get(cf, b"batch1").expect("get"), None);
    assert_eq!(
        adapter.get(cf, b"batch2").expect("get"),
        Some(Bytes::from_static(b"value2"))
    );
}

#[test]
fn should_allow_only_one_of_two_concurrent_inserts() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = StdArc::new(KvStoreAdapter::new(Arc::new(engine)));
    let cf = adapter.default_column_family();

    // Act
    let a1 = adapter.clone();
    let t1 = thread::spawn(move || a1.insert(cf, b"race_key", b"v1"));
    let a2 = adapter.clone();
    let t2 = thread::spawn(move || a2.insert(cf, b"race_key", b"v2"));
    let r1 = t1.join().unwrap();
    let r2 = t2.join().unwrap();

    // Assert
    let success_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|b| *b).count();
    assert_eq!(success_count, 1, "Exactly one insert should succeed");
}

#[test]
fn should_allow_only_one_of_two_concurrent_cas_with_none_expected() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = StdArc::new(KvStoreAdapter::new(Arc::new(engine)));
    let cf = adapter.default_column_family();

    // Act
    let a1 = adapter.clone();
    let t1 = thread::spawn(move || a1.compare_and_swap(cf, b"race_cas", None, b"v1"));
    let a2 = adapter.clone();
    let t2 = thread::spawn(move || a2.compare_and_swap(cf, b"race_cas", None, b"v2"));
    let r1 = t1.join().unwrap().expect("cas ok");
    let r2 = t2.join().unwrap().expect("cas ok");

    // Assert
    assert_ne!(r1, r2, "Exactly one CAS should succeed");
    let val = adapter.get(cf, b"race_cas").expect("get");
    assert!(
        val.as_deref() == Some(&b"v1"[..]) || val.as_deref() == Some(&b"v2"[..]),
        "Final value must be winner's value"
    );
}

#[test]
fn should_create_use_column_families_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    // Act
    let cf_config = cntryl_midge::ColumnFamilyConfig::default();
    let new_cf = adapter
        .create_column_family("test_cf", cf_config)
        .expect("create_cf");

    adapter.put(new_cf, b"cf_key", b"cf_value").expect("put");

    // Assert
    let result = adapter.get(new_cf, b"cf_key").expect("get");
    assert_eq!(result, Some(Bytes::from_static(b"cf_value")));

    // Verify isolation from default CF
    let default_cf = adapter.default_column_family();
    let default_result = adapter.get(default_cf, b"cf_key").expect("get");
    assert_eq!(default_result, None);
}

#[test]
fn should_begin_commit_transaction_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();

    // Act - Begin transaction
    let mut txn = adapter.begin_transaction(cf).expect("begin");
    txn.put(b"txn_key", b"txn_value").expect("txn put");

    adapter
        .commit_transaction(txn, WriteOptions::sync())
        .expect("commit");

    // Assert
    let result = adapter.get(cf, b"txn_key").expect("get");
    assert_eq!(result.as_deref(), Some(b"txn_value".as_ref()));
}

#[test]
fn should_rollback_transaction_via_adapter() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let adapter = KvStoreAdapter::new(Arc::new(engine));

    let cf = adapter.default_column_family();

    // Act
    let mut txn = adapter.begin_transaction(cf).expect("begin");
    txn.put(b"rollback_key", b"rollback_value")
        .expect("txn put");

    adapter.rollback_transaction(txn).expect("rollback");

    // Assert
    let result = adapter.get(cf, b"rollback_key").expect("get");
    assert_eq!(result, None);
}
