mod common;
use bytes::Bytes;
use cntryl_midge::api::kv_store::{BatchOperation, KvStore};
use cntryl_midge::api::WriteOptions;
use cntryl_midge::core::engine::KvStoreAdapter;
use cntryl_midge::{MidgeEngine, MidgeOptions};
use common::{create_storage_mode, disk_storage_modes};
use std::sync::Arc;
use std::thread;

#[test]
fn should_support_kvstore_trait_operations() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        let cf = adapter.default_column_family();

        // Act
        adapter.put(cf, b"key1", b"value1").expect("put during trait test");
        let result = adapter.get(cf, b"key1").expect("get during trait test");

        // Assert
        assert_eq!(result.as_deref(), Some(b"value1".as_ref()), "KvStore trait operations failed for {}", mode);
    }
}

#[test]
fn should_insert_new_key_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        let cf = adapter.default_column_family();

        // Act
        let result = adapter.insert(cf, b"newkey", b"newvalue");

        // Assert
        assert!(result.is_ok(), "Insert should succeed for new key for {}", mode);
        let value = adapter.get(cf, b"newkey").expect("get during insert test");
        assert_eq!(value, Some(Bytes::from_static(b"newvalue")), "Insert value mismatch for {}", mode);
    }
}

#[test]
fn should_fail_insert_given_existing_key_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));
        let cf = adapter.default_column_family();
        adapter.put(cf, b"existingkey", b"value1").expect("put during conflict test");

        // Act
        let result = adapter.insert(cf, b"existingkey", b"value2");

        // Assert
        assert!(result.is_err(), "Insert should fail for existing key for {}", mode);
    }
}

#[test]
fn should_delete_key_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        let cf = adapter.default_column_family();
        adapter.put(cf, b"key1", b"value1").expect("put during delete test");

        // Act
        adapter.delete(cf, b"key1").expect("delete during delete test");

        // Assert
        let result = adapter.get(cf, b"key1").expect("get during delete test");
        assert_eq!(result, None, "Key should be deleted for {}", mode);
    }
}

#[test]
fn should_scan_range_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        let cf = adapter.default_column_family();
        adapter.put(cf, b"key1", b"value1").expect("put during scan test");
        adapter.put(cf, b"key2", b"value2").expect("put during scan test");
        adapter.put(cf, b"key3", b"value3").expect("put during scan test");

        // Act
        let results = adapter.scan(cf, b"key1", b"key3").expect("scan during scan test");

        // Assert
        assert_eq!(results.len(), 2, "Should return keys in range [key1, key3) for {}", mode);
        assert_eq!(results[0].0, Bytes::from_static(b"key1"));
        assert_eq!(results[1].0, Bytes::from_static(b"key2"));
    }
}

#[test]
fn should_delete_range_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        let cf = adapter.default_column_family();
        adapter.put(cf, b"range1", b"value1").expect("put during range delete test");
        adapter.put(cf, b"range2", b"value2").expect("put during range delete test");
        adapter.put(cf, b"range3", b"value3").expect("put during range delete test");
        adapter.put(cf, b"range4", b"value4").expect("put during range delete test");

        // Act
        adapter
            .delete_range(cf, b"range2", b"range4")
            .expect("delete_range during range delete test");

        // Assert
        assert_eq!(
            adapter.get(cf, b"range1").expect("get during range delete test"),
            Some(Bytes::from_static(b"value1")),
            "range1 should remain for {}", mode
        );
        assert_eq!(adapter.get(cf, b"range2").expect("get during range delete test"), None, "range2 should be deleted for {}", mode);
        assert_eq!(adapter.get(cf, b"range3").expect("get during range delete test"), None, "range3 should be deleted for {}", mode);
        assert_eq!(
            adapter.get(cf, b"range4").expect("get during range delete test"),
            Some(Bytes::from_static(b"value4")),
            "range4 should remain for {}", mode
        );
    }
}

#[test]
fn should_perform_cas_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        let cf = adapter.default_column_family();
        adapter.put(cf, b"caskey", b"oldvalue").expect("put during CAS test");

        // Act - CAS with correct expected value
        let success = adapter
            .compare_and_swap(cf, b"caskey", Some(b"oldvalue"), b"newvalue")
            .expect("cas during CAS test");

        // Assert
        assert!(success, "CAS should succeed with matching expected value for {}", mode);
        let result = adapter.get(cf, b"caskey").expect("get during CAS test");
        assert_eq!(result, Some(Bytes::from_static(b"newvalue")), "CAS result mismatch for {}", mode);
    }
}

#[test]
fn should_fail_cas_with_mismatched_value() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        let cf = adapter.default_column_family();
        adapter.put(cf, b"caskey", b"actualvalue").expect("put during CAS mismatch test");

        // Act - CAS with wrong expected value
        let success = adapter
            .compare_and_swap(cf, b"caskey", Some(b"wrongvalue"), b"newvalue")
            .expect("cas during CAS mismatch test");

        // Assert
        assert!(!success, "CAS should fail with mismatched expected value for {}", mode);
        let result = adapter.get(cf, b"caskey").expect("get during CAS mismatch test");
        assert_eq!(result, Some(Bytes::from_static(b"actualvalue")), "CAS should not change value for {}", mode);
    }
}

#[test]
fn should_execute_batch_operations_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
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
        adapter.batch(cf, operations).expect("batch during batch test");

        // Assert
        assert_eq!(adapter.get(cf, b"batch1").expect("get during batch test"), None, "batch1 should be deleted for {}", mode);
        assert_eq!(
            adapter.get(cf, b"batch2").expect("get during batch test"),
            Some(Bytes::from_static(b"value2")),
            "batch2 should have value2 for {}", mode
        );
    }
}

#[test]
fn should_allow_only_one_of_two_concurrent_inserts() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = Arc::new(KvStoreAdapter::new(Arc::new(engine)));
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
        assert_eq!(success_count, 1, "Exactly one insert should succeed for {}", mode);
    }
}

#[test]
fn should_allow_only_one_of_two_concurrent_cas_with_none_expected() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = Arc::new(KvStoreAdapter::new(Arc::new(engine)));
        let cf = adapter.default_column_family();

        // Act
        let a1 = adapter.clone();
        let t1 = thread::spawn(move || a1.compare_and_swap(cf, b"race_cas", None, b"v1"));
        let a2 = adapter.clone();
        let t2 = thread::spawn(move || a2.compare_and_swap(cf, b"race_cas", None, b"v2"));
        let r1 = t1.join().unwrap().expect("cas ok during concurrent CAS test");
        let r2 = t2.join().unwrap().expect("cas ok during concurrent CAS test");

        // Assert
        assert_ne!(r1, r2, "Exactly one CAS should succeed for {}", mode);
        let val = adapter.get(cf, b"race_cas").expect("get during concurrent CAS test");
        assert!(
            val.as_deref() == Some(&b"v1"[..]) || val.as_deref() == Some(&b"v2"[..]),
            "Final value must be winner's value for {}", mode
        );
    }
}

#[test]
fn should_create_use_column_families_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        // Act
        let cf_config = cntryl_midge::ColumnFamilyConfig::default();
        let new_cf = adapter
            .create_column_family("test_cf", cf_config)
            .expect("create_cf during CF test");

        adapter.put(new_cf, b"cf_key", b"cf_value").expect("put during CF test");

        // Assert
        let result = adapter.get(new_cf, b"cf_key").expect("get during CF test");
        assert_eq!(result, Some(Bytes::from_static(b"cf_value")), "CF value mismatch for {}", mode);

        // Verify isolation from default CF
        let default_cf = adapter.default_column_family();
        let default_result = adapter.get(default_cf, b"cf_key").expect("get during CF isolation test");
        assert_eq!(default_result, None, "CF isolation failed for {}", mode);
    }
}

#[test]
fn should_begin_commit_transaction_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        let cf = adapter.default_column_family();

        // Act - Begin transaction
        let mut txn = adapter.begin_transaction(cf).expect("begin during transaction test");
        txn.put(b"txn_key", b"txn_value").expect("txn put during transaction test");

        adapter
            .commit_transaction(txn, WriteOptions::sync())
            .expect("commit during transaction test");

        // Assert
        let result = adapter.get(cf, b"txn_key").expect("get during transaction test");
        assert_eq!(result.as_deref(), Some(b"txn_value".as_ref()), "Transaction commit failed for {}", mode);
    }
}

#[test]
fn should_rollback_transaction_via_adapter() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("Failed to open engine");
        let adapter = KvStoreAdapter::new(Arc::new(engine));

        let cf = adapter.default_column_family();

        // Act
        let mut txn = adapter.begin_transaction(cf).expect("begin during rollback test");
        txn.put(b"rollback_key", b"rollback_value")
            .expect("txn put during rollback test");

        adapter.rollback_transaction(txn).expect("rollback during rollback test");

        // Assert
        let result = adapter.get(cf, b"rollback_key").expect("get during rollback test");
        assert_eq!(result, None, "Transaction rollback failed for {}", mode);
    }
}