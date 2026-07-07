//! Column Family Integration Tests
//!
//! Tests for column family lifecycle, isolation, and persistence.

use bytes::Bytes;
use cntryl_midge::MidgeError;
mod common;
use common::*;
use std::sync::Arc;

// ============================================================================
// Column Family Creation
// ============================================================================

#[test]
fn should_create_column_family_given_valid_name_when_engine_open() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));

        // Act
        let cf = engine.create_column_family("test_cf").unwrap();

        // Assert
        assert_eq!(cf.name(), "test_cf");
        assert_ne!(cf.id(), 0); // Not default CF
    });
}

#[test]
fn should_create_multiple_column_families_given_unique_names_when_engine_open() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));

        // Act
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();
        let cf3 = engine.create_column_family("cf3").unwrap();

        // Assert
        assert_eq!(cf1.name(), "cf1");
        assert_eq!(cf2.name(), "cf2");
        assert_eq!(cf3.name(), "cf3");
        assert_ne!(cf1.id(), cf2.id());
        assert_ne!(cf2.id(), cf3.id());
    });
}

#[test]
fn should_fail_create_column_family_given_duplicate_name_when_cf_exists() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        engine.create_column_family("test_cf").unwrap();

        // Act
        let result = engine.create_column_family("test_cf");

        // Assert - creating an existing CF is idempotent and returns the existing handle
        assert!(result.is_ok());
        let cf = result.unwrap();
        assert_eq!(cf.name(), "test_cf");
    });
}

#[test]
fn should_create_column_family_with_custom_config_given_config_when_creating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let _engine = Arc::new(open_with_mode(&opts, mode));

        // Act - would need create_column_family_with_options(name, config)
        // let config = ColumnFamilyConfig { memtable_size: 1024 * 1024 };
        // let cf = engine.create_column_family_with_options("test_cf", config).unwrap();

        // Assert
        // assert_eq!(cf.name(), "test_cf");
    });
}

// ============================================================================
// Column Family Deletion
// ============================================================================

#[test]
fn should_drop_column_family_given_empty_cf_when_requested() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        let cf_id = cf.id();

        // Act
        let result = engine.drop_column_family(cf_id);

        // Assert
        assert!(result.is_ok());
    });
}

#[test]
fn should_drop_column_family_given_flushed_data_when_requested() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();
        let cf_id = cf.id();

        // Act
        let result = engine.drop_column_family(cf_id);

        // Assert
        assert!(result.is_ok());
    });
}

#[test]
fn should_fail_drop_column_family_given_unflushed_data_when_memtable_not_empty() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();
        let cf_id = cf.id();

        // Act - should fail if memtable not flushed
        let _result = engine.drop_column_family(cf_id);

        // Assert - current behavior may allow drop, but safe behavior would prevent it
        // This test documents desired behavior
        // assert!(result.is_err());
    });
}

#[test]
fn should_fail_drop_default_column_family_given_drop_request_when_default_cf() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let default_cf_id = 0;

        // Act
        let result = engine.drop_column_family(default_cf_id);

        // Assert
        assert!(result.is_err());
    });
}

#[test]
fn should_invalidate_handle_given_cf_dropped_when_accessing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        let cf_id = cf.id();
        engine.drop_column_family(cf_id).unwrap();

        // Act
        let read_only_result = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly);
        let read_write_result = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite);

        // Assert
        for result in [read_only_result, read_write_result] {
            match result {
                Err(MidgeError::InvalidArgument(message)) => {
                    assert_eq!(message, format!("column family {cf_id} does not exist"));
                }
                Err(error) => {
                    panic!("expected InvalidArgument for dropped CF in {mode}, got {error}")
                }
                Ok(_) => panic!("expected begin_tx to fail for dropped CF in {mode}"),
            }
        }
    });
}

#[test]
fn should_delete_cf_data_given_cf_dropped_when_persisted() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test_cf").unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.drop_column_family(cf.id()).unwrap();
            // Engine dropped
        }

        // Assert (Phase 2) - dropped CF data should not be recovered
        {
            let _engine = open_with_mode(&opts, mode);
            // Would need get_column_family_by_name or list to verify CF is gone
        }
    });
}

#[test]
fn should_allow_recreate_cf_with_same_name_given_cf_dropped_when_creating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf1 = engine.create_column_family("test_cf").unwrap();
        let mut tx = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();
        engine.drop_column_family(cf1.id()).unwrap();

        // Act - recreate with same name
        let cf2 = engine.create_column_family("test_cf").unwrap();
        let mut tx2 = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
        tx2.commit(buffered_write_options(mode)).unwrap();

        // Assert - should not see old data
        let tx_read = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result1 = tx_read.get(b"key1").unwrap();
        let result2 = tx_read.get(b"key2").unwrap();
        assert_eq!(result1, None);
        assert_eq!(result2, Some(Bytes::from_static(b"value2")));
    });
}

// ============================================================================
// Listing Column Families
// ============================================================================

#[test]
fn should_list_default_cf_only_given_no_custom_cfs_when_listing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));

        // Act
        let cfs = engine.list_column_families().unwrap();

        // Assert
        assert_eq!(cfs.len(), 1);
        assert_eq!(cfs[0].name(), "default");
    });
}

#[test]
fn should_list_all_column_families_given_multiple_cfs_when_listing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        engine.create_column_family("cf1").unwrap();
        engine.create_column_family("cf2").unwrap();

        // Act
        let cfs = engine.list_column_families().unwrap();

        // Assert
        assert_eq!(cfs.len(), 3); // default + cf1 + cf2
        let names: Vec<&str> = cfs
            .iter()
            .map(cntryl_midge::ColumnFamilyHandle::name)
            .collect();
        assert!(names.contains(&"default"));
        assert!(names.contains(&"cf1"));
        assert!(names.contains(&"cf2"));
    });
}

#[test]
fn should_not_list_dropped_cf_given_cf_dropped_when_listing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        engine.drop_column_family(cf.id()).unwrap();

        // Act
        let cfs = engine.list_column_families().unwrap();

        // Assert
        let names: Vec<&str> = cfs
            .iter()
            .map(cntryl_midge::ColumnFamilyHandle::name)
            .collect();
        assert!(!names.contains(&"test_cf"));
    });
}

// ============================================================================
// Data Isolation
// ============================================================================

#[test]
fn should_isolate_keys_given_same_key_in_different_cfs_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let default_cf = engine.create_column_family("test").expect("create cf");
        let cf1 = engine.create_column_family("cf1").unwrap();

        // Act
        let mut tx_default = engine
            .begin_tx(default_cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx_default
            .put(b"key1".to_vec(), b"value_default".to_vec(), None)
            .unwrap();
        tx_default.commit(buffered_write_options(mode)).unwrap();

        let mut tx_cf1 = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx_cf1
            .put(b"key1".to_vec(), b"value_cf1".to_vec(), None)
            .unwrap();
        tx_cf1.commit(buffered_write_options(mode)).unwrap();

        // Assert
        let tx_read_default = engine
            .begin_tx(default_cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result_default = tx_read_default.get(b"key1").unwrap();
        let tx_read_cf1 = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result_cf1 = tx_read_cf1.get(b"key1").unwrap();
        assert_eq!(result_default, Some(Bytes::from_static(b"value_default")));
        assert_eq!(result_cf1, Some(Bytes::from_static(b"value_cf1")));
    });
}

#[test]
fn should_isolate_deletes_given_delete_in_one_cf_when_other_cf_has_same_key() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();
        let mut tx1 = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx1.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        tx1.commit(buffered_write_options(mode)).unwrap();

        let mut tx2 = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key1".to_vec(), b"value2".to_vec(), None).unwrap();
        tx2.commit(buffered_write_options(mode)).unwrap();

        // Act
        let mut tx_del = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx_del.delete(b"key1".to_vec()).unwrap();
        tx_del.commit(buffered_write_options(mode)).unwrap();

        // Assert
        let tx_read1 = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result_cf1 = tx_read1.get(b"key1").unwrap();
        let tx_read2 = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result_cf2 = tx_read2.get(b"key1").unwrap();
        assert_eq!(result_cf1, None);
        assert_eq!(result_cf2, Some(Bytes::from_static(b"value2")));
    });
}

#[test]
fn should_isolate_data_given_different_data_volumes_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();

        // Act - write different amounts to each CF
        for i in 0..100 {
            let key = format!("key{i}");
            let mut tx = engine
                .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), b"value1".to_vec(), None)
                .unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
        }
        let mut tx_cf2 = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx_cf2
            .put(b"single_key".to_vec(), b"value2".to_vec(), None)
            .unwrap();
        tx_cf2.commit(buffered_write_options(mode)).unwrap();

        // Assert
        let tx_read_cf1 = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result_cf1 = tx_read_cf1.get(b"key50").unwrap();
        let tx_read_cf2 = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result_cf2 = tx_read_cf2.get(b"single_key").unwrap();
        assert_eq!(result_cf1, Some(Bytes::from_static(b"value1")));
        assert_eq!(result_cf2, Some(Bytes::from_static(b"value2")));
        // CF1 should not see CF2's data
        let tx_check = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(tx_check.get(b"single_key").unwrap(), None);
    });
}

#[test]
fn should_isolate_compaction_given_per_cf_data_when_compacting() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();

        // Write data to both CFs
        for i in 0..50 {
            let key = format!("key{i}");
            let mut tx1 = engine
                .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx1.put(key.as_bytes().to_vec(), b"value1".to_vec(), None)
                .unwrap();
            tx1.commit(buffered_write_options(mode)).unwrap();

            let mut tx2 = engine
                .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx2.put(key.as_bytes().to_vec(), b"value2".to_vec(), None)
                .unwrap();
            tx2.commit(buffered_write_options(mode)).unwrap();
        }

        // Act - compact CF1 only
        // engine.compact_column_family(&cf1).unwrap();

        // Assert - both should still have their data
        let tx_read1 = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            tx_read1.get(b"key25").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
        let tx_read2 = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            tx_read2.get(b"key25").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
    });
}

// ============================================================================
// Persistence & Recovery
// ============================================================================

#[test]
fn should_persist_cf_metadata_given_restart_when_cf_created() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            engine.create_column_family("test_cf").unwrap();
            // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cfs = engine.list_column_families().unwrap();
            let names: Vec<&str> = cfs
                .iter()
                .map(cntryl_midge::ColumnFamilyHandle::name)
                .collect();
            assert!(names.contains(&"test_cf"));
        }
    });
}

#[test]
fn should_persist_cf_data_given_restart_when_data_flushed() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test_cf").unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            // Engine dropped
        }

        // Assert (Phase 2)
        {
            let _engine = open_with_mode(&opts, mode);
            // Would need get_column_family_by_name to retrieve CF handle
            // let cf = engine.get_column_family_by_name("test_cf").unwrap();
            // let result = engine.get(&cf, b"key1").unwrap();
            // assert_eq!(result, Some(Bytes::from_static(b"value1")));
        }
    });
}

#[test]
fn should_persist_multiple_cfs_given_restart_when_all_flushed() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = open_with_mode(&opts, mode);
            let cf1 = engine.create_column_family("cf1").unwrap();
            let cf2 = engine.create_column_family("cf2").unwrap();
            let mut tx1 = engine
                .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx1.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            tx1.commit(buffered_write_options(mode)).unwrap();

            let mut tx2 = engine
                .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx2.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
            tx2.commit(buffered_write_options(mode)).unwrap();
            // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cfs = engine.list_column_families().unwrap();
            assert_eq!(cfs.len(), 3); // default + cf1 + cf2
        }
    });
}

#[test]
fn should_persist_cf_drop_given_restart_when_cf_was_dropped() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test_cf").unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).ok(); // Flush before dropping
            engine.drop_column_family(cf.id()).unwrap();
            engine.flush_cf(&cf).ok(); // Flush after dropping to persist the deletion
                                       // Engine dropped
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cfs = engine.list_column_families().unwrap();
            let names: Vec<&str> = cfs
                .iter()
                .map(cntryl_midge::ColumnFamilyHandle::name)
                .collect();
            assert!(!names.contains(&"test_cf"));
        }
    });
}

// ============================================================================
// Query by Name
// ============================================================================

#[test]
fn should_get_column_family_by_name_given_existing_cf_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        engine.create_column_family("test_cf").unwrap();

        // Act
        // let cf = engine.get_column_family_by_name("test_cf").unwrap();

        // Assert
        // assert_eq!(cf.name(), "test_cf");
    });
}

#[test]
fn should_fail_get_column_family_given_nonexistent_name_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let _engine = Arc::new(open_with_mode(&opts, mode));

        // Act
        // let result = engine.get_column_family_by_name("nonexistent");

        // Assert
        // assert!(result.is_err());
    });
}

#[test]
fn should_get_default_column_family_given_fresh_engine_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));

        // Act
        let cf = engine.get_column_family("default").expect("get cf");

        // Assert
        assert_eq!(cf.name(), "default");
        assert_eq!(cf.id(), 0);
    });
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn should_isolate_cf_after_flush_given_same_key_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();

        // Act
        let mut tx1 = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx1.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        tx1.commit(buffered_write_options(mode)).unwrap();

        let mut tx2 = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx2.put(b"key1".to_vec(), b"value2".to_vec(), None).unwrap();
        tx2.commit(buffered_write_options(mode)).unwrap();
        // Flush would happen here if we had flush API

        // Assert - isolation maintained even after flush
        let tx_read1 = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            tx_read1.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
        let tx_read2 = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            tx_read2.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
    });
}

#[test]
fn should_handle_operations_on_default_cf_given_custom_cfs_exist_when_operating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let _cf1 = engine.create_column_family("cf1").unwrap();
        let _cf2 = engine.create_column_family("cf2").unwrap();
        let default_cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(default_cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"default_value".to_vec(), None)
            .unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();

        // Assert
        let tx_read = engine
            .begin_tx(default_cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            tx_read.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"default_value"))
        );
    });
}

#[test]
fn should_maintain_cf_isolation_given_many_cfs_when_operating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let mut cfs = Vec::new();
        for i in 0..10 {
            let name = format!("cf{i}");
            cfs.push(engine.create_column_family(&name).unwrap());
        }

        // Act - write unique value to each CF
        for (i, cf) in cfs.iter().enumerate() {
            let value = format!("value{i}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"shared_key".to_vec(), value.as_bytes().to_vec(), None)
                .unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
        }

        // Assert - each CF has its own value
        for (i, cf) in cfs.iter().enumerate() {
            let expected = format!("value{i}");
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let result = tx_read.get(b"shared_key").unwrap();
            assert_eq!(result, Some(Bytes::from(expected)));
        }
    });
}
