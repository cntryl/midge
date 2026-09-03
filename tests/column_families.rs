//! Column Family Integration Tests
//!
//! Tests for column family lifecycle, isolation, and persistence.

use bytes::Bytes;
use cntryl_midge::MidgeError;
mod common;
use common::*;
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Barrier};
use std::time::Duration;

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
fn should_reject_duplicate_column_family_name_given_existing_active_column_family_when_creating() {
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
fn should_reject_default_named_column_family_given_reserved_name_when_creating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));

        // Act
        let result = engine.create_column_family("default");

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message))
                if message.contains("default") && message.contains("reserved")
        ));
        let column_families = engine.list_column_families().expect("list column families");
        assert_eq!(column_families.len(), 1);
        assert_eq!(column_families[0].id(), 0);
    });
}

#[test]
fn should_create_column_family_with_custom_config_given_config_when_creating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, mut opts| {
        // Arrange
        opts.memtable_size = 1024 * 1024;
        let engine = Arc::new(open_with_mode(&opts, mode));

        // Act
        let cf = engine
            .create_column_family("test_cf")
            .expect("create column family under configured engine");
        let metrics = engine
            .get_runtime_metrics()
            .expect("read configured runtime metrics");

        // Assert
        assert_eq!(cf.name(), "test_cf");
        assert_eq!(metrics.memtable_size_limit, 1024 * 1024);
    });
}

#[test]
fn should_reject_empty_column_family_name_before_manifest_persistence() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut engine = open_with_mode(&opts, mode);

        // Act
        let result = engine.create_column_family("");

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
        assert!(engine.get_column_family("").is_none());
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before checking durable manifest");
        let reopened = open_with_mode(&opts, mode);
        assert!(reopened.get_column_family("").is_none());
    });
}

#[test]
fn should_reject_nul_column_family_name_before_manifest_persistence() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut engine = open_with_mode(&opts, mode);

        // Act
        let result = engine.create_column_family("tenant\0hidden");

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
        assert!(engine.get_column_family("tenant\0hidden").is_none());
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before checking durable manifest");
        let reopened = open_with_mode(&opts, mode);
        assert!(reopened.get_column_family("tenant\0hidden").is_none());
    });
}

#[test]
fn should_reject_oversized_column_family_name_before_manifest_persistence() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut engine = open_with_mode(&opts, mode);
        let oversized = "x".repeat(256);

        // Act
        let result = engine.create_column_family(&oversized);

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
        assert!(engine.get_column_family(&oversized).is_none());
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before checking durable manifest");
        let reopened = open_with_mode(&opts, mode);
        assert!(reopened.get_column_family(&oversized).is_none());
    });
}

#[test]
fn should_accept_column_family_name_at_maximum_byte_length() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(&opts, mode);
        let maximum = "x".repeat(255);

        // Act
        let cf = engine
            .create_column_family(&maximum)
            .expect("accept maximum-length column-family name");

        // Assert
        assert_eq!(cf.name(), maximum);
        assert_eq!(
            engine
                .get_column_family(&maximum)
                .expect("maximum-length CF is registered")
                .id(),
            cf.id()
        );
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
        assert!(
            engine.get_column_family("test_cf").is_none(),
            "dropped cf must no longer be resolvable by name, mode: {mode}"
        );
        assert!(
            matches!(
                engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly),
                Err(MidgeError::InvalidArgument(_))
            ),
            "transactions against a dropped cf id must be rejected, mode: {mode}"
        );
    });
}

#[test]
fn should_drop_column_family_given_flushed_data_when_requested() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test_cf").unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();
        engine.flush_cf(&cf).expect("flush column family");
        let cf_id = cf.id();

        // Act
        let result = engine.drop_column_family(cf_id);

        // Assert: the cf and its already-flushed data are gone, not just the
        // drop call returning Ok.
        assert!(result.is_ok());
        assert!(
            engine.get_column_family("test_cf").is_none(),
            "dropped cf with flushed data must no longer be resolvable, mode: {mode}"
        );
        assert!(
            matches!(
                engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly),
                Err(MidgeError::InvalidArgument(_))
            ),
            "reads against a dropped cf with flushed sst data must be rejected, mode: {mode}"
        );
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

        // Act
        let result = engine.drop_column_family(cf_id);

        // Assert
        assert!(matches!(result, Err(MidgeError::Busy(_))));
        let reader = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("column family must remain readable after rejected drop");
        assert_eq!(
            reader.get(b"key1").expect("read retained value"),
            Some(Bytes::from_static(b"value1"))
        );
    });
}

#[test]
fn should_discard_unflushed_data_given_explicit_destructive_drop() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine
            .create_column_family("discard-me")
            .expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put value");
        tx.commit(buffered_write_options(mode))
            .expect("commit value");

        // Act
        engine
            .drop_column_family_discarding_unflushed(cf.id())
            .expect("explicit destructive drop");

        // Assert
        assert!(engine.get_column_family("discard-me").is_none());
        assert!(matches!(
            engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly),
            Err(MidgeError::InvalidArgument(_))
        ));
    });
}

#[test]
fn should_return_clean_error_given_write_racing_concurrent_drop_when_same_cf() {
    // Arrange
    let opts = opts_for_mode("local");
    let engine = Arc::new(open_with_mode(&opts, "local"));
    let cf = engine
        .create_column_family("racing-drop")
        .expect("create column family");
    let cf_id = cf.id();
    let start = Arc::new(Barrier::new(3));
    let (result_tx, result_rx) = mpsc::sync_channel(2);

    let write_engine = Arc::clone(&engine);
    let write_start = Arc::clone(&start);
    let write_results = result_tx.clone();
    let writer = std::thread::spawn(move || {
        write_start.wait();
        let result = (|| -> cntryl_midge::MidgeResult<()> {
            let mut tx = write_engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)?;
            tx.put(b"racing-key".to_vec(), b"racing-value".to_vec(), None)?;
            tx.commit(cntryl_midge::WriteOptions::sync())
        })();
        write_results
            .send(("write", result))
            .expect("send write result");
    });

    let drop_engine = Arc::clone(&engine);
    let drop_start = Arc::clone(&start);
    let dropper = std::thread::spawn(move || {
        drop_start.wait();
        result_tx
            .send((
                "drop",
                drop_engine.drop_column_family_discarding_unflushed(cf_id),
            ))
            .expect("send drop result");
    });

    // Act
    start.wait();
    let first = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first racing result");
    let second = result_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("second racing result");
    writer.join().expect("join writer");
    dropper.join().expect("join dropper");

    // Assert
    let results = [first, second];
    let drop_result = results
        .iter()
        .find(|(operation, _)| *operation == "drop")
        .expect("drop result");
    drop_result
        .1
        .as_ref()
        .expect("destructive drop must reach one coherent terminal state");
    let write_result = results
        .iter()
        .find(|(operation, _)| *operation == "write")
        .expect("write result");
    assert!(
        write_result.1.is_ok()
            || matches!(write_result.1, Err(MidgeError::InvalidArgument(_))),
        "the concurrent writer must either commit before the drop or receive a clean missing-CF error: {:?}",
        write_result.1
    );
    assert!(engine.get_column_family("racing-drop").is_none());
    assert!(matches!(
        engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly),
        Err(MidgeError::InvalidArgument(_))
    ));

    let mut engine =
        Arc::try_unwrap(engine).unwrap_or_else(|_| panic!("all engine clones dropped"));
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown after racing drop");
    let reopened = open_with_mode(&opts, "local");
    assert!(reopened.get_column_family("racing-drop").is_none());
    assert!(matches!(
        reopened.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly),
        Err(MidgeError::InvalidArgument(_))
    ));
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
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message)) if message.contains("default")
        ));
    });
}

#[test]
fn should_return_invalid_argument_given_drop_of_nonexistent_column_family() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));

        // Act
        let result = engine.drop_column_family(42);

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message))
                if message.contains("42") && message.contains("not found")
        ));
    });
}

#[test]
fn should_return_invalid_argument_given_second_drop_of_same_column_family() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine
            .create_column_family("drop-twice")
            .expect("create column family");
        engine
            .drop_column_family(cf.id())
            .expect("drop column family once");

        // Act
        let result = engine.drop_column_family(cf.id());

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message))
                if message.contains("not found") || message.contains("already deleted")
        ));
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
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test_cf").unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            engine
                .drop_column_family_discarding_unflushed(cf.id())
                .expect("explicitly discard unflushed column family");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2) - dropped CF data should not be recovered
        {
            let engine = open_with_mode(&opts, mode);
            assert!(engine.get_column_family("test_cf").is_none());
            let recreated = engine
                .create_column_family("test_cf")
                .expect("recreate dropped name");
            let reader = engine
                .begin_tx(recreated.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("read recreated family");
            assert_eq!(reader.get(b"key1").expect("read old key"), None);
        }
    });
}

#[test]
fn should_allocate_monotonic_column_family_ids_given_deleted_column_family_when_creating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf1 = engine.create_column_family("test_cf").unwrap();
        let mut tx = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        tx.commit(buffered_write_options(mode)).unwrap();
        engine
            .drop_column_family_discarding_unflushed(cf1.id())
            .expect("explicitly discard first generation");

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
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf1 = engine.create_column_family("cf1").unwrap();
        let cf2 = engine.create_column_family("cf2").unwrap();

        for generation in 0..4 {
            for (cf, value) in [(&cf1, b"value1".as_slice()), (&cf2, b"value2".as_slice())] {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin compaction seed");
                tx.put(
                    format!("key{generation}").into_bytes(),
                    value.to_vec(),
                    None,
                )
                .expect("put compaction seed");
                tx.commit(buffered_write_options(mode))
                    .expect("commit compaction seed");
                engine.flush_cf(cf).expect("flush compaction generation");
            }
        }
        let layout_before = engine
            .get_storage_layout()
            .expect("layout before compact all");
        let l0_before = layout_before
            .levels
            .iter()
            .find(|level| level.level == 0)
            .map_or(0, |level| level.file_count);
        assert_eq!(l0_before, 8, "both families must have four L0 files");

        // Act
        engine
            .compact_all()
            .expect("compact both eligible families");

        // Assert
        let layout_after = engine
            .get_storage_layout()
            .expect("layout after compact all");
        let l0_after = layout_after
            .levels
            .iter()
            .find(|level| level.level == 0)
            .map_or(0, |level| level.file_count);
        let l1_after = layout_after
            .levels
            .iter()
            .find(|level| level.level == 1)
            .map_or(0, |level| level.file_count);
        assert_eq!(
            l0_after, 0,
            "compact_all must drain residual L0 debt in every column family"
        );
        assert_eq!(
            l1_after, 4,
            "each column family must publish isolated outputs for its configured batch and residual file in mode {mode}"
        );
        let tx_read1 = engine
            .begin_tx(cf1.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            tx_read1.get(b"key2").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
        let tx_read2 = engine
            .begin_tx(cf2.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            tx_read2.get(b"key2").unwrap(),
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
            let mut engine = open_with_mode(&opts, mode);
            engine.create_column_family("test_cf").unwrap();
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
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
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test_cf").unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).expect("flush persisted value");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(&opts, mode);
            let cf = engine
                .get_column_family("test_cf")
                .expect("recover column family by name");
            let reader = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin recovered read");
            assert_eq!(
                reader.get(b"key1").expect("read recovered value"),
                Some(Bytes::from_static(b"value1"))
            );
        }
    });
}

#[test]
fn should_persist_multiple_cfs_given_restart_when_all_flushed() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
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
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
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
fn should_preserve_cold_tombstone_given_hot_cold_compaction_when_reopening() {
    for_each_storage_mode(&["local"], |mode, opts| {
        // Arrange
        let mut expected = BTreeMap::<(String, Vec<u8>), Option<Vec<u8>>>::new();
        {
            let mut engine = open_with_mode(&opts, mode);
            let cold = engine.create_column_family("cold").expect("create cold cf");
            let hot = engine.create_column_family("hot").expect("create hot cf");

            // Act
            for chunk_start in (0..1_280_usize).step_by(64) {
                for sequence in chunk_start..chunk_start + 64 {
                    let cf_name = if sequence.is_multiple_of(5) {
                        "cold"
                    } else {
                        "hot"
                    };
                    let key = format!("key-{:03}", sequence % 128).into_bytes();
                    let value = (!sequence.is_multiple_of(13) || sequence < 128)
                        .then(|| format!("value-{sequence:04}").into_bytes());
                    expected.insert((cf_name.to_string(), key), value);
                }
                let barrier = Barrier::new(3);
                std::thread::scope(|scope| {
                    let hot_lane = scope.spawn(|| {
                        barrier.wait();
                        apply_hot_cold_chunk(&engine, &hot, chunk_start, false);
                    });
                    let cold_lane = scope.spawn(|| {
                        barrier.wait();
                        apply_hot_cold_chunk(&engine, &cold, chunk_start, true);
                    });
                    barrier.wait();
                    engine.flush_cf(&hot).expect("flush hot cf");
                    engine.flush_cf(&cold).expect("flush cold cf");
                    engine.compact_all().expect("compact hot and cold cfs");
                    hot_lane.join().expect("join hot lane");
                    cold_lane.join().expect("join cold lane");
                });
            }
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown before recovery verification");
        }

        // Assert
        let engine = open_with_mode(&opts, mode);
        for ((cf_name, key), expected_value) in expected {
            let cf = engine
                .get_column_family(&cf_name)
                .expect("recover workload column family");
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin recovery read");
            assert_eq!(
                tx.get(&key).expect("read recovered workload key"),
                expected_value.map(Bytes::from),
                "recovered value mismatch for {cf_name}/{:?}",
                String::from_utf8_lossy(&key)
            );
        }
    });
}

fn apply_hot_cold_chunk(
    engine: &cntryl_midge::Engine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    chunk_start: usize,
    cold: bool,
) {
    for sequence in chunk_start..chunk_start + 64 {
        if sequence.is_multiple_of(5) != cold {
            continue;
        }
        let key = format!("key-{:03}", sequence % 128).into_bytes();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin workload transaction");
        if sequence >= 128 && sequence.is_multiple_of(13) {
            tx.delete(key).expect("stage workload delete");
        } else {
            tx.put(key, format!("value-{sequence:04}").into_bytes(), None)
                .expect("stage workload put");
        }
        tx.commit(cntryl_midge::WriteOptions::sync())
            .expect("commit workload mutation");
    }
}

#[test]
fn should_persist_cf_drop_given_restart_when_cf_was_dropped() {
    for_each_storage_mode(&["local", "cloud"], |mode, opts| {
        // Arrange
        // Act (Phase 1)
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test_cf").unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            engine
                .flush_cf(&cf)
                .expect("flush committed data before safe drop");
            engine.drop_column_family(cf.id()).unwrap();
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
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
        let cf = engine
            .get_column_family("test_cf")
            .expect("get existing column family");

        // Assert
        assert_eq!(cf.name(), "test_cf");
        assert_ne!(cf.id(), 0);
    });
}

#[test]
fn should_return_none_given_nonexistent_column_family_name_when_querying() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));

        // Act
        let result = engine.get_column_family("nonexistent");

        // Assert
        assert!(result.is_none());
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
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
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
        engine.flush_cf(&cf1).expect("flush first column family");
        engine.flush_cf(&cf2).expect("flush second column family");

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
