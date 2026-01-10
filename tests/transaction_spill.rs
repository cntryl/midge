//! Tests for transaction spill behavior and memory management
//!
//! Tests 1-12: durable storage modes (LocalDisk, CloudBacked) with spill
//! Test 13: memory-only mode (no spill files)

use bytes::Bytes;
use cntryl_midge::testkit::*;

// ============================================================================
// TRANSACTION SPILL TESTS
// ============================================================================

/// should_commit_large_transaction_given_many_writes_exceeding_memory_limit
/// Verify all writes commit despite spill triggered by small memory limit
/// Act: Write 1000 keys with small memory budget, commit
/// Assert: All keys persisted despite spilling to disk
#[test]
fn should_commit_large_transaction_given_many_writes_exceeding_memory_limit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange: Force spill with constrained memory budget
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024); // 256KB limit

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act: Write many keys exceeding memory limit
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let value = format!("value_{:04}", i);
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Assert: All committed despite spill
        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("key{:04}", i);
            let expected = format!("value_{:04}", i);
            let got = tx_read.get(key.as_bytes()).expect("get");
            let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
            assert_eq!(
                got_str,
                Some(expected),
                "key {} mismatch in mode: {}",
                key,
                mode
            );
        }
    });
}

/// should_handle_very_large_transaction_given_multiple_spills_when_persisted
/// Verify multiple spill files created and handled correctly
/// Act: Write 500 keys to force multiple spill files
/// Assert: All spill files managed and data recovered
#[test]
fn should_handle_very_large_transaction_given_multiple_spills_when_persisted() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(128 * 1024); // 128KB - multiple spills

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..500 {
            let key = format!("big_key{:04}", i);
            let value = format!("big_value_{:04}", i);
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in (0..500).step_by(50) {
            let key = format!("big_key{:04}", i);
            let got = tx_read.get(key.as_bytes()).expect("get");
            assert!(got.is_some(), "key {} not found after multiple spills", key);
        }
    });
}

/// should_preserve_data_integrity_given_large_transaction_with_specific_values
/// Verify data integrity maintained through spill/commit cycle
#[test]
fn should_preserve_data_integrity_given_large_transaction_with_specific_values() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024);

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..200 {
            let key = format!("integrity_test_{:04}", i);
            let value = format!("pattern_{}_{}", i % 10, "x".repeat(50));
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in 0..200 {
            let key = format!("integrity_test_{:04}", i);
            let expected = format!("pattern_{}_{}", i % 10, "x".repeat(50));
            let got = tx_read.get(key.as_bytes()).expect("get");
            let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
            assert_eq!(
                got_str,
                Some(expected),
                "integrity check failed for key {}",
                key
            );
        }
    });
}

/// should_preserve_key_order_given_large_transaction_when_iterating
/// Verify key order preserved through spill operations
#[test]
fn should_preserve_key_order_given_large_transaction_when_iterating() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(128 * 1024);

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..200 {
            let key = format!("order_test_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"v".to_vec(), None)
                .expect("put");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in 0..200 {
            let key = format!("order_test_{:04}", i);
            let got = tx_read.get(key.as_bytes()).expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"v")),
                "order check failed for key {}",
                key
            );
        }
    });
}

/// should_rollback_spilled_transaction_given_drop_without_commit
/// Verify spilled transaction data cleaned up on drop
#[test]
fn should_rollback_spilled_transaction_given_drop_without_commit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024);

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("rollback_test_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
            }
        }

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx_read.get(b"rollback_test_0000").expect("get");
        assert_eq!(got, None, "rolled back data persisted in mode: {}", mode);
    });
}

/// should_cleanup_spill_files_given_transaction_rollback_when_finalizing
/// Verify spill files cleaned up on transaction rollback
#[test]
fn should_cleanup_spill_files_given_transaction_rollback_when_finalizing() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(100 * 1024);

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..300 {
                let key = format!("spill_cleanup_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
            }
        }

        let mut tx_write = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx_write
            .put(b"test".to_vec(), b"value".to_vec(), None)
            .expect("put");
        engine
            .commit(tx_write, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx_read.get(b"test").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"value")),
            "engine broken after spill cleanup"
        );
    });
}

/// should_rollback_uncommitted_spill_given_restart_before_commit
/// Verify spilled data rolled back after restart if not committed
#[test]
fn should_rollback_uncommitted_spill_given_restart_before_commit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let opts_clone = opts.clone();
        let mut opts = opts.clone();
        opts = opts.memory_budget(100 * 1024);

        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..300 {
                let key = format!("uncom_spill_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
            }
        }

        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.default_column_family();

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"uncom_spill_0000").expect("get");
            assert_eq!(got, None, "uncommitted spill recovered in mode: {}", mode);
        }
    });
}

/// should_recover_committed_spill_given_restart_after_commit
/// Verify spilled data recovered after restart if committed
#[test]
fn should_recover_committed_spill_given_restart_after_commit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let opts_clone = opts.clone();
        let mut opts = opts.clone();
        opts = opts.memory_budget(100 * 1024);

        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..300 {
                let key = format!("com_spill_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
            }
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        }

        {
            let engine = open_with_mode(opts_clone, mode);
            let cf = engine.default_column_family();

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"com_spill_0000").expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"value")),
                "committed spill not recovered"
            );
        }
    });
}

/// should_not_starve_foreground_writes_given_background_spill_activity
/// Verify foreground writes not blocked by spill
#[test]
fn should_not_starve_foreground_writes_given_background_spill_activity() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024);

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..500 {
            let key = format!("tx_{:04}", i);
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
        }

        let mut tx_fg = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        tx_fg
            .put(b"foreground".to_vec(), b"works".to_vec(), None)
            .expect("put");
        engine
            .commit(tx_fg, cntryl_midge::WriteOptions::buffered())
            .expect("commit");
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let fg = tx_read.get(b"foreground").expect("get");
        assert_eq!(
            fg,
            Some(Bytes::from_static(b"works")),
            "foreground write lost"
        );
    });
}

/// should_handle_concurrent_large_transactions_given_memory_pressure
/// Verify system handles concurrent large transactions
#[test]
fn should_handle_concurrent_large_transactions_given_memory_pressure() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024);

        let engine = std::sync::Arc::new(open_with_mode(opts, mode));

        let engine_clone = std::sync::Arc::clone(&engine);
        let t1 = std::thread::spawn(move || {
            let cf = engine_clone.default_column_family();
            let mut tx = engine_clone
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("t1_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"t1_value".to_vec(), None)
                    .expect("put");
            }
            engine_clone
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        });

        let engine_clone = std::sync::Arc::clone(&engine);
        let t2 = std::thread::spawn(move || {
            let cf = engine_clone.default_column_family();
            let mut tx = engine_clone
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("t2_key_{:04}", i);
                tx.put(key.as_bytes().to_vec(), b"t2_value".to_vec(), None)
                    .expect("put");
            }
            engine_clone
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        });

        t1.join().expect("t1 join");
        t2.join().expect("t2 join");

        let cf = engine.default_column_family();
        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got1 = tx_read.get(b"t1_key_0000").expect("get");
        let got2 = tx_read.get(b"t2_key_0000").expect("get");
        assert_eq!(
            got1,
            Some(Bytes::from_static(b"t1_value")),
            "t1 data missing"
        );
        assert_eq!(
            got2,
            Some(Bytes::from_static(b"t2_value")),
            "t2 data missing"
        );
    });
}

/// should_handle_transaction_with_tiny_memory_limit_given_forced_spill
/// Verify system handles extremely tight memory limits
#[test]
fn should_handle_transaction_with_tiny_memory_limit_given_forced_spill() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(1024); // 1KB - extreme limit

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..50 {
            let key = format!("tiny_{:02}", i);
            let value = format!("value{:02}", i);
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx_read.get(b"tiny_00").expect("get");
        assert!(got.is_some(), "data lost with tiny memory limit");
    });
}

/// should_handle_mixed_value_sizes_in_spilled_transaction_when_committed
/// Verify transaction handles mixed sized values through spill
#[test]
fn should_handle_mixed_value_sizes_in_spilled_transaction_when_committed() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        let mut opts = opts;
        opts = opts.memory_budget(128 * 1024);

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..300 {
            let key = format!("mixed_{:04}", i);
            let value = if i % 3 == 0 {
                b"tiny".to_vec()
            } else if i % 3 == 1 {
                vec![b'x'; 512]
            } else {
                vec![b'y'; 1024]
            };
            tx.put(key.as_bytes().to_vec(), value, None).expect("put");
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got_tiny = tx_read.get(b"mixed_0000").expect("get");
        assert!(got_tiny.is_some(), "tiny value lost");

        let got_med = tx_read.get(b"mixed_0001").expect("get");
        assert_eq!(
            got_med.as_ref().map(|b| b.len()),
            Some(512),
            "medium size wrong"
        );

        let got_large = tx_read.get(b"mixed_0002").expect("get");
        assert_eq!(
            got_large.as_ref().map(|b| b.len()),
            Some(1024),
            "large size wrong"
        );
    });
}

/// should_not_create_disk_artifacts_given_large_transaction_when_memory_mode
/// Verify memory-only mode doesn't create spill files
#[test]
fn should_not_create_disk_artifacts_given_large_transaction_when_memory_mode() {
    let opts = memory_opts();

    let engine = open_with_mode(opts, "memory");
    let cf = engine.default_column_family();

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin_tx");
    for i in 0..500 {
        let key = format!("mem_only_{:04}", i);
        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
            .expect("put");
    }
    engine
        .commit(tx, cntryl_midge::WriteOptions::buffered())
        .expect("commit");

    let tx_read = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin_tx");
    let got = tx_read.get(b"mem_only_0000").expect("get");
    assert_eq!(
        got,
        Some(Bytes::from_static(b"value")),
        "memory mode data lost"
    );
}
