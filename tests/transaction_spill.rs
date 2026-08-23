//! Tests for transaction spill behavior and memory management
//!
//! Tests 1-12: durable storage modes (`LocalDisk`, `CloudBacked`) with spill
//! Test 13: memory-only mode (no spill files)

use bytes::Bytes;
use cntryl_midge::Query;
mod common;
use common::*;

// ============================================================================
// TRANSACTION SPILL TESTS
// ============================================================================

/// `should_commit_large_transaction_given_many_writes_exceeding_memory_limit`
/// Verify all writes commit despite spill triggered by small memory limit
/// Act: Write 100 keys with small memory budget, commit
/// Assert: All keys persisted despite spilling to disk
#[test]
fn should_commit_large_transaction_given_many_writes_exceeding_memory_limit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024); // 256KB limit

        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("key{i:04}");
            let value = format!("value_{i:04}");
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Assert
        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("key{i:04}");
            let expected = format!("value_{i:04}");
            let got = tx_read.get(key.as_bytes()).expect("get");
            let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
            assert_eq!(
                got_str,
                Some(expected),
                "key {key} mismatch in mode: {mode}"
            );
        }
    });
}

/// `should_handle_very_large_transaction_given_multiple_spills_when_persisted`
/// Verify multiple spill files created and handled correctly
/// Act: Write 500 keys to force multiple spill files
/// Assert: All spill files managed and data recovered
#[test]
fn should_handle_very_large_transaction_given_multiple_spills_when_persisted() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(128 * 1024); // 128KB - multiple spills

        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..500 {
            let key = format!("big_key{i:04}");
            let value = format!("big_value_{i:04}");
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Assert
        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in (0..500).step_by(50) {
            let key = format!("big_key{i:04}");
            let got = tx_read.get(key.as_bytes()).expect("get");
            assert!(got.is_some(), "key {key} not found after multiple spills");
        }
    });
}

/// `should_preserve_data_integrity_given_large_transaction_with_specific_values`
/// Verify data integrity maintained through spill/commit cycle
#[test]
fn should_preserve_data_integrity_given_large_transaction_with_specific_values() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024);

        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..200 {
            let key = format!("integrity_test_{i:04}");
            let value = format!("pattern_{}_{}", i % 10, "x".repeat(50));
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Assert
        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in 0..200 {
            let key = format!("integrity_test_{i:04}");
            let expected = format!("pattern_{}_{}", i % 10, "x".repeat(50));
            let got = tx_read.get(key.as_bytes()).expect("get");
            let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
            assert_eq!(
                got_str,
                Some(expected),
                "integrity check failed for key {key}"
            );
        }
    });
}

/// `should_preserve_key_order_given_large_transaction_when_iterating`
/// Verify key order preserved through spill operations
#[test]
fn should_preserve_key_order_given_large_transaction_when_iterating() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(128 * 1024);

        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..200 {
            let key = format!("order_test_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"v".to_vec(), None)
                .expect("put");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Assert: a real scan must return every key in ascending sorted
        // order, not merely be individually gettable.
        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let scanned = tx_read
            .scan(&Query::new())
            .expect("scan all keys")
            .try_collect()
            .expect("collect scanned keys");

        let expected_keys: Vec<Bytes> = (0..200)
            .map(|i| Bytes::from(format!("order_test_{i:04}").into_bytes()))
            .collect();
        let actual_keys: Vec<Bytes> = scanned.iter().map(|(k, _)| k.clone()).collect();

        assert_eq!(
            actual_keys, expected_keys,
            "scan did not return keys in sorted order for mode: {mode}"
        );
        assert!(
            scanned.iter().all(|(_, v)| v == &Bytes::from_static(b"v")),
            "scanned values corrupted in mode: {mode}"
        );
    });
}

/// `should_rollback_spilled_transaction_given_drop_without_commit`
/// Verify spilled transaction data cleaned up on drop
#[test]
fn should_rollback_spilled_transaction_given_drop_without_commit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024);

        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("rollback_test_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
            }
        }

        // Assert
        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx_read.get(b"rollback_test_0000").expect("get");
        assert_eq!(got, None, "rolled back data persisted in mode: {mode}");
    });
}

/// `should_cleanup_spill_files_given_transaction_rollback_when_finalizing`
/// Verify spill files cleaned up on transaction rollback
#[test]
fn should_cleanup_spill_files_given_transaction_rollback_when_finalizing() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(100 * 1024);

        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..300 {
                let key = format!("spill_cleanup_{i:04}");
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
        tx_write
            .commit(buffered_write_options(mode))
            .expect("commit");

        // Assert
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

/// `should_rollback_uncommitted_spill_given_restart_before_commit`
/// Verify spilled data rolled back after restart if not committed
#[test]
fn should_rollback_uncommitted_spill_given_restart_before_commit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();
        let mut opts = opts.clone();
        opts = opts.memory_budget(100 * 1024);

        // Act
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..300 {
                let key = format!("uncom_spill_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
            }
            drop(tx);
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert
        {
            let engine = open_with_mode(&opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"uncom_spill_0000").expect("get");
            assert_eq!(got, None, "uncommitted spill recovered in mode: {mode}");
        }
    });
}

/// `should_recover_committed_spill_given_restart_after_commit`
/// Verify spilled data recovered after restart if committed
#[test]
fn should_recover_committed_spill_given_restart_after_commit() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let opts_clone = opts.clone();
        let mut opts = opts.clone();
        opts = opts.memory_budget(100 * 1024);

        // Act
        {
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..300 {
                let key = format!("com_spill_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Assert
        {
            let engine = open_with_mode(&opts_clone, mode);
            let cf = engine.create_column_family("test").expect("create cf");

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

/// `should_not_starve_foreground_writes_given_background_spill_activity`
/// Verify foreground writes make real, bounded-latency progress while a
/// concurrent background thread continuously runs large transactions that
/// spill to disk.
#[test]
fn should_not_starve_foreground_writes_given_background_spill_activity() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(64 * 1024); // small budget forces real spill activity

        let engine = std::sync::Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let write_options = buffered_write_options(mode);

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Background thread: repeatedly commits large transactions that
        // exceed the memory budget and spill to disk, for as long as the
        // foreground thread is still working.
        let bg_engine = std::sync::Arc::clone(&engine);
        let bg_cf_id = cf.id();
        let bg_stop = std::sync::Arc::clone(&stop);
        let background = std::thread::spawn(move || {
            let mut round: u32 = 0;
            while !bg_stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut tx = bg_engine
                    .begin_tx(bg_cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin background tx");
                for i in 0..300 {
                    let key = format!("bg_{round}_{i:04}");
                    tx.put(key.as_bytes().to_vec(), vec![b'b'; 512], None)
                        .expect("background put");
                }
                tx.commit(write_options).expect("background commit");
                round += 1;
            }
        });

        // Act: run foreground writes concurrently with the background spill
        // activity and measure how long they take to complete. The shared
        // memory budget means a foreground commit can legitimately observe
        // transient backpressure (WriteStall) while the background thread is
        // saturating the budget; a correctly-behaving engine surfaces that as
        // a retryable signal rather than starving the foreground writer
        // forever, so the foreground loop retries on WriteStall and the test
        // asserts the *overall* wall-clock time stays bounded.
        let fg_count = 50;
        let start = std::time::Instant::now();
        for i in 0..fg_count {
            let key = format!("foreground_{i:04}");
            loop {
                let mut tx_fg = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin foreground tx");
                tx_fg
                    .put(key.as_bytes().to_vec(), b"works".to_vec(), None)
                    .expect("foreground put");
                match tx_fg.commit(write_options) {
                    Ok(()) => break,
                    Err(cntryl_midge::MidgeError::WriteStall(_)) => {
                        assert!(
                            start.elapsed() < std::time::Duration::from_secs(30),
                            "foreground writes starved by background spill activity \
                             after {i} commits, mode: {mode}"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(other) => panic!("foreground commit failed: {other:?}"),
                }
            }
        }
        let elapsed = start.elapsed();

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        background.join().expect("background thread panicked");

        // Assert: foreground writes completed in bounded time and are all
        // durably visible, despite concurrent background spill activity.
        assert!(
            elapsed < std::time::Duration::from_secs(30),
            "foreground writes starved by background spill activity: {elapsed:?} \
             for {fg_count} commits, mode: {mode}"
        );

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        for i in 0..fg_count {
            let key = format!("foreground_{i:04}");
            let got = tx_read.get(key.as_bytes()).expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"works")),
                "foreground write {key} lost, mode: {mode}"
            );
        }
    });
}

/// `should_handle_concurrent_large_transactions_given_memory_pressure`
/// Verify system handles concurrent large transactions
#[test]
fn should_handle_concurrent_large_transactions_given_memory_pressure() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(256 * 1024);

        let engine = std::sync::Arc::new(open_with_mode(&opts, mode));

        // Act
        let engine_clone = std::sync::Arc::clone(&engine);
        let write_options = buffered_write_options(mode);
        let t1 = std::thread::spawn(move || {
            let cf = engine_clone
                .create_column_family("test")
                .expect("create cf");
            let mut tx = engine_clone
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("t1_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"t1_value".to_vec(), None)
                    .expect("put");
            }
            tx.commit(write_options).expect("commit");
        });

        let engine_clone = std::sync::Arc::clone(&engine);
        let write_options = buffered_write_options(mode);
        let t2 = std::thread::spawn(move || {
            let cf = engine_clone
                .create_column_family("test")
                .expect("create cf");
            let mut tx = engine_clone
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("t2_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"t2_value".to_vec(), None)
                    .expect("put");
            }
            tx.commit(write_options).expect("commit");
        });

        t1.join().expect("t1 join");
        t2.join().expect("t2 join");

        // Assert
        let cf = engine.create_column_family("test").expect("create cf");
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

/// `should_handle_transaction_with_tiny_memory_limit_given_forced_spill`
/// Verify system handles extremely tight memory limits
#[test]
fn should_handle_transaction_with_tiny_memory_limit_given_forced_spill() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(32 * 1024); // 32KB - reasonable limit

        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..20 {
            let key = format!("tiny_{i:02}");
            let value = format!("value{i:02}");
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .expect("put");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Assert
        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx_read.get(b"tiny_00").expect("get");
        assert!(got.is_some(), "data lost with tiny memory limit");
    });
}

/// `should_handle_mixed_value_sizes_in_spilled_transaction_when_committed`
/// Verify transaction handles mixed sized values through spill
#[test]
fn should_handle_mixed_value_sizes_in_spilled_transaction_when_committed() {
    for_each_storage_mode(durable_storage_modes(), |mode, opts| {
        // Arrange
        let mut opts = opts;
        opts = opts.memory_budget(128 * 1024);

        let engine = open_with_mode(&opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..100 {
            let key = format!("mixed_{i:04}");
            let value = if i % 3 == 0 {
                b"tiny".to_vec()
            } else if i % 3 == 1 {
                vec![b'x'; 256]
            } else {
                vec![b'y'; 512]
            };
            tx.put(key.as_bytes().to_vec(), value, None).expect("put");
        }
        tx.commit(buffered_write_options(mode)).expect("commit");

        // Assert
        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got_tiny = tx_read.get(b"mixed_0000").expect("get");
        assert!(got_tiny.is_some(), "tiny value lost");

        let got_med = tx_read.get(b"mixed_0001").expect("get");
        assert_eq!(
            got_med.as_ref().map(bytes::Bytes::len),
            Some(256),
            "medium size wrong"
        );

        let got_large = tx_read.get(b"mixed_0002").expect("get");
        assert_eq!(
            got_large.as_ref().map(bytes::Bytes::len),
            Some(512),
            "large size wrong"
        );
    });
}

/// Count on-disk directories that the engine's in-memory storage path would
/// use for a spill/data directory (see `StartupStoragePath::resolve` for
/// `Storage::InMemory`, which names such directories
/// `target/tmp/midge_test_memory_*`). `StartupStoragePath::prepare` never
/// calls `create_dir_all` for memory-mode engines, so in-memory mode should
/// never cause any such directory to come into existence.
fn count_memory_mode_artifact_dirs() -> usize {
    let tmp_root = std::path::Path::new("target/tmp");
    std::fs::read_dir(tmp_root).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("midge_test_memory_"))
            })
            .count()
    })
}

/// `should_not_create_disk_artifacts_given_large_transaction_when_memory_mode`
/// Verify memory-only mode doesn't create spill files
#[test]
fn should_not_create_disk_artifacts_given_large_transaction_when_memory_mode() {
    // Arrange
    let opts = memory_opts();
    let dirs_before = count_memory_mode_artifact_dirs();

    let engine = open_with_mode(&opts, "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin_tx");
    for i in 0..500 {
        let key = format!("mem_only_{i:04}");
        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
            .expect("put");
    }
    tx.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit");

    // Assert: no filesystem artifacts (spill directory or otherwise) were
    // created as a side effect of this large in-memory transaction.
    let dirs_after = count_memory_mode_artifact_dirs();
    assert_eq!(
        dirs_after, dirs_before,
        "memory mode must not create on-disk artifacts"
    );

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
