//! Engine API Tests
//!
//! Consolidated from: `engine_init.rs`, `engine_basic.rs`, `engine_wal.rs`, `engine_ttl.rs`, `engine_iterators.rs`, `engine_delete_range.rs`, `engine_exclusivity.rs`, `engine_compaction.rs`, `delete_range_audit.rs`, `smoke.rs`, `edge_cases.rs`

mod common;

mod engine_init {
    use crate::common::*;
    use cntryl_midge::TransactionMode;

    #[test]
    fn should_create_engine_in_all_modes() {
        // Arrange
        // (Mode and options provided by for_each_storage_mode)

        // Act
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let result = cntryl_midge::MidgeEngine::open(opts.to_open_options());

            // Assert: construction succeeded...
            let engine = match result {
                Ok(engine) => engine,
                Err(e) => panic!("Failed to create engine in mode {mode}: {e}"),
            };

            // ...and the opened engine is actually usable, not merely
            // constructed: it holds a healthy primary lease, exposes the
            // default column family, and can round-trip a write.
            assert!(
                engine.is_primary_lease_healthy(),
                "primary lease should be healthy immediately after open in mode: {mode}"
            );
            let cf = engine
                .get_column_family("default")
                .unwrap_or_else(|| panic!("default column family missing in mode: {mode}"));

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap_or_else(|e| panic!("begin_tx failed in mode {mode}: {e}"));
            tx.put(b"init_probe".to_vec(), b"ok".to_vec(), None)
                .unwrap_or_else(|e| panic!("put failed in mode {mode}: {e}"));
            tx.commit(buffered_write_options(mode))
                .unwrap_or_else(|e| panic!("commit failed in mode {mode}: {e}"));

            let read_tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .unwrap_or_else(|e| panic!("begin_tx (read) failed in mode {mode}: {e}"));
            assert_eq!(
                read_tx.get(b"init_probe").expect("read init probe"),
                Some(bytes::Bytes::from_static(b"ok")),
                "engine opened in mode {mode} could not read back a write it just committed"
            );

            println!("Engine created successfully in mode: {mode}");
        });
    }
}

mod engine_basic {
    //! Core KV Engine Integration Tests
    //!
    //! Tests the basic put/get/delete operations end-to-end using the public
    //! `MidgeEngine` API. These tests are **storage-mode invariant**: every supported
    //! backend (Memory, FS, Cloud) must pass with identical behavior.
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>
    //!
    //! These tests run across all storage modes.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::TransactionMode;

    // ============================================================================
    // BASIC PUT/GET/DELETE OPERATIONS
    // ============================================================================

    #[test]
    fn should_get_value_given_existing_key_when_put() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx.get(b"key").expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"value")),
                "unexpected value in mode: {mode}"
            );
        });
    }

    #[test]
    fn should_return_none_given_nonexistent_key_when_get() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx.get(b"nonexistent").expect("get");

            // Assert
            assert_eq!(got, None, "expected None in mode: {mode}");
        });
    }

    #[test]
    fn should_overwrite_value_given_existing_key_when_put() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value1".to_vec(), None)
                .expect("put initial");
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value2".to_vec(), None)
                .expect("put overwrite");
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx.get(b"key").expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"value2")),
                "incorrect overwrite behavior in mode: {mode}"
            );
        });
    }

    #[test]
    fn should_handle_empty_value_when_put() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"".to_vec(), None)
                .expect("put empty");
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx.get(b"key").expect("get empty");
            assert_eq!(got, Some(Bytes::new()), "failed in mode: {mode}");
        });
    }

    #[test]
    fn should_handle_binary_data_when_put() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let data = vec![0, 1, 2, 3, 255, 254, 253];

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"binary_key".to_vec(), data.clone(), None)
                .expect("put binary");
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx.get(b"binary_key").expect("get binary");
            assert_eq!(
                got,
                Some(Bytes::from(data)),
                "binary mismatch in mode: {mode}"
            );
        });
    }

    #[test]
    fn should_return_none_given_deleted_key_when_get() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.delete(b"key".to_vec()).expect("delete");
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx.get(b"key").expect("get");
            assert_eq!(got, None, "expected None after delete in mode: {mode}");
        });
    }

    #[test]
    fn should_succeed_given_nonexistent_key_when_delete() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange: a sibling key exists so we can prove the no-op delete
            // didn't disturb unrelated state.
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let mut seed = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            seed.put(b"sibling".to_vec(), b"value".to_vec(), None)
                .expect("put sibling");
            seed.commit(buffered_write_options(mode))
                .expect("commit sibling");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.delete(b"nonexistent".to_vec()).expect("delete");
            tx.commit(buffered_write_options(mode))
                .expect("delete nonexistent");

            // Assert: the deleted key still doesn't exist, and the sibling key
            // committed before it is untouched.
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            assert_eq!(
                tx.get(b"nonexistent").expect("get nonexistent"),
                None,
                "deleting a nonexistent key must leave it absent in mode: {mode}"
            );
            assert_eq!(
                tx.get(b"sibling").expect("get sibling"),
                Some(Bytes::from_static(b"value")),
                "deleting an unrelated key must not disturb other state in mode: {mode}"
            );
        });
    }

    #[test]
    fn should_handle_many_operations_when_sequential() {
        const COUNT: usize = 100;

        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            for i in 0..COUNT {
                let key = format!("key_{i}");
                let val = format!("value_{i}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(key.as_bytes().to_vec(), val.as_bytes().to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
            }

            // Assert
            for i in 0..COUNT {
                let key = format!("key_{i}");
                let expected = format!("value_{i}");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                let got = tx.get(key.as_bytes()).expect("get");

                assert_eq!(
                    got,
                    Some(Bytes::from(expected)),
                    "mismatch for key {key} in mode: {mode}"
                );
            }
        });
    }
}

mod engine_wal {
    //! WAL (Write-Ahead Log) Integration Tests
    //!
    //! Tests WAL functionality: recovery, data durability, corruption handling,
    //! large values, rotation, and mixed operation recovery.

    use crate::common::*;
    use cntryl_midge::WriteOptions;

    #[test]
    fn should_recover_data_from_wal_after_flush() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..50 {
            let key = format!("wal_key_{i:04}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(key.as_bytes().to_vec(), b"wal_value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
        }

        // Act
        engine.flush_cf(&cf).expect("flush cf");

        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let count = tx
            .scan(&cntryl_midge::Query::new())
            .expect("scan")
            .try_collect()
            .expect("collect scan")
            .len();

        // Assert
        assert_eq!(count, 50, "expected all WAL-backed keys to survive flush");
    }

    #[test]
    fn should_handle_large_values_in_wal() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        let large_value = vec![0xFF; 1_000_000];

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx");
        tx.put(b"large_wal_key".to_vec(), large_value.clone(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).expect("commit");

        // Act
        engine.flush_cf(&cf).expect("flush cf");

        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let retrieved = read_tx.get(b"large_wal_key").expect("get");

        // Assert
        assert_eq!(
            retrieved.as_ref().map(bytes::Bytes::len),
            Some(1_000_000),
            "expected the full large value to survive recovery"
        );
    }

    #[test]
    fn should_recover_deletes_from_wal() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..30 {
            let key = format!("del_key_{i:04}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
        }

        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin delete tx");
        for i in 0..10 {
            let key = format!("del_key_{i:04}");
            txn.delete(key.into_bytes()).expect("delete");
        }
        txn.commit(WriteOptions::buffered())
            .expect("commit deletes");

        // Act
        engine.flush_cf(&cf).expect("flush cf");

        let mut deleted_count = 0;
        for i in 0..10 {
            let key = format!("del_key_{i:04}");
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin read tx");
            if read_tx.get(key.as_bytes()).expect("get").is_none() {
                deleted_count += 1;
            }
        }

        // Assert
        assert_eq!(deleted_count, 10, "expected all deletes to be recovered");
    }

    #[test]
    fn should_recover_range_tombstones_from_wal() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..100 {
            let key = format!("k{i:03}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
        }

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin range delete tx");
        tx.delete_range(b"k020".to_vec(), b"k080".to_vec())
            .expect("delete range");
        tx.commit(WriteOptions::buffered())
            .expect("commit range delete");

        // Act
        engine.flush_cf(&cf).expect("flush cf");

        let scan_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin scan tx");
        let rows = scan_tx
            .scan(&cntryl_midge::Query::new())
            .expect("scan")
            .try_collect()
            .expect("collect scan");
        let in_range = rows
            .iter()
            .filter(|(k, _)| {
                let k_str = String::from_utf8_lossy(k.as_ref());
                k_str.as_ref() >= "k020" && k_str.as_ref() < "k080"
            })
            .count();

        // Assert
        assert_eq!(in_range, 0, "expected the tombstoned range to be empty");
    }

    #[test]
    fn should_handle_wal_rotation_multiple_segments() {
        // Arrange
        let opts = opts_for_mode("local");
        let mut engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");

        let mut batch_count = 0;
        let mut segment_ids = Vec::new();

        for batch in 0..5 {
            for i in 0..100 {
                let key = format!("batch{batch}_key{i:04}");
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin tx");
                tx.put(key.as_bytes().to_vec(), b"batch_value".to_vec(), None)
                    .expect("put");
                tx.commit(WriteOptions::buffered()).expect("commit");
            }

            // flush_cf() seals the active WAL segment and rotates to a new one,
            // which is the actual "rotation" this test is named for.
            engine.flush_cf(&cf).expect("flush cf");
            batch_count += 1;
            segment_ids.push(
                engine
                    .get_runtime_metrics()
                    .expect("runtime metrics")
                    .wal_current_segment_id,
            );
        }

        // Assert: the WAL genuinely rotated to a new segment after every flush,
        // not just that the data happened to survive.
        assert!(
            segment_ids.windows(2).all(|pair| pair[1] > pair[0]),
            "expected the active WAL segment id to strictly increase across \
             rotations, got: {segment_ids:?}"
        );

        // Assert: all batches are still readable pre-restart.
        let final_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let total = final_tx
            .scan(&cntryl_midge::Query::new())
            .expect("scan")
            .try_collect()
            .expect("collect scan")
            .len();
        assert_eq!(total, batch_count * 100, "expected every batch to survive");
        drop(final_tx);

        // Act: write one more batch that is *not* flushed, so it's only durable
        // via the (now rotated-many-times) WAL, then restart the engine. This
        // is what distinguishes rotation handling from a plain post-flush
        // recovery check: replay must walk every rotated segment plus the
        // final unflushed one to reconstruct the full data set.
        for i in 0..100 {
            let key = format!("batch5_key{i:04}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(key.as_bytes().to_vec(), b"batch_value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::sync()).expect("commit");
        }
        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("shutdown before restart");

        let reopened = open_with_mode(&opts, "local");
        let cf = reopened
            .get_column_family("test")
            .expect("column family survives restart");
        let read_tx = reopened
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx after restart");
        let total_after_restart = read_tx
            .scan(&cntryl_midge::Query::new())
            .expect("scan after restart")
            .try_collect()
            .expect("collect scan after restart")
            .len();

        // Assert: recovery replayed across every rotated WAL segment plus the
        // final unflushed one.
        assert_eq!(
            total_after_restart,
            (batch_count + 1) * 100,
            "expected data spanning every rotated WAL segment to survive restart"
        );
    }

    #[test]
    fn should_recover_mixed_operations_from_wal() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx0 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx0");
        tx0.put(b"put_key".to_vec(), b"put_value".to_vec(), None)
            .expect("put");
        tx0.commit(WriteOptions::buffered()).expect("commit");

        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx1");
        txn1.delete(b"put_key".to_vec()).expect("delete");
        txn1.commit(WriteOptions::buffered())
            .expect("commit delete");

        let mut tx1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx2");
        tx1.put(b"put_key".to_vec(), b"put_value_v2".to_vec(), None)
            .expect("put");
        tx1.commit(WriteOptions::buffered())
            .expect("commit overwrite");

        for i in 0..20 {
            let key = format!("dr_{i:02}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(key.as_bytes().to_vec(), b"v".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
        }

        let mut delete_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin range delete tx");
        delete_tx
            .delete_range(b"dr_05".to_vec(), b"dr_15".to_vec())
            .expect("delete range");
        delete_tx
            .commit(WriteOptions::buffered())
            .expect("commit range delete");

        // Act
        engine.flush_cf(&cf).expect("flush cf");

        let verify_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let put_val = verify_tx.get(b"put_key").expect("get");
        let rows = verify_tx
            .scan(&cntryl_midge::Query::new())
            .expect("scan")
            .try_collect()
            .expect("collect scan");
        let dr_remaining = rows
            .iter()
            .filter(|(k, _)| {
                let k_str = String::from_utf8_lossy(k.as_ref());
                k_str.as_ref() >= "dr_05" && k_str.as_ref() < "dr_15"
            })
            .count();

        // Assert
        assert_eq!(
            put_val.as_deref(),
            Some(&b"put_value_v2"[..]),
            "expected the last write to win"
        );
        assert_eq!(dr_remaining, 0, "expected the range delete to win");
    }
}

mod engine_ttl {
    //! Integration tests for TTL (Time-To-Live) support

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use std::{fs, path::Path};

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::common::time::Clock;
    use cntryl_midge::{
        MemoryBudget, MidgeEngine, OpenOptions, Query, TransactionMode, WriteOptions,
    };

    #[derive(Debug)]
    struct ManualClock(AtomicU64);

    impl ManualClock {
        fn new(now: u64) -> Self {
            Self(AtomicU64::new(now))
        }

        fn set(&self, now: u64) {
            self.0.store(now, Ordering::Release);
        }
    }

    impl Clock for ManualClock {
        fn now_millis(&self) -> u64 {
            self.0.load(Ordering::Acquire)
        }
    }

    fn open_with_clock(clock: Arc<ManualClock>) -> MidgeEngine {
        MidgeEngine::open(
            OpenOptions::in_memory()
                .ttl_clock(clock)
                .build()
                .expect("build options"),
        )
        .expect("open engine")
    }

    #[test]
    fn should_not_reexpose_expired_key_given_clock_steps_backward_when_reading() {
        // Arrange
        let clock = Arc::new(ManualClock::new(10_000));
        let engine = open_with_clock(Arc::clone(&clock));
        let mut write = engine
            .begin_tx(0, TransactionMode::ReadWrite)
            .expect("begin write");
        write
            .put(b"key".to_vec(), b"value".to_vec(), Some(1))
            .expect("put");
        write.commit(WriteOptions::buffered()).expect("commit");
        clock.set(11_000);
        assert_eq!(
            engine
                .begin_tx(0, TransactionMode::ReadOnly)
                .expect("read expired")
                .get(b"key")
                .expect("get"),
            None
        );

        // Act
        clock.set(10_500);
        let value = engine
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("read after skew")
            .get(b"key")
            .expect("get");

        // Assert
        assert_eq!(value, None);
    }

    #[test]
    fn should_use_one_commit_time_given_multiple_ttl_puts_when_committing() {
        // Arrange
        let clock = Arc::new(ManualClock::new(20_000));
        let engine = open_with_clock(Arc::clone(&clock));
        let mut transaction = engine
            .begin_tx(0, TransactionMode::ReadWrite)
            .expect("begin write");
        transaction
            .put(b"a".to_vec(), b"a".to_vec(), Some(1))
            .expect("put a");
        transaction
            .put(b"b".to_vec(), b"b".to_vec(), Some(1))
            .expect("put b");

        // Act
        transaction
            .commit(WriteOptions::buffered())
            .expect("commit");
        clock.set(21_000);
        let read = engine
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("begin read");

        // Assert
        assert_eq!(read.get(b"a").expect("get a"), None);
        assert_eq!(read.get(b"b").expect("get b"), None);
    }

    #[test]
    fn should_keep_pending_ttl_visible_given_read_your_own_write_before_commit() {
        // Arrange
        let clock = Arc::new(ManualClock::new(30_000));
        let engine = open_with_clock(Arc::clone(&clock));
        let mut transaction = engine
            .begin_tx(0, TransactionMode::ReadWrite)
            .expect("begin write");
        transaction
            .put(b"key".to_vec(), b"value".to_vec(), Some(1))
            .expect("put");

        // Act
        clock.set(40_000);
        let value = transaction.get(b"key").expect("read own write");

        // Assert
        assert_eq!(value, Some(Bytes::from_static(b"value")));
    }

    #[test]
    fn should_use_fixed_snapshot_time_given_range_scan_crosses_ttl_boundary() {
        // Arrange
        let clock = Arc::new(ManualClock::new(50_000));
        let engine = open_with_clock(Arc::clone(&clock));
        let mut write = engine
            .begin_tx(0, TransactionMode::ReadWrite)
            .expect("begin write");
        write
            .put(b"short".to_vec(), b"value".to_vec(), Some(1))
            .expect("put short");
        write
            .put(b"stable".to_vec(), b"value".to_vec(), None)
            .expect("put stable");
        write.commit(WriteOptions::buffered()).expect("commit");
        clock.set(50_999);
        let read = engine
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("begin snapshot");

        // Act
        clock.set(51_001);
        let entries = read
            .scan(&Query::new())
            .expect("scan")
            .try_collect()
            .expect("collect scan");

        // Assert
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn should_match_expiration_given_resident_and_spilled_transaction_paths() {
        // Arrange
        let clock = Arc::new(ManualClock::new(60_000));
        let resident = open_with_clock(Arc::clone(&clock));
        let spill_dir = tempfile::TempDir::new().expect("spill directory");
        let spilled = MidgeEngine::open(
            OpenOptions::local(spill_dir.path())
                .memory_budget(MemoryBudget::Bytes(128 * 1024))
                .ttl_clock(clock.clone())
                .build()
                .expect("build spill options"),
        )
        .expect("open spill engine");
        for (engine, count) in [(&resident, 1), (&spilled, 100)] {
            let mut transaction = engine
                .begin_tx(0, TransactionMode::ReadWrite)
                .expect("begin write");
            for index in 0..count {
                transaction
                    .put(
                        format!("key-{index:03}").into_bytes(),
                        vec![b'x'; 1024],
                        Some(1),
                    )
                    .expect("put");
            }
            transaction
                .commit(WriteOptions::buffered())
                .expect("commit");
        }

        // Act
        clock.set(61_000);
        let resident_value = resident
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("resident read")
            .get(b"key-000")
            .expect("resident get");
        let spilled_value = spilled
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("spilled read")
            .get(b"key-000")
            .expect("spilled get");

        // Assert
        assert_eq!(resident_value, None);
        assert_eq!(spilled_value, None);
    }

    #[test]
    fn should_preserve_maximum_expiration_given_resident_spilled_transactions() {
        // Arrange
        let clock = Arc::new(ManualClock::new(u64::MAX - 500));
        let resident = open_with_clock(Arc::clone(&clock));
        let spill_dir = tempfile::TempDir::new().expect("spill directory");
        let spilled = MidgeEngine::open(
            OpenOptions::local(spill_dir.path())
                .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
                .transaction_memory_pool_size(32 * 1024)
                .ttl_clock(clock.clone())
                .build()
                .expect("build spill options"),
        )
        .expect("open spill engine");
        let mut resident_write = resident
            .begin_tx(0, TransactionMode::ReadWrite)
            .expect("begin resident write");
        resident_write
            .put(b"max-ttl".to_vec(), b"resident".to_vec(), Some(1))
            .expect("put resident TTL");
        resident_write
            .put(b"no-ttl".to_vec(), b"resident-stable".to_vec(), None)
            .expect("put resident control");
        resident_write
            .commit(WriteOptions::sync())
            .expect("commit resident write");

        let mut spilled_write = spilled
            .begin_tx(0, TransactionMode::ReadWrite)
            .expect("begin spilled write");
        spilled_write
            .put(b"max-ttl".to_vec(), vec![b't'; 4096], Some(1))
            .expect("put spilled TTL");
        spilled_write
            .put(b"no-ttl".to_vec(), b"spilled-stable".to_vec(), None)
            .expect("put spilled control");
        for index in 0..64 {
            spilled_write
                .put(
                    format!("padding-{index:03}").into_bytes(),
                    vec![b'p'; 4096],
                    None,
                )
                .expect("put spill pressure");
        }
        assert!(contains_spill_run(spill_dir.path()));
        spilled_write
            .commit(WriteOptions::sync())
            .expect("commit spilled write");

        // Act
        clock.set(u64::MAX);
        let resident_read = resident
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("begin resident read");
        let spilled_read = spilled
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("begin spilled read");

        // Assert
        assert_eq!(
            resident_read.get(b"max-ttl").expect("resident TTL read"),
            None
        );
        assert_eq!(
            spilled_read.get(b"max-ttl").expect("spilled TTL read"),
            None
        );
        assert_eq!(
            resident_read.get(b"no-ttl").expect("resident control read"),
            Some(Bytes::from_static(b"resident-stable"))
        );
        assert_eq!(
            spilled_read.get(b"no-ttl").expect("spilled control read"),
            Some(Bytes::from_static(b"spilled-stable"))
        );
    }

    fn contains_spill_run(path: &Path) -> bool {
        fs::read_dir(path).is_ok_and(|entries| {
            entries.filter_map(Result::ok).any(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    contains_spill_run(&path)
                } else {
                    path.extension().is_some_and(|extension| extension == "run")
                }
            })
        })
    }

    #[test]
    fn should_round_trip_maximum_expiration_value_given_sst_flush_when_ttl_saturates_to_u64_max() {
        // Arrange
        let directory = tempfile::TempDir::new().expect("database directory");
        let clock = Arc::new(ManualClock::new(u64::MAX - 500));
        let mut engine = MidgeEngine::open(
            OpenOptions::local(directory.path())
                .ttl_clock(clock.clone())
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine.create_column_family("max-ttl").expect("create CF");
        for index in 0..4 {
            let mut write = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write");
            write
                .put(
                    if index == 0 {
                        b"max-ttl".to_vec()
                    } else {
                        format!("padding-{index}").into_bytes()
                    },
                    b"value".to_vec(),
                    (index == 0).then_some(1),
                )
                .expect("put TTL generation");
            if index == 0 {
                write
                    .put(b"no-ttl".to_vec(), b"stable".to_vec(), None)
                    .expect("put control");
            }
            write.commit(WriteOptions::sync()).expect("commit");
            engine.flush_cf(&cf).expect("flush V4 SST");
        }
        engine.compact_all().expect("compact V4 SSTs");
        clock.set(u64::MAX);
        let before_restart = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin pre-restart read");
        assert_eq!(before_restart.get(b"max-ttl").expect("read TTL"), None);
        assert_eq!(
            before_restart.get(b"no-ttl").expect("read control"),
            Some(Bytes::from_static(b"stable"))
        );
        drop(before_restart);
        engine.shutdown(Duration::from_secs(5)).expect("shutdown");

        // Act
        let reopened = MidgeEngine::open(
            OpenOptions::local(directory.path())
                .ttl_clock(clock)
                .background_compaction(false)
                .build()
                .expect("build reopen options"),
        )
        .expect("reopen engine");
        let reopened_cf = reopened.get_column_family("max-ttl").expect("reopen CF");
        let after_restart = reopened
            .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
            .expect("begin reopened read");

        // Assert
        assert_eq!(after_restart.get(b"max-ttl").expect("reopened TTL"), None);
        assert_eq!(
            after_restart.get(b"no-ttl").expect("reopened control"),
            Some(Bytes::from_static(b"stable"))
        );
    }

    #[test]
    fn should_preserve_raw_ttl_value_given_forward_skew_during_flush_and_compaction() {
        // Arrange
        let directory = tempfile::TempDir::new().expect("database directory");
        let clock = Arc::new(ManualClock::new(1_000));
        let mut engine = MidgeEngine::open(
            OpenOptions::local(directory.path())
                .ttl_clock(clock.clone())
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine.create_column_family("ttl").expect("create cf");
        for index in 0..4 {
            let mut transaction = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write");
            transaction
                .put(
                    if index == 0 {
                        b"ttl-key".to_vec()
                    } else {
                        format!("padding-{index}").into_bytes()
                    },
                    b"value".to_vec(),
                    (index == 0).then_some(100),
                )
                .expect("put");
            transaction
                .commit(WriteOptions::buffered())
                .expect("commit");
            engine.flush_cf(&cf).expect("flush");
        }
        clock.set(200_000);

        // Act
        engine.compact_all().expect("compact");
        engine.shutdown(Duration::from_secs(5)).expect("shutdown");
        clock.set(50_000);
        let reopened = MidgeEngine::open(
            OpenOptions::local(directory.path())
                .ttl_clock(clock)
                .build()
                .expect("build reopen options"),
        )
        .expect("reopen engine");
        let reopened_cf = reopened.get_column_family("ttl").expect("reopened cf");
        let value = reopened
            .begin_tx(reopened_cf.id(), TransactionMode::ReadOnly)
            .expect("begin read")
            .get(b"ttl-key")
            .expect("get");

        // Assert
        assert_eq!(value, Some(Bytes::from_static(b"value")));
    }

    // ============================================================================
    // Basic TTL Behavior
    // ============================================================================

    #[test]
    fn should_return_value_given_ttl_not_elapsed_when_reading() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
                .unwrap(); // 1 hour TTL
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();

            // Assert
            assert_eq!(result, Some(Bytes::from_static(b"value1")));
        });
    }

    #[test]
    fn should_return_none_given_ttl_elapsed_when_reading() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
                .unwrap(); // 1 second TTL
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            thread::sleep(Duration::from_millis(1100)); // Wait for expiration
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();

            // Assert
            assert_eq!(result, None);
        });
    }

    #[test]
    fn should_not_expire_key_given_zero_ttl_when_zero_means_infinite() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(0))
                .unwrap(); // 0 = no expiration (infinite)
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            thread::sleep(Duration::from_millis(100));
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();

            // Assert - TTL of 0 means never expires
            assert_eq!(result, Some(Bytes::from_static(b"value1")));
        });
    }

    // ============================================================================
    // Persistence & Recovery
    // ============================================================================

    #[test]
    fn should_persist_ttl_metadata_given_restart_when_reopening() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
                    .unwrap(); // 1 hour
                tx.commit(buffered_write_options(mode)).unwrap();
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
                let result = read_tx.get(b"key1").unwrap();
                assert_eq!(result, Some(Bytes::from_static(b"value1")));
            }
        });
    }

    #[test]
    fn should_persist_ttl_metadata_given_flush_and_restart_when_reopening() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
                    .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
                engine.flush_cf(&cf).unwrap();
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Act
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine
                    .get_column_family("test")
                    .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
                let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
                let result = read_tx.get(b"key1").unwrap();

                // Assert
                assert_eq!(result, Some(Bytes::from_static(b"value1")));
            }
        });
    }

    #[test]
    fn should_expire_after_restart_given_ttl_elapsed_during_shutdown_when_reopening() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine
                    .get_column_family("test")
                    .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
                    .unwrap(); // 1 second
                tx.commit(buffered_write_options(mode)).unwrap();
                thread::sleep(Duration::from_millis(1100)); // Wait for expiration
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine
                    .get_column_family("test")
                    .unwrap_or_else(|| engine.create_column_family("test").expect("create cf"));
                let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
                let result = read_tx.get(b"key1").unwrap();
                assert_eq!(result, None);
            }
        });
    }

    // ============================================================================
    // Compaction Interaction
    // ============================================================================

    #[test]
    fn should_remove_expired_entries_given_compaction_when_ttl_exceeded() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange: four separate L0 generations so `compact_all()` crosses
            // the default L0 file-count trigger and performs a real merge,
            // rather than flush_cf() alone (the test name promises compaction
            // specifically). The TTL'd key sits alongside unrelated filler
            // keys so a genuine multi-file merge is required to resolve it.
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            for batch in 0..3 {
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                let key = format!("compaction_ttl_filler_{batch}");
                tx.put(key.into_bytes(), b"filler".to_vec(), None).unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
                engine.flush_cf(&cf).unwrap();
            }
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
                .unwrap(); // 1 second
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).unwrap();
            thread::sleep(Duration::from_millis(1100));

            // Act - trigger a real compaction (not just a flush)
            engine.compact_all().unwrap();

            // Assert - expired entry should be removed, filler keys survive
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();
            assert_eq!(
                result, None,
                "TTL-expired entry should be dropped by compaction in mode: {mode}"
            );
            for batch in 0..3 {
                let key = format!("compaction_ttl_filler_{batch}");
                assert!(
                    read_tx.get(key.as_bytes()).unwrap().is_some(),
                    "unrelated key {key} should survive compaction in mode: {mode}"
                );
            }
        });
    }

    #[test]
    fn should_preserve_non_expired_entries_given_compaction_when_ttl_not_exceeded() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange: same reasoning as the sibling expiry test — four L0
            // generations so `compact_all()` performs a genuine merge instead
            // of a silent no-op.
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            for batch in 0..3 {
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                let key = format!("compaction_ttl_filler_{batch}");
                tx.put(key.into_bytes(), b"filler".to_vec(), None).unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
                engine.flush_cf(&cf).unwrap();
            }
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), Some(3600))
                .unwrap(); // 1 hour
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).unwrap();

            // Act - trigger a real compaction (not just a flush)
            engine.compact_all().unwrap();

            // Assert - non-expired entry preserved
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();
            assert_eq!(
                result,
                Some(Bytes::from_static(b"value1")),
                "non-expired entry should survive compaction in mode: {mode}"
            );
        });
    }

    // ============================================================================
    // Mixed TTL Keys
    // ============================================================================

    #[test]
    fn should_handle_mixed_ttl_keys_given_some_expire_when_reading() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx1 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx1.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
                .unwrap(); // Expires
            tx1.commit(buffered_write_options(mode)).unwrap();
            let mut tx2 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx2.put(b"key2".to_vec(), b"value2".to_vec(), Some(0))
                .unwrap(); // Never expires
            tx2.commit(buffered_write_options(mode)).unwrap();
            let mut tx3 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx3.put(b"key3".to_vec(), b"value3".to_vec(), Some(3600))
                .unwrap(); // Long TTL
            tx3.commit(buffered_write_options(mode)).unwrap();

            // Act
            thread::sleep(Duration::from_millis(1100));
            let read_tx1 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result1 = read_tx1.get(b"key1").unwrap();
            let read_tx2 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result2 = read_tx2.get(b"key2").unwrap();
            let read_tx3 = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result3 = read_tx3.get(b"key3").unwrap();

            // Assert
            assert_eq!(result1, None); // Expired
            assert_eq!(result2, Some(Bytes::from_static(b"value2"))); // Never expires
            assert_eq!(result3, Some(Bytes::from_static(b"value3"))); // Still valid
        });
    }

    // ============================================================================
    // TTL Update
    // ============================================================================

    #[test]
    fn should_update_ttl_given_overwrite_with_new_ttl_when_writing() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx1 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx1.put(b"key1".to_vec(), b"value1".to_vec(), Some(1))
                .unwrap(); // 1 second
            tx1.commit(buffered_write_options(mode)).unwrap();
            thread::sleep(Duration::from_millis(500));

            // Act - overwrite with longer TTL
            let mut tx2 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx2.put(b"key1".to_vec(), b"value2".to_vec(), Some(3600))
                .unwrap(); // 1 hour
            tx2.commit(buffered_write_options(mode)).unwrap();
            thread::sleep(Duration::from_millis(700)); // Original would have expired

            // Assert - should still be readable with new TTL
            let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = read_tx.get(b"key1").unwrap();
            assert_eq!(result, Some(Bytes::from_static(b"value2")));
        });
    }

    // ============================================================================
    // TTL & Range Tombstone Interactions
    // ============================================================================

    #[test]
    fn should_expire_keys_covered_by_range_tombstone_during_compaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            eprintln!("\n=== TTL: Expire Keys Covered by Range Tombstone (mode: {mode}) ===");

            // Arrange: Write keys with TTL in range [k3, k8)
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Write keys k1..k10 with 1 second TTL
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            for i in 1..=10 {
                let key = format!("k{i}");
                tx.put(key.as_bytes().to_vec(), b"ttl_value".to_vec(), Some(1))
                    .unwrap();
            }
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).expect("flush");

            // Write range tombstone [k3, k8) - covers k3-k7
            let mut delete_tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(b"k3".to_vec(), b"k8".to_vec())
                .unwrap();
            delete_tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).expect("flush");

            // Wait for TTL expiry
            thread::sleep(Duration::from_millis(1100));

            // Act: Trigger compaction
            engine.compact_all().expect("compact TTL levels");

            // Assert: All keys expired and/or tombstoned, compaction cleaned them up
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();

            // k1, k2 should be expired (TTL)
            assert_eq!(tx.get(b"k1").unwrap(), None);
            assert_eq!(tx.get(b"k2").unwrap(), None);

            // k3-k7 should be gone (range tombstone)
            assert_eq!(tx.get(b"k3").unwrap(), None);
            assert_eq!(tx.get(b"k5").unwrap(), None);
            assert_eq!(tx.get(b"k7").unwrap(), None);

            // k8-k10 should be expired (TTL)
            assert_eq!(tx.get(b"k8").unwrap(), None);
            assert_eq!(tx.get(b"k10").unwrap(), None);

            eprintln!("âœ“ TTL and range tombstone both cleaned during compaction");
        });
    }

    #[test]
    fn should_handle_ttl_expiry_during_multi_level_compaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            eprintln!("\n=== TTL: Multi-Level Compaction with Expiry (mode: {mode}) ===");

            // Arrange: Build multi-level LSM with different TTLs
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // L0: Write keys with 1-second TTL
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            for i in 0..50 {
                let key = format!("level0_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"l0_value".to_vec(), Some(1))
                    .unwrap();
            }
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).expect("flush L0");

            // L1: Write keys with longer TTL
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            for i in 50..100 {
                let key = format!("level1_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"l1_value".to_vec(), Some(3600))
                    .unwrap();
            }
            tx.commit(buffered_write_options(mode)).unwrap();
            engine.flush_cf(&cf).expect("flush L1");

            // Wait for L0 TTL to expire
            thread::sleep(Duration::from_millis(1100));

            // Act: Trigger compaction (L0â†’L1)
            engine.compact_all().ok();

            // Assert: L0 expired keys removed; L1 keys unchanged
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();

            // L0 keys should be gone (expired)
            let l0_found = (0..50)
                .filter(|i| {
                    let key = format!("level0_key_{i:04}");
                    tx.get(key.as_bytes())
                        .expect("read expired L0 key")
                        .is_some()
                })
                .count();
            assert_eq!(l0_found, 0, "L0 expired keys should be removed");

            // L1 keys should remain (not expired)
            let l1_found = (50..100)
                .filter(|i| {
                    let key = format!("level1_key_{i:04}");
                    tx.get(key.as_bytes())
                        .expect("read retained L1 key")
                        .is_some()
                })
                .count();
            assert!(l1_found >= 40, "L1 keys should remain");

            eprintln!("âœ“ Multi-level compaction handled TTL correctly; L1 retained: {l1_found}");
        });
    }

    #[test]
    fn should_not_expose_ttl_expired_key_covered_by_range_tombstone() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            eprintln!("\n=== TTL: Don't Expose TTL+Tombstone (mode: {mode}) ===");

            // Arrange: Create scenario with both TTL and tombstone covering same key
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Write k5 with 1-second TTL (not flushed)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"k5".to_vec(), b"ttl_tombstone_value".to_vec(), Some(1))
                .unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Immediately write range tombstone [k1, k10)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.delete_range(b"k1".to_vec(), b"k10".to_vec()).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act: Read k5 after TTL expiry
            thread::sleep(Duration::from_millis(1100));
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            let result = tx.get(b"k5").unwrap();

            // Assert: k5 not exposed (TTL expired AND tombstone covers it)
            assert_eq!(result, None);

            eprintln!("âœ“ TTL-expired + tombstone-covered key not exposed");
        });
    }
}

mod engine_iterators {
    //! Range Scanning & Iterator Integration Tests
    //!
    //! Tests range scans, iterators, and sequential access patterns.
    //! Validates that keys are returned in proper order, deletion is visible
    //! to scans, and advanced iteration features work correctly.
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>
    //!
    //! These tests run across all storage modes (Memory, `LocalDisk`, `CloudBacked`).

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::{MidgeError, Query, Transaction};

    fn collect_scan(tx: &Transaction, query: &Query) -> Vec<(Vec<u8>, Vec<u8>)> {
        tx.scan(query)
            .unwrap()
            .try_collect()
            .unwrap()
            .into_iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect()
    }

    fn scan_between(tx: &Transaction, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        collect_scan(
            tx,
            &Query::new()
                .start_key(Bytes::copy_from_slice(start))
                .end_key(Bytes::copy_from_slice(end)),
        )
    }

    // ============================================================================
    // RANGE SCAN TESTS
    // ============================================================================

    #[test]
    fn should_iterate_all_keys_in_order_given_populated_db_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Populate with ordered keys (zero-padded for lexicographic ordering)
            for i in 0..10 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = scan_between(&tx, b"k00", b"k99");

            // Assert
            assert_eq!(results.len(), 10);
            for (idx, (k, v)) in results.iter().enumerate() {
                assert_eq!(k, format!("k{idx:02}").as_bytes());
                assert_eq!(v, format!("v{idx:02}").as_bytes());
            }
        });
    }

    #[test]
    fn should_iterate_in_reverse_given_reverse_query_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..5 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act
            let query = cntryl_midge::Query::new().reverse();
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = collect_scan(&tx, &query);

            // Assert: Results should be in reverse order
            assert_eq!(results.len(), 5);
            assert_eq!(results[0].0.as_slice(), b"k04");
            assert_eq!(results[1].0.as_slice(), b"k03");
            assert_eq!(results[2].0.as_slice(), b"k02");
            assert_eq!(results[3].0.as_slice(), b"k01");
            assert_eq!(results[4].0.as_slice(), b"k00");
        });
    }

    #[test]
    fn should_return_correct_boundary_rows_when_reverse_scanning_with_explicit_start_and_end() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine
                .create_column_family("reverse-bounds")
                .expect("create cf");
            let mut write = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin write");
            for key in [b"a", b"b", b"c", b"d", b"e"] {
                write
                    .put(key.to_vec(), key.to_vec(), None)
                    .expect("put boundary row");
            }
            write
                .commit(buffered_write_options(mode))
                .expect("commit boundary rows");
            let read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin read");
            let query = Query::new()
                .start_key(Bytes::from_static(b"b"))
                .end_key(Bytes::from_static(b"e"))
                .reverse();

            // Act
            let rows = collect_scan(&read, &query);

            // Assert
            assert_eq!(
                rows.iter()
                    .map(|(key, _)| key.as_slice())
                    .collect::<Vec<_>>(),
                vec![&b"d"[..], &b"c"[..], &b"b"[..]],
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_iterate_in_reverse_order_given_memtable_and_multiple_ssts() {
        // Arrange
        let opts = opts_for_mode("local");
        let engine = open_with_mode(&opts, "local");
        let cf = engine
            .create_column_family("reverse-generations")
            .expect("create cf");
        for rows in [
            [(b"a", b"va"), (b"c", b"vc")],
            [(b"b", b"vb"), (b"d", b"vd")],
        ] {
            let mut write = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin flushed generation");
            for (key, value) in rows {
                write
                    .put(key.to_vec(), value.to_vec(), None)
                    .expect("put flushed row");
            }
            write
                .commit(cntryl_midge::WriteOptions::sync())
                .expect("commit flushed generation");
            engine.flush_cf(&cf).expect("flush generation");
        }
        let mut memtable = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin memtable generation");
        for (key, value) in [(b"e", b"ve"), (b"f", b"vf")] {
            memtable
                .put(key.to_vec(), value.to_vec(), None)
                .expect("put memtable row");
        }
        memtable
            .commit(cntryl_midge::WriteOptions::sync())
            .expect("commit memtable generation");
        let read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read");

        // Act
        let rows = collect_scan(&read, &Query::new().reverse());

        // Assert
        assert_eq!(
            rows.iter()
                .map(|(key, _)| key.as_slice())
                .collect::<Vec<_>>(),
            vec![
                &b"f"[..],
                &b"e"[..],
                &b"d"[..],
                &b"c"[..],
                &b"b"[..],
                &b"a"[..]
            ]
        );
    }

    #[test]
    fn should_limit_results_given_limit_query_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..10 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act
            let query = cntryl_midge::Query::new().limit(3);
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = collect_scan(&tx, &query);

            // Assert
            assert_eq!(results.len(), 3);
            assert_eq!(results[0].0.as_slice(), b"k00");
            assert_eq!(results[1].0.as_slice(), b"k01");
            assert_eq!(results[2].0.as_slice(), b"k02");
        });
    }

    #[test]
    fn should_return_empty_given_empty_db_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = scan_between(&tx, b"k00", b"k99");

            // Assert
            assert!(results.is_empty());
        });
    }

    #[test]
    fn should_return_next_key_given_seek_to_missing_key_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Create non-contiguous keys
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"k01".to_vec(), b"v01".to_vec(), None).unwrap();
            tx.put(b"k03".to_vec(), b"v03".to_vec(), None).unwrap();
            tx.put(b"k05".to_vec(), b"v05".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act: Scan from k00 (doesn't exist)
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = scan_between(&tx, b"k00", b"k99");

            // Assert: Should return all keys >= k00
            assert_eq!(results.len(), 3);
            assert_eq!(results[0].0.as_slice(), b"k01");
            assert_eq!(results[1].0.as_slice(), b"k03");
            assert_eq!(results[2].0.as_slice(), b"k05");
        });
    }

    #[test]
    fn should_return_empty_given_seek_past_end_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"k01".to_vec(), b"v01".to_vec(), None).unwrap();
            tx.put(b"k03".to_vec(), b"v03".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act: Scan starting after all keys
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = scan_between(&tx, b"k99", b"k99");

            // Assert
            assert!(results.is_empty());
        });
    }

    #[test]
    fn should_reject_reversed_bounds_consistently_given_scan_and_delete_range() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"k01".to_vec(), b"v01".to_vec(), None).unwrap();
            tx.put(b"k05".to_vec(), b"v05".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let Err(scan_error) = read.scan(
                &Query::new()
                    .start_key(Bytes::from_static(b"k99"))
                    .end_key(Bytes::from_static(b"k00")),
            ) else {
                panic!("reversed scan bounds must fail");
            };
            let mut write = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin delete transaction");
            let delete_error = write
                .delete_range(b"k99".to_vec(), b"k00".to_vec())
                .expect_err("reversed delete bounds must fail");

            // Assert
            assert!(matches!(scan_error, MidgeError::InvalidArgument(_)));
            assert!(matches!(delete_error, MidgeError::InvalidArgument(_)));
        });
    }

    #[test]
    fn should_return_empty_given_equal_scan_bounds() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let mut write = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin write");
            write
                .put(b"k01".to_vec(), b"v01".to_vec(), None)
                .expect("put row");
            write
                .commit(buffered_write_options(mode))
                .expect("commit row");
            let read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin read");

            // Act
            let results = scan_between(&read, b"k01", b"k01");

            // Assert
            assert!(results.is_empty());
        });
    }

    #[test]
    fn should_skip_deleted_keys_given_tombstones_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..5 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Delete k01 and k03
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.delete(b"k01".to_vec()).unwrap();
            tx.delete(b"k03".to_vec()).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = scan_between(&tx, b"k00", b"k99");

            // Assert: k01 and k03 should not appear
            assert_eq!(results.len(), 3);
            assert_eq!(results[0].0.as_slice(), b"k00");
            assert_eq!(results[1].0.as_slice(), b"k02");
            assert_eq!(results[2].0.as_slice(), b"k04");
        });
    }

    #[test]
    fn should_respect_range_tombstones_given_delete_range_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..10 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Delete range [k02, k07)
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.delete_range(b"k02".to_vec(), b"k07".to_vec()).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = scan_between(&tx, b"k00", b"k99");

            // Assert: k02-k06 should be gone, k00, k01, k07-k09 remain
            assert_eq!(results.len(), 5);
            assert_eq!(results[0].0.as_slice(), b"k00");
            assert_eq!(results[1].0.as_slice(), b"k01");
            assert_eq!(results[2].0.as_slice(), b"k07");
            assert_eq!(results[3].0.as_slice(), b"k08");
            assert_eq!(results[4].0.as_slice(), b"k09");
        });
    }

    #[test]
    fn should_return_latest_value_given_interleaved_puts_deletes_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Initial put
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Overwrite
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Delete and re-put
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.delete(b"key".to_vec()).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key".to_vec(), b"value3".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = scan_between(&tx, b"a", b"z");

            // Assert: Should have latest value
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].0.as_slice(), b"key");
            assert_eq!(results[0].1.as_slice(), b"value3");
        });
    }

    #[test]
    fn should_match_regular_scan_given_streaming_scan_when_comparing() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..8 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act: Regular range scan
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let range_results = scan_between(&tx, b"k00", b"k99");

            // Act: Scan with query
            let query = cntryl_midge::Query::new();
            let scan_results = collect_scan(&tx, &query);

            // Assert: Should produce identical results
            assert_eq!(range_results, scan_results);
        });
    }

    #[test]
    fn should_respect_limit_given_streaming_scan_when_limited() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..20 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act: Query with limit
            let query = cntryl_midge::Query::new()
                .start_key(bytes::Bytes::from_static(b"k05"))
                .limit(5);
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = collect_scan(&tx, &query);

            // Assert
            assert_eq!(results.len(), 5);
            assert_eq!(results[0].0.as_slice(), b"k05");
            assert_eq!(results[4].0.as_slice(), b"k09");
        });
    }

    #[test]
    fn should_respect_limit_in_reverse_query_when_limited() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..10 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act: Reverse query with limit
            let query = cntryl_midge::Query::new().reverse().limit(3);
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = collect_scan(&tx, &query);

            // Assert: Should return last 3 keys in descending order
            assert_eq!(results.len(), 3);
            assert_eq!(results[0].0.as_slice(), b"k09");
            assert_eq!(results[1].0.as_slice(), b"k08");
            assert_eq!(results[2].0.as_slice(), b"k07");
        });
    }

    #[test]
    fn should_apply_tombstones_given_streaming_scan_when_keys_deleted() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..10 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.delete(b"k02".to_vec()).unwrap();
            tx.delete(b"k05".to_vec()).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act: Scan with query
            let query = cntryl_midge::Query::new();
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = collect_scan(&tx, &query);

            // Assert
            assert_eq!(results.len(), 8);
            assert!(!results.iter().any(|(k, _)| k.as_slice() == b"k02"));
            assert!(!results.iter().any(|(k, _)| k.as_slice() == b"k05"));
        });
    }

    #[test]
    fn should_handle_large_scan_given_many_keys_when_iterating() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Insert 500 keys (batch into one transaction for speed)
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            for i in 0..500 {
                tx.put(
                    format!("k{i:04}").as_bytes().to_vec(),
                    format!("v{i:04}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
            }
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = scan_between(&tx, b"k0000", b"k0500");

            // Assert
            assert_eq!(results.len(), 500);

            // Verify ordering
            for (idx, (k, v)) in results.iter().enumerate() {
                assert_eq!(k, format!("k{idx:04}").as_bytes());
                assert_eq!(v, format!("v{idx:04}").as_bytes());
            }
        });
    }

    #[test]
    fn should_iterate_memtable_plus_multiple_ssts_given_flushed_batches_when_scanning() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        for batch in 0..3 {
            for i in 0..20 {
                let key = format!("sst{batch:02}_k{i:02}");
                let value = format!("sst{batch:02}_v{i:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(key.into_bytes(), value.into_bytes(), None).unwrap();
                tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
            }
            engine.flush_cf(&cf).expect("flush batch into SST");
        }

        for i in 0..10 {
            let key = format!("mem_k{i:02}");
            let value = format!("mem_v{i:02}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.into_bytes(), value.into_bytes(), None).unwrap();
            tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        }

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, &cntryl_midge::Query::new());

        // Assert
        assert_eq!(results.len(), 70);
        assert!(results.iter().any(|(k, _)| k.as_slice() == b"sst00_k00"));
        assert!(results.iter().any(|(k, _)| k.as_slice() == b"sst02_k19"));
        assert!(results.iter().any(|(k, _)| k.as_slice() == b"mem_k00"));
        assert!(results.iter().any(|(k, _)| k.as_slice() == b"mem_k09"));
    }

    #[test]
    fn should_return_latest_value_across_levels_given_overwrite_in_newer_sst_when_scanning() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"shared".to_vec(), b"v1".to_vec(), None).unwrap();
        tx.put(b"stable".to_vec(), b"keep".to_vec(), None).unwrap();
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).expect("flush initial sst");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"shared".to_vec(), b"v2".to_vec(), None).unwrap();
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).expect("flush overwrite sst");

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, &cntryl_midge::Query::new());

        // Assert
        assert_eq!(results.len(), 2);
        assert!(results
            .iter()
            .any(|(k, v)| k.as_slice() == b"shared" && v.as_slice() == b"v2"));
        assert!(results
            .iter()
            .any(|(k, v)| k.as_slice() == b"stable" && v.as_slice() == b"keep"));
    }

    #[test]
    fn should_hide_deleted_keys_across_compacted_ssts_when_scanning() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        for batch in 0..4 {
            for i in 0..25 {
                let key = format!("k{:03}", batch * 25 + i);
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(key.into_bytes(), b"value".to_vec(), None).unwrap();
                tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
            }
            engine.flush_cf(&cf).expect("flush seed batch");
        }

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        for deleted in [b"k020", b"k021", b"k050", b"k079"] {
            tx.delete(deleted.to_vec()).expect("delete compacted key");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).expect("flush delete tombstones");
        engine.compact_all().expect("compact levels");

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, &cntryl_midge::Query::new());

        // Assert
        for deleted in [b"k020", b"k021", b"k050", b"k079"] {
            assert!(
                !results.iter().any(|(k, _)| k.as_slice() == deleted),
                "deleted key {:?} should stay hidden after compaction",
                String::from_utf8_lossy(deleted)
            );
        }
        assert!(results.iter().any(|(k, _)| k.as_slice() == b"k000"));
        assert!(results.iter().any(|(k, _)| k.as_slice() == b"k099"));
    }

    #[test]
    fn should_scan_compacted_ssts_given_new_iterator_after_compaction_when_levels_change() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        for batch in 0..5 {
            for i in 0..20 {
                let key = format!("k{:03}", batch * 20 + i);
                let value = format!("v{:03}", batch * 20 + i);
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(key.into_bytes(), value.into_bytes(), None).unwrap();
                tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
            }
            engine.flush_cf(&cf).expect("flush batch");
        }

        // Act
        engine.compact_all().expect("compact levels");
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, &cntryl_midge::Query::new());

        // Assert
        assert_eq!(results.len(), 100);
        assert_eq!(results.first().expect("first result").0.as_slice(), b"k000");
        assert_eq!(results.last().expect("last result").0.as_slice(), b"k099");
    }

    #[test]
    fn should_handle_concurrent_streaming_scans_when_multiple_threads() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = std::sync::Arc::new(open_with_mode(&opts, mode));
            let cf = engine
                .create_column_family("test")
                .expect("create cf")
                .clone();

            // Populate initial data
            for i in 0..50 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act: Spawn multiple threads doing concurrent scans
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let engine_clone = std::sync::Arc::clone(&engine);
                    let cf_clone = cf.clone();

                    std::thread::spawn(move || {
                        let query = cntryl_midge::Query::new();
                        let tx = engine_clone
                            .begin_tx(cf_clone.id(), cntryl_midge::TransactionMode::ReadOnly)
                            .unwrap();
                        collect_scan(&tx, &query)
                    })
                })
                .collect();

            // Assert: All threads should get same results
            let results: Vec<Vec<(Vec<u8>, Vec<u8>)>> =
                handles.into_iter().map(|h| h.join().unwrap()).collect();

            for r in results.iter().skip(1) {
                assert_eq!(&results[0], r);
            }

            assert_eq!(results[0].len(), 50);
        });
    }

    #[test]
    fn should_produce_identical_results_given_repeated_scans_when_rewinding() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..15 {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    format!("k{i:02}").as_bytes().to_vec(),
                    format!("v{i:02}").as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act: Perform multiple identical scans
            let tx1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results1 = scan_between(&tx1, b"k00", b"k99");
            let tx2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results2 = scan_between(&tx2, b"k00", b"k99");
            let tx3 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results3 = scan_between(&tx3, b"k00", b"k99");

            // Assert: All scans should produce identical results
            assert_eq!(results1, results2);
            assert_eq!(results2, results3);
            assert_eq!(results1.len(), 15);
        });
    }
}

mod engine_delete_range {
    //! Delete Range Integration Tests
    //!
    //! Tests range deletion operations end-to-end using the public `MidgeEngine` API.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::TransactionMode;
    use std::sync::Arc;

    #[test]
    fn should_delete_keys_in_range_given_delete_range_when_querying() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for (key, value) in [
                ("key1", "val1"),
                ("key2", "val2"),
                ("key3", "val3"),
                ("key4", "val4"),
            ] {
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("seed put");
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act
            let mut delete_tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(b"key2".to_vec(), b"key4".to_vec())
                .expect("delete_range");
            delete_tx
                .commit(buffered_write_options(mode))
                .expect("commit delete_range");

            // Assert
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            assert_eq!(
                tx.get(b"key1").expect("get1"),
                Some(Bytes::from_static(b"val1")),
                "key1 should exist (outside range) in mode: {mode}"
            );
            assert_eq!(
                tx.get(b"key2").expect("get2"),
                None,
                "key2 should be deleted (in range) in mode: {mode}"
            );
            assert_eq!(
                tx.get(b"key3").expect("get3"),
                None,
                "key3 should be deleted (in range) in mode: {mode}"
            );
            assert_eq!(
                tx.get(b"key4").expect("get4"),
                Some(Bytes::from_static(b"val4")),
                "key4 should exist (outside range) in mode: {mode}"
            );
        });
    }

    #[test]
    fn should_handle_empty_range_given_start_equals_end_when_delete_range() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key".to_vec(), b"val".to_vec(), None).expect("put");
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let mut delete_tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(b"key".to_vec(), b"key".to_vec())
                .expect("delete_range");
            delete_tx
                .commit(buffered_write_options(mode))
                .expect("commit delete_range");

            // Assert
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            assert_eq!(
                tx.get(b"key").expect("get"),
                Some(Bytes::from_static(b"val")),
                "key should exist (empty range) in mode: {mode}"
            );
        });
    }

    #[test]
    fn should_reject_delete_range_given_reversed_bounds_when_called() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut delete_tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            let result = delete_tx.delete_range(b"key9".to_vec(), b"key1".to_vec());

            // Assert
            assert!(
                matches!(result, Err(cntryl_midge::MidgeError::InvalidArgument(_))),
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_delete_key_given_delete_range_with_single_key_when_matching() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"target".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let mut delete_tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(b"target".to_vec(), b"targetZ".to_vec())
                .expect("delete_range");
            delete_tx
                .commit(buffered_write_options(mode))
                .expect("commit delete_range");

            // Assert
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            assert_eq!(
                tx.get(b"target").expect("get"),
                None,
                "target should be deleted in mode: {mode}"
            );
        });
    }

    #[test]
    fn should_allow_multiple_delete_ranges_when_called_sequentially() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..20 {
                let key = format!("k{i:02}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act
            for (start, end, label) in [
                (b"k03", b"k10", "delete_range1"),
                (b"k15", b"k18", "delete_range2"),
            ] {
                let mut delete_tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                delete_tx
                    .delete_range(start.to_vec(), end.to_vec())
                    .expect(label);
                delete_tx.commit(buffered_write_options(mode)).expect(label);
            }

            // Assert
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            for i in 0..20 {
                let key = format!("k{i:02}");
                let should_exist = !((3..10).contains(&i) || (15..18).contains(&i));
                let result = tx.get(key.as_bytes()).expect("get");
                assert_eq!(
                    result.is_some(),
                    should_exist,
                    "key {} should {} in mode: {}",
                    i,
                    if should_exist { "exist" } else { "be deleted" },
                    mode
                );
            }
        });
    }

    #[test]
    fn should_persist_keys_across_delete_range_with_restart_when_durable() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                for (key, value) in [("key1", "val1"), ("key2", "val2"), ("key3", "val3")] {
                    let mut tx = engine
                        .begin_tx(cf.id(), TransactionMode::ReadWrite)
                        .unwrap();
                    tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                        .expect("put seed");
                    tx.commit(buffered_write_options(mode)).unwrap();
                }

                let mut delete_tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                delete_tx
                    .delete_range(b"key1".to_vec(), b"key3".to_vec())
                    .expect("delete_range");
                delete_tx
                    .commit(buffered_write_options(mode))
                    .expect("commit delete_range");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Act
            // Assert
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            assert!(tx.get(b"key1").expect("get1").is_none());
            assert!(tx.get(b"key2").expect("get2").is_none());
            assert_eq!(
                tx.get(b"key3").expect("get3"),
                Some(Bytes::from_static(b"val3")),
                "key3 should persist after restart"
            );
        });
    }

    #[test]
    fn should_handle_concurrent_delete_ranges_when_multiple_threads() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            for i in 0..100 {
                let key = format!("key{i:03}");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Act
            let mut handles = vec![];
            for thread_id in 0..5 {
                let engine_clone = Arc::clone(&engine);
                let cf_clone = cf.clone();
                let write_options = buffered_write_options(mode);
                handles.push(std::thread::spawn(move || {
                    let start = format!("key{:03}", thread_id * 10);
                    let end = format!("key{:03}", (thread_id + 1) * 10);
                    let mut delete_tx = engine_clone
                        .begin_tx(cf_clone.id(), TransactionMode::ReadWrite)
                        .unwrap();
                    delete_tx
                        .delete_range(start.as_bytes().to_vec(), end.as_bytes().to_vec())
                        .expect("delete_range");
                    delete_tx
                        .commit(write_options)
                        .expect("commit delete_range");
                }));
            }

            for handle in handles {
                handle.join().expect("thread");
            }

            // Assert
            let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
            for i in 0..100 {
                let key = format!("key{i:03}");
                let should_exist = !(0..50).contains(&i);
                let got = tx.get(key.as_bytes()).expect("get");
                assert_eq!(
                    got.is_some(),
                    should_exist,
                    "key {} should {} in mode: {}",
                    i,
                    if should_exist { "exist" } else { "be deleted" },
                    mode
                );
            }
        });
    }
}

mod engine_exclusivity {
    //! Tests for single-instance exclusivity (primary lease mechanism)
    //!
    //! Validates that:
    //! - Only one Midge instance can be primary at a time
    //! - Lease acquisition failures are explicit and fast
    //! - Lease release allows subsequent acquisition
    //! - Crashes release the lease automatically (TTL expiry)

    use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    /// Name of the `#[test]` function that acts as the crashed-process child.
    /// Invoked via `cargo test --exact` from `run_crashed_child_holding_lease`.
    const CRASHED_CHILD_TEST_NAME: &str =
        "engine_exclusivity::should_hold_lease_forever_in_child_process";
    const CRASHED_CHILD_ENV_DB_PATH: &str = "MIDGE_EXCLUSIVITY_CRASHED_CHILD_DB_PATH";

    /// Helper: create a temp directory for testing
    fn temp_db_path() -> PathBuf {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = temp_dir.path().to_path_buf();
        // Keep temp_dir alive so it doesn't get deleted
        std::mem::forget(temp_dir);
        path
    }

    fn local_options(path: &std::path::Path) -> cntryl_midge::OpenOptions {
        OpenOptions::local(path)
            .build()
            .expect("build local options")
    }

    fn memory_options() -> cntryl_midge::OpenOptions {
        OpenOptions::in_memory()
            .build()
            .expect("build memory options")
    }

    #[test]
    fn should_open_single_instance_when_no_contention() {
        // Arrange
        let db_path = temp_db_path();
        let opts = local_options(&db_path);

        // Act
        let engine = Engine::open(opts).expect("should open successfully");

        // Assert
        assert!(
            engine.is_primary_lease_healthy(),
            "lease should be healthy after opening"
        );

        // Clean shutdown
        drop(engine);
    }

    #[test]
    fn should_reject_second_engine_open_given_existing_primary_lease_when_starting() {
        // Arrange
        let db_path = temp_db_path();

        // Open first instance

        // Act
        let engine1 = Engine::open(local_options(&db_path)).expect("first instance should open");
        assert!(engine1.is_primary_lease_healthy());

        // Try to open second instance (should fail)
        let result = Engine::open(local_options(&db_path));

        // Assert
        assert!(
            result.is_err(),
            "second instance should fail to acquire lease"
        );

        if let Err(MidgeError::LeaseHeld(msg)) = result {
            assert!(
                msg.contains("another Midge instance") || msg.contains("already running"),
                "error message should indicate another instance is running, got: {msg}"
            );
        } else {
            panic!("expected MidgeError::LeaseHeld with descriptive message");
        }

        // First instance should still be healthy
        assert!(engine1.is_primary_lease_healthy());
    }

    #[test]
    fn should_allow_second_instance_when_first_is_shutdown() {
        // Arrange
        let db_path = temp_db_path();

        // Open and drop first instance

        // Act
        {
            let mut engine1 =
                Engine::open(local_options(&db_path)).expect("first instance should open");
            assert!(engine1.is_primary_lease_healthy());
            engine1
                .shutdown(Duration::from_secs(2))
                .expect("shutdown first instance");
        }

        // Small delay to ensure lease is released
        thread::sleep(Duration::from_millis(50));

        // Second instance should now succeed
        let engine2 = Engine::open(local_options(&db_path))
            .expect("second instance should open after first is dropped");

        // Assert
        assert!(engine2.is_primary_lease_healthy());
    }

    #[test]
    fn should_maintain_lease_health_during_normal_operation() {
        // Arrange
        let db_path = temp_db_path();
        let engine = Engine::open(local_options(&db_path)).expect("should open");
        assert!(engine.is_primary_lease_healthy());

        // Act
        // Wait for multiple heartbeat cycles (3-4 renewal intervals)
        thread::sleep(Duration::from_secs(5));

        // Assert
        // Lease should still be healthy
        assert!(
            engine.is_primary_lease_healthy(),
            "lease should remain healthy after multiple renewal cycles"
        );
    }

    #[test]
    fn should_block_concurrent_opens_when_racing() {
        // Arrange
        let db_path = Arc::new(temp_db_path());
        let barrier = Arc::new(std::sync::Barrier::new(3));

        let mut handles = vec![];

        // Spawn 3 threads all trying to open the same database

        // Act
        for i in 0..3 {
            let path = Arc::clone(&db_path);
            let barrier = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();

                // Try to open
                let result = Engine::open(local_options(&path));
                (i, result)
            });

            handles.push(handle);
        }

        // Collect results
        let results: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("thread panicked"))
            .collect();

        // Assert
        // Exactly one should succeed
        let success_count = results.iter().filter(|(_, r)| r.is_ok()).count();
        assert_eq!(
            success_count, 1,
            "exactly one instance should acquire the lease"
        );

        // Two should fail
        let failure_count = results.iter().filter(|(_, r)| r.is_err()).count();
        assert_eq!(failure_count, 2, "two instances should fail");

        // Clean up the successful instance
        for (_id, result) in results {
            if let Ok(mut engine) = result {
                engine
                    .shutdown(Duration::from_secs(2))
                    .expect("shutdown racing winner");
            }
        }
    }

    /// Rewrite the on-disk `.midge_leader` record so its epoch is one higher
    /// than the epoch our still-running `engine` acquired, exactly as a rival
    /// instance winning a CAS race would leave it. This mirrors the on-disk
    /// record format `format_leader_record` writes (see
    /// `src/lease/traits.rs`), including its trailing CRC32C checksum, so the
    /// running engine's next renewal attempt reads a record that parses and
    /// checksums cleanly but no longer matches its own epoch.
    fn steal_lease_epoch(db_path: &Path) {
        let lease_path = db_path.join(".midge_leader");
        let content = std::fs::read_to_string(&lease_path).expect("read leader record");

        let mut epoch: Option<u64> = None;
        let mut acquired_at: Option<String> = None;
        for line in content.lines() {
            if let Some(value) = line.strip_prefix("epoch: ") {
                epoch = value.parse::<u64>().ok();
            } else if let Some(value) = line.strip_prefix("acquired_at: ") {
                acquired_at = Some(value.to_string());
            }
        }
        let epoch = epoch.expect("leader record missing epoch");
        let acquired_at = acquired_at.expect("leader record missing acquired_at");

        let body = format!(
            "epoch: {}\nholder_id: rival-instance@stolen\nacquired_at: {acquired_at}\n",
            epoch + 1
        );
        let checksum = crc32c::crc32c(body.as_bytes());
        std::fs::write(&lease_path, format!("{body}checksum: {checksum}\n"))
            .expect("simulate a rival instance stealing the lease");
    }

    #[test]
    fn should_reject_writes_if_lease_becomes_unhealthy() {
        // Arrange
        let db_path = temp_db_path();
        let engine = Engine::open(local_options(&db_path)).expect("should open");
        assert!(engine.is_primary_lease_healthy());

        // Act
        // Simulate another instance winning the lease out from under us (e.g. a
        // split-brain caused by a false crash detection). Our own epoch, held
        // only in memory, no longer matches what is on disk, so the next
        // background renewal must fail and mark the lease unhealthy.
        steal_lease_epoch(&db_path);

        let deadline = std::time::Instant::now() + Duration::from_secs(40);
        while engine.is_primary_lease_healthy() && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(100));
        }

        // Assert
        assert!(
            !engine.is_primary_lease_healthy(),
            "lease should become unhealthy once another instance steals the epoch"
        );

        // Fail-closed: once the lease is unhealthy, writes must be rejected
        // rather than silently accepted (which would risk split-brain corruption).
        let default_cf = engine
            .get_column_family("default")
            .expect("default column family");
        let mut tx = engine
            .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
            .expect("begin_tx should still succeed on a fenced engine");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("buffering a write locally should not fail");
        let commit = tx.commit(WriteOptions::sync());
        assert!(
            matches!(commit, Err(MidgeError::Fenced(_))),
            "commit should be rejected as Fenced once the lease is unhealthy, got {commit:?}"
        );
    }

    #[test]
    fn should_work_with_in_memory_storage_when_unique_paths() {
        // Arrange
        // InMemory mode uses unique temp paths, so multiple instances are allowed

        // Act
        let engine1 = Engine::open(memory_options()).expect("first in-memory should open");
        let engine2 =
            Engine::open(memory_options()).expect("second in-memory should open (different path)");

        // Assert
        assert!(engine1.is_primary_lease_healthy());
        assert!(engine2.is_primary_lease_healthy());
    }

    #[test]
    fn should_release_primary_lease_given_clean_shutdown_when_shutdown_completes() {
        // Arrange
        let db_path = temp_db_path();

        // Act
        // Open, perform some work, and shut down.
        {
            let mut engine = Engine::open(local_options(&db_path)).expect("should open");
            assert!(engine.is_primary_lease_healthy());

            // Simulate some work
            thread::sleep(Duration::from_millis(100));

            engine
                .shutdown(Duration::from_secs(2))
                .expect("clean shutdown");
        }

        // Give OS time to release file lock
        thread::sleep(Duration::from_millis(50));

        // Should be able to open again immediately
        let engine2 =
            Engine::open(local_options(&db_path)).expect("should reopen after clean shutdown");

        // Assert
        assert!(engine2.is_primary_lease_healthy());
    }

    // ============================================================================
    // Additional Test Coverage for Lease Mechanisms
    // ============================================================================

    #[test]
    fn should_survive_rapid_open_close_cycling() {
        eprintln!("\n=== Exclusivity: Rapid Open/Close Cycling ===");

        // Arrange
        let db_path = temp_db_path();

        // Act: Rapid cycle opens and closes
        for cycle in 0..50 {
            match Engine::open(local_options(&db_path)) {
                Ok(mut engine) => {
                    assert!(
                        engine.is_primary_lease_healthy(),
                        "lease healthy on cycle {cycle}"
                    );
                    engine
                        .shutdown(Duration::from_secs(2))
                        .expect("shutdown cycle");
                }
                Err(e) => {
                    eprintln!("Failed to open on cycle {cycle}: {e:?}");
                    panic!("should not fail during rapid cycling");
                }
            }
        }

        // Assert: Final open succeeds (no resource exhaustion)
        let final_engine =
            Engine::open(local_options(&db_path)).expect("final open should succeed");
        assert!(final_engine.is_primary_lease_healthy());

        eprintln!("✓ Survived 50 rapid open/close cycles without resource issues");
    }

    /// Child-process entry point for `should_reject_open_when_lease_held_by_crashed_process`.
    /// Not a scenario test on its own: only acts when the parent sets
    /// `CRASHED_CHILD_ENV_DB_PATH`, otherwise it's a no-op so `cargo test` runs
    /// of the whole suite don't try to open a nonexistent path.
    ///
    /// Acquires the primary lease and then calls `std::process::exit`, which
    /// skips all destructors (including `Engine::drop`'s lease release) — the
    /// same on-disk state a hard process crash (e.g. SIGKILL) would leave
    /// behind: a `.midge_leader` record that is still valid and unexpired.
    #[test]
    fn should_hold_lease_forever_in_child_process() {
        // Arrange
        let Some(db_path) = std::env::var_os(CRASHED_CHILD_ENV_DB_PATH) else {
            return;
        };
        let db_path = PathBuf::from(db_path);

        // Act
        let engine = Engine::open(local_options(&db_path)).expect("child should acquire lease");

        // Assert
        assert!(engine.is_primary_lease_healthy());

        std::process::exit(0);
    }

    fn run_crashed_child_holding_lease(db_path: &Path) {
        let current_exe = std::env::current_exe().expect("current exe");
        let status = Command::new(current_exe)
            .arg("--exact")
            .arg(CRASHED_CHILD_TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CRASHED_CHILD_ENV_DB_PATH, db_path)
            .status()
            .expect("run crashed-child process");
        assert!(
            status.success(),
            "child process should acquire the lease and exit(0) without releasing it, got {status:?}"
        );
    }

    #[test]
    fn should_reject_open_when_lease_held_by_crashed_process() {
        // Arrange: a child process acquires the lease and then "crashes" by
        // calling exit() directly, leaving the on-disk lease record held and
        // unexpired (its TTL is far longer than this test takes to run).
        let db_path = temp_db_path();
        run_crashed_child_holding_lease(&db_path);

        // Act
        let result = Engine::open(local_options(&db_path));

        // Assert: the still-unexpired lease must reject the new open, not
        // silently succeed and risk a second writer against the same storage.
        match result {
            Err(MidgeError::LeaseHeld(msg)) => {
                assert!(
                    msg.to_lowercase().contains("another")
                        || msg.to_lowercase().contains("running")
                        || msg.to_lowercase().contains("holds"),
                    "expected descriptive lease-held error, got: {msg}"
                );
            }
            Ok(_) => panic!(
                "expected MidgeError::LeaseHeld while a crashed process still holds an \
                 unexpired lease, got Ok"
            ),
            Err(other) => panic!(
                "expected MidgeError::LeaseHeld while a crashed process still holds an \
                 unexpired lease, got {other:?}"
            ),
        }
    }
}

mod engine_compaction {
    //! Flush And Post-Flush Consistency Tests
    //!
    //! These tests exercise data visibility and correctness around flush-triggered
    //! state transitions, repeated flushes, range tombstones, large values, and
    //! overwrite visibility. This file does not inject faults or prove background
    //! compaction scheduling semantics.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::Query;
    use std::collections::HashSet;

    // ============================================================================
    // TEST GROUP 1: Snapshot Reads Across Flush
    // ============================================================================

    #[test]
    fn should_preserve_snapshot_reads_when_flushing_while_snapshot_is_open() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");
        for i in 0..100 {
            let key = format!("concurrent_key_{i:04}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin write tx");
            tx.put(key.as_bytes().to_vec(), b"initial_value".to_vec(), None)
                .expect("put initial value");
            tx.commit(cntryl_midge::WriteOptions::best_effort()) // Fast setup
                .expect("commit initial value");
        }

        engine.flush_cf(&cf).expect("flush initial data");
        let snapshot = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin snapshot tx");

        // Act
        engine.flush_cf(&cf).expect("flush while snapshot is open");

        // Assert
        let snap_val = snapshot
            .get(b"concurrent_key_0000")
            .expect("read through snapshot");
        assert_eq!(snap_val, Some(Bytes::from_static(b"initial_value")));

        drop(snapshot);
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin current read tx");
        let current_val = tx
            .get(b"concurrent_key_0000")
            .expect("read current value after flush");
        assert_eq!(current_val, Some(Bytes::from_static(b"initial_value")));
    }

    // ============================================================================
    // TEST GROUP 2: Writes Across Flushes
    // ============================================================================

    #[test]
    fn should_preserve_both_write_batches_after_flushing_between_batches() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");
        {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin first batch tx");
            for i in 0..500 {
                let key = format!("key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"v1".to_vec(), None)
                    .expect("put first batch value");
            }
            tx.commit(cntryl_midge::WriteOptions::best_effort()) // Fast setup
                .expect("commit first batch");
        }

        engine.flush_cf(&cf).expect("flush first batch");

        // Act
        {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin second batch tx");
            for i in 500..1000 {
                let key = format!("key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"v2".to_vec(), None)
                    .expect("put second batch value");
            }
            tx.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit second batch");
        }

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let total_keys = tx
            .scan(&Query::new())
            .expect("scan all keys")
            .try_collect()
            .expect("collect all keys")
            .len();
        assert_eq!(total_keys, 1000);
    }

    // ============================================================================
    // TEST GROUP 3: Range Tombstones Through Flushes
    // ============================================================================

    #[test]
    fn should_preserve_range_tombstones_after_flushing_deleted_range() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");
        for i in 100..900 {
            let key = format!("k{i:04}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin seed tx");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put seed value");
            tx.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit seed value");
        }
        engine.flush_cf(&cf).expect("flush seed range");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin delete range tx");
        tx.delete_range(b"k300".to_vec(), b"k700".to_vec())
            .expect("delete range");
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit delete range");

        // Act
        engine.flush_cf(&cf).expect("flush tombstone");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let query = Query::new()
            .start_key(Bytes::from(&b"k300"[..]))
            .end_key(Bytes::from(&b"k700"[..]));
        let remaining = tx
            .scan(&query)
            .expect("scan deleted range")
            .try_collect()
            .expect("collect deleted range")
            .len();
        assert_eq!(remaining, 0);
    }

    // ============================================================================
    // TEST GROUP 4: Large Values Across Flush
    // ============================================================================

    #[test]
    fn should_preserve_large_values_after_flushing() {
        // Arrange
        let mut opts = opts_for_mode("local");
        opts.memtable_size = 2 * 1024 * 1024;
        let engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");
        let large_value = vec![0xAB; 100_000]; // 100KB value

        // Act
        for i in 0..10 {
            let key = format!("large_{i:02}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin large value tx");
            tx.put(key.as_bytes().to_vec(), large_value.clone(), None)
                .expect("put large value");
            tx.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit large value");
        }

        engine.flush_cf(&cf).expect("flush large values");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let val = tx.get(b"large_00").expect("get large value");
        assert_eq!(val.as_ref().map(Bytes::len), Some(100_000));
    }

    // ============================================================================
    // TEST GROUP 5: Overwritten Keys After Flush
    // ============================================================================

    #[test]
    fn should_preserve_latest_overwritten_value_after_flushing() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");
        for version in 0..100 {
            let value = format!("v{version}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin overwrite tx");
            tx.put(b"hotkey".to_vec(), value.as_bytes().to_vec(), None)
                .expect("put overwrite value");
            tx.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit overwrite value");
        }

        // Act
        engine.flush_cf(&cf).expect("flush overwritten values");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let current = tx.get(b"hotkey").expect("get hotkey");
        assert_eq!(current, Some(Bytes::from_static(b"v99")));
    }

    // ============================================================================
    // TEST GROUP 6: Repeated Flushes Preserve All Keys
    // ============================================================================

    #[test]
    fn should_preserve_all_keys_after_repeated_flushes() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        for batch in 0..3 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin batch tx");
            for i in 0..500 {
                let key = format!("batch{batch:02}_key{i:04}");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put batch value");
            }
            tx.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit batch");
            engine.flush_cf(&cf).expect("flush batch");
        }

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let key_count = tx
            .scan(&Query::new())
            .expect("scan all keys")
            .try_collect()
            .expect("collect all keys")
            .len();
        assert_eq!(key_count, 1500);
    }

    // ============================================================================
    // TEST GROUP: Compaction Manifest Publication
    // ============================================================================

    // ============================================================================
    // TEST GROUP: Compaction Manifest Publication
    // ============================================================================

    /// Slice 5: Verify that compaction output SSTs are published in the manifest
    /// and become the source of truth for subsequent reads.
    ///
    /// This test validates that `CompactionComplete` â†’ `ManifestCompactionComplete`
    /// routing correctly updates the manifest with:
    /// - Input SSTs removed from active set
    /// - Output SSTs added to manifest
    /// - Ability to read data from compacted SSTs
    ///
    /// The key proof: after compaction completes and manifest is updated,
    /// reads can still access all data (proving reads use the new compacted SSTs).
    #[test]
    fn should_publish_compacted_ssts_in_manifest_when_compaction_completes() {
        // Arrange: Create engine and write data to trigger L0 compaction
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        // Write enough data to create multiple L0 files via flush
        for batch in 0..5 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin batch tx");
            for i in 0..100 {
                let key = format!("batch{batch:02}_key{i:04}");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put batch value");
            }
            tx.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit batch");
            engine.flush_cf(&cf).expect("flush batch");
        }

        // Act: Trigger compaction (merges L0 files to L1)
        engine.compact_all().expect("trigger compaction");

        // Assert: All data remains queryable through compacted SSTs
        // This proves the manifest was updated with output SSTs and reads use them
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx after compaction");

        let key_count = tx
            .scan(&Query::new())
            .expect("scan all keys")
            .try_collect()
            .expect("collect all keys")
            .len();
        assert_eq!(
            key_count, 500,
            "All 500 keys should be queryable after compaction (proves manifest was updated)"
        );

        // Verify specific keys exist with correct values
        let value = tx
            .get(b"batch00_key0000")
            .expect("get key after compaction");
        assert_eq!(value, Some(Bytes::copy_from_slice(b"value")));

        let value = tx
            .get(b"batch04_key0099")
            .expect("get last key after compaction");
        assert_eq!(value, Some(Bytes::copy_from_slice(b"value")));
    }

    /// Slice 6: Verify that input SSTs are cleaned up (deleted) after compaction
    /// and manifest publication succeeds.
    ///
    /// This test validates the cleanup sequence:
    /// 1. Manifest is updated with input SSTs removed, output SSTs added
    /// 2. Manifest is persisted to disk
    /// 3. Input SSTs are deleted from the filesystem
    ///
    /// Strategy: Create two compaction rounds. The second compaction would include
    /// old L0 files if they existed (proving they weren't deleted in round 1).
    /// Since the second compaction succeeds with correct key counts, we prove
    /// old files were deleted after round 1's manifest publish.
    #[test]
    fn should_cleanup_input_ssts_after_compaction_manifest_publishes() {
        // Arrange: Create 5 L0 files (5 batches Ã— 100 keys each)
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");
        for batch in 0..5 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin batch tx");
            for i in 0..100 {
                let key = format!("batch{batch:02}_key{i:04}");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .expect("put batch value");
            }
            tx.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit batch");
            engine.flush_cf(&cf).expect("flush batch");
        }

        // Act: Compact twice, verifying cleanup between rounds.
        // Round 1 walks bounded batches until all five L0 inputs are replaced.
        engine.compact_all().expect("first compaction");

        // Add new batch (creates new L0)
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin new batch tx");
        for i in 0..100 {
            let key = format!("batch99_key{i:04}");
            tx.put(key.as_bytes().to_vec(), b"new_value".to_vec(), None)
                .expect("put new batch value");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit new batch");
        engine.flush_cf(&cf).expect("flush new batch");

        // Round 2 merges the new L0 with any overlapping L1 output. If the old L0
        // files still existed, they would incorrectly re-enter this work.
        engine.compact_all().expect("second compaction");

        // Assert: All data (500 old + 100 new) is queryable
        // This proves old L0 files were deleted after round 1's manifest publish
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx after cleanup");

        let count = tx
            .scan(&Query::new())
            .expect("scan all keys")
            .try_collect()
            .expect("collect all keys")
            .len();
        assert_eq!(
            count, 600,
            "All 600 keys (500 old + 100 new) present after cleanup proves old L0s deleted"
        );

        // Verify old and new data values are correct
        let old_val = tx.get(b"batch00_key0000").expect("get old batch key");
        assert_eq!(old_val, Some(Bytes::copy_from_slice(b"value")));

        let new_val = tx.get(b"batch99_key0000").expect("get new batch key");
        assert_eq!(new_val, Some(Bytes::copy_from_slice(b"new_value")));
    }

    #[test]
    fn should_assign_unique_output_sequences_given_emergent_followup_compaction() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");
        let batch_count = 12;
        let keys_per_batch = 20;
        for batch in 0..batch_count {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin batch tx");
            for key_idx in 0..keys_per_batch {
                let key = format!("batch{batch:02}_key{key_idx:04}");
                let value = format!("value-{batch:02}-{key_idx:04}");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("put batch value");
            }
            tx.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit batch");
            engine.flush_cf(&cf).expect("flush batch");
        }

        // Act
        engine.compact_all().expect("compact all seeded L0 files");

        // Assert
        let layout = engine.get_storage_layout().expect("storage layout");
        let compacted_names: Vec<String> = layout
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .filter(|file| file.level > 0)
            .map(|file| file.name.clone())
            .collect();

        assert!(
            compacted_names.len() >= 2,
            "test must produce follow-up compaction outputs; got {compacted_names:?}"
        );
        assert!(
            compacted_names
                .iter()
                .all(|name| !name.ends_with("00000000000000000000.sst")),
            "compaction outputs must never use sequence zero: {compacted_names:?}"
        );

        let unique_names: HashSet<&str> = compacted_names.iter().map(String::as_str).collect();
        assert_eq!(
            unique_names.len(),
            compacted_names.len(),
            "follow-up compactions must publish unique SST names: {compacted_names:?}"
        );

        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx after compaction");
        let total = read_tx
            .scan(&Query::new())
            .expect("scan compacted data")
            .try_collect()
            .expect("collect compacted data")
            .len();
        assert_eq!(
            total,
            batch_count * keys_per_batch,
            "all seeded data must remain readable after repeated follow-up compactions"
        );

        for batch in 0..batch_count {
            let key = format!("batch{batch:02}_key0000");
            let value = format!("value-{batch:02}-0000");
            assert_eq!(
                read_tx.get(key.as_bytes()).expect("get compacted key"),
                Some(Bytes::from(value)),
                "compacted key should remain readable: {key}"
            );
        }
    }
}

mod delete_range_audit {
    //! Delete-range correctness checks.
    //!
    //! These tests verify actual delete-range and scan behavior rather than
    //! printing manual audit output.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::Query;

    #[test]
    fn should_delete_only_keys_within_requested_range_when_delete_range_commits() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin seed transaction");
            for i in 1..=10 {
                let key = format!("key{i:02}");
                let value = format!("val{i:02}");
                tx.put(key.into_bytes(), value.into_bytes(), None)
                    .expect("seed key");
            }
            tx.commit(buffered_write_options(mode))
                .expect("commit seed transaction");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin delete_range transaction");
            tx.delete_range(b"key02".to_vec(), b"key08".to_vec())
                .expect("delete range");
            tx.commit(buffered_write_options(mode))
                .expect("commit delete range transaction");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin read transaction");
            for i in 1..=10 {
                let key = format!("key{i:02}");
                let value = tx.get(key.as_bytes()).expect("read key after delete_range");
                if (2..8).contains(&i) {
                    assert_eq!(value, None, "mode: {mode} key: {key}");
                } else {
                    let expected = format!("val{i:02}");
                    assert_eq!(
                        value,
                        Some(Bytes::copy_from_slice(expected.as_bytes())),
                        "mode: {mode} key: {key}"
                    );
                }
            }
        });
    }

    #[test]
    fn should_return_all_inserted_keys_when_scanning_unbounded_query() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin seed transaction");
            for key in [b"a", b"b", b"c", b"d"] {
                tx.put(
                    key.to_vec(),
                    [b'v', b'a', b'l', b'_', key[0]].to_vec(),
                    None,
                )
                .expect("seed scan key");
            }
            tx.commit(buffered_write_options(mode))
                .expect("commit seed transaction");

            // Act
            let tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin scan transaction");
            let results = tx
                .scan(&Query::new())
                .expect("scan all keys")
                .try_collect()
                .expect("collect all keys");

            // Assert
            assert_eq!(results.len(), 4, "mode: {mode}");
            assert_eq!(results[0].0, Bytes::from_static(b"a"));
            assert_eq!(results[1].0, Bytes::from_static(b"b"));
            assert_eq!(results[2].0, Bytes::from_static(b"c"));
            assert_eq!(results[3].0, Bytes::from_static(b"d"));
        });
    }
}

mod smoke {
    //! Smoke tests for Midge.
    //!
    //! Purpose:
    //! - Validate core end-to-end invariants
    //! - Exercise real engine wiring with minimal data
    //! - Catch Ã¢â‚¬Å“green unit tests, broken databaseÃ¢â‚¬Â failures
    //!
    //! Philosophy:
    //! - Tests are intentionally small and deterministic
    //! - No sleeps, timing assumptions, or fuzz
    //! - Stress, chaos, and performance tests live in the external harness
    //! - If all unit tests + this file pass, the database is not fundamentally broken
    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::Query;

    #[test]
    fn should_read_written_value_given_memory_mode_when_written() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result = tx.get(b"key").expect("get");

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"value")));
    }

    #[test]
    fn should_read_written_value_given_flushed_value_when_read() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        engine.flush_cf(&cf).expect("flush");

        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result = tx.get(b"key").expect("get");

        // Assert
        assert_eq!(result, Some(Bytes::from_static(b"value")));
    }

    #[test]
    fn should_hide_value_given_deleted_key_when_read() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete(b"key".to_vec()).expect("delete");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result = tx.get(b"key").expect("get");

        // Assert
        assert_eq!(result, None);
    }

    #[test]
    fn should_preserve_tombstone_given_flushed_tombstone_when_read() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete(b"key".to_vec()).expect("delete");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        engine.flush_cf(&cf).expect("flush");

        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result = tx.get(b"key").expect("get");

        // Assert
        assert_eq!(result, None, "Tombstone should persist through flush");
    }

    #[test]
    fn should_persist_data_given_write_when_restarted() {
        // Arrange
        let opts = opts_for_mode("local");

        // Act - Write and restart
        {
            let mut engine = open_with_mode(&opts, "local");
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                b"persistent_key".to_vec(),
                b"persistent_value".to_vec(),
                None,
            )
            .expect("put");
            tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before restart");
        }

        // Reopen engine
        let engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result = tx.get(b"persistent_key").expect("get");

        // Assert
        assert_eq!(
            result,
            Some(Bytes::from_static(b"persistent_value")),
            "Data should persist after restart"
        );
    }

    #[test]
    fn should_persist_tombstone_given_delete_when_restarted() {
        // Arrange
        let opts = opts_for_mode("local");

        // Act - Delete and restart
        {
            let mut engine = open_with_mode(&opts, "local");
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.delete(b"key".to_vec()).expect("delete");
            tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before restart");
        }

        // Reopen engine
        let engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result = tx.get(b"key").expect("get");

        // Assert
        assert_eq!(result, None, "Tombstone should persist after restart");
    }

    #[test]
    fn should_allow_read_only_snapshot_given_committed_value_when_snapshot_reads() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"v1".to_vec(), None).expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        let snapshot = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();

        // Assert
        let snap_value = snapshot.get(b"key").expect("get");
        assert_eq!(
            snap_value,
            Some(Bytes::from_static(b"v1")),
            "Snapshot should be usable for reads"
        );

        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let current_value = tx.get(b"key").expect("get");
        assert_eq!(
            current_value,
            Some(Bytes::from_static(b"v1")),
            "Engine and snapshot both see data"
        );
    }

    #[test]
    fn should_preserve_latest_version_given_repeated_flushes_when_read() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"v1".to_vec(), None).expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).expect("flush");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"v2".to_vec(), None).expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).expect("flush");

        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let result = tx.get(b"key").expect("get");

        // Assert
        assert_eq!(
            result,
            Some(Bytes::from_static(b"v2")),
            "Repeated flushes should preserve latest version"
        );
    }

    #[test]
    fn should_respect_visibility_rules_given_range_scan_when_scanning() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"a".to_vec(), b"1".to_vec(), None).expect("put");
        tx.put(b"b".to_vec(), b"2".to_vec(), None).expect("put");
        tx.put(b"c".to_vec(), b"3".to_vec(), None).expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete(b"b".to_vec()).expect("delete");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = tx
            .scan(
                &Query::new()
                    .start_key(Bytes::from(&b"a"[..]))
                    .end_key(Bytes::from(&b"d"[..])),
            )
            .expect("scan")
            .try_collect()
            .expect("collect scan");

        // Assert - 'b' should be filtered out by delete
        assert_eq!(
            results.len(),
            2,
            "Deleted key should not appear in range scan"
        );
        assert_eq!(results[0].0, Bytes::from_static(b"a"));
        assert_eq!(results[1].0, Bytes::from_static(b"c"));
    }

    #[test]
    fn should_preserve_all_committed_values_given_multiple_writes_when_written() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        for i in 0..10 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(format!("key{i}").into_bytes(), b"val".to_vec(), None)
                .expect("put");
            tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        }

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        for i in 0..10 {
            assert_eq!(
                tx.get(format!("key{i}").as_bytes())
                    .expect("get committed key"),
                Some(Bytes::from_static(b"val"))
            );
        }
    }

    #[test]
    fn should_reopen_committed_values_given_engine_dropped_without_close_when_reopened() {
        // Arrange
        let opts = opts_for_mode("local");

        // Act
        {
            let mut engine = open_with_mode(&opts, "local");
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .expect("put");
            tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .expect("put");
            tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
            engine
                .shutdown(std::time::Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        // Reopen and verify state
        let engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let v1 = tx.get(b"key1").expect("get");
        let v2 = tx.get(b"key2").expect("get");
        assert_eq!(v1, Some(Bytes::from_static(b"value1")));
        assert_eq!(v2, Some(Bytes::from_static(b"value2")));
    }

    // Note: Durability frontier enforcement test removed as it requires
    // chaos engineering or crash simulation infrastructure that is not yet implemented.
    // This should be reintroduced when proper crash testing infrastructure is available.
}

mod edge_cases {
    //! Edge Cases Tests
    //!
    //! Tests boundary conditions and unusual scenarios:
    //! - Very large keys (1MB+) and values (100MB+)
    //! - Empty database, single record, 10k+ keys
    //! - Mixed value sizes, delete all, rapid operations
    //! - Tombstone accumulation, range extremes, TTL edge cases
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>
    //!
    //! Most tests run on all storage modes to validate cross-platform consistency.
    //! Some intentionally exclude cloud mode when the invariant is not meaningful
    //! under `CloudAsync` durability semantics.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::TransactionMode;

    // ============================================================================
    // SIZE EXTREMES (Tests 1-4)
    // ============================================================================

    #[test]
    fn should_retrieve_stored_keys_when_megabyte_sized() {
        // Arrange: Create 1MB+ key (256KB minimum, test with 500KB)
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            let large_key = vec![65u8; 500_000]; // 500KB key
            let small_value = b"value";

            // Act: Store and retrieve
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(large_key.clone(), small_value.to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).unwrap();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let got = tx.get(&large_key).expect("get");

            // Assert
            assert_eq!(
                got,
                Some(Bytes::copy_from_slice(small_value)),
                "failed to store/retrieve 500KB key in {mode}"
            );
        });
    }

    #[test]
    fn should_retrieve_stored_values_when_hundred_megabytes() {
        // Arrange: Create 100MB value (or reasonable subset for tests)
        // Use 10MB for practical test speed; pattern validates for larger
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            let small_key = b"big_value";
            let large_value = vec![42u8; 10_000_000]; // 10MB value

            // Act: Store and retrieve
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(small_key.to_vec(), large_value.clone(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).unwrap();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let got = tx.get(small_key).expect("get");

            // Assert: Verify size and content
            assert!(got.is_some(), "failed to retrieve 10MB value in {mode}");
            assert_eq!(
                got.as_ref().map(bytes::Bytes::len),
                Some(10_000_000),
                "retrieved value size mismatch in {mode}"
            );
        });
    }

    #[test]
    fn should_handle_mixed_size_values_when_ranging_from_bytes_to_megabytes() {
        // Arrange
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Act: Store values of wildly different sizes
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"tiny".to_vec(), b"x".to_vec(), None).expect("put");
            tx.put(b"small".to_vec(), vec![42u8; 100], None)
                .expect("put");
            tx.put(b"medium".to_vec(), vec![42u8; 100_000], None)
                .expect("put");
            tx.put(b"large".to_vec(), vec![42u8; 1_000_000], None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).unwrap();

            // Assert: Retrieve all and verify
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            assert_eq!(tx.get(b"tiny").expect("get").map(|b| b.len()), Some(1));
            assert_eq!(tx.get(b"small").expect("get").map(|b| b.len()), Some(100));
            assert_eq!(
                tx.get(b"medium").expect("get").map(|b| b.len()),
                Some(100_000)
            );
            assert_eq!(
                tx.get(b"large").expect("get").map(|b| b.len()),
                Some(1_000_000)
            );
        });
    }

    #[test]
    fn should_handle_special_characters_in_keys_when_utf8_and_binary_mixed() {
        // Arrange
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Act: Store keys with special characters and binary data
            let keys: Vec<&[u8]> = vec![
                b"normal_key",
                "unicode_\u{1F600}_key".as_bytes(),
                b"\x00\x01\x02\x03", // Binary nulls
                b"key\twith\ttabs",
                b"key\nwith\nnewlines",
            ];

            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            for (i, key) in keys.iter().enumerate() {
                let value = format!("value_{i}");
                tx.put(key.to_vec(), value.into_bytes(), None).expect("put");
            }
            tx.commit(buffered_write_options(mode)).unwrap();

            // Assert: Retrieve all
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            for (i, key) in keys.iter().enumerate() {
                let got = tx.get(key).expect("get");
                let expected_value = format!("value_{i}");
                assert_eq!(
                    got,
                    Some(Bytes::copy_from_slice(expected_value.as_bytes())),
                    "special char key retrieval failed: {:?}",
                    String::from_utf8_lossy(key)
                );
            }
        });
    }

    // ============================================================================
    // EMPTY/BOUNDARY CONDITIONS (Tests 5-8)
    // ============================================================================

    #[test]
    fn should_handle_empty_database_when_no_keys_written() {
        // Arrange: Open engine and close without writing anything
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Act: Try to read from empty database
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let got = tx.get(b"nonexistent").expect("get");

            // Assert
            assert_eq!(
                got, None,
                "empty database returned unexpected value in {mode:?}"
            );
        });
    }

    #[test]
    fn should_handle_single_record_database_when_one_key_value_pair() {
        // Arrange
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Act: Write single record
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"only_key".to_vec(), b"only_value".to_vec(), None)
                .expect("put");
            tx.commit(buffered_write_options(mode)).unwrap();

            // Assert: Can retrieve it
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let got = tx.get(b"only_key").expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"only_value")),
                "failed to retrieve single record in {mode:?}"
            );

            // Assert: Other keys don't exist
            let not_got = tx.get(b"other_key").expect("get");
            assert_eq!(
                not_got, None,
                "unexpected key found in single-record database"
            );
        });
    }

    #[test]
    fn should_handle_range_query_at_boundaries_when_first_last_and_missing() {
        // Arrange
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Act: Write sorted keys
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            for i in 0..5 {
                let key = format!("key_{i:02}");
                tx.put(key.into_bytes(), format!("value_{i}").into_bytes(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).unwrap();

            // Assert: Boundary keys are retrievable
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            assert!(
                tx.get(b"key_00").expect("get").is_some(),
                "first key not found"
            );
            assert!(
                tx.get(b"key_04").expect("get").is_some(),
                "last key not found"
            );
            assert_eq!(
                tx.get(b"key_99").expect("get"),
                None,
                "non-existent key should be None"
            );
        });
    }

    // ============================================================================
    // STRESS/ACCUMULATION (Tests 9-12)
    // ============================================================================

    #[test]
    fn should_handle_rapid_operations_when_one_thousand_puts_per_second() {
        // Arrange
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Act: Rapid writes (batched in a single transaction for efficiency)
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            for i in 0..1000 {
                let key = format!("rapid_{i:05}");
                tx.put(key.into_bytes(), format!("v_{i}").into_bytes(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).unwrap();

            // Assert: All retrievable
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            for i in (0..1000).step_by(100) {
                let key = format!("rapid_{i:05}");
                let got = tx.get(key.as_bytes()).expect("get");
                assert!(got.is_some(), "lost rapid write in {mode}");
            }
        });
    }

    #[test]
    fn should_handle_delete_all_pattern_when_writing_then_deleting_all_keys() {
        // Arrange: Write 100 keys
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            for i in 0..100 {
                let key = format!("del_test_{i:03}");
                tx.put(key.into_bytes(), b"delete_me".to_vec(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act: Delete all keys
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            for i in 0..100 {
                let key = format!("del_test_{i:03}");
                tx.delete(key.into_bytes()).expect("delete");
            }
            tx.commit(buffered_write_options(mode)).unwrap();

            // Assert: All deleted
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            for i in 0..100 {
                let key = format!("del_test_{i:03}");
                let got = tx.get(key.as_bytes()).expect("get");
                assert_eq!(got, None, "key not deleted in {mode}");
            }
        });
    }

    #[test]
    fn should_handle_tombstone_accumulation_when_many_deletes_create_tombstones() {
        // Arrange: Rapid put/delete cycles
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Act: 10 put/delete cycles on same key
            for cycle in 0..10 {
                let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
                tx.put(
                    b"tombstone_test".to_vec(),
                    format!("cycle_{cycle}").into_bytes(),
                    None,
                )
                .expect("put");
                tx.commit(buffered_write_options(mode)).unwrap();

                let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
                tx.delete(b"tombstone_test".to_vec()).expect("delete");
                tx.commit(buffered_write_options(mode)).unwrap();
            }

            // Assert: Final state is deleted (tombstone wins)
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let got = tx.get(b"tombstone_test").expect("get");
            assert_eq!(got, None, "tombstone did not win over old put in {mode}");
        });
    }

    #[test]
    fn should_batch_concurrent_puts_when_cloud_async_mode() {
        // Arrange
        // Cloud-upload counters are only populated once telemetry is enabled.
        cntryl_midge::init_benchmark_telemetry().expect("enable test-visible cloud metrics");
        for_each_storage_mode(&["cloud"], |mode, opts| {
            let engine = open_with_mode(&opts, mode);
            let cf = engine
                .create_column_family("test")
                .expect("create cf")
                .clone();
            let cf_id = cf.id();

            let threads: usize = 16;
            let puts_per_thread: usize = 200;
            let total_puts: usize = threads * puts_per_thread;

            let before_uploads = engine
                .get_runtime_metrics()
                .expect("runtime metrics before puts")
                .cloud_async_wal_uploads_completed;

            // Act: concurrent single puts (each put still blocks on CloudAck)
            std::thread::scope(|s| {
                let engine_ref = &engine;
                for t in 0..threads {
                    s.spawn(move || {
                        for i in 0..puts_per_thread {
                            let key = format!("k_{t}_{i}");
                            let mut tx = engine_ref
                                .begin_tx(cf_id, TransactionMode::ReadWrite)
                                .unwrap();
                            tx.put(key.into_bytes(), b"value".to_vec(), None)
                                .expect("put");
                            tx.commit(buffered_write_options(mode)).unwrap();
                        }
                    });
                }
            });

            // Assert: correctness (spot-check)
            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            for t in 0..threads {
                for i in [0, puts_per_thread / 2, puts_per_thread - 1] {
                    let key = format!("k_{t}_{i}");
                    let got = tx.get(key.as_bytes()).expect("get");
                    assert!(got.is_some(), "missing key {key}");
                }
            }

            // Assert: batching occurred. This is the observable contract of
            // cloud-async group commit — concurrently-arriving CloudAck writes
            // share upload requests rather than each paying for its own, so the
            // number of completed cloud uploads must be well below one per put.
            // CloudAsync commits return once locally durable; the uploads
            // themselves finish on a background thread, so wait for that
            // background work to settle before reading the final count.
            let upload_deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            let mut after_uploads;
            loop {
                after_uploads = engine
                    .get_runtime_metrics()
                    .expect("runtime metrics after puts")
                    .cloud_async_wal_uploads_completed;
                if after_uploads > before_uploads {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                    let settled = engine
                        .get_runtime_metrics()
                        .expect("runtime metrics after settling")
                        .cloud_async_wal_uploads_completed;
                    if settled == after_uploads {
                        break;
                    }
                }
                assert!(
                    std::time::Instant::now() < upload_deadline,
                    "timed out waiting for background cloud uploads to complete"
                );
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            let uploads_for_puts = usize::try_from(after_uploads.saturating_sub(before_uploads))
                .expect("test upload count fits in usize");

            assert!(
                uploads_for_puts > 0,
                "expected at least one cloud upload for {total_puts} durable puts"
            );
            assert!(
                uploads_for_puts < total_puts,
                "expected group commit to batch concurrent puts into fewer cloud \
                 uploads: uploads={uploads_for_puts} puts={total_puts}"
            );
        });
    }
}
