//! Storage Layer Tests
//!
//! Consolidated from: `hybrid_storage.rs`, `storage_verification_hardening.rs`, `sst_reads_integration.rs`, `sst_regressions.rs`, `streaming_scan_hardening.rs`, `compression_compatibility.rs`, `compatibility_fixtures.rs`, `provider_feature_contract.rs`

mod common;

mod sst_reads_integration {
    use crate::common;
    // Integration test for SST reads with read amplification metrics

    use crate::common::{open_with_mode, opts_for_mode, StorageMode};
    use cntryl_midge::WriteOptions;
    use cntryl_midge::{MidgeResult, Query};

    fn local_db_path(opts: &common::MidgeOptions) -> std::path::PathBuf {
        match &opts.storage_mode {
            StorageMode::LocalDisk { db_path } => db_path.clone(),
            _ => panic!("expected local disk options"),
        }
    }

    fn corrupt_first_sst_data_byte(db_path: &std::path::Path) -> MidgeResult<()> {
        use std::io::{Read, Seek, Write};

        let sst_path = std::fs::read_dir(db_path.join("sst"))?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
            .expect("flushed SST file");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(sst_path)?;
        file.seek(std::io::SeekFrom::Start(4))?;
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte)?;
        file.seek(std::io::SeekFrom::Start(4))?;
        file.write_all(&[byte[0] ^ 0x01])?;
        file.sync_all()?;
        Ok(())
    }

    #[test]
    fn should_read_from_sst_after_flush() -> MidgeResult<()> {
        // Arrange: Create engine
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Write keys that will be flushed to SST
        for i in 0..10 {
            let key = format!("key_{i:03}");
            let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
            tx.put(key.as_bytes().to_vec(), b"value_from_sst".to_vec(), None)?;
            tx.commit(WriteOptions::buffered())?;
        }

        // Force flush to SST
        engine.flush_cf(&cf)?;
        std::thread::sleep(std::time::Duration::from_millis(100)); // Give flush time to complete

        // Act: Read keys that should be in SST now
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
        let value1 = read_tx.get(b"key_000")?;
        let value2 = read_tx.get(b"key_005")?;
        let value3 = read_tx.get(b"missing_key")?;

        // Assert: Verify values
        assert_eq!(value1, Some(b"value_from_sst".to_vec().into()));
        assert_eq!(value2, Some(b"value_from_sst".to_vec().into()));
        assert_eq!(value3, None);

        println!("SST reads completed successfully");
        Ok(())
    }

    #[test]
    fn should_use_key_ranges_for_higher_levels() -> MidgeResult<()> {
        // Arrange: Create engine
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Write sorted keys
        for i in 0..20 {
            let key = format!("key_{i:03}");
            let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
            tx.put(key.as_bytes().to_vec(), b"test_value".to_vec(), None)?;
            tx.commit(WriteOptions::buffered())?;
        }

        engine.flush_cf(&cf)?;
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Act: Read various keys
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
        let first = read_tx.get(b"key_000")?;
        let middle = read_tx.get(b"key_010")?;
        let last = read_tx.get(b"key_019")?;
        let missing = read_tx.get(b"key_999")?;

        // Assert
        assert_eq!(first, Some(b"test_value".to_vec().into()));
        assert_eq!(middle, Some(b"test_value".to_vec().into()));
        assert_eq!(last, Some(b"test_value".to_vec().into()));
        assert_eq!(missing, None);

        println!("Range-aware SST reads completed successfully");
        Ok(())
    }

    #[test]
    fn should_handle_memtable_and_sst_reads() -> MidgeResult<()> {
        // Arrange: Mix of memtable and SST data
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Write to SST
        let mut tx1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
        tx1.put(b"sst_key".to_vec(), b"sst_value".to_vec(), None)?;
        tx1.commit(WriteOptions::buffered())?;
        engine.flush_cf(&cf)?;
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Write to memtable
        let mut tx2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
        tx2.put(b"mem_key".to_vec(), b"mem_value".to_vec(), None)?;
        tx2.commit(WriteOptions::buffered())?;

        // Act: Read from both
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
        let from_sst = read_tx.get(b"sst_key")?;
        let from_mem = read_tx.get(b"mem_key")?;

        // Assert
        assert_eq!(from_sst, Some(b"sst_value".to_vec().into()));
        assert_eq!(from_mem, Some(b"mem_value".to_vec().into()));

        // Update SST key in memtable (newer version should win)
        let mut tx3 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
        tx3.put(b"sst_key".to_vec(), b"updated_value".to_vec(), None)?;
        tx3.commit(WriteOptions::buffered())?;
        let read_tx2 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
        let updated = read_tx2.get(b"sst_key")?;
        assert_eq!(updated, Some(b"updated_value".to_vec().into()));

        println!("Mixed memtable/SST reads completed successfully");
        Ok(())
    }

    #[test]
    fn should_error_given_corrupt_sst_when_transaction_get_reads_flushed_key() -> MidgeResult<()> {
        // Arrange
        let opts = opts_for_mode("local");
        let db_path = local_db_path(&opts);
        let engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
        tx.put(b"corrupt-key".to_vec(), b"corrupt-value".to_vec(), None)?;
        tx.commit(WriteOptions::buffered())?;
        engine.flush_cf(&cf)?;
        corrupt_first_sst_data_byte(&db_path)?;

        // Act
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
        let error = read_tx
            .get(b"corrupt-key")
            .expect_err("corrupt SST point read must surface an error");

        // Assert
        assert!(
            error.to_string().to_ascii_lowercase().contains("corrupt")
                || error.to_string().contains("CRC32C"),
            "expected corruption-oriented error, got {error}"
        );
        Ok(())
    }

    #[test]
    fn should_error_given_corrupt_sst_when_transaction_scan_reads_flushed_range() -> MidgeResult<()>
    {
        // Arrange
        let opts = opts_for_mode("local");
        let db_path = local_db_path(&opts);
        let engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");
        for i in 0..3 {
            let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
            tx.put(
                format!("corrupt-range-{i}").into_bytes(),
                b"corrupt-value".to_vec(),
                None,
            )?;
            tx.commit(WriteOptions::buffered())?;
        }
        engine.flush_cf(&cf)?;
        corrupt_first_sst_data_byte(&db_path)?;

        // Act
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
        let mut scan = read_tx.scan(&Query::new())?;
        let error = scan
            .next()
            .transpose()
            .expect_err("corrupt SST range scan must surface an error while advancing");

        // Assert
        assert!(
            error.to_string().to_ascii_lowercase().contains("corrupt")
                || error.to_string().contains("CRC32C"),
            "expected corruption-oriented error, got {error}"
        );
        Ok(())
    }
}

mod sst_regressions {
    #[test]
    fn should_preserve_empty_key_through_engine_flush_with_deep_trie() {
        use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::open(
            OpenOptions::local(dir.path())
                .background_compaction(false)
                .build()
                .unwrap(),
        )
        .unwrap();
        let cf = engine.create_column_family("probe").unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        let value = vec![b'v'; 32 * 1024];
        tx.put(Vec::new(), value.clone(), None).unwrap();
        for n in (1..=300).rev() {
            let mut key = vec![b'a'; n];
            key.push(b'b');
            tx.put(key, value.clone(), None).unwrap();
        }
        tx.commit(WriteOptions::sync()).unwrap();
        assert!(engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap()
            .get(b"")
            .unwrap()
            .is_some());
        // Act
        engine.flush_cf(&cf).unwrap();
        let result = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap()
            .get(b"")
            .unwrap();
        let verification = engine
            .verify_storage(std::time::Duration::from_secs(30))
            .unwrap();
        assert!(verification.authoritative);
        engine.shutdown(std::time::Duration::from_secs(30)).unwrap();
        let reopened = Engine::open(
            OpenOptions::local(dir.path())
                .background_compaction(false)
                .build()
                .unwrap(),
        )
        .unwrap();
        let cf = reopened.get_column_family("probe").unwrap();
        let after_reopen = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap()
            .get(b"")
            .unwrap();

        // Assert
        assert_eq!(after_reopen.as_deref().map(<[u8]>::len), Some(value.len()));
        assert_eq!(result.as_deref().map(<[u8]>::len), Some(value.len()));
    }

    #[test]
    fn should_reject_oversized_value_before_transaction_stages_it() {
        use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(OpenOptions::local(dir.path()).build().unwrap()).unwrap();
        let cf = engine.create_column_family("probe").unwrap();
        for insert in [false, true] {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            let value = vec![b'v'; 64 * 1024 * 1024];
            // Act
            let result = if insert {
                tx.insert(b"key".to_vec(), value, Some(60))
            } else {
                tx.put(b"key".to_vec(), value, None)
            };
            // Assert
            assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
            tx.put(b"valid".to_vec(), b"value".to_vec(), None).unwrap();
            tx.commit(WriteOptions::sync()).unwrap();
            assert!(engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .unwrap()
                .get(b"key")
                .unwrap()
                .is_none());
        }
        engine.flush_cf(&cf).unwrap();
        assert!(engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap()
            .get(b"valid")
            .unwrap()
            .is_some());
    }

    #[test]
    fn should_preserve_transaction_when_oversized_range_delete_is_rejected() {
        use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(OpenOptions::local(dir.path()).build().unwrap()).unwrap();
        let cf = engine.create_column_family("range-admission").unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"middle".to_vec(), b"value".to_vec(), None).unwrap();
        // Act
        let result = tx.delete_range(b"a".to_vec(), vec![b'z'; 64 * 1024 * 1024]);
        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(
            tx.get(b"middle").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        tx.commit(WriteOptions::sync()).unwrap();
        engine.flush_cf(&cf).unwrap();
        assert_eq!(
            engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .unwrap()
                .get(b"middle")
                .unwrap()
                .as_deref(),
            Some(b"value".as_slice())
        );
    }

    #[test]
    fn should_continue_writing_across_l0_ceiling_when_background_compaction_is_disabled() {
        use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
        use std::time::Duration;
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::open(
            OpenOptions::local(dir.path())
                .background_compaction(false)
                .build()
                .unwrap(),
        )
        .unwrap();
        let cf = engine.create_column_family("l0-pressure").unwrap();
        // Act: 32 separate flushes exceed the default hard L0 ceiling. Only the
        // production pressure-recovery path can restore admission in this process.
        for index in 0_u32..32 {
            let commit = || {
                let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
                tx.put(index.to_be_bytes().to_vec(), b"value".to_vec(), None)?;
                tx.commit(WriteOptions::sync())
            };
            match commit() {
                Ok(()) => {}
                Err(MidgeError::WriteStall(_)) => {
                    assert!(engine
                        .wait_for_write_stall_clear(cf.id(), Duration::from_secs(10))
                        .unwrap());
                    commit().unwrap();
                }
                Err(error) => panic!("unexpected write failure: {error}"),
            }
            engine.flush_cf(&cf).unwrap();
        }
        engine.shutdown(Duration::from_secs(30)).unwrap();
        let reopened = Engine::open(
            OpenOptions::local(dir.path())
                .background_compaction(false)
                .build()
                .unwrap(),
        )
        .unwrap();
        let cf = reopened.get_column_family("l0-pressure").unwrap();
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap();
        // Assert
        for index in 0_u32..32 {
            assert_eq!(
                tx.get(&index.to_be_bytes()).unwrap().as_deref(),
                Some(b"value".as_slice())
            );
        }
    }
}

mod streaming_scan_hardening {
    //! Hardening contract for lazy, fallible transaction scans.

    use bytes::Bytes;
    use cntryl_midge::{
        Engine, Goal, IteratorState, MidgeError, MidgeResult, OpenOptions, Query, TransactionMode,
        WriteOptions,
    };
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn should_stop_prefix_scan_before_neighboring_prefix() -> MidgeResult<()> {
        // Arrange
        let mut engine = open_memory()?;
        let cf = default_cf(&engine);
        seed_rows(
            &engine,
            cf.id(),
            &[(b"aa:1", b"one"), (b"aa:2", b"two"), (b"ab:0", b"other")],
        )?;
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Act
        let rows = read
            .scan(&Query::new().prefix(Bytes::from_static(b"aa:")))?
            .try_collect()?;

        // Assert
        assert_eq!(keys(&rows), vec![b"aa:1".to_vec(), b"aa:2".to_vec()]);
        drop(read);
        engine.shutdown(Duration::from_secs(2))?;
        Ok(())
    }

    #[test]
    fn should_intersect_prefix_with_explicit_end_bound() -> MidgeResult<()> {
        // Arrange
        let mut engine = open_memory()?;
        let cf = default_cf(&engine);
        seed_rows(
            &engine,
            cf.id(),
            &[
                (b"scan:1", b"one"),
                (b"scan:2", b"two"),
                (b"scan:3", b"three"),
                (b"scao:0", b"neighbor"),
            ],
        )?;
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        let query = Query::new()
            .prefix(Bytes::from_static(b"scan:"))
            .end_key(Bytes::from_static(b"scan:2"));

        // Act
        let rows = read.scan(&query)?.try_collect()?;

        // Assert
        assert_eq!(keys(&rows), vec![b"scan:1".to_vec()]);
        drop(read);
        engine.shutdown(Duration::from_secs(2))?;
        Ok(())
    }

    #[test]
    fn should_return_empty_given_valid_bounds_disjoint_from_prefix() -> MidgeResult<()> {
        // Arrange
        let mut engine = open_memory()?;
        let cf = default_cf(&engine);
        seed_rows(
            &engine,
            cf.id(),
            &[(b"alpha:1", b"one"), (b"zulu:1", b"two")],
        )?;
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        let query = Query::new()
            .start_key(Bytes::from_static(b"zulu"))
            .end_key(Bytes::from_static(b"zulu;"))
            .prefix(Bytes::from_static(b"alpha:"));

        // Act
        let rows = read.scan(&query)?.try_collect()?;

        // Assert
        assert!(rows.is_empty());
        drop(read);
        engine.shutdown(Duration::from_secs(2))?;
        Ok(())
    }

    #[test]
    fn should_scan_binary_prefix_without_finite_successor() -> MidgeResult<()> {
        // Arrange
        let mut engine = open_memory()?;
        let cf = default_cf(&engine);
        let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        for key in [
            vec![0xFE, 0xFF],
            vec![0xFF],
            vec![0xFF, 0x00],
            vec![0xFF, 0xFF, 0x01],
        ] {
            write.put(key, b"value".to_vec(), None)?;
        }
        write.commit(WriteOptions::sync())?;
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Act
        let rows = read
            .scan(&Query::new().prefix(Bytes::from_static(&[0xFF])))?
            .try_collect()?;

        // Assert
        assert_eq!(
            keys(&rows),
            vec![vec![0xFF], vec![0xFF, 0x00], vec![0xFF, 0xFF, 0x01]]
        );
        drop(read);
        engine.shutdown(Duration::from_secs(2))?;
        Ok(())
    }

    #[test]
    fn should_read_only_one_candidate_block_for_forward_limit() -> MidgeResult<()> {
        // Arrange
        let temp = TempDir::new()?;
        let mut engine = open_local(temp.path())?;
        let cf = default_cf(&engine);
        seed_flushed_blocks(&engine, cf.id(), 24)?;
        let before_construction = engine.read_path_diagnostics_snapshot_for_benchmarks();
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Act
        let mut scan = read.scan(&Query::new().limit(1))?;
        let after_construction = engine.read_path_diagnostics_snapshot_for_benchmarks();
        let first = scan.next().transpose()?.expect("first row");
        let after_first = engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Assert
        assert_eq!(first.0, Bytes::from_static(b"block-000"));
        assert_eq!(
            after_construction.data_blocks_read, before_construction.data_blocks_read,
            "iterator construction must not read an SST data block"
        );
        assert!(
            after_first
                .data_blocks_read
                .saturating_sub(after_construction.data_blocks_read)
                <= 2,
            "limit(1) read too many data blocks: before={after_construction:?} after={after_first:?}"
        );
        drop(scan);
        drop(read);
        engine.shutdown(Duration::from_secs(2))?;
        Ok(())
    }

    #[test]
    fn should_read_only_one_candidate_block_for_reverse_limit() -> MidgeResult<()> {
        // Arrange
        let temp = TempDir::new()?;
        let mut engine = open_local(temp.path())?;
        let cf = default_cf(&engine);
        seed_flushed_blocks(&engine, cf.id(), 24)?;
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        let before = engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Act
        let mut scan = read.scan(&Query::new().reverse().limit(1))?;
        let first = scan.next().transpose()?.expect("last row");
        let after = engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Assert
        assert_eq!(first.0, Bytes::from_static(b"block-023"));
        assert!(
            after
                .data_blocks_read
                .saturating_sub(before.data_blocks_read)
                <= 2,
            "reverse limit read too many data blocks: before={before:?} after={after:?}"
        );
        drop(scan);
        drop(read);
        engine.shutdown(Duration::from_secs(2))?;
        Ok(())
    }

    #[test]
    fn should_surface_corrupt_later_sst_block_from_iterator_item() -> MidgeResult<()> {
        // Arrange
        let temp = TempDir::new()?;
        let mut engine = open_local(temp.path())?;
        let cf = default_cf(&engine);
        seed_flushed_blocks(&engine, cf.id(), 8)?;
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        let mut scan = read.scan(&Query::new())?;
        corrupt_second_sst_data_block(temp.path())?;

        // Act
        let first = scan.next().transpose()?.expect("row before corruption");
        let late_error = scan
            .find_map(Result::err)
            .expect("corrupt later block must surface from an iterator item");
        let replayed_once = scan.next().expect("failed scan must replay its error");
        let replayed_twice = scan.next().expect("failed scan must remain failed");

        // Assert
        assert_eq!(first.0, Bytes::from_static(b"block-000"));
        assert!(
            late_error
                .to_string()
                .to_ascii_lowercase()
                .contains("corrupt")
                || late_error.to_string().contains("CRC32C"),
            "expected corruption error, got {late_error}"
        );
        assert!(matches!(replayed_once, Err(MidgeError::Corruption(_))));
        assert!(matches!(replayed_twice, Err(MidgeError::Corruption(_))));
        assert_eq!(scan.state(), IteratorState::Failed);
        assert!(scan.failed());
        assert!(!scan.exhausted());
        drop(scan);
        drop(read);
        engine.shutdown(Duration::from_secs(2))?;
        Ok(())
    }

    #[test]
    fn should_preserve_snapshot_visibility_during_streaming_scan() -> MidgeResult<()> {
        // Arrange
        let mut engine = open_memory()?;
        let cf = default_cf(&engine);
        seed_rows(&engine, cf.id(), &[(b"stable", b"old")])?;
        let old_snapshot = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        seed_rows(
            &engine,
            cf.id(),
            &[(b"stable", b"new"), (b"new-key", b"new")],
        )?;

        // Act
        let rows = old_snapshot.scan(&Query::new())?.try_collect()?;

        // Assert
        assert_eq!(
            rows,
            vec![(Bytes::from_static(b"stable"), Bytes::from_static(b"old"))]
        );
        drop(old_snapshot);
        engine.shutdown(Duration::from_secs(2))?;
        Ok(())
    }

    #[test]
    fn should_apply_range_tombstones_before_streaming_rows() -> MidgeResult<()> {
        // Arrange
        let mut engine = open_memory()?;
        let cf = default_cf(&engine);
        seed_rows(
            &engine,
            cf.id(),
            &[(b"range:a", b"a"), (b"range:b", b"b"), (b"range:c", b"c")],
        )?;
        let mut delete = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        delete.delete_range(b"range:b".to_vec(), b"range:d".to_vec())?;
        delete.commit(WriteOptions::sync())?;
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Act
        let rows = read.scan(&Query::new())?.try_collect()?;

        // Assert
        assert_eq!(keys(&rows), vec![b"range:a".to_vec()]);
        drop(read);
        engine.shutdown(Duration::from_secs(2))?;
        Ok(())
    }

    fn open_memory() -> MidgeResult<Engine> {
        Engine::open(OpenOptions::in_memory().build()?)
    }

    fn open_local(path: &Path) -> MidgeResult<Engine> {
        Engine::open(
            OpenOptions::local(path)
                .goal(Goal::Latency)
                .background_compaction(false)
                .build()?,
        )
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }

    fn seed_rows(engine: &Engine, cf_id: u32, rows: &[(&[u8], &[u8])]) -> MidgeResult<()> {
        let mut write = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
        for (key, value) in rows {
            write.put(key.to_vec(), value.to_vec(), None)?;
        }
        write.commit(WriteOptions::sync())
    }

    fn seed_flushed_blocks(engine: &Engine, cf_id: u32, count: usize) -> MidgeResult<()> {
        let mut write = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
        for index in 0..count {
            write.put(
                format!("block-{index:03}").into_bytes(),
                vec![b'x'; 8 * 1024],
                None,
            )?;
        }
        write.commit(WriteOptions::sync())?;
        let cf = engine
            .get_column_family("default")
            .expect("default column family");
        engine.flush_cf(&cf)
    }

    fn corrupt_second_sst_data_block(db_path: &Path) -> MidgeResult<()> {
        use std::io::{Read, Seek, Write};

        let sst_path = std::fs::read_dir(db_path.join("sst"))?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
            .expect("flushed SST file");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(sst_path)?;
        let mut length = [0_u8; 4];
        file.read_exact(&mut length)?;
        let second_block = 4_u64.saturating_add(u64::from(u32::from_le_bytes(length)));
        file.seek(std::io::SeekFrom::Start(second_block + 4))?;
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte)?;
        file.seek(std::io::SeekFrom::Start(second_block + 4))?;
        file.write_all(&[byte[0] ^ 0x01])?;
        file.sync_all()?;
        Ok(())
    }

    fn keys(rows: &[(Bytes, Bytes)]) -> Vec<Vec<u8>> {
        rows.iter().map(|(key, _)| key.to_vec()).collect()
    }
}

mod compression_compatibility {
    use cntryl_midge::sst::compression::{
        compress_block_with_trailer, decompress_block_with_trailer, CompressionAlgo,
        CompressionPolicy, BLOCK_TRAILER_SIZE,
    };
    use cntryl_midge::{
        Engine, EngineHealth, Goal, OpenOptions, Query, RecoveryPolicy, TransactionMode,
        WorkloadProfile, WriteOptions,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use xxhash_rust::xxh3::xxh3_64;

    fn structured_block(size: usize) -> Vec<u8> {
        let pattern = b"account=0042|region=east|status=active|segment=business|";
        pattern.iter().copied().cycle().take(size).collect()
    }

    fn adaptive_records(prefix: &str, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|index| {
                let pattern = format!(
                    "order={index:04}|region={}|state=committed|class={}|",
                    ["apac", "emea", "amer"][index % 3],
                    ["standard", "priority"][index % 2],
                );
                let value = pattern
                    .as_bytes()
                    .iter()
                    .copied()
                    .cycle()
                    .take(4 * 1024)
                    .collect();
                (format!("{prefix}:{index:04}").into_bytes(), value)
            })
            .collect()
    }

    fn canonical_data_digest<'a>(rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>) -> u64 {
        let mut canonical = Vec::new();
        for (key, value) in rows {
            canonical.extend_from_slice(
                &u32::try_from(key.len())
                    .expect("test key length fits in u32")
                    .to_le_bytes(),
            );
            canonical.extend_from_slice(key);
            canonical.extend_from_slice(
                &u32::try_from(value.len())
                    .expect("test value length fits in u32")
                    .to_le_bytes(),
            );
            canonical.extend_from_slice(value);
        }
        xxh3_64(&canonical)
    }

    fn local_options(path: &Path, goal: Goal) -> cntryl_midge::OpenOptions {
        OpenOptions::local(path)
            .goal(goal)
            .workload(WorkloadProfile::WriteHeavy)
            .recovery_policy(RecoveryPolicy::Strict)
            .background_compaction(false)
            .build()
            .expect("build local throughput options")
    }

    fn write_records_and_flush(
        engine: &Engine,
        column_family: &cntryl_midge::ColumnFamilyHandle,
        records: &[(Vec<u8>, Vec<u8>)],
    ) {
        let mut transaction = engine
            .begin_tx(column_family.id(), TransactionMode::ReadWrite)
            .expect("begin write transaction");
        for (key, value) in records {
            transaction
                .put(key.clone(), value.clone(), None)
                .expect("write compression test record");
        }
        transaction
            .commit(WriteOptions::sync())
            .expect("commit compression test records");
        engine
            .flush_cf(column_family)
            .expect("complete compression test flush");
    }

    fn write_fresh_adaptive_database(path: &Path, records: &[(Vec<u8>, Vec<u8>)]) {
        let mut engine = Engine::open(local_options(path, Goal::Throughput))
            .expect("open adaptive test database");
        let column_family = engine
            .create_column_family("adaptive")
            .expect("create adaptive column family");
        write_records_and_flush(&engine, &column_family, records);
        engine
            .shutdown(Duration::from_secs(10))
            .expect("clean adaptive database shutdown");
    }

    fn sorted_sst_files(path: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut files: Vec<_> = fs::read_dir(path.join("sst"))
            .expect("read SST directory")
            .filter_map(|entry| {
                let entry = entry.expect("read SST entry");
                let path = entry.path();
                (path.is_file()
                    && path.extension().and_then(|extension| extension.to_str()) == Some("sst"))
                .then(|| {
                    (
                        PathBuf::from(entry.file_name()),
                        fs::read(path).expect("read SST file"),
                    )
                })
            })
            .collect();
        files.sort_by(|left, right| left.0.cmp(&right.0));
        files
    }

    fn sst_block_algorithms(bytes: &[u8]) -> Vec<u8> {
        let footer_start = bytes
            .len()
            .checked_sub(84)
            .expect("V4 SST has fixed footer");
        let mut cursor = 0usize;
        let mut algorithms = Vec::new();
        while cursor < footer_start {
            let length_end = cursor.checked_add(4).expect("block prefix end");
            let payload_len = usize::try_from(u32::from_le_bytes(
                bytes[cursor..length_end]
                    .try_into()
                    .expect("four-byte block prefix"),
            ))
            .expect("block payload length fits usize");
            let block_end = length_end
                .checked_add(payload_len)
                .expect("block extent fits usize");
            assert!(block_end <= footer_start, "block extends into V4 footer");
            algorithms.push(bytes[block_end - BLOCK_TRAILER_SIZE]);
            cursor = block_end;
        }
        assert_eq!(cursor, footer_start, "blocks exactly precede V4 footer");
        algorithms
    }

    #[test]
    fn should_preserve_exact_sst_codec_codes() {
        // Arrange
        let expected = [
            (CompressionAlgo::None, 0),
            (CompressionAlgo::Lz4, 1),
            (CompressionAlgo::Zstd3, 2),
            (CompressionAlgo::Zstd9, 3),
        ];

        // Act
        for (algorithm, code) in expected {
            // Assert
            assert_eq!(algorithm.to_u8(), code);
            assert_eq!(CompressionAlgo::from_u8(code), Some(algorithm));
        }
        assert_eq!(CompressionAlgo::from_u8(4), None);
        assert_eq!(CompressionAlgo::from_u8(u8::MAX), None);
    }

    #[test]
    fn should_preserve_five_byte_trailer_layout_with_crc_coverage() {
        // Arrange
        let data = b"trailer-format-fixture";

        // Act
        let block = compress_block_with_trailer(data, &CompressionPolicy::None)
            .expect("build checksummed raw block");
        let algorithm_offset = block.len() - BLOCK_TRAILER_SIZE;
        let crc_offset = block.len() - size_of::<u32>();
        let stored_crc = u32::from_le_bytes(
            block[crc_offset..]
                .try_into()
                .expect("four-byte CRC32C trailer"),
        );

        // Assert
        assert_eq!(BLOCK_TRAILER_SIZE, 5);
        assert_eq!(&block[..algorithm_offset], data);
        assert_eq!(block[algorithm_offset], CompressionAlgo::None.to_u8());
        assert_eq!(stored_crc, crc32c::crc32c(&block[..crc_offset]));
    }

    #[test]
    fn should_preserve_baseline_compressed_block_fixture() {
        // Arrange
        let data = structured_block(16 * 1024);
        let cases = [
            (
                CompressionPolicy::Fixed(CompressionAlgo::Lz4),
                CompressionAlgo::Lz4,
                0xf8ab_776d_208c_bd15_u64,
            ),
            (
                CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
                CompressionAlgo::Zstd3,
                0x4e7b_d7fc_d9a0_d5c5_u64,
            ),
            (
                CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
                CompressionAlgo::Zstd9,
                0xe2b7_653b_ded1_b28e_u64,
            ),
        ];

        // Act
        for (policy, expected_algorithm, expected_digest) in cases {
            let block = compress_block_with_trailer(&data, &policy)
                .expect("compress baseline block fixture");
            let algorithm_offset = block.len() - BLOCK_TRAILER_SIZE;
            let actual_digest = xxh3_64(&block);
            eprintln!(
                "{expected_algorithm:?}: len={}, xxh3_64={actual_digest:016x}",
                block.len()
            );

            // Assert
            assert_eq!(block[algorithm_offset], expected_algorithm.to_u8());
            assert_eq!(actual_digest, expected_digest);
        }
    }

    #[test]
    fn should_roundtrip_every_emitted_sst_block_deterministically() {
        // Arrange
        let data = structured_block(16 * 1024);
        let policies = [
            CompressionPolicy::None,
            CompressionPolicy::Fixed(CompressionAlgo::None),
            CompressionPolicy::Fixed(CompressionAlgo::Lz4),
            CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
            CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
            CompressionPolicy::Adaptive {
                min_savings_bytes: 256,
                min_ratio: 0.95,
                check_algorithms: vec![
                    CompressionAlgo::None,
                    CompressionAlgo::Lz4,
                    CompressionAlgo::Zstd3,
                ],
            },
        ];

        // Act
        for policy in policies {
            let first = compress_block_with_trailer(&data, &policy).expect("first compression");
            let second = compress_block_with_trailer(&data, &policy).expect("second compression");
            let decoded = decompress_block_with_trailer(&first).expect("roundtrip block");

            // Assert
            assert_eq!(first, second, "policy must be deterministic: {policy:?}");
            assert_eq!(decoded.as_ref(), data.as_slice());
        }
    }

    #[test]
    fn should_reject_invalid_sst_block_trailers() {
        // Arrange
        let data = structured_block(1024);
        let valid = compress_block_with_trailer(&data, &CompressionPolicy::None)
            .expect("build valid block");
        let mut corrupt = valid.to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x01;

        let mut unknown = data.clone();
        unknown.push(u8::MAX);
        let crc = crc32c::crc32c(&unknown);
        unknown.extend_from_slice(&crc.to_le_bytes());

        // Act
        let corrupt_error =
            decompress_block_with_trailer(&corrupt).expect_err("corrupt CRC must fail");
        let truncated_error =
            decompress_block_with_trailer(&valid[..4]).expect_err("short trailer must fail");
        let unknown_error =
            decompress_block_with_trailer(&unknown).expect_err("unknown codec must fail");

        // Assert
        assert!(corrupt_error.to_string().contains("CRC32C mismatch"));
        assert!(truncated_error
            .to_string()
            .contains("too small for trailer"));
        assert!(unknown_error
            .to_string()
            .contains("unknown compression algorithm code"));
    }

    #[test]
    fn should_reject_nonshipping_codec_codes_without_fallback() {
        // Arrange
        let encoded = [4_u8, 5, u8::MAX].map(|code| {
            let mut block = b"payload".to_vec();
            block.push(code);
            block.extend_from_slice(&crc32c::crc32c(&block).to_le_bytes());
            block
        });

        // Act
        let errors = encoded.map(|block| {
            decompress_block_with_trailer(&block).expect_err("unknown codec must fail closed")
        });

        // Assert
        for error in errors {
            assert!(matches!(error, cntryl_midge::MidgeError::Corruption(_)));
            assert!(error
                .to_string()
                .contains("unknown compression algorithm code"));
        }
    }

    #[test]
    fn should_reject_corrupt_compressed_payload_for_every_shipping_codec() {
        // Arrange
        let data = structured_block(16 * 1024);
        let cases = [
            CompressionAlgo::Lz4,
            CompressionAlgo::Zstd3,
            CompressionAlgo::Zstd9,
        ];

        // Act
        for algorithm in cases {
            let mut block =
                compress_block_with_trailer(&data, &CompressionPolicy::Fixed(algorithm))
                    .expect("compress shipping codec")
                    .to_vec();
            match algorithm {
                CompressionAlgo::Lz4 => {
                    block[..4].copy_from_slice(&(64 * 1024 * 1024_u32 + 1).to_le_bytes());
                }
                CompressionAlgo::Zstd3 | CompressionAlgo::Zstd9 => block[0] ^= 0xff,
                CompressionAlgo::None => unreachable!(),
            }
            let crc_offset = block.len() - size_of::<u32>();
            let crc = crc32c::crc32c(&block[..crc_offset]);
            block[crc_offset..].copy_from_slice(&crc.to_le_bytes());
            let error = decompress_block_with_trailer(&block)
                .expect_err("codec payload corruption must not fall back to raw bytes");

            // Assert
            assert!(
                matches!(error, cntryl_midge::MidgeError::Corruption(_)),
                "{algorithm:?} returned {error}"
            );
        }
    }

    #[test]
    fn should_roundtrip_edge_case_values_when_written_through_full_sst_pipeline() {
        // Arrange
        let temp = tempfile::tempdir().expect("create database");
        let mut engine =
            Engine::open(local_options(temp.path(), Goal::Latency)).expect("open engine");
        let cf = engine
            .create_column_family("payloads")
            .expect("create column family");
        let incompressible = seeded_bytes(16 * 1024, 0x8f21_49da);
        let mut write = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write");
        write
            .put(b"empty".to_vec(), Vec::new(), None)
            .expect("put empty value");
        write
            .put(b"random".to_vec(), incompressible.clone(), None)
            .expect("put incompressible value");
        write.commit(WriteOptions::sync()).expect("commit values");
        engine.flush_cf(&cf).expect("flush values");
        engine
            .shutdown(Duration::from_secs(10))
            .expect("shutdown engine");

        // Act
        let reopened =
            Engine::open(local_options(temp.path(), Goal::Latency)).expect("reopen engine");
        let cf = reopened
            .get_column_family("payloads")
            .expect("reopen column family");
        let read = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read");

        // Assert
        assert_eq!(
            read.get(b"empty").expect("read empty"),
            Some(Vec::new().into())
        );
        assert_eq!(
            read.get(b"random").expect("read incompressible").as_deref(),
            Some(incompressible.as_slice())
        );
    }

    fn seeded_bytes(size: usize, seed: u32) -> Vec<u8> {
        let mut state = seed;
        (0..size)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                u8::try_from(state >> 24).expect("shifted state fits u8")
            })
            .collect()
    }

    #[test]
    fn should_preserve_data_given_deliberate_compression_policy_change_when_reopening_populated_database(
    ) {
        // Arrange
        let temp = tempfile::tempdir().expect("create policy-change database");
        let latency_records = adaptive_records("latency", 48);
        let economy_records = adaptive_records("economy", 48);
        let mut latency =
            Engine::open(local_options(temp.path(), Goal::Latency)).expect("open latency engine");
        let cf = latency
            .create_column_family("policies")
            .expect("create column family");
        write_records_and_flush(&latency, &cf, &latency_records);
        latency
            .shutdown(Duration::from_secs(10))
            .expect("shutdown latency engine");

        // Act
        let mut economy =
            Engine::open(local_options(temp.path(), Goal::Economy)).expect("open economy engine");
        let cf = economy
            .get_column_family("policies")
            .expect("reopen column family");
        write_records_and_flush(&economy, &cf, &economy_records);
        let read = economy
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin cross-policy read");
        let rows = read
            .scan(&Query::new())
            .expect("scan cross-policy rows")
            .try_collect()
            .expect("collect cross-policy rows");

        // Assert
        assert_eq!(rows.len(), latency_records.len() + economy_records.len());
        for (key, value) in latency_records.iter().chain(&economy_records) {
            assert_eq!(
                read.get(key).expect("read cross-policy value").as_deref(),
                Some(value.as_slice())
            );
        }
        drop(read);
        economy
            .shutdown(Duration::from_secs(10))
            .expect("shutdown economy engine");
    }

    #[test]
    fn should_select_current_policy_for_new_blocks_while_preserving_prior_algorithm_when_compacting_after_goal_change(
    ) {
        // Arrange
        let temp = tempfile::tempdir().expect("create policy-change database");
        let batches = (0..4)
            .map(|batch| adaptive_records(&format!("policy-{batch}"), 48))
            .collect::<Vec<_>>();
        let mut latency =
            Engine::open(local_options(temp.path(), Goal::Latency)).expect("open latency engine");
        let cf = latency
            .create_column_family("policies")
            .expect("create column family");
        for batch in &batches[..3] {
            write_records_and_flush(&latency, &cf, batch);
        }
        latency
            .shutdown(Duration::from_secs(10))
            .expect("shutdown latency engine");
        let latency_ssts = sorted_sst_files(temp.path());
        assert!(latency_ssts.iter().any(|(_, bytes)| {
            sst_block_algorithms(bytes).contains(&CompressionAlgo::Lz4.to_u8())
        }));

        let mut economy =
            Engine::open(local_options(temp.path(), Goal::Economy)).expect("open economy engine");
        let cf = economy
            .get_column_family("policies")
            .expect("reopen column family");
        write_records_and_flush(&economy, &cf, &batches[3]);
        assert!(sorted_sst_files(temp.path()).iter().any(|(_, bytes)| {
            sst_block_algorithms(bytes).contains(&CompressionAlgo::Zstd9.to_u8())
        }));

        // Act
        economy.compact_all().expect("compact mixed-policy SSTs");
        economy
            .shutdown(Duration::from_secs(10))
            .expect("shutdown economy engine");
        let report = Engine::verify_path(temp.path()).expect("verify compacted database");
        let compacted_ssts = sorted_sst_files(temp.path());
        let reopened = Engine::open(local_options(temp.path(), Goal::Throughput))
            .expect("reopen compacted engine");
        let cf = reopened
            .get_column_family("policies")
            .expect("reopen compacted column family");
        let read = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin compacted read");
        let rows = read
            .scan(&Query::new())
            .expect("scan compacted rows")
            .try_collect()
            .expect("collect compacted rows");

        // Assert
        assert!(report.authoritative);
        assert!(compacted_ssts.iter().any(|(_, bytes)| {
            sst_block_algorithms(bytes).contains(&CompressionAlgo::Zstd9.to_u8())
        }));
        assert_eq!(rows.len(), batches.iter().map(Vec::len).sum::<usize>());
        for batch in &batches {
            for (key, value) in batch {
                assert_eq!(
                    read.get(key).expect("read policy-change key").as_deref(),
                    Some(value.as_slice())
                );
            }
        }
    }

    #[test]
    fn should_report_meaningful_error_given_footer_corruption_when_running_explicit_verification() {
        // Arrange
        let temp = tempfile::tempdir().expect("create footer database");
        let records = adaptive_records("footer", 32);
        write_fresh_adaptive_database(temp.path(), &records);
        let sst_path = fs::read_dir(temp.path().join("sst"))
            .expect("read SST directory")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
            .expect("SST path");
        let mut bytes = fs::read(&sst_path).expect("read SST");
        let footer_handle_byte = bytes.len() - 84 + 8;
        bytes[footer_handle_byte] ^= 0x01;
        fs::write(&sst_path, bytes).expect("corrupt footer");

        // Act
        let error =
            Engine::verify_path(temp.path()).expect_err("footer corruption must fail verify");

        // Assert
        assert!(matches!(error, cntryl_midge::MidgeError::Corruption(_)));
        assert!(
            error.to_string().contains("CRC mismatch")
                || error.to_string().contains("footer CRC32C mismatch"),
            "expected checksummed verification failure, got {error}"
        );
    }

    #[test]
    fn should_preserve_candidate_adaptive_sst_across_strict_reopen() {
        // Arrange
        let temp = tempfile::tempdir().expect("create candidate database");
        let records = adaptive_records("adaptive:key", 384);
        let expected_digest = canonical_data_digest(
            records
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        );
        write_fresh_adaptive_database(temp.path(), &records);

        // Act
        let report = Engine::verify_path(temp.path()).expect("verify candidate adaptive database");
        let mut reopened = Engine::open(local_options(temp.path(), Goal::Throughput))
            .expect("strictly reopen candidate");
        let column_family = reopened
            .get_column_family("adaptive")
            .expect("reopen adaptive column family");
        let read = reopened
            .begin_tx(column_family.id(), TransactionMode::ReadOnly)
            .expect("begin candidate read");
        let first = read.get(b"adaptive:key:0000").expect("read first key");
        let middle = read.get(b"adaptive:key:0192").expect("read middle key");
        let last = read.get(b"adaptive:key:0383").expect("read last key");
        let rows = read
            .scan(&Query::new())
            .expect("scan candidate SST")
            .try_collect()
            .expect("collect candidate scan");
        drop(read);

        // Assert
        assert_eq!(report.health, EngineHealth::Healthy);
        assert!(report.authoritative);
        assert!(report.sst_files_verified >= 1);
        assert_eq!(first.as_deref(), Some(records[0].1.as_slice()));
        assert_eq!(middle.as_deref(), Some(records[192].1.as_slice()));
        assert_eq!(last.as_deref(), Some(records[383].1.as_slice()));
        assert_eq!(rows.len(), records.len());
        assert_eq!(
            canonical_data_digest(
                rows.iter()
                    .map(|(key, value)| (key.as_ref(), value.as_ref()))
            ),
            expected_digest
        );
        reopened
            .shutdown(Duration::from_secs(10))
            .expect("shutdown reopened candidate");
    }

    #[test]
    fn should_strictly_reopen_completed_adaptive_compaction() {
        // Arrange
        let temp = tempfile::tempdir().expect("create compacted database");
        let first_batch = adaptive_records("compacted:a", 192);
        let second_batch = adaptive_records("compacted:b", 192);
        let mut expected_records = first_batch.clone();
        expected_records.extend(second_batch.clone());
        expected_records.sort_by(|left, right| left.0.cmp(&right.0));
        let expected_digest = canonical_data_digest(
            expected_records
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        );

        let mut engine = Engine::open(local_options(temp.path(), Goal::Throughput))
            .expect("open compacted database");
        let column_family = engine
            .create_column_family("adaptive")
            .expect("create compacted column family");
        write_records_and_flush(&engine, &column_family, &first_batch);
        write_records_and_flush(&engine, &column_family, &second_batch);

        // Act
        engine
            .compact_all()
            .expect("complete adaptive SST compaction");
        engine
            .shutdown(Duration::from_secs(10))
            .expect("cleanly shut down compacted database");
        let report = Engine::verify_path(temp.path()).expect("verify compacted database");
        let mut reopened = Engine::open(local_options(temp.path(), Goal::Throughput))
            .expect("strictly reopen compaction");
        let column_family = reopened
            .get_column_family("adaptive")
            .expect("reopen compacted column family");
        let read = reopened
            .begin_tx(column_family.id(), TransactionMode::ReadOnly)
            .expect("begin compacted read");
        let first = read.get(b"compacted:a:0000").expect("read first key");
        let middle = read.get(b"compacted:a:0191").expect("read middle key");
        let last = read.get(b"compacted:b:0191").expect("read last key");
        let rows = read
            .scan(&Query::new())
            .expect("scan compacted SST")
            .try_collect()
            .expect("collect compacted scan");
        drop(read);

        // Assert
        assert_eq!(report.health, EngineHealth::Healthy);
        assert!(report.authoritative);
        assert!(report.sst_files_verified >= 1);
        assert_eq!(first.as_deref(), Some(first_batch[0].1.as_slice()));
        assert_eq!(middle.as_deref(), Some(first_batch[191].1.as_slice()));
        assert_eq!(last.as_deref(), Some(second_batch[191].1.as_slice()));
        assert_eq!(rows.len(), expected_records.len());
        assert_eq!(
            canonical_data_digest(
                rows.iter()
                    .map(|(key, value)| (key.as_ref(), value.as_ref()))
            ),
            expected_digest
        );
        reopened
            .shutdown(Duration::from_secs(10))
            .expect("shutdown reopened compaction");
    }

    #[test]
    fn should_produce_byte_identical_sst_files_from_identical_adaptive_input() {
        // Arrange
        let first = tempfile::tempdir().expect("create first deterministic database");
        let second = tempfile::tempdir().expect("create second deterministic database");
        let records = adaptive_records("adaptive:key", 384);

        // Act
        write_fresh_adaptive_database(first.path(), &records);
        write_fresh_adaptive_database(second.path(), &records);
        let first_ssts = sorted_sst_files(first.path());
        let second_ssts = sorted_sst_files(second.path());

        // Assert
        assert!(!first_ssts.is_empty());
        assert_eq!(first_ssts, second_ssts);
    }
}

mod compatibility_fixtures {
    use cntryl_midge::{
        Engine, EngineHealth, MidgeError, OpenOptions, Query, RecoveryPolicy, TransactionMode,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use xxhash_rust::xxh3::xxh3_64;

    fn fixtures_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compatibility")
    }

    fn copy_fixture_dir(name: &str) -> tempfile::TempDir {
        let source = fixtures_root().join(name);
        assert!(
            source.exists(),
            "missing compatibility fixture: {}",
            source.display()
        );

        let temp = tempfile::tempdir().expect("create temp dir");
        copy_dir_recursive(&source, temp.path()).expect("copy compatibility fixture");
        temp
    }

    fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
        fs::create_dir_all(destination)?;

        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let ty = entry.file_type()?;
            let dest_path = destination.join(entry.file_name());

            if ty.is_dir() {
                copy_dir_recursive(&entry.path(), &dest_path)?;
            } else {
                fs::copy(entry.path(), &dest_path)?;
            }
        }

        Ok(())
    }

    fn assert_compatibility_error(error: MidgeError) {
        match error {
            MidgeError::CompatibilityError(message) => {
                assert!(
                    message.contains("unsupported on-disk format version"),
                    "expected unsupported-format compatibility error, got: {message}"
                );
            }
            other => panic!("expected CompatibilityError, got: {other:?}"),
        }
    }

    fn logical_rows_digest(rows: &[(bytes::Bytes, bytes::Bytes)]) -> u64 {
        let mut canonical = Vec::new();
        for (key, value) in rows {
            canonical.extend_from_slice(
                &u32::try_from(key.len())
                    .expect("fixture key length fits u32")
                    .to_le_bytes(),
            );
            canonical.extend_from_slice(key);
            canonical.extend_from_slice(
                &u32::try_from(value.len())
                    .expect("fixture value length fits u32")
                    .to_le_bytes(),
            );
            canonical.extend_from_slice(value);
        }
        xxh3_64(&canonical)
    }

    #[test]
    fn should_verify_populated_release_v3_v4_fixture_given_supported_format_when_reopening() {
        // Arrange
        let temp = copy_fixture_dir("v3_populated_v4_sst_db");

        // Act
        let report = Engine::verify_path(temp.path()).expect("verify release fixture");
        assert_eq!(report.health, EngineHealth::Healthy);
        assert_eq!(report.manifest_files_verified, 1);
        assert_eq!(report.sst_files_verified, 1);
        assert_eq!(report.bytes_verified, 437);
        assert_eq!(report.data_blocks_verified, 1);
        assert_eq!(report.wal_recovery_records_replayed, 0);
        assert_eq!(report.wal_recovery_bytes_replayed, 0);
        assert_eq!(report.intent_entries_loaded, 0);

        let mut engine = Engine::open(
            OpenOptions::local(temp.path())
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        )
        .expect("open release fixture");
        let cf = engine
            .get_column_family("default")
            .expect("fixture default column family");
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin fixture read");
        let rows = read
            .scan(&Query::new())
            .expect("scan fixture")
            .try_collect()
            .expect("collect fixture rows");
        let runtime = engine.get_runtime_metrics().expect("runtime metrics");

        // Assert
        assert_eq!(runtime.health, EngineHealth::Healthy);
        assert_eq!(rows.len(), 3);
        assert_eq!(logical_rows_digest(&rows), 14_948_492_731_235_234_299);
        drop(read);
        engine
            .shutdown(Duration::from_secs(5))
            .expect("shutdown fixture engine");
    }

    #[test]
    fn should_reject_v2_empty_fixture_given_breaking_v4_sst_format() {
        // Arrange
        let temp = copy_fixture_dir("v2_empty_db");

        // Act
        let verify_error = Engine::verify_path(temp.path()).expect_err("V2 verify must fail");
        let Err(open_error) = Engine::open(
            OpenOptions::local(temp.path())
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        ) else {
            panic!("V2 fixture must fail open");
        };

        // Assert
        assert_compatibility_error(verify_error);
        assert_compatibility_error(open_error);
    }

    #[test]
    fn should_reject_future_v4_fixture_given_unsupported_version_when_reopening() {
        // Arrange
        let temp = copy_fixture_dir("future_v4");

        // Act
        let verify_error =
            Engine::verify_path(temp.path()).expect_err("future fixture should fail verify");
        assert_compatibility_error(verify_error);

        let Err(open_error) = Engine::open(
            OpenOptions::local(temp.path())
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        ) else {
            panic!("future fixture should fail open");
        };
        // Assert
        assert_compatibility_error(open_error);
    }
}

mod provider_feature_contract {
    use std::fs;
    use std::path::{Path, PathBuf};

    use cntryl_midge::CloudProviderConfig;

    fn repository_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
    }

    fn read_repository_file(path: &str) -> String {
        fs::read_to_string(repository_root().join(path))
            .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
    }

    fn feature_definition<'a>(manifest: &'a str, feature: &str) -> &'a str {
        let marker = format!("{feature} = [");
        let start = manifest
            .find(&marker)
            .unwrap_or_else(|| panic!("missing Cargo feature {feature}"));
        let definition = &manifest[start..];
        let end = definition
            .find("]\n")
            .unwrap_or_else(|| panic!("unterminated Cargo feature {feature}"));
        &definition[..=end]
    }

    /// Collapse every run of whitespace (including newlines) to a single space.
    ///
    /// These tests assert on architectural invariants ("this attribute governs
    /// that item") by matching source-code fragments. Comparing on token
    /// sequence rather than exact byte layout keeps that assertion real while no
    /// longer breaking on a harmless rustfmt reflow (e.g. an attribute wrapping
    /// onto its own line).
    fn normalize_whitespace(source: &str) -> String {
        source.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// `haystack` contains `needle` once both are normalized with
    /// [`normalize_whitespace`].
    fn contains_normalized(haystack: &str, needle: &str) -> bool {
        normalize_whitespace(haystack).contains(&normalize_whitespace(needle))
    }

    #[test]
    fn should_keep_common_cloud_transport_provider_neutral_when_features_are_split() {
        // Arrange
        let manifest = read_repository_file("Cargo.toml");

        // Act
        let common = feature_definition(&manifest, "cloud-common");

        // Assert
        assert!(manifest.contains("default = [\"cloud-all\"]"));
        assert!(common.contains("dep:reqwest"));
        assert!(common.contains("dep:tokio"));
        for provider_dependency in [
            "dep:base64",
            "dep:hmac",
            "dep:percent-encoding",
            "dep:sha1",
            "dep:sha2",
            "dep:url",
            "dep:urlencoding",
        ] {
            assert!(
                !common.contains(provider_dependency),
                "cloud-common must not activate {provider_dependency}"
            );
        }
    }

    #[test]
    fn should_assign_each_provider_dependency_set_to_its_cargo_feature() {
        // Arrange
        let manifest = read_repository_file("Cargo.toml");

        // Act
        let aws = feature_definition(&manifest, "cloud-aws");
        let azure = feature_definition(&manifest, "cloud-azure");
        let gcp = feature_definition(&manifest, "cloud-gcp");
        let oci = feature_definition(&manifest, "cloud-oci");

        // Assert
        for s3_feature in [aws, oci] {
            for dependency in [
                "cloud-common",
                "dep:hmac",
                "dep:percent-encoding",
                "dep:sha2",
                "dep:url",
                "dep:urlencoding",
            ] {
                assert!(s3_feature.contains(dependency));
            }
            assert!(!s3_feature.contains("dep:rsa"));
        }

        for dependency in [
            "cloud-common",
            "dep:base64",
            "dep:hmac",
            "dep:percent-encoding",
            "dep:sha2",
            "dep:url",
            "dep:urlencoding",
        ] {
            assert!(azure.contains(dependency));
        }
        assert!(!azure.contains("dep:rsa"));
        assert!(!azure.contains("dep:sha1"));

        for dependency in [
            "cloud-common",
            "dep:base64",
            "dep:hmac",
            "dep:percent-encoding",
            "dep:sha1",
            "dep:sha2",
            "dep:url",
            "dep:urlencoding",
        ] {
            assert!(gcp.contains(dependency));
        }
    }

    #[test]
    fn should_compile_provider_modules_only_for_their_owner_features() {
        // Arrange
        let providers = read_repository_file("src/storage/providers/mod.rs");
        let factory = read_repository_file("src/storage/providers/factory.rs");

        // Act
        // Assert
        assert!(contains_normalized(
            &providers,
            "#[cfg(feature = \"cloud-azure\")]\npub mod azure;"
        ));
        assert!(contains_normalized(
            &providers,
            "#[cfg(feature = \"cloud-gcp\")]\npub mod gcs;"
        ));
        assert!(contains_normalized(
            &providers,
            "#[cfg(any(feature = \"cloud-aws\", feature = \"cloud-oci\"))]\npub mod s3;"
        ));
        assert!(!contains_normalized(
            &providers,
            "#[cfg(feature = \"cloud-common\")]\npub mod azure;"
        ));
        assert!(!contains_normalized(
            &providers,
            "#[cfg(feature = \"cloud-common\")]\npub mod gcs;"
        ));
        assert!(!contains_normalized(
            &providers,
            "#[cfg(feature = \"cloud-common\")]\npub mod s3;"
        ));
        assert!(factory.contains("CloudProviderConfig::AwsS3(_)"));
        assert!(factory.contains("CloudProviderConfig::S3Compatible(_)"));
        assert!(factory.contains("CloudProviderConfig::AzureBlob(_)"));
        assert!(factory.contains("CloudProviderConfig::OciObjectStorage(_)"));
        assert!(contains_normalized(
            &factory,
            "#[cfg(any(feature = \"cloud-aws\", feature = \"cloud-oci\"))]\n    fn build_s3_compatible"
        ));
    }

    #[test]
    fn should_keep_provider_dtos_in_configuration_layer() {
        // Arrange
        let provider_config = read_repository_file("src/config/provider.rs");
        let storage_providers = read_repository_file("src/storage/providers/mod.rs");

        // Act
        // Assert
        assert!(contains_normalized(
            &provider_config,
            "pub enum CloudProviderConfig"
        ));
        assert!(!provider_config.contains("cfg(feature = \"cloud-"));
        assert!(contains_normalized(
            &storage_providers,
            "pub(crate) use crate::config::CloudProviderConfig;"
        ));
        assert!(!repository_root()
            .join("src/storage/providers/config.rs")
            .exists());
    }

    #[test]
    fn should_expose_provider_configuration_without_implementation_features() {
        // Arrange
        let providers = [
            CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
            CloudProviderConfig::s3_compatible_static(
                "bucket",
                "https://objectstorage.example.test",
                "access",
                "secret",
            ),
            CloudProviderConfig::azure_blob_shared_key("account", "container", "secret"),
            CloudProviderConfig::gcs_hmac("bucket", "access", "secret"),
        ];

        // Act
        let object_names = providers
            .iter()
            .map(CloudProviderConfig::bucket_or_container)
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(object_names, ["bucket", "bucket", "container", "bucket"]);
    }
}
