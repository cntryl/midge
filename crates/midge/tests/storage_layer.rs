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
            .map(|entry| entry.expect("read SST directory entry"))
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

    fn assert_published_sst(engine: &cntryl_midge::Engine) -> MidgeResult<()> {
        let layout = engine.metrics().get_storage_layout()?;
        assert!(layout.levels.iter().any(|level| level.file_count > 0));
        Ok(())
    }

    #[test]
    fn should_read_from_sst_after_flush() -> MidgeResult<()> {
        // Arrange: Create engine
        let engine = open_with_mode(&opts_for_mode("local"), "local");
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
        assert_published_sst(&engine)?;

        // Act: Read keys that should be in SST now
        let read_tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)?;
        let value1 = read_tx.get(b"key_000")?;
        let value2 = read_tx.get(b"key_005")?;
        let value3 = read_tx.get(b"missing_key")?;

        // Assert: Verify values
        assert_eq!(value1, Some(b"value_from_sst".to_vec().into()));
        assert_eq!(value2, Some(b"value_from_sst".to_vec().into()));
        assert_eq!(value3, None);

        Ok(())
    }

    #[test]
    fn should_read_boundary_keys_given_flushed_sst() -> MidgeResult<()> {
        // Arrange: Create engine
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        // Write sorted keys
        for i in 0..20 {
            let key = format!("key_{i:03}");
            let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
            tx.put(key.as_bytes().to_vec(), b"test_value".to_vec(), None)?;
            tx.commit(WriteOptions::buffered())?;
        }

        engine.flush_cf(&cf)?;
        assert_published_sst(&engine)?;

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

        Ok(())
    }

    #[test]
    fn should_handle_memtable_and_sst_reads() -> MidgeResult<()> {
        // Arrange: Mix of memtable and SST data
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("test").expect("create cf");

        // Write to SST
        let mut tx1 = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)?;
        tx1.put(b"sst_key".to_vec(), b"sst_value".to_vec(), None)?;
        tx1.commit(WriteOptions::buffered())?;
        engine.flush_cf(&cf)?;
        assert_published_sst(&engine)?;

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
            .storage_verifier()
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
        assert_eq!(after_reopen.as_deref(), Some(value.as_slice()));
        assert_eq!(result.as_deref(), Some(value.as_slice()));
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
        // Arrange: callers immediately attempt the next commit after each flush.
        let recover_before_commit = false;
        // Act
        let (_, published_compactions, rewritten_bytes) =
            exercise_l0_pressure_recovery(recover_before_commit);
        // Assert: disabled ordinary background work still completes real pressure recovery.
        assert!(published_compactions > 0);
        assert!(rewritten_bytes > 0);
    }

    #[test]
    fn should_continue_writing_when_pressure_recovery_precedes_commit() {
        // Arrange: callers wait for recovery to clear pressure before admitting each write.
        let recover_before_commit = true;
        // Act
        let (rejected, published_compactions, rewritten_bytes) =
            exercise_l0_pressure_recovery(recover_before_commit);
        // Assert: completed recovery can keep every caller from observing a rejection.
        assert_eq!(rejected, 0);
        assert!(published_compactions > 0);
        assert!(rewritten_bytes > 0);
    }

    const L0_PRESSURE_ANCHOR: &[u8] = b"l0-pressure/anchor";

    fn exercise_l0_pressure_recovery(recover_before_commit: bool) -> (u64, u64, u64) {
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
        let mut rejected = 0_u64;
        // Act: the common anchor makes every flushed SST overlap. Pressure
        // recovery must physically merge this history instead of moving files.
        for index in 0_u32..32 {
            if recover_before_commit {
                assert!(engine
                    .wait_for_write_stall_clear(cf.id(), Duration::from_secs(10))
                    .unwrap());
            }
            let before = engine.metrics().get_runtime_metrics().unwrap();
            let commit = || {
                let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
                tx.put(index.to_be_bytes().to_vec(), b"value".to_vec(), None)?;
                tx.put(
                    L0_PRESSURE_ANCHOR.to_vec(),
                    index.to_be_bytes().to_vec(),
                    None,
                )?;
                tx.commit(WriteOptions::sync())
            };
            match commit() {
                Ok(()) => {}
                Err(MidgeError::WriteStall(_)) => {
                    rejected += 1;
                    let after = engine.metrics().get_runtime_metrics().unwrap();
                    assert_eq!(after.current_sequence, before.current_sequence);
                    assert_eq!(after.wal_append_count, before.wal_append_count);
                    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
                    assert_eq!(read.get(&index.to_be_bytes()).unwrap(), None);
                    let previous_anchor = index.checked_sub(1).map(u32::to_be_bytes);
                    assert_eq!(
                        read.get(L0_PRESSURE_ANCHOR).unwrap().as_deref(),
                        previous_anchor.as_ref().map(<[u8; 4]>::as_slice)
                    );
                    drop(read);
                    assert!(engine
                        .wait_for_write_stall_clear(cf.id(), Duration::from_secs(10))
                        .unwrap());
                    commit().unwrap();
                }
                Err(error) => panic!("unexpected write failure: {error}"),
            }
            engine.flush_cf(&cf).unwrap();
        }
        // Publication can win the race with the caller's next admission. Count
        // actual completed pressure work instead of requiring a losing caller.
        let recovered = engine.metrics().get_runtime_metrics().unwrap();
        let counts = cntryl_midge::__internal::diagnostics::write_admission_snapshot(&engine);
        assert_eq!(counts.commit_write_stall_total, rejected);
        assert_eq!(counts.l0_total + counts.ingest_hint_total, rejected);
        assert_eq!(
            counts.queue_total + counts.cloud_generation_total + counts.cloud_wal_total,
            0
        );
        engine.shutdown(Duration::from_secs(30)).unwrap();
        let reopened = Engine::open(
            OpenOptions::local(dir.path())
                .background_compaction(false)
                .build()
                .unwrap(),
        )
        .unwrap();
        assert_l0_history_after_reopen(&reopened);
        eprintln!(
            "L0 recovery: wait_before_commit={recover_before_commit} rejected={rejected} published_compactions={} rewritten_bytes={}",
            recovered.compactions_run, recovered.compaction_bytes_rewritten,
        );
        (
            rejected,
            recovered.compactions_run,
            recovered.compaction_bytes_rewritten,
        )
    }

    fn assert_l0_history_after_reopen(reopened: &cntryl_midge::Engine) {
        let cf = reopened.get_column_family("l0-pressure").unwrap();
        let tx = reopened
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        // Assert: every original row and the last atomic anchor survive restart.
        for index in 0_u32..32 {
            assert_eq!(
                tx.get(&index.to_be_bytes()).unwrap().as_deref(),
                Some(b"value".as_slice())
            );
        }
        assert_eq!(
            tx.get(L0_PRESSURE_ANCHOR).unwrap().as_deref(),
            Some(31_u32.to_be_bytes().as_slice())
        );
        let index_rows = tx
            .scan(&cntryl_midge::Query::new().prefix(vec![0_u8].into()))
            .unwrap()
            .try_collect()
            .unwrap();
        assert_eq!(index_rows.len(), 32);
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
        assert_eq!(rows, [] as [(bytes::Bytes, bytes::Bytes); 0]);
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
            .map(|entry| entry.expect("read SST directory entry"))
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
    use cntryl_midge::__internal::codec::{CompressionAlgo, BLOCK_TRAILER_SIZE};
    use cntryl_midge::{
        Engine, EngineHealth, Goal, OpenOptions, Query, RecoveryPolicy, TransactionMode,
        WorkloadProfile, WriteOptions,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;
    use xxhash_rust::xxh3::xxh3_64;

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
            .expect("V5 SST has fixed footer");
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
            assert!(block_end <= footer_start, "block extends into V5 footer");
            algorithms.push(bytes[block_end - BLOCK_TRAILER_SIZE]);
            cursor = block_end;
        }
        assert_eq!(cursor, footer_start, "blocks exactly precede V5 footer");
        algorithms
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
        let report = cntryl_midge::StorageVerifier::verify_path(temp.path())
            .expect("verify compacted database");
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
            .map(|entry| entry.expect("read SST directory entry"))
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
            .expect("SST path");
        let mut bytes = fs::read(&sst_path).expect("read SST");
        let footer_handle_byte = bytes.len() - 84 + 8;
        bytes[footer_handle_byte] ^= 0x01;
        fs::write(&sst_path, bytes).expect("corrupt footer");

        // Act
        let error = cntryl_midge::StorageVerifier::verify_path(temp.path())
            .expect_err("footer corruption must fail verify");

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
        let report = cntryl_midge::StorageVerifier::verify_path(temp.path())
            .expect("verify candidate adaptive database");
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
        let report = cntryl_midge::StorageVerifier::verify_path(temp.path())
            .expect("verify compacted database");
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
        assert_ne!(
            first_ssts,
            [] as [(std::path::PathBuf, std::vec::Vec<u8>); 0]
        );
        assert_eq!(first_ssts, second_ssts);
    }
}

mod compatibility_fixtures {
    use cntryl_midge::{
        Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

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

    fn alter_binary_manifest_fixture(path: &Path, field: &str, scalar: Option<u8>) {
        let mut bytes = fs::read(path).expect("read binary fixture manifest");
        assert_eq!(&bytes[..8], b"MDGEMD09");
        let payload_end = bytes.len() - 4;
        let value = bytes[13..payload_end]
            .windows(field.len())
            .position(|candidate| candidate == field.as_bytes())
            .expect("fixture field")
            + 13
            + field.len();
        if let Some(scalar) = scalar {
            // This fixture uses positive fixints and booleans for these fields.
            assert!(bytes[value] < 128 || matches!(bytes[value], 0xc2 | 0xc3));
            bytes[value] = scalar;
        } else {
            assert_eq!(
                bytes[value], 0xc4,
                "fixture bound must be a short byte string"
            );
            let length = usize::from(bytes[value + 1]);
            bytes[value + 2..value + 2 + length].fill(b'z');
        }
        let checksum = crc32c::crc32c(&bytes[..payload_end]);
        bytes[payload_end..].copy_from_slice(&checksum.to_be_bytes());
        fs::write(path, bytes).expect("write independently checksummed fixture mutation");
    }

    #[test]
    fn should_preserve_populated_v9_fixture_given_strict_recovery_when_reopening() {
        // Arrange
        let temp = copy_fixture_dir("v9_populated_v5_sst_db");
        let before = fs::read(temp.path().join("FORMAT")).expect("read fixture marker");

        for _ in 0..2 {
            // Act
            let report = cntryl_midge::StorageVerifier::verify_path(temp.path())
                .expect("verify format 9 fixture");
            let mut engine = Engine::open(
                OpenOptions::local(temp.path())
                    .recovery_policy(RecoveryPolicy::Strict)
                    .build()
                    .expect("build fixture options"),
            )
            .expect("open format 9 fixture");
            let cf = engine
                .get_column_family("default")
                .expect("fixture column family");
            let read = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin fixture read");

            // Assert
            assert_eq!(report.health, EngineHealth::Healthy);
            assert_eq!(report.sst_files_verified, 1);
            assert_eq!(
                read.get(b"fixture/alpha")
                    .expect("read persisted point")
                    .as_deref(),
                Some(b"value-alpha".as_slice())
            );
            drop(read);
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown fixture engine");
            assert_eq!(
                fs::read(temp.path().join("FORMAT")).expect("reread fixture marker"),
                before
            );
        }
    }

    fn open_published_v0_5_3_fixture(path: &Path, cloud: bool) -> Engine {
        let options = if cloud {
            OpenOptions::cloud_simulated(path, "midge-fixtures", "published-v0.5.3")
        } else {
            OpenOptions::local(path)
        };
        Engine::open(
            options
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build strict published fixture options"),
        )
        .expect("open published v0.5.3 fixture")
    }

    fn assert_published_v0_5_3_fixture_state(engine: &Engine) {
        let default = engine
            .get_column_family("default")
            .expect("published default family");
        let read = engine
            .begin_tx(default.id(), TransactionMode::ReadOnly)
            .expect("begin published default read");
        assert_eq!(
            read.get(b"fixture/current")
                .expect("read current fixture value")
                .as_deref(),
            Some(b"v2-current".as_slice())
        );
        assert_eq!(
            read.get(b"fixture/deleted")
                .expect("read tombstoned fixture key"),
            None
        );
        assert_eq!(
            read.get(b"fixture/wal-only")
                .expect("read acknowledged legacy WAL value")
                .as_deref(),
            Some(b"published-unflushed".as_slice())
        );
        drop(read);

        let archive = engine
            .get_column_family("archive")
            .expect("published archive family");
        let read = engine
            .begin_tx(archive.id(), TransactionMode::ReadOnly)
            .expect("begin published archive read");
        assert_eq!(
            read.get(b"archive/alpha")
                .expect("read archived fixture value")
                .as_deref(),
            Some(b"archived-value".as_slice())
        );
    }

    fn write_published_fixture_upgrade_values(engine: &Engine, cloud: bool) {
        let durability = if cloud {
            cntryl_midge::WriteOptions::cloud_strict()
        } else {
            cntryl_midge::WriteOptions::sync()
        };
        for (family, key, value) in [
            (
                "default",
                b"fixture/after-upgrade".as_slice(),
                b"upgraded-default".as_slice(),
            ),
            (
                "archive",
                b"archive/after-upgrade".as_slice(),
                b"upgraded-archive".as_slice(),
            ),
        ] {
            let cf = engine
                .get_column_family(family)
                .expect("published fixture family");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin post-upgrade write");
            tx.put(key.to_vec(), value.to_vec(), None)
                .expect("stage post-upgrade value");
            tx.commit(durability).expect("commit post-upgrade value");
            engine.flush_cf(&cf).expect("flush post-upgrade value");
        }
    }

    fn assert_published_fixture_artifacts(path: &Path, cloud: bool) {
        assert_eq!(
            fs::read_to_string(path.join("FORMAT")).expect("read fixture format"),
            "midge-format-version=9\n"
        );
        assert!(
            fs::metadata(path.join("intent_log.json"))
                .expect("read fixture intent journal")
                .len()
                > 0
        );
        assert!(path.join("wal/LEDGER").is_file());
        if cloud {
            assert!(path.join("wal/00000000000000000006.wal").is_file());
            for selected in [
                "cloud_store/metadata/authority-initialized.v1",
                "cloud_store/metadata/committed-pointer.v1",
                "cloud_store/metadata/ddl.registry.json",
                "cloud_store/midge_primary_lease.json",
                "cloud_store/wal/publication-catalog.v1.json",
                "cloud_store/wal/publication-catalog.v1.mirror.json",
                "cloud_store/wal/epochs/00000000000000000001/00000000000000000006.wal",
                "cloud_store/sst/000000_00_00000000000000000001.sst",
                "cloud_store/sst/000001_00_00000000000000000001.sst",
            ] {
                assert!(path.join(selected).is_file(), "missing {selected}");
            }
        } else {
            assert!(path.join("wal/00000000000000000002.wal").is_file());
            assert!(path
                .join("sst/000000_00_00000000000000000001.sst")
                .is_file());
            assert!(path
                .join("sst/000001_00_00000000000000000001.sst")
                .is_file());
            assert!(
                fs::metadata(path.join("manifest.journal"))
                    .expect("read fixture manifest journal")
                    .len()
                    > 0
            );
        }
    }

    fn assert_published_fixture_upgrade_values(engine: &Engine) {
        for (family, key, value) in [
            (
                "default",
                b"fixture/after-upgrade".as_slice(),
                b"upgraded-default".as_slice(),
            ),
            (
                "archive",
                b"archive/after-upgrade".as_slice(),
                b"upgraded-archive".as_slice(),
            ),
        ] {
            let cf = engine
                .get_column_family(family)
                .expect("post-upgrade fixture family");
            let read = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin post-upgrade fixture read");
            assert_eq!(
                read.get(key)
                    .expect("read post-upgrade fixture value")
                    .as_deref(),
                Some(value)
            );
        }
    }

    fn assert_published_fixture_upgrade_values_absent(engine: &Engine) {
        for (family, key) in [
            ("default", b"fixture/after-upgrade".as_slice()),
            ("archive", b"archive/after-upgrade".as_slice()),
        ] {
            let cf = engine
                .get_column_family(family)
                .expect("published fixture family");
            let read = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin restored fixture read");
            assert_eq!(read.get(key).expect("read restored key"), None);
        }
    }

    #[test]
    fn should_reopen_published_v0_5_3_fixtures_given_strict_recovery_and_new_writes() {
        for (fixture, cloud) in [
            ("v0_5_3_published_local", false),
            ("v0_5_3_published_cloud", true),
        ] {
            // Arrange
            let temp = copy_fixture_dir(fixture);
            assert_published_fixture_artifacts(temp.path(), cloud);

            // Act: open the published artifacts with current Strict startup.
            let mut engine = open_published_v0_5_3_fixture(temp.path(), cloud);

            // Assert: multiple families, values, and the stored tombstone survive.
            assert_published_v0_5_3_fixture_state(&engine);
            write_published_fixture_upgrade_values(&engine, cloud);
            engine
                .shutdown(Duration::from_secs(10))
                .expect("shutdown after upgrade writes");

            // Act: a second Strict open recovers and reads the new flushed state.
            let mut reopened = open_published_v0_5_3_fixture(temp.path(), cloud);

            // Assert
            assert_published_v0_5_3_fixture_state(&reopened);
            assert_published_fixture_upgrade_values(&reopened);
            reopened
                .shutdown(Duration::from_secs(10))
                .expect("shutdown after second fixture reopen");
        }
    }

    #[test]
    fn should_preserve_published_format_9_state_when_upgrading_local_metadata() {
        // Arrange
        let temp = copy_fixture_dir("v0_5_3_published_local");
        let names = [
            "manifest.snapshot.json",
            "manifest.journal",
            "intent_log.json",
            "wal/LEDGER",
            "wal/00000000000000000002.wal",
        ];
        let source =
            names.map(|name| fs::read(temp.path().join(name)).expect("read legacy source"));
        let opts = OpenOptions::local(temp.path())
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("strict local upgrade options");

        // Act
        Engine::upgrade_format_9(&opts, Duration::from_secs(10))
            .expect("upgrade published FORMAT 9 metadata");

        // Assert
        assert_eq!(
            fs::read(temp.path().join("FORMAT")).expect("read selected format"),
            b"midge-format-version=10\n"
        );
        for (name, original) in names.into_iter().zip(source) {
            assert_eq!(
                fs::read(temp.path().join(name)).expect("read preserved legacy bytes"),
                original,
                "upgrade mutated {name}"
            );
        }
        let mut engine = Engine::open(opts.clone()).expect("open upgraded published fixture");
        assert_published_v0_5_3_fixture_state(&engine);
        write_published_fixture_upgrade_values(&engine, false);
        engine
            .shutdown(Duration::from_secs(10))
            .expect("shutdown after FORMAT 10 writes");
        let mut reopened = Engine::open(opts).expect("reopen upgraded fixture");
        assert_published_v0_5_3_fixture_state(&reopened);
        assert_published_fixture_upgrade_values(&reopened);
        reopened
            .shutdown(Duration::from_secs(10))
            .expect("shutdown reopened upgrade fixture");
    }

    #[test]
    fn should_restore_published_data_from_format_10_backup() {
        // Arrange
        let source = copy_fixture_dir("v0_5_3_published_local");
        let output = tempfile::tempdir().expect("backup and restore parent");
        let artifact = output.path().join("artifact");
        let restore = output.path().join("restored");
        let timeout = Duration::from_secs(10);
        let opts = OpenOptions::local(source.path()).build().unwrap();
        Engine::upgrade_format_9(&opts, timeout).expect("upgrade published fixture");
        let mut engine = Engine::open(opts).expect("open FORMAT 10 source");

        // Act
        let inventory = engine
            .backup_to(&artifact, timeout)
            .expect("backup upgraded source");
        write_published_fixture_upgrade_values(&engine, false);
        engine
            .shutdown(timeout)
            .expect("shut down source after post-backup v10 writes");
        let restore_opts = OpenOptions::local(restore).build().unwrap();
        Engine::restore_backup(&artifact, restore_opts.clone()).expect("restore FORMAT 10 backup");
        let mut restored = Engine::open(restore_opts).expect("open restored FORMAT 10 data");

        // Assert
        assert_eq!(inventory.database_format_version, 10);
        assert!(inventory
            .objects
            .iter()
            .any(|object| object.path == "wal/LEDGER"));
        assert!(inventory
            .objects
            .iter()
            .any(|object| object.path.starts_with("sst/")));
        assert_published_v0_5_3_fixture_state(&restored);
        assert_published_fixture_upgrade_values_absent(&restored);
        restored.shutdown(timeout).expect("shut down restored data");
    }

    #[test]
    fn should_recover_published_cloudsim_data_after_format_10_upgrade() {
        // Arrange
        let source = copy_fixture_dir("v0_5_3_published_cloud");
        let cloud_options = |path: &Path| {
            OpenOptions::cloud_simulated(path, "midge-fixtures", "published-v0.5.3")
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("strict CloudSim options")
        };
        let fresh_cache = |prior: &Path| {
            let temp = tempfile::tempdir().expect("new cloud cache");
            copy_dir_recursive(&prior.join("cloud_store"), &temp.path().join("cloud_store"))
                .expect("retain only the simulated provider data");
            temp
        };
        let timeout = Duration::from_secs(30);

        // Act
        Engine::upgrade_format_9(&cloud_options(source.path()), timeout)
            .expect("upgrade published cloud metadata");
        let first = fresh_cache(source.path());
        let mut engine = Engine::open(cloud_options(first.path())).expect("recover upgraded cloud");
        assert_published_v0_5_3_fixture_state(&engine);
        // Six flushes require reusing the three durable cloud staging slots.
        for _ in 0..3 {
            write_published_fixture_upgrade_values(&engine, true);
        }
        engine.shutdown(timeout).expect("shut down upgraded cloud");
        let second = fresh_cache(first.path());
        let mut reopened =
            Engine::open(cloud_options(second.path())).expect("recover post-upgrade provider data");

        // Assert
        assert_eq!(
            fs::read(second.path().join("FORMAT")).expect("hydrated FORMAT"),
            b"midge-format-version=10\n"
        );
        assert_published_v0_5_3_fixture_state(&reopened);
        assert_published_fixture_upgrade_values(&reopened);
        reopened
            .shutdown(timeout)
            .expect("shut down upgraded recovery");
    }

    #[test]
    fn should_restore_format_9_from_backup_after_format_10_writes() {
        // Arrange
        let source = copy_fixture_dir("v0_5_3_published_local");
        let destination = tempfile::tempdir().expect("backup and restore workspace");
        let artifact = destination.path().join("legacy-backup");
        let options = OpenOptions::local(source.path())
            .build()
            .expect("source options");
        let restored_root = destination.path().join("restored");
        let restored_options = OpenOptions::local(&restored_root)
            .build()
            .expect("restore options");
        let timeout = Duration::from_secs(30);
        let mut original = Engine::open(options.clone()).expect("open original FORMAT 9");
        original
            .backup_to(&artifact, timeout)
            .expect("capture rollback backup");
        original
            .shutdown(timeout)
            .expect("release the original writer lease");

        // Act
        Engine::upgrade_format_9(&options, timeout).expect("upgrade after backup");
        let mut upgraded = Engine::open(options).expect("open upgraded source");
        write_published_fixture_upgrade_values(&upgraded, false);
        upgraded
            .shutdown(timeout)
            .expect("close after FORMAT 10 writes");
        Engine::restore_backup(&artifact, restored_options.clone()).expect("restore FORMAT 9");
        let mut restored = Engine::open(restored_options).expect("reopen restored FORMAT 9");

        // Assert
        assert_eq!(
            fs::read(restored_root.join("FORMAT")).expect("restored marker"),
            b"midge-format-version=9\n"
        );
        assert_published_v0_5_3_fixture_state(&restored);
        assert_published_fixture_upgrade_values_absent(&restored);
        restored.shutdown(timeout).expect("close restored FORMAT 9");
    }

    #[test]
    fn should_reject_populated_release_v3_fixture_given_unsupported_format_when_reopening() {
        // Arrange: format 3 predates the WAL segment ledger (#842).
        let temp = copy_fixture_dir("v3_populated_v4_sst_db");
        let before = fs::read(temp.path().join("FORMAT")).expect("read fixture marker");

        // Act
        let verify_error = cntryl_midge::StorageVerifier::verify_path(temp.path())
            .expect_err("format 3 fixture must fail verify");
        let Err(open_error) = Engine::open(
            OpenOptions::local(temp.path())
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        ) else {
            panic!("format 3 fixture must fail open");
        };

        // Assert
        assert_compatibility_error(verify_error);
        assert_compatibility_error(open_error);
        assert_eq!(
            fs::read(temp.path().join("FORMAT")).expect("reread fixture marker"),
            before,
            "a rejected open must not rewrite the marker"
        );
        assert!(!temp.path().join("wal").join("LEDGER").exists());
    }

    #[test]
    fn should_reject_populated_v8_fixture_when_format_nine_has_no_migration() {
        for policy in [RecoveryPolicy::Strict, RecoveryPolicy::Salvage] {
            // Arrange
            let temp = copy_fixture_dir("v8_populated_v4_sst_db");
            let before = fs::read(temp.path().join("FORMAT")).expect("read old marker");

            // Act
            let result = Engine::open(
                OpenOptions::local(temp.path())
                    .recovery_policy(policy)
                    .build()
                    .expect("build options"),
            );

            // Assert
            match result {
                Err(error) => assert_compatibility_error(error),
                Ok(_) => panic!("format 8 must be rejected under {policy:?}"),
            }
            assert_eq!(
                fs::read(temp.path().join("FORMAT")).expect("read retained marker"),
                before
            );
        }
    }

    #[test]
    fn should_reject_format_nine_sst_v4_data_under_every_recovery_policy() {
        for policy in [RecoveryPolicy::Strict, RecoveryPolicy::Salvage] {
            // Arrange: its database marker and metadata codec are current;
            // only the checksummed SST footer names the unsupported layout.
            let temp = copy_fixture_dir("v9_populated_v4_sst_db");
            let marker = fs::read(temp.path().join("FORMAT")).unwrap();
            let sst = temp.path().join("sst/000000_00_00000000000000000001.sst");
            let before = fs::read(&sst).unwrap();
            // Act
            let verification = cntryl_midge::StorageVerifier::verify_path(temp.path());
            let opened = Engine::open(
                OpenOptions::local(temp.path())
                    .recovery_policy(policy)
                    .build()
                    .unwrap(),
            );
            // Assert
            assert!(
                matches!(verification, Err(MidgeError::CompatibilityError(ref message)) if message.contains("SST format version 4") && message.contains("V5")),
                "{verification:?}"
            );
            let error = match opened {
                Err(error) => error,
                Ok(mut engine) => {
                    let cf = engine.get_column_family("default").unwrap();
                    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
                    let error = read
                        .get(b"fixture/alpha")
                        .expect_err("V4 reader must fail explicitly");
                    drop(read);
                    engine.shutdown(Duration::from_secs(5)).unwrap();
                    error
                }
            };
            assert!(
                matches!(error, MidgeError::CompatibilityError(ref message) if message.contains("SST format version 4") && message.contains("V5"))
            );
            assert_eq!(fs::read(temp.path().join("FORMAT")).unwrap(), marker);
            assert_eq!(fs::read(sst).unwrap(), before);
        }
    }

    #[test]
    fn should_reject_manifest_bounds_when_valid_sst_summary_disagrees() {
        // Arrange: the release fixture has a valid SST and a readable point value.
        let baseline = copy_fixture_dir("v9_populated_v5_sst_db");
        let baseline_report = cntryl_midge::StorageVerifier::verify_path(baseline.path())
            .expect("verify unmodified fixture");
        assert_eq!(baseline_report.health, EngineHealth::Healthy);
        let mut baseline_engine = Engine::open(
            OpenOptions::local(baseline.path())
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build baseline options"),
        )
        .expect("open unmodified fixture");
        let cf = baseline_engine
            .get_column_family("default")
            .expect("fixture column family");
        let read = baseline_engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin baseline read");
        assert_eq!(
            read.get(b"fixture/alpha")
                .expect("read baseline point")
                .as_deref(),
            Some(b"value-alpha".as_slice())
        );
        drop(read);
        baseline_engine
            .shutdown(Duration::from_secs(5))
            .expect("shutdown baseline engine");

        for wrong_key_bounds in [true, false] {
            let temp = copy_fixture_dir("v9_populated_v5_sst_db");
            {
                let name = "manifest.snapshot.json";
                let path = temp.path().join(name);
                if wrong_key_bounds {
                    alter_binary_manifest_fixture(&path, "smallest_key", None);
                    alter_binary_manifest_fixture(&path, "largest_key", None);
                    alter_binary_manifest_fixture(&path, "key_bounds_complete", Some(0xc3));
                } else {
                    alter_binary_manifest_fixture(&path, "smallest_seq", Some(99));
                    alter_binary_manifest_fixture(&path, "largest_seq", Some(99));
                }
            }

            // Act: SST bytes and their recorded size and CRC are unchanged.
            let result = cntryl_midge::StorageVerifier::verify_path(temp.path());

            // Assert: neither incorrect key bounds nor sequence bounds can pass.
            assert!(
                matches!(result, Err(MidgeError::Corruption(_))),
                "wrong_key_bounds={wrong_key_bounds}: {result:?}"
            );
        }
    }

    #[test]
    fn should_verify_manifest_key_bounds_according_to_trust_state() {
        for complete in [true, false] {
            // Arrange: complete bounds agree with the SST; older advisory bounds
            // may disagree without hiding a point key from readers.
            let temp = copy_fixture_dir("v9_populated_v5_sst_db");
            {
                let name = "manifest.snapshot.json";
                let path = temp.path().join(name);
                alter_binary_manifest_fixture(
                    &path,
                    "key_bounds_complete",
                    Some(if complete { 0xc3 } else { 0xc2 }),
                );
                if !complete {
                    alter_binary_manifest_fixture(&path, "smallest_key", None);
                    alter_binary_manifest_fixture(&path, "largest_key", None);
                }
            }

            // Act
            let report = cntryl_midge::StorageVerifier::verify_path(temp.path())
                .expect("valid complete or advisory bounds must verify");
            let mut engine = Engine::open(
                OpenOptions::local(temp.path())
                    .recovery_policy(RecoveryPolicy::Strict)
                    .build()
                    .expect("build fixture options"),
            )
            .expect("open fixture");
            let cf = engine
                .get_column_family("default")
                .expect("fixture column family");
            let read = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin fixture read");

            // Assert
            assert_eq!(report.health, EngineHealth::Healthy);
            assert_eq!(
                read.get(b"fixture/alpha")
                    .expect("read point through fixture")
                    .as_deref(),
                Some(b"value-alpha".as_slice()),
                "complete={complete}"
            );
            drop(read);
            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown fixture engine");
        }
    }

    #[test]
    fn should_reject_v2_empty_fixture_given_breaking_v5_sst_format() {
        // Arrange
        let temp = copy_fixture_dir("v2_empty_db");

        // Act
        let verify_error = cntryl_midge::StorageVerifier::verify_path(temp.path())
            .expect_err("V2 verify must fail");
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
    fn should_reject_v10_fixture_without_selected_head_when_reopening() {
        // Arrange
        let temp = copy_fixture_dir("future_v10");

        // Act
        let verify_error = cntryl_midge::StorageVerifier::verify_path(temp.path())
            .expect_err("FORMAT 10 fixture without HEAD should fail verify");
        assert!(verify_error.to_string().contains("FORMAT 10 head"));

        let Err(open_error) = Engine::open(
            OpenOptions::local(temp.path())
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        ) else {
            panic!("FORMAT 10 fixture without HEAD should fail open");
        };
        // Assert
        assert!(open_error.to_string().contains("FORMAT 10 head"));
    }

    #[test]
    fn should_reject_future_v11_fixture_when_reopening() {
        // Arrange
        let temp = copy_fixture_dir("future_v10");
        std::fs::write(temp.path().join("FORMAT"), b"midge-format-version=11\n")
            .expect("write future FORMAT 11 marker");

        // Act
        let verify_error = cntryl_midge::StorageVerifier::verify_path(temp.path())
            .expect_err("future fixture should fail verify");
        let Err(open_error) = Engine::open(
            OpenOptions::local(temp.path())
                .recovery_policy(RecoveryPolicy::Strict)
                .build()
                .expect("build options"),
        ) else {
            panic!("future fixture should fail open");
        };

        // Assert
        assert_compatibility_error(verify_error);
        assert_compatibility_error(open_error);
    }
}
