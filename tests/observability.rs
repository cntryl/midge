//! Observability Tests
//!
//! Consolidated from: `observability_api.rs`, `telemetry_integration.rs`, `read_amp_api.rs`, `read_path_diagnostics.rs`, `recovery_metrics_api.rs`, `hot_sst_tracking.rs`, `config_api.rs`

mod common;

mod telemetry_integration {
    //! Operation-integrity checks for code paths that are expected to emit
    //! telemetry when instrumentation is available, plus direct assertions
    //! against the real runtime-metrics/telemetry API (`Engine::get_runtime_metrics`,
    //! `Engine::flush_cf`'s flush counters, `Engine::compact_all`'s compaction
    //! counters) where such an API exists.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::TransactionMode;

    #[test]
    fn should_initialize_global_telemetry_once_given_repeated_init_calls_when_starting() {
        // Arrange
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let callers = (0..2)
            .map(|_| {
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    cntryl_midge::init_benchmark_telemetry()
                })
            })
            .collect::<Vec<_>>();

        // Act
        barrier.wait();
        let concurrent_results = callers
            .into_iter()
            .map(|caller| caller.join().expect("telemetry caller must not panic"))
            .collect::<Vec<_>>();
        let later = cntryl_midge::init_benchmark_telemetry();

        // Assert
        for result in concurrent_results {
            result.expect("concurrent public telemetry initialization should be idempotent");
        }
        later.expect("later public telemetry initialization should replay success");
    }

    #[test]
    fn should_preserve_all_values_given_repeated_reads_when_values_accessed_repeatedly() {
        cntryl_midge::init_benchmark_telemetry().expect("enable test-visible cache metrics");

        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write transaction");
            for i in 0..50 {
                let key = format!("metrics_read_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"metric_value".to_vec(), None)
                    .expect("put read-path value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Act: first read pass is expected to miss the block cache (data was
            // just flushed to disk), the repeated second pass should hit it.
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin first-pass read transaction");
            for i in 0..50 {
                let key = format!("metrics_read_key_{i:04}");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("read repeated-read key"),
                    Some(Bytes::from_static(b"metric_value")),
                    "mode: {mode} key: {key}"
                );
            }
            drop(tx);

            let before = engine.get_runtime_metrics().expect("runtime metrics");

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin repeated-read transaction");
            for i in 0..50 {
                let key = format!("metrics_read_key_{i:04}");
                assert_eq!(
                    tx.get(key.as_bytes()).expect("read repeated-read key"),
                    Some(Bytes::from_static(b"metric_value")),
                    "mode: {mode} key: {key}"
                );
            }

            // Assert: repeatedly accessing the same values must register block
            // cache hits in the telemetry runtime metrics.
            let after = engine.get_runtime_metrics().expect("runtime metrics");
            assert!(
                after.cache_hits > before.cache_hits,
                "mode: {mode} repeated reads should register block cache hits (before: {}, after: {})",
                before.cache_hits,
                after.cache_hits
            );
        });
    }

    #[test]
    fn should_preserve_all_values_given_compaction_when_requested() {
        cntryl_midge::init_benchmark_telemetry().expect("enable test-visible compaction metrics");

        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Flush enough separate L0 files (default l0_file_count_threshold is
            // 4) that compact_all() actually has work to schedule instead of
            // observing only two files and deciding no compaction is needed.
            let batches = 5;
            let keys_per_batch = 50;
            for batch in 0..batches {
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin batch transaction");
                for i in 0..keys_per_batch {
                    let key = format!("compact_metric_key_{:04}", batch * keys_per_batch + i);
                    let value = format!("gen{batch}").into_bytes();
                    tx.put(key.as_bytes().to_vec(), value, None)
                        .expect("put compaction batch value");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush batch");
            }

            let before = engine.get_runtime_metrics().expect("runtime metrics");

            // Act
            engine.compact_all().ok();

            // Assert: compaction must be recorded in the telemetry runtime
            // metrics, not just leave the data intact.
            let after = engine.get_runtime_metrics().expect("runtime metrics");
            assert!(
                after.compactions_run > before.compactions_run,
                "mode: {mode} compact_all should increment compactions_run (before: {}, after: {})",
                before.compactions_run,
                after.compactions_run
            );

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin verification transaction");
            for batch in 0..batches {
                for i in 0..keys_per_batch {
                    let key = format!("compact_metric_key_{:04}", batch * keys_per_batch + i);
                    let expected = Bytes::from(format!("gen{batch}").into_bytes());
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("read compacted key"),
                        Some(expected),
                        "mode: {mode} key: {key}"
                    );
                }
            }
        });
    }

    #[test]
    fn should_preserve_large_values_given_wal_backed_write_batch_when_flushed() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let value = vec![b'W'; 1024];

            let before = engine.get_runtime_metrics().expect("runtime metrics");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin wal-sized write transaction");
            for i in 0..100 {
                let key = format!("wal_metric_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), value.clone(), None)
                    .expect("put wal-sized value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Assert: flushing the WAL-backed write batch to an SST must be
            // recorded by the flush build/publish runtime metrics.
            let after = engine.get_runtime_metrics().expect("runtime metrics");
            assert!(
                after.flush_build_count > before.flush_build_count,
                "mode: {mode} flush_cf should increment flush_build_count (before: {}, after: {})",
                before.flush_build_count,
                after.flush_build_count
            );
            assert!(
                after.flush_publish_count > before.flush_publish_count,
                "mode: {mode} flush_cf should increment flush_publish_count (before: {}, after: {})",
                before.flush_publish_count,
                after.flush_publish_count
            );

            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin verification transaction");
            assert_eq!(
                tx.get(b"wal_metric_key_0000").expect("read first wal key"),
                Some(Bytes::from(value.clone()))
            );
            assert_eq!(
                tx.get(b"wal_metric_key_0099").expect("read last wal key"),
                Some(Bytes::from(value))
            );
        });
    }
}

mod read_amp_api {
    use cntryl_midge::{
        ColumnFamilyHandle, Engine, MidgeResult, OpenOptions, TransactionMode, WriteOptions,
    };
    use tempfile::TempDir;

    fn open_local_without_compaction(temp_dir: &TempDir) -> Engine {
        Engine::open(
            OpenOptions::local(temp_dir.path())
                .background_compaction(false)
                .build()
                .expect("build local options"),
        )
        .expect("open local engine")
    }

    fn flush_hot_key_generation(
        engine: &Engine,
        cf: &ColumnFamilyHandle,
        generation: usize,
    ) -> MidgeResult<()> {
        let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        tx.put(
            b"hot-key".to_vec(),
            format!("value-{generation:02}").into_bytes(),
            None,
        )?;
        tx.commit(WriteOptions::buffered())?;
        engine.flush_cf(cf)
    }

    #[test]
    fn should_expose_exact_read_amp_metrics_for_local_sst_reads() -> MidgeResult<()> {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_local_without_compaction(&temp_dir);
        let cf = engine.create_column_family("read-amp")?;
        flush_hot_key_generation(&engine, &cf, 0)?;
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Act
        for _ in 0..5 {
            assert_eq!(tx.get(b"hot-key")?.as_deref(), Some(b"value-00".as_slice()));
        }
        let metrics = engine.get_read_amp_metrics()?;

        // Assert
        assert_eq!(metrics.reads_total, 5);
        assert_eq!(metrics.ssts_touched_total, 5);
        assert_eq!(metrics.l0_ssts_touched_total, 5);
        assert_eq!(metrics.blocks_read_total, 10);
        assert!((metrics.avg_ssts_per_read - 1.0).abs() <= f64::EPSILON);
        assert!((metrics.avg_l0_ssts_per_read - 1.0).abs() <= f64::EPSILON);
        assert!((metrics.avg_blocks_per_read - 2.0).abs() <= f64::EPSILON);
        assert!((metrics.l0_overlap_rate - 1.0).abs() <= f64::EPSILON);
        Ok(())
    }

    #[test]
    fn should_track_exact_l0_overlap_across_overlapping_ssts() -> MidgeResult<()> {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_local_without_compaction(&temp_dir);
        let cf = engine.create_column_family("l0-overlap")?;
        for generation in 0..3 {
            flush_hot_key_generation(&engine, &cf, generation)?;
        }
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Act
        let value = tx.get(b"hot-key")?;
        let metrics = engine.get_read_amp_metrics()?;

        // Assert
        assert_eq!(value.as_deref(), Some(b"value-02".as_slice()));
        assert_eq!(metrics.reads_total, 1);
        assert_eq!(metrics.ssts_touched_total, 3);
        assert_eq!(metrics.l0_ssts_touched_total, 3);
        assert!((metrics.avg_ssts_per_read - 3.0).abs() <= f64::EPSILON);
        assert!((metrics.avg_l0_ssts_per_read - 3.0).abs() <= f64::EPSILON);
        assert!((metrics.l0_overlap_rate - 1.0).abs() <= f64::EPSILON);
        Ok(())
    }

    #[test]
    fn should_show_zero_read_amp_metrics_for_fresh_engine() -> MidgeResult<()> {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_local_without_compaction(&temp_dir);

        // Act
        let metrics = engine.get_read_amp_metrics()?;

        // Assert
        assert_eq!(metrics.reads_total, 0);
        assert_eq!(metrics.ssts_touched_total, 0);
        assert_eq!(metrics.l0_ssts_touched_total, 0);
        assert_eq!(metrics.blocks_read_total, 0);
        assert!(metrics.avg_ssts_per_read.abs() <= f64::EPSILON);
        assert!(metrics.avg_l0_ssts_per_read.abs() <= f64::EPSILON);
        assert!(metrics.avg_blocks_per_read.abs() <= f64::EPSILON);
        assert!(metrics.l0_overlap_rate.abs() <= f64::EPSILON);
        assert!(metrics.sst_budget_violation_rate.abs() <= f64::EPSILON);
        assert!(metrics.block_budget_violation_rate.abs() <= f64::EPSILON);
        Ok(())
    }

    #[test]
    fn should_accumulate_exact_average_across_multiple_local_sst_reads() -> MidgeResult<()> {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_local_without_compaction(&temp_dir);
        let cf = engine.create_column_family("read-average")?;
        for generation in 0..2 {
            flush_hot_key_generation(&engine, &cf, generation)?;
        }
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Act
        for _ in 0..10 {
            let _ = tx.get(b"hot-key")?;
        }
        let metrics = engine.get_read_amp_metrics()?;

        // Assert
        assert_eq!(metrics.reads_total, 10);
        assert_eq!(metrics.ssts_touched_total, 20);
        assert_eq!(metrics.l0_ssts_touched_total, 20);
        assert!((metrics.avg_ssts_per_read - 2.0).abs() <= f64::EPSILON);
        assert!((metrics.avg_l0_ssts_per_read - 2.0).abs() <= f64::EPSILON);
        Ok(())
    }

    #[test]
    fn should_report_budget_violations_for_one_high_amplification_read() -> MidgeResult<()> {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = open_local_without_compaction(&temp_dir);
        let cf = engine.create_column_family("budget-violation")?;
        for generation in 0..11 {
            flush_hot_key_generation(&engine, &cf, generation)?;
        }
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Act
        let value = tx.get(b"hot-key")?;
        let metrics = engine.get_read_amp_metrics()?;

        // Assert
        assert_eq!(value.as_deref(), Some(b"value-10".as_slice()));
        assert_eq!(metrics.reads_total, 1);
        assert_eq!(metrics.ssts_touched_total, 11);
        assert_eq!(metrics.l0_ssts_touched_total, 11);
        assert_eq!(metrics.blocks_read_total, 22);
        assert!((metrics.sst_budget_violation_rate - 1.0).abs() <= f64::EPSILON);
        assert!((metrics.block_budget_violation_rate - 1.0).abs() <= f64::EPSILON);
        Ok(())
    }
}

mod read_path_diagnostics {
    //! Regression coverage for benchmark read-path diagnostics.

    use crate::common::{open_with_mode, opts_for_mode};
    use cntryl_midge::{MidgeResult, Query, TransactionMode, WriteOptions};

    #[test]
    fn should_report_flushed_sst_read_deltas_without_setup_leakage() -> MidgeResult<()> {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("diagnostics")?;
        let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        write.put(b"key".to_vec(), b"value".to_vec(), None)?;
        write.commit(WriteOptions::best_effort())?;
        engine.flush_cf(&cf)?;
        let start = engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Act - the first read opens/populates caches; the second verifies hits.
        for _ in 0..2 {
            let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
            assert_eq!(read.get(b"key")?.as_deref(), Some(b"value".as_slice()));
        }
        let end = engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Assert - only the measured window contributes to each delta.
        assert!(end.read_only_begin_tx_count > start.read_only_begin_tx_count);
        assert!(end.read_only_snapshot_cache_hits > start.read_only_snapshot_cache_hits);
        assert!(end.sst_reader_cache_hits > start.sst_reader_cache_hits);
        assert!(end.sst_block_cache_hits > start.sst_block_cache_hits);
        assert!(end.candidate_blocks_checked > start.candidate_blocks_checked);
        assert!(end.data_blocks_read > start.data_blocks_read);
        Ok(())
    }

    #[test]
    fn should_isolate_read_path_diagnostics_between_engines() -> MidgeResult<()> {
        // Arrange
        let idle_engine = open_with_mode(&opts_for_mode("local"), "local");
        let busy_engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = busy_engine.create_column_family("diagnostics")?;
        let mut write = busy_engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        write.put(b"key".to_vec(), b"value".to_vec(), None)?;
        write.commit(WriteOptions::best_effort())?;
        busy_engine.flush_cf(&cf)?;

        let idle_start = idle_engine.read_path_diagnostics_snapshot_for_benchmarks();
        let busy_start = busy_engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Act
        for _ in 0..2 {
            let read = busy_engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
            assert_eq!(read.get(b"key")?.as_deref(), Some(b"value".as_slice()));
        }

        let idle_end = idle_engine.read_path_diagnostics_snapshot_for_benchmarks();
        let busy_end = busy_engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Assert
        assert_eq!(
            idle_end, idle_start,
            "another engine leaked into idle metrics"
        );
        assert!(
            busy_end.read_only_begin_tx_count > busy_start.read_only_begin_tx_count,
            "the owning engine did not observe its read-only transactions"
        );
        assert!(
            busy_end.sst_block_cache_hits > busy_start.sst_block_cache_hits,
            "the owning engine did not observe its block-cache activity"
        );
        Ok(())
    }

    #[test]
    fn should_report_zero_delta_for_an_idle_engine_window() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");

        // Act
        let start = engine.read_path_diagnostics_snapshot_for_benchmarks();
        let end = engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Assert
        assert_eq!(end, start);
    }

    #[test]
    fn should_reject_absent_key_with_bloom_without_reading_data_block() -> MidgeResult<()> {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("bloom-observability")?;
        let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        for index in (0..400).step_by(2) {
            write.put(
                format!("key-{index:04}").into_bytes(),
                b"present".to_vec(),
                None,
            )?;
        }
        write.commit(WriteOptions::best_effort())?;
        engine.flush_cf(&cf)?;

        // Act: find a deterministic in-range miss that the persisted bloom rejects.
        let mut observed_reject = false;
        for index in (1..399).step_by(2) {
            let before = engine.get_runtime_metrics()?;
            let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
            assert!(read.get(format!("key-{index:04}").as_bytes())?.is_none());
            let after = engine.get_runtime_metrics()?;
            if after.sst_bloom_rejects_total > before.sst_bloom_rejects_total {
                assert!(after.sst_bloom_checks_total > before.sst_bloom_checks_total);
                assert_eq!(
                    after.sst_data_blocks_read_total, before.sst_data_blocks_read_total,
                    "a definite bloom rejection must avoid data-block I/O"
                );
                observed_reject = true;
                break;
            }
        }

        // Assert
        assert!(
            observed_reject,
            "expected at least one persisted-bloom rejection"
        );
        Ok(())
    }

    #[test]
    fn should_count_flushed_range_scan_work_in_its_own_window() -> MidgeResult<()> {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("diagnostics")?;
        let mut write = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        write.put(b"a".to_vec(), b"one".to_vec(), None)?;
        write.put(b"b".to_vec(), b"two".to_vec(), None)?;
        write.put(b"c".to_vec(), b"three".to_vec(), None)?;
        write.commit(WriteOptions::best_effort())?;

        let mut delete = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        delete.delete_range(b"b".to_vec(), b"c".to_vec())?;
        delete.commit(WriteOptions::best_effort())?;
        engine.flush_cf(&cf)?;
        let start = engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Act
        let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        let rows = read.scan(&Query::new())?.try_collect()?;
        let end = engine.read_path_diagnostics_snapshot_for_benchmarks();

        // Assert
        assert_eq!(rows.len(), 2);
        assert!(end.candidate_sst_files_checked > start.candidate_sst_files_checked);
        assert!(end.candidate_blocks_checked > start.candidate_blocks_checked);
        assert!(end.data_blocks_read > start.data_blocks_read);
        assert!(end.range_tombstone_scans > start.range_tombstone_scans);
        Ok(())
    }
}

mod recovery_metrics_api {
    //! Integration tests for recovery metrics API.
    //!
    //! Validates engine-level visibility into startup recovery work.

    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use serde::Serialize;
    use std::fs;
    use std::time::Duration;
    use tempfile::TempDir;

    #[derive(Serialize)]
    enum TestIntentLogEntry {
        WalSynced { segment_id: u64, seqno: u64 },
    }

    fn initialize_format_marker(db_path: &std::path::Path) {
        let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("initialize engine");
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown initialized engine");
    }

    #[test]
    fn should_report_wal_recovery_metrics_after_reopen_when_wal_replay_occurs() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        {
            let mut engine =
                Engine::open(OpenOptions::local(db_path).build().expect("build options"))
                    .expect("open engine");
            let cf = engine
                .get_column_family("default")
                .expect("default column family exists");

            // Commit without explicit flush to force WAL replay on reopen.
            for i in 0..20 {
                let key = format!("recovery-key-{i}").into_bytes();
                let value = format!("recovery-value-{i}").into_bytes();
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin tx");
                tx.put(key, value, None).expect("put");
                tx.commit(WriteOptions::buffered()).expect("commit");
            }
            engine
                .shutdown(Duration::from_secs(2))
                .expect("shutdown before reopen");
        }

        // Act
        let reopened = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("reopen engine");
        let recovery = reopened
            .get_recovery_metrics()
            .expect("get recovery metrics");

        // Assert
        assert!(
            recovery.wal_recovery_records_replayed > 0,
            "expected WAL recovery to replay at least one record"
        );
        assert!(
            recovery.wal_recovery_bytes_replayed > 0,
            "expected WAL recovery to replay at least one byte"
        );
        assert!(
            recovery.intent_log_replay_runs <= 1,
            "startup performs at most one intent replay run"
        );
    }

    #[test]
    fn should_expose_zero_recovery_metrics_on_fresh_engine_when_no_replay_needed() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        // Act
        let engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let recovery = engine.get_recovery_metrics().expect("get recovery metrics");

        // Assert
        assert_eq!(recovery.wal_recovery_records_replayed, 0);
        assert_eq!(recovery.wal_recovery_bytes_replayed, 0);
        assert_eq!(recovery.intent_log_replay_runs, 0);
        assert_eq!(recovery.intent_log_entries_replayed, 0);
    }

    #[test]
    fn should_report_intent_log_replay_metrics_after_reopen_when_manifest_intents_persisted() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();
        initialize_format_marker(db_path);

        let intents = vec![
            TestIntentLogEntry::WalSynced {
                segment_id: 7,
                seqno: 11,
            },
            TestIntentLogEntry::WalSynced {
                segment_id: 8,
                seqno: 12,
            },
        ];

        let intent_json = serde_json::to_string_pretty(&intents).expect("serialize intent fixture");
        fs::write(db_path.join("intent_log.json"), intent_json).expect("write intent fixture");

        // Act
        let engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
            .expect("open engine");
        let recovery = engine.get_recovery_metrics().expect("get recovery metrics");

        // Assert
        assert_eq!(
            recovery.intent_log_replay_runs, 1,
            "expected exactly one intent replay run during startup"
        );
        assert!(
            recovery.intent_log_entries_replayed >= 2,
            "expected fixture intent entries to be replayed"
        );
    }
}

mod hot_sst_tracking {
    //! Read-path behavior across multiple flushed SST generations.
    //!
    //! The engine does not currently expose hot-SST counters through a public test
    //! API, so these tests verify only the externally observable read behavior that
    //! exercises those code paths.

    use crate::common::{open_with_mode, opts_for_mode};
    use bytes::Bytes;
    use cntryl_midge::MidgeResult;
    use cntryl_midge::{TransactionMode, WriteOptions};

    #[test]
    fn should_return_latest_value_given_overlapping_l0_keys_when_multiple_batches_flushed(
    ) -> MidgeResult<()> {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        for batch in 0..3 {
            for i in 0..5 {
                let key = format!("key{i:03}");
                let value = format!("value_batch{batch}");
                let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)?;
                tx.commit(WriteOptions::best_effort())?;
            }
            engine.flush_cf(&cf)?;
        }

        // Act
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Assert
        assert_eq!(
            read_tx.get(b"key000")?,
            Some(Bytes::from_static(b"value_batch2"))
        );
        assert_eq!(
            read_tx.get(b"key004")?,
            Some(Bytes::from_static(b"value_batch2"))
        );
        Ok(())
    }

    #[test]
    fn should_find_keys_given_disjoint_key_ranges_when_flushed() -> MidgeResult<()> {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..10 {
            let key = format!("a{i:03}");
            let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
            tx.put(key.as_bytes().to_vec(), b"value_a".to_vec(), None)?;
            tx.commit(WriteOptions::best_effort())?;
        }
        engine.flush_cf(&cf)?;

        for i in 0..10 {
            let key = format!("b{i:03}");
            let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
            tx.put(key.as_bytes().to_vec(), b"value_b".to_vec(), None)?;
            tx.commit(WriteOptions::best_effort())?;
        }
        engine.flush_cf(&cf)?;

        for i in 0..10 {
            let key = format!("c{i:03}");
            let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
            tx.put(key.as_bytes().to_vec(), b"value_c".to_vec(), None)?;
            tx.commit(WriteOptions::best_effort())?;
        }
        engine.flush_cf(&cf)?;

        // Act
        let read_tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;

        // Assert
        assert_eq!(read_tx.get(b"b005")?, Some(Bytes::from_static(b"value_b")));
        assert_eq!(read_tx.get(b"a005")?, Some(Bytes::from_static(b"value_a")));
        assert_eq!(read_tx.get(b"c005")?, Some(Bytes::from_static(b"value_c")));
        Ok(())
    }
}

mod config_api {
    //! Config API Integration Tests
    //!
    //! Tests the builder-based configuration system that derives low-level parameters
    //! from high-level optimization goals (latency/throughput/cost), memory budgets,
    //! and workload profiles.
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>
    //!
    //! These tests validate the config builder's behavior without requiring an
    //! engine instance, since configuration is orthogonal to storage modes.

    use cntryl_midge::{
        BlockCachePolicy, Goal, MemoryBudget, MidgeError, MidgeResult, OpenOptions,
        OpenOptionsBuilder, Storage, WorkloadProfile,
    };
    use std::path::PathBuf;
    use std::time::Duration;

    // ============================================================================
    // BUILDER INITIALIZATION TESTS
    // ============================================================================

    #[test]
    fn should_require_finalization_when_constructing_open_options() {
        // Arrange

        // Act
        let builder: OpenOptionsBuilder = OpenOptions::in_memory();
        let finalized: MidgeResult<OpenOptions> = builder.build();

        // Assert: `build()` is what actually produces a usable `OpenOptions` (the
        // builder type itself exposes no storage/goal/etc. getters), and the
        // finalized value carries through what was configured.
        let opts = finalized.expect("builder must finalize into OpenOptions");
        assert_eq!(opts.storage(), &Storage::InMemory);
    }

    #[test]
    fn should_derive_same_pools_when_builder_calls_are_reordered() {
        // Arrange
        let budget = MemoryBudget::Bytes(512 * 1024 * 1024);

        // Act
        let first = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .workload(WorkloadProfile::WriteHeavy)
            .memory_budget(budget)
            .build()
            .expect("build first order");
        let second = OpenOptions::in_memory()
            .memory_budget(budget)
            .workload(WorkloadProfile::WriteHeavy)
            .goal(Goal::Throughput)
            .build()
            .expect("build second order");

        // Assert
        assert_eq!(first.memtable_size_limit(), second.memtable_size_limit());
        assert_eq!(first.block_cache_size(), second.block_cache_size());
        assert_eq!(
            first.transaction_memory_pool_size(),
            second.transaction_memory_pool_size()
        );
    }

    #[test]
    fn should_keep_accounted_pools_within_total_given_tiny_and_normal_budgets() {
        // Arrange
        let budgets = [10usize, 31, 1024, 128 * 1024, 512 * 1024 * 1024];

        for budget in budgets {
            // Act
            let opts = OpenOptions::in_memory()
                .memory_budget(MemoryBudget::Bytes(budget))
                .build()
                .expect("derive bounded pools");
            let accounted = opts
                .memtable_size_limit()
                .saturating_mul(2)
                .saturating_add(opts.block_cache_size())
                .saturating_add(opts.transaction_memory_pool_size())
                .saturating_add((budget / 5).min(256 * 1024 * 1024));

            // Assert
            assert!(
                accounted <= opts.memory_budget_bytes(),
                "budget={budget} accounted={accounted}"
            );
            assert!(
                opts.transaction_memory_pool_size() > 0,
                "budget={budget} must retain a nonzero transaction pool"
            );
        }
    }

    #[test]
    fn should_reject_tiny_budget_when_bounded_compaction_pool_cannot_fit() {
        // Arrange
        let budgets = [1usize, 2, 3];

        for budget in budgets {
            // Act
            let result = OpenOptions::in_memory()
                .memory_budget(MemoryBudget::Bytes(budget))
                .build();

            // Assert
            assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        }
    }

    #[test]
    fn should_allocate_ten_percent_to_transactions_given_default_pool() {
        // Arrange
        let budget = 1_000usize;

        // Act
        let opts = OpenOptions::in_memory()
            .memory_budget(MemoryBudget::Bytes(budget))
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.transaction_memory_pool_size(), 100);
    }

    #[test]
    fn should_reject_transaction_pool_given_override_exceeds_total_budget() {
        // Arrange

        // Act
        let error = OpenOptions::in_memory()
            .memory_budget(MemoryBudget::Bytes(1024))
            .transaction_memory_pool_size(1025)
            .build()
            .expect_err("oversized pool must fail");

        // Assert
        assert!(matches!(error, MidgeError::ResourceLimit(_)));
    }

    #[test]
    fn should_preserve_storage_io_timeout_given_valid_override() {
        // Arrange
        let timeout = Duration::from_millis(17);

        // Act
        let opts = OpenOptions::in_memory()
            .storage_io_timeout(timeout)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.storage_io_timeout(), timeout);
    }

    #[test]
    fn should_preserve_runtime_response_timeout_given_valid_override() {
        // Arrange
        let timeout = Duration::from_secs(45);

        // Act
        let opts = OpenOptions::in_memory()
            .runtime_response_timeout(timeout)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.runtime_response_timeout(), timeout);
    }

    #[test]
    fn should_derive_runtime_response_timeout_above_storage_timeout_when_not_overridden() {
        // Arrange
        let storage_timeout = Duration::from_secs(75);

        // Act
        let opts = OpenOptions::in_memory()
            .storage_io_timeout(storage_timeout)
            .build()
            .expect("derive coherent runtime deadline");

        // Assert
        assert!(opts.runtime_response_timeout() > storage_timeout);
    }

    #[test]
    fn should_reject_runtime_response_timeout_when_not_above_storage_timeout() {
        // Arrange
        let timeout = Duration::from_secs(30);

        // Act
        let error = OpenOptions::in_memory()
            .storage_io_timeout(timeout)
            .runtime_response_timeout(timeout)
            .build()
            .expect_err("runtime response timeout must enclose storage I/O");

        // Assert
        assert!(matches!(
            error,
            MidgeError::InvalidArgument(message)
                if message.contains("runtime response timeout")
                    && message.contains("greater than storage I/O timeout")
        ));
    }

    #[test]
    fn should_reject_zero_storage_io_timeout_when_building() {
        // Arrange

        // Act
        let error = OpenOptions::in_memory()
            .storage_io_timeout(Duration::ZERO)
            .build()
            .expect_err("zero timeout must fail");

        // Assert
        assert!(matches!(error, MidgeError::InvalidArgument(_)));
    }

    #[test]
    fn should_reject_sub_millisecond_storage_io_timeout_when_building() {
        // Arrange
        let timeout = Duration::from_micros(999);

        // Act
        let error = OpenOptions::in_memory()
            .storage_io_timeout(timeout)
            .build()
            .expect_err("sub-millisecond timeout cannot be represented by cloud providers");

        // Assert
        assert!(matches!(
            error,
            MidgeError::InvalidArgument(message) if message.contains("at least 1 millisecond")
        ));
    }

    #[test]
    fn should_build_config_given_minimal_defaults_when_only_path_provided() {
        // Arrange

        // Act
        let opts = OpenOptions::in_memory().build().expect("build options");

        // Assert
        assert_eq!(opts.storage(), &Storage::InMemory);
        assert_eq!(opts.goal(), Goal::Latency);
        assert_eq!(opts.memory_budget(), MemoryBudget::Auto);
        assert_eq!(opts.workload(), WorkloadProfile::Mixed);
        assert_eq!(opts.block_cache_policy_value(), BlockCachePolicy::Lru);
    }

    #[test]
    fn should_set_block_cache_policy_given_override_when_building() {
        // Arrange

        // Act
        let opts = OpenOptions::in_memory()
            .block_cache_policy(BlockCachePolicy::TinyLfu)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.block_cache_policy_value(), BlockCachePolicy::TinyLfu);
    }

    // ============================================================================
    // GOAL SETTING TESTS
    // ============================================================================

    #[test]
    fn should_set_goal_given_latency_when_optimizing_for_p99() {
        // Arrange

        // Act
        let opts = OpenOptions::in_memory()
            .goal(Goal::Latency)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.goal(), Goal::Latency);
        assert!(
            opts.block_size() <= 32 * 1024,
            "Latency goal should use small blocks"
        );
    }

    #[test]
    fn should_set_goal_given_throughput_when_optimizing_for_bulk_writes() {
        // Arrange

        // Act
        let opts = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.goal(), Goal::Throughput);
        assert!(
            opts.block_size() >= 64 * 1024,
            "Throughput goal should use larger blocks"
        );
    }

    #[test]
    fn should_set_goal_given_cost_when_minimizing_resources() {
        // Arrange

        // Act
        let opts = OpenOptions::in_memory()
            .goal(Goal::Economy)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.goal(), Goal::Economy);
        // Cost should allocate less to cache and memtables
        assert!(
            opts.block_cache_size() <= 256 * 1024 * 1024,
            "Cost should limit cache"
        );
    }

    // ============================================================================
    // MEMORY BUDGET TESTS
    // ============================================================================

    #[test]
    fn should_respect_memory_budget_given_explicit_bytes_when_configured() {
        // Arrange
        let budget = MemoryBudget::Bytes(256 * 1024 * 1024); // 256MB

        // Act
        let opts = OpenOptions::in_memory()
            .memory_budget(budget)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.memory_budget(), budget);
        assert!(
            opts.block_cache_size() > 0,
            "Cache should be allocated from budget"
        );
    }

    #[test]
    fn should_use_auto_memory_given_no_explicit_budget_when_default() {
        // Arrange

        // Act
        let opts = OpenOptions::in_memory().build().expect("build options");

        // Assert
        assert_eq!(opts.memory_budget(), MemoryBudget::Auto);
        // Auto should pick a sensible default
        assert!(
            opts.block_cache_size() > 0,
            "Auto budget should still allocate cache"
        );
    }

    #[test]
    fn should_preserve_default_memtable_size_given_no_override_when_building() {
        // Arrange

        // Act
        let opts = OpenOptions::in_memory().build().expect("build options");

        // Assert
        assert_eq!(opts.memtable_size_limit(), 64 * 1024 * 1024);
    }

    #[test]
    fn should_override_memtable_size_given_explicit_builder_when_building() {
        // Arrange
        let memtable_size = 128 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .with_memtable_size_limit(memtable_size)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.memtable_size_limit(), memtable_size);
    }

    // ============================================================================
    // WORKLOAD PROFILE OPTIMIZATION TESTS
    // ============================================================================

    #[test]
    fn should_optimize_params_given_write_heavy_profile_when_configured() {
        // Arrange
        let normal = OpenOptions::in_memory()
            .workload(WorkloadProfile::Mixed)
            .build()
            .expect("build normal options");

        // Act
        let write_heavy = OpenOptions::in_memory()
            .workload(WorkloadProfile::WriteHeavy)
            .build()
            .expect("build write-heavy options");

        // Assert
        assert_eq!(write_heavy.workload(), WorkloadProfile::WriteHeavy);
        assert!(
            write_heavy.memtable_size_limit() >= normal.memtable_size_limit(),
            "Write-heavy should have larger memtables"
        );
    }

    #[test]
    fn should_optimize_params_given_read_mostly_profile_when_configured() {
        // Arrange
        let normal = OpenOptions::in_memory()
            .workload(WorkloadProfile::Mixed)
            .build()
            .expect("build normal options");

        // Act
        let read_mostly = OpenOptions::in_memory()
            .workload(WorkloadProfile::ReadMostly)
            .build()
            .expect("build read-mostly options");

        // Assert
        assert_eq!(read_mostly.workload(), WorkloadProfile::ReadMostly);
        assert!(
            read_mostly.block_cache_size() >= normal.block_cache_size(),
            "Read-mostly should prioritize cache"
        );
    }

    #[test]
    fn should_optimize_params_given_range_scan_profile_when_configured() {
        // Arrange
        let normal = OpenOptions::in_memory()
            .build()
            .expect("build normal options");

        // Act
        let range_scan = OpenOptions::in_memory()
            .workload(WorkloadProfile::RangeScan)
            .build()
            .expect("build range-scan options");

        // Assert
        assert_eq!(range_scan.workload(), WorkloadProfile::RangeScan);
        assert!(
            range_scan.block_size() >= normal.block_size(),
            "Range scan should use larger blocks"
        );
    }

    // ============================================================================
    // INTERACTION TESTS (Multiple Knobs)
    // ============================================================================

    #[test]
    fn should_derive_consistent_params_given_all_knobs_set_when_building() {
        // Arrange

        // Act
        let opts = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .memory_budget(MemoryBudget::Bytes(1024 * 1024 * 1024)) // 1GB
            .workload(WorkloadProfile::WriteHeavy)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.goal(), Goal::Throughput);
        assert!(
            opts.memtable_size_limit() > 64 * 1024 * 1024,
            "Write-heavy + throughput should have large memtables"
        );
    }

    #[test]
    fn should_derive_different_params_given_latency_vs_throughput_when_comparing() {
        // Arrange
        let latency_opts = OpenOptions::in_memory()
            .goal(Goal::Latency)
            .build()
            .expect("build latency options");

        // Act
        let throughput_opts = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .build()
            .expect("build throughput options");

        // Assert
        assert_ne!(
            latency_opts.block_size(),
            throughput_opts.block_size(),
            "Latency and throughput should use different block sizes"
        );
        assert_ne!(
            latency_opts.memtable_size_limit(),
            throughput_opts.memtable_size_limit(),
            "Latency and throughput should use different memtable sizes"
        );
        assert_ne!(
            latency_opts.target_sst_size(),
            throughput_opts.target_sst_size(),
            "Latency and throughput should use different target SST sizes"
        );
    }

    // ============================================================================
    // GETTER TESTS
    // ============================================================================

    #[test]
    fn should_provide_getter_access_given_derived_params_when_querying() {
        // Arrange: a small and a much larger explicit memory budget, so the getters
        // can be checked against a concrete, falsifiable relationship instead of just
        // being non-zero.
        let small = OpenOptions::in_memory()
            .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
            .build()
            .expect("build small-budget options");
        let large = OpenOptions::in_memory()
            .memory_budget(MemoryBudget::Bytes(1024 * 1024 * 1024))
            .build()
            .expect("build large-budget options");

        // Act
        let cache_grows = large.block_cache_size() > small.block_cache_size();

        // Assert: getters actually reflect the derivation from the memory budget,
        // and the derived pools respect the invariants the engine relies on.
        assert!(
            cache_grows,
            "a larger memory budget must derive a larger block cache"
        );
        assert!(
            large.memtable_size_limit() >= small.memtable_size_limit(),
            "a larger memory budget must derive at least as large a memtable limit"
        );
        for opts in [&small, &large] {
            assert!(
                opts.memtable_flush_threshold() <= opts.memtable_size_limit(),
                "flush threshold must never exceed the memtable size limit"
            );
            assert!(opts.target_sst_size() > 0);
            assert!(opts.wal_buffer_size() > 0);
            assert!(opts.l0_compaction_trigger() > 0);
        }
    }

    // ============================================================================
    // PATH HANDLING TESTS
    // ============================================================================

    #[test]
    fn should_store_path_given_relative_path_when_building() {
        // Arrange

        // Act
        let opts = OpenOptions::local("./relative/path")
            .build()
            .expect("build options");

        // Assert
        assert_eq!(
            opts.storage(),
            &Storage::Local {
                path: PathBuf::from("./relative/path")
            }
        );
    }

    // ============================================================================
    // CLONE AND DEFAULT TESTS
    // ============================================================================

    #[test]
    fn should_clone_options_preserving_all_settings_given_configured_opts_when_cloning() {
        // Arrange
        let original = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .workload(WorkloadProfile::WriteHeavy)
            .runtime_response_timeout(Duration::from_secs(45))
            .build()
            .expect("build options");

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.storage(), original.storage());
        assert_eq!(cloned.goal(), original.goal());
        assert_eq!(cloned.workload(), original.workload());
        assert_eq!(cloned.block_size(), original.block_size());
        assert_eq!(cloned.memtable_size_limit(), original.memtable_size_limit());
        assert_eq!(
            cloned.runtime_response_timeout(),
            original.runtime_response_timeout()
        );
        assert_eq!(
            cloned.block_cache_policy_value(),
            original.block_cache_policy_value()
        );
    }
}
