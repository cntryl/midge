//! Cloud Local-Disk Admission and Flush Budget Tests
//!
//! Consolidated from: `cloud_eventual_flush.rs`, `cloud_local_disk_admission.rs`, `cloud_many_families.rs`, `cloud_small_memory.rs`

mod common;

mod cloud_eventual_flush {
    use crate::common::{opts_for_mode, MidgeOptions};
    use cntryl_midge::{
        CloudWritePolicy, Engine, RuntimeMetricsSnapshot, TransactionMode, WriteOptions,
    };
    use std::thread;
    use std::time::{Duration, Instant};

    const LARGE_MEMTABLE_BYTES: usize = 512 * 1024 * 1024;
    const BUFFERED_TEST_GAP: u64 = 4;

    fn buffered_cloud_policy() -> CloudWritePolicy {
        CloudWritePolicy {
            eventual_flush_segment_gap: BUFFERED_TEST_GAP,
            wal_seal_min_segment_bytes: usize::MAX,
            wal_seal_max_flush_delay: Duration::from_hours(1),
            wal_seal_max_pending_writes: 1,
        }
    }

    fn open_large_cloud_engine(opts: &MidgeOptions) -> Engine {
        let mut opts = opts.clone();
        opts.memtable_size = LARGE_MEMTABLE_BYTES;
        Engine::open(opts.to_open_options()).expect("open cloud engine")
    }

    fn default_cf(engine: &Engine) -> cntryl_midge::ColumnFamilyHandle {
        engine
            .get_column_family("default")
            .expect("default column family")
    }

    fn commit_small_write(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        opts: WriteOptions,
    ) {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(key.to_vec(), value.to_vec(), None)
            .expect("put small value");
        tx.commit(opts).expect("commit small value");
    }

    fn wait_for_metrics<F>(
        engine: &Engine,
        timeout: Duration,
        predicate: F,
    ) -> RuntimeMetricsSnapshot
    where
        F: Fn(&RuntimeMetricsSnapshot) -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            let metrics = engine.get_runtime_metrics().expect("runtime metrics");
            if predicate(&metrics) {
                return metrics;
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for runtime condition; last metrics: sst_count={} persisted_seq={} wal_segment={} current_seq={} cloud_seq={} gap={}",
                metrics.sst_count,
                metrics.manifest_last_persisted_sequence,
                metrics.wal_current_segment_id,
                metrics.current_sequence,
                metrics.wal_cloud_durable_seq,
                metrics.max_memtable_wal_segment_gap
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    #[test]
    fn should_eventually_publish_sst_given_many_cloud_strict_writes_when_memtable_never_reaches_size_threshold(
    ) {
        // Arrange
        let engine = open_large_cloud_engine(&opts_for_mode("cloud"));
        let cf = default_cf(&engine);

        for i in 0..160 {
            let key = format!("strict-gap-key-{i:04}");
            commit_small_write(
                &engine,
                &cf,
                key.as_bytes(),
                b"strict-gap-value",
                WriteOptions::cloud_strict(),
            );
        }

        let pre_flush_metrics = engine.get_runtime_metrics().expect("runtime metrics");
        // Act
        // Assert
        assert!(
            pre_flush_metrics.max_memtable_wal_segment_gap > 0,
            "cloud segment gap metric should grow while WAL segments rotate ahead of SST publication"
        );

        let metrics = wait_for_metrics(&engine, Duration::from_secs(3), |metrics| {
            metrics.sst_count >= 1 && metrics.manifest_last_persisted_sequence > 0
        });

        assert!(
            metrics.wal_current_segment_id > 100,
            "expected many sealed WAL segments before the first SST publish; saw segment {}",
            metrics.wal_current_segment_id
        );
        assert!(
            metrics.max_memtable_wal_segment_gap < 128,
            "automatic flush should reset the active memtable segment gap below the production threshold; saw {}",
            metrics.max_memtable_wal_segment_gap
        );

        let layout = engine.get_storage_layout().expect("storage layout");
        assert!(
            layout
                .levels
                .iter()
                .map(|level| level.file_count)
                .sum::<usize>()
                >= 1,
            "storage layout should report the eventually published SST"
        );
    }

    #[test]
    fn should_eventually_publish_sst_given_many_cloud_buffered_writes_when_memtable_never_reaches_size_threshold(
    ) {
        // Arrange
        let opts = opts_for_mode("cloud").with_cloud_write_policy(buffered_cloud_policy());
        let engine = open_large_cloud_engine(&opts);
        let cf = default_cf(&engine);

        for i in 0..(BUFFERED_TEST_GAP - 1) {
            let key = format!("buffered-gap-preflush-key-{i:04}");
            commit_small_write(
                &engine,
                &cf,
                key.as_bytes(),
                b"buffered-gap-value",
                WriteOptions::cloud_async(),
            );
        }

        let pre_flush_metrics = engine.get_runtime_metrics().expect("runtime metrics");
        // Act
        // Assert
        assert_eq!(
            pre_flush_metrics.max_memtable_wal_segment_gap,
            BUFFERED_TEST_GAP - 1,
            "buffered cloud writes should grow the memtable segment gap before the first automatic flush"
        );

        for i in (BUFFERED_TEST_GAP - 1)..16 {
            let key = format!("buffered-gap-key-{i:04}");
            commit_small_write(
                &engine,
                &cf,
                key.as_bytes(),
                b"buffered-gap-value",
                WriteOptions::cloud_async(),
            );
        }

        let metrics = wait_for_metrics(&engine, Duration::from_secs(3), |metrics| {
            metrics.sst_count >= 1 && metrics.manifest_last_persisted_sequence > 0
        });

        assert!(
            metrics.wal_current_segment_id > BUFFERED_TEST_GAP,
            "buffered cloud writes should seal multiple WAL segments in the background; saw segment {}",
            metrics.wal_current_segment_id
        );
        assert!(
            metrics.max_memtable_wal_segment_gap < BUFFERED_TEST_GAP,
            "automatic flush should reset the cloud segment gap after buffered publication; saw {}",
            metrics.max_memtable_wal_segment_gap
        );
    }

    #[test]
    fn should_publish_lightly_written_column_family_given_busy_neighbor_when_cloud_segment_gap_flush_runs(
    ) {
        // Arrange
        let opts = opts_for_mode("cloud").with_cloud_write_policy(buffered_cloud_policy());
        let engine = open_large_cloud_engine(&opts);
        let light_cf = engine
            .create_column_family("light")
            .expect("create light cf");
        let busy_cf = engine.create_column_family("busy").expect("create busy cf");

        for i in 0..(BUFFERED_TEST_GAP - 1) {
            let key = format!("light-gap-key-{i:04}");
            commit_small_write(
                &engine,
                &light_cf,
                key.as_bytes(),
                b"light-gap-value",
                WriteOptions::cloud_async(),
            );
        }

        for i in 0..24 {
            let key = format!("busy-gap-key-{i:04}");
            commit_small_write(
                &engine,
                &busy_cf,
                key.as_bytes(),
                b"busy-gap-value",
                WriteOptions::cloud_async(),
            );
        }

        let deadline = Instant::now() + Duration::from_secs(3);
        let layout = loop {
            let layout = engine.get_storage_layout().expect("storage layout");
            let light_has_sst = layout
                .levels
                .iter()
                .flat_map(|level| level.files.iter())
                .any(|file| file.cf_id == light_cf.id());
            if light_has_sst {
                break layout;
            }

            let metrics = engine.get_runtime_metrics().expect("runtime metrics");
            // Act
            // Assert
            assert!(
                Instant::now() < deadline,
                "timed out waiting for a lightly written CF SST; last metrics: sst_count={} gap={} segment={}",
                metrics.sst_count,
                metrics.max_memtable_wal_segment_gap,
                metrics.wal_current_segment_id
            );
            thread::sleep(Duration::from_millis(25));
        };

        assert!(
            layout
                .levels
                .iter()
                .flat_map(|level| level.files.iter())
                .any(|file| file.cf_id == light_cf.id()),
            "lightly written column family should receive its own SST publication"
        );
    }

    #[test]
    fn should_reset_memtable_wal_gap_given_graceful_checkpoint_reopen() {
        // Arrange
        let opts = opts_for_mode("cloud").with_cloud_write_policy(CloudWritePolicy {
            eventual_flush_segment_gap: CloudWritePolicy::default().eventual_flush_segment_gap,
            wal_seal_min_segment_bytes: usize::MAX,
            wal_seal_max_flush_delay: Duration::from_hours(1),
            wal_seal_max_pending_writes: 1,
        });

        {
            let mut engine = open_large_cloud_engine(&opts);
            let cf = default_cf(&engine);
            for i in 0..16 {
                let key = format!("reopen-gap-key-{i:04}");
                commit_small_write(
                    &engine,
                    &cf,
                    key.as_bytes(),
                    b"reopen-gap-value",
                    WriteOptions::cloud_async(),
                );
            }

            let metrics = engine
                .get_runtime_metrics()
                .expect("runtime metrics before reopen");
            // Act
            // Assert
            assert_eq!(
                metrics.sst_count, 0,
                "pre-restart workload should remain WAL-backed"
            );
            assert!(
                metrics.wal_current_segment_id > 10,
                "pre-restart workload should build historical WAL segment churn; saw {}",
                metrics.wal_current_segment_id
            );

            engine
                .shutdown(Duration::from_secs(5))
                .expect("shutdown before reopen");
        }

        let reopened = open_large_cloud_engine(&opts);
        let reopened_metrics = reopened
            .get_runtime_metrics()
            .expect("runtime metrics after reopen");
        assert_eq!(
            reopened_metrics.max_memtable_wal_segment_gap, 0,
            "reopened non-empty memtables should start counting segment churn from the new runtime"
        );
        assert_eq!(
            reopened_metrics.sst_count, 1,
            "graceful shutdown should checkpoint the WAL-backed memtable exactly once"
        );
    }
}

mod cloud_local_disk_admission {
    use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
    use std::path::Path;
    use std::time::Duration;

    fn random_value(length: usize, random: &mut u32) -> Vec<u8> {
        (0..length)
            .map(|_| {
                *random ^= *random << 13;
                *random ^= *random >> 17;
                *random ^= *random << 5;
                random.to_le_bytes()[0]
            })
            .collect()
    }

    fn wal_bytes(directory: &Path) -> u64 {
        std::fs::read_dir(directory.join("wal"))
            .expect("WAL directory")
            .map(|entry| {
                entry
                    .expect("WAL entry")
                    .metadata()
                    .expect("WAL metadata")
                    .len()
            })
            .sum()
    }

    #[test]
    fn should_keep_ordinary_cloud_commits_flushable_when_local_budget_is_smaller_than_memtable_limit(
    ) {
        for (lengths, spill_pool) in [
            ([96, 96, 96, 96], None),
            ([40, 96, 96, 96], None),
            ([40, 96, 96, 96], Some(1)),
        ] {
            // Arrange
            let directory = tempfile::tempdir().expect("database directory");
            let mut builder =
                OpenOptions::cloud_simulated(directory.path(), "bucket", "bounded-commits")
                    .local_storage_budget(1024 * 1024)
                    .background_compaction(false);
            if let Some(pool) = spill_pool {
                builder = builder.transaction_memory_pool_size(pool);
            }
            let options = builder.build().expect("options");
            let mut engine = Engine::open(options.clone()).expect("open");
            let cf = engine.create_column_family("data").expect("column family");
            let mut random = 0x9e37_79b9_u32;
            let values: Vec<_> = lengths
                .into_iter()
                .map(|length| random_value(length * 1024, &mut random))
                .collect();

            // Act
            for (index, value) in values.iter().enumerate() {
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("transaction");
                tx.put(index.to_be_bytes().to_vec(), value.clone(), None)
                    .expect("put");
                tx.commit(WriteOptions::cloud_strict())
                    .expect("cloud commit");
            }
            let flush = engine.flush_cf(&cf);
            let shutdown = engine.shutdown(Duration::from_secs(10));

            // Assert
            assert!(
                flush.is_ok(),
                "accepted commits must remain flushable: {flush:?}"
            );
            shutdown.expect("shutdown");
            drop(engine);
            let mut reopened = Engine::open(options).expect("reopen");
            let cf = reopened
                .get_column_family("data")
                .expect("recovered column family");
            let tx = reopened
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("reader");
            for (index, value) in values.iter().enumerate() {
                assert_eq!(
                    tx.get(&index.to_be_bytes())
                        .expect("recovered value")
                        .as_deref(),
                    Some(value.as_slice())
                );
            }
            drop(tx);
            reopened
                .shutdown(Duration::from_secs(10))
                .expect("recovered shutdown");
        }
    }

    #[test]
    fn should_reject_atomic_cloud_work_before_wal_append_when_its_flush_cannot_fit() {
        for (value_lengths, spill_pool) in [
            (vec![160 * 1024], None),
            (vec![96 * 1024, 96 * 1024], None),
            (vec![96 * 1024, 96 * 1024], Some(1)),
        ] {
            // Arrange
            let directory = tempfile::tempdir().expect("database directory");
            let mut builder =
                OpenOptions::cloud_simulated(directory.path(), "bucket", "oversized-atomic")
                    .local_storage_budget(1024 * 1024)
                    .background_compaction(false);
            if let Some(pool) = spill_pool {
                builder = builder.transaction_memory_pool_size(pool);
            }
            let mut engine = Engine::open(builder.build().expect("options")).expect("open");
            let cf = engine.create_column_family("data").expect("column family");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            let mut random = 0x9e37_79b9_u32;
            for (index, length) in value_lengths.iter().enumerate() {
                tx.put(
                    index.to_be_bytes().to_vec(),
                    random_value(*length, &mut random),
                    None,
                )
                .expect("put");
            }
            let before_wal_bytes = wal_bytes(directory.path());

            // Act
            let result = tx.commit(WriteOptions::cloud_strict());
            let after_wal_bytes = wal_bytes(directory.path());
            let reader = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("reader");
            let visible = reader
                .get(&0_usize.to_be_bytes())
                .expect("read after rejected commit");
            drop(reader);
            let shutdown = engine.shutdown(Duration::from_secs(10));

            // Assert
            assert!(
                matches!(result, Err(MidgeError::NoSpace(_))),
                "atomic work must be rejected before commit: {result:?}; spill={spill_pool:?}"
            );
            assert_eq!(after_wal_bytes, before_wal_bytes);
            assert!(visible.is_none());
            shutdown.expect("shutdown after rejected work");
        }
    }

    #[test]
    fn should_reject_oversized_cloud_tombstones_before_wal_append() {
        for range in [false, true] {
            // Arrange
            let directory = tempfile::tempdir().expect("database directory");
            let options =
                OpenOptions::cloud_simulated(directory.path(), "bucket", "oversized-delete")
                    .local_storage_budget(1024 * 1024)
                    .build()
                    .expect("options");
            let mut engine = Engine::open(options).expect("open");
            let cf = engine.create_column_family("data").expect("column family");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            if range {
                tx.delete_range(vec![b'a'; 24 * 1024], vec![b'z'; 24 * 1024])
                    .expect("range tombstone");
            } else {
                tx.delete(vec![b'a'; 32 * 1024]).expect("point tombstone");
            }
            let before = wal_bytes(directory.path());

            // Act
            let result = tx.commit(WriteOptions::cloud_strict());
            let after = wal_bytes(directory.path());
            engine.shutdown(Duration::from_secs(10)).expect("shutdown");

            // Assert
            assert!(
                matches!(result, Err(MidgeError::NoSpace(_))),
                "oversized tombstone must be rejected: {result:?}"
            );
            assert_eq!(before, after);
        }
    }
}

mod cloud_many_families {
    //! Shared staging must keep accepted work flushable across small column families.

    use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
    use std::path::Path;
    use std::time::Duration;

    fn file_bytes(path: &Path) -> u64 {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    file_bytes(&path)
                } else {
                    entry.metadata().map_or(0, |metadata| metadata.len())
                }
            })
            .sum()
    }

    fn working_bytes(path: &Path) -> u64 {
        ["wal", "sst", "staging", "hybrid_local"]
            .iter()
            .map(|name| file_bytes(&path.join(name)))
            .sum()
    }

    fn capture_lease_diagnostics() {
        tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::INFO)
            .try_init()
            .expect("install lease and persistence failure diagnostics");
    }

    /// Build `count` values of `len` xorshift bytes each, so the flush path
    /// cannot compress them away and the local budget is exercised for real.
    fn incompressible_values(count: usize, len: usize) -> Vec<Vec<u8>> {
        let mut random = 0x1f37_9945_u32;
        (0..count)
            .map(|_| {
                (0..len)
                    .map(|_| {
                        random ^= random << 13;
                        random ^= random >> 17;
                        random ^= random << 5;
                        random.to_le_bytes()[0]
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn should_flush_many_small_cloud_families_without_exhausting_shared_wal_staging() {
        // Arrange
        capture_lease_diagnostics();
        let directory = tempfile::tempdir().expect("database directory");
        let db_path = directory.path();
        let limit = 256 * 1024;
        let options = OpenOptions::cloud_simulated(db_path, "bucket", "many-small-families")
            .local_storage_budget(limit)
            .background_compaction(false)
            .build()
            .expect("options");
        let mut engine = Engine::open(options.clone()).expect("engine");
        let families: Vec<_> = (0..160)
            .map(|index| {
                engine
                    .create_column_family(&format!("cf-{index}"))
                    .expect("family")
            })
            .collect();
        let values = incompressible_values(families.len(), 2048);

        let wal_before = file_bytes(&db_path.join("wal"));
        let mut oversized = engine
            .begin_tx(families[0].id(), TransactionMode::ReadWrite)
            .expect("oversized transaction");
        oversized
            .put(b"too-large".to_vec(), vec![1; 64 * 1024], None)
            .expect("buffer put");
        assert!(matches!(
            oversized.commit(WriteOptions::cloud_strict()),
            Err(MidgeError::NoSpace(_))
        ));
        assert_eq!(
            file_bytes(&db_path.join("wal")),
            wal_before,
            "unflushable indivisible transactions must fail before WAL growth"
        );

        // Act
        for (index, (family, value)) in families.iter().zip(&values).enumerate() {
            let mut committed = false;
            for _ in 0..3 {
                let mut tx = engine
                    .begin_tx(family.id(), TransactionMode::ReadWrite)
                    .expect("transaction");
                tx.put(b"key".to_vec(), value.clone(), None).expect("put");
                match tx.commit(WriteOptions::cloud_strict()) {
                    Ok(()) => {
                        committed = true;
                        break;
                    }
                    Err(MidgeError::NoSpace(_) | MidgeError::WriteStall(_)) => {
                        for accepted in &families[..index] {
                            engine
                                .flush_cf(accepted)
                                .expect("accepted work must retain flush capacity");
                        }
                    }
                    Err(error) => panic!("cloud commit: {error}"),
                }
            }
            assert!(
                committed,
                "family {index} must make progress after flushing accepted work"
            );
            assert!(
                working_bytes(directory.path()) <= limit,
                "physical working files must stay inside shared budget"
            );
        }
        for family in &families {
            engine.flush_cf(family).expect("flush accepted family");
        }
        engine.shutdown(Duration::from_secs(30)).expect("shutdown");
        drop(engine);
        let mut reopened = Engine::open(options).expect("reopen");

        // Assert
        for (index, value) in values.iter().enumerate() {
            let family = reopened
                .get_column_family(&format!("cf-{index}"))
                .expect("recovered family");
            let tx = reopened
                .begin_tx(family.id(), TransactionMode::ReadOnly)
                .expect("read");
            assert_eq!(
                tx.get(b"key").expect("get").as_deref(),
                Some(value.as_slice())
            );
        }
        assert!(working_bytes(directory.path()) <= limit);
        assert!(file_bytes(&directory.path().join("cloud_store/sst")) > limit);
        reopened
            .shutdown(Duration::from_secs(30))
            .expect("reopened shutdown");
    }
}

mod cloud_small_memory {
    //! Small public memory configurations must still provide bounded cold reads.

    use cntryl_midge::{Engine, MemoryBudget, OpenOptions, TransactionMode, WriteOptions};
    use std::time::Duration;

    #[test]
    fn should_read_cold_cloud_sst_when_default_memory_budget_is_eight_mebibytes() {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let database = directory.path().join("nested-cache-".repeat(12));
        let budget = 8 * 1024 * 1024;
        let options = OpenOptions::cloud_simulated(database, "bucket", "small-memory")
            .memory_budget(MemoryBudget::Bytes(budget))
            .background_compaction(false)
            .build()
            .expect("small cloud options");
        let mut engine = Engine::open(options.clone()).expect("seed engine");
        let cf = engine.create_column_family("data").expect("create CF");
        let mut transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("write transaction");
        transaction
            .put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        transaction
            .commit(WriteOptions::cloud_strict())
            .expect("cloud acknowledgement");
        engine.flush_cf(&cf).expect("publish SST");
        engine.shutdown(Duration::from_secs(30)).expect("shutdown");
        drop(engine);

        // Act
        let mut reopened = Engine::open(options.clone()).expect("cold engine");
        let cf = reopened.get_column_family("data").expect("recovered CF");
        let transaction = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        let value = transaction.get(b"key").expect("bounded cold SST read");
        let metrics = reopened.get_runtime_metrics().expect("read metrics");

        // Assert
        assert_eq!(value.as_deref(), Some(b"value".as_slice()));
        assert!(metrics.remote_range_requests_total > 0);
        assert!(options.block_cache_size() > 0);
        let public_pools = options
            .memtable_size_limit()
            .saturating_mul(2)
            .saturating_add(options.transaction_memory_pool_size())
            .saturating_add(options.block_cache_size());
        assert!(public_pools <= budget);
        drop(transaction);
        reopened
            .shutdown(Duration::from_secs(30))
            .expect("shutdown reader");
    }
}
