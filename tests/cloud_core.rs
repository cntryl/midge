//! Cloud Storage Tests
//!
//! Consolidated from: `cloud_recovery.rs`, `cloud_crash_recovery.rs`, `cloud_eventual_flush.rs`, `cloud_persistence_hardening.rs`, `cloud_remote_sst.rs`, `cloud_remote_sst_compaction_recovery.rs`, `cloud_salvage_local.rs`, `cloud_small_memory.rs`, `cloud_many_families.rs`, `cloud_local_disk_admission.rs`, `cloud_ddl_two_phase_hardening.rs`, `cloud_provider_validation.rs`, `cloudfirst_benchmark_safe.rs`, `engine_cloud.rs`, `engine_gc_cloud.rs`

mod common;

mod cloud_recovery {
    //! Reopen Consistency After Flush And Background Upload Activity
    //!
    //! These tests cover clean-shutdown reopen behavior after flushes, compaction
    //! requests, and short background-upload windows across durable storage modes.
    //! They do not inject real cloud failures, partial uploads, or process crashes.
    //!
    //! **Storage Modes**: Durable modes only (local, cloud)
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::TransactionMode;
    use std::thread;
    use std::time::Duration;

    // ============================================================================
    // TEST GROUP: Cloud Recovery Scenarios
    // ============================================================================

    #[test]
    fn should_preserve_flushed_values_when_reopening_after_short_upload_window() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            let cloud_cache_path = match &opts.storage_mode {
                StorageMode::CloudBacked { local_cache_path } => Some(local_cache_path.clone()),
                _ => None,
            };
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..100 {
                    let key = format!("partial_upload_key_{i:04}");
                    tx.put(
                        key.as_bytes().to_vec(),
                        b"value_before_upload".to_vec(),
                        None,
                    )
                    .expect("put value");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");

                engine.flush_cf(&cf).expect("flush");
                thread::sleep(Duration::from_millis(50));

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..100 {
                    let key = format!("partial_upload_key_{i:04}");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get reopened value"),
                        Some(Bytes::from_static(b"value_before_upload")),
                        "mode: {mode} key: {key}"
                    );
                }
            }

            if let Some(local_cache_path) = cloud_cache_path {
                assert!(
                    !local_cache_path.join("cloud_recovery").exists(),
                    "cloud reopen should not leave recovery staging directories behind"
                );
            }
        });
    }

    #[test]
    fn should_preserve_both_flushed_batches_when_reopening_after_compaction_request() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..50 {
                    let key = format!("manifest_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"v1".to_vec(), None)
                        .expect("put first flushed batch");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 50..100 {
                    let key = format!("manifest_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"v2".to_vec(), None)
                        .expect("put second flushed batch");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                engine.compact_all().ok();

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..100 {
                    let key = format!("manifest_fail_key_{i:04}");
                    let expected = if i < 50 {
                        Bytes::from_static(b"v1")
                    } else {
                        Bytes::from_static(b"v2")
                    };
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get reopened batch value"),
                        Some(expected),
                        "mode: {mode} key: {key}"
                    );
                }
            }
        });
    }

    #[test]
    fn should_preserve_flushed_values_when_reopening_after_background_upload_delay() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..75 {
                    let key = format!("retry_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"retry_value".to_vec(), None)
                        .expect("put retry value");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");

                engine.flush_cf(&cf).expect("flush");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                thread::sleep(Duration::from_millis(200)); // Wait for background retry

                let cf = engine.create_column_family("test").expect("create cf");
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..75 {
                    let key = format!("retry_key_{i:04}");
                    assert_eq!(
                        tx.get(key.as_bytes())
                            .expect("get retry value after reopen"),
                        Some(Bytes::from_static(b"retry_value")),
                        "mode: {mode} key: {key}"
                    );
                }
            }
        });
    }

    #[test]
    fn should_preserve_snapshot_visibility_when_flushing_with_snapshot_open() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write data
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("exposure_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"safe_value".to_vec(), None)
                    .expect("put safe value");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Act
            engine.flush_cf(&cf).expect("flush");

            // Assert
            for i in 0..100 {
                let key = format!("exposure_key_{i:04}");
                assert_eq!(
                    snapshot.get(key.as_bytes()).expect("snapshot get"),
                    Some(Bytes::from_static(b"safe_value")),
                    "snapshot saw unexpected value in mode: {mode} key: {key}"
                );
            }
        });
    }

    #[test]
    fn should_preserve_multiple_flushed_batches_when_reopening_after_short_upload_window() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..50 {
                    let key = format!("resume_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"batch1".to_vec(), None)
                        .expect("put batch1 value");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 50..100 {
                    let key = format!("resume_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"batch2".to_vec(), None)
                        .expect("put batch2 value");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                thread::sleep(Duration::from_millis(50));

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                thread::sleep(Duration::from_millis(300));

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..100 {
                    let key = format!("resume_key_{i:04}");
                    let expected = if i < 50 {
                        Bytes::from_static(b"batch1")
                    } else {
                        Bytes::from_static(b"batch2")
                    };
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get reopened resume value"),
                        Some(expected),
                        "mode: {mode} key: {key}"
                    );
                }
            }
        });
    }
}

mod cloud_remote_sst {
    //! Cloud SST reads must not turn the ephemeral disk into a database replica.

    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::path::Path;
    use std::time::Duration;

    fn sst_bytes(path: &Path) -> u64 {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
            .map(|entry| entry.metadata().expect("SST metadata").len())
            .sum()
    }

    #[test]
    fn should_report_remote_read_costs_only_for_the_engine_performing_cold_reads() {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let other_directory = tempfile::tempdir().expect("independent database directory");
        let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "read-costs")
            .background_compaction(false)
            .build()
            .expect("options");
        let mut engine = Engine::open(options.clone()).expect("seed engine");
        let cf = engine.create_column_family("data").expect("create CF");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("write transaction");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::cloud_strict())
            .expect("cloud commit");
        engine.flush_cf(&cf).expect("flush");
        engine.shutdown(Duration::from_secs(30)).expect("shutdown");
        drop(engine);
        let mut engine = Engine::open(options).expect("cold engine");
        let mut unrelated = Engine::open(
            OpenOptions::cloud_simulated(other_directory.path(), "bucket", "other-read-costs")
                .background_compaction(false)
                .build()
                .expect("independent options"),
        )
        .expect("independent engine");
        let cf = engine.get_column_family("data").expect("recovered CF");
        let before = engine
            .metrics()
            .get_runtime_metrics()
            .expect("before metrics");
        // Act
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        assert_eq!(
            tx.get(b"key").expect("cold read").as_deref(),
            Some(b"value".as_slice())
        );
        let cold = engine
            .metrics()
            .get_runtime_metrics()
            .expect("cold metrics");
        assert_eq!(
            tx.get(b"key").expect("warm read").as_deref(),
            Some(b"value".as_slice())
        );
        let warm = engine
            .metrics()
            .get_runtime_metrics()
            .expect("warm metrics");
        let other = unrelated
            .metrics()
            .get_runtime_metrics()
            .expect("other metrics");
        // Assert
        assert!(cold.remote_range_requests_total > before.remote_range_requests_total);
        assert!(cold.remote_range_bytes_total > before.remote_range_bytes_total);
        assert!(cold.remote_range_latency_ns_total > before.remote_range_latency_ns_total);
        assert!(cold.remote_range_latency_ns_max > 0);
        assert_eq!(cold.remote_range_failures_total, 0);
        assert_eq!(
            warm.remote_range_requests_total,
            cold.remote_range_requests_total
        );
        assert_eq!(warm.remote_range_bytes_total, cold.remote_range_bytes_total);
        assert_eq!(other.remote_range_requests_total, 0);
        assert_eq!(other.remote_range_bytes_total, 0);
        assert_eq!(sst_bytes(&directory.path().join("sst")), 0);
        drop(tx);
        engine
            .shutdown(Duration::from_secs(30))
            .expect("shutdown reader");
        unrelated
            .shutdown(Duration::from_secs(30))
            .expect("shutdown unrelated");
    }

    #[test]
    fn should_operate_cloud_database_when_inventory_exceeds_local_cache() {
        // Arrange
        let dir = tempfile::tempdir().expect("database directory");
        let local_budget = 1024 * 1024;
        let options = OpenOptions::cloud_simulated(dir.path(), "bucket", "remote-sst")
            .local_storage_budget(local_budget)
            .background_compaction(false)
            .build()
            .expect("options");
        let mut engine = Engine::open(options.clone()).expect("open");
        let cf = engine.create_column_family("data").expect("create CF");
        // Distinct values prevent repeated-value compression from hiding the
        // relationship between cloud inventory and the ephemeral disk budget.
        let mut random = 0x9e37_79b9_u32;
        let values: Vec<Vec<u8>> = (0..12)
            .map(|_| {
                (0..96 * 1024)
                    .map(|_| {
                        random ^= random << 13;
                        random ^= random >> 17;
                        random ^= random << 5;
                        random.to_le_bytes()[0]
                    })
                    .collect()
            })
            .collect();

        // Act
        for (index, value) in values.iter().enumerate() {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            tx.put(format!("key-{index}").into_bytes(), value.clone(), None)
                .expect("put");
            tx.commit(WriteOptions::cloud_strict())
                .expect("cloud commit");
            engine.flush_cf(&cf).expect("flush");
            assert_eq!(
                sst_bytes(&dir.path().join("sst")),
                0,
                "published SSTs must be evicted"
            );
        }
        engine.shutdown(Duration::from_secs(30)).expect("shutdown");
        drop(engine);
        assert!(sst_bytes(&dir.path().join("cloud_store/sst")) > local_budget);
        let mut reopened = Engine::open(options).expect("cold reopen");
        assert_eq!(
            sst_bytes(&dir.path().join("sst")),
            0,
            "startup must not hydrate SSTs"
        );
        let cf = reopened.get_column_family("data").expect("recovered CF");
        {
            let tx = reopened
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("read transaction");
            for (index, value) in values.iter().enumerate() {
                assert_eq!(
                    tx.get(format!("key-{index}").as_bytes())
                        .expect("remote read")
                        .as_deref(),
                    Some(value.as_slice())
                );
            }
        }
        reopened.compact_all().expect("remote input compaction");

        // Assert
        assert_eq!(
            sst_bytes(&dir.path().join("sst")),
            0,
            "reads and compaction must leave published SSTs remote"
        );
        assert_eq!(
            sst_bytes(&dir.path().join("hybrid_local/sst")),
            0,
            "no duplicate full-file cache"
        );
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        for (index, value) in values.iter().enumerate() {
            assert_eq!(
                tx.get(format!("key-{index}").as_bytes())
                    .expect("compacted read")
                    .as_deref(),
                Some(value.as_slice())
            );
        }
        drop(tx);
        reopened
            .verify_storage(Duration::from_secs(30))
            .expect("verify remote-only SSTs");
        reopened
            .shutdown(Duration::from_secs(30))
            .expect("shutdown reopened engine");
    }

    #[test]
    fn should_report_corrupt_remote_data_block_when_read_after_metadata_only_startup() {
        // Arrange
        let dir = tempfile::tempdir().expect("database directory");
        let options = OpenOptions::cloud_simulated(dir.path(), "bucket", "corrupt-block")
            .background_compaction(false)
            .build()
            .expect("options");
        let mut engine = Engine::open(options.clone()).expect("open");
        let cf = engine.create_column_family("data").expect("CF");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("transaction");
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::cloud_strict()).expect("commit");
        engine.flush_cf(&cf).expect("flush");
        engine.shutdown(Duration::from_secs(30)).expect("shutdown");
        drop(engine);
        let path = std::fs::read_dir(dir.path().join("cloud_store/sst"))
            .expect("cloud inventory")
            .flatten()
            .find(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
            .expect("published SST")
            .path();
        let mut bytes = std::fs::read(&path).expect("fixture SST");
        bytes[4] ^= 0x80;
        std::fs::write(path, bytes).expect("inject data-block corruption");

        // Act
        let mut reopened = Engine::open(options).expect("startup only validates metadata");
        let cf = reopened.get_column_family("data").expect("CF");
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        let result = tx.get(b"key");

        // Assert
        assert!(
            result.is_err(),
            "corruption must not be returned as a missing key"
        );
        assert_eq!(sst_bytes(&dir.path().join("sst")), 0);
        drop(tx);
        reopened
            .shutdown(Duration::from_secs(30))
            .expect("shutdown");
    }
}

mod cloud_salvage_local {
    //! Explicit salvage may read a verified local SST when cloud authority is lost.

    use cntryl_midge::{
        Engine, EngineHealth, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
    };
    use std::time::Duration;

    #[test]
    fn should_read_verified_local_sst_during_salvage_when_remote_object_is_missing_or_invalid() {
        for remote_failure in ["missing", "truncated", "same-size corruption"] {
            for local_copy in ["primary", "secondary", "secondary with corrupt primary"] {
                // Arrange
                let dir = tempfile::tempdir().expect("database directory");
                let options = OpenOptions::cloud_simulated(dir.path(), "bucket", "salvage-local")
                    .background_compaction(false)
                    .build()
                    .expect("options");
                let mut engine = Engine::open(options).expect("open");
                let cf = engine.create_column_family("data").expect("column family");
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("transaction");
                tx.put(b"key".to_vec(), b"verified local value".to_vec(), None)
                    .expect("put");
                tx.commit(WriteOptions::cloud_strict())
                    .expect("cloud commit");
                engine.flush_cf(&cf).expect("flush");
                engine.shutdown(Duration::from_secs(30)).expect("shutdown");
                drop(engine);
                let remote = std::fs::read_dir(dir.path().join("cloud_store/sst"))
                    .expect("remote files")
                    .map(|entry| entry.expect("remote entry").path())
                    .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
                    .expect("remote SST");
                let canonical_local = dir.path().join("sst").join(remote.file_name().unwrap());
                let local = if local_copy == "primary" {
                    canonical_local.clone()
                } else {
                    dir.path()
                        .join("hybrid_local/sst")
                        .join(remote.file_name().unwrap())
                };
                std::fs::create_dir_all(local.parent().unwrap()).expect("local cache directory");
                std::fs::copy(&remote, &local).expect("preserve valid local copy");
                if local_copy == "secondary with corrupt primary" {
                    std::fs::write(&canonical_local, b"corrupt primary cache")
                        .expect("corrupt primary");
                }
                match remote_failure {
                    "missing" => std::fs::remove_file(&remote).expect("remove remote"),
                    "truncated" => {
                        std::fs::write(&remote, b"invalid remote object")
                            .expect("invalidate remote");
                    }
                    _ => {
                        let mut bytes = std::fs::read(&remote).expect("read remote SST");
                        bytes[0] ^= 1;
                        std::fs::write(&remote, bytes)
                            .expect("corrupt remote without changing size");
                    }
                }
                let salvage_options =
                    OpenOptions::cloud_simulated(dir.path(), "bucket", "salvage-local")
                        .background_compaction(false)
                        .recovery_policy(RecoveryPolicy::Salvage)
                        .build()
                        .expect("salvage options");
                // Act
                let mut reopened = Engine::open(salvage_options).expect("salvage reopen");
                let cf = reopened.get_column_family("data").expect("recovered CF");
                let tx = reopened
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("read transaction");
                let actual = tx.get(b"key").expect("verified salvage read");
                // Assert
                assert_eq!(actual.as_deref(), Some(b"verified local value".as_slice()));
                assert!(
                    canonical_local.exists(),
                    "salvage must preserve the only verified SST copy"
                );
                assert_eq!(
                    reopened.get_runtime_metrics().expect("metrics").health,
                    EngineHealth::SalvageMode
                );
                drop(tx);
                reopened
                    .verify_storage(Duration::from_secs(30))
                    .expect("verify salvage local copy");
                reopened
                    .shutdown(Duration::from_secs(30))
                    .expect("salvage shutdown");
            }
        }
    }
}

mod cloud_provider_validation {
    #![cfg(feature = "cloud-oci")]

    use cntryl_midge::{
        CloudProviderConfig, CloudStorageLocation, MemoryBudget, MidgeError, OpenOptions,
        S3CredentialSource,
    };

    #[test]
    fn should_reject_unsafe_oci_endpoint_given_open_options_when_endpoint_is_overridden() {
        // Arrange
        let invalid_endpoints = [
            "ftp://objectstorage.example.test",
            "https://user:secret@objectstorage.example.test",
            "https://objectstorage.example.test?credential=secret",
            "https://objectstorage.example.test#fragment",
        ];

        // Act
        let errors = invalid_endpoints.map(|endpoint| {
            let cache = tempfile::tempdir().expect("temporary OCI cache");
            let provider = CloudProviderConfig::oci_object_storage(
                "namespace",
                "bucket",
                "us-phoenix-1",
                S3CredentialSource::access_key("access", "secret"),
            )
            .with_endpoint(endpoint)
            .expect("OCI supports endpoint overrides");
            OpenOptions::cloud(
                cache.path(),
                CloudStorageLocation::new(provider, "validation/"),
            )
            .memory_budget(MemoryBudget::Bytes(8 * 1024 * 1024))
            .build()
            .expect_err("unsafe OCI endpoint must fail before engine startup")
        });

        // Assert
        for error in errors {
            assert!(matches!(
                error,
                MidgeError::InvalidArgument(message) if message.contains("endpoint")
            ));
        }
    }
}

mod cloudfirst_benchmark_safe {
    //! `CloudAsync` durability policy verification tests
    //!
    //! Ensures `CloudAsync` background mode never blocks on single writes,
    //! while `CloudStrict` mode provides explicit cloud durability guarantees.

    use crate::common::{open_with_mode, opts_for_mode};
    use cntryl_midge::{TransactionMode, WriteOptions};

    #[test]
    fn should_batch_writes_when_using_cloud_mode() {
        // Arrange
        let opts = opts_for_mode("cloud");
        let engine = open_with_mode(&opts, "cloud");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Act: Write multiple records with CloudAsync background durability.
        // CloudAsync batches uploads in the background, so commits should not block
        for i in 0..100 {
            let mut tx = engine
                .begin_tx(cf_id, TransactionMode::ReadWrite)
                .expect("begin");
            let key = format!("key_{i:04}");
            let value = format!("value_{i:04}");
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .unwrap();
            tx.commit(WriteOptions::cloud_async()).unwrap();
        }

        // Assert: Verify all data is readable (correctness check)
        for i in 0..100 {
            let tx = engine
                .begin_tx(cf_id, TransactionMode::ReadOnly)
                .expect("begin");
            let key = format!("key_{i:04}");
            let value = tx.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "key_{i:04} should exist");
        }
    }

    #[test]
    fn should_support_cloud_strict_for_explicit_durability() {
        // Arrange
        let opts = opts_for_mode("cloud");
        let engine = open_with_mode(&opts, "cloud");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Act: Write with CloudStrict policy (explicit cloud durability)
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(b"strict_key".to_vec(), b"strict_value".to_vec(), None)
            .unwrap();

        // CloudStrict forces immediate WAL seal + rotate + upload, blocking until complete
        tx.commit(WriteOptions::cloud_strict()).unwrap();

        // Assert: Data should be readable immediately
        let tx = engine
            .begin_tx(cf_id, TransactionMode::ReadOnly)
            .expect("begin");
        let value = tx.get(b"strict_key").unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref(), b"strict_value");
    }

    #[test]
    fn should_flush_cloud_segments_on_shutdown() {
        // Arrange
        let opts = opts_for_mode("cloud");
        let mut engine = open_with_mode(&opts, "cloud");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Act: Write data with background CloudAsync
        for i in 0..50 {
            let mut tx = engine
                .begin_tx(cf_id, TransactionMode::ReadWrite)
                .expect("begin");
            let key = format!("shutdown_key_{i:04}");
            let value = format!("shutdown_value_{i:04}");
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .unwrap();
            tx.commit(WriteOptions::cloud_async()).unwrap();
        }

        engine
            .shutdown(std::time::Duration::from_secs(5))
            .expect("bounded cloud shutdown");

        // Assert: Shutdown must have actually flushed pending CloudAsync uploads to
        // the simulated remote store, not merely returned without panicking. Reopen
        // against the same simulated bucket/prefix and confirm every write survived.
        let reopened = open_with_mode(&opts, "cloud");
        let cf = reopened.get_column_family("test").expect("get cf");
        let cf_id = cf.id();
        for i in 0..50 {
            let tx = reopened
                .begin_tx(cf_id, TransactionMode::ReadOnly)
                .expect("begin");
            let key = format!("shutdown_key_{i:04}");
            let value = format!("shutdown_value_{i:04}");
            assert_eq!(
                tx.get(key.as_bytes()).unwrap(),
                Some(bytes::Bytes::from(value.into_bytes())),
                "key {key} must be recovered after a shutdown that flushed cloud segments"
            );
        }
    }
}

mod engine_cloud {
    //! Cloud-mode integration tests with real assertions.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::Query;

    #[test]
    fn should_create_column_family_given_cloud_mode_when_requested() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");

        // Act
        let cf = engine.create_column_family("test").expect("create cf");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read transaction");
        assert_eq!(tx.get(b"missing_key").expect("read missing key"), None);
    }

    #[test]
    fn should_read_written_value_given_cloud_mode_when_written_and_read() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write transaction");
        tx.put(b"cloud_key".to_vec(), b"cloud_value".to_vec(), None)
            .expect("put cloud value");
        tx.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit cloud write");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read transaction");
        assert_eq!(
            tx.get(b"cloud_key").expect("read cloud key"),
            Some(Bytes::from_static(b"cloud_value"))
        );
    }

    #[test]
    fn should_read_committed_transaction_value_given_cloud_mode_when_committed() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin transaction");
        txn.put(b"tx_key1".to_vec(), b"tx_value1".to_vec(), None)
            .expect("put transaction value");
        txn.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit cloud transaction");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read transaction");
        assert_eq!(
            tx.get(b"tx_key1").expect("read transaction key"),
            Some(Bytes::from_static(b"tx_value1"))
        );
    }

    #[test]
    fn should_scan_inserted_keys_given_cloud_mode_when_range_scanned() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..20 {
            let key = format!("cloud_scan_{i:02}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin scan seed transaction");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put scan seed value");
            tx.commit(cntryl_midge::WriteOptions::cloud_async())
                .expect("commit scan seed transaction");
        }

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin scan transaction");
        let results = tx
            .scan(&Query::new())
            .expect("scan cloud keys")
            .try_collect()
            .expect("collect cloud keys");

        // Assert
        assert_eq!(results.len(), 20);
    }

    #[test]
    fn should_preserve_snapshot_value_given_cloud_mode_when_overwritten_after_snapshot() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        tx.put(b"snap_key".to_vec(), b"snap_v1".to_vec(), None)
            .expect("put initial snapshot value");
        tx.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit seed transaction");

        let snapshot = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin snapshot transaction");

        // Act
        let mut tx2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin overwrite transaction");
        tx2.put(b"snap_key".to_vec(), b"snap_v2".to_vec(), None)
            .expect("put overwrite value");
        tx2.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit overwrite transaction");

        // Assert
        assert_eq!(
            snapshot.get(b"snap_key").expect("read snapshot key"),
            Some(Bytes::from_static(b"snap_v1"))
        );
        let current = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin current read transaction");
        assert_eq!(
            current.get(b"snap_key").expect("read current key"),
            Some(Bytes::from_static(b"snap_v2"))
        );
    }

    #[test]
    fn should_hide_deleted_key_given_cloud_mode_when_deleted() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        tx.put(b"del_key".to_vec(), b"to_delete".to_vec(), None)
            .expect("put delete seed value");
        tx.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit seed transaction");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin delete transaction");
        tx.delete(b"del_key".to_vec()).expect("delete cloud key");
        tx.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit delete transaction");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        assert_eq!(tx.get(b"del_key").expect("read deleted key"), None);
    }

    #[test]
    fn should_apply_last_write_wins_given_cloud_mode_when_multiple_writes() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin first write transaction");
        tx.put(b"lww_key".to_vec(), b"v1".to_vec(), None)
            .expect("put first value");
        tx.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit first write");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin second write transaction");
        tx.put(b"lww_key".to_vec(), b"v2".to_vec(), None)
            .expect("put second value");
        tx.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit second write");

        // Assert
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read transaction");
        assert_eq!(
            tx.get(b"lww_key").expect("read lww key"),
            Some(Bytes::from_static(b"v2"))
        );
    }
}
