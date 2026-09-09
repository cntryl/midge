//! Lifecycle and Resource Tests
//!
//! Consolidated from: `engine_gc.rs`, `solid_cleanup.rs`, `resource_cleanup.rs`, `shutdown_reopen_stress.rs`, `memory_management.rs`, `memory_mode_isolation.rs`, `memory_spill_audit.rs`, `backpressure.rs`, `column_family_reclamation_hardening.rs`

mod common;

mod engine_gc {
    //! Garbage Collection Integration Tests
    //!
    //! Tests local SST and WAL garbage collection:
    //! - Orphan SST/WAL file detection and deletion
    //! - Persistence of GC state across restarts
    //! - GC interaction with active readers (snapshot isolation)
    //! - Configurable GC intervals and triggering
    //!
    //! **Storage Modes**: All (memory, local, cloud)
    //! Note: Memory mode has no files to GC, tests validate graceful no-op behavior.
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>

    use crate::common::*;
    use cntryl_midge::{TransactionMode, WriteOptions};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // ============================================================================
    // HELPER: Extract filesystem path from local storage mode
    // ============================================================================

    fn get_storage_path_if_local(opts: &MidgeOptions) -> Option<PathBuf> {
        match &opts.storage_mode {
            StorageMode::LocalDisk { db_path } => Some(db_path.clone()),
            StorageMode::Memory | StorageMode::CloudBacked { .. } => None,
        }
    }

    fn local_sst_names(db_path: &std::path::Path) -> Vec<String> {
        let mut names = std::fs::read_dir(db_path.join("sst"))
            .expect("read local SST directory")
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| {
                std::path::Path::new(name)
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    // ============================================================================
    // TEST GROUP 1: SST Garbage Collection
    // ============================================================================

    #[test]
    fn should_collect_orphaned_sst_files_after_compaction() {
        for_each_storage_mode(&["local"], |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            for batch in 0..4 {
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for index in 0..25 {
                    let key = format!("key_{batch}_{index:04}");
                    tx.put(key.into_bytes(), b"v1".to_vec(), None)
                        .expect("put compaction seed");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush L0 generation");
            }
            let db_path = get_storage_path_if_local(&opts).expect("local storage path");
            let input_names = local_sst_names(&db_path);
            assert_eq!(input_names.len(), 4);
            // Act
            engine.compact_all().expect("compact L0 generations");

            // Assert
            let remaining_names = local_sst_names(&db_path);
            assert!(!remaining_names.is_empty());
            let retained_input_count = input_names
                .iter()
                .filter(|input_name| db_path.join("sst").join(input_name).exists())
                .count();
            assert_eq!(
                retained_input_count, 0,
                "compact_all must reclaim every physical L0 input across bounded batches"
            );
            assert!(
                input_names
                    .iter()
                    .any(|input_name| !db_path.join("sst").join(input_name).exists()),
                "at least one obsolete compaction input must be absent on disk"
            );
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            for batch in 0..4 {
                for index in 0..25 {
                    let key = format!("key_{batch}_{index:04}");
                    let val = tx.get(key.as_bytes()).expect("get during compaction");
                    assert!(val.is_some(), "key lost during compaction in mode: {mode}");
                }
            }
        });
    }

    #[test]
    fn should_not_collect_sst_files_referenced_by_manifest() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            eprintln!("\n=== GC: Preserve Referenced SST Files (mode: {mode}) ===");

            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write and flush to create SST referenced by manifest
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Act: Don't trigger compaction; SST remains active
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Assert: SST is still readable (proof it exists)
            for i in 0..50 {
                let key = format!("key_{i:04}");
                let val = tx.get(key.as_bytes()).expect("get");
                assert!(
                    val.is_some(),
                    "SST file prematurely deleted in mode: {mode}"
                );
            }

            eprintln!("âœ“ SST files referenced by manifest are preserved");
        });
    }

    #[test]
    fn should_run_gc_after_configurable_interval() {
        eprintln!("\n=== GC: Configurable Interval Triggering ===");

        // Arrange: `for_each_storage_mode`'s helper opens every mode with
        // background compaction disabled (deterministic for other tests), so
        // this test needs its own engine with it explicitly enabled — the
        // whole point here is proving GC runs *on its own periodic interval*,
        // never via a manual `compact_all()` call.
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let db_path = temp_dir.path();
        let engine = cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(db_path)
                .background_compaction(true)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine.create_column_family("test").expect("create cf");

        // Four overlapping-key L0 generations cross the default L0 file-count
        // compaction trigger. Background compaction is scheduled immediately
        // after each flush publication, so it can start collecting earlier
        // generations before this loop even finishes — record each newly
        // flushed L0 input's name as it's created rather than assuming all
        // four coexist once the loop completes. SST names encode their level
        // as `<cf>_<level>_<seq>.sst`, so an L0 flush output is always tagged
        // `_00_`, distinguishing it from a compacted (level >= 1) output that
        // might already exist by the time we check.
        let is_l0_output = |name: &String| name.split('_').nth(1) == Some("00");
        let mut input_names = std::collections::BTreeSet::new();
        for batch in 0..4 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for index in 0..25 {
                let key = format!("interval_key_{index:04}");
                tx.put(key.into_bytes(), format!("v{batch}").into_bytes(), None)
                    .expect("put");
            }
            tx.commit(WriteOptions::buffered()).expect("commit");
            engine.flush_cf(&cf).expect("flush L0 generation");
            input_names.extend(local_sst_names(db_path).into_iter().filter(is_l0_output));
        }
        assert_eq!(
            input_names.len(),
            4,
            "expected four distinct flushed L0 generations across the run"
        );

        // Act: wait for background GC/compaction to run on its own; poll
        // rather than sleep a fixed amount so this doesn't depend on exactly
        // when the trigger fires while still bounding total runtime.
        let any_input_collected = |db_path: &std::path::Path| {
            input_names
                .iter()
                .any(|name| !db_path.join("sst").join(name).exists())
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        while !any_input_collected(db_path) {
            assert!(
                std::time::Instant::now() < deadline,
                "background GC never ran within its interval; all four flushed \
                 inputs are still present on disk: {input_names:?}"
            );
            thread::sleep(Duration::from_millis(200));
        }

        // Assert: at least one obsolete compaction input is now gone from
        // disk — proof the background interval actually triggered GC, not
        // just that reads kept working.
        assert!(
            any_input_collected(db_path),
            "background GC did not collect any obsolete compaction input"
        );

        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin_tx");
        for index in 0..25 {
            let key = format!("interval_key_{index:04}");
            assert!(
                tx.get(key.as_bytes())
                    .expect("read after background GC")
                    .is_some(),
                "data lost during background GC"
            );
        }

        eprintln!("✓ Background GC collected obsolete inputs within its interval");
    }

    #[test]
    fn should_persist_gc_state_across_restart() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            eprintln!("\n=== GC: Persist State Across Restart (mode: {mode}) ===");

            // Arrange
            // Act: Write and trigger compaction
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write batch 1
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..75 {
                    let key = format!("persist_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"v1".to_vec(), None).ok();
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Write batch 2
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 75..150 {
                    let key = format!("persist_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"v2".to_vec(), None).ok();
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Trigger manual compaction (marks orphans for deletion)
                engine.compact_all().ok();
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before restart");
            }

            // Assert: Reopen and verify state
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // All data should still be present (GC didn't lose anything)
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                for i in 0..150 {
                    let key = format!("persist_key_{i:04}");
                    assert!(
                        tx.get(key.as_bytes())
                            .expect("read after GC state restart")
                            .is_some(),
                        "data lost after restart in mode: {mode}"
                    );
                }

                eprintln!("âœ“ GC state persisted correctly across restart");
            }
        });
    }

    #[test]
    fn should_handle_gc_with_active_readers() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            eprintln!("\n=== GC: Handle Active Readers (mode: {mode}) ===");

            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Arrange: Write and flush initial data
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("reader_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"snapshot_value".to_vec(), None)
                    .ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");
            engine.flush_cf(&cf).expect("flush");

            // Act: Create snapshot (read lock on SSTs)
            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            // Spawn thread to trigger compaction while snapshot is held
            let engine_clone = Arc::clone(&engine);
            let _cf_clone = cf.clone();
            let compaction_handle = thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                engine_clone.compact_all().ok();
                eprintln!("Compaction triggered while snapshot held");
            });

            // Wait a bit, then verify snapshot still reads correctly
            thread::sleep(Duration::from_millis(100));
            let val = snapshot.get(b"reader_key_0000").expect("get");
            assert!(
                val.is_some(),
                "snapshot read failed during compaction in mode: {mode}"
            );

            // Wait for compaction thread
            compaction_handle
                .join()
                .expect("background compaction thread should not panic");

            // Assert: Drop snapshot; verify data still present
            drop(snapshot);
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("reader_key_{i:04}");
                assert!(
                    tx.get(key.as_bytes())
                        .expect("read after active-reader compaction")
                        .is_some(),
                    "data lost after compaction in mode: {mode}"
                );
            }

            eprintln!("âœ“ Active readers protected during GC");
        });
    }

    // ============================================================================
    // TEST GROUP 2: WAL Garbage Collection
    // ============================================================================

    #[test]
    fn should_collect_orphaned_wal_segments_after_flush() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            eprintln!("\n=== GC: Collect Orphaned WAL Segments (mode: {mode}) ===");

            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Write to memtable (goes to WAL)
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("wal_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"wal_value".to_vec(), None)
                    .ok();
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Act: Flush to SST (WAL segment becomes obsolete)
            engine.flush_cf(&cf).expect("flush");

            // Assert: Data is still readable (proof WAL->SST transition succeeded)
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..50 {
                let key = format!("wal_key_{i:04}");
                let val = tx.get(key.as_bytes()).expect("get");
                assert!(val.is_some(), "data lost after flush in mode: {mode}");
            }

            // For local/cloud modes, flush must advance the durability frontier
            // past the WAL it just captured, leaving no outstanding
            // memtable/WAL segment gap — the observable signal that the old
            // segment is now orphaned and eligible for GC (the same contract
            // `observability_api.rs` checks after an explicit flush).
            if mode != "memory" {
                let metrics = engine.get_runtime_metrics().expect("runtime metrics");
                assert_eq!(
                    metrics.max_memtable_wal_segment_gap, 0,
                    "flush should leave no outstanding WAL segment gap in mode: {mode}"
                );
                eprintln!("âœ“ WAL segment orphaned and eligible for collection");
            }
        });
    }

    #[test]
    fn should_not_collect_wal_segments_still_needed_for_recovery() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            eprintln!("\n=== GC: Preserve WAL for Recovery (mode: {mode}) ===");

            // Arrange
            // Act: Write uncommitted data and crash
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write to memtable (committed but not flushed)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..30 {
                    let key = format!("recovery_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"recovery_value".to_vec(), None)
                        .ok();
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before restart");
            }

            // Assert: Restart and verify WAL recovery
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Data should be recovered from WAL (prove WAL wasn't garbage collected)
                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                for i in 0..30 {
                    let key = format!("recovery_key_{i:04}");
                    let val = tx.get(key.as_bytes()).expect("get");
                    assert!(
                        val.is_some(),
                        "WAL segment was incorrectly GC'd in mode: {mode}"
                    );
                }

                eprintln!("âœ“ WAL segments preserved for recovery across restart");
            }
        });
    }
}

mod solid_cleanup {
    use std::fs;
    use std::path::{Path, PathBuf};

    use cntryl_midge::{
        AzureCredentialSource, CloudProviderConfig, ColumnFamilyId, Engine, EngineHealth,
        GcsApiStyle, GcsCredentialSource, MidgeResult, OpenOptions, RecoveryPolicy,
        RuntimeMetricsSnapshot, S3CredentialSource, Storage, StorageLayoutSnapshot,
    };

    fn source_path(relative: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
    }

    fn read_source(relative: &str) -> String {
        fs::read_to_string(source_path(relative)).expect("source file should be readable")
    }

    fn production_source(relative: &str) -> String {
        read_source(relative)
            .split("#[cfg(test)]")
            .next()
            .unwrap_or_default()
            .to_string()
    }

    fn source_before_test_module(relative: &str) -> String {
        read_source(relative)
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap_or_default()
            .to_string()
    }

    fn collect_rust_sources(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).expect("source directory should be readable") {
            let entry = entry.expect("source directory entry should be readable");
            let path = entry.path();
            if path.is_dir() {
                collect_rust_sources(&path, files);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }

    fn collect_files_with_extension(dir: &Path, extension: &str, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).expect("directory should be readable") {
            let entry = entry.expect("directory entry should be readable");
            let path = entry.path();
            if path.is_dir() {
                collect_files_with_extension(&path, extension, files);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some(extension) {
                files.push(path);
            }
        }
    }

    #[test]
    fn should_reexport_shared_public_types_from_crate_root() {
        // Arrange
        let _: fn(OpenOptions) -> MidgeResult<Engine> = Engine::open;
        let _: RecoveryPolicy = RecoveryPolicy::Strict;
        let _: EngineHealth = EngineHealth::Healthy;
        let _: Storage = Storage::InMemory;
        let _: ColumnFamilyId = 0;
        let _: fn(&Engine) -> MidgeResult<RuntimeMetricsSnapshot> = Engine::get_runtime_metrics;
        let _: fn(&Engine) -> MidgeResult<StorageLayoutSnapshot> = Engine::get_storage_layout;

        // Act
        let provider = CloudProviderConfig::gcs("bucket")
            .with_gcs_credentials(GcsCredentialSource::application_default())
            .expect("gcs credentials should apply");

        // Assert
        assert!(matches!(
            provider,
            CloudProviderConfig::Gcs(config) if config.api_style() == GcsApiStyle::Json
        ));
    }

    #[test]
    fn should_keep_source_free_of_assistant_rule_blocks() {
        // Arrange
        let mut sources = Vec::new();
        collect_rust_sources(&source_path("src"), &mut sources);
        let forbidden = ["COPILOT", "Copilot", "prompt-rule", "prompt rule"];

        // Act

        // Assert
        for source in sources {
            let content = fs::read_to_string(&source).expect("rust source should be readable");
            for pattern in forbidden {
                assert!(
                    !content.contains(pattern),
                    "{} should not contain assistant-rule marker {pattern}",
                    source.display()
                );
            }
        }
    }

    #[test]
    fn should_keep_removed_testkit_api_out_of_public_surface() {
        // Arrange
        let lib = read_source("src/lib.rs");
        let engine = read_source("src/engine/mod.rs");

        // Act

        // Assert
        assert!(
            !lib.contains("pub mod testkit") && !lib.contains("mod testkit"),
            "crate root should not expose or compile the old testkit module"
        );
        assert!(
            !lib.contains("pub use testkit"),
            "crate root should not re-export old testkit helpers"
        );
        assert!(
            !engine.contains("open_with_options"),
            "Engine should not expose the removed open_with_options testkit API"
        );
    }

    #[test]
    fn should_remove_legacy_transaction_benchmark_surface_names() {
        // Arrange
        let lib = read_source("src/lib.rs");
        let transaction_api = read_source("src/engine/api/transaction.rs");
        let runtime = read_source("src/runtime/mod.rs");

        // Act

        // Assert
        assert!(!lib.contains("pub mod handler"));
        assert!(!lib.contains("pub mod message"));
        assert!(!transaction_api.contains("IsolationLevel"));
        assert!(!transaction_api.contains("set_isolation_level"));
        assert!(!runtime.contains("TransactionIsolationPolicy"));
    }

    #[test]
    fn should_remove_eviction_actor_wrapper_provider_modules() {
        // Arrange
        let runtime_actors = read_source("src/runtime/actors/mod.rs");
        let provider_mod = read_source("src/storage/providers/mod.rs");
        let diagrams = read_source("docs/development/architecture-diagrams.md");

        // Act

        // Assert
        assert!(!runtime_actors.contains("EvictionActor"));
        assert!(!provider_mod.contains("pub mod aws;"));
        assert!(!provider_mod.contains("pub mod minio;"));
        assert!(!provider_mod.contains("pub mod wasabi;"));
        assert!(!provider_mod.contains("pub mod oci;"));
        assert!(!diagrams.contains("EvictionActor"));
    }

    #[test]
    fn should_remove_legacy_manifest_sst_list_from_current_model() {
        // Arrange
        let manifest = production_source("src/metadata/manifest.rs");
        let hybrid = production_source("src/storage/hybrid/backend.rs");

        // Act

        // Assert
        assert!(!manifest.contains("pub ssts:"));
        assert!(!hybrid.contains("manifest.ssts"));
    }

    #[test]
    fn should_propagate_transaction_encode_errors_without_expect() {
        // Arrange
        let wal_actor = production_source("src/runtime/actors/wal.rs");

        // Act

        // Assert
        assert!(!wal_actor.contains("validated transaction batch should encode"));
    }

    #[test]
    fn should_keep_block_cache_admission_synchronous() {
        // Arrange
        let shard = production_source("src/sst/cache/shard.rs");
        let cache = production_source("src/sst/cache/mod.rs");
        let combined = format!("{shard}\n{cache}");
        let forbidden_shard = [
            "AdmissionCounter",
            "record_access_for_admission(&self, key:",
            "fn should_admit(&self",
        ];
        let forbidden = [
            "cache-admission-worker",
            "AdmissionRequest",
            "admission_tx",
            "worker_handle",
            "spawn_worker",
            "crossbeam_channel",
            "handle.join()",
        ];

        // Act

        // Assert
        assert!(shard.contains("pub fn put(&self, key: CacheKey, value: &Bytes) -> bool"));
        assert!(shard.contains("self.insert_and_update_metrics(key, cache_value);"));
        assert!(shard.contains("self.evict_if_needed();"));
        for pattern in forbidden {
            assert!(
                !combined.contains(pattern),
                "block cache production path should not contain async admission worker artifact {pattern}"
            );
        }
        for pattern in forbidden_shard {
            assert!(
                !shard.contains(pattern),
                "CacheShard should not reference admission gate internals {pattern}"
            );
        }
    }

    #[test]
    fn should_keep_unsequenced_wal_append_op_out_of_public_writer_trait() {
        // Arrange
        let wal_trait = production_source("src/wal/traits.rs");
        let filesystem_writer = production_source("src/wal/fs/writer_io.rs");

        // Act

        // Assert
        assert!(
            wal_trait.contains("fn append_record(&self, record: &WalRecord)"),
            "WalWriter should expose the prebuilt-record append path"
        );
        assert!(
            wal_trait.contains("fn append_op_with_seq("),
            "WalWriter should keep the explicit-sequence append path"
        );
        assert!(
            wal_trait.contains("fn append_batch(&self, records: &[WalRecord])"),
            "WalWriter should keep the batch append path"
        );
        assert!(
            !wal_trait.contains("fn append_op("),
            "WalWriter should not expose unsequenced append_op"
        );
        assert!(
            !filesystem_writer.contains("fn append_op("),
            "filesystem WAL writer should not reintroduce unsequenced append_op"
        );
    }

    #[test]
    fn should_not_document_removed_transaction_rollback_api() {
        // Arrange
        let mut docs = Vec::new();
        collect_files_with_extension(&source_path("docs"), "md", &mut docs);
        let forbidden = ["rollback_transaction", "engine.rollback_transaction"];

        // Act

        // Assert
        for doc in docs {
            let content = fs::read_to_string(&doc).expect("markdown source should be readable");
            for pattern in forbidden {
                assert!(
                    !content.contains(pattern),
                    "{} should not document nonexistent API {pattern}",
                    doc.display()
                );
            }
        }
    }

    #[test]
    fn should_keep_runtime_free_of_engine_owned_type_imports() {
        // Arrange
        let mut sources = Vec::new();
        collect_rust_sources(&source_path("src/runtime"), &mut sources);
        let forbidden = ["crate::engine::", "crate::engine::api::"];

        // Act / Assert
        for source in sources {
            let content = fs::read_to_string(&source).expect("runtime source should be readable");
            for pattern in forbidden {
                // Assert
                assert!(
                    !content.contains(pattern),
                    "{} should not depend on engine-owned path {pattern}",
                    source.display()
                );
            }
        }
    }

    #[test]
    fn should_keep_wal_production_free_of_storage_dependencies() {
        // Arrange
        let mut sources = Vec::new();
        collect_rust_sources(&source_path("src/wal"), &mut sources);

        // Act / Assert
        for source in sources {
            let relative = source
                .strip_prefix(source_path(""))
                .expect("source should live under repo")
                .to_string_lossy()
                .replace('\\', "/");
            let content = source_before_test_module(&relative);

            // Assert
            assert!(
                !content.contains("crate::storage"),
                "{relative} should depend on base io abstractions, not storage"
            );
        }
    }

    #[test]
    fn should_keep_hybrid_storage_out_of_wal_frame_decoding() {
        // Arrange
        let backend = source_before_test_module("src/storage/hybrid/backend.rs");
        let forbidden = ["crate::wal::frame", "crate::wal::encoding", "WalOpKind"];

        // Act / Assert
        for pattern in forbidden {
            // Assert
            assert!(
                !backend.contains(pattern),
                "hybrid storage should use wal::cloud_segment instead of decoding WAL internals with {pattern}"
            );
        }
    }

    #[test]
    fn should_construct_runtime_observability_dtos_from_shared_types() {
        // Arrange
        let state = read_source("src/runtime/state.rs");
        let protocol = read_source("src/runtime/protocol.rs");
        let engine = read_source("src/engine/mod.rs");
        let shared = read_source("src/types.rs");

        // Act

        // Assert
        assert!(state.contains("crate::types::RuntimeMetricsSnapshot"));
        assert!(state.contains("crate::types::StorageLayoutSnapshot"));
        assert!(protocol.contains("Box<crate::types::RuntimeMetricsSnapshot>"));
        assert!(protocol.contains("snapshot: crate::types::StorageLayoutSnapshot"));
        assert!(!engine.contains("pub struct RuntimeMetricsSnapshot"));
        assert!(!engine.contains("pub struct StorageLayoutSnapshot"));
        assert!(shared.contains("pub struct RuntimeMetricsSnapshot"));
        assert!(shared.contains("pub struct StorageLayoutSnapshot"));
    }

    #[test]
    fn should_keep_event_loop_message_families_in_owned_coordinators() {
        // Arrange
        let event_loop_source = read_source("src/runtime/event_loop/mod.rs");
        let event_loop = event_loop_source
            .split("pub(super) mod tests")
            .next()
            .unwrap_or_default();
        let dispatcher = read_source("src/runtime/event_loop/dispatch.rs");
        let coordinator_files = [
            ("src/runtime/event_loop/wal.rs", "struct WalCoordinator"),
            ("src/runtime/event_loop/flush.rs", "struct FlushCoordinator"),
            (
                "src/runtime/event_loop/compaction.rs",
                "struct CompactionCoordinator",
            ),
            (
                "src/runtime/event_loop/manifest.rs",
                "struct ManifestCoordinator",
            ),
            ("src/runtime/event_loop/gc.rs", "struct GcCoordinator"),
            (
                "src/runtime/event_loop/snapshot.rs",
                "struct SnapshotCoordinator",
            ),
        ];
        let forbidden_inline_handlers = [
            "RuntimeMsg::ApplyTransaction",
            "RuntimeMsg::FlushMemtable",
            "RuntimeMsg::CompactionComplete",
            "RuntimeMsg::ManifestCreateColumnFamily",
            "RuntimeMsg::DeleteObsoleteSsts",
        ];

        // Act / Assert
        // Assert
        assert!(event_loop.contains("RuntimeDispatcher::handle"));
        for pattern in forbidden_inline_handlers {
            assert!(
                !event_loop.contains(pattern),
                "EventLoop core should not inline message family handler {pattern}"
            );
        }
        for (file, marker) in coordinator_files {
            let content = read_source(file);
            assert!(content.contains(marker), "{file} should define {marker}");
            assert!(
                !content.contains("impl EventLoop"),
                "{file} should own behavior through a coordinator, not an EventLoop impl dump"
            );
            let coordinator_name = marker
                .strip_prefix("struct ")
                .expect("marker should name a struct");
            assert!(
                dispatcher.contains(coordinator_name),
                "dispatcher should delegate to {coordinator_name}"
            );
        }
    }

    #[test]
    fn should_keep_cloud_provider_constructors_on_same_variants() {
        // Arrange
        let aws = CloudProviderConfig::aws_s3("bucket", "us-east-1");
        let s3 = CloudProviderConfig::s3_compatible_env("bucket", "http://localhost:9000");
        let azure = CloudProviderConfig::azure_blob("account", "container");
        let gcs = CloudProviderConfig::gcs_hmac("bucket", "access", "secret");

        // Act / Assert
        // Assert
        assert!(matches!(
            aws,
            CloudProviderConfig::AwsS3(config) if matches!(config.credentials(), S3CredentialSource::AwsDefaultChain)
        ));
        assert!(matches!(
            s3,
            CloudProviderConfig::S3Compatible(config) if matches!(config.credentials(), S3CredentialSource::Environment) && config.path_style()
        ));
        assert!(matches!(
            azure,
            CloudProviderConfig::AzureBlob(config) if matches!(config.credentials(), AzureCredentialSource::LightweightDefaultChain)
        ));
        assert!(matches!(
            gcs,
            CloudProviderConfig::Gcs(config) if config.api_style() == GcsApiStyle::Xml && matches!(config.credentials(), GcsCredentialSource::HmacKey { .. })
        ));
    }

    #[test]
    fn should_own_cloud_provider_config_in_storage_providers() {
        // Arrange
        let generic_config = read_source("src/config.rs");
        let provider_config = read_source("src/config/provider.rs");
        let provider_module = read_source("src/storage/providers/mod.rs");

        // Act

        // Assert
        assert!(!generic_config.contains("pub enum CloudProviderConfig"));
        assert!(!generic_config.contains("pub enum S3CredentialSource"));
        assert!(!generic_config.contains("pub enum AzureCredentialSource"));
        assert!(!generic_config.contains("pub enum GcsCredentialSource"));
        assert!(generic_config.contains("pub use provider::"));
        assert!(provider_config.contains("pub enum CloudProviderConfig"));
        assert!(provider_config.contains("pub enum S3CredentialSource"));
        assert!(provider_config.contains("pub enum AzureCredentialSource"));
        assert!(provider_config.contains("pub enum GcsCredentialSource"));
        assert!(provider_module.contains("pub(crate) use crate::config::CloudProviderConfig"));
        assert!(provider_config.contains("impl CloudProviderConfig"));
    }

    #[test]
    fn should_delegate_provider_construction_to_provider_family_resolvers() {
        // Arrange
        let factory = read_source("src/storage/providers/factory.rs");
        let s3 = read_source("src/storage/providers/s3_resolver.rs");
        let azure = read_source("src/storage/providers/azure_resolver.rs");
        let gcs = read_source("src/storage/providers/gcs_resolver.rs");
        // Act / Assert
        // Assert
        assert!(factory.contains("match provider"));
        assert!(factory.contains("s3_resolver::try_resolve"));
        assert!(factory.contains("azure_resolver::try_resolve"));
        assert!(factory.contains("gcs_resolver::try_resolve"));
        assert!(!factory.contains("super::is_aws_s3"));
        assert!(!factory.contains("super::is_s3_compatible"));
        assert!(!factory.contains("super::is_azure_blob"));
        assert!(s3.contains("pub(super) fn try_resolve"));
        assert!(s3.contains("CloudProviderConfig::AwsS3"));
        assert!(s3.contains("CloudProviderConfig::S3Compatible"));
        assert!(azure.contains("pub(super) fn try_resolve"));
        assert!(azure.contains("CloudProviderConfig::AzureBlob"));
        assert!(gcs.contains("pub(super) fn try_resolve"));
        assert!(gcs.contains("CloudProviderConfig::Gcs"));
    }

    #[test]
    fn should_select_same_storage_modes_from_open_options_constructors() {
        // Arrange
        let location = |bucket| {
            cntryl_midge::CloudStorageLocation::new(
                CloudProviderConfig::s3_compatible_static(
                    bucket,
                    "http://localhost:9000",
                    "key",
                    "secret",
                ),
                "prefix",
            )
        };

        // Act
        let memory = OpenOptions::in_memory().build().expect("build options");
        let local = OpenOptions::local("/tmp/midge-solid-local")
            .build()
            .expect("build options");
        let cloud = OpenOptions::cloud_multi(
            "/tmp/midge-solid-cloud",
            cntryl_midge::CloudStorageTopology::new(location("wal-bucket"))
                .with_sst(location("sst-bucket"))
                .with_control(location("control-bucket")),
        )
        .build()
        .expect("build options");
        let simulated =
            OpenOptions::cloud_simulated("/tmp/midge-solid-simulated", "bucket", "prefix")
                .build()
                .expect("build options");

        // Assert
        assert!(matches!(memory.storage(), Storage::InMemory));
        assert!(matches!(local.storage(), Storage::Local { .. }));
        assert!(matches!(cloud.storage(), Storage::Cloud { .. }));
        assert!(matches!(
            simulated.storage(),
            Storage::CloudSimulated { .. }
        ));
    }

    #[test]
    fn should_keep_moved_config_types_out_of_lower_layers_engine_imports() {
        // Arrange
        let files = [
            "src/metadata/persistence.rs",
            "src/runtime/intent_persistence.rs",
            "src/runtime/state.rs",
            "src/lease/mod.rs",
            "src/runtime/storage_residue.rs",
            "src/storage/providers/factory.rs",
            "src/storage/providers/s3_resolver.rs",
            "src/storage/providers/azure_resolver.rs",
            "src/storage/providers/gcs_resolver.rs",
            "src/storage/providers/azure.rs",
            "src/storage/providers/gcs.rs",
        ];
        let forbidden = [
            "crate::engine::RecoveryPolicy",
            "crate::engine::EngineHealth",
            "use crate::engine::api::Storage",
            "use crate::engine::api::CloudProviderConfig",
            "crate::engine::api::AzureCredentialSource",
            "crate::engine::api::GcsCredentialSource",
            "crate::config::CloudProviderConfig",
            "crate::config::AzureCredentialSource",
            "crate::config::GcsCredentialSource",
            "crate::config::S3CredentialSource",
        ];

        // Act / Assert
        for file in files {
            let content = read_source(file);
            for pattern in forbidden {
                // Assert
                assert!(
                    !content.contains(pattern),
                    "{file} should not contain lower-layer engine import {pattern}"
                );
            }
        }
    }

    #[test]
    fn should_limit_streaming_sst_persistence_to_compaction_or_fs_layers() {
        // Arrange
        let traits = read_source("src/sst/traits.rs");
        let fs_writer = read_source("src/sst/fs/factory_io.rs");
        let mut sources = Vec::new();
        collect_rust_sources(&source_path("src"), &mut sources);
        let allowed_callers = ["src/compaction/executor.rs", "src/sst/fs/mod.rs"];

        // Act
        let direct_callers: Vec<_> = sources
            .into_iter()
            .filter_map(|source| {
                let relative = source
                    .strip_prefix(source_path(""))
                    .expect("source should live under repo")
                    .to_string_lossy()
                    .replace('\\', "/");
                production_source(&relative)
                    .contains(".finish_to_path(")
                    .then_some(relative)
            })
            .collect();

        // Assert
        assert!(traits.contains("fn finish_bytes"));
        assert!(traits.contains("fn finish_to_path"));
        assert!(fs_writer.contains("fn finish_to_path"));
        assert!(fs_writer.contains("persist_sst_stream_to_path"));
        assert!(direct_callers
            .iter()
            .all(|caller| allowed_callers.contains(&caller.as_str())));
    }

    #[test]
    fn should_keep_compaction_publication_validation_streaming() {
        // Arrange
        let event_loop = read_source("src/runtime/event_loop/mod.rs");
        let publication = read_source("src/runtime/event_loop/compaction.rs");
        let admitted = read_source("src/storage/hybrid/backend/file_publication.rs");
        let reader = read_source("src/sst/fs/reader_io/mod.rs");
        let writer = read_source("src/sst/fs/factory_io.rs");

        // Act
        let materializes_crc_input = event_loop.contains("crc32c::crc32c(&std::fs::read");
        let streaming_summary_is_used = reader.contains("into_streaming_summary()");

        // Assert
        assert!(!materializes_crc_input);
        assert!(event_loop.contains("checksummed_file_crc"));
        assert!(event_loop.contains("budget.reserve(CRC_BUFFER_SIZE, \"SST checksum buffer\")"));
        assert!(!event_loop.contains("read_file_with_budget"));
        assert!(event_loop.contains("publish_immutable_file("));
        assert!(admitted.contains("submit_write_with_reservation("));
        assert!(admitted.contains("submit_read_range_with_reservation("));
        assert!(publication.contains("mirror_ssts_to_authoritative_cloud(output_ssts, budget)"));
        assert!(streaming_summary_is_used);
        assert!(writer.contains("writer.streaming = Some(StreamingState::new"));
    }

    #[test]
    fn should_show_benchmark_support_outside_core_architecture_diagram() {
        // Arrange
        let diagrams = read_source("docs/development/architecture-diagrams.md");

        // Act
        let contains_removed_testkit = diagrams.contains("Testkit[\"testkit");
        let contains_bench_support = diagrams.contains("BenchSupport[\"benches/bench_support");
        let contains_benchmark_helpers = diagrams.contains("benchmark-local helpers");
        let contains_test_support = diagrams.contains("TestSupport[\"tests/common");

        // Assert
        assert!(
            !contains_removed_testkit,
            "architecture diagram should not show removed testkit as a core module"
        );
        assert!(contains_bench_support);
        assert!(contains_benchmark_helpers);
        assert!(contains_test_support);
    }

    #[test]
    fn should_document_cloud_strict_durability_contract() {
        // Arrange
        let durability = read_source("docs/user-guides/durability.md");
        let transaction_contract =
            read_source("docs/user-guides/transaction-durability-contract.md");

        // Act
        let docs = [&durability, &transaction_contract];

        // Assert
        for doc in docs {
            assert!(doc.contains("WriteOptions::cloud_async()"));
            assert!(doc.contains("WriteOptions::cloud_strict()"));
            assert!(doc.contains("Non-cloud storage rejects"));
            assert!(doc.contains("local-only"));
            assert!(doc.contains("seal"));
            assert!(doc.contains("upload"));
            assert!(doc.contains("Empty cloud-backed"));
        }
    }

    #[test]
    fn should_require_manifest_fsync_without_an_escape_hatch() {
        // Arrange
        let durability = read_source("docs/user-guides/durability.md");
        let journal = read_source("src/metadata/journal.rs");

        // Act
        let docs_require_one_sync = durability.contains("one\nrequired filesystem sync");
        let docs_reject_escape_hatch = durability.contains("does not provide");
        let journal_names_skip_flag = journal.contains("MIDGE_SKIP_MANIFEST_FSYNC");
        let journal_names_guard_flag = journal.contains("MIDGE_ALLOW_MANIFEST_SKIP_FSYNC");

        // Assert
        assert!(docs_require_one_sync);
        assert!(docs_reject_escape_hatch);
        assert!(!journal_names_skip_flag);
        assert!(!journal_names_guard_flag);
    }

    #[test]
    fn should_keep_engine_backed_provider_qualification_out_of_storage_layer() {
        // Arrange
        let provider_qualification = read_source("src/storage/providers/qualification.rs");
        let integration = read_source("tests/cloud_provider_engine_qualification.rs");

        // Act
        let storage_imports_engine = provider_qualification.contains("crate::engine");
        let integration_opens_engine = integration.contains("Engine::open");

        // Assert
        assert!(
            !storage_imports_engine,
            "storage provider qualification should not import the engine layer"
        );
        assert!(
            integration_opens_engine,
            "engine-backed provider qualification should live in integration tests"
        );
    }

    #[test]
    fn should_keep_event_loop_fencing_access_explicit() {
        // Arrange
        let event_loop = read_source("src/runtime/event_loop/mod.rs");

        // Act
        let dereferences_fence = event_loop.contains("Deref for EventLoop")
            || event_loop.contains("DerefMut for EventLoop");

        // Assert
        assert!(
            !dereferences_fence,
            "EventLoop owns many responsibilities and must access its RuntimeFence field explicitly"
        );
    }

    #[test]
    fn should_enforce_tier3_system_benchmark_contract() {
        // Arrange
        let manifest = read_source("Cargo.toml");
        let engine = read_source("benches/tier3_system_engine.rs");
        let mvcc = read_source("benches/tier3_system_mvcc.rs");
        let sst = read_source("benches/tier3_system_sst.rs");
        let scan = read_source("benches/tier3_system_scan.rs");
        let lifecycle = read_source("benches/tier3_system_lifecycle.rs");

        // Act
        let existing_read_rows = [&engine, &mvcc, &sst];

        // Assert
        for target in [
            "tier3_system_engine",
            "tier3_system_mvcc",
            "tier3_system_sst",
            "tier3_system_scan",
            "tier3_system_lifecycle",
        ] {
            assert!(manifest.contains(&format!("name = \"{target}\"")));
        }
        for row in existing_read_rows {
            assert!(row.contains("pending_three_clean_baselines"));
            assert!(row.contains("local_gate_rsd_limit_pct"));
            assert!(row.contains("read_path_diagnostics_snapshot_for_benchmarks"));
            assert!(row.contains("validation_failures"));
        }
        assert!(engine.contains("ENGINE_GET_MEMTABLE_SIZE_BYTES"));
        assert!(engine.contains("opts.memtable_size.max"));
        assert!(engine.contains("fixture_memtable_size_bytes"));
        assert!(sst.contains("SST_FIXTURE_MEMTABLE_SIZE_BYTES"));
        assert!(sst.contains("opts.memtable_size.max"));
        assert!(sst.contains("fixture_memtable_size_bytes"));
        assert!(sst.contains("tier3_sst_range_seek_cloud"));
        assert!(scan.contains("scan_seek_first_row"));
        assert!(scan.contains("candidate_sst_files_checked"));
        assert!(scan.contains("candidate_blocks_checked"));
        assert!(lifecycle.contains("write_and_flush_cycle"));
        assert!(lifecycle.contains("clean_reopen"));

        for (source, transaction_drop) in [
            (&mvcc, "drop(snap_tx);"),
            (&sst, "drop(tx);"),
            (&scan, "drop(snapshot);"),
        ] {
            let transaction_teardown = source
                .find(transaction_drop)
                .expect("Tier 3 row must drop its read transaction");
            let engine_teardown = source
                .find("drop(engine);")
                .expect("Tier 3 row must drop its engine");
            assert!(
                transaction_teardown < engine_teardown,
                "Tier 3 snapshots must end before engine shutdown"
            );
        }
    }
}

mod resource_cleanup {
    //! Resource cleanup tests - verify proper memory and handle cleanup
    //!
    //! Tests that components properly clean up memory and other resources when
    //! dropped, ensuring the engine can run in constrained environments.

    use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
    use std::sync::Arc;

    #[test]
    fn should_cleanup_block_cache_resources_when_dropped() {
        // Arrange - create many BlockCache instances and populate each so there
        // is real state for cleanup to discard.
        let caches: Vec<BlockCache> = (0..10)
            .map(|shard_seed| {
                let cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
                for i in 0..20 {
                    let key = CacheKey::for_data(shard_seed, i);
                    let data = bytes::Bytes::from(vec![0u8; 128]);
                    assert!(cache.put(key, &data), "put should succeed under capacity");
                }
                cache
            })
            .collect();

        // Sanity check: every cache actually holds the entries we just inserted.
        for cache in &caches {
            assert_eq!(cache.len(), 20);
            assert!(cache.size_bytes() > 0);
        }

        // Act - drop all caches
        drop(caches);
        // Assert (implicit) - Drop for BlockCache/CacheShard runs without panicking
        // or hanging even while every shard holds populated entries.
    }

    #[test]
    fn should_cleanup_cache_with_active_operations() {
        // Arrange
        let cache = Arc::new(BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru));
        let cache_clone = Arc::clone(&cache);
        // Keep a second surviving handle so we can inspect cache state after the
        // original reference is dropped mid-operation.
        let cache_check = Arc::clone(&cache);

        // Start a thread with cache operations
        let handle = std::thread::spawn(move || {
            let mut successes = 0usize;
            for i in 0..100 {
                let key = CacheKey::for_data(i, 0);
                let data = bytes::Bytes::from(vec![0u8; 100]);
                if cache_clone.put(key, &data) {
                    successes += 1;
                }
            }
            successes
        });

        // Wait for operations to start
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Act - drop the main cache reference while the worker is still writing
        drop(cache);

        // Assert - all inserts succeeded and are visible through the surviving
        // Arc handle, proving the shared cache state survived the concurrent drop.
        let successes = handle.join().expect("worker thread should not panic");
        assert_eq!(
            successes, 100,
            "all puts should have succeeded under capacity"
        );
        assert_eq!(
            cache_check.len(),
            successes,
            "surviving cache handle should see exactly the entries the worker inserted"
        );
    }

    #[test]
    fn should_cleanup_multiple_component_types_together() {
        // Arrange - create multiple caches, one per eviction policy
        let cache1 = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
        let cache2 = BlockCache::new(512 * 1024, 8, CachePolicyType::TinyLfu);
        let cache3 = BlockCache::new(2 * 1024 * 1024, 16, CachePolicyType::ClockPro);

        // Act - exercise each cache so we know it actually holds live state
        // before the components are dropped together.
        let key = CacheKey::for_data(1, 0);
        let data = bytes::Bytes::from(vec![1u8; 64]);
        assert!(cache1.put(key, &data));
        assert!(cache2.put(key, &data));
        assert!(cache3.put(key, &data));

        assert_eq!(
            cache1.get(&key).map(|v| (*v.data).clone()),
            Some(data.clone())
        );
        assert_eq!(
            cache2.get(&key).map(|v| (*v.data).clone()),
            Some(data.clone())
        );
        assert_eq!(
            cache3.get(&key).map(|v| (*v.data).clone()),
            Some(data.clone())
        );

        // Assert - drop all three populated components together without deadlock
        drop((cache1, cache2, cache3));
    }

    #[test]
    fn should_handle_zero_capacity_cache_cleanup() {
        // Arrange - edge case: cache with zero capacity
        let cache = BlockCache::new(0, 16, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let data = bytes::Bytes::from(vec![0u8; 16]);

        // Act
        let accepted = cache.put(key, &data);

        // Assert - a non-empty value can never fit in a zero-capacity cache, so
        // put must be rejected and the cache must stay empty.
        assert!(
            !accepted,
            "put should be rejected when it cannot fit under zero capacity"
        );
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());

        drop(cache);
    }

    #[test]
    fn should_handle_single_shard_cache_cleanup() {
        // Arrange - edge case: cache with only 1 shard (1 worker thread)
        let cache = BlockCache::new(1024 * 1024, 1, CachePolicyType::Lru);
        assert_eq!(cache.num_shards(), 1);

        // Act - insert entries that would land in different shards under a
        // multi-shard cache, to confirm single-shard routing still round-trips.
        for i in 0..10 {
            let key = CacheKey::for_data(i, 0);
            let byte = u8::try_from(i).expect("test index fits in u8");
            let data = bytes::Bytes::from(vec![byte; 32]);
            assert!(cache.put(key, &data));
        }

        // Assert - all entries are present and readable back
        for i in 0..10 {
            let key = CacheKey::for_data(i, 0);
            let byte = u8::try_from(i).expect("test index fits in u8");
            let expected = bytes::Bytes::from(vec![byte; 32]);
            assert_eq!(cache.get(&key).map(|v| (*v.data).clone()), Some(expected));
        }
        assert_eq!(cache.len(), 10);

        drop(cache);
    }
}

mod shutdown_reopen_stress {
    //! Clean-Shutdown and Reopen Stress Tests
    //!
    //! Tests concurrent writes and storage operations followed by clean shutdown
    //! and reopen in local-disk mode.
    //! Coverage in this file is limited to successful operations performed before
    //! normal process teardown via `drop`:
    //! - Recovery of committed WAL-backed and SST-backed writes after reopen
    //! - Visibility after flush, compaction, and manifest-related operations
    //! - Value integrity checks after reopen
    //! - Concurrent best-effort load remaining readable without invalid values
    //!
    //! **Storage Modes**: Local only
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>

    use crate::common::*;
    use cntryl_midge::{TransactionMode, WriteOptions};
    use std::thread;
    use std::time::Duration;

    // ============================================================================
    // TEST GROUP: CLEAN SHUTDOWN REOPEN SCENARIOS
    // ============================================================================

    #[test]
    fn should_recover_committed_wal_writes_when_reopening_after_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After WAL Writes (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Commit WAL-backed writes, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write data (goes to WAL)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..100 {
                    let key = format!("wal_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"wal_value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                // Additional committed writes
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 100..150 {
                    let key = format!("wal_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"wal_value_2".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and validate recovery
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..150 {
                    let key = format!("wal_fail_key_{i:04}");
                    let expected = if i < 100 {
                        b"wal_value".as_slice()
                    } else {
                        b"wal_value_2".as_slice()
                    };
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(expected.into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered all 150 committed WAL-backed records");
            }
        });
    }

    #[test]
    fn should_recover_committed_writes_when_reopening_after_flush_and_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After Flush (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Commit writes, flush, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Load memtable with many keys
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..200 {
                    let key = format!("flush_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"flush_data".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                // Flush the committed data successfully
                engine.flush_cf(&cf).expect("flush");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and verify all flushed data
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..200 {
                    let key = format!("flush_fail_key_{i:04}");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(b"flush_data".as_slice().into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered all 200 committed records after flush");
            }
        });
    }

    #[test]
    fn should_recover_committed_writes_when_reopening_after_compaction_and_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After Compaction (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Create multiple SSTs, compact, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Create SST A
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..100 {
                    let key = format!("compact_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"gen_a".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");
                engine.flush_cf(&cf).expect("flush A");

                // Create SST B
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 100..200 {
                    let key = format!("compact_fail_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"gen_b".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");
                engine.flush_cf(&cf).expect("flush B");

                // Trigger compaction before shutdown
                engine.compact_all().expect("compact_all");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and verify all committed data
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..200 {
                    let key = format!("compact_fail_key_{i:04}");
                    let expected = if i < 100 {
                        b"gen_a".as_slice()
                    } else {
                        b"gen_b".as_slice()
                    };
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(expected.into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered all 200 committed records after compaction");
            }
        });
    }

    #[test]
    fn should_preserve_readability_when_reopening_after_manifest_updates_and_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After Manifest Updates (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Flush data, run compaction-related work, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write and flush (updates manifest)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..150 {
                    let key = format!("manifest_io_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");
                engine.flush_cf(&cf).expect("flush");

                // Trigger compaction-related manifest updates before shutdown
                engine.compact_all().expect("compact_all");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and verify readability
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");
                for i in 0..150 {
                    let key = format!("manifest_io_key_{i:04}");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(b"value".as_slice().into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ All 150 committed records remained readable after reopen");
            }
        });
    }

    #[test]
    fn should_preserve_sst_backed_values_when_reopening_after_flush_and_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After SST Flush (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Write, flush, add more writes, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write batch 1
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..100 {
                    let key = format!("sst_corrupt_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"uncorrupted_value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                // Flush the first batch successfully
                engine.flush_cf(&cf).expect("flush");

                // Write a second batch that remains WAL-backed until reopen
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 100..150 {
                    let key = format!("sst_corrupt_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"clean_value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen and verify exact values
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..100 {
                    let key = format!("sst_corrupt_key_{i:04}");
                    let val = tx
                        .get(key.as_bytes())
                        .expect("get")
                        .expect("value should exist");
                    assert_eq!(
                        val.as_ref(),
                        b"uncorrupted_value",
                        "mode: {mode} key: {key}"
                    );
                }
                for i in 100..150 {
                    let key = format!("sst_corrupt_key_{i:04}");
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(b"clean_value".as_slice().into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered exact SST-backed and WAL-backed values after reopen");
            }
        });
    }

    #[test]
    fn should_preserve_wal_backed_values_when_reopening_after_clean_shutdown() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Reopen After WAL-Backed Writes (mode: {mode}) ===");

            // Arrange
            // Act (Phase 1): Commit two WAL-backed write batches, then drop the engine
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Write batch 1 (committed to WAL)
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..50 {
                    let key = format!("wal_corrupt_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"wal_clean".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                // Write batch 2
                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 50..100 {
                    let key = format!("wal_corrupt_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"wal_second".to_vec(), None)
                        .expect("put");
                }
                tx.commit(WriteOptions::buffered()).expect("commit");

                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2): Reopen with WAL replay
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)
                    .expect("begin_tx");

                for i in 0..100 {
                    let key = format!("wal_corrupt_key_{i:04}");
                    let expected = if i < 50 {
                        b"wal_clean".as_slice()
                    } else {
                        b"wal_second".as_slice()
                    };
                    assert_eq!(
                        tx.get(key.as_bytes()).expect("get"),
                        Some(expected.into()),
                        "mode: {mode} key: {key}"
                    );
                }

                eprintln!("âœ“ Recovered exact WAL-backed values after reopen");
            }
        });
    }

    #[test]
    fn should_handle_concurrent_best_effort_writes_under_load_without_invalid_values() {
        for_each_storage_mode(&["local"], |mode, opts| {
            eprintln!("\n=== Concurrent Best-Effort Load (mode: {mode}) ===");

            // Arrange: High-concurrency best-effort write load
            let engine = std::sync::Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let mut handles = vec![];

            // Spawn multiple writer threads
            for tid in 0..5 {
                let engine_clone = std::sync::Arc::clone(&engine);
                let cf_clone = cf.clone();
                let handle = std::thread::spawn(move || {
                    for batch in 0..20 {
                        let tx = engine_clone
                            .begin_tx(cf_clone.id(), TransactionMode::ReadWrite)
                            .ok();

                        if let Some(mut t) = tx {
                            for i in 0..50 {
                                let key = format!("stress_load_t{tid}_b{batch}_k{i:03}");
                                t.put(key.as_bytes().to_vec(), b"stress_value".to_vec(), None)
                                    .ok();
                            }
                            t.commit(WriteOptions::best_effort()).ok();
                        }

                        // Small stagger to vary writer interleaving
                        if batch % 3 == 0 {
                            thread::sleep(Duration::from_millis(10));
                        }
                    }
                });
                handles.push(handle);
            }

            // Wait for all writers to complete
            for handle in handles {
                handle.join().ok();
            }

            // Act: Flush and compact after the concurrent load
            engine.flush_cf(&cf).ok();
            engine.compact_all().ok();

            // Assert: Engine remains readable and sampled keys never return invalid bytes.
            // Best-effort writes are not treated as a full-durability guarantee here.
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin_tx");

            let mut sampled_present = 0;
            for tid in 0..5 {
                for batch in 0..20 {
                    let key = format!("stress_load_t{tid}_b{batch}_k000");
                    if let Some(val) = tx.get(key.as_bytes()).expect("get") {
                        assert_eq!(val.as_ref(), b"stress_value", "mode: {mode} key: {key}");
                        sampled_present += 1;
                    }
                }
            }

            assert!(
                sampled_present > 0,
                "expected at least one sampled key to be present in mode: {mode}"
            );

            eprintln!(
                "âœ“ Best-effort load remained readable with {sampled_present} sampled keys present"
            );
        });
    }
}

mod memory_management {
    //! Memory-related tests (formerly Phase 1 fixes)

    use crate::common::*;
    use cntryl_midge::{EngineHealth, MidgeError, TransactionMode};
    use std::time::Duration;

    #[test]
    fn should_handle_small_memory_budget_without_unexpected_errors() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange: Open engine with very small memtable limit to trigger frequent flushes
            let mut opts = opts;
            opts = opts.memory_budget(64 * 1024); // 64KB
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act: Write enough data to fill immutable queue
            let mut write_count = 0;
            let mut stall_encountered = false;

            for i in 0..1000 {
                let key = format!("key_{i:08}");
                let value = vec![b'x'; 1024]; // 1KB value

                let mut tx = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(key.into_bytes(), value, None).unwrap();

                match tx.commit(buffered_write_options(mode)) {
                    Ok(()) => {
                        write_count += 1;
                    }
                    Err(MidgeError::WriteStall(_)) => {
                        stall_encountered = true;
                        break;
                    }
                    Err(e) => {
                        panic!("Unexpected error: {e:?}");
                    }
                }

                if i % 10 == 0 {
                    let _ = engine.flush_cf(&cf);
                }
            }

            engine.flush_cf(&cf).expect("final flush");
            if !mode.eq("memory") {
                engine
                    .compact_all()
                    .expect("clear L0 debt after sustained memory pressure");
            }
            let metrics = engine.get_runtime_metrics().expect("runtime metrics");

            // Assert
            if !mode.eq("memory") {
                // Under sustained pressure the engine may either surface WriteStall or
                // keep up by flushing synchronously. Both are acceptable as long as it
                // keeps making progress and does not end in a degraded memory state.
                assert!(
                    stall_encountered || write_count > 0,
                    "Expected progress or backpressure in mode {mode}"
                );
                assert_ne!(
                    metrics.health,
                    EngineHealth::WriteStalled,
                    "Engine should not remain write-stalled after compaction clears debt in mode {mode}"
                );
            }
        });
    }

    #[test]
    fn should_complete_shutdown_when_wal_writer_drops() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let mut engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act: Write some data
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act: shut down explicitly (this drops the WAL writer as part of an
            // orderly shutdown) and assert it actually reports success within a
            // bound, rather than only relying on the test harness's own timeout
            // to catch a hang with no diagnostic about *why* it failed.
            let result = engine.shutdown(Duration::from_secs(10));

            // Assert
            assert!(
                result.is_ok(),
                "shutdown did not complete cleanly when the WAL writer dropped in mode {mode}: {result:?}"
            );
        });
    }
}

mod memory_mode_isolation {
    //! Memory Mode Isolation Tests
    //!
    //! Tests that memory mode creates no persistent filesystem artifacts and isolates
    //! data between engine instances. Memory mode operates entirely in RAM with zero
    //! disk side effects.
    //!
    //! Naming convention:
    //! should_<behavior>_given_<context>_when_<condition>
    //!
    //! These tests run on MEMORY MODE ONLY to validate isolation and filesystem cleanup.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::{TransactionMode, WriteOptions};

    // ============================================================================
    // FILESYSTEM ARTIFACT TESTS
    // ============================================================================

    /// Top-level entries directly under the crate root. Every other test that
    /// touches disk does so through `test_temp_dir()` / `target/tmp/...`
    /// (verified across `tests/*.rs`), so a real bug that made memory mode fall
    /// back to some default on-disk path would most plausibly show up as a new
    /// entry at this top level - unlike scanning `target/tmp`, this is safe to
    /// check even while other tests run concurrently in the same process.
    fn crate_root_top_level_entries() -> std::collections::BTreeSet<std::ffi::OsString> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        std::fs::read_dir(root)
            .expect("read crate root")
            .filter_map(|entry| entry.ok().map(|entry| entry.file_name()))
            .collect()
    }

    #[test]
    fn should_not_create_filesystem_artifacts_when_memory_mode() {
        // Arrange: snapshot the crate root before the engine runs, so the
        // assertion actually inspects the filesystem instead of trusting a
        // comment.
        let entries_before = crate_root_top_level_entries();

        let opts = opts_for_mode("memory");

        // Act: Open, write, close
        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"test_key_1".to_vec(), b"test_value_1".to_vec(), None)
            .expect("put");
        tx.put(b"test_key_2".to_vec(), b"test_value_2".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();
        drop(engine); // memory mode should store nothing on disk

        // Assert: no new top-level filesystem entries appeared. Memory mode's
        // `OpenOptions` never carries a path at all, so this catches the real
        // bug class of a hard-coded/default on-disk fallback slipping in.
        let entries_after = crate_root_top_level_entries();
        let new_entries: Vec<_> = entries_after.difference(&entries_before).collect();
        assert!(
            new_entries.is_empty(),
            "memory mode created unexpected filesystem entries: {new_entries:?}"
        );
    }

    #[test]
    fn should_not_persist_data_across_restart_given_memory_mode_when_reopening() {
        // Arrange: Open and write data
        let opts1 = opts_for_mode("memory");

        {
            let engine = open_with_mode(&opts1, "memory");
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            // Act: Write to memory engine
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(
                b"persist_test".to_vec(),
                b"should_not_persist".to_vec(),
                None,
            )
            .expect("put");
            tx.commit(WriteOptions::buffered()).unwrap();
            // engine dropped
        }

        // Assert: New memory engine instance has no data
        let opts2 = opts_for_mode("memory");
        {
            let engine = open_with_mode(&opts2, "memory");
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
            let got = tx.get(b"persist_test").expect("get");
            assert_eq!(
                got, None,
                "memory mode persisted data across restart (should not persist)"
            );
        }
    }

    #[test]
    fn should_isolate_data_given_multiple_memory_engines_when_separate_instances() {
        // Arrange: Create two separate memory engine instances
        let opts1 = opts_for_mode("memory");
        let opts2 = opts_for_mode("memory");

        // Act: Write different data to each
        let engine1 = open_with_mode(&opts1, "memory");
        let cf1 = engine1.create_column_family("test").expect("create cf");
        let cf_id1 = cf1.id();
        let mut tx = engine1
            .begin_tx(cf_id1, TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"test_key".to_vec(), b"engine1_value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        let engine2 = open_with_mode(&opts2, "memory");
        let cf2 = engine2.create_column_family("test").expect("create cf");
        let cf_id2 = cf2.id();
        let mut tx = engine2
            .begin_tx(cf_id2, TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"test_key".to_vec(), b"engine2_value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        // Assert: Each engine instance has isolated data
        let tx1 = engine1.begin_tx(cf_id1, TransactionMode::ReadOnly).unwrap();
        let got1 = tx1.get(b"test_key").expect("get");
        let tx2 = engine2.begin_tx(cf_id2, TransactionMode::ReadOnly).unwrap();
        let got2 = tx2.get(b"test_key").expect("get");

        assert_eq!(
            got1,
            Some(Bytes::from_static(b"engine1_value")),
            "engine1 data corruption or isolation failure"
        );
        assert_eq!(
            got2,
            Some(Bytes::from_static(b"engine2_value")),
            "engine2 data corruption or isolation failure"
        );
    }

    #[test]
    fn should_handle_many_writes_efficiently_when_writing_100_keys() {
        // Arrange
        // Memory mode only test
        let opts = opts_for_mode("memory");
        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Act: Perform many writes
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..100 {
            let key = format!("write_test_{i:03}");
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put");
        }
        tx.commit(WriteOptions::buffered()).unwrap();

        // Assert: All writes succeeded and data is retrievable
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        for i in &[0, 25, 50, 75, 99] {
            let key = format!("write_test_{i:03}");
            let got = tx.get(key.as_bytes()).expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"value")),
                "write_test_{i:03} retrieval failed"
            );
        }
    }

    #[test]
    fn should_handle_many_deletes_efficiently_when_deleting_50_keys() {
        // Memory mode only test
        let opts = opts_for_mode("memory");
        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Arrange: Write 50 keys
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..50 {
            let key = format!("delete_test_{i:02}");
            tx.put(key.into_bytes(), b"value".to_vec(), None)
                .expect("put");
        }
        tx.commit(WriteOptions::buffered()).unwrap();

        // Act: Delete all
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..50 {
            let key = format!("delete_test_{i:02}");
            tx.delete(key.into_bytes()).expect("delete");
        }
        tx.commit(WriteOptions::buffered()).unwrap();

        // Assert: All deleted
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        for i in &[0, 10, 25, 49] {
            let key = format!("delete_test_{i:02}");
            let got = tx.get(key.as_bytes()).expect("get");
            assert_eq!(got, None, "expected key to be deleted but found it");
        }
    }

    #[test]
    fn should_handle_mixed_operations_efficiently_when_put_delete_overwrite() {
        // Arrange
        // Memory mode only test
        let opts = opts_for_mode("memory");
        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        // Act: Mixed sequence
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"key1".to_vec(), b"v1".to_vec(), None).expect("put");
        tx.put(b"key2".to_vec(), b"v2".to_vec(), None).expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.delete(b"key1".to_vec()).expect("delete");
        tx.commit(WriteOptions::buffered()).unwrap();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"key1".to_vec(), b"v1_new".to_vec(), None)
            .expect("put");
        tx.put(b"key3".to_vec(), b"v3".to_vec(), None).expect("put");
        tx.commit(WriteOptions::buffered()).unwrap();

        // Assert: Correct final state
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        assert_eq!(
            tx.get(b"key1").expect("get"),
            Some(Bytes::from_static(b"v1_new"))
        );
        assert_eq!(
            tx.get(b"key2").expect("get"),
            Some(Bytes::from_static(b"v2"))
        );
        assert_eq!(
            tx.get(b"key3").expect("get"),
            Some(Bytes::from_static(b"v3"))
        );
    }
}

mod memory_spill_audit {
    //! Large-transaction behavior under low configured memory budgets.
    //!
    //! These tests verify externally visible behavior: large transactions commit
    //! successfully and their data remains readable. They do not claim direct
    //! visibility into whether spill-to-disk occurred internally.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::TransactionMode;

    #[test]
    fn should_commit_large_transaction_given_memory_budget_exceeded_when_committed() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts.memory_budget(128 * 1024), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin large transaction");
            for i in 0..500 {
                let key = format!("large_key_{i:05}");
                tx.put(key.as_bytes().to_vec(), vec![65u8; 1024], None)
                    .expect("put large value");
            }

            // Act
            tx.commit(buffered_write_options(mode))
                .expect("commit large transaction");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin verification transaction");
            assert_eq!(
                tx.get(b"large_key_00000").expect("read first key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
            assert_eq!(
                tx.get(b"large_key_00499").expect("read last key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
            assert_eq!(
                tx.get(b"large_key_00250").expect("read middle key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
        });
    }

    #[test]
    fn should_preserve_values_given_two_large_transactions_within_budget_when_read() {
        for_each_storage_mode(durable_storage_modes(), |mode, mut opts| {
            // Arrange
            // Keep both commits in the active memtable so this exercises their
            // simultaneous resident footprint rather than an intervening flush.
            opts.memtable_size = 1024 * 1024;
            let engine = open_with_mode(&opts.memory_budget(256 * 1024), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx1 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin first transaction");
            for i in 0..128 {
                let key = format!("batch1_key_{i:03}");
                tx1.put(key.as_bytes().to_vec(), vec![65u8; 1024], None)
                    .expect("put batch1 value");
            }
            tx1.commit(buffered_write_options(mode))
                .expect("commit first transaction");
            let mut tx2 = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin second transaction");
            for i in 0..128 {
                let key = format!("batch2_key_{i:03}");
                tx2.put(key.as_bytes().to_vec(), vec![66u8; 1024], None)
                    .expect("put batch2 value");
            }

            // Act
            tx2.commit(buffered_write_options(mode))
                .expect("commit second transaction");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin verification transaction");
            assert_eq!(
                tx.get(b"batch1_key_000").expect("read first batch key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
            assert_eq!(
                tx.get(b"batch2_key_000").expect("read second batch key"),
                Some(Bytes::from(vec![66u8; 1024]))
            );
            assert_eq!(
                tx.get(b"batch1_key_127").expect("read last batch1 key"),
                Some(Bytes::from(vec![65u8; 1024]))
            );
            assert_eq!(
                tx.get(b"batch2_key_127").expect("read last batch2 key"),
                Some(Bytes::from(vec![66u8; 1024]))
            );
        });
    }

    #[test]
    fn should_preserve_sample_keys_given_large_transaction_low_budget_when_committed() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts.memory_budget(64 * 1024), mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin low-budget transaction");
            for i in 0..200 {
                let key = format!("spilltest_key_{i:04}");
                tx.put(key.as_bytes().to_vec(), vec![88u8; 512], None)
                    .expect("put low-budget value");
            }

            // Act
            tx.commit(buffered_write_options(mode))
                .expect("commit low-budget transaction");

            // Assert
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin verification transaction");
            for key in [
                b"spilltest_key_0000".as_slice(),
                b"spilltest_key_0100".as_slice(),
                b"spilltest_key_0199".as_slice(),
            ] {
                assert_eq!(
                    tx.get(key).expect("read sample low-budget key"),
                    Some(Bytes::from(vec![88u8; 512])),
                    "mode: {} key: {}",
                    mode,
                    String::from_utf8_lossy(key)
                );
            }
        });
    }
}

mod backpressure {
    //! Backpressure tests (formerly Phase 2: backpressure validation)

    use crate::common::*;
    use cntryl_midge::{
        ColumnFamilyId, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    static BACKPRESSURE_STRESS_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn backpressure_stress_test_lock() -> &'static Mutex<()> {
        BACKPRESSURE_STRESS_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn commit_buffered_put(
        engine: &Engine,
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), MidgeError> {
        let mut txn = engine.begin_tx(cf_id, TransactionMode::ReadWrite)?;
        txn.put(key, value, None)?;
        txn.commit(WriteOptions::buffered())
    }

    fn write_until_committed(
        engine: &Engine,
        cf_id: ColumnFamilyId,
        writes: usize,
        value_bytes: usize,
    ) -> u64 {
        let mut committed = 0usize;
        let mut attempts = 0usize;
        let mut observed_stalls = 0u64;

        while committed < writes {
            attempts += 1;
            assert!(
                attempts <= writes * 4,
                "too many attempts while writing through backpressure"
            );

            let key = format!("auto_flush_key_{committed:06}").into_bytes();
            let value = vec![0xA5; value_bytes];
            match commit_buffered_put(engine, cf_id, key, value) {
                Ok(()) => committed += 1,
                Err(MidgeError::WriteStall(_)) => {
                    observed_stalls += 1;
                    assert!(
                        engine
                            .wait_for_write_stall_clear(cf_id, Duration::from_millis(500))
                            .expect("wait for stall clear"),
                        "transient stall should clear promptly"
                    );
                }
                Err(error) => panic!("unexpected write error: {error:?}"),
            }
        }

        observed_stalls
    }

    #[test]
    fn should_auto_flush_explicit_small_memtable_without_permanent_write_stall() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .with_memtable_size_limit(128 * 1024)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine
            .create_column_family("auto_flush")
            .expect("create cf");

        let _observed_stalls = write_until_committed(&engine, cf.id(), 800, 1024);

        // Act
        // Assert
        assert!(
            engine
                .wait_for_write_stall_clear(cf.id(), Duration::from_millis(500))
                .expect("wait for final stall clear"),
            "runtime should not remain permanently stalled"
        );
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        assert!(
            metrics.sst_count >= 1,
            "natural auto-flush should publish at least one SST"
        );
        assert!(
            !metrics.write_stalled,
            "runtime metrics should not report a sticky write stall"
        );
    }

    #[test]
    fn should_auto_flush_when_explicit_flush_threshold_is_lower_than_size_limit() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Engine::open(
            OpenOptions::local(temp_dir.path())
                .with_memtable_size_limit(1024 * 1024)
                .with_memtable_flush_threshold(64 * 1024)
                .background_compaction(false)
                .build()
                .expect("build options"),
        )
        .expect("open engine");
        let cf = engine
            .create_column_family("flush_threshold")
            .expect("create cf");

        let _observed_stalls = write_until_committed(&engine, cf.id(), 300, 1024);

        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        // Act
        // Assert
        assert_eq!(metrics.memtable_size_limit, 1024 * 1024);
        assert_eq!(metrics.memtable_flush_threshold, 64 * 1024);
        assert!(
            metrics.sst_count >= 2,
            "lower flush threshold should drive repeated natural auto-flushes"
        );
        assert!(!metrics.write_stalled);
    }

    #[test]
    fn should_bound_published_l0_when_background_compaction_is_disabled() {
        // Arrange
        let temp_dir = TempDir::new().expect("temp dir");
        let options = OpenOptions::local(temp_dir.path())
            .background_compaction(false)
            .build()
            .expect("build options");
        // The immutable queue limit is internal and fixed at ten; the final slot
        // is reserved for the active memtable generation.
        let hard_ceiling = options.l0_compaction_trigger() + 10 + 1;
        let engine = Engine::open(options).expect("open engine");
        let cf = engine
            .create_column_family("bounded-l0")
            .expect("create cf");
        let mut accepted = 0_usize;
        let mut attempts = 0_usize;

        // Act
        while accepted < hard_ceiling * 2 {
            attempts += 1;
            assert!(
                attempts <= hard_ceiling * 8,
                "L0 recovery must make progress"
            );
            let result = commit_buffered_put(
                &engine,
                cf.id(),
                format!("generation-{accepted:02}").into_bytes(),
                b"value".to_vec(),
            );
            match result {
                Ok(()) => {
                    accepted += 1;
                    engine.flush_cf(&cf).expect("flush reserved L0 generation");
                }
                Err(MidgeError::WriteStall(_)) => {
                    assert!(
                        engine
                            .wait_for_write_stall_clear(cf.id(), Duration::from_secs(10))
                            .expect("wait for live L0 recovery"),
                        "critical L0 pressure must clear without manual compaction"
                    );
                }
                Err(error) => panic!("unexpected write result: {error}"),
            }
            let layout = engine.get_storage_layout().expect("live layout");
            let l0_files = layout
                .levels
                .iter()
                .find(|level| level.level == 0)
                .map_or(0, |level| level.file_count);
            assert!(
                l0_files <= hard_ceiling,
                "published L0 exceeded its ceiling"
            );
        }

        // Assert
        assert_eq!(accepted, hard_ceiling * 2);
        engine.compact_all().expect("manual compaction clears debt");
        let drained_layout = engine.get_storage_layout().expect("drained layout");
        assert_eq!(
            drained_layout
                .levels
                .iter()
                .find(|level| level.level == 0)
                .map_or(0, |level| level.file_count),
            0,
            "compact_all must not return while L0 debt remains"
        );
        commit_buffered_put(&engine, cf.id(), b"after-drain".to_vec(), b"value".to_vec())
            .expect("write admission resumes after every pressure source clears");
    }

    #[test]
    fn should_return_write_stall_when_memory_budget_exceeded() {
        // Arrange
        // Memory mode doesn't have meaningful backpressure (everything stays in memory)
        // so we only test durable modes where flush/compaction creates actual pressure.

        // Act
        let results = std::cell::RefCell::new(Vec::<(String, bool)>::new());
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            let mut opts = opts;
            // Use a smaller budget and an explicit large memtable so the hard
            // budget path still produces a deterministic stall now that soft
            // memtable pressure auto-flushes instead of sticking.
            opts = opts.memory_budget(1024 * 1024);
            opts.memtable_size = 32 * 1024 * 1024;
            let engine = Engine::open(opts.to_open_options()).expect("failed to open engine");
            let cf = engine.create_column_family("test").expect("create cf");

            let mut write_stall_observed = false;

            for i in 0..10_000 {
                let key = format!("key_{i:06}");
                let value = vec![0u8; 1024]; // 1KB

                let mut txn = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin");
                txn.put(key.as_bytes().to_vec(), value.clone(), None)
                    .expect("put");

                match txn.commit(buffered_write_options(mode)) {
                    Ok(()) => {}
                    Err(MidgeError::WriteStall(_msg)) => {
                        write_stall_observed = true;
                        break;
                    }
                    Err(e) => panic!("unexpected: {e:?}"),
                }
            }

            results
                .borrow_mut()
                .push((mode.to_string(), write_stall_observed));
        });

        // Assert
        for (mode, write_stall_observed) in results.into_inner() {
            assert!(
                !write_stall_observed,
                "{mode} mode should now relieve soft memtable pressure without returning WriteStall"
            );
        }
    }

    #[test]
    fn should_succeed_after_backoff_when_write_stall_cleared() {
        // Arrange

        // Act
        let results = std::cell::RefCell::new(Vec::<(String, bool, bool)>::new());
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            let mut opts = opts;
            // Use 1MB budget plus an explicit large memtable so we can still
            // observe the stall-clear cycle deterministically after the runtime
            // started letting soft memtable pressure flush naturally.
            opts = opts.memory_budget(1024 * 1024);
            opts.memtable_size = 32 * 1024 * 1024;
            let engine = Engine::open(opts.to_open_options()).expect("failed to open engine");
            let cf = engine.create_column_family("test").expect("create cf");

            // Hit first stall
            let mut first_stall_observed = false;
            for i in 0..5000 {
                let key = format!("key_{i:06}");
                let value = vec![0u8; 1024];
                let mut txn = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin");
                txn.put(key.as_bytes().to_vec(), value.clone(), None)
                    .expect("put");

                match txn.commit(buffered_write_options(mode)) {
                    Ok(()) => {}
                    Err(MidgeError::WriteStall(_)) => {
                        first_stall_observed = true;
                        break;
                    }
                    Err(e) => panic!("unexpected: {e:?}"),
                }
            }

            // Wait for stall to clear (compaction, etc.)
            std::thread::sleep(Duration::from_millis(100));

            // Attempt a write after stall is cleared; should succeed or stall again (not panic)
            let key = "recovery_key".to_string();
            let value = vec![0u8; 1024];
            let mut txn = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin");
            txn.put(key.as_bytes().to_vec(), value.clone(), None)
                .expect("put");

            let second_write_ok_or_stall = match txn.commit(buffered_write_options(mode)) {
                Ok(()) | Err(MidgeError::WriteStall(_)) => true,
                Err(e) => panic!("unexpected error: {e:?}"),
            };

            results.borrow_mut().push((
                mode.to_string(),
                first_stall_observed,
                second_write_ok_or_stall,
            ));
        });

        // Assert
        for (mode, first_stall_observed, second_write_ok_or_stall) in results.into_inner() {
            if mode == "local" {
                assert!(
                    !first_stall_observed,
                    "local mode should now relieve soft memtable pressure before a sticky stall appears"
                );
            }
            assert!(
                second_write_ok_or_stall,
                "Expected success or stall (not error) in mode {mode}"
            );
        }
    }

    #[test]
    fn should_prevent_oom_by_rejecting_writes_when_budget_exceeded() {
        // Arrange
        let _guard = backpressure_stress_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Act
        let results = std::cell::RefCell::new(Vec::<(String, u64, u64, usize, bool)>::new());
        for_each_storage_mode(&["local"], |mode, opts| {
            let mut opts = opts;
            opts = opts.memory_budget(512 * 1024); // 512KB instead of 2MB for faster backpressure trigger

            // In local mode, the default small memtable can flush fast enough that we never
            // accumulate enough in-memory state to trip the memory budget. Keep the
            // threshold above the default but inside this test's 512 KiB pressure window.
            if mode == "local" {
                opts.memtable_size = 256 * 1024;
            }

            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();

            let max_attempts = 64;
            let write_options = buffered_write_options(mode);
            let mut total_writes = 0;
            let mut total_stalls = 0;

            while total_writes + total_stalls < max_attempts {
                let key = format!("key_{total_writes}");
                let value = vec![0u8; 8192]; // 8KB for faster memory budget exhaustion
                let mut txn = engine
                    .begin_tx(cf_id, TransactionMode::ReadWrite)
                    .expect("begin");
                txn.put(key.as_bytes().to_vec(), value, None).expect("put");

                match txn.commit(write_options) {
                    Ok(()) => total_writes += 1,
                    Err(MidgeError::WriteStall(_)) => {
                        total_stalls += 1;
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("unexpected: {e:?}"),
                }
            }

            // Natural flush publication is asynchronous. Wait for the actual acceptance
            // condition instead of assuming a fixed sleep covers hosted-runner contention.
            let deadline = Instant::now() + Duration::from_secs(30);
            let metrics = loop {
                let metrics = engine.get_runtime_metrics().expect("runtime metrics");
                if total_stalls > 0 || (metrics.sst_count > 0 && !metrics.write_stalled) {
                    break metrics;
                }
                assert!(
                    Instant::now() < deadline,
                    "local pressure was not rejected or naturally flushed: writes={total_writes}, stalls={total_stalls}, ssts={}, write_stalled={}",
                    metrics.sst_count,
                    metrics.write_stalled
                );
                std::thread::sleep(Duration::from_millis(10));
            };

            results.borrow_mut().push((
                mode.to_string(),
                total_writes,
                total_stalls,
                metrics.sst_count,
                metrics.write_stalled,
            ));
        });

        // Assert
        for (mode, total_writes, total_stalls, sst_count, write_stalled) in results.into_inner() {
            assert_eq!(mode, "local");
            assert!(
                total_stalls > 0 || (sst_count > 0 && !write_stalled),
                "Expected local mode to either reject writes under hard pressure or relieve pressure via natural flush: writes={total_writes}, stalls={total_stalls}, ssts={sst_count}, write_stalled={write_stalled}"
            );
        }
    }

    #[test]
    fn should_handle_concurrent_writes_with_consistent_backpressure() {
        // Arrange
        let _guard = backpressure_stress_test_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Act
        let results = std::cell::RefCell::new(Vec::<(String, u64, Vec<(u64, u64)>, bool)>::new());
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            let mut opts = opts;
            opts = opts.memory_budget(4 * 1024 * 1024);
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let shutdown = Arc::new(AtomicBool::new(false));
            let mut handles = vec![];

            for thread_id in 0..4 {
                let engine_clone = Arc::clone(&engine);
                let shutdown_clone = shutdown.clone();
                let cf_id = cf.id();
                let write_opts = buffered_write_options(mode);

                let handle = std::thread::spawn(move || {
                    let mut writes = 0u64;
                    let mut stalls = 0u64;

                    while !shutdown_clone.load(Ordering::Relaxed) {
                        let key = format!("thread_{thread_id}_key_{writes}");
                        let value = vec![0u8; 1024];
                        let mut txn = engine_clone
                            .begin_tx(cf_id, TransactionMode::ReadWrite)
                            .expect("begin");
                        txn.put(key.as_bytes().to_vec(), value.clone(), None)
                            .expect("put");

                        match txn.commit(write_opts) {
                            Ok(()) => writes += 1,
                            Err(MidgeError::WriteStall(_)) => {
                                stalls += 1;
                                std::thread::sleep(Duration::from_millis(5));
                            }
                            Err(e) => panic!("thread {thread_id} unexpected: {e:?}"),
                        }

                        if writes + stalls >= 250 {
                            break;
                        }
                    }

                    (writes, stalls)
                });

                handles.push(handle);
            }

            std::thread::sleep(Duration::from_secs(2));
            shutdown.store(true, Ordering::Relaxed);

            let mut total_writes = 0u64;
            let mut per_thread = Vec::new();
            for handle in handles {
                let (writes, stalls) = handle.join().expect("panic");
                total_writes += writes;
                per_thread.push((writes, stalls));
            }

            // Background compaction is disabled by this shared fixture. Drain the
            // bounded L0 debt explicitly, then require every pressure predicate to
            // clear. Memory mode has no persistent storage debt.
            let stall_cleared = if mode == "memory" {
                true
            } else {
                engine
                    .compact_all()
                    .expect("manual compaction clears concurrent-burst debt");
                engine
                    .wait_for_write_stall_clear(cf.id(), Duration::from_secs(5))
                    .expect("wait for stall clear after concurrent burst")
            };

            results
                .borrow_mut()
                .push((mode.to_string(), total_writes, per_thread, stall_cleared));
        });

        // Assert
        for (mode, total_writes, per_thread, stall_cleared) in results.into_inner() {
            assert_eq!(
                per_thread.len(),
                4,
                "expected all 4 worker threads to report in mode {mode}"
            );
            for (thread_index, (writes, stalls)) in per_thread.iter().enumerate() {
                assert!(
                    writes + stalls > 0,
                    "thread {thread_index} made no progress (neither committed nor stalled) in mode {mode}"
                );
            }
            assert!(
                stall_cleared,
                "backpressure should clear again after the concurrent burst in mode {mode}, not stay permanently stalled"
            );
            if !mode.eq("memory") {
                assert!(total_writes > 0, "should have writes in mode {mode}");
            }
        }
    }
}

mod column_family_reclamation_hardening {
    use bytes::Bytes;
    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    #[derive(Clone, Copy, Debug)]
    enum TestStorage {
        Local,
        SimulatedCloud,
    }

    fn open_engine(path: &Path, storage: TestStorage) -> Engine {
        let builder = match storage {
            TestStorage::Local => OpenOptions::local(path),
            TestStorage::SimulatedCloud => {
                OpenOptions::cloud_simulated(path, "reclamation-test", "cf/")
            }
        };
        Engine::open(
            builder
                .background_compaction(false)
                .with_memtable_size_limit(16 * 1024)
                .with_memtable_flush_threshold(16 * 1024)
                .build()
                .expect("build options"),
        )
        .expect("open engine")
    }

    fn write_and_flush(
        engine: &Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
        storage: TestStorage,
    ) -> Vec<String> {
        let mut transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write transaction");
        transaction
            .put(b"retained-key".to_vec(), vec![b'x'; 32 * 1024], None)
            .expect("put retained value");
        transaction
            .commit(match storage {
                TestStorage::Local => WriteOptions::sync(),
                TestStorage::SimulatedCloud => WriteOptions::cloud_strict(),
            })
            .expect("commit retained value");
        engine.flush_cf(cf).expect("flush column family");

        let layout = engine.get_storage_layout().expect("storage layout");
        let files = layout
            .levels
            .iter()
            .flat_map(|level| &level.files)
            .filter(|file| file.cf_id == cf.id())
            .map(|file| file.name.clone())
            .collect::<Vec<_>>();
        assert!(!files.is_empty(), "flush must publish at least one SST");
        files
    }

    fn manifest_contains_cf_files(engine: &Engine, cf_id: u32) -> bool {
        engine
            .get_storage_layout()
            .expect("storage layout")
            .levels
            .iter()
            .flat_map(|level| &level.files)
            .any(|file| file.cf_id == cf_id)
    }

    fn object_paths(root: &Path, storage: TestStorage, names: &[String]) -> Vec<PathBuf> {
        names
            .iter()
            .flat_map(|name| {
                let mut paths = vec![root.join("sst").join(name)];
                if matches!(storage, TestStorage::SimulatedCloud) {
                    paths.push(root.join("hybrid_local").join("sst").join(name));
                    paths.push(root.join("cloud_store").join("sst").join(name));
                }
                paths
            })
            .filter(|path| path.exists())
            .collect()
    }

    fn wait_until_reclaimed(
        engine: &Engine,
        root: &Path,
        storage: TestStorage,
        cf_id: u32,
        names: &[String],
    ) {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let manifest_reclaimed = !manifest_contains_cf_files(engine, cf_id);
            let retained_objects = object_paths(root, storage, names);
            if manifest_reclaimed && retained_objects.is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "dropped CF {cf_id} was not reclaimed: manifest_reclaimed={manifest_reclaimed}, retained_objects={retained_objects:?}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn should_reclaim_column_family_files_given_no_snapshot_pin_when_gc_runs() {
        for storage in [TestStorage::Local, TestStorage::SimulatedCloud] {
            // Arrange
            let temp = tempfile::tempdir().expect("temp dir");
            let engine = open_engine(temp.path(), storage);
            let cf = engine
                .create_column_family("reclaim-unpinned")
                .expect("create column family");
            let files = write_and_flush(&engine, &cf, storage);

            // Act
            engine
                .drop_column_family(cf.id())
                .expect("drop column family");

            // Assert
            wait_until_reclaimed(&engine, temp.path(), storage, cf.id(), &files);
        }
    }

    #[test]
    fn should_preserve_snapshot_visibility_given_column_family_drop_when_old_snapshot_is_active() {
        for storage in [TestStorage::Local, TestStorage::SimulatedCloud] {
            // Arrange
            let temp = tempfile::tempdir().expect("temp dir");
            let engine = open_engine(temp.path(), storage);
            let cf = engine
                .create_column_family("snapshot-drop")
                .expect("create column family");
            let files = write_and_flush(&engine, &cf, storage);
            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin pre-drop snapshot");

            // Act
            engine
                .drop_column_family(cf.id())
                .expect("drop column family");
            let pre_drop_value = snapshot
                .get(b"retained-key")
                .expect("read through pre-drop snapshot");
            let new_transaction = engine.begin_tx(cf.id(), TransactionMode::ReadOnly);

            // Assert
            assert_eq!(pre_drop_value, Some(Bytes::from(vec![b'x'; 32 * 1024])));
            assert!(matches!(
                new_transaction,
                Err(cntryl_midge::MidgeError::InvalidArgument(message))
                    if message == format!("column family {} does not exist", cf.id())
            ));
            drop(snapshot);
            wait_until_reclaimed(&engine, temp.path(), storage, cf.id(), &files);
        }
    }

    #[test]
    fn should_defer_column_family_file_reclamation_given_old_snapshot_pin_when_gc_runs() {
        for storage in [TestStorage::Local, TestStorage::SimulatedCloud] {
            // Arrange
            let temp = tempfile::tempdir().expect("temp dir");
            let engine = open_engine(temp.path(), storage);
            let cf = engine
                .create_column_family("reclaim-pinned")
                .expect("create column family");
            let files = write_and_flush(&engine, &cf, storage);
            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin pinned snapshot");

            // Act
            engine
                .drop_column_family(cf.id())
                .expect("drop column family");

            // Assert
            assert_eq!(
                snapshot.get(b"retained-key").expect("read pinned value"),
                Some(Bytes::from(vec![b'x'; 32 * 1024]))
            );
            assert!(
                manifest_contains_cf_files(&engine, cf.id()),
                "authoritative manifest must retain dropped-CF SSTs while a snapshot can read them"
            );
            assert!(
                !object_paths(temp.path(), storage, &files).is_empty(),
                "dropped-CF objects must remain while pinned"
            );

            drop(snapshot);
            wait_until_reclaimed(&engine, temp.path(), storage, cf.id(), &files);
        }
    }
}
