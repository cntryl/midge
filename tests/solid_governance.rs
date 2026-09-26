#[test]
fn should_keep_metric_verification_delegates_off_engine() {
    // Arrange
    let source = include_str!("../src/engine/mod.rs");

    // Act
    let forbidden = [
        "pub fn get_read_amp_metrics(",
        "pub fn get_recovery_metrics(",
        "pub fn get_runtime_metrics(",
        "pub fn get_runtime_metrics_with_timeout(",
        "pub fn verify_storage(",
        "pub fn verify_path(",
        "pub fn get_storage_layout(",
    ];

    // Assert
    for signature in forbidden {
        assert!(
            !source.contains(signature),
            "Engine still exposes {signature}"
        );
    }
}

#[test]
fn should_keep_test_only_protocols_out_of_runtime() {
    // Arrange
    let state = include_str!("../src/runtime/state.rs");
    let protocol = include_str!("../src/runtime/protocol.rs");
    let durability = include_str!("../src/runtime/durability.rs");
    let engine = include_str!("../src/engine/mod.rs");

    // Act
    // Assert
    for (name, source) in [
        ("state", state),
        ("protocol", protocol),
        ("durability", durability),
        ("engine", engine),
    ] {
        for forbidden in [
            "ingest_active",
            "ingest_epoch",
            "BeginIngest",
            "EndIngest",
            "idempotency_cache",
            "ConfirmWalAppend",
        ] {
            assert!(!source.contains(forbidden), "{name} retains {forbidden}");
        }
    }
    assert!(state.contains("HashSet<u64>"));
    assert!(!state.contains("HashMap<u64, String>"));
}

#[test]
fn should_require_explicit_sst_reader_behavior() {
    // Arrange
    let traits = include_str!("../src/sst/traits.rs");

    // Act
    let reader_contract = traits
        .split("/// Materializing cursor for explicit test doubles.")
        .next()
        .expect("reader contract");
    // Assert
    assert!(reader_contract.contains("fn range_tombstones(&self) -> Vec<RangeTombstone>;"));
    assert!(!traits.contains("persist_sst_bytes_with_host_fs"));
    assert!(traits.contains("fn finish_to_path(self: Box<Self>, path: &Path) -> MidgeResult<()>;"));
}

#[test]
fn should_use_one_sst_reader_decode_path() {
    // Arrange
    let reader = include_str!("../src/sst/fs/reader_io/mod.rs");
    let io = include_str!("../src/sst/fs/reader_io/io.rs");
    let recovery = include_str!("../src/sst/fs/reader_io/recovery.rs");
    let state = include_str!("../src/sst/fs/reader_io/state.rs");
    let scan = include_str!("../src/sst/fs/reader_io/scan.rs");

    // Act
    let sources = [reader, io, recovery, state, scan];
    // Assert
    assert_eq!(
        sources
            .iter()
            .map(|source| source.matches("u32::from_le_bytes").count())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        sources
            .iter()
            .map(|source| source.matches("Invalid shared prefix length").count())
            .sum::<usize>(),
        1
    );
    assert!(io.contains("fn read_framed_block"));
    assert!(reader.contains("struct BlockEntryDecoder"));
    assert!(!state.contains("encoding::decode_with_format("));
    assert!(scan.matches("self.block_span(").count() >= 2);
}

#[test]
fn should_keep_dead_interfaces_out_of_production() {
    // Arrange
    let types = include_str!("../src/sst/types.rs");
    let traits = include_str!("../src/sst/traits.rs");
    let reader = include_str!("../src/sst/fs/reader_io/mod.rs");
    let skiplist = include_str!("../src/memtable/skiplist.rs");

    // Act
    // Assert
    assert!(!types.contains("enum SstBlockType"));
    assert!(!traits.contains("pub trait SstReader: "));
    assert!(!reader.contains("    bloom_metrics: BloomMetrics"));
    assert!(!reader.contains("    read_amp_metrics: ReadAmpMetrics"));
    for signature in [
        "pub fn get(&self, key: &[u8], snapshot_seq: u64)",
        "pub fn get_all_keys(&self)",
        "pub fn tombstones_range_visible(",
    ] {
        let position = skiplist.find(signature).expect("test reader remains gated");
        assert!(skiplist[..position].ends_with("#[cfg(test)]\n    "));
    }
}

#[test]
fn should_route_compaction_output_checks_through_injected_fs() {
    // Arrange
    let executor = include_str!("../src/compaction/executor.rs");
    let actor = include_str!("../src/runtime/actors/compaction.rs");
    let flush = include_str!("../src/runtime/actors/flush/build.rs");
    let reader = include_str!("../src/sst/fs/reader_io/mod.rs");

    // Act
    let output_code = executor.split("#[cfg(test)]\nmod tests").next().unwrap();
    // Assert
    assert!(!output_code.contains("std::fs::metadata("));
    assert!(!output_code.contains("std::fs::remove_file("));
    assert!(!actor.contains("std::fs::remove_file("));
    assert!(!actor.contains("std::fs::read_dir("));
    assert!(!flush.contains("std::fs::create_dir_all("));
    assert!(!flush.contains("file_identity(&task.staging_path)"));
    assert!(!reader.contains("summarize_with_real_fs_for_compaction"));
}

#[test]
fn should_include_range_tombstones_in_skippable_sst_sequence_bounds() {
    // Arrange
    let flush = include_str!("../src/runtime/actors/flush/build.rs");
    let summary = include_str!("../src/sst/fs/reader_io/mod.rs");
    let compaction = include_str!("../src/runtime/actors/compaction.rs");
    let backfill = include_str!("../src/runtime/event_loop/read_path.rs");
    let snapshot = include_str!("../src/runtime/read_snapshot.rs");

    // Act
    // Assert
    assert!(flush.contains("largest_seq = largest_seq.max(range.seq)"));
    assert!(summary.contains("accumulator.observe(size_bytes, &range.start, range.seq"));
    assert!(summary.contains("accumulator.observe(size_bytes, &range.end, range.seq"));
    assert!(compaction.contains("largest_seq: Some(summary.largest_seq)"));
    assert!(backfill.contains("updated.largest_seq = Some(summary.largest_seq)"));
    let conflict_check = snapshot
        .split("pub fn any_sequence_after_in_range(")
        .nth(1)
        .unwrap()
        .split("/// Perform a range scan")
        .next()
        .unwrap();
    assert!(conflict_check.contains("reader.raw_state_scan("));
    assert!(!conflict_check.contains("scan_range_state_with_time("));
}

#[test]
fn should_copy_file_meta_proofs_only_in_central_conversions() {
    // Arrange
    let producers = [
        include_str!("../src/runtime/actors/manifest.rs"),
        include_str!("../src/runtime/state/manifest.rs"),
        include_str!("../src/runtime/actors/flush.rs"),
        include_str!("../src/runtime/event_loop/flush_pipeline.rs"),
        include_str!("../src/engine/startup/storage.rs"),
    ];

    // Act
    // Assert
    for source in producers {
        assert!(source.lines().all(|line| {
            !(line.contains("key_bounds_complete:") && line.contains(".key_bounds_complete"))
        }));
    }
}

#[test]
fn should_keep_dead_storage_verbs_out_of_backend() {
    // Arrange
    let sources = [
        include_str!("../src/storage/mod.rs"),
        include_str!("../src/storage/cloud/adapter.rs"),
        include_str!("../src/storage/filesystem.rs"),
    ];

    // Act
    // Assert
    for source in sources {
        for forbidden in [
            "fn submit_read(",
            "fn submit_read_with_timeout(",
            "fn submit_list(",
            "ReadComplete",
            "ListComplete",
        ] {
            assert!(
                !source.contains(forbidden),
                "storage backend retains {forbidden}"
            );
        }
    }
}

#[test]
fn should_keep_filesystem_error_conversion_out_of_public_api() {
    // Arrange
    let source = include_str!("../src/io/traits.rs");

    // Act
    // Assert
    assert!(
        !source.contains("impl From<FsError> for"),
        "a public From<FsError> impl exposes FsError through MidgeError"
    );
    assert!(
        !source.contains("allow(unnameable_types)"),
        "FsError must be unreachable from default builds without a lint allow"
    );
}

#[test]
fn should_not_glob_reexport_internal_modules_when_exposing_internal_testing_surface() {
    // Arrange
    let source = include_str!("../src/lib.rs");
    let internal = source
        .split("pub mod __internal ")
        .nth(1)
        .expect("lib.rs declares __internal");
    assert!(internal.starts_with(char::from(123_u8)));

    // Act
    let glob_reexports: Vec<&str> = internal
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("pub use crate::") && line.ends_with("::*;"))
        .collect();

    // Assert
    assert!(
        glob_reexports.is_empty(),
        "__internal glob re-exports hide dead pub items: {glob_reexports:?}"
    );
    assert!(
        !source.contains("allow(dead_code"),
        "lib.rs must not suppress dead_code for whole modules"
    );
}

#[test]
fn should_keep_wal_retention_in_one_module_when_splitting_event_loop() {
    // Arrange
    let event_loop =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/runtime/event_loop");
    let read = |relative: &str| {
        std::fs::read_to_string(event_loop.join(relative))
            .unwrap_or_else(|error| panic!("read event_loop/{relative}: {error}"))
    };
    let mut sources = Vec::new();
    let mut pending = vec![event_loop.clone()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("list event_loop sources") {
            let path = entry.expect("event_loop entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let relative = path
                    .strip_prefix(&event_loop)
                    .expect("event_loop source")
                    .to_string_lossy()
                    .into_owned();
                sources.push((relative.clone(), read(&relative)));
            }
        }
    }

    // Act
    let defining = |signature: &str| -> Vec<String> {
        sources
            .iter()
            .filter(|(_, source)| source.contains(signature))
            .map(|(relative, _)| relative.clone())
            .collect()
    };
    let local = defining("fn prune_local_wal_segments_covered_by_manifest(");
    let cloud = defining("fn prune_cloud_wal_segments_covered_by_manifest(");
    let mod_lines = read("mod.rs").lines().count();
    let dispatch = read("dispatch.rs");

    // Assert
    assert_eq!(local, vec!["wal_retention.rs".to_string()]);
    assert_eq!(cloud, vec!["wal_retention/cloud.rs".to_string()]);
    assert!(mod_lines < 900, "event_loop/mod.rs has {mod_lines} lines");
    assert!(
        !dispatch.lines().any(|line| {
            let line = line.trim_start();
            line.starts_with("enum ") && line.contains("Route")
        }),
        "dispatch.rs still re-encodes messages into route enums"
    );
}

#[test]
fn should_group_runtime_state_fields_by_owner() {
    // Arrange
    let source = include_str!("../src/runtime/state.rs");
    let body = source
        .split("pub struct RuntimeState {")
        .nth(1)
        .and_then(|rest| rest.split("\n}\n").next())
        .expect("RuntimeState body");

    // Act
    let loose: Vec<&str> = [
        "memtable_size_limit:",
        "memtable_flush_threshold:",
        "eventual_flush_segment_gap:",
        "max_immutable_memtables:",
        "l0_compaction_trigger:",
        "compaction_config:",
        "wal_recovery_records_replayed:",
        "wal_recovery_bytes_replayed:",
        "intent_log_replay_runs:",
        "intent_log_entries_replayed:",
    ]
    .into_iter()
    .filter(|field| body.contains(field))
    .collect();

    // Assert
    assert!(
        loose.is_empty(),
        "RuntimeState still owns {loose:?} directly"
    );
    assert!(body.contains("pub limits: RuntimeLimits,"));
    assert!(body.contains("pub recovery_stats: RecoveryStats,"));
    assert!(body.contains("pub manifest: ManifestRuntimeState,"));
    assert!(!include_str!("../src/runtime/event_loop/mod.rs").contains("sst_read_views"));
}

#[test]
fn should_route_manifest_file_list_mutation_through_runtime_owner() {
    // Arrange
    let production_sources = [
        include_str!("../src/runtime/state/manifest.rs"),
        include_str!("../src/runtime/actors/manifest.rs"),
        include_str!("../src/runtime/ddl.rs"),
        include_str!("../src/engine/startup/cloud_recovery/mod.rs"),
    ];

    // Act
    let bypasses: Vec<_> = production_sources
        .iter()
        .enumerate()
        .flat_map(|(index, source)| {
            [
                ".manifest.files.push(",
                ".manifest.files.retain(",
                ".manifest.files =",
            ]
            .into_iter()
            .filter(move |pattern| source.contains(pattern))
            .map(move |pattern| (index, pattern))
        })
        .collect();

    // Assert
    assert!(
        bypasses.is_empty(),
        "manifest file-list owner bypasses: {bypasses:?}"
    );
}

#[test]
fn should_keep_cloud_fields_in_cloud_coordinator() {
    // Arrange
    let source = include_str!("../src/runtime/event_loop/mod.rs");
    let body = source
        .split("pub struct EventLoop {")
        .nth(1)
        .and_then(|rest| rest.split("\n}\n").next())
        .expect("EventLoop body");

    // Act
    let loose: Vec<_> = [
        "hybrid_storage:",
        "hybrid_storage_events:",
        "cloud_metadata_storage:",
        "cloud_wal:",
        "cloud_wal_prune_worker:",
        "cloud_wal_prune_progress:",
        "cloud_maintenance:",
    ]
    .into_iter()
    .filter(|field| body.contains(field))
    .collect();

    // Assert
    assert!(loose.is_empty(), "EventLoop still owns {loose:?} directly");
    assert!(body.contains("cloud_coordinator: CloudCoordinator,"));
}
