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
fn should_keep_rest_pagination_state_in_shared_provider_core() {
    // Arrange
    let rest = include_str!("../src/storage/providers/rest/mod.rs");
    let providers = [
        include_str!("../src/storage/providers/s3.rs"),
        include_str!("../src/storage/providers/azure.rs"),
        include_str!("../src/storage/providers/gcs.rs"),
    ];

    // Act
    // Assert
    assert!(rest.contains("pub(super) struct PagedList"));
    assert!(rest.contains("pub(super) fn finish_paged_list"));
    for source in providers {
        assert!(!source.contains("struct S3ListState"));
        assert!(!source.contains("struct AzureListState"));
        assert!(!source.contains("struct GcsListState"));
        assert!(!source.contains("checked list error"));
    }
}

#[test]
fn should_keep_rest_error_detail_in_shared_provider_core() {
    // Arrange
    let rest = include_str!("../src/storage/providers/rest/mod.rs");
    let providers = [
        include_str!("../src/storage/providers/s3.rs"),
        include_str!("../src/storage/providers/azure.rs"),
        include_str!("../src/storage/providers/gcs.rs"),
    ];

    // Act
    // Assert
    assert!(rest.contains("pub(super) fn response_error_detail"));
    assert!(rest.contains("pub(super) fn map_response"));
    for source in providers {
        assert!(!source.contains("format!(\"{operation}: {code}: {message}\")"));
        assert!(!source.contains("format!(\"{operation}: {reason}: {message}\")"));
    }
}

#[test]
fn should_keep_request_timeout_parser_in_cloud_core() {
    // Arrange
    let cloud = include_str!("../src/storage/cloud/mod.rs");
    let rest = include_str!("../src/storage/providers/rest/mod.rs");
    let providers = [
        include_str!("../src/storage/providers/s3.rs"),
        include_str!("../src/storage/providers/azure.rs"),
        include_str!("../src/storage/providers/gcs.rs"),
    ];

    // Act
    // Assert
    assert!(cloud.contains("fn parse_request_timeout"));
    assert!(rest.contains("pub(super) fn apply_caller_headers"));
    assert!(rest.contains("pub(super) fn split_request_timeout"));
    for source in providers {
        assert!(!source.contains("fn parse_request_timeout"));
        assert!(!source.contains("REQUEST_TIMEOUT_HEADER"));
        assert!(!source.contains("split_request_timeout_header"));
        assert!(source.matches("super::rest::apply_caller_headers(").count() >= 2);
    }
}

#[test]
fn should_keep_sqrzl_signing_in_shared_test_support() {
    // Arrange
    let support = include_str!("support/sqrzl.rs");
    let qualification = include_str!("../src/storage/providers/qualification.rs");
    let engine = include_str!("cloud_provider_engine_qualification.rs");

    // Act
    // Assert
    for helper in [
        "fn signed_s3_request(",
        "fn signed_gcs_request(",
        "fn azure_string_to_sign(",
        "fn signed_azure_request(",
    ] {
        assert!(support.contains(helper));
        assert!(!qualification.contains(helper));
        assert!(!engine.contains(helper));
    }
    assert!(support.contains("fn configured_real_s3_settings("));
    assert!(!qualification.contains("REAL_S3_BUCKET_ENV"));
    assert!(!engine.contains("REAL_S3_BUCKET_ENV"));
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
    assert!(state.contains("BTreeMap<u64, crate::common::OperationDeadline>"));
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

fn assert_typed_storage_backend_contract() -> usize {
    let source = include_str!("../src/storage/mod.rs");
    let contract_end = std::str::from_utf8(&[10, 125, 10]).expect("trait closing delimiter");
    let contract = source
        .split("pub trait StorageBackend:")
        .nth(1)
        .expect("StorageBackend declaration")
        .split(contract_end)
        .next()
        .expect("StorageBackend body");

    let methods: Vec<_> = contract
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("fn submit_"))
        .map(|line| line.split('(').next().expect("method name"))
        .collect();

    assert_eq!(
        methods,
        [
            "range_read_request",
            "range_head_request",
            "metadata_read_request",
            "head_request",
            "delete_request",
            "write_request",
        ]
    );
    assert!(
        !contract.contains("#[cfg("),
        "test builds must see the same trait"
    );
    assert!(
        !contract.contains(") {"),
        "storage methods must be required"
    );
    for source in [
        include_str!("../src/storage/cloud/adapter.rs"),
        include_str!("../src/storage/filesystem.rs"),
        include_str!("../src/storage/test_support.rs"),
    ] {
        for old in [
            "fn submit_write(",
            "fn submit_write_with_headers(",
            "fn submit_delete(",
            "fn submit_delete_with_headers(",
            "fn submit_head(",
            "fn submit_range_head(",
            "fn submit_read_range(",
            "fn submit_read_with_metadata(",
        ] {
            assert!(
                !source.contains(old),
                "legacy storage method remains: {old}"
            );
        }
    }
    for source in [
        include_str!("../src/storage/hybrid/backend/proofs.rs"),
        include_str!("../src/storage/hybrid/backend/uploads.rs"),
    ] {
        assert!(!source.contains("\"If-Match\""));
        assert!(!source.contains("\"If-None-Match\""));
    }
    methods.len()
}

#[test]
fn should_require_only_typed_storage_backend_methods_in_every_build_mode() {
    // Arrange
    let required_method_count = 6;

    // Act
    let actual_method_count = assert_typed_storage_backend_contract();

    // Assert
    assert_eq!(actual_method_count, required_method_count);
}

#[test]
fn should_keep_conditional_http_headers_at_the_cloud_boundary() {
    // Arrange
    let callers = [
        include_str!("../src/runtime/hybrid_persistence/metadata_snapshot.rs"),
        include_str!("../src/runtime/cloud_startup/cloud_recovery/metadata.rs"),
        include_str!("../src/storage/hybrid/backend/proofs.rs"),
    ];

    // Act
    let raw_header_count = callers
        .iter()
        .flat_map(|source| {
            [
                "\"If-Match\"",
                "\"If-None-Match\"",
                "\"x-goog-if-generation-match\"",
            ]
            .map(|header| source.matches(header).count())
        })
        .sum::<usize>();

    // Assert
    assert_eq!(raw_header_count, 0);
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
                    .replace(std::path::MAIN_SEPARATOR, "/");
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
    let owner_source = include_str!("../src/runtime/state.rs");
    let production_sources = [
        include_str!("../src/runtime/state/manifest.rs"),
        include_str!("../src/runtime/actors/manifest.rs"),
        include_str!("../src/runtime/ddl.rs"),
        include_str!("../src/runtime/cloud_startup/cloud_recovery/mod.rs"),
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
        !owner_source.contains("impl std::ops::DerefMut for ManifestRuntimeState"),
        "mutable dereferencing would allow file-set edits without an explicit owner method"
    );
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

#[test]
fn should_mutate_cloud_lifecycle_state_through_coordinator_methods() {
    // Arrange
    let event_loop = include_str!("../src/runtime/event_loop/mod.rs");
    let scheduler = include_str!("../src/runtime/event_loop/scheduler.rs");
    let retention = include_str!("../src/runtime/event_loop/wal_retention/cloud.rs");

    // Act
    let direct_writes = [
        event_loop.contains("cloud_coordinator.hybrid_storage ="),
        scheduler.contains("cloud_coordinator.hybrid_storage_events ="),
        retention.contains("cloud_coordinator.cloud_wal_prune_worker ="),
        retention.contains("cloud_coordinator.cloud_wal_prune_worker.take()"),
    ];

    // Assert
    assert!(!direct_writes.into_iter().any(|write| write));
}

#[test]
fn should_register_background_maintenance_with_retry_deadlines() {
    // Arrange
    let event_loop = include_str!("../src/runtime/event_loop/mod.rs");
    let compaction = include_str!("../src/runtime/event_loop/compaction.rs");
    let scheduler = include_str!("../src/runtime/event_loop/scheduler.rs");

    // Act
    let uses_raw_timer = event_loop.contains("next_background_compaction_check")
        || compaction.contains("next_background_compaction_check");

    // Assert
    assert!(!uses_raw_timer);
    assert!(event_loop
        .contains("background_compaction_schedule: crate::runtime::retry_schedule::RetrySchedule"));
    assert!(scheduler.contains("Some(self.background_maintenance_timeout())"));
}

#[test]
fn should_bound_legacy_sst_backfill_work_when_persisting() {
    // Arrange
    let read_path = include_str!("../src/runtime/event_loop/read_path.rs");

    // Act
    let uses_worker = read_path.contains("midge-legacy-sst-bound-backfill");
    let batches_edits = read_path
        .contains(".append_batch_for(crate::metadata::accounting::Origin::Administration, &edits)");

    // Assert
    assert!(uses_worker);
    assert!(batches_edits);
}

/// Source scanning shared by the #808 WAL-authority rules: production code is
/// every `src/` file outside test-only modules, minus items gated on `test`,
/// `internal-testing` or `failpoints`.
mod source_scan {
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    pub(crate) struct SourceFile {
        pub(crate) relative: String,
        pub(crate) production: String,
    }

    fn src_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
    }

    fn relative(path: &Path) -> String {
        path.strip_prefix(Path::new(env!("CARGO_MANIFEST_DIR")))
            .expect("source below manifest")
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/")
    }

    /// Split a `cfg` predicate list at its top-level commas.
    fn predicate_list(list: &str) -> Vec<&str> {
        let mut parts = Vec::new();
        let (mut depth, mut start) = (0usize, 0);
        for (index, character) in list.char_indices() {
            match character {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    parts.push(list[start..index].trim());
                    start = index + 1;
                }
                _ => {}
            }
        }
        parts.push(list[start..].trim());
        parts.retain(|part| !part.is_empty());
        parts
    }

    /// Whether a `cfg` predicate holds only in test builds: `test`, or the
    /// `internal-testing`/`failpoints` features.
    fn test_only(predicate: &str) -> bool {
        let predicate = predicate.trim();
        let inner = |prefix: &str| {
            predicate
                .strip_prefix(prefix)
                .and_then(|tail| tail.strip_suffix(')'))
                .map(predicate_list)
        };
        if predicate == "test" {
            true
        } else if let Some(feature) = predicate
            .strip_prefix("feature = \"")
            .and_then(|tail| tail.strip_suffix('"'))
        {
            matches!(feature, "internal-testing" | "failpoints")
        } else if let Some(parts) = inner("all(") {
            parts.into_iter().any(test_only)
        } else if let Some(parts) = inner("any(") {
            !parts.is_empty() && parts.into_iter().all(test_only)
        } else {
            false
        }
    }

    /// Whether `line` opens a `cfg` attribute whose item compiles only in
    /// test builds.
    fn is_gate(line: &str) -> bool {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("#[cfg(") {
            return false;
        }
        let end = attribute_end(trimmed);
        trimmed[..end]
            .strip_prefix("#[cfg(")
            .and_then(|tail| tail.strip_suffix(")]"))
            .is_some_and(test_only)
    }

    /// Byte offset just past the `cfg` attribute that starts `line`.
    fn attribute_end(line: &str) -> usize {
        let mut depth = 0usize;
        for (index, character) in line.char_indices() {
            match character {
                '[' | '(' => depth += 1,
                ']' | ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return index + 1;
                    }
                }
                _ => {}
            }
        }
        line.len()
    }

    /// Byte length of the item starting at `text`: through its matching `}`,
    /// or through the first top-level `;` when that comes first.
    fn item_len(text: &str) -> usize {
        let bytes = text.as_bytes();
        let mut depth = 0usize;
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'/' if bytes.get(index + 1) == Some(&b'/') => {
                    while index < bytes.len() && bytes[index] != b'\n' {
                        index += 1;
                    }
                }
                b'/' if bytes.get(index + 1) == Some(&b'*') => {
                    let mut nesting = 0usize;
                    while index < bytes.len() {
                        if bytes[index..].starts_with(b"/*") {
                            nesting += 1;
                            index += 2;
                        } else if bytes[index..].starts_with(b"*/") {
                            nesting -= 1;
                            index += 2;
                            if nesting == 0 {
                                break;
                            }
                        } else {
                            index += 1;
                        }
                    }
                    continue;
                }
                b'r' if matches!(bytes.get(index + 1), Some(b'"' | b'#'))
                    && (index == 0 || !bytes[index - 1].is_ascii_alphanumeric()) =>
                {
                    let hashes = bytes[index + 1..]
                        .iter()
                        .take_while(|byte| **byte == b'#')
                        .count();
                    let open = index + 1 + hashes;
                    if bytes.get(open) == Some(&b'"') {
                        let mut closing = b"\"".to_vec();
                        closing.extend(std::iter::repeat_n(b'#', hashes));
                        index = text[open + 1..]
                            .find(std::str::from_utf8(&closing).expect("ASCII delimiter"))
                            .map_or(bytes.len(), |end| open + 1 + end + closing.len());
                        continue;
                    }
                }
                b'"' => {
                    index += 1;
                    while index < bytes.len() && bytes[index] != b'"' {
                        if bytes[index] == b'\\' {
                            index += 1;
                        }
                        index += 1;
                    }
                }
                b'\'' if bytes.get(index + 2) == Some(&b'\'') => index += 2,
                b'{' => depth += 1,
                b'}' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return index + 1;
                    }
                }
                b';' if depth == 0 => return index + 1,
                _ => {}
            }
            index += 1;
        }
        bytes.len()
    }

    /// Remove gated items, returning the remaining text and the names of
    /// gated `mod name;` declarations.
    enum GatedModule {
        Named(String),
        Path(String),
    }

    fn strip_gated(source: &str) -> (String, Vec<GatedModule>) {
        let mut production = String::with_capacity(source.len());
        let mut gated_modules = Vec::new();
        let mut rest = source;
        while !rest.is_empty() {
            let line_end = rest.find('\n').map_or(rest.len(), |end| end + 1);
            let line = &rest[..line_end];
            if !is_gate(line) {
                production.push_str(line);
                rest = &rest[line_end..];
                continue;
            }
            let after_attribute = attribute_end(line);
            let mut path_attribute = None;
            if line[after_attribute..].trim().is_empty() {
                rest = &rest[line_end..];
            } else {
                // The gated item starts on the attribute's own line.
                rest = &rest[after_attribute..];
            }
            // Further attributes belong to the same gated item.
            while rest.trim_start().starts_with("#[") {
                let end = rest.find('\n').map_or(rest.len(), |end| end + 1);
                if let Some(path) = rest[..end]
                    .trim()
                    .strip_prefix("#[path = \"")
                    .and_then(|tail| tail.strip_suffix("\"]"))
                {
                    path_attribute = Some(path.to_string());
                }
                rest = &rest[end..];
            }
            let len = item_len(rest);
            let item = rest[..len].trim();
            let declaration = item
                .trim_start_matches("pub(crate) ")
                .trim_start_matches("pub(super) ")
                .trim_start_matches("pub ");
            if let Some(name) = declaration.strip_prefix("mod ").map(|tail| {
                tail.split(|character: char| !(character.is_alphanumeric() || character == '_'))
                    .next()
                    .unwrap_or_default()
            }) {
                gated_modules.push(path_attribute.map_or_else(
                    || GatedModule::Named(name.trim().to_string()),
                    GatedModule::Path,
                ));
            }
            rest = &rest[len..];
        }
        (production, gated_modules)
    }

    fn module_dir(file: &Path) -> PathBuf {
        let parent = file.parent().expect("source directory").to_path_buf();
        match file.file_name().and_then(|name| name.to_str()) {
            Some("mod.rs" | "lib.rs" | "main.rs") => parent,
            _ => parent.join(file.file_stem().expect("source stem")),
        }
    }

    /// Every production `src/` file with its production-only text.
    pub(crate) fn production_files() -> Vec<SourceFile> {
        let mut files = Vec::new();
        let mut excluded: BTreeSet<PathBuf> = BTreeSet::new();
        let mut pending = vec![src_root()];
        let mut all = Vec::new();
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(&directory).expect("list sources") {
                let path = entry.expect("source entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    all.push(path);
                }
            }
        }
        all.sort();
        let mut stripped = Vec::new();
        for path in all {
            let source = std::fs::read_to_string(&path).expect("read source");
            let (production, gated) = strip_gated(&source);
            let directory = module_dir(&path);
            for module in gated {
                match module {
                    GatedModule::Named(name) => {
                        excluded.insert(directory.join(format!("{name}.rs")));
                        excluded.insert(directory.join(&name));
                    }
                    GatedModule::Path(relative) => {
                        let parent = path.parent().expect("source directory");
                        excluded.insert(parent.join(&relative));
                        excluded.insert(parent.join(relative.trim_end_matches(".rs")));
                    }
                }
            }
            stripped.push((path, production));
        }
        for (path, production) in stripped {
            if path.starts_with(src_root().join("bin"))
                || excluded
                    .iter()
                    .any(|gated| path == *gated || path.starts_with(gated))
            {
                continue;
            }
            files.push(SourceFile {
                relative: relative(&path),
                production,
            });
        }
        files
    }
}

/// Calls that reach host storage without the injected database root.
const HOST_IO: &[&str] = &[
    "std::fs",
    "tokio::fs",
    "RealFs::new(",
    "RealFs::open_existing(",
    "tempfile::",
    ".exists()",
    ".try_exists(",
    ".is_file()",
    ".is_dir()",
    ".read_dir()",
    ".metadata()",
    ".symlink_metadata()",
    ".canonicalize()",
    "FileSystem::new(",
    "FileSystem::open_existing(",
];

/// Whether `source` imports `std::fs` through a grouped `use std::{..}`.
fn imports_std_fs_in_group(source: &str) -> bool {
    source.match_indices("use std::{").any(|(start, _)| {
        let group = &source[start..];
        let group = &group[..group.find('}').map_or(group.len(), |end| end + 1)];
        group
            .split(|character: char| !(character.is_alphanumeric() || character == '_'))
            .any(|word| word == "fs")
    })
}

/// Code that is the host boundary itself, or reads host state that is not
/// database storage. Each entry says why it may touch the host directly.
const HOST_BOUNDARY: &[(&str, &str)] = &[
    ("src/io/real.rs", "the production Fs adapter"),
    (
        "src/io/durable_dir.rs",
        "host directory creation behind RealFs roots",
    ),
    (
        "src/engine/startup/storage_root.rs",
        "the single constructor of the engine's host-backed root",
    ),
    (
        "src/engine/api/options/memory.rs",
        "reads cgroup memory limits",
    ),
    (
        "src/compaction/repair.rs",
        "resolves the host mount for free-space checks",
    ),
    (
        "src/storage/providers/azure.rs",
        "reads workload-identity token files",
    ),
    (
        "src/storage/providers/gcs.rs",
        "reads service-account credential files",
    ),
    (
        "src/storage/providers/s3.rs",
        "reads web-identity and profile files",
    ),
    (
        "src/wal/recovery/streaming.rs",
        "anonymous delete-on-close replay spool holding no database state; #808 stage 6 deletes it",
    ),
    (
        "src/storage/filesystem.rs",
        "the host-directory object store: inode identity, flock and hard-link publication",
    ),
    (
        "src/storage/filesystem/windows.rs",
        "Windows range reads for the host-directory object store",
    ),
    (
        "src/storage/simulated.rs",
        "places a simulated cloud in the root's host directory unless a test injects a store",
    ),
    (
        "src/engine/backup.rs",
        "captures a host database directory by file handle and restores into one; \
         a root or store without a host directory is refused",
    ),
    (
        "src/engine/backup/paths.rs",
        "resolves and publishes user-chosen backup and restore directories",
    ),
];

/// Production code reaches durable storage only through the injected `Fs`
/// root, so a crash-simulating filesystem sees every effect (#808 stage 1).
#[test]
fn should_route_storage_io_through_injected_fs() {
    // Arrange
    let files = source_scan::production_files();

    // Act
    let mut violations = std::collections::BTreeMap::new();
    for file in files {
        let hits: Vec<_> = file
            .production
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .filter(|line| HOST_IO.iter().any(|token| line.contains(token)))
            .map(str::trim)
            .map(str::to_string)
            .collect();
        let mut hits = hits;
        if imports_std_fs_in_group(&file.production) {
            hits.push("use std::{.., fs, ..}".to_string());
        }
        if !hits.is_empty() {
            violations.insert(file.relative, hits);
        }
    }

    // Assert
    let unexpected: Vec<_> = violations
        .iter()
        .filter(|(file, _)| !HOST_BOUNDARY.iter().any(|(allowed, _)| allowed == file))
        .collect();
    assert!(
        unexpected.is_empty(),
        "production code bypasses the injected filesystem root: {unexpected:#?}"
    );
    let stale_boundary: Vec<_> = HOST_BOUNDARY
        .iter()
        .filter(|(file, _)| !violations.contains_key(*file))
        .map(|(file, _)| file)
        .collect();
    assert!(
        stale_boundary.is_empty(),
        "remove files that no longer touch the host from HOST_BOUNDARY: {stale_boundary:?}"
    );
}

/// The crash simulator is test support: its module compiles only for tests
/// and `internal-testing`, and production code never reaches it (#817).
#[test]
fn should_keep_simulation_out_of_production_builds() {
    // Arrange
    let files = source_scan::production_files();

    // Act
    let compiled: Vec<_> = files
        .iter()
        .filter(|file| file.relative.starts_with("src/simulation/"))
        .map(|file| file.relative.clone())
        .collect();
    let reaching: Vec<_> = files
        .iter()
        .filter(|file| file.production.contains("simulation::"))
        .map(|file| file.relative.clone())
        .collect();

    // Assert
    assert!(
        compiled.is_empty(),
        "simulation compiles into production builds: {compiled:?}"
    );
    assert!(
        reaching.is_empty(),
        "production code reaches the simulator: {reaching:?}"
    );
}

/// Code the #808 redesign deleted. Each entry names the issue that removed
/// it; nothing in the crate, its tests, benches, examples or fuzz targets may
/// bring it back. Entries are whole identifiers or path segments.
const DELETED_BY_WAL_REDESIGN: &[(&str, &str)] = &[
    ("hybrid_local", "#827"),
    ("hybrid-local", "#827"),
    ("retire_legacy_local_store", "#827"),
    ("local_store_if_active", "#827"),
    ("evict_local_object_cache", "#827"),
    ("local_object_cache_is_absent", "#827"),
    ("build_with_local_fs", "#816"),
    ("build_with_local_fs_within", "#816"),
    ("recovery_staging_fs", "#816"),
    ("open_with_real_fs", "#814"),
    ("summarize_with_real_fs", "#814"),
    ("summarize_with_real_fs_for_compaction", "#814"),
    ("scan_sst_dir", "#814"),
    ("relative_anchor", "#814"),
    ("reject_simulated_cloud_wal_without_catalog", "#825"),
    ("reject_cloud_wal_without_catalog_within", "#825"),
    ("ensure_local_sst_cache_from_cloud", "#825"),
    ("ensure_local_sst_cache_from_cloud_storage", "#825"),
    ("local_sst_is_definitively_missing", "#825"),
    ("verify_storage_path_with_sst_fs", "#825"),
    ("recovery_cloud_wal_dir", "#825"),
];

/// Whether `source` holds `token` as a whole identifier, not as part of a
/// longer one.
fn mentions_identifier(source: &str, token: &str) -> bool {
    let is_identifier = |character: char| character.is_alphanumeric() || character == '_';
    source.match_indices(token).any(|(start, _)| {
        let before = source[..start].chars().next_back();
        let after = source[start + token.len()..].chars().next();
        !before.is_some_and(is_identifier) && !after.is_some_and(is_identifier)
    })
}

#[test]
fn should_match_whole_identifiers_when_scanning_for_deleted_code() {
    // Arrange
    let source = "let x = rescan_sst_dirs(); let path = \"hybrid_local/sst\";";

    // Act
    let partial = mentions_identifier(source, "scan_sst_dir");
    let segment = mentions_identifier(source, "hybrid_local");

    // Assert
    assert!(!partial);
    assert!(segment);
}

#[test]
fn should_not_reintroduce_code_deleted_by_wal_redesign() {
    // Arrange
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut pending: Vec<_> = ["src", "tests", "benches", "examples", "fuzz"]
        .iter()
        .map(|directory| root.join(directory))
        .filter(|directory| directory.is_dir())
        .collect();
    let mut sources = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("list sources") {
            let path = entry.expect("source entry").path();
            if path.is_dir() {
                if !path.ends_with("target") {
                    pending.push(path);
                }
            } else if path.extension().is_some_and(|extension| extension == "rs")
                && !path.ends_with("tests/solid_governance.rs")
            {
                sources.push(path);
            }
        }
    }

    // Act
    let mut reintroduced = Vec::new();
    for path in sources {
        let source = std::fs::read_to_string(&path).expect("read source");
        for (token, issue) in DELETED_BY_WAL_REDESIGN {
            if mentions_identifier(&source, token) {
                reintroduced.push(format!(
                    "{} uses `{token}`, deleted by {issue}",
                    path.strip_prefix(root).unwrap_or(&path).display()
                ));
            }
        }
    }

    // Assert
    assert!(reintroduced.is_empty(), "{reintroduced:#?}");
}
