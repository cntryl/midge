//! Repository Governance Tests
//!
//! Consolidated from: `repository_gates.rs`, `testing_governance.rs`, `coverage_manifests.rs`, `architecture_ladder.rs`, `external_adopter_smoke.rs`, `failpoints_contract.rs`

mod common;

mod coverage_manifests {
    //! Compile-enforced manifests for enum-shaped behavior axes.
    //! Internal `FsError` coverage lives in `src/io/traits.rs` unit tests because the
    //! filesystem module is intentionally private to library consumers.

    use cntryl_midge::__internal::codec::{CompressionAlgo, CompressionPolicy};
    use cntryl_midge::{
        AzureCredentialSource, DurabilityPolicy, Engine, GcsCredentialSource,
        HybridStorageBudgetSnapshot, LocalStorageUsage, MidgeError, OpenOptions, RecoveryPolicy,
        S3CredentialSource, StorageAdmissionBlock, StorageAdmissionKind, StorageAdmissionReason,
        TransactionMode,
    };
    use std::time::Duration;

    fn s3_coverage(source: &S3CredentialSource) -> &'static str {
        match source {
            S3CredentialSource::Static { .. } => "provider request/qualification tests",
            S3CredentialSource::Environment => "configuration resolution tests",
            S3CredentialSource::SharedProfile { .. } => "profile parsing tests; real AWS scheduled",
            S3CredentialSource::AwsDefaultChain => "chain unit tests; real AWS scheduled",
        }
    }

    fn azure_coverage(source: &AzureCredentialSource) -> &'static str {
        match source {
            AzureCredentialSource::SharedKey { .. } => "Sqrzl qualification",
            AzureCredentialSource::SasToken { .. } => "request signing tests",
            AzureCredentialSource::ConnectionString { .. } => "configuration tests",
            AzureCredentialSource::StorageEnvironment => "environment resolution tests",
            AzureCredentialSource::EnvironmentClientSecret => "client-secret identity tests",
            AzureCredentialSource::WorkloadIdentity { .. } => "workload-identity tests",
            AzureCredentialSource::ManagedIdentity { .. } => "managed-identity tests",
            AzureCredentialSource::LightweightDefaultChain => {
                "chain unit tests; real Azure scheduled"
            }
        }
    }

    fn gcs_coverage(source: &GcsCredentialSource) -> &'static str {
        match source {
            GcsCredentialSource::BearerToken { .. } => "JSON request tests",
            GcsCredentialSource::HmacKey { .. } => "Sqrzl XML qualification",
            GcsCredentialSource::ApplicationDefault => "ADC unit tests; real GCS scheduled",
            GcsCredentialSource::ServiceAccountJsonFile { .. } => "service-account parsing tests",
            GcsCredentialSource::AuthorizedUserJsonFile { .. } => "authorized-user parsing tests",
            GcsCredentialSource::MetadataServer => "metadata transport tests",
        }
    }

    fn compression_coverage(algorithm: CompressionAlgo) -> &'static str {
        match algorithm {
            CompressionAlgo::None => "V4 raw-block roundtrip and integrity verification",
            CompressionAlgo::Lz4 => "V4 LZ4 roundtrip and adaptive selection",
            CompressionAlgo::Zstd3 => "V4 Zstd level-3 roundtrip and adaptive selection",
            CompressionAlgo::Zstd9 => "V4 Zstd level-9 roundtrip and adaptive selection",
        }
    }

    fn compression_policy_coverage(policy: &CompressionPolicy) -> &'static str {
        match policy {
            CompressionPolicy::None => "uncompressed production roundtrip",
            CompressionPolicy::Fixed(_) => "fixed-policy production roundtrip",
            CompressionPolicy::Adaptive { .. } => "adaptive production roundtrip",
        }
    }

    fn recovery_coverage(policy: RecoveryPolicy) -> &'static str {
        match policy {
            RecoveryPolicy::Strict => "strict corruption and recovery suites",
            RecoveryPolicy::Salvage => "salvage-prefix and degraded-health suites",
        }
    }

    fn durability_coverage(policy: DurabilityPolicy) -> &'static str {
        match policy {
            DurabilityPolicy::Sync => "local sync durability suites",
            DurabilityPolicy::Buffered => "local buffered recovery suites",
            DurabilityPolicy::BestEffort => "best-effort loss/flush suites",
            DurabilityPolicy::CloudAsync => "cloud async recovery suites",
            DurabilityPolicy::CloudStrict => "cloud strict qualification suites",
        }
    }

    fn storage_admission_kind_coverage(kind: StorageAdmissionKind) -> &'static str {
        match kind {
            StorageAdmissionKind::Wal => "cloud WAL admission and rollback accounting tests",
            StorageAdmissionKind::TransactionSpill => "public rejected-spill diagnostic snapshot",
            StorageAdmissionKind::Flush => "flush admission and publication reservation tests",
            StorageAdmissionKind::Compaction => "compaction admission and scratch cleanup tests",
            StorageAdmissionKind::FlushHeadroom => "shared reusable flush headroom tests",
            StorageAdmissionKind::StartupResidue => "startup residue reconciliation tests",
        }
    }

    fn storage_admission_reason_coverage(reason: StorageAdmissionReason) -> &'static str {
        match reason {
            StorageAdmissionReason::LocalCapacity => "public oversized-spill admission rejection",
            StorageAdmissionReason::CloudUpload => {
                "cloud upload pressure and admission history tests"
            }
            StorageAdmissionReason::Compaction => "high-watermark compaction pressure tests",
        }
    }

    #[test]
    fn should_keep_coverage_manifest_exhaustive_given_public_storage_admission_axes() {
        // Arrange
        let operations = [
            StorageAdmissionKind::Wal,
            StorageAdmissionKind::TransactionSpill,
            StorageAdmissionKind::Flush,
            StorageAdmissionKind::Compaction,
            StorageAdmissionKind::FlushHeadroom,
            StorageAdmissionKind::StartupResidue,
        ];
        let reasons = [
            StorageAdmissionReason::LocalCapacity,
            StorageAdmissionReason::CloudUpload,
            StorageAdmissionReason::Compaction,
        ];

        // Act
        let operations = operations.map(storage_admission_kind_coverage);
        let reasons = reasons.map(storage_admission_reason_coverage);

        // Assert
        for axis in [&operations[..], &reasons[..]] {
            assert!(axis.iter().all(|note| !note.is_empty()));
            let unique: std::collections::HashSet<_> = axis.iter().collect();
            assert_eq!(
                unique.len(),
                axis.len(),
                "distinct coverage notes: {axis:?}"
            );
        }
    }

    #[test]
    fn should_expose_typed_storage_pressure_when_transaction_spill_exceeds_local_capacity() {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let local_budget = 1024 * 1024;
        let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "typed-metrics")
            .local_storage_budget(local_budget)
            .transaction_memory_pool_size(8 * 1024)
            .background_compaction(false)
            .build()
            .expect("options");
        let mut engine = Engine::open(options).expect("engine");
        let cf = engine.create_column_family("data").expect("column family");
        let mut transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("transaction");

        // Act
        let result = transaction.put(b"oversized".to_vec(), vec![1; 2 * 1024 * 1024], None);
        let snapshot = engine.get_runtime_metrics().expect("runtime metrics");
        let storage: HybridStorageBudgetSnapshot =
            snapshot.local_storage.expect("cloud disk budget");
        let usage: LocalStorageUsage = storage.usage;
        let pressure: StorageAdmissionBlock =
            storage.blocked_admission.expect("rejected admission");
        drop(transaction);
        engine.shutdown(Duration::from_secs(10)).expect("shutdown");

        // Assert
        assert!(matches!(result, Err(MidgeError::NoSpace(_))));
        assert_eq!(pressure.operation, StorageAdmissionKind::TransactionSpill);
        assert_eq!(pressure.reason, StorageAdmissionReason::LocalCapacity);
        assert!(pressure.requested_bytes > pressure.free_bytes_at_rejection);
        assert!(pressure.attempts > 0);
        assert!(storage.admission_rejections_total >= pressure.attempts);
        assert_eq!(storage.max_local_bytes, local_budget);
        assert_eq!(
            storage.total_committed_bytes,
            usage.wal_bytes
                + usage.transaction_spill_bytes
                + usage.resident_sst_bytes
                + usage.startup_residue_bytes
                + usage.flush_staging_reserved_bytes
                + usage.flush_headroom_reserved_bytes
                + usage.compaction_staging_reserved_bytes
                + usage.wal_headroom_reserved_bytes
        );
        assert_eq!(usage.transaction_spill_bytes, 0, "failed work owns no disk");
        assert_eq!(
            storage.free_bytes,
            local_budget.saturating_sub(storage.total_committed_bytes)
        );
    }

    #[test]
    fn should_keep_coverage_manifest_exhaustive_given_public_behavior_axes() {
        // Arrange
        // The real exhaustiveness guarantee comes from the non-wildcard `match`
        // arms in the `*_coverage` functions above: adding a new enum variant
        // without updating them fails to *compile*, not merely to pass this test.
        //
        // What this test adds at runtime is a check those compile-time-exhaustive
        // functions can't make for themselves: that every variant's description is
        // non-empty *and* distinct within its axis, catching a copy-pasted
        // coverage note left over from an adjacent match arm.
        let recovery = [
            recovery_coverage(RecoveryPolicy::Strict),
            recovery_coverage(RecoveryPolicy::Salvage),
        ];
        let durability = [
            durability_coverage(DurabilityPolicy::Sync),
            durability_coverage(DurabilityPolicy::Buffered),
            durability_coverage(DurabilityPolicy::BestEffort),
            durability_coverage(DurabilityPolicy::CloudAsync),
            durability_coverage(DurabilityPolicy::CloudStrict),
        ];
        let compression = [
            compression_coverage(CompressionAlgo::None),
            compression_coverage(CompressionAlgo::Lz4),
            compression_coverage(CompressionAlgo::Zstd3),
            compression_coverage(CompressionAlgo::Zstd9),
        ];
        let compression_policy = [
            compression_policy_coverage(&CompressionPolicy::None),
            compression_policy_coverage(&CompressionPolicy::Fixed(CompressionAlgo::Lz4)),
            compression_policy_coverage(&CompressionPolicy::Adaptive {
                min_savings_bytes: 64,
                min_ratio: 0.1,
                check_algorithms: vec![CompressionAlgo::Zstd3],
            }),
        ];
        let azure = [
            azure_coverage(&AzureCredentialSource::default_chain()),
            azure_coverage(&AzureCredentialSource::StorageEnvironment),
            azure_coverage(&AzureCredentialSource::EnvironmentClientSecret),
        ];
        let gcs = [
            gcs_coverage(&GcsCredentialSource::application_default()),
            gcs_coverage(&GcsCredentialSource::MetadataServer),
        ];
        let s3 = [
            s3_coverage(&S3CredentialSource::environment()),
            s3_coverage(&S3CredentialSource::AwsDefaultChain),
        ];

        // Act
        let axes = [
            &recovery[..],
            &durability[..],
            &compression[..],
            &compression_policy[..],
            &azure[..],
            &gcs[..],
            &s3[..],
        ];

        // Assert
        for axis in axes {
            assert!(
                axis.iter().all(|entry| !entry.is_empty()),
                "every variant must carry a non-empty coverage note: {axis:?}"
            );
            let mut unique = axis.to_vec();
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(
                unique.len(),
                axis.len(),
                "coverage notes within one axis must be distinct per variant, found a duplicate in {axis:?}"
            );
        }
    }
}

mod architecture_ladder {
    use std::path::{Path, PathBuf};

    fn rust_sources_under(relative: &str) -> Vec<PathBuf> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
        if root.is_file() {
            return vec![root];
        }
        let mut pending = vec![root];
        let mut sources = Vec::new();
        while let Some(path) = pending.pop() {
            for entry in std::fs::read_dir(path).expect("read source directory") {
                let path = entry.expect("read source entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    sources.push(path);
                }
            }
        }
        sources
    }

    #[test]
    fn should_load_metadata_strictly_when_outside_startup_recovery() {
        // Salvage loads rewrite the journal. Only startup may do that, because
        // only startup records salvage mode, which turns the orphan sweep into
        // a quarantine instead of a delete.
        // Arrange
        let startup = Path::new("src/runtime/state/recovery.rs");
        let mut offenders = Vec::new();

        // Act
        for path in rust_sources_under("src") {
            let relative = path
                .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                .expect("source under manifest dir");
            let name = relative.to_string_lossy();
            if relative == startup || name.contains("tests") {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("read source");
            let source = source.split("#[cfg(test)]\nmod tests").next().unwrap_or("");
            for (index, _) in source.match_indices("load_with_fs_and_policy(") {
                let window: String = source[index..].chars().take(160).collect();
                let definition = source[..index].ends_with("fn ");
                if !definition && !window.contains("RecoveryPolicy::Strict") {
                    offenders.push(name.to_string());
                }
            }
        }
        // Assert
        assert!(
            offenders.is_empty(),
            "metadata loads outside startup recovery must be Strict: {offenders:?}"
        );
    }

    #[test]
    fn should_classify_lease_errors_by_type_when_validating_writer_authority() {
        // Lease errors carry their class as a variant; classifying them by
        // message text drifts as soon as a provider words an error differently.
        // Arrange
        let mut offenders = Vec::new();

        // Act
        for root in ["src/lease", "src/runtime", "src/engine/startup"] {
            for path in rust_sources_under(root) {
                let relative = path
                    .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                    .expect("source under manifest dir");
                let name = relative.to_string_lossy();
                if name.contains("tests") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("read source");
                let source = source.split("#[cfg(test)]\nmod tests").next().unwrap_or("");
                if source.contains("contains(\"timed out\")") {
                    offenders.push(name.to_string());
                }
            }
        }

        // Assert
        assert!(
            offenders.is_empty(),
            "classify lease errors by variant, not message text: {offenders:?}"
        );
    }

    #[test]
    fn should_classify_errors_by_kind_when_mapping_storage_failures() {
        // Error classes travel as variants; guessing them back from message
        // text breaks on another platform's wording or a reworded Display.
        // Arrange
        // The WAL writer's own copies are #349.
        let allowed = ["src/wal/fs/writer_io.rs", "src/wal/fs/writer_runner.rs"];
        let needles = [
            "contains(\"no space\")",
            "contains(\"disk full\")",
            "contains(\"Resource limit:\")",
            "contains(\"No such file\")",
            "contains(\"no such file\")",
            "contains(\"not found\")",
        ];
        let mut offenders = Vec::new();

        // Act
        for path in rust_sources_under("src") {
            let relative = path
                .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                .expect("source under manifest dir");
            let name = relative.to_string_lossy();
            if name.contains("tests") || allowed.contains(&name.as_ref()) {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("read source");
            let source = source.split("#[cfg(test)]\nmod tests").next().unwrap_or("");
            if needles.iter().any(|needle| source.contains(needle)) {
                offenders.push(name.to_string());
            }
        }

        // Assert
        assert!(
            offenders.is_empty(),
            "classify errors by kind, not message text: {offenders:?}"
        );
    }

    #[test]
    fn should_import_config_types_directly_when_storage_needs_them() {
        // Arrange
        let source = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage/providers/mod.rs"),
        )
        .expect("read storage/providers/mod.rs");

        // Act
        let reexports: Vec<&str> = source
            .lines()
            .map(str::trim)
            .filter(|line| {
                line.starts_with("pub(crate) use crate::config")
                    || line.starts_with("pub use crate::config")
            })
            .collect();

        // Assert
        assert!(
            reexports.is_empty(),
            "storage must import config types where it uses them, not re-export them as if \
             it owned them: {reexports:?}"
        );
    }

    #[test]
    fn should_keep_metadata_object_naming_out_of_the_storage_layer() {
        // Arrange
        let needle = "fn cloud_metadata_key";

        // Act
        let offenders: Vec<PathBuf> = rust_sources_under("src/storage")
            .into_iter()
            .filter(|path| {
                std::fs::read_to_string(path)
                    .expect("read Rust source")
                    .contains(needle)
            })
            .collect();

        // Assert
        assert!(
            offenders.is_empty(),
            "object naming for recovery metadata belongs to CloudObjectLayout, not the storage \
             layer: {offenders:?}"
        );
    }

    const UNFLUSHED_DATA_PRESENT_CONSTRUCTION: &str = "UnflushedDataPresent {";

    #[test]
    fn should_construct_the_unflushed_discard_licence_in_only_the_active_memtable_check() {
        // Arrange: MidgeError::UnflushedDataPresent is a permission to throw
        // committed data away. Busy used to carry that meaning implicitly,
        // which let four unrelated producers forge it; a second construction
        // site is a second forgery, so the count is the invariant.
        let needle = UNFLUSHED_DATA_PRESENT_CONSTRUCTION;
        let classifier = Path::new("src").join("common").join("error.rs");
        let expected = Path::new("src").join("runtime").join("ddl.rs");

        // Act
        let producers: Vec<PathBuf> = rust_sources_under("src")
            .into_iter()
            .filter(|path| !path.ends_with(&classifier))
            .filter(|path| production_source(path).contains(needle))
            .collect();

        // Assert
        assert_eq!(
            producers.len(),
            1,
            "only the active-memtable check may emit the discard licence: {producers:?}"
        );
        assert!(
            producers[0].ends_with(&expected),
            "the discard licence moved out of runtime/ddl.rs: {producers:?}"
        );
    }

    #[test]
    fn should_define_shared_provider_helpers_once_when_providers_need_them() {
        // Arrange
        let shared = ["fn current_unix_secs(", "fn object_metadata_from_"];
        let sources = rust_sources_under("src/storage/providers");

        // Act
        let duplicated: Vec<(&str, usize)> = shared
            .into_iter()
            .map(|needle| {
                let definitions = sources
                    .iter()
                    .map(|path| {
                        std::fs::read_to_string(path)
                            .expect("read Rust source")
                            .matches(needle)
                            .count()
                    })
                    .sum::<usize>();
                (needle, definitions)
            })
            .filter(|(_, definitions)| *definitions > 1)
            .collect();

        // Assert
        assert!(
            duplicated.is_empty(),
            "provider helpers copied into more than one provider: {duplicated:?}"
        );
    }

    #[test]
    fn should_keep_cloud_adapter_callback_waits_in_one_helper() {
        // Arrange
        let source = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage/cloud/adapter.rs"),
        )
        .expect("read storage/cloud/adapter.rs");
        // One in the shared helper and one in the proof path, which reports its
        // own messages.
        let allowed = 2;

        // Act
        let disconnect_arms = source.matches("RecvTimeoutError::Disconnected").count();

        // Assert
        assert!(
            disconnect_arms <= allowed,
            "{disconnect_arms} hand-written callback wait arms in storage/cloud/adapter.rs \
             (allowed {allowed}): route adapter waits through await_cloud_event"
        );
    }

    #[test]
    fn should_keep_cloud_boundary_runtime_publication_owners_separate() {
        // Arrange
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let module = std::fs::read_to_string(root.join("src/storage/cloud/mod.rs"))
            .expect("read storage/cloud/mod.rs");
        let backend = std::fs::read_to_string(root.join("src/storage/cloud/backend.rs"))
            .expect("read storage/cloud/backend.rs");
        let dispatcher = std::fs::read_to_string(root.join("src/storage/cloud/dispatcher.rs"))
            .expect("read storage/cloud/dispatcher.rs");
        let cloud_sources = rust_sources_under("src/storage/cloud");

        // Act
        let publication_owner_leaks: Vec<_> = cloud_sources
            .iter()
            .filter_map(|path| {
                let source = std::fs::read_to_string(path).expect("read cloud source");
                (source.contains("MetadataPublicationLock")
                    || source.contains("metadata_publication_lock")
                    || source.contains("lock_metadata_publication"))
                .then(|| path.display().to_string())
            })
            .collect();

        // Assert
        assert!(backend.contains("pub trait CloudBackend"));
        assert!(dispatcher.contains("pub struct CloudStorage"));
        assert!(module.contains("mod backend;"));
        assert!(module.contains("mod dispatcher;"));
        assert!(!module.contains("pub trait CloudBackend"));
        assert!(!module.contains("pub struct CloudStorage"));
        assert!(
            !backend.contains("cloud backend does not support GET"),
            "core provider operations must remain required, not regain runtime unsupported defaults"
        );
        assert!(
            publication_owner_leaks.is_empty(),
            "metadata-publication serialization belongs to runtime, not cloud transport: {publication_owner_leaks:?}"
        );
    }

    #[test]
    fn should_keep_config_independent_of_storage_when_lib_declares_crate_aliases() {
        // Arrange
        let lib = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"))
            .expect("read lib.rs");
        let forbidden = ["crate::storage", "cloud_preflight_backend"];

        // Act
        let alias_declared = lib.contains("mod cloud_preflight_backend");
        let offenders: Vec<PathBuf> = rust_sources_under("src/config.rs")
            .into_iter()
            .chain(rust_sources_under("src/config"))
            .filter(|path| {
                let source = std::fs::read_to_string(path).expect("read Rust source");
                forbidden.iter().any(|needle| source.contains(needle))
            })
            .collect();

        // Assert
        assert!(
            !alias_declared,
            "lib.rs must not declare a crate-level alias that gives config a path into storage"
        );
        assert!(
            offenders.is_empty(),
            "config is the foundation layer and must not reach storage, directly or through \
             an alias: {offenders:?}"
        );
    }

    #[test]
    fn should_keep_cloud_storage_module_below_its_size_budget() {
        // Arrange
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage/cloud/mod.rs");
        let budget_lines = 520;

        // Act
        let lines = std::fs::read_to_string(&path)
            .expect("read storage/cloud/mod.rs")
            .lines()
            .count();

        // Assert
        assert!(
            lines <= budget_lines,
            "storage/cloud/mod.rs has {lines} lines (budget {budget_lines}): its errors, \
             backend trait, mock, proofs, adapter and tests each have their own reason to change"
        );
    }

    #[test]
    fn should_keep_sst_factory_io_below_its_size_budget() {
        // Arrange
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/sst/fs/factory_io.rs");
        let budget_lines = 800;

        // Act
        let lines = std::fs::read_to_string(&path)
            .expect("read factory_io.rs")
            .lines()
            .count();

        // Assert
        assert!(
            lines <= budget_lines,
            "factory_io.rs has {lines} lines (budget {budget_lines}): the writer, its size \
             bounds and its tests belong in separate modules"
        );
    }

    #[test]
    fn should_keep_skiplist_memtable_ownership_outside_the_sst_module() {
        // Arrange
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let memtable = std::fs::read_to_string(root.join("src/memtable/mod.rs"))
            .expect("read memtable module source");
        let misplaced_sst_surface = [
            "pub struct SkipListMemtable",
            "pub trait Memtable",
            "fn entry_type_of",
            "mod size_bound",
            "crate::memtable::SkipListMemtable",
            "crate::memtable::entry_type_of",
            "pub use crate::memtable",
        ];
        let legacy_memtable_surface = [
            "seq_generator",
            "pub fn put(",
            "pub fn delete(",
            "pub fn put_with_exp(",
            "iter_all_with_meta(&self, _max_seq",
            "iter_all(&self, max_seq",
        ];

        // Act
        let lingering_sst_symbols: Vec<_> = rust_sources_under("src/sst")
            .into_iter()
            .flat_map(|path| {
                let source = std::fs::read_to_string(&path).expect("read SST source");
                misplaced_sst_surface
                    .iter()
                    .filter(move |symbol| source.contains(**symbol))
                    .map(move |symbol| format!("{}:{symbol}", path.display()))
                    .collect::<Vec<_>>()
            })
            .collect();
        let lingering_legacy_surface: Vec<_> = legacy_memtable_surface
            .into_iter()
            .filter(|symbol| memtable.contains(symbol))
            .collect();

        // Assert
        assert!(
            lingering_sst_symbols.is_empty(),
            "SST still owns: {lingering_sst_symbols:?}"
        );
        assert!(
            root.join("src/memtable/size_bound.rs").is_file(),
            "encoded memtable bounds must live with their owner"
        );
        assert!(
            !root.join("src/sst/size_bound.rs").exists(),
            "SST must not retain a compatibility size-bound module"
        );
        assert!(memtable.contains("pub struct SkipListMemtable"));
        assert!(memtable.contains("pub(crate) mod size_bound;"));
        assert!(
            lingering_legacy_surface.is_empty(),
            "memtable still exposes legacy mutation or iteration surface: {lingering_legacy_surface:?}"
        );
    }

    const SST_VERSION_STATE_LEGACY_DEFINITIONS: [&str; 8] = [
        "pub enum EntryType",
        "pub struct RangeTombstone",
        "pub enum KeyState",
        "pub use crate::types::EntryType",
        "pub use crate::types::RangeTombstone",
        "pub use crate::types::KeyState",
        "pub use crate::types::{",
        "pub(crate) use crate::types::{",
    ];

    #[test]
    fn should_keep_version_state_types_owned_below_sst_codecs() {
        // Arrange
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let shared =
            std::fs::read_to_string(root.join("src/types.rs")).expect("read shared types source");
        let sst_sources = rust_sources_under("src/sst");
        // Act
        let offenders: Vec<_> = sst_sources
            .iter()
            .flat_map(|path| {
                let source = std::fs::read_to_string(path).expect("read SST source");
                SST_VERSION_STATE_LEGACY_DEFINITIONS
                    .iter()
                    .filter(move |needle| source.contains(**needle))
                    .map(move |needle| format!("{}:{needle}", path.display()))
                    .collect::<Vec<_>>()
            })
            .collect();

        // Assert
        assert!(shared.contains("pub enum EntryType"));
        assert!(shared.contains("pub struct RangeTombstone"));
        assert!(shared.contains("pub enum KeyState"));
        assert!(
            offenders.is_empty(),
            "SST codecs must consume shared version state instead of defining or re-exporting it: {offenders:?}"
        );
    }

    const LEGACY_PERSISTED_SST_DEFINITIONS: [&str; 8] = [
        "mod name;",
        "struct PersistedSstName",
        "SST_SEQUENCE_WIDTH",
        "fn file_name(",
        "fn compaction_file_name(",
        "fn parse_compaction_file_name",
        "fn object_key(",
        "fn temp_object_key(",
    ];

    const LEGACY_PERSISTED_SST_SYMBOLS: [&str; 7] = [
        "PersistedSstName",
        "SST_SEQUENCE_WIDTH",
        "file_name",
        "compaction_file_name",
        "parse_compaction_file_name",
        "object_key",
        "temp_object_key",
    ];

    fn is_public_use_statement(statement: &str) -> bool {
        statement.contains("pubuse")
            || statement
                .match_indices("pub(")
                .any(|(index, _)| statement[index..].contains(")use"))
    }

    fn persisted_sst_naming_offenders(sst_sources: &[PathBuf]) -> Vec<String> {
        sst_sources
            .iter()
            .flat_map(|path| {
                let source = std::fs::read_to_string(path).expect("read SST source");
                let definitions = LEGACY_PERSISTED_SST_DEFINITIONS
                    .iter()
                    .filter(|symbol| source.contains(**symbol))
                    .map(move |symbol| format!("{}:{symbol}", path.display()))
                    .collect::<Vec<_>>();
                let reexports = source
                    .split(';')
                    .map(|statement| statement.split_whitespace().collect::<String>())
                    .filter(|statement| is_public_use_statement(statement))
                    .flat_map(|statement| {
                        let symbols = LEGACY_PERSISTED_SST_SYMBOLS
                            .iter()
                            .filter(|symbol| statement.contains(**symbol))
                            .map(|symbol| format!("{}:{statement}:{symbol}", path.display()))
                            .collect::<Vec<_>>();
                        let glob = statement
                            .contains('*')
                            .then(|| format!("{}:{statement}:glob re-export", path.display()));
                        symbols.into_iter().chain(glob).collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                let layout_import = source
                    .contains("cloud_layout")
                    .then(|| format!("{}:cloud_layout compatibility import", path.display()));
                definitions
                    .into_iter()
                    .chain(reexports)
                    .chain(layout_import)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn persisted_sst_proof_offenders(sst_sources: &[PathBuf]) -> Vec<String> {
        sst_sources
            .iter()
            .flat_map(|path| {
                let source = std::fs::read_to_string(path).expect("read SST source");
                let compact = source.split_whitespace().collect::<String>();
                let aliases = ["structExpectedSst", "typeExpectedSst"]
                    .iter()
                    .filter(|symbol| compact.contains(**symbol))
                    .map(|symbol| format!("{}:{symbol}", path.display()))
                    .collect::<Vec<_>>();
                let reexports = source
                    .split(';')
                    .map(|statement| statement.split_whitespace().collect::<String>())
                    .filter(|statement| is_public_use_statement(statement))
                    .filter(|statement| {
                        statement.contains("ExpectedSst") || statement.contains('*')
                    })
                    .map(move |statement| format!("{}:{statement}", path.display()))
                    .collect::<Vec<_>>();
                aliases.into_iter().chain(reexports).collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn should_keep_persisted_sst_layout_proof_views_below_sst() {
        // Arrange
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let layout = std::fs::read_to_string(root.join("src/cloud_layout.rs"))
            .expect("read cloud layout source");
        let shared =
            std::fs::read_to_string(root.join("src/types.rs")).expect("read shared types source");
        let sst_sources = rust_sources_under("src/sst");

        // Act
        let naming_offenders = persisted_sst_naming_offenders(&sst_sources);
        let proof_offenders = persisted_sst_proof_offenders(&sst_sources);

        // Assert
        assert!(layout.contains("pub(crate) struct PersistedSstName"));
        assert!(layout.contains("pub(crate) const SST_SEQUENCE_WIDTH"));
        assert!(layout.contains("pub(crate) fn file_name"));
        assert!(layout.contains("pub(crate) fn compaction_file_name"));
        assert!(layout.contains("pub(crate) fn parse_compaction_file_name"));
        assert!(layout.contains("pub(crate) fn object_key"));
        assert!(layout.contains("pub(crate) fn temp_object_key"));
        assert!(shared.contains("pub(crate) struct ExpectedSst"));
        assert!(
            naming_offenders.is_empty(),
            "SST must not own or re-export persisted naming/layout helpers: {naming_offenders:?}"
        );
        assert!(
            proof_offenders.is_empty(),
            "SST must consume ExpectedSst without owning or re-exporting it: {proof_offenders:?}"
        );
        assert!(
            !root.join("src/sst/name.rs").exists(),
            "SST must not retain a compatibility naming module"
        );
    }

    const CLOUD_SEAL_FUNCTION_END: &str = "\n    }\n";
    const DYN_SST_WRITER_TRAIT_END: &str = "\n}\n";
    const OPENING_BRACE: char = '\x7b';

    fn dyn_sst_writer_method_declaration(name: &str) -> String {
        format!("fn {name}(")
    }

    #[test]
    fn should_bound_cloud_seal_when_the_event_loop_forces_a_cloud_async_seal() {
        // Arrange
        let source = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/runtime/event_loop/cloud_integration/sealing.rs"),
        )
        .expect("read cloud sealing source");
        let start = source
            .find("pub(crate) fn seal_current_cloud_segment(")
            .expect("seal_current_cloud_segment");
        let end = source[start..]
            .find(CLOUD_SEAL_FUNCTION_END)
            .expect("end of seal_current_cloud_segment");

        // Act
        let body = &source[start..start + end];

        // Assert
        assert!(
            !body.contains("OperationDeadline::unbounded()"),
            "the event loop must not wait indefinitely on a cloud seal: everything queued \
             behind it stalls until the provider answers"
        );
    }

    #[test]
    fn should_require_lossless_entry_methods_when_implementing_sst_writer() {
        // Arrange
        let source = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("src/sst/traits.rs"),
        )
        .expect("read SST traits");
        let start = source
            .find("pub trait DynSstWriter")
            .expect("DynSstWriter trait");
        let trait_end = source[start..]
            .find(DYN_SST_WRITER_TRAIT_END)
            .expect("DynSstWriter trait end");
        let trait_source = &source[start..start + trait_end];
        let required = [
            "add_with_meta",
            "add_sorted_with_meta",
            "add_range_tombstone",
            "encoded_size_upper_bound",
            "encoded_size_upper_bound_after_sorted_entry",
            "additional_range_tombstone_size_upper_bound",
        ];
        let removed = [
            "preserves_versioned_entries",
            "require_versioned_entries",
            "add",
        ];

        // Act
        let defaulted: Vec<&str> = required
            .into_iter()
            .filter(|name| {
                let declaration = trait_source
                    .find(&dyn_sst_writer_method_declaration(name))
                    .unwrap_or_else(|| panic!("DynSstWriter method missing"));
                let terminator = trait_source[declaration..]
                    .find([OPENING_BRACE, ';'])
                    .expect("declaration terminator");
                trait_source.as_bytes()[declaration + terminator] == OPENING_BRACE as u8
            })
            .collect();
        let lingering: Vec<&str> = removed
            .into_iter()
            .filter(|name| trait_source.contains(&dyn_sst_writer_method_declaration(name)))
            .collect();

        // Assert
        assert!(
            defaulted.is_empty(),
            "DynSstWriter methods must be required so a writer cannot silently drop tombstones, \
             sequences or TTLs"
        );
        assert!(
            lingering.is_empty(),
            "the lossy entry API must not return (add drops the sequence, kind and TTL)"
        );
    }

    #[test]
    fn should_not_implement_storage_backend_for_hybrid_storage() {
        // Arrange
        let needle = "impl StorageBackend for HybridStorage";

        // Act
        let offenders: Vec<PathBuf> = rust_sources_under("src/storage/hybrid")
            .into_iter()
            .filter(|path| {
                std::fs::read_to_string(path)
                    .expect("read Rust source")
                    .contains(needle)
            })
            .collect();

        // Assert
        assert!(
            offenders.is_empty(),
            "HybridStorage must not be a StorageBackend: its local-first reads, local-only \
             writes and lossy lists are unsafe for an object layer whose authority is the \
             cloud: {offenders:?}"
        );
    }

    /// Dependency-checked source for one file.
    ///
    /// A file is not exempt because it is named `tests.rs`: a test module still
    /// belongs to the layer it lives in, and an exemption there let storage
    /// tests own WAL, SST, manifest and runtime behaviour unnoticed.
    fn production_source(path: &Path) -> String {
        let source = std::fs::read_to_string(path).expect("read Rust source");
        if source.trim_start().starts_with("#![cfg(test)]") {
            return String::new();
        }
        source
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap_or(&source)
            .to_string()
    }

    #[test]
    fn should_exclude_explicit_test_modules_from_production_dependency_checks() {
        // Arrange
        let directory = tempfile::tempdir().expect("source fixtures");
        let test_module = directory.path().join("test_fixture.rs");
        let production_module = directory.path().join("production.rs");
        let import = "use crate::runtime::Runtime;\n";
        std::fs::write(&test_module, format!("#![cfg(test)]\n{import}")).expect("test fixture");
        std::fs::write(&production_module, import).expect("production fixture");

        // Act
        let test_source = production_source(&test_module);
        let production = production_source(&production_module);

        // Assert
        assert!(test_source.is_empty());
        assert!(production.contains("crate::runtime"));
    }

    fn prohibited_edges_under(relative: &str, forbidden: &[&str]) -> Vec<String> {
        rust_sources_under(relative)
            .into_iter()
            .flat_map(|path| {
                let source = production_source(&path);
                forbidden
                    .iter()
                    .filter(move |edge| source.contains(**edge))
                    .map(move |edge| format!("{} imports {edge}", path.display()))
            })
            .collect()
    }

    #[test]
    fn should_keep_common_independent_from_higher_subsystems() {
        // Arrange
        let forbidden = [
            "crate::engine",
            "crate::lease",
            "crate::metadata",
            "crate::runtime",
            "crate::sst",
            "crate::storage",
            "crate::wal",
        ];

        // Act
        let violations: Vec<_> = rust_sources_under("src/common")
            .into_iter()
            .flat_map(|path| {
                let source = std::fs::read_to_string(&path).expect("read common source");
                forbidden
                    .iter()
                    .filter(move |edge| source.contains(**edge))
                    .map(move |edge| format!("{} imports {edge}", path.display()))
            })
            .collect();

        // Assert
        assert!(
            violations.is_empty(),
            "common must remain the bottom dependency layer: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_provider_configuration_owned_by_config_layer() {
        // Arrange
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut sources = vec![manifest_dir.join("src/config.rs")];
        let config_dir = manifest_dir.join("src/config");
        if config_dir.exists() {
            sources.extend(rust_sources_under("src/config"));
        }

        // Act
        let violations: Vec<_> = sources
            .into_iter()
            .filter_map(|path| {
                let source = std::fs::read_to_string(&path).expect("read config source");
                source
                    .contains("crate::storage")
                    .then(|| format!("{} imports crate::storage", path.display()))
            })
            .collect();

        // Assert
        assert!(
            violations.is_empty(),
            "configuration DTOs must not be owned or re-exported by storage: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_storage_as_raw_object_io() {
        // Arrange
        let forbidden = [
            "crate::engine",
            "crate::metadata",
            "crate::runtime",
            "crate::sst",
            "crate::wal",
        ];

        // Act
        let violations = prohibited_edges_under("src/storage", &forbidden);

        // Assert
        assert!(
            violations.is_empty(),
            "storage must provide raw bounded object I/O without format or runtime ownership: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_storage_tests_free_of_runtime_format_imports() {
        // Arrange
        let forbidden = ["crate::runtime", "crate::wal", "crate::metadata"];

        // Act
        let violations = prohibited_edges_under("src/storage", &forbidden);

        // Assert
        assert!(
            violations.is_empty(),
            "storage test modules must not reach into runtime orchestration or persistence formats: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_sst_below_read_layers() {
        // Arrange
        let forbidden = [
            "crate::engine",
            "crate::metadata",
            "crate::runtime",
            "crate::storage",
            "crate::wal",
        ];

        // Act
        let violations = prohibited_edges_under("src/sst", &forbidden);

        // Assert
        assert!(
            violations.is_empty(),
            "SST format and readers must not depend on iterator facades or orchestration: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_persistence_formats_below_storage_orchestration() {
        // Arrange
        let forbidden = [
            "crate::engine",
            "crate::runtime",
            "crate::sst",
            "crate::storage",
        ];

        // Act
        let mut violations = prohibited_edges_under("src/wal", &forbidden);
        violations.extend(prohibited_edges_under("src/metadata", &forbidden));

        // Assert
        assert!(
            violations.is_empty(),
            "WAL and metadata formats must not depend on storage or runtime orchestration: {violations:#?}"
        );
    }

    #[test]
    fn should_enforce_declared_architecture_boundaries_given_diagnostics_and_cli() {
        // Arrange
        let diagnostics_forbidden = [
            "crate::engine",
            "crate::metadata",
            "crate::runtime",
            "crate::storage",
            "crate::wal",
        ];
        let cli_forbidden = [
            "cntryl_midge::engine",
            "cntryl_midge::metadata",
            "cntryl_midge::runtime",
            "cntryl_midge::storage",
            "cntryl_midge::wal",
        ];

        // Act
        let mut violations = prohibited_edges_under("src/diagnostics.rs", &diagnostics_forbidden);
        violations.extend(prohibited_edges_under("src/bin/midge.rs", &cli_forbidden));

        // Assert
        assert!(
            violations.is_empty(),
            "diagnostics and the verify CLI must stay on their declared dependency boundaries: {violations:#?}"
        );
    }
}

mod failpoints_contract {
    #[cfg(feature = "failpoints")]
    use crate::common::crash;
    use std::path::PathBuf;

    fn repository_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    fn repository_file(path: &str) -> String {
        std::fs::read_to_string(repository_root().join(path))
            .unwrap_or_else(|error| panic!("read {path}: {error}"))
    }

    fn rust_files_below(root: &std::path::Path) -> Vec<PathBuf> {
        fn visit(directory: &std::path::Path, files: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(directory).expect("read source directory") {
                let path = entry.expect("read source entry").path();
                if path.is_dir() {
                    visit(&path, files);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    files.push(path);
                }
            }
        }

        let mut files = Vec::new();
        visit(root, &mut files);
        files.sort();
        files
    }

    fn direct_failpoint_bypasses(
        root: &std::path::Path,
        adapter: &std::path::Path,
    ) -> Vec<PathBuf> {
        rust_files_below(root)
            .into_iter()
            .filter(|path| path != adapter)
            .filter(|path| {
                let source = std::fs::read_to_string(path).expect("read Rust source");
                source.contains("fail::fail_point!") || source.contains("fail::eval(")
            })
            .collect()
    }

    fn poison_fragile_test_locks(roots: &[PathBuf]) -> Vec<PathBuf> {
        let expect_pattern = [".lock()", ".expect("].concat();
        let unwrap_pattern = [".lock()", ".unwrap("].concat();
        let mut fragile = Vec::new();
        for root in roots {
            for path in rust_files_below(root) {
                let source = std::fs::read_to_string(&path).expect("read Rust source");
                let compact: String = source
                    .chars()
                    .filter(|character| !character.is_whitespace())
                    .collect();
                if compact.contains(&expect_pattern) || compact.contains(&unwrap_pattern) {
                    fragile.push(path);
                }
            }
        }
        fragile.sort();
        fragile
    }

    #[test]
    fn should_exclude_fail_dependency_when_default_features_are_selected() {
        // Arrange
        let manifest = repository_file("Cargo.toml");
        let default_features = manifest
            .lines()
            .find(|line| line.starts_with("default = "))
            .expect("default feature declaration");

        // Act
        let fail_dependency_is_optional = manifest
            .contains("fail = { version = \"0.5\", features = [\"failpoints\"], optional = true }");
        let explicit_feature_exists = manifest.contains("failpoints = [\"dep:fail\"]");

        // Assert
        assert!(fail_dependency_is_optional);
        assert!(explicit_feature_exists);
        assert!(!default_features.contains("failpoints"));
    }

    #[test]
    fn should_require_failpoints_feature_when_injection_only_targets_are_selected() {
        // Arrange
        let manifest = repository_file("Cargo.toml");
        // Every injection-only suite lives in the single `fault_injection`
        // target, so one gated declaration keeps them all off default builds.
        let injection_targets = ["fault_injection"];

        // Act
        let missing_gate = injection_targets.iter().find(|target| {
            let declaration = format!(
                "name = \"{target}\"\npath = \"tests/{target}.rs\"\nrequired-features = [\"failpoints\"]"
            );
            !manifest.contains(&declaration)
        });

        // Assert
        assert_eq!(missing_gate, None);
    }

    #[test]
    fn should_route_production_injection_through_internal_adapter() {
        // Arrange
        let source_root = repository_root().join("src");
        let adapter = source_root.join("failpoints.rs");

        // Act
        let direct_production_references = direct_failpoint_bypasses(&source_root, &adapter);

        // Assert
        assert!(
            direct_production_references.is_empty(),
            "production code bypassed src/failpoints.rs: {direct_production_references:?}"
        );
    }

    #[test]
    fn should_flag_new_production_file_when_it_bypasses_internal_failpoint_adapter() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_root = temp_dir.path().join("src");
        let nested = source_root.join("runtime/event_loop");
        std::fs::create_dir_all(&nested).expect("create nested source directory");
        let adapter = source_root.join("failpoints.rs");
        std::fs::write(&adapter, "macro_rules! fail_point { () => {} }\n")
            .expect("write adapter fixture");
        let bypass = nested.join("bypass.rs");
        std::fs::write(&bypass, "fn inject() { fail::fail_point!(\"raw\"); }\n")
            .expect("write bypass fixture");

        // Act
        let detected = direct_failpoint_bypasses(&source_root, &adapter);

        // Assert
        assert_eq!(detected, vec![bypass]);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_report_specific_failpoint_marker_when_child_process_aborts_at_intended_boundary() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let marker = temp_dir.path().join("trigger.sentinel");

        // Act
        let missing = crash::validate_trigger_sentinel(&marker, "scenario", "expected-trigger");
        std::fs::write(&marker, "scenario=scenario\ntrigger=wrong-trigger\n")
            .expect("write wrong trigger sentinel");
        let wrong = crash::validate_trigger_sentinel(&marker, "scenario", "expected-trigger");
        std::fs::write(&marker, "scenario=scenario\ntrigger=expected-trigger\n")
            .expect("write expected trigger sentinel");
        let exact = crash::validate_trigger_sentinel(&marker, "scenario", "expected-trigger");

        // Assert
        assert!(missing.is_err());
        assert!(wrong.is_err());
        assert_eq!(exact, Ok(()));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_reject_non_abort_child_failure_even_when_trigger_marker_matches() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let marker = temp_dir.path().join("trigger.sentinel");
        std::fs::write(&marker, "scenario=scenario\ntrigger=expected-trigger\n")
            .expect("write exact trigger sentinel");
        let output = std::process::Command::new(
            std::env::current_exe().expect("locate failpoint contract test executable"),
        )
        .arg("--definitely-not-a-valid-test-harness-option")
        .output()
        .expect("run ordinary failing child");

        // Act
        let validation =
            crash::validate_child_crash(&output, &marker, "scenario", "expected-trigger");

        // Assert
        assert!(validation
            .expect_err("ordinary failure must not count as an abort")
            .contains("failed without process abort"));
    }

    #[test]
    fn should_use_poison_tolerant_shared_test_locks_across_repository() {
        // Arrange
        let root = repository_root();
        let source_roots = [root.join("src"), root.join("tests")];

        // Act
        let fragile = poison_fragile_test_locks(&source_roots);

        // Assert
        assert!(
            fragile.is_empty(),
            "shared test locks must recover poisoned guards: {fragile:?}"
        );
    }

    #[test]
    fn should_cascade_no_further_test_failures_when_prior_failpoint_guard_panics() {
        // Arrange: this exercises the poison-tolerant lock pattern that
        // `should_use_poison_tolerant_shared_test_locks_across_repository`
        // enforces repository-wide for locks shared across tests (including
        // failpoint test guards, which are `pub(crate)` and so cannot be driven
        // directly from an external integration-test binary). A panic while
        // holding such a lock must not cascade into a failure for whoever
        // acquires it next.
        let lock = std::sync::Arc::new(std::sync::Mutex::new(0u32));
        let poisoner_lock = std::sync::Arc::clone(&lock);
        let poisoner = std::thread::spawn(move || {
            let mut guard = poisoner_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard += 1;
            panic!("synthetic assertion failure while holding test guard");
        });
        assert!(poisoner.join().is_err());
        assert!(lock.is_poisoned());

        // Act: a second, independent acquisition - standing in for the next
        // test in the suite grabbing the same shared lock - must still succeed
        // and observe the poisoner's partial work, rather than cascading.
        let follower_lock = std::sync::Arc::clone(&lock);
        let follower = std::thread::spawn(move || {
            let guard = follower_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard
        });
        let value_seen_by_follower = follower
            .join()
            .expect("follower must not cascade the panic");

        // Assert
        assert_eq!(
            value_seen_by_follower, 1,
            "follower must observe the poisoner's work rather than a reset/lost state"
        );
    }

    #[test]
    fn should_verify_default_release_graph_excludes_failpoints_in_workflows() {
        // Arrange
        let ci = repository_file(".github/workflows/ci.yml");
        let publish = repository_file(".github/workflows/publish.yml");
        let graph_check = "cargo tree --edges normal | grep -E '(^|[[:space:]])fail v'";

        // Act
        let ci_has_gate = ci.contains(graph_check) && ci.contains("cargo check --release");
        let publish_has_gate =
            publish.contains(graph_check) && publish.contains("cargo check --release");

        // Assert
        assert!(ci_has_gate);
        assert!(publish_has_gate);
    }

    #[test]
    fn should_enable_failpoints_when_release_runs_injection_suites() {
        // Arrange
        let publish = repository_file(".github/workflows/publish.yml");
        let injection_commands = [
            "cargo test --test fault_injection --features failpoints -- --test-threads=1 external_adopter_smoke",
            "cargo test --test fault_injection --features failpoints -- --test-threads=1 failure_injection",
            "cargo test --test fault_injection --features failpoints -- --test-threads=1 chaos_compaction",
        ];

        // Act
        let missing_feature = injection_commands
            .iter()
            .find(|command| !publish.contains(*command));

        // Assert
        assert_eq!(missing_feature, None);
    }
}

mod public_api_surface {
    //! Guards the canonical public export surface of the crate root.
    //!
    //! Implementation modules are private; the only way for this crate's own
    //! tests, benches and fuzz targets to reach them is `__internal`, which is
    //! compiled only under the non-default `internal-testing` feature.

    use std::path::Path;

    const INTERNAL_MODULE: &str = "__internal";
    const CANONICAL_PUBLIC_MODULES: [&str; 1] = ["prelude"];

    /// Return every `pub mod` declared in `source` that is neither a canonical
    /// public module nor nested inside the feature-gated `__internal` module.
    fn offending_pub_modules(source: &str) -> Vec<String> {
        let mut offenders = Vec::new();
        let mut depth: usize = 0;
        let mut internal_depth: Option<usize> = None;

        for line in source.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("pub mod ") {
                let name = rest
                    .trim_end()
                    .trim_end_matches(['{', ';'])
                    .trim()
                    .to_string();
                let inside_internal = internal_depth.is_some_and(|start| depth > start);
                let allowed = inside_internal
                    || name == INTERNAL_MODULE
                    || CANONICAL_PUBLIC_MODULES.contains(&name.as_str());
                if !allowed {
                    offenders.push(name.clone());
                }
                if name == INTERNAL_MODULE {
                    internal_depth = Some(depth);
                }
            }

            depth = (depth + line.matches('{').count()).saturating_sub(line.matches('}').count());
            if internal_depth.is_some_and(|start| depth <= start) {
                internal_depth = None;
            }
        }

        offenders
    }

    fn crate_root_source() -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
        std::fs::read_to_string(path).expect("read src/lib.rs")
    }

    #[test]
    fn should_expose_only_canonical_api_without_internal_feature() {
        // Arrange
        let source = crate_root_source();

        // Act
        let offenders = offending_pub_modules(&source);

        // Assert
        assert!(
            offenders.is_empty(),
            "src/lib.rs must keep implementation modules private; move these behind \
             `#[cfg(feature = \"internal-testing\")] pub mod __internal`: {offenders:#?}"
        );
    }

    const INTERNAL_TESTING_MODULE_GATE: &str =
        "#[cfg(feature = \"internal-testing\")]\n#[doc(hidden)]\npub mod __internal {";

    #[test]
    fn should_keep_internal_module_behind_the_internal_testing_feature() {
        // Arrange
        let source = crate_root_source();

        // Act
        let gated = source.contains(INTERNAL_TESTING_MODULE_GATE);

        // Assert
        assert!(
            gated,
            "`pub mod __internal` must be preceded by `#[cfg(feature = \"internal-testing\")]`"
        );
    }

    #[test]
    fn should_flag_public_module_when_reintroduced_outside_internal_module() {
        // Arrange
        let regressed = concat!(
            "mod common;\n",
            "pub mod wal;\n",
            "pub mod prelude {\n",
            "    pub use crate::Engine;\n",
            "}\n",
            "#[cfg(feature = \"internal-testing\")]\n",
            "#[doc(hidden)]\n",
            "pub mod __internal {\n",
            "    pub mod sst {\n",
            "        pub use crate::sst::*;\n",
            "    }\n",
            "}\n",
        );

        // Act
        let offenders = offending_pub_modules(regressed);

        // Assert
        assert_eq!(offenders, vec!["wal".to_string()]);
    }

    /// Return the `pub fn` names in `source` whose attribute block marks them
    /// `#[doc(hidden)]` without a `#[cfg(feature = ...)]` gate. Hidden hooks on
    /// the public surface must not ship in default builds.
    fn ungated_hidden_public_fns(source: &str) -> Vec<String> {
        let mut offenders = Vec::new();
        let mut hidden = false;
        let mut gated = false;
        for line in source.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("#[") {
                hidden |= trimmed == "#[doc(hidden)]";
                gated |= trimmed.starts_with("#[cfg(feature");
                continue;
            }
            if trimmed.starts_with("///") || trimmed.starts_with("//") {
                continue;
            }
            if hidden && !gated {
                if let Some(rest) = trimmed.strip_prefix("pub fn ") {
                    let name: String = rest
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect();
                    offenders.push(name);
                }
            }
            hidden = false;
            gated = false;
        }
        offenders
    }

    fn rust_sources(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read source dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                rust_sources(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    #[test]
    fn should_gate_hidden_public_functions_when_on_the_public_surface() {
        // Arrange: lib.rs and the engine API are the only public surface;
        // other modules are private and reachable only through `__internal`.
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = vec![root.join("lib.rs")];
        rust_sources(&root.join("engine"), &mut files);

        // Act
        let offenders: Vec<String> = files
            .iter()
            .flat_map(|path| {
                let source = std::fs::read_to_string(path).expect("read source");
                ungated_hidden_public_fns(&source)
                    .into_iter()
                    .map(move |name| format!("{}: {name}", path.display()))
            })
            .collect();

        // Assert
        assert!(
            offenders.is_empty(),
            "`#[doc(hidden)] pub fn` on the public surface must be gated with \
             `#[cfg(feature = \"internal-testing\")]`: {offenders:#?}"
        );
    }

    #[test]
    fn should_flag_hidden_public_function_when_feature_gate_is_missing() {
        // Arrange
        let source = concat!(
            "#[doc(hidden)]\n",
            "#[must_use]\n",
            "pub fn leaked_hook() {}\n",
            "#[cfg(feature = \"internal-testing\")]\n",
            "#[doc(hidden)]\n",
            "pub fn gated_hook() {}\n",
            "    /// Documented.\n",
            "    #[doc(hidden)]\n",
            "    pub fn leaked_method(&self) {}\n",
        );

        // Act
        let offenders = ungated_hidden_public_fns(source);

        // Assert
        assert_eq!(offenders, vec!["leaked_hook", "leaked_method"]);
    }

    #[test]
    fn should_not_reexport_filesystem_abstraction_when_building_public_surface() {
        // Arrange
        let source = crate_root_source();

        // Act
        let reexports_io = source.lines().any(|line| {
            let trimmed = line.trim();
            trimmed.starts_with("pub use io::") || trimmed.starts_with("pub use crate::io::")
        });

        // Assert
        assert!(
            !reexports_io,
            "src/lib.rs must not re-export the internal `io` filesystem abstraction"
        );
    }

    #[test]
    fn should_reach_internals_only_through_the_gated_module() {
        // Arrange
        use cntryl_midge::__internal::wal::{encoding, WalOpKind, WalRecord};
        let record = WalRecord::new(
            WalOpKind::Put,
            cntryl_midge::Bytes::from_static(b"governance-key"),
            Some(cntryl_midge::Bytes::from_static(b"governance-value")),
            7,
            1,
        );

        // Act
        let encoded = encoding::encode(&record).expect("encode WAL record");

        // Assert
        assert_eq!(
            encoding::decode(encoded.as_ref()).expect("decode WAL record"),
            record
        );
    }
}
