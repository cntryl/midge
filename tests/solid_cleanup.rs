use std::fs;
use std::path::{Path, PathBuf};

use cntryl_midge::{
    AzureCredentialSource, CloudProviderConfig, ColumnFamilyId, Engine, EngineHealth, GcsApiStyle,
    GcsCredentialSource, MidgeResult, OpenOptions, RecoveryPolicy, RuntimeMetricsSnapshot,
    S3CredentialSource, Storage, StorageLayoutSnapshot,
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
fn should_not_suppress_dead_code_in_production_sources() {
    // Arrange
    let mut sources = Vec::new();
    collect_rust_sources(&source_path("src"), &mut sources);
    let forbidden = [
        "#![allow(dead_code",
        "#![expect(dead_code",
        "#[allow(dead_code",
        "#[expect(dead_code",
    ];

    // Act
    // Assert
    for source in sources {
        let relative = source
            .strip_prefix(source_path(""))
            .expect("source should live under repo")
            .to_string_lossy()
            .replace('\\', "/");
        let content = production_source(&relative);

        for pattern in forbidden {
            assert!(
                !content.contains(pattern),
                "{} should not suppress production dead_code with {pattern}",
                source.display()
            );
        }
    }
}

#[test]
fn should_keep_production_sources_free_of_allow_expect_suppressions() {
    // Arrange
    let mut sources = Vec::new();
    collect_rust_sources(&source_path("src"), &mut sources);
    let forbidden = ["#[allow(", "#![allow(", "#[expect(", "#![expect("];

    // Act

    // Assert
    for source in sources {
        let relative = source
            .strip_prefix(source_path(""))
            .expect("source should live under repo")
            .to_string_lossy()
            .replace('\\', "/");
        let content = production_source(&relative);

        for pattern in forbidden {
            assert!(
                !content.contains(pattern),
                "{} should not suppress production lints with {pattern}",
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
        ("src/runtime/event_loop/cloud.rs", "struct CloudCoordinator"),
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
        "RuntimeMsg::CloudUploadSst",
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
fn should_make_startup_recovery_phases_owned() {
    // Arrange
    let startup = [
        "src/engine/startup/mod.rs",
        "src/engine/startup/cloud_recovery.rs",
        "src/engine/startup/storage.rs",
        "src/engine/startup/assembly.rs",
        "src/engine/startup/streaming_recovery.rs",
        "src/engine/startup/streaming_wal_plan.rs",
    ]
    .into_iter()
    .map(read_source)
    .collect::<String>();
    let required_phases = [
        "struct StartupStoragePath",
        "struct StartupLease",
        "struct RuntimeStorageMaterialization",
        "struct RuntimeRecoveryMaterialization",
        "struct StartedRuntime",
        "struct FacadeAssembly",
        "struct CloudReplay",
        "struct StreamingCloudWalRecovery",
    ];
    let forbidden_engine_owned_recovery_calls = [
        "Engine::hydrate_cloud_metadata",
        "Engine::materialize_cloud_wal_recovery_dir",
        "Engine::cloud_recovery_sst_proofs_for_intent_replay",
        "Engine::ensure_named_sst_cache_from_cloud_storage",
        "Engine::ensure_local_sst_cache_from_cloud",
        "Engine::ensure_local_sst_cache_from_cloud_storage",
        "Engine::mirror_cloud_metadata",
    ];

    // Act / Assert
    // Assert
    assert!(startup.contains("struct CloudStartupRecovery"));
    assert!(startup.contains("StartupStoragePath::resolve"));
    assert!(startup.contains("StartupLease::acquire"));
    assert!(startup.contains("RuntimeStorageMaterialization::materialize"));
    assert!(startup.contains("RuntimeRecoveryMaterialization::replay_and_repair"));
    assert!(startup.contains("StartedRuntime::start"));
    assert!(startup.contains("FacadeAssembly::assemble"));
    assert!(startup.contains("CloudStartupRecovery::hydrate_cloud_metadata"));
    assert!(startup.contains("StreamingCloudWalRecovery::build"));
    assert!(startup.contains("replay_wal_with_checkpoint("));
    assert!(
        !read_source("src/engine/startup/storage.rs")
            .contains("materialize_cloud_wal_recovery_dir"),
        "shipping cloud startup must not materialize the complete WAL backlog"
    );
    assert!(startup.contains("CloudStartupRecovery::cloud_recovery_sst_proofs_for_intent_replay"));

    for phase in required_phases {
        assert!(startup.contains(phase), "startup should define {phase}");
    }
    for pattern in forbidden_engine_owned_recovery_calls {
        assert!(
            !startup.contains(pattern),
            "startup should not call engine-owned recovery helper {pattern}"
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
    let simulated = OpenOptions::cloud_simulated("/tmp/midge-solid-simulated", "bucket", "prefix")
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
    let transaction_contract = read_source("docs/user-guides/transaction-durability-contract.md");

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
