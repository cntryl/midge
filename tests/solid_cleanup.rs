use std::fs;
use std::path::{Path, PathBuf};

use cntryl_midge::{
    AzureCredentialSource, CloudCredentialSource, CloudProviderConfig, ColumnFamilyId, Engine,
    EngineHealth, GcsApiStyle, GcsCredentialSource, MidgeResult, OpenOptions, RecoveryPolicy,
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
    let _: CloudCredentialSource = CloudCredentialSource::S3(S3CredentialSource::environment());
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
        CloudProviderConfig::Gcs {
            api: GcsApiStyle::Json,
            ..
        }
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
fn should_construct_runtime_observability_dtos_from_shared_types() {
    // Arrange
    let state = read_source("src/runtime/state.rs");
    let runtime = read_source("src/runtime/mod.rs");
    let engine = read_source("src/engine/mod.rs");
    let shared = read_source("src/types.rs");

    // Act / Assert
    // Assert
    assert!(state.contains("crate::types::RuntimeMetricsSnapshot"));
    assert!(state.contains("crate::types::StorageLayoutSnapshot"));
    assert!(runtime.contains("Box<crate::types::RuntimeMetricsSnapshot>"));
    assert!(runtime.contains("snapshot: crate::types::StorageLayoutSnapshot"));
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
    let startup = read_source("src/engine/startup.rs");
    let required_phases = [
        "struct StartupStoragePath",
        "struct StartupLease",
        "struct RuntimeStorageMaterialization",
        "struct RuntimeRecoveryMaterialization",
        "struct StartedRuntime",
        "struct FacadeAssembly",
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
    assert!(startup.contains("CloudStartupRecovery::materialize_cloud_wal_recovery_dir"));
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
        CloudProviderConfig::AwsS3 {
            credentials: S3CredentialSource::AwsDefaultChain,
            ..
        }
    ));
    assert!(matches!(
        s3,
        CloudProviderConfig::S3Compatible {
            credentials: S3CredentialSource::Environment,
            path_style: true,
            ..
        }
    ));
    assert!(matches!(
        azure,
        CloudProviderConfig::AzureBlob {
            credential: AzureCredentialSource::LightweightDefaultChain,
            ..
        }
    ));
    assert!(matches!(
        gcs,
        CloudProviderConfig::Gcs {
            api: GcsApiStyle::Xml,
            credential: GcsCredentialSource::HmacKey { .. },
            ..
        }
    ));
}

#[test]
fn should_own_cloud_provider_config_in_storage_providers() {
    // Arrange
    let generic_config = read_source("src/config.rs");
    let provider_config = read_source("src/storage/providers/config.rs");

    // Act / Assert
    // Assert
    assert!(!generic_config.contains("pub enum CloudProviderConfig"));
    assert!(!generic_config.contains("pub enum S3CredentialSource"));
    assert!(!generic_config.contains("pub enum AzureCredentialSource"));
    assert!(!generic_config.contains("pub enum GcsCredentialSource"));
    assert!(generic_config.contains("pub use crate::storage::providers"));
    assert!(provider_config.contains("pub enum CloudProviderConfig"));
    assert!(provider_config.contains("pub enum S3CredentialSource"));
    assert!(provider_config.contains("pub enum AzureCredentialSource"));
    assert!(provider_config.contains("pub enum GcsCredentialSource"));
    assert!(provider_config.contains("impl CloudProviderConfig"));
}

#[test]
fn should_delegate_provider_construction_to_provider_family_resolvers() {
    // Arrange
    let factory = read_source("src/storage/providers/factory.rs");
    let s3 = read_source("src/storage/providers/s3_resolver.rs");
    let azure = read_source("src/storage/providers/azure_resolver.rs");
    let gcs = read_source("src/storage/providers/gcs_resolver.rs");
    let forbidden_factory_variants = [
        "CloudProviderConfig::AwsS3",
        "CloudProviderConfig::S3Compatible",
        "CloudProviderConfig::Minio",
        "CloudProviderConfig::Wasabi",
        "CloudProviderConfig::OciS3Compatible",
        "CloudProviderConfig::AzureBlob",
        "CloudProviderConfig::Gcs",
    ];

    // Act / Assert
    // Assert
    assert!(factory.contains("s3_resolver::try_resolve"));
    assert!(factory.contains("azure_resolver::try_resolve"));
    assert!(factory.contains("gcs_resolver::try_resolve"));
    for pattern in forbidden_factory_variants {
        assert!(
            !factory.contains(pattern),
            "provider factory should not centrally match {pattern}"
        );
    }
    assert!(s3.contains("pub(super) fn try_resolve"));
    assert!(s3.contains("CloudProviderConfig::AwsS3"));
    assert!(s3.contains("CloudProviderConfig::OciS3Compatible"));
    assert!(azure.contains("pub(super) fn try_resolve"));
    assert!(azure.contains("CloudProviderConfig::AzureBlob"));
    assert!(gcs.contains("pub(super) fn try_resolve"));
    assert!(gcs.contains("CloudProviderConfig::Gcs"));
}

#[test]
fn should_select_same_storage_modes_from_open_options_constructors() {
    // Arrange
    let provider = CloudProviderConfig::s3_compatible_static(
        "bucket",
        "http://localhost:9000",
        "key",
        "secret",
    );

    // Act
    let memory = OpenOptions::in_memory().build();
    let local = OpenOptions::local("/tmp/midge-solid-local").build();
    let cloud = OpenOptions::cloud("/tmp/midge-solid-cloud", provider, "prefix").build();
    let simulated =
        OpenOptions::cloud_simulated("/tmp/midge-solid-simulated", "bucket", "prefix").build();

    // Assert
    assert!(matches!(memory.storage, Storage::InMemory));
    assert!(matches!(local.storage, Storage::Local { .. }));
    assert!(matches!(cloud.storage, Storage::Cloud { .. }));
    assert!(matches!(simulated.storage, Storage::CloudSimulated { .. }));
}

#[test]
fn should_keep_moved_config_types_out_of_lower_layers_engine_imports() {
    // Arrange
    let files = [
        "src/metadata/persistence.rs",
        "src/runtime/intent_persistence.rs",
        "src/runtime/state.rs",
        "src/lease/mod.rs",
        "src/storage/residue.rs",
        "src/storage/providers/mod.rs",
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
fn should_keep_filesystem_persistence_out_of_dyn_sst_writer_trait() {
    // Arrange
    let traits = read_source("src/sst/traits.rs");
    let mut sources = Vec::new();
    collect_rust_sources(&source_path("src"), &mut sources);

    // Act / Assert
    // Assert
    assert!(traits.contains("fn finish_bytes"));
    assert!(!traits.contains("finish_to_path"));
    for source in sources {
        let content = fs::read_to_string(&source).expect("rust source should be readable");
        assert!(
            !content.contains(".finish_to_path("),
            "{} should use sst::fs::finish_writer_to_path",
            source.display()
        );
    }
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
        assert!(doc.contains("WriteOptions::cloud_strict()"));
        assert!(doc.contains("Non-cloud storage rejects"));
        assert!(doc.contains("seal"));
        assert!(doc.contains("upload"));
        assert!(doc.contains("Empty cloud-backed"));
    }
}

#[test]
fn should_document_manifest_fsync_skip_as_benchmark_only_double_opt_in() {
    // Arrange
    let durability = read_source("docs/user-guides/durability.md");
    let journal = read_source("src/metadata/journal.rs");

    // Act
    let docs_flag_skip = durability.contains("MIDGE_SKIP_MANIFEST_FSYNC=1");
    let docs_flag_double_opt_in = durability.contains("MIDGE_ALLOW_MANIFEST_SKIP_FSYNC=1");
    let docs_flag_benchmark_only = durability.contains("benchmark-only");
    let docs_flag_double_opt_in_text = durability.contains("double opt-in");
    let journal_names_skip_flag = journal.contains("MIDGE_SKIP_MANIFEST_FSYNC");
    let journal_names_guard_flag = journal.contains("MIDGE_ALLOW_MANIFEST_SKIP_FSYNC");

    // Assert
    assert!(docs_flag_skip);
    assert!(docs_flag_double_opt_in);
    assert!(docs_flag_benchmark_only);
    assert!(docs_flag_double_opt_in_text);
    assert!(journal_names_skip_flag);
    assert!(journal_names_guard_flag);
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
