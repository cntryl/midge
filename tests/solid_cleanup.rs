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

#[test]
fn should_reexport_shared_public_types_from_crate_root() {
    // Arrange
    let _open_fn: fn(OpenOptions) -> MidgeResult<Engine> = Engine::open;
    let _policy = RecoveryPolicy::Strict;
    let _health = EngineHealth::Healthy;
    let _storage = Storage::InMemory;
    let _credential = CloudCredentialSource::S3(S3CredentialSource::environment());
    let _cf_id: ColumnFamilyId = 0;
    let _runtime_metrics: fn(&Engine) -> MidgeResult<RuntimeMetricsSnapshot> =
        Engine::get_runtime_metrics;
    let _storage_layout: fn(&Engine) -> MidgeResult<StorageLayoutSnapshot> =
        Engine::get_storage_layout;

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
fn should_keep_runtime_free_of_engine_owned_type_imports() {
    // Arrange
    let mut sources = Vec::new();
    collect_rust_sources(&source_path("src/runtime"), &mut sources);
    let forbidden = ["crate::engine::", "crate::engine::api::"];

    // Act / Assert
    for source in sources {
        let content = fs::read_to_string(&source).expect("runtime source should be readable");
        for pattern in forbidden {
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
    let event_loop = production_source("src/runtime/event_loop/mod.rs");
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
fn should_make_startup_phases_and_cloud_recovery_startup_owned() {
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
    ];

    // Act / Assert
    for file in files {
        let content = read_source(file);
        for pattern in forbidden {
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
