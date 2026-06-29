use std::fs;
use std::path::{Path, PathBuf};

use cntryl_midge::{
    AzureCredentialSource, CloudCredentialSource, CloudProviderConfig, Engine, EngineHealth,
    GcsApiStyle, GcsCredentialSource, MidgeResult, OpenOptions, RecoveryPolicy, S3CredentialSource,
    Storage,
};

fn source_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn read_source(relative: &str) -> String {
    fs::read_to_string(source_path(relative)).expect("source file should be readable")
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
