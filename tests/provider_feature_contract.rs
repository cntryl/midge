use std::fs;
use std::path::{Path, PathBuf};

use cntryl_midge::CloudProviderConfig;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

fn read_repository_file(path: &str) -> String {
    fs::read_to_string(repository_root().join(path))
        .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
}

fn feature_definition<'a>(manifest: &'a str, feature: &str) -> &'a str {
    let marker = format!("{feature} = [");
    let start = manifest
        .find(&marker)
        .unwrap_or_else(|| panic!("missing Cargo feature {feature}"));
    let definition = &manifest[start..];
    let end = definition
        .find("]\n")
        .unwrap_or_else(|| panic!("unterminated Cargo feature {feature}"));
    &definition[..=end]
}

#[test]
fn should_keep_common_cloud_transport_provider_neutral_when_features_are_split() {
    // Arrange
    let manifest = read_repository_file("Cargo.toml");

    // Act
    let common = feature_definition(&manifest, "cloud-common");

    // Assert
    assert!(manifest.contains("default = [\"cloud-all\"]"));
    assert!(common.contains("dep:reqwest"));
    assert!(common.contains("dep:tokio"));
    for provider_dependency in [
        "dep:base64",
        "dep:hmac",
        "dep:percent-encoding",
        "dep:rsa",
        "dep:sha1",
        "dep:sha2",
        "dep:url",
        "dep:urlencoding",
    ] {
        assert!(
            !common.contains(provider_dependency),
            "cloud-common must not activate {provider_dependency}"
        );
    }
}

#[test]
fn should_assign_each_provider_dependency_set_to_its_cargo_feature() {
    // Arrange
    let manifest = read_repository_file("Cargo.toml");

    // Act
    let aws = feature_definition(&manifest, "cloud-aws");
    let azure = feature_definition(&manifest, "cloud-azure");
    let gcp = feature_definition(&manifest, "cloud-gcp");
    let oci = feature_definition(&manifest, "cloud-oci");

    // Assert
    for s3_feature in [aws, oci] {
        for dependency in [
            "cloud-common",
            "dep:hmac",
            "dep:percent-encoding",
            "dep:sha2",
            "dep:url",
            "dep:urlencoding",
        ] {
            assert!(s3_feature.contains(dependency));
        }
        assert!(!s3_feature.contains("dep:rsa"));
    }

    for dependency in [
        "cloud-common",
        "dep:base64",
        "dep:hmac",
        "dep:percent-encoding",
        "dep:sha2",
        "dep:url",
        "dep:urlencoding",
    ] {
        assert!(azure.contains(dependency));
    }
    assert!(!azure.contains("dep:rsa"));
    assert!(!azure.contains("dep:sha1"));

    for dependency in [
        "cloud-common",
        "dep:base64",
        "dep:hmac",
        "dep:percent-encoding",
        "dep:rsa",
        "dep:sha1",
        "dep:sha2",
        "dep:url",
        "dep:urlencoding",
    ] {
        assert!(gcp.contains(dependency));
    }
}

#[test]
fn should_compile_provider_modules_only_for_their_owner_features() {
    // Arrange
    let providers = read_repository_file("src/storage/providers/mod.rs");
    let factory = read_repository_file("src/storage/providers/factory.rs");

    // Act
    // Assert
    assert!(providers.contains("#[cfg(feature = \"cloud-azure\")]\npub mod azure;"));
    assert!(providers.contains("#[cfg(feature = \"cloud-gcp\")]\npub mod gcs;"));
    assert!(providers
        .contains("#[cfg(any(feature = \"cloud-aws\", feature = \"cloud-oci\"))]\npub mod s3;"));
    assert!(!providers.contains("#[cfg(feature = \"cloud-common\")]\npub mod azure;"));
    assert!(!providers.contains("#[cfg(feature = \"cloud-common\")]\npub mod gcs;"));
    assert!(!providers.contains("#[cfg(feature = \"cloud-common\")]\npub mod s3;"));
    assert!(factory.contains("super::is_aws_s3(provider)"));
    assert!(factory.contains("super::is_s3_compatible(provider)"));
    assert!(factory.contains("super::is_azure_blob(provider)"));
    assert!(factory.contains(
        "#[cfg(any(feature = \"cloud-aws\", feature = \"cloud-oci\"))]\n    fn build_s3_compatible"
    ));
}

#[test]
fn should_keep_provider_dtos_in_configuration_layer() {
    // Arrange
    let provider_config = read_repository_file("src/config/provider.rs");
    let storage_providers = read_repository_file("src/storage/providers/mod.rs");

    // Act
    // Assert
    assert!(provider_config.contains("pub enum CloudProviderConfig"));
    assert!(!provider_config.contains("cfg(feature = \"cloud-"));
    assert!(storage_providers.contains("pub(crate) use crate::config::CloudProviderConfig;"));
    assert!(!repository_root()
        .join("src/storage/providers/config.rs")
        .exists());
}

#[test]
fn should_expose_provider_configuration_without_implementation_features() {
    // Arrange
    let providers = [
        CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
        CloudProviderConfig::s3_compatible_static(
            "bucket",
            "https://objectstorage.example.test",
            "access",
            "secret",
        ),
        CloudProviderConfig::azure_blob_shared_key("account", "container", "secret"),
        CloudProviderConfig::gcs_hmac("bucket", "access", "secret"),
    ];

    // Act
    let object_names = providers
        .iter()
        .map(CloudProviderConfig::bucket_or_container)
        .collect::<Vec<_>>();

    // Assert
    assert_eq!(object_names, ["bucket", "bucket", "container", "bucket"]);
}
