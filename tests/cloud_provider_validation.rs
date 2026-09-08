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
