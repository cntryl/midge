//! Compile-enforced manifests for enum-shaped behavior axes.

use cntryl_midge::sst::compression::{CompressionAlgo, CompressionPolicy};
use cntryl_midge::{
    AzureCredentialSource, DurabilityPolicy, GcsCredentialSource, RecoveryPolicy,
    S3CredentialSource,
};

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
        AzureCredentialSource::LightweightDefaultChain => "chain unit tests; real Azure scheduled",
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

#[test]
fn should_keep_coverage_manifest_exhaustive_given_public_behavior_axes() {
    // Arrange
    let entries = [
        s3_coverage(&S3CredentialSource::environment()),
        azure_coverage(&AzureCredentialSource::default_chain()),
        gcs_coverage(&GcsCredentialSource::application_default()),
        compression_coverage(CompressionAlgo::Lz4),
        compression_policy_coverage(&CompressionPolicy::None),
        recovery_coverage(RecoveryPolicy::Strict),
        durability_coverage(DurabilityPolicy::Sync),
    ];

    // Act
    let every_axis_is_classified = entries.iter().all(|entry| !entry.is_empty());

    // Assert
    assert!(every_axis_is_classified);
}
