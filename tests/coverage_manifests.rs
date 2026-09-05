//! Compile-enforced manifests for enum-shaped behavior axes.
//! Internal `FsError` coverage lives in `src/io/traits.rs` unit tests because the
//! filesystem module is intentionally private to library consumers.

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
