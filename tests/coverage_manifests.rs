//! Compile-enforced manifests for enum-shaped behavior axes.
//! Internal `FsError` coverage lives in `src/io/traits.rs` unit tests because the
//! filesystem module is intentionally private to library consumers.

use cntryl_midge::sst::compression::{CompressionAlgo, CompressionPolicy};
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
        StorageAdmissionReason::CloudUpload => "cloud upload pressure and admission history tests",
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
    let storage: HybridStorageBudgetSnapshot = snapshot.local_storage.expect("cloud disk budget");
    let usage: LocalStorageUsage = storage.usage;
    let pressure: StorageAdmissionBlock = storage.blocked_admission.expect("rejected admission");
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
