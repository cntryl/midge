//! Public API behavior that does not depend on repository source layout.

use std::time::Duration;

use cntryl_midge::{
    AzureCredentialSource, CloudProviderConfig, CloudStorageLocation, CloudStorageTopology,
    ColumnFamilyId, Engine, EngineHealth, GcsApiStyle, GcsCredentialSource,
    HybridStorageBudgetSnapshot, LocalStorageUsage, MidgeError, MidgeResult, OpenOptions,
    RecoveryPolicy, RuntimeMetricsSnapshot, S3CredentialSource, Storage, StorageAdmissionBlock,
    StorageAdmissionKind, StorageAdmissionReason, StorageLayoutSnapshot, TransactionMode,
};

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
    assert_eq!(usage.transaction_spill_bytes, 0);
    assert_eq!(
        storage.free_bytes,
        local_budget.saturating_sub(storage.total_committed_bytes)
    );
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
fn should_keep_cloud_provider_constructors_on_same_variants() {
    // Arrange
    let aws = CloudProviderConfig::aws_s3("bucket", "us-east-1");
    let s3 = CloudProviderConfig::s3_compatible_env("bucket", "http://localhost:9000");
    let azure = CloudProviderConfig::azure_blob("account", "container");
    let gcs = CloudProviderConfig::gcs_hmac("bucket", "access", "secret");

    // Act
    let providers = [aws, s3, azure, gcs];

    // Assert
    assert!(matches!(
        &providers[0],
        CloudProviderConfig::AwsS3(config) if matches!(config.credentials(), S3CredentialSource::AwsDefaultChain)
    ));
    assert!(matches!(
        &providers[1],
        CloudProviderConfig::S3Compatible(config) if matches!(config.credentials(), S3CredentialSource::Environment) && config.path_style()
    ));
    assert!(matches!(
        &providers[2],
        CloudProviderConfig::AzureBlob(config) if matches!(config.credentials(), AzureCredentialSource::LightweightDefaultChain)
    ));
    assert!(matches!(
        &providers[3],
        CloudProviderConfig::Gcs(config) if config.api_style() == GcsApiStyle::Xml && matches!(config.credentials(), GcsCredentialSource::HmacKey { .. })
    ));
}

#[test]
fn should_select_same_storage_modes_from_open_options_constructors() {
    // Arrange
    let location = |bucket| {
        CloudStorageLocation::new(
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
        CloudStorageTopology::new(location("wal-bucket"))
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
fn should_expose_provider_configuration_across_provider_families() {
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

#[test]
fn should_roundtrip_wal_record_through_internal_test_api() {
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
