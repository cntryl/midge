//! Public API behavior that does not depend on repository source layout.

use std::time::Duration;

use cntryl_midge::{
    CloudProviderConfig, Engine, GcsApiStyle, GcsCredentialSource, HybridStorageBudgetSnapshot,
    LocalStorageUsage, MidgeError, OpenOptions, StorageAdmissionBlock, StorageAdmissionKind,
    StorageAdmissionReason, TransactionMode,
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
    let snapshot = engine
        .metrics()
        .get_runtime_metrics()
        .expect("runtime metrics");
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
fn should_select_json_api_given_application_default_gcs_credentials() {
    // Arrange
    let provider = CloudProviderConfig::gcs("bucket");

    // Act
    let configured = provider
        .with_gcs_credentials(GcsCredentialSource::application_default())
        .expect("configure GCS credentials");

    // Assert
    assert!(
        matches!(configured, CloudProviderConfig::Gcs(config) if config.api_style() == GcsApiStyle::Json)
    );
}
