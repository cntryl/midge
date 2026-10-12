use super::*;
use crate::wal::cloud_catalog::CatalogEntry;
use crate::wal::format::ChainLink;

fn populated_catalog() -> WalPublicationCatalog {
    let mut catalog = WalPublicationCatalog::empty(7).unwrap();
    let mut prev = ChainLink::GENESIS;
    let entries: Vec<CatalogEntry> = (1..=4096_u64)
        .map(|segment_id| {
            let record = crate::wal::WalRecord::new(
                crate::wal::WalOpKind::Put,
                bytes::Bytes::from_static(b"key"),
                Some(bytes::Bytes::from_static(b"value")),
                segment_id,
            );
            let (bytes, link) =
                crate::wal::format::segment::SegmentBuilder::new(segment_id, 7, prev)
                    .record(&record)
                    .seal();
            let entry = CatalogEntry::from_sealed_bytes(&bytes).expect("verified fixture segment");
            prev = link;
            entry
        })
        .collect();
    catalog.append_run(7, &entries).unwrap();
    catalog.set_aside_suffix(7, None, &[], 123).unwrap();
    catalog
}

#[test]
fn should_admit_compact_catalog_without_reserving_pretty_print_padding() {
    // Arrange
    let catalog = populated_catalog();
    let expected = serde_json::to_vec(&catalog).unwrap();
    let budget = ResourceBudget::new(expected.len());

    // Act
    let encoded = AdmittedEncoding::new(&catalog, &budget).unwrap();

    // Assert
    assert_eq!(encoded.bytes, expected);
    assert_eq!(
        WalPublicationCatalog::decode(&encoded.bytes).unwrap(),
        catalog
    );
    assert_eq!(budget.used(), expected.len());
    drop(encoded);
    assert_eq!(budget.used(), 0);
}

fn storage_fixture() -> (tempfile::TempDir, super::super::CloudPersistence) {
    use crate::runtime::hybrid_persistence::{CloudPersistence, CloudStorage};
    use std::sync::Arc;
    let directory = tempfile::tempdir().unwrap();
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        Arc::new(CloudStorage::with_mock()),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    (directory, storage)
}

#[test]
fn should_publish_compact_catalog_when_fencing_existing_pretty_authority() {
    // Arrange
    let (_directory, storage) = storage_fixture();
    let catalog = populated_catalog();
    let original = catalog.encode().unwrap();
    let primary = crate::wal::cloud_catalog::OBJECT_KEY;
    let mirror = crate::wal::cloud_catalog::MIRROR_OBJECT_KEY;
    storage
        .compare_exchange_remote_object(primary, None, original.clone())
        .unwrap();
    storage
        .compare_exchange_remote_object(mirror, None, original)
        .unwrap();
    let mut expected = catalog.clone();
    expected.fence_to(8).unwrap();
    let compact = serde_json::to_vec(&expected).unwrap();

    // Act
    storage.fence_cloud_wal_catalog(8).unwrap();

    // Assert
    assert_eq!(
        storage.remote_object_proof(primary).unwrap().bytes(),
        compact
    );
    assert_eq!(
        storage.remote_object_proof(mirror).unwrap().bytes(),
        compact
    );
    assert_eq!(WalPublicationCatalog::decode(&compact).unwrap(), expected);
}

#[test]
fn should_repair_corrupt_mirror_from_compact_authority() {
    // Arrange
    let (_directory, storage) = storage_fixture();
    let catalog = populated_catalog();
    let compact = serde_json::to_vec(&catalog).unwrap();
    let primary = crate::wal::cloud_catalog::OBJECT_KEY;
    let mirror = crate::wal::cloud_catalog::MIRROR_OBJECT_KEY;
    storage
        .compare_exchange_remote_object(primary, None, compact.clone())
        .unwrap();
    storage
        .compare_exchange_remote_object(mirror, None, b"corrupt".to_vec())
        .unwrap();

    // Act
    storage.fence_cloud_wal_catalog(7).unwrap();

    // Assert
    assert_eq!(
        storage.remote_object_proof(primary).unwrap().bytes(),
        compact
    );
    assert_eq!(
        storage.remote_object_proof(mirror).unwrap().bytes(),
        compact
    );
}

#[test]
fn should_reject_invalid_catalog_before_admitting_encoded_storage() {
    // Arrange
    let mut json = serde_json::to_value(populated_catalog()).unwrap();
    json["next_segment_id"] = serde_json::json!(1);
    let catalog: WalPublicationCatalog = serde_json::from_value(json).unwrap();
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let result = AdmittedEncoding::new(&catalog, &budget);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_decode_same_catalog_when_json_whitespace_changes() {
    // Arrange
    let catalog = populated_catalog();
    let budget = ResourceBudget::new(64 * 1024 * 1024);
    let pretty = serde_json::to_vec_pretty(&catalog).unwrap();
    let compact = serde_json::to_vec(&catalog).unwrap();
    for bytes in [&pretty, &compact] {
        // Act
        let decoded = AdmittedCatalog::decode(bytes, &budget).unwrap();

        // Assert: formatting changes neither authority nor reservation ownership.
        assert_eq!(*decoded, catalog);
        assert!(budget.used() > 0);
        drop(decoded);
        assert_eq!(budget.used(), 0);
    }
}
