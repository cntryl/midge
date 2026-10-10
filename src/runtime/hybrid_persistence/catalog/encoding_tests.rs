use super::*;
use crate::wal::cloud_catalog::PublishedWalSegment;

fn populated_catalog() -> WalPublicationCatalog {
    let mut catalog = WalPublicationCatalog::empty(7).unwrap();
    for segment_id in 1..=4096 {
        let segment = PublishedWalSegment::from_validated_bytes(segment_id, segment_id, 7, b"wal");
        catalog.publish(7, segment).unwrap();
    }
    catalog.raise_sequence_floor(7, 123).unwrap();
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
        Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap()),
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
    let mut catalog = populated_catalog();
    catalog.segments.get_mut(&1).unwrap().object_key = "foreign".to_string();
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let result = AdmittedEncoding::new(&catalog, &budget);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(budget.used(), 0);
}

#[test]
#[ignore = "bounded codec timing probe; run explicitly with --release --ignored --nocapture"]
fn should_report_catalog_codec_cost() {
    // Arrange
    let catalog = populated_catalog();
    let budget = ResourceBudget::new(64 * 1024 * 1024);
    let pretty = serde_json::to_vec_pretty(&catalog).unwrap();
    let compact = serde_json::to_vec(&catalog).unwrap();
    let iterations = 256;

    // Act
    for repeat in 0..6 {
        for (name, compact_encoding) in if repeat % 2 == 0 {
            [("pretty", false), ("compact", true)]
        } else {
            [("compact", true), ("pretty", false)]
        } {
            let started = std::time::Instant::now();
            for _ in 0..iterations {
                if compact_encoding {
                    std::hint::black_box(AdmittedEncoding::new(&catalog, &budget).unwrap());
                } else {
                    catalog.validate().unwrap();
                    let mut count = crate::common::resource_budget::ByteCounter::default();
                    serde_json::to_writer_pretty(&mut count, &catalog).unwrap();
                    let _memory = budget.reserve(count.0, "codec probe").unwrap();
                    let mut bytes = Vec::with_capacity(count.0);
                    serde_json::to_writer_pretty(&mut bytes, &catalog).unwrap();
                    std::hint::black_box(bytes);
                }
            }
            eprintln!("catalog_codec repeat={repeat} format={name} entries=4096 iterations={iterations} elapsed_us={}", started.elapsed().as_micros());
        }
        for (name, bytes) in [("pretty", &pretty), ("compact", &compact)] {
            let started = std::time::Instant::now();
            for _ in 0..iterations {
                std::hint::black_box(AdmittedCatalog::decode(bytes, &budget).unwrap());
            }
            eprintln!("catalog_decode repeat={repeat} format={name} bytes={} iterations={iterations} elapsed_us={}", bytes.len(), started.elapsed().as_micros());
        }
    }

    // Assert
    assert_eq!(WalPublicationCatalog::decode(&pretty).unwrap(), catalog);
    assert_eq!(WalPublicationCatalog::decode(&compact).unwrap(), catalog);
    assert_eq!(budget.used(), 0);
}
