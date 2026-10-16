use super::*;
use crate::wal::cloud_catalog::CatalogEntry;
use crate::wal::format::ChainLink;

fn populated_catalog() -> WalPublicationCatalog {
    populated_catalog_with_segments(4096)
}

fn populated_catalog_with_segments(count: u64) -> WalPublicationCatalog {
    let mut catalog = WalPublicationCatalog::empty(7).unwrap();
    let mut prev = ChainLink::GENESIS;
    let entries: Vec<CatalogEntry> = (1..=count)
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

fn catalog_with_family_frontiers() -> WalPublicationCatalog {
    let mut builder = crate::wal::format::segment::SegmentBuilder::new(1, 7, ChainLink::GENESIS);
    for family in 1..=512_u32 {
        let mut record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            u64::from(family),
        );
        record.cf_id = family;
        builder = builder.record(&record);
    }
    let (bytes, _) = builder.seal();
    let entry = CatalogEntry::from_sealed_bytes(&bytes).unwrap();
    let mut catalog = WalPublicationCatalog::empty(7).unwrap();
    catalog.append_run(7, &[entry]).unwrap();
    catalog
}

#[test]
fn should_retain_admission_for_every_decoded_family_frontier() {
    // Arrange
    let original = catalog_with_family_frontiers();
    let bytes = original.encode().unwrap();
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let decoded = AdmittedCatalog::decode(&bytes, &budget).unwrap();

    // Assert
    assert_eq!(*decoded, original);
    let frontier_bytes =
        decoded.entries()[0].facts().max_seq_per_cf.capacity() * std::mem::size_of::<(u32, u64)>();
    assert!(budget.used() >= frontier_bytes + std::mem::size_of::<CatalogEntry>());
    drop(decoded);
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_release_preflight_scratch_when_frontiers_exceed_admission() {
    // Arrange
    let bytes = catalog_with_family_frontiers().encode().unwrap();
    let bound = WalPublicationCatalog::decode_allocation_bound(&bytes).unwrap();
    assert!(bound > bytes.len() * 2 + 4096);
    let budget = ResourceBudget::new(bound - 1);

    // Act
    let result = AdmittedCatalog::decode(&bytes, &budget);

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_preserve_catalog_authority_with_escaped_schema_identifiers() {
    // Arrange
    let original = populated_catalog();
    let bytes = String::from_utf8(original.encode().unwrap())
        .unwrap()
        .replace("\"segments\"", "\"\\u0073egments\"");
    let budget = ResourceBudget::new(8 * 1024 * 1024);

    // Act
    let decoded = AdmittedCatalog::decode(bytes.as_bytes(), &budget).unwrap();

    // Assert
    assert_eq!(*decoded, original);
    drop(decoded);
    assert_eq!(budget.used(), 0);
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

#[test]
fn should_decode_unescaped_catalog_without_admitting_parser_scratch() {
    // Arrange: escape-free JSON borrows every string from the input.
    let bytes = populated_catalog().encode().unwrap();
    let bound = WalPublicationCatalog::decode_allocation_bound(&bytes).unwrap();
    assert!(bound < bytes.len() * 2);
    let budget = ResourceBudget::new(bound);

    // Act
    let decoded = AdmittedCatalog::decode(&bytes, &budget);

    // Assert
    assert_eq!(*decoded.unwrap(), populated_catalog());
}

#[test]
fn should_refuse_escaped_document_before_parsing_when_scratch_exceeds_budget() {
    // Arrange: a long raw prefix then one escape. The parser copies the
    // prefix, then doubles its scratch while the old buffer is still live.
    let key = "a".repeat(1024 * 1024);
    let version = crate::wal::cloud_catalog::FORMAT_VERSION;
    let bytes = format!("{{\"format_version\":{version},\"{key}\\n\":0}}").into_bytes();
    let budget = ResourceBudget::new(bytes.len() * 2 + 8192);

    // Act
    let result = AdmittedCatalog::decode(&bytes, &budget);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::ResourceLimit(_))),
        "{result:?}"
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_retain_only_decoded_capacity_when_decode_peak_is_released() {
    // Arrange
    let catalog = populated_catalog();
    let bytes = catalog.encode().unwrap();
    let peak = WalPublicationCatalog::decode_allocation_bound(&bytes).unwrap();
    let budget = ResourceBudget::new(64 * 1024 * 1024);

    // Act
    let decoded = AdmittedCatalog::decode(&bytes, &budget).unwrap();

    // Assert
    let retained = decoded.retained_allocation_bytes();
    assert!(retained < peak);
    assert_eq!(budget.used(), retained);
}

fn chained_entry(head: ChainLink, epoch: u64) -> CatalogEntry {
    let id = head.id + 1;
    let (bytes, _) = crate::wal::format::segment::SegmentBuilder::new(id, epoch, head)
        .record(&crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"new"),
            Some(bytes::Bytes::from_static(b"value")),
            head.last_seq + 1,
        ))
        .seal();
    CatalogEntry::from_sealed_bytes(&bytes).unwrap()
}

#[test]
fn should_report_fencing_and_refusal_before_admission_when_maintenance_pool_is_full() {
    // Arrange: compaction holds the rest of the pool.
    let catalog = populated_catalog();
    let budget = ResourceBudget::new(16 * 1024 * 1024).with_contention_errors();
    let mut admitted = AdmittedCatalog::decode(&catalog.encode().unwrap(), &budget).unwrap();
    let _compaction = budget
        .reserve(budget.limit() - budget.used(), "compaction")
        .unwrap();
    let stale = chained_entry(catalog.head(), 6);
    let unchained = chained_entry(ChainLink::GENESIS, 7);

    // Act
    let fenced = admitted.append_run(6, &[stale]);
    let refused = admitted.append_run(7, &[unchained]);

    // Assert
    assert!(
        matches!(fenced, Err(super::super::PublicationError::Fenced(_))),
        "{fenced:?}"
    );
    assert!(
        matches!(refused, Err(super::super::PublicationError::Refused(_))),
        "{refused:?}"
    );
    assert_eq!(*admitted, catalog);
}
