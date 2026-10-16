//! Incident-sized WAL catalogs (#886) through each native provider's receipt
//! path, at Fitz's unchanged maintenance pool. Receipts keep the confirmed
//! authority cached between mutations, which the mock provider never does.

use super::*;
use crate::runtime::hybrid_persistence::tests::{
    fixture_manifest_frontier, SharedRetirementFixture,
};
use crate::wal::cloud_catalog::{
    CatalogEntry, WalPublicationCatalog, MIRROR_OBJECT_KEY, OBJECT_KEY,
};

/// Fitz's maintenance pool at 512 MiB RAM.
const FITZ_MAINTENANCE_POOL: usize = 53_687_091;
/// The incident's largest qualified catalog: about 10.76 MB encoded.
const INCIDENT_SEGMENTS: u64 = 32_768;

fn incident_catalog(epoch: u64) -> WalPublicationCatalog {
    catalog_with_segments(epoch, INCIDENT_SEGMENTS)
}

fn catalog_with_segments(epoch: u64, count: u64) -> WalPublicationCatalog {
    let mut catalog = WalPublicationCatalog::empty(epoch).unwrap();
    let mut previous = ChainLink::GENESIS;
    let entries: Vec<CatalogEntry> = (1..=count)
        .map(|id| {
            let (bytes, link) = SegmentBuilder::new(id, epoch, previous)
                .record(&crate::wal::WalRecord::new(
                    crate::wal::WalOpKind::Put,
                    bytes::Bytes::from_static(b"key"),
                    Some(bytes::Bytes::from_static(b"value")),
                    id,
                ))
                .seal();
            previous = link;
            CatalogEntry::from_sealed_bytes(&bytes).unwrap()
        })
        .collect();
    catalog.append_run(epoch, &entries).unwrap();
    catalog
}

impl Fixture {
    /// Install `catalog` as both copies, as an earlier writer left them.
    fn seed_catalog(&self, catalog: &WalPublicationCatalog) {
        let bytes = catalog.encode().unwrap();
        let mut state = self.state.lock().unwrap();
        for key in [OBJECT_KEY, MIRROR_OBJECT_KEY] {
            state
                .objects
                .insert(self.dialect.path(key), (bytes.clone(), 1));
        }
    }

    /// Both copies, which must agree.
    fn published_catalog(&self) -> WalPublicationCatalog {
        let (primary, _) = self.object(OBJECT_KEY);
        let (mirror, _) = self.object(MIRROR_OBJECT_KEY);
        assert_eq!(primary, mirror, "{:?}", self.dialect);
        WalPublicationCatalog::decode(&primary).unwrap()
    }
}

fn next_entry(head: ChainLink, id: u64, epoch: u64) -> CatalogEntry {
    let (bytes, _) = SegmentBuilder::new(id, epoch, head)
        .record(&crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"new"),
            Some(bytes::Bytes::from_static(b"value")),
            id,
        ))
        .seal();
    CatalogEntry::from_sealed_bytes(&bytes).unwrap()
}

fn catalog_with_many_frontiers(manifest: &mut crate::metadata::Manifest) -> WalPublicationCatalog {
    let mut builder = SegmentBuilder::new(1, 1, ChainLink::GENESIS);
    for sequence in 1..=512 {
        let cf_id = manifest.create_column_family(format!("family-{sequence}"));
        fixture_manifest_frontier(manifest, cf_id, sequence);
        let mut record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            sequence,
        );
        record.cf_id = cf_id;
        builder = builder.record(&record);
    }
    let (bytes, _) = builder.seal();
    let mut catalog = WalPublicationCatalog::empty(1).unwrap();
    catalog
        .append_run(1, &[CatalogEntry::from_sealed_bytes(&bytes).unwrap()])
        .unwrap();
    catalog
}

#[test]
fn should_admit_nested_retirement_evidence_before_changing_authority() {
    // Arrange
    let fixture = Fixture::new(Dialect::S3);
    let mut manifest = crate::metadata::Manifest::default();
    let catalog = catalog_with_many_frontiers(&mut manifest);
    fixture.seed_catalog(&catalog);
    let storage = fixture.storage();
    let budget = storage.configure_maintenance_memory(1024 * 1024);
    let fenced = storage.fence_cloud_wal_catalog(2).unwrap();
    let bound = WalPublicationCatalog::decode_allocation_bound(&fenced.encode().unwrap()).unwrap();
    drop(fenced);
    let held = budget
        .reserve(
            budget.limit() - budget.used() - bound,
            "competing maintenance",
        )
        .unwrap();
    let retirement = SharedRetirementFixture::new(&manifest);
    retirement.publish(&manifest);
    let authority = retirement.authority();
    let before = fixture.published_catalog();

    // Act
    let refused = storage.prune_cloud_wal_segments_within(
        &[(1, 512)],
        &authority,
        2,
        &OperationDeadline::unbounded(),
    );
    let after_refusal = fixture.published_catalog();
    drop(held);
    let retried = storage.prune_cloud_wal_segments_within(
        &[(1, 512)],
        &authority,
        2,
        &OperationDeadline::unbounded(),
    );

    // Assert
    assert!(
        matches!(refused, Err(crate::common::MidgeError::ResourceLimit(_))),
        "{refused:?}"
    );
    assert_eq!(after_refusal, before);
    assert!(retried.is_ok(), "{retried:?}");
    assert_eq!(fixture.published_catalog().anchor().id, 1);
}

#[test]
fn should_publish_incident_catalog_within_fitz_pool_when_authority_is_cached_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange
        let fixture = Fixture::new(dialect);
        let catalog = incident_catalog(7);
        fixture.seed_catalog(&catalog);
        let storage = fixture.storage();
        storage.configure_maintenance_memory(FITZ_MAINTENANCE_POOL);
        drop(storage.fence_cloud_wal_catalog(8).unwrap());
        let entry = next_entry(catalog.head(), INCIDENT_SEGMENTS + 1, 8);

        // Act
        let result = storage.publish_wal_segments(
            std::slice::from_ref(&entry),
            8,
            &OperationDeadline::unbounded(),
        );

        // Assert
        assert!(result.is_ok(), "{dialect:?}: {result:?}");
        let published = fixture.published_catalog();
        assert_eq!(published.head(), entry.link(), "{dialect:?}");
        assert_eq!(published.fencing_epoch(), 8, "{dialect:?}");
    }
}

#[test]
fn should_publish_repeatedly_across_incident_vector_capacity_on_every_provider() {
    for dialect in Dialect::ALL {
        verify_repeated_incident_publication(dialect, false);
    }
}

#[test]
fn should_publish_repeatedly_across_incident_vector_capacity_without_receipts() {
    verify_repeated_incident_publication(Dialect::S3, true);
}

fn verify_repeated_incident_publication(dialect: Dialect, omit_receipts: bool) {
    // Arrange
    let fixture = Fixture::new(dialect);
    fixture.state.lock().unwrap().omit_receipts = omit_receipts;
    let catalog = incident_catalog(7);
    fixture.seed_catalog(&catalog);
    let storage = fixture.storage();
    storage.configure_maintenance_memory(FITZ_MAINTENANCE_POOL);
    drop(storage.fence_cloud_wal_catalog(8).unwrap());
    let mut head = catalog.head();

    // Act
    for id in INCIDENT_SEGMENTS + 1..=INCIDENT_SEGMENTS + 4 {
        let entry = next_entry(head, id, 8);
        let result = storage.publish_wal_segments(
            std::slice::from_ref(&entry),
            8,
            &OperationDeadline::unbounded(),
        );
        assert!(result.is_ok(), "{dialect:?}, segment {id}: {result:?}");
        head = entry.link();
    }

    // Assert
    let published = fixture.published_catalog();
    assert_eq!(published.head(), head, "{dialect:?}");
    assert_eq!(published.entries().len(), 32_772, "{dialect:?}");
    assert_eq!(published.fencing_epoch(), 8, "{dialect:?}");
}

#[test]
fn should_retire_incident_catalog_within_fitz_pool_when_authority_is_cached_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange: fencing leaves both receipted copies in the catalog cache.
        let fixture = Fixture::new(dialect);
        fixture.seed_catalog(&incident_catalog(1));
        let storage = fixture.storage();
        storage.configure_maintenance_memory(FITZ_MAINTENANCE_POOL);
        drop(storage.fence_cloud_wal_catalog(2).unwrap());
        let mut manifest = crate::metadata::Manifest::default();
        let retirement = SharedRetirementFixture::new(&manifest);
        fixture_manifest_frontier(&mut manifest, 0, 1);
        retirement.publish(&manifest);
        let authority = retirement.authority();

        // Act
        let result = storage.prune_cloud_wal_segments_within(
            &[(1, 1)],
            &authority,
            2,
            &OperationDeadline::unbounded(),
        );

        // Assert
        assert!(result.is_ok(), "{dialect:?}: {result:?}");
        let published = fixture.published_catalog();
        assert_eq!(published.anchor().id, 1, "{dialect:?}");
        assert_eq!(
            published.entries().len(),
            usize::try_from(INCIDENT_SEGMENTS).unwrap() - 1,
            "{dialect:?}"
        );
    }
}

#[test]
fn should_set_aside_incident_suffix_within_fitz_pool_while_startup_holds_fenced_catalog_on_every_provider(
) {
    for dialect in Dialect::ALL {
        for count in [INCIDENT_SEGMENTS, INCIDENT_SEGMENTS + 1] {
            // Arrange: startup keeps the fenced catalog through recovery planning.
            let fixture = Fixture::new(dialect);
            fixture.seed_catalog(&catalog_with_segments(7, count));
            let storage = fixture.storage();
            storage.configure_maintenance_memory(FITZ_MAINTENANCE_POOL);
            let fenced = storage.fence_cloud_wal_catalog(8).unwrap();
            let suffix = fenced.entries().last().unwrap().clone();
            let floor = suffix.max_sequence() + 1;

            // Act
            let result = storage.set_aside_catalog_suffix_with_authority_within(
                8,
                Some(suffix.id()),
                std::slice::from_ref(&suffix),
                floor,
                &|| Ok(()),
                &OperationDeadline::unbounded(),
            );

            // Assert
            assert!(result.is_ok(), "{dialect:?}, {count} segments: {result:?}");
            let published = fixture.published_catalog();
            assert_eq!(published.head().id, suffix.id() - 1, "{dialect:?}");
            assert!(published.seq_floor() >= floor, "{dialect:?}");
            assert_eq!(fenced.head(), suffix.link(), "{dialect:?}");
        }
    }
}

#[test]
fn should_keep_cached_authority_when_catalog_refuses_publication_on_every_provider() {
    for dialect in Dialect::ALL {
        // Arrange
        let fixture = Fixture::new(dialect);
        let storage = fixture.storage();
        storage.fence_cloud_wal_catalog(7).unwrap();
        let previous = fixture.publish(&storage, 1, ChainLink::GENESIS);
        let unchained = next_entry(ChainLink::GENESIS, 3, 7);
        let refused = storage.publish_wal_segments(
            std::slice::from_ref(&unchained),
            7,
            &OperationDeadline::unbounded(),
        );
        assert!(
            matches!(
                refused,
                Err(crate::runtime::hybrid_persistence::PublicationError::Refused(_))
            ),
            "{dialect:?}: {refused:?}"
        );
        fixture.state.lock().unwrap().requests.clear();

        // Act
        fixture.publish(&storage, 2, previous);

        // Assert
        let requests = fixture.state.lock().unwrap().requests.clone();
        assert_eq!(
            requests.len(),
            3,
            "{dialect:?}: one WAL PUT and two catalog PUTs: {requests:?}"
        );
    }
}

#[test]
fn should_keep_cached_authority_when_decode_admission_is_temporarily_refused() {
    // Arrange
    let fixture = Fixture::new(Dialect::S3);
    let storage = fixture.storage();
    let budget = storage.configure_maintenance_memory(1024 * 1024);
    drop(storage.fence_cloud_wal_catalog(7).unwrap());
    let held = budget
        .reserve(budget.limit() - budget.used(), "other maintenance")
        .unwrap();
    let entry = next_entry(ChainLink::GENESIS, 1, 7);

    // Act
    let refused = storage.publish_wal_segments(
        std::slice::from_ref(&entry),
        7,
        &OperationDeadline::unbounded(),
    );
    drop(held);
    fixture.state.lock().unwrap().requests.clear();
    let retry = storage.publish_wal_segments(
        std::slice::from_ref(&entry),
        7,
        &OperationDeadline::unbounded(),
    );

    // Assert
    assert!(
        matches!(
            refused,
            Err(crate::runtime::hybrid_persistence::PublicationError::Retriable(_))
        ),
        "{refused:?}"
    );
    assert!(retry.is_ok(), "{retry:?}");
    let requests = fixture.state.lock().unwrap().requests.clone();
    assert_eq!(requests.len(), 2, "only the two catalog PUTs: {requests:?}");
    assert_eq!(fixture.published_catalog().head(), entry.link());
}
