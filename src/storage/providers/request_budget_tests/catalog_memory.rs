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
    let mut catalog = WalPublicationCatalog::empty(epoch).unwrap();
    let mut previous = ChainLink::GENESIS;
    let entries: Vec<CatalogEntry> = (1..=INCIDENT_SEGMENTS)
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
        // Arrange: startup keeps the fenced catalog through recovery planning.
        let fixture = Fixture::new(dialect);
        fixture.seed_catalog(&incident_catalog(7));
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
        assert!(result.is_ok(), "{dialect:?}: {result:?}");
        let published = fixture.published_catalog();
        assert_eq!(published.head().id, suffix.id() - 1, "{dialect:?}");
        assert!(published.seq_floor() >= floor, "{dialect:?}");
        assert_eq!(fenced.head(), suffix.link(), "{dialect:?}");
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
