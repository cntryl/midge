use super::*;
use crate::metadata::Manifest;
use crate::runtime::hybrid_persistence::tests::{
    fixture_manifest_frontier, SharedRetirementFixture,
};
use crate::storage::cloud::{BlockingCloud, CloudStorage};
use crate::storage::HybridStorage;
use crate::wal::cloud_catalog::{CatalogEntry, WalPublicationCatalog};
use crate::wal::format::segment::SegmentBuilder;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

struct Fixture {
    shared: SharedRetirementFixture,
    cloud: Arc<CloudStorage>,
    storage: Arc<HybridStorage>,
    catalog: WalPublicationCatalog,
    first: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        let mut manifest = Manifest::default();
        fixture_manifest_frontier(&mut manifest, 0, 100);
        let shared = SharedRetirementFixture::new(&manifest);
        shared.publish(&manifest);
        let cloud = Arc::new(CloudStorage::with_mock());
        let storage = Arc::new(HybridStorage::with_policy(
            cloud.clone(),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        storage
            .configure_object_inventories(cloud.clone(), cloud.clone())
            .unwrap();
        storage.configure_maintenance_memory(32 * 1024 * 1024);
        let mut catalog = WalPublicationCatalog::empty(2).unwrap();
        let mut entries = Vec::new();
        let mut previous = ChainLink::GENESIS;
        let mut first = Vec::new();
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        let io = BlockingCloud::new(&cloud, &deadline);
        for id in 1..=4 {
            let record = crate::wal::WalRecord::new(
                crate::wal::WalOpKind::Put,
                Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"value")),
                id,
            );
            let (bytes, link) = SegmentBuilder::new(id, 1, previous).record(&record).seal();
            let key = crate::wal::segment_object_key(id, 1);
            let valid = crate::wal::cloud_segment::validate_segment_bytes(&key, &bytes).unwrap();
            entries.push(CatalogEntry::describing(&valid, &bytes));
            io.put_with_headers(&key, bytes.clone(), Vec::new())
                .unwrap();
            if id == 1 {
                first = bytes;
            }
            previous = link;
        }
        catalog.append_run(2, &entries).unwrap();
        catalog
            .retire_prefix(2, &entries[..3], crate::lease::SharedGenerationId::FIRST)
            .unwrap();
        let fixture = Self {
            shared,
            cloud,
            storage,
            catalog,
            first,
        };
        fixture.write_catalog(&fixture.catalog);
        fixture
    }

    fn write_catalog(&self, catalog: &WalPublicationCatalog) {
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        let io = BlockingCloud::new(&self.cloud, &deadline);
        for key in [
            crate::wal::cloud_catalog::OBJECT_KEY,
            crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
        ] {
            io.put_with_headers(key, catalog.encode().unwrap(), Vec::new())
                .unwrap();
        }
    }

    fn sweep(&self, cursor: InventoryCursor) -> MidgeResult<OrphanSweepProgress> {
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        sweep_retired_wal_within(
            self.shared.orphan_context(&self.storage, &deadline),
            cursor,
            8,
            |_| Ok(()),
        )
    }

    fn object(&self, epoch: u64, id: u64) -> Option<Vec<u8>> {
        BlockingCloud::new(
            &self.cloud,
            &OperationDeadline::from_budget(Duration::from_secs(5)),
        )
        .get_optional(&crate::wal::segment_object_key(id, epoch))
        .unwrap()
    }
}

#[test]
fn should_rediscover_retired_wal_when_anchor_committed_before_process_queue_was_lost() {
    // Arrange: valid catalog mutators produced this post-retirement image;
    // the replacement sweeper owns no original process-local delete queue.
    let fixture = Fixture::new();

    // Act
    let progress = fixture.sweep(InventoryCursor::default()).unwrap();

    // Assert
    assert_eq!(progress.deleted, 3);
    assert!(progress.pages <= 1 && progress.probes <= 8);
    for id in 1..=3 {
        assert!(fixture.object(1, id).is_none());
    }
    assert!(fixture.object(1, 4).is_some());
    let catalog = load_and_repair_catalog_within(
        &fixture.storage,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    )
    .unwrap()
    .unwrap();
    assert_eq!(catalog.catalog.anchor(), fixture.catalog.anchor());
    assert!(catalog.catalog.entry(4).is_some());
}

#[test]
fn should_retain_future_epoch_and_inconsistent_headers_below_anchor() {
    // Arrange
    let fixture = Fixture::new();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let io = BlockingCloud::new(&fixture.cloud, &deadline);
    let future = SegmentHeader {
        id: 1,
        epoch: 3,
        holder: [0; 16],
        first_seq: 1,
        prev: ChainLink::GENESIS,
    }
    .encode();
    io.put_with_headers(
        &crate::wal::segment_object_key(1, 3),
        future.to_vec(),
        Vec::new(),
    )
    .unwrap();
    let wrong = SegmentHeader {
        id: 2,
        epoch: 1,
        holder: [0; 16],
        first_seq: 999,
        prev: ChainLink::GENESIS,
    }
    .encode();
    io.put_with_headers(
        &crate::wal::segment_object_key(2, 1),
        wrong.to_vec(),
        Vec::new(),
    )
    .unwrap();

    // Act
    fixture.sweep(InventoryCursor::default()).unwrap();

    // Assert
    assert!(fixture.object(3, 1).is_some());
    assert!(fixture.object(1, 2).is_some());
    assert!(fixture.object(1, 4).is_some());
    assert!(fixture.object(1, 1).is_none());
}

#[test]
fn should_revisit_late_retired_put_without_restoring_catalog_membership() {
    // Arrange
    let fixture = Fixture::new();
    let first = fixture.sweep(InventoryCursor::default()).unwrap();
    assert!(fixture.object(1, 1).is_none());

    // Act: a predecessor's admitted PUT can finish after the successor sweep.
    BlockingCloud::new(
        &fixture.cloud,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    )
    .put_with_headers(
        &crate::wal::segment_object_key(1, 1),
        fixture.first.clone(),
        Vec::new(),
    )
    .unwrap();
    fixture.sweep(first.cursor).unwrap();

    // Assert
    assert!(fixture.object(1, 1).is_none());
    assert!(fixture.object(1, 4).is_some());
    assert!(fixture.catalog.entry(1).is_none());
}

#[test]
fn should_retain_wal_when_catalog_epoch_changes_before_conditional_delete() {
    // Arrange
    let fixture = Fixture::new();
    let mut successor = fixture.catalog.clone();
    successor.fence_to(3).unwrap();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let mut validations = 0;

    // Act: the foreign epoch uses its own provider path, outside this
    // process's mutation mutex, between header proof and the final check.
    let result = sweep_retired_wal_within(
        fixture.shared.orphan_context(&fixture.storage, &deadline),
        InventoryCursor::default(),
        8,
        |_| {
            validations += 1;
            if validations == 2 {
                fixture.write_catalog(&successor);
            }
            Ok(())
        },
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))));
    assert!(fixture.object(1, 1).is_some());
}
