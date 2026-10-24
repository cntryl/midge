//! Tests for runtime-owned hybrid persistence.
//!
//! These exercise WAL catalog publication and takeover, manifest-coverage WAL
//! pruning, and SST publication proofs. They live beside the code that owns
//! those formats rather than inside the format-neutral storage backend.

use super::*;
use crate::storage::test_support::StorageBackendTestExt;

use crate::sst::SstFactory;

use crate::storage::cloud::{CloudBackend, CloudStorage, MockCloudBackend};

use crate::storage::hybrid::backend::HybridStorage;

use crate::storage::hybrid::backend::HybridQueueLimits;

use crate::storage::{StorageCallback, StorageEvent, StorageOutcome};

use parking_lot::Mutex;

use bytes::Bytes;

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Barrier,
};

use crate::types::EntryType;
use std::time::{Duration, Instant};

/// Test-only entry points into cloud persistence.
///
/// These exist so tests can drive one segment, one manifest or one SST at a
/// time. They live here rather than on `CloudPersistence` itself so production
/// keeps exactly the entry points it calls - the batched
/// `prune_cloud_wal_segments_within` and nothing narrower.
impl CloudPersistence {
    /// Verify one segment's upload and publish it alone: the two production
    /// calls the runtime makes, without its contiguous selection.
    pub(crate) fn publish_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        source: &crate::io::FsLocation,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        let upload =
            self.verify_uploaded_wal_segment(segment_id, expected_max_sequence, source, deadline)?;
        Ok(self.publish_wal_segments(&[upload], fencing_epoch, deadline)?)
    }

    pub(crate) fn verify_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<(), String> {
        let entry = authoritative_wal_entry(self, segment_id)?;
        if entry.max_sequence() != expected_max_sequence {
            return Err(format!(
                "cloud WAL catalog segment {segment_id} max sequence {} does not match expected {expected_max_sequence}",
                entry.max_sequence()
            ));
        }
        validate_remote_wal(self, &entry, &crate::common::OperationDeadline::unbounded())
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn verify_manifest_cloud_objects(&self, manifest: &Manifest) -> Result<(), String> {
        self.verify_manifest_cloud_objects_within(
            manifest,
            &crate::common::OperationDeadline::unbounded(),
        )
        .map_err(|error| error.to_string())
    }

    pub(crate) fn verify_manifest_cloud_objects_within(
        &self,
        manifest: &Manifest,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        for file in &manifest.files {
            validate_remote_sst_within(self, file, deadline)?;
        }
        Ok(())
    }

    pub(crate) fn write_sst_object(&self, sst_name: &str, data: &[u8]) -> MidgeResult<()> {
        self.write_sst_object_within(
            sst_name,
            data,
            &crate::common::OperationDeadline::unbounded(),
        )
    }

    pub(crate) fn write_sst_object_within(
        &self,
        sst_name: &str,
        data: &[u8],
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        self.write_sst_object_with_proof(sst_name, data, deadline)
            .map(|_| ())
    }

    pub(crate) fn write_sst_object_with_proof(
        &self,
        sst_name: &str,
        data: &[u8],
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<GuardedObjectProof> {
        let expected_size = data.len() as u64;
        let expected_crc = crc32c::crc32c(data);
        validate_sst_object_bytes(sst_name, expected_size, None, None, data)
            .map_err(MidgeError::Internal)?;
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_upload", |_| Err(
            MidgeError::Internal("failpoint: cloud SST upload failed".to_string())
        ));

        let key = crate::cloud_layout::object_key(sst_name);
        self.publish_immutable_object_within(&key, data, deadline)?;
        let proof = self
            .remote_object_proof_within(&key, deadline)
            .map_err(|error| contextualize_cloud_error(error, "cloud SST readback failed"))?;
        validate_sst_object_bytes(
            sst_name,
            expected_size,
            Some(expected_crc),
            None,
            proof.bytes(),
        )
        .map_err(MidgeError::Internal)?;
        Ok(GuardedObjectProof::range_identity(
            self.remote_sst_backend(),
            key,
            proof.metadata().clone(),
        ))
    }
}

#[test]
fn should_preserve_timeout_variant_when_adding_cloud_publication_context() {
    // Arrange
    let timeout = MidgeError::Timeout("remote CAS timed out".to_string());

    // Act
    let contextualized = contextualize_cloud_error(timeout, "cloud WAL catalog publication failed");

    // Assert
    assert!(matches!(
        contextualized,
        MidgeError::Timeout(message)
            if message.contains("catalog publication")
                && message.contains("remote CAS timed out")
    ));
}

#[test]
fn should_reject_wal_catalog_publication_from_stale_epoch_after_takeover() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create stale publisher WAL dir");
    let segment_id = 1;
    let max_sequence = 11;
    let bytes = valid_wal_bytes(segment_id, max_sequence);
    let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&local_path, &bytes).expect("write stale publisher local WAL");
    let object_key = crate::wal::segment_object_key(segment_id, 1);
    write_cloud_object(&storage, &object_key, bytes);
    storage
        .fence_cloud_wal_catalog(3)
        .expect("new lease fences WAL catalog");

    // Act
    let result = storage.publish_remote_wal_segment(
        segment_id,
        max_sequence,
        &crate::io::FsLocation::host_file(&local_path),
        2,
        &crate::common::OperationDeadline::unbounded(),
    );

    // Assert
    let error = result.expect_err("stale lease epoch must not publish WAL authority");
    assert!(matches!(
        error,
        crate::common::MidgeError::Fenced(message)
            if message.contains("requires fencing epoch 3")
    ));
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read winning WAL catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode winning WAL catalog");
    assert!(catalog.entry(segment_id).is_none());
    assert_cloud_object_exists(&storage, &object_key);
}

#[test]
fn should_converge_catalog_copies_after_publication() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create mirrored catalog WAL dir");
    let segment_id = 1;
    let max_sequence = 11;
    let bytes = valid_wal_bytes(segment_id, max_sequence);
    let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&local_path, &bytes).expect("write mirrored catalog local WAL");
    let object_key = crate::wal::segment_object_key(segment_id, 1);
    write_cloud_object(&storage, &object_key, bytes);

    // Act
    storage
        .publish_remote_wal_segment(
            segment_id,
            max_sequence,
            &crate::io::FsLocation::host_file(&local_path),
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
        .expect("publish WAL through mirrored catalog");

    // Assert
    let catalog = assert_wal_catalog_copies_match(&storage);
    assert_eq!(catalog.fencing_epoch(), 2);
    assert!(catalog.entry(segment_id).is_some());
}

#[test]
fn should_converge_catalog_copies_after_takeover() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();

    // Act
    storage
        .fence_cloud_wal_catalog(3)
        .expect("advance mirrored catalog authority");

    // Assert
    let catalog = assert_wal_catalog_copies_match(&storage);
    assert_eq!(catalog.fencing_epoch(), 3);
}

#[test]
fn should_not_publish_losing_mirror_when_initial_catalog_cas_loses() {
    // Arrange
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let winning_catalog = crate::wal::cloud_catalog::WalPublicationCatalog::empty(7)
        .expect("create winning catalog")
        .encode()
        .expect("encode winning catalog");
    let racing_cloud = Arc::new(WinningInitialCatalogCasBackend {
        inner: Arc::clone(&mock_cloud),
        winning_catalog: winning_catalog.clone(),
        inject_winner: AtomicBool::new(true),
    });
    let cloud = Arc::new(CloudStorage::new(racing_cloud, String::new()));
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));

    // Act
    let error = storage
        .fence_cloud_wal_catalog(2)
        .expect_err("losing initial catalog CAS must fail");

    // Assert: the winner's later epoch owns the catalog.
    assert!(
        matches!(error, crate::common::MidgeError::Fenced(_)),
        "{error:?}"
    );
    assert_eq!(
        storage
            .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
            .expect("read winning primary catalog")
            .bytes(),
        winning_catalog
    );
    assert!(
        storage
            .remote_object_proof_optional(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
            .expect("check losing mirror")
            .is_none(),
        "a draft that lost primary authority must never be published to the mirror"
    );
}

#[test]
fn should_refuse_publication_whose_first_segment_does_not_link_the_catalog_head() {
    // Arrange: the catalog publishes segment 1; this writer's segment 2
    // links a different segment 1.
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create unchained publisher WAL dir");
    let put = |seq| {
        crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            seq,
        )
    };
    let (first, _) = crate::wal::format::segment::SegmentBuilder::new(
        1,
        1,
        crate::wal::format::ChainLink::GENESIS,
    )
    .record(&put(1))
    .seal();
    let stranger = crate::wal::format::ChainLink {
        id: 1,
        last_seq: 1,
        digest: [0x5a; 16],
    };
    let (second, _) = crate::wal::format::segment::SegmentBuilder::new(2, 1, stranger)
        .record(&put(2))
        .seal();
    let mut locations = Vec::new();
    for (segment_id, bytes) in [(1, first), (2, second)] {
        let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
        std::fs::write(&local_path, &bytes).expect("write unchained publisher local WAL");
        write_cloud_object(
            &storage,
            &crate::wal::segment_object_key(segment_id, 1),
            bytes,
        );
        locations.push(crate::io::FsLocation::host_file(&local_path));
    }
    storage
        .publish_remote_wal_segment(
            1,
            1,
            &locations[0],
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
        .expect("publish the catalog head");

    // Act
    let result = storage.publish_remote_wal_segment(
        2,
        2,
        &locations[1],
        2,
        &crate::common::OperationDeadline::unbounded(),
    );

    // Assert: no retry can chain it, and the catalog keeps only segment 1.
    assert!(
        matches!(&result, Err(crate::common::MidgeError::Corruption(message)) if message.contains("catalog head")),
        "{result:?}"
    );
    let catalog = assert_wal_catalog_copies_match(&storage);
    assert_eq!(
        catalog
            .entries()
            .iter()
            .map(crate::wal::cloud_catalog::CatalogEntry::id)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
fn should_report_fenced_when_catalog_authority_changes_before_publication() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create takeover race test dir");
    let (pausing_cloud, storage) = hybrid_with_pausing_catalog_cloud();

    let segment_id = 1;
    let max_sequence = 11;
    let bytes = valid_wal_bytes(segment_id, max_sequence);
    let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&local_path, &bytes).expect("write publisher local WAL");
    let object_key = crate::wal::segment_object_key(segment_id, 1);
    write_cloud_object(&storage, &object_key, bytes);

    pausing_cloud.pause_next_catalog_compare_exchange();
    let publisher_storage = Arc::clone(&storage);
    let publisher = std::thread::spawn(move || {
        publisher_storage.publish_remote_wal_segment(
            segment_id,
            max_sequence,
            &crate::io::FsLocation::host_file(&local_path),
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
    });
    pausing_cloud.wait_for_catalog_compare_exchange();
    // The successor is another process over the same bucket.
    let successor = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        Arc::new(CloudStorage::new(pausing_cloud.clone(), String::new())),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    successor
        .fence_cloud_wal_catalog(3)
        .expect("takeover must advance catalog authority");

    // Act
    pausing_cloud.release_catalog_compare_exchange();
    let error = publisher
        .join()
        .expect("publisher thread must not panic")
        .expect_err("stale publisher must lose catalog authority");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::Fenced(_)));
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read takeover catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode takeover catalog");
    assert_eq!(catalog.fencing_epoch(), 3);
    assert!(catalog.entry(segment_id).is_none());
    assert_cloud_object_exists(&storage, &object_key);
}

#[test]
fn should_bound_wal_catalog_lock_wait_by_publication_deadline() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create bounded catalog lock test dir");
    let (pausing_cloud, storage) = hybrid_with_pausing_catalog_cloud();

    let holding_segment_id = 1;
    let holding_max_sequence = 11;
    let holding_bytes = valid_wal_bytes(holding_segment_id, holding_max_sequence);
    let holding_local_path = tmp
        .path()
        .join(crate::wal::segment_file_name(holding_segment_id));
    std::fs::write(&holding_local_path, &holding_bytes).expect("write lock-holding local WAL");
    write_cloud_object(
        &storage,
        &crate::wal::segment_object_key(holding_segment_id, 1),
        holding_bytes,
    );

    let waiting_segment_id = 2;
    let waiting_max_sequence = 12;
    let waiting_bytes = valid_wal_bytes(waiting_segment_id, waiting_max_sequence);
    let waiting_local_path = tmp
        .path()
        .join(crate::wal::segment_file_name(waiting_segment_id));
    std::fs::write(&waiting_local_path, &waiting_bytes).expect("write lock-waiting local WAL");
    write_cloud_object(
        &storage,
        &crate::wal::segment_object_key(waiting_segment_id, 1),
        waiting_bytes,
    );

    pausing_cloud.pause_next_catalog_compare_exchange();
    let publisher_storage = Arc::clone(&storage);
    let holder = std::thread::spawn(move || {
        publisher_storage.publish_remote_wal_segment(
            holding_segment_id,
            holding_max_sequence,
            &crate::io::FsLocation::host_file(&holding_local_path),
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
    });
    pausing_cloud.wait_for_catalog_compare_exchange();

    // Act
    let started = Instant::now();
    let waiting_result = storage.publish_remote_wal_segment(
        waiting_segment_id,
        waiting_max_sequence,
        &crate::io::FsLocation::host_file(&waiting_local_path),
        2,
        &crate::common::OperationDeadline::from_budget(Duration::from_millis(100)),
    );
    let elapsed = started.elapsed();
    pausing_cloud.release_catalog_compare_exchange();
    let holding_result = holder.join().expect("lock holder thread must not panic");

    // Assert
    holding_result.expect("lock-holding publication must finish after release");
    assert!(
        matches!(
            &waiting_result,
            Err(crate::common::MidgeError::Timeout(message))
                if message.contains("waiting to mutate the cloud WAL catalog")
        ),
        "bounded publication must time out waiting for the catalog lock: {waiting_result:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "catalog lock wait exceeded the shared publication deadline: {elapsed:?}"
    );
}

#[test]
fn should_reject_stale_catalog_compare_exchange_after_takeover() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let stale_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read pre-takeover WAL catalog");
    let mut takeover_catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(stale_proof.bytes())
            .expect("decode pre-takeover WAL catalog");
    takeover_catalog
        .fence_to(3)
        .expect("advance winning takeover epoch");
    storage
        .compare_exchange_remote_object(
            crate::wal::cloud_catalog::OBJECT_KEY,
            Some(stale_proof.metadata()),
            takeover_catalog.encode().expect("encode winning catalog"),
        )
        .expect("publish winning takeover catalog");
    let mut losing_catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(stale_proof.bytes())
            .expect("decode stale WAL catalog");
    losing_catalog
        .fence_to(4)
        .expect("prepare losing catalog mutation");

    // Act
    let result = storage.compare_exchange_remote_object(
        crate::wal::cloud_catalog::OBJECT_KEY,
        Some(stale_proof.metadata()),
        losing_catalog.encode().expect("encode losing catalog"),
    );

    // Assert
    assert!(matches!(result, Err(crate::common::MidgeError::Busy(_))));
    let winning_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read winning WAL catalog");
    let winning_catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(winning_proof.bytes())
            .expect("decode winning WAL catalog");
    assert_eq!(winning_catalog.fencing_epoch(), 3);
}

fn hybrid_with_mock_cloud() -> (Arc<MockCloudBackend>, CloudPersistence) {
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(
        mock_cloud.clone(),
        "hybrid-test".to_string(),
    ));
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    storage
        .fence_cloud_wal_catalog(2)
        .expect("initialize test WAL publication catalog");
    (mock_cloud, storage)
}

fn assert_wal_catalog_copies_match(
    storage: &HybridStorage,
) -> crate::wal::cloud_catalog::WalPublicationCatalog {
    let primary = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read primary WAL catalog");
    let mirror = storage
        .remote_object_proof(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
        .expect("read WAL catalog mirror");
    assert_eq!(
        primary.bytes(),
        mirror.bytes(),
        "successful catalog mutations must converge both copies"
    );
    crate::wal::cloud_catalog::WalPublicationCatalog::decode(primary.bytes())
        .expect("decode converged WAL catalog")
}

fn hybrid_with_pausing_catalog_cloud() -> (Arc<PausingCatalogCasBackend>, Arc<CloudPersistence>) {
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let pausing_cloud = Arc::new(PausingCatalogCasBackend::new(Arc::clone(&mock_cloud)));
    let cloud = Arc::new(CloudStorage::new(pausing_cloud.clone(), String::new()));
    let storage = Arc::new(CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ))));
    storage
        .fence_cloud_wal_catalog(2)
        .expect("initialize pausing WAL publication catalog");
    (pausing_cloud, storage)
}

struct PausingCatalogCasBackend {
    inner: Arc<MockCloudBackend>,
    pause_next_catalog_compare_exchange: AtomicBool,
    catalog_compare_exchange_started: Barrier,
    release_catalog_compare_exchange: Barrier,
}

struct WinningInitialCatalogCasBackend {
    inner: Arc<MockCloudBackend>,
    winning_catalog: Vec<u8>,
    inject_winner: AtomicBool,
}

impl crate::storage::cloud::CloudReadCapability for WinningInitialCatalogCasBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity);
}

impl crate::storage::cloud::CloudWriteCapability for WinningInitialCatalogCasBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let is_initial_catalog_compare_exchange = key == crate::wal::cloud_catalog::OBJECT_KEY
            && headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("if-none-match") && value == "*");
        if is_initial_catalog_compare_exchange && self.inject_winner.swap(false, Ordering::SeqCst) {
            let (winner_tx, winner_rx) = std::sync::mpsc::channel();
            self.inner
                .submit_put(key, self.winning_catalog.clone(), Vec::new(), winner_tx);
            winner_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("competing initial catalog write must complete");
        }
        self.inner.submit_put(key, data, headers, callback);
    }
}

impl crate::storage::cloud::CloudDeleteCapability for WinningInitialCatalogCasBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
}

impl crate::storage::cloud::CloudListCapability for WinningInitialCatalogCasBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
}

impl crate::storage::cloud::CloudHeadCapability for WinningInitialCatalogCasBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}

impl CloudBackend for WinningInitialCatalogCasBackend {}

impl PausingCatalogCasBackend {
    fn new(inner: Arc<MockCloudBackend>) -> Self {
        Self {
            inner,
            pause_next_catalog_compare_exchange: AtomicBool::new(false),
            catalog_compare_exchange_started: Barrier::new(2),
            release_catalog_compare_exchange: Barrier::new(2),
        }
    }

    fn pause_next_catalog_compare_exchange(&self) {
        self.pause_next_catalog_compare_exchange
            .store(true, Ordering::SeqCst);
    }

    fn wait_for_catalog_compare_exchange(&self) {
        self.catalog_compare_exchange_started.wait();
    }

    fn release_catalog_compare_exchange(&self) {
        self.release_catalog_compare_exchange.wait();
    }
}

impl crate::storage::cloud::CloudReadCapability for PausingCatalogCasBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity);
}

impl crate::storage::cloud::CloudWriteCapability for PausingCatalogCasBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let is_catalog_compare_exchange = key == crate::wal::cloud_catalog::OBJECT_KEY
            && headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("if-match"));
        if is_catalog_compare_exchange
            && self
                .pause_next_catalog_compare_exchange
                .swap(false, Ordering::SeqCst)
        {
            self.catalog_compare_exchange_started.wait();
            self.release_catalog_compare_exchange.wait();
        }
        self.inner.submit_put(key, data, headers, callback);
    }
}

impl crate::storage::cloud::CloudDeleteCapability for PausingCatalogCasBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
}

impl crate::storage::cloud::CloudListCapability for PausingCatalogCasBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
}

impl crate::storage::cloud::CloudHeadCapability for PausingCatalogCasBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}

impl CloudBackend for PausingCatalogCasBackend {}

#[test]
fn should_route_each_object_class_to_its_separate_cloud_store() {
    // Arrange
    let wal_mock = Arc::new(MockCloudBackend::new());
    let sst_mock = Arc::new(MockCloudBackend::new());
    let control_mock = Arc::new(MockCloudBackend::new());
    let wal_cloud = Arc::new(CloudStorage::new(wal_mock.clone(), String::new()));
    let sst_cloud = Arc::new(CloudStorage::new(sst_mock.clone(), String::new()));
    let control_cloud = Arc::new(CloudStorage::new(control_mock.clone(), String::new()));
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_class_stores_policy_event_sender_and_limits(
            wal_cloud,
            sst_cloud,
            control_cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            HybridQueueLimits::default(),
        ),
    ));
    // The sealed segment exists only in an in-memory root: the upload must
    // read it through the root, not from a host path.
    let wal_bytes = valid_wal_bytes(17, 17);
    let root: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());
    crate::io::Fs::open(
        root.as_ref(),
        &crate::wal::segment_key(17),
        crate::io::OpenOptions {
            mode: crate::io::OpenMode::ReadWrite,
            create: true,
            create_new: true,
            truncate: false,
        },
    )
    .and_then(|mut file| file.append(Bytes::from(wal_bytes.clone())))
    .expect("write WAL segment");

    // Act
    storage
        .enqueue_wal_segment(
            &crate::io::FsLocation::new(root, crate::wal::segment_key(17)),
            17,
            17,
        )
        .expect("enqueue WAL segment");
    storage.process_uploads();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && wal_mock.get_uploads().is_empty() {
        std::thread::yield_now();
        storage.process_uploads();
    }
    storage
        .write_sst_object("000017.sst", &valid_sst_bytes(b"key", b"value", 17))
        .expect("publish SST");
    storage
        .compare_exchange_remote_object(
            crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY,
            None,
            br#"{"epoch":17}"#.to_vec(),
        )
        .expect("publish control object");

    // Assert
    assert_eq!(
        wal_mock.get_uploads(),
        vec![(
            crate::wal::segment_object_key(17, 1),
            wal_bytes.len() as u64
        )]
    );
    assert_eq!(
        sst_mock
            .get_uploads()
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        vec![crate::cloud_layout::object_key("000017.sst")]
    );
    assert_eq!(
        control_mock.get_uploads(),
        vec![(
            crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY.to_string(),
            br#"{"epoch":17}"#.len() as u64,
        )]
    );
}

fn valid_sst_bytes(key: &[u8], value: &[u8], seq: u64) -> Vec<u8> {
    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("create SST writer");
    writer
        .add_with_meta(key, Some(value), seq, EntryType::Put, None)
        .expect("add SST entry");
    writer.finish_bytes().expect("finish SST bytes")
}

/// Sealed segment `segment_id`, written by epoch 1, holding one put at
/// `seq`.
fn valid_wal_bytes(segment_id: u64, seq: u64) -> Vec<u8> {
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"k"),
        Some(Bytes::from_static(b"v")),
        seq,
    );
    sealed_wal(segment_id, &frame_wal(&record))
}

/// `record` as one bare WAL frame.
fn frame_wal(record: &crate::wal::WalRecord) -> Vec<u8> {
    let payload = crate::wal::encoding::encode(record).expect("encode WAL record");
    let mut bytes = Vec::new();
    crate::wal::frame::append_frame(&mut bytes, &payload).expect("append WAL frame");
    bytes
}

/// `body`, bare frames, sealed as segment `segment_id` written by epoch 1:
/// the form every cloud WAL object takes (#843). Its first sequence is the
/// lowest the chain allows; tests here do not replay the chain.
fn sealed_wal(segment_id: u64, body: &[u8]) -> Vec<u8> {
    crate::wal::format::segment::SegmentBuilder::new(
        segment_id,
        1,
        crate::wal::format::ChainLink::GENESIS,
    )
    .bytes(body)
    .seal()
    .0
}

/// Publish `bytes` as segment `segment_id` in the catalog, linked to its
/// head as a writer's next segment is (#844): bare frames, or the body of a
/// sealed segment, are sealed again after the head. `catalog_max_sequence`
/// is the last sequence they hold.
fn write_authoritative_cloud_wal(
    storage: &HybridStorage,
    segment_id: u64,
    catalog_max_sequence: u64,
    bytes: Vec<u8>,
) -> String {
    let (epoch, body) = match crate::wal::format::segment::SegmentHeader::decode(&bytes) {
        Ok(header) => {
            let end = crate::wal::format::segment::sealed_footer(&bytes)
                .expect("scan a test WAL segment")
                .map_or(bytes.len(), |footer| {
                    usize::try_from(footer.data_len).expect("footer offset")
                });
            (
                header.epoch,
                bytes[crate::wal::format::HEADER_LEN..end].to_vec(),
            )
        }
        Err(_) => (1, bytes),
    };
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read test WAL catalog");
    let mut catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
            .expect("decode test WAL catalog");
    let (bytes, _) =
        crate::wal::format::segment::SegmentBuilder::new(segment_id, epoch, catalog.head())
            .bytes(&body)
            .seal();
    let publication = crate::wal::cloud_catalog::CatalogEntry::from_sealed_bytes(&bytes)
        .expect("a sealed test WAL segment");
    // The catalog names what the object holds: a caller expecting
    // `catalog_max_sequence` sees any difference when it verifies.
    let _ = catalog_max_sequence;
    let object_key = publication.object_key();
    write_cloud_object(storage, &object_key, bytes);
    catalog
        .append_run(2, std::slice::from_ref(&publication))
        .expect("publish test WAL authority");
    storage
        .compare_exchange_remote_object(
            crate::wal::cloud_catalog::OBJECT_KEY,
            Some(catalog_proof.metadata()),
            catalog.encode().expect("encode test WAL catalog"),
        )
        .expect("update test WAL catalog");
    object_key
}

struct AlwaysFailingWriteBackend {
    write_attempts: Arc<AtomicUsize>,
}

struct PanickingWriteBackend {
    write_attempts: Arc<AtomicUsize>,
}

impl AlwaysFailingWriteBackend {
    fn new(write_attempts: Arc<AtomicUsize>) -> Self {
        Self { write_attempts }
    }
}

impl PanickingWriteBackend {
    fn new(write_attempts: Arc<AtomicUsize>) -> Self {
        Self { write_attempts }
    }
}

impl StorageBackend for PanickingWriteBackend {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        let _ = (request, range, callback);
        panic!("test backend received undeclared range-read capability");
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared range HEAD capability");
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared metadata-read capability");
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared HEAD capability");
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_delete_request(
            request,
            callback,
            |key, precondition, callback| {
                let result = if matches!(precondition, crate::storage::StoragePrecondition::None) {
                    StorageOutcome::Ok(())
                } else {
                    StorageOutcome::Err(crate::storage::StorageError::precondition_failed(
                        "fixture cannot enforce a delete precondition",
                    ))
                };
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result,
                });
            },
        );
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_write_request(
            request,
            data,
            callback,
            |_key, _precondition, _data, _callback| {
                self.write_attempts.fetch_add(1, Ordering::SeqCst);
                panic!("injected cloud upload worker panic");
            },
        );
    }
}

impl StorageBackend for AlwaysFailingWriteBackend {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        let _ = (request, range, callback);
        panic!("test backend received undeclared range-read capability");
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared range HEAD capability");
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared metadata-read capability");
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::dispatch_head_request(request, callback, |key, _, callback| {
            let _ = callback.send(StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("head unavailable".to_string().into()),
            });
        });
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_delete_request(
            request,
            callback,
            |key, precondition, callback| {
                let result = if matches!(precondition, crate::storage::StoragePrecondition::None) {
                    StorageOutcome::Ok(())
                } else {
                    StorageOutcome::Err(crate::storage::StorageError::precondition_failed(
                        "fixture cannot enforce a delete precondition",
                    ))
                };
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result,
                });
            },
        );
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_write_request(
            request,
            data,
            callback,
            |key, _precondition, _data, callback| {
                self.write_attempts.fetch_add(1, Ordering::SeqCst);
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err("write unavailable".to_string().into()),
                });
            },
        );
    }
}

#[derive(Default)]
struct NeverCompletesBackend {
    callbacks: Mutex<Vec<StorageCallback>>,
}

struct BudgetConsumingSstPublicationBackend {
    first_head_delay: Duration,
    head_calls: AtomicUsize,
    retained_callbacks: Mutex<Vec<StorageCallback>>,
}

impl BudgetConsumingSstPublicationBackend {
    fn new(first_head_delay: Duration) -> Self {
        Self {
            first_head_delay,
            head_calls: AtomicUsize::new(0),
            retained_callbacks: Mutex::new(Vec::new()),
        }
    }

    fn retain_callback(&self, callback: StorageCallback) {
        self.retained_callbacks.lock().push(callback);
    }
}

impl StorageBackend for BudgetConsumingSstPublicationBackend {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        let _ = (request, range, callback);
        panic!("test backend received undeclared range-read capability");
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared range HEAD capability");
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared metadata-read capability");
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::dispatch_head_request(request, callback, |key, _, callback| {
            if self.head_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let delay = self.first_head_delay;
                let key = key.to_string();
                std::thread::spawn(move || {
                    std::thread::sleep(delay);
                    let _ = callback.send(StorageEvent::HeadComplete {
                        key,
                        result: StorageOutcome::Err(crate::storage::StorageError::not_found(
                            "delayed miss",
                        )),
                    });
                });
            } else {
                self.retain_callback(callback);
            }
        });
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_delete_request(
            request,
            callback,
            |_key, _precondition, callback| {
                self.retain_callback(callback);
            },
        );
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_write_request(
            request,
            data,
            callback,
            |_key, _precondition, _data, callback| {
                self.retain_callback(callback);
            },
        );
    }
}

struct RacingReadDeleteBackend {
    object: Mutex<Option<Vec<u8>>>,
    read_started: Barrier,
    release_read: Barrier,
}

impl RacingReadDeleteBackend {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            object: Mutex::new(Some(bytes)),
            read_started: Barrier::new(2),
            release_read: Barrier::new(2),
        }
    }

    fn metadata(bytes: &[u8]) -> StorageObjectMetadata {
        StorageObjectMetadata {
            size: bytes.len() as u64,
            etag: "race-etag".to_string(),
            generation: None,
        }
    }
}

impl StorageBackend for RacingReadDeleteBackend {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        let _ = (request, range, callback);
        panic!("test backend received undeclared range-read capability");
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        self.submit_head_request(request, callback);
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        crate::storage::dispatch_metadata_read_request(request, callback, |_, _, callback| {
            let snapshot = self.object.lock().clone();
            self.read_started.wait();
            self.release_read.wait();
            let result = snapshot
                .map(|bytes| {
                    let metadata = Self::metadata(&bytes);
                    (bytes, metadata)
                })
                .ok_or_else(|| crate::storage::StorageError::not_found("object"));
            let _ = callback.send(result);
        });
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::dispatch_head_request(request, callback, |key, _, callback| {
            let result = self.object.lock().as_deref().map_or_else(
                || StorageOutcome::Err("object not found".to_string().into()),
                |bytes| StorageOutcome::Ok(Self::metadata(bytes)),
            );
            let _ = callback.send(StorageEvent::HeadComplete {
                key: key.to_string(),
                result,
            });
        });
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_delete_request(
            request,
            callback,
            |key, precondition, callback| {
                let matches = match precondition {
                    crate::storage::StoragePrecondition::None => true,
                    crate::storage::StoragePrecondition::IfAbsent => false,
                    crate::storage::StoragePrecondition::IfMatch(expected) => {
                        expected.generation.is_none()
                            && expected.etag.trim_matches('"') == "race-etag"
                    }
                };
                let result = if matches {
                    self.object.lock().take();
                    StorageOutcome::Ok(())
                } else {
                    StorageOutcome::Err(crate::storage::StorageError::precondition_failed(
                        "racing fixture identity mismatch",
                    ))
                };
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result,
                });
            },
        );
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_write_request(
            request,
            data,
            callback,
            |key, precondition, data, callback| {
                if !matches!(precondition, crate::storage::StoragePrecondition::None) {
                    let _ = callback.send(StorageEvent::WriteComplete {
                        key: key.to_string(),
                        result: StorageOutcome::Err(
                            crate::storage::StorageError::precondition_failed(
                                "racing fixture cannot enforce a write precondition",
                            ),
                        ),
                    });
                    return;
                }
                *self.object.lock() = Some(data);
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Ok(()),
                });
            },
        );
    }
}

impl NeverCompletesBackend {
    fn retain_callback(&self, callback: StorageCallback) {
        self.callbacks.lock().push(callback);
    }
}

impl StorageBackend for NeverCompletesBackend {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        let _ = (request, range, callback);
        panic!("test backend received undeclared range-read capability");
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared range HEAD capability");
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared metadata-read capability");
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::dispatch_head_request(request, callback, |_key, _, callback| {
            self.retain_callback(callback);
        });
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_delete_request(
            request,
            callback,
            |_key, _precondition, callback| {
                self.retain_callback(callback);
            },
        );
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::dispatch_test_write_request(
            request,
            data,
            callback,
            |_key, _precondition, _data, callback| {
                self.retain_callback(callback);
            },
        );
    }
}

fn write_cloud_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.sst_store().write_for_test(key, data, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::WriteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("cloud write for '{key}' failed: {other:?}"),
    }
}

fn read_cloud_object(storage: &HybridStorage, key: &str) -> Vec<u8> {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.sst_store().submit_metadata_read_request(
        crate::storage::StorageRequest::new(
            key,
            crate::common::OperationDeadline::unbounded(),
            Duration::from_secs(1),
        ),
        tx,
    );
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(Ok((data, _metadata))) => data,
        other => panic!("cloud read for '{key}' failed: {other:?}"),
    }
}

fn delete_cloud_object(storage: &HybridStorage, key: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.sst_store().delete_for_test(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::DeleteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("cloud delete for '{key}' failed: {other:?}"),
    }
}

fn assert_cloud_object_exists(storage: &HybridStorage, key: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.sst_store().submit_head_request(
        crate::storage::StorageRequest::new(
            key,
            crate::common::OperationDeadline::from_budget(Duration::from_secs(1)),
            Duration::from_secs(1),
        ),
        tx,
    );
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::HeadComplete {
            result: StorageOutcome::Ok(_),
            ..
        }) => {}
        other => panic!("expected cloud object '{key}' to exist, got {other:?}"),
    }
}

fn assert_cloud_object_missing(storage: &HybridStorage, key: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.sst_store().submit_head_request(
        crate::storage::StorageRequest::new(
            key,
            crate::common::OperationDeadline::from_budget(Duration::from_secs(1)),
            Duration::from_secs(1),
        ),
        tx,
    );
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::HeadComplete {
            result: StorageOutcome::Err(_),
            ..
        }) => {}
        other => panic!("expected cloud object '{key}' to be missing, got {other:?}"),
    }
}

fn wait_for_wal_prune_result(storage: &HybridStorage, segment_id: u64) -> StorageOutcome<()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        for event in storage.process_uploads() {
            if let StorageEvent::CloudWalPruneComplete {
                segment_id: event_segment,
                result,
            } = event
            {
                if event_segment == segment_id {
                    return result;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for WAL prune result for segment {segment_id}");
}

fn manifest_for_ssts(files: &[(&str, u64)]) -> crate::metadata::Manifest {
    let files_with_crc: Vec<_> = files
        .iter()
        .map(|(name, size_bytes)| (*name, *size_bytes, None))
        .collect();
    manifest_for_ssts_with_crc(&files_with_crc)
}

fn manifest_for_ssts_with_crc(files: &[(&str, u64, Option<u32>)]) -> crate::metadata::Manifest {
    let mut manifest = crate::metadata::Manifest::default();
    manifest.files = files
        .iter()
        .map(
            |(name, size_bytes, content_crc32c)| crate::metadata::FileMeta {
                name: (*name).to_string(),
                level: 0,
                size_bytes: *size_bytes,
                content_crc32c: *content_crc32c,
                cf_id: 0,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"a".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
                ..Default::default()
            },
        )
        .collect();
    manifest
}

#[test]
fn should_revalidate_manifest_ssts_on_repeated_validation() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "cached.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    write_cloud_object(
        &storage,
        &crate::cloud_layout::object_key(sst_name),
        bytes.clone(),
    );
    let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

    mock_cloud.clear_history();
    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("first manifest validation");
    let first_downloads = mock_cloud.get_downloads();
    // Act
    // Assert
    assert!(
        first_downloads
            .iter()
            .any(|key| key.ends_with("sst/cached.sst")),
        "first validation should read the cloud SST, got {first_downloads:?}"
    );

    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("second manifest validation");

    let downloads = mock_cloud.get_downloads();
    assert_eq!(
        downloads
            .iter()
            .filter(|key| key.ends_with("sst/cached.sst"))
            .count(),
        2,
        "authoritative validation must reread the immutable SST: {downloads:?}"
    );
}

#[test]
fn should_revalidate_full_manifest_after_extension() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let first_name = "first.sst";
    let second_name = "second.sst";
    let first_bytes = valid_sst_bytes(b"a", b"v1", 1);
    let second_bytes = valid_sst_bytes(b"b", b"v2", 2);
    write_cloud_object(
        &storage,
        &crate::cloud_layout::object_key(first_name),
        first_bytes.clone(),
    );
    write_cloud_object(
        &storage,
        &crate::cloud_layout::object_key(second_name),
        second_bytes.clone(),
    );

    let first_manifest = manifest_for_ssts(&[(first_name, first_bytes.len() as u64)]);
    storage
        .verify_manifest_cloud_objects(&first_manifest)
        .expect("first manifest validation");

    mock_cloud.clear_history();
    let mut extended_manifest = crate::metadata::Manifest::default();
    extended_manifest.files = vec![
        crate::metadata::FileMeta {
            name: first_name.to_string(),
            level: 0,
            size_bytes: first_bytes.len() as u64,
            cf_id: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"a".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        },
        crate::metadata::FileMeta {
            name: second_name.to_string(),
            level: 0,
            size_bytes: second_bytes.len() as u64,
            cf_id: 0,
            smallest_key: Some(b"b".to_vec()),
            largest_key: Some(b"b".to_vec()),
            smallest_seq: Some(2),
            largest_seq: Some(2),
            ..Default::default()
        },
    ];
    storage
        .verify_manifest_cloud_objects(&extended_manifest)
        .expect("extended manifest validation");
    let downloads = mock_cloud.get_downloads();

    // Act
    // Assert
    assert!(
        downloads.iter().any(|key| key.ends_with("sst/second.sst")),
        "extended validation should read the new SST, got {downloads:?}"
    );
    assert!(
        downloads.iter().any(|key| key.ends_with("sst/first.sst")),
        "authoritative validation should reread the existing SST, got {downloads:?}"
    );
}

#[test]
fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_deleted() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "deleted-after-proof.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::cloud_layout::object_key(sst_name);
    write_cloud_object(&storage, &key, bytes.clone());
    let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest validation");
    delete_cloud_object(&storage, &key);

    let error = storage
        .verify_manifest_cloud_objects(&manifest)
        .expect_err("deleted SST must invalidate cached proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation") || error.contains("unreadable"),
        "unexpected stale SST proof error: {error}"
    );
}

#[test]
fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_overwritten() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "overwritten-after-proof.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::cloud_layout::object_key(sst_name);
    write_cloud_object(&storage, &key, bytes.clone());
    let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest validation");
    write_cloud_object(&storage, &key, b"not a valid sst".to_vec());

    let error = storage
        .verify_manifest_cloud_objects(&manifest)
        .expect_err("overwritten SST must invalidate cached proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation") || error.contains("size mismatch"),
        "unexpected stale SST proof error: {error}"
    );
}

#[test]
fn should_reject_manifest_sst_when_content_crc_differs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "wrong-crc.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::cloud_layout::object_key(sst_name);
    let wrong_crc = crc32c::crc32c(&bytes) ^ 0xffff_ffff;
    write_cloud_object(&storage, &key, bytes.clone());
    let manifest = manifest_for_ssts_with_crc(&[(sst_name, bytes.len() as u64, Some(wrong_crc))]);

    let error = storage
        .verify_manifest_cloud_objects(&manifest)
        .expect_err("manifest SST with mismatched content CRC must not validate");

    // Act
    // Assert
    assert!(
        error.contains("crc") || error.contains("content"),
        "unexpected manifest SST CRC validation error: {error}"
    );
}

#[test]
fn should_not_reuse_size_only_sst_proof_when_manifest_later_requires_crc() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "crc-after-size-proof.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::cloud_layout::object_key(sst_name);
    let wrong_crc = crc32c::crc32c(&bytes) ^ 0xffff_ffff;
    write_cloud_object(&storage, &key, bytes.clone());
    let size_only_manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);
    storage
        .verify_manifest_cloud_objects(&size_only_manifest)
        .expect("initial size-only manifest validation");
    let crc_manifest =
        manifest_for_ssts_with_crc(&[(sst_name, bytes.len() as u64, Some(wrong_crc))]);

    let error = storage
        .verify_manifest_cloud_objects(&crc_manifest)
        .expect_err("cached size-only SST proof must not satisfy later CRC requirement");

    // Act
    // Assert
    assert!(
        error.contains("crc") || error.contains("content"),
        "unexpected cached SST CRC validation error: {error}"
    );
}

#[test]
fn should_not_overwrite_remote_object_given_different_content_when_authoritative_upload_runs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "collision.sst";
    let key = crate::cloud_layout::object_key(sst_name);
    let existing_bytes = valid_sst_bytes(b"a", b"already-committed", 1);
    let upload_bytes = valid_sst_bytes(b"b", b"new-upload", 2);
    write_cloud_object(&storage, &key, existing_bytes.clone());

    let error = storage
        .write_sst_object(sst_name, &upload_bytes)
        .expect_err("existing remote SST object must fail authoritative upload");

    // Act
    // Assert
    assert!(
        error.to_string().contains("cloud immutable upload failed")
            || error.to_string().contains("precondition failed"),
        "unexpected SST collision error: {error}"
    );
    assert_eq!(
        read_cloud_object(&storage, &key),
        existing_bytes,
        "authoritative SST upload must not overwrite an existing remote object"
    );
}

#[test]
fn should_resume_same_content_sst_publication_after_remote_only_success() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "remote-only-retry.sst";
    let key = crate::cloud_layout::object_key(sst_name);
    let bytes = valid_sst_bytes(b"retry", b"value", 7);
    write_cloud_object(&storage, &key, bytes.clone());
    assert_cloud_object_exists(&storage, &key);

    // Act
    storage
        .write_sst_object(sst_name, &bytes)
        .expect("same-content retry should finish publication");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
}

#[test]
fn should_retry_same_content_upload_idempotently_given_remote_object_already_exists() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "fully-published-retry.sst";
    let key = crate::cloud_layout::object_key(sst_name);
    let bytes = valid_sst_bytes(b"retry", b"value", 8);
    storage
        .write_sst_object(sst_name, &bytes)
        .expect("publish both copies");

    // Act
    storage
        .write_sst_object(sst_name, &bytes)
        .expect("same-content retry must be idempotent");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
}

#[test]
fn should_reread_remote_wal_given_repeated_authoritative_validation() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 7;
    let max_sequence = 11;
    let _key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(segment_id, max_sequence),
    );

    mock_cloud.clear_history();
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("first remote WAL validation");
    let first_downloads = mock_cloud.get_downloads();
    // Act
    // Assert
    assert!(
        first_downloads
            .iter()
            .any(|key| key.ends_with("wal/epochs/00000000000000000001/00000000000000000007.wal")),
        "first validation should read the cloud WAL, got {first_downloads:?}"
    );

    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("second remote WAL validation");
    let downloads = mock_cloud.get_downloads();
    assert_eq!(
        downloads
            .iter()
            .filter(|key| key.ends_with("wal/epochs/00000000000000000001/00000000000000000007.wal"))
            .count(),
        2,
        "authoritative WAL validation must reread the segment: {downloads:?}"
    );
}

#[test]
fn should_reject_cached_remote_wal_proof_when_cloud_object_is_deleted() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 8;
    let max_sequence = 12;
    let key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(segment_id, max_sequence),
    );

    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    delete_cloud_object(&storage, &key);

    let error = storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect_err("deleted WAL must invalidate cached proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation") || error.contains("unreadable"),
        "unexpected stale WAL proof error: {error}"
    );
}

#[test]
fn should_reject_stale_remote_wal_target_identity_when_guarded_delete_runs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 11;
    let max_sequence = 21;
    let key = crate::wal::segment_object_key(segment_id, 1);
    write_cloud_object(&storage, &key, valid_wal_bytes(segment_id, max_sequence));
    let stale_proof = storage
        .remote_object_proof(&key)
        .expect("initial remote object proof");

    write_cloud_object(&storage, &key, valid_wal_bytes(segment_id, max_sequence));
    storage
        .delete_remote_objects_by_identity(vec![(
            segment_id,
            key.clone(),
            stale_proof.metadata().clone(),
        )])
        .expect("schedule prune");

    let result = wait_for_wal_prune_result(&storage, segment_id);
    // Act
    // Assert
    assert!(
        result.is_err(),
        "stale WAL proof must make remote prune fail conservatively"
    );
    assert_cloud_object_exists(&storage, &key);
}

#[test]
fn should_reject_reader_proof_when_guarded_prune_deletes_wal_during_download() {
    // Arrange
    let segment_id = 12;
    let key = crate::wal::segment_object_key(segment_id, 1);
    let bytes = valid_wal_bytes(segment_id, 22);
    let backend = Arc::new(RacingReadDeleteBackend::new(bytes.clone()));
    let cloud: Arc<dyn StorageBackend> = backend.clone();
    let storage = Arc::new(CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ))));
    let reader_storage = Arc::clone(&storage);
    let reader_key = key.clone();
    let reader = std::thread::spawn(move || reader_storage.remote_object_proof(&reader_key));
    backend.read_started.wait();

    // Act
    storage
        .delete_remote_objects_by_identity(vec![(
            segment_id,
            key.clone(),
            RacingReadDeleteBackend::metadata(&bytes),
        )])
        .expect("schedule guarded WAL prune");
    let prune_result = wait_for_wal_prune_result(&storage, segment_id);
    backend.release_read.wait();
    let reader_result = reader.join().expect("join concurrent WAL reader");

    // Assert
    assert!(
        prune_result.is_ok(),
        "guarded prune failed: {prune_result:?}"
    );
    let reader_error = reader_result.expect_err("racing read must not return a stale proof");
    assert!(
        reader_error.to_string().contains("unreadable"),
        "unexpected racing reader error: {reader_error}"
    );
    assert_cloud_object_missing(&storage, &key);
}

#[test]
fn should_reject_remote_wal_segment_with_sequence_beyond_expected_max() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 10;
    let expected_max_sequence = 20;
    write_authoritative_cloud_wal(
        &storage,
        segment_id,
        expected_max_sequence,
        valid_wal_bytes(segment_id, expected_max_sequence + 1),
    );

    let error = storage
        .verify_remote_wal_segment(segment_id, expected_max_sequence)
        .expect_err("WAL segment with records beyond expected max must be rejected");
    // Act
    // Assert
    assert!(
        error.contains("max sequence 21 does not match expected 20"),
        "unexpected WAL max-sequence error: {error}"
    );
}

#[test]
fn should_not_overwrite_existing_remote_wal_during_upload() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create WAL dir");
    let segment_id = 12;
    let upload_max_sequence = 22;
    let key = crate::wal::segment_object_key(segment_id, 1);
    let existing_bytes = valid_wal_bytes(segment_id, upload_max_sequence + 100);
    write_cloud_object(&storage, &key, existing_bytes.clone());

    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(segment_id, upload_max_sequence))
        .expect("write local WAL");
    storage
        .enqueue_wal_segment(
            &crate::io::FsLocation::host_file(&wal_path),
            segment_id,
            upload_max_sequence,
        )
        .expect("enqueue WAL upload");

    // Act
    let events = drive_wal_upload_until(&storage, Duration::from_secs(2), |events| {
        events.iter().any(|event| {
            matches!(
                event,
                StorageEvent::CloudFail { .. } | StorageEvent::CloudAck { .. }
            )
        })
    });

    // Assert
    assert!(
        events.iter().any(|event| matches!(
            event,
            StorageEvent::CloudFail {
                segment_id: failed_segment,
                failures: 1,
                failure_kind: crate::storage::CloudUploadFailureKind::Corruption,
                ..
            } if *failed_segment == segment_id
        )),
        "an existing object with other bytes is corruption: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, StorageEvent::CloudAck { .. })),
        "existing remote WAL object must not produce a CloudAck"
    );
    storage.process_uploads();
    assert_eq!(
        storage.pending_upload_count(),
        0,
        "no retry can upload over a corrupt object"
    );
    assert_eq!(
        read_cloud_object(&storage, &key),
        existing_bytes,
        "upload must not overwrite an existing remote WAL object"
    );
}

#[test]
fn should_acknowledge_wal_upload_when_remote_object_holds_the_same_bytes() {
    // Arrange: an earlier attempt already landed the same bytes.
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create WAL dir");
    let segment_id = 14;
    let max_sequence = 24;
    let bytes = valid_wal_bytes(segment_id, max_sequence);
    let key = crate::wal::segment_object_key(segment_id, 1);
    write_cloud_object(&storage, &key, bytes.clone());
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, &bytes).expect("write local WAL");
    storage
        .enqueue_wal_segment(
            &crate::io::FsLocation::host_file(&wal_path),
            segment_id,
            max_sequence,
        )
        .expect("enqueue WAL upload");

    // Act
    let events = drive_wal_upload_until(&storage, Duration::from_secs(2), |events| {
        events
            .iter()
            .any(|event| matches!(event, StorageEvent::CloudAck { .. }))
    });

    // Assert
    assert!(
        events.iter().any(|event| matches!(
            event,
            StorageEvent::CloudAck {
                segment_id: acked_segment,
                max_sequence: acked_max_sequence,
            } if *acked_segment == segment_id && *acked_max_sequence == max_sequence
        )),
        "an existing object with the same bytes is this upload: {events:?}"
    );
    assert_eq!(read_cloud_object(&storage, &key), bytes);
}

/// Drive the upload queue until `done` holds for the events seen so far, or
/// `budget` passes; return those events.
fn drive_wal_upload_until(
    storage: &HybridStorage,
    budget: Duration,
    done: impl Fn(&[StorageEvent]) -> bool,
) -> Vec<StorageEvent> {
    let deadline = Instant::now() + budget;
    let mut events = Vec::new();
    while Instant::now() < deadline {
        events.extend(storage.process_uploads());
        if done(&events) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    events
}

/// Each failed attempt's `(failures, failure_kind)`, in order.
fn wal_upload_failures(
    events: &[StorageEvent],
) -> Vec<(u32, crate::storage::CloudUploadFailureKind)> {
    events
        .iter()
        .filter_map(|event| match event {
            StorageEvent::CloudFail {
                failures,
                failure_kind,
                ..
            } => Some((*failures, *failure_kind)),
            _ => None,
        })
        .collect()
}

#[test]
fn should_readback_remote_wal_before_upload_worker_emits_ack() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create WAL dir");
    let segment_id = 9;
    let max_sequence = 13;
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(segment_id, max_sequence)).expect("write local WAL");

    mock_cloud.clear_history();
    storage
        .enqueue_wal_segment(
            &crate::io::FsLocation::host_file(&wal_path),
            segment_id,
            max_sequence,
        )
        .expect("enqueue WAL upload");
    storage.process_uploads();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        events.extend(storage.process_uploads());
        if events
            .iter()
            .any(|event| matches!(event, StorageEvent::CloudAck { .. }))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Act
    // Assert
    assert!(
        events.iter().any(|event| matches!(
            event,
            StorageEvent::CloudAck {
                segment_id: 9,
                max_sequence: 13
            }
        )),
        "worker should emit CloudAck after upload and readback, got {events:?}"
    );
    assert!(
        mock_cloud
            .get_downloads()
            .iter()
            .any(|key| key.ends_with("wal/epochs/00000000000000000001/00000000000000000009.wal")),
        "upload worker must read back the remote WAL before ack"
    );
}

/// Hybrid storage over `cloud` whose failed WAL uploads retry by `retry`.
fn hybrid_with_wal_upload_retry(
    cloud: Arc<dyn crate::storage::StorageBackend>,
    retry: crate::storage::hybrid::backend::WalUploadRetryPolicy,
) -> CloudPersistence {
    CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            HybridQueueLimits {
                wal_upload_retry: retry,
                ..HybridQueueLimits::default()
            },
        ),
    ))
}

/// Queue a valid sealed WAL segment for upload.
fn enqueue_test_wal_upload(
    storage: &CloudPersistence,
    dir: &std::path::Path,
    segment_id: u64,
    max_sequence: u64,
) {
    let wal_path = dir.join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(segment_id, max_sequence)).expect("write local WAL");
    storage
        .enqueue_wal_segment(
            &crate::io::FsLocation::host_file(&wal_path),
            segment_id,
            max_sequence,
        )
        .expect("enqueue WAL upload");
}

#[test]
fn should_keep_retrying_failed_wal_upload_when_every_attempt_fails() {
    // Arrange: retries are due at once, so the test waits on attempts, not
    // on the wall clock.
    let tmp = tempfile::tempdir().expect("create WAL retry test dir");
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let storage = hybrid_with_wal_upload_retry(
        Arc::new(AlwaysFailingWriteBackend::new(Arc::clone(&write_attempts))),
        crate::storage::hybrid::backend::WalUploadRetryPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        },
    );
    enqueue_test_wal_upload(&storage, tmp.path(), 11, 21);

    // Act
    let events = drive_wal_upload_until(&storage, Duration::from_secs(30), |events| {
        wal_upload_failures(events).len() >= 6
    });

    // Assert: every failure is reported with its count, past the three
    // attempts a waiter is owed, and the upload stays queued.
    let other = crate::storage::CloudUploadFailureKind::Other;
    assert_eq!(
        wal_upload_failures(&events)[..6],
        [
            (1, other),
            (2, other),
            (3, other),
            (4, other),
            (5, other),
            (6, other)
        ]
    );
    assert_eq!(
        storage.pending_upload_count(),
        1,
        "a failed WAL upload is never dropped from the queue"
    );
}

#[test]
fn should_hold_failed_wal_upload_until_its_retry_delay_passes() {
    // Arrange: a retry delay far longer than the test.
    let tmp = tempfile::tempdir().expect("create WAL backoff test dir");
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let hour = Duration::from_secs(3600);
    let storage = hybrid_with_wal_upload_retry(
        Arc::new(AlwaysFailingWriteBackend::new(Arc::clone(&write_attempts))),
        crate::storage::hybrid::backend::WalUploadRetryPolicy {
            base_delay: hour,
            max_delay: hour,
        },
    );
    enqueue_test_wal_upload(&storage, tmp.path(), 11, 21);
    drive_wal_upload_until(&storage, Duration::from_secs(30), |events| {
        !wal_upload_failures(events).is_empty()
    });

    // Act
    for _ in 0..20 {
        storage.process_uploads();
    }
    let turn = storage.next_upload_turn(Duration::from_millis(1));

    // Assert
    assert_eq!(write_attempts.load(Ordering::SeqCst), 1);
    assert!(
        turn.is_some_and(|turn| turn > Duration::from_mins(59)),
        "a backed-off upload must not keep its caller polling: {turn:?}"
    );
}

#[test]
fn should_keep_wal_upload_queued_when_upload_worker_panics() {
    // Arrange: retries wait an hour, so the first failure is the last.
    let tmp = tempfile::tempdir().expect("create WAL worker panic test dir");
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let hour = Duration::from_secs(3600);
    let storage = hybrid_with_wal_upload_retry(
        Arc::new(PanickingWriteBackend::new(Arc::clone(&write_attempts))),
        crate::storage::hybrid::backend::WalUploadRetryPolicy {
            base_delay: hour,
            max_delay: hour,
        },
    );
    enqueue_test_wal_upload(&storage, tmp.path(), 12, 22);

    // Act
    let events = drive_wal_upload_until(&storage, Duration::from_secs(30), |events| {
        !wal_upload_failures(events).is_empty()
    });

    // Assert
    assert_eq!(write_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        wal_upload_failures(&events),
        [(1, crate::storage::CloudUploadFailureKind::Other)],
        "a panicking worker is a failed attempt like any other"
    );
    assert_eq!(storage.pending_upload_count(), 1);
}

#[test]
fn should_end_wal_upload_without_attempting_it_when_writer_is_fenced() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create fenced WAL upload dir");
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let cloud = Arc::new(AlwaysFailingWriteBackend::new(Arc::clone(&write_attempts)));
    let hybrid = Arc::new(HybridStorage::with_policy(
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    hybrid
        .configure_write_authority(Arc::new(|| {
            Err(MidgeError::Fenced("test writer lost its lease".to_string()))
        }))
        .expect("install write authority");
    let storage = CloudPersistence::new(hybrid);
    let segment_id = 13;
    let max_sequence = 23;
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(segment_id, max_sequence)).expect("write local WAL");
    storage
        .enqueue_wal_segment(
            &crate::io::FsLocation::host_file(&wal_path),
            segment_id,
            max_sequence,
        )
        .expect("enqueue WAL upload");

    // Act
    let events = drive_wal_upload_until(&storage, Duration::from_millis(500), |events| {
        !wal_upload_failures(events).is_empty()
    });
    storage.process_uploads();

    // Assert
    assert_eq!(
        wal_upload_failures(&events),
        vec![(1, crate::storage::CloudUploadFailureKind::Fenced)]
    );
    assert_eq!(write_attempts.load(Ordering::SeqCst), 0);
    assert_eq!(storage.pending_upload_count(), 0);
}

#[test]
fn should_reject_wal_upload_when_entry_or_byte_capacity_is_exhausted() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create bounded upload test dir");
    let cloud = Arc::new(NeverCompletesBackend::default());
    let first_path = tmp.path().join(crate::wal::segment_file_name(1));
    let second_path = tmp.path().join(crate::wal::segment_file_name(2));
    let wal_bytes = valid_wal_bytes(1, 1);
    std::fs::write(&first_path, &wal_bytes).expect("write first WAL");
    std::fs::write(&second_path, valid_wal_bytes(2, 1)).expect("write second WAL");
    let limits = HybridQueueLimits {
        upload_entries: 1,
        upload_bytes: wal_bytes.len() as u64,
        callback_timeout: Duration::from_millis(20),
        ..HybridQueueLimits::default()
    };
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        ),
    ));
    storage
        .enqueue_wal_segment(&crate::io::FsLocation::host_file(&first_path), 1, 1)
        .expect("first upload must fit");
    assert!(matches!(
        storage.ensure_wal_write_admission(crate::storage::hybrid::backend::HeldUploads::default()),
        Err(crate::common::MidgeError::WriteStall(_))
    ));

    // Act
    let error = storage
        .enqueue_wal_segment(&crate::io::FsLocation::host_file(&second_path), 2, 1)
        .expect_err("second upload must be rejected at capacity");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::WriteStall(_)));
    assert!(matches!(
        storage.ensure_wal_write_admission(crate::storage::hybrid::backend::HeldUploads::default()),
        Err(crate::common::MidgeError::WriteStall(_))
    ));
    assert_eq!(storage.pending_upload_count(), 1);
    assert_eq!(storage.pending_upload_bytes(), wal_bytes.len() as u64);
}

#[test]
fn should_restore_wal_admission_after_upload_capacity_drains() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create admission release test dir");
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(
        mock_cloud,
        "bounded-admission".to_string(),
    ));
    let limits = HybridQueueLimits {
        upload_entries: 1,
        ..HybridQueueLimits::default()
    };
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        ),
    ));
    let first_path = tmp.path().join(crate::wal::segment_file_name(21));
    let second_path = tmp.path().join(crate::wal::segment_file_name(22));
    std::fs::write(&first_path, valid_wal_bytes(21, 21)).expect("write first WAL");
    std::fs::write(&second_path, valid_wal_bytes(22, 22)).expect("write second WAL");
    storage
        .enqueue_wal_segment(&crate::io::FsLocation::host_file(&first_path), 21, 21)
        .expect("enqueue first upload");
    let _ = storage
        .enqueue_wal_segment(&crate::io::FsLocation::host_file(&second_path), 22, 22)
        .expect_err("capacity must reject second upload");
    assert!(storage
        .ensure_wal_write_admission(crate::storage::hybrid::backend::HeldUploads::default())
        .is_err());

    // Act
    storage.process_uploads();
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline
        && storage
            .ensure_wal_write_admission(crate::storage::hybrid::backend::HeldUploads::default())
            .is_err()
    {
        storage.process_uploads();
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    storage
        .ensure_wal_write_admission(crate::storage::hybrid::backend::HeldUploads::default())
        .expect("completed upload must reopen admission");
    assert_eq!(storage.pending_upload_count(), 0);
}

#[test]
fn should_keep_storage_reservation_for_retry_when_upload_callbacks_are_lost() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create callback timeout test dir");
    let cloud = Arc::new(NeverCompletesBackend::default());
    let limits = HybridQueueLimits {
        callback_timeout: Duration::from_millis(20),
        upload_entries: 1,
        ..HybridQueueLimits::default()
    };
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        ),
    ));
    let wal_path = tmp.path().join(crate::wal::segment_file_name(3));
    std::fs::write(&wal_path, valid_wal_bytes(3, 3)).expect("write WAL");
    storage
        .enqueue_wal_segment(&crate::io::FsLocation::host_file(&wal_path), 3, 3)
        .expect("enqueue WAL upload");
    assert!(
        storage
            .ensure_wal_write_admission(crate::storage::hybrid::backend::HeldUploads::default())
            .is_err(),
        "the only queue reservation must close write admission while upload is stuck"
    );
    storage.process_uploads();

    // Act
    let events = drive_wal_upload_until(&storage, Duration::from_secs(1), |events| {
        wal_upload_failures(events).len() >= 3
    });

    // Assert: every lost callback times out, so the worker is never stuck,
    // and the upload stays queued for its next attempt.
    let timeout = crate::storage::CloudUploadFailureKind::Timeout;
    assert_eq!(
        wal_upload_failures(&events),
        [(1, timeout), (2, timeout), (3, timeout)]
    );
    assert!(events.iter().all(|event| match event {
        StorageEvent::CloudFail { error, .. } => error.contains("timed out"),
        _ => true,
    }));
    assert_eq!(storage.pending_upload_count(), 1);
    assert!(
        storage
            .ensure_wal_write_admission(crate::storage::hybrid::backend::HeldUploads::default())
            .is_err(),
        "the retained upload keeps its queue reservation"
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn should_poll_no_faster_than_its_interval_when_overdue_retries_cannot_start() {
    // Arrange: one worker slot. The worker holds the first upload and the
    // channel the second; the next two fail at once with no retry delay,
    // so their retries are overdue but cannot start.
    let tmp = tempfile::tempdir().expect("create overdue retry dir");
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            Arc::new(NeverCompletesBackend::default()),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            HybridQueueLimits {
                worker_entries: 1,
                callback_timeout: Duration::from_millis(100),
                wal_upload_retry: crate::storage::hybrid::backend::WalUploadRetryPolicy {
                    base_delay: Duration::ZERO,
                    max_delay: Duration::ZERO,
                },
                ..HybridQueueLimits::default()
            },
        ),
    ));
    // Holding the failpoint gate keeps the worker on its first upload.
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    enqueue_test_wal_upload(&storage, tmp.path(), 1, 1);
    enqueue_test_wal_upload(&storage, tmp.path(), 2, 2);
    storage.process_uploads();
    enqueue_test_wal_upload(&storage, tmp.path(), 3, 3);
    enqueue_test_wal_upload(&storage, tmp.path(), 4, 4);
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
        .expect("configure WAL upload failures");
    storage.process_uploads();
    fail::remove("midge::cloud::inject_fail_wal_upload");
    let poll = Duration::from_millis(5);

    // Act
    storage.process_uploads();
    let turn = storage.next_upload_turn(poll);

    // Assert
    scenario.teardown();
    drop(test_guard);
    assert_eq!(
        turn,
        Some(poll),
        "overdue retries behind a full worker must not spin the event loop"
    );
}

#[test]
fn should_bound_sst_publication_with_shared_deadline_when_remote_preflight_consumes_budget() {
    // Arrange: remote HEAD consumes most of the shared budget, then the
    // conditional PUT never answers. A fresh per-call timeout would let this
    // attempt outlive its advertised deadline by roughly two seconds, leaving
    // a full second of scheduler headroom in the bounded assertion.
    let cloud = Arc::new(BudgetConsumingSstPublicationBackend::new(
        Duration::from_millis(300),
    ));
    let limits = HybridQueueLimits {
        callback_timeout: Duration::from_secs(2),
        ..HybridQueueLimits::default()
    };
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        ),
    ));
    let deadline = crate::common::OperationDeadline::from_budget(Duration::from_millis(500));

    // Act
    let started = Instant::now();
    let result = storage.write_sst_object_within(
        "deadline-publication.sst",
        &valid_sst_bytes(b"deadline", b"value", 1),
        &deadline,
    );
    let elapsed = started.elapsed();

    // Assert
    assert!(
        matches!(result, Err(crate::common::MidgeError::Timeout(_))),
        "deadline expiry must remain typed as Timeout: {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "SST publication reused a fresh callback timeout: {elapsed:?}"
    );
}

pub(crate) struct SharedRetirementFixture {
    _directory: tempfile::TempDir,
    fs: Arc<dyn crate::io::Fs>,
    cloud: Arc<CloudStorage>,
    store: Arc<crate::lease::fs_leader_store::FsLeaderStore>,
    lock: crate::runtime::MetadataPublicationLock,
}

impl SharedRetirementFixture {
    pub(crate) fn orphan_context<'a>(
        &'a self,
        storage: &'a HybridStorage,
        deadline: &'a crate::common::OperationDeadline,
    ) -> OrphanSweepContext<'a> {
        OrphanSweepContext {
            storage,
            metadata: &self.cloud,
            authority: CloudMetadataMirrorAuthority {
                store: self.store.as_ref(),
                holder_id: "writer",
                writer_epoch: 2,
            },
            publication_lock: &self.lock,
            deadline,
        }
    }

    pub(crate) fn new(manifest: &Manifest) -> Self {
        use crate::lease::LeaderStore;
        let directory = tempfile::tempdir().unwrap();
        let fs = crate::io::real::test_root(directory.path());
        crate::metadata::ensure_or_create_format_marker(&fs, directory.path()).unwrap();
        crate::metadata::ManifestPersistence::save_with_fs(&fs, manifest).unwrap();
        let store = Arc::new(crate::lease::fs_leader_store::FsLeaderStore::new(
            Arc::clone(&fs),
            crate::lease::LeaseClock::default(),
        ));
        store
            .acquire_leadership_with_minimum_epoch("writer", 1)
            .unwrap();
        Self {
            _directory: directory,
            fs,
            cloud: Arc::new(CloudStorage::with_mock()),
            store,
            lock: crate::runtime::MetadataPublicationLock::default(),
        }
    }

    pub(crate) fn publish(&self, manifest: &Manifest) {
        crate::metadata::ManifestPersistence::save_with_fs(&self.fs, manifest).unwrap();
        mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &self.cloud,
                fs: self.fs.as_ref(),
                publication_lock: &self.lock,
                lock_wait_budget: Duration::from_secs(5),
                deadline: &crate::common::OperationDeadline::from_budget(Duration::from_secs(5)),
                authority: CloudMetadataMirrorAuthority {
                    store: self.store.as_ref(),
                    holder_id: "writer",
                    writer_epoch: 2,
                },
            },
            |_| Ok(()),
        )
        .unwrap();
    }

    pub(crate) fn authority(&self) -> SharedRetirementAuthority {
        SharedMetadataSnapshot::new(
            Arc::clone(&self.cloud),
            crate::common::resource_budget::ResourceBudget::new(8 * 1024 * 1024),
            self.store.clone(),
            "writer".into(),
            2,
            self.lock.clone(),
        )
        .committed_retirement_authority_within(&crate::common::OperationDeadline::unbounded())
        .unwrap()
    }
}

pub(crate) fn fixture_manifest_frontier(manifest: &mut Manifest, cf_id: u32, through: u64) {
    let mut value = serde_json::to_value(&*manifest).unwrap();
    let row = value["flushed_through"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|row| row["cf_id"] == cf_id)
        .unwrap();
    row["through_sequence"] = through.into();
    *manifest = serde_json::from_value(value).unwrap();
    manifest.validate_persisted().unwrap();
}

#[test]
fn should_retain_remote_wal_until_the_family_frontier_is_committed_to_shared_metadata() {
    // Arrange: a local-only flush cannot retire shared WAL authority.
    let (mock, storage) = hybrid_with_mock_cloud();
    let key = write_authoritative_cloud_wal(&storage, 1, 7, valid_wal_bytes(1, 7));
    let mut manifest = Manifest::default();
    let fixture = SharedRetirementFixture::new(&manifest);
    fixture.publish(&manifest);
    fixture_manifest_frontier(&mut manifest, 0, 7);
    crate::metadata::ManifestPersistence::save_with_fs(&fixture.fs, &manifest).unwrap();
    let old_authority = fixture.authority();
    let deadline = crate::common::OperationDeadline::unbounded();

    // Act
    let results = storage
        .prune_cloud_wal_segments_within(&[(1, 7)], &old_authority, 2, &deadline)
        .unwrap();

    // Assert
    assert!(results[0].1.is_err());
    assert!(storage
        .fence_cloud_wal_catalog(2)
        .unwrap()
        .entry(1)
        .is_some());
    assert_cloud_object_exists(&storage, &key);

    // Act: publishing the durable frontier permits catalog retirement without
    // reading any WAL or SST payload, including with an empty local cache.
    fixture.publish(&manifest);
    let authority = fixture.authority();
    let prior_reads = mock.get_downloads().len();
    let results = storage
        .prune_cloud_wal_segments_within(&[(1, 7)], &authority, 2, &deadline)
        .unwrap();

    // Assert
    assert!(results.is_empty());
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
    let reads = mock.get_downloads();
    assert!(
        reads[prior_reads..]
            .iter()
            .all(|key| !key.contains("/wal/epochs/")
                && !std::path::Path::new(key)
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))),
        "retirement fetched payloads: {:?}",
        &reads[prior_reads..]
    );
    let catalog = storage.fence_cloud_wal_catalog(2).unwrap();
    assert_eq!(catalog.entries(), []);
    assert_eq!(catalog.retired_by(), authority.generation_id());
    assert_cloud_object_missing(&storage, &key);
}

#[test]
fn should_retain_a_newer_flushed_family_until_the_oldest_family_is_dropped() {
    // Arrange
    let (_, storage) = hybrid_with_mock_cloud();
    let mut manifest = Manifest::default();
    let family = manifest.create_column_family("blocking".into());
    let mut first = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"old"),
        Some(Bytes::from_static(b"value")),
        3,
    );
    first.cf_id = family;
    let first_bytes = crate::wal::format::segment::SegmentBuilder::new(
        1,
        1,
        crate::wal::format::ChainLink::GENESIS,
    )
    .record(&first)
    .seal()
    .0;
    write_authoritative_cloud_wal(&storage, 1, 3, first_bytes);
    write_authoritative_cloud_wal(&storage, 2, 7, valid_wal_bytes(2, 7));
    fixture_manifest_frontier(&mut manifest, 0, 7);
    let fixture = SharedRetirementFixture::new(&manifest);
    fixture.publish(&manifest);
    let deadline = crate::common::OperationDeadline::unbounded();

    // Act
    let results = storage
        .prune_cloud_wal_segments_within(&[(1, 3), (2, 7)], &fixture.authority(), 2, &deadline)
        .unwrap();

    assert!(results.iter().all(|(_, result)| result.is_err()));
    assert_eq!(
        storage.fence_cloud_wal_catalog(2).unwrap().entries().len(),
        2
    );

    // A durable family drop removes the oldest prefix's remaining obligation.
    manifest
        .apply_edit(&crate::metadata::ManifestEdit::DropColumnFamilyAt {
            id: family,
            drop_sequence: 8,
            dropped_sst_names: Vec::new(),
        })
        .unwrap();
    fixture.publish(&manifest);
    storage
        .prune_cloud_wal_segments_within(&[(1, 3), (2, 7)], &fixture.authority(), 2, &deadline)
        .unwrap();

    // Assert
    assert_eq!(storage.fence_cloud_wal_catalog(2).unwrap().entries(), []);
}

#[test]
fn should_retain_catalog_authority_when_the_captured_shared_generation_changes() {
    // Arrange
    let (_, storage) = hybrid_with_mock_cloud();
    let key = write_authoritative_cloud_wal(&storage, 1, 7, valid_wal_bytes(1, 7));
    let mut manifest = Manifest::default();
    fixture_manifest_frontier(&mut manifest, 0, 7);
    let fixture = SharedRetirementFixture::new(&manifest);
    fixture.publish(&manifest);
    let stale = fixture.authority();
    manifest.create_column_family("new-family".into());
    fixture.publish(&manifest);

    // Act
    let result = storage.prune_cloud_wal_segments_within(
        &[(1, 7)],
        &stale,
        2,
        &crate::common::OperationDeadline::unbounded(),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))));
    assert!(storage
        .fence_cloud_wal_catalog(2)
        .unwrap()
        .entry(1)
        .is_some());
    assert_cloud_object_exists(&storage, &key);
}
