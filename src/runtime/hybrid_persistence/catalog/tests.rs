use super::*;
use crate::runtime::hybrid_persistence::{CloudPersistence, CloudStorage, PublishedWalSegment};
use std::sync::Arc;

struct PausedHead {
    key: String,
    started: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

struct ExpiringCatalogRead {
    inner: crate::storage::cloud::MockCloudBackend,
    validity: Arc<crate::lease::LeaseValidity>,
    expire_read_key: std::sync::Mutex<Option<String>>,
    writes_after_expiry: std::sync::atomic::AtomicUsize,
    paused_head: std::sync::Mutex<Option<PausedHead>>,
    delete_submissions: std::sync::atomic::AtomicUsize,
}

impl crate::storage::cloud::CloudBackend for ExpiringCatalogRead {
    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        // The read begins while authorized and returns after expiry.
        let mut armed = self.expire_read_key.lock().unwrap();
        if armed.as_deref() == Some(key) {
            *armed = None;
            self.validity.expire_for_test();
        }
        drop(armed);
        let paused = {
            let mut slot = self.paused_head.lock().unwrap();
            if slot.as_ref().is_some_and(|paused| paused.key == key) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(paused) = paused {
            paused.started.send(()).unwrap();
            paused
                .release
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        }
        self.inner.submit_head(key, callback);
    }

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        if self.validity.remaining(7).is_err() {
            self.writes_after_expiry
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.submit_put(key, data, headers, callback);
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.delete_submissions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.submit_delete(key, headers, callback);
    }

    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity, submit_list);
}

fn framed_wal_bytes(sequence: u64) -> Vec<u8> {
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from_static(b"key"),
        Some(bytes::Bytes::from_static(b"value")),
        sequence,
        7,
    );
    let payload = crate::wal::encoding::encode(&record).unwrap();
    let mut bytes = Vec::new();
    crate::wal::frame::append_frame(&mut bytes, &payload).unwrap();
    bytes
}

struct CatalogExpiryFixture {
    directory: tempfile::TempDir,
    validity: Arc<crate::lease::LeaseValidity>,
    provider: Arc<ExpiringCatalogRead>,
    storage: CloudPersistence,
    segment: PublishedWalSegment,
    candidate: super::super::ValidatedWalPruneCandidate,
    publication_path: std::path::PathBuf,
    catalog: WalPublicationCatalog,
    bytes: Vec<u8>,
    primary_before: Option<Vec<u8>>,
}

impl CatalogExpiryFixture {
    fn new(schedule: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let validity = Arc::new(crate::lease::LeaseValidity::new());
        validity
            .activate(
                7,
                std::time::Instant::now() + std::time::Duration::from_mins(1),
            )
            .unwrap();
        let provider = Arc::new(ExpiringCatalogRead {
            inner: crate::storage::cloud::MockCloudBackend::new(),
            validity: Arc::clone(&validity),
            expire_read_key: std::sync::Mutex::new(None),
            writes_after_expiry: std::sync::atomic::AtomicUsize::new(0),
            paused_head: std::sync::Mutex::new(None),
            delete_submissions: std::sync::atomic::AtomicUsize::new(0),
        });
        let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap()),
            Arc::new(CloudStorage::new(provider.clone(), String::new())),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )));
        let primary = crate::wal::cloud_catalog::OBJECT_KEY;
        let mirror = crate::wal::cloud_catalog::MIRROR_OBJECT_KEY;
        let first_wal = framed_wal_bytes(1);
        let segment = PublishedWalSegment::from_validated_bytes(1, 1, 7, &first_wal);
        storage
            .compare_exchange_remote_object(&segment.object_key, None, first_wal)
            .unwrap();
        let candidate = super::super::ValidatedWalPruneCandidate {
            segment_id: 1,
            entry: segment.clone(),
            validated: super::super::ValidatedWalObject {
                proof: storage.remote_object_proof(&segment.object_key).unwrap(),
            },
        };
        let second_wal = framed_wal_bytes(2);
        let publication_path = directory.path().join("second.wal");
        std::fs::write(&publication_path, &second_wal).unwrap();
        storage
            .compare_exchange_remote_object(
                &crate::wal::cloud_segment_object_key(2, 7),
                None,
                second_wal,
            )
            .unwrap();
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        catalog.publish(7, segment.clone()).unwrap();
        let bytes = catalog.encode().unwrap();
        let primary_before = match schedule {
            "missing-primary" => None,
            "corrupt-primary" => Some(b"corrupt".to_vec()),
            _ => Some(bytes.clone()),
        };
        if let Some(bytes) = &primary_before {
            storage
                .compare_exchange_remote_object(primary, None, bytes.clone())
                .unwrap();
        }
        storage
            .compare_exchange_remote_object(mirror, None, bytes.clone())
            .unwrap();
        let guarded_validity = Arc::clone(&validity);
        storage
            .configure_write_authority(Arc::new(move || {
                guarded_validity
                    .remaining(7)
                    .map(|_| ())
                    .map_err(|error| MidgeError::Fenced(error.to_string()))
            }))
            .unwrap();
        Self {
            directory,
            validity,
            provider,
            storage,
            segment,
            candidate,
            publication_path,
            catalog,
            bytes,
            primary_before,
        }
    }

    fn arm(&self, schedule: &str) {
        let primary = crate::wal::cloud_catalog::OBJECT_KEY;
        let mirror = crate::wal::cloud_catalog::MIRROR_OBJECT_KEY;
        *self.provider.expire_read_key.lock().unwrap() = if schedule.starts_with("prepared-") {
            None
        } else {
            Some(
                if matches!(
                    schedule,
                    "startup-fence"
                        | "publication"
                        | "prune-proof"
                        | "prune-retirement"
                        | "floor"
                        | "retirement"
                ) {
                    primary
                } else {
                    mirror
                }
                .to_string(),
            )
        };
        #[cfg(feature = "failpoints")]
        if schedule.starts_with("prepared-") {
            let source = Arc::clone(&self.validity);
            let counter = std::sync::atomic::AtomicUsize::new(0);
            let ordinal = if schedule == "prepared-mirror" { 2 } else { 1 };
            fail::cfg_callback("midge::control::after_prepare_before_write", move || {
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == ordinal {
                    source.expire_for_test();
                }
            })
            .unwrap();
        }
    }

    fn run(self, schedule: &str) {
        // Arrange
        self.arm(schedule);
        let Self {
            directory: _directory,
            validity,
            provider,
            storage,
            segment,
            candidate,
            publication_path,
            mut catalog,
            bytes,
            primary_before,
        } = self;
        let primary = crate::wal::cloud_catalog::OBJECT_KEY;
        let deadline = crate::common::OperationDeadline::unbounded();
        let expected = if matches!(
            schedule,
            "mirror-after-commit" | "prepared-primary" | "prepared-mirror"
        ) {
            storage
                .read_control_object(primary, &catalog_budget(&storage), &deadline)
                .unwrap()
        } else {
            None
        };
        let validate = || {
            validity
                .remaining(7)
                .map(|_| ())
                .map_err(|error| MidgeError::Fenced(error.to_string()))
        };

        // Act
        let result = match schedule {
            "startup-fence" => storage.fence_cloud_wal_catalog(7).map(|_| ()),
            "publication" => {
                storage.publish_remote_wal_segment(2, 2, &publication_path, 7, &deadline)
            }
            "prune-proof" => {
                super::super::authoritative_wal_catalog_within(&storage, &deadline).map(|_| ())
            }
            "prune-retirement" | "prepared-prune-retirement" => {
                super::super::retire_covered_wal_catalog_prefix_within(
                    &storage,
                    vec![candidate],
                    7,
                    &deadline,
                    &mut Vec::new(),
                )
                .map(|_| ())
            }
            "floor" => storage.raise_wal_sequence_floor_with_authority(7, 11, &validate),
            "retirement" => {
                storage.retire_unreplayed_wal_segments_with_authority(7, &[segment], &validate)
            }
            "mirror-after-commit" | "prepared-primary" | "prepared-mirror" => {
                catalog.raise_sequence_floor(7, 11).unwrap();
                commit_catalog_with_authority(
                    &storage,
                    expected.as_ref(),
                    &catalog,
                    &deadline,
                    &validate,
                )
                .map(|_| ())
            }
            _ => load_and_repair_catalog_with_authority(&storage, &deadline, &validate).map(|_| ()),
        };

        assert!(
            matches!(result, Err(MidgeError::Fenced(_))),
            "{schedule}: {result:?}"
        );
        assert_retained_catalogs(
            &storage,
            &provider,
            schedule,
            primary_before,
            &catalog,
            &bytes,
        );
    }
}

fn assert_retained_catalogs(
    storage: &CloudPersistence,
    provider: &ExpiringCatalogRead,
    schedule: &str,
    primary_before: Option<Vec<u8>>,
    catalog: &WalPublicationCatalog,
    bytes: &[u8],
) {
    // Assert
    let primary = crate::wal::cloud_catalog::OBJECT_KEY;
    let mirror = crate::wal::cloud_catalog::MIRROR_OBJECT_KEY;
    assert_eq!(
        provider
            .writes_after_expiry
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "{schedule}"
    );
    let primary_after = storage.remote_object_proof_optional(primary).unwrap();
    let expected_primary = if matches!(schedule, "mirror-after-commit" | "prepared-mirror") {
        Some(serde_json::to_vec(catalog).unwrap())
    } else {
        primary_before
    };
    assert_eq!(
        primary_after
            .as_ref()
            .map(crate::storage::hybrid::backend::RemoteObjectProof::bytes),
        expected_primary.as_deref(),
        "{schedule}"
    );
    assert_eq!(
        storage.remote_object_proof(mirror).unwrap().bytes(),
        bytes,
        "{schedule}"
    );
}

#[test]
fn should_preserve_catalog_when_validity_expires_during_provider_read() {
    // Arrange
    #[cfg(feature = "failpoints")]
    let _guard = crate::failpoints::test_failpoint_guard();
    #[cfg(feature = "failpoints")]
    let _scenario = fail::FailScenario::setup();
    for schedule in [
        "startup-fence",
        "publication",
        "prune-proof",
        "prune-retirement",
        "floor",
        "retirement",
        "missing-primary",
        "corrupt-primary",
        "mirror-after-commit",
        #[cfg(feature = "failpoints")]
        "prepared-primary",
        #[cfg(feature = "failpoints")]
        "prepared-mirror",
        #[cfg(feature = "failpoints")]
        "prepared-prune-retirement",
    ] {
        let fixture = CatalogExpiryFixture::new(schedule);
        // Act
        fixture.run(schedule);
        // Assert: run checks Fenced, provider submissions, and both catalog copies.
    }
}

fn strict_recovery(
    root: &std::path::Path,
    remote: &Arc<dyn crate::storage::StorageBackend>,
    catalog: &WalPublicationCatalog,
) -> crate::runtime::cloud_startup::streaming_wal_plan::StreamingCloudWalRecovery {
    crate::runtime::cloud_startup::streaming_wal_plan::StreamingCloudWalRecovery::build(
        root,
        remote,
        catalog,
        crate::config::RecoveryPolicy::Strict,
        std::time::Duration::from_secs(5),
        127,
        crate::wal::recovery::streaming::StreamingReplayLimits {
            max_frame_bytes: 128 * 1024,
            max_pending_txn_bytes: 256 * 1024,
            max_memtable_encoded_bytes: 256 * 1024,
            target_memtable_encoded_bytes: 256 * 1024,
        },
    )
    .unwrap()
}

fn wait_wal_ack(storage: &CloudPersistence) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        for event in storage.process_uploads() {
            if let crate::storage::StorageEvent::CloudAck {
                segment_id: 1,
                max_sequence: 1,
            } = event
            {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "WAL acknowledgement watchdog expired"
        );
        std::thread::yield_now();
    }
}

fn wait_prune_completion(storage: &CloudPersistence) -> crate::storage::StorageOutcome<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        for event in storage.process_uploads() {
            if let crate::storage::StorageEvent::CloudWalPruneComplete {
                segment_id: 1,
                result,
            } = event
            {
                return result;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "prune completion watchdog expired"
        );
        std::thread::yield_now();
    }
}

fn successor_storage(
    root: &std::path::Path,
    remote: &Arc<dyn crate::storage::StorageBackend>,
) -> CloudPersistence {
    let successor = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        Arc::new(crate::storage::filesystem::FileSystem::new(root.join("successor")).unwrap()),
        Arc::clone(remote),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let successor_validity = Arc::new(crate::lease::LeaseValidity::new());
    successor_validity
        .activate(
            8,
            std::time::Instant::now() + std::time::Duration::from_mins(1),
        )
        .unwrap();
    successor
        .configure_write_authority(Arc::new(move || {
            successor_validity
                .remaining(8)
                .map(|_| ())
                .map_err(|error| MidgeError::Fenced(error.to_string()))
        }))
        .unwrap();
    successor
}

#[test]
fn should_preserve_republished_wal_when_predecessor_validity_expires_before_delete() {
    // Arrange: retain the valid local seal, as a failed unlink does.
    let fixture = CatalogExpiryFixture::new("gc-republication");
    let retained = fixture.directory.path().join("retained");
    std::fs::create_dir_all(retained.join("wal")).unwrap();
    let seal = retained.join("wal").join(crate::wal::segment_file_name(1));
    std::fs::write(&seal, framed_wal_bytes(1)).unwrap();
    let target = fixture.candidate.validated.proof.clone();
    let retired = super::super::retire_covered_wal_catalog_prefix_within(
        &fixture.storage,
        vec![fixture.candidate],
        7,
        &crate::common::OperationDeadline::unbounded(),
        &mut Vec::new(),
    )
    .unwrap();
    assert_eq!(retired.len(), 1);
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    *fixture.provider.paused_head.lock().unwrap() = Some(PausedHead {
        key: fixture.segment.object_key.clone(),
        started: started_tx,
        release: release_rx,
    });
    fixture
        .storage
        .delete_remote_object_guarded(1, target.clone())
        .unwrap();
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();

    // Act: successor plans and acknowledges the retained old-epoch seal.
    fixture.validity.expire_for_test();
    let remote: Arc<dyn crate::storage::StorageBackend> =
        Arc::new(CloudStorage::new(fixture.provider.clone(), String::new()));
    let successor = successor_storage(fixture.directory.path(), &remote);
    let catalog = successor.fence_cloud_wal_catalog(8).unwrap();
    assert!(strict_recovery(&retained, &remote, &catalog)
        .plan
        .local_segments
        .contains_key(&1));
    assert_eq!(
        successor.enqueue_wal_segment(1, &seal, 1).unwrap(),
        fixture.segment.object_key
    );
    wait_wal_ack(&successor);
    successor
        .publish_remote_wal_segment(
            1,
            1,
            &seal,
            8,
            &crate::common::OperationDeadline::unbounded(),
        )
        .unwrap();
    let republished = successor
        .remote_object_proof(&fixture.segment.object_key)
        .unwrap();
    assert!(republished.metadata().same_version(target.metadata()));
    std::fs::remove_file(&seal).unwrap();
    release_tx.send(()).unwrap();
    let completion = wait_prune_completion(&fixture.storage);

    // Assert
    assert!(
        matches!(completion, crate::storage::StorageOutcome::Err(_)),
        "{completion:?}"
    );
    assert_eq!(
        fixture
            .provider
            .delete_submissions
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(successor
        .remote_object_proof_optional(&fixture.segment.object_key)
        .unwrap()
        .is_some());
    let catalog = successor.fence_cloud_wal_catalog(8).unwrap();
    assert!(strict_recovery(&retained, &remote, &catalog)
        .plan
        .remote_segments
        .contains_key(&1));
}

#[test]
fn should_share_catalog_budget_when_maintenance_is_not_yet_configured() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap());
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local.clone(),
        local,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let first = catalog_budget(&storage);
    let _held = first.reserve(4096, "retained catalog").unwrap();

    // Act
    let second = catalog_budget(&storage);

    // Assert
    assert_eq!(second.used(), 4096);
    assert_eq!(storage.maintenance_memory().unwrap().used(), 4096);
}

#[test]
fn should_retain_catalog_decode_charge_until_authority_is_dropped() {
    // Arrange
    let bytes = WalPublicationCatalog::empty(7).unwrap().encode().unwrap();
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let catalog = AdmittedCatalog::decode(&bytes, &budget).unwrap();
    let retained = budget.used();
    drop(catalog);

    // Assert
    assert!(retained >= bytes.len());
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_preserve_catalog_encoding_when_serialization_is_admitted() {
    // Arrange
    let catalog = WalPublicationCatalog::empty(7).unwrap();
    let expected = serde_json::to_vec(&catalog).unwrap();
    let budget = ResourceBudget::new(expected.len());

    // Act
    let encoded = AdmittedEncoding::new(&catalog, &budget).unwrap();

    // Assert
    assert_eq!(encoded.bytes, expected);
    assert_eq!(budget.used(), expected.len());
    drop(encoded);
    assert_eq!(budget.used(), 0);
    assert!(matches!(
        AdmittedEncoding::new(&catalog, &ResourceBudget::new(expected.len() - 1)),
        Err(MidgeError::ResourceLimit(_))
    ));
}

#[test]
fn should_leave_catalog_authority_unchanged_when_decode_admission_fails() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap()),
        Arc::new(CloudStorage::with_mock()),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let mut catalog = WalPublicationCatalog::empty(7).unwrap();
    for segment_id in 0..128 {
        catalog
            .publish(
                7,
                PublishedWalSegment {
                    segment_id,
                    writer_epoch: 7,
                    max_sequence: segment_id + 1,
                    size_bytes: 1,
                    content_crc32c: 0,
                    object_key: crate::wal::cloud_segment_object_key(segment_id, 7),
                },
            )
            .unwrap();
    }
    let original = catalog.encode().unwrap();
    let key = crate::wal::cloud_catalog::OBJECT_KEY;
    storage
        .compare_exchange_remote_object(key, None, original.clone())
        .unwrap();
    storage.configure_maintenance_memory(256 * 1024);

    // Act
    let result = storage.fence_cloud_wal_catalog(8);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::ResourceLimit(_))),
        "{result:?}"
    );
    assert_eq!(storage.remote_object_proof(key).unwrap().bytes(), original);
    assert!(storage
        .remote_object_proof_optional(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
        .unwrap()
        .is_none());
    let budget = storage.maintenance_memory().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while budget.used() != 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(budget.used(), 0);
}

#[test]
fn should_release_decode_admission_when_catalog_is_corrupt() {
    // Arrange
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let result = AdmittedCatalog::decode(b"{broken", &budget);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(budget.used(), 0);
}
