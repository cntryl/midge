use super::*;
use crate::runtime::hybrid_persistence::{CatalogEntry, CloudPersistence, CloudStorage};
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

/// Sealed segment `segment_id` of the fixture's chain, written by epoch 7:
/// segment `n` holds one put at sequence `n` and links segment `n - 1`.
fn sealed_wal_bytes(segment_id: u64) -> Vec<u8> {
    let mut prev = crate::wal::format::ChainLink::GENESIS;
    let mut bytes = Vec::new();
    for id in 1..=segment_id {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            id,
        );
        (bytes, prev) = crate::wal::format::segment::SegmentBuilder::new(id, 7, prev)
            .record(&record)
            .seal();
    }
    bytes
}

struct CatalogExpiryFixture {
    directory: tempfile::TempDir,
    validity: Arc<crate::lease::LeaseValidity>,
    provider: Arc<ExpiringCatalogRead>,
    storage: CloudPersistence,
    segment: CatalogEntry,
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
            Arc::new(CloudStorage::new(provider.clone(), String::new())),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )));
        let primary = crate::wal::cloud_catalog::OBJECT_KEY;
        let mirror = crate::wal::cloud_catalog::MIRROR_OBJECT_KEY;
        let first_wal = sealed_wal_bytes(1);
        let segment = CatalogEntry::from_sealed_bytes(&first_wal).unwrap();
        storage
            .compare_exchange_remote_object(&segment.object_key(), None, first_wal)
            .unwrap();
        let second_wal = sealed_wal_bytes(2);
        let publication_path = directory.path().join("second.wal");
        std::fs::write(&publication_path, &second_wal).unwrap();
        storage
            .compare_exchange_remote_object(&crate::wal::segment_object_key(2, 7), None, second_wal)
            .unwrap();
        let mut catalog = WalPublicationCatalog::empty(7).unwrap();
        catalog
            .append_run(7, std::slice::from_ref(&segment))
            .unwrap();
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
                        | "retirement-read"
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
            "publication" => storage.publish_remote_wal_segment(
                2,
                2,
                &crate::io::FsLocation::host_file(&publication_path),
                7,
                &deadline,
            ),
            "retirement-read" => {
                super::super::authoritative_wal_catalog_within(&storage, &deadline).map(|_| ())
            }
            "prune-retirement" | "prepared-prune-retirement" => mutate_wal_catalog_within(
                &storage,
                "cloud WAL retirement",
                &deadline,
                &|| storage.check_write_authority(),
                Missing::Refuse,
                |catalog| {
                    catalog
                        .retire_prefix(
                            7,
                            std::slice::from_ref(&segment),
                            crate::lease::SharedGenerationId::FIRST,
                        )
                        .map(|retired| Mutation::of(!retired.is_empty(), retired))
                },
            )
            .and_then(|result| result.map(|_| ()).map_err(super::super::refusal_error)),
            "floor" => storage.set_aside_catalog_suffix_with_authority_within(
                7,
                None,
                &[],
                11,
                &validate,
                &deadline,
            ),
            "retirement" => storage.set_aside_catalog_suffix_with_authority_within(
                7,
                Some(segment.id()),
                &[segment],
                11,
                &validate,
                &deadline,
            ),
            "mirror-after-commit" | "prepared-primary" | "prepared-mirror" => {
                catalog.set_aside_suffix(7, None, &[], 11).unwrap();
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
        "retirement-read",
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

/// Plans strict recovery as cloud startup does, over a cache whose WAL
/// ledger starts past every cataloged segment.
fn strict_recovery(
    root: &std::path::Path,
    remote: &Arc<dyn crate::storage::StorageBackend>,
    catalog: &WalPublicationCatalog,
) -> crate::runtime::cloud_startup::streaming_wal_plan::StreamingCloudWalRecovery {
    let wal: Arc<dyn crate::io::Fs> = Arc::new(crate::io::RealFs::new(root.join("wal")).unwrap());
    // A cache that joined the catalog: its first segment follows the head.
    let history = match crate::runtime::wal_history::WalHistory::load(Arc::clone(&wal), 0).unwrap()
    {
        Some(history) => history,
        None => crate::runtime::wal_history::WalHistory::genesis(
            wal,
            0,
            catalog.head(),
            catalog.next_segment_id(),
        )
        .unwrap(),
    };
    crate::runtime::cloud_startup::streaming_wal_plan::StreamingCloudWalRecovery::build(
        &crate::io::real::test_root(root),
        Some(history.ledger()),
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

fn successor_storage(remote: &Arc<dyn crate::storage::StorageBackend>) -> CloudPersistence {
    let successor = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
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

fn retain_local_seal(fixture: &CatalogExpiryFixture) -> (std::path::PathBuf, std::path::PathBuf) {
    let retained = fixture.directory.path().join("retained");
    std::fs::create_dir_all(retained.join("wal")).unwrap();
    let seal = retained.join("wal").join(crate::wal::segment_file_name(1));
    std::fs::write(&seal, sealed_wal_bytes(1)).unwrap();
    crate::runtime::wal_history::fixture::record_at(&retained, &[1], 2).unwrap();
    (retained, seal)
}

#[test]
fn should_preserve_republished_wal_when_predecessor_validity_expires_before_delete() {
    // Arrange: retain the valid local seal, as a cache whose ledger has not
    // retired it yet does.
    let fixture = CatalogExpiryFixture::new("gc-republication");
    let (retained, seal) = retain_local_seal(&fixture);
    let target = fixture
        .storage
        .remote_object_proof(&fixture.segment.object_key())
        .unwrap();
    let retired = mutate_wal_catalog_within(
        &fixture.storage,
        "cloud WAL retirement",
        &crate::common::OperationDeadline::unbounded(),
        &|| fixture.storage.check_write_authority(),
        Missing::Refuse,
        |catalog| {
            catalog
                .retire_prefix(
                    7,
                    std::slice::from_ref(&fixture.segment),
                    crate::lease::SharedGenerationId::FIRST,
                )
                .map(|retired| Mutation::of(!retired.is_empty(), retired))
        },
    )
    .unwrap()
    .unwrap()
    .0;
    assert_eq!(retired.len(), 1);
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    *fixture.provider.paused_head.lock().unwrap() = Some(PausedHead {
        key: fixture.segment.object_key(),
        started: started_tx,
        release: release_rx,
    });
    fixture
        .storage
        .delete_remote_objects_by_identity(vec![(
            1,
            fixture.segment.object_key(),
            target.metadata().clone(),
        )])
        .unwrap();
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();

    // Act: the successor plans over the retained seal of segment 1, which
    // the catalog already retired, and uploads its identical bytes again.
    fixture.validity.expire_for_test();
    let remote: Arc<dyn crate::storage::StorageBackend> =
        Arc::new(CloudStorage::new(fixture.provider.clone(), String::new()));
    let successor = successor_storage(&remote);
    let catalog = successor.fence_cloud_wal_catalog(8).unwrap();
    assert!(
        !strict_recovery(&retained, &remote, &catalog)
            .plan
            .local_segments
            .contains_key(&1),
        "the catalog retired segment 1, so no writer owes it again"
    );
    assert_eq!(
        successor
            .enqueue_wal_segment(&crate::io::FsLocation::host_file(&seal), 1, 1)
            .unwrap(),
        fixture.segment.object_key()
    );
    wait_wal_ack(&successor);
    let republished = successor
        .remote_object_proof(&fixture.segment.object_key())
        .unwrap();
    assert!(republished.metadata().same_version(target.metadata()));
    let wal: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(retained.join("wal")).unwrap());
    crate::runtime::wal_history::WalHistory::open(wal, 1, 0)
        .unwrap()
        .retire_below(2)
        .unwrap();
    assert!(
        seal.exists(),
        "retired files await GC and cannot be replayed"
    );
    release_tx.send(()).unwrap();
    let completion = wait_prune_completion(&fixture.storage);

    // Assert: the expired predecessor deletes nothing, and the object the
    // successor wrote again survives.
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
        .remote_object_proof_optional(&fixture.segment.object_key())
        .unwrap()
        .is_some());
    let catalog = successor.fence_cloud_wal_catalog(8).unwrap();
    assert_eq!(catalog.anchor(), fixture.segment.link());
    assert_eq!(catalog.entries(), []);
}

#[test]
fn should_report_incompatible_catalog_version_without_repairing_from_mirror() {
    // Arrange: the primary catalog is another format version; the mirror is
    // a valid catalog of this one.
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        Arc::new(CloudStorage::with_mock()),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let primary = br#"{"format_version":99,"fencing_epoch":7,"segments":{}}"#.to_vec();
    let mirror = WalPublicationCatalog::empty(7).unwrap().encode().unwrap();
    storage
        .compare_exchange_remote_object(
            crate::wal::cloud_catalog::OBJECT_KEY,
            None,
            primary.clone(),
        )
        .unwrap();
    storage
        .compare_exchange_remote_object(
            crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
            None,
            mirror.clone(),
        )
        .unwrap();

    // Act
    let result = storage.fence_cloud_wal_catalog(8);

    // Assert: the open refuses before any write; neither copy changes.
    assert!(
        matches!(result, Err(MidgeError::CompatibilityError(_))),
        "{result:?}"
    );
    assert_eq!(
        storage
            .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
            .unwrap()
            .bytes(),
        primary
    );
    assert_eq!(
        storage
            .remote_object_proof(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
            .unwrap()
            .bytes(),
        mirror
    );
}

#[test]
fn should_share_catalog_budget_when_maintenance_is_not_yet_configured() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap());
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
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
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        Arc::new(CloudStorage::with_mock()),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let mut catalog = WalPublicationCatalog::empty(7).unwrap();
    let mut prev = crate::wal::format::ChainLink::GENESIS;
    for segment_id in 1..=1024_u64 {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            segment_id,
        );
        let (bytes, link) = crate::wal::format::segment::SegmentBuilder::new(segment_id, 7, prev)
            .record(&record)
            .seal();
        let entry = CatalogEntry::from_sealed_bytes(&bytes).expect("verified fixture segment");
        prev = link;
        catalog.append_run(7, &[entry]).unwrap();
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
fn should_map_to_a_non_retriable_error_when_a_catalog_rule_refuses() {
    // Arrange
    let fenced = CatalogRefusal::EpochMismatch {
        catalog: 9,
        writer: 8,
    };
    let contradicted = CatalogRefusal::NotOldestPrefix {
        segment_id: 2,
        oldest: Some(1),
    };

    // Act
    let fenced = refusal_error(fenced);
    let contradicted = refusal_error(contradicted);

    // Assert: neither is an error a retry could clear.
    assert!(matches!(fenced, MidgeError::Fenced(_)));
    assert!(matches!(contradicted, MidgeError::Corruption(_)));
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
