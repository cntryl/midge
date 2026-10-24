use super::super::read_committed_metadata_bodies_within;
use super::*;
use crate::io::{Fs, FsPath};
use crate::lease::fs_leader_store::FsLeaderStore;
use crate::lease::LeaderStore;
use crate::metadata::{files, Manifest, ManifestPersistence};
use crate::runtime::{
    IntentLogEntry, IntentPersistence, MetadataPublicationLock, PublicationPhase,
};
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudReadCapability, CloudStorage, CloudWriteCapability,
    MockCloudBackend,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

struct HeldManifestBackend {
    inner: Arc<MockCloudBackend>,
    hold: AtomicBool,
    entered: mpsc::Sender<()>,
    released: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

impl CloudReadCapability for HeldManifestBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity);
}
impl CloudWriteCapability for HeldManifestBackend {
    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        if key.contains("manifest/") && self.hold.swap(false, Ordering::AcqRel) {
            let inner = Arc::clone(&self.inner);
            let released = Arc::clone(&self.released);
            let key = key.to_string();
            self.entered.send(()).unwrap();
            std::thread::spawn(move || {
                let (lock, changed) = &*released;
                let _ready = changed
                    .wait_while(lock.lock().unwrap(), |ready| !*ready)
                    .unwrap();
                inner.submit_put(&key, bytes, headers, callback);
            });
        } else {
            self.inner.submit_put(key, bytes, headers, callback);
        }
    }
}
impl crate::storage::cloud::CloudDeleteCapability for HeldManifestBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
}
impl crate::storage::cloud::CloudListCapability for HeldManifestBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
}
impl crate::storage::cloud::CloudHeadCapability for HeldManifestBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}
impl CloudBackend for HeldManifestBackend {}

struct Fixture {
    _directory: tempfile::TempDir,
    fs: Arc<dyn Fs>,
    cloud: Arc<CloudStorage>,
    store: Arc<FsLeaderStore>,
    lock: MetadataPublicationLock,
}

struct LostCasStore {
    inner: Arc<FsLeaderStore>,
    memory: crate::common::resource_budget::ResourceBudget,
    occupied: parking_lot::Mutex<Option<crate::common::resource_budget::ResourceReservation>>,
    mutations: std::sync::atomic::AtomicUsize,
}

impl LeaderStore for LostCasStore {
    fn acquire_leadership(&self, holder: &str) -> Result<crate::lease::LeaderRecord, LeaseError> {
        self.inner.acquire_leadership(holder)
    }
    fn read_current(&self) -> Result<Option<crate::lease::LeaderRecord>, LeaseError> {
        self.inner.read_current()
    }
    fn read_committed_metadata(&self, timeout: Duration) -> Result<CloudMetadataHead, LeaseError> {
        self.inner.read_committed_metadata(timeout)
    }
    fn publish_committed_metadata(
        &self,
        holder: &str,
        epoch: u64,
        previous: Option<&CloudMetadataGeneration>,
        generation: CloudMetadataGeneration,
        timeout: Duration,
    ) -> Result<(), LeaseError> {
        self.inner
            .publish_committed_metadata(holder, epoch, previous, generation, timeout)?;
        self.mutations.fetch_add(1, Ordering::AcqRel);
        // This is the genuine durable pointer mutation; its response alone is
        // lost. Occupy all remaining shared room before exact readback.
        *self.occupied.lock() = Some(
            self.memory
                .reserve(
                    self.memory.limit() - self.memory.used(),
                    "other maintenance before lost receipt readback",
                )
                .unwrap(),
        );
        Err(LeaseError::Indeterminate(
            "lost successful conditional pointer response".into(),
        ))
    }
}

#[test]
fn should_resolve_successful_unknown_pointer_cas_in_small_pool_without_second_body_allocation() {
    // Arrange
    let fixture = Fixture::new(CloudStorage::with_mock());
    let memory = crate::common::resource_budget::ResourceBudget::new(32 * 1024);
    fixture.lock.configure_capture_memory(memory.clone());
    let cut = fixture.cut();
    let store = LostCasStore {
        inner: Arc::clone(&fixture.store),
        memory: memory.clone(),
        occupied: parking_lot::Mutex::new(None),
        mutations: std::sync::atomic::AtomicUsize::new(0),
    };
    let deadline = OperationDeadline::from_budget(Duration::from_millis(100));

    // Act: exact validated staged identity uses the capture's already charged
    // workspace even though no additional read allocation can be admitted.
    mirror_captured_control_metadata_within(
        CloudMetadataMirrorContext {
            cloud: &fixture.cloud,
            fs: fixture.fs.as_ref(),
            publication_lock: &fixture.lock,
            lock_wait_budget: Duration::from_millis(100),
            deadline: &deadline,
            authority: super::super::CloudMetadataMirrorAuthority {
                store: &store,
                holder_id: "writer",
                writer_epoch: 1,
            },
        },
        &cut,
        |_| Ok(()),
    )
    .unwrap();

    // Assert: one real pointer effect, no blind CAS retry, and owned charge
    // remains until both the foreign owner and actual source are released.
    assert_eq!(store.mutations.load(Ordering::Acquire), 1);
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert_eq!(memory.used(), memory.limit());
    assert!(matches!(
        fixture
            .store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Committed(_)
    ));
    drop(store.occupied.lock().take());
    drop(cut);
    assert_eq!(memory.used(), 0);
}

impl Fixture {
    fn new(cloud: CloudStorage) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let fs = crate::io::real::test_root(directory.path());
        crate::metadata::ensure_or_create_format_marker(&fs, directory.path()).unwrap();
        ManifestPersistence::save_with_fs(&fs, &Manifest::default()).unwrap();
        let store = Arc::new(FsLeaderStore::new(
            Arc::clone(&fs),
            crate::lease::LeaseClock::default(),
        ));
        store.acquire_leadership("writer").unwrap();
        Self {
            _directory: directory,
            fs,
            cloud: Arc::new(cloud),
            store,
            lock: MetadataPublicationLock::default(),
        }
    }
    fn cut(&self) -> Arc<CapturedMetadataCut> {
        self.lock
            .capture_control_cut(
                &self.fs,
                1,
                &OperationDeadline::from_budget(Duration::from_secs(5)),
            )
            .unwrap()
    }
    fn mirror(&self, cut: &CapturedMetadataCut) -> MidgeResult<()> {
        self.mirror_for(cut, Duration::from_secs(5))
    }
    fn mirror_for(&self, cut: &CapturedMetadataCut, timeout: Duration) -> MidgeResult<()> {
        let deadline = OperationDeadline::from_budget(timeout);
        mirror_captured_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &self.cloud,
                fs: self.fs.as_ref(),
                publication_lock: &self.lock,
                lock_wait_budget: Duration::from_secs(5),
                deadline: &deadline,
                authority: super::super::CloudMetadataMirrorAuthority {
                    store: self.store.as_ref(),
                    holder_id: "writer",
                    writer_epoch: 1,
                },
            },
            cut,
            |_| Ok(()),
        )
    }
    fn selected_bodies(&self) -> (CloudMetadataGeneration, BTreeMap<String, Vec<u8>>) {
        let CloudMetadataHead::Committed(generation) = self
            .store
            .read_committed_metadata(Duration::from_secs(5))
            .unwrap()
        else {
            panic!("selected generation");
        };
        let bodies = read_committed_metadata_bodies_within(
            &self.cloud,
            &generation,
            &OperationDeadline::from_budget(Duration::from_secs(5)),
        )
        .unwrap();
        (generation, bodies)
    }
}

#[test]
fn should_charge_shared_capture_pool_before_copy_and_retain_it_until_source_owner_finishes() {
    // Arrange
    let fixture = Fixture::new(CloudStorage::with_mock());
    let memory = crate::common::resource_budget::ResourceBudget::new(128 * 1024);
    fixture.lock.configure_capture_memory(memory.clone());
    let busy = memory
        .reserve(memory.limit() - 128, "other admitted maintenance")
        .unwrap();

    // Act / Assert: rejected admission allocates no owned cut and leaves the
    // existing maintenance owner charged. Releasing it permits real capture.
    let rejected = fixture.lock.capture_control_cut(
        &fixture.fs,
        1,
        &OperationDeadline::from_budget(Duration::from_secs(1)),
    );
    let Err(error) = rejected else {
        panic!("shared pool must reject capture while occupied");
    };
    assert!(fixture.lock.capture_contention(&error));
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert_eq!(memory.used(), memory.limit() - 128);
    drop(busy);
    assert!(!fixture.lock.capture_contention(&error));
    assert!(
        fixture.lock.capture_admission_retryable(&error),
        "release between rejection and classification still requires retry"
    );
    let cut = fixture.cut();
    let source_charge = memory.used();
    assert!(source_charge > 4096);
    fixture.mirror(&cut).unwrap();
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert_eq!(
        memory.used(),
        source_charge,
        "a selected cut may still have a live source task"
    );
    drop(cut);
    assert_eq!(memory.used(), 0);
}

#[test]
fn should_retry_owned_cut_slot_contention_and_release_capacity_after_source_owners_finish() {
    // Arrange
    let fixture = Fixture::new(CloudStorage::with_mock());
    let mut owned = (0..64).map(|_| fixture.cut()).collect::<Vec<_>>();
    let rejected = fixture.lock.capture_control_cut(
        &fixture.fs,
        1,
        &OperationDeadline::from_budget(Duration::from_secs(1)),
    );
    let Err(error) = rejected else {
        panic!("owned capture capacity must be bounded");
    };

    // Act / Assert: capacity exhaustion is the originating slot-pool
    // contention, rather than intent-proof Busy or a permanent size failure.
    assert!(fixture.lock.capture_admission_retryable(&error));
    fixture.mirror(owned.last().unwrap()).unwrap();
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert!(fixture.lock.capture_admission_retryable(&error));
    owned.clear();
    let successor = fixture.cut();
    fixture.mirror(&successor).unwrap();
    assert_eq!(fixture.lock.pending_captures(), 0);
}

#[test]
fn should_keep_unknown_physical_put_charged_after_later_cut_covers_its_capture() {
    // Arrange
    let (entered, arrived) = mpsc::channel();
    let released = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let backend = Arc::new(HeldManifestBackend {
        inner: Arc::new(MockCloudBackend::new()),
        hold: AtomicBool::new(true),
        entered,
        released: Arc::clone(&released),
    });
    let fixture = Fixture::new(CloudStorage::new(backend, "physical-capture-budget".into()));
    let memory = crate::common::resource_budget::ResourceBudget::new(256 * 1024);
    fixture.lock.configure_capture_memory(memory.clone());
    let first = fixture.cut();
    assert!(matches!(
        fixture.mirror_for(&first, Duration::from_millis(20)),
        Err(MidgeError::Timeout(_))
    ));
    arrived.recv_timeout(Duration::from_secs(1)).unwrap();
    drop(first);

    // Act: the later selected proof settles the logical capture while the
    // earlier actual transport still retains its payload/reservation callback.
    let second = fixture.cut();
    fixture.mirror(&second).unwrap();
    drop(second);
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert!(
        memory.used() > 0,
        "physical request must retain its source accounting after coalescence"
    );
    let (lock, changed) = &*released;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while memory.used() != 0 && std::time::Instant::now() < until {
        std::thread::yield_now();
    }

    // Assert
    assert_eq!(memory.used(), 0);
}

#[test]
fn should_coalesce_older_capture_when_later_cut_commits_during_held_provider_put() {
    // Arrange: the first immutable PUT remains physically outstanding while
    // the later exact capture reaches its own pointer CAS independently.
    let (entered_tx, entered_rx) = mpsc::channel();
    let released = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let backend = Arc::new(HeldManifestBackend {
        inner: Arc::new(MockCloudBackend::new()),
        hold: AtomicBool::new(true),
        entered: entered_tx,
        released: Arc::clone(&released),
    });
    let fixture = Arc::new(Fixture::new(CloudStorage::new(
        backend,
        "ordered-cuts".into(),
    )));
    let first = fixture.cut();
    let owner = Arc::clone(&fixture);
    let old_task = std::thread::spawn(move || owner.mirror(&first));
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    IntentPersistence::save_with_fs(
        &fixture.fs,
        &[IntentLogEntry::SeqnoAllocated {
            seqno: 29,
            cf_id: 0,
        }],
    )
    .unwrap();
    let second = fixture.cut();

    // Act
    fixture.mirror(&second).unwrap();
    let (selected, expected) = fixture.selected_bodies();
    let (lock, changed) = &*released;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    old_task.join().unwrap().unwrap();

    // Assert: delayed older work neither rewrites the head nor acknowledges
    // an invented scalar boundary; the selected exact body includes its proof.
    let (after, actual) = fixture.selected_bodies();
    assert_eq!(after, selected);
    assert_eq!(actual, expected);
    assert_eq!(fixture.lock.pending_captures(), 0);
    let recovered: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    for (name, bytes) in actual {
        recovered
            .open(
                &FsPath::new(name),
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .unwrap()
            .append(bytes.into())
            .unwrap();
    }
    let intents = IntentPersistence::load_with_fs_and_policy(
        &recovered,
        crate::config::RecoveryPolicy::Strict,
    )
    .unwrap();
    assert!(matches!(
        intents.as_slice(),
        [IntentLogEntry::SeqnoAllocated {
            seqno: 29,
            cf_id: 0
        }]
    ));
    assert_eq!(
        fixture.lock.lock().cached.as_ref().unwrap().generation,
        selected
    );
}

fn flush_intent() -> IntentLogEntry {
    IntentLogEntry::FlushPublish {
        phase: PublicationPhase::OutputDurable,
        cf_id: 0,
        sequence: 10,
        file_meta: crate::runtime::FileMeta {
            name: crate::cloud_layout::file_name(0, 0, 10),
            level: 0,
            size_bytes: 3,
            content_crc32c: Some(7),
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            smallest_seq: None,
            largest_seq: None,
            key_bounds_complete: false,
            earliest_expiration_millis: None,
        },
    }
}

#[test]
fn should_retain_unsettled_intent_when_new_capture_attempts_to_drop_it() {
    // Arrange
    let fixture = Fixture::new(CloudStorage::with_mock());
    IntentPersistence::save_with_fs(&fixture.fs, &[flush_intent()]).unwrap();
    let first = fixture.cut();
    IntentPersistence::save_with_fs(&fixture.fs, &[]).unwrap();

    // Act / Assert: no witness may grant coverage of an omitted obligation.
    assert!(matches!(
        fixture.lock.capture_control_cut(
            &fixture.fs,
            1,
            &OperationDeadline::from_budget(Duration::from_secs(1))
        ),
        Err(MidgeError::Busy(_))
    ));
    assert_eq!(fixture.lock.pending_captures(), 1);
    fixture.mirror(&first).unwrap();
    // Selecting OutputDurable is not a terminal operation receipt.
    assert!(matches!(
        fixture.lock.capture_control_cut(
            &fixture.fs,
            1,
            &OperationDeadline::from_budget(Duration::from_secs(1))
        ),
        Err(MidgeError::Busy(_))
    ));
    let mut published = flush_intent();
    let IntentLogEntry::FlushPublish {
        phase, file_meta, ..
    } = &mut published
    else {
        unreachable!()
    };
    *phase = PublicationPhase::ManifestPublished;
    let store = crate::metadata::store::ManifestStore::new(Arc::clone(&fixture.fs));
    store
        .append(&crate::metadata::ManifestEdit::Batch(vec![
            crate::metadata::ManifestEdit::AddSst(crate::metadata::FileMeta::from(&*file_meta)),
            crate::metadata::ManifestEdit::AdvanceFlushFrontier {
                cf_id: 0,
                freeze_sequence: 10,
            },
        ]))
        .unwrap();
    IntentPersistence::save_with_fs(&fixture.fs, &[published]).unwrap();
    let manifest_cut = fixture.cut();
    fixture.mirror(&manifest_cut).unwrap();
    IntentPersistence::save_with_fs(&fixture.fs, &[]).unwrap();
    let second = fixture.cut();
    fixture.mirror(&second).unwrap();
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert!(IntentPersistence::load_with_fs_and_policy(
        second.fs(),
        crate::config::RecoveryPolicy::Strict
    )
    .unwrap()
    .is_empty());
}

#[test]
fn should_keep_unknown_capture_owned_until_exact_later_coverage_is_selected() {
    // Arrange: dropping a caller handle is not proof a provider effect ended.
    let fixture = Fixture::new(CloudStorage::with_mock());
    let first = fixture.cut();
    let weak = Arc::downgrade(&first);
    drop(first);
    assert!(weak.upgrade().is_some());
    assert_eq!(fixture.lock.pending_captures(), 1);

    // Act
    let second = fixture.cut();
    fixture.mirror(&second).unwrap();

    // Assert
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert!(weak.upgrade().is_none());
    let (_, bodies) = fixture.selected_bodies();
    assert_eq!(
        bodies[files::FORMAT],
        crate::metadata::format::current_format_marker_bytes()
    );
}

#[test]
fn should_rebase_newer_capture_when_earlier_cut_wins_its_predecessor_cas() {
    // Arrange
    let (entered_tx, entered_rx) = mpsc::channel();
    let released = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let backend = Arc::new(HeldManifestBackend {
        inner: Arc::new(MockCloudBackend::new()),
        hold: AtomicBool::new(true),
        entered: entered_tx,
        released: Arc::clone(&released),
    });
    let fixture = Arc::new(Fixture::new(CloudStorage::new(
        backend,
        "rebased-cuts".into(),
    )));
    let first = fixture.cut();
    IntentPersistence::save_with_fs(
        &fixture.fs,
        &[IntentLogEntry::SeqnoAllocated {
            seqno: 31,
            cf_id: 0,
        }],
    )
    .unwrap();
    let second = fixture.cut();
    let owner = Arc::clone(&fixture);
    let new_task = std::thread::spawn(move || owner.mirror(&second));
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    // Act: the older cut legitimately wins before the newer cut's upload ends.
    fixture.mirror(&first).unwrap();
    let (earlier, _) = fixture.selected_bodies();
    let (lock, changed) = &*released;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    new_task.join().unwrap().unwrap();

    // Assert: same-epoch contention advances to the exact next generation and
    // retains the newer capture's bytes rather than fencing a healthy writer.
    let (later, bodies) = fixture.selected_bodies();
    assert_eq!(later.id, earlier.id.checked_next().unwrap());
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert_eq!(
        bodies[files::INTENT_LOG],
        crate::io::read_all(fixture.fs.as_ref(), &FsPath::new(files::INTENT_LOG)).unwrap()
    );
}

#[test]
fn should_reject_owned_capture_when_successor_takes_writer_epoch() {
    // Arrange
    let fixture = Fixture::new(CloudStorage::with_mock());
    let cut = fixture.cut();
    fixture.store.release_leadership("writer", 1).unwrap();
    fixture.store.acquire_leadership("successor").unwrap();

    // Act / Assert
    assert!(matches!(fixture.mirror(&cut), Err(MidgeError::Fenced(_))));
    assert_eq!(fixture.lock.pending_captures(), 1);
    assert_eq!(
        fixture
            .store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Uncommitted
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn should_capture_exact_checkpoint_cut_when_snapshot_installation_is_between_rename_and_truncate() {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: the production checkpoint owns the writer lock after its new
    // snapshot is durable but before its covered journal prefix is removed.
    let fixture = Arc::new(Fixture::new(CloudStorage::with_mock()));
    let store = Arc::new(crate::metadata::store::ManifestStore::new(Arc::clone(
        &fixture.fs,
    )));
    let edit = crate::metadata::ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: 10,
    };
    let id = store.append(&edit).unwrap();
    let mut checkpoint = Manifest::default();
    checkpoint.apply_edit(&edit).unwrap();
    checkpoint.note_applied_journal_edit(id);
    let (entered, arrived) = crossbeam::channel::bounded(1);
    let (release, resumed) = crossbeam::channel::bounded(1);
    fail::cfg_callback(
        "midge::manifest::after_snapshot_rename_before_journal_truncate",
        move || {
            entered.send(()).unwrap();
            resumed.recv().unwrap();
        },
    )
    .unwrap();
    let writer = Arc::clone(&store);
    let install = std::thread::spawn(move || {
        writer.save_captured_snapshot_for(
            crate::metadata::accounting::Origin::CompactionBeforeGc,
            &checkpoint,
        )
    });
    arrived.recv_timeout(Duration::from_secs(2)).unwrap();

    // Act: capture must wait for the complete writer cut, rather than select
    // its files independently while checkpoint truncation changes them.
    let blocked = fixture.lock.capture_control_cut(
        &fixture.fs,
        1,
        &OperationDeadline::from_budget(Duration::from_millis(20)),
    );
    release.send(()).unwrap();
    install.join().unwrap().unwrap();
    fail::remove("midge::manifest::after_snapshot_rename_before_journal_truncate");
    let cut = fixture.cut();
    store
        .append(&crate::metadata::ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 20,
        })
        .unwrap();
    fixture.mirror(&cut).unwrap();
    let (generation, bodies) = fixture.selected_bodies();
    let selected = decode_generation_manifest(
        &generation,
        &bodies,
        &OperationDeadline::from_budget(Duration::from_secs(1)),
    )
    .unwrap();

    // Assert: the admitted immutable cut remains exactly the checkpoint even
    // after another live journal edit; recovery never invents a mixed image.
    assert!(matches!(blocked, Err(MidgeError::Timeout(_))));
    assert_eq!(selected.next_sst_seqs[&0], 10);
    assert_eq!(selected.edit_checkpoint_id, id);
    let current = ManifestPersistence::load_with_fs_and_policy(
        &fixture.fs,
        crate::config::RecoveryPolicy::Strict,
    )
    .unwrap();
    assert_eq!(current.next_sst_seqs[&0], 20);
    scenario.teardown();
}
