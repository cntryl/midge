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

pub(in crate::runtime::hybrid_persistence) struct HeldReadbackBackend {
    inner: Arc<MockCloudBackend>,
    hold: AtomicBool,
    entered: mpsc::Sender<()>,
    released: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

impl HeldReadbackBackend {
    pub(in crate::runtime::hybrid_persistence) fn new(
        inner: Arc<MockCloudBackend>,
        entered: mpsc::Sender<()>,
        released: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    ) -> Self {
        Self {
            inner,
            hold: AtomicBool::new(false),
            entered,
            released,
        }
    }

    pub(in crate::runtime::hybrid_persistence) fn hold_next_read(&self) {
        self.hold.store(true, Ordering::Release);
    }
}

impl CloudReadCapability for HeldReadbackBackend {
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        if !key.contains("manifest/") || !self.hold.swap(false, Ordering::AcqRel) {
            self.inner.submit_get(key, callback);
            return;
        }
        // Fetch the genuine immutable body before delaying its callback. The
        // default typed read boundary retains any source reservation until
        // this physical payload/callback owner actually completes.
        let (tx, rx) = mpsc::channel();
        self.inner.submit_get(key, tx);
        let event = rx.recv().unwrap();
        assert!(matches!(
            &event,
            crate::storage::cloud::CloudEvent::Get { result: Ok(bytes), .. } if !bytes.is_empty()
        ));
        let released = Arc::clone(&self.released);
        let entered = self.entered.clone();
        std::thread::spawn(move || {
            entered.send(()).unwrap();
            let (lock, changed) = &*released;
            let _ready = changed
                .wait_while(lock.lock().unwrap(), |ready| !*ready)
                .unwrap();
            let _ = callback.send(event);
        });
    }
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get_with_metadata, submit_get_range, submit_get_range_with_identity);
}
impl CloudWriteCapability for HeldReadbackBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_put);
}
impl crate::storage::cloud::CloudDeleteCapability for HeldReadbackBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
}
impl crate::storage::cloud::CloudListCapability for HeldReadbackBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
}
impl crate::storage::cloud::CloudHeadCapability for HeldReadbackBackend {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}
impl CloudBackend for HeldReadbackBackend {}

impl Drop for HeldReadbackBackend {
    fn drop(&mut self) {
        let (lock, changed) = &*self.released;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
}

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
    // The compatibility provider also owns its existing 64 KiB completion
    // stack during immutable upload. The CAS hook then fills every remaining
    // byte, so exact selected readback cannot reserve a second body workspace.
    let memory = crate::common::resource_budget::ResourceBudget::new(128 * 1024);
    fixture.lock.configure_capture_memory(memory.clone());
    let cut = fixture.cut();
    let store = LostCasStore {
        inner: Arc::clone(&fixture.store),
        memory: memory.clone(),
        occupied: parking_lot::Mutex::new(None),
        mutations: std::sync::atomic::AtomicUsize::new(0),
    };
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

    // Act: exact validated staged identity uses the capture's already charged
    // workspace even though no additional read allocation can be admitted.
    mirror_captured_control_metadata_within(
        CloudMetadataMirrorContext {
            cloud: &fixture.cloud,
            fs: fixture.fs.as_ref(),
            publication_lock: &fixture.lock,
            lock_wait_budget: Duration::from_secs(5),
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
fn should_account_owned_capture_bytes_until_the_source_owner_finishes() {
    // Arrange
    let fixture = Fixture::new(CloudStorage::with_mock());
    let memory = crate::common::resource_budget::ResourceBudget::new(128 * 1024);
    fixture.lock.configure_capture_memory(memory.clone());
    let busy = memory
        .reserve(memory.limit() - 128, "other admitted maintenance")
        .unwrap();

    // Act: attempt capture while the existing maintenance owner is charged.
    let rejected = fixture.lock.capture_control_cut(
        &fixture.fs,
        1,
        &OperationDeadline::from_budget(Duration::from_secs(1)),
    );
    // Assert: rejected admission preserves the existing maintenance charge.
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
fn should_restore_capture_slot_availability_after_owned_sources_finish() {
    // Arrange
    let fixture = Fixture::new(CloudStorage::with_mock());
    let mut owned = (0..64).map(|_| fixture.cut()).collect::<Vec<_>>();
    // Act: request one capture beyond the capacity owned by live sources.
    let rejected = fixture.lock.capture_control_cut(
        &fixture.fs,
        1,
        &OperationDeadline::from_budget(Duration::from_secs(1)),
    );
    let Err(error) = rejected else {
        panic!("owned capture capacity must be bounded");
    };

    // Assert: capacity exhaustion is the originating slot-pool
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
        hold: AtomicBool::new(false),
        entered,
        released: Arc::clone(&released),
    });
    let fixture = Fixture::new(CloudStorage::new(
        backend.clone(),
        "physical-capture-budget".into(),
    ));
    let memory = crate::common::resource_budget::ResourceBudget::new(256 * 1024);
    fixture.lock.configure_capture_memory(memory.clone());
    install_terminal_flush_fixture(&fixture, 1);
    let predecessor = fixture.cut();
    fixture.mirror(&predecessor).unwrap();
    drop(predecessor);
    backend.hold.store(true, Ordering::Release);
    let first = fixture.cut();
    assert!(matches!(
        fixture.mirror_for(&first, Duration::from_millis(20)),
        Err(MidgeError::Timeout(_))
    ));
    arrived.recv_timeout(Duration::from_secs(1)).unwrap();
    drop(first);
    IntentPersistence::save_with_fs(&fixture.fs, &[]).unwrap();

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
fn should_keep_readback_payload_charged_until_its_actual_callback_finishes() {
    // Arrange: the provider has fetched the real immutable body, but its
    // callback remains held beyond the logical publication deadline.
    let (entered, arrived) = mpsc::channel();
    let released = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let backend = Arc::new(HeldReadbackBackend {
        inner: Arc::new(MockCloudBackend::new()),
        hold: AtomicBool::new(false),
        entered,
        released: Arc::clone(&released),
    });
    let fixture = Fixture::new(CloudStorage::new(
        backend.clone(),
        "physical-readback-budget".into(),
    ));
    let memory = crate::common::resource_budget::ResourceBudget::new(256 * 1024);
    fixture.lock.configure_capture_memory(memory.clone());
    install_terminal_flush_fixture(&fixture, 1);
    let predecessor = fixture.cut();
    fixture.mirror(&predecessor).unwrap();
    drop(predecessor);
    backend.hold.store(true, Ordering::Release);
    let first = fixture.cut();
    assert!(matches!(
        fixture.mirror_for(&first, Duration::from_millis(20)),
        Err(MidgeError::Timeout(_))
    ));
    arrived.recv_timeout(Duration::from_secs(1)).unwrap();
    drop(first);

    // Act: a later selected cut clears the intent and settles logical coverage
    // while the actual earlier GET payload/callback is still owned.
    IntentPersistence::save_with_fs(&fixture.fs, &[]).unwrap();
    let second = fixture.cut();
    fixture.mirror(&second).unwrap();
    drop(second);
    assert_eq!(fixture.lock.pending_captures(), 0);
    let until = std::time::Instant::now() + Duration::from_secs(1);
    while memory.used() > 64 * 1024 && std::time::Instant::now() < until {
        std::thread::yield_now();
    }

    // Assert: logical settlement cannot release either the source charge or
    // its exclusive witness credit until the real readback callback drains.
    assert!(
        memory.used() > 0,
        "held readback body must retain source accounting after coalescence"
    );
    let (lock, changed) = &*released;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while memory.used() != 0 && std::time::Instant::now() < until {
        std::thread::yield_now();
    }
    assert_eq!(memory.used(), 0);
}

fn install_terminal_flush_fixture(fixture: &Fixture, count: u64) -> Vec<IntentLogEntry> {
    let mut edits = Vec::new();
    let mut intents = Vec::new();
    for index in 1..=count {
        let mut intent = flush_intent();
        let IntentLogEntry::FlushPublish {
            phase,
            sequence,
            file_meta,
            ..
        } = &mut intent
        else {
            unreachable!()
        };
        *phase = PublicationPhase::ManifestPublished;
        *sequence = index * 10;
        file_meta.name = crate::cloud_layout::file_name(0, 0, *sequence);
        file_meta.smallest_seq = Some(*sequence);
        file_meta.largest_seq = Some(*sequence);
        edits.push(crate::metadata::ManifestEdit::AddSst(
            crate::metadata::FileMeta::from(&*file_meta),
        ));
        intents.push(intent);
    }
    edits.push(crate::metadata::ManifestEdit::BumpNextSstSeq {
        cf_id: 0,
        next_seq: count * 10 + 1,
    });
    edits.push(crate::metadata::ManifestEdit::AdvanceFlushFrontier {
        cf_id: 0,
        freeze_sequence: count * 10,
    });
    crate::metadata::store::ManifestStore::new(Arc::clone(&fixture.fs))
        .append(&crate::metadata::ManifestEdit::Batch(edits))
        .unwrap();
    IntentPersistence::save_with_fs(&fixture.fs, &intents).unwrap();
    intents
}

#[test]
fn should_retain_remaining_terminal_witnesses_when_a_selected_cut_clears_only_some_intents() {
    // Arrange: many genuine selected terminal phases occupy a compact cache,
    // while a later owned source can outlive several partial intent clears.
    let fixture = Fixture::new(CloudStorage::with_mock());
    let intents = install_terminal_flush_fixture(&fixture, 32);
    let encoded = files::CLOUD_MIRRORED
        .iter()
        .map(|name| usize::try_from(fixture.fs.metadata(&FsPath::new(*name)).unwrap().len).unwrap())
        .sum::<usize>();
    let limit = (encoded * 16 + 4096) * 3 + 128 * 1024;
    let root = crate::common::resource_budget::ResourceBudget::new(limit);
    let memory = root.child(limit);
    fixture.lock.configure_capture_memory(memory.clone());
    let selected = fixture.cut();
    fixture.mirror(&selected).unwrap();
    drop(selected);
    let retained = fixture.cut();

    // Act: the later selected cache drops most IDs, but those receipts remain
    // necessary until every earlier owned source can settle its exact coverage.
    IntentPersistence::save_with_fs(&fixture.fs, &intents[31..]).unwrap();
    let partial = fixture.cut();
    fixture.mirror(&partial).unwrap();
    IntentPersistence::save_with_fs(&fixture.fs, &[]).unwrap();
    let clear = fixture.cut();
    fixture.mirror(&clear).unwrap();
    fixture.mirror(&retained).unwrap();

    // Assert: shared witness credit never spends the private body/decoder
    // floor or escapes its parent pool, even with several source callbacks.
    assert!(retained.memory_reservation().bytes() >= encoded * 8 + 4096);
    assert!(root.used() >= memory.used());
    assert!(root.peak() <= limit);
    assert_eq!(fixture.lock.pending_captures(), 0);
    drop((retained, partial, clear));
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while root.used() != 0 && std::time::Instant::now() < until {
        std::thread::yield_now();
    }
    assert_eq!(memory.used(), 0);
    assert_eq!(root.used(), 0);
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
            smallest_seq: Some(10),
            largest_seq: Some(10),
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

    // Act: request a witness for the cut omitting the unsettled intent.
    let omitted = fixture.lock.capture_control_cut(
        &fixture.fs,
        1,
        &OperationDeadline::from_budget(Duration::from_secs(1)),
    );
    // Assert: no witness may grant coverage of an omitted obligation.
    assert!(matches!(omitted, Err(MidgeError::Busy(_))));
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
fn should_transfer_selected_terminal_witness_when_its_source_finishes_before_later_capture() {
    // Arrange: a real selected cut proves the terminal phase. Its source can
    // finish before a later task captures the still durable local intent.
    let fixture = Fixture::new(CloudStorage::with_mock());
    let mut intent = flush_intent();
    let IntentLogEntry::FlushPublish {
        phase, file_meta, ..
    } = &mut intent
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
    IntentPersistence::save_with_fs(&fixture.fs, &[intent]).unwrap();
    let selected = fixture.cut();
    fixture.mirror(&selected).unwrap();
    drop(selected);
    // A later durable edit means whole-body equality is insufficient proof.
    store
        .append(&crate::metadata::ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 100,
        })
        .unwrap();
    let later = fixture.cut();

    // Act: only the already selected terminal intent is removed. The new
    // capture must keep its exact historical selected witness across ownership.
    IntentPersistence::save_with_fs(&fixture.fs, &[]).unwrap();
    let cleared = fixture.cut();
    fixture.mirror(&cleared).unwrap();

    // Assert: the clear selects without pretending the retained later task
    // selected; its exact coverage can then settle against the genuine head.
    fixture.mirror(&later).unwrap();
    assert_eq!(fixture.lock.pending_captures(), 0);
    let (_, bodies) = fixture.selected_bodies();
    let recovered: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    let mut file = recovered
        .open(
            &FsPath::new(files::INTENT_LOG),
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .unwrap();
    file.append(bodies[files::INTENT_LOG].clone().into())
        .unwrap();
    assert!(IntentPersistence::load_with_fs_and_policy_typed(
        &recovered,
        crate::config::RecoveryPolicy::Strict,
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
    fixture.store.acquire_leadership("successor").unwrap();

    // Act
    let publication = fixture.mirror(&cut);

    // Assert
    assert!(matches!(publication, Err(MidgeError::Fenced(_))));
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
    let failpoint_gate = crate::failpoints::worker_gate();
    let install = std::thread::spawn(move || {
        failpoint_gate.run(|| {
            writer.save_captured_snapshot_for(
                crate::metadata::accounting::Origin::CompactionBeforeGc,
                &checkpoint,
            )
        })
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
