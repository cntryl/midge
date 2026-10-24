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
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
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
