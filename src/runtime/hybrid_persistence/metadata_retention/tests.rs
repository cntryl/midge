use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::common::OperationDeadline;
use crate::io::{Fs, FsPath};
use crate::lease::fs_leader_store::FsLeaderStore;
use crate::lease::LeaderStore;
use crate::metadata::{files, Manifest, ManifestPersistence};
use crate::runtime::hybrid_persistence::{
    mirror_control_metadata_within, read_committed_metadata_bodies_within,
    CloudMetadataMirrorAuthority, SharedMetadataSnapshot,
};
use crate::storage::cloud::{CloudStorage, MockCloudBackend};
use std::sync::Arc;

struct Fixture {
    _directory: tempfile::TempDir,
    fs: Arc<dyn Fs>,
    backend: Arc<MockCloudBackend>,
    cloud: Arc<CloudStorage>,
    store: Arc<FsLeaderStore>,
    lock: crate::runtime::MetadataPublicationLock,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let fs = crate::io::real::test_root(directory.path());
        crate::metadata::ensure_or_create_format_marker(&fs, directory.path()).unwrap();
        ManifestPersistence::save_with_fs(&fs, &Manifest::default()).unwrap();
        let store = Arc::new(FsLeaderStore::new(
            Arc::clone(&fs),
            crate::lease::LeaseClock::default(),
        ));
        store.acquire_leadership("writer").unwrap();
        let backend = Arc::new(MockCloudBackend::new());
        let cloud = Arc::new(CloudStorage::new(backend.clone(), "retention".into()));
        Self {
            _directory: directory,
            fs,
            backend,
            cloud,
            store,
            lock: crate::runtime::MetadataPublicationLock::default(),
        }
    }

    fn context<'a>(&'a self, deadline: &'a OperationDeadline) -> CloudMetadataMirrorContext<'a> {
        CloudMetadataMirrorContext {
            cloud: &self.cloud,
            fs: self.fs.as_ref(),
            publication_lock: &self.lock,
            lock_wait_budget: Duration::from_secs(5),
            deadline,
            authority: CloudMetadataMirrorAuthority {
                store: self.store.as_ref(),
                holder_id: "writer",
                writer_epoch: 1,
            },
        }
    }

    fn publish(&self, sequence: u64, snapshot: bool) -> CloudMetadataGeneration {
        let intent = if snapshot {
            crate::runtime::IntentLogEntry::CloudUploadComplete {
                resource: format!("{sequence}:{}", "x".repeat(32 * 1024)),
                seqno: sequence,
            }
        } else {
            crate::runtime::IntentLogEntry::SeqnoAllocated {
                seqno: sequence,
                cf_id: 0,
            }
        };
        crate::runtime::IntentPersistence::save_with_fs(&self.fs, &[intent]).unwrap();
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        mirror_control_metadata_within(self.context(&deadline), |_| Ok(())).unwrap();
        let CloudMetadataHead::Committed(generation) = self
            .store
            .read_committed_metadata(deadline.remaining())
            .unwrap()
        else {
            panic!("committed generation")
        };
        generation
    }

    fn collect(&self, generation: &CloudMetadataGeneration, state: &mut MetadataRetentionState) {
        let _guard = self.lock.lock();
        state.last_run = None;
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        collect_after_commit(&self.context(&deadline), generation, state);
    }
}

#[test]
fn should_reclaim_obsolete_snapshot_versions_when_restart_rediscovers_history() {
    // Arrange: every large edit starts a snapshot, leaving no older dependency.
    let fixture = Fixture::new();
    let mut generation = fixture.publish(1, true);
    for sequence in 2..=10 {
        generation = fixture.publish(sequence, true);
    }
    assert_eq!(generation.objects.len(), 1);
    // A new maintenance state models loss of the process-local delete queue.
    let mut restarted = MetadataRetentionState::default();

    // Act: each bounded pass visits at most four ordinal prefixes.
    fixture.collect(&generation, &mut restarted);
    fixture.collect(&generation, &mut restarted);

    // Assert: selected head plus its three retained predecessor ordinals survive.
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let keys = BlockingCloud::new(&fixture.cloud, &deadline)
        .list("manifest/")
        .unwrap();
    assert_eq!(keys.len(), 4);
    assert_eq!(restarted.totals.deleted, 6);
    assert_eq!(restarted.totals.lists, 6);
    let bodies =
        read_committed_metadata_bodies_within(&fixture.cloud, &generation, &deadline).unwrap();
    assert_eq!(
        bodies[files::INTENT_LOG],
        crate::io::read_all(fixture.fs.as_ref(), &FsPath::new(files::INTENT_LOG)).unwrap()
    );
}

#[test]
fn should_retain_every_selected_predecessor_when_edit_chain_exceeds_retention_window() {
    // Arrange
    let fixture = Fixture::new();
    let mut generation = fixture.publish(1, false);
    for sequence in 2..=10 {
        generation = fixture.publish(sequence, false);
    }
    assert_eq!(generation.objects.len(), 10);
    let mut state = MetadataRetentionState::default();

    // Act
    fixture.collect(&generation, &mut state);
    fixture.collect(&generation, &mut state);

    // Assert: count and age never substitute for exact recovery dependencies.
    assert_eq!(state.totals.deleted, 0);
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    assert_eq!(
        BlockingCloud::new(&fixture.cloud, &deadline)
            .list("manifest/")
            .unwrap()
            .len(),
        10
    );
    read_committed_metadata_bodies_within(&fixture.cloud, &generation, &deadline).unwrap();
}

#[test]
fn should_retain_candidate_when_pointer_changes_before_final_delete_check() {
    // Arrange
    let fixture = Fixture::new();
    let candidate = fixture.publish(1, true).objects[0].clone();
    let stale_head = fixture.publish(2, true);
    fixture.publish(3, true);
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

    // Act
    let result = reclaim_candidate(
        &fixture.context(&deadline),
        &stale_head,
        &candidate.object_key,
        1,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))));
    assert!(BlockingCloud::new(&fixture.cloud, &deadline)
        .get_optional(&candidate.object_key)
        .unwrap()
        .is_some());
}

#[test]
fn should_exclude_metadata_fetch_when_retention_owns_publication_capture() {
    // Arrange
    let fixture = Fixture::new();
    fixture.publish(1, false);
    let snapshot = SharedMetadataSnapshot::new(
        Arc::clone(&fixture.cloud),
        ResourceBudget::new(1024 * 1024),
        fixture.store.clone(),
        "writer".into(),
        1,
        fixture.lock.clone(),
    );
    let held = fixture.lock.lock();
    fixture.backend.clear_history();

    // Act
    let blocked = snapshot.committed_retirement_authority_within(&OperationDeadline::from_budget(
        Duration::from_millis(1),
    ));
    assert!(fixture.backend.get_downloads().is_empty());
    drop(held);
    let resumed = snapshot.committed_retirement_authority_within(&OperationDeadline::from_budget(
        Duration::from_secs(5),
    ));

    // Assert
    assert!(matches!(blocked, Err(MidgeError::Timeout(_))));
    assert!(resumed.is_ok());
}

#[test]
fn should_retain_replacement_when_delete_identity_changes_after_probe() {
    // Arrange
    let fixture = Fixture::new();
    let candidate = fixture.publish(1, true).objects[0].clone();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let io = BlockingCloud::new(&fixture.cloud, &deadline);
    let original = io.head_optional(&candidate.object_key).unwrap().unwrap();
    io.put_with_headers(&candidate.object_key, b"replacement".to_vec(), Vec::new())
        .unwrap();

    // Act: the admitted delete still names its original object version.
    let removed =
        delete_identity(&fixture.context(&deadline), &candidate.object_key, original).unwrap();

    // Assert
    assert!(!removed);
    assert_eq!(
        io.get_optional(&candidate.object_key).unwrap().unwrap(),
        b"replacement"
    );
}
