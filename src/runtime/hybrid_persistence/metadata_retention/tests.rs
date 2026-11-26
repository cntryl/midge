use super::*;
use cntryl_midge_core::common::resource_budget::ResourceBudget;
use cntryl_midge_core::common::OperationDeadline;
use cntryl_midge_core::io::{Fs, FsPath};
use cntryl_midge_durability::lease::fs_leader_store::FsLeaderStore;
use cntryl_midge_durability::lease::LeaderStore;
use cntryl_midge_durability::metadata::{files, Manifest, ManifestPersistence};
use crate::runtime::hybrid_persistence::{
    mirror_control_metadata_within, read_committed_metadata_bodies_within,
    CloudMetadataMirrorAuthority, SharedMetadataSnapshot,
};
use cntryl_midge_storage::cloud::{CloudStorage, MockCloudBackend};
use std::sync::Arc;

struct Fixture {
    directory: tempfile::TempDir,
    fs: Arc<dyn Fs>,
    backend: Arc<MockCloudBackend>,
    cloud: Arc<CloudStorage>,
    store: Arc<FsLeaderStore>,
    lock: crate::runtime::MetadataPublicationLock,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let fs = cntryl_midge_core::io::real::test_root(directory.path());
        cntryl_midge_durability::metadata::ensure_or_create_format_marker(&fs, directory.path()).unwrap();
        ManifestPersistence::save_with_fs(&fs, &Manifest::default()).unwrap();
        let store = Arc::new(FsLeaderStore::new(
            Arc::clone(&fs),
            cntryl_midge_durability::lease::LeaseClock::default(),
        ));
        store.acquire_leadership("writer").unwrap();
        let backend = Arc::new(MockCloudBackend::new());
        let cloud = Arc::new(CloudStorage::new(backend.clone(), "retention".into()));
        Self {
            directory,
            fs,
            backend,
            cloud,
            store,
            lock: crate::runtime::MetadataPublicationLock::default(),
        }
    }

    fn context<'a>(&'a self, deadline: &'a OperationDeadline) -> CloudMetadataMirrorContext<'a> {
        self.context_at_epoch(deadline, "writer", 1)
    }

    fn context_at_epoch<'a>(
        &'a self,
        deadline: &'a OperationDeadline,
        holder_id: &'a str,
        writer_epoch: u64,
    ) -> CloudMetadataMirrorContext<'a> {
        CloudMetadataMirrorContext {
            cloud: &self.cloud,
            fs: self.fs.as_ref(),
            publication_lock: &self.lock,
            lock_wait_budget: Duration::from_secs(5),
            deadline,
            authority: CloudMetadataMirrorAuthority {
                store: self.store.as_ref(),
                holder_id,
                writer_epoch,
            },
        }
    }

    fn publish(&self, sequence: u64, snapshot: bool) -> CloudMetadataGeneration {
        self.publish_at_epoch(sequence, snapshot, "writer", 1)
    }

    fn publish_at_epoch(
        &self,
        sequence: u64,
        snapshot: bool,
        holder_id: &str,
        writer_epoch: u64,
    ) -> CloudMetadataGeneration {
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
        mirror_control_metadata_within(
            self.context_at_epoch(&deadline, holder_id, writer_epoch),
            |_| Ok(()),
        )
        .unwrap();
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
        if let Some(last) = state.last_run {
            std::thread::sleep(state.interval.saturating_sub(last.elapsed()));
        }
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        collect_after_commit(&self.context(&deadline), generation, state);
    }
}

fn format_10_generation(fixture: &Fixture, finish: bool) -> CloudMetadataGeneration {
    use cntryl_midge_durability::lease::SharedGenerationId;

    let directory = tempfile::tempdir_in(fixture.directory.path()).unwrap();
    let fs: Arc<dyn Fs> = Arc::new(cntryl_midge_core::io::RealFs::new(directory.path()).unwrap());
    let manifest =
        cntryl_midge_durability::metadata::codec::encode(cntryl_midge_durability::metadata::codec::MANIFEST, &Manifest::default())
            .unwrap();
    let intents = serde_json::to_vec(&Vec::<crate::runtime::IntentLogEntry>::new()).unwrap();
    cntryl_midge_durability::metadata::local_authority::LocalMetadataAuthority::initialize(
        Arc::clone(&fs),
        "retention-generation",
        &manifest,
        &intents,
    )
    .unwrap();
    std::fs::write(
        directory.path().join(files::FORMAT),
        b"midge-format-version=10\n",
    )
    .unwrap();

    let publication_id = uuid::Uuid::new_v4();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let captured =
        super::super::capture_selected_format10_closure(fs.as_ref(), &fixture.lock, &deadline)
            .unwrap();
    let (manifest_horizon, objects) = super::super::publish_format10_closure(
        &fixture.cloud,
        &deadline,
        publication_id,
        1,
        &captured.bodies,
        None,
        &captured.reservation,
    )
    .unwrap();
    if finish {
        Format10AdmissionStore::new(&fixture.cloud, &deadline)
            .finish(publication_id)
            .unwrap();
    }
    CloudMetadataGeneration {
        id: SharedGenerationId::FIRST,
        manifest_horizon,
        objects,
    }
}

fn put_metadata_object(fixture: &Fixture, key: &str, bytes: &[u8]) {
    BlockingCloud::new(
        &fixture.cloud,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    )
    .put_with_headers(key, bytes.to_vec(), Vec::new())
    .unwrap();
}

fn object_is_present(fixture: &Fixture, key: &str) -> bool {
    BlockingCloud::new(
        &fixture.cloud,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    )
    .get_optional(key)
    .unwrap()
    .is_some()
}

fn publish_format_10_head(fixture: &Fixture, generation: &CloudMetadataGeneration) {
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    fixture
        .store
        .publish_committed_metadata("writer", 1, None, generation.clone(), deadline.remaining())
        .unwrap();
}

#[test]
fn should_fence_predecessor_retention_until_successor_epoch_takes_over() {
    // Arrange: the predecessor publishes an old generation and later history.
    let fixture = Fixture::new();
    let old_generation = fixture.publish(1, true);
    let predecessor_head = old_generation.clone();
    for sequence in 2..=10 {
        fixture.publish(sequence, true);
    }
    let old_key = old_generation.objects[0].object_key.clone();
    let predecessor_epoch = 1;
    fixture
        .store
        .release_if_owner("writer", predecessor_epoch)
        .unwrap();
    let successor = fixture.store.acquire_leadership("successor").unwrap();
    assert!(successor.epoch > predecessor_epoch);
    let head = fixture.publish_at_epoch(11, true, "successor", successor.epoch);
    fixture.lock.lock().retention.interval = Duration::ZERO;
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

    // Act: a stale predecessor cannot collect after takeover; the current
    // successor can collect the same unreachable object under its own epoch.
    let stale = collect_selected_metadata_within(
        &fixture.context_at_epoch(&deadline, "writer", predecessor_epoch),
        |_| Ok(()),
    );
    assert!(stale.is_err(), "fenced predecessor must not run collection");
    read_committed_metadata_bodies_within(&fixture.cloud, &predecessor_head, &deadline).unwrap();
    assert!(BlockingCloud::new(&fixture.cloud, &deadline)
        .get_optional(&old_key)
        .unwrap()
        .is_some());

    collect_selected_metadata_within(
        &fixture.context_at_epoch(&deadline, "successor", successor.epoch),
        |_| Ok(()),
    )
    .unwrap();

    // Assert: successor reclaimed only stale history; committed recovery is intact.
    assert!(BlockingCloud::new(&fixture.cloud, &deadline)
        .get_optional(&old_key)
        .unwrap()
        .is_none());
    read_committed_metadata_bodies_within(&fixture.cloud, &head, &deadline).unwrap();
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
    let mut restarted = MetadataRetentionState {
        interval: Duration::from_millis(1),
        ..Default::default()
    };

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
        cntryl_midge_core::io::read_all(fixture.fs.as_ref(), &FsPath::new(files::INTENT_LOG)).unwrap()
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
    let mut state = MetadataRetentionState {
        interval: Duration::from_millis(1),
        ..Default::default()
    };

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
fn should_skip_metadata_collection_while_an_owned_capture_is_pending() {
    // Arrange
    let fixture = Fixture::new();
    let first = fixture.publish(1, true);
    let mut head = first.clone();
    for sequence in 2..=10 {
        head = fixture.publish(sequence, true);
    }
    let candidate_key = first.objects[0].object_key.clone();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let pending = fixture
        .lock
        .capture_control_cut(&fixture.fs, 1, &deadline)
        .unwrap();
    assert_eq!(fixture.lock.pending_captures(), 1);
    fixture.lock.lock().retention.interval = Duration::ZERO;

    // Act
    collect_selected_metadata_within(&fixture.context(&deadline), |_| Ok(())).unwrap();

    // Assert: the admitted capture may still become selected, so even an
    // otherwise reclaimable predecessor remains until that ownership settles.
    assert_eq!(fixture.lock.pending_captures(), 1);
    assert!(BlockingCloud::new(&fixture.cloud, &deadline)
        .get_optional(&candidate_key)
        .unwrap()
        .is_some());
    assert!(head.id.get() > first.id.get());
    drop(pending);
}

#[test]
fn should_retain_metadata_when_selected_authority_read_is_uncertain() {
    // Arrange
    let fixture = Fixture::new();
    let first = fixture.publish(1, true);
    for sequence in 2..=10 {
        fixture.publish(sequence, true);
    }
    let candidate_key = first.objects[0].object_key.clone();
    let pointer_path = fixture.directory.path().join(".midge_shared_metadata");
    let committed_pointer = std::fs::read(&pointer_path).unwrap();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    fixture.lock.lock().retention.interval = Duration::ZERO;
    std::fs::write(&pointer_path, b"uncertain selected pointer").unwrap();

    // Act
    let result = collect_selected_metadata_within(&fixture.context(&deadline), |_| Ok(()));

    // Assert: malformed authority cannot authorize deletion; after restoring
    // the exact committed pointer the selected chain remains readable.
    assert!(result.is_err());
    assert!(BlockingCloud::new(&fixture.cloud, &deadline)
        .get_optional(&candidate_key)
        .unwrap()
        .is_some());
    std::fs::write(&pointer_path, committed_pointer).unwrap();
    let CloudMetadataHead::Committed(generation) = fixture
        .store
        .read_committed_metadata(deadline.remaining())
        .unwrap()
    else {
        panic!("restored committed metadata pointer required")
    };
    read_committed_metadata_bodies_within(&fixture.cloud, &generation, &deadline).unwrap();
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
    assert_eq!(fixture.backend.get_downloads(), Vec::<String>::new());
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

#[test]
fn should_continue_bounded_pages_when_one_old_ordinal_has_many_orphan_attempts() {
    // Arrange: successful epochs do not give all failed attempts distinct
    // ordinals; one ordinal can contain many immutable operation UUIDs.
    let fixture = Fixture::new();
    let first = fixture.publish(1, true);
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let bodies = read_committed_metadata_bodies_within(&fixture.cloud, &first, &deadline).unwrap();
    for _ in 0..25 {
        super::super::metadata_chain::publish(
            &fixture.context(&deadline),
            &bodies,
            None,
            None,
            None,
            first.id,
        )
        .unwrap();
    }
    let mut head = first;
    for sequence in 2..=10 {
        head = fixture.publish(sequence, true);
    }
    let mut state = MetadataRetentionState {
        interval: Duration::from_millis(1),
        ..Default::default()
    };

    // Act: real cooldown expiry allows each next page; no state reset is used.
    for _ in 0..16 {
        let lists = state.totals.lists;
        let probes = state.totals.probes;
        fixture.collect(&head, &mut state);
        assert!(state.totals.lists - lists <= LIST_BUDGET as u64);
        assert!(state.totals.probes - probes <= CANDIDATE_BUDGET as u64);
    }

    // Assert: operation-token paging makes eventual progress without ever
    // collecting the complete ordinal prefix or abandoning its unprobed tail.
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let remaining = BlockingCloud::new(&fixture.cloud, &deadline)
        .list("manifest/")
        .unwrap();
    assert_eq!(remaining.len(), 4);
    assert_eq!(state.totals.deleted, 31);
    read_committed_metadata_bodies_within(&fixture.cloud, &head, &deadline).unwrap();
}

#[test]
fn should_reclaim_only_finished_unselected_format_10_stages() {
    // Arrange
    let fixture = Fixture::new();
    let selected = format_10_generation(&fixture, true);
    publish_format_10_head(&fixture, &selected);
    let selected_keys = selected
        .objects
        .iter()
        .map(|object| object.object_key.clone())
        .collect::<Vec<_>>();
    let orphan = format_10_generation(&fixture, true);
    let pending_id = uuid::Uuid::new_v4();
    let pending = selected
        .objects
        .iter()
        .cloned()
        .map(|mut object| {
            object.object_key = format!("metadata/generations/{pending_id}/{}", object.file_name);
            object
        })
        .collect::<Vec<_>>();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let admission = Format10AdmissionStore::new(&fixture.cloud, &deadline);
    admission.reserve(pending_id, 1, &pending).unwrap();

    let legacy_id = uuid::Uuid::new_v4();
    let legacy_marker = format!("metadata/generations/{legacy_id}/FORMAT");
    let legacy_snapshot = format!("metadata/generations/{legacy_id}/manifest.snapshot.json");
    put_metadata_object(&fixture, &legacy_marker, b"midge-format-version=9\n");
    put_metadata_object(&fixture, &legacy_snapshot, b"legacy v9 snapshot");
    let migration_id = uuid::Uuid::new_v4();
    let migration_candidate = format!("metadata/migrations/{migration_id}/candidate.json");
    let migration_source = format!("metadata/migrations/{migration_id}/source.json");
    put_metadata_object(
        &fixture,
        &migration_candidate,
        b"pending candidate descriptor",
    );
    put_metadata_object(
        &fixture,
        &migration_source,
        b"pre-upgrade source descriptor",
    );

    // Act: a pending body is still missing when cleanup checks ownership.
    let _turn = fixture.lock.lock();
    reclaim_finished_format10_stages(&fixture.context(&deadline), &selected).unwrap();
    let late_marker = pending
        .iter()
        .find(|object| object.file_name == files::FORMAT)
        .unwrap();
    put_metadata_object(
        &fixture,
        &late_marker.object_key,
        b"midge-format-version=10\n",
    );

    // Assert: only canonical v10 body objects outside the exact selected set
    // were removed; legacy sources and migration descriptors stay intact.
    assert!(selected_keys
        .iter()
        .all(|key| object_is_present(&fixture, key)));
    assert!(orphan
        .objects
        .iter()
        .all(|object| !object_is_present(&fixture, &object.object_key)));
    assert!(object_is_present(&fixture, &late_marker.object_key));
    assert!(admission
        .list_slots()
        .unwrap()
        .into_iter()
        .flatten()
        .any(
            |record| record.publication_id == pending_id && record.phase == AdmissionPhase::Pending
        ));
    assert!(object_is_present(&fixture, &legacy_marker));
    assert!(object_is_present(&fixture, &legacy_snapshot));
    assert!(object_is_present(&fixture, &migration_candidate));
    assert!(object_is_present(&fixture, &migration_source));
}

#[test]
fn should_retain_format_10_candidate_when_pointer_changes_before_delete() {
    // Arrange
    let fixture = Fixture::new();
    let selected = format_10_generation(&fixture, true);
    publish_format_10_head(&fixture, &selected);
    let orphan = format_10_generation(&fixture, true);
    let mut successor = selected.clone();
    successor.id = cntryl_midge_durability::lease::SharedGenerationId::new(2).unwrap();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    fixture
        .store
        .publish_committed_metadata(
            "writer",
            1,
            Some(&selected),
            successor,
            deadline.remaining(),
        )
        .unwrap();

    // Act
    let _turn = fixture.lock.lock();
    let result = reclaim_finished_format10_stages(&fixture.context(&deadline), &selected);

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))));
    assert!(orphan
        .objects
        .iter()
        .all(|object| object_is_present(&fixture, &object.object_key)));
}

#[test]
fn should_reclaim_uploaded_format_10_stage_after_writer_takeover() {
    // Arrange
    let fixture = Fixture::new();
    let selected = format_10_generation(&fixture, true);
    publish_format_10_head(&fixture, &selected);
    let orphan = format_10_generation(&fixture, false);
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    fixture.store.release_if_owner("writer", 1).unwrap();
    let successor = fixture.store.acquire_leadership("successor").unwrap();

    // Act
    let _turn = fixture.lock.lock();
    reclaim_finished_format10_stages(
        &fixture.context_at_epoch(&deadline, "successor", successor.epoch),
        &selected,
    )
    .unwrap();

    // Assert
    assert!(orphan
        .objects
        .iter()
        .all(|object| !object_is_present(&fixture, &object.object_key)));
    assert_eq!(
        Format10AdmissionStore::new(&fixture.cloud, &deadline)
            .list_slots()
            .unwrap()
            .into_iter()
            .flatten()
            .count(),
        1
    );
    read_committed_metadata_bodies_within(&fixture.cloud, &selected, &deadline).unwrap();
}
