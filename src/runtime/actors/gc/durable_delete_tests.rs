use super::*;
use crate::common::OperationDeadline;
use crate::lease::fs_leader_store::FsLeaderStore;
use crate::lease::LeaderStore;
use crate::metadata::{FileMeta, ManifestEdit, ManifestPersistence};
use crate::runtime::hybrid_persistence::{
    mirror_control_metadata_within, CloudMetadataMirrorAuthority, CloudMetadataMirrorContext,
};
use crate::storage::cloud::{BlockingCloud, CloudStorage};

struct Fixture {
    _directory: tempfile::TempDir,
    state: RuntimeState,
    cloud: Arc<CloudStorage>,
    storage: Arc<crate::storage::HybridStorage>,
    store: Arc<FsLeaderStore>,
    file: FileMeta,
    publication_lock: crate::runtime::MetadataPublicationLock,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
        crate::metadata::ensure_or_create_format_marker(&state.fs, directory.path()).unwrap();
        std::fs::create_dir_all(state.host_sst_dir()).unwrap();
        let original = b"original";
        let file = FileMeta {
            name: crate::cloud_layout::file_name(0, 1, 7),
            level: 1,
            cf_id: 0,
            sst_seq: 7,
            size_bytes: original.len() as u64,
            content_crc32c: Some(crc32c::crc32c(original)),
            ..FileMeta::default()
        };
        state.manifest.add_file(file.clone());
        ManifestPersistence::save_with_fs(&state.fs, &state.manifest).unwrap();
        let edit = ManifestEdit::RemoveSst {
            name: file.name.clone(),
        };
        let id = state
            .manifest_store
            .append_for(crate::metadata::accounting::Origin::Administration, &edit)
            .unwrap();
        state.manifest.apply_edit(&edit).unwrap();
        state.manifest.note_applied_journal_edit(id);
        let checkpoint = state.manifest_store.save_snapshot(&state.manifest).unwrap();
        state.manifest.adopt_checkpoint(checkpoint);
        std::fs::write(state.host_sst_dir().join(&file.name), original).unwrap();
        let cloud = Arc::new(CloudStorage::with_mock());
        let storage = Arc::new(crate::storage::HybridStorage::with_policy(
            cloud.clone(),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        let store = Arc::new(FsLeaderStore::new(
            Arc::clone(&state.fs),
            crate::lease::LeaseClock::default(),
        ));
        store.acquire_leadership("writer").unwrap();
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        let publication_lock = crate::runtime::MetadataPublicationLock::default();
        BlockingCloud::new(&cloud, &deadline)
            .put_with_headers(
                &crate::cloud_layout::object_key(&file.name),
                original.to_vec(),
                Vec::new(),
            )
            .unwrap();
        mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &cloud,
                fs: state.fs.as_ref(),
                publication_lock: &publication_lock,
                lock_wait_budget: Duration::from_secs(5),
                deadline: &deadline,
                authority: CloudMetadataMirrorAuthority {
                    store: store.as_ref(),
                    holder_id: "writer",
                    writer_epoch: 1,
                },
            },
            |_| Ok(()),
        )
        .unwrap();
        Self {
            _directory: directory,
            state,
            cloud,
            storage,
            store,
            file,
            publication_lock,
        }
    }

    fn remote(&self) -> Option<Vec<u8>> {
        BlockingCloud::new(
            &self.cloud,
            &OperationDeadline::from_budget(Duration::from_secs(5)),
        )
        .get_optional(&crate::cloud_layout::object_key(&self.file.name))
        .unwrap()
    }
}

#[test]
fn should_defer_known_retired_sst_until_admitted_metadata_cut_commits() {
    // Arrange
    let mut fixture = Fixture::new();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    let cut = fixture
        .publication_lock
        .capture_control_cut(&fixture.state.fs, 1, &deadline)
        .unwrap();
    let mut actor = GcActor::new();
    actor.set_metadata_publication_lock(fixture.publication_lock.clone());

    // Act: a capture still owns its source authority even though the live
    // local manifest already records the original immutable as retired.
    actor.delete_ssts(
        &mut fixture.state,
        &[fixture.file.name.clone()],
        Some(Arc::clone(&fixture.storage)),
    );
    actor.shutdown_workers();

    // Assert: both copies and the durable original proof survive deferral.
    assert_eq!(fixture.remote().unwrap(), b"original");
    assert!(fixture
        .state
        .host_sst_dir()
        .join(&fixture.file.name)
        .exists());
    assert_eq!(actor.pending_obsolete.len(), 1);
    assert!(fixture
        .state
        .manifest
        .retired_ssts
        .contains_key(&fixture.file.name));

    crate::runtime::hybrid_persistence::mirror_captured_control_metadata_within(
        CloudMetadataMirrorContext {
            cloud: &fixture.cloud,
            fs: fixture.state.fs.as_ref(),
            publication_lock: &fixture.publication_lock,
            lock_wait_budget: Duration::from_secs(5),
            deadline: &deadline,
            authority: CloudMetadataMirrorAuthority {
                store: fixture.store.as_ref(),
                holder_id: "writer",
                writer_epoch: 1,
            },
        },
        &cut,
        |_| Ok(()),
    )
    .unwrap();
    actor.retry_pending(&mut fixture.state, Some(Arc::clone(&fixture.storage)));
    actor.shutdown_workers();
    actor.retry_pending(&mut fixture.state, Some(Arc::clone(&fixture.storage)));
    assert!(fixture.remote().is_none());
    assert!(fixture.state.manifest.retired_ssts.is_empty());
}

#[test]
fn should_recheck_pending_cuts_inside_physical_retired_sst_worker() {
    // Arrange: the worker may have been queued before this capture admitted.
    let fixture = Fixture::new();
    let cut = fixture
        .publication_lock
        .capture_control_cut(
            &fixture.state.fs,
            1,
            &OperationDeadline::from_budget(Duration::from_secs(5)),
        )
        .unwrap();
    let storage = CloudPersistence::new(Arc::clone(&fixture.storage));

    // Act
    let result = GcActor::delete_cloud_and_local_sst(
        &storage,
        &fixture.state.fs,
        &fixture.file,
        &fixture.publication_lock,
    );

    // Assert: the final worker-owned check prevents both physical effects.
    assert!(matches!(result, Err(crate::common::MidgeError::Busy(_))));
    assert_eq!(fixture.remote().unwrap(), b"original");
    assert!(fixture
        .state
        .host_sst_dir()
        .join(&fixture.file.name)
        .exists());
    assert_eq!(cut.writer_epoch(), 1);
    assert_eq!(fixture.publication_lock.pending_captures(), 1);
}

#[test]
fn should_rediscover_original_delete_obligation_when_cloud_recovery_loses_local_queue() {
    // Arrange: the owning cloud publication committed, but physical DELETE did
    // not run before the cache and process-local queue were lost.
    let mut fixture = Fixture::new();
    let cold = tempfile::tempdir().unwrap();
    let fs = crate::io::real::test_root(cold.path());

    // Act
    crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
        &fixture.cloud,
        fixture.store.as_ref(),
        &fs,
        cold.path(),
        crate::config::RecoveryPolicy::Strict,
    )
    .unwrap();
    let recovered =
        ManifestPersistence::load_with_fs_and_policy(&fs, crate::config::RecoveryPolicy::Strict)
            .unwrap();
    fixture.state.manifest.replace(recovered);
    let mut restarted = GcActor::new();
    restarted.restore_durable_sst_deletes(&fixture.state, true);

    // Assert: restart first queues authoritative republishing; it cannot delete
    // from an unmirrored local state merely because a file is absent there.
    assert!(restarted.has_manifest_reclamation());
    assert_eq!(fixture.remote().unwrap(), b"original");
    let names = restarted.take_manifest_reclamation();
    restarted.delete_ssts(
        &mut fixture.state,
        &names,
        Some(Arc::clone(&fixture.storage)),
    );
    restarted.shutdown_workers();
    restarted.retry_pending(&mut fixture.state, Some(Arc::clone(&fixture.storage)));
    assert!(fixture.remote().is_none());
    assert!(!fixture
        .state
        .host_sst_dir()
        .join(&fixture.file.name)
        .exists());
    let confirmed = ManifestPersistence::load_with_fs_and_policy(
        &fixture.state.fs,
        crate::config::RecoveryPolicy::Strict,
    )
    .unwrap();
    assert!(confirmed.retired_ssts.is_empty());
}

#[test]
fn should_keep_durable_obligation_when_remote_identity_is_replaced() {
    // Arrange
    let mut fixture = Fixture::new();
    BlockingCloud::new(
        &fixture.cloud,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    )
    .put_with_headers(
        &crate::cloud_layout::object_key(&fixture.file.name),
        b"replaced".to_vec(),
        Vec::new(),
    )
    .unwrap();
    let mut actor = GcActor::new();

    // Act
    actor.delete_ssts(
        &mut fixture.state,
        &[fixture.file.name.clone()],
        Some(Arc::clone(&fixture.storage)),
    );
    actor.shutdown_workers();
    actor.retry_pending(&mut fixture.state, Some(Arc::clone(&fixture.storage)));

    // Assert
    assert_eq!(fixture.remote().unwrap(), b"replaced");
    assert!(fixture
        .state
        .host_sst_dir()
        .join(&fixture.file.name)
        .exists());
    let recovered = ManifestPersistence::load_with_fs_and_policy(
        &fixture.state.fs,
        crate::config::RecoveryPolicy::Strict,
    )
    .unwrap();
    assert!(recovered.retired_ssts[&fixture.file.name].same_identity(&fixture.file));
}

#[test]
fn should_reject_stale_completion_when_original_retired_identity_disagrees() {
    // Arrange
    let mut fixture = Fixture::new();
    let mut wrong = fixture.file.clone();
    wrong.content_crc32c = Some(1);
    let mut actor = GcActor::new();
    actor.completed_sst_deletes.lock().push_back(wrong);

    // Act
    actor.confirm_completed_sst_deletes(&mut fixture.state);

    // Assert
    assert!(fixture.state.manifest.retired_ssts[&fixture.file.name].same_identity(&fixture.file));
    assert_eq!(actor.completed_sst_deletes.lock().len(), 1);
    assert_eq!(fixture.remote().unwrap(), b"original");
}

#[test]
fn should_retain_remote_snapshot_source_until_its_final_pin_is_released() {
    // Arrange
    let mut fixture = Fixture::new();
    fixture
        .state
        .register_snapshot(943, 1, vec![fixture.file.name.clone()]);
    let mut actor = GcActor::new();

    // Act: a current manifest no longer names this original, but its reader does.
    actor.delete_ssts(
        &mut fixture.state,
        &[fixture.file.name.clone()],
        Some(Arc::clone(&fixture.storage)),
    );
    actor.shutdown_workers();
    assert_eq!(fixture.remote().unwrap(), b"original");
    assert!(fixture
        .state
        .manifest
        .retired_ssts
        .contains_key(&fixture.file.name));
    fixture.state.unregister_snapshot(943);
    actor.retry_pending(&mut fixture.state, Some(Arc::clone(&fixture.storage)));
    actor.shutdown_workers();
    actor.retry_pending(&mut fixture.state, Some(Arc::clone(&fixture.storage)));

    // Assert
    assert!(fixture.remote().is_none());
    assert!(fixture.state.manifest.retired_ssts.is_empty());
}

#[test]
fn should_defer_retired_identity_tail_when_delete_batch_exceeds_eight() {
    // Arrange: recovered obligations may already be physically absent, but
    // every identity still needs its own terminal absence confirmation.
    let mut fixture = Fixture::new();
    let mut names = vec![fixture.file.name.clone()];
    for sequence in 20..28 {
        let mut file = fixture.file.clone();
        file.name = crate::cloud_layout::file_name(0, 1, sequence);
        file.sst_seq = sequence;
        names.push(file.name.clone());
        fixture
            .state
            .manifest
            .test_mut()
            .retired_ssts
            .insert(file.name.clone(), file);
    }
    let checkpoint = fixture
        .state
        .manifest_store
        .save_snapshot(&fixture.state.manifest)
        .unwrap();
    fixture.state.manifest.adopt_checkpoint(checkpoint);
    let mut actor = GcActor::new();

    // Act
    actor.delete_ssts(
        &mut fixture.state,
        &names,
        Some(Arc::clone(&fixture.storage)),
    );
    actor.shutdown_workers();

    // Assert: eight terminal operations cannot consume the ninth obligation.
    assert_eq!(actor.pending_obsolete.len(), 1);
    assert!(fixture
        .state
        .manifest
        .retired_ssts
        .contains_key(names.last().unwrap()));
    actor.retry_pending(&mut fixture.state, Some(Arc::clone(&fixture.storage)));
    actor.shutdown_workers();
    actor.retry_pending(&mut fixture.state, Some(Arc::clone(&fixture.storage)));
    assert!(fixture.state.manifest.retired_ssts.is_empty());
}
