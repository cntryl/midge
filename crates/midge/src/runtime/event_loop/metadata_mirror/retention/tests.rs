use super::*;
use crate::runtime::event_loop::tests::create_test_cloud_event_loop;
use cntryl_midge_core::common::{MidgeResult, OperationDeadline};
use cntryl_midge_durability::lease::CloudMetadataHead;
use cntryl_midge_storage::cloud::BlockingCloud;

#[test]
fn should_reclaim_bounded_backlog_on_idle_turns_without_new_metadata_writes() -> MidgeResult<()> {
    // Arrange: twenty-five standalone snapshots exceed a single GC pass.
    let mut event_loop = create_test_cloud_event_loop(
        cntryl_midge_storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    for sequence in 1..=25 {
        cntryl_midge_durability::metadata::intent_persistence::IntentPersistence::save_with_fs(
            &event_loop.state.fs,
            &[
                cntryl_midge_durability::metadata::intent::IntentLogEntry::CloudUploadComplete {
                    resource: format!("{sequence}:{}", "x".repeat(32 * 1024)),
                    seqno: sequence,
                },
            ],
        )
        .map_err(cntryl_midge_core::common::MidgeError::Internal)?;
        event_loop.mirror_metadata_to_authoritative_cloud()?;
    }
    // Settle the independent first-use name prewarm before freezing the idle
    // authority. This is a genuine metadata edit, rather than retention work.
    event_loop.prefetch_flush_sst_names_if_due();
    // Inject a short interval in both owners; do not reset the collector's
    // cooldown or cursor. Every following attempt is scheduled by idle time.
    let interval = Duration::from_millis(5);
    event_loop
        .publication
        .metadata_publication_lock
        .lock()
        .retention
        .interval = interval;
    let pipeline = &mut event_loop.actors.cloud_coordinator.metadata_mirror;
    pipeline.retention_interval = interval;
    pipeline.retention_due = Instant::now() + interval;
    event_loop.pipeline.inline_flush_worker = false;
    let cloud = event_loop
        .actors
        .cloud_coordinator
        .cloud_metadata_storage
        .clone()
        .unwrap();
    let deadline = OperationDeadline::from_budget(Duration::from_secs(3));
    let selected_before = event_loop
        .authority
        .fencing
        .leader_store
        .as_ref()
        .unwrap()
        .read_committed_metadata(deadline.remaining())?;

    // Act: only production background progress and its actual idle wakeups run.
    loop {
        event_loop.background_progress(None);
        let keys = BlockingCloud::new(&cloud, &deadline).list("manifest/")?;
        if keys.len() <= 4 {
            break;
        }
        assert!(
            !deadline.is_expired(),
            "idle metadata retention stranded its backlog"
        );
        let wait = event_loop
            .idle_progress_timeout()
            .expect("retention has an idle wakeup");
        std::thread::sleep(wait.min(interval));
    }
    event_loop.drain_metadata_mirror_within(&deadline)?;

    // Assert: multiple bounded passes completed, and selected recovery bytes
    // remain readable after every process-local delete request has disappeared.
    let deleted = event_loop
        .publication
        .metadata_publication_lock
        .lock()
        .retention
        .totals
        .deleted;
    assert!(deleted >= 21);
    let store = event_loop.authority.fencing.leader_store.as_ref().unwrap();
    let CloudMetadataHead::Committed(generation) =
        store.read_committed_metadata(deadline.remaining())?
    else {
        panic!("selected history remains committed")
    };
    assert_eq!(
        CloudMetadataHead::Committed(generation.clone()),
        selected_before,
        "idle cleanup must not publish new manifest versions"
    );
    crate::runtime::hybrid_persistence::read_committed_metadata_bodies_within(
        &cloud,
        &generation,
        &deadline,
    )?;
    Ok(())
}
