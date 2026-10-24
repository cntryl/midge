//! Physical task completion never releases a different phase or generation.
use super::*;
use std::time::Duration;

struct HeldStage {
    entered: crossbeam::channel::Receiver<()>,
    release: crossbeam::channel::Sender<()>,
}
impl HeldStage {
    fn new(point: &'static str) -> Self {
        let (entered_tx, entered) = crossbeam::channel::bounded(1);
        let (release, released) = crossbeam::channel::bounded(1);
        fail::cfg_callback(point, move || {
            entered_tx.send(()).unwrap();
            released.recv().unwrap();
        })
        .unwrap();
        Self { entered, release }
    }
    fn entered(&self) {
        self.entered.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}
impl Drop for HeldStage {
    fn drop(&mut self) {
        let _ = self.release.try_send(());
    }
}

fn false_publish(identity: FlushIdentity) -> FlushWorkerResult {
    FlushWorkerResult::Publish(FlushPublishCompletion {
        identity,
        reservation: None,
        publish_ns: 0,
        result: Err(MidgeError::Internal("unowned test result".into())),
    })
}

fn complete_build(
    actor: &mut FlushActor,
    results: &crossbeam::channel::Receiver<FlushWorkerResult>,
    identity: FlushIdentity,
    memtable: Arc<crate::memtable::SkipListMemtable>,
) -> MidgeResult<FlushWorkerResult> {
    let staging = PathBuf::from("sst/.flush-staging/phase-proof.tmp");
    let held = HeldStage::new("midge::flush_worker::before_build");
    actor.submit_build(identity, Arc::clone(&memtable), staging.clone(), None)?;
    held.entered();
    let early = FlushWorkerResult::Build(FlushBuildCompletion {
        identity,
        memtable,
        staging_path: staging,
        reservation: None,
        build_ns: 0,
        result: Err(MidgeError::Internal("early build".into())),
    });
    assert!(!actor.take_completion(&early));
    actor.finish_pipeline(identity.flush_id);
    assert!(actor.is_inflight());
    assert!(!actor.can_start_or_resume(0, 2));
    held.release.send(()).unwrap();
    let built = results.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(actor.take_completion(&built));
    assert!(!actor.is_inflight());
    assert!(
        !actor.has_capacity(),
        "parked build retains its CF pipeline"
    );
    fail::remove("midge::flush_worker::before_build");
    Ok(built)
}

fn publication_task(fs: Arc<dyn crate::io::Fs>, build: &FlushBuildCompletion) -> FlushPublishTask {
    FlushPublishTask {
        build: FlushBuildOutput {
            identity: build.identity,
            staging_path: build.staging_path.clone(),
            file_meta: build.result.as_ref().unwrap().clone(),
            reservation: build.reservation,
        },
        sst_name: crate::cloud_layout::file_name(0, 0, 1),
        sst_seq: 1,
        fs,
        storage: Arc::new(HybridFlushStorage::default()),
        lease_healthy: None,
        lease_validity: None,
        leader_store: None,
        leader_holder_id: None,
    }
}

#[test]
fn should_retain_cf_owner_until_its_exact_physical_completion() -> MidgeResult<()> {
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    // Arrange: actual worker, memtable, encoded SST and publication filesystem.
    let directory = tempfile::tempdir()?;
    let fs = crate::io::real::test_root(directory.path());
    let (completed, results) = crossbeam::channel::unbounded();
    let mut actor = FlushActor::new_with_worker_count(
        &fs,
        false,
        &crate::codec::CompressionPolicy::default(),
        &completed,
        16 * 1024 * 1024,
        None,
        1,
    )?;
    let identity = FlushIdentity {
        flush_id: 1,
        writer_epoch: 7,
        cf_id: 0,
        sequence: 1,
    };
    let memtable = Arc::new(crate::memtable::SkipListMemtable::new());
    memtable.put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
    // Act: only the exact completed physical phase can settle.
    let completion = complete_build(&mut actor, &results, identity, memtable)?;
    let FlushWorkerResult::Build(output) = &completion else {
        panic!("build result");
    };
    let publish_hold = HeldStage::new("midge::flush_worker::before_publication");
    actor.submit_publish(publication_task(fs, output))?;
    publish_hold.entered();
    // Assert: completed build proof cannot settle the executing publish phase.
    assert!(
        !actor.take_completion(&completion),
        "old phase cannot complete new physical work"
    );
    assert!(!actor.take_completion(&false_publish(identity)));
    actor.finish_pipeline(identity.flush_id);
    assert!(actor.is_inflight());
    publish_hold.release.send(()).unwrap();
    let published = results.recv_timeout(Duration::from_secs(5)).unwrap();
    let stale_epoch = FlushIdentity {
        writer_epoch: 6,
        ..identity
    };
    assert!(!actor.take_completion(&false_publish(stale_epoch)));
    assert!(actor.take_completion(&published));
    assert!(
        !actor.take_completion(&published),
        "terminal result is consumed once"
    );
    assert!(!actor.is_inflight());
    assert!(!actor.has_capacity());
    actor.finish_pipeline(identity.flush_id);
    assert!(actor.has_capacity());
    assert!(directory
        .path()
        .join("sst")
        .join(crate::cloud_layout::file_name(0, 0, 1))
        .exists());
    actor.shutdown_and_join()?;
    fail::remove("midge::flush_worker::before_publication");
    scenario.teardown();
    Ok(())
}
