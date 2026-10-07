//! Actual manual compaction across one caller budget and three durable phases.
//!
//! The real response route's original registration and configured wait budget
//! establish the manual obligation before preparation and filename mirroring.
//!
//! This drives the actual compute actor, conditional provider metadata lease,
//! coordinator authority transitions and non-inline three-phase publisher.
//! `MockCloudBackend` delegates real immutable bytes, `ETags` and conditional writes;
//! this is an owner/accounting fixture, not native socket-cancellation proof.
//! The SST inputs are genuine seeded rows, not public transaction acks.
//!
use super::*;
use crate::common::OperationDeadline;
use crate::lease::{LeaseGuard, PrimaryLease as _};
use crate::runtime::actors::compaction::publication::{
    CompactionPublishActor, CompactionPublishPhase,
};
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudStorage, MockCloudBackend,
};

mod authority_timeout_tests;
mod deadline_owner_tests;
mod queued_phase_tests;

const CALLER_BUDGET: Duration = Duration::from_secs(5);
// Keep the ordinary provider cap above the aggregate caller allowance.
// Target-phase expiry must not require wasting time in an unrelated earlier
// phase just to make its remaining allowance smaller than the provider cap.
const PROVIDER_CAP: Duration = Duration::from_secs(8);
const FIXTURE_WAIT: Duration = Duration::from_secs(8);
const MANUAL_REQUEST: u64 = 92_400;

#[derive(Clone, Copy, Debug)]
struct ReadHold {
    phase: CompactionPublishPhase,
    until: Instant,
}

#[derive(Clone, Debug)]
struct ReadEvidence {
    phase: CompactionPublishPhase,
    key: String,
    delegated_at: Instant,
    forwarded_at: Instant,
    provider_budget: Option<Duration>,
    genuine_success: bool,
}

#[derive(Clone, Debug)]
struct CasEvidence {
    key: String,
    condition: String,
    committed_at: Instant,
    forwarded_at: Instant,
    before: Vec<u8>,
    after: Vec<u8>,
    before_etag: String,
    after_etag: String,
}

#[derive(Default)]
struct PhasedMetadataBackend {
    inner: MockCloudBackend,
    hold: Mutex<Option<ReadHold>>,
    release: AtomicBool,
    reads: Mutex<Vec<ReadEvidence>>,
    cas_hold: Mutex<Option<Instant>>,
    cas_writes: Mutex<Vec<CasEvidence>>,
}

impl PhasedMetadataBackend {
    fn arm_cas(&self, until: Instant) {
        assert!(self.cas_hold.lock().unwrap().replace(until).is_none());
    }

    fn descriptor(&self, key: &str) -> (Vec<u8>, crate::storage::StorageObjectMetadata) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.inner.submit_get_with_metadata(key, tx);
        match rx
            .recv_timeout(PROVIDER_CAP)
            .expect("actual descriptor read")
        {
            CloudEvent::GetWithMetadata {
                key: actual,
                result: Ok(value),
            } if actual == key => value,
            event => panic!("required actual descriptor: {event:?}"),
        }
    }

    fn conditional_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let condition = headers.iter().find_map(|(header, value)| {
            header
                .eq_ignore_ascii_case("if-match")
                .then(|| value.clone())
        });
        let hold = if key == crate::cloud_layout::CloudObjectLayout::LEASE_OBJECT_KEY
            && condition.is_some()
        {
            self.cas_hold.lock().unwrap().take()
        } else {
            None
        };
        let Some(until) = hold else {
            self.inner.submit_put(key, data, headers, callback);
            return;
        };
        let (before, before_metadata) = self.descriptor(key);
        let (tx, rx) = std::sync::mpsc::channel();
        self.inner.submit_put(key, data, headers, tx);
        let event = rx
            .recv_timeout(PROVIDER_CAP)
            .expect("actual conditional PUT");
        if matches!(&event, CloudEvent::Put { key: actual, result: Ok(()) } if actual == key) {
            let committed_at = Instant::now();
            let (after, after_metadata) = self.descriptor(key);
            while Instant::now() < until && !self.release.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(5));
            }
            self.cas_writes.lock().unwrap().push(CasEvidence {
                key: key.into(),
                condition: condition.expect("actual If-Match"),
                committed_at,
                forwarded_at: Instant::now(),
                before,
                after,
                before_etag: before_metadata.etag,
                after_etag: after_metadata.etag,
            });
        }
        let _ = callback.send(event);
    }

    fn arm(&self, phase: CompactionPublishPhase, until: Instant) {
        let previous = self.hold.lock().unwrap().replace(ReadHold { phase, until });
        assert!(
            previous.is_none(),
            "the preceding real read must consume its hold"
        );
    }

    fn metadata_get(&self, key: &str, timeout: Option<Duration>, callback: &CloudCallback) {
        let (observed, result) = std::sync::mpsc::channel();
        self.inner.submit_get_with_metadata(key, observed);
        let event = result
            .recv_timeout(PROVIDER_CAP)
            .expect("actual delegated metadata read");
        let hold = self.hold.lock().unwrap().take();
        if let Some(hold) = hold {
            let delegated_at = Instant::now();
            let genuine_success = matches!(&event,
                CloudEvent::GetWithMetadata { key: actual, result: Ok((_, metadata)) }
                if actual == key && !metadata.etag.is_empty() && metadata.size > 0);
            while Instant::now() < hold.until && !self.release.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(5));
            }
            self.reads.lock().unwrap().push(ReadEvidence {
                phase: hold.phase,
                key: key.to_string(),
                delegated_at,
                forwarded_at: Instant::now(),
                provider_budget: timeout,
                genuine_success,
            });
        }
        // Forward the unchanged real result. Production checks the absolute
        // deadline after synchronous submission before crediting a late reply.
        let _ = callback.send(event);
    }
}

impl CloudBackend for PhasedMetadataBackend {
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_get(key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.metadata_get(key, None, &callback);
    }

    fn submit_get_with_metadata_with_timeout(
        &self,
        key: &str,
        timeout: Duration,
        callback: CloudCallback,
    ) {
        self.metadata_get(key, Some(timeout), &callback);
    }

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.conditional_put(key, data, headers, callback);
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        self.inner.submit_delete(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}

#[derive(Debug)]
struct PhaseEvidence {
    phase: CompactionPublishPhase,
    completed_at: Instant,
    error: Option<MidgeError>,
}

struct BudgetFixture {
    el: EventLoop,
    compute: crossbeam::channel::Receiver<RuntimeMsg>,
    cloud: Arc<CloudStorage>,
    backend: Arc<PhasedMetadataBackend>,
    lease: Arc<crate::lease::CloudStorageLease>,
    _token: LeaseGuard,
    inputs: Vec<(String, Vec<u8>)>,
    outputs: Vec<String>,
    stopped: bool,
}

impl BudgetFixture {
    fn new() -> MidgeResult<Self> {
        Self::new_with_all_families(false)
    }

    fn new_with_all_families(all_families: bool) -> MidgeResult<Self> {
        let (mut el, compute, _) = cloud_debt(4)?;
        // One actual generation; independent multi-CF coverage is separate.
        if !all_families {
            el.state
                .manifest
                .test_mut()
                .files
                .retain(|file| file.cf_id == 0);
        }
        el.state.set_compaction_enabled(false);
        el.runtime_response_timeout = CALLER_BUDGET;
        crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
            .map_err(MidgeError::Internal)?;
        let inputs = el
            .state
            .manifest
            .files
            .iter()
            .map(|file| {
                Ok((
                    file.name.clone(),
                    std::fs::read(el.state.sst_dir.join(&file.name))?,
                ))
            })
            .collect::<MidgeResult<Vec<_>>>()?;
        let backend = Arc::new(PhasedMetadataBackend::default());
        let cloud = Arc::new(CloudStorage::new_with_timeout(
            backend.clone(),
            String::new(),
            PROVIDER_CAP,
        ));
        let lease = Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
            crate::lease::CloudLeaseConfig {
                bucket: "test".into(),
                prefix: "compaction-budget".into(),
            },
            el.state.db_path.clone(),
            Arc::clone(&cloud),
        ));
        let token = Arc::clone(&lease)
            .try_acquire()
            .map_err(|error| error.into_validation_error("fixture lease acquisition"))?;
        el.fencing.writer_epoch = lease.epoch();
        el.fencing.leader_holder_id = Some(lease.holder_id());
        el.fencing.leader_store = lease.get_leader_store();
        el.fencing.lease_validity = Some(lease.lease_validity());
        el.fencing.lease_healthy = Some(Arc::new(AtomicBool::new(true)));
        el.cloud_coordinator.cloud_metadata_storage = Some(Arc::clone(&cloud));
        let (publication_tx, publication_rx) = crossbeam::channel::unbounded();
        el.compaction_publish_actor = CompactionPublishActor::new(publication_tx, false)?;
        el.compaction_publish_result_rx = publication_rx;
        let mut fixture = Self {
            el,
            compute,
            cloud,
            backend,
            lease,
            _token: token,
            inputs,
            outputs: Vec::new(),
            stopped: false,
        };
        fixture.seed_committed_metadata()?;
        Ok(fixture)
    }

    fn seed_committed_metadata(&mut self) -> MidgeResult<()> {
        let store = self
            .lease
            .get_leader_store()
            .expect("actual provider authority");
        let deadline = OperationDeadline::from_budget(FIXTURE_WAIT);
        crate::runtime::hybrid_persistence::mirror_control_metadata_within(
            crate::runtime::hybrid_persistence::CloudMetadataMirrorContext {
                cloud: &self.cloud,
                fs: self.el.state.fs.as_ref(),
                publication_lock: &self.el.metadata_publication_lock,
                lock_wait_budget: PROVIDER_CAP,
                local_manifest_sequence: self.el.state.manifest.last_persisted_sequence,
                deadline: &deadline,
                authority: crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority {
                    store: store.as_ref(),
                    holder_id: &self.lease.holder_id(),
                    writer_epoch: self.lease.epoch(),
                },
            },
            |deadline| {
                store
                    .validate_epoch_with_timeout(
                        &self.lease.holder_id(),
                        self.lease.epoch(),
                        deadline.clamp(PROVIDER_CAP),
                    )
                    .map_err(|error| error.into_validation_error("fixture metadata publication"))
            },
        )
    }

    fn start_manual(&mut self) -> (Instant, crossbeam::channel::Receiver<RuntimeResponse>) {
        // Capture only after all fixture/lease/baseline work has finished.
        let started = Instant::now();
        let response = self
            .el
            .router
            .register_at_for_test(MANUAL_REQUEST, "CompactAll", started);
        self.el.cloud_coordinator.cloud_maintenance.next =
            crate::runtime::event_loop::cloud_maintenance::MaintenanceTask::Compaction;
        CompactionCoordinator::compact_all(&mut self.el, MANUAL_REQUEST);
        (started, response)
    }

    fn complete_actual_compute(&mut self) {
        let completion = self
            .compute
            .recv_timeout(FIXTURE_WAIT)
            .expect("actual compute result");
        match &completion {
            RuntimeMsg::CompactionComplete {
                succeeded: true,
                input_ssts,
                output_ssts,
                cf_id: 0,
                ..
            } => {
                assert_eq!(input_ssts.len(), self.inputs.len());
                self.outputs = output_ssts.clone();
                assert!(!self.outputs.is_empty(), "real finalized output identity");
            }
            actual => panic!("required successful real compute phase: {actual:?}"),
        }
        let (_, messages) = crossbeam::channel::unbounded();
        self.el.handle_runtime_msg(completion, &messages);
    }

    fn complete_actual_phase(&mut self, next_hold: Option<ReadHold>) -> PhaseEvidence {
        let completion = self
            .el
            .compaction_publish_result_rx
            .recv_timeout(FIXTURE_WAIT)
            .expect("actual publication worker completion");
        let observed = PhaseEvidence {
            phase: completion.phase,
            completed_at: Instant::now(),
            error: completion.result.as_ref().err().map(MidgeError::replay),
        };
        // Arm before the coordinator can submit its next genuine task. This
        // provides deterministic phase construction without thread-name tests
        // or a scheduling sleep between completion and publication submission.
        if let Some(hold) = next_hold {
            self.backend.arm(hold.phase, hold.until);
        }
        CompactionCoordinator::handle_publication_completion(&mut self.el, completion);
        observed
    }

    fn committed_manifest(&self) -> MidgeResult<crate::metadata::Manifest> {
        let directory = tempfile::tempdir()?;
        let store = self
            .lease
            .get_leader_store()
            .expect("actual provider authority");
        crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
            &self.cloud,
            store.as_ref(),
            directory.path(),
            crate::config::RecoveryPolicy::Strict,
        )?;
        crate::metadata::ManifestPersistence::load(directory.path()).map_err(MidgeError::Internal)
    }

    fn exact_output_rows(&self) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        let mut rows = Vec::new();
        for output in &self.outputs {
            let reader = self
                .el
                .compaction_actor
                .open_sst_reader(&Path::new("sst").join(output))?;
            rows.extend(reader.scan_range_raw_state(None, None)?);
        }
        Ok(rows)
    }

    fn drive_manual_response(
        &mut self,
        response: &crossbeam::channel::Receiver<RuntimeResponse>,
    ) -> Result<RuntimeResponse, crossbeam::channel::RecvTimeoutError> {
        let until = Instant::now() + FIXTURE_WAIT;
        let (_, messages) = crossbeam::channel::unbounded();
        loop {
            match response.try_recv() {
                Ok(response) => return Ok(response),
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    return Err(crossbeam::channel::RecvTimeoutError::Disconnected);
                }
                Err(crossbeam::channel::TryRecvError::Empty) => {}
            }
            // Preserve actual dispatch/fairness ordering. A retirement turn
            // can own the publication gate after phase three; the next manual
            // turn completes only after its genuine storage events are drained.
            while let Ok(message) = self.compute.try_recv() {
                self.el.process_one(message, &messages);
            }
            if let Some(message) = self.el.pending_msg.take() {
                self.el.process_restored_one(message, &messages);
            }
            self.el.run_request_fairness_slot();
            if Instant::now() >= until {
                return Err(crossbeam::channel::RecvTimeoutError::Timeout);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn stop(&mut self) -> MidgeResult<()> {
        if self.stopped {
            return Ok(());
        }
        // Finite emergency release precedes joins; authority stays owned until
        // all actual compute/publication/GC/preflight worker joins finish.
        self.backend.release.store(true, Ordering::Release);
        // A publisher may still be reading accepted output identity. Join it
        // before actor cancellation can settle bookkeeping or remove scratch.
        self.el.compaction_publish_actor.shutdown_and_join()?;
        let storage = self
            .el
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .map(|storage| {
                Arc::clone(storage)
                    as Arc<dyn crate::runtime::actors::compaction::CompactionStorage>
            });
        self.el
            .compaction_actor
            .cancel_and_join_worker(&mut self.el.state, storage.as_ref());
        self.el.gc_actor.shutdown_workers();
        self.el.join_cloud_wal_prune_worker();
        self.stopped = true;
        self.lease
            .release()
            .map_err(|error| error.into_validation_error("fixture conditional release"))
    }
}

impl Drop for BudgetFixture {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn assert_one_exact_fixture_row(rows: MidgeResult<Vec<(Bytes, KeyState)>>) {
    let rows = rows.expect("read actual finalized output bytes");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.as_ref(), b"prune-candidate");
    assert!(
        matches!(&rows[0].1, KeyState::Value(value, 81, None, EntryType::Put)
        if value.as_ref() == b"value")
    );
}

fn assert_actual_reads_fit_io_cap(reads: &[ReadEvidence], started: Instant) {
    assert_eq!(
        reads.len(),
        2,
        "both actual phased read holds must execute: {reads:?}"
    );
    assert!(reads.iter().all(|read| read.genuine_success
        && !read.key.is_empty()
        && read.forwarded_at.duration_since(read.delegated_at) < PROVIDER_CAP
        && read
            .provider_budget
            .is_some_and(|budget| budget <= PROVIDER_CAP)));
    assert_eq!(reads[0].phase, CompactionPublishPhase::OutputDurable);
    assert_eq!(reads[1].phase, CompactionPublishPhase::ManifestPublished);
    assert!(reads[0].forwarded_at < started + CALLER_BUDGET);
    assert!(reads[1].forwarded_at >= started + CALLER_BUDGET);
}

#[test]
fn should_complete_manual_compaction_when_all_real_phases_fit_one_budget() -> MidgeResult<()> {
    // Arrange: acquire the shared failpoint scope before fixture setup and clocks.
    // Act: drive the real publication pipeline while retaining that scope.
    // Assert: the case checks exact rows, all three phases and caller settlement.
    crate::failpoints::with_read_gate(complete_manual_case)
}

fn complete_manual_case() -> MidgeResult<()> {
    // Arrange: actual remote metadata and compute fixture precede caller time.
    let mut fixture = BudgetFixture::new()?;
    let (started, response) = fixture.start_manual();

    // Act: drive only actual worker messages, including all three durable phases.
    fixture.complete_actual_compute();
    let mut phases = Vec::new();
    for _ in 0..3 {
        phases.push(fixture.complete_actual_phase(None));
    }
    let rows = fixture.exact_output_rows();
    let committed = fixture.committed_manifest();
    let outcome = fixture.drive_manual_response(&response);
    let route_state = describe_settle_state(&fixture.el);
    fixture.el.gc_actor.shutdown_workers();
    let input_retired: Vec<_> = fixture
        .inputs
        .iter()
        .map(|(name, _)| !remote_sst_path_for_test(&fixture.el, name).exists())
        .collect();
    let has_intent = fixture.el.state.has_compaction_publication_intent(
        &fixture
            .inputs
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>(),
        &fixture.outputs,
    );
    let quotas = fixture
        .el
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .budget_snapshot();
    fixture.stop()?;

    // Assert: real success, identity, exact value and retirement; zero failures.
    assert_eq!(
        phases.iter().map(|phase| phase.phase).collect::<Vec<_>>(),
        vec![
            CompactionPublishPhase::OutputDurable,
            CompactionPublishPhase::ManifestPublished,
            CompactionPublishPhase::IntentCleared,
        ]
    );
    assert!(phases
        .iter()
        .all(|phase| phase.error.is_none() && phase.completed_at < started + CALLER_BUDGET));
    assert!(
        matches!(
            outcome,
            Ok(RuntimeResponse::Ok {
                request_id: MANUAL_REQUEST
            })
        ),
        "actual manual route: {outcome:?}; {route_state}"
    );
    assert_one_exact_fixture_row(rows);
    let committed = committed?;
    assert_eq!(
        committed
            .files
            .iter()
            .map(|file| &file.name)
            .collect::<std::collections::BTreeSet<_>>(),
        fixture
            .outputs
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
    );
    assert!(!has_intent);
    assert!(input_retired.iter().all(|retired| *retired));
    assert_eq!(quotas.usage.compaction_staging_reserved_bytes, 0);
    assert_eq!(quotas.usage.reservations, 0);
    Ok(())
}

#[test]
fn should_retain_both_authorities_when_second_real_phase_exceeds_caller_budget() -> MidgeResult<()>
{
    // Arrange: acquire the shared failpoint scope before fixture setup and clocks.
    // Act: let the actual second publication phase cross the original deadline.
    // Assert: the case checks retained authorities and the genuine caller error.
    crate::failpoints::with_read_gate(second_phase_timeout_case)
}

fn second_phase_timeout_case() -> MidgeResult<()> {
    // Arrange: each delayed GET fits the ordinary four-second I/O cap.
    let mut fixture = BudgetFixture::new()?;
    let (started, response) = fixture.start_manual();
    fixture.backend.arm(
        CompactionPublishPhase::OutputDurable,
        started + Duration::from_secs(3),
    );

    // Act: compute plus first durable phase are real successful accepted work.
    fixture.complete_actual_compute();
    let first = fixture.complete_actual_phase(Some(ReadHold {
        phase: CompactionPublishPhase::ManifestPublished,
        until: started + CALLER_BUDGET + Duration::from_millis(500),
    }));
    let first_live = first.phase == CompactionPublishPhase::OutputDurable
        && first.error.is_none()
        && first.completed_at < started + CALLER_BUDGET;
    if !first_live {
        fixture.stop()?;
        panic!("invalid RED: first genuine phase did not finish within caller budget: {first:?}");
    }
    // The second hold was installed before the actual next task submission.
    let second = fixture.complete_actual_phase(None);
    let mut phases = vec![first, second];
    if phases[1].error.is_none() {
        // Let old per-phase-reset behavior complete genuinely, so an unrelated
        // cleanup error or still-pending route cannot count as the intended RED.
        phases.push(fixture.complete_actual_phase(None));
    }
    let rows = fixture.exact_output_rows();
    let committed = fixture.committed_manifest();
    let outcome = fixture.drive_manual_response(&response);
    fixture.el.gc_actor.shutdown_workers();
    let names: Vec<_> = fixture
        .inputs
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    let has_intent = fixture
        .el
        .state
        .has_compaction_publication_intent(&names, &fixture.outputs);
    let retained_inputs: Vec<_> = fixture
        .inputs
        .iter()
        .map(|(name, expected)| {
            std::fs::read(remote_sst_path_for_test(&fixture.el, name))
                .is_ok_and(|actual| actual == *expected)
        })
        .collect();
    let local_outputs = fixture
        .outputs
        .iter()
        .all(|name| fixture.el.state.manifest_has_file(name));
    let reads = fixture.backend.reads.lock().unwrap().clone();
    fixture.stop()?;

    // Assert after actual worker joins and conditional lease release.
    assert_one_exact_fixture_row(rows);
    assert_actual_reads_fit_io_cap(&reads, started);
    let committed = committed?;
    assert!(
        matches!(&phases[1].error, Some(MidgeError::Timeout(_))),
        "actual phase result: {phases:?}"
    );
    assert_eq!(phases[1].phase, CompactionPublishPhase::ManifestPublished);
    assert_eq!(
        phases.len(),
        2,
        "no new IntentCleared provider phase after expiry"
    );
    assert!(
        matches!(
            outcome,
            Ok(RuntimeResponse::Error {
                request_id: MANUAL_REQUEST,
                error: MidgeError::Timeout(_),
            })
        ),
        "actual manual route: {outcome:?}"
    );
    assert!(local_outputs && has_intent);
    assert!(retained_inputs.iter().all(|retained| *retained));
    assert_eq!(
        committed
            .files
            .iter()
            .map(|file| &file.name)
            .collect::<std::collections::BTreeSet<_>>(),
        names.iter().collect::<std::collections::BTreeSet<_>>()
    );
    Ok(())
}
