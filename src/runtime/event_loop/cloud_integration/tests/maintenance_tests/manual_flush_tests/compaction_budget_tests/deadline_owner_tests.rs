//! Genuine accepted generations keep their origin independently of waiters.
//!
//! Real compute messages are received before dispatch, then later `CompactAll`
//! routes join through the actual coordinator. Provider holds forward genuine
//! conditional mock results; these controls prove ownership and accounting,
//! not native socket cancellation or public transaction acknowledgements.

use super::*;
use std::collections::BTreeSet;

const SHORT_WAIT: Duration = Duration::from_secs(2);
const LONG_WAIT: Duration = Duration::from_secs(8);
const SHORT_REQUEST: u64 = MANUAL_REQUEST - 2;
const LONG_REQUEST: u64 = MANUAL_REQUEST - 1;

struct GenerationReceipt {
    message: Option<RuntimeMsg>,
    cf_id: u32,
    target_level: u32,
    generation: u64,
    inputs: Vec<String>,
    outputs: Vec<String>,
    accepted_origin: Option<OperationDeadline>,
}

impl GenerationReceipt {
    fn from_actual(fixture: &BudgetFixture, message: RuntimeMsg) -> MidgeResult<Self> {
        let (cf_id, target_level, inputs, outputs) = match &message {
            RuntimeMsg::CompactionComplete {
                cf_id,
                target_level,
                input_ssts,
                output_ssts,
                succeeded: true,
                ..
            } if !input_ssts.is_empty() && !output_ssts.is_empty() => (
                *cf_id,
                *target_level,
                input_ssts.clone(),
                output_ssts.clone(),
            ),
            actual => {
                return Err(MidgeError::Internal(format!(
                    "requires a genuine successful accepted compute result: {actual:?}"
                )));
            }
        };
        let generation = fixture
            .el
            .compaction_actor
            .accepted_output_generation(cf_id, target_level)?;
        let accepted_origin = fixture.el.compaction_actor.manual_deadline_for_generation(
            cf_id,
            target_level,
            generation,
        )?;
        Ok(Self {
            message: Some(message),
            cf_id,
            target_level,
            generation,
            inputs,
            outputs,
            accepted_origin,
        })
    }

    fn current_origin(&self, fixture: &BudgetFixture) -> MidgeResult<Option<OperationDeadline>> {
        fixture.el.compaction_actor.manual_deadline_for_generation(
            self.cf_id,
            self.target_level,
            self.generation,
        )
    }
}

fn receive_generation(fixture: &mut BudgetFixture) -> MidgeResult<GenerationReceipt> {
    let until = Instant::now() + FIXTURE_WAIT;
    let (_, messages) = crossbeam::channel::unbounded();
    loop {
        match fixture.compute.try_recv() {
            Ok(message @ RuntimeMsg::CompactionComplete { .. }) => {
                return GenerationReceipt::from_actual(fixture, message);
            }
            Ok(message) => {
                fixture.el.process_one(message, &messages);
            }
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                return Err(MidgeError::Internal("actual compute channel closed".into()));
            }
            Err(crossbeam::channel::TryRecvError::Empty) => {}
        }
        if let Some(message) = fixture.el.pending_msg.take() {
            fixture.el.process_restored_one(message, &messages);
        }
        // Preserve genuine retirement/flush fairness and accepted handle reaping
        // between CFs. Never fabricate a completion or clear an owner flag.
        fixture.el.run_request_fairness_slot();
        if Instant::now() >= until {
            return Err(MidgeError::Internal(format!(
                "no actual next compute receipt: {}",
                describe_settle_state(&fixture.el)
            )));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn dispatch_generation(fixture: &mut BudgetFixture, receipt: &mut GenerationReceipt) {
    fixture.outputs.clone_from(&receipt.outputs);
    let (_, messages) = crossbeam::channel::unbounded();
    let message = receipt
        .message
        .take()
        .expect("dispatch each genuine receipt once");
    fixture.el.handle_runtime_msg(message, &messages);
}

fn join_manual(
    fixture: &mut BudgetFixture,
    request_id: u64,
    budget: Duration,
) -> (
    OperationDeadline,
    crossbeam::channel::Receiver<RuntimeResponse>,
) {
    let started = Instant::now();
    let deadline = OperationDeadline::from_start(started, budget);
    let response =
        fixture
            .el
            .router
            .register_with_deadline(request_id, "CompactAll", started, deadline);
    let (_, messages) = crossbeam::channel::unbounded();
    fixture
        .el
        .handle_runtime_msg(RuntimeMsg::CompactAll { request_id }, &messages);
    (deadline, response)
}

fn complete_three_phases(fixture: &mut BudgetFixture) -> Vec<PhaseEvidence> {
    (0..3)
        .map(|_| fixture.complete_actual_phase(None))
        .collect()
}

fn rows_for_generation(
    fixture: &BudgetFixture,
    receipt: &GenerationReceipt,
) -> MidgeResult<Vec<(Bytes, KeyState)>> {
    let mut rows = Vec::new();
    for output in &receipt.outputs {
        let reader = fixture
            .el
            .compaction_actor
            .open_sst_reader(&Path::new("sst").join(output))?;
        rows.extend(reader.scan_range_raw_state(None, None)?);
    }
    Ok(rows)
}

fn retained_inputs(fixture: &BudgetFixture, receipt: &GenerationReceipt) -> Vec<bool> {
    receipt
        .inputs
        .iter()
        .map(|name| {
            fixture
                .inputs
                .iter()
                .find(|(original, _)| original == name)
                .is_some_and(|(_, expected)| {
                    std::fs::read(remote_sst_path_for_test(&fixture.el, name))
                        .is_ok_and(|bytes| bytes == *expected)
                })
        })
        .collect()
}

fn retired_inputs(fixture: &BudgetFixture, receipt: &GenerationReceipt) -> Vec<bool> {
    receipt
        .inputs
        .iter()
        .map(|name| !remote_sst_path_for_test(&fixture.el, name).exists())
        .collect()
}

fn assert_phases_succeeded(phases: &[PhaseEvidence]) {
    assert_eq!(phases.len(), 3);
    assert_eq!(
        phases.iter().map(|phase| phase.phase).collect::<Vec<_>>(),
        vec![
            CompactionPublishPhase::OutputDurable,
            CompactionPublishPhase::ManifestPublished,
            CompactionPublishPhase::IntentCleared,
        ]
    );
    assert!(
        phases.iter().all(|phase| phase.error.is_none()),
        "{phases:?}"
    );
}

fn assert_route_ok(
    outcome: &Result<RuntimeResponse, crossbeam::channel::RecvTimeoutError>,
    id: u64,
) {
    assert!(
        matches!(outcome, Ok(RuntimeResponse::Ok { request_id }) if *request_id == id),
        "actual successful manual route {id}: {outcome:?}"
    );
}

fn assert_route_timeout(
    outcome: &Result<RuntimeResponse, crossbeam::channel::RecvTimeoutError>,
    id: u64,
) {
    assert!(
        matches!(outcome,
        Ok(RuntimeResponse::Error { request_id, error: MidgeError::Timeout(_) })
            if *request_id == id),
        "actual typed manual route {id}: {outcome:?}"
    );
}

fn manifest_names(manifest: &crate::metadata::Manifest) -> BTreeSet<String> {
    manifest
        .files
        .iter()
        .map(|file| file.name.clone())
        .collect()
}

fn assert_genuine_holds(reads: &[ReadEvidence], expected: usize) {
    assert_eq!(
        reads.len(),
        expected,
        "positive actual read observations: {reads:?}"
    );
    assert!(
        reads.iter().all(|read| read.genuine_success
            && !read.key.is_empty()
            && read.forwarded_at.duration_since(read.delegated_at) < PROVIDER_CAP
            && read
                .provider_budget
                .is_some_and(|budget| !budget.is_zero() && budget <= PROVIDER_CAP)),
        "actual individual provider caps/results: {reads:?}"
    );
}

#[test]
fn should_preserve_background_owner_when_later_manual_waiter_expires() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: launch through the actual background fair turn, without a route.
        let mut fixture = BudgetFixture::new()?;
        fixture.el.state.set_compaction_enabled(true);
        fixture.el.cloud_coordinator.cloud_maintenance.next =
            crate::runtime::event_loop::cloud_maintenance::MaintenanceTask::Compaction;
        let launch = fixture.el.schedule_cloud_maintenance();
        let mut receipt = receive_generation(&mut fixture)?;
        let joined_at = Instant::now();
        let (short_deadline, response) = join_manual(&mut fixture, SHORT_REQUEST, SHORT_WAIT);
        let after_join = receipt.current_origin(&fixture)?;
        let pending_short = fixture
            .el
            .state
            .pending_compaction_waits
            .get(&SHORT_REQUEST)
            .copied();
        fixture.backend.arm(
            CompactionPublishPhase::OutputDurable,
            joined_at + SHORT_WAIT + Duration::from_millis(500),
        );

        // Act: the real background publisher outlives only the joining waiter.
        dispatch_generation(&mut fixture, &mut receipt);
        let phases = complete_three_phases(&mut fixture);
        let outcome = fixture.drive_manual_response(&response);
        let rows = rows_for_generation(&fixture, &receipt);
        let committed = fixture.committed_manifest();
        fixture.el.gc_actor.shutdown_workers();
        let retired = retired_inputs(&fixture, &receipt);
        let has_intent = fixture
            .el
            .state
            .has_compaction_publication_intent(&receipt.inputs, &receipt.outputs);
        let reads = fixture.backend.reads.lock().unwrap().clone();
        let healthy = fixture.el.check_lease_health();
        fixture.stop()?;

        // Assert: no later waiter imposed its clock on the accepted background job.
        assert_eq!(
            launch,
            Some(crate::runtime::event_loop::cloud_maintenance::MaintenanceTask::Compaction)
        );
        assert_eq!(receipt.accepted_origin, None);
        assert_eq!(after_join, None);
        assert_eq!(pending_short, Some(short_deadline));
        assert_genuine_holds(&reads, 1);
        assert!(reads[0].forwarded_at >= joined_at + SHORT_WAIT);
        assert_phases_succeeded(&phases);
        assert_route_timeout(&outcome, SHORT_REQUEST);
        assert_one_exact_fixture_row(rows);
        assert_eq!(
            manifest_names(&committed?),
            receipt.outputs.iter().cloned().collect()
        );
        assert!(retired.iter().all(|retired| *retired) && !has_intent);
        healthy.expect("background owner remains healthy while later caller expires");
        Ok(())
    })
}

#[test]
fn should_keep_manual_origin_when_later_shorter_waiter_expires() -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: lower IDs intentionally tempt a resampled map-origin policy.
        let mut fixture = BudgetFixture::new()?;
        let (started, original_response) = fixture.start_manual();
        let original = OperationDeadline::from_start(started, CALLER_BUDGET);
        let mut receipt = receive_generation(&mut fixture)?;
        let short_started = Instant::now();
        let (short_deadline, short_response) = join_manual(&mut fixture, SHORT_REQUEST, SHORT_WAIT);
        let (long_deadline, long_response) = join_manual(&mut fixture, LONG_REQUEST, LONG_WAIT);
        let after_join = receipt.current_origin(&fixture)?;
        let pending_short = fixture
            .el
            .state
            .pending_compaction_waits
            .get(&SHORT_REQUEST)
            .copied();
        let pending_long = fixture
            .el
            .state
            .pending_compaction_waits
            .get(&LONG_REQUEST)
            .copied();
        fixture.backend.arm(
            CompactionPublishPhase::OutputDurable,
            short_started + SHORT_WAIT + Duration::from_millis(500),
        );

        // Act: the shorter route ends independently of the accepted generation.
        dispatch_generation(&mut fixture, &mut receipt);
        let first = fixture.complete_actual_phase(None);
        CompactionCoordinator::expire_manual_compaction_waiters(&mut fixture.el);
        let after_expiry = receipt.current_origin(&fixture)?;
        let mut phases = vec![first];
        phases.extend((0..2).map(|_| fixture.complete_actual_phase(None)));
        let short_outcome = fixture.drive_manual_response(&short_response);
        let original_outcome = fixture.drive_manual_response(&original_response);
        let long_outcome = fixture.drive_manual_response(&long_response);
        let rows = rows_for_generation(&fixture, &receipt);
        let committed = fixture.committed_manifest();
        fixture.el.gc_actor.shutdown_workers();
        let retired = retired_inputs(&fixture, &receipt);
        let healthy = fixture.el.check_lease_health();
        fixture.stop()?;

        // Assert: neither joining route replaced the actual initiating deadline.
        assert_eq!(receipt.accepted_origin, Some(original));
        assert_eq!(after_join, Some(original));
        assert_eq!(after_expiry, Some(original));
        assert_eq!(pending_short, Some(short_deadline));
        assert_eq!(pending_long, Some(long_deadline));
        assert_phases_succeeded(&phases);
        assert!(phases
            .iter()
            .all(|phase| phase.completed_at < started + CALLER_BUDGET));
        assert_route_timeout(&short_outcome, SHORT_REQUEST);
        assert_route_ok(&original_outcome, MANUAL_REQUEST);
        assert_route_ok(&long_outcome, LONG_REQUEST);
        assert_one_exact_fixture_row(rows);
        assert_eq!(
            manifest_names(&committed?),
            receipt.outputs.iter().cloned().collect()
        );
        assert!(retired.iter().all(|retired| *retired));
        healthy.expect("accepted manual owner remains healthy after independent waiter expiry");
        Ok(())
    })
}

#[test]
fn should_refuse_budget_extension_when_longer_waiter_joins_accepted_manual_work() -> MidgeResult<()>
{
    crate::failpoints::with_read_gate(|| {
        // Arrange: real phase holds each fit the four-second provider cap.
        let mut fixture = BudgetFixture::new()?;
        let (started, original_response) = fixture.start_manual();
        let original = OperationDeadline::from_start(started, CALLER_BUDGET);
        let mut receipt = receive_generation(&mut fixture)?;
        let (long_deadline, long_response) = join_manual(&mut fixture, LONG_REQUEST, LONG_WAIT);
        let after_join = receipt.current_origin(&fixture)?;
        fixture.backend.arm(
            CompactionPublishPhase::OutputDurable,
            started + Duration::from_secs(3),
        );

        // Act: a live longer waiter cannot renew the earlier accepted generation.
        dispatch_generation(&mut fixture, &mut receipt);
        let first = fixture.complete_actual_phase(Some(ReadHold {
            phase: CompactionPublishPhase::ManifestPublished,
            until: started + CALLER_BUDGET + Duration::from_millis(500),
        }));
        let second = fixture.complete_actual_phase(None);
        let longer_still_live = !long_deadline.is_expired();
        let original_outcome = fixture.drive_manual_response(&original_response);
        let long_outcome = fixture.drive_manual_response(&long_response);
        let rows = rows_for_generation(&fixture, &receipt);
        let committed = fixture.committed_manifest();
        let retained = retained_inputs(&fixture, &receipt);
        let has_intent = fixture
            .el
            .state
            .has_compaction_publication_intent(&receipt.inputs, &receipt.outputs);
        let local_outputs = receipt
            .outputs
            .iter()
            .all(|name| fixture.el.state.manifest_has_file(name));
        let no_third_phase = fixture.el.compaction_publish_result_rx.is_empty()
            && !fixture.el.compaction_publish_actor.is_inflight();
        let reads = fixture.backend.reads.lock().unwrap().clone();
        let healthy = fixture.el.check_lease_health();
        fixture.stop()?;

        // Assert: exact rows and both authorities survive original caller expiry.
        assert_eq!(receipt.accepted_origin, Some(original));
        assert_eq!(after_join, Some(original));
        assert!(
            first.error.is_none() && first.completed_at < started + CALLER_BUDGET,
            "positive first genuine phase: {first:?}"
        );
        assert_eq!(second.phase, CompactionPublishPhase::ManifestPublished);
        assert!(
            matches!(second.error, Some(MidgeError::Timeout(_))),
            "{second:?}"
        );
        assert!(longer_still_live && no_third_phase);
        assert_actual_reads_fit_io_cap(&reads, started);
        assert_route_timeout(&original_outcome, MANUAL_REQUEST);
        assert_route_timeout(&long_outcome, LONG_REQUEST);
        assert_one_exact_fixture_row(rows);
        assert_eq!(
            manifest_names(&committed?),
            receipt.inputs.iter().cloned().collect()
        );
        assert!(retained.iter().all(|retained| *retained) && local_outputs && has_intent);
        healthy.expect("caller expiry leaves actual provider lease healthy");
        Ok(())
    })
}

#[test]
fn should_inherit_original_clock_when_actual_manual_work_reaches_second_family() -> MidgeResult<()>
{
    crate::failpoints::with_read_gate(|| {
        // Arrange: both families have genuine input bytes before the original clock.
        let mut fixture = BudgetFixture::new_with_all_families(true)?;
        let (started, response) = fixture.start_manual();
        let original = OperationDeadline::from_start(started, CALLER_BUDGET);
        let mut first = receive_generation(&mut fixture)?;

        // Act: use each family's actual compute receipt and all three real phases.
        dispatch_generation(&mut fixture, &mut first);
        let first_phases = complete_three_phases(&mut fixture);
        let mut second = receive_generation(&mut fixture)?;
        dispatch_generation(&mut fixture, &mut second);
        let second_phases = complete_three_phases(&mut fixture);
        let outcome = fixture.drive_manual_response(&response);
        let first_rows = rows_for_generation(&fixture, &first);
        let second_rows = rows_for_generation(&fixture, &second);
        let committed = fixture.committed_manifest();
        fixture.el.gc_actor.shutdown_workers();
        let retired = [
            retired_inputs(&fixture, &first),
            retired_inputs(&fixture, &second),
        ];
        let intents = [
            fixture
                .el
                .state
                .has_compaction_publication_intent(&first.inputs, &first.outputs),
            fixture
                .el
                .state
                .has_compaction_publication_intent(&second.inputs, &second.outputs),
        ];
        let healthy = fixture.el.check_lease_health();
        fixture.stop()?;

        // Assert: a new CF generation consumes the same immutable caller allowance.
        assert_ne!(first.cf_id, second.cf_id);
        assert_eq!(first.inputs.len(), 4);
        assert_eq!(second.inputs.len(), 4);
        assert_eq!(first.accepted_origin, Some(original));
        assert_eq!(second.accepted_origin, Some(original));
        assert_phases_succeeded(&first_phases);
        assert_phases_succeeded(&second_phases);
        assert!(first_phases
            .iter()
            .chain(&second_phases)
            .all(|phase| phase.completed_at < started + CALLER_BUDGET));
        assert_route_ok(&outcome, MANUAL_REQUEST);
        assert_one_exact_fixture_row(first_rows);
        assert_one_exact_fixture_row(second_rows);
        let expected: BTreeSet<_> = first
            .outputs
            .iter()
            .chain(&second.outputs)
            .cloned()
            .collect();
        let committed = committed?;
        assert_eq!(manifest_names(&committed), expected);
        assert!(committed.files.iter().any(|file| file.cf_id == first.cf_id));
        assert!(committed
            .files
            .iter()
            .any(|file| file.cf_id == second.cf_id));
        assert!(retired.iter().flatten().all(|retired| *retired));
        assert!(intents.iter().all(|intent| !intent));
        healthy.expect("healthy two-family publication retains provider authority");
        Ok(())
    })
}

#[test]
fn should_stop_second_family_when_original_manual_clock_expires() -> MidgeResult<()> {
    // Arrange: genuine accepted generations share the original clock.
    // Act: the helper dispatches actual compute and publication receipts.
    // Assert: preserve exact per-family authority at expiry.
    verify_second_family_expiry(false)
}

#[test]
fn should_retain_input_authority_when_second_family_compute_arrives_after_manual_deadline(
) -> MidgeResult<()> {
    // Arrange: receive a genuine accepted second-family compute result.
    // Act: hold its dispatch until the original deadline expires.
    // Assert: no publication callback is required for unsubmitted work.
    verify_second_family_expiry(true)
}

// Keep both real completion dispositions beside their shared authority ledger.
#[allow(clippy::too_many_lines)]
fn verify_second_family_expiry(queue_past_deadline: bool) -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: one real caller owns both CF generations; no late route refresh.
        let mut fixture = BudgetFixture::new_with_all_families(true)?;
        let (started, response) = fixture.start_manual();
        let original = OperationDeadline::from_start(started, CALLER_BUDGET);
        let mut first = receive_generation(&mut fixture)?;
        fixture.backend.arm(
            CompactionPublishPhase::OutputDurable,
            started + Duration::from_secs(3),
        );

        // Act: complete the first CF, then forward a late actual response for CF2.
        dispatch_generation(&mut fixture, &mut first);
        let first_phases = complete_three_phases(&mut fixture);
        let mut second = receive_generation(&mut fixture)?;
        // Rejected pre-intent outputs may be cleaned up; verify the genuine
        // compute bytes before dispatch, without requiring orphan retention.
        let mut second_rows = rows_for_generation(&fixture, &second);
        fixture.backend.arm(
            CompactionPublishPhase::OutputDurable,
            started + CALLER_BUDGET + Duration::from_millis(500),
        );
        if queue_past_deadline {
            std::thread::sleep(original.remaining() + Duration::from_millis(20));
            assert!(
                original.is_expired(),
                "queued receipt must exhaust the original clock"
            );
        }
        dispatch_generation(&mut fixture, &mut second);
        let rejected_before_publication = original.is_expired()
            && !fixture.el.compaction_publish_actor.is_inflight()
            && fixture.el.compaction_publish_result_rx.is_empty();
        let second_phase = if rejected_before_publication {
            None
        } else {
            Some(fixture.complete_actual_phase(None))
        };
        if second_phase.is_some() {
            // A persisted publication intent retains the output generation.
            second_rows = rows_for_generation(&fixture, &second);
        }
        let no_later_phase = fixture.el.compaction_publish_result_rx.is_empty()
            && !fixture.el.compaction_publish_actor.is_inflight();
        let outcome = fixture.drive_manual_response(&response);
        let first_rows = rows_for_generation(&fixture, &first);
        let committed = fixture.committed_manifest();
        fixture.el.gc_actor.shutdown_workers();
        let first_retired = retired_inputs(&fixture, &first);
        let second_retained = retained_inputs(&fixture, &second);
        let second_intent = fixture
            .el
            .state
            .has_compaction_publication_intent(&second.inputs, &second.outputs);
        let second_local_inputs = second
            .inputs
            .iter()
            .all(|name| fixture.el.state.manifest_has_file(name));
        let reads = fixture.backend.reads.lock().unwrap().clone();
        let healthy = fixture.el.check_lease_health();
        fixture.stop()?;

        // Assert: CF1 stays committed; CF2 retains exact prior authority and bytes.
        assert_ne!(first.cf_id, second.cf_id);
        assert_eq!(first.inputs.len(), 4);
        assert_eq!(second.inputs.len(), 4);
        assert_eq!(first.accepted_origin, Some(original));
        assert_eq!(second.accepted_origin, Some(original));
        assert_phases_succeeded(&first_phases);
        assert!(first_phases
            .iter()
            .all(|phase| phase.completed_at < started + CALLER_BUDGET));
        if let Some(second_phase) = &second_phase {
            assert_eq!(second_phase.phase, CompactionPublishPhase::OutputDurable);
            assert!(
                matches!(second_phase.error, Some(MidgeError::Timeout(_))),
                "{second_phase:?}"
            );
            assert!(second_intent);
            assert_genuine_holds(&reads, 2);
            assert!(reads[1].forwarded_at >= started + CALLER_BUDGET);
        } else {
            assert!(rejected_before_publication && original.is_expired());
            assert!(
                !second_intent,
                "no phase or intent was accepted after expiry"
            );
            assert_genuine_holds(&reads, 1);
        }
        if queue_past_deadline {
            assert!(
                second_phase.is_none(),
                "expired queued compute must not submit publication"
            );
        }
        assert!(no_later_phase && second_local_inputs);
        assert!(reads[0].forwarded_at < started + CALLER_BUDGET);
        assert_route_timeout(&outcome, MANUAL_REQUEST);
        assert_one_exact_fixture_row(first_rows);
        assert_one_exact_fixture_row(second_rows);
        let expected: BTreeSet<_> = first
            .outputs
            .iter()
            .chain(&second.inputs)
            .cloned()
            .collect();
        assert_eq!(manifest_names(&committed?), expected);
        assert!(first_retired.iter().all(|retired| *retired));
        assert!(second_retained.iter().all(|retained| *retained));
        healthy.expect("expiry of one caller does not invalidate actual lease ownership");
        Ok(())
    })
}
