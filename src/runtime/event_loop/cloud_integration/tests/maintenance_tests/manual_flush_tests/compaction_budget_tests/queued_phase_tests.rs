//! Delay delivery of genuine successful receipts across the accepted deadline.

use super::*;
use crate::runtime::actors::compaction::publication::CompactionPublishCompletion;

struct QueuedEvidence {
    outcome: Result<RuntimeResponse, crossbeam::channel::RecvTimeoutError>,
    rows: MidgeResult<Vec<(Bytes, KeyState)>>,
    committed: MidgeResult<crate::metadata::Manifest>,
    local_files: std::collections::BTreeSet<String>,
    inputs: Vec<String>,
    outputs: Vec<String>,
    retained_inputs: Vec<bool>,
    retired_inputs: Vec<bool>,
    has_intent: bool,
    no_phase: bool,
    active: usize,
    reservations: usize,
    staging_reserved: u64,
    uploads_before: Vec<(String, u64)>,
    uploads_after: Vec<(String, u64)>,
}

fn input_names(fixture: &BudgetFixture) -> Vec<String> {
    fixture
        .inputs
        .iter()
        .map(|(name, _)| name.clone())
        .collect()
}

fn capture_queued_evidence(
    fixture: &mut BudgetFixture,
    response: &crossbeam::channel::Receiver<RuntimeResponse>,
    uploads_before: Vec<(String, u64)>,
) -> MidgeResult<QueuedEvidence> {
    let outcome = fixture.drive_manual_response(response);
    fixture.el.gc_actor.shutdown_workers();
    let inputs = input_names(fixture);
    let quotas = fixture
        .el
        .cloud_coordinator
        .hybrid_storage
        .as_ref()
        .unwrap()
        .budget_snapshot();
    let rows = fixture.exact_output_rows();
    let committed = fixture.committed_manifest();
    let evidence = QueuedEvidence {
        outcome,
        rows,
        committed,
        local_files: fixture
            .el
            .state
            .manifest
            .files
            .iter()
            .map(|file| file.name.clone())
            .collect(),
        inputs: inputs.clone(),
        outputs: fixture.outputs.clone(),
        retained_inputs: fixture
            .inputs
            .iter()
            .map(|(name, bytes)| {
                std::fs::read(remote_sst_path_for_test(&fixture.el, name))
                    .is_ok_and(|actual| actual == *bytes)
            })
            .collect(),
        retired_inputs: fixture
            .inputs
            .iter()
            .map(|(name, _)| !remote_sst_path_for_test(&fixture.el, name).exists())
            .collect(),
        has_intent: fixture
            .el
            .state
            .has_compaction_publication_intent(&inputs, &fixture.outputs),
        no_phase: !fixture.el.compaction_publish_actor.is_inflight()
            && fixture.el.compaction_publish_result_rx.is_empty()
            && fixture.el.compaction_publication.get().is_none(),
        active: fixture.el.state.active_compactions.load(Ordering::Acquire),
        reservations: quotas.usage.reservations,
        staging_reserved: quotas.usage.compaction_staging_reserved_bytes,
        uploads_before,
        uploads_after: fixture.backend.inner.get_uploads(),
    };
    fixture.stop()?;
    Ok(evidence)
}

fn receive_live_success(
    fixture: &mut BudgetFixture,
    phase: CompactionPublishPhase,
    started: Instant,
) -> CompactionPublishCompletion {
    let completion = fixture
        .el
        .compaction_publish_result_rx
        .recv_timeout(FIXTURE_WAIT)
        .expect("actual successful worker receipt");
    assert_eq!(completion.phase, phase);
    assert!(
        completion.result.is_ok(),
        "actual phase: {:?}",
        completion.result.as_ref().err()
    );
    assert!(
        Instant::now() < started + CALLER_BUDGET,
        "positive phase must genuinely complete while original owner is live"
    );
    completion
}

fn delayed_success_case(phase: CompactionPublishPhase) -> MidgeResult<QueuedEvidence> {
    crate::failpoints::with_read_gate(|| delayed_success_case_inner(phase))
}

fn delayed_success_case_inner(phase: CompactionPublishPhase) -> MidgeResult<QueuedEvidence> {
    let mut fixture = BudgetFixture::new()?;
    let (started, response) = fixture.start_manual();
    fixture.complete_actual_compute();
    if phase != CompactionPublishPhase::OutputDurable {
        let prior = fixture.complete_actual_phase(None);
        assert!(prior.error.is_none() && prior.phase == CompactionPublishPhase::OutputDurable);
    }
    if phase == CompactionPublishPhase::IntentCleared {
        let prior = fixture.complete_actual_phase(None);
        assert!(prior.error.is_none() && prior.phase == CompactionPublishPhase::ManifestPublished);
    }
    let completion = receive_live_success(&mut fixture, phase, started);
    let duplicate =
        (phase == CompactionPublishPhase::IntentCleared).then(|| CompactionPublishCompletion {
            token: completion.token.clone(),
            phase: completion.phase,
            result: Ok(()),
        });
    let uploads_before = fixture.backend.inner.get_uploads();
    while Instant::now() < started + CALLER_BUDGET {
        std::thread::sleep(Duration::from_millis(5));
    }
    // Deliver the unchanged actual receipt. The optional second message is
    // explicitly a replay of that observed final receipt, not another success.
    CompactionCoordinator::handle_publication_completion(&mut fixture.el, completion);
    if let Some(duplicate) = duplicate {
        CompactionCoordinator::handle_publication_completion(&mut fixture.el, duplicate);
    }
    capture_queued_evidence(&mut fixture, &response, uploads_before)
}

fn assert_common_queued_evidence(evidence: &QueuedEvidence) {
    assert!(evidence.no_phase);
    assert_eq!(evidence.active, 0);
    assert_eq!(
        evidence.uploads_after, evidence.uploads_before,
        "delayed coordinator delivery admits no new authority write"
    );
    assert!(
        matches!(
            evidence.outcome,
            Ok(RuntimeResponse::Error {
                request_id: MANUAL_REQUEST,
                error: MidgeError::Timeout(_)
            })
        ),
        "actual caller route: {:?}",
        evidence.outcome
    );
}

fn names(files: &[crate::metadata::FileMeta]) -> std::collections::BTreeSet<String> {
    files.iter().map(|file| file.name.clone()).collect()
}

#[test]
fn should_retain_input_authority_when_live_output_receipt_is_delivered_after_deadline(
) -> MidgeResult<()> {
    // Arrange: the actual output worker finishes within the original budget.
    // Act: defer only delivery of its genuine success until original expiry.
    let evidence = delayed_success_case(CompactionPublishPhase::OutputDurable)?;

    // Assert: no manifest replacement or GC; exact completed output/input data.
    assert_common_queued_evidence(&evidence);
    assert!(evidence.has_intent && evidence.retained_inputs.iter().all(|retained| *retained));
    let expected = evidence.inputs.into_iter().collect();
    assert_eq!(evidence.local_files, expected);
    assert_eq!(names(&evidence.committed?.files), expected);
    assert_one_exact_fixture_row(evidence.rows);
    Ok(())
}

#[test]
fn should_retain_remote_inputs_when_live_manifest_receipt_is_delivered_after_deadline(
) -> MidgeResult<()> {
    // Arrange: real output and manifest phases genuinely complete while live.
    // Act: defer only the actual successful manifest receipt across expiry.
    let evidence = delayed_success_case(CompactionPublishPhase::ManifestPublished)?;

    // Assert: real committed outputs cover the row; no new GC/final provider task.
    assert_common_queued_evidence(&evidence);
    assert!(evidence.has_intent && evidence.retained_inputs.iter().all(|retained| *retained));
    let expected = evidence.outputs.into_iter().collect();
    assert_eq!(evidence.local_files, expected);
    assert_eq!(names(&evidence.committed?.files), expected);
    assert_one_exact_fixture_row(evidence.rows);
    Ok(())
}

#[test]
fn should_settle_completed_clear_when_actual_final_receipt_is_delivered_after_deadline(
) -> MidgeResult<()> {
    // Arrange: all three actual phases succeed and GC is accepted while live.
    // Act: delayed final receipt and explicit replay only passively settle owner.
    let evidence = delayed_success_case(CompactionPublishPhase::IntentCleared)?;

    // Assert: caller expires, exact published row survives, owner settles once.
    assert_common_queued_evidence(&evidence);
    assert!(!evidence.has_intent && evidence.retired_inputs.iter().all(|retired| *retired));
    assert_eq!(evidence.reservations, 0);
    assert_eq!(evidence.staging_reserved, 0);
    let expected = evidence.outputs.into_iter().collect();
    assert_eq!(evidence.local_files, expected);
    assert_eq!(names(&evidence.committed?.files), expected);
    assert_one_exact_fixture_row(evidence.rows);
    Ok(())
}

fn final_clear_timeout_case() -> MidgeResult<(QueuedEvidence, Vec<ReadEvidence>, PhaseEvidence)> {
    let mut fixture = BudgetFixture::new()?;
    let (started, response) = fixture.start_manual();
    // Only the final genuine read should consume the original clock. Keeping
    // prior phases unheld leaves room for the coordinator's durable GC/clear.
    fixture.complete_actual_compute();
    let first = fixture.complete_actual_phase(None);
    assert!(first.error.is_none() && first.completed_at < started + CALLER_BUDGET);
    let final_budget_bound = OperationDeadline::from_start(started, CALLER_BUDGET).remaining();
    let second = fixture.complete_actual_phase(Some(ReadHold {
        phase: CompactionPublishPhase::IntentCleared,
        until: started + CALLER_BUDGET + Duration::from_millis(500),
    }));
    assert!(second.error.is_none() && second.completed_at < started + CALLER_BUDGET);
    assert!(
        fixture.el.compaction_publish_actor.is_inflight(),
        "the genuine final worker must be submitted before expiry"
    );
    let completion = fixture
        .el
        .compaction_publish_result_rx
        .recv_timeout(FIXTURE_WAIT)
        .expect("actual final mirror completion");
    let phase = PhaseEvidence {
        phase: completion.phase,
        completed_at: Instant::now(),
        error: completion.result.as_ref().err().map(MidgeError::replay),
    };
    // Explicit transport replay preserves the actual observed error variant.
    let replay = CompactionPublishCompletion {
        token: completion.token.clone(),
        phase: completion.phase,
        result: completion
            .result
            .as_ref()
            .copied()
            .map_err(MidgeError::replay),
    };
    let uploads_before = fixture.backend.inner.get_uploads();
    CompactionCoordinator::handle_publication_completion(&mut fixture.el, completion);
    CompactionCoordinator::handle_publication_completion(&mut fixture.el, replay);
    let reads = fixture.backend.reads.lock().unwrap().clone();
    assert_eq!(reads.len(), 1);
    assert!(final_budget_bound < CALLER_BUDGET);
    assert!(reads[0].forwarded_at >= started + CALLER_BUDGET);
    assert!(reads[0]
        .provider_budget
        .is_some_and(|budget| !budget.is_zero() && budget <= final_budget_bound));
    let evidence = capture_queued_evidence(&mut fixture, &response, uploads_before)?;
    Ok((evidence, reads, phase))
}

#[test]
fn should_keep_typed_timeout_when_actual_final_clear_mirror_exhausts_original_budget(
) -> MidgeResult<()> {
    // Arrange: real output/manifest phases leave less than the ordinary I/O cap.
    // Act: actual final read returns late; replay only that observed error receipt.
    let (evidence, reads, phase) = crate::failpoints::with_read_gate(final_clear_timeout_case)?;

    // Assert: safe settled authority and exact row precede error classification.
    assert_common_queued_evidence(&evidence);
    assert!(
        reads.len() == 1
            && reads.iter().all(|read| read.genuine_success
                && read.forwarded_at.duration_since(read.delegated_at) < PROVIDER_CAP)
    );
    assert_eq!(reads[0].phase, CompactionPublishPhase::IntentCleared);
    assert!(!evidence.has_intent && evidence.retired_inputs.iter().all(|retired| *retired));
    assert_eq!(evidence.reservations, 0);
    assert_eq!(evidence.staging_reserved, 0);
    let expected = evidence.outputs.iter().cloned().collect();
    assert_eq!(evidence.local_files, expected);
    assert_eq!(names(&evidence.committed?.files), expected);
    assert_one_exact_fixture_row(evidence.rows);
    assert_eq!(phase.phase, CompactionPublishPhase::IntentCleared);
    assert!(
        matches!(phase.error, Some(MidgeError::Timeout(_))),
        "actual final phase: {phase:?}"
    );
    Ok(())
}
