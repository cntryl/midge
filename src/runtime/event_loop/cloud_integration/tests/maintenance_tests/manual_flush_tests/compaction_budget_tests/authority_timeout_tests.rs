//! A real metadata CAS can commit before its successful response arrives late.
//!
//! This uses genuine seeded SST rows and the conditional mock provider. It
//! proves accepted publication ownership and same-path metadata/intent
//! recovery, not public transaction acknowledgements or native cancellation.

use super::*;
use std::collections::BTreeSet;

struct RetainedEvidence {
    local_files: BTreeSet<String>,
    has_intent: bool,
    durable_intents: MidgeResult<Vec<crate::runtime::IntentLogEntry>>,
    no_final_phase: bool,
    rows: MidgeResult<Vec<(Bytes, KeyState)>>,
    cas: Vec<CasEvidence>,
}

struct RecoveryInputs {
    db_path: PathBuf,
    cloud: Arc<CloudStorage>,
    authority: Arc<dyn crate::lease::LeaderStore>,
    remote: Arc<dyn crate::storage::StorageBackend>,
    inputs: Vec<(String, Vec<u8>)>,
    outputs: Vec<String>,
}

impl RecoveryInputs {
    fn capture(fixture: &BudgetFixture) -> Self {
        Self {
            db_path: fixture.el.state.db_path.clone(),
            cloud: Arc::clone(&fixture.cloud),
            authority: fixture.lease.get_leader_store().expect("actual authority"),
            remote: fixture
                .el
                .cloud_coordinator
                .hybrid_storage
                .as_ref()
                .expect("actual remote SST store")
                .remote_sst_backend(),
            inputs: fixture.inputs.clone(),
            outputs: fixture.outputs.clone(),
        }
    }

    fn recover_same_path(&self) -> MidgeResult<()> {
        crate::runtime::cloud_startup::CloudStartupRecovery::hydrate_cloud_metadata(
            &self.cloud,
            self.authority.as_ref(),
            &self.db_path,
            crate::config::RecoveryPolicy::Strict,
        )?;
        let mut recovered = RuntimeState::try_new_before_cloud_replay(
            self.db_path.clone(),
            crate::config::RecoveryPolicy::Strict,
        )?;
        let recovered_names = manifest_names(&recovered.manifest);
        let before_replay = recovered.intent_log.clone();
        let remote_fs: Arc<dyn crate::io::Fs> =
            Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
                Arc::new(crate::io::RealFs::new(&self.db_path).map_err(FsError::into_midge)?),
                Arc::clone(&self.remote),
                PROVIDER_CAP,
            ));
        recovered.recovery_sst_fs = Some(Arc::clone(&remote_fs));
        recovered.replay_intent_log()?;
        let factory = crate::sst::FsSstFactoryIo::new(remote_fs, 4096);
        let mut rows = Vec::new();
        for name in &self.outputs {
            rows.extend(
                factory
                    .open(&Path::new("sst").join(name))?
                    .scan_range_raw_state(None, None)?,
            );
        }

        assert_eq!(recovered_names, self.outputs.iter().cloned().collect());
        assert_matching_intent(&before_replay, &self.inputs, &self.outputs);
        assert!(recovered.intent_log.is_empty());
        assert!(
            crate::runtime::IntentPersistence::load_with_fs_and_policy_typed(
                &recovered.fs,
                crate::config::RecoveryPolicy::Strict,
            )?
            .is_empty()
        );
        assert_one_exact_fixture_row(Ok(rows));
        for (name, bytes) in &self.inputs {
            assert_eq!(
                std::fs::read(self.db_path.join("cloud_store/sst").join(name))?,
                *bytes,
                "same-path local intent repair does not remove remote inputs"
            );
        }
        Ok(())
    }
}

fn manifest_names(manifest: &crate::metadata::Manifest) -> BTreeSet<String> {
    manifest
        .files
        .iter()
        .map(|file| file.name.clone())
        .collect()
}

fn begin_held_manifest_phase(
    fixture: &mut BudgetFixture,
    started: Instant,
) -> MidgeResult<PhaseEvidence> {
    let completion = fixture
        .el
        .compaction_publish_result_rx
        .recv_timeout(FIXTURE_WAIT)
        .expect("genuine OutputDurable completion");
    let evidence = PhaseEvidence {
        phase: completion.phase,
        completed_at: Instant::now(),
        error: completion.result.as_ref().err().map(MidgeError::replay),
    };
    if evidence.phase != CompactionPublishPhase::OutputDurable
        || evidence.error.is_some()
        || evidence.completed_at >= started + CALLER_BUDGET
    {
        fixture.stop()?;
        return Err(MidgeError::Internal(format!(
            "invalid held-CAS prerequisite: {evidence:?}"
        )));
    }
    fixture
        .backend
        .arm_cas(started + CALLER_BUDGET + Duration::from_millis(500));
    CompactionCoordinator::handle_publication_completion(&mut fixture.el, completion);
    Ok(evidence)
}

fn capture_retained_evidence(fixture: &BudgetFixture) -> RetainedEvidence {
    let names = fixture
        .inputs
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    RetainedEvidence {
        local_files: manifest_names(&fixture.el.state.manifest),
        has_intent: fixture
            .el
            .state
            .has_compaction_publication_intent(&names, &fixture.outputs),
        durable_intents: crate::runtime::IntentPersistence::load_with_fs_and_policy_typed(
            &fixture.el.state.fs,
            crate::config::RecoveryPolicy::Strict,
        ),
        no_final_phase: !fixture.el.compaction_publish_actor.is_inflight()
            && fixture.el.compaction_publish_result_rx.is_empty()
            && fixture.el.compaction_publication.get().is_none(),
        rows: fixture.exact_output_rows(),
        cas: fixture.backend.cas_writes.lock().unwrap().clone(),
    }
}

fn assert_retained_remote_bytes(
    recovery: &RecoveryInputs,
    outputs: &[(String, Vec<u8>)],
) -> MidgeResult<()> {
    for (name, expected) in recovery.inputs.iter().chain(outputs) {
        assert_eq!(
            std::fs::read(recovery.db_path.join("cloud_store/sst").join(name))?,
            *expected,
            "both exact remote generations survive owned worker joins"
        );
    }
    Ok(())
}

fn assert_matching_intent(
    intents: &[crate::runtime::IntentLogEntry],
    inputs: &[(String, Vec<u8>)],
    outputs: &[String],
) {
    let input_names = inputs
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    assert!(intents.iter().any(|intent| matches!(
        intent,
        crate::runtime::IntentLogEntry::CompactionPublish {
            phase: crate::runtime::PublicationPhase::ManifestPublished,
            cf_id: 0,
            removed,
            added,
        } if removed.iter().cloned().collect::<BTreeSet<_>>() == input_names
            && added.iter().map(|file| file.name.clone()).collect::<BTreeSet<_>>()
                == outputs.iter().cloned().collect()
    )));
}

fn descriptor_field<'a>(bytes: &'a [u8], prefix: &str) -> &'a str {
    std::str::from_utf8(bytes)
        .expect("actual V2 lease descriptor UTF-8")
        .lines()
        .find_map(|line| line.strip_prefix(prefix))
        .expect("actual lease descriptor field")
}

fn descriptor_generation(bytes: &[u8]) -> crate::lease::CloudMetadataGeneration {
    serde_json::from_str::<Option<crate::lease::CloudMetadataGeneration>>(descriptor_field(
        bytes,
        "metadata: ",
    ))
    .expect("actual committed descriptor JSON")
    .expect("actual committed metadata pointer")
}

fn assert_committed_cas(
    writes: &[CasEvidence],
    started: Instant,
    recovery: &RecoveryInputs,
) -> MidgeResult<()> {
    assert_eq!(
        writes.len(),
        1,
        "one actual held conditional CAS: {writes:?}"
    );
    let write = &writes[0];
    assert_eq!(
        write.key,
        crate::cloud_layout::CloudObjectLayout::LEASE_OBJECT_KEY
    );
    assert_ne!(write.condition, "");
    assert_eq!(write.condition, write.before_etag);
    assert_ne!(write.before_etag, write.after_etag);
    assert_ne!(write.before, write.after);
    assert!(write.committed_at < started + CALLER_BUDGET);
    assert!(write.forwarded_at >= started + CALLER_BUDGET + Duration::from_millis(500));
    for prefix in [
        "fencing_epoch: ",
        "holder: ",
        "owner: ",
        "acquired: ",
        "expires: ",
    ] {
        assert_eq!(
            descriptor_field(&write.before, prefix),
            descriptor_field(&write.after, prefix)
        );
    }
    let before = descriptor_generation(&write.before);
    let after = descriptor_generation(&write.after);
    assert_ne!(before, after, "the real CAS switched the metadata pointer");
    let actual = recovery
        .authority
        .read_committed_metadata(PROVIDER_CAP)
        .map_err(|error| error.into_validation_error("late CAS readback"))?;
    assert_eq!(
        actual,
        crate::lease::CloudMetadataHead::Committed(after.clone())
    );
    let scope = crate::common::DeadlineScope::new(OperationDeadline::from_budget(FIXTURE_WAIT));
    for generation in [&before, &after] {
        crate::runtime::cloud_startup::CloudStartupRecovery::read_committed_cloud_metadata_within(
            &recovery.cloud,
            generation,
            &scope,
        )?;
    }
    Ok(())
}

#[test]
fn should_retain_both_generations_when_committed_metadata_cas_response_exceeds_manual_deadline(
) -> MidgeResult<()> {
    crate::failpoints::with_read_gate(|| {
        // Arrange: all setup precedes the immutable actual manual-caller origin.
        let mut fixture = BudgetFixture::new()?;
        let (started, response) = fixture.start_manual();
        fixture.complete_actual_compute();
        let output_bytes = fixture
            .outputs
            .iter()
            .map(|name| {
                Ok((
                    name.clone(),
                    std::fs::read(remote_sst_path_for_test(&fixture.el, name))?,
                ))
            })
            .collect::<MidgeResult<Vec<_>>>()?;

        // Act: arm only before the real ManifestPublished task is submitted.
        let first = begin_held_manifest_phase(&mut fixture, started)?;
        let second = fixture.complete_actual_phase(None);
        let outcome = fixture.drive_manual_response(&response);
        let evidence = capture_retained_evidence(&fixture);
        let committed = fixture.committed_manifest();
        let recovery = RecoveryInputs::capture(&fixture);
        fixture.stop()?;
        drop(fixture);

        // Assert: genuine committed authority, retained bytes, joined ownership.
        assert_eq!(first.phase, CompactionPublishPhase::OutputDurable);
        assert!(first.error.is_none() && first.completed_at < started + CALLER_BUDGET);
        assert_eq!(second.phase, CompactionPublishPhase::ManifestPublished);
        assert!(
            matches!(second.error, Some(MidgeError::Timeout(_))),
            "actual phase: {second:?}"
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
        assert!(
            evidence.no_final_phase,
            "no fresh IntentCleared phase after expiry"
        );
        assert_retained_remote_bytes(&recovery, &output_bytes)?;
        assert!(evidence.has_intent);
        assert_matching_intent(
            &evidence.durable_intents?,
            &recovery.inputs,
            &recovery.outputs,
        );
        assert_one_exact_fixture_row(evidence.rows);
        let expected = recovery.outputs.iter().cloned().collect::<BTreeSet<_>>();
        assert_eq!(evidence.local_files, expected);
        assert_eq!(manifest_names(&committed?), expected);
        assert_committed_cas(&evidence.cas, started, &recovery)?;
        recovery.recover_same_path()?;
        Ok(())
    })
}
