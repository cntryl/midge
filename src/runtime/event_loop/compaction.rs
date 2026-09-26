use super::{EventLoop, HandleOutcome};
use crate::runtime::actors::compaction::publication::{
    CompactionPublicationToken, CompactionPublishCompletion, CompactionPublishPhase,
    CompactionPublishTask,
};
use crate::runtime::actors::compaction::PreparedCompactionOutput;
use crate::runtime::state::CompactionWait;
#[cfg(test)]
use crate::runtime::CompactionPlan;
use crate::runtime::RuntimeResponse;

#[cfg(test)]
mod tests;

pub(super) struct CompactionCoordinator;

pub(super) struct CompactionCompleteRequest {
    pub request_id: u64,
    pub input_ssts: Vec<String>,
    pub output_ssts: Vec<String>,
    pub cf_id: crate::types::ColumnFamilyId,
    pub target_level: u32,
    pub succeeded: bool,
}

/// Event-loop-owned state for one publication handoff. The worker receives
/// immutable copies and returns only its completion; manifest state remains
/// serialized here.
#[derive(Clone)]
pub(crate) struct PendingCompactionPublication {
    token: CompactionPublicationToken,
    outputs: Vec<PreparedCompactionOutput>,
    added: Vec<crate::runtime::FileMeta>,
    /// Outputs of an earlier intent for the same inputs. They are deleted only
    /// after the worker has mirrored the replacement intent.
    superseded_outputs: Vec<String>,
    expected_phase: CompactionPublishPhase,
}

impl CompactionCoordinator {
    #[cfg(test)]
    pub(super) fn check(event_loop: &mut EventLoop, request_id: u64) -> HandleOutcome {
        match event_loop.schedule_one_background_compaction_if_needed("CheckCompaction") {
            Ok(_) => event_loop.respond(request_id, RuntimeResponse::Ok { request_id }),
            Err(error) => {
                event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            }
        }
        HandleOutcome::Continue
    }

    #[cfg(test)]
    pub(super) fn run(
        event_loop: &mut EventLoop,
        request_id: u64,
        plan: CompactionPlan,
    ) -> HandleOutcome {
        let cplan = crate::compaction::CompactionPlan {
            source_files: plan.input_files.clone(),
            target_files: Vec::new(),
            input_files: plan.input_files,
            source_level: plan.source_level,
            target_level: plan.target_level,
            cf_id: plan.cf_id,
            output_seq: 0,
            target_sst_size: crate::compaction::DEFAULT_TARGET_SST_SIZE,
            compaction_memory_limit: crate::compaction::DEFAULT_COMPACTION_MEMORY_LIMIT,
            snapshot_horizon: None,
            point_tombstone_gc_eligible: false,
            range_tombstone_gc_eligible: false,
        };

        let schedule_res = event_loop.launch_compaction(cplan);
        let resp = match schedule_res {
            Ok(()) => RuntimeResponse::Ok { request_id },
            Err(error) => RuntimeResponse::Error { request_id, error },
        };

        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }

    pub(super) fn compact_all(event_loop: &mut EventLoop, request_id: u64) -> HandleOutcome {
        if let Err(error) = Self::validate_manual_compaction_request(event_loop) {
            event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            return HandleOutcome::Continue;
        }

        if event_loop.cloud_maintenance_enabled() {
            event_loop
                .state
                .pending_compaction_waits
                .insert(request_id, CompactionWait::CompactAll);
            event_loop.schedule_cloud_maintenance();
            return HandleOutcome::Continue;
        }

        if event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst)
            > 0
        {
            event_loop
                .state
                .pending_compaction_waits
                .insert(request_id, CompactionWait::CompactAll);
            return HandleOutcome::Continue;
        }

        let mut scheduled = 0usize;
        loop {
            let plan = match event_loop
                .compaction_actor
                .check_manual_compaction(&event_loop.state)
            {
                Ok(Some(plan)) => plan,
                Ok(None) => break,
                Err(error) => {
                    event_loop.state.mark_persistence_anomaly();
                    event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
                    return HandleOutcome::Continue;
                }
            };
            match event_loop.launch_compaction(plan) {
                Ok(()) => scheduled += 1,
                Err(error) => {
                    event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
                    return HandleOutcome::Continue;
                }
            }
        }

        if scheduled == 0 {
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
            return HandleOutcome::Continue;
        }

        event_loop
            .state
            .pending_compaction_waits
            .insert(request_id, CompactionWait::CompactAll);
        HandleOutcome::Continue
    }

    fn validate_manual_compaction_request(
        event_loop: &EventLoop,
    ) -> crate::common::MidgeResult<()> {
        if event_loop.compaction_publication_degraded {
            return Err(crate::common::MidgeError::Fenced(
                "compaction publication is unsettled; refusing another compaction until recovery"
                    .into(),
            ));
        }
        if event_loop.fencing.ddl_authority_ambiguous {
            return Err(crate::common::MidgeError::Fenced(
                "DDL authority is ambiguous; refusing compaction until reconciliation".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn complete(
        event_loop: &mut EventLoop,
        request: CompactionCompleteRequest,
    ) -> HandleOutcome {
        let CompactionCompleteRequest {
            request_id,
            input_ssts,
            output_ssts,
            cf_id,
            target_level,
            succeeded,
        } = request;
        let worker_error = event_loop.compaction_actor.take_worker_error();

        if succeeded {
            event_loop.last_compaction_publication_error = None;
            event_loop.compaction_actor.join_completed_worker();
            if let Err(error) = Self::start_publication(
                event_loop,
                request_id,
                &input_ssts,
                &output_ssts,
                cf_id,
                target_level,
            ) {
                if event_loop.compaction_publication.is_some() {
                    Self::finish_failed_publication(event_loop, &error);
                } else {
                    Self::finish_failed_start(
                        event_loop,
                        request_id,
                        &input_ssts,
                        &output_ssts,
                        &error,
                    );
                }
            }
        } else {
            event_loop.state.diagnostics.record(|m| {
                m.record_compaction_failure();
            });
            tracing::warn!(
                input_count = input_ssts.len(),
                output_count = output_ssts.len(),
                "compaction worker failed or aborted; leaving manifest unchanged"
            );
            let error = worker_error.unwrap_or_else(|| {
                crate::common::MidgeError::Internal(
                    "compaction worker failed without an error".to_string(),
                )
            });
            // Partitions uploaded before the failure are named by a reserved,
            // never-reused generation and referenced by no manifest or
            // intent. Reclaim them now, or every retry of a deterministically
            // failing plan leaks another set of remote objects.
            let orphaned = event_loop.compaction_actor.take_prepared_output_names();
            if !orphaned.is_empty() {
                let hybrid_storage = event_loop.hybrid_storage.clone();
                event_loop
                    .gc_actor
                    .delete_ssts(&mut event_loop.state, &orphaned, hybrid_storage);
            }
            let reservation = event_loop.compaction_actor.handle_complete(
                &mut event_loop.state,
                &input_ssts,
                &output_ssts,
            );
            if let (Some(hybrid), Some(token)) = (&event_loop.hybrid_storage, reservation) {
                event_loop
                    .compaction_actor
                    .settle_compaction_error_reservation(
                        &event_loop.state,
                        hybrid.as_ref(),
                        token,
                        Some(&error),
                    );
            }
            let completion_error = error.replay();
            event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            Self::complete_pending_waits(event_loop, false, Some(&completion_error));
            event_loop.drain_auto_flush_memtables();
            event_loop.wake_write_stall_waiters();
        }
        Self::drain_inline_publish_worker(event_loop);
        HandleOutcome::Continue
    }

    fn start_publication(
        event_loop: &mut EventLoop,
        request_id: u64,
        input_ssts: &[String],
        output_ssts: &[String],
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
    ) -> crate::common::MidgeResult<()> {
        // Outputs rejected before any intent names them are unreferenced and
        // safe to delete. Once intent persistence is attempted, retain them:
        // startup reconciles the residue against whatever reached disk.
        let (output_generation, outputs, added) = match Self::validate_publication_start(
            event_loop,
            input_ssts,
            output_ssts,
            cf_id,
            target_level,
        ) {
            Ok(validated) => validated,
            Err(error) => {
                event_loop.gc_actor.delete_ssts(
                    &mut event_loop.state,
                    output_ssts,
                    event_loop.hybrid_storage.clone(),
                );
                return Err(error);
            }
        };
        let mut canonical_inputs = input_ssts.to_vec();
        let mut canonical_outputs = output_ssts.to_vec();
        canonical_inputs.sort_unstable();
        canonical_outputs.sort_unstable();
        let token = CompactionPublicationToken {
            request_id,
            writer_epoch: event_loop.fencing.writer_epoch,
            cf_id,
            target_level,
            output_generation,
            input_ssts: canonical_inputs,
            output_ssts: canonical_outputs,
        };
        let publication_owner = Self::publication_owner(&token);
        if !event_loop
            .publication_gate
            .try_acquire(publication_owner.clone())
        {
            return Err(crate::common::MidgeError::Busy(
                "manifest publication is already in progress".to_string(),
            ));
        }
        let superseded_outputs = match event_loop.state.record_compaction_publication_intent(
            cf_id,
            token.input_ssts.clone(),
            added.clone(),
        ) {
            Ok(outputs) => outputs,
            Err(error) => {
                event_loop.publication_gate.release(&publication_owner);
                return Err(error);
            }
        };
        event_loop.compaction_publication = Some(PendingCompactionPublication {
            token,
            outputs,
            added,
            superseded_outputs,
            expected_phase: CompactionPublishPhase::OutputDurable,
        });
        Self::submit_publication_phase(event_loop, CompactionPublishPhase::OutputDurable)
    }

    fn publication_owner(
        token: &CompactionPublicationToken,
    ) -> crate::runtime::event_loop::coordination::ManifestPublicationOwner {
        crate::runtime::event_loop::coordination::ManifestPublicationOwner::Compaction {
            request_id: token.request_id,
            output_generation: token.output_generation,
        }
    }

    fn validate_publication_start(
        event_loop: &EventLoop,
        input_ssts: &[String],
        output_ssts: &[String],
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
    ) -> crate::common::MidgeResult<(
        u64,
        Vec<PreparedCompactionOutput>,
        Vec<crate::runtime::FileMeta>,
    )> {
        if event_loop.state.get_cf(cf_id).is_none() {
            return Err(crate::common::MidgeError::Corruption(format!(
                "compaction completed for inactive column family {cf_id}; retained inputs and rejected outputs"
            )));
        }
        let output_generation = Self::validate_output_names(cf_id, target_level, output_ssts)?;
        Self::validate_captured_target_span(event_loop, input_ssts, cf_id, target_level)?;
        let prepared =
            event_loop
                .compaction_actor
                .prepared_outputs_exact(output_ssts, cf_id, target_level);
        #[cfg(test)]
        let prepared = prepared.or_else(|_| {
            event_loop
                .compaction_actor
                .prepare_outputs_from_local_files_for_test(
                    output_ssts,
                    cf_id,
                    target_level,
                    &event_loop.state.sst_dir,
                )
        });
        let outputs = prepared?;
        let added =
            Self::validate_prepared_output_metadata(cf_id, target_level, output_ssts, &outputs)?;
        Ok((output_generation, outputs, added))
    }

    fn validate_output_names(
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
        output_ssts: &[String],
    ) -> Result<u64, crate::common::MidgeError> {
        if output_ssts.windows(2).any(|names| names[0] >= names[1]) {
            return Err(crate::common::MidgeError::Corruption(
                "compaction output set must be sorted and uniquely named".to_string(),
            ));
        }

        let mut generation = None;
        for (expected_partition, name) in output_ssts.iter().enumerate() {
            let (name_cf, name_level, name_generation, name_partition) =
                crate::cloud_layout::parse_compaction_file_name(name).ok_or_else(|| {
                    crate::common::MidgeError::Corruption(format!(
                        "compaction output has non-canonical partition name: {name}"
                    ))
                })?;
            let expected_partition = u32::try_from(expected_partition).map_err(|_| {
                crate::common::MidgeError::ResourceLimit(
                    "compaction output count exceeds partition identity capacity".to_string(),
                )
            })?;
            if name_cf != cf_id
                || name_level != target_level
                || name_partition != expected_partition
                || generation.is_some_and(|expected| expected != name_generation)
            {
                return Err(crate::common::MidgeError::Corruption(format!(
                    "compaction output does not belong to expected cf={cf_id} level={target_level} generation/partition set: {name}"
                )));
            }
            generation.get_or_insert(name_generation);
        }
        Ok(generation.unwrap_or_default())
    }

    fn validate_prepared_output_metadata(
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
        output_ssts: &[String],
        outputs: &[PreparedCompactionOutput],
    ) -> Result<Vec<crate::runtime::FileMeta>, crate::common::MidgeError> {
        if outputs.len() != output_ssts.len() {
            return Err(crate::common::MidgeError::Corruption(
                "compaction publication output metadata count changed".to_string(),
            ));
        }
        let metadata = outputs
            .iter()
            .zip(output_ssts)
            .map(|(output, name)| {
                if output.metadata.name != *name
                    || output.metadata.cf_id != cf_id
                    || output.metadata.level != target_level
                    || output.metadata.content_crc32c.is_none()
                    || !output.metadata.key_bounds_complete
                {
                    return Err(crate::common::MidgeError::Corruption(format!(
                        "prepared compaction output metadata is incomplete or mismatched for {name}"
                    )));
                }
                Ok(output.metadata.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        for pair in metadata.windows(2) {
            if let (Some(left_largest), Some(right_smallest)) =
                (&pair[0].largest_key, &pair[1].smallest_key)
            {
                if left_largest > right_smallest {
                    return Err(crate::common::MidgeError::Corruption(format!(
                        "compaction output key ranges overlap out of order: {} then {}",
                        pair[0].name, pair[1].name
                    )));
                }
            }
        }
        Ok(metadata)
    }

    fn validate_captured_target_span(
        event_loop: &EventLoop,
        input_ssts: &[String],
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
    ) -> Result<(), crate::common::MidgeError> {
        let selected: std::collections::HashSet<_> =
            input_ssts.iter().map(String::as_str).collect();
        let selected_files = event_loop
            .state
            .manifest
            .files
            .iter()
            .filter(|file| selected.contains(file.name.as_str()))
            .collect::<Vec<_>>();
        if selected_files.len() != selected.len() {
            return Err(crate::common::MidgeError::Fenced(
                "compaction input authority changed before publication".to_string(),
            ));
        }
        let min_key = selected_files
            .iter()
            .filter_map(|file| file.smallest_key.as_ref())
            .min()
            .ok_or_else(|| {
                crate::common::MidgeError::Corruption(
                    "compaction inputs have no smallest-key bound".to_string(),
                )
            })?;
        let max_key = selected_files
            .iter()
            .filter_map(|file| file.largest_key.as_ref())
            .max()
            .ok_or_else(|| {
                crate::common::MidgeError::Corruption(
                    "compaction inputs have no largest-key bound".to_string(),
                )
            })?;
        let mut captured_target = selected_files
            .iter()
            .filter(|file| file.cf_id == cf_id && file.level == target_level)
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>();
        captured_target.sort_unstable();
        let mut live_target = event_loop
            .state
            .manifest
            .files
            .iter()
            .filter(|file| {
                file.cf_id == cf_id
                    && file.level == target_level
                    && file
                        .smallest_key
                        .as_ref()
                        .zip(file.largest_key.as_ref())
                        .is_some_and(|(smallest, largest)| {
                            smallest.as_slice() <= max_key.as_slice()
                                && largest.as_slice() >= min_key.as_slice()
                        })
            })
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>();
        live_target.sort_unstable();
        if live_target != captured_target {
            return Err(crate::common::MidgeError::Fenced(
                "compaction target-level span changed before publication".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn handle_publication_completion(
        event_loop: &mut EventLoop,
        completion: CompactionPublishCompletion,
    ) {
        event_loop.compaction_publish_actor.finish_task();
        let Some(pending) = event_loop.compaction_publication.clone() else {
            tracing::warn!(?completion.phase, "ignored stale compaction publication completion");
            return;
        };
        if pending.token != completion.token || pending.expected_phase != completion.phase {
            Self::finish_failed_publication(
                event_loop,
                &crate::common::MidgeError::Fenced(
                    "compaction publication completion did not match the active token".to_string(),
                ),
            );
            return;
        }
        if let Err(error) = completion.result {
            Self::finish_failed_publication(event_loop, &error);
            return;
        }
        if event_loop.fencing.writer_epoch != pending.token.writer_epoch {
            Self::finish_failed_publication(
                event_loop,
                &crate::common::MidgeError::Fenced(
                    "compaction publication completed under a stale writer epoch".to_string(),
                ),
            );
            return;
        }
        if let Err(error) = event_loop.check_lease_health() {
            Self::finish_failed_publication(event_loop, &error);
            return;
        }

        let result = match completion.phase {
            CompactionPublishPhase::OutputDurable => {
                Self::install_manifest_publication(event_loop, &pending)
            }
            CompactionPublishPhase::ManifestPublished => {
                Self::begin_intent_clear_publication(event_loop, &pending)
            }
            CompactionPublishPhase::IntentCleared => {
                Self::finish_successful_publication(event_loop, &pending);
                Ok(())
            }
        };
        if let Err(error) = result {
            Self::finish_failed_publication(event_loop, &error);
        }
    }

    fn install_manifest_publication(
        event_loop: &mut EventLoop,
        pending: &PendingCompactionPublication,
    ) -> crate::common::MidgeResult<()> {
        if event_loop.state.get_cf(pending.token.cf_id).is_none() {
            return Err(crate::common::MidgeError::Fenced(
                "compaction column family was removed while publication was pending".to_string(),
            ));
        }
        if !pending.superseded_outputs.is_empty() {
            event_loop.gc_actor.delete_ssts(
                &mut event_loop.state,
                &pending.superseded_outputs,
                event_loop.hybrid_storage.clone(),
            );
        }
        Self::validate_captured_target_span(
            event_loop,
            &pending.token.input_ssts,
            pending.token.cf_id,
            pending.token.target_level,
        )?;
        event_loop.manifest_actor.compaction_complete(
            &mut event_loop.state,
            &pending.token.input_ssts,
            &pending.added,
        )?;
        event_loop.invalidate_sst_read_views();
        crate::failpoints::fail_point!(
            "midge::compaction::inject_failure_after_manifest_batch",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: compaction failed after durable manifest batch".to_string()
            ))
        );
        event_loop.state.transition_compaction_publication_intent(
            &pending.token.input_ssts,
            &pending.token.output_ssts,
            crate::runtime::PublicationPhase::ManifestPublished,
        )?;
        crate::failpoints::fail_point!("slice6::after_compaction_update_before_manifest_persist");
        crate::runtime::actors::ManifestActor::persist(&mut event_loop.state)?;
        Self::submit_publication_phase(event_loop, CompactionPublishPhase::ManifestPublished)
    }

    fn begin_intent_clear_publication(
        event_loop: &mut EventLoop,
        pending: &PendingCompactionPublication,
    ) -> crate::common::MidgeResult<()> {
        crate::failpoints::fail_point!("slice6::after_manifest_persist_before_sst_gc");
        event_loop.publish_snapshot();
        let output_sizes =
            Self::resident_output_sizes(&event_loop.state.sst_dir, &pending.token.output_ssts)?;
        let reservation = event_loop.compaction_actor.finish_publication(
            &mut event_loop.state,
            &pending.token.input_ssts,
            &pending.token.output_ssts,
        );
        // From here the compaction is settled; any later failure only leaves
        // the cleared intent unmirrored.
        if let Some(active) = event_loop.compaction_publication.as_mut() {
            active.expected_phase = CompactionPublishPhase::IntentCleared;
        }
        let hybrid_storage = event_loop.hybrid_storage.clone();
        if let (Some(hybrid), Some(token)) = (&hybrid_storage, reservation) {
            hybrid.compaction_completed_with_token(token, &output_sizes);
        }
        if event_loop.check_lease_health().is_ok() {
            event_loop.gc_actor.delete_ssts(
                &mut event_loop.state,
                &pending.token.input_ssts,
                hybrid_storage,
            );
        } else {
            event_loop.state.mark_persistence_anomaly();
            tracing::error!(
                retained_inputs = pending.token.input_ssts.len(),
                "writer lease is unhealthy before compaction input GC; retaining inputs"
            );
        }
        crate::failpoints::fail_point!("midge::compaction::after_input_sst_gc");
        event_loop.state.clear_compaction_publication_intent(
            &pending.token.input_ssts,
            &pending.token.output_ssts,
        )?;
        Self::submit_publication_phase(event_loop, CompactionPublishPhase::IntentCleared)
    }

    fn submit_publication_phase(
        event_loop: &mut EventLoop,
        phase: CompactionPublishPhase,
    ) -> crate::common::MidgeResult<()> {
        let pending = event_loop.compaction_publication.as_mut().ok_or_else(|| {
            crate::common::MidgeError::Internal(
                "compaction publication state disappeared before worker submission".to_string(),
            )
        })?;
        pending.expected_phase = phase;
        let task = CompactionPublishTask {
            token: pending.token.clone(),
            phase,
            outputs: if phase == CompactionPublishPhase::OutputDurable {
                pending.outputs.clone()
            } else {
                Vec::new()
            },
            sst_dir: event_loop.state.sst_dir.clone(),
            fs: std::sync::Arc::clone(&event_loop.state.fs),
            hybrid_storage: event_loop.hybrid_storage.clone(),
            cloud_metadata_storage: event_loop.cloud_metadata_storage.clone(),
            metadata_publication_lock: event_loop.metadata_publication_lock.clone(),
            lease_healthy: event_loop.fencing.lease_healthy.clone(),
            leader_store: event_loop.fencing.leader_store.clone(),
            leader_holder_id: event_loop.fencing.leader_holder_id.clone(),
            metadata_sequence: event_loop.state.manifest.last_persisted_sequence,
            publication_memory_limit: event_loop.compaction_actor.compaction_memory_limit(),
            runtime_response_timeout: event_loop.runtime_response_timeout,
        };
        event_loop.compaction_publish_actor.submit(task)
    }

    fn manifest_authority_switched(
        event_loop: &EventLoop,
        input_ssts: &[String],
        output_ssts: &[String],
    ) -> bool {
        input_ssts
            .iter()
            .all(|name| !event_loop.state.manifest_has_file(name))
            && output_ssts
                .iter()
                .all(|name| event_loop.state.manifest_has_file(name))
    }

    fn settle_incomplete_authoritative_publication(
        event_loop: &mut EventLoop,
        input_ssts: &[String],
        output_ssts: &[String],
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) {
        event_loop.compaction_publication_degraded = true;
        event_loop.publish_snapshot();
        if let (Some(hybrid), Some(token)) = (&event_loop.hybrid_storage, reservation) {
            match Self::resident_output_sizes(&event_loop.state.sst_dir, output_ssts) {
                Ok(output_sizes) => {
                    hybrid.compaction_inputs_retained_with_token(token, &output_sizes);
                }
                Err(error) => {
                    tracing::warn!(%error, ?token, "retaining compaction staging allowance because published output residency cannot be measured");
                }
            }
            tracing::warn!(
                input_count = input_ssts.len(),
                output_count = output_ssts.len(),
                "retaining both compaction generations until cloud publication recovers"
            );
        } else {
            // The local manifest batch is the durable authority. With no
            // remote authority to reconcile, its removed inputs are safe to
            // submit for local GC even if a later phase/checkpoint write
            // failed. The intent remains for idempotent restart recovery.
            event_loop
                .gc_actor
                .delete_ssts(&mut event_loop.state, input_ssts, None);
        }
    }

    fn resident_output_sizes(
        directory: &std::path::Path,
        outputs: &[String],
    ) -> crate::common::MidgeResult<Vec<u64>> {
        outputs
            .iter()
            .map(|name| {
                let path = directory.join(name);
                match std::fs::metadata(&path) {
                    Ok(metadata) if metadata.is_file() => Ok(metadata.len()),
                    Ok(_) => Err(crate::common::MidgeError::Internal(format!(
                        "published compaction output is not a regular file: {}",
                        path.display()
                    ))),
                    // A remotely proved output can already have been evicted.
                    // Other metadata errors cannot establish that disk is free.
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
                    Err(error) => Err(error.into()),
                }
            })
            .collect()
    }

    fn finish_successful_publication(
        event_loop: &mut EventLoop,
        pending: &PendingCompactionPublication,
    ) {
        Self::record_compaction_metrics(event_loop, &pending.token.output_ssts);
        event_loop.evict_published_sst_cache(&pending.token.output_ssts);
        event_loop.publish_snapshot();
        event_loop.respond(
            pending.token.request_id,
            RuntimeResponse::Ok {
                request_id: pending.token.request_id,
            },
        );
        event_loop.compaction_publication = None;
        event_loop
            .publication_gate
            .release(&Self::publication_owner(&pending.token));
        Self::complete_pending_waits(event_loop, true, None);
        event_loop.restore_publication_deferred_message();
        event_loop.schedule_next_flush_worker();
        event_loop.drain_auto_flush_memtables();
        event_loop.wake_write_stall_waiters();
    }

    fn finish_failed_start(
        event_loop: &mut EventLoop,
        request_id: u64,
        input_ssts: &[String],
        output_ssts: &[String],
        error: &crate::common::MidgeError,
    ) {
        // A failed intent append can still have reached the journal. Keep the
        // outputs and refuse further compaction until restart reconciles them.
        if event_loop
            .state
            .has_compaction_publication_intent(input_ssts, output_ssts)
        {
            event_loop.compaction_publication_degraded = true;
        }
        let reservation = event_loop.compaction_actor.finish_publication(
            &mut event_loop.state,
            input_ssts,
            output_ssts,
        );
        if let (Some(hybrid), Some(token)) = (&event_loop.hybrid_storage, reservation) {
            event_loop
                .compaction_actor
                .settle_failed_compaction_reservation(&event_loop.state, hybrid.as_ref(), token);
        }
        let wait_error = error.replay();
        Self::respond_publish_failure(event_loop, request_id, error);
        Self::complete_pending_waits(event_loop, false, Some(&wait_error));
        event_loop.drain_auto_flush_memtables();
        event_loop.wake_write_stall_waiters();
    }

    fn finish_failed_publication(event_loop: &mut EventLoop, error: &crate::common::MidgeError) {
        let Some(pending) = event_loop.compaction_publication.take() else {
            tracing::warn!(%error, "compaction publication failed without pending state");
            return;
        };
        if pending.expected_phase == CompactionPublishPhase::IntentCleared {
            Self::finish_failed_intent_clear(event_loop, &pending, error);
            return;
        }
        let input_ssts = &pending.token.input_ssts;
        let output_ssts = &pending.token.output_ssts;
        let authoritative = Self::manifest_authority_switched(event_loop, input_ssts, output_ssts);
        let reservation = event_loop.compaction_actor.finish_publication(
            &mut event_loop.state,
            input_ssts,
            output_ssts,
        );
        if authoritative {
            Self::settle_incomplete_authoritative_publication(
                event_loop,
                input_ssts,
                output_ssts,
                reservation,
            );
        } else {
            if event_loop
                .state
                .has_compaction_publication_intent(input_ssts, output_ssts)
            {
                event_loop.compaction_publication_degraded = true;
            }
            if let (Some(hybrid), Some(token)) = (&event_loop.hybrid_storage, reservation) {
                event_loop
                    .compaction_actor
                    .settle_failed_compaction_reservation(
                        &event_loop.state,
                        hybrid.as_ref(),
                        token,
                    );
            }
        }
        let wait_error = error.replay();
        Self::respond_publish_failure(event_loop, pending.token.request_id, error);
        event_loop
            .publication_gate
            .release(&Self::publication_owner(&pending.token));
        Self::complete_pending_waits(event_loop, false, Some(&wait_error));
        event_loop.restore_publication_deferred_message();
        event_loop.schedule_next_flush_worker();
        event_loop.drain_auto_flush_memtables();
        event_loop.wake_write_stall_waiters();
    }

    /// The manifest is published, inputs were handed to GC, and the local
    /// intent is cleared; only its mirror failed. Everything is settled except
    /// the remote record, so degrade instead of re-running the failure settle.
    fn finish_failed_intent_clear(
        event_loop: &mut EventLoop,
        pending: &PendingCompactionPublication,
        error: &crate::common::MidgeError,
    ) {
        Self::record_compaction_failure(event_loop);
        tracing::error!(
            ?error,
            "failed to mirror cleared compaction publication intent"
        );
        event_loop.compaction_publication_degraded = true;
        event_loop.publish_snapshot();
        let response_error = crate::common::MidgeError::Internal(format!(
            "failed to mirror cleared compaction publication intent: {error}"
        ));
        let wait_error = response_error.replay();
        event_loop.last_compaction_publication_error = Some(response_error.replay());
        event_loop.respond(
            pending.token.request_id,
            RuntimeResponse::Error {
                request_id: pending.token.request_id,
                error: response_error,
            },
        );
        event_loop
            .publication_gate
            .release(&Self::publication_owner(&pending.token));
        Self::complete_pending_waits(event_loop, false, Some(&wait_error));
        event_loop.restore_publication_deferred_message();
        event_loop.schedule_next_flush_worker();
        event_loop.drain_auto_flush_memtables();
        event_loop.wake_write_stall_waiters();
    }

    /// Focused reservation tests construct an already-published manifest
    /// directly. Keep their terminal-state helper local-only; production uses
    /// the worker result state machine above.
    #[cfg(test)]
    fn finalize_published_compaction(
        event_loop: &mut EventLoop,
        request_id: u64,
        input_ssts: &[String],
        output_ssts: &[String],
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) -> bool {
        event_loop.publish_snapshot();
        let hybrid_storage = event_loop.hybrid_storage.clone();
        if let (Some(hybrid), Some(token)) = (&hybrid_storage, reservation) {
            let output_sizes =
                match Self::resident_output_sizes(&event_loop.state.sst_dir, output_ssts) {
                    Ok(sizes) => sizes,
                    Err(error) => {
                        return Self::respond_publish_failure(event_loop, request_id, &error)
                    }
                };
            hybrid.compaction_completed_with_token(token, &output_sizes);
        }
        if event_loop.check_lease_health().is_ok() {
            event_loop
                .gc_actor
                .delete_ssts(&mut event_loop.state, input_ssts, hybrid_storage);
        } else {
            event_loop.state.mark_persistence_anomaly();
        }
        if let Err(error) = event_loop
            .state
            .clear_compaction_publication_intent(input_ssts, output_ssts)
        {
            return Self::respond_publish_failure(event_loop, request_id, &error);
        }
        Self::record_compaction_metrics(event_loop, output_ssts);
        event_loop.evict_published_sst_cache(output_ssts);
        event_loop.publish_snapshot();
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        true
    }

    fn record_compaction_metrics(event_loop: &mut EventLoop, output_ssts: &[String]) {
        let bytes_rewritten: u64 = event_loop
            .state
            .manifest
            .files
            .iter()
            .filter(|file| output_ssts.contains(&file.name))
            .map(|file| file.size_bytes)
            .sum();
        event_loop
            .state
            .diagnostics
            .record(|m| m.record_compaction(bytes_rewritten));
    }

    fn respond_publish_failure(
        event_loop: &mut EventLoop,
        request_id: u64,
        error: &crate::common::MidgeError,
    ) -> bool {
        Self::record_compaction_failure(event_loop);
        event_loop.last_compaction_publication_error = Some(error.replay());
        tracing::error!(error = ?error, "failed to apply compaction to manifest");
        event_loop.respond(
            request_id,
            RuntimeResponse::Error {
                request_id,
                error: crate::common::MidgeError::Internal(format!(
                    "failed to apply compaction to manifest: {error}"
                )),
            },
        );
        false
    }

    fn complete_pending_waits(
        event_loop: &mut EventLoop,
        allow_emergent_followup: bool,
        completion_error: Option<&crate::common::MidgeError>,
    ) {
        let active = event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst);
        if active != 0 {
            return;
        }

        if let Some(error) = completion_error {
            Self::fail_pending_compaction_waits(event_loop, error);
            return;
        }

        if event_loop.cloud_maintenance_enabled() {
            // CompactAll keeps its obligation until a later fair compaction
            // turn proves that no eligible manual plan remains.
            Self::complete_idle_compaction_waits(event_loop, false);
            return;
        }

        let mut emergent_scheduled = false;
        // Publication-deferred work (notably CF drops) has already waited for
        // this authority switch. Let the run loop restore it before taking the
        // global compaction slot again, otherwise steady compaction debt can
        // starve destructive DDL indefinitely.
        if allow_emergent_followup
            && Self::has_manual_compaction_waiters(event_loop)
            && event_loop.publication_gate.deferred_messages_is_empty()
        {
            loop {
                let plan = match event_loop
                    .compaction_actor
                    .check_manual_compaction(&event_loop.state)
                {
                    Ok(Some(plan)) => plan,
                    Ok(None) => break,
                    Err(error) => {
                        Self::record_compaction_failure(event_loop);
                        Self::fail_pending_compaction_waits(event_loop, &error);
                        return;
                    }
                };
                match event_loop.launch_compaction(plan) {
                    Ok(()) => {
                        emergent_scheduled = true;
                    }
                    Err(error) => {
                        Self::fail_pending_compaction_waits(event_loop, &error);
                        return;
                    }
                }
            }
        }

        let active_now = event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst);
        if active_now == 0 {
            Self::complete_idle_compaction_waits(event_loop, true);
        } else if emergent_scheduled {
            let pending = &event_loop.state.pending_compaction_waits;
            tracing::debug!(
                "emergent compactions scheduled; {} requests still waiting",
                pending.len()
            );
        }
    }

    pub(super) fn has_manual_compaction_waiters(event_loop: &EventLoop) -> bool {
        !event_loop.state.pending_compaction_waits.is_empty()
    }

    pub(super) fn complete_idle_compaction_waits(event_loop: &mut EventLoop, include_manual: bool) {
        if !include_manual {
            return;
        }
        for (request_id, CompactionWait::CompactAll) in
            event_loop.state.pending_compaction_waits.drain()
        {
            event_loop
                .router
                .complete(RuntimeResponse::Ok { request_id });
        }
    }

    pub(super) fn fail_pending_compaction_waits(
        event_loop: &mut EventLoop,
        error: &crate::common::MidgeError,
    ) {
        for (request_id, CompactionWait::CompactAll) in
            event_loop.state.pending_compaction_waits.drain()
        {
            event_loop.router.complete(RuntimeResponse::Error {
                request_id,
                error: error.replay(),
            });
        }
    }

    fn record_compaction_failure(event_loop: &mut EventLoop) {
        event_loop.state.diagnostics.record(|m| {
            m.record_compaction_failure();
        });
        event_loop.state.mark_persistence_anomaly();
    }

    pub(super) fn drain_publish_results(event_loop: &mut EventLoop) {
        while let Ok(completion) = event_loop.compaction_publish_result_rx.try_recv() {
            Self::handle_publication_completion(event_loop, completion);
        }
    }

    fn drain_inline_publish_worker(event_loop: &mut EventLoop) {
        if !event_loop.compaction_publish_actor.is_inline() {
            return;
        }
        while event_loop.compaction_publish_actor.is_inflight() {
            let Ok(completion) = event_loop.compaction_publish_result_rx.try_recv() else {
                break;
            };
            Self::handle_publication_completion(event_loop, completion);
        }
    }
}
