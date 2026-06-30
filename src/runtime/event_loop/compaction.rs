use super::{EventLoop, HandleOutcome};
use crate::runtime::{CompactionPlan, RuntimeResponse};

pub(super) struct CompactionCoordinator;

pub(super) struct CompactionCompleteRequest {
    pub request_id: u64,
    pub input_ssts: Vec<String>,
    pub output_ssts: Vec<String>,
    pub cf_id: crate::types::ColumnFamilyId,
    pub target_level: u32,
    pub succeeded: bool,
}

impl CompactionCoordinator {
    pub(super) fn check(event_loop: &mut EventLoop, request_id: u64) -> HandleOutcome {
        match event_loop.schedule_one_background_compaction_if_needed("CheckCompaction") {
            Ok(_) => event_loop.respond(request_id, RuntimeResponse::Ok { request_id }),
            Err(error) => {
                event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            }
        }
        HandleOutcome::Continue
    }

    pub(super) fn run(
        event_loop: &mut EventLoop,
        request_id: u64,
        plan: CompactionPlan,
    ) -> HandleOutcome {
        if event_loop
            .state
            .ingest_active
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            let epoch = event_loop
                .state
                .ingest_epoch
                .load(std::sync::atomic::Ordering::SeqCst);
            tracing::error!(
                component = "compaction",
                invariant = "no_compaction_during_ingest",
                ingest_epoch = epoch,
                input_files = ?plan.input_files,
                "BUG: RunCompaction called while ingest mode is active."
            );
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Internal(
                        "BUG: compaction execution attempted during ingest mode - violated invariant"
                            .to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }

        let cplan = crate::compaction::CompactionPlan {
            input_files: plan.input_files,
            output_files: Vec::new(),
            source_level: plan.source_level,
            target_level: plan.target_level,
            cf_id: plan.cf_id,
            output_seq: event_loop.state.next_sequence(),
            snapshot_horizon: event_loop.state.oldest_active_snapshot_sequence(),
        };

        let schedule_res = event_loop.compaction_actor.run_compaction(
            &mut event_loop.state,
            &cplan,
            event_loop.hybrid_storage.as_ref(),
            event_loop.worker_msg_tx.clone(),
        );
        let resp = match schedule_res {
            Ok(_) => RuntimeResponse::Ok { request_id },
            Err(error) => RuntimeResponse::Error {
                request_id,
                error: crate::common::MidgeError::Internal(error.to_string()),
            },
        };

        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }

    pub(super) fn compact_all(event_loop: &mut EventLoop, request_id: u64) -> HandleOutcome {
        if event_loop
            .state
            .ingest_active
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            let epoch = event_loop
                .state
                .ingest_epoch
                .load(std::sync::atomic::Ordering::SeqCst);
            tracing::error!(
                component = "compaction",
                invariant = "no_compaction_during_ingest",
                ingest_epoch = epoch,
                "BUG: CompactAll called while ingest mode is active."
            );
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Internal(
                        "BUG: compact_all attempted during ingest mode - violated invariant"
                            .to_string(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }

        let mut scheduled = 0usize;
        while let Some(plan) = event_loop
            .compaction_actor
            .check_compaction(&event_loop.state)
        {
            let plan = event_loop.assign_compaction_output_sequence(plan);
            let schedule_res = event_loop.compaction_actor.run_compaction(
                &mut event_loop.state,
                &plan,
                event_loop.hybrid_storage.as_ref(),
                event_loop.worker_msg_tx.clone(),
            );

            match schedule_res {
                Ok(_) => scheduled += 1,
                Err(error) => {
                    event_loop.respond(
                        request_id,
                        RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(error.to_string()),
                        },
                    );
                    break;
                }
            }
        }

        if scheduled == 0 {
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
            return HandleOutcome::Continue;
        }

        let mut pending = event_loop.state.pending_compaction_waits.lock();
        pending.insert(request_id, "CompactAll".to_string());
        HandleOutcome::Continue
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
        let mut allow_emergent_followup = false;

        let _prev = event_loop
            .state
            .active_compactions
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

        event_loop.compaction_actor.handle_complete(
            &mut event_loop.state,
            &input_ssts,
            &output_ssts,
        );

        if !succeeded {
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_compaction_failure();
            }
            tracing::warn!(
                input_count = input_ssts.len(),
                output_count = output_ssts.len(),
                "compaction worker failed or aborted; leaving manifest unchanged"
            );
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        } else if Self::publish_success(
            event_loop,
            request_id,
            &input_ssts,
            &output_ssts,
            cf_id,
            target_level,
        ) {
            allow_emergent_followup = true;
        }

        Self::complete_pending_waits(event_loop, allow_emergent_followup);
        event_loop.drain_auto_flush_memtables();
        HandleOutcome::Continue
    }

    fn publish_success(
        event_loop: &mut EventLoop,
        request_id: u64,
        input_ssts: &[String],
        output_ssts: &[String],
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
    ) -> bool {
        let added: Result<Vec<_>, _> = output_ssts
            .iter()
            .map(|name| event_loop.build_sst_file_meta(cf_id, target_level, name))
            .collect();

        if let Err(error) = added.and_then(|added| {
            event_loop.mirror_ssts_to_authoritative_cloud(output_ssts)?;
            event_loop.state.record_compaction_publication_intent(
                cf_id,
                input_ssts.to_vec(),
                added.clone(),
            )?;

            fail::fail_point!("slice7::after_compaction_output_durable_before_manifest_publish");

            event_loop.manifest_actor.compaction_complete(
                &mut event_loop.state,
                input_ssts,
                &added,
            )?;
            event_loop.state.transition_compaction_publication_intent(
                output_ssts,
                crate::runtime::PublicationPhase::ManifestPublished,
            )
        }) {
            Self::record_compaction_failure(event_loop);
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
            return false;
        }

        fail::fail_point!("slice6::after_compaction_update_before_manifest_persist");

        if let Err(error) = crate::runtime::actors::ManifestActor::persist(&event_loop.state) {
            Self::record_compaction_failure(event_loop);
            tracing::error!(error = ?error, "failed to persist manifest after compaction");
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Internal(format!(
                        "failed to persist manifest after compaction: {error}"
                    )),
                },
            );
            return false;
        }

        if let Err(error) =
            event_loop.mirror_metadata_after_local_commit("compaction manifest publish")
        {
            Self::record_compaction_failure(event_loop);
            tracing::error!(error = ?error, "failed to mirror manifest after compaction");
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Internal(format!(
                        "failed to mirror manifest after compaction: {error}"
                    )),
                },
            );
            return false;
        }

        fail::fail_point!("slice6::after_manifest_persist_before_sst_gc");

        let hybrid_storage = event_loop.hybrid_storage.clone();
        event_loop.gc_actor.delete_ssts(
            &mut event_loop.state,
            input_ssts,
            hybrid_storage,
        );
        tracing::info!(
            removed_count = input_ssts.len(),
            "Submitted compaction input SSTs for GC"
        );

        let cleared_intent_mirror_error = match event_loop
            .state
            .clear_compaction_publication_intent(output_ssts)
        {
            Ok(()) => match event_loop
                .mirror_metadata_after_local_commit("compaction publication intent clear")
            {
                Ok(()) => None,
                Err(error) => {
                    Self::record_compaction_failure(event_loop);
                    tracing::error!(
                        error = ?error,
                        "failed to mirror cleared compaction publication intent"
                    );
                    Some(error)
                }
            },
            Err(error) => {
                event_loop.state.mark_persistence_anomaly();
                tracing::warn!(
                    %error,
                    "failed to clear compaction publication intent after GC"
                );
                None
            }
        };

        if let Some(error) = cleared_intent_mirror_error {
            event_loop.publish_snapshot();
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Internal(format!(
                        "failed to mirror cleared compaction publication intent: {error}"
                    )),
                },
            );
            return false;
        }

        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            let bytes_rewritten: u64 = event_loop
                .state
                .manifest
                .files
                .iter()
                .filter(|file| output_ssts.contains(&file.name))
                .map(|file| file.size_bytes)
                .sum();
            telemetry.metrics().record_compaction(bytes_rewritten);
        }
        event_loop.publish_snapshot();
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        true
    }

    fn complete_pending_waits(event_loop: &mut EventLoop, allow_emergent_followup: bool) {
        let active = event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst);
        if active != 0 {
            return;
        }

        let mut emergent_scheduled = false;
        if allow_emergent_followup {
            while let Some(plan) = event_loop
                .compaction_actor
                .check_compaction(&event_loop.state)
            {
                if event_loop
                        .compaction_actor
                        .run_compaction(
                            &mut event_loop.state,
                        &plan,
                            event_loop.hybrid_storage.as_ref(),
                            event_loop.worker_msg_tx.clone(),
                        )
                    .is_ok()
                {
                    emergent_scheduled = true;
                    continue;
                }

                break;
            }
        }

        let active_now = event_loop
            .state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst);
        if active_now == 0 {
            let mut pending = event_loop.state.pending_compaction_waits.lock();
            for (req_id, condition) in pending.drain() {
                tracing::debug!(
                    "responding to pending {:?} request (request_id={})",
                    condition,
                    req_id
                );
                event_loop
                    .router
                    .complete(RuntimeResponse::Ok { request_id: req_id });
            }
        } else if emergent_scheduled {
            let pending = event_loop.state.pending_compaction_waits.lock();
            tracing::debug!(
                "emergent compactions scheduled; {} requests still waiting",
                pending.len()
            );
        }
    }

    fn record_compaction_failure(event_loop: &mut EventLoop) {
        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_compaction_failure();
        }
        event_loop.state.mark_persistence_anomaly();
    }
}
