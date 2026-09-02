use super::{EventLoop, HandleOutcome};
#[cfg(test)]
use crate::runtime::CompactionPlan;
use crate::runtime::RuntimeResponse;

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
        if event_loop.compaction_publication_degraded {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Fenced(
                        "compaction publication is unsettled; refusing another compaction until recovery"
                            .into(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }
        if event_loop.ddl_authority_ambiguous {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Fenced(
                        "DDL authority is ambiguous; refusing compaction until reconciliation"
                            .into(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }
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

        let reservation = event_loop.compaction_actor.handle_complete(
            &mut event_loop.state,
            &input_ssts,
            &output_ssts,
        );

        if succeeded {
            let published = Self::publish_success(
                event_loop,
                request_id,
                &input_ssts,
                &output_ssts,
                cf_id,
                target_level,
                reservation,
            );
            if published {
                allow_emergent_followup = true;
            } else if Self::manifest_authority_switched(event_loop, &input_ssts, &output_ssts) {
                Self::settle_incomplete_authoritative_publication(
                    event_loop,
                    &input_ssts,
                    &output_ssts,
                    reservation,
                );
            } else {
                // Once OutputDurable is persisted, a failed manifest append
                // can be ambiguous: the batch and marker may have reached the
                // journal even when its required sync reports an error. Do not
                // let a retry consume this output until restart reconciles the
                // journal and durable intent together.
                if event_loop
                    .state
                    .has_compaction_publication_intent(&input_ssts, &output_ssts)
                {
                    event_loop.compaction_publication_degraded = true;
                }
                if let (Some(hybrid), Some(token)) = (&event_loop.hybrid_storage, reservation) {
                    hybrid.compaction_aborted_with_token(token);
                }
            }
        } else {
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_compaction_failure();
            }
            tracing::warn!(
                input_count = input_ssts.len(),
                output_count = output_ssts.len(),
                "compaction worker failed or aborted; leaving manifest unchanged"
            );
            if let (Some(hybrid), Some(token)) = (&event_loop.hybrid_storage, reservation) {
                hybrid.compaction_aborted_with_token(token);
            }
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
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
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) -> bool {
        if event_loop.state.get_cf(cf_id).is_none() {
            event_loop.gc_actor.delete_ssts(
                &mut event_loop.state,
                output_ssts,
                event_loop.hybrid_storage.clone(),
            );
            return Self::respond_publish_failure(
                event_loop,
                request_id,
                &crate::common::MidgeError::Corruption(format!(
                    "compaction completed for inactive column family {cf_id}; retained inputs and rejected outputs"
                )),
            );
        }

        let publication_budget = crate::common::resource_budget::ResourceBudget::new(
            event_loop.compaction_actor.compaction_memory_limit(),
        );
        let added = match Self::build_output_metadata(
            event_loop,
            cf_id,
            target_level,
            output_ssts,
            &publication_budget,
        ) {
            Ok(added) => added,
            Err(error) => {
                event_loop.gc_actor.delete_ssts(
                    &mut event_loop.state,
                    output_ssts,
                    event_loop.hybrid_storage.clone(),
                );
                return Self::respond_publish_failure(event_loop, request_id, &error);
            }
        };

        if let Err(error) = Self::publish_compaction_manifest(
            event_loop,
            input_ssts,
            output_ssts,
            cf_id,
            &added,
            &publication_budget,
        ) {
            return Self::respond_publish_failure(event_loop, request_id, &error);
        }

        if let Err(error) = Self::persist_published_compaction(event_loop) {
            return Self::respond_publish_failure(event_loop, request_id, &error);
        }

        Self::finalize_published_compaction(
            event_loop,
            request_id,
            input_ssts,
            output_ssts,
            reservation,
        )
    }

    fn build_output_metadata(
        event_loop: &mut EventLoop,
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
        output_ssts: &[String],
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> Result<Vec<crate::runtime::FileMeta>, crate::common::MidgeError> {
        if output_ssts.windows(2).any(|names| names[0] >= names[1]) {
            return Err(crate::common::MidgeError::Corruption(
                "compaction output set must be sorted and uniquely named".to_string(),
            ));
        }

        let mut generation = None;
        for (expected_partition, name) in output_ssts.iter().enumerate() {
            let (name_cf, name_level, name_generation, name_partition) =
                crate::sst::parse_compaction_file_name(name).ok_or_else(|| {
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

        let metadata: Vec<_> = output_ssts
            .iter()
            .map(|name| event_loop.build_sst_file_meta(cf_id, target_level, name, budget))
            .collect::<Result<_, _>>()?;
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

    fn publish_compaction_manifest(
        event_loop: &mut EventLoop,
        input_ssts: &[String],
        output_ssts: &[String],
        cf_id: crate::types::ColumnFamilyId,
        added: &[crate::runtime::FileMeta],
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> Result<(), crate::common::MidgeError> {
        // Persist the rollback/cleanup obligation before remote upload. If
        // intent persistence fails, no untracked cloud object is created; if
        // upload or publication later fails, startup can prove and remove the
        // non-authoritative object from this durable record.
        let superseded_outputs = event_loop.state.record_compaction_publication_intent(
            cf_id,
            input_ssts.to_vec(),
            added.to_vec(),
        )?;
        // Real cloud startup hydrates local metadata from the control store
        // before intent replay. Make the cleanup obligation authoritative
        // there before any SST object can appear remotely; the salvage policy
        // must not weaken this ordering.
        event_loop.mirror_metadata_to_authoritative_cloud()?;
        if !superseded_outputs.is_empty() {
            event_loop.gc_actor.delete_ssts(
                &mut event_loop.state,
                &superseded_outputs,
                event_loop.hybrid_storage.clone(),
            );
        }
        event_loop.mirror_ssts_to_authoritative_cloud(output_ssts, budget)?;

        crate::failpoints::fail_point!(
            "slice7::after_compaction_output_durable_before_manifest_publish"
        );

        event_loop
            .manifest_actor
            .compaction_complete(&mut event_loop.state, input_ssts, added)?;
        crate::failpoints::fail_point!(
            "midge::compaction::inject_failure_after_manifest_batch",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: compaction failed after durable manifest batch".to_string()
            ))
        );
        event_loop.state.transition_compaction_publication_intent(
            input_ssts,
            output_ssts,
            crate::runtime::PublicationPhase::ManifestPublished,
        )
    }

    fn persist_published_compaction(
        event_loop: &mut EventLoop,
    ) -> Result<(), crate::common::MidgeError> {
        crate::failpoints::fail_point!("slice6::after_compaction_update_before_manifest_persist");
        crate::runtime::actors::ManifestActor::persist(&event_loop.state)?;
        event_loop.mirror_metadata_after_local_commit("compaction manifest publish")
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
            let output_sizes: Vec<u64> = output_ssts
                .iter()
                .filter_map(|name| {
                    std::fs::metadata(event_loop.state.sst_dir.join(name))
                        .ok()
                        .map(|metadata| metadata.len())
                })
                .collect();
            hybrid.compaction_inputs_retained_with_token(token, &output_sizes);
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

    fn finalize_published_compaction(
        event_loop: &mut EventLoop,
        request_id: u64,
        input_ssts: &[String],
        output_ssts: &[String],
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) -> bool {
        crate::failpoints::fail_point!("slice6::after_manifest_persist_before_sst_gc");

        // Make the replacement manifest visible before GC samples pins and
        // removes its inputs. Snapshot acquisition shares the pin registry's
        // acquisition guard with GC, so readers either pin the old generation
        // before this publication or capture the replacement generation.
        event_loop.publish_snapshot();

        let hybrid_storage = event_loop.hybrid_storage.clone();
        if let (Some(hybrid), Some(token)) = (&hybrid_storage, reservation) {
            let output_sizes: Vec<u64> = output_ssts
                .iter()
                .filter_map(|name| {
                    std::fs::metadata(event_loop.state.sst_dir.join(name))
                        .ok()
                        .map(|metadata| metadata.len())
                })
                .collect();
            hybrid.compaction_completed_with_token(token, &output_sizes);
        }
        event_loop
            .gc_actor
            .delete_ssts(&mut event_loop.state, input_ssts, hybrid_storage);
        crate::failpoints::fail_point!("midge::compaction::after_input_sst_gc");
        tracing::info!(
            removed_count = input_ssts.len(),
            "Submitted compaction input SSTs for GC"
        );

        if let Some(error) =
            Self::clear_published_compaction_intent(event_loop, input_ssts, output_ssts)
        {
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
            event_loop.compaction_publication_degraded = true;
            return false;
        }

        Self::record_compaction_metrics(event_loop, output_ssts);
        event_loop.publish_snapshot();
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        true
    }

    fn clear_published_compaction_intent(
        event_loop: &mut EventLoop,
        input_ssts: &[String],
        output_ssts: &[String],
    ) -> Option<crate::common::MidgeError> {
        match event_loop
            .state
            .clear_compaction_publication_intent(input_ssts, output_ssts)
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
                tracing::warn!(%error, "failed to clear compaction publication intent after GC");
                Some(error)
            }
        }
    }

    fn record_compaction_metrics(event_loop: &mut EventLoop, output_ssts: &[String]) {
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
    }

    fn respond_publish_failure(
        event_loop: &mut EventLoop,
        request_id: u64,
        error: &crate::common::MidgeError,
    ) -> bool {
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
        false
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
        // Publication-deferred work (notably CF drops) has already waited for
        // this authority switch. Let the run loop restore it before taking the
        // global compaction slot again, otherwise steady compaction debt can
        // starve destructive DDL indefinitely.
        if allow_emergent_followup && event_loop.publication_gate.deferred_messages.is_empty() {
            loop {
                let plan = match event_loop
                    .compaction_actor
                    .check_manual_compaction(&event_loop.state)
                {
                    Ok(Some(plan)) => plan,
                    Ok(None) => break,
                    Err(error) => {
                        Self::record_compaction_failure(event_loop);
                        let mut pending = event_loop.state.pending_compaction_waits.lock();
                        for (request_id, _) in pending.drain() {
                            event_loop.router.complete(RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Corruption(error.to_string()),
                            });
                        }
                        return;
                    }
                };
                if event_loop.launch_compaction(plan).is_ok() {
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
