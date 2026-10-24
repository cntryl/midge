use super::cloud_maintenance::MaintenanceTask;
use super::{
    EventLoop, HandleOutcome, BACKGROUND_COMPACTION_CHECK_INTERVAL, STARTUP_CLOUD_MAINTENANCE_DELAY,
};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::runtime::actors::compaction::publication::{
    CompactionPublicationToken, CompactionPublishCompletion, CompactionPublishPhase,
    CompactionPublishTask,
};
use crate::runtime::actors::compaction::PreparedCompactionOutput;
#[cfg(test)]
use crate::runtime::CompactionPlan;
use crate::runtime::RuntimeResponse;
use std::sync::Arc;
use std::time::Duration;
mod compute;
#[cfg(all(test, feature = "failpoints"))]
mod concurrency_tests;

pub(super) enum CompactionLaunch {
    Started,
    WaitingForMetadata,
}

impl EventLoop {
    #[cfg(test)]
    pub(super) fn assign_compaction_output_sequence(
        &mut self,
        plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<crate::compaction::CompactionPlan> {
        self.assign_compaction_output_sequence_within(plan, None)
    }

    fn assign_compaction_output_sequence_within(
        &mut self,
        mut plan: crate::compaction::CompactionPlan,
        manual_deadline: Option<OperationDeadline>,
    ) -> crate::common::MidgeResult<crate::compaction::CompactionPlan> {
        CompactionCoordinator::check_manual_deadline(manual_deadline)?;
        if plan.output_seq == 0 {
            plan.output_seq = self.state.next_compaction_output_generation()?;
        }
        if self.actors.cloud_coordinator.hybrid_storage.is_some() {
            // Early remote output staging can leave harmless orphans after a
            // crash. Persist the filename allocation before any such object
            // is uploaded so a cold replacement never reuses its identity.
            if let Some(deadline) = manual_deadline {
                self.reserve_sst_name_durably_within(plan.cf_id, plan.output_seq, &deadline)?;
            } else {
                self.reserve_sst_name_durably(plan.cf_id, plan.output_seq)?;
            }
        }
        Ok(plan)
    }

    #[cfg(test)]
    pub(super) fn prepare_compaction_plan_for_launch(
        &mut self,
        plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<crate::compaction::CompactionPlan> {
        self.prepare_compaction_plan_for_launch_within(plan, None)
    }

    fn prepare_compaction_plan_for_launch_within(
        &mut self,
        plan: crate::compaction::CompactionPlan,
        manual_deadline: Option<OperationDeadline>,
    ) -> crate::common::MidgeResult<crate::compaction::CompactionPlan> {
        self.available_compaction_memory()?;
        let memory_limit = self.actors.compaction_actor.memory_limit_for_plan(&plan);
        let mut plan = self.assign_compaction_output_sequence_within(plan, manual_deadline)?;
        plan.snapshot_horizon = self.state.oldest_active_snapshot_sequence();
        // Read snapshots pin their original SST names through GC's exclusive
        // capture/registration sampling boundary. Rewriting cannot remove the
        // source of a pre-delete read until its final snapshot unpins.
        plan.retained_snapshot_sources = true;
        plan.compaction_time_millis = Some(self.state.observed_time_millis());
        let grandparent_level = plan.target_level.saturating_add(1);
        plan.grandparent_overlap = crate::compaction::GrandparentOverlap::from_files(
            self.state
                .manifest
                .files
                .iter()
                .filter(|file| file.cf_id == plan.cf_id && file.level == grandparent_level),
            self.actors.compaction_actor.max_grandparent_overlap_bytes(),
        );
        plan.target_sst_size = self.actors.compaction_actor.target_sst_size();
        plan.compaction_memory_limit = memory_limit;

        if plan.output_seq == 0 {
            return Err(crate::common::MidgeError::Internal(
                "BUG: compaction output sequence was not assigned before actor launch".to_string(),
            ));
        }

        Ok(plan)
    }

    pub(super) fn available_compaction_memory(&self) -> crate::common::MidgeResult<usize> {
        if self
            .actors
            .cloud_coordinator
            .cloud_wal_prune_worker
            .is_some()
            || self.publication.publication_gate.is_active()
        {
            return Err(crate::common::MidgeError::Busy(
                "compaction memory is owned by an active publication turn".into(),
            ));
        }
        Ok(self.actors.compaction_actor.compaction_memory_limit())
    }

    pub(super) fn launch_compaction(
        &mut self,
        plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<()> {
        self.launch_compaction_disposition(plan).map(|_| ())
    }

    fn launch_compaction_disposition(
        &mut self,
        plan: crate::compaction::CompactionPlan,
    ) -> crate::common::MidgeResult<CompactionLaunch> {
        let had_manual_waiters = !self.state.pending_compaction_waits.is_empty();
        CompactionCoordinator::expire_manual_compaction_waiters(self);
        if self.actors.compaction_actor.active_count().saturating_add(self.actors.cloud_coordinator.metadata_mirror.pending_compactions.len()) >= self.actors.compaction_actor.concurrency_limit() {
            return Err(MidgeError::Busy("compaction owner capacity is reserved".into()));
        }
        if self.publication.publication_gate.is_active() {
            return Err(crate::common::MidgeError::Busy(
                "manifest publication is already in progress".to_string(),
            ));
        }
        self.authority.compaction_fence.admit_compaction()?;
        // Select once, before any preparation or provider work. Later waiters
        // cannot replace the accepted generation's original allowance.
        let manual_deadline = self.state.pending_compaction_waits.values().next().copied();
        if had_manual_waiters && manual_deadline.is_none() {
            return Err(MidgeError::Timeout(
                "manual compaction caller no longer waiting".into(),
            ));
        }
        self.launch_compaction_with_original_deadline(plan, manual_deadline)
    }

    fn launch_compaction_with_original_deadline(
        &mut self,
        mut plan: crate::compaction::CompactionPlan,
        manual_deadline: Option<OperationDeadline>,
    ) -> crate::common::MidgeResult<CompactionLaunch> {
        CompactionCoordinator::check_manual_deadline(manual_deadline)?;
        self.state.retry_metadata_reload()?;
        CompactionCoordinator::check_manual_deadline(manual_deadline)?;
        if plan.output_seq == 0 {
            plan.output_seq = self.state.next_compaction_output_generation()?;
        }
        if self.actors.cloud_coordinator.hybrid_storage.is_some()
            && self.begin_sst_name_reservation(
                plan.cf_id,
                plan.output_seq,
                manual_deadline.as_ref(),
                crate::metadata::accounting::Origin::CompactionBeforeGc,
            )?
        {
            // This accepted plan and its initial caller allowance survive the
            // provider round trip. It has no worker/output ownership yet.
            self.actors
                .cloud_coordinator
                .metadata_mirror
                .pending_compactions.push_back((plan, manual_deadline));
            return Ok(CompactionLaunch::WaitingForMetadata);
        }
        let plan = self.prepare_compaction_plan_for_launch_within(plan, manual_deadline)?;
        CompactionCoordinator::check_manual_deadline(manual_deadline)?;

        let compaction_storage =
            self.actors
                .cloud_coordinator
                .hybrid_storage
                .as_ref()
                .map(|storage| {
                    Arc::clone(storage)
                        as Arc<dyn crate::runtime::actors::compaction::CompactionStorage>
                });
        self.actors
            .compaction_actor
            .run_compaction(
                &mut self.state,
                &plan,
                compaction_storage.as_ref(),
                self.pipeline.worker_msg_tx.clone(),
                manual_deadline,
            )
            .map(|_| CompactionLaunch::Started)
    }

    pub(super) fn resume_compaction_after_metadata_mirror(
        &mut self,
        plan: crate::compaction::CompactionPlan,
        deadline: Option<OperationDeadline>,
        result: MidgeResult<()>,
    ) {
        let result = result.and_then(|()| {
            self.authority.compaction_fence.admit_compaction()?;
            self.launch_compaction_with_original_deadline(plan, deadline)
        });
        if let Err(error) = result {
            CompactionCoordinator::fail_pending_compaction_waits(self, &error);
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "compaction reservation completion retained its inputs");
        }
    }

    pub(super) fn schedule_one_background_compaction_if_needed(
        &mut self,
        operation: &str,
    ) -> crate::common::MidgeResult<bool> {
        if self.cloud_maintenance_enabled()
            && !self.actors.cloud_coordinator.cloud_maintenance.dispatching
        {
            return Ok(self.schedule_cloud_maintenance() == Some(MaintenanceTask::Compaction));
        }
        let manual = self.cloud_maintenance_enabled()
            && CompactionCoordinator::has_manual_compaction_waiters(self);
        let result = self.schedule_background_compaction_plan(operation, manual);
        if manual {
            match &result {
                Ok(false) => {
                    CompactionCoordinator::complete_idle_compaction_waits(self, true);
                }
                Err(error) => {
                    CompactionCoordinator::fail_pending_compaction_waits(self, error);
                }
                Ok(true) => {}
            }
        }
        result
    }

    pub(super) fn schedule_background_compaction_plan(
        &mut self,
        operation: &str,
        manual: bool,
    ) -> crate::common::MidgeResult<bool> {
        if self.publication.publication_gate.is_active()
            || !self.actors.compaction_actor.has_capacity()
        {
            return Ok(false);
        }
        CompactionCoordinator::expire_manual_compaction_waiters(self);
        let manual = manual && CompactionCoordinator::has_manual_compaction_waiters(self);
        // Disabling ordinary background work must not permanently wedge L0
        // admission. Use the same authority and worker gates for
        // pressure recovery at startup, after flush, and during live maintenance.
        let background_enabled = self.state.compaction_enabled();
        if !background_enabled && !manual {
            crate::failpoints::fail_point!("midge::compaction::defer_pressure_recovery", |_| {
                // Recheck held fixtures promptly when released. This hook and
                // its scheduling override are absent from ordinary builds.
                self.pipeline
                    .background_compaction_schedule
                    .defer_for(Duration::from_millis(10));
                Ok(false)
            });
        }
        if !background_enabled && !manual && !self.state.has_any_critical_l0_debt() {
            return Ok(false);
        }
        if self.authority.fencing.ddl_authority_ambiguous {
            return Err(crate::common::MidgeError::Fenced(
                "DDL authority is ambiguous; refusing compaction until reconciliation".into(),
            ));
        }
        let timed_out = self.state.warn_timed_out_snapshots();
        if timed_out > 0 {
            tracing::warn!(
                timed_out,
                operation,
                "Observed timed-out snapshots before compaction check; retaining pins"
            );
        }

        let planned = if background_enabled && !manual {
            self.actors.compaction_actor.check_compaction(&self.state)
        } else {
            self.actors
                .compaction_actor
                .check_manual_compaction(&self.state)
        };
        let Some(plan) = planned.inspect_err(|_| self.state.mark_persistence_anomaly())? else {
            return Ok(false);
        };

        self.launch_compaction(plan)?;
        Ok(true)
    }

    pub(super) fn schedule_compaction_after_flush_publication(&mut self, sst_name: &str) {
        let next_check = self.next_background_compaction_delay();
        if self.background_maintenance_timeout() > next_check {
            self.pipeline
                .background_compaction_schedule
                .defer_for(next_check);
        }
        match self.schedule_one_background_compaction_if_needed("flush publication") {
            Ok(true) => tracing::debug!(
                sst_name,
                "Scheduled background compaction after flush publication"
            ),
            Ok(false) => {}
            Err(error) => tracing::warn!(
                %error,
                sst_name,
                "Skipping automatic compaction after flush publication"
            ),
        }
    }

    pub(super) fn background_maintenance_timeout(&self) -> Duration {
        self.pipeline
            .background_compaction_schedule
            .remaining()
            .unwrap_or(Duration::ZERO)
    }

    fn next_background_compaction_delay(&self) -> Duration {
        if !self.state.compaction_enabled() {
            return BACKGROUND_COMPACTION_CHECK_INTERVAL;
        }
        let now = self.state.observed_time_millis();
        self.state
            .manifest
            .files
            .iter()
            .filter_map(|file| file.earliest_expiration_millis)
            .min()
            .map_or(BACKGROUND_COMPACTION_CHECK_INTERVAL, |expiration| {
                // Recheck due/uncertain TTL debt at a bounded cadence instead
                // of busy-spinning when a job or publication owns the pool.
                Duration::from_millis(expiration.saturating_sub(now))
                    .max(Duration::from_secs(1))
                    .min(BACKGROUND_COMPACTION_CHECK_INTERVAL)
            })
    }

    pub(super) fn run_background_compaction_maintenance_if_due(&mut self) {
        // A restored request owns the publication turn that released it.
        if self.routing.pending_msg.is_some()
            || self.background_maintenance_timeout() != Duration::ZERO
        {
            return;
        }

        let next_check = self.next_background_compaction_delay();
        self.pipeline
            .background_compaction_schedule
            .defer_for(next_check);
        match self.schedule_one_background_compaction_if_needed("periodic maintenance") {
            Ok(true) => tracing::debug!("Scheduled background compaction during maintenance"),
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%error, "Background compaction maintenance check failed");
            }
        }
        self.prune_cloud_wal_segments_covered_by_manifest();
    }

    pub(in crate::runtime) fn schedule_background_compaction_on_startup(&mut self) {
        self.pipeline
            .background_compaction_schedule
            .defer_for(STARTUP_CLOUD_MAINTENANCE_DELAY);
        match self.schedule_one_background_compaction_if_needed("runtime startup") {
            Ok(true) => tracing::debug!("Scheduled compaction during runtime startup"),
            Ok(false) => {}
            Err(error) => tracing::warn!(%error, "Startup background compaction check failed"),
        }
    }

    /// Local copies are disposable only after the remote manifest publication
    /// has completed. Snapshot readers pin remote objects, not these files.
    pub(super) fn evict_published_sst_cache(&self, names: &[String]) {
        let Some(storage) = &self.actors.cloud_coordinator.hybrid_storage else {
            return;
        };
        for name in names {
            let path = crate::sst::fs::sst_key(name);
            let size = self
                .state
                .fs
                .metadata(&path)
                .ok()
                .map(|metadata| metadata.len);
            match self.state.fs.remove_file(&path) {
                Ok(()) => storage.release_local_sst_bytes(size.unwrap_or(0)),
                Err(crate::io::FsError::NotFound(_)) => {}
                Err(error) => {
                    tracing::warn!(%error, sst_name = name, "retaining local SST cache after failed eviction");
                }
            }
        }
    }
}

mod fence;
pub(super) use fence::CompactionPublicationFence;
mod slot;
pub(super) use slot::PublicationSlot;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod deadline_tests;

#[cfg(test)]
mod name_budget_tests;

#[cfg(test)]
mod fragmentation_tests;

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
    manual_deadline: Option<OperationDeadline>,
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
            purpose: if plan.source_level == plan.target_level {
                crate::compaction::strategy::CompactionPurpose::Repair
            } else {
                crate::compaction::strategy::CompactionPurpose::Merge
            },
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
            retained_snapshot_sources: false,
            compaction_time_millis: None,
            grandparent_overlap: Default::default(),
        };

        let schedule_res = event_loop.launch_compaction(cplan);
        let resp = match schedule_res {
            Ok(_) => RuntimeResponse::Ok { request_id },
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

        let deadline = event_loop
            .routing
            .router
            .request_deadline(request_id, event_loop.routing.runtime_response_timeout)
            .unwrap_or_else(|| OperationDeadline::from_budget(Duration::ZERO));
        if let Err(error) = Self::check_manual_deadline(Some(deadline)) {
            event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            return HandleOutcome::Continue;
        }
        event_loop
            .state
            .pending_compaction_waits
            .insert(request_id, deadline);

        if event_loop.cloud_maintenance_enabled() {
            event_loop.schedule_cloud_maintenance();
            return HandleOutcome::Continue;
        }

        if !event_loop.actors.compaction_actor.has_capacity() {
            return HandleOutcome::Continue;
        }

        let mut scheduled = 0usize;
        loop {
            let plan = match event_loop
                .actors
                .compaction_actor
                .check_manual_compaction(&event_loop.state)
            {
                Ok(Some(plan)) => plan,
                Ok(None) => break,
                Err(error) => {
                    event_loop.state.mark_persistence_anomaly();
                    event_loop
                        .state
                        .pending_compaction_waits
                        .remove(&request_id);
                    event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
                    return HandleOutcome::Continue;
                }
            };
            match event_loop.launch_compaction_disposition(plan) {
                Ok(CompactionLaunch::Started) => scheduled += 1,
                Ok(CompactionLaunch::WaitingForMetadata) => {
                    scheduled += 1;
                    break;
                }
                Err(error) => {
                    event_loop
                        .state
                        .pending_compaction_waits
                        .remove(&request_id);
                    event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
                    return HandleOutcome::Continue;
                }
            }
        }

        if scheduled == 0 {
            event_loop
                .state
                .pending_compaction_waits
                .remove(&request_id);
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
            return HandleOutcome::Continue;
        }

        HandleOutcome::Continue
    }

    fn validate_manual_compaction_request(
        event_loop: &EventLoop,
    ) -> crate::common::MidgeResult<()> {
        event_loop.authority.compaction_fence.admit_compaction()?;
        if event_loop.authority.fencing.ddl_authority_ambiguous {
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
        if !event_loop.actors.compaction_actor.select_completion(request_id, &input_ssts, &output_ssts, cf_id, target_level, succeeded) {
            tracing::debug!(request_id, "ignored stale, early or mismatched compaction completion");
            return HandleOutcome::Continue;
        }
        let worker_error = event_loop.actors.compaction_actor.take_worker_error();

        if succeeded {
            event_loop.actors.compaction_actor.join_completed_worker();
            if let Err(error) = Self::start_publication(
                event_loop,
                request_id,
                &input_ssts,
                &output_ssts,
                cf_id,
                target_level,
            ) {
                if event_loop.publication.compaction_publication.is_active() {
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
            let repair = event_loop
                .actors
                .compaction_actor
                .active_same_level_repair();
            let error = worker_error.unwrap_or_else(|| {
                crate::common::MidgeError::Internal(
                    "compaction worker failed without an error".to_string(),
                )
            });
            tracing::warn!(
                %error,
                repair,
                cf_id,
                target_level,
                input_count = input_ssts.len(),
                output_count = output_ssts.len(),
                "compaction worker failed or aborted; leaving manifest unchanged"
            );
            Self::defer_failed_repair_retry(event_loop, repair, "worker");
            // Partitions uploaded before the failure are named by a reserved,
            // never-reused generation and referenced by no manifest or
            // intent. Reclaim them now, or every retry of a deterministically
            // failing plan leaks another set of remote objects.
            let orphaned = event_loop
                .actors
                .compaction_actor
                .take_prepared_output_names();
            if !orphaned.is_empty() {
                let hybrid_storage = event_loop.actors.cloud_coordinator.hybrid_storage.clone();
                event_loop.actors.gc_actor.delete_ssts(
                    &mut event_loop.state,
                    &orphaned,
                    hybrid_storage,
                );
            }
            let reservation = event_loop.actors.compaction_actor.handle_complete(
                &mut event_loop.state,
                &input_ssts,
                &output_ssts,
            );
            if let (Some(hybrid), Some(token)) = (
                &event_loop.actors.cloud_coordinator.hybrid_storage,
                reservation,
            ) {
                event_loop
                    .actors
                    .compaction_actor
                    .settle_compaction_error_reservation(hybrid.as_ref(), token, Some(&error));
            }
            let completion_error = error.replay();
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
                event_loop.actors.gc_actor.delete_ssts(
                    &mut event_loop.state,
                    output_ssts,
                    event_loop.actors.cloud_coordinator.hybrid_storage.clone(),
                );
                return Err(error);
            }
        };
        let manual_deadline = event_loop
            .actors
            .compaction_actor
            .manual_deadline_for_generation(cf_id, target_level, output_generation)?;
        Self::check_manual_deadline(manual_deadline)?;
        let started_at = event_loop
            .actors
            .compaction_actor
            .started_at_for_generation(cf_id, target_level, output_generation)?;
        let mut canonical_inputs = input_ssts.to_vec();
        let mut canonical_outputs = output_ssts.to_vec();
        canonical_inputs.sort_unstable();
        canonical_outputs.sort_unstable();
        let token = CompactionPublicationToken {
            request_id,
            writer_epoch: event_loop.authority.fencing.writer_epoch,
            cf_id,
            target_level,
            output_generation,
            input_ssts: canonical_inputs,
            output_ssts: canonical_outputs,
            started_at,
        };
        let publication_owner = Self::publication_owner(&token);
        if !event_loop
            .publication
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
                event_loop
                    .publication
                    .publication_gate
                    .release(&publication_owner);
                return Err(error);
            }
        };
        let pending = PendingCompactionPublication {
            manual_deadline,
            token,
            outputs,
            added,
            superseded_outputs,
            expected_phase: CompactionPublishPhase::OutputDurable,
        };
        if let Err(error) = event_loop.publication.compaction_publication.install(
            &event_loop.publication.publication_gate,
            publication_owner.clone(),
            pending,
        ) {
            event_loop
                .publication
                .publication_gate
                .release(&publication_owner);
            return Err(error);
        }
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
        event_loop: &mut EventLoop,
        input_ssts: &[String],
        output_ssts: &[String],
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
    ) -> crate::common::MidgeResult<(
        u64,
        Vec<PreparedCompactionOutput>,
        Vec<crate::runtime::FileMeta>,
    )> {
        event_loop
            .actors
            .compaction_actor
            .validate_reserved_inputs(&event_loop.state.manifest.files)?;
        if event_loop.state.get_cf(cf_id).is_none() {
            return Err(crate::common::MidgeError::Corruption(format!(
                "compaction completed for inactive column family {cf_id}; retained inputs and rejected outputs"
            )));
        }
        let output_generation = Self::validate_output_names(cf_id, target_level, output_ssts)?;
        Self::validate_captured_target_span(event_loop, input_ssts, cf_id, target_level)?;
        let prepared = event_loop.actors.compaction_actor.prepared_outputs_exact(
            output_ssts,
            cf_id,
            target_level,
        );
        #[cfg(test)]
        let prepared: MidgeResult<Vec<PreparedCompactionOutput>> = prepared.or_else(|_| {
            let outputs = event_loop
                .actors
                .compaction_actor
                .prepare_outputs_from_local_files_for_test(
                    output_ssts,
                    cf_id,
                    target_level,
                    &event_loop.state.host_sst_dir(),
                )?;
            // Legacy phase fixtures prepare local bytes without launching a
            // worker. Give their accepted synthetic owner an explicit identity.
            // Real compute and the deadline regressions never use this fallback.
            event_loop
                .actors
                .compaction_actor
                .prepare_publication_generation_for_test(cf_id, target_level, output_generation)?;
            Ok(outputs)
        });
        let outputs = prepared?;
        let added =
            Self::validate_prepared_output_metadata(cf_id, target_level, output_ssts, &outputs)?;
        Self::validate_fragment_replacement(event_loop, input_ssts, &added, cf_id, target_level)?;
        if event_loop
            .actors
            .compaction_actor
            .active_same_level_repair()
        {
            Self::validate_repair_replacement_level(
                event_loop,
                input_ssts,
                &added,
                cf_id,
                target_level,
            )?;
        }
        let accepted_generation = event_loop
            .actors
            .compaction_actor
            .accepted_output_generation(cf_id, target_level)?;
        if !output_ssts.is_empty() && accepted_generation != output_generation {
            return Err(MidgeError::Fenced(
                "compaction output generation differs from its accepted owner".into(),
            ));
        }
        Ok((accepted_generation, outputs, added))
    }

    fn validate_repair_replacement_level(
        event_loop: &EventLoop,
        input_ssts: &[String],
        added: &[crate::runtime::FileMeta],
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
    ) -> crate::common::MidgeResult<()> {
        let removed: std::collections::HashSet<_> = input_ssts.iter().map(String::as_str).collect();
        let added_names: std::collections::HashSet<_> =
            added.iter().map(|file| file.name.as_str()).collect();
        let added_metadata: Vec<crate::metadata::FileMeta> = added.iter().map(Into::into).collect();
        let level: Vec<_> = event_loop
            .state
            .manifest
            .files
            .iter()
            .filter(|file| {
                file.cf_id == cf_id
                    && file.level == target_level
                    && !removed.contains(file.name.as_str())
            })
            .chain(&added_metadata)
            .collect();
        if crate::compaction::layout::repair_components(&level)
            .iter()
            .any(|component| {
                component
                    .iter()
                    .any(|&index| added_names.contains(level[index].name.as_str()))
            })
        {
            return Err(crate::common::MidgeError::Fenced(
                "overlap-repair replacement would leave defective target-level coverage".into(),
            ));
        }
        Ok(())
    }

    fn validate_fragment_replacement(
        event_loop: &EventLoop,
        input_ssts: &[String],
        added: &[crate::runtime::FileMeta],
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
    ) -> crate::common::MidgeResult<()> {
        let Some(split_key) = event_loop
            .actors
            .compaction_actor
            .active_source_fragmentation()
        else {
            return Ok(());
        };
        let source = event_loop
            .state
            .manifest
            .files
            .iter()
            .find(|file| input_ssts.first().is_some_and(|name| file.name == *name))
            .ok_or_else(|| {
                MidgeError::Fenced("fragment source is no longer authoritative".into())
            })?;
        let bounds = source
            .smallest_key
            .as_ref()
            .zip(source.largest_key.as_ref());
        let contained = bounds.is_some_and(|(smallest, largest)| {
            added.iter().all(|file| {
                file.smallest_key
                    .as_ref()
                    .zip(file.largest_key.as_ref())
                    .is_some_and(|(start, end)| smallest <= start && start <= end && end <= largest)
            })
        });
        let split_preserved = added.windows(2).any(|pair| {
            pair[0]
                .largest_key
                .as_deref()
                .is_some_and(|end| end <= split_key)
                && pair[1]
                    .smallest_key
                    .as_deref()
                    .is_some_and(|start| start >= split_key)
        });
        if !contained || !split_preserved {
            return Err(MidgeError::Fenced(
                "source fragmentation did not preserve its captured range and split".into(),
            ));
        }
        Self::validate_repair_replacement_level(event_loop, input_ssts, added, cf_id, target_level)
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
        let captured = event_loop.actors.compaction_actor.captured_input_metadata();
        if !captured.is_empty() {
            let live: std::collections::HashMap<_, _> = selected_files
                .iter()
                .map(|file| (file.name.as_str(), *file))
                .collect();
            if captured.len() != selected_files.len()
                || captured.iter().any(|file| {
                    live.get(file.name.as_str())
                        .is_none_or(|current| !file.same_identity(current))
                })
            {
                return Err(crate::common::MidgeError::Fenced(
                    "compaction input metadata changed before publication".into(),
                ));
            }
        }
        if let Some(split_key) = event_loop
            .actors
            .compaction_actor
            .active_source_fragmentation()
        {
            return Self::validate_captured_fragment_source(
                event_loop,
                &selected_files,
                cf_id,
                target_level,
                split_key,
            );
        }
        if event_loop
            .actors
            .compaction_actor
            .active_same_level_repair()
        {
            return Self::validate_captured_repair_component(
                event_loop,
                &selected,
                cf_id,
                target_level,
            );
        }
        Self::validate_captured_promotion_span(event_loop, &selected_files, cf_id, target_level)
    }

    fn validate_captured_promotion_span(
        event_loop: &EventLoop,
        selected_files: &[&crate::metadata::FileMeta],
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
    ) -> crate::common::MidgeResult<()> {
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

    fn validate_captured_fragment_source(
        event_loop: &EventLoop,
        selected: &[&crate::metadata::FileMeta],
        cf_id: crate::types::ColumnFamilyId,
        level: u32,
        split_key: &[u8],
    ) -> crate::common::MidgeResult<()> {
        let [source] = selected else {
            return Err(MidgeError::Fenced(
                "source fragmentation must replace one file".into(),
            ));
        };
        if level == 0
            || source.cf_id != cf_id
            || source.level != level
            || !source.key_bounds_complete
            || !source
                .smallest_key
                .as_deref()
                .zip(source.largest_key.as_deref())
                .is_some_and(|(start, end)| start < split_key && split_key < end)
        {
            return Err(MidgeError::Fenced(
                "source fragmentation bounds changed".into(),
            ));
        }
        let files: Vec<_> = event_loop
            .state
            .manifest
            .files
            .iter()
            .filter(|file| file.cf_id == cf_id && file.level == level)
            .collect();
        if files.iter().any(|file| {
            !file.key_bounds_complete
                || file
                    .smallest_key
                    .as_ref()
                    .zip(file.largest_key.as_ref())
                    .is_none_or(|(start, end)| start > end)
        }) || crate::compaction::layout::repair_components(&files)
            .iter()
            .any(|component| {
                component
                    .iter()
                    .any(|&index| files[index].name == source.name)
            })
        {
            return Err(MidgeError::Fenced(
                "source fragmentation layout changed".into(),
            ));
        }
        Ok(())
    }

    fn validate_captured_repair_component(
        event_loop: &EventLoop,
        selected: &std::collections::HashSet<&str>,
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
    ) -> Result<(), crate::common::MidgeError> {
        let level: Vec<_> = event_loop
            .state
            .manifest
            .files
            .iter()
            .filter(|file| file.cf_id == cf_id && file.level == target_level)
            .collect();
        let matches_component = crate::compaction::layout::repair_components(&level)
            .into_iter()
            .any(|component| {
                let names: std::collections::HashSet<_> = component
                    .iter()
                    .map(|&index| level[index].name.as_str())
                    .collect();
                names == *selected
            });
        if !matches_component {
            return Err(crate::common::MidgeError::Fenced(
                "overlap-repair component changed before publication".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn handle_publication_completion(
        event_loop: &mut EventLoop,
        completion: CompactionPublishCompletion,
    ) {
        let Some(pending) = event_loop.publication.compaction_publication.get().cloned() else {
            tracing::warn!(?completion.phase, "ignored stale compaction publication completion");
            return;
        };
        if pending.token != completion.token || pending.expected_phase != completion.phase {
            // A late/duplicate result has no authority over the current job,
            // worker ticket, reservation, publication owner or durable intent.
            tracing::warn!(?completion.phase, "ignored mismatched compaction publication completion");
            return;
        }
        if pending.expected_phase != CompactionPublishPhase::IntentCleared
            && event_loop.actors.compaction_actor.active_count() > 0
            && !event_loop.actors.compaction_actor.select_job(pending.token.request_id)
        {
            event_loop.authority.compaction_fence.degrade();
            event_loop.state.mark_persistence_anomaly();
            tracing::error!(job_id = pending.token.request_id, "retaining publication whose compute owner cannot be selected");
            return;
        }
        event_loop.actors.compaction_publish_actor.finish_task();
        if let Err(error) = completion.result {
            Self::finish_failed_publication(event_loop, &error);
            return;
        }
        if event_loop.authority.fencing.writer_epoch != pending.token.writer_epoch {
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
        Self::check_manual_deadline(pending.manual_deadline)?;
        if event_loop.state.get_cf(pending.token.cf_id).is_none() {
            return Err(crate::common::MidgeError::Fenced(
                "compaction column family was removed while publication was pending".to_string(),
            ));
        }
        if !pending.superseded_outputs.is_empty() {
            event_loop.actors.gc_actor.delete_ssts(
                &mut event_loop.state,
                &pending.superseded_outputs,
                event_loop.actors.cloud_coordinator.hybrid_storage.clone(),
            );
        }
        Self::validate_captured_target_span(
            event_loop,
            &pending.token.input_ssts,
            pending.token.cf_id,
            pending.token.target_level,
        )?;
        Self::validate_fragment_replacement(
            event_loop,
            &pending.token.input_ssts,
            &pending.added,
            pending.token.cf_id,
            pending.token.target_level,
        )?;
        event_loop.check_lease_health()?;
        Self::check_manual_deadline(pending.manual_deadline)?;
        event_loop.actors.manifest_actor.compaction_complete(
            &mut event_loop.state,
            &pending.token.input_ssts,
            &pending.added,
        )?;
        crate::failpoints::fail_point!(
            "midge::compaction::inject_failure_after_manifest_batch",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: compaction failed after durable manifest batch".to_string()
            ))
        );
        event_loop.check_lease_health()?;
        Self::check_manual_deadline(pending.manual_deadline)?;
        event_loop.state.transition_compaction_publication_intent(
            &pending.token.input_ssts,
            &pending.token.output_ssts,
            crate::runtime::PublicationPhase::ManifestPublished,
        )?;
        crate::failpoints::fail_point!("slice6::after_compaction_update_before_manifest_persist");
        event_loop.check_lease_health()?;
        Self::check_manual_deadline(pending.manual_deadline)?;
        Self::submit_publication_phase(event_loop, CompactionPublishPhase::ManifestPublished)
    }

    fn begin_intent_clear_publication(
        event_loop: &mut EventLoop,
        pending: &PendingCompactionPublication,
    ) -> crate::common::MidgeResult<()> {
        crate::failpoints::fail_point!("slice6::after_manifest_persist_before_sst_gc");
        event_loop.check_lease_health()?;
        Self::check_manual_deadline(pending.manual_deadline)?;
        event_loop.publish_snapshot();
        let output_sizes =
            Self::resident_output_sizes(event_loop.state.fs.as_ref(), &pending.token.output_ssts)?;
        event_loop.check_lease_health()?;
        Self::check_manual_deadline(pending.manual_deadline)?;
        let reservation = event_loop.actors.compaction_actor.finish_publication(
            &mut event_loop.state,
            &pending.token.input_ssts,
            &pending.token.output_ssts,
        );
        // From here the compaction is settled; any later failure only leaves
        // the cleared intent unmirrored.
        if let Some(active) = event_loop.publication.compaction_publication.get_mut() {
            active.expected_phase = CompactionPublishPhase::IntentCleared;
        }
        let hybrid_storage = event_loop.actors.cloud_coordinator.hybrid_storage.clone();
        if let (Some(hybrid), Some(token)) = (&hybrid_storage, reservation) {
            hybrid.compaction_completed_with_token(token, &output_sizes);
        }
        event_loop.check_lease_health()?;
        event_loop.actors.gc_actor.delete_ssts(
            &mut event_loop.state,
            &pending.token.input_ssts,
            hybrid_storage,
        );
        crate::failpoints::fail_point!("midge::compaction::after_input_sst_gc");
        event_loop.check_lease_health()?;
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
        let pending = event_loop
            .publication
            .compaction_publication
            .get_mut()
            .ok_or_else(|| {
                crate::common::MidgeError::Internal(
                    "compaction publication state disappeared before worker submission".to_string(),
                )
            })?;
        Self::check_manual_deadline(pending.manual_deadline)?;
        pending.expected_phase = phase;
        let checkpoint = (phase == CompactionPublishPhase::ManifestPublished
            && !event_loop.state.is_memory_mode()
            && event_loop
                .state
                .manifest_store
                .local_checkpoint_due(event_loop.state.manifest.edit_checkpoint_id))
        .then(|| {
            (
                std::sync::Arc::clone(&event_loop.state.manifest_store),
                (*event_loop.state.manifest).clone(),
            )
        });
        let task = CompactionPublishTask {
            metadata_cut: if !event_loop.pipeline.inline_flush_worker
                && event_loop
                    .actors
                    .cloud_coordinator
                    .cloud_metadata_storage
                    .is_some()
            {
                let deadline = pending.manual_deadline.unwrap_or_else(|| {
                    OperationDeadline::from_budget(event_loop.routing.runtime_response_timeout)
                });
                Some(
                    event_loop
                        .publication
                        .metadata_publication_lock
                        .capture_control_cut(
                            &event_loop.state.fs,
                            pending.token.writer_epoch,
                            &deadline,
                        )?,
                )
            } else {
                None
            },
            checkpoint,
            manual_deadline: pending.manual_deadline,
            token: pending.token.clone(),
            phase,
            outputs: if phase == CompactionPublishPhase::OutputDurable {
                pending.outputs.clone()
            } else {
                Vec::new()
            },
            fs: std::sync::Arc::clone(&event_loop.state.fs),
            hybrid_storage: event_loop.actors.cloud_coordinator.hybrid_storage.clone(),
            cloud_metadata_storage: event_loop
                .actors
                .cloud_coordinator
                .cloud_metadata_storage
                .clone(),
            metadata_publication_lock: event_loop.publication.metadata_publication_lock.clone(),
            lease_validity: event_loop.authority.fencing.lease_validity.clone(),
            lease_healthy: event_loop.authority.fencing.lease_healthy.clone(),
            leader_store: event_loop.authority.fencing.leader_store.clone(),
            leader_holder_id: event_loop.authority.fencing.leader_holder_id.clone(),
            publication_memory_limit: event_loop.actors.compaction_actor.compaction_memory_limit(),
            runtime_response_timeout: event_loop.routing.runtime_response_timeout,
        };
        event_loop.actors.compaction_publish_actor.submit(task)
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
        event_loop.authority.compaction_fence.degrade();
        event_loop.publish_snapshot();
        if let (Some(hybrid), Some(token)) = (
            &event_loop.actors.cloud_coordinator.hybrid_storage,
            reservation,
        ) {
            match Self::resident_output_sizes(event_loop.state.fs.as_ref(), output_ssts) {
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
        } else if event_loop.check_lease_health().is_ok() {
            // The local manifest batch is the durable authority. With no
            // remote authority to reconcile, its removed inputs are safe to
            // submit for local GC even if a later phase/checkpoint write
            // failed. The intent remains for idempotent restart recovery.
            event_loop
                .actors
                .gc_actor
                .delete_ssts(&mut event_loop.state, input_ssts, None);
        } else {
            event_loop.state.mark_persistence_anomaly();
            tracing::warn!("retaining compaction inputs after writer authority loss");
        }
    }

    fn resident_output_sizes(
        fs: &dyn crate::io::Fs,
        outputs: &[String],
    ) -> crate::common::MidgeResult<Vec<u64>> {
        outputs
            .iter()
            .map(|name| {
                let path = crate::sst::fs::sst_key(name);
                match fs.metadata(&path) {
                    Ok(metadata) if !metadata.is_dir => Ok(metadata.len),
                    Ok(_) => Err(crate::common::MidgeError::Internal(format!(
                        "published compaction output is not a regular file: {}",
                        path.0
                    ))),
                    // A remotely proved output can already have been evicted.
                    // Other metadata errors cannot establish that disk is free.
                    Err(crate::io::FsError::NotFound(_)) => Ok(0),
                    Err(error) => Err(error.into_midge()),
                }
            })
            .collect()
    }

    fn finish_successful_publication(
        event_loop: &mut EventLoop,
        pending: &PendingCompactionPublication,
    ) {
        Self::record_compaction_metrics(event_loop, &pending.token.output_ssts);
        Self::record_compaction_latency(event_loop, pending.token.started_at);
        let output_bytes = pending
            .added
            .iter()
            .try_fold(0_u64, |sum, file| sum.checked_add(file.size_bytes));
        if let Some(bytes) = output_bytes {
            event_loop
                .state
                .metadata_accounting()
                .compaction_committed(event_loop.state.metadata_medium(), bytes);
        } else {
            event_loop
                .state
                .metadata_accounting()
                .invalidate_missing_publication_start();
        }
        event_loop.evict_published_sst_cache(&pending.token.output_ssts);
        event_loop.publish_snapshot();
        event_loop
            .publication
            .compaction_publication
            .finish(&mut event_loop.publication.publication_gate);
        Self::complete_pending_waits(event_loop, true, None);
        event_loop.restore_publication_deferred_message();
        event_loop.schedule_next_flush_worker();
        event_loop.drain_auto_flush_memtables();
        event_loop.wake_write_stall_waiters();
        // Re-run the picker after every completed publication. Deep-level and
        // ordinary L0 debt must not wait for another flush or a 30s timer.
        event_loop
            .pipeline
            .background_compaction_schedule
            .mark_due();
    }

    fn finish_failed_start(
        event_loop: &mut EventLoop,
        request_id: u64,
        input_ssts: &[String],
        output_ssts: &[String],
        error: &crate::common::MidgeError,
    ) {
        let repair = event_loop
            .actors
            .compaction_actor
            .active_same_level_repair();
        // A failed intent append can still have reached the journal. Keep the
        // outputs and refuse further compaction until restart reconciles them.
        if event_loop
            .state
            .has_compaction_publication_intent(input_ssts, output_ssts)
        {
            event_loop.authority.compaction_fence.degrade();
        }
        let reservation = event_loop.actors.compaction_actor.finish_publication(
            &mut event_loop.state,
            input_ssts,
            output_ssts,
        );
        if let (Some(hybrid), Some(token)) = (
            &event_loop.actors.cloud_coordinator.hybrid_storage,
            reservation,
        ) {
            event_loop
                .actors
                .compaction_actor
                .settle_failed_compaction_reservation(hybrid.as_ref(), token);
        }
        Self::defer_failed_repair_retry(event_loop, repair, "publication start");
        let wait_error = error.replay();
        Self::record_publish_failure(event_loop, request_id, error);
        Self::complete_pending_waits(event_loop, false, Some(&wait_error));
        event_loop.drain_auto_flush_memtables();
        event_loop.wake_write_stall_waiters();
    }

    fn finish_failed_publication(event_loop: &mut EventLoop, error: &crate::common::MidgeError) {
        // Settling releases the gate here; nothing below reads it before the
        // deferred messages are restored.
        let Some(pending) = event_loop
            .publication
            .compaction_publication
            .finish(&mut event_loop.publication.publication_gate)
        else {
            tracing::warn!(%error, "compaction publication failed without pending state");
            return;
        };
        if pending.expected_phase == CompactionPublishPhase::IntentCleared {
            Self::finish_failed_intent_clear(event_loop, error);
            return;
        }
        let repair = event_loop
            .actors
            .compaction_actor
            .active_same_level_repair();
        let input_ssts = &pending.token.input_ssts;
        let output_ssts = &pending.token.output_ssts;
        let authoritative = Self::manifest_authority_switched(event_loop, input_ssts, output_ssts);
        let reservation = event_loop.actors.compaction_actor.finish_publication(
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
                event_loop.authority.compaction_fence.degrade();
            }
            if let (Some(hybrid), Some(token)) = (
                &event_loop.actors.cloud_coordinator.hybrid_storage,
                reservation,
            ) {
                event_loop
                    .actors
                    .compaction_actor
                    .settle_failed_compaction_reservation(hybrid.as_ref(), token);
            }
        }
        Self::defer_failed_repair_retry(event_loop, repair && !authoritative, "publication");
        let wait_error = error.replay();
        Self::record_publish_failure(event_loop, pending.token.request_id, error);
        Self::complete_pending_waits(event_loop, false, Some(&wait_error));
        event_loop.restore_publication_deferred_message();
        event_loop.schedule_next_flush_worker();
        event_loop.drain_auto_flush_memtables();
        event_loop.wake_write_stall_waiters();
    }

    fn defer_failed_repair_retry(event_loop: &mut EventLoop, repair: bool, phase: &'static str) {
        if !repair || event_loop.authority.compaction_fence.is_degraded() {
            return;
        }
        let retry_after = BACKGROUND_COMPACTION_CHECK_INTERVAL;
        event_loop
            .pipeline
            .background_compaction_schedule
            .defer_for(retry_after);
        tracing::info!(
            phase,
            retry_after_ms = u64::try_from(retry_after.as_millis()).unwrap_or(u64::MAX),
            "deferred failed overlap repair until the next maintenance retry"
        );
    }

    /// The manifest is published, inputs were handed to GC, and the local
    /// intent is cleared; only its mirror failed. Everything is settled except
    /// the remote record, so degrade instead of re-running the failure settle.
    fn finish_failed_intent_clear(event_loop: &mut EventLoop, error: &crate::common::MidgeError) {
        Self::record_compaction_failure(event_loop);
        tracing::error!(
            ?error,
            "failed to mirror cleared compaction publication intent"
        );
        event_loop.authority.compaction_fence.degrade();
        event_loop.publish_snapshot();
        let response_error = if matches!(error, MidgeError::Timeout(_)) {
            error.replay()
        } else {
            MidgeError::Internal(format!(
                "failed to mirror cleared compaction publication intent: {error}"
            ))
        };
        let wait_error = response_error.replay();
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
        let hybrid_storage = event_loop.actors.cloud_coordinator.hybrid_storage.clone();
        if let (Some(hybrid), Some(token)) = (&hybrid_storage, reservation) {
            let output_sizes =
                match Self::resident_output_sizes(event_loop.state.fs.as_ref(), output_ssts) {
                    Ok(sizes) => sizes,
                    Err(error) => {
                        return Self::record_publish_failure(event_loop, request_id, &error)
                    }
                };
            hybrid.compaction_completed_with_token(token, &output_sizes);
        }
        if event_loop.check_lease_health().is_ok() {
            event_loop.actors.gc_actor.delete_ssts(
                &mut event_loop.state,
                input_ssts,
                hybrid_storage,
            );
        } else {
            event_loop.state.mark_persistence_anomaly();
        }
        if let Err(error) = event_loop
            .state
            .clear_compaction_publication_intent(input_ssts, output_ssts)
        {
            return Self::record_publish_failure(event_loop, request_id, &error);
        }
        Self::record_compaction_metrics(event_loop, output_ssts);
        event_loop.evict_published_sst_cache(output_ssts);
        event_loop.publish_snapshot();
        true
    }

    /// Record worker start to publication for one compaction. A missing start
    /// time (the actor had no record of the generation) records nothing rather
    /// than a zero.
    fn record_compaction_latency(
        event_loop: &mut EventLoop,
        started_at: Option<std::time::Instant>,
    ) {
        if let Some(started_at) = started_at {
            let elapsed = started_at.elapsed();
            event_loop
                .state
                .diagnostics
                .record(|m| m.record_compaction_latency(elapsed));
        }
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

    fn record_publish_failure(
        event_loop: &mut EventLoop,
        request_id: u64,
        error: &crate::common::MidgeError,
    ) -> bool {
        Self::record_compaction_failure(event_loop);
        tracing::error!(notification_id = request_id, error = ?error, "failed to apply compaction to manifest");
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
        if !event_loop
            .actors
            .cloud_coordinator
            .metadata_mirror
            .pending_compactions
            .is_empty()
        {
            return;
        }

        if let Some(error) = completion_error {
            Self::fail_pending_compaction_waits(event_loop, error);
            return;
        }
        Self::expire_manual_compaction_waiters(event_loop);

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
            && event_loop
                .publication
                .publication_gate
                .deferred_messages_is_empty()
        {
            loop {
                let plan = match event_loop
                    .actors
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
                match event_loop.launch_compaction_disposition(plan) {
                    Ok(CompactionLaunch::Started) => {
                        emergent_scheduled = true;
                    }
                    Ok(CompactionLaunch::WaitingForMetadata) => return,
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
        event_loop
            .state
            .pending_compaction_waits
            .values()
            .any(|deadline| !deadline.is_expired())
    }

    fn check_manual_deadline(deadline: Option<OperationDeadline>) -> MidgeResult<()> {
        if deadline.is_some_and(|deadline| deadline.is_expired()) {
            return Err(MidgeError::Timeout(
                "manual compaction exceeded the caller deadline".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn expire_manual_compaction_waiters(event_loop: &mut EventLoop) {
        let ended: Vec<_> = event_loop
            .state
            .pending_compaction_waits
            .iter()
            .filter_map(|(&id, deadline)| {
                (deadline.is_expired()
                    || event_loop
                        .routing
                        .router
                        .request_deadline(id, event_loop.routing.runtime_response_timeout)
                        .is_none())
                .then_some(id)
            })
            .collect();
        for request_id in ended {
            event_loop
                .state
                .pending_compaction_waits
                .remove(&request_id);
            if event_loop
                .routing
                .router
                .request_deadline(request_id, event_loop.routing.runtime_response_timeout)
                .is_some()
            {
                event_loop.routing.router.complete(RuntimeResponse::Error {
                    request_id,
                    error: MidgeError::Timeout(
                        "manual compaction exceeded the caller deadline".into(),
                    ),
                });
            }
        }
    }

    pub(super) fn manual_compaction_wait_timeout(event_loop: &EventLoop) -> Option<Duration> {
        event_loop
            .state
            .pending_compaction_waits
            .values()
            .map(OperationDeadline::remaining)
            .min()
    }

    pub(super) fn complete_idle_compaction_waits(event_loop: &mut EventLoop, include_manual: bool) {
        Self::expire_manual_compaction_waiters(event_loop);
        if !include_manual
            || !event_loop
                .actors
                .cloud_coordinator
                .metadata_mirror
                .pending_compactions
                .is_empty()
        {
            return;
        }
        for request_id in std::mem::take(&mut event_loop.state.pending_compaction_waits).into_keys()
        {
            event_loop
                .routing
                .router
                .complete(RuntimeResponse::Ok { request_id });
        }
    }

    pub(super) fn fail_pending_compaction_waits(
        event_loop: &mut EventLoop,
        error: &crate::common::MidgeError,
    ) {
        for request_id in std::mem::take(&mut event_loop.state.pending_compaction_waits).into_keys()
        {
            event_loop.routing.router.complete(RuntimeResponse::Error {
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
        while let Ok(completion) = event_loop
            .publication
            .compaction_publish_result_rx
            .try_recv()
        {
            Self::handle_publication_completion(event_loop, completion);
        }
    }

    fn drain_inline_publish_worker(event_loop: &mut EventLoop) {
        if !event_loop.actors.compaction_publish_actor.is_inline() {
            return;
        }
        while event_loop.actors.compaction_publish_actor.is_inflight() {
            let Ok(completion) = event_loop
                .publication
                .compaction_publish_result_rx
                .try_recv()
            else {
                break;
            };
            Self::handle_publication_completion(event_loop, completion);
        }
    }
}
