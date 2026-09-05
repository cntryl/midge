//! Compaction Actor - handles SST compaction
//!
//! Responsible for:
//! - Detecting when compaction is needed
//! - Planning and executing compaction jobs
//! - Merging SST files across levels

use super::super::state::RuntimeState;
use crate::common::{MidgeError, MidgeResult};
use crate::compaction::{Compactor, LeveledCompactionConfig};
use crate::runtime::hybrid_persistence::HybridPersistence;
use crate::runtime::{next_request_id, RuntimeMsg};
use crate::sst::SstFactory;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

type PreparedRemoteOutputs = Arc<
    parking_lot::Mutex<
        std::collections::HashMap<
            String,
            (
                crate::runtime::FileMeta,
                crate::storage::hybrid::backend::GuardedObjectProof,
            ),
        >,
    >,
>;

/// Actor handling SST compaction
pub struct CompactionActor {
    /// Whether a compaction is currently running
    compaction_running: bool,
    /// SST factory for creating readers/writers
    sst_factory: Arc<dyn SstFactory>,
    /// Compaction strategy
    compactor: Compactor,
    target_sst_size: usize,
    compaction_memory_limit: usize,
    /// Last column family selected for the global compaction slot. Planning
    /// resumes after this ID so a perpetually eligible low ID cannot starve
    /// unrelated families.
    last_scheduled_cf: Option<crate::types::ColumnFamilyId>,
    /// Accounting reservation for the one active compaction. The actor
    /// serializes compactions, but the token still prevents a delayed terminal
    /// message from settling a later operation.
    storage_reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    /// Exact manifest inputs owned by the active operation.
    active_input_ssts: Vec<String>,
    /// Kept through completion so failed publication can charge its residue.
    active_output_generation: Option<(u32, u32, u64)>,
    /// Cooperative cancellation flag for the active background worker.
    worker_cancel: Arc<AtomicBool>,
    /// Active background worker, joined before runtime shutdown completes.
    worker_handle: Option<JoinHandle<Vec<String>>>,
    /// Exact terminal worker error transferred out-of-band so the public
    /// runtime message shape remains unchanged.
    worker_error: Arc<std::sync::Mutex<Option<MidgeError>>>,
    /// Metadata and exact provider identities proved before each remote
    /// partition's local staging file was released.
    prepared_remote_outputs: PreparedRemoteOutputs,
}

impl CompactionActor {
    #[cfg(test)]
    pub fn new(sst_factory: Arc<dyn SstFactory>) -> Self {
        Self::new_with_config(sst_factory, LeveledCompactionConfig::default())
    }

    pub fn new_with_config(
        sst_factory: Arc<dyn SstFactory>,
        config: LeveledCompactionConfig,
    ) -> Self {
        Self {
            compaction_running: false,
            sst_factory,
            compactor: Compactor::with_config(config),
            target_sst_size: crate::compaction::DEFAULT_TARGET_SST_SIZE,
            compaction_memory_limit: crate::compaction::DEFAULT_COMPACTION_MEMORY_LIMIT,
            last_scheduled_cf: None,
            storage_reservation: None,
            active_input_ssts: Vec::new(),
            active_output_generation: None,
            worker_cancel: Arc::new(AtomicBool::new(false)),
            worker_handle: None,
            worker_error: Arc::new(std::sync::Mutex::new(None)),
            prepared_remote_outputs: Arc::new(parking_lot::Mutex::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    pub fn set_l0_file_count_threshold(&mut self, threshold: usize) {
        self.compactor.config.l0_file_count_threshold = threshold.max(1);
    }

    pub(crate) fn set_execution_limits(
        &mut self,
        target_sst_size: usize,
        compaction_memory_limit: usize,
    ) {
        self.target_sst_size = target_sst_size.max(1);
        self.compaction_memory_limit = compaction_memory_limit;
    }

    pub(crate) fn target_sst_size(&self) -> usize {
        self.target_sst_size
    }

    pub(crate) fn compaction_memory_limit(&self) -> usize {
        self.compaction_memory_limit
    }

    #[cfg(test)]
    pub fn l0_file_count_threshold(&self) -> usize {
        self.compactor.config.l0_file_count_threshold
    }

    /// Open an SST reader using the actor's configured `SstFactory`
    #[cfg(test)]
    pub fn open_sst_reader(
        &self,
        path: &std::path::Path,
    ) -> crate::common::MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        self.sst_factory.open(path)
    }

    pub fn check_compaction(
        &mut self,
        state: &RuntimeState,
    ) -> MidgeResult<Option<crate::compaction::CompactionPlan>> {
        self.check_compaction_with_mode(state, true)
    }

    pub fn check_manual_compaction(
        &mut self,
        state: &RuntimeState,
    ) -> MidgeResult<Option<crate::compaction::CompactionPlan>> {
        self.check_compaction_with_mode(state, false)
    }

    fn check_compaction_with_mode(
        &mut self,
        state: &RuntimeState,
        respect_background_enabled: bool,
    ) -> MidgeResult<Option<crate::compaction::CompactionPlan>> {
        // If compaction is disabled via runtime configuration, skip checks
        if respect_background_enabled && !state.compaction_enabled() {
            tracing::debug!("compaction disabled in runtime state");
            return Ok(None);
        }

        if self.compaction_running {
            return Ok(None);
        }

        // Count files per level for logging
        let mut level_counts = [0usize; 7];
        for file in &state.manifest.files {
            let level = file.level as usize;
            if level < level_counts.len() {
                level_counts[level] += 1;
            }
        }

        tracing::debug!(
            l0 = level_counts[0],
            l1 = level_counts[1],
            l2 = level_counts[2],
            "Compaction check"
        );

        let cf_ids = self.round_robin_cf_ids(state);

        // Critical L0 debt always wins the global worker, but rotates across
        // affected families so one hot tenant cannot monopolize recovery.
        for cf_id in &cf_ids {
            if state.has_critical_l0_debt(*cf_id) {
                if let Some(plan) =
                    self.compactor
                        .pick_l0_compaction(&state.manifest.files, *cf_id, true)?
                {
                    return Ok(Some(self.decorate_plan(state, plan)));
                }
            }
        }

        // Once no family is critical, pay down the deepest overfull level
        // before creating more downstream debt from ordinary L0 work.
        let mut deepest_plan: Option<crate::compaction::CompactionPlan> = None;
        for cf_id in &cf_ids {
            let candidate = self
                .compactor
                .pick_deepest_inner_compaction(&state.manifest.files, *cf_id)?;
            if candidate.as_ref().is_some_and(|plan| {
                deepest_plan
                    .as_ref()
                    .is_none_or(|current| plan.source_level > current.source_level)
            }) {
                deepest_plan = candidate;
            }
        }
        if let Some(plan) = deepest_plan {
            return Ok(Some(self.decorate_plan(state, plan)));
        }

        // Manual compaction drains every L0 generation. Background work keeps
        // the configured soft threshold but is still pre-empted by critical
        // debt above.
        let force_l0 = !respect_background_enabled;
        for cf_id in &cf_ids {
            if let Some(plan) =
                self.compactor
                    .pick_l0_compaction(&state.manifest.files, *cf_id, force_l0)?
            {
                return Ok(Some(self.decorate_plan(state, plan)));
            }
        }

        if !respect_background_enabled {
            for cf_id in cf_ids {
                if self
                    .compactor
                    .compaction_debt_is_clear(&state.manifest.files, cf_id)?
                {
                    continue;
                }
                return Err(MidgeError::Internal(format!(
                    "compaction debt remains for column family {cf_id}, but no valid plan exists"
                )));
            }
        }

        Ok(None)
    }

    fn round_robin_cf_ids(&self, state: &RuntimeState) -> Vec<crate::types::ColumnFamilyId> {
        let mut cf_ids: Vec<u32> = state.column_families.keys().copied().collect();
        cf_ids.sort_unstable();
        if let Some(last_scheduled) = self.last_scheduled_cf {
            let next_index = cf_ids
                .iter()
                .position(|cf_id| *cf_id > last_scheduled)
                .unwrap_or(0);
            cf_ids.rotate_left(next_index);
        }
        cf_ids
    }

    fn decorate_plan(
        &mut self,
        state: &RuntimeState,
        mut plan: crate::compaction::CompactionPlan,
    ) -> crate::compaction::CompactionPlan {
        plan.snapshot_horizon = state.oldest_active_snapshot_sequence();
        plan.target_sst_size = self.target_sst_size;
        plan.compaction_memory_limit = self.compaction_memory_limit;
        self.last_scheduled_cf = Some(plan.cf_id);
        plan
    }

    /// Execute a compaction plan
    ///
    /// If SBA is available, notifies it before and after compaction for disk accounting.
    pub fn run_compaction(
        &mut self,
        state: &mut RuntimeState,
        plan: &crate::compaction::CompactionPlan,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
        worker_msg_tx: Option<crossbeam::channel::Sender<RuntimeMsg>>,
    ) -> MidgeResult<Vec<String>> {
        let mut plan = plan.clone();
        if let Some(hybrid) = sba.filter(|hybrid| hybrid.ephemeral_sst_cache_enabled()) {
            let target_limit =
                usize::try_from(hybrid.budget_snapshot().max_local_bytes / 8).unwrap_or(usize::MAX);
            if target_limit == 0 {
                return Err(MidgeError::ResourceLimit(
                    "local storage budget cannot hold a compaction partition".into(),
                ));
            }
            plan.target_sst_size = plan.target_sst_size.min(target_limit);
        }
        self.prepare_compaction(state, &plan, sba)?;
        if let Some(tx) = worker_msg_tx {
            let result = self.run_async_compaction(state, tx, &plan, sba.cloned());
            if result.is_err() {
                self.abort_compaction(state, &plan, sba, result.as_ref().err());
            }
            return result;
        }

        let result = self.run_sync_compaction(state, &plan, sba);
        if result.is_err() {
            self.abort_compaction(state, &plan, sba, result.as_ref().err());
        }
        result
    }

    fn abort_compaction(
        &mut self,
        state: &mut RuntimeState,
        plan: &crate::compaction::CompactionPlan,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
        error: Option<&MidgeError>,
    ) {
        self.finish_active_bookkeeping(state, &plan.input_files);
        if let (Some(hybrid), Some(token)) = (sba, self.storage_reservation.take()) {
            self.settle_compaction_error_reservation(state, hybrid, token, error);
        }
    }

    pub(crate) fn settle_compaction_error_reservation(
        &self,
        state: &RuntimeState,
        hybrid: &crate::storage::HybridStorage,
        token: crate::storage::hybrid::actor::StorageReservationToken,
        error: Option<&MidgeError>,
    ) {
        if hybrid.ephemeral_sst_cache_enabled()
            && error.is_some()
            && !self.sst_factory.compaction_scratch_cleanup_verified()
        {
            // Error type cannot prove deletion: cancellation, resource limits
            // and corrupt inputs can all follow partial scratch writes.
            tracing::warn!(
                ?token,
                "retaining compaction staging allowance because scratch cleanup is unverified"
            );
        } else {
            self.settle_failed_compaction_reservation(state, hybrid, token);
        }
    }

    pub(crate) fn settle_failed_compaction_reservation(
        &self,
        state: &RuntimeState,
        hybrid: &crate::storage::HybridStorage,
        token: crate::storage::hybrid::actor::StorageReservationToken,
    ) {
        if !hybrid.ephemeral_sst_cache_enabled() {
            hybrid.compaction_aborted_with_token(token);
            return;
        }
        match self.retained_output_bytes(&state.sst_dir) {
            Ok(bytes) => hybrid.compaction_inputs_retained_with_token(token, &[bytes]),
            Err(error) => {
                // An unreadable directory cannot prove cleanup. Leaving the
                // token charged is a safe capacity leak until startup readback.
                tracing::warn!(%error, ?token, "retaining compaction staging allowance because residue cannot be measured");
            }
        }
    }

    fn retained_output_bytes(&self, output_dir: &std::path::Path) -> MidgeResult<u64> {
        let generation = self.active_output_generation.ok_or_else(|| {
            MidgeError::Internal("failed compaction output identity is unavailable".into())
        })?;
        let entries = match std::fs::read_dir(output_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error.into()),
        };
        let mut bytes = 0_u64;
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let name = name.strip_suffix(".tmp").unwrap_or(&name);
            let Some((cf, level, output_generation, _)) =
                crate::sst::parse_compaction_file_name(name)
            else {
                continue;
            };
            if (cf, level, output_generation) != generation {
                continue;
            }
            if !entry.file_type()?.is_file() {
                return Err(MidgeError::Internal(
                    "compaction residue is not a regular file".into(),
                ));
            }
            bytes = bytes.checked_add(entry.metadata()?.len()).ok_or_else(|| {
                MidgeError::ResourceLimit("compaction residue size overflow".into())
            })?;
        }
        Ok(bytes)
    }

    /// Handle compaction completion
    pub fn handle_complete(
        &mut self,
        state: &mut RuntimeState,
        input_ssts: &[String],
        output_ssts: &[String],
    ) -> Option<crate::storage::hybrid::actor::StorageReservationToken> {
        // Invariant: completion only clears in-memory "running" state. The
        // actual authority switch happens when manifest publication removes the
        // old SSTs and adds the replacement set.
        self.finish_active_bookkeeping(state, input_ssts);
        let _ = self.join_worker();

        tracing::info!(
            input_count = input_ssts.len(),
            output_count = output_ssts.len(),
            "Compaction completed"
        );

        self.storage_reservation.take()
    }

    /// Cancel and join any active worker before runtime shutdown releases its
    /// storage lease. The worker checks this flag before and after output
    /// finalization, so it cannot publish or leave a staged replacement after
    /// shutdown has begun.
    pub fn cancel_and_join_worker(
        &mut self,
        state: &mut RuntimeState,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) {
        self.worker_cancel.store(true, Ordering::Release);
        let staged_outputs = self.join_worker();
        if !self.compaction_running {
            return;
        }

        let mut cleanup_failed = false;
        for output in staged_outputs {
            let path = state.sst_dir.join(&output);
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    tracing::debug!(file = %path.display(), "removed canceled compaction output");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    cleanup_failed = true;
                    state.mark_persistence_anomaly();
                    tracing::warn!(file = %path.display(), %error, "retaining canceled compaction output after cleanup failure");
                }
            }
        }

        let active_inputs = self.active_input_ssts.clone();
        self.finish_active_bookkeeping(state, &active_inputs);
        if let (Some(hybrid), Some(token)) = (sba, self.storage_reservation.take()) {
            if cleanup_failed && hybrid.ephemeral_sst_cache_enabled() {
                tracing::warn!(
                    ?token,
                    "retaining canceled compaction staging allowance after cleanup failure"
                );
            } else {
                self.settle_failed_compaction_reservation(state, hybrid, token);
            }
        }
    }

    fn join_worker(&mut self) -> Vec<String> {
        let Some(handle) = self.worker_handle.take() else {
            return Vec::new();
        };

        if let Ok(outputs) = handle.join() {
            tracing::debug!("compaction worker joined");
            outputs
        } else {
            tracing::warn!("compaction worker panicked during join");
            Vec::new()
        }
    }

    fn finish_active_bookkeeping(&mut self, state: &mut RuntimeState, fallback_inputs: &[String]) {
        if !self.compaction_running {
            return;
        }
        let inputs = if self.active_input_ssts.is_empty() {
            fallback_inputs
        } else {
            &self.active_input_ssts
        };
        state
            .compaction
            .compacting_ssts
            .retain(|name| !inputs.contains(name));
        if state
            .active_compactions
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |active| active.checked_sub(1),
            )
            .is_err()
        {
            tracing::warn!("active compaction accounting was already settled");
        }
        self.active_input_ssts.clear();
        self.compaction_running = false;
    }

    fn prepare_compaction(
        &mut self,
        state: &mut RuntimeState,
        plan: &crate::compaction::CompactionPlan,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<()> {
        if self.compaction_running {
            return Err(crate::common::MidgeError::WriteStall(
                "compaction already in progress".to_string(),
            ));
        }

        self.prepared_remote_outputs.lock().clear();
        self.storage_reservation =
            if let Some(hybrid) = sba.filter(|hybrid| hybrid.ephemeral_sst_cache_enabled()) {
                Some(
                    hybrid
                        .reserve_compaction_staging_with_token(
                            hybrid.budget_snapshot().max_local_bytes / 2,
                        )
                        .map_err(|pressure| {
                            MidgeError::WriteStall(format!(
                                "local compaction staging budget unavailable: {pressure:?}"
                            ))
                        })?,
                )
            } else {
                None
            };

        self.compaction_running = true;
        self.active_output_generation = Some((plan.cf_id, plan.target_level, plan.output_seq));
        self.active_input_ssts.clone_from(&plan.input_files);

        state
            .compaction
            .compacting_ssts
            .extend(plan.input_files.clone());

        let input_sizes: Vec<u64> = state
            .manifest
            .files
            .iter()
            .filter(|f| plan.input_files.contains(&f.name))
            .map(|f| f.size_bytes)
            .collect();

        if self.storage_reservation.is_none() {
            self.storage_reservation =
                sba.map(|hybrid| hybrid.compaction_planned_with_token(&input_sizes));
        }

        tracing::info!(
            input_count = plan.input_files.len(),
            source_level = plan.source_level,
            target_level = plan.target_level,
            cf_id = plan.cf_id,
            "Compaction started"
        );

        state
            .active_compactions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn prepare_for_completion_test(
        &mut self,
        state: &mut RuntimeState,
        input_ssts: &[String],
    ) -> MidgeResult<()> {
        self.prepare_for_completion_with_storage_test(state, input_ssts, None)
    }

    #[cfg(test)]
    pub(crate) fn prepare_for_completion_with_storage_test(
        &mut self,
        state: &mut RuntimeState,
        input_ssts: &[String],
        storage: Option<&Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<()> {
        let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1);
        plan.input_files = input_ssts.to_vec();
        self.prepare_compaction(state, &plan, storage)
    }

    fn run_sync_compaction(
        &mut self,
        state: &RuntimeState,
        plan: &crate::compaction::CompactionPlan,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<Vec<String>> {
        let output_ssts = Self::execute_with_storage(
            plan,
            self.sst_factory.as_ref(),
            &state.sst_dir,
            None,
            sba,
            &self.prepared_remote_outputs,
        )?;

        tracing::info!(
            input_count = plan.input_files.len(),
            output_count = output_ssts.len(),
            "Compaction completed"
        );
        Ok(output_ssts)
    }

    fn execute_with_storage(
        plan: &crate::compaction::CompactionPlan,
        factory: &dyn SstFactory,
        output_dir: &std::path::Path,
        abort_check: Option<&dyn Fn() -> bool>,
        storage: Option<&Arc<crate::storage::HybridStorage>>,
        prepared: &PreparedRemoteOutputs,
    ) -> MidgeResult<Vec<String>> {
        let sink = |name: &str,
                    path: &std::path::Path,
                    budget: &crate::common::resource_budget::ResourceBudget| {
            Self::prepare_remote_partition(
                storage.expect("ephemeral storage"),
                prepared,
                plan.cf_id,
                plan.target_level,
                name,
                path,
                budget,
            )
        };
        let output_sink = storage
            .filter(|hybrid| hybrid.ephemeral_sst_cache_enabled())
            .map(|_| &sink as &crate::compaction::CompactionOutputSink<'_>);
        crate::compaction::execute_compaction_with_output_sink(
            plan,
            factory,
            output_dir,
            abort_check,
            output_sink,
            storage
                .filter(|hybrid| hybrid.ephemeral_sst_cache_enabled())
                .map(|hybrid| {
                    usize::try_from(hybrid.budget_snapshot().max_local_bytes / 4)
                        .unwrap_or(usize::MAX)
                }),
        )
    }

    fn run_async_compaction(
        &mut self,
        state: &RuntimeState,
        tx: crossbeam::channel::Sender<RuntimeMsg>,
        plan: &crate::compaction::CompactionPlan,
        hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<Vec<String>> {
        let sst_factory = Arc::clone(&self.sst_factory);
        let sst_dir = state.sst_dir.clone();
        let input_files = plan.input_files.clone();
        let plan_clone = plan.clone();
        let epoch = std::sync::Arc::clone(&state.ingest_epoch);
        self.worker_cancel.store(false, Ordering::Release);
        let worker_cancel = Arc::clone(&self.worker_cancel);
        let worker_error = Arc::clone(&self.worker_error);
        let prepared_outputs = Arc::clone(&self.prepared_remote_outputs);
        store_compaction_worker_error(&worker_error, None);
        let job_id = next_request_id()?;

        let worker = std::thread::Builder::new()
            .name(format!("midge-compaction-{job_id}"))
            .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let my_epoch = epoch.load(std::sync::atomic::Ordering::SeqCst);
                let abort_check = || {
                    worker_cancel.load(Ordering::Acquire)
                        || epoch.load(std::sync::atomic::Ordering::SeqCst) != my_epoch
                };
                let result = Self::execute_with_storage(
                    &plan_clone,
                    sst_factory.as_ref(),
                    &sst_dir,
                    Some(&abort_check),
                    hybrid_storage.as_ref(),
                    &prepared_outputs,
                );

                let (output_ssts, error) = match result {
                    Ok(v) => (v, None),
                    Err(e) => {
                        if matches!(e, MidgeError::Aborted(_)) {
                            let new_epoch = epoch.load(std::sync::atomic::Ordering::SeqCst);
                            tracing::info!(
                                component = "compaction",
                                invariant = "cooperative_cancellation",
                                job_id = job_id,
                                old_epoch = my_epoch,
                                new_epoch = new_epoch,
                                input_files = ?input_files,
                                "compaction: aborting due to ingest epoch change (job_id={}, old_epoch={}, new_epoch={})",
                                job_id, my_epoch, new_epoch
                            );
                        } else {
                            tracing::warn!(
                                component = "compaction",
                                job_id = job_id,
                                error = %e,
                                input_files = ?input_files,
                                "compaction worker aborted or failed"
                            );
                        }
                        (Vec::new(), Some(e))
                    }
                };

                (output_ssts, error)
            }));

            let (output_ssts, error) = match result {
                Ok(result) => result,
                Err(panic_info) => {
                    tracing::error!(
                        component = "compaction",
                        job_id = job_id,
                        input_files = ?input_files,
                        panic_info = ?panic_info,
                        "compaction worker thread panicked; returning empty output to unblock event loop"
                    );
                    (Vec::new(), Some(compaction_worker_panic_error()))
                }
            };
            let succeeded = error.is_none();
            store_compaction_worker_error(&worker_error, error.as_ref());

            Self::notify_worker_completion(&tx, &plan_clone, input_files, &output_ssts, succeeded, job_id);
            output_ssts
        })
            .map_err(|error| MidgeError::Internal(format!("spawn compaction worker: {error}")))?;
        self.worker_handle = Some(worker);

        Ok(Vec::new())
    }

    fn notify_worker_completion(
        tx: &crossbeam::channel::Sender<RuntimeMsg>,
        plan: &crate::compaction::CompactionPlan,
        input_ssts: Vec<String>,
        output_ssts: &[String],
        succeeded: bool,
        job_id: u64,
    ) {
        let Ok(request_id) = next_request_id() else {
            tracing::error!(
                component = "compaction",
                job_id,
                "compaction worker could not allocate completion request ID"
            );
            return;
        };
        if tx
            .send(RuntimeMsg::CompactionComplete {
                request_id,
                input_ssts,
                output_ssts: output_ssts.to_vec(),
                cf_id: plan.cf_id,
                target_level: plan.target_level,
                succeeded,
            })
            .is_err()
        {
            tracing::warn!(
                component = "compaction",
                job_id,
                "compaction completion receiver closed"
            );
        }
    }

    pub(crate) fn take_worker_error(&mut self) -> Option<MidgeError> {
        self.worker_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    pub(crate) fn prepared_remote_output(
        &self,
        name: &str,
    ) -> Option<(
        crate::runtime::FileMeta,
        crate::storage::hybrid::backend::GuardedObjectProof,
    )> {
        self.prepared_remote_outputs.lock().get(name).cloned()
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_remote_partition(
        hybrid: &crate::storage::HybridStorage,
        prepared: &PreparedRemoteOutputs,
        cf_id: u32,
        level: u32,
        name: &str,
        path: &std::path::Path,
        budget: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<()> {
        let summary =
            crate::sst::fs::SstFileIo::summarize_with_real_fs_for_compaction(path, budget.clone())?;
        let bytes_len = usize::try_from(summary.size_bytes).map_err(|_| {
            MidgeError::ResourceLimit("compaction partition exceeds addressable memory".into())
        })?;
        let _reservation = budget.reserve(bytes_len, "remote compaction partition upload")?;
        let bytes = std::fs::read(path)?;
        if bytes.len() != bytes_len {
            return Err(MidgeError::Corruption(
                "compaction partition changed before upload".into(),
            ));
        }
        let crc = crc32c::crc32c(&bytes);
        let proof = hybrid.write_sst_object_with_proof(
            name,
            bytes,
            &crate::common::OperationDeadline::unbounded(),
        )?;
        let metadata = crate::runtime::FileMeta {
            name: name.to_string(),
            level,
            size_bytes: summary.size_bytes,
            content_crc32c: Some(crc),
            cf_id,
            smallest_key: Some(summary.smallest_key),
            largest_key: Some(summary.largest_key),
            smallest_seq: Some(summary.smallest_seq),
            largest_seq: Some(summary.largest_seq),
            key_bounds_complete: true,
        };
        // Input authority has not changed. An interrupted job may leak this
        // immutable remote object, but it cannot lose a committed input.
        std::fs::remove_file(path)?;
        prepared.lock().insert(name.to_string(), (metadata, proof));
        crate::failpoints::fail_point!("midge::compaction::after_remote_partition_evicted", |_| {
            Err(MidgeError::Internal(
                "failpoint: compaction interrupted after remote partition eviction".into(),
            ))
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_worker_error_for_test(&mut self, error: MidgeError) {
        *self
            .worker_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
    }
}

fn compaction_worker_panic_error() -> MidgeError {
    MidgeError::Internal("compaction worker panicked".to_string())
}

fn store_compaction_worker_error(
    slot: &std::sync::Mutex<Option<MidgeError>>,
    error: Option<&MidgeError>,
) {
    *slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = error.map(MidgeError::replay);
}

impl Clone for CompactionActor {
    fn clone(&self) -> Self {
        Self {
            compaction_running: self.compaction_running,
            sst_factory: Arc::clone(&self.sst_factory),
            compactor: Compactor::with_config(self.compactor.config.clone()),
            target_sst_size: self.target_sst_size,
            compaction_memory_limit: self.compaction_memory_limit,
            last_scheduled_cf: self.last_scheduled_cf,
            storage_reservation: self.storage_reservation,
            active_input_ssts: self.active_input_ssts.clone(),
            active_output_generation: self.active_output_generation,
            worker_cancel: Arc::clone(&self.worker_cancel),
            worker_handle: None,
            worker_error: Arc::clone(&self.worker_error),
            prepared_remote_outputs: Arc::clone(&self.prepared_remote_outputs),
        }
    }
}

impl Drop for CompactionActor {
    fn drop(&mut self) {
        self.worker_cancel.store(true, Ordering::Release);
        let _ = self.join_worker();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_keep_failed_output_bytes_charged_when_compaction_residue_remains() -> MidgeResult<()>
    {
        // Arrange
        let temp = tempfile::tempdir()?;
        let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
        let fs = Arc::new(crate::io::RealFs::new(&state.sst_dir)?);
        let mut actor = CompactionActor::new(Arc::new(crate::sst::FsSstFactoryIo::new(fs, 4096)));
        let local = Arc::new(crate::storage::filesystem::FileSystem::new(
            temp.path().join("local"),
        )?);
        let cloud = Arc::new(crate::storage::filesystem::FileSystem::new(
            temp.path().join("cloud"),
        )?);
        let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::new(1_000),
        ));
        hybrid.enable_ephemeral_sst_cache(1_000);
        let plan = crate::compaction::CompactionPlan::new(0, 0, 1).with_output_seq(42);
        actor.prepare_compaction(&mut state, &plan, Some(&hybrid))?;
        let residue = state
            .sst_dir
            .join(crate::sst::compaction_file_name(0, 1, 42, 0));
        std::fs::write(&residue, [0_u8; 300])?;

        // Act
        actor.abort_compaction(&mut state, &plan, Some(&hybrid), None);

        // Assert
        assert!(residue.exists());
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 300);
        assert!(hybrid.admit_local_wal_bytes(701).is_err());
        Ok(())
    }

    struct BlockingFinalizeFactory {
        delegate: Arc<dyn crate::sst::SstFactory>,
        reached_finalize: std::sync::mpsc::SyncSender<()>,
        cancel_probe: Arc<std::sync::Mutex<Option<Arc<AtomicBool>>>>,
    }

    impl crate::sst::SstFactory for BlockingFinalizeFactory {
        fn create(&self) -> MidgeResult<Box<dyn crate::sst::traits::DynSstWriter>> {
            Ok(Box::new(BlockingFinalizeWriter {
                inner: self.delegate.create()?,
                reached_finalize: self.reached_finalize.clone(),
                cancel_probe: Arc::clone(&self.cancel_probe),
            }))
        }

        fn open(
            &self,
            path: &std::path::Path,
        ) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
            self.delegate.open(path)
        }
    }

    struct BlockingFinalizeWriter {
        inner: Box<dyn crate::sst::traits::DynSstWriter>,
        reached_finalize: std::sync::mpsc::SyncSender<()>,
        cancel_probe: Arc<std::sync::Mutex<Option<Arc<AtomicBool>>>>,
    }

    impl crate::sst::traits::DynSstWriter for BlockingFinalizeWriter {
        fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
            self.inner.add(key, value)
        }

        fn add_with_meta(
            &mut self,
            key: &[u8],
            value: Option<&[u8]>,
            seq: u64,
            op_type: u8,
            expiration: Option<u64>,
        ) -> MidgeResult<()> {
            self.inner
                .add_with_meta(key, value, seq, op_type, expiration)
        }

        fn add_sorted_with_meta(
            &mut self,
            key: &[u8],
            value: Option<&[u8]>,
            seq: u64,
            op_type: u8,
            expiration: Option<u64>,
        ) -> MidgeResult<()> {
            self.inner
                .add_sorted_with_meta(key, value, seq, op_type, expiration)
        }

        fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) -> MidgeResult<()> {
            self.inner.add_range_tombstone(start, end, seq)
        }

        fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
            let Self {
                inner,
                reached_finalize,
                cancel_probe,
            } = *self;
            let bytes = inner.finish_bytes()?;
            let _ = reached_finalize.try_send(());
            let cancel = cancel_probe
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .cloned()
                .expect("install worker cancellation probe before compaction");
            while !cancel.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            Ok(bytes)
        }
    }

    fn make_l0_file(
        name: &str,
        cf_id: u32,
        smallest_key: &[u8],
        largest_key: &[u8],
    ) -> crate::metadata::FileMeta {
        crate::metadata::FileMeta {
            name: name.to_string(),
            level: 0,
            size_bytes: 1,
            cf_id,
            smallest_key: Some(smallest_key.to_vec()),
            largest_key: Some(largest_key.to_vec()),
            ..Default::default()
        }
    }

    fn make_level_file(
        name: &str,
        cf_id: u32,
        level: u32,
        size_bytes: u64,
        smallest_key: &[u8],
        largest_key: &[u8],
    ) -> crate::metadata::FileMeta {
        crate::metadata::FileMeta {
            name: name.to_string(),
            level,
            size_bytes,
            cf_id,
            smallest_key: Some(smallest_key.to_vec()),
            largest_key: Some(largest_key.to_vec()),
            ..Default::default()
        }
    }

    fn create_test_compaction_actor() -> CompactionActor {
        // Use the modern io::Fs-backed factory
        let fs = Arc::new(crate::io::MockFs::new());
        let sst_factory = Arc::new(crate::sst::FsSstFactoryIo::new(fs, 64 * 1024));
        CompactionActor::new(sst_factory)
    }

    fn create_test_compaction_actor_with_config(
        config: LeveledCompactionConfig,
    ) -> CompactionActor {
        let fs = Arc::new(crate::io::MockFs::new());
        let sst_factory = Arc::new(crate::sst::FsSstFactoryIo::new(fs, 64 * 1024));
        CompactionActor::new_with_config(sst_factory, config)
    }

    #[test]
    fn should_initialize_compaction_actor_with_no_running_compaction() {
        // Arrange
        // (no setup needed)

        // Act
        let actor = create_test_compaction_actor();

        // Assert
        assert!(!actor.compaction_running);
    }

    #[test]
    fn should_return_none_when_compaction_already_running() {
        // Arrange - a compaction-eligible state, but the actor is already mid-compaction
        let mut actor = create_test_compaction_actor();
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.set_compaction_enabled(true);
        state.manifest.files.extend([
            make_l0_file("cf0_0001.sst", 0, b"a00", b"a99"),
            make_l0_file("cf0_0002.sst", 0, b"b00", b"b99"),
            make_l0_file("cf0_0003.sst", 0, b"c00", b"c99"),
            make_l0_file("cf0_0004.sst", 0, b"d00", b"d99"),
        ]);
        actor.compaction_running = true;

        // Act - the real check must not schedule a second concurrent compaction
        let plan = actor.check_compaction(&state).expect("compaction check");

        // Assert
        assert!(plan.is_none());
    }

    #[test]
    fn should_set_running_flag_when_compaction_starts() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.set_compaction_enabled(true);
        state.manifest.files.extend([
            make_l0_file("cf0_0001.sst", 0, b"a00", b"a99"),
            make_l0_file("cf0_0002.sst", 0, b"b00", b"b99"),
            make_l0_file("cf0_0003.sst", 0, b"c00", b"c99"),
            make_l0_file("cf0_0004.sst", 0, b"d00", b"d99"),
        ]);
        let plan = actor
            .check_compaction(&state)
            .expect("compaction planning")
            .expect("expected compaction plan");
        assert!(!actor.compaction_running);

        // Act - drive the real state transition that guards concurrent compactions
        actor
            .prepare_compaction(&mut state, &plan, None)
            .expect("prepare compaction");

        // Assert
        assert!(actor.compaction_running);
        assert_eq!(actor.active_input_ssts, plan.input_files);
        assert!(actor.prepare_compaction(&mut state, &plan, None).is_err());
    }

    #[test]
    fn should_clear_running_flag_when_compaction_completes() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.set_compaction_enabled(true);
        state.manifest.files.extend([
            make_l0_file("cf0_0001.sst", 0, b"a00", b"a99"),
            make_l0_file("cf0_0002.sst", 0, b"b00", b"b99"),
            make_l0_file("cf0_0003.sst", 0, b"c00", b"c99"),
            make_l0_file("cf0_0004.sst", 0, b"d00", b"d99"),
        ]);
        let plan = actor
            .check_compaction(&state)
            .expect("compaction planning")
            .expect("expected compaction plan");
        actor
            .prepare_compaction(&mut state, &plan, None)
            .expect("prepare compaction");
        assert!(actor.compaction_running);

        // Act - the real completion handler, not a direct field write
        let leftover_token = actor.handle_complete(&mut state, &plan.input_files, &[]);

        // Assert
        assert!(!actor.compaction_running);
        assert!(actor.active_input_ssts.is_empty());
        assert!(leftover_token.is_none());
        assert!(state
            .compaction
            .compacting_ssts
            .iter()
            .all(|name| !plan.input_files.contains(name)));
    }

    #[test]
    fn should_use_configured_l0_file_count_threshold_when_picking_compaction() {
        // Arrange
        let mut actor = create_test_compaction_actor_with_config(LeveledCompactionConfig {
            l0_file_count_threshold: 2,
            ..LeveledCompactionConfig::default()
        });
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.set_compaction_enabled(true);
        state.manifest.files.extend([
            make_l0_file("cf0_0001.sst", 0, b"a00", b"a99"),
            make_l0_file("cf0_0002.sst", 0, b"b00", b"b99"),
        ]);

        let plan = actor
            .check_compaction(&state)
            .expect("compaction planning")
            .expect("expected compaction plan at configured file-count threshold");

        // Act
        // Assert
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
        assert_eq!(plan.input_files.len(), 2);
    }

    #[test]
    fn should_pick_non_default_column_family_when_default_has_no_candidates() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.set_compaction_enabled(true);
        let cf_id = state
            .create_cf("tenant_cf".to_string())
            .expect("create non-default cf");

        state.manifest.files.extend([
            make_l0_file("cf1_0001.sst", cf_id, b"a00", b"a99"),
            make_l0_file("cf1_0002.sst", cf_id, b"b00", b"b99"),
            make_l0_file("cf1_0003.sst", cf_id, b"c00", b"c99"),
            make_l0_file("cf1_0004.sst", cf_id, b"d00", b"d99"),
        ]);

        // Act
        let plan = actor
            .check_compaction(&state)
            .expect("compaction planning")
            .expect("expected compaction plan for non-default cf");

        // Assert
        assert_eq!(plan.cf_id, cf_id);
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
        assert_eq!(plan.input_files.len(), 4);
    }

    #[test]
    fn should_pick_lowest_column_family_id_when_multiple_non_default_families_need_compaction() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.set_compaction_enabled(true);
        let cf1_id = state.create_cf("cf1".to_string()).expect("create cf1");
        let cf2_id = state.create_cf("cf2".to_string()).expect("create cf2");

        state.manifest.files.extend([
            make_l0_file("cf1_0001.sst", cf1_id, b"a00", b"a99"),
            make_l0_file("cf1_0002.sst", cf1_id, b"b00", b"b99"),
            make_l0_file("cf1_0003.sst", cf1_id, b"c00", b"c99"),
            make_l0_file("cf1_0004.sst", cf1_id, b"d00", b"d99"),
            make_l0_file("cf2_0001.sst", cf2_id, b"m00", b"m99"),
            make_l0_file("cf2_0002.sst", cf2_id, b"n00", b"n99"),
            make_l0_file("cf2_0003.sst", cf2_id, b"o00", b"o99"),
            make_l0_file("cf2_0004.sst", cf2_id, b"p00", b"p99"),
        ]);

        // Act
        let plan = actor
            .check_compaction(&state)
            .expect("compaction planning")
            .expect("expected compaction plan when multiple cfs need work");

        // Assert
        assert_eq!(plan.cf_id, cf1_id);
        assert!(plan.input_files.iter().all(|name| name.starts_with("cf1_")));
    }

    #[test]
    fn should_service_next_column_family_across_compaction_completion_cycles() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.set_compaction_enabled(true);
        let first_cf = state
            .create_cf("first".to_string())
            .expect("create first cf");
        let second_cf = state
            .create_cf("second".to_string())
            .expect("create second cf");
        for (cf_id, prefix) in [(first_cf, "first"), (second_cf, "second")] {
            state.manifest.files.extend((0..4).map(|index| {
                make_l0_file(
                    &format!("{prefix}_{index}.sst"),
                    cf_id,
                    &[u8::try_from(index).expect("fixture index")],
                    &[u8::try_from(index).expect("fixture index")],
                )
            }));
        }

        // Act
        let first_plan = actor
            .check_compaction(&state)
            .expect("first compaction planning")
            .expect("first eligible compaction plan");
        actor.compaction_running = true;
        let _ = actor.handle_complete(&mut state, &first_plan.input_files, &[]);
        let second_plan = actor
            .check_compaction(&state)
            .expect("second compaction planning")
            .expect("second eligible compaction plan");
        actor.compaction_running = true;
        let _ = actor.handle_complete(&mut state, &second_plan.input_files, &[]);
        let wrapped_plan = actor
            .check_compaction(&state)
            .expect("wrapped compaction planning")
            .expect("first column family remains eligible");

        // Assert
        assert_eq!(first_plan.cf_id, first_cf);
        assert_eq!(second_plan.cf_id, second_cf);
        assert_eq!(wrapped_plan.cf_id, first_cf);
    }

    #[test]
    fn should_prioritize_critical_l0_round_robin_across_column_families() {
        // Arrange
        let config = LeveledCompactionConfig {
            l0_file_count_threshold: 2,
            l1_target_size: 1,
            ..LeveledCompactionConfig::default()
        };
        let mut actor = create_test_compaction_actor_with_config(config);
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.set_compaction_enabled(true);
        state.l0_compaction_trigger = 2;
        state.max_immutable_memtables = 0;
        let first_cf = state.create_cf("first".to_string()).expect("create first");
        let second_cf = state
            .create_cf("second".to_string())
            .expect("create second");
        for (cf_id, prefix) in [(first_cf, "first"), (second_cf, "second")] {
            state.manifest.files.extend((0..3).map(|index| {
                make_l0_file(
                    &format!("{prefix}-l0-{index}.sst"),
                    cf_id,
                    &[u8::try_from(index).expect("fixture key")],
                    &[u8::try_from(index).expect("fixture key")],
                )
            }));
        }
        state
            .manifest
            .files
            .push(make_level_file("deep-overfull.sst", 0, 1, 2, b"a", b"z"));

        // Act
        let first = actor
            .check_compaction(&state)
            .expect("first planning")
            .expect("first critical plan");
        actor.compaction_running = true;
        let _ = actor.handle_complete(&mut state, &first.input_files, &[]);
        let second = actor
            .check_compaction(&state)
            .expect("second planning")
            .expect("second critical plan");

        // Assert
        assert_eq!(first.cf_id, first_cf);
        assert_eq!(second.cf_id, second_cf);
        assert_eq!(first.source_level, 0);
        assert_eq!(second.source_level, 0);
    }

    #[test]
    fn should_pick_deepest_overfull_inner_level_before_soft_l0() {
        // Arrange
        let config = LeveledCompactionConfig {
            l0_file_count_threshold: 2,
            l1_target_size: 1,
            level_multiplier: 1,
            max_levels: 5,
            ..LeveledCompactionConfig::default()
        };
        let mut actor = create_test_compaction_actor_with_config(config);
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.set_compaction_enabled(true);
        state.l0_compaction_trigger = 2;
        state.max_immutable_memtables = 10;
        state.manifest.files.extend([
            make_l0_file("l0-a.sst", 0, b"a", b"b"),
            make_l0_file("l0-b.sst", 0, b"c", b"d"),
            make_level_file("l1-overfull.sst", 0, 1, 2, b"e", b"f"),
            make_level_file("l3-overfull.sst", 0, 3, 2, b"g", b"h"),
        ]);

        // Act
        let plan = actor
            .check_compaction(&state)
            .expect("compaction planning")
            .expect("deep debt plan");

        // Assert
        assert_eq!(plan.source_level, 3);
        assert_eq!(plan.target_level, 4);
    }

    #[test]
    fn should_force_soft_l0_work_during_manual_debt_drain() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state
            .manifest
            .files
            .push(make_l0_file("one-soft-l0.sst", 0, b"a", b"z"));
        assert!(actor
            .check_compaction(&state)
            .expect("background planning")
            .is_none());

        // Act
        let plan = actor
            .check_manual_compaction(&state)
            .expect("manual planning")
            .expect("manual L0 plan");

        // Assert
        assert_eq!(plan.source_files, vec!["one-soft-l0.sst".to_string()]);
        assert_eq!(plan.source_level, 0);
    }

    #[test]
    fn should_reconcile_compaction_state_given_shutdown_mid_async_compaction() {
        // Arrange
        let temp = tempfile::tempdir().expect("temp dir");
        let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
        std::fs::create_dir_all(&state.sst_dir).expect("create SST dir");
        let input_name = "000000_00_00000000000000000001.sst".to_string();
        let output_name = "000000_01_00000000000000000002.sst".to_string();
        let input_path = state.sst_dir.join(&input_name);
        let output_path = state.sst_dir.join(&output_name);
        let real_fs = Arc::new(crate::io::RealFs::new(&state.sst_dir).expect("create real SST fs"));
        let delegate: Arc<dyn crate::sst::SstFactory> =
            Arc::new(crate::sst::FsSstFactoryIo::new(real_fs, 4096));
        let mut input_writer = delegate.create().expect("create input SST writer");
        input_writer
            .add_with_meta(b"key", Some(b"authoritative value"), 1, 0, None)
            .expect("write input value");
        crate::sst::fs::finish_writer_to_path(input_writer, &input_path)
            .expect("finalize input SST");
        let input_bytes = std::fs::read(&input_path).expect("read input fixture");
        state.manifest.files.push(crate::metadata::FileMeta {
            name: input_name.clone(),
            level: 0,
            size_bytes: u64::try_from(input_bytes.len()).expect("input size fits u64"),
            cf_id: 0,
            smallest_key: Some(b"key".to_vec()),
            largest_key: Some(b"key".to_vec()),
            ..Default::default()
        });

        let local = Arc::new(
            crate::storage::filesystem::FileSystem::new(temp.path().join("hybrid-local"))
                .expect("create local storage"),
        );
        let cloud = Arc::new(
            crate::storage::filesystem::FileSystem::new(temp.path().join("hybrid-cloud"))
                .expect("create cloud storage"),
        );
        let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
        let cancel_probe = Arc::new(std::sync::Mutex::new(None));
        let factory = Arc::new(BlockingFinalizeFactory {
            delegate,
            reached_finalize: reached_tx,
            cancel_probe: Arc::clone(&cancel_probe),
        });
        let mut actor = CompactionActor::new(factory);
        *cancel_probe
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Arc::clone(&actor.worker_cancel));
        let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1).with_output_seq(2);
        plan.input_files.push(input_name.clone());
        let (completion_tx, completion_rx) = crossbeam::channel::unbounded();
        actor
            .run_compaction(&mut state, &plan, Some(&hybrid), Some(completion_tx))
            .expect("launch actual async compaction");
        assert_eq!(state.active_compactions.load(Ordering::SeqCst), 1);
        assert!(hybrid.budget_snapshot().total_committed_bytes > 0);
        reached_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("real compaction worker must reach output finalization");

        // Act
        actor.cancel_and_join_worker(&mut state, Some(&hybrid));
        actor.cancel_and_join_worker(&mut state, Some(&hybrid));

        // Assert
        assert!(matches!(
            completion_rx.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(RuntimeMsg::CompactionComplete {
                succeeded: false,
                output_ssts,
                ..
            }) if output_ssts.is_empty()
        ));
        assert!(
            actor.worker_handle.is_none(),
            "shutdown must join the worker"
        );
        assert_eq!(state.active_compactions.load(Ordering::SeqCst), 0);
        assert!(state.compaction.compacting_ssts.is_empty());
        assert_eq!(
            std::fs::read(&input_path).expect("read retained input"),
            input_bytes,
            "shutdown must retain authoritative inputs byte-for-byte"
        );
        assert!(!output_path.exists(), "shutdown must remove staged output");
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 0);
        assert!(actor.storage_reservation.is_none());
    }
}
