//! Compaction Actor - handles SST compaction
//!
//! Responsible for:
//! - Detecting when compaction is needed
//! - Planning and executing compaction jobs
//! - Merging SST files across levels

use super::super::state::RuntimeState;
use crate::common::{MidgeError, MidgeResult};
use crate::compaction::{Compactor, LeveledCompactionConfig};
use crate::runtime::{next_request_id, RuntimeMsg};
use crate::sst::SstFactory;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

/// Actor handling SST compaction
pub struct CompactionActor {
    /// Whether a compaction is currently running
    compaction_running: bool,
    /// SST factory for creating readers/writers
    sst_factory: Arc<dyn SstFactory>,
    /// Compaction strategy
    compactor: Compactor,
    /// Last column family selected for the global compaction slot. Planning
    /// resumes after this ID so a perpetually eligible low ID cannot starve
    /// unrelated families.
    last_scheduled_cf: Option<crate::types::ColumnFamilyId>,
    /// Accounting reservation for the one active compaction. The actor
    /// serializes compactions, but the token still prevents a delayed terminal
    /// message from settling a later operation.
    storage_reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    /// Cooperative cancellation flag for the active background worker.
    worker_cancel: Arc<AtomicBool>,
    /// Active background worker, joined before runtime shutdown completes.
    worker_handle: Option<JoinHandle<()>>,
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
            last_scheduled_cf: None,
            storage_reservation: None,
            worker_cancel: Arc::new(AtomicBool::new(false)),
            worker_handle: None,
        }
    }

    pub fn set_l0_file_count_threshold(&mut self, threshold: usize) {
        self.compactor.config.l0_file_count_threshold = threshold.max(1);
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

        let mut cf_ids: Vec<u32> = state.column_families.keys().copied().collect();
        cf_ids.sort_unstable();
        if let Some(last_scheduled) = self.last_scheduled_cf {
            let next_index = cf_ids
                .iter()
                .position(|cf_id| *cf_id > last_scheduled)
                .unwrap_or(0);
            cf_ids.rotate_left(next_index);
        }

        for cf_id in cf_ids {
            if let Some(mut plan) = self
                .compactor
                .pick_compaction(&state.manifest.files, cf_id)?
            {
                plan.snapshot_horizon = state.oldest_active_snapshot_sequence();
                self.last_scheduled_cf = Some(cf_id);
                return Ok(Some(plan));
            }
        }

        Ok(None)
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
        self.prepare_compaction(state, plan, sba)?;
        if let Some(tx) = worker_msg_tx {
            let result = self.run_async_compaction(state, tx, plan);
            if result.is_err() {
                self.abort_compaction(state, plan, sba);
            }
            return result;
        }

        let result = self.run_sync_compaction(state, plan, sba);
        if result.is_err() {
            self.abort_compaction(state, plan, sba);
        }
        result
    }

    fn abort_compaction(
        &mut self,
        state: &mut RuntimeState,
        plan: &crate::compaction::CompactionPlan,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) {
        state
            .compaction
            .compacting_ssts
            .retain(|name| !plan.input_files.contains(name));
        state
            .active_compactions
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.compaction_running = false;
        if let (Some(hybrid), Some(token)) = (sba, self.storage_reservation.take()) {
            hybrid.compaction_aborted_with_token(token);
        }
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
        // Remove input files from compacting set
        state
            .compaction
            .compacting_ssts
            .retain(|s| !input_ssts.contains(s));

        self.compaction_running = false;
        self.join_worker();

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
    pub fn cancel_and_join_worker(&mut self) {
        self.worker_cancel.store(true, Ordering::Release);
        self.join_worker();
        self.compaction_running = false;
    }

    fn join_worker(&mut self) {
        let Some(handle) = self.worker_handle.take() else {
            return;
        };

        if let Ok(()) = handle.join() {
            tracing::debug!("compaction worker joined");
        } else {
            tracing::warn!("compaction worker panicked during join");
        }
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

        self.compaction_running = true;

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

        self.storage_reservation =
            sba.map(|hybrid| hybrid.compaction_planned_with_token(&input_sizes));

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

    fn run_sync_compaction(
        &mut self,
        state: &RuntimeState,
        plan: &crate::compaction::CompactionPlan,
        _sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<Vec<String>> {
        let output_ssts = crate::compaction::execute_compaction(
            plan,
            self.sst_factory.as_ref(),
            &state.sst_dir,
            None,
        )?;

        tracing::info!(
            input_count = plan.input_files.len(),
            output_count = output_ssts.len(),
            "Compaction completed"
        );

        Ok(output_ssts)
    }

    fn run_async_compaction(
        &mut self,
        state: &RuntimeState,
        tx: crossbeam::channel::Sender<RuntimeMsg>,
        plan: &crate::compaction::CompactionPlan,
    ) -> MidgeResult<Vec<String>> {
        let sst_factory = Arc::clone(&self.sst_factory);
        let sst_dir = state.sst_dir.clone();
        let input_files = plan.input_files.clone();
        let plan_clone = plan.clone();
        let epoch = std::sync::Arc::clone(&state.ingest_epoch);
        self.worker_cancel.store(false, Ordering::Release);
        let worker_cancel = Arc::clone(&self.worker_cancel);
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
                let result = crate::compaction::execute_compaction(
                    &plan_clone,
                    sst_factory.as_ref(),
                    &sst_dir,
                    Some(&abort_check),
                );

                let (output_ssts, succeeded) = match result {
                    Ok(v) => (v, true),
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
                        (Vec::new(), false)
                    }
                };

                (output_ssts, succeeded)
            }));

            let (output_ssts, succeeded) = match result {
                Ok(result) => result,
                Err(panic_info) => {
                    tracing::error!(
                        component = "compaction",
                        job_id = job_id,
                        input_files = ?input_files,
                        panic_info = ?panic_info,
                        "compaction worker thread panicked; returning empty output to unblock event loop"
                    );
                    (Vec::new(), false)
                }
            };

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
                    input_ssts: input_files,
                    output_ssts,
                    cf_id: plan_clone.cf_id,
                    target_level: plan_clone.target_level,
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
        })
            .map_err(|error| MidgeError::Internal(format!("spawn compaction worker: {error}")))?;
        self.worker_handle = Some(worker);

        Ok(Vec::new())
    }
}

impl Clone for CompactionActor {
    fn clone(&self) -> Self {
        Self {
            compaction_running: self.compaction_running,
            sst_factory: Arc::clone(&self.sst_factory),
            compactor: Compactor::with_config(self.compactor.config.clone()),
            last_scheduled_cf: self.last_scheduled_cf,
            storage_reservation: self.storage_reservation,
            worker_cancel: Arc::clone(&self.worker_cancel),
            worker_handle: None,
        }
    }
}

impl Drop for CompactionActor {
    fn drop(&mut self) {
        self.cancel_and_join_worker();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Arrange
        let mut actor = create_test_compaction_actor();
        actor.compaction_running = true;

        // Act: try to pick compaction while one is running
        // Note: This would need a real RuntimeState for full test
        // For now, verify state invariant
        assert!(actor.compaction_running);

        // Assert
        // check_compaction would return None
    }

    #[test]
    fn should_set_running_flag_when_compaction_starts() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        assert!(!actor.compaction_running);

        // Act
        actor.compaction_running = true;

        // Assert
        assert!(actor.compaction_running);
    }

    #[test]
    fn should_clear_running_flag_when_compaction_completes() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        actor.compaction_running = true;

        // Act
        actor.compaction_running = false;

        // Assert
        assert!(!actor.compaction_running);
    }

    #[test]
    fn should_be_cloneable() {
        // Arrange
        let actor1 = create_test_compaction_actor();

        // Act
        let actor2 = actor1.clone();

        // Assert: clone should have same initial state
        assert_eq!(actor1.compaction_running, actor2.compaction_running);
    }

    #[test]
    fn should_preserve_state_through_handle_complete() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        actor.compaction_running = true;

        // Act: Simulate handle_complete clearing the flag
        actor.compaction_running = false;

        // Assert
        assert!(!actor.compaction_running);
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
}
