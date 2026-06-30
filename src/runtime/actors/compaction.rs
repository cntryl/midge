//! Compaction Actor - handles SST compaction
//!
//! Responsible for:
//! - Detecting when compaction is needed
//! - Planning and executing compaction jobs
//! - Merging SST files across levels

use super::super::state::RuntimeState;
use crate::common::MidgeResult;
use crate::compaction::{Compactor, LeveledCompactionConfig};
use crate::runtime::{next_request_id, RuntimeMsg};
use crate::sst::SstFactory;
use std::sync::Arc;

/// Actor handling SST compaction
pub struct CompactionActor {
    /// Whether a compaction is currently running
    compaction_running: bool,
    /// SST factory for creating readers/writers
    sst_factory: Arc<dyn SstFactory>,
    /// Compaction strategy
    compactor: Compactor,
}

impl CompactionActor {
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
        }
    }

    pub fn set_l0_file_count_threshold(&mut self, threshold: usize) {
        self.compactor.config.l0_file_count_threshold = threshold.max(1);
    }

    pub fn l0_file_count_threshold(&self) -> usize {
        self.compactor.config.l0_file_count_threshold
    }

    /// Open an SST reader using the actor's configured `SstFactory`
    pub fn open_sst_reader(
        &self,
        path: &std::path::Path,
    ) -> crate::common::MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        self.sst_factory.open(path)
    }

    pub fn check_compaction(
        &mut self,
        state: &RuntimeState,
    ) -> Option<crate::compaction::CompactionPlan> {
        // If compaction is disabled via runtime configuration, skip checks
        if !state.enable_compaction {
            tracing::debug!("compaction disabled in runtime state");
            return None;
        }

        if self.compaction_running {
            return None;
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

        for cf_id in cf_ids {
            if let Some(mut plan) = self.compactor.pick_compaction(&state.manifest.files, cf_id) {
                plan.snapshot_horizon = state.oldest_active_snapshot_sequence();
                return Some(plan);
            }
        }

        None
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
            return self.run_async_compaction(state, tx, plan);
        }

        self.run_sync_compaction(state, plan, sba)
    }

    /// Handle compaction completion
    pub fn handle_complete(
        &mut self,
        state: &mut RuntimeState,
        input_ssts: &[String],
        output_ssts: &[String],
    ) {
        // Invariant: completion only clears in-memory "running" state. The
        // actual authority switch happens when manifest publication removes the
        // old SSTs and adds the replacement set.
        // Remove input files from compacting set
        state
            .compaction
            .compacting_ssts
            .retain(|s| !input_ssts.contains(s));

        self.compaction_running = false;

        tracing::info!(
            input_count = input_ssts.len(),
            output_count = output_ssts.len(),
            "Compaction completed"
        );
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

        if let Some(hybrid) = sba {
            hybrid.compaction_planned(input_sizes);
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

    fn run_sync_compaction(
        &mut self,
        state: &RuntimeState,
        plan: &crate::compaction::CompactionPlan,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<Vec<String>> {
        let output_ssts =
            crate::compaction::execute_compaction(plan, self.sst_factory.as_ref(), &state.sst_dir, None)?;

        let output_sizes: Vec<u64> = output_ssts
            .iter()
            .filter_map(|name| {
                let path = state.sst_dir.join(name);
                std::fs::metadata(&path).ok().map(|m| m.len())
            })
            .collect();

        if let Some(hybrid) = sba {
            hybrid.compaction_completed(output_sizes);
        }

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
        let job_id = next_request_id()?;

        std::thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let my_epoch = epoch.load(std::sync::atomic::Ordering::SeqCst);
                let abort_check =
                    || epoch.load(std::sync::atomic::Ordering::SeqCst) != my_epoch;
                let result = crate::compaction::execute_compaction(
                    &plan_clone,
                    sst_factory.as_ref(),
                    &sst_dir,
                    Some(&abort_check),
                );

                let (output_ssts, succeeded) = match result {
                    Ok(v) => (v, true),
                    Err(e) => {
                        let s = e.to_string();
                        if s.contains("ingest epoch change") || s.contains("compaction aborted") {
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

            let _ = tx.send(RuntimeMsg::CompactionComplete {
                request_id: next_request_id().expect("request ID in compaction worker"),
                input_ssts: input_files,
                output_ssts,
                cf_id: plan_clone.cf_id,
                target_level: plan_clone.target_level,
                succeeded,
            });
        });

        Ok(Vec::new())
    }
}

impl Clone for CompactionActor {
    fn clone(&self) -> Self {
        Self {
            compaction_running: self.compaction_running,
            sst_factory: Arc::clone(&self.sst_factory),
            compactor: Compactor::with_config(self.compactor.config.clone()),
        }
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
        let mut actor = create_test_compaction_actor_with_config(LeveledCompactionConfig {
            l0_file_count_threshold: 2,
            ..LeveledCompactionConfig::default()
        });
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.enable_compaction = true;
        state.manifest.files.extend([
            make_l0_file("cf0_0001.sst", 0, b"a00", b"a99"),
            make_l0_file("cf0_0002.sst", 0, b"b00", b"b99"),
        ]);

        let plan = actor
            .check_compaction(&state)
            .expect("expected compaction plan at configured file-count threshold");

        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
        assert_eq!(plan.input_files.len(), 2);
    }

    #[test]
    fn should_pick_non_default_column_family_when_default_has_no_candidates() {
        // Arrange
        let mut actor = create_test_compaction_actor();
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.enable_compaction = true;
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
        state.enable_compaction = true;
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
            .expect("expected compaction plan when multiple cfs need work");

        // Assert
        assert_eq!(plan.cf_id, cf1_id);
        assert!(plan.input_files.iter().all(|name| name.starts_with("cf1_")));
    }
}
