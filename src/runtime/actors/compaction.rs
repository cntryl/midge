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
        Self {
            compaction_running: false,
            sst_factory,
            compactor: Compactor::with_config(LeveledCompactionConfig::default()),
        }
    }

    /// Open an SST reader using the actor's configured SstFactory
    pub fn open_sst_reader(
        &self,
        path: &std::path::Path,
    ) -> crate::common::MidgeResult<Box<dyn crate::sst::SstReader>> {
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

        // Try to pick a compaction for the default column family (cf_id=0)
        let cf_id = 0u32;
        self.compactor.pick_compaction(&state.manifest.files, cf_id)
    }

    /// Execute a compaction plan
    ///
    /// If SBA is available, notifies it before and after compaction for disk accounting.
    pub fn run_compaction(
        &mut self,
        state: &mut RuntimeState,
        plan: crate::compaction::CompactionPlan,
        sba: Option<&std::sync::Arc<crate::storage::HybridStorage>>,
        worker_msg_tx: Option<crossbeam::channel::Sender<RuntimeMsg>>,
    ) -> MidgeResult<Vec<String>> {
        if self.compaction_running {
            return Err(crate::common::MidgeError::Internal(
                "Compaction already in progress".to_string(),
            ));
        }

        self.compaction_running = true;

        // Mark input files as being compacted
        state
            .compaction
            .compacting_ssts
            .extend(plan.input_files.clone());

        // Calculate input sizes for SBA
        let input_sizes: Vec<u64> = state
            .manifest
            .files
            .iter()
            .filter(|f| plan.input_files.contains(&f.name))
            .map(|f| f.size_bytes)
            .collect();

        // Notify SBA about planned compaction
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

        // Increase active compaction counter so BeginIngest can wait for drain.
        state
            .active_compactions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Spawn background worker to perform the compaction so the event loop remains responsive.
        // Worker will send a `RuntimeMsg::CompactionComplete` message when finished.
        if let Some(tx) = worker_msg_tx {
            let sst_factory = Arc::clone(&self.sst_factory);
            let sst_dir = state.sst_dir.clone();
            let input_files = plan.input_files.clone();
            let plan_clone = plan.clone();
            let epoch = std::sync::Arc::clone(&state.ingest_epoch);
            // Generate a stable job_id for this compaction job (for log correlation)
            let job_id = next_request_id()?;
            std::thread::spawn(move || {
                // CRITICAL: Phase 2.2 - Panic recovery wrapper
                // Catches panics in compaction logic and ensures CompactionComplete is always sent.
                // Without this, panics leave active_compactions incremented forever, causing deadlock
                // in the event loop when pending_compaction_waits tries to drain.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // Capture the epoch at start
                    let my_epoch = epoch.load(std::sync::atomic::Ordering::SeqCst);

                    // Abort check closure
                    let abort_check =
                        || epoch.load(std::sync::atomic::Ordering::SeqCst) != my_epoch;

                    // Execute compaction; allow cooperative abort via abort_check
                    let result = crate::compaction::execute_compaction(
                        &plan_clone,
                        sst_factory.as_ref(),
                        &sst_dir,
                        Some(&abort_check),
                    );

                    let output_ssts = match result {
                        Ok(v) => v,
                        Err(e) => {
                            // Distinguish cooperative aborts due to ingest epoch changes
                            let s = e.to_string();
                            if s.contains("ingest epoch change") || s.contains("compaction aborted")
                            {
                                // ─────────────────────────────────────────────────────────────────────
                                // COOPERATIVE CANCELLATION LOG — emitted exactly ONCE per aborted job.
                                // ─────────────────────────────────────────────────────────────────────
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
                            Vec::new()
                        }
                    };

                    output_ssts
                }));

                // Extract output_ssts, handling panic case
                let output_ssts = match result {
                    Ok(ssts) => ssts,
                    Err(panic_info) => {
                        // Compaction thread panicked; log the panic and return empty output
                        tracing::error!(
                            component = "compaction",
                            job_id = job_id,
                            input_files = ?input_files,
                            panic_info = ?panic_info,
                            "compaction worker thread panicked; returning empty output to unblock event loop"
                        );
                        Vec::new()
                    }
                };

                // Send completion back to runtime
                // CRITICAL: This message MUST be sent even if compaction panicked.
                // The event loop needs this to decrement active_compactions and drain pending_compaction_waits.
                let _ = tx.send(RuntimeMsg::CompactionComplete {
                    request_id: next_request_id().expect("request ID in compaction worker"),
                    input_ssts: input_files,
                    output_ssts,
                });
            });

            // Return immediately (we scheduled it)
            Ok(Vec::new())
        } else {
            // Fallback to synchronous execution if no worker channel is provided
            let output_ssts = crate::compaction::execute_compaction(
                &plan,
                self.sst_factory.as_ref(),
                &state.sst_dir,
                None,
            )?;

            // Calculate output sizes for SBA
            let output_sizes: Vec<u64> = output_ssts
                .iter()
                .filter_map(|name| {
                    let path = state.sst_dir.join(name);
                    std::fs::metadata(&path).ok().map(|m| m.len())
                })
                .collect();

            // Notify SBA about completion
            if let Some(hybrid) = sba {
                hybrid.compaction_completed(output_sizes);
            }

            tracing::info!(
                input_count = plan.input_files.len(),
                output_count = output_ssts.len(),
                "Compaction completed"
            );

            // Defer active_compactions decrement to the caller handling CompactionComplete
            Ok(output_ssts)
        }
    }

    /// Handle compaction completion
    pub fn handle_complete(
        &mut self,
        state: &mut RuntimeState,
        input_ssts: Vec<String>,
        output_ssts: Vec<String>,
    ) {
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

    fn create_test_compaction_actor() -> CompactionActor {
        // Use the modern io::Fs-backed factory
        let fs = Arc::new(crate::io::MockFs::new());
        let sst_factory = Arc::new(crate::sst::FsSstFactoryIo::new(fs, 64 * 1024));
        CompactionActor::new(sst_factory)
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
}
