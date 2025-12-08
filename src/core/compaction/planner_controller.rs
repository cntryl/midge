//! Controller for executing compaction plans through the runtime.
//!
//! This module bridges the deterministic planner with the runtime executor,
//! ensuring compaction decisions are logged durably and replayed after crashes.

use crate::core::compaction::{CompactionLogManager, CompactionTask, Planner};
use crate::core::manifest::Manifest;
use crate::core::runtime::{EngineRuntime, RuntimeTaskKind};
use crate::error::MidgeResult;
use std::path::Path;
use std::sync::Arc;

/// Coordinates compaction planning and execution through the runtime
pub struct PlannerController {
    planner: Planner,
    log_manager: CompactionLogManager,
    runtime: Arc<EngineRuntime>,
    next_task_id: u64,
}

impl PlannerController {
    /// Create a new planner controller
    pub fn new(engine_dir: &Path, planner: Planner, runtime: Arc<EngineRuntime>) -> Self {
        let log_manager = CompactionLogManager::new(engine_dir);
        Self {
            planner,
            log_manager,
            runtime,
            next_task_id: 1,
        }
    }

    /// Load any pending compaction tasks from the log after crash
    pub fn recover_pending_tasks(&mut self) -> MidgeResult<Vec<CompactionTask>> {
        let tasks = self.log_manager.load()?;
        if !tasks.is_empty() {
            let max_id = tasks.iter().map(|t| t.task_id).max().unwrap_or(0);
            self.next_task_id = max_id + 1;
            tracing::info!(
                "Recovered {} pending compaction tasks from log",
                tasks.len()
            );
        }
        Ok(tasks)
    }

    /// Generate and submit compaction plans for a given manifest
    pub fn submit_compaction_plans(&mut self, manifest: &Manifest) -> MidgeResult<()> {
        // Use pure planner to generate deterministic plans
        let plans = self.planner.plan(manifest);

        for plan in plans {
            let task = CompactionTask::new(self.next_task_id, &plan);
            self.next_task_id += 1;

            // Persist to log before submitting (durability)
            self.log_manager.append(&task)?;

            // Submit to runtime for execution
            self.submit_plan_as_task(task)?;
        }

        Ok(())
    }

    /// Submit a single compaction task to the runtime
    fn submit_plan_as_task(&self, task: CompactionTask) -> MidgeResult<()> {
        let task_id = task.task_id;
        let cf_id = task.cf_id;
        let source_level = task.source_level;
        let target_level = task.target_level;

        let description = format!(
            "Compaction task {}: CF {} L{} -> L{}",
            task_id, cf_id, source_level, target_level
        );

        // DECISION (Phase 8.3): Remove PlannerController - unused dead code.
        // CompactionController is the real implementation (in maintenance.rs).
        // PlannerController is vestigial placeholder from earlier design.
        let action = Box::new(move || {
            tracing::debug!("Executing compaction task {}", task_id);
            // Placeholder: compaction is handled by CompactionController in maintenance.rs
        });

        let runtime_task = crate::core::runtime::RuntimeTask::new(
            RuntimeTaskKind::CompactionPlanExecution,
            description,
            action,
        );

        self.runtime.submit(runtime_task)?;

        Ok(())
    }

    /// Clear the log after successful checkpoint (all plans executed)
    pub fn checkpoint(&self) -> MidgeResult<()> {
        self.log_manager.clear()?;
        tracing::info!("Checkpointed compaction log");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn should_recover_pending_tasks() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let log_manager = CompactionLogManager::new(temp_dir.path());
        use crate::core::compaction::CompactionPlan;
        let plan = CompactionPlan {
            source_level: 0,
            target_level: 1,
            cf_id: 0,
            input_files: vec!["sst_001.blob".to_string()],
            output_files: Vec::new(),
        };
        let task = CompactionTask::new(1, &plan);

        // Act
        log_manager.append(&task).unwrap();
        let recovered = log_manager.load().unwrap();

        // Assert
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].task_id, 1);
    }
}
