//! Compaction Actor - handles SST compaction
//!
//! Responsible for:
//! - Detecting when compaction is needed
//! - Planning and executing compaction jobs
//! - Merging SST files across levels

use super::super::state::RuntimeState;
use crate::common::MidgeResult;
use crate::compaction::{execute_compaction, Compactor, LeveledCompactionConfig};
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

    /// Check if compaction is needed based on current state and pick a plan if so
    pub fn check_compaction(
        &mut self,
        state: &RuntimeState,
    ) -> Option<crate::compaction::CompactionPlan> {
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

        // Execute the compaction via the compaction module
        let output_ssts = execute_compaction(&plan, self.sst_factory.as_ref(), &state.sst_dir)?;

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

        Ok(output_ssts)
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
