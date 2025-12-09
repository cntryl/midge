//! GC Actor - handles garbage collection
//!
//! Responsible for:
//! - Identifying obsolete SST files
//! - Deleting files that are no longer referenced
//! - Coordinating with snapshots to avoid deleting live data

use super::super::state::RuntimeState;
use crate::common::MidgeResult;

/// Actor handling garbage collection
pub struct GcActor {
    /// Last GC run timestamp
    last_gc_run: Option<std::time::Instant>,
}

impl GcActor {
    pub fn new() -> Self {
        Self { last_gc_run: None }
    }

    /// Check for garbage collection opportunities
    pub fn check(&self, state: &RuntimeState) {
        // Find SST files that are no longer in the manifest
        let manifest_ssts: std::collections::HashSet<_> = state
            .manifest
            .files
            .iter()
            .map(|f| f.name.as_str())
            .collect();

        // TODO: List actual files on disk and compare
        // Files on disk but not in manifest are candidates for deletion

        tracing::debug!(manifest_sst_count = manifest_ssts.len(), "GC check");
    }

    /// Delete obsolete SST files
    pub fn delete_ssts(
        &mut self,
        state: &mut RuntimeState,
        sst_names: &[String],
    ) -> MidgeResult<()> {
        for sst_name in sst_names {
            let sst_path = state.sst_dir.join(sst_name);

            // Check that file is not in active manifest
            let is_active = state.manifest.files.iter().any(|f| f.name == *sst_name);
            if is_active {
                tracing::warn!(sst_name, "Skipping delete of active SST file");
                continue;
            }

            // Check that file is not being compacted
            if state.compaction.compacting_ssts.contains(sst_name) {
                tracing::warn!(sst_name, "Skipping delete of SST being compacted");
                continue;
            }

            tracing::info!(sst_name, path = %sst_path.display(), "Deleting obsolete SST");

            // TODO: Actually delete the file
            // std::fs::remove_file(&sst_path)?;
        }

        self.last_gc_run = Some(std::time::Instant::now());

        Ok(())
    }
}

impl Default for GcActor {
    fn default() -> Self {
        Self::new()
    }
}
