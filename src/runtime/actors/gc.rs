//! GC Actor - handles garbage collection
//!
//! Responsible for:
//! - Identifying obsolete SST files
//! - Deleting files that are no longer referenced
//! - Coordinating with snapshots to avoid deleting live data

use super::super::state::RuntimeState;
use crate::common::MidgeResult;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

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
        // Find SST files that are still referenced in the manifest
        let manifest_ssts: HashSet<String> = state
            .manifest
            .files
            .iter()
            .map(|f| f.name.clone())
            .collect();

        // List actual files on disk
        let disk_ssts = match std::fs::read_dir(&state.sst_dir) {
            Ok(entries) => entries
                .filter_map(|entry| {
                    entry
                        .ok()
                        .and_then(|e| e.file_name().into_string().ok())
                        .filter(|name| name.ends_with(".sst") || name.ends_with(".sst.tmp"))
                })
                .collect::<HashSet<_>>(),
            Err(e) => {
                tracing::warn!(error = %e, "Failed to read SST directory for GC check");
                return;
            }
        };

        // Files on disk but not in manifest are candidates for deletion
        let orphaned: Vec<_> = disk_ssts
            .iter()
            .filter(|name| !manifest_ssts.contains(*name))
            .cloned()
            .collect();

        tracing::debug!(
            manifest_sst_count = manifest_ssts.len(),
            disk_sst_count = disk_ssts.len(),
            orphaned_count = orphaned.len(),
            "GC check complete"
        );

        if !orphaned.is_empty() {
            tracing::info!(
                orphaned_files = ?orphaned,
                "Found orphaned SST files eligible for deletion"
            );
        }
    }

    /// Delete obsolete SST files
    ///
    /// Before deleting an SST, checks:
    /// 1. File is not active in manifest
    /// 2. File is not being compacted
    /// 3. File is not pinned by active snapshot (BLOCKER #8)
    pub fn delete_ssts(
        &mut self,
        state: &mut RuntimeState,
        sst_names: &[String],
        hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    ) -> MidgeResult<()> {
        // Get set of SSTs pinned by active snapshots
        let pinned_ssts = state.get_pinned_sst_names();

        let mut deleted_count = 0;
        let mut scheduled_count = 0;
        let mut skipped_count = 0;
        let mut cloud_sst_deletes = Vec::new();

        for sst_name in sst_names {
            let sst_path = state.sst_dir.join(sst_name);

            // Check that file is not in active manifest
            let is_active = state.manifest.files.iter().any(|f| f.name == *sst_name);
            if is_active {
                tracing::warn!(sst_name, "Skipping delete of active SST file");
                skipped_count += 1;
                continue;
            }

            // Check that file is not being compacted
            if state.compaction.compacting_ssts.contains(sst_name) {
                tracing::warn!(sst_name, "Skipping delete of SST being compacted");
                skipped_count += 1;
                continue;
            }

            // === BLOCKER #8 FIX: Check that file is not pinned by a snapshot ===
            if pinned_ssts.contains(sst_name) {
                tracing::warn!(sst_name, "Skipping delete of SST pinned by active snapshot");
                skipped_count += 1;
                continue;
            }

            if hybrid_storage.is_some() {
                cloud_sst_deletes.push((sst_name.clone(), sst_path));
                continue;
            }

            // Actually delete the file
            match std::fs::remove_file(&sst_path) {
                Ok(()) => {
                    tracing::info!(sst_name, path = %sst_path.display(), "Deleted obsolete SST");
                    deleted_count += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        sst_name,
                        path = %sst_path.display(),
                        error = %e,
                        "Failed to delete SST file"
                    );
                    skipped_count += 1;
                }
            }
        }

        if let Some(storage) = hybrid_storage {
            scheduled_count = cloud_sst_deletes.len();
            if !cloud_sst_deletes.is_empty() {
                match Self::spawn_cloud_sst_delete_worker(storage, cloud_sst_deletes) {
                    Ok(()) => {}
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            scheduled = scheduled_count,
                            "Failed to schedule obsolete cloud SST deletion"
                        );
                        skipped_count += scheduled_count;
                        scheduled_count = 0;
                    }
                }
            }
        }

        self.last_gc_run = Some(std::time::Instant::now());

        if deleted_count > 0 || scheduled_count > 0 || skipped_count > 0 {
            tracing::info!(
                deleted = deleted_count,
                scheduled = scheduled_count,
                skipped = skipped_count,
                "GC deletion batch complete"
            );
        }

        Ok(())
    }

    /// Get timestamp of last GC run
    pub fn last_gc_run(&self) -> Option<std::time::Instant> {
        self.last_gc_run
    }

    fn spawn_cloud_sst_delete_worker(
        storage: Arc<crate::storage::HybridStorage>,
        ssts: Vec<(String, PathBuf)>,
    ) -> MidgeResult<()> {
        std::thread::Builder::new()
            .name("midge-sst-gc".to_string())
            .spawn(move || {
                for (sst_name, sst_path) in ssts {
                    if let Err(error) = storage.delete_sst_object_blocking(&sst_name) {
                        tracing::warn!(
                            sst_name,
                            %error,
                            "Failed to delete obsolete SST from cloud storage; keeping local orphan for retry"
                        );
                        continue;
                    }

                    match std::fs::remove_file(&sst_path) {
                        Ok(()) => {
                            tracing::info!(
                                sst_name,
                                path = %sst_path.display(),
                                "Deleted obsolete SST after cloud provider cleanup"
                            );
                        }
                        Err(error) => {
                            tracing::warn!(
                                sst_name,
                                path = %sst_path.display(),
                                %error,
                                "Failed to delete obsolete local SST after cloud provider cleanup"
                            );
                        }
                    }
                }
            })
            .map(|_| ())
            .map_err(|error| crate::common::MidgeError::Internal(error.to_string()))
    }
}

impl Default for GcActor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_initialize_gc_actor_with_no_last_run() {
        // Arrange
        // (no setup needed)

        // Act
        let actor = GcActor::new();

        // Assert
        assert!(actor.last_gc_run().is_none());
    }

    #[test]
    fn should_initialize_via_default() {
        // Arrange
        // (no setup needed)

        // Act
        let actor = GcActor::default();

        // Assert
        assert!(actor.last_gc_run().is_none());
    }

    #[test]
    fn should_record_gc_run_timestamp() {
        // Arrange
        let mut actor = GcActor::new();
        assert!(actor.last_gc_run().is_none());

        // Act
        actor.last_gc_run = Some(std::time::Instant::now());

        // Assert
        assert!(actor.last_gc_run().is_some());
    }

    #[test]
    fn should_update_timestamp_on_successive_gc_runs() {
        // Arrange
        let mut actor = GcActor::new();

        // Act
        let run1 = std::time::Instant::now();
        actor.last_gc_run = Some(run1);
        let first_run = actor.last_gc_run();

        // Sleep briefly to ensure different timestamp
        std::thread::sleep(std::time::Duration::from_millis(1));

        let run2 = std::time::Instant::now();
        actor.last_gc_run = Some(run2);
        let second_run = actor.last_gc_run();

        // Assert: both runs recorded
        assert!(first_run.is_some());
        assert!(second_run.is_some());
        // Second run should be later
        assert!(second_run.unwrap() > first_run.unwrap());
    }

    #[test]
    fn should_clear_gc_run_timestamp() {
        // Arrange
        let mut actor = GcActor::new();
        actor.last_gc_run = Some(std::time::Instant::now());
        assert!(actor.last_gc_run().is_some());

        // Act
        actor.last_gc_run = None;

        // Assert
        assert!(actor.last_gc_run().is_none());
    }
}
