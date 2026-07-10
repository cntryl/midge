//! GC Actor - handles garbage collection
//!
//! Responsible for:
//! - Identifying obsolete SST files
//! - Deleting files that are no longer referenced
//! - Coordinating with snapshots to avoid deleting live data

use super::super::state::RuntimeState;
use crate::common::MidgeResult;
use crate::runtime::RuntimeMsg;
#[cfg(test)]
use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

// Failed deletion must remain retryable, but immediately resubmitting a
// permanently failing provider request would turn the event loop into a busy
// loop. The short test delay keeps the regression test deterministic while the
// production delay keeps retries bounded.
#[cfg(not(test))]
const CLOUD_DELETE_RETRY_DELAY: Duration = Duration::from_secs(1);
#[cfg(test)]
const CLOUD_DELETE_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Actor handling garbage collection
pub struct GcActor {
    /// Last GC run timestamp
    last_gc_run: Option<std::time::Instant>,
    /// Obsolete files that were retained because a transaction still pins them.
    pending_obsolete: VecDeque<String>,
    /// Failed asynchronous cloud deletes. The worker owns this queue until the
    /// event loop imports it for a delayed retry, so a failed provider request
    /// cannot be lost when the worker exits.
    failed_cloud_deletes: Arc<parking_lot::Mutex<VecDeque<String>>>,
    /// Earliest time at which the failed cloud-delete queue may be retried.
    failed_delete_retry_at: Option<Instant>,
    /// Wakes the event loop when an asynchronous worker adds a failed delete.
    /// The event loop then owns retry scheduling and never lets a detached
    /// worker mutate runtime state directly.
    retry_notifier: Option<crossbeam::channel::Sender<RuntimeMsg>>,
    /// Cloud delete workers that must finish before runtime shutdown releases
    /// its lease.
    cloud_delete_workers: Vec<JoinHandle<()>>,
}

impl GcActor {
    pub fn new() -> Self {
        Self {
            last_gc_run: None,
            pending_obsolete: VecDeque::new(),
            failed_cloud_deletes: Arc::new(parking_lot::Mutex::new(VecDeque::new())),
            failed_delete_retry_at: None,
            retry_notifier: None,
            cloud_delete_workers: Vec::new(),
        }
    }

    /// Configure the event-loop wakeup channel used by cloud delete workers.
    pub fn set_retry_notifier(&mut self, notifier: Option<crossbeam::channel::Sender<RuntimeMsg>>) {
        self.retry_notifier = notifier;
    }

    /// Check for garbage collection opportunities
    #[cfg(test)]
    pub fn check(state: &RuntimeState) {
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
                        .filter(|name| {
                            let path = std::path::Path::new(&name);
                            path.extension()
                                .is_some_and(|ext| ext.eq_ignore_ascii_case("sst"))
                                || path
                                    .file_name()
                                    .and_then(|file_name| file_name.to_str())
                                    .is_some_and(|file_name| file_name.ends_with(".sst.tmp"))
                        })
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
    ) {
        self.reap_finished_cloud_delete_workers();

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
                if !self
                    .pending_obsolete
                    .iter()
                    .any(|pending| pending == sst_name)
                {
                    self.pending_obsolete.push_back(sst_name.clone());
                }
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
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(sst_name, path = %sst_path.display(), "Obsolete SST already missing");
                    deleted_count += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        sst_name,
                        path = %sst_path.display(),
                        error = %e,
                        "Failed to delete SST file"
                    );
                    self.queue_failed_cloud_deletes(std::iter::once(sst_name.clone()));
                    skipped_count += 1;
                }
            }
        }

        if let Some(storage) = hybrid_storage {
            scheduled_count = cloud_sst_deletes.len();
            if !cloud_sst_deletes.is_empty() {
                let retry_names = cloud_sst_deletes
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>();
                match Self::spawn_cloud_sst_delete_worker(
                    storage,
                    cloud_sst_deletes,
                    Arc::clone(&self.failed_cloud_deletes),
                    self.retry_notifier.clone(),
                ) {
                    Ok(worker) => self.cloud_delete_workers.push(worker),
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            scheduled = scheduled_count,
                            "Failed to schedule obsolete cloud SST deletion"
                        );
                        skipped_count += scheduled_count;
                        self.queue_failed_cloud_deletes(retry_names);
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
    }

    /// Get timestamp of last GC run
    #[cfg(test)]
    pub fn last_gc_run(&self) -> Option<std::time::Instant> {
        self.last_gc_run
    }

    /// Retry files retained by a previous GC pass. A still-pinned file is
    /// requeued by `delete_ssts`; no restart is required after the last pin is
    /// released.
    pub fn retry_pending(
        &mut self,
        state: &mut RuntimeState,
        hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    ) {
        self.schedule_failed_cloud_delete_retry();
        if self.pending_obsolete.is_empty() {
            return;
        }
        let pending = self.pending_obsolete.drain(..).collect::<Vec<_>>();
        self.delete_ssts(state, &pending, hybrid_storage);
    }

    /// Return the duration until the next failed cloud-delete retry, if one is
    /// scheduled. The event loop folds this into its idle wakeup deadline.
    #[must_use]
    pub fn retry_deadline_timeout(&self) -> Option<Duration> {
        self.failed_delete_retry_at
            .map(|retry_at| retry_at.saturating_duration_since(Instant::now()))
    }

    /// Retry only failures that came back from an asynchronous cloud worker.
    /// Pinned files remain on the explicit snapshot-release path above, which
    /// means releasing a pin does not have to wait for unrelated cloud retry
    /// backoff.
    pub fn retry_failed_cloud_deletes_if_due(
        &mut self,
        state: &mut RuntimeState,
        hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    ) {
        self.schedule_failed_cloud_delete_retry();

        let Some(retry_at) = self.failed_delete_retry_at else {
            return;
        };
        if Instant::now() < retry_at {
            return;
        }

        let failed = self
            .failed_cloud_deletes
            .lock()
            .drain(..)
            .collect::<Vec<_>>();
        self.failed_delete_retry_at = None;
        if !failed.is_empty() {
            self.delete_ssts(state, &failed, hybrid_storage);
        }
    }

    /// Join every cloud delete worker before runtime shutdown can release the
    /// storage lease. This is intentionally blocking: an in-flight delete is
    /// storage mutation and must finish while the old fencing epoch is valid.
    pub fn shutdown_workers(&mut self) {
        for worker in self.cloud_delete_workers.drain(..) {
            if let Ok(()) = worker.join() {
                tracing::debug!("cloud SST GC worker joined");
            } else {
                tracing::warn!("cloud SST GC worker panicked during join");
            }
        }
    }

    fn reap_finished_cloud_delete_workers(&mut self) {
        let mut still_running = Vec::new();
        for worker in self.cloud_delete_workers.drain(..) {
            if worker.is_finished() {
                if let Ok(()) = worker.join() {
                    tracing::debug!("cloud SST GC worker completed");
                } else {
                    tracing::warn!("cloud SST GC worker panicked");
                }
            } else {
                still_running.push(worker);
            }
        }
        self.cloud_delete_workers = still_running;
    }

    /// Import worker failures and arm a bounded retry. The queue is shared
    /// with workers because a worker can fail after the runtime has returned
    /// from the initiating delete request.
    fn schedule_failed_cloud_delete_retry(&mut self) {
        if self.failed_cloud_deletes.lock().is_empty() {
            return;
        }

        self.failed_delete_retry_at
            .get_or_insert_with(|| Instant::now() + CLOUD_DELETE_RETRY_DELAY);
    }

    /// Queue a failed deletion without mutating manifest state. The next event
    /// loop idle deadline will retry it; retention is safer than deleting on
    /// uncertain failure.
    fn queue_failed_cloud_deletes(&mut self, names: impl IntoIterator<Item = String>) {
        let mut failed = self.failed_cloud_deletes.lock();
        let mut inserted = false;
        for name in names {
            if !failed.iter().any(|pending| pending == &name) {
                failed.push_back(name);
                inserted = true;
            }
        }
        drop(failed);

        if inserted {
            self.failed_delete_retry_at
                .get_or_insert_with(|| Instant::now() + CLOUD_DELETE_RETRY_DELAY);
        }
    }

    fn spawn_cloud_sst_delete_worker(
        storage: Arc<crate::storage::HybridStorage>,
        ssts: Vec<(String, PathBuf)>,
        failed_cloud_deletes: Arc<parking_lot::Mutex<VecDeque<String>>>,
        retry_notifier: Option<crossbeam::channel::Sender<RuntimeMsg>>,
    ) -> MidgeResult<JoinHandle<()>> {
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
                        Self::queue_worker_failed_delete(
                            &failed_cloud_deletes,
                            retry_notifier.as_ref(),
                            sst_name,
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
                            Self::queue_worker_failed_delete(
                                &failed_cloud_deletes,
                                retry_notifier.as_ref(),
                                sst_name,
                            );
                        }
                    }
                }
            })
            .map_err(|error| crate::common::MidgeError::Internal(error.to_string()))
    }

    fn queue_worker_failed_delete(
        failed_cloud_deletes: &parking_lot::Mutex<VecDeque<String>>,
        retry_notifier: Option<&crossbeam::channel::Sender<RuntimeMsg>>,
        sst_name: String,
    ) {
        let inserted = {
            let mut failed = failed_cloud_deletes.lock();
            if failed.iter().any(|pending| pending == &sst_name) {
                false
            } else {
                failed.push_back(sst_name);
                true
            }
        };

        if let (true, Some(notifier)) = (inserted, retry_notifier) {
            let _ = notifier.send(RuntimeMsg::RetryGc);
        }
    }
}

impl Default for GcActor {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for GcActor {
    fn drop(&mut self) {
        self.shutdown_workers();
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
