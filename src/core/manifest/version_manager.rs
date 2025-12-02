//! VersionManager: Actor for serialized version management
//!
//! Processes VersionEdit messages serially to ensure atomic visibility transitions.
//! All manifest updates and version publishes go through this single actor.

use crossbeam_channel::{bounded, Receiver, Sender};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::core::manifest::{AtomicVersionSet, VersionEdit};
use crate::error::{MidgeError, MidgeResult};

/// Actor that serializes all version changes.
/// Ensures atomic visibility: manifest write + version publish happens atomically.
pub struct VersionManager {
    tx: Mutex<Option<Sender<VersionEditRequest>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    #[allow(dead_code)] // Used by actor thread
    test_hooks: Option<crate::common::test_hooks::TestHooks>,
    #[allow(dead_code)] // Passed to actor thread
    mem_mode: bool,
}

/// Internal request type for the version manager actor.
struct VersionEditRequest {
    edit: VersionEdit,
    response_tx: Option<Sender<MidgeResult<()>>>,
}

impl VersionManager {
    /// Create and start a new version manager actor.
    ///
    /// # Arguments
    /// * `version_set` - Atomic version set to update
    /// * `db_path` - Database path for manifest storage
    /// * `test_hooks` - Optional test hooks for fault injection
    pub fn new(
        version_set: AtomicVersionSet,
        db_path: PathBuf,
        test_hooks: Option<crate::common::test_hooks::TestHooks>,
        mem_mode: bool,
    ) -> Self {
        let (tx, rx) = bounded(100); // Backpressure after 100 pending edits

        let test_hooks_for_actor = test_hooks.clone();
        let handle = thread::Builder::new()
            .name("version-manager".to_string())
            .spawn(move || {
                Self::run_actor(version_set, db_path, rx, test_hooks_for_actor, mem_mode);
            })
            .expect("Failed to spawn version manager thread");

        Self {
            tx: Mutex::new(Some(tx)),
            handle: Mutex::new(Some(handle)),
            test_hooks,
            mem_mode,
        }
    }

    /// Main actor loop - processes edits serially.
    fn run_actor(
        version_set: AtomicVersionSet,
        db_path: PathBuf,
        rx: Receiver<VersionEditRequest>,
        test_hooks: Option<crate::common::test_hooks::TestHooks>,
        mem_mode: bool,
    ) {
        tracing::info!("Version manager actor started");

        while let Ok(request) = rx.recv() {
            let start = std::time::Instant::now();
            let result = Self::process_edit(
                &version_set,
                &db_path,
                request.edit,
                test_hooks.as_ref(),
                mem_mode,
            );

            tracing::trace!(dur_ms = %start.elapsed().as_millis(), "version_manager.process_edit duration (ms)");

            // Send response if caller is waiting
            if let Some(response_tx) = request.response_tx {
                let _ = response_tx.send(result);
            } else if let Err(e) = result {
                tracing::error!("Version edit failed: {}", e);
            }
        }

        tracing::info!("Version manager actor stopped");
    }

    /// Process a single edit: load → apply → save → publish.
    fn process_edit(
        version_set: &AtomicVersionSet,
        db_path: &Path,
        edit: VersionEdit,
        test_hooks: Option<&crate::common::test_hooks::TestHooks>,
        mem_mode: bool,
    ) -> MidgeResult<()> {
        // Load current version
        let current = version_set.load();

        // Apply edit to create new version
        let new_version = current.apply_edit(edit)?;

        // Save manifest atomically with test hooks (skip in memory mode)
        if !mem_mode {
            new_version
                .manifest
                .save_atomic_with_hooks(db_path, test_hooks)?;
        }

        // Publish new version atomically
        version_set.store(Arc::new(new_version));

        Ok(())
    }

    /// Apply a version edit asynchronously.
    /// Returns immediately - edit is queued for processing.
    pub fn apply_edit_async(&self, edit: VersionEdit) -> MidgeResult<()> {
        let tx_guard = self.tx.lock();
        let started = std::time::Instant::now();
        let res = tx_guard
            .as_ref()
            .ok_or_else(|| MidgeError::internal("version manager stopped"))?
            .send(VersionEditRequest {
                edit,
                response_tx: None,
            })
            .map_err(|_| MidgeError::internal("version manager stopped"));

        if res.is_ok() {
            tracing::trace!(dur_ms = %started.elapsed().as_millis(), "version_manager.apply_edit_async send duration (ms)");
        }

        res
    }

    /// Apply a version edit synchronously.
    /// Blocks until edit is processed and returns result.
    pub fn apply_edit_sync(&self, edit: VersionEdit) -> MidgeResult<()> {
        let (response_tx, response_rx) = bounded(1);

        let tx_guard = self.tx.lock();
        let started = std::time::Instant::now();
        tx_guard
            .as_ref()
            .ok_or_else(|| MidgeError::internal("version manager stopped"))?
            .send(VersionEditRequest {
                edit,
                response_tx: Some(response_tx),
            })
            .map_err(|_| MidgeError::internal("version manager stopped"))?;
        tracing::trace!(send_ms = %started.elapsed().as_millis(), "version_manager.apply_edit_sync send duration (ms)");
        drop(tx_guard); // Release lock before blocking on response

        // Wait for the actor to process the request and send a response.
        let started_recv = std::time::Instant::now();
        let res = response_rx
            .recv()
            .map_err(|_| MidgeError::internal("version manager response lost"))?;
        tracing::trace!(recv_ms = %started_recv.elapsed().as_millis(), "version_manager.apply_edit_sync recv duration (ms)");

        res
    }

    /// Shutdown the version manager and wait for pending edits.
    pub fn shutdown(&self) {
        if let Some(tx) = self.tx.lock().take() {
            drop(tx); // Close channel to signal shutdown
        }

        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for VersionManager {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.lock().take() {
            tracing::warn!("VersionManager dropped without explicit shutdown");
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::manifest::types::{FileMeta, Manifest};
    use crate::core::manifest::VersionSet;

    #[test]
    fn should_process_add_file_edit() {
        // Arrange - use mem_mode to skip disk I/O
        let db_path = PathBuf::from("/nonexistent");

        let manifest = Manifest::default();
        let version = VersionSet::new(manifest);
        let version_set = AtomicVersionSet::new(version);

        let manager = VersionManager::new(version_set.clone(), db_path, None, true);

        let file = FileMeta {
            name: "test.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            ..Default::default()
        };

        // Act
        manager
            .apply_edit_sync(VersionEdit::AddFile {
                file: Box::new(file),
            })
            .unwrap();

        // Assert
        let current = version_set.load();
        assert_eq!(current.manifest.files.len(), 1);
        assert_eq!(current.manifest.files[0].name, "test.sst");

        manager.shutdown();
    }

    #[test]
    fn should_process_multiple_edits_serially() {
        // Arrange - use mem_mode to skip disk I/O
        let db_path = PathBuf::from("/nonexistent");

        let manifest = Manifest::default();
        let version = VersionSet::new(manifest);
        let version_set = AtomicVersionSet::new(version);

        let manager = VersionManager::new(version_set.clone(), db_path, None, true);

        // Act
        for i in 0..5 {
            let file = FileMeta {
                name: format!("test_{}.sst", i),
                level: 0,
                size_bytes: 1024,
                ..Default::default()
            };
            manager
                .apply_edit_sync(VersionEdit::AddFile {
                    file: Box::new(file),
                })
                .unwrap();
        }

        // Assert
        let current = version_set.load();
        assert_eq!(current.manifest.files.len(), 5);

        manager.shutdown();
    }

    #[test]
    fn should_handle_async_edits() {
        // Arrange - use mem_mode to skip disk I/O
        let db_path = PathBuf::from("/nonexistent");

        let manifest = Manifest::default();
        let version = VersionSet::new(manifest);
        let version_set = AtomicVersionSet::new(version);

        let manager = VersionManager::new(version_set.clone(), db_path, None, true);

        let file = FileMeta {
            name: "test.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            ..Default::default()
        };

        // Act - send async edit, then use sync edit to ensure ordering
        manager
            .apply_edit_async(VersionEdit::AddFile {
                file: Box::new(file),
            })
            .unwrap();

        // Use a sync edit to wait for the async edit to complete (deterministic)
        let file2 = FileMeta {
            name: "test2.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            ..Default::default()
        };
        manager
            .apply_edit_sync(VersionEdit::AddFile {
                file: Box::new(file2),
            })
            .unwrap();

        // Assert - both edits should be processed
        let current = version_set.load();
        assert_eq!(current.manifest.files.len(), 2);

        manager.shutdown();
    }
}
