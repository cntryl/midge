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
    pub fn new(version_set: AtomicVersionSet, db_path: PathBuf) -> Self {
        let (tx, rx) = bounded(100); // Backpressure after 100 pending edits

        let handle = thread::Builder::new()
            .name("version-manager".to_string())
            .spawn(move || {
                Self::run_actor(version_set, db_path, rx);
            })
            .expect("Failed to spawn version manager thread");

        Self {
            tx: Mutex::new(Some(tx)),
            handle: Mutex::new(Some(handle)),
        }
    }

    /// Main actor loop - processes edits serially.
    fn run_actor(
        version_set: AtomicVersionSet,
        db_path: PathBuf,
        rx: Receiver<VersionEditRequest>,
    ) {
        tracing::info!("Version manager actor started");

        while let Ok(request) = rx.recv() {
            let result = Self::process_edit(&version_set, &db_path, request.edit);

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
    ) -> MidgeResult<()> {
        // Load current version
        let current = version_set.load();

        // Apply edit to create new version
        let new_version = current.apply_edit(edit)?;

        // Save manifest atomically
        new_version.manifest.save_atomic(db_path)?;

        // Publish new version atomically
        version_set.store(Arc::new(new_version));

        Ok(())
    }

    /// Apply a version edit asynchronously.
    /// Returns immediately - edit is queued for processing.
    pub fn apply_edit_async(&self, edit: VersionEdit) -> MidgeResult<()> {
        let tx_guard = self.tx.lock();
        tx_guard
            .as_ref()
            .ok_or_else(|| MidgeError::internal("version manager stopped"))?
            .send(VersionEditRequest {
                edit,
                response_tx: None,
            })
            .map_err(|_| MidgeError::internal("version manager stopped"))
    }

    /// Apply a version edit synchronously.
    /// Blocks until edit is processed and returns result.
    pub fn apply_edit_sync(&self, edit: VersionEdit) -> MidgeResult<()> {
        let (response_tx, response_rx) = bounded(1);

        let tx_guard = self.tx.lock();
        tx_guard
            .as_ref()
            .ok_or_else(|| MidgeError::internal("version manager stopped"))?
            .send(VersionEditRequest {
                edit,
                response_tx: Some(response_tx),
            })
            .map_err(|_| MidgeError::internal("version manager stopped"))?;
        drop(tx_guard); // Release lock before blocking on response

        response_rx
            .recv()
            .map_err(|_| MidgeError::internal("version manager response lost"))?
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
    use tempfile::TempDir;

    #[test]
    fn should_process_add_file_edit() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_path_buf();

        let manifest = Manifest::default();
        let version = VersionSet::new(manifest);
        let version_set = AtomicVersionSet::new(version);

        let manager = VersionManager::new(version_set.clone(), db_path);

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
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_path_buf();

        let manifest = Manifest::default();
        let version = VersionSet::new(manifest);
        let version_set = AtomicVersionSet::new(version);

        let manager = VersionManager::new(version_set.clone(), db_path);

        // Act - add multiple files
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
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_path_buf();

        let manifest = Manifest::default();
        let version = VersionSet::new(manifest);
        let version_set = AtomicVersionSet::new(version);

        let manager = VersionManager::new(version_set.clone(), db_path);

        let file = FileMeta {
            name: "test.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            ..Default::default()
        };

        // Act
        manager
            .apply_edit_async(VersionEdit::AddFile {
                file: Box::new(file),
            })
            .unwrap();

        // Wait for processing
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Assert
        let current = version_set.load();
        assert_eq!(current.manifest.files.len(), 1);

        manager.shutdown();
    }
}
