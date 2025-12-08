//! WAL upload coordination through EngineRuntime
//!
//! Phase 6.3: Coordinates WAL segment uploads and syncs through the centralized
//! EngineRuntime executor for deterministic ordering with flushes and compaction.
//!
//! Instead of independent background WAL upload threads, all WAL upload operations
//! are submitted as `RuntimeTaskKind::WalUpload` tasks, ensuring:
//! - Deterministic ordering relative to flush/compaction
//! - Single point of control for WAL durability
//! - Simplified state ownership (WAL state owned by runtime)

use crate::error::MidgeResult;
use crate::core::runtime::{RuntimeTask, RuntimeTaskKind};
use std::sync::Arc;

/// Lightweight coordinator for WAL uploads through EngineRuntime
/// 
/// This coordinator doesn't own the WAL controller - it takes a callback
/// closure at task submission time to perform the actual sync operation.
/// This allows coordination without requiring Arc<WalController>.
pub struct WalUploadCoordinator;

impl WalUploadCoordinator {
    /// Create a new WAL upload coordinator
    pub fn new() -> Self {
        Self
    }

    /// Submit a WAL sync operation as a runtime task
    ///
    /// All WAL sync operations (both local and cloud) are submitted through
    /// this method to ensure deterministic ordering with flushes and compaction.
    ///
    /// # Arguments
    /// * `runtime` - EngineRuntime to submit task to
    /// * `wal_sync_fn` - Callback to perform the actual WAL sync
    /// * `wait_for_cloud` - Whether to wait for cloud uploads to complete
    pub fn submit_sync_task<F>(
        &self,
        runtime: &Arc<crate::core::runtime::EngineRuntime>,
        wal_sync_fn: F,
        wait_for_cloud: bool,
    ) -> MidgeResult<()>
    where
        F: Fn(bool) + Send + 'static,
    {
        let description = if wait_for_cloud {
            "wal_sync_with_cloud".to_string()
        } else {
            "wal_sync_local".to_string()
        };

        let task = RuntimeTask::new(
            RuntimeTaskKind::WalUpload,
            description,
            Box::new(move || {
                wal_sync_fn(wait_for_cloud);
            }),
        );

        runtime.submit(task)
    }
}

impl Default for WalUploadCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_coordinator_successfully() {
        // Arrange (creating coordinator with default settings)

        // Act
        let _coordinator = WalUploadCoordinator::new();

        // Assert - creation should succeed
    }

    #[test]
    fn should_create_default_successfully() {
        // Arrange (using default trait implementation)

        // Act
        let _coordinator = WalUploadCoordinator::default();

        // Assert - creation should succeed
    }

    #[test]
    fn should_submit_local_sync_task_successfully() {
        // Arrange
        let coordinator = WalUploadCoordinator::new();
        let (shutdown_tx, shutdown_rx) = crossbeam::channel::unbounded();
        let runtime = std::sync::Arc::new(crate::core::runtime::EngineRuntime::new(shutdown_tx, shutdown_rx));
        let sync_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sync_called_clone = Arc::clone(&sync_called);

        // Act
        let result = coordinator.submit_sync_task(&runtime, move |_| {
            sync_called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        }, false);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_submit_cloud_sync_task_successfully() {
        // Arrange
        let coordinator = WalUploadCoordinator::new();
        let (shutdown_tx, shutdown_rx) = crossbeam::channel::unbounded();
        let runtime = std::sync::Arc::new(crate::core::runtime::EngineRuntime::new(shutdown_tx, shutdown_rx));

        // Act
        let result = coordinator.submit_sync_task(&runtime, |_| {
            // No-op for test
        }, true);

        // Assert
        assert!(result.is_ok());
    }
}

