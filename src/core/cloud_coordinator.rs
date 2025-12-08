//! Cloud storage coordination through EngineRuntime
//!
//! Phase 7.1-7.3: Coordinates all cloud operations (SST uploads, WAL uploads, cache eviction)
//! through the centralized EngineRuntime executor for deterministic ordering with other
//! background operations (flush, compaction).
//!
//! Instead of independent cloud operation threads, all cloud work is submitted as
//! runtime tasks enabling:
//! - Deterministic ordering: same manifest → same sequence of cloud operations
//! - Unified visibility: all background work visible through runtime
//! - Coordinated resource usage: throttling applies across all operation types
//! - Graceful shutdown: cloud ops included in runtime shutdown sequence

use crate::error::MidgeResult;
use crate::core::runtime::{RuntimeTask, RuntimeTaskKind};
use std::sync::Arc;

/// Coordinates all cloud storage operations through EngineRuntime
///
/// This coordinator is lightweight - it doesn't own cloud resources but provides
/// a coordinated interface for submitting cloud operations as runtime tasks.
pub struct CloudCoordinator;

impl CloudCoordinator {
    /// Create a new cloud coordinator
    pub fn new() -> Self {
        Self
    }

    /// Submit a cloud SST upload as a runtime task
    ///
    /// # Arguments
    /// * `runtime` - EngineRuntime to submit task to
    /// * `sst_id` - SST identifier
    /// * `upload_fn` - Callback to perform the actual upload
    pub fn submit_sst_upload_task<F>(
        &self,
        runtime: &Arc<crate::core::runtime::EngineRuntime>,
        sst_id: String,
        upload_fn: F,
    ) -> MidgeResult<()>
    where
        F: Fn() + Send + 'static,
    {
        let description = format!("cloud_upload_sst({})", sst_id);

        let task = RuntimeTask::new(
            RuntimeTaskKind::Maintenance,
            description,
            Box::new(upload_fn),
        );

        runtime.submit(task)
    }

    /// Submit a cloud SST download as a runtime task
    ///
    /// # Arguments
    /// * `runtime` - EngineRuntime to submit task to
    /// * `sst_id` - SST identifier
    /// * `download_fn` - Callback to perform the actual download
    pub fn submit_sst_download_task<F>(
        &self,
        runtime: &Arc<crate::core::runtime::EngineRuntime>,
        sst_id: String,
        download_fn: F,
    ) -> MidgeResult<()>
    where
        F: Fn() + Send + 'static,
    {
        let description = format!("cloud_download_sst({})", sst_id);

        let task = RuntimeTask::new(
            RuntimeTaskKind::Maintenance,
            description,
            Box::new(download_fn),
        );

        runtime.submit(task)
    }

    /// Submit a cache eviction task as a runtime task
    ///
    /// # Arguments
    /// * `runtime` - EngineRuntime to submit task to
    /// * `evict_fn` - Callback to perform the actual eviction
    pub fn submit_eviction_task<F>(
        &self,
        runtime: &Arc<crate::core::runtime::EngineRuntime>,
        evict_fn: F,
    ) -> MidgeResult<()>
    where
        F: Fn() + Send + 'static,
    {
        let description = "cloud_evict_cache".to_string();

        let task = RuntimeTask::new(
            RuntimeTaskKind::Maintenance,
            description,
            Box::new(evict_fn),
        );

        runtime.submit(task)
    }
}

impl Default for CloudCoordinator {
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
        let _coordinator = CloudCoordinator::new();

        // Assert - creation should succeed
    }

    #[test]
    fn should_create_default_successfully() {
        // Arrange (using default trait implementation)

        // Act
        let _coordinator = CloudCoordinator::default();

        // Assert - creation should succeed
    }

    #[test]
    fn should_submit_sst_upload_task_successfully() {
        // Arrange
        let coordinator = CloudCoordinator::new();
        let (shutdown_tx, shutdown_rx) = crossbeam::channel::unbounded();
        let runtime = Arc::new(crate::core::runtime::EngineRuntime::new(shutdown_tx, shutdown_rx));

        // Act
        let result = coordinator.submit_sst_upload_task(&runtime, "sst1.sst".to_string(), || {
            // No-op for test
        });

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_submit_sst_download_task_successfully() {
        // Arrange
        let coordinator = CloudCoordinator::new();
        let (shutdown_tx, shutdown_rx) = crossbeam::channel::unbounded();
        let runtime = Arc::new(crate::core::runtime::EngineRuntime::new(shutdown_tx, shutdown_rx));

        // Act
        let result = coordinator.submit_sst_download_task(&runtime, "sst1.sst".to_string(), || {
            // No-op for test
        });

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_submit_eviction_task_successfully() {
        // Arrange
        let coordinator = CloudCoordinator::new();
        let (shutdown_tx, shutdown_rx) = crossbeam::channel::unbounded();
        let runtime = Arc::new(crate::core::runtime::EngineRuntime::new(shutdown_tx, shutdown_rx));

        // Act
        let result = coordinator.submit_eviction_task(&runtime, || {
            // No-op for test
        });

        // Assert
        assert!(result.is_ok());
    }
}
