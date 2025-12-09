//! Cloud configuration and provider resolution.
//!
//! Implements cloud mode configuration with automatic provider detection.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{CloudMode, ConfigError, ConfigResult, Goal};

/// Context for storage naming (hierarchical path / logical namespace).
///
/// This is intentionally lightweight and used by builders and config
/// conversion code to carry path/prefix information for cloud-backed
/// storage modes. Users can leverage the path for multi-tenancy, departments,
/// organizations, environments, or any other logical partitioning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageContext {
    /// Path component for hierarchical organization in cloud storage.
    /// Examples: "customer-123", "prod/us-east", "dept/engineering"
    pub path: String,
}

impl StorageContext {
    /// Create a new StorageContext with a path component.
    ///
    /// # Examples
    /// ```
    /// # use cntryl_midge::config::cloud::StorageContext;
    /// // Multi-tenancy
    /// let ctx = StorageContext::new("customer-123");
    ///
    /// // Environment separation
    /// let ctx = StorageContext::new("prod/us-east-1");
    ///
    /// // Organizational hierarchy
    /// let ctx = StorageContext::new("acme-corp/engineering/team-a");
    /// ```
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
        }
    }

    /// Get the path component.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the full storage prefix including base path.
    pub fn prefix(&self) -> String {
        if self.path.is_empty() {
            "midge".to_string()
        } else {
            format!("midge/{}", self.path)
        }
    }
}

/// Cloud storage configuration.
#[derive(Clone)]
pub struct CloudConfig {
    /// Cloud storage mode
    pub mode: CloudMode,

    /// Cloud storage backend (S3, GCS, Azure, or mock)
    pub backend: Arc<dyn crate::cloud::StorageBackend>,

    /// Bucket name
    pub bucket: String,

    /// Optional prefix for all objects
    pub prefix: Option<String>,

    /// Derived upload parameters
    pub upload_params: UploadParams,

    /// Derived download parameters
    pub download_params: DownloadParams,
}

impl std::fmt::Debug for CloudConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudConfig")
            .field("mode", &self.mode)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("upload_params", &self.upload_params)
            .field("download_params", &self.download_params)
            .finish_non_exhaustive()
    }
}

/// Upload behavior parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadParams {
    /// Upload concurrency (parallel parts)
    pub concurrency: usize,

    /// Multipart chunk size in bytes
    pub chunk_size: usize,

    /// Maximum pending upload queue size in bytes
    pub max_queue_size: usize,

    /// Retry attempts
    pub max_retries: u32,

    /// Optional bandwidth cap in bytes/sec
    pub bandwidth_cap: Option<usize>,
}

/// Download behavior parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadParams {
    /// Prefetch depth (0 = disabled)
    pub prefetch_depth: usize,

    /// Download concurrency
    pub concurrency: usize,

    /// Chunk size for multipart downloads
    pub chunk_size: usize,
}

impl CloudConfig {
    /// Create a new cloud configuration.
    pub fn new(
        mode: CloudMode,
        backend: Arc<dyn crate::cloud::StorageBackend>,
        bucket: String,
        prefix: Option<String>,
        goal: Goal,
    ) -> ConfigResult<Self> {
        // Validate bucket name
        if bucket.is_empty() {
            return Err(ConfigError::CloudBucketRequired { mode });
        }

        // Derive upload/download parameters from mode and goal
        let upload_params = Self::derive_upload_params(mode, goal);
        let download_params = Self::derive_download_params(mode, goal);

        Ok(Self {
            mode,
            backend,
            bucket,
            prefix,
            upload_params,
            download_params,
        })
    }

    /// Derive upload parameters from mode and goal.
    fn derive_upload_params(mode: CloudMode, goal: Goal) -> UploadParams {
        let (concurrency, chunk_size, max_queue_size) = match (mode, goal) {
            // Cache mode: eager upload, moderate concurrency
            (CloudMode::Cache, Goal::Latency) => (4, 16 * 1024 * 1024, 256 * 1024 * 1024),
            (CloudMode::Cache, Goal::Throughput) => (8, 64 * 1024 * 1024, 512 * 1024 * 1024),
            (CloudMode::Cache, Goal::Cost) => (2, 32 * 1024 * 1024, 128 * 1024 * 1024),

            // Tiered mode: similar to cache
            (CloudMode::Tiered, Goal::Latency) => (4, 16 * 1024 * 1024, 256 * 1024 * 1024),
            (CloudMode::Tiered, Goal::Throughput) => (8, 64 * 1024 * 1024, 512 * 1024 * 1024),
            (CloudMode::Tiered, Goal::Cost) => (2, 32 * 1024 * 1024, 128 * 1024 * 1024),

            // Replicated mode: higher concurrency for dual-region
            (CloudMode::Replicated, Goal::Latency) => (8, 16 * 1024 * 1024, 512 * 1024 * 1024),
            (CloudMode::Replicated, Goal::Throughput) => (16, 64 * 1024 * 1024, 1024 * 1024 * 1024),
            (CloudMode::Replicated, Goal::Cost) => (4, 32 * 1024 * 1024, 256 * 1024 * 1024),

            // Off mode: no uploads
            (CloudMode::Off, _) => (0, 0, 0),
        };

        let max_retries = 3; // Standard retry count
        let bandwidth_cap = match goal {
            Goal::Cost => Some(50 * 1024 * 1024), // 50 MB/s cap for cost optimization
            _ => None,                            // No cap for latency/throughput
        };

        UploadParams {
            concurrency,
            chunk_size,
            max_queue_size,
            max_retries,
            bandwidth_cap,
        }
    }

    /// Derive download parameters from mode and goal.
    fn derive_download_params(mode: CloudMode, goal: Goal) -> DownloadParams {
        let (prefetch_depth, concurrency, chunk_size) = match (mode, goal) {
            // Cache mode: no prefetch (always local)
            (CloudMode::Cache, _) => (0, 0, 0),

            // Tiered mode: prefetch for range scans
            (CloudMode::Tiered, Goal::Latency) => (2, 4, 16 * 1024 * 1024),
            (CloudMode::Tiered, Goal::Throughput) => (4, 8, 64 * 1024 * 1024),
            (CloudMode::Tiered, Goal::Cost) => (1, 2, 32 * 1024 * 1024),

            // Replicated mode: same as cache (always local)
            (CloudMode::Replicated, _) => (0, 0, 0),

            // Off mode: no downloads
            (CloudMode::Off, _) => (0, 0, 0),
        };

        DownloadParams {
            prefetch_depth,
            concurrency,
            chunk_size,
        }
    }

    /// Get upload concurrency.
    pub fn upload_concurrency(&self) -> usize {
        self.upload_params.concurrency
    }

    /// Get multipart chunk size.
    pub fn multipart_chunk_size(&self) -> usize {
        self.upload_params.chunk_size
    }

    /// Get prefetch depth.
    pub fn prefetch_depth(&self) -> usize {
        self.download_params.prefetch_depth
    }
}

/// Auto-detect cloud provider from environment.
pub fn auto_detect_provider() -> Option<String> {
    // Check AWS credentials
    if std::env::var("AWS_ACCESS_KEY_ID").is_ok()
        || std::env::var("AWS_PROFILE").is_ok()
        || std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").is_ok()
    {
        return Some("aws".to_string());
    }

    // Check GCP credentials
    if std::env::var("GOOGLE_APPLICATION_CREDENTIALS").is_ok()
        || std::env::var("GCLOUD_PROJECT").is_ok()
    {
        return Some("gcp".to_string());
    }

    // Check Azure credentials
    if std::env::var("AZURE_STORAGE_ACCOUNT").is_ok()
        || std::env::var("AZURE_STORAGE_CONNECTION_STRING").is_ok()
    {
        return Some("azure".to_string());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::mock::MockCloudBackend;

    #[test]
    fn should_configure_cache_mode_params() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let config = CloudConfig::new(
            CloudMode::Cache,
            backend,
            "test-bucket".to_string(),
            None,
            Goal::Latency,
        )
        .unwrap();

        // Assert
        assert_eq!(config.upload_params.concurrency, 4);
        assert_eq!(config.download_params.prefetch_depth, 0); // Cache = always local
    }

    #[test]
    fn should_configure_tiered_mode_params() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let config = CloudConfig::new(
            CloudMode::Tiered,
            backend,
            "test-bucket".to_string(),
            None,
            Goal::Latency,
        )
        .unwrap();

        // Assert
        assert_eq!(config.download_params.prefetch_depth, 2); // Tiered = prefetch enabled
    }

    #[test]
    fn should_configure_replicated_mode_params() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let config = CloudConfig::new(
            CloudMode::Replicated,
            backend,
            "test-bucket".to_string(),
            None,
            Goal::Throughput,
        )
        .unwrap();

        // Assert
        assert_eq!(config.upload_params.concurrency, 16); // Higher for dual-region
    }

    #[test]
    fn should_reject_empty_bucket() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let result = CloudConfig::new(
            CloudMode::Cache,
            backend,
            String::new(),
            None,
            Goal::Latency,
        );

        // Assert
        assert!(matches!(
            result,
            Err(ConfigError::CloudBucketRequired { .. })
        ));
    }

    #[test]
    fn should_apply_bandwidth_cap_for_cost_goal() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let config = CloudConfig::new(
            CloudMode::Cache,
            backend,
            "test-bucket".to_string(),
            None,
            Goal::Cost,
        )
        .unwrap();

        // Assert
        assert!(config.upload_params.bandwidth_cap.is_some());
    }
}
