//! Easy-to-use builder for cloud storage configurations.
//!
//! This module provides simple, opinionated builders for common cloud deployment patterns.
//! All builders work seamlessly with MockCloudBackend for testing and development.
//!
//! # Examples
//!
//! ```rust,no_run
//! use cntryl_midge::config::cloud_builder::CloudConfigBuilder;
//! use cntryl_midge::cloud::MockCloudBackend;
//! use std::sync::Arc;
//!
//! // Strict durability (fsync + verified cloud upload)
//! let config = CloudConfigBuilder::strict_durability(
//!     Arc::new(MockCloudBackend::new()),
//!     "./local_cache"
//! )
//! .with_max_cache_size_mb(500)
//! .build();
//!
//! // Steady durability (async cloud uploads)
//! let config = CloudConfigBuilder::balanced_durability(
//!     Arc::new(MockCloudBackend::new()),
//!     "./local_cache"
//! )
//! .with_sync_interval_ms(20)
//! .build();
//!
//! // Cloud-replicated (full cloud durability)
//! let config = CloudConfigBuilder::replicated_durability(
//!     Arc::new(MockCloudBackend::new()),
//!     "./local_cache"
//! )
//! .with_local_cache_enabled(true)
//! .with_path("customer-123") // Hierarchical organization
//! .build();
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::cloud::{HybridStorage, HybridStorageBackend, StorageBackend};
use crate::config::cloud::StorageContext;
use crate::config::{CloudMode, CloudStorageBuilder, Durability, StorageMode};

/// Builder for cloud storage configurations with opinionated defaults.
///
/// This builder simplifies cloud configuration by providing preset configurations
/// for common durability requirements:
/// - **Strict**: Every write synced + verified in cloud (highest durability, highest latency)
/// - **Steady**: Async cloud uploads with configurable intervals (balanced)
/// - **CloudReplicated**: Cloud-first durability with optional local cache (cloud-native)
#[derive(Clone)]
pub struct CloudConfigBuilder {
    backend: Arc<dyn StorageBackend>,
    local_cache_path: PathBuf,
    storage_context: StorageContext,

    // Durability settings
    durability: Durability,
    cloud_mode: CloudMode,

    // Cache settings
    max_cache_size_mb: Option<usize>,
    cache_enabled: bool,

    // WAL settings
    local_wal_sync: bool,
    wal_batch_size: usize,
    sync_interval_ms: Option<u64>,

    // SST settings
    sst_cache_capacity: usize,
    prefetch_enabled: bool,
}

impl CloudConfigBuilder {
    /// Create builder for **Strict Durability** mode (Zero Data Loss).
    ///
    /// **Guarantee:** Every write is synced to local disk AND verified in cloud before ack.
    ///
    /// This provides **absolute durability** at the cost of latency. Use this when
    /// you cannot afford to lose ANY data, even in catastrophic failures.
    ///
    /// **How it works:**
    /// 1. Write to local WAL + fsync (durable on local disk)
    /// 2. Upload to cloud storage
    /// 3. Wait for cloud confirmation
    /// 4. Only then return success to caller
    ///
    /// **Characteristics:**
    /// - Local WAL fsync: ✅ Enabled (every write)
    /// - Cloud upload: ✅ **Synchronous** (blocks until cloud confirms)
    /// - Local cache: ✅ Enabled (large cache for fast reads)
    /// - RPO: **0** (zero data loss, even if node explodes)
    /// - Latency: High (50-200ms depending on cloud RTT)
    ///
    /// **Use case:** Financial transactions, audit logs, critical metadata
    ///
    /// **vs Steady:** Trades 100x higher latency for zero data loss
    /// **vs CloudReplicated:** Much higher latency, but guaranteed durability
    pub fn strict_durability(
        backend: Arc<dyn StorageBackend>,
        local_cache_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            backend,
            local_cache_path: local_cache_path.into(),
            storage_context: StorageContext::default(),
            durability: Durability::Strict,
            cloud_mode: CloudMode::Cache, // Always cache locally for fast reads
            max_cache_size_mb: Some(1024), // 1GB default
            cache_enabled: true,
            local_wal_sync: true,       // Sync to local disk
            wal_batch_size: 256 * 1024, // 256KB (small batches for low latency)
            sync_interval_ms: None,     // Synchronous uploads
            sst_cache_capacity: 100,
            prefetch_enabled: false, // Strict mode = single-threaded
        }
    }

    /// Create builder for **Balanced Durability** mode.
    ///
    /// **Guarantee:** Async cloud uploads every ~20ms (configurable interval).
    ///
    /// This is the **recommended default** for most applications. It provides:
    /// - Fast writes (no blocking on cloud)
    /// - Good durability (~20ms RPO)
    /// - Efficient cloud uploads (batched)
    ///
    /// **How it works:**
    /// 1. Writes sync to local WAL every ~20ms (interval-based fsync)
    /// 2. Background thread uploads to cloud asynchronously
    /// 3. Large cache holds all SSTs locally for fast reads
    ///
    /// **Characteristics:**
    /// - Local WAL fsync: ✅ Enabled (interval-based, every ~20ms)
    /// - Cloud upload: ⚡ Asynchronous (background worker)
    /// - Local cache: ✅ Enabled (large cache for all SSTs)
    /// - RPO: ~20ms (lose up to 20ms of writes on node crash)
    /// - Latency: Low (writes return immediately after local sync)
    ///
    /// **Use case:** Most applications - high throughput OLTP, general purpose databases
    ///
    /// **vs Strict:** Trades 20ms of potential data loss for ~100x lower write latency
    /// **vs Replicated:** Has local WAL sync for better durability, larger cache
    pub fn balanced_durability(
        backend: Arc<dyn StorageBackend>,
        local_cache_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            backend,
            local_cache_path: local_cache_path.into(),
            storage_context: StorageContext::default(),
            durability: Durability::Steady,
            cloud_mode: CloudMode::Cache,
            max_cache_size_mb: Some(2048), // 2GB default
            cache_enabled: true,
            local_wal_sync: true,
            wal_batch_size: 2 * 1024 * 1024, // 2MB
            sync_interval_ms: Some(20),      // 20ms default
            sst_cache_capacity: 200,
            prefetch_enabled: true,
        }
    }

    /// Create builder for **Replicated Durability** mode (Cloud-First).
    ///
    /// **Guarantee:** Data durable in cloud (11+ nines). Survives node failure.
    ///
    /// This is optimized for **ephemeral compute** (containers, spot instances, serverless).
    /// Cloud is the source of truth - local disk is just a cache.
    ///
    /// **How it works:**
    /// 1. Write to local WAL (NO fsync - cloud is source of truth)
    /// 2. Background thread uploads to cloud every ~100ms
    /// 3. Small local cache (256MB) for hot data only
    /// 4. Cold data served directly from cloud
    ///
    /// **Characteristics:**
    /// - Local WAL fsync: ❌ **Disabled** (cloud is source of truth)
    /// - Cloud upload: ⚡ Asynchronous (batched every ~100ms)
    /// - Local cache: ⚠️ **Small** (256MB default - only hot data)
    /// - RPO: ~100ms (lose up to 100ms of writes on node crash)
    /// - Latency: Lowest (<0.5ms - no fsync overhead)
    ///
    /// **Use case:** Kubernetes, Docker, Lambda, spot instances, multi-region replication
    ///
    /// **vs Strict:** Much lower latency, but 100ms potential data loss
    /// **vs Balanced:** No local fsync (faster), smaller cache (less disk usage)
    pub fn replicated_durability(
        backend: Arc<dyn StorageBackend>,
        local_cache_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            backend,
            local_cache_path: local_cache_path.into(),
            storage_context: StorageContext::default(),
            durability: Durability::CloudReplicated,
            cloud_mode: CloudMode::Tiered, // Hot data local, cold data cloud
            max_cache_size_mb: Some(512),  // Smaller cache (ephemeral)
            cache_enabled: true,
            local_wal_sync: false, // Cloud-first (no local sync required)
            wal_batch_size: 4 * 1024 * 1024, // 4MB (larger batches)
            sync_interval_ms: Some(100), // Less frequent syncs
            sst_cache_capacity: 50, // Smaller cache
            prefetch_enabled: true,
        }
    }

    /// Set custom storage context for hierarchical organization.
    pub fn with_storage_context(mut self, context: StorageContext) -> Self {
        self.storage_context = context;
        self
    }

    /// Set custom path for hierarchical cloud naming.
    ///
    /// The path is used to organize data in cloud storage. Common use cases:
    /// - Multi-tenancy: `with_path("customer-123")`
    /// - Environments: `with_path("prod/us-east-1")`
    /// - Departments: `with_path("engineering/team-backend")`
    /// - Organizations: `with_path("acme-corp/division-b")`
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.storage_context = StorageContext::new(&path.into());
        self
    }

    /// Set maximum local cache size in megabytes.
    ///
    /// When cache exceeds this size, LRU eviction will remove cold SSTs.
    pub fn with_max_cache_size_mb(mut self, mb: usize) -> Self {
        self.max_cache_size_mb = Some(mb);
        self
    }

    /// Enable or disable local caching.
    ///
    /// When disabled, all reads go directly to cloud (useful for stateless deployments).
    pub fn with_local_cache_enabled(mut self, enabled: bool) -> Self {
        self.cache_enabled = enabled;
        if !enabled {
            self.max_cache_size_mb = None;
        }
        self
    }

    /// Set WAL sync interval in milliseconds (Steady mode only).
    ///
    /// This controls the RPO (Recovery Point Objective).
    /// - Lower = better durability, higher write latency
    /// - Higher = better throughput, larger data loss window
    pub fn with_sync_interval_ms(mut self, ms: u64) -> Self {
        self.sync_interval_ms = Some(ms);
        self
    }

    /// Set WAL batch size for cloud uploads.
    ///
    /// Larger batches = more efficient uploads but higher latency.
    pub fn with_wal_batch_size(mut self, bytes: usize) -> Self {
        self.wal_batch_size = bytes;
        self
    }

    /// Set SST download cache capacity (number of files).
    pub fn with_sst_cache_capacity(mut self, capacity: usize) -> Self {
        self.sst_cache_capacity = capacity;
        self
    }

    /// Enable or disable prefetching for range scans.
    pub fn with_prefetch_enabled(mut self, enabled: bool) -> Self {
        self.prefetch_enabled = enabled;
        self
    }

    /// Set cloud mode explicitly (overrides preset).
    pub fn with_cloud_mode(mut self, mode: CloudMode) -> Self {
        self.cloud_mode = mode;
        self
    }

    /// Build the StorageMode configuration.
    pub fn build(self) -> StorageMode {
        // Determine the effective backend (wrapped with caching if enabled)
        let effective_backend: Arc<dyn StorageBackend> = if self.cache_enabled
            && matches!(self.cloud_mode, CloudMode::Cache | CloudMode::Tiered)
        {
            // Adjust cache size based on mode
            let max_cache_mb = match self.cloud_mode {
                CloudMode::Tiered => {
                    // Tiered mode: small cache for hot data only
                    // Use configured size or default to 256MB for tiered
                    self.max_cache_size_mb.unwrap_or(256)
                }
                CloudMode::Cache => {
                    // Cache mode: large cache for all data
                    // Use configured size or default to 1024MB
                    self.max_cache_size_mb.unwrap_or(1024)
                }
                _ => self.max_cache_size_mb.unwrap_or(1024),
            };

            let max_cache_bytes = max_cache_mb as u64 * 1024 * 1024;

            let hybrid = HybridStorage::new(
                self.local_cache_path.clone(),
                self.backend.clone(),
                max_cache_bytes,
            )
            .expect("Failed to create HybridStorage");

            // Spawn background workers for async uploads and eviction
            hybrid.spawn_background_workers();

            // Determine sync mode based on durability
            let sync_writes = matches!(self.durability, Durability::Strict);

            Arc::new(HybridStorageBackend::new(Arc::new(hybrid), sync_writes))
        } else {
            // Use cloud backend directly (no caching)
            self.backend
        };

        CloudStorageBuilder::new(self.local_cache_path, effective_backend)
            .with_storage_context(self.storage_context)
            .with_local_wal_sync(self.local_wal_sync)
            .with_wal_batch_size(self.wal_batch_size)
            .with_sst_cache_capacity(self.sst_cache_capacity)
            .build()
    }

    /// Get the durability mode.
    pub fn durability(&self) -> Durability {
        self.durability
    }

    /// Get the cloud mode.
    pub fn cloud_mode(&self) -> CloudMode {
        self.cloud_mode
    }

    /// Get the sync interval.
    pub fn sync_interval(&self) -> Option<Duration> {
        self.sync_interval_ms.map(Duration::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::MockCloudBackend;

    #[test]
    fn should_create_strict_durability_config() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let builder = CloudConfigBuilder::strict_durability(backend, "./cache");

        // Assert
        assert_eq!(builder.durability(), Durability::Strict);
        assert_eq!(builder.cloud_mode(), CloudMode::Cache);
        assert!(builder.local_wal_sync);
        assert!(builder.cache_enabled);
        assert_eq!(builder.sync_interval(), None); // Synchronous
    }

    #[test]
    fn should_create_balanced_durability_config() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let builder = CloudConfigBuilder::balanced_durability(backend, "./cache");

        // Assert
        assert_eq!(builder.durability(), Durability::Steady);
        assert_eq!(builder.cloud_mode(), CloudMode::Cache);
        assert!(builder.local_wal_sync);
        assert_eq!(builder.sync_interval(), Some(Duration::from_millis(20)));
    }

    #[test]
    fn should_create_replicated_durability_config() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let builder = CloudConfigBuilder::replicated_durability(backend, "./cache");

        // Assert
        assert_eq!(builder.durability(), Durability::CloudReplicated);
        assert_eq!(builder.cloud_mode(), CloudMode::Tiered);
        assert!(!builder.local_wal_sync); // Cloud-first
    }

    #[test]
    fn should_customize_cache_size() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let builder = CloudConfigBuilder::balanced_durability(backend, "./cache")
            .with_max_cache_size_mb(4096);

        // Assert
        assert_eq!(builder.max_cache_size_mb, Some(4096));
    }

    #[test]
    fn should_disable_local_cache() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let builder = CloudConfigBuilder::replicated_durability(backend, "./cache")
            .with_local_cache_enabled(false);

        // Assert
        assert!(!builder.cache_enabled);
        assert_eq!(builder.max_cache_size_mb, None);
    }

    #[test]
    fn should_customize_sync_interval() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let builder =
            CloudConfigBuilder::balanced_durability(backend, "./cache").with_sync_interval_ms(50);

        // Assert
        assert_eq!(builder.sync_interval(), Some(Duration::from_millis(50)));
    }

    #[test]
    fn should_set_custom_path() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let builder =
            CloudConfigBuilder::balanced_durability(backend, "./cache").with_path("customer-123");

        // Assert
        assert_eq!(builder.storage_context.path(), "customer-123");
    }

    #[test]
    fn should_build_storage_mode_successfully() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let storage_mode = CloudConfigBuilder::strict_durability(backend, "./cache").build();

        // Assert
        assert!(storage_mode.is_cloud_backed());
        assert!(storage_mode.cloud_backend().is_some());
    }

    #[test]
    fn should_use_small_batches_for_strict_mode() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let strict = CloudConfigBuilder::strict_durability(backend.clone(), "./cache");
        let steady = CloudConfigBuilder::balanced_durability(backend, "./cache");

        // Assert
        assert!(strict.wal_batch_size < steady.wal_batch_size);
    }

    #[test]
    fn should_use_larger_batches_for_replicated_durability() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let cloud = CloudConfigBuilder::replicated_durability(backend.clone(), "./cache");
        let steady = CloudConfigBuilder::balanced_durability(backend, "./cache");

        // Assert
        assert!(cloud.wal_batch_size > steady.wal_batch_size);
    }
}
