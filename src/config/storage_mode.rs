//! Storage mode configuration for Midge Engine
//!
//! Provides type-safe configuration for three storage modes:
//! 1. Pure in-memory (no persistence)
//! 2. Local files only (traditional LSM)
//! 3. Cloud-backed (WAL and SSTs in cloud storage)

use std::path::PathBuf;
use std::sync::Arc;

use crate::config::cloud::StorageContext;

/// Storage mode configuration for the engine
///
/// This determines where data is persisted and how durability is achieved.
///
/// # Examples
///
/// ```rust,no_run
/// use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
///
/// // Pure in-memory (no persistence)
/// let opts = MidgeOptions {
///     storage_mode: StorageMode::Memory,
///     ..Default::default()
/// };
///
/// // Local files only
/// let opts = MidgeOptions {
///     storage_mode: StorageMode::LocalDisk {
///         db_path: "./my_db".into(),
///     },
///     ..Default::default()
/// };
/// ```
#[derive(Clone)]
pub enum StorageMode {
    /// Pure in-memory storage with no persistence.
    ///
    /// - WAL: In-memory buffer
    /// - SST: In-memory tables
    /// - Manifest: In-memory
    ///
    /// **Use case:** Testing, caching, ephemeral data
    ///
    /// **Durability:** None - data lost on restart
    Memory,

    /// Local disk storage with file-based WAL and SSTs.
    ///
    /// - WAL: Local filesystem
    /// - SST: Local filesystem
    /// - Manifest: Local filesystem
    ///
    /// **Use case:** Traditional embedded database, single-node deployment
    ///
    /// **Durability:** Local disk only - vulnerable to disk failure
    LocalDisk {
        /// Path to database directory
        db_path: PathBuf,
    },

    /// Cloud-backed storage with optional local caching.
    ///
    /// - WAL: Cloud storage (with optional local sync)
    /// - SST: Cloud storage (with optional local caching)
    /// - Manifest: Coordinated via cloud checkpoint
    ///
    /// **Use case:** Distributed systems, high durability requirements, S3-based storage
    ///
    /// **Durability:** Cloud storage (11+ nines) - survives node failure
    CloudBacked {
        /// Local directory for temporary files and cache
        local_cache_path: PathBuf,

        /// Cloud storage backend (S3, Azure Blob, GCS, or mock)
        cloud_backend: Arc<dyn crate::cloud::StorageBackend>,

        /// Cloud storage context for hierarchical naming
        ///
        /// Used to generate paths like: `/realm/area/resource/sst/sst_000123.blob`
        ///
        /// Enables multi-tenancy and logical isolation in shared storage.
        storage_context: StorageContext,

        /// Whether to sync WAL to local disk before cloud upload
        ///
        /// - `true`: Sync to local disk first (fast local durability)
        /// - `false`: Cloud-only durability (simpler, no local state)
        local_wal_sync: bool,

        /// Maximum batch size for cloud WAL uploads (bytes)
        ///
        /// Larger batches = more efficient uploads but higher latency.
        /// Recommended: 1-4MB
        wal_batch_size: usize,

        /// Download cache capacity for cloud SSTs (number of files)
        ///
        /// Caches recently accessed SSTs in memory to reduce cloud reads.
        /// 0 = disabled (always fetch from cloud)
        sst_cache_capacity: usize,
    },
}

impl std::fmt::Debug for StorageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageMode::Memory => write!(f, "StorageMode::Memory"),
            StorageMode::LocalDisk { db_path } => {
                write!(f, "StorageMode::LocalDisk {{ db_path: {:?} }}", db_path)
            }
            StorageMode::CloudBacked {
                local_cache_path,
                storage_context,
                local_wal_sync,
                wal_batch_size,
                sst_cache_capacity,
                ..
            } => f
                .debug_struct("StorageMode::CloudBacked")
                .field("local_cache_path", local_cache_path)
                .field("storage_context", storage_context)
                .field("local_wal_sync", local_wal_sync)
                .field("wal_batch_size", wal_batch_size)
                .field("sst_cache_capacity", sst_cache_capacity)
                .finish_non_exhaustive(),
        }
    }
}

impl Default for StorageMode {
    fn default() -> Self {
        StorageMode::LocalDisk {
            db_path: PathBuf::from("./target/tmp/midge_db"),
        }
    }
}

impl StorageMode {
    /// Returns true if this is a memory-only mode
    pub fn is_memory(&self) -> bool {
        matches!(self, StorageMode::Memory)
    }

    /// Returns true if this is local disk storage
    pub fn is_local_disk(&self) -> bool {
        matches!(self, StorageMode::LocalDisk { .. })
    }

    /// Returns true if this is cloud-backed storage
    pub fn is_cloud_backed(&self) -> bool {
        matches!(self, StorageMode::CloudBacked { .. })
    }

    /// Get the local path for this storage mode
    ///
    /// Returns the path where local files (WAL, SST, manifest) are stored.
    /// For cloud-backed mode, this is the cache directory.
    pub fn local_path(&self) -> PathBuf {
        match self {
            StorageMode::Memory => std::env::temp_dir()
                .join("midge-mem")
                .join(uuid::Uuid::new_v4().to_string()),
            StorageMode::LocalDisk { db_path } => db_path.clone(),
            StorageMode::CloudBacked {
                local_cache_path, ..
            } => local_cache_path.clone(),
        }
    }

    /// Get the cloud backend if this is cloud-backed mode
    pub fn cloud_backend(&self) -> Option<Arc<dyn crate::cloud::StorageBackend>> {
        match self {
            StorageMode::CloudBacked { cloud_backend, .. } => Some(Arc::clone(cloud_backend)),
            _ => None,
        }
    }

    /// Get the storage context if this is cloud-backed mode
    pub fn storage_context(&self) -> Option<&StorageContext> {
        match self {
            StorageMode::CloudBacked {
                storage_context, ..
            } => Some(storage_context),
            _ => None,
        }
    }

    /// Get the cloud storage prefix for SST/WAL keys
    pub fn cloud_prefix(&self) -> Option<String> {
        match self {
            StorageMode::CloudBacked {
                storage_context, ..
            } => Some(storage_context.prefix()),
            _ => None,
        }
    }
}

/// Builder for cloud-backed storage mode with sensible defaults
#[derive(Clone)]
pub struct CloudStorageBuilder {
    local_cache_path: PathBuf,
    cloud_backend: Arc<dyn crate::cloud::StorageBackend>,
    storage_context: StorageContext,
    local_wal_sync: bool,
    wal_batch_size: usize,
    sst_cache_capacity: usize,
}

impl CloudStorageBuilder {
    /// Create a new cloud storage builder with required parameters
    ///
    /// # Arguments
    ///
    /// * `local_cache_path` - Directory for local cache and temporary files
    /// * `cloud_backend` - Cloud storage backend implementation
    pub fn new(
        local_cache_path: PathBuf,
        cloud_backend: Arc<dyn crate::cloud::StorageBackend>,
    ) -> Self {
        Self {
            local_cache_path,
            cloud_backend,
            storage_context: StorageContext::default(),
            local_wal_sync: true, // Enable local sync by default for fast durability
            wal_batch_size: 2_097_152, // 2MB default
            sst_cache_capacity: 100, // Cache 100 SSTs by default
        }
    }

    /// Set the storage context for hierarchical cloud naming
    pub fn with_storage_context(mut self, context: StorageContext) -> Self {
        self.storage_context = context;
        self
    }

    /// Set custom path for hierarchical organization
    ///
    /// The path can be used for multi-tenancy, departments, environments, etc.
    pub fn with_path(mut self, path: String) -> Self {
        self.storage_context = StorageContext::new(&path);
        self
    }

    /// Enable or disable local WAL sync
    pub fn with_local_wal_sync(mut self, enabled: bool) -> Self {
        self.local_wal_sync = enabled;
        self
    }

    /// Set WAL batch size for cloud uploads
    pub fn with_wal_batch_size(mut self, size: usize) -> Self {
        self.wal_batch_size = size;
        self
    }

    /// Set SST download cache capacity
    pub fn with_sst_cache_capacity(mut self, capacity: usize) -> Self {
        self.sst_cache_capacity = capacity;
        self
    }

    /// Build the storage mode
    pub fn build(self) -> StorageMode {
        StorageMode::CloudBacked {
            local_cache_path: self.local_cache_path,
            cloud_backend: self.cloud_backend,
            storage_context: self.storage_context,
            local_wal_sync: self.local_wal_sync,
            wal_batch_size: self.wal_batch_size,
            sst_cache_capacity: self.sst_cache_capacity,
        }
    }
}
