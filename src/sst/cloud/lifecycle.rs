use crate::cloud::StorageBackend;
use crate::common::timestamp;
use crate::error::{MidgeError, MidgeResult};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, info};

/// The storage tier for archived SSTs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveTier {
    Hot,
    Warm,
    Cold,
    /// Provider-specific deep archive tier
    Glacier,
    /// Less-frequently accessed tier
    InfrequentAccess,
}

/// Lifecycle state for an SST in cloud. This type is serialized into the
/// manifest, so it must derive Serialize/Deserialize.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SstLifecycleState {
    /// SST is present and accessible locally (or in the cache)
    Active,

    /// SST has been archived to cold storage and is not directly accessible
    Archived {
        tier: ArchiveTier,
        archived_at: SystemTime,
        location: String,
        checksum: Option<String>,
    },

    /// Soft-deleted: kept for a grace period before permanent deletion
    SoftDeleted {
        deleted_at: SystemTime,
        grace_period_ends: SystemTime,
    },

    /// Permanently deleted
    Deleted { deleted_at: SystemTime },
}

impl SstLifecycleState {
    /// Return true if the SST is currently accessible for download
    pub fn is_accessible(&self) -> bool {
        matches!(self, SstLifecycleState::Active)
    }
}

/// Metadata produced when an SST is uploaded to cloud storage.
#[derive(Clone, Debug)]
pub struct SstUploadMeta {
    pub sst_id: String,
    pub location: String,
    pub size_bytes: u64,
    pub checksum: u64,
    pub uploaded_at: SystemTime,
    pub sequence_range: (u64, u64),
}

/// Lightweight SST metadata used by engine/manifest tracking.
#[derive(Clone, Debug)]
pub struct SstMetadata {
    pub sst_id: String,
    pub id: Option<u64>,
    pub len: u64,
    pub size_bytes: u64,
    pub level: u32,
    pub created_at: SystemTime,
    pub last_accessed: Option<SystemTime>,
    pub state: SstLifecycleState,
}

/// Trait representing an SST object stored in cloud.
pub trait CloudSst: Send + Sync {
    fn id(&self) -> u64;
    fn metadata(&self) -> &SstMetadata;
    fn download_bytes(&self) -> MidgeResult<Vec<u8>>;
}

/// Configuration for the cloud SST manager.
#[derive(Clone, Debug, Default)]
pub struct CloudSstManagerConfig {
    pub bucket: String,
    pub prefix: Option<String>,
    pub cache_dir: Option<PathBuf>,
}

// StorageContext has moved to `crate::config::cloud::StorageContext`.

/// Manager responsible for uploading/downloading SSTs to/from cloud.
///
/// Manages SST lifecycle in cloud storage including uploads, downloads,
/// caching, and metadata tracking.
pub struct CloudSstManager {
    pub config: CloudSstManagerConfig,
    backend: Arc<dyn StorageBackend>,
}

impl CloudSstManager {
    /// Create a new manager with cloud backend.
    pub fn new(
        config: CloudSstManagerConfig,
        backend: Arc<dyn StorageBackend>,
    ) -> MidgeResult<Self> {
        // Create cache directory if specified
        if let Some(cache_dir) = &config.cache_dir {
            std::fs::create_dir_all(cache_dir)?;
        }

        Ok(Self { config, backend })
    }

    /// Upload an SST's bytes with provided metadata.
    pub fn upload(&self, meta: SstUploadMeta, bytes: &[u8]) -> MidgeResult<SstMetadata> {
        let key = self.sst_key(&meta.sst_id);

        debug!(
            "Uploading SST {} ({} bytes) to cloud storage at key: {}",
            meta.sst_id,
            bytes.len(),
            key
        );

        // Upload to cloud storage (throttle via global limiter if configured)
        let limiter = crate::common::rate_limiter::global_rate_limiter();
        limiter.request(bytes.len() as u64);

        self.backend.put_blob(&key, Bytes::copy_from_slice(bytes))?;

        info!(
            "Successfully uploaded SST {} ({} bytes) to cloud",
            meta.sst_id,
            bytes.len()
        );

        Ok(SstMetadata {
            sst_id: meta.sst_id.clone(),
            id: None,
            len: bytes.len() as u64,
            size_bytes: meta.size_bytes,
            level: 0, // Will be updated by caller
            created_at: meta.uploaded_at,
            last_accessed: None,
            state: SstLifecycleState::Active,
        })
    }

    /// Placeholder to obtain a CloudSst by id. Returns an error in the stub.
    pub fn get(&self, _id: u64) -> MidgeResult<Box<dyn CloudSst>> {
        Err(MidgeError::internal(
            "CloudSstManager::get unimplemented in stub",
        ))
    }

    /// Async SST upload from filesystem path.
    ///
    /// Reads the SST file from disk and uploads it to cloud storage.
    /// This can be called from flush/compaction paths.
    pub fn upload_sst_async(
        &self,
        sst_id: String,
        path: PathBuf,
        sequence_range: (u64, u64),
        _key_range: (Option<Vec<u8>>, Option<Vec<u8>>),
        metadata: Option<SstMetadata>,
    ) -> MidgeResult<()> {
        debug!(
            "Starting async upload of SST {} from path {:?}",
            sst_id, path
        );

        // Read SST file from disk
        let bytes = std::fs::read(&path).map_err(|e| {
            MidgeError::internal(format!("Failed to read SST file {:?}: {}", path, e))
        })?;

        let checksum = crc32fast::hash(&bytes) as u64;

        // Create upload metadata
        let upload_meta = SstUploadMeta {
            sst_id: sst_id.clone(),
            location: self.sst_key(&sst_id),
            size_bytes: bytes.len() as u64,
            checksum,
            uploaded_at: timestamp::now(),
            sequence_range,
        };

        // Perform upload
        let result_meta = self.upload(upload_meta, &bytes)?;

        info!(
            "Completed async upload of SST {} ({} bytes)",
            sst_id, result_meta.size_bytes
        );

        // If caller provided metadata, they may want to update manifest
        // For now, just log it
        if let Some(meta) = metadata {
            debug!("Upload metadata: {:?}", meta);
        }

        Ok(())
    }

    /// Download an SST from cloud storage.
    pub fn download_sst(&self, sst_id: &str) -> MidgeResult<Vec<u8>> {
        let key = self.sst_key(sst_id);

        debug!("Downloading SST {} from cloud storage", sst_id);

        let bytes = self.backend.get_blob(&key)?;

        info!("Downloaded SST {} ({} bytes)", sst_id, bytes.len());

        Ok(bytes.to_vec())
    }

    /// Generate cloud storage key for an SST.
    fn sst_key(&self, sst_id: &str) -> String {
        match &self.config.prefix {
            Some(prefix) => format!("{}/sst/{}", prefix.trim_end_matches('/'), sst_id),
            None => format!("sst/{}", sst_id),
        }
    }
}

/// Reader factory stub used by higher-level components that expect a factory
/// for creating streaming readers for remote SSTs.
#[allow(dead_code)]
pub struct CloudSstReaderFactory {}

#[allow(dead_code)]
impl CloudSstReaderFactory {
    pub fn new() -> Self {
        Self {}
    }

    /// Create a reader for the given SST id. Stub: returns error.
    pub fn create_reader(&self, _id: u64) -> MidgeResult<()> {
        Err(MidgeError::internal(
            "CloudSstReaderFactory::create_reader unimplemented in stub",
        ))
    }
}

// Keep the API surface minimal but public so other modules can reference these
// types while the full cloud implementation is migrated in.
