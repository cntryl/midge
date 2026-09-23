use std::path::PathBuf;
use std::sync::Arc;

#[cfg(test)]
use super::OpenOptions;
#[cfg(test)]
use crate::common::MidgeResult;
use crate::runtime::{Runtime, RuntimeState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CloudSstRecoveryProof {
    name: String,
    expected_size_bytes: Option<u64>,
    expected_crc32c: Option<u32>,
}

mod assembly;
pub(in crate::engine) mod cloud_io;
mod cloud_recovery;
mod epoch_floor;
mod storage;
mod streaming_recovery;
mod streaming_wal_fs;
mod streaming_wal_plan;
mod timing;

impl CloudSstRecoveryProof {
    #[cfg(test)]
    pub(super) fn name_only(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            expected_size_bytes: None,
            expected_crc32c: None,
        }
    }

    pub(super) fn from_manifest(file: &crate::metadata::FileMeta) -> Self {
        Self {
            name: file.name.clone(),
            expected_size_bytes: Some(file.size_bytes),
            expected_crc32c: file.content_crc32c,
        }
    }

    pub(super) fn from_runtime(file: &crate::runtime::FileMeta) -> Self {
        Self {
            name: file.name.clone(),
            expected_size_bytes: Some(file.size_bytes),
            expected_crc32c: file.content_crc32c,
        }
    }

    pub(super) fn merge_from(&mut self, other: &Self) {
        if self.expected_size_bytes.is_none() {
            self.expected_size_bytes = other.expected_size_bytes;
        }
        if self.expected_crc32c.is_none() {
            self.expected_crc32c = other.expected_crc32c;
        }
    }
}

pub(super) struct CloudStartupRecovery;

struct StartupStoragePath {
    db_path: PathBuf,
    memory_mode: bool,
}

struct StartupLease {
    lease: Arc<dyn crate::lease::PrimaryLease>,
    lease_guard: Option<crate::lease::LeaseGuard>,
    writer_epoch: u64,
    leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
    lease_healthy: Arc<std::sync::atomic::AtomicBool>,
    lease_validity: Option<Arc<crate::lease::LeaseValidity>>,
    lease_heartbeat: Option<crate::lease::LeaseHeartbeat>,
}

struct RuntimeStorageMaterialization {
    state: RuntimeState,
    runtime_config: crate::runtime::RuntimeConfig,
    cloud_root: Option<PathBuf>,
    cloud_storage_for_restore: Option<Arc<crate::storage::cloud::CloudStorage>>,
    cloud_metadata_storage_for_mirror: Option<Arc<crate::storage::cloud::CloudStorage>>,
    streaming_wal: Option<streaming_recovery::CloudReplay>,
}

pub(in crate::engine) struct CloudWalRecoveryPlan {
    pub(in crate::engine) remote_segments:
        std::collections::BTreeMap<u64, crate::runtime::RecoveredCloudWalSegment>,
    pub(in crate::engine) local_segments:
        std::collections::BTreeMap<u64, crate::runtime::RecoveredCloudWalSegment>,
    pub(in crate::engine) active_wal: Option<crate::runtime::RecoveredCloudActiveWal>,
    pub(in crate::engine) opened_in_salvage_mode: bool,
    /// Cataloged segments at and after the first hole that salvage stopped
    /// at. They are not replayed; startup retires them from the catalog and
    /// keeps their objects.
    pub(in crate::engine) unreplayed_segments: Vec<crate::wal::cloud_catalog::PublishedWalSegment>,
    /// Highest sequence held by WAL salvage set aside, now or on an earlier
    /// open (the catalog's persisted floor), so new writes never reuse one.
    /// Zero when nothing was ever set aside.
    pub(in crate::engine) max_unreplayed_sequence: u64,
    /// Local WAL files at or past the hole, including `wal.log`. Salvage
    /// renames them aside only after the floor covering them is durable.
    pub(in crate::engine) set_aside_local_paths: Vec<std::path::PathBuf>,
}

impl CloudWalRecoveryPlan {
    /// Makes a salvage set-aside durable, in the one order that survives a
    /// crash between any two steps:
    ///
    /// 1. Persist the sequence floor. Until the local files are renamed, the
    ///    next open finds the same hole and recomputes it anyway.
    /// 2. Rename the local files aside. After this they no longer parse as
    ///    segments, so only the persisted floor remembers their sequences.
    /// 3. Retire the cataloged segments. Retiring before step 2 would let the
    ///    next open replay local copies past the hole.
    pub(in crate::engine) fn commit_set_aside(
        &self,
        persistence: &crate::runtime::hybrid_persistence::CloudPersistence,
        writer_epoch: u64,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        db_path: &std::path::Path,
    ) -> crate::common::MidgeResult<()> {
        if self.max_unreplayed_sequence > catalog.sequence_floor {
            persistence.raise_wal_sequence_floor(writer_epoch, self.max_unreplayed_sequence)?;
        }
        self.set_aside_local_wal(db_path)?;
        if !self.unreplayed_segments.is_empty() {
            persistence.retire_unreplayed_wal_segments(writer_epoch, &self.unreplayed_segments)?;
        }
        Ok(())
    }

    /// Renames the local WAL files salvage stopped short of and syncs `wal/`.
    pub(in crate::engine) fn set_aside_local_wal(
        &self,
        db_path: &std::path::Path,
    ) -> crate::common::MidgeResult<()> {
        let mut renamed = false;
        for path in &self.set_aside_local_paths {
            if path.try_exists()? {
                CloudStartupRecovery::quarantine_local_wal_alias(path)?;
                renamed = true;
            }
        }
        if renamed {
            std::fs::File::open(db_path.join("wal"))?.sync_all()?;
        }
        Ok(())
    }

    fn remote_max_sequences(&self) -> std::collections::BTreeMap<u64, u64> {
        self.remote_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.max_sequence))
            .collect()
    }

    fn remote_writer_epochs(&self) -> std::collections::BTreeMap<u64, u64> {
        self.remote_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.writer_epoch))
            .collect()
    }

    fn local_max_sequences(&self) -> std::collections::BTreeMap<u64, u64> {
        self.local_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.max_sequence))
            .collect()
    }

    fn local_writer_epochs(&self) -> std::collections::BTreeMap<u64, u64> {
        self.local_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.writer_epoch))
            .collect()
    }
}

struct RuntimeRecoveryMaterialization {
    state: RuntimeState,
    runtime_config: crate::runtime::RuntimeConfig,
    recovered_sequence: u64,
    recovered_cf_metas: Vec<crate::metadata::ColumnFamilyMeta>,
}

struct StartedRuntime {
    runtime: Runtime,
    runtime_handle: crate::runtime::RuntimeHandle,
    recovered_sequence: u64,
    recovered_cf_metas: Vec<crate::metadata::ColumnFamilyMeta>,
}

struct FacadeAssembly;

pub(super) struct EngineStartup;

fn provider_kind(provider: &crate::config::CloudProviderConfig) -> &'static str {
    match provider {
        crate::config::CloudProviderConfig::AwsS3(_) => "aws-s3",
        crate::config::CloudProviderConfig::S3Compatible(_) => "s3-compatible",
        crate::config::CloudProviderConfig::AzureBlob(_) => "azure-blob",
        crate::config::CloudProviderConfig::Gcs(_) => "gcs",
        crate::config::CloudProviderConfig::OciObjectStorage(_) => "oci-object-storage",
    }
}

fn redact_endpoint_metadata(endpoint: &str) -> String {
    let endpoint = endpoint.split(['?', '#']).next().unwrap_or(endpoint);
    let Some((scheme, authority_and_path)) = endpoint.split_once("://") else {
        return endpoint.to_string();
    };
    let authority = authority_and_path
        .split('/')
        .next()
        .unwrap_or(authority_and_path);
    format!("{scheme}://{authority}")
}

#[cfg(test)]
mod tests;
