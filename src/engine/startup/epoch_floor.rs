//! Lease epoch floor recovered from durable writer state before acquisition.
//!
//! `format/lease.md` §4 step 4 requires a recovering engine to acquire with a
//! minimum epoch computed from its own durable state. Otherwise a lost or
//! restored leader record lets acquisition grant an epoch at or below one
//! already present in the WAL, and fencing silently stops distinguishing the
//! new writer from a stale one.
//!
//! Discovery only reads. It runs before the lease is held, so it never
//! repairs or rewrites anything; the authoritative, repairing reads happen
//! after acquisition exactly as before.

use super::super::OpenOptions;
use super::StartupStoragePath;
use crate::common::{MidgeError, MidgeResult};
use crate::config::{RecoveryPolicy, Storage};
use std::path::Path;
use std::sync::Arc;

pub(super) struct StartupEpochFloor;

impl StartupEpochFloor {
    /// Highest writer epoch recorded in the local WAL and, for cloud storage,
    /// the WAL publication catalog.
    ///
    /// Fails closed: an unreadable source is an error, never a zero floor.
    pub(super) fn discover(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
    ) -> MidgeResult<u64> {
        if storage_path.memory_mode {
            return Ok(0);
        }
        let local_wal = Self::local_wal_epoch(&storage_path.db_path, opts.recovery_policy())?;
        let catalog = match opts.storage() {
            Storage::Cloud { topology, .. } => {
                let wal = crate::storage::providers::build_cloud_storage_with_timeout(
                    topology.wal().provider(),
                    topology.wal().prefix(),
                    opts.storage_io_timeout(),
                )?;
                Self::catalog_epoch(&(wal as Arc<dyn crate::storage::StorageBackend>), opts)?
            }
            Storage::CloudSimulated { .. } => {
                let cloud_root =
                    crate::storage::test_support::simulated_cloud_root(&storage_path.db_path);
                if cloud_root.exists() {
                    let backend: Arc<dyn crate::storage::StorageBackend> =
                        Arc::new(crate::storage::filesystem::FileSystem::new(cloud_root)?);
                    Self::catalog_epoch(&backend, opts)?
                } else {
                    0
                }
            }
            Storage::InMemory | Storage::Local { .. } => 0,
        };
        Ok(local_wal.max(catalog))
    }

    /// WAL failures surface as `RecoveryFailed`, the same classification
    /// replay itself would report for them.
    fn local_wal_epoch(db_path: &Path, recovery_policy: RecoveryPolicy) -> MidgeResult<u64> {
        let wal_dir = db_path.join("wal");
        if !wal_dir.exists() {
            return Ok(0);
        }
        let replay_policy = match recovery_policy {
            RecoveryPolicy::Strict => crate::wal::recovery::ReplayPolicy::Strict,
            RecoveryPolicy::Salvage => crate::wal::recovery::ReplayPolicy::SalvageValidPrefix,
        };
        crate::io::RealFs::new(&wal_dir)
            .map_err(MidgeError::from)
            .and_then(|storage| {
                crate::wal::recovery::max_writer_epoch(
                    &storage,
                    &crate::io::FsPath::new(""),
                    replay_policy,
                )
            })
            .map_err(|error| {
                MidgeError::RecoveryFailed(format!(
                    "WAL recovery failed while establishing the lease epoch floor: {error}"
                ))
            })
    }

    /// Highest fencing epoch across the primary catalog and its mirror.
    ///
    /// Either copy may lag the other, so the larger valid epoch wins. A copy
    /// that fails to decode contributes nothing here: the authoritative
    /// catalog load right after acquisition rejects an unrepairable catalog
    /// before any write is admitted, with its established error.
    fn catalog_epoch(
        backend: &Arc<dyn crate::storage::StorageBackend>,
        opts: &OpenOptions,
    ) -> MidgeResult<u64> {
        let budget =
            crate::common::resource_budget::ResourceBudget::new(opts.compaction_memory_pool_size());
        let deadline = crate::common::OperationDeadline::unbounded();
        let mut floor = 0;
        for key in [
            crate::wal::cloud_catalog::OBJECT_KEY,
            crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
        ] {
            let Some(object) = crate::storage::HybridStorage::read_control_from_backend(
                backend,
                key,
                &budget,
                opts.storage_io_timeout(),
                &deadline,
            )?
            else {
                continue;
            };
            if let Ok(catalog) =
                crate::wal::cloud_catalog::WalPublicationCatalog::decode(object.bytes())
            {
                floor = floor.max(catalog.fencing_epoch);
            }
        }
        Ok(floor)
    }
}
