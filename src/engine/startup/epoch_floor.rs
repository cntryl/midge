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
use crate::common::{DeadlineScope, MidgeError, MidgeResult};
use crate::config::Storage;
use crate::io::FsError;
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
        Self::discover_scoped(opts, storage_path, None)
    }

    pub(super) fn discover_within(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        scope: &DeadlineScope,
    ) -> MidgeResult<u64> {
        scope.check("lease epoch discovery")?;
        Self::discover_scoped(opts, storage_path, Some(scope))
    }

    fn discover_scoped(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<u64> {
        if storage_path.memory_mode {
            return Ok(0);
        }
        let local_wal = Self::local_wal_epoch(&storage_path.root, scope)?;
        let catalog = match opts.storage() {
            Storage::Cloud { topology, .. } => {
                let wal = crate::storage::providers::build_class_store(
                    topology.wal().provider(),
                    topology.wal().prefix(),
                    opts.storage_io_timeout(),
                    opts.injected_object_store(),
                )?;
                let wal = scope.map_or_else(
                    || wal.clone(),
                    |scope| Arc::new(wal.with_startup_scope(scope.clone())),
                );
                Self::catalog_epoch(
                    &(wal as Arc<dyn crate::storage::StorageBackend>),
                    opts,
                    scope,
                )?
            }
            Storage::CloudSimulated { .. } => {
                let existing = crate::storage::simulated::SimulatedCloud::locate(
                    storage_path.root.as_ref(),
                    opts.injected_object_store(),
                    opts.storage_io_timeout(),
                )?
                .open_existing()?;
                match existing {
                    Some(store) => Self::catalog_epoch(&(store as _), opts, scope)?,
                    None => 0,
                }
            }
            Storage::InMemory | Storage::Local { .. } => 0,
        };
        Ok(local_wal.max(catalog))
    }

    /// The newest writer epoch the WAL ledger records (#843): every segment
    /// header names the epoch that wrote it, and epochs never fall along the
    /// chain, so no segment is read. Failures surface as `RecoveryFailed`,
    /// the classification recovery reports for them.
    fn local_wal_epoch(
        root: &Arc<dyn crate::io::Fs>,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<u64> {
        if !root
            .exists(&crate::io::FsPath::new("wal"))
            .map_err(FsError::into_midge)?
        {
            return Ok(0);
        }
        let mut storage: Arc<dyn crate::io::ReadFs> =
            crate::io::subdirectory(Arc::clone(root), "wal");
        if let Some(scope) = scope {
            storage = crate::io::scope_read_fs(storage, scope.clone());
        }
        // Read only: the lease is not held yet, so nothing may change.
        let result = crate::runtime::wal_history::WalHistory::read(&storage)
            .map(|ledger| ledger.map_or(0, |ledger| ledger.newest_epoch()));
        if let Some(scope) = scope {
            scope.check("local WAL epoch discovery")?;
        }
        result.map_err(|error| {
            if matches!(error, MidgeError::Timeout(_)) {
                return error;
            }
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
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<u64> {
        let budget =
            crate::common::resource_budget::ResourceBudget::new(opts.compaction_memory_pool_size());
        let deadline = scope.map_or_else(
            crate::common::OperationDeadline::unbounded,
            DeadlineScope::deadline,
        );
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
                floor = floor.max(catalog.fencing_epoch());
            }
        }
        Ok(floor)
    }
}

#[cfg(test)]
mod tests {
    use super::StartupEpochFloor;
    use crate::common::MidgeResult;
    use crate::simulation::crash_fs::{CrashDevice, Fault, FaultEffect, FaultError, FaultOp};
    use std::sync::Arc;

    #[test]
    fn should_discover_epoch_floor_from_ledger_without_reading_segments() -> MidgeResult<()> {
        // Arrange: epoch 7 opened the newest segment the ledger names, and
        // every open of that segment's file fails.
        let device = CrashDevice::new();
        let root: Arc<dyn crate::io::Fs> = Arc::new(device.fs());
        let wal = crate::io::subdirectory(Arc::clone(&root), crate::wal::WAL_DIR);
        drop(crate::runtime::wal_history::WalHistory::open(wal, 1, 7)?);
        let segment_open = device.inject(Fault {
            op: FaultOp::Open,
            path: format!("wal/{}", crate::wal::segment_file_name(1)),
            nth: 1,
            effect: FaultEffect::Before,
            error: FaultError::Io,
        });

        // Act
        let floor = StartupEpochFloor::local_wal_epoch(&root, None)?;

        // Assert
        assert_eq!(floor, 7);
        assert!(!device.fired(segment_open), "discovery read a segment");
        Ok(())
    }
}
