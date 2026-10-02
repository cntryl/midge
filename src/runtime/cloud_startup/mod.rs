//! Cloud startup recovery authority: which cloud WAL segments and local WAL
//! files replay, which manifest SSTs survive, what salvage cuts or sets aside,
//! and which remote objects startup may delete.
//!
//! `engine::startup` wires these decisions into engine startup; the coverage
//! rule they rely on is the shared one in `runtime::hybrid_persistence`.

use crate::io::{Fs, FsError};

pub(crate) mod cloud_io;
mod cloud_recovery;
pub(crate) mod replay_coverage;
pub(crate) mod streaming_wal_fs;
pub(crate) mod streaming_wal_plan;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CloudSstRecoveryProof {
    pub(crate) name: String,
    pub(crate) expected_size_bytes: Option<u64>,
    pub(crate) expected_crc32c: Option<u32>,
}

impl CloudSstRecoveryProof {
    #[cfg(test)]
    pub(crate) fn name_only(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            expected_size_bytes: None,
            expected_crc32c: None,
        }
    }

    pub(crate) fn from_manifest(file: &crate::metadata::FileMeta) -> Self {
        Self {
            name: file.name.clone(),
            expected_size_bytes: Some(file.size_bytes),
            expected_crc32c: file.content_crc32c,
        }
    }

    pub(crate) fn from_runtime(file: &crate::runtime::FileMeta) -> Self {
        Self {
            name: file.name.clone(),
            expected_size_bytes: Some(file.size_bytes),
            expected_crc32c: file.content_crc32c,
        }
    }

    pub(crate) fn merge_from(&mut self, other: &Self) {
        if self.expected_size_bytes.is_none() {
            self.expected_size_bytes = other.expected_size_bytes;
        }
        if self.expected_crc32c.is_none() {
            self.expected_crc32c = other.expected_crc32c;
        }
    }
}

pub(crate) struct CloudStartupRecovery;

pub(crate) struct CloudWalRecoveryPlan {
    pub(crate) remote_segments:
        std::collections::BTreeMap<u64, crate::runtime::RecoveredCloudWalSegment>,
    pub(crate) local_segments:
        std::collections::BTreeMap<u64, crate::runtime::RecoveredCloudWalSegment>,
    pub(crate) active_wal: Option<crate::runtime::RecoveredCloudActiveWal>,
    pub(crate) opened_in_salvage_mode: bool,
    /// Cataloged segments at and after the first hole that salvage stopped
    /// at. They are not replayed; startup retires them from the catalog and
    /// keeps their objects.
    pub(crate) unreplayed_segments: Vec<crate::wal::cloud_catalog::PublishedWalSegment>,
    /// Highest sequence held by WAL salvage set aside, now or on an earlier
    /// open (the catalog's persisted floor), so new writes never reuse one.
    /// Zero when nothing was ever set aside.
    pub(crate) max_unreplayed_sequence: u64,
    /// Local WAL files at or past the hole, including `wal.log`. Salvage
    /// renames them aside only after the floor covering them is durable.
    pub(crate) set_aside_local_paths: Vec<std::path::PathBuf>,
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
    #[cfg(test)]
    pub(crate) fn commit_set_aside(
        &self,
        persistence: &crate::runtime::hybrid_persistence::CloudPersistence,
        writer_epoch: u64,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        db_path: &std::path::Path,
    ) -> crate::common::MidgeResult<()> {
        self.commit_set_aside_with_authority(
            persistence,
            writer_epoch,
            catalog,
            db_path,
            &|| Ok(()),
        )
    }

    pub(crate) fn commit_set_aside_with_authority(
        &self,
        persistence: &crate::runtime::hybrid_persistence::CloudPersistence,
        writer_epoch: u64,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        db_path: &std::path::Path,
        validate: &dyn Fn() -> crate::common::MidgeResult<()>,
    ) -> crate::common::MidgeResult<()> {
        validate()?;
        if self.max_unreplayed_sequence > catalog.sequence_floor {
            persistence.raise_wal_sequence_floor(writer_epoch, self.max_unreplayed_sequence)?;
        }
        crate::failpoints::fail_point!("midge::recovery::after_salvage_floor_before_quarantine");
        validate()?;
        self.set_aside_local_wal_with_authority(db_path, validate)?;
        crate::failpoints::fail_point!("midge::recovery::before_salvage_catalog_retirement");
        validate()?;
        if !self.unreplayed_segments.is_empty() {
            persistence.retire_unreplayed_wal_segments(writer_epoch, &self.unreplayed_segments)?;
        }
        validate()
    }

    /// Renames the local WAL files salvage stopped short of and syncs `wal/`.
    #[cfg(test)]
    pub(crate) fn set_aside_local_wal(
        &self,
        db_path: &std::path::Path,
    ) -> crate::common::MidgeResult<()> {
        self.set_aside_local_wal_with_authority(db_path, &|| Ok(()))
    }

    pub(crate) fn set_aside_local_wal_with_authority(
        &self,
        db_path: &std::path::Path,
        validate: &dyn Fn() -> crate::common::MidgeResult<()>,
    ) -> crate::common::MidgeResult<()> {
        let fs = crate::io::RealFs::open_existing(db_path).map_err(FsError::into_midge)?;
        let mut renamed = false;
        for path in &self.set_aside_local_paths {
            let path = streaming_wal_plan::local_path(path)?;
            if fs.exists(&path).map_err(FsError::into_midge)? {
                CloudStartupRecovery::quarantine_local_wal_alias_with_authority(
                    &fs, &path, validate,
                )?;
                renamed = true;
                crate::failpoints::fail_point!("midge::recovery::after_salvage_quarantine_rename");
            }
        }
        if renamed {
            validate()?;
            fs.sync_dir(
                &crate::io::FsPath::new("wal"),
                crate::io::Durability::Durable,
            )
            .map_err(FsError::into_midge)?;
        }
        Ok(())
    }

    pub(crate) fn remote_max_sequences(&self) -> std::collections::BTreeMap<u64, u64> {
        self.remote_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.max_sequence))
            .collect()
    }

    pub(crate) fn remote_writer_epochs(&self) -> std::collections::BTreeMap<u64, u64> {
        self.remote_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.writer_epoch))
            .collect()
    }

    pub(crate) fn local_max_sequences(&self) -> std::collections::BTreeMap<u64, u64> {
        self.local_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.max_sequence))
            .collect()
    }

    pub(crate) fn local_writer_epochs(&self) -> std::collections::BTreeMap<u64, u64> {
        self.local_segments
            .iter()
            .map(|(segment_id, segment)| (*segment_id, segment.writer_epoch))
            .collect()
    }
}
