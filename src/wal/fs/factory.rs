//! Filesystem WAL factory
//!
//! Architectural rules (Copilot: read and obey):
//! --------------------------------------------------
//! - Rotated WAL segment files MUST be named `{segment_id}.wal`.
//! - Active writer always uses the filename `wal.log`.
//! - No sequencing logic belongs in this module.
//! - Rotation is triggered/formalized by the WAL actor.
//! - The writer MUST have been fsync'd by the actor before rotate().
//! - Do NOT add async or background threads in this module.

use crate::common::{MidgeError, MidgeResult};
use crate::wal::fs::{FsWalReader, FsWalWriter};
use crate::wal::traits::{WalFactory, WalReaderDyn, WalWriter};
use std::fs;
use std::path::Path;

/// Factory for creating filesystem-backed WAL readers and writers.
pub struct FsWalFactory;

impl WalFactory for FsWalFactory {
    fn create_writer(&self, dir: &Path) -> MidgeResult<Box<dyn WalWriter>> {
        let writer = FsWalWriter::new(dir)?;
        Ok(Box::new(writer))
    }

    fn create_reader(&self, dir: &Path) -> MidgeResult<Box<dyn WalReaderDyn>> {
        let reader = FsWalReader::new(dir)?;
        Ok(Box::new(reader))
    }

    /// Rotate the active WAL segment.
    ///
    /// Filesystem convention:
    ///     wal.log → {segment_id}.wal
    ///
    /// NOTE:
    /// - segment_id is assigned & incremented by the WAL actor.
    /// - This method MUST NOT touch or modify segment_id values.
    fn rotate_writer(&self, dir: &Path, segment_id: u64) -> MidgeResult<Box<dyn WalWriter>> {
        let old_path = dir.join("wal.log");
        let new_path = dir.join(format!("{segment_id}.wal"));

        if old_path.exists() {
            fs::rename(&old_path, &new_path)
                .map_err(MidgeError::Io)?;
        }

        // Create a fresh active WAL segment.
        self.create_writer(dir)
    }
}
