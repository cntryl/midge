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
            fs::rename(&old_path, &new_path).map_err(MidgeError::Io)?;
        }

        // Create a fresh active WAL segment.
        self.create_writer(dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn should_create_writer_in_directory() {
        let dir = TempDir::new().unwrap();
        let factory = FsWalFactory;

        let writer = factory.create_writer(dir.path());

        assert!(writer.is_ok());
        assert!(dir.path().join("wal.log").exists());
    }

    #[test]
    fn should_create_reader_for_existing_wal() {
        let dir = TempDir::new().unwrap();
        let factory = FsWalFactory;

        // First create a writer
        let _writer = factory.create_writer(dir.path()).unwrap();

        // Then create a reader
        let reader = factory.create_reader(dir.path());

        assert!(reader.is_ok());
    }

    #[test]
    fn should_rotate_writer_and_rename_file() {
        let dir = TempDir::new().unwrap();
        let factory = FsWalFactory;

        // Create initial writer and write some data
        let writer = factory.create_writer(dir.path()).unwrap();
        drop(writer);

        assert!(dir.path().join("wal.log").exists());

        // Rotate - this renames wal.log to 1.wal and creates a new wal.log
        let _new_writer = factory.rotate_writer(dir.path(), 1).unwrap();

        // After rotation, both files should exist (old segment + new wal.log)
        assert!(dir.path().join("wal.log").exists());
        assert!(dir.path().join("1.wal").exists());
    }

    #[test]
    fn should_handle_multiple_rotations() {
        let dir = TempDir::new().unwrap();
        let factory = FsWalFactory;

        for segment_id in 1..=5 {
            let writer = factory.create_writer(dir.path()).unwrap();
            drop(writer);
            let _new_writer = factory.rotate_writer(dir.path(), segment_id).unwrap();

            let segment_file = dir.path().join(format!("{}.wal", segment_id));
            assert!(segment_file.exists());
        }
    }

    #[test]
    fn should_handle_rotation_of_nonexistent_wal() {
        let dir = TempDir::new().unwrap();
        let factory = FsWalFactory;

        // Rotate even though wal.log doesn't exist yet
        let result = factory.rotate_writer(dir.path(), 1);

        assert!(result.is_ok());
        // Should have created a new wal.log
        assert!(dir.path().join("wal.log").exists());
    }
}
