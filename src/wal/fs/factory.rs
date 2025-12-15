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
use crate::storage::abstraction::{
    Durability, RenameOptions, Storage, StorageErrorKind, StoragePath,
};
use crate::wal::fs::{FsWalReader, FsWalWriter};
use crate::wal::traits::{WalFactory, WalReaderDyn, WalWriter};

fn map_storage_error(err: crate::storage::abstraction::StorageError) -> MidgeError {
    match err.kind {
        StorageErrorKind::NotFound => MidgeError::NotFound,
        StorageErrorKind::Unsupported => MidgeError::NotSupported(err.message),
        StorageErrorKind::Corruption => MidgeError::Corruption(err.message),
        StorageErrorKind::InvalidInput => MidgeError::InvalidArgument(err.message),
        _ => MidgeError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            err.to_string(),
        )),
    }
}

/// Factory for creating filesystem-backed WAL readers and writers.
pub struct FsWalFactory;

impl WalFactory for FsWalFactory {
    fn create_writer(
        &self,
        storage: &dyn Storage,
        dir: &StoragePath,
    ) -> MidgeResult<Box<dyn WalWriter>> {
        let writer = FsWalWriter::new(storage, dir)?;
        Ok(Box::new(writer))
    }

    fn create_reader(
        &self,
        storage: &dyn Storage,
        dir: &StoragePath,
    ) -> MidgeResult<Box<dyn WalReaderDyn>> {
        let reader = FsWalReader::new(storage, dir)?;
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
    fn rotate_writer(
        &self,
        storage: &dyn Storage,
        dir: &StoragePath,
        segment_id: u64,
    ) -> MidgeResult<Box<dyn WalWriter>> {
        let old_path = super::join(dir, "wal.log");
        let new_path = super::join(dir, &format!("{segment_id}.wal"));

        let rename_opts = RenameOptions {
            require_atomic: false,
            overwrite: false,
            durability: Durability::Unsafe,
        };

        match storage.rename(&old_path, &new_path, rename_opts) {
            Ok(_) => {}
            Err(e) if e.kind == StorageErrorKind::NotFound => {}
            Err(e) => return Err(map_storage_error(e)),
        }

        // Create a fresh active WAL segment.
        self.create_writer(storage, dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::abstraction::StoragePath;
    use crate::storage::LocalFsStorage;
    use tempfile::TempDir;

    #[test]
    fn should_create_writer_in_directory() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("");
        let factory = FsWalFactory;

        let writer = factory.create_writer(&storage, &wal_dir);

        assert!(writer.is_ok());
        assert!(dir.path().join("wal.log").exists());
    }

    #[test]
    fn should_create_reader_for_existing_wal() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("");
        let factory = FsWalFactory;

        // First create a writer
        let _writer = factory.create_writer(&storage, &wal_dir).unwrap();

        // Then create a reader
        let reader = factory.create_reader(&storage, &wal_dir);

        assert!(reader.is_ok());
    }

    #[test]
    fn should_rotate_writer_and_rename_file() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("");
        let factory = FsWalFactory;

        // Create initial writer and write some data
        let writer = factory.create_writer(&storage, &wal_dir).unwrap();
        drop(writer);

        assert!(dir.path().join("wal.log").exists());

        // Rotate - this renames wal.log to 1.wal and creates a new wal.log
        let _new_writer = factory.rotate_writer(&storage, &wal_dir, 1).unwrap();

        // After rotation, both files should exist (old segment + new wal.log)
        assert!(dir.path().join("wal.log").exists());
        assert!(dir.path().join("1.wal").exists());
    }

    #[test]
    fn should_handle_multiple_rotations() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("");
        let factory = FsWalFactory;

        for segment_id in 1..=5 {
            let writer = factory.create_writer(&storage, &wal_dir).unwrap();
            drop(writer);
            let _new_writer = factory
                .rotate_writer(&storage, &wal_dir, segment_id)
                .unwrap();

            let segment_file = dir.path().join(format!("{}.wal", segment_id));
            assert!(segment_file.exists());
        }
    }

    #[test]
    fn should_handle_rotation_of_nonexistent_wal() {
        let dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new(dir.path()).unwrap();
        let wal_dir = StoragePath::new("");
        let factory = FsWalFactory;

        // Rotate even though wal.log doesn't exist yet
        let result = factory.rotate_writer(&storage, &wal_dir, 1);

        assert!(result.is_ok());
        // Should have created a new wal.log
        assert!(dir.path().join("wal.log").exists());
    }
}
