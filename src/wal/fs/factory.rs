use std::path::Path;

use crate::error::{MidgeError, MidgeResult};

use super::writer::Wal;

/// Factory for creating filesystem-backed WAL writers and readers
pub struct FsWalFactory;

impl FsWalFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FsWalFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::wal::WalFactory for FsWalFactory {
    fn create_writer(&self, dir: &Path) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        Ok(Box::new(Wal::open(dir)?))
    }

    fn create_reader(&self, _dir: &Path) -> MidgeResult<Box<dyn crate::wal::WalReaderDyn>> {
        // For now, return an error as WalReader will be implemented in the file split
        Err(MidgeError::Internal {
            message: "WalReader not yet implemented - use replay_wal_file() directly".into(),
        })
    }

    fn rotate_writer(&self, dir: &Path, _seq: u64) -> MidgeResult<Box<dyn crate::wal::WalWriter>> {
        // For filesystem WAL, rotation is handled internally by Wal
        // Just create a new writer which will use the latest file
        Ok(Box::new(Wal::open(dir)?))
    }
}
