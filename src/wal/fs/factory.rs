//! Filesystem WAL factory

use crate::common::MidgeResult;
use crate::wal::traits::{WalFactory, WalWriter, WalReaderDyn};
use crate::wal::fs::{FsWalWriter, FsWalReader};
use std::path::Path;

/// Factory for creating filesystem WAL writers and readers
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
    
    fn rotate_writer(&self, dir: &Path, seq: u64) -> MidgeResult<Box<dyn WalWriter>> {
        use std::fs;
        
        // Rename current wal.log to wal-<seq>.log
        let old_path = dir.join("wal.log");
        let new_path = dir.join(format!("wal-{}.log", seq));
        
        if old_path.exists() {
            fs::rename(&old_path, &new_path)
                .map_err(|e| crate::common::MidgeError::Io(e))?;
        }
        
        // Create new writer
        self.create_writer(dir)
    }
}
