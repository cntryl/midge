//! Filesystem WAL reader using io::Fs abstraction
//!
//! This reader uses the base io::Fs trait instead of storage abstractions directly,
//! allowing for swappable implementations (Real, Mock, Chaos) for testing.
//!
//! Architectural invariants (Copilot: DO NOT VIOLATE):
//! --------------------------------------------------
//! • FsWalReaderIo reads **only** from the active WAL file `wal.log`.
//! • It must treat EOF mid-record as **corruption**, not success.
//! • It must not assume the file ends cleanly.
//! • It must use the canonical format:
//!       <u32 length prefix><encoded record bytes>
//! • It must NOT attempt to fix, truncate, or adjust the file.
//! • It must update `current_pos` monotonically.

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::wal::encoding;
use crate::wal::traits::{WalReader, WalReaderDyn};
use crate::wal::types::{WalPos, WalRecord};
use std::sync::Arc;

/// Filesystem-backed WAL reader using io::Fs.
///
/// This struct provides low-level, corruption-aware reading semantics
/// with swappable filesystem backends.
pub struct FsWalReaderIo {
    path: FsPath,
    fs: Arc<dyn Fs>,
    current_pos: WalPos,
}

impl FsWalReaderIo {
    /// Open `wal.log` in read-only mode using the provided filesystem.
    pub fn new(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        Ok(Self {
            path: FsPath::new(path_str),
            fs,
            current_pos: 0,
        })
    }

    /// Open and verify the WAL file exists.
    pub fn open(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        let reader = Self::new(path_str, fs.clone())?;
        // Verify file exists by attempting to get metadata
        fs.metadata(&reader.path)?;
        Ok(reader)
    }
}

impl WalReader for FsWalReaderIo {
    /// Read a single WAL record at an explicit offset.
    ///
    /// Returns:
    /// - Ok(Some(record)) if a valid record is found
    /// - Ok(None) if clean EOF at `pos`
    /// - Err(Corruption) if EOF occurs mid-record
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        // Open file in read-only mode
        let file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;

        // Read 4-byte length prefix
        let len_bytes = file.read_at(pos, 4)?;
        if len_bytes.is_empty() {
            return Ok(None);
        }
        if len_bytes.len() < 4 {
            return Err(MidgeError::Corruption(format!(
                "Incomplete WAL length prefix at pos {} (got {} bytes)",
                pos,
                len_bytes.len()
            )));
        }

        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&len_bytes[..4]);
        let len = u32::from_le_bytes(len_buf) as u64;

        // Read the encoded record bytes
        let buf = file.read_at(pos + 4, len)?;
        if buf.len() < len as usize {
            return Err(MidgeError::Corruption(format!(
                "Incomplete WAL record at pos {} (len={}, got={})",
                pos,
                len,
                buf.len()
            )));
        }

        // Decode the record
        let record = encoding::decode(&buf[..len as usize])?;
        self.current_pos = pos + 4 + len;

        Ok(Some(record))
    }

    fn replay<F>(&mut self, start: WalPos, mut cb: F) -> MidgeResult<()>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>,
    {
        let mut pos = start;
        while let Some(record) = <Self as WalReader>::read_at(self, pos)? {
            cb(&record)?;
            pos = self.current_pos;
        }
        Ok(())
    }

    fn close(&mut self) -> MidgeResult<()> {
        // io::Fs doesn't require explicit close, but reset position
        self.current_pos = 0;
        Ok(())
    }
}

impl WalReaderDyn for FsWalReaderIo {
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        WalReader::read_at(self, pos)
    }

    fn replay_boxed(
        &mut self,
        start: WalPos,
        cb: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    ) -> MidgeResult<()> {
        let mut pos = start;
        while let Some(record) = WalReader::read_at(self, pos)? {
            cb(&record)?;
            pos = self.current_pos;
        }
        Ok(())
    }

    fn close(&mut self) -> MidgeResult<()> {
        WalReader::close(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_wal_reader_io() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let reader = FsWalReaderIo::new("wal.log", fs);

        // Assert
        assert!(reader.is_ok());
    }

    #[test]
    fn should_support_close() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let mut reader = FsWalReaderIo::new("wal.log", fs).unwrap();

        // Act
        let result = WalReader::close(&mut reader);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_return_none_on_eof() -> MidgeResult<()> {
        // Arrange: Create reader on newly created empty file
        let fs = Arc::new(crate::io::MockFs::new());
        let mut reader = FsWalReaderIo::new("wal.log", fs)?;

        // Act: Try to read at position 0 from empty file
        let result = WalReader::read_at(&mut reader, 0);

        // Assert: Should return None for clean EOF at position 0
        match result {
            Ok(None) => Ok(()),
            Ok(Some(_)) => panic!("Expected None for empty file"),
            Err(_) => panic!("Expected None for empty file, not error"),
        }
    }
}
