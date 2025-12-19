//! Filesystem WAL writer using io::Fs abstraction
//!
//! This writer uses the base io::Fs trait instead of storage abstractions directly,
//! allowing for swappable implementations (Real, Mock, Chaos) for testing.
//!
//! Architectural rules (Copilot: read carefully and DO NOT modify):
//! ---------------------------------------------------------------
//! • FsWalWriterIo ONLY appends bytes to the active WAL file `wal.log`.
//! • It NEVER assigns sequence numbers.
//! • It NEVER rotates WAL segments.
//! • It NEVER writes metadata beyond the encoded WAL record format.
//! • It MUST write records as: <u32 length prefix><encoded record bytes>.
//! • It MUST update the write position monotonically.
//! • It MUST flush/sync exactly and only when asked.
//! • All concurrency protection is via `Mutex` — do NOT add async constructs.

use crate::common::MidgeResult;
use crate::io::{Durability, Fs, FsPath};
use crate::wal::encoding;
use crate::wal::traits::WalWriter;
use crate::wal::types::{WalOpKind, WalPos, WalRecord};
use bytes::Bytes;
use std::sync::{Arc, Mutex};

/// Filesystem-backed WAL writer using io::Fs.
///
/// This struct is responsible ONLY for writing bytes to `wal.log`.
/// It does not manage segment rotation, sequence assignment, recovery,
/// or any other higher-level concerns. Those belong to the WAL actor.
pub struct FsWalWriterIo {
    path: FsPath,
    fs: Arc<dyn Fs>,
    current_pos: Mutex<WalPos>,
}

impl FsWalWriterIo {
    /// Create a new WAL writer targeting `wal.log` using the provided filesystem.
    pub fn new(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        let path = FsPath::new(path_str);

        // Verify file exists or can be created by checking metadata
        {
            let _ = fs.open(
                &path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: false,
                },
            )?;
        }

        // Get current file size
        let metadata = fs.metadata(&path)?;
        let current_pos = metadata.len;

        Ok(Self { path, fs, current_pos: Mutex::new(current_pos) })
    }


}

impl WalWriter for FsWalWriterIo {
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // Encode using canonical WAL binary encoding
        let encoded = encoding::encode(record)?;

        // Compute prefix
        let len_prefix = (encoded.len() as u32).to_le_bytes();

        // Get current position atomically
        let mut pos_guard = self.current_pos.lock().expect("current_pos mutex poisoned");
        let start_pos = *pos_guard;

        // Coalesce prefix + encoded into one buffer and append once to reduce syscall overhead
        let mut buf = Vec::with_capacity(4 + encoded.len());
        buf.extend_from_slice(&len_prefix);
        buf.extend_from_slice(&encoded);

        let mut file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        file.append(Bytes::from(buf))?;

        let expected = 4u64 + encoded.len() as u64;
        *pos_guard += expected;

        Ok(start_pos)
    }

    fn append_op(
        &self,
        _kind: WalOpKind,
        _key: &[u8],
        _value: Option<&[u8]>,
    ) -> MidgeResult<WalPos> {
        // Default implementation: error, as we need a sequence number
        Err(crate::common::MidgeError::NotSupported(
            "append_op without sequence number not supported".into(),
        ))
    }

    fn flush(&self) -> MidgeResult<()> {
        // Synchronous writes already flushed
        Ok(())
    }

    fn sync(&self) -> MidgeResult<()> {
        let mut file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        file.sync(Durability::Durable)?;
        Ok(())
    }

    fn current_pos(&self) -> WalPos {
        *self.current_pos.lock().expect("current_pos mutex poisoned")
    }

    fn close(&self) -> MidgeResult<()> {
        // io::Fs doesn't require explicit close
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_wal_writer_io() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let writer = FsWalWriterIo::new("wal.log", fs)?;

        // Assert
        assert_eq!(writer.current_pos(), 0);
        Ok(())
    }

    #[test]
    fn should_support_flush() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let writer = FsWalWriterIo::new("wal.log", fs)?;

        // Act
        let result = writer.flush();

        // Assert
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn should_support_close() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let writer = FsWalWriterIo::new("wal.log", fs)?;

        // Act
        let result = writer.close();

        // Assert
        assert!(result.is_ok());
        Ok(())
    }
}
