//! Filesystem WAL reader implementation
//!
//! Architectural invariants (Copilot: DO NOT VIOLATE):
//! --------------------------------------------------
//! • FsWalReader reads **only** from the active WAL file `wal.log`.
//! • It must treat EOF mid-record as **corruption**, not success.
//! • It must not assume the file ends cleanly — RocksDB, Pebble,
//!   TiKV, and LMDB all rely on readers being strict and defensive.
//! • It must use the canonical format:
//!       <u32 length prefix><encoded record bytes>
//! • It must NOT attempt to fix, truncate, or adjust the file.
//! • It must update `current_pos` monotonically.
//!
//! This reader is intentionally synchronous and blocking —
//! higher-level async abstractions belong in the runtime.

use crate::common::{MidgeError, MidgeResult};
use crate::wal::encoding;
use crate::wal::traits::{WalReader, WalReaderDyn};
use crate::wal::types::{WalPos, WalRecord};

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Filesystem-backed WAL reader.
///
/// This struct provides low-level, corruption-aware reading semantics.
pub struct FsWalReader {
    file: File,
    current_pos: WalPos,
}

impl FsWalReader {
    /// Open `wal.log` in read-only mode.
    pub fn new(dir: &Path) -> MidgeResult<Self> {
        let path = dir.join("wal.log");
        let file = File::open(&path).map_err(MidgeError::Io)?;

        Ok(Self {
            file,
            current_pos: 0,
        })
    }
}

impl WalReader for FsWalReader {
    /// Read a single WAL record at an explicit offset.
    ///
    /// Returns:
    /// - Ok(Some(record)) if a valid record is found
    /// - Ok(None) if clean EOF at `pos`
    /// - Err(Corruption) if EOF occurs mid-record
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        self.file.seek(SeekFrom::Start(pos)).map_err(MidgeError::Io)?;

        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        match self.file.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Clean EOF — no more records
                return Ok(None);
            }
            Err(e) => return Err(MidgeError::Io(e)),
        }

        let len = u32::from_le_bytes(len_buf) as usize;

        // Read the encoded record bytes
        let mut buf = vec![0u8; len];
        match self.file.read_exact(&mut buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // EOF *inside* record → corruption
                return Err(MidgeError::Corruption(
                    format!("Incomplete WAL record at pos {} (len={})", pos, len),
                ));
            }
            Err(e) => return Err(MidgeError::Io(e)),
        }

        let record = encoding::decode(&buf[..])?;
        self.current_pos = pos + 4 + len as u64;

        Ok(Some(record))
    }

    /// Replay WAL from `start` forward, invoking callback for each record.
    ///
    /// Stops at:
    /// - clean EOF → success
    /// - corruption → error
    /// - callback error → error
    fn replay<F>(&mut self, start: WalPos, mut cb: F) -> MidgeResult<()>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>,
    {
        self.file.seek(SeekFrom::Start(start)).map_err(MidgeError::Io)?;

        let mut pos = start;

        loop {
            // Read the length prefix
            let mut len_buf = [0u8; 4];
            match self.file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // Clean EOF
                Err(e) => return Err(MidgeError::Io(e)),
            }

            let len = u32::from_le_bytes(len_buf) as usize;

            // Read record payload
            let mut buf = vec![0u8; len];
            match self.file.read_exact(&mut buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(MidgeError::Corruption(format!(
                        "Incomplete WAL record at pos {} (len={})",
                        pos, len
                    )));
                }
                Err(e) => return Err(MidgeError::Io(e)),
            }

            // Decode
            let record = encoding::decode(&buf[..])?;

            // Dispatch to callback
            cb(&record)?;

            pos += 4 + len as u64;
        }

        self.current_pos = pos;
        Ok(())
    }

    fn close(&mut self) -> MidgeResult<()> {
        // File closes automatically — nothing to do.
        Ok(())
    }
}

/// Object-safe wrapper for trait objects.
impl WalReaderDyn for FsWalReader {
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        WalReader::read_at(self, pos)
    }

    fn replay_boxed(
        &mut self,
        start: WalPos,
        cb: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    ) -> MidgeResult<()> {
        self.replay(start, cb)
    }

    fn close(&mut self) -> MidgeResult<()> {
        WalReader::close(self)
    }
}
