//! Filesystem WAL writer implementation

use crate::common::MidgeResult;
use crate::wal::encoding;
use crate::wal::traits::WalWriter;
use crate::wal::types::{WalOpKind, WalPos, WalRecord};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

/// Filesystem-backed WAL writer
pub struct FsWalWriter {
    file_path: String,
    file: Mutex<std::fs::File>,
    current_pos: Mutex<WalPos>,
}

impl FsWalWriter {
    /// Create a new filesystem WAL writer
    pub fn new(dir: &Path) -> MidgeResult<Self> {
        // Create WAL directory if it doesn't exist
        std::fs::create_dir_all(dir).map_err(|e| crate::common::MidgeError::Io(e))?;

        let file_path = dir.join("wal.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .map_err(|e| crate::common::MidgeError::Io(e))?;

        // Get current position (file size)
        let current_pos = file
            .metadata()
            .map_err(|e| crate::common::MidgeError::Io(e))?
            .len();

        Ok(Self {
            file_path: file_path.to_string_lossy().to_string(),
            file: Mutex::new(file),
            current_pos: Mutex::new(current_pos),
        })
    }
}

impl WalWriter for FsWalWriter {
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // Encode record
        let encoded = encoding::encode(record)?;

        // Write with length prefix (4 bytes)
        let mut file = self.file.lock().unwrap();
        file.write_all(&(encoded.len() as u32).to_le_bytes())
            .map_err(|e| crate::common::MidgeError::Io(e))?;
        file.write_all(&encoded)
            .map_err(|e| crate::common::MidgeError::Io(e))?;

        // Update position
        let mut pos = self.current_pos.lock().unwrap();
        let prev_pos = *pos;
        *pos += 4 + encoded.len() as u64;

        Ok(prev_pos)
    }

    fn append_op(&self, kind: WalOpKind, key: &[u8], value: Option<&[u8]>) -> MidgeResult<WalPos> {
        let record = WalRecord::new(
            kind,
            bytes::Bytes::copy_from_slice(key),
            value.map(bytes::Bytes::copy_from_slice),
            0, // Sequence 0 for simple append_op
        );
        self.append_record(&record)
    }

    fn flush(&self) -> MidgeResult<()> {
        let mut file = self.file.lock().unwrap();
        file.flush().map_err(|e| crate::common::MidgeError::Io(e))
    }

    fn sync(&self) -> MidgeResult<()> {
        let mut file = self.file.lock().unwrap();
        file.sync_all()
            .map_err(|e| crate::common::MidgeError::Io(e))
    }

    fn sync_local(&self) -> MidgeResult<()> {
        self.sync()
    }

    fn current_pos(&self) -> WalPos {
        *self.current_pos.lock().unwrap()
    }

    fn close(&self) -> MidgeResult<()> {
        self.sync()
    }
}
