//! Filesystem WAL writer implementation
//!
//! Architectural rules (Copilot: read carefully and DO NOT modify):
//! ---------------------------------------------------------------
//! • FsWalWriter ONLY appends bytes to the active WAL file `wal.log`.
//! • It NEVER assigns sequence numbers.
//! • It NEVER rotates WAL segments.
//! • It NEVER writes metadata beyond the encoded WAL record format.
//! • It MUST write records as: <u32 length prefix><encoded record bytes>.
//! • It MUST update the write position monotonically.
//! • It MUST flush/sync exactly and only when asked.
//! • All concurrency protection is via `Mutex` — do NOT add async constructs.

use crate::common::{MidgeError, MidgeResult};
use crate::wal::encoding;
use crate::wal::traits::WalWriter;
use crate::wal::types::{WalOpKind, WalPos, WalRecord};

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

/// Filesystem-backed WAL writer.
///
/// This struct is responsible ONLY for writing bytes to `wal.log`.
/// It does not manage segment rotation, sequence assignment, recovery,
/// or any other higher-level concerns. Those belong to the WAL actor.
pub struct FsWalWriter {
    _file_path: String,
    file: Mutex<File>,
    current_pos: Mutex<WalPos>,
}

impl FsWalWriter {
    /// Create a new filesystem-backed WAL writer targeting `wal.log`.
    pub fn new(dir: &Path) -> MidgeResult<Self> {
        // Ensure directory exists.
        std::fs::create_dir_all(dir).map_err(MidgeError::Io)?;

        let file_path = dir.join("wal.log");

        // Open or create active WAL file in append mode.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .map_err(MidgeError::Io)?;

        // Determine current write position (file size).
        let current_pos = file.metadata().map_err(MidgeError::Io)?.len();

        Ok(Self {
            _file_path: file_path.to_string_lossy().into_owned(),
            file: Mutex::new(file),
            current_pos: Mutex::new(current_pos),
        })
    }
}

impl WalWriter for FsWalWriter {
    /// Append a fully constructed WAL record.
    ///
    /// The position returned is the starting offset of this record.
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // Encode using canonical WAL binary encoding.
        let encoded = encoding::encode(record)?;

        // Compute prefix.
        let len_prefix = (encoded.len() as u32).to_le_bytes();

        // Write atomically under file lock.
        let mut file = self.file.lock().expect("file mutex poisoned");

        file.write_all(&len_prefix).map_err(MidgeError::Io)?;
        file.write_all(&encoded).map_err(MidgeError::Io)?;

        // Update write position.
        let mut pos = self.current_pos.lock().expect("position mutex poisoned");
        let prev = *pos;
        *pos += 4 + encoded.len() as u64;

        Ok(prev)
    }

    /// This method is intentionally unsupported because it hides sequence semantics.
    ///
    /// The runtime MUST NOT rely on inference or automatic sequence assignment here.
    /// Always construct a full WalRecord and call append_record(), or call
    /// append_op_with_seq() from the WAL actor.
    fn append_op(
        &self,
        _kind: WalOpKind,
        _key: &[u8],
        _value: Option<&[u8]>,
    ) -> MidgeResult<WalPos> {
        Err(MidgeError::Internal(
            "append_op() without explicit sequence is unsupported; \
             use append_record() or append_op_with_seq() in the WAL actor"
                .to_string(),
        ))
    }

    /// Flush buffered writes to OS buffers.
    fn flush(&self) -> MidgeResult<()> {
        let mut file = self.file.lock().expect("file mutex poisoned");
        file.flush().map_err(MidgeError::Io)
    }

    /// Flush + fsync() — ensures durability.
    fn sync(&self) -> MidgeResult<()> {
        let file = self.file.lock().expect("file mutex poisoned");
        file.sync_all().map_err(MidgeError::Io)
    }

    fn sync_local(&self) -> MidgeResult<()> {
        // Local sync is identical for filesystem backend.
        self.sync()
    }

    fn current_pos(&self) -> WalPos {
        *self.current_pos.lock().expect("current_pos mutex poisoned")
    }

    fn close(&self) -> MidgeResult<()> {
        self.sync()
    }
}
