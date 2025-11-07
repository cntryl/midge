use crate::error::MidgeResult;
use crate::wal::traits::WalWriter;
use crate::wal::types::{WalOpKind, WalPos, WalRecord};
use parking_lot::Mutex;
use std::sync::Arc;

use super::shared::MemInner;

/// In-memory WAL writer backed by a shared buffer
pub struct WalMem {
    pub(super) inner: Arc<Mutex<MemInner>>,
}

impl Clone for WalMem {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl WalMem {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemInner::default())),
        }
    }

    /// Create a paired writer/reader sharing the same in-memory buffer.
    pub fn new_pair() -> (WalMemWriter, WalMemReaderHandle) {
        let inner = Arc::new(Mutex::new(MemInner::default()));
        (
            WalMem {
                inner: inner.clone(),
            },
            super::reader::WalMemReader { inner },
        )
    }

    /// Truncate/clear the in-memory WAL.
    pub fn truncate(&self) -> MidgeResult<()> {
        let mut g = self.inner.lock();
        g.buf.clear();
        Ok(())
    }
}

impl Default for WalMem {
    fn default() -> Self {
        Self::new()
    }
}

impl WalWriter for WalMem {
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // OPTIMIZATION: Serialize the record before acquiring the lock.
        // This reduces the critical section and improves write throughput.
        let data = bincode::serialize(record)?;
        let len = data.len() as u32;
        let len_bytes = len.to_le_bytes();

        // Now acquire lock only for the append operation
        let mut g = self.inner.lock();
        let pos = g.buf.len() as WalPos;
        g.buf.extend_from_slice(&len_bytes);
        g.buf.extend_from_slice(&data);
        Ok(pos)
    }

    fn append_op(&self, kind: WalOpKind, key: &[u8], value: Option<&[u8]>) -> MidgeResult<WalPos> {
        let rec = WalRecord::new(
            kind,
            bytes::Bytes::copy_from_slice(key),
            value.map(bytes::Bytes::copy_from_slice),
            0,
        );
        self.append_record(&rec)
    }

    fn flush(&self) -> MidgeResult<()> {
        Ok(())
    }
    fn sync(&self) -> MidgeResult<()> {
        Ok(())
    }

    fn current_pos(&self) -> WalPos {
        let g = self.inner.lock();
        g.buf.len() as WalPos
    }

    fn close(&self) -> MidgeResult<()> {
        Ok(())
    }
}

// Convenience re-export for downstream code/tests
pub use super::reader::WalMemReader as WalMemReaderHandle;
pub use WalMem as WalMemWriter;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalWriter;

    #[test]
    fn should_initialize_with_zero_position() {
        // Arrange & Act
        let wal = WalMem::new();

        // Assert
        assert_eq!(wal.current_pos(), 0);
    }

    #[test]
    fn should_accept_operations_successfully() {
        // Arrange
        let wal = WalMem::new();

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_increment_position_after_each_append() {
        // Arrange
        let wal = WalMem::new();
        let pos1 = wal.current_pos();

        // Act
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");
        let pos2 = wal.current_pos();

        // Assert
        assert!(pos2 > pos1);
    }

    #[test]
    fn should_reset_position_when_truncated() {
        // Arrange
        let wal = WalMem::new();
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");
        let pos_before = wal.current_pos();

        // Act
        wal.truncate().expect("truncate");
        let pos_after = wal.current_pos();

        // Assert
        assert!(pos_after < pos_before || pos_after == 0);
    }

    #[test]
    fn should_complete_sync_as_noop() {
        // Arrange
        let wal = WalMem::new();
        wal.append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");

        // Act
        let result = wal.sync();

        // Assert: in-memory WAL sync is a no-op but should succeed
        assert!(result.is_ok());
    }

    #[test]
    fn should_track_positions_given_multiple_appends() {
        // Arrange
        let wal = WalMem::new();

        // Act
        for i in 0..100 {
            let result = wal.append_op(
                crate::wal::WalOpKind::Put,
                format!("key{}", i).as_bytes(),
                Some(format!("value{}", i).as_bytes()),
            );
            assert!(result.is_ok());
        }

        // Assert
        assert!(wal.current_pos() > 0);
    }

    #[test]
    fn should_append_delete_operations_successfully() {
        // Arrange
        let wal = WalMem::new();

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Delete, b"key1", None);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_append_large_values_successfully() {
        // Arrange
        let wal = WalMem::new();
        let large_value = vec![0xAB; 1024 * 1024]; // 1MB

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Put, b"large_key", Some(&large_value));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_append_empty_keys_successfully() {
        // Arrange
        let wal = WalMem::new();

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Put, b"", Some(b"value"));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_append_binary_data_successfully() {
        // Arrange
        let wal = WalMem::new();
        let binary_key = vec![0x00, 0xFF, 0x80, 0x7F];
        let binary_value = vec![0xDE, 0xAD, 0xBE, 0xEF];

        // Act
        let result = wal.append_op(crate::wal::WalOpKind::Put, &binary_key, Some(&binary_value));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_initialize_connected_writer_reader_pair() {
        // Arrange & Act
        let (writer, _reader) = WalMem::new_pair();

        // Assert
        assert_eq!(writer.current_pos(), 0);
    }
}
