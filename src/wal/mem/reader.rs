use crate::error::{MidgeError, MidgeResult};
use crate::wal::traits::WalReader;
use crate::wal::types::{WalPos, WalRecord};
use parking_lot::Mutex;
use std::sync::Arc;

use super::shared::MemInner;

/// In-memory WAL reader that reads from the shared buffer
pub struct WalMemReader {
    pub(super) inner: Arc<Mutex<MemInner>>,
}

impl WalMemReader {
    pub fn new() -> Self {
        WalMemReader {
            inner: Arc::new(Mutex::new(MemInner::default())),
        }
    }
}

impl Default for WalMemReader {
    fn default() -> Self {
        Self::new()
    }
}

impl WalReader for WalMemReader {
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        let g = self.inner.lock();
        let buf = &g.buf;
        let mut off = pos as usize;
        if off >= buf.len() {
            return Ok(None);
        }
        if off + 4 > buf.len() {
            return Err(MidgeError::corruption("truncated length prefix"));
        }
        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&buf[off..off + 4]);
        off += 4;
        let len = u32::from_le_bytes(len_buf) as usize;
        if off + len > buf.len() {
            return Err(MidgeError::corruption("truncated record body"));
        }
        let rec_bytes = &buf[off..off + len];
        let rec: WalRecord = bincode::deserialize(rec_bytes)?;
        Ok(Some(rec))
    }

    fn replay<F>(&mut self, start: WalPos, mut cb: F) -> MidgeResult<()>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>,
    {
        let mut off = start as usize;
        loop {
            // scope lock to read one record worth of bytes
            let (len_opt, rec_opt) = {
                let g = self.inner.lock();
                let buf = &g.buf;
                if off >= buf.len() {
                    (None, None)
                } else {
                    if off + 4 > buf.len() {
                        return Err(MidgeError::corruption("truncated length prefix"));
                    }
                    let mut len_buf = [0u8; 4];
                    len_buf.copy_from_slice(&buf[off..off + 4]);
                    off += 4;
                    let len = u32::from_le_bytes(len_buf) as usize;
                    if off + len > buf.len() {
                        return Err(MidgeError::corruption("truncated record body"));
                    }
                    let rec_bytes = &buf[off..off + len];
                    off += len;
                    let rec: WalRecord = bincode::deserialize(rec_bytes)?;
                    (Some(len), Some(rec))
                }
            };

            match (len_opt, rec_opt) {
                (None, None) => break, // EOF
                (Some(_), Some(rec)) => cb(&rec)?,
                // Inconsistent state detected while reading the in-memory WAL buffer.
                // Treat this as corruption rather than panicking.
                _ => return Err(MidgeError::corruption("inconsistent WAL buffer state")),
            }
        }
        Ok(())
    }

    fn close(&mut self) -> MidgeResult<()> {
        Ok(())
    }
}

// Adapter to make WalMemReader implement WalReaderDyn
#[allow(dead_code)]
pub(super) struct WalMemReaderDynAdapter(pub(super) WalMemReader);

impl crate::wal::WalReaderDyn for WalMemReaderDynAdapter {
    fn read_at(
        &mut self,
        pos: crate::wal::WalPos,
    ) -> crate::error::MidgeResult<Option<crate::wal::WalRecord>> {
        crate::wal::WalReader::read_at(&mut self.0, pos)
    }

    fn replay_boxed(
        &mut self,
        start: crate::wal::WalPos,
        cb: &mut dyn FnMut(&crate::wal::WalRecord) -> crate::error::MidgeResult<()>,
    ) -> crate::error::MidgeResult<()> {
        crate::wal::WalReader::replay(&mut self.0, start, |rec| cb(rec))
    }

    fn close(&mut self) -> crate::error::MidgeResult<()> {
        crate::wal::WalReader::close(&mut self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::wal::{WalReader, WalWriter};

    #[test]
    fn should_read_written_operations_in_pair() {
        // Arrange
        let (writer, mut reader) = super::super::writer::WalMem::new_pair();

        // Act: write an operation
        writer
            .append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");

        // Act: read it back
        let mut records = vec![];
        reader
            .replay(0, &mut |rec: &crate::wal::WalRecord| {
                records.push(rec.clone());
                Ok(())
            })
            .expect("replay");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, bytes::Bytes::from_static(b"key1"));
    }

    #[test]
    fn should_maintain_write_order_when_reading_pair() {
        // Arrange
        let (writer, mut reader) = super::super::writer::WalMem::new_pair();

        // Act: write multiple operations
        for i in 0..5 {
            writer
                .append_op(
                    crate::wal::WalOpKind::Put,
                    format!("key{}", i).as_bytes(),
                    Some(format!("value{}", i).as_bytes()),
                )
                .expect("append");
        }

        // Act: read them back
        let mut records = vec![];
        reader
            .replay(0, &mut |rec: &crate::wal::WalRecord| {
                records.push(rec.clone());
                Ok(())
            })
            .expect("replay");

        // Assert: order preserved
        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate().take(5) {
            let expected_key = format!("key{}", i);
            assert_eq!(record.key.as_ref(), expected_key.as_bytes());
        }
    }

    #[test]
    fn should_clear_readable_data_when_pair_truncated() {
        // Arrange
        let (writer, mut reader) = super::super::writer::WalMem::new_pair();
        writer
            .append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");

        // Act: truncate
        writer.truncate().expect("truncate");

        // Act: read after truncate
        let mut records = vec![];
        reader
            .replay(0, &mut |rec: &crate::wal::WalRecord| {
                records.push(rec.clone());
                Ok(())
            })
            .expect("replay");

        // Assert: should be empty after truncate
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn should_support_concurrent_reads_from_readers() {
        // Arrange
        let (writer, mut reader) = super::super::writer::WalMem::new_pair();
        writer
            .append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");
        writer
            .append_op(crate::wal::WalOpKind::Put, b"key2", Some(b"value2"))
            .expect("append");

        // Act: read twice
        let mut records1 = vec![];
        reader
            .replay(0, &mut |rec: &crate::wal::WalRecord| {
                records1.push(rec.clone());
                Ok(())
            })
            .expect("replay");

        let mut records2 = vec![];
        reader
            .replay(0, &mut |rec: &crate::wal::WalRecord| {
                records2.push(rec.clone());
                Ok(())
            })
            .expect("replay");

        // Assert: both reads should see the same data
        assert_eq!(records1.len(), records2.len());
    }

    #[test]
    fn should_replay_from_specified_position() {
        // Arrange
        let (writer, mut reader) = super::super::writer::WalMem::new_pair();
        writer
            .append_op(crate::wal::WalOpKind::Put, b"key1", Some(b"value1"))
            .expect("append");
        let mid_pos = writer.current_pos();
        writer
            .append_op(crate::wal::WalOpKind::Put, b"key2", Some(b"value2"))
            .expect("append");

        // Act: replay from mid position
        let mut records = vec![];
        reader
            .replay(mid_pos, &mut |rec: &crate::wal::WalRecord| {
                records.push(rec.clone());
                Ok(())
            })
            .expect("replay");

        // Assert: should only see second record
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, bytes::Bytes::from_static(b"key2"));
    }
}
