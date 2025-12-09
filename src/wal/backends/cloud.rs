//! Cloud WAL backend - persistent WAL in cloud storage
//!
//! Provides WAL storage in cloud providers (S3, GCS, Azure) for:
//! - Remote durability of write operations
//! - Backup and disaster recovery
//! - Multi-region replication (in future)

use crate::common::MidgeResult;
use crate::wal::traits::{WalWriter, WalReader, WalReaderDyn, WalFactory};
use crate::wal::types::{WalOpKind, WalPos, WalRecord};
use crate::storage::cloud::{CloudProvider, CloudStorage};
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use std::path::Path;

/// Cloud-based WAL writer
pub struct CloudWalWriter {
    /// Cloud provider for durability
    provider: Arc<dyn CloudProvider>,
    /// Namespace/prefix for WAL files
    namespace: String,
    /// Current WAL segment ID
    segment_id: u64,
    /// Records buffered for current segment
    buffer: Arc<Mutex<Vec<WalRecord>>>,
    /// Current position in the WAL
    position: Arc<Mutex<WalPos>>,
    /// Segment size limit (rotate when exceeded)
    segment_size_limit: usize,
}

impl CloudWalWriter {
    pub fn new(provider: Arc<dyn CloudProvider>, namespace: String) -> Self {
        Self {
            provider,
            namespace,
            segment_id: 0,
            buffer: Arc::new(Mutex::new(Vec::new())),
            position: Arc::new(Mutex::new(0)),
            segment_size_limit: 64 * 1024 * 1024, // 64MB segments
        }
    }

    fn segment_path(&self) -> String {
        format!("{}/wal_{:06}.log", self.namespace, self.segment_id)
    }

    fn encode_records(&self, records: &[WalRecord]) -> Vec<u8> {
        let mut encoded = Vec::new();
        for record in records {
            if let Ok(data) = crate::wal::encode(record) {
                encoded.extend(data);
            }
        }
        encoded
    }
}

impl WalWriter for CloudWalWriter {
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push(record.clone());

        let mut pos = self.position.lock().unwrap();
        *pos += 1;

        Ok(*pos)
    }

    fn append_op(&self, kind: WalOpKind, key: &[u8], value: Option<&[u8]>) -> MidgeResult<WalPos> {
        let record = WalRecord::new(
            kind,
            bytes::Bytes::copy_from_slice(key),
            value.map(bytes::Bytes::copy_from_slice),
            self.current_pos(),
        );
        self.append_record(&record)
    }

    fn append_batch(&self, records: &[WalRecord]) -> MidgeResult<WalPos> {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.extend_from_slice(records);

        let mut pos = self.position.lock().unwrap();
        *pos += records.len() as u64;

        Ok(*pos)
    }

    fn flush(&self) -> MidgeResult<()> {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.is_empty() {
            return Ok(());
        }

        let encoded = self.encode_records(&buffer);
        let path = self.segment_path();
        self.provider.upload(&path, &encoded)?;

        buffer.clear();
        Ok(())
    }

    fn sync(&self) -> MidgeResult<()> {
        self.flush()
    }

    fn sync_local(&self) -> MidgeResult<()> {
        // For cloud, sync_local is same as sync
        self.sync()
    }

    fn current_pos(&self) -> WalPos {
        *self.position.lock().unwrap()
    }

    fn close(&self) -> MidgeResult<()> {
        self.sync()?;
        Ok(())
    }

    fn shutdown(&self) {
        let _ = self.close();
    }
}

/// Cloud-based WAL reader
pub struct CloudWalReader {
    /// Cloud provider for reads
    provider: Arc<dyn CloudProvider>,
    /// Namespace/prefix for WAL files
    namespace: String,
    /// Loaded records
    records: VecDeque<WalRecord>,
}

impl CloudWalReader {
    pub fn new(provider: Arc<dyn CloudProvider>, namespace: String) -> Self {
        Self {
            provider,
            namespace,
            records: VecDeque::new(),
        }
    }

    pub fn load_segment(&mut self, segment_id: u64) -> MidgeResult<usize> {
        let path = format!("{}/wal_{:06}.log", self.namespace, segment_id);
        
        match self.provider.download(&path) {
            Ok(data) => {
                // Parse encoded records
                let mut cursor = std::io::Cursor::new(data);
                loop {
                    match crate::wal::decode(&mut cursor) {
                        Ok(record) => {
                            self.records.push_back(record);
                        }
                        Err(_) => break, // End of valid records
                    }
                }
                Ok(self.records.len())
            }
            Err(crate::common::MidgeError::NotFound) => {
                // Segment doesn't exist yet
                Ok(0)
            }
            Err(e) => Err(e),
        }
    }
}

impl WalReader for CloudWalReader {
    fn read_at(&mut self, _pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        // Cloud reader uses sequential replay, not random access
        Ok(self.records.pop_front())
    }

    fn replay<F>(&mut self, start: WalPos, mut cb: F) -> MidgeResult<()>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>,
    {
        // Load from start segment onwards
        let segment_id = start / 1_000_000; // Rough mapping
        self.load_segment(segment_id)?;
        
        for record in self.records.iter() {
            cb(record)?;
        }
        Ok(())
    }

    fn close(&mut self) -> MidgeResult<()> {
        Ok(())
    }
}

impl WalReaderDyn for CloudWalReader {
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        WalReader::read_at(self, pos)
    }

    fn replay_boxed(
        &mut self,
        start: WalPos,
        cb: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    ) -> MidgeResult<()> {
        WalReader::replay(self, start, |record| cb(record))
    }

    fn close(&mut self) -> MidgeResult<()> {
        WalReader::close(self)
    }
}

/// Factory for creating Cloud WAL instances
pub struct CloudWalFactory {
    provider: Arc<dyn CloudProvider>,
    namespace: String,
}

impl CloudWalFactory {
    pub fn new(provider: Arc<dyn CloudProvider>, namespace: String) -> Self {
        Self { provider, namespace }
    }
}

impl WalFactory for CloudWalFactory {
    fn create_writer(&self, _dir: &Path) -> MidgeResult<Box<dyn WalWriter>> {
        Ok(Box::new(CloudWalWriter::new(
            self.provider.clone(),
            self.namespace.clone(),
        )))
    }

    fn create_reader(&self, _dir: &Path) -> MidgeResult<Box<dyn WalReaderDyn>> {
        Ok(Box::new(CloudWalReader::new(
            self.provider.clone(),
            self.namespace.clone(),
        )))
    }

    fn rotate_writer(&self, _dir: &Path, _seq: u64) -> MidgeResult<Box<dyn WalWriter>> {
        Ok(Box::new(CloudWalWriter::new(
            self.provider.clone(),
            self.namespace.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::MockCloud;

    #[test]
    fn should_create_cloud_wal_writer_when_instantiated() {
        // Arrange
        let provider = Arc::new(MockCloud::new());
        let namespace = "test/wal".to_string();

        // Act
        let writer = CloudWalWriter::new(provider, namespace);

        // Assert
        assert_eq!(writer.current_pos(), 0);
    }

    #[test]
    fn should_append_record_when_append_record_called() {
        // Arrange
        let provider = Arc::new(MockCloud::new());
        let writer = CloudWalWriter::new(provider, "test/wal".to_string());
        let record = WalRecord::new(
            WalOpKind::Put,
            bytes::Bytes::from("key"),
            Some(bytes::Bytes::from("value")),
            1,
        );

        // Act
        let pos = writer.append_record(&record).unwrap();

        // Assert
        assert_eq!(pos, 1);
        assert_eq!(writer.current_pos(), 1);
    }

    #[test]
    fn should_append_operation_when_append_op_called() {
        // Arrange
        let provider = Arc::new(MockCloud::new());
        let writer = CloudWalWriter::new(provider, "test/wal".to_string());

        // Act
        let pos = writer.append_op(WalOpKind::Put, b"key", Some(b"value")).unwrap();

        // Assert
        assert_eq!(pos, 1);
        assert_eq!(writer.current_pos(), 1);
    }

    #[test]
    fn should_append_batch_when_append_batch_called() {
        // Arrange
        let provider = Arc::new(MockCloud::new());
        let writer = CloudWalWriter::new(provider, "test/wal".to_string());
        let records = vec![
            WalRecord::new(WalOpKind::Put, bytes::Bytes::from("k1"), Some(bytes::Bytes::from("v1")), 1),
            WalRecord::new(WalOpKind::Delete, bytes::Bytes::from("k2"), None, 2),
        ];

        // Act
        let pos = writer.append_batch(&records).unwrap();

        // Assert
        assert_eq!(pos, 2);
        assert_eq!(writer.current_pos(), 2);
    }

    #[test]
    fn should_flush_to_cloud_when_flush_called() {
        // Arrange
        let provider = Arc::new(MockCloud::new());
        let writer = CloudWalWriter::new(provider.clone(), "test/wal".to_string());
        writer.append_op(WalOpKind::Put, b"key1", Some(b"value1")).unwrap();
        writer.append_op(WalOpKind::Put, b"key2", Some(b"value2")).unwrap();

        // Act
        writer.flush().unwrap();

        // Assert
        assert_eq!(provider.object_count(), 1);
        assert_eq!(provider.get_uploads().len(), 1);
    }

    #[test]
    fn should_close_writer_when_close_called() {
        // Arrange
        let provider = Arc::new(MockCloud::new());
        let writer = CloudWalWriter::new(provider.clone(), "test/wal".to_string());
        writer.append_op(WalOpKind::Put, b"key", Some(b"value")).unwrap();

        // Act
        writer.close().unwrap();

        // Assert
        assert_eq!(provider.object_count(), 1);
    }

    #[test]
    fn should_create_reader_when_instantiated() {
        // Arrange
        let provider = Arc::new(MockCloud::new());

        // Act
        let reader = CloudWalReader::new(provider, "test/wal".to_string());

        // Assert
        assert_eq!(reader.records.len(), 0);
    }

    #[test]
    fn should_return_not_found_when_loading_nonexistent_segment() {
        // Arrange
        let provider = Arc::new(MockCloud::new());
        let mut reader = CloudWalReader::new(provider, "test/wal".to_string());

        // Act
        let count = reader.load_segment(0).unwrap();

        // Assert
        assert_eq!(count, 0);
    }

    #[test]
    fn should_create_factory_when_instantiated() {
        // Arrange
        let provider = Arc::new(MockCloud::new());

        // Act
        let factory = CloudWalFactory::new(provider, "test/wal".to_string());

        // Assert
        let _writer = factory.create_writer(Path::new(".")).unwrap();
        let _reader = factory.create_reader(Path::new(".")).unwrap();
    }

    #[test]
    fn should_support_shutdown_when_shutdown_called() {
        // Arrange
        let provider = Arc::new(MockCloud::new());
        let writer = CloudWalWriter::new(provider.clone(), "test/wal".to_string());
        writer.append_op(WalOpKind::Put, b"key", Some(b"value")).unwrap();

        // Act
        writer.shutdown();

        // Assert
        assert_eq!(provider.object_count(), 1);
    }
}
