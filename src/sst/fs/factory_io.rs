//! Factory for creating io::Fs-backed SST readers and writers

use crate::common::MidgeResult;
use crate::sst::traits::{DynSstWriter, SstFactory};
use std::path::Path;
use std::sync::Arc;

use crate::io::Fs;

use crate::sst::compression::CompressionPolicy;
use crate::sst::encoding::EntryType;
use crate::sst::types::{
    encode_range_tombstones, BlockHandle, Footer, RangeTombstone, SstMetadata, SST_FORMAT_V2,
};

/// SST factory that uses io::Fs abstraction
/// Allows using different filesystem implementations (Real, Mock, Chaos) for testing
pub struct FsSstFactoryIo {
    fs: Arc<dyn Fs>,
    block_size: usize,
    compression_policy: CompressionPolicy,
}

impl FsSstFactoryIo {
    /// Create a new factory with a custom filesystem implementation
    pub fn new(fs: Arc<dyn Fs>, block_size: usize) -> Self {
        Self {
            fs,
            block_size,
            compression_policy: CompressionPolicy::default(),
        }
    }

    /// Create with custom block size
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    /// Set the compression policy for SST blocks produced by this factory.
    pub fn with_compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.compression_policy = policy;
        self
    }

    /// Open an SST file using the io::Fs backend
    pub fn open(&self, path: &Path) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        let path_str = path.to_str().unwrap_or("").to_string();
        let start = std::time::Instant::now();
        let reader = super::SstFileIo::open(&path_str, Arc::clone(&self.fs))?;
        let elapsed = start.elapsed();
        // Try to gather file size for diagnostics (best-effort)
        let size = self
            .fs
            .metadata(&crate::io::FsPath::new(path_str.as_str()))
            .ok()
            .map(|m| m.len)
            .unwrap_or(0);
        tracing::info!(path = ?path, size_bytes = size, open_ms = elapsed.as_secs_f64() * 1000.0, "sst reader opened");
        Ok(Box::new(reader))
    }
}

/// Simple in-memory SST writer that applies block-level compression.
struct InMemorySstWriter {
    entries: Vec<PendingEntry>,
    range_tombstones: Vec<RangeTombstone>,
    block_size: usize,
    compression_policy: CompressionPolicy,
}

#[derive(Debug, Clone)]
struct PendingEntry {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    sequence: u64,
    op_type: u8,
    expiration: Option<u64>,
}

impl InMemorySstWriter {
    fn new(compression_policy: CompressionPolicy, block_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            range_tombstones: Vec::new(),
            block_size,
            compression_policy,
        }
    }

    fn append_block(
        file_bytes: &mut Vec<u8>,
        block_bytes: &[u8],
        compression_policy: &CompressionPolicy,
    ) -> MidgeResult<BlockHandle> {
        use crate::sst::compression;

        let compressed = compression::compress_block_with_trailer(block_bytes, compression_policy)?;
        let offset = file_bytes.len() as u64;
        let size = 4 + compressed.len() as u64;
        file_bytes.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        file_bytes.extend_from_slice(&compressed);
        Ok(BlockHandle::new(offset, size))
    }

    fn serialize_index(index_entries: &[(Vec<u8>, BlockHandle)]) -> Vec<u8> {
        let mut index_bytes = Vec::new();
        for (key, handle) in index_entries {
            index_bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            index_bytes.extend_from_slice(key);
            index_bytes.extend_from_slice(&handle.offset.to_le_bytes());
            index_bytes.extend_from_slice(&handle.size.to_le_bytes());
        }
        index_bytes
    }

    fn shared_prefix_len(previous_key: &[u8], key: &[u8]) -> u16 {
        let shared = previous_key
            .iter()
            .zip(key.iter())
            .take_while(|(left, right)| left == right)
            .count();
        shared.min(u16::MAX as usize) as u16
    }
}

impl DynSstWriter for InMemorySstWriter {
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.add_with_meta(key, Some(value), 0, 0, None)
    }

    fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: u8,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        self.entries.push(PendingEntry {
            key: key.to_vec(),
            value: value.map(|bytes| bytes.to_vec()),
            sequence: seq,
            op_type,
            expiration,
        });
        Ok(())
    }

    fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) -> MidgeResult<()> {
        self.range_tombstones
            .push(RangeTombstone::new(start.to_vec(), end.to_vec(), seq));
        Ok(())
    }

    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
        let mut entries = self.entries;
        entries.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| right.sequence.cmp(&left.sequence))
        });

        let target_block_size = self.block_size.max(4 * 1024);
        let mut file_bytes = Vec::new();
        let mut index_entries: Vec<(Vec<u8>, BlockHandle)> = Vec::new();
        let mut current_block = Vec::new();
        let mut current_first_key: Option<Vec<u8>> = None;
        let mut previous_key = Vec::new();

        for entry in entries {
            let shared_len = Self::shared_prefix_len(&previous_key, &entry.key);
            let key_delta = &entry.key[shared_len as usize..];
            let encoded = crate::sst::encoding::encode_v2(
                key_delta,
                shared_len,
                entry.value.as_deref(),
                entry.sequence,
                match entry.op_type {
                    1 => EntryType::Insert,
                    2 => EntryType::Delete,
                    3 => EntryType::Merge,
                    _ => EntryType::Put,
                },
                entry.expiration,
            );

            if !current_block.is_empty() && current_block.len() + encoded.len() > target_block_size
            {
                let handle =
                    Self::append_block(&mut file_bytes, &current_block, &self.compression_policy)?;
                if let Some(first_key) = current_first_key.take() {
                    index_entries.push((first_key, handle));
                }
                current_block.clear();
                previous_key.clear();
            }

            if current_first_key.is_none() {
                current_first_key = Some(entry.key.clone());
            }

            current_block.extend_from_slice(&encoded);
            previous_key = entry.key;
        }

        if !current_block.is_empty() {
            let handle =
                Self::append_block(&mut file_bytes, &current_block, &self.compression_policy)?;
            if let Some(first_key) = current_first_key.take() {
                index_entries.push((first_key, handle));
            }
        }

        let range_tombstone_handle = if self.range_tombstones.is_empty() {
            None
        } else {
            let block_bytes = encode_range_tombstones(&self.range_tombstones);
            Some(Self::append_block(
                &mut file_bytes,
                &block_bytes,
                &self.compression_policy,
            )?)
        };

        let metadata = SstMetadata {
            format_version: SST_FORMAT_V2,
            range_tombstone_handle,
        };
        let meta_handle = Self::append_block(
            &mut file_bytes,
            &metadata.encode(),
            &self.compression_policy,
        )?;

        let index_bytes = Self::serialize_index(&index_entries);
        let index_handle =
            Self::append_block(&mut file_bytes, &index_bytes, &self.compression_policy)?;

        let footer = Footer::new(meta_handle, index_handle);
        file_bytes.extend_from_slice(&footer.encode());

        Ok(file_bytes)
    }
}

impl SstFactory for FsSstFactoryIo {
    /// Create a new SST writer
    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>> {
        Ok(Box::new(InMemorySstWriter::new(
            self.compression_policy.clone(),
            self.block_size,
        )))
    }

    /// Open an existing SST file
    fn open(&self, path: &Path) -> MidgeResult<Box<dyn crate::sst::traits::SstReaderExt>> {
        FsSstFactoryIo::open(self, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn should_create_factory_with_mock_fs() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let factory = FsSstFactoryIo::new(fs, 4096);

        // Assert
        assert_eq!(factory.block_size, 4096);
    }

    #[test]
    fn should_create_factory_with_real_fs() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);

        // Act
        let factory = FsSstFactoryIo::new(fs, 4096);

        // Assert
        assert_eq!(factory.block_size, 4096);
        Ok(())
    }

    #[test]
    fn should_support_method_chaining() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let factory = FsSstFactoryIo::new(fs, 4096).with_block_size(8192);

        // Assert
        assert_eq!(factory.block_size, 8192);
    }

    #[test]
    fn should_roundtrip_stateful_entries_when_sst_contains_range_tombstones() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = FsSstFactoryIo::new(fs, 4096);
        let path = temp_dir.path().join("stateful.sst");

        let mut writer = factory.create()?;
        writer.add_with_meta(b"alpha", Some(b"value-a"), 10, 0, Some(5_000))?;
        writer.add_with_meta(b"alpha", None, 9, 2, None)?;
        writer.add_with_meta(b"beta", Some(b"value-b"), 8, 1, None)?;
        writer.add_range_tombstone(b"cat", b"cow", 7)?;
        writer.finish_to_path(&path)?;

        // Act
        let reader = factory.open(std::path::Path::new("stateful.sst"))?;
        let states = reader.scan_range_state(None, None)?;

        // Assert
        assert_eq!(states.len(), 3);
        match &states[0].1 {
            crate::sst::types::KeyState::Value(value, seq, expiration, op_type) => {
                assert_eq!(states[0].0.as_ref(), b"alpha");
                assert_eq!(value.as_ref(), b"value-a");
                assert_eq!(*seq, 10);
                assert_eq!(*expiration, Some(5_000));
                assert_eq!(*op_type, 0);
            }
            other => panic!("expected value state, got {other:?}"),
        }

        match &states[1].1 {
            crate::sst::types::KeyState::Tombstone(seq) => {
                assert_eq!(states[1].0.as_ref(), b"alpha");
                assert_eq!(*seq, 9);
            }
            other => panic!("expected tombstone state, got {other:?}"),
        }

        assert_eq!(reader.range_tombstones().len(), 1);
        assert_eq!(reader.range_tombstones()[0].start, b"cat".to_vec());
        assert_eq!(reader.range_tombstones()[0].end, b"cow".to_vec());

        Ok(())
    }
}
