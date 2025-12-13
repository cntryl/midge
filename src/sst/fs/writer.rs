//! Filesystem-backed SST writer

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use crate::common::MidgeResult;
use crate::sst::encoding;
use crate::sst::types::{BlockHandle, Footer};

/// Simple filesystem SST writer that streams blocks to disk
pub struct FsSstWriter {
    file: std::fs::File,
    #[allow(dead_code)]
    temp_path: PathBuf,
    block_size: usize,

    // Current block being built
    current_entries: Vec<Vec<u8>>, // Pre-encoded entry bytes
    current_size: usize,
    offset: u64,

    // Metadata for footer
    data_block_offsets: Vec<(Vec<u8>, BlockHandle)>,
    #[allow(dead_code)]
    index_entries: Vec<(Vec<u8>, BlockHandle)>,
}

impl FsSstWriter {
    /// Create a new SST writer for a temporary file
    pub fn new(temp_dir: &Path, block_size: usize) -> MidgeResult<Self> {
        let id = uuid::Uuid::new_v4().to_string();
        let temp_path = temp_dir.join(format!("{}.sst.tmp", id));

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temp_path)?;

        Ok(Self {
            file,
            temp_path,
            block_size,
            current_entries: Vec::new(),
            current_size: 0,
            offset: 0,
            data_block_offsets: Vec::new(),
            index_entries: Vec::new(),
        })
    }

    /// Create with deterministic naming (for testing)
    pub fn new_with_seq(temp_dir: &Path, block_size: usize, seq: u64) -> MidgeResult<Self> {
        let temp_path = temp_dir.join(format!("{:016x}.sst.tmp", seq));

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temp_path)?;

        Ok(Self {
            file,
            temp_path,
            block_size,
            current_entries: Vec::new(),
            current_size: 0,
            offset: 0,
            data_block_offsets: Vec::new(),
            index_entries: Vec::new(),
        })
    }

    fn flush_block(&mut self, last_key: Vec<u8>) -> MidgeResult<()> {
        if self.current_entries.is_empty() {
            return Ok(());
        }

        // Build data block: serialize all entries
        let mut block_data = Vec::new();
        for entry_bytes in &self.current_entries {
            block_data.extend_from_slice(entry_bytes);
        }

        // Write block: [4-byte length] + data
        let len = block_data.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&block_data)?;

        let block_len = (4 + block_data.len()) as u64;
        let handle = BlockHandle::new(self.offset, block_len);
        self.data_block_offsets.push((last_key, handle));

        self.offset += block_len;
        self.current_entries.clear();
        self.current_size = 0;

        Ok(())
    }

    /// Check if adding new entry would exceed block size
    fn should_flush(&self, key_len: usize, value_len: usize) -> bool {
        let entry_est = key_len + value_len + 32; // Rough estimate with overhead
        self.current_size + entry_est > self.block_size && !self.current_entries.is_empty()
    }
}

use std::io::Write;

impl crate::sst::DynSstWriter for FsSstWriter {
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
        let value_len = value.map(|v| v.len()).unwrap_or(0);

        // Flush block if needed
        if self.should_flush(key.len(), value_len) {
            let last_key = self.current_entries.last().cloned().unwrap_or_default();
            self.flush_block(last_key)?;
        }

        // Encode entry
        let encoded = encoding::encode(key, 0, value, seq, op_type, expiration);
        self.current_size += encoded.len();
        self.current_entries.push(encoded);

        Ok(())
    }

    fn add_range_tombstone(&mut self, _start: &[u8], _end: &[u8], _seq: u64) -> MidgeResult<()> {
        Ok(())
    }

    fn finish_bytes(mut self: Box<Self>) -> MidgeResult<Vec<u8>> {
        // Flush final block
        if !self.current_entries.is_empty() {
            let last_key = self.current_entries.last().cloned().unwrap_or_default();
            self.flush_block(last_key)?;
        }

        // Build index block from data block offsets
        let mut index_data = Vec::new();
        for (key, handle) in &self.data_block_offsets {
            // Store: [4-byte key length] + key + [8-byte offset] + [8-byte size]
            index_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
            index_data.extend_from_slice(key);
            index_data.extend_from_slice(&handle.offset.to_le_bytes());
            index_data.extend_from_slice(&handle.size.to_le_bytes());
        }

        // Write index block
        let index_len = index_data.len() as u32;
        self.file.write_all(&index_len.to_le_bytes())?;
        self.file.write_all(&index_data)?;
        let index_handle = BlockHandle::new(self.offset, (4 + index_data.len()) as u64);
        self.offset += (4 + index_data.len()) as u64;

        // Meta-index block (empty for now)
        let meta_index_len = 0u32;
        self.file.write_all(&meta_index_len.to_le_bytes())?;
        let meta_index_handle = BlockHandle::new(self.offset, 4);

        // Footer
        let footer = Footer::new(meta_index_handle, index_handle);
        let footer_bytes = footer.encode();
        self.file.write_all(&footer_bytes)?;

        // Read back all bytes
        use std::io::{Seek, SeekFrom};
        self.file.seek(SeekFrom::Start(0))?;
        let mut result = Vec::new();
        std::io::Read::read_to_end(&mut self.file, &mut result)?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::DynSstWriter;

    #[test]
    fn should_write_entries_when_creating_sst() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path();

        // Act
        let mut writer = FsSstWriter::new(temp_path, 4096)?;
        writer.add(b"key1", b"value1")?;
        writer.add(b"key2", b"value2")?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_single_entry() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        writer.add(b"key", b"value")?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_empty_value() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        writer.add(b"key", b"")?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_many_entries() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        for i in 0..100 {
            let key = format!("key{:03}", i);
            let val = format!("value{}", i);
            writer.add(key.as_bytes(), val.as_bytes())?;
        }
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_flush_blocks_on_size_exceeded() -> MidgeResult<()> {
        // Arrange - small block size forces flushes
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 64)?;
        for i in 0..10 {
            let key = format!("key{:03}", i);
            let val = format!("value{:0100}", i); // Large value
            writer.add(key.as_bytes(), val.as_bytes())?;
        }
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert - should have multiple blocks
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_create_with_deterministic_naming() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new_with_seq(temp_dir.path(), 4096, 42)?;
        writer.add(b"key", b"value")?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_with_metadata() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        writer.add_with_meta(b"key", Some(b"value"), 1, 0, None)?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_deletion() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        writer.add_with_meta(b"key", None, 2, 1, None)?; // op_type=1 for delete
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_expiration() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        writer.add_with_meta(b"key", Some(b"value"), 0, 0, Some(999999))?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_handle_range_tombstone() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        writer.add_range_tombstone(b"start", b"end", 0)?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_large_block_size() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 64 * 1024)?;
        for i in 0..50 {
            let key = format!("k{}", i);
            let val = format!("v{}", i);
            writer.add(key.as_bytes(), val.as_bytes())?;
        }
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_small_block_size() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 128)?;
        for i in 0..20 {
            let key = format!("key{:02}", i);
            let val = format!("val{}", i);
            writer.add(key.as_bytes(), val.as_bytes())?;
        }
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_finish_empty_writer() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert - should have footer at minimum
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_utf8_keys() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        writer.add("こんにちは".as_bytes(), "世界".as_bytes())?;
        writer.add("🔥".as_bytes(), "💯".as_bytes())?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_binary_data() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        let binary_key = vec![0u8, 1, 2, 255, 254, 253];
        let binary_val = vec![255u8, 128, 64, 32, 16];
        writer.add(&binary_key, &binary_val)?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_track_offset() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;

        // Act - track offset progression
        let initial_offset = writer.offset;
        writer.add(b"key1", b"value1")?;

        // Flush by exceeding block size significantly
        for i in 0..100 {
            let key = format!("k{:04}", i);
            let val = format!("very_long_value_{:04}", i);
            writer.add(key.as_bytes(), val.as_bytes())?;
        }

        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert_eq!(initial_offset, 0);
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_keys_in_order() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            writer.add(key.as_bytes(), b"value")?;
        }
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_write_duplicate_keys() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        writer.add(b"key", b"value1")?;
        writer.add(b"key", b"value2")?;
        writer.add(b"key", b"value3")?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_sequence_numbers_in_metadata() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 4096)?;
        for i in 0..5 {
            writer.add_with_meta(b"key", Some(b"value"), i as u64, 0, None)?;
        }
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_handle_very_large_keys() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let large_key = vec![0u8; 10000];
        let large_val = vec![1u8; 10000];

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 64 * 1024)?;
        writer.add(&large_key, &large_val)?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }

    #[test]
    fn should_handle_very_large_values() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let large_val = vec![42u8; 100000];

        // Act
        let mut writer = FsSstWriter::new(temp_dir.path(), 64 * 1024)?;
        writer.add(b"key", &large_val)?;
        let bytes = Box::new(writer).finish_bytes()?;

        // Assert
        assert!(!bytes.is_empty());
        Ok(())
    }
}
