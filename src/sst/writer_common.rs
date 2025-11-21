//! Common SST writer functionality shared across implementations.
//!
//! This module extracts the duplicated logic from cloud/mem/fs writers into
//! reusable components to reduce code duplication and improve maintainability.

use crate::common::codec::CompressionType;
use crate::error::MidgeResult;
use crate::sst::bloom::BloomFilterBuilder;
use crate::sst::format::{
    Block, BlockHandle, BlockType, DataBlockBuilder, Footer, IndexBlockBuilder,
};
use crate::sst::traits::RangeTombstone;
use bytes::{BufMut, Bytes, BytesMut};

/// Common configuration for SST writers.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub block_size: usize,
    pub compression: CompressionType,
    pub bloom_bits_per_key: u32,
    pub use_internal_keys: bool,
}

impl WriterConfig {
    pub fn new(block_size: usize, compression: CompressionType) -> Self {
        Self {
            block_size,
            compression,
            bloom_bits_per_key: 10, // Default: ~1% false positive rate
            use_internal_keys: false,
        }
    }

    pub fn with_internal_keys(mut self, use_internal: bool) -> Self {
        self.use_internal_keys = use_internal;
        self
    }

    pub fn with_bloom_bits(mut self, bits_per_key: u32) -> Self {
        self.bloom_bits_per_key = bits_per_key;
        self
    }
}

/// Common state maintained during SST construction.
pub struct WriterState {
    pub config: WriterConfig,
    pub cur_block: DataBlockBuilder,
    pub last_key_in_block: Option<Bytes>,
    pub index: IndexBlockBuilder,
    pub bloom_builder: BloomFilterBuilder,
    pub range_tombstones: Vec<RangeTombstone>,
    /// Cache for decoded user key to avoid redundant decode_internal_key() calls.
    last_internal_key: Option<Bytes>,
    last_user_key_cache: Option<Bytes>,
}

impl WriterState {
    pub fn new(config: WriterConfig) -> Self {
        let bloom_builder = BloomFilterBuilder::with_bits_per_key(config.bloom_bits_per_key);
        Self {
            config,
            cur_block: DataBlockBuilder::new(16),
            last_key_in_block: None,
            index: IndexBlockBuilder::new(),
            bloom_builder,
            range_tombstones: Vec::new(),
            last_internal_key: None,
            last_user_key_cache: None,
        }
    }

    /// Create with expected entry count for better bloom filter sizing.
    pub fn with_expected_entries(config: WriterConfig, expected_entries: usize) -> Self {
        let bloom_builder =
            BloomFilterBuilder::with_expected_keys(expected_entries, config.bloom_bits_per_key);
        Self {
            config,
            cur_block: DataBlockBuilder::new(16),
            last_key_in_block: None,
            index: IndexBlockBuilder::new(),
            bloom_builder,
            range_tombstones: Vec::new(),
            last_internal_key: None,
            last_user_key_cache: None,
        }
    }

    /// Check if the current block would exceed the target size with the given entry.
    pub fn should_flush_block(&self, key: &[u8], value: Option<&[u8]>) -> bool {
        if self.cur_block.is_empty() {
            return false;
        }
        self.cur_block.estimated_size() + key.len() + value.unwrap_or(&[]).len() + 16
            > self.config.block_size
    }

    /// Flush the current block and return the (last_key, encoded_block) pair.
    pub fn flush_current_block(&mut self) -> Option<(Bytes, Bytes)> {
        if self.cur_block.is_empty() {
            return None;
        }
        let last_key = self.last_key_in_block.clone().unwrap_or_default();
        let builder = std::mem::replace(&mut self.cur_block, DataBlockBuilder::new(16));
        let payload = builder.finish();
        let block = Block::new(payload, BlockType::Data, self.config.compression);
        let encoded = block.encode().expect("encode block");
        self.last_key_in_block = None;
        Some((last_key, encoded))
    }

    /// Add an entry with metadata, handling internal key encoding.
    pub fn add_entry(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        tombstone: bool,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        if self.config.use_internal_keys {
            self.add_entry_internal_key(key, value, seq, tombstone, expiration)
        } else {
            self.add_entry_plain_key(key, value, seq, tombstone, expiration)
        }
    }

    fn add_entry_internal_key(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        tombstone: bool,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        // Key may already be encoded as an internal key (user||seq||kind).
        // Use cache to avoid redundant decode_internal_key() calls.

        // Check if this is the same key we just processed
        let user_key_bytes = if let Some(ref cached_ikey) = self.last_internal_key {
            if cached_ikey.as_ref() == key {
                // Cache hit! Reuse the cached user key if present, otherwise decode again
                if let Some(ref cached_user) = self.last_user_key_cache {
                    cached_user.clone()
                } else if let Some((user, _s, _t)) =
                    crate::common::internal_key::decode_internal_key(key)
                {
                    // Populate the cache and return the decoded user key
                    let user_bytes = Bytes::copy_from_slice(&user);
                    self.last_user_key_cache = Some(user_bytes.clone());
                    user_bytes
                } else {
                    // Fall back to copying the raw key
                    Bytes::copy_from_slice(key)
                }
            } else {
                // Different key - decode and update cache
                if let Some((user, _s, _t)) = crate::common::internal_key::decode_internal_key(key)
                {
                    let user_bytes = Bytes::copy_from_slice(&user);
                    self.last_internal_key = Some(Bytes::copy_from_slice(key));
                    self.last_user_key_cache = Some(user_bytes.clone());
                    user_bytes
                } else {
                    // Key is plain user key, not internal format
                    self.last_internal_key = None;
                    self.last_user_key_cache = None;
                    Bytes::copy_from_slice(key)
                }
            }
        } else {
            // First call or cache miss - decode and populate cache
            if let Some((user, _s, _t)) = crate::common::internal_key::decode_internal_key(key) {
                let user_bytes = Bytes::copy_from_slice(&user);
                self.last_internal_key = Some(Bytes::copy_from_slice(key));
                self.last_user_key_cache = Some(user_bytes.clone());
                user_bytes
            } else {
                // Plain key
                Bytes::copy_from_slice(key)
            }
        };

        // Now do the actual work
        if self.last_internal_key.is_some() {
            // Key was already encoded as internal key
            self.cur_block
                .add_with_meta(key, value, seq, tombstone, true, expiration)?;
            self.last_key_in_block = Some(Bytes::copy_from_slice(key));
            self.bloom_builder.add_key(&user_key_bytes);
        } else {
            // Plain user key - encode it
            let ik = crate::common::internal_key::encode_internal_key(key, seq, tombstone);
            self.cur_block
                .add_with_meta(&ik, value, seq, tombstone, true, expiration)?;
            self.last_key_in_block = Some(Bytes::copy_from_slice(&ik));
            self.bloom_builder.add_key(key);
        }
        Ok(())
    }

    fn add_entry_plain_key(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        tombstone: bool,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        self.cur_block
            .add_with_meta(key, value, seq, tombstone, false, expiration)?;
        self.last_key_in_block = Some(Bytes::copy_from_slice(key));
        self.bloom_builder.add_key(key);
        Ok(())
    }

    pub fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) {
        self.range_tombstones.push(RangeTombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            seq,
        });
    }
}

/// Build the final SST image from collected blocks and metadata.
pub struct SstImageBuilder {
    blocks: Vec<(Bytes, Bytes)>,
    state: WriterState,
}

impl SstImageBuilder {
    pub fn new(blocks: Vec<(Bytes, Bytes)>, state: WriterState) -> Self {
        Self { blocks, state }
    }

    /// Build the complete SST image as bytes.
    pub fn build(mut self) -> MidgeResult<Vec<u8>> {
        let mut buf = BytesMut::new();
        let mut current_offset = 0u64;

        // Write data blocks and populate index
        for (last_key, encoded_block) in &self.blocks {
            let handle = BlockHandle {
                offset: current_offset,
                size: encoded_block.len() as u64,
            };

            // Extract user key from internal key if using internal key format
            // Sparse index should store user keys only for correct comparison
            if self.state.config.use_internal_keys {
                if let Some((user_key, _, _)) =
                    crate::common::internal_key::decode_internal_key(last_key)
                {
                    self.state.index.add_index_entry(&user_key, handle)?;
                } else {
                    self.state.index.add_index_entry(last_key, handle)?;
                }
            } else {
                self.state.index.add_index_entry(last_key, handle)?;
            }
            buf.put_slice(encoded_block);
            current_offset += encoded_block.len() as u64;
        }

        // Build and write bloom filter if we have keys
        let mut bloom_handle = BlockHandle { offset: 0, size: 0 };

        if !self.state.bloom_builder.is_empty() {
            let bloom_filter = self.state.bloom_builder.finish();
            let bloom_data = bloom_filter.encode();
            let bloom_block = Block::new(bloom_data, BlockType::Filter, CompressionType::None);
            let bloom_encoded = bloom_block.encode()?;

            bloom_handle.offset = current_offset;
            bloom_handle.size = bloom_encoded.len() as u64;
            buf.put_slice(&bloom_encoded);
            current_offset += bloom_encoded.len() as u64;
        }

        // Build and write range tombstones if present
        let mut tombstone_handle = BlockHandle { offset: 0, size: 0 };

        if !self.state.range_tombstones.is_empty() {
            let tomb_data =
                crate::sst::range_tombstone::encode_range_tombstones(&self.state.range_tombstones)?;
            let tomb_block = Block::new(tomb_data, BlockType::Filter, CompressionType::None);
            let tomb_encoded = tomb_block.encode()?;

            tombstone_handle.offset = current_offset;
            tombstone_handle.size = tomb_encoded.len() as u64;
            buf.put_slice(&tomb_encoded);
            current_offset += tomb_encoded.len() as u64;
        }

        // Build meta index
        let mut meta_builder = DataBlockBuilder::new(1);
        if bloom_handle.size > 0 {
            meta_builder.add(b"filter.bloom", &bloom_handle.encode())?;
        }
        if self.state.config.use_internal_keys {
            meta_builder.add(b"format.internal_keys", b"1")?;
        }
        if tombstone_handle.size > 0 {
            meta_builder.add(b"tombstones.range", &tombstone_handle.encode())?;
        }

        // Write meta index block
        let meta_index_data = meta_builder.finish();
        let meta_index_block =
            Block::new(meta_index_data, BlockType::MetaIndex, CompressionType::None);
        let meta_index_encoded = meta_index_block.encode()?;

        let meta_index_handle = BlockHandle {
            offset: current_offset,
            size: meta_index_encoded.len() as u64,
        };
        buf.put_slice(&meta_index_encoded);
        current_offset += meta_index_encoded.len() as u64;

        // Build and write sparse index block
        let index_data = self.state.index.finish();
        let index_block = Block::new(index_data, BlockType::Index, CompressionType::None);
        let index_encoded = index_block.encode()?;

        let index_handle = BlockHandle {
            offset: current_offset,
            size: index_encoded.len() as u64,
        };
        buf.put_slice(&index_encoded);

        // Write footer
        let footer = Footer::new(index_handle, meta_index_handle);
        let footer_bytes = footer.encode();
        buf.put_slice(&footer_bytes);

        Ok(buf.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_default_config() {
        // Arrange & Act
        let config = WriterConfig::new(4096, CompressionType::None);

        // Assert
        assert_eq!(config.block_size, 4096);
        assert_eq!(config.bloom_bits_per_key, 10);
        assert!(!config.use_internal_keys);
    }

    #[test]
    fn should_create_config_with_internal_keys() {
        // Arrange
        let base_config = WriterConfig::new(4096, CompressionType::None);

        // Act
        let config = base_config.with_internal_keys(true);

        // Assert
        assert!(config.use_internal_keys);
    }

    #[test]
    fn should_create_config_with_bloom_bits() {
        // Arrange
        let base_config = WriterConfig::new(4096, CompressionType::None);

        // Act
        let config = base_config.with_bloom_bits(15);

        // Assert
        assert_eq!(config.bloom_bits_per_key, 15);
    }

    #[test]
    fn should_create_writer_state() {
        // Arrange
        let config = WriterConfig::new(4096, CompressionType::None);

        // Act
        let state = WriterState::new(config);

        // Assert
        assert!(state.bloom_builder.is_empty());
        assert!(state.range_tombstones.is_empty());
    }

    #[test]
    fn should_not_flush_empty_block() {
        // Arrange
        let config = WriterConfig::new(4096, CompressionType::None);
        let mut state = WriterState::new(config);

        // Act
        let result = state.flush_current_block();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_detect_block_flush_needed() {
        // Arrange
        let config = WriterConfig::new(100, CompressionType::None);
        let mut state = WriterState::new(config);
        state
            .add_entry(b"key1", Some(b"value1"), 0, false, None)
            .unwrap();

        // Act - Add another entry that would exceed block size
        let should_flush = state.should_flush_block(b"key2", Some(&[0u8; 200]));

        // Assert
        assert!(should_flush);
    }

    #[test]
    fn should_add_entry_with_plain_key() {
        // Arrange
        let config = WriterConfig::new(4096, CompressionType::None);
        let mut state = WriterState::new(config);

        // Act
        let result = state.add_entry(b"test_key", Some(b"test_value"), 0, false, None);

        // Assert
        assert!(result.is_ok());
        assert_eq!(state.bloom_builder.keys_count(), 1);
    }

    #[test]
    fn should_add_entry_with_internal_key() {
        // Arrange
        let config = WriterConfig::new(4096, CompressionType::None).with_internal_keys(true);
        let mut state = WriterState::new(config);

        // Act
        let result = state.add_entry(b"test_key", Some(b"test_value"), 42, false, None);

        // Assert
        assert!(result.is_ok());
        assert_eq!(state.bloom_builder.keys_count(), 1);
    }

    #[test]
    fn should_add_range_tombstone() {
        // Arrange
        let config = WriterConfig::new(4096, CompressionType::None);
        let mut state = WriterState::new(config);

        // Act
        state.add_range_tombstone(b"start", b"end", 100);

        // Assert
        assert_eq!(state.range_tombstones.len(), 1);
        assert_eq!(state.range_tombstones[0].start, b"start");
        assert_eq!(state.range_tombstones[0].end, b"end");
        assert_eq!(state.range_tombstones[0].seq, 100);
    }

    #[test]
    fn should_flush_block_with_data() {
        // Arrange
        let config = WriterConfig::new(4096, CompressionType::None);
        let mut state = WriterState::new(config);
        state
            .add_entry(b"key", Some(b"value"), 0, false, None)
            .unwrap();

        // Act
        let result = state.flush_current_block();

        // Assert
        assert!(result.is_some());
        let (last_key, encoded) = result.unwrap();
        assert_eq!(last_key.as_ref(), b"key");
        assert!(!encoded.is_empty());
    }
}
