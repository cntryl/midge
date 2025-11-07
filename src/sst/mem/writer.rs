//! In-memory SST writer implementation.

use crate::common::codec::CompressionType;
use crate::error::MidgeResult;
use crate::sst::writer_common::{SstImageBuilder, WriterConfig, WriterState};
use bytes::Bytes;

use super::reader::SstMemReader;

/// In-memory SST writer that builds data blocks and index, returns a reader
pub struct SstMemWriter {
    state: WriterState,
    blocks: Vec<(Bytes, Bytes)>, // (last_key, encoded block)
}

impl SstMemWriter {
    pub fn new(compression: CompressionType, block_size: usize) -> Self {
        let config = WriterConfig::new(block_size, compression);
        // Pre-allocate for ~100 blocks (reasonable default, avoids most reallocations)
        let blocks = Vec::with_capacity(100);
        Self {
            state: WriterState::new(config),
            blocks,
        }
    }

    pub fn new_with_internal(
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
    ) -> Self {
        let config = WriterConfig::new(block_size, compression).with_internal_keys(use_internal);
        let blocks = Vec::with_capacity(100);
        Self {
            state: WriterState::new(config),
            blocks,
        }
    }

    pub fn new_with_bloom(
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
        bloom_bits_per_key: u32,
    ) -> Self {
        let config = WriterConfig::new(block_size, compression)
            .with_internal_keys(use_internal)
            .with_bloom_bits(bloom_bits_per_key);
        let blocks = Vec::with_capacity(100);
        Self {
            state: WriterState::new(config),
            blocks,
        }
    }

    /// Create writer with expected size hint for better memory allocation.
    pub fn with_expected_size(
        compression: CompressionType,
        block_size: usize,
        expected_bytes: usize,
    ) -> Self {
        let config = WriterConfig::new(block_size, compression);
        // Estimate number of blocks needed
        let estimated_blocks = (expected_bytes / block_size).max(1) + 1;
        let blocks = Vec::with_capacity(estimated_blocks);
        Self {
            state: WriterState::new(config),
            blocks,
        }
    }

    fn flush_block_if_needed(&mut self) -> Option<(Bytes, Bytes)> {
        self.state.flush_current_block()
    }

    /// Add a range tombstone to this SST.
    pub fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) {
        self.state.add_range_tombstone(start, end, seq);
    }
}

impl crate::sst::SstWriter for SstMemWriter {
    type Reader = SstMemReader;

    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        // Delegate to add_with_meta which handles internal-key layout when enabled
        self.add_with_meta(key, Some(value), 0, false, None)
    }

    fn finish(self) -> MidgeResult<Self::Reader> {
        let raw = SstMemWriter::finish_bytes(self)?;
        let reader = SstMemReader::from_bytes(raw)?;
        Ok(reader)
    }
}

impl SstMemWriter {
    // Convenience inherent methods to ease usage in tests/examples without importing the trait
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        <Self as crate::sst::SstWriter>::add(self, key, value)
    }

    pub fn finish(self) -> MidgeResult<SstMemReader> {
        <Self as crate::sst::SstWriter>::finish(self)
    }

    /// Add with explicit metadata for tests or advanced usage.
    pub fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        tombstone: bool,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        if self.state.should_flush_block(key, value) {
            if let Some((last_key, encoded)) = self.flush_block_if_needed() {
                self.blocks.push((last_key, encoded));
            }
        }

        self.state
            .add_entry(key, value, seq, tombstone, expiration)?;
        Ok(())
    }

    /// Produce the raw SST image as bytes (without writing to disk).
    pub fn finish_bytes(mut self) -> MidgeResult<Vec<u8>> {
        if let Some((last_key, encoded)) = self.flush_block_if_needed() {
            self.blocks.push((last_key, encoded));
        }

        let builder = SstImageBuilder::new(self.blocks, self.state);
        builder.build()
    }
}
