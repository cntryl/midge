//! Block value types for the block cache.
//!
//! `BlockData` holds the actual block payload along with metadata used for
//! accounting and compression awareness.

use std::sync::Arc;

use super::key::BlockKind;

/// The cached block payload and associated metadata.
///
/// Stored behind an `Arc` so multiple `BlockHandle`s can share the same data
/// without copying. The cache accounts memory usage based on `charge_bytes()`.
#[derive(Debug, Clone)]
pub struct BlockData {
    /// The raw block bytes (may be compressed or uncompressed depending on config).
    bytes: Arc<[u8]>,
    /// Size of the block when uncompressed (used for memory accounting).
    uncompressed_size: u32,
    /// Size of the block in its compressed form (0 if not compressed).
    compressed_size: u32,
    /// Whether `bytes` is currently in compressed form.
    compressed: bool,
    /// The type of block (data, index, filter, etc.).
    block_kind: BlockKind,
}

impl BlockData {
    /// Create a new `BlockData` for an uncompressed block.
    #[inline]
    pub fn uncompressed(bytes: Arc<[u8]>, block_kind: BlockKind) -> Self {
        let size = bytes.len() as u32;
        Self {
            bytes,
            uncompressed_size: size,
            compressed_size: 0,
            compressed: false,
            block_kind,
        }
    }

    /// Create a new `BlockData` for a compressed block.
    ///
    /// `uncompressed_size` should be the size after decompression (for accounting).
    #[inline]
    pub fn compressed(bytes: Arc<[u8]>, uncompressed_size: u32, block_kind: BlockKind) -> Self {
        let compressed_size = bytes.len() as u32;
        Self {
            bytes,
            uncompressed_size,
            compressed_size,
            compressed: true,
            block_kind,
        }
    }

    /// Access the underlying bytes.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Clone the `Arc<[u8]>` for shared ownership.
    #[inline]
    pub fn bytes_arc(&self) -> Arc<[u8]> {
        Arc::clone(&self.bytes)
    }

    /// Returns `true` if the stored bytes are compressed.
    #[inline]
    pub fn is_compressed(&self) -> bool {
        self.compressed
    }

    /// The uncompressed size of the block (used for memory accounting).
    #[inline]
    pub fn uncompressed_size(&self) -> u32 {
        self.uncompressed_size
    }

    /// The compressed size of the block (0 if not compressed).
    #[inline]
    pub fn compressed_size(&self) -> u32 {
        self.compressed_size
    }

    /// The block kind.
    #[inline]
    pub fn block_kind(&self) -> BlockKind {
        self.block_kind
    }

    /// The number of bytes to charge against cache capacity.
    ///
    /// By default we charge uncompressed size to reflect actual memory pressure
    /// when the block is used. Override this policy via `BlockCacheOptions` if
    /// you want to charge compressed size instead.
    #[inline]
    pub fn charge_bytes(&self) -> usize {
        self.uncompressed_size as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_report_correct_size_given_uncompressed_block_when_queried() {
        // Arrange
        let data: Arc<[u8]> = vec![0u8; 4096].into();

        // Act
        let block = BlockData::uncompressed(data, BlockKind::Data);

        // Assert
        assert!(!block.is_compressed());
        assert_eq!(block.uncompressed_size(), 4096);
        assert_eq!(block.compressed_size(), 0);
        assert_eq!(block.charge_bytes(), 4096);
    }

    #[test]
    fn should_report_correct_sizes_given_compressed_block_when_queried() {
        // Arrange
        let data: Arc<[u8]> = vec![0u8; 1024].into(); // compressed payload

        // Act
        let block = BlockData::compressed(data, 4096, BlockKind::Data);

        // Assert
        assert!(block.is_compressed());
        assert_eq!(block.uncompressed_size(), 4096);
        assert_eq!(block.compressed_size(), 1024);
        assert_eq!(block.charge_bytes(), 4096); // charges uncompressed
    }

    #[test]
    fn should_share_bytes_given_clone_when_bytes_arc_called() {
        // Arrange
        let data: Arc<[u8]> = vec![1, 2, 3].into();
        let block = BlockData::uncompressed(Arc::clone(&data), BlockKind::Index);

        // Act
        let cloned = block.bytes_arc();

        // Assert
        assert!(Arc::ptr_eq(&data, &cloned));
    }
}
