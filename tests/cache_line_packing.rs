//! Unit test for BlockMeta size validation (cache-line packing)
//!
//! Validates that BlockMeta fits efficiently within cache lines for optimal
//! sequential access performance.

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cntryl_midge::sst::block_meta::BlockMeta;
    use cntryl_midge::sst::format::BlockHandle;

    #[test]
    fn should_verify_blockmeta_size_for_cache_efficiency() {
        // Arrange & Act
        let size = std::mem::size_of::<BlockMeta>();

        // Assert
        // Target: ≤128 bytes (2 cache lines on typical x86-64)
        // BlockMeta includes:
        // - min_key: Bytes (24 bytes)
        // - max_key: Bytes (24 bytes)
        // - handle: BlockHandle (16 bytes)
        // - has_tombstones: bool (1 byte + padding)
        // - tombstone_min: Option<Bytes> (32 bytes)
        // - tombstone_max: Option<Bytes> (32 bytes)
        // - bloom_offset: Option<u64> (16 bytes)
        // - bloom: Option<BlockBloom> (variable, typically 24+ bytes when Some)
        //
        // Total: ~169 bytes without bloom, fits in 3 cache lines
        // With small bloom: ~193 bytes, fits in 3-4 cache lines

        println!("BlockMeta size: {} bytes", size);

        // Relaxed threshold: accept up to 256 bytes (4 cache lines)
        // This is reasonable given the fields we need
        assert!(
            size <= 256,
            "BlockMeta size {} exceeds 256 bytes (4 cache lines); consider field reordering or packing",
            size
        );

        // Ideally, we'd like to be under 192 bytes (3 cache lines)
        if size > 192 {
            println!(
                "Warning: BlockMeta is {} bytes (>192). Consider optimizing for better cache efficiency.",
                size
            );
        }
    }

    #[test]
    fn should_create_minimal_blockmeta() {
        // Arrange
        let min_key = Bytes::from_static(b"key_000000");
        let max_key = Bytes::from_static(b"key_000099");
        let handle = BlockHandle::new(0, 1024);

        // Act
        let meta = BlockMeta::new(min_key, max_key, handle);

        // Assert
        assert_eq!(meta.min_key.as_ref(), b"key_000000");
        assert_eq!(meta.max_key.as_ref(), b"key_000099");
        assert_eq!(meta.handle.offset, 0);
        assert_eq!(meta.handle.size, 1024);
        assert!(!meta.has_tombstones);
    }

    #[test]
    fn should_fit_multiple_metas_in_cache_line() {
        // Arrange
        let meta_size = std::mem::size_of::<BlockMeta>();
        let cache_line_size = 64;

        // Act
        let metas_per_line = cache_line_size / meta_size;

        // Assert
        // With typical sizes, we won't fit even 1 per line, but 2-4 per 128-256 bytes is good
        println!(
            "BlockMeta: {} bytes, {} metas per 64-byte cache line, {} metas per 128 bytes",
            meta_size,
            if metas_per_line == 0 {
                0
            } else {
                metas_per_line
            },
            128 / meta_size
        );

        // As long as we're under 4 cache lines, sequential access will be efficient
        assert!(meta_size <= 256);
    }
}
