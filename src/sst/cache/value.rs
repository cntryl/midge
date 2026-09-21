//! Cached block value.

use bytes::Bytes;

/// Fixed per-entry bookkeeping charged on top of the block payload.
///
/// A cached block costs more than its payload: the shard's `DashMap` stores a
/// [`crate::sst::cache::CacheKey`] and a `Bytes` handle per entry, and the
/// eviction policy keeps its own slot plus index entry for the same key.
/// Charging a flat constant keeps the configured cache budget close to the
/// memory the cache actually holds instead of accounting for payloads alone.
///
/// The value is a deliberate under-estimate of the real per-entry cost: it is
/// better to hold slightly more than the budget suggests than to evict blocks
/// the budget could have kept.
pub const ENTRY_OVERHEAD_BYTES: usize = 64;

/// A cached block value.
///
/// This is a thin wrapper around the block payload. `Bytes` is already
/// reference counted, so cloning a `CacheValue` out of a shard does not copy
/// the block.
#[derive(Clone, Debug)]
pub struct CacheValue {
    /// The actual block data.
    pub data: Bytes,
}

impl CacheValue {
    /// Wrap block data for caching.
    #[must_use]
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    /// Bytes this entry charges against shard capacity.
    ///
    /// This is the payload length plus [`ENTRY_OVERHEAD_BYTES`], not the raw
    /// payload length.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        Self::charged_bytes(self.data.len())
    }

    /// Bytes a payload of `payload_len` would charge against shard capacity.
    #[must_use]
    pub const fn charged_bytes(payload_len: usize) -> usize {
        payload_len.saturating_add(ENTRY_OVERHEAD_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::{CacheValue, ENTRY_OVERHEAD_BYTES};
    use bytes::Bytes;

    #[test]
    fn should_charge_payload_plus_fixed_overhead_when_sizing_an_entry() {
        // Arrange
        let data = Bytes::from(&b"hello"[..]);

        // Act
        let value = CacheValue::new(data);

        // Assert
        assert_eq!(value.size_bytes(), 5 + ENTRY_OVERHEAD_BYTES);
        assert_eq!(CacheValue::charged_bytes(5), value.size_bytes());
    }

    #[test]
    fn should_charge_only_the_fixed_overhead_when_the_block_is_empty() {
        // Arrange
        let data = Bytes::new();

        // Act
        let value = CacheValue::new(data);

        // Assert
        assert_eq!(value.size_bytes(), ENTRY_OVERHEAD_BYTES);
    }

    #[test]
    fn should_saturate_charged_size_when_payload_length_is_at_the_maximum() {
        // Arrange
        let payload_len = usize::MAX;

        // Act
        let charged = CacheValue::charged_bytes(payload_len);

        // Assert
        assert_eq!(charged, usize::MAX);
    }

    #[test]
    fn should_carry_no_per_entry_metadata_beyond_the_payload_handle() {
        // Arrange
        let handle_size = std::mem::size_of::<Bytes>();

        // Act
        let value_size = std::mem::size_of::<CacheValue>();

        // Assert: no timestamp, no access counter, no extra `Arc` indirection.
        assert_eq!(value_size, handle_size);
        assert!(value_size <= 32, "CacheValue grew to {value_size} bytes");
    }

    #[test]
    fn should_share_the_payload_without_copying_when_cloned() {
        // Arrange
        let data = Bytes::from(&b"important data"[..]);
        let value = CacheValue::new(data.clone());

        // Act
        let clone = value.clone();

        // Assert
        assert_eq!(clone.data, data);
        assert_eq!(clone.data.as_ptr(), value.data.as_ptr());
        assert_eq!(clone.size_bytes(), value.size_bytes());
    }
}
