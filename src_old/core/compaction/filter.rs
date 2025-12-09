use super::executor::CompactionVersion;
use bytes::Bytes;

/// Decision returned by a compaction filter when evaluating a key-value pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterDecision {
    /// Keep the key-value pair in the compacted output
    Keep,
    /// Drop the key-value pair (remove from output)
    Remove,
    /// Change the value to a tombstone (effectively delete)
    RemoveAndTombstone,
}

/// Trait for filtering keys during compaction.
///
/// Compaction filters allow users to drop or modify keys during compaction,
/// useful for implementing TTL expiration, garbage collection of obsolete data,
/// or other application-specific cleanup logic.
pub trait CompactionFilter: Send + Sync {
    /// Called for each key-value version during compaction.
    ///
    /// # Arguments
    /// * `level` - The target output level for this compaction
    /// * `version` - The complete version including key, value, sequence, tombstone, and expiration
    ///
    /// # Returns
    /// A `FilterDecision` indicating what to do with this key-value pair.
    fn filter(&self, level: u32, version: &CompactionVersion) -> FilterDecision;

    /// Optional: Called at the start of compaction to allow initialization.
    fn start_compaction(&mut self, _source_level: u32, _target_level: u32) {}

    /// Optional: Called at the end of compaction to allow cleanup.
    fn finish_compaction(&mut self) {}
}

/// A no-op filter that keeps all keys.
#[derive(Debug, Clone, Default)]
pub struct NoOpFilter;

impl CompactionFilter for NoOpFilter {
    fn filter(&self, _level: u32, _version: &CompactionVersion) -> FilterDecision {
        FilterDecision::Keep
    }
}

/// A simple TTL-based filter that drops keys older than a time threshold.
///
/// This filter assumes the key format includes a timestamp that can be extracted
/// and compared against a TTL threshold.
#[derive(Debug, Clone)]
pub struct TtlFilter {
    /// Time-to-live in seconds
    pub ttl_seconds: u64,
    /// Current time (Unix timestamp)
    pub now_seconds: u64,
    /// Function to extract timestamp from key
    extract_timestamp: fn(&[u8]) -> Option<u64>,
}

impl TtlFilter {
    pub fn new(
        ttl_seconds: u64,
        now_seconds: u64,
        extract_timestamp: fn(&[u8]) -> Option<u64>,
    ) -> Self {
        Self {
            ttl_seconds,
            now_seconds,
            extract_timestamp,
        }
    }
}

impl CompactionFilter for TtlFilter {
    fn filter(&self, _level: u32, version: &CompactionVersion) -> FilterDecision {
        // Don't filter tombstones
        if version.tombstone {
            return FilterDecision::Keep;
        }

        // Check expiration metadata first (preferred method)
        if let Some(exp_millis) = version.expiration {
            let now_millis = self.now_seconds * 1000;
            if now_millis > exp_millis {
                // Key has expired, remove it
                return FilterDecision::Remove;
            }
            return FilterDecision::Keep;
        }

        // Fallback: Extract timestamp from key if expiration not set
        if let Some(timestamp) = (self.extract_timestamp)(&version.user_key) {
            let age = self.now_seconds.saturating_sub(timestamp);
            if age > self.ttl_seconds {
                // Key has expired, remove it
                return FilterDecision::Remove;
            }
        }

        FilterDecision::Keep
    }
}

/// A filter that drops keys matching a specific prefix.
#[derive(Debug, Clone)]
pub struct PrefixDropFilter {
    /// Prefix to match for dropping
    pub drop_prefix: Bytes,
}

impl PrefixDropFilter {
    pub fn new(drop_prefix: Bytes) -> Self {
        Self { drop_prefix }
    }
}

impl CompactionFilter for PrefixDropFilter {
    fn filter(&self, _level: u32, version: &CompactionVersion) -> FilterDecision {
        if version.user_key.starts_with(&self.drop_prefix) {
            FilterDecision::Remove
        } else {
            FilterDecision::Keep
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // Helper function to create a CompactionVersion for testing
    fn make_version(
        key: &[u8],
        seq: u64,
        value: Option<&[u8]>,
        expiration: Option<u64>,
    ) -> CompactionVersion {
        CompactionVersion {
            user_key: key.to_vec(),
            seq,
            tombstone: value.is_none(),
            value: value.map(Bytes::copy_from_slice),
            expiration,
            op_type: if value.is_none() { 2 } else { 0 }, // Delete or Put
        }
    }

    #[test]
    fn should_keep_all_versions_given_noop_filter() {
        // Arrange
        let filter = NoOpFilter;
        let version = make_version(b"key", 1, Some(b"value"), None);
        let tombstone = make_version(b"key", 2, None, None);

        // Act
        let version_decision = filter.filter(0, &version);
        let tombstone_decision = filter.filter(0, &tombstone);

        // Assert
        assert_eq!(version_decision, FilterDecision::Keep);
        assert_eq!(tombstone_decision, FilterDecision::Keep);
    }

    #[test]
    fn should_remove_expired_entries_given_ttl_filter_and_key_timestamp() {
        // Arrange
        fn extract_timestamp(key: &[u8]) -> Option<u64> {
            // Assume key format: "key:{timestamp}"
            let s = std::str::from_utf8(key).ok()?;
            let parts: Vec<&str> = s.split(':').collect();
            parts.get(1)?.parse().ok()
        }

        let filter = TtlFilter::new(100, 1000, extract_timestamp);
        let recent = make_version(b"key:950", 1, Some(b"value"), None);
        let expired = make_version(b"key:800", 1, Some(b"value"), None);
        let tombstone = make_version(b"key:800", 1, None, None);

        // Act
        let recent_decision = filter.filter(0, &recent);
        let expired_decision = filter.filter(0, &expired);
        let tombstone_decision = filter.filter(0, &tombstone);

        // Assert
        assert_eq!(recent_decision, FilterDecision::Keep);
        assert_eq!(expired_decision, FilterDecision::Remove);
        assert_eq!(tombstone_decision, FilterDecision::Keep);
    }

    #[test]
    fn should_use_expiration_metadata_when_no_key_timestamp_extractor() {
        // Arrange
        fn no_extract(_key: &[u8]) -> Option<u64> {
            None
        }

        let filter = TtlFilter::new(100, 1000, no_extract);
        let expired = make_version(b"key1", 1, Some(b"value"), Some(900_000));
        let not_expired = make_version(b"key2", 1, Some(b"value"), Some(1_100_000));

        // Act
        let expired_decision = filter.filter(0, &expired);
        let not_expired_decision = filter.filter(0, &not_expired);

        // Assert
        assert_eq!(expired_decision, FilterDecision::Remove);
        assert_eq!(not_expired_decision, FilterDecision::Keep);
    }

    #[test]
    fn should_remove_entries_with_prefix_given_prefix_drop_filter() {
        // Arrange
        let filter = PrefixDropFilter::new(Bytes::from("temp:"));
        let temp_key = make_version(b"temp:key1", 1, Some(b"value"), None);
        let perm_key = make_version(b"permanent:key1", 1, Some(b"value"), None);

        // Act
        let temp_decision = filter.filter(0, &temp_key);
        let perm_decision = filter.filter(0, &perm_key);

        // Assert
        assert_eq!(temp_decision, FilterDecision::Remove);
        assert_eq!(perm_decision, FilterDecision::Keep);
    }
}
