//! Range filter for efficient range scan optimization
//!
//! Stores min/max keys to enable zero-copy rejection of ranges that don't overlap.
//! Useful for time-range queries and temporal locality workloads.
//!
//! Used alongside block blooms for complete SST/block filtering.

use crate::common::MidgeResult;

/// Range filter for efficient range query optimization
///
/// Stores min/max keys to enable quick elimination of SSTs/blocks
/// that don't overlap with query ranges.
#[derive(Debug, Clone)]
pub struct RangeFilter {
    /// Minimum key (inclusive)
    min_key: Vec<u8>,
    /// Maximum key (inclusive)
    max_key: Vec<u8>,
}

impl RangeFilter {
    /// Create a range filter from min and max keys
    pub fn new(min_key: Vec<u8>, max_key: Vec<u8>) -> MidgeResult<Self> {
        if min_key > max_key {
            return Err(crate::common::MidgeError::InvalidArgument(
                "min_key must be <= max_key".to_string(),
            ));
        }

        Ok(Self { min_key, max_key })
    }

    /// Get the minimum key
    pub fn min_key(&self) -> &[u8] {
        &self.min_key
    }

    /// Get the maximum key
    pub fn max_key(&self) -> &[u8] {
        &self.max_key
    }

    /// Check if a query range overlaps with this filter's range
    ///
    /// Ranges [a, b] and [c, d] overlap if NOT (b < c OR d < a)
    pub fn overlaps_with(&self, query_min: &[u8], query_max: &[u8]) -> bool {
        !(self.max_key.as_slice() < query_min || query_max < self.min_key.as_slice())
    }

    /// Check if a point key is within the filter's range
    pub fn contains_point(&self, key: &[u8]) -> bool {
        key >= self.min_key.as_slice() && key <= self.max_key.as_slice()
    }

    /// Serialize the range filter
    ///
    /// Format: [min_len: u32][min_key...][max_len: u32][max_key...]
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&(self.min_key.len() as u32).to_le_bytes());
        result.extend_from_slice(&self.min_key);
        result.extend_from_slice(&(self.max_key.len() as u32).to_le_bytes());
        result.extend_from_slice(&self.max_key);
        result
    }

    /// Deserialize a range filter
    pub fn deserialize(data: &[u8]) -> MidgeResult<Self> {
        if data.len() < 8 {
            return Err(crate::common::MidgeError::Corruption(
                "Range filter data too short".to_string(),
            ));
        }

        let mut offset = 0;

        // Read min_key length
        let min_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + min_len > data.len() {
            return Err(crate::common::MidgeError::Corruption(
                "Range filter: min_key truncated".to_string(),
            ));
        }

        let min_key = data[offset..offset + min_len].to_vec();
        offset += min_len;

        // Read max_key length
        if offset + 4 > data.len() {
            return Err(crate::common::MidgeError::Corruption(
                "Range filter: max_key length truncated".to_string(),
            ));
        }

        let max_len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + max_len > data.len() {
            return Err(crate::common::MidgeError::Corruption(
                "Range filter: max_key truncated".to_string(),
            ));
        }

        let max_key = data[offset..offset + max_len].to_vec();

        Self::new(min_key, max_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_range_filter() {
        // Arrange, Act
        let filter = RangeFilter::new(b"key1".to_vec(), b"key9".to_vec());

        // Assert
        assert!(filter.is_ok());
        let filter = filter.unwrap();
        assert_eq!(filter.min_key(), b"key1");
        assert_eq!(filter.max_key(), b"key9");
    }

    #[test]
    fn should_reject_invalid_range() {
        // Arrange, Act
        let result = RangeFilter::new(b"key9".to_vec(), b"key1".to_vec());

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_allow_equal_min_max() {
        // Arrange, Act
        let result = RangeFilter::new(b"key5".to_vec(), b"key5".to_vec());

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_detect_range_overlap() {
        // Arrange
        let filter = RangeFilter::new(b"key10".to_vec(), b"key20".to_vec()).unwrap();

        // Debug: Check actual byte comparisons
        // key10 = [107, 101, 121, 49, 48]
        // key15 = [107, 101, 121, 49, 53]
        // key20 = [107, 101, 121, 50, 48]
        // So lexicographically: key10 < key15 < key20

        // Assert - various overlap cases
        assert!(filter.overlaps_with(b"key15", b"key25")); // Partial overlap: key15..key25 overlaps key10..key20
        assert!(filter.overlaps_with(b"key05", b"key15")); // Partial overlap: key05..key15 overlaps key10..key20
        assert!(filter.overlaps_with(b"key10", b"key20")); // Exact match
        assert!(filter.overlaps_with(b"key12", b"key18")); // Contained: key12..key18 inside key10..key20
        assert!(!filter.overlaps_with(b"key01", b"key09")); // Before: key01..key09 does not overlap key10..key20
        assert!(!filter.overlaps_with(b"key21", b"key30")); // After: key21..key30 does not overlap key10..key20
    }

    #[test]
    fn should_test_point_containment() {
        // Arrange
        let filter = RangeFilter::new(b"key10".to_vec(), b"key20".to_vec()).unwrap();

        // Assert
        assert!(filter.contains_point(b"key10")); // Min
        assert!(filter.contains_point(b"key20")); // Max
        assert!(filter.contains_point(b"key15")); // Inside
        assert!(!filter.contains_point(b"key9")); // Before
        assert!(!filter.contains_point(b"key21")); // After
    }

    #[test]
    fn should_round_trip_serialization() {
        // Arrange - use keys where min < max lexicographically
        let filter =
            RangeFilter::new(b"aaa_start".to_vec(), b"zzz_end_with_longer_name".to_vec()).unwrap();

        // Act
        let serialized = filter.serialize();
        let deserialized = RangeFilter::deserialize(&serialized);

        // Assert
        assert!(deserialized.is_ok());
        let deserialized = deserialized.unwrap();
        assert_eq!(deserialized.min_key(), filter.min_key());
        assert_eq!(deserialized.max_key(), filter.max_key());
    }

    #[test]
    fn should_reject_truncated_deserialization() {
        // Arrange
        let filter = RangeFilter::new(b"key1".to_vec(), b"key9".to_vec()).unwrap();
        let mut serialized = filter.serialize();
        serialized.truncate(2); // Truncate to invalid state

        // Act
        let result = RangeFilter::deserialize(&serialized);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_estimate_space_vs_bloom() {
        // Arrange - Range filter overhead vs bloom
        let filter = RangeFilter::new(b"key_0000".to_vec(), b"key_9999".to_vec()).unwrap();

        // Act
        let serialized = filter.serialize();

        // Assert
        // Range filters are tiny: just 2 u32 lengths + keys
        assert!(serialized.len() < 1024, "Range filter should be <1KB");
    }
}
