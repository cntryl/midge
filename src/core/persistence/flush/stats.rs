//! Flush statistics and metrics calculation helpers.
//!
//! Extracts metrics computation from the core flush path to reduce noise
//! and improve testability.

use crate::core::EntryMeta;

/// Statistics computed from flush job entries.
///
/// Captures all metrics needed for:
/// - Throughput tracking
/// - Tombstone counts
/// - FileMeta population
#[derive(Debug, Clone, Default)]
pub struct FlushStats {
    /// Total bytes flushed (keys + values)
    pub total_bytes: u64,
    /// Number of point tombstones (delete markers)
    pub point_tombstone_count: u64,
    /// Number of range tombstones
    pub range_tombstone_count: u64,
    /// Total number of entries (including tombstones)
    pub total_entries: u64,
}

impl FlushStats {
    /// Compute statistics from entries and range tombstones.
    ///
    /// This is intentionally separated from the flush path to:
    /// - Keep correctness logic clean
    /// - Enable isolated testing
    /// - Allow future optimizations (SIMD, parallel)
    #[inline]
    pub fn compute(entries: &[EntryMeta], range_tombstones: &[(Vec<u8>, Vec<u8>, u64)]) -> Self {
        // Calculate total bytes for throughput tracking
        let entry_bytes: u64 = entries
            .iter()
            .map(|e| (e.key.len() + e.value.as_ref().map_or(0, |v| v.len())) as u64)
            .sum();

        let tombstone_bytes: u64 = range_tombstones
            .iter()
            .map(|(s, e, _)| (s.len() + e.len()) as u64)
            .sum();

        let point_tombstone_count = entries.iter().filter(|e| e.is_tombstone).count() as u64;

        Self {
            total_bytes: entry_bytes + tombstone_bytes,
            point_tombstone_count,
            range_tombstone_count: range_tombstones.len() as u64,
            total_entries: entries.len() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::skiplist::OpType;

    fn make_entry(key: &[u8], value: Option<&[u8]>, is_tombstone: bool) -> EntryMeta {
        EntryMeta {
            key: key.to_vec(),
            value: value.map(|v| v.to_vec()),
            sequence: 1,
            is_tombstone,
            expiration_millis: None,
            op_type: if is_tombstone {
                OpType::Delete
            } else {
                OpType::Put
            },
        }
    }

    #[test]
    fn should_compute_bytes_from_entries() {
        // Arrange
        let entries = vec![
            make_entry(b"key1", Some(b"value1"), false), // 4 + 6 = 10
            make_entry(b"key2", Some(b"val"), false),    // 4 + 3 = 7
        ];

        // Act
        let stats = FlushStats::compute(&entries, &[]);

        // Assert
        assert_eq!(stats.total_bytes, 17);
        assert_eq!(stats.total_entries, 2);
    }

    #[test]
    fn should_count_point_tombstones() {
        // Arrange
        let entries = vec![
            make_entry(b"key1", Some(b"value1"), false),
            make_entry(b"key2", None, true), // tombstone
            make_entry(b"key3", None, true), // tombstone
        ];

        // Act
        let stats = FlushStats::compute(&entries, &[]);

        // Assert
        assert_eq!(stats.point_tombstone_count, 2);
        assert_eq!(stats.total_entries, 3);
    }

    #[test]
    fn should_count_range_tombstones() {
        // Arrange
        let range_tombstones = vec![
            (b"a".to_vec(), b"m".to_vec(), 1), // 1 + 1 = 2 bytes
            (b"n".to_vec(), b"z".to_vec(), 2), // 1 + 1 = 2 bytes
        ];

        // Act
        let stats = FlushStats::compute(&[], &range_tombstones);

        // Assert
        assert_eq!(stats.range_tombstone_count, 2);
        assert_eq!(stats.total_bytes, 4);
    }

    #[test]
    fn should_handle_empty_inputs() {
        // Act
        let stats = FlushStats::compute(&[], &[]);

        // Assert
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.point_tombstone_count, 0);
        assert_eq!(stats.range_tombstone_count, 0);
        assert_eq!(stats.total_entries, 0);
    }

    #[test]
    fn should_compute_mixed_stats() {
        // Arrange
        let entries = vec![
            make_entry(b"key1", Some(b"value1"), false), // 4 + 6 = 10
            make_entry(b"del", None, true),              // 3 + 0 = 3
        ];
        let range_tombstones = vec![
            (b"start".to_vec(), b"end".to_vec(), 1), // 5 + 3 = 8
        ];

        // Act
        let stats = FlushStats::compute(&entries, &range_tombstones);

        // Assert
        assert_eq!(stats.total_bytes, 21); // 10 + 3 + 8
        assert_eq!(stats.point_tombstone_count, 1);
        assert_eq!(stats.range_tombstone_count, 1);
        assert_eq!(stats.total_entries, 2);
    }
}
