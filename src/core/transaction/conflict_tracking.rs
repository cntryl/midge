//! Conflict tracking for optimistic concurrency control.
//!
//! This module provides tracking of read and write sets for transactions,
//! enabling conflict detection during transaction commit.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};

/// Tracks read and write operations for a transaction to detect conflicts.
///
/// The tracker maintains:
/// - Read set: Keys that have been read by the transaction
/// - Write set: Keys that have been modified by the transaction  
/// - Write ranges: Ranges that have been modified by the transaction
/// - Read versions: Sequence numbers at which keys were read
///
/// This enables optimistic concurrency control by detecting conflicts at commit time.
#[derive(Debug, Clone)]
pub struct ConflictTracker {
    /// Keys that have been read (cf_id, key)
    read_set: HashSet<(u32, Bytes)>,

    /// Keys that have been written (cf_id, key)
    write_set: HashSet<(u32, Bytes)>,

    /// Ranges that have been written (cf_id, start_key, end_key)
    write_ranges: HashSet<(u32, Bytes, Bytes)>,

    /// Sequence numbers at which keys were read (cf_id, key) -> version
    read_versions: HashMap<(u32, Bytes), u64>,
}

impl ConflictTracker {
    /// Create a new conflict tracker.
    pub fn new() -> Self {
        Self {
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            write_ranges: HashSet::new(),
            read_versions: HashMap::new(),
        }
    }

    /// Track a read operation for conflict detection.
    ///
    /// Records that the transaction read the given key at the specified version.
    /// This is used to detect read-write conflicts during commit.
    pub fn track_read(&mut self, cf_id: u32, key: Bytes, version: u64) {
        self.read_set.insert((cf_id, key.clone()));
        self.read_versions.insert((cf_id, key), version);
    }

    /// Track a write operation for conflict detection.
    ///
    /// Records that the transaction modified the given key.
    /// This is used to detect write-write conflicts during commit.
    pub fn track_write(&mut self, cf_id: u32, key: Bytes) {
        self.write_set.insert((cf_id, key));
    }

    /// Track a range write operation for conflict detection.
    ///
    /// Records that the transaction modified the given key range.
    /// This is used to detect write-write conflicts during commit.
    pub fn track_write_range(&mut self, cf_id: u32, start_key: Bytes, end_key: Bytes) {
        self.write_ranges.insert((cf_id, start_key, end_key));
    }

    /// Get the write set (keys modified by this transaction).
    pub fn write_set(&self) -> &HashSet<(u32, Bytes)> {
        &self.write_set
    }

    /// Get the write ranges (ranges modified by this transaction).
    pub fn write_ranges(&self) -> &HashSet<(u32, Bytes, Bytes)> {
        &self.write_ranges
    }

    /// Get the read set (keys read by this transaction).
    pub fn read_set(&self) -> &HashSet<(u32, Bytes)> {
        &self.read_set
    }

    /// Get the read versions map (keys -> sequence numbers).
    pub fn read_versions(&self) -> &HashMap<(u32, Bytes), u64> {
        &self.read_versions
    }

    /// Get read version for a specific key.
    pub fn read_version(&self, cf_id: u32, key: &[u8]) -> Option<u64> {
        self.read_versions
            .get(&(cf_id, Bytes::copy_from_slice(key)))
            .copied()
    }

    /// Check if there's a write-write conflict with given write set.
    ///
    /// Returns true if this transaction's write set overlaps with the provided write set.
    pub fn has_write_conflict(&self, other_writes: &HashSet<(u32, Bytes)>) -> bool {
        !self.write_set.is_disjoint(other_writes)
    }

    /// Check if there's a write-write conflict with given write ranges.
    ///
    /// Returns true if this transaction's write set overlaps with the provided write ranges.
    pub fn has_write_range_conflict(&self, other_ranges: &HashSet<(u32, Bytes, Bytes)>) -> bool {
        // Check if any of our individual writes conflict with the other transaction's ranges
        for (cf, key) in &self.write_set {
            for (other_cf, start, end) in other_ranges {
                if cf == other_cf && key >= start && key < end {
                    return true;
                }
            }
        }
        false
    }

    /// Check if there's a write-write conflict between two range sets.
    ///
    /// Returns true if the ranges overlap.
    pub fn has_range_range_conflict(&self, other_ranges: &HashSet<(u32, Bytes, Bytes)>) -> bool {
        for (cf, start, end) in &self.write_ranges {
            for (other_cf, other_start, other_end) in other_ranges {
                if cf == other_cf && ranges_overlap(start, end, other_start, other_end) {
                    return true;
                }
            }
        }
        false
    }

    /// Clear all tracked operations.
    pub fn clear(&mut self) {
        self.read_set.clear();
        self.write_set.clear();
        self.write_ranges.clear();
        self.read_versions.clear();
    }

    /// Check if any operations have been tracked.
    pub fn is_empty(&self) -> bool {
        self.read_set.is_empty() && self.write_set.is_empty() && self.write_ranges.is_empty()
    }

    /// Get the number of read operations tracked.
    pub fn read_count(&self) -> usize {
        self.read_set.len()
    }

    /// Get the number of write operations tracked.
    pub fn write_count(&self) -> usize {
        self.write_set.len() + self.write_ranges.len()
    }
}

/// Check if two key ranges overlap.
/// Returns true if the ranges [start1, end1) and [start2, end2) overlap.
fn ranges_overlap(start1: &[u8], end1: &[u8], start2: &[u8], end2: &[u8]) -> bool {
    start1 < end2 && start2 < end1
}

impl Default for ConflictTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_track_read_operation() {
        // Arrange
        let mut tracker = ConflictTracker::new();

        // Act
        tracker.track_read(0, Bytes::from("key1"), 100);

        // Assert
        assert!(tracker.read_set().contains(&(0, Bytes::from("key1"))));
        assert_eq!(tracker.read_version(0, b"key1"), Some(100));
    }

    #[test]
    fn should_track_write_operation() {
        // Arrange
        let mut tracker = ConflictTracker::new();

        // Act
        tracker.track_write(0, Bytes::from("key1"));

        // Assert
        assert!(tracker.write_set().contains(&(0, Bytes::from("key1"))));
    }

    #[test]
    fn should_detect_write_conflict() {
        // Arrange
        let mut tracker1 = ConflictTracker::new();
        let mut tracker2 = ConflictTracker::new();

        tracker1.track_write(0, Bytes::from("key1"));
        tracker2.track_write(0, Bytes::from("key1"));

        // Act
        let has_conflict = tracker1.has_write_conflict(tracker2.write_set());

        // Assert
        assert!(has_conflict);
    }

    #[test]
    fn should_not_detect_conflict_with_disjoint_keys() {
        // Arrange
        let mut tracker1 = ConflictTracker::new();
        let mut tracker2 = ConflictTracker::new();

        tracker1.track_write(0, Bytes::from("key1"));
        tracker2.track_write(0, Bytes::from("key2"));

        // Act
        let has_conflict = tracker1.has_write_conflict(tracker2.write_set());

        // Assert
        assert!(!has_conflict);
    }

    #[test]
    fn should_clear_all_operations() {
        // Arrange
        let mut tracker = ConflictTracker::new();
        tracker.track_read(0, Bytes::from("key1"), 100);
        tracker.track_write(0, Bytes::from("key2"));

        // Act
        tracker.clear();

        // Assert
        assert!(tracker.is_empty());
        assert_eq!(tracker.read_count(), 0);
        assert_eq!(tracker.write_count(), 0);
    }

    #[test]
    fn should_return_read_count() {
        // Arrange
        let mut tracker = ConflictTracker::new();

        // Act
        tracker.track_read(0, Bytes::from("key1"), 100);
        tracker.track_read(0, Bytes::from("key2"), 101);

        // Assert
        assert_eq!(tracker.read_count(), 2);
    }

    #[test]
    fn should_return_write_count() {
        // Arrange
        let mut tracker = ConflictTracker::new();

        // Act
        tracker.track_write(0, Bytes::from("key3"));

        // Assert
        assert_eq!(tracker.write_count(), 1);
    }

    #[test]
    fn should_track_write_range_successfully() {
        // Arrange
        let mut tracker = ConflictTracker::new();

        // Act
        tracker.track_write_range(0, Bytes::from("a"), Bytes::from("z"));

        // Assert
        assert!(tracker
            .write_ranges()
            .contains(&(0, Bytes::from("a"), Bytes::from("z"))));
        assert_eq!(tracker.write_count(), 1);
    }

    #[test]
    fn should_detect_write_range_conflict() {
        // Arrange
        let mut tracker1 = ConflictTracker::new();
        let mut tracker2 = ConflictTracker::new();
        tracker1.track_write(0, Bytes::from("m"));
        tracker2.track_write_range(0, Bytes::from("a"), Bytes::from("z"));

        // Act
        let has_conflict = tracker1.has_write_range_conflict(tracker2.write_ranges());

        // Assert
        assert!(has_conflict);
    }

    #[test]
    fn should_not_detect_write_range_conflict_outside_range() {
        // Arrange
        let mut tracker1 = ConflictTracker::new();
        let mut tracker2 = ConflictTracker::new();
        tracker1.track_write(0, Bytes::from("z"));
        tracker2.track_write_range(0, Bytes::from("a"), Bytes::from("m"));

        // Act
        let has_conflict = tracker1.has_write_range_conflict(tracker2.write_ranges());

        // Assert
        assert!(!has_conflict);
    }

    #[test]
    fn should_detect_range_range_overlap() {
        // Arrange
        let mut tracker1 = ConflictTracker::new();
        let mut tracker2 = ConflictTracker::new();
        tracker1.track_write_range(0, Bytes::from("a"), Bytes::from("m"));
        tracker2.track_write_range(0, Bytes::from("f"), Bytes::from("z"));

        // Act
        let has_conflict = tracker1.has_range_range_conflict(tracker2.write_ranges());

        // Assert
        assert!(has_conflict);
    }

    #[test]
    fn should_not_detect_range_range_overlap_when_disjoint() {
        // Arrange
        let mut tracker1 = ConflictTracker::new();
        let mut tracker2 = ConflictTracker::new();
        tracker1.track_write_range(0, Bytes::from("a"), Bytes::from("m"));
        tracker2.track_write_range(0, Bytes::from("n"), Bytes::from("z"));

        // Act
        let has_conflict = tracker1.has_range_range_conflict(tracker2.write_ranges());

        // Assert
        assert!(!has_conflict);
    }

    #[test]
    fn should_track_multiple_reads_for_same_key() {
        // Arrange
        let mut tracker = ConflictTracker::new();

        // Act
        tracker.track_read(0, Bytes::from("key1"), 100);
        tracker.track_read(0, Bytes::from("key1"), 150);

        // Assert
        assert_eq!(tracker.read_count(), 1);
        assert_eq!(tracker.read_version(0, b"key1"), Some(150));
    }

    #[test]
    fn should_track_operations_in_different_column_families() {
        // Arrange
        let mut tracker = ConflictTracker::new();

        // Act
        tracker.track_read(0, Bytes::from("key1"), 100);
        tracker.track_read(1, Bytes::from("key1"), 101);
        tracker.track_write(0, Bytes::from("key2"));
        tracker.track_write(1, Bytes::from("key2"));

        // Assert
        assert_eq!(tracker.read_count(), 2);
        assert_eq!(tracker.write_count(), 2);
    }

    #[test]
    fn should_not_detect_conflict_in_different_column_families() {
        // Arrange
        let mut tracker1 = ConflictTracker::new();
        let mut tracker2 = ConflictTracker::new();
        tracker1.track_write(0, Bytes::from("key1"));
        tracker2.track_write(1, Bytes::from("key1"));

        // Act
        let has_conflict = tracker1.has_write_conflict(tracker2.write_set());

        // Assert
        assert!(!has_conflict);
    }

    #[test]
    fn should_detect_range_overlap_at_boundaries() {
        // Arrange
        let mut tracker1 = ConflictTracker::new();
        let mut tracker2 = ConflictTracker::new();
        tracker1.track_write_range(0, Bytes::from("a"), Bytes::from("m"));
        tracker2.track_write_range(0, Bytes::from("m"), Bytes::from("z"));

        // Act
        let has_conflict = tracker1.has_range_range_conflict(tracker2.write_ranges());

        // Assert
        assert!(!has_conflict);
    }

    #[test]
    fn should_return_none_for_unknown_read_version() {
        // Arrange
        let tracker = ConflictTracker::new();

        // Act
        let version = tracker.read_version(0, b"unknown");

        // Assert
        assert_eq!(version, None);
    }

    #[test]
    fn should_handle_empty_tracker_operations() {
        // Arrange
        let tracker1 = ConflictTracker::new();
        let tracker2 = ConflictTracker::new();

        // Act
        let has_write_conflict = tracker1.has_write_conflict(tracker2.write_set());
        let has_range_conflict = tracker1.has_write_range_conflict(tracker2.write_ranges());
        let has_range_range_conflict = tracker1.has_range_range_conflict(tracker2.write_ranges());

        // Assert
        assert!(!has_write_conflict);
        assert!(!has_range_conflict);
        assert!(!has_range_range_conflict);
    }

    #[test]
    fn should_count_write_operations_including_ranges() {
        // Arrange
        let mut tracker = ConflictTracker::new();

        // Act
        tracker.track_write(0, Bytes::from("key1"));
        tracker.track_write(0, Bytes::from("key2"));
        tracker.track_write_range(0, Bytes::from("a"), Bytes::from("z"));

        // Assert
        assert_eq!(tracker.write_count(), 3);
    }
}
