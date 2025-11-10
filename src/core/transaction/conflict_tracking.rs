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
/// - Read versions: Sequence numbers at which keys were read
///
/// This enables optimistic concurrency control by detecting conflicts at commit time.
#[derive(Debug, Clone)]
pub struct ConflictTracker {
    /// Keys that have been read (cf_id, key)
    read_set: HashSet<(u32, Bytes)>,
    
    /// Keys that have been written (cf_id, key)
    write_set: HashSet<(u32, Bytes)>,
    
    /// Sequence numbers at which keys were read (cf_id, key) -> version
    read_versions: HashMap<(u32, Bytes), u64>,
}

impl ConflictTracker {
    /// Create a new conflict tracker.
    pub fn new() -> Self {
        Self {
            read_set: HashSet::new(),
            write_set: HashSet::new(),
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

    /// Get the write set (keys modified by this transaction).
    pub fn write_set(&self) -> &HashSet<(u32, Bytes)> {
        &self.write_set
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

    /// Clear all tracked operations.
    pub fn clear(&mut self) {
        self.read_set.clear();
        self.write_set.clear();
        self.read_versions.clear();
    }

    /// Check if any operations have been tracked.
    pub fn is_empty(&self) -> bool {
        self.read_set.is_empty() && self.write_set.is_empty()
    }

    /// Get the number of read operations tracked.
    pub fn read_count(&self) -> usize {
        self.read_set.len()
    }

    /// Get the number of write operations tracked.
    pub fn write_count(&self) -> usize {
        self.write_set.len()
    }
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
    fn should_return_read_and_write_counts() {
        // Arrange
        let mut tracker = ConflictTracker::new();

        // Act
        tracker.track_read(0, Bytes::from("key1"), 100);
        tracker.track_read(0, Bytes::from("key2"), 101);
        tracker.track_write(0, Bytes::from("key3"));

        // Assert
        assert_eq!(tracker.read_count(), 2);
        assert_eq!(tracker.write_count(), 1);
    }
}
