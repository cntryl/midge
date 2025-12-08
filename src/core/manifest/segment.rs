//! Mutable SST segments for write-amplification reduction.
//!
//! A segment is a mutable, block-based data structure that:
//! - Accepts writes during normal engine operation
//! - Can be sealed (made read-only) at flush or time-based triggers
//! - Is promoted to level 0 or 1 after sealing
//!
//! Segments sit between the memtable and sealed SSTs in the read path.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use bytes::Bytes;

/// Unique segment identifier (monotonically increasing per column family)
pub type SegmentId = u64;

/// State of a segment in its lifecycle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentState {
    /// Segment is mutable; accepts new writes
    Mutable,
    /// Segment is being sealed; no new writes, reads allowed
    Sealing,
    /// Segment is sealed (read-only); ready for promotion
    Sealed,
    /// Segment is promoted to LSM level; no longer tracked as active segment
    Promoted,
}

/// Mutable segment metadata tracked in the manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    /// Unique segment identifier (per CF)
    pub id: SegmentId,
    /// Column family this segment belongs to
    pub cf_id: u32,
    /// Current state in the segment lifecycle
    pub state: SegmentState,
    /// Minimum key in this segment (inclusive)
    pub min_key: Bytes,
    /// Maximum key in this segment (inclusive)
    pub max_key: Bytes,
    /// Estimated size in bytes
    pub size_bytes: u64,
    /// Number of entries (blocks or records) in this segment
    pub entry_count: u64,
    /// Timestamp when segment was created (Unix epoch)
    pub created_at: u64,
    /// Timestamp when segment was sealed (if applicable)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed_at: Option<u64>,
    /// Timestamp when segment was promoted to LSM (if applicable)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_at: Option<u64>,
    /// File name when promoted to SST (if applicable)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sst_name: Option<String>,
    /// Level assigned after promotion (if applicable)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_level: Option<u32>,
}

impl Segment {
    /// Create a new mutable segment
    pub fn new(
        id: SegmentId,
        cf_id: u32,
        min_key: Bytes,
        max_key: Bytes,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            id,
            cf_id,
            state: SegmentState::Mutable,
            min_key,
            max_key,
            size_bytes: 0,
            entry_count: 0,
            created_at: now,
            sealed_at: None,
            promoted_at: None,
            sst_name: None,
            promoted_level: None,
        }
    }

    /// Check if this segment can accept writes
    pub fn is_mutable(&self) -> bool {
        self.state == SegmentState::Mutable
    }

    /// Check if this segment can be read
    pub fn is_readable(&self) -> bool {
        matches!(self.state, SegmentState::Mutable | SegmentState::Sealing | SegmentState::Sealed)
    }

    /// Seal this segment (transition to read-only)
    pub fn seal(&mut self) -> Result<(), String> {
        if !self.is_mutable() {
            return Err(format!(
                "Cannot seal segment {} in state {:?}",
                self.id, self.state
            ));
        }

        self.state = SegmentState::Sealed;
        self.sealed_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        Ok(())
    }

    /// Promote this segment to an SST at the specified level
    pub fn promote(&mut self, sst_name: String, level: u32) -> Result<(), String> {
        if self.state != SegmentState::Sealed {
            return Err(format!(
                "Cannot promote segment {} in state {:?}; must be sealed first",
                self.id, self.state
            ));
        }

        self.state = SegmentState::Promoted;
        self.sst_name = Some(sst_name);
        self.promoted_level = Some(level);
        self.promoted_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        Ok(())
    }

    /// Check if key range overlaps with this segment
    pub fn overlaps(&self, start: &[u8], end: &[u8]) -> bool {
        // [start, end) overlaps [min_key, max_key] if:
        // start <= max_key AND end > min_key
        start.as_ref() <= self.max_key.as_ref() && Bytes::from(end.to_vec()) > self.min_key
    }

    /// Check if key is within this segment's range
    pub fn contains_key(&self, key: &[u8]) -> bool {
        key >= self.min_key.as_ref() && key <= self.max_key.as_ref()
    }
}

/// Lightweight reference to a segment (for read-path queries)
#[derive(Debug, Clone)]
pub struct SegmentRef {
    pub id: SegmentId,
    pub cf_id: u32,
    pub state: SegmentState,
    pub min_key: Bytes,
    pub max_key: Bytes,
    pub created_at: u64,
}

impl From<&Segment> for SegmentRef {
    fn from(seg: &Segment) -> Self {
        Self {
            id: seg.id,
            cf_id: seg.cf_id,
            state: seg.state,
            min_key: seg.min_key.clone(),
            max_key: seg.max_key.clone(),
            created_at: seg.created_at,
        }
    }
}

/// Global segment sequence counter (monotonic per column family)
#[derive(Debug)]
pub struct SegmentSequencer {
    /// Next available segment ID (per CF)
    next_ids: Arc<std::sync::Mutex<std::collections::HashMap<u32, AtomicU64>>>,
}

impl SegmentSequencer {
    pub fn new() -> Self {
        Self {
            next_ids: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Allocate next segment ID for the given column family
    pub fn next_id(&self, cf_id: u32) -> SegmentId {
        let mut map = self.next_ids.lock().expect("SegmentSequencer mutex poisoned");
        let counter = map
            .entry(cf_id)
            .or_insert_with(|| AtomicU64::new(1));

        counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Set next segment ID to at least the given value (for recovery)
    pub fn set_min_id(&self, cf_id: u32, min_id: SegmentId) {
        let mut map = self.next_ids.lock().expect("SegmentSequencer mutex poisoned");
        let counter = map
            .entry(cf_id)
            .or_insert_with(|| AtomicU64::new(min_id));

        loop {
            let current = counter.load(Ordering::SeqCst);
            if current >= min_id {
                break;
            }
            if counter
                .compare_exchange(current, min_id, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }
}

impl Default for SegmentSequencer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_new_segment() {
        // Arrange
        let id = 1;
        let cf_id = 0;

        // Act
        let seg = Segment::new(id, cf_id, Bytes::from("apple"), Bytes::from("zebra"));

        // Assert
        assert_eq!(seg.id, 1);
        assert_eq!(seg.cf_id, 0);
        assert_eq!(seg.state, SegmentState::Mutable);
        assert!(seg.is_mutable());
        assert!(seg.is_readable());
    }

    #[test]
    fn should_seal_mutable_segment() {
        // Arrange
        let mut seg = Segment::new(1, 0, Bytes::from("apple"), Bytes::from("zebra"));

        // Act
        let result = seg.seal();

        // Assert
        assert!(result.is_ok());
        assert_eq!(seg.state, SegmentState::Sealed);
        assert!(!seg.is_mutable());
        assert!(seg.is_readable());
        assert!(seg.sealed_at.is_some());
    }

    #[test]
    fn should_reject_seal_of_sealed_segment() {
        // Arrange
        let mut seg = Segment::new(1, 0, Bytes::from("apple"), Bytes::from("zebra"));
        let _ = seg.seal();

        // Act
        let result = seg.seal();

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Cannot seal"));
    }

    #[test]
    fn should_promote_sealed_segment() {
        // Arrange
        let mut seg = Segment::new(1, 0, Bytes::from("apple"), Bytes::from("zebra"));
        let _ = seg.seal();

        // Act
        let result = seg.promote("sst_001.db".to_string(), 0);

        // Assert
        assert!(result.is_ok());
        assert_eq!(seg.state, SegmentState::Promoted);
        assert_eq!(seg.sst_name, Some("sst_001.db".to_string()));
        assert_eq!(seg.promoted_level, Some(0));
    }

    #[test]
    fn should_reject_promote_of_mutable_segment() {
        // Arrange
        let mut seg = Segment::new(1, 0, Bytes::from("apple"), Bytes::from("zebra"));

        // Act
        let result = seg.promote("sst_001.db".to_string(), 0);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be sealed first"));
    }

    #[test]
    fn should_detect_key_range_overlap() {
        // Arrange
        let seg = Segment::new(1, 0, Bytes::from("apple"), Bytes::from("cherry"));

        // Act
        let overlap1 = seg.overlaps(b"b", b"d");
        let overlap2 = seg.overlaps(b"z", b"zz");
        let overlap3 = seg.overlaps(b"apple", b"cherry");

        // Assert
        assert!(overlap1);
        assert!(!overlap2);
        assert!(overlap3);
    }

    #[test]
    fn should_check_key_containment() {
        // Arrange
        let seg = Segment::new(1, 0, Bytes::from("apple"), Bytes::from("cherry"));

        // Act
        let contains1 = seg.contains_key(b"apple");
        let contains2 = seg.contains_key(b"banana");
        let contains3 = seg.contains_key(b"cherry");
        let not_contains1 = seg.contains_key(b"a");
        let not_contains2 = seg.contains_key(b"date");

        // Assert
        assert!(contains1);
        assert!(contains2);
        assert!(contains3);
        assert!(!not_contains1);
        assert!(!not_contains2);
    }

    #[test]
    fn should_allocate_sequential_segment_ids() {
        // Arrange
        let sequencer = SegmentSequencer::new();

        // Act
        let id1 = sequencer.next_id(0);
        let id2 = sequencer.next_id(0);
        let id3 = sequencer.next_id(0);

        // Assert
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn should_track_ids_per_column_family() {
        // Arrange
        let sequencer = SegmentSequencer::new();

        // Act
        let cf0_id1 = sequencer.next_id(0);
        let cf1_id1 = sequencer.next_id(1);
        let cf0_id2 = sequencer.next_id(0);

        // Assert
        assert_eq!(cf0_id1, 1);
        assert_eq!(cf1_id1, 1);
        assert_eq!(cf0_id2, 2);
    }

    #[test]
    fn should_set_minimum_id_for_recovery() {
        // Arrange
        let sequencer = SegmentSequencer::new();

        // Act
        sequencer.set_min_id(0, 100);
        let id1 = sequencer.next_id(0);

        // Assert
        assert!(id1 >= 100);
    }
}
