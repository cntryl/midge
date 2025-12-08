//! Segment flush integration: creates and manages segments during flush operations.
//!
//! This module handles the lifecycle of segments during memtable flushing:
//! - Creating segments from flushed entries
//! - Sealing segments when flush completes
//! - Promoting sealed segments to L0 with SST metadata
//! - Updating manifest with segment lifecycle changes

use bytes::Bytes;
use crate::core::manifest::{Manifest, Segment, SegmentState};
use crate::core::EntryMeta;

/// Create a new segment from flushed entries.
///
/// Computes key bounds and metadata from the entries to initialize a segment
/// in the Mutable state. The segment will later be sealed when flush completes.
///
/// # Arguments
/// * `cf_id` - Column family ID for this segment
/// * `segment_id` - Unique segment identifier
/// * `entries` - Drained memtable entries
///
/// # Returns
/// New segment in Mutable state, ready for sealing
pub fn create_segment_from_entries(
    cf_id: u32,
    segment_id: u64,
    entries: &[EntryMeta],
) -> Option<Segment> {
    if entries.is_empty() {
        return None;
    }

    // Compute key bounds
    let min_key = entries
        .iter()
        .min_by(|a, b| a.key.cmp(&b.key))
        .map(|e| e.key.clone())?;

    let max_key = entries
        .iter()
        .max_by(|a, b| a.key.cmp(&b.key))
        .map(|e| e.key.clone())?;

    // Compute size (rough estimate: sum of key + value sizes)
    let size_bytes: u64 = entries
        .iter()
        .map(|e| {
            (e.key.len() as u64)
                + e.value
                    .as_ref()
                    .map(|v| v.len() as u64)
                    .unwrap_or(0)
        })
        .sum();

    Some(Segment {
        id: segment_id,
        cf_id,
        state: SegmentState::Mutable,
        min_key: Bytes::from(min_key),
        max_key: Bytes::from(max_key),
        size_bytes,
        entry_count: entries.len() as u64,
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        sealed_at: None,
        promoted_at: None,
        sst_name: None,
        promoted_level: None,
    })
}

/// Seal a segment during flush completion.
///
/// Transitions a mutable segment to Sealed state. This is called after
/// the segment's entries have been written and persisted.
///
/// # Arguments
/// * `segment` - Mutable segment to seal
///
/// # Returns
/// Result with the sealed segment, or error if transition is invalid
pub fn seal_segment_on_flush(segment: &mut Segment) -> Result<(), String> {
    segment.seal()
}

/// Promote a sealed segment to L0 with SST metadata.
///
/// Transitions a sealed segment to Promoted state and assigns it an SST filename
/// and level (always L0 for flush-created segments).
///
/// # Arguments
/// * `segment` - Sealed segment to promote
/// * `sst_name` - SST filename to assign (e.g., "00/000001.sst")
///
/// # Returns
/// Result with promoted segment, or error if transition is invalid
pub fn promote_segment_to_l0(segment: &mut Segment, sst_name: String) -> Result<(), String> {
    segment.promote(sst_name, 0)
}

/// Update manifest with segment lifecycle changes.
///
/// Adds or updates segment entries in the manifest to reflect sealing and
/// promotion transitions. This maintains the manifest as the single source
/// of truth for segment state.
///
/// # Arguments
/// * `manifest` - Manifest to update
/// * `segment` - Segment with updated state to record
pub fn update_manifest_with_segment(manifest: &mut Manifest, segment: Segment) {
    // Check if segment already exists
    if let Some(existing) = manifest.segments.iter_mut().find(|s| s.id == segment.id) {
        // Update existing segment with new state
        *existing = segment;
    } else {
        // Add new segment
        manifest.segments.push(segment);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_segment_from_entries() {
        // Arrange
        let entries = vec![
            crate::core::EntryMeta {
                key: b"aaa".to_vec(),
                value: Some(b"val1".to_vec()),
                sequence: 1,
                is_tombstone: false,
                op_type: crate::core::data_structures::skiplist::OpType::Put,
                expiration_millis: None,
            },
            crate::core::EntryMeta {
                key: b"bbb".to_vec(),
                value: Some(b"val2".to_vec()),
                sequence: 2,
                is_tombstone: false,
                op_type: crate::core::data_structures::skiplist::OpType::Put,
                expiration_millis: None,
            },
        ];

        // Act
        let segment = create_segment_from_entries(0, 1, &entries);

        // Assert
        assert!(segment.is_some());
        let seg = segment.unwrap();
        assert_eq!(seg.cf_id, 0);
        assert_eq!(seg.id, 1);
        assert_eq!(seg.state, SegmentState::Mutable);
        assert_eq!(seg.entry_count, 2);
        assert!(seg.size_bytes > 0);
    }

    #[test]
    fn should_return_none_for_empty_entries() {
        // Act
        let segment = create_segment_from_entries(0, 1, &[]);

        // Assert
        assert!(segment.is_none());
    }

    #[test]
    fn should_seal_segment_on_flush() {
        // Arrange
        let mut segment = Segment {
            id: 1,
            cf_id: 0,
            state: SegmentState::Mutable,
            min_key: Bytes::from("aaa"),
            max_key: Bytes::from("bbb"),
            size_bytes: 100,
            entry_count: 10,
            created_at: 100,
            sealed_at: None,
            promoted_at: None,
            sst_name: None,
            promoted_level: None,
        };

        // Act
        let seal_result = seal_segment_on_flush(&mut segment);

        // Assert
        assert!(seal_result.is_ok());
        assert_eq!(segment.state, SegmentState::Sealed);
    }

    #[test]
    fn should_promote_sealed_segment_to_l0() {
        // Arrange
        let mut segment = Segment {
            id: 1,
            cf_id: 0,
            state: SegmentState::Sealed,
            min_key: Bytes::from("aaa"),
            max_key: Bytes::from("bbb"),
            size_bytes: 100,
            entry_count: 10,
            created_at: 100,
            sealed_at: Some(101),
            promoted_at: None,
            sst_name: None,
            promoted_level: None,
        };

        // Act
        let promote_result = promote_segment_to_l0(&mut segment, "00/000001.sst".to_string());

        // Assert
        assert!(promote_result.is_ok());
        assert_eq!(segment.state, SegmentState::Promoted);
        assert_eq!(segment.sst_name, Some("00/000001.sst".to_string()));
        assert_eq!(segment.promoted_level, Some(0));
    }

    #[test]
    fn should_update_manifest_with_new_segment() {
        // Arrange
        let mut manifest = Manifest::default();
        let segment = Segment {
            id: 1,
            cf_id: 0,
            state: SegmentState::Sealed,
            min_key: Bytes::from("aaa"),
            max_key: Bytes::from("bbb"),
            size_bytes: 100,
            entry_count: 10,
            created_at: 100,
            sealed_at: Some(101),
            promoted_at: None,
            sst_name: None,
            promoted_level: None,
        };

        // Act
        update_manifest_with_segment(&mut manifest, segment.clone());

        // Assert
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].id, segment.id);
        assert_eq!(manifest.segments[0].state, SegmentState::Sealed);
    }

    #[test]
    fn should_update_existing_segment_in_manifest() {
        // Arrange
        let mut manifest = Manifest::default();
        let segment = Segment {
            id: 1,
            cf_id: 0,
            state: SegmentState::Mutable,
            min_key: Bytes::from("aaa"),
            max_key: Bytes::from("bbb"),
            size_bytes: 100,
            entry_count: 10,
            created_at: 100,
            sealed_at: None,
            promoted_at: None,
            sst_name: None,
            promoted_level: None,
        };
        manifest.segments.push(segment.clone());
        let mut updated_segment = segment.clone();
        updated_segment.state = SegmentState::Sealed;
        updated_segment.sealed_at = Some(101);

        // Act
        update_manifest_with_segment(&mut manifest, updated_segment);

        // Assert
        assert_eq!(manifest.segments.len(), 1); // Still one segment
        assert_eq!(manifest.segments[0].state, SegmentState::Sealed);
    }
}
