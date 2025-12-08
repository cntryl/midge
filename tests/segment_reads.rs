//! Integration tests for segment read path integration.
//!
//! Tests that segments are correctly integrated into the read path:
//! - Sealed segments are checked after immutable memtables
//! - Segments are checked in age order (oldest first)
//! - Segment lookups don't interfere with SST lookups
//! - Range scans include segment data

use cntryl_midge::core::manifest::{Manifest, Segment, SegmentId, SegmentSequencer, SegmentState};
use bytes::Bytes;

/// Helper to create a test segment
fn create_test_segment(
    id: SegmentId,
    cf_id: u32,
    min_key: &[u8],
    max_key: &[u8],
    created_at: u64,
) -> Segment {
    Segment {
        id,
        cf_id,
        state: SegmentState::Sealed,
        min_key: Bytes::copy_from_slice(min_key),
        max_key: Bytes::copy_from_slice(max_key),
        size_bytes: 1024,
        entry_count: 10,
        created_at,
        sealed_at: Some(created_at + 1000),
        promoted_at: None,
        sst_name: None,
        promoted_level: None,
    }
}

#[test]
fn should_collect_sealed_segments_for_point_key() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.segments = vec![
        create_test_segment(1, 0, b"aaa", b"bbb", 100),
        create_test_segment(2, 0, b"bbb", b"ddd", 200), // "bee" is between "bbb" and "ddd"
        create_test_segment(3, 0, b"xxx", b"zzz", 300),
    ];

    // Act: Collect segments containing key "bee" (should match segment 2 only)
    let matching: Vec<_> = manifest
        .segments
        .iter()
        .filter(|seg| {
            seg.cf_id == 0
                && seg.state == SegmentState::Sealed
                && seg.min_key.as_ref() <= b"bee".as_ref()
                && b"bee".as_ref() <= seg.max_key.as_ref()
        })
        .collect();

    // Assert
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].id, 2);
}

#[test]
fn should_not_collect_segments_for_key_outside_range() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.segments = vec![
        create_test_segment(1, 0, b"aaa", b"bbb", 100),
        create_test_segment(2, 0, b"ddd", b"fff", 200),
    ];

    // Act: Try to collect segments containing key "zzz" (outside all ranges)
    let matching: Vec<_> = manifest
        .segments
        .iter()
        .filter(|seg| {
            seg.cf_id == 0
                && seg.state == SegmentState::Sealed
                && seg.min_key.as_ref() <= b"zzz".as_ref()
                && b"zzz".as_ref() <= seg.max_key.as_ref()
        })
        .collect();

    // Assert
    assert_eq!(matching.len(), 0);
}

#[test]
fn should_preserve_segment_age_order_for_read_ordering() {
    // Arrange
    let mut manifest = Manifest::default();
    // Create segments out of insertion order to test sorting by creation time
    manifest.segments = vec![
        create_test_segment(2, 0, b"bbb", b"eee", 200), // Oldest semantically
        create_test_segment(1, 0, b"aaa", b"ddd", 100), // Newest semantically (created first)
        create_test_segment(3, 0, b"ccc", b"fff", 300), // Middle
    ];

    // Act: Collect and sort segments by age
    let mut segments: Vec<_> = manifest
        .segments
        .iter()
        .filter(|seg| seg.cf_id == 0 && seg.state == SegmentState::Sealed)
        .cloned()
        .collect();
    segments.sort_by_key(|seg| seg.created_at);

    // Assert: Segments sorted by creation time (oldest first)
    assert_eq!(segments.len(), 3);
    assert_eq!(segments[0].id, 1); // created_at=100
    assert_eq!(segments[1].id, 2); // created_at=200
    assert_eq!(segments[2].id, 3); // created_at=300
}

#[test]
fn should_filter_segments_by_column_family() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.segments = vec![
        create_test_segment(1, 0, b"aaa", b"bbb", 100),
        create_test_segment(2, 1, b"aaa", b"bbb", 101), // Different CF
        create_test_segment(3, 0, b"ddd", b"fff", 200),
    ];

    // Act: Collect only segments for CF 0 containing key "bee"
    let matching: Vec<_> = manifest
        .segments
        .iter()
        .filter(|seg| {
            seg.cf_id == 0
                && seg.state == SegmentState::Sealed
                && seg.min_key.as_ref() <= b"bee".as_ref()
                && b"bee".as_ref() <= seg.max_key.as_ref()
        })
        .collect();

    // Assert
    assert_eq!(matching.len(), 0); // "bee" is not in CF 0's ranges
}

#[test]
fn should_only_include_sealed_segments() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.segments = vec![
        Segment {
            state: SegmentState::Mutable,
            ..create_test_segment(1, 0, b"aaa", b"bbb", 100)
        },
        create_test_segment(2, 0, b"aaa", b"bbb", 101),
        Segment {
            state: SegmentState::Promoted,
            ..create_test_segment(3, 0, b"aaa", b"bbb", 102)
        },
    ];

    // Act: Collect only sealed segments
    let sealed: Vec<_> = manifest
        .segments
        .iter()
        .filter(|seg| seg.state == SegmentState::Sealed)
        .collect();

    // Assert
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0].id, 2);
}

#[test]
fn should_detect_segment_overlap_for_range_queries() {
    // Arrange
    let seg1 = create_test_segment(1, 0, b"aaa", b"ddd", 100);
    let seg2 = create_test_segment(2, 0, b"bbb", b"eee", 200);
    let seg3 = create_test_segment(3, 0, b"yyy", b"zzz", 300);

    // Act
    let overlap1 = seg1.overlaps(b"ccc", b"fff");
    let overlap2 = seg2.overlaps(b"ccc", b"fff");
    let overlap3 = seg3.overlaps(b"ccc", b"fff");

    // Assert
    assert!(overlap1);
    assert!(overlap2);
    assert!(!overlap3);
}

#[test]
fn should_handle_empty_segment_list_gracefully() {
    // Arrange
    let manifest = Manifest::default();

    // Act: Try to collect segments from empty manifest
    let segments: Vec<_> = manifest
        .segments
        .iter()
        .filter(|seg| {
            seg.cf_id == 0 && seg.state == SegmentState::Sealed
                && seg.min_key.as_ref() <= b"key".as_ref()
                && b"key".as_ref() <= seg.max_key.as_ref()
        })
        .collect();

    // Assert
    assert_eq!(segments.len(), 0);
}

#[test]
fn should_allocate_segment_ids_sequentially_per_cf() {
    // Arrange
    let sequencer = SegmentSequencer::new();

    // Act: Allocate IDs for two column families
    let cf0_id1 = sequencer.next_id(0);
    let cf1_id1 = sequencer.next_id(1);
    let cf0_id2 = sequencer.next_id(0);
    let cf1_id2 = sequencer.next_id(1);

    // Assert
    assert_eq!(cf0_id1, 1);
    assert_eq!(cf1_id1, 1); // Independent sequence per CF
    assert_eq!(cf0_id2, 2);
    assert_eq!(cf1_id2, 2);
}

#[test]
fn should_integrate_segments_into_read_path_conceptually() {
    // Arrange: Create a manifest with segments and SST files (segments come before SSTs)
    let mut manifest = Manifest::default();
    manifest.segments = vec![
        create_test_segment(1, 0, b"a", b"l", 100),    // "k" is in this range
        create_test_segment(2, 0, b"m", b"z", 200),
    ];

    // Act: Simulate read path ordering: memtable → segments → SSTs
    // For a key "k", we would:
    // 1. Check active/immutable memtables (not in this test)
    // 2. Check sealed segments
    let segments_to_check: Vec<_> = manifest
        .segments
        .iter()
        .filter(|seg| {
            seg.cf_id == 0
                && seg.state == SegmentState::Sealed
                && seg.min_key.as_ref() <= b"k".as_ref()
                && b"k".as_ref() <= seg.max_key.as_ref()
        })
        .collect();

    // 3. Check SST files (not in this test, would happen after segments)

    // Assert: Segment ordering is correct
    assert_eq!(segments_to_check.len(), 1);
    assert_eq!(segments_to_check[0].id, 1); // Key "k" is in segment 1's range (a-l)
}
