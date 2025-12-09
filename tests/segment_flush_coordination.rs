//! Integration tests for segment flush and sealing coordination.
//!
//! Tests that segments properly integrate with flush operations:
//! - Segments can be sealed on flush trigger
//! - Sealed segments are promoted to Level 0
//! - Flush coordinates segment lifecycle changes

use bytes::Bytes;
use cntryl_midge::core::manifest::{Manifest, Segment, SegmentState};

/// Helper to create a test segment
fn create_test_segment(
    id: u64,
    cf_id: u32,
    min_key: &[u8],
    max_key: &[u8],
    created_at: u64,
    state: SegmentState,
) -> Segment {
    Segment {
        id,
        cf_id,
        state,
        min_key: Bytes::copy_from_slice(min_key),
        max_key: Bytes::copy_from_slice(max_key),
        size_bytes: 1024,
        entry_count: 10,
        created_at,
        sealed_at: if state == SegmentState::Sealed {
            Some(created_at + 1000)
        } else {
            None
        },
        promoted_at: None,
        sst_name: None,
        promoted_level: None,
    }
}

#[test]
fn should_transition_mutable_segment_to_sealed() {
    // Arrange
    let mut segment = create_test_segment(1, 0, b"aaa", b"bbb", 100, SegmentState::Mutable);

    // Act
    let result = segment.seal();

    // Assert
    assert!(result.is_ok());
    assert_eq!(segment.state, SegmentState::Sealed);
    assert!(segment.sealed_at.is_some());
}

#[test]
fn should_transition_sealed_segment_to_promoted() {
    // Arrange
    let mut segment = create_test_segment(1, 0, b"aaa", b"bbb", 100, SegmentState::Sealed);

    // Act
    let result = segment.promote("sst_001.db".to_string(), 0);

    // Assert
    assert!(result.is_ok());
    assert_eq!(segment.state, SegmentState::Promoted);
    assert_eq!(segment.sst_name, Some("sst_001.db".to_string()));
    assert_eq!(segment.promoted_level, Some(0));
    assert!(segment.promoted_at.is_some());
}

#[test]
fn should_fail_to_seal_already_sealed_segment() {
    // Arrange
    let mut segment = create_test_segment(1, 0, b"aaa", b"bbb", 100, SegmentState::Sealed);

    // Act
    let result = segment.seal();

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Cannot seal"));
}

#[test]
fn should_fail_to_promote_mutable_segment() {
    // Arrange
    let mut segment = create_test_segment(1, 0, b"aaa", b"bbb", 100, SegmentState::Mutable);

    // Act
    let result = segment.promote("sst_001.db".to_string(), 0);

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must be sealed first"));
}

#[test]
fn should_track_segments_by_state_in_manifest() {
    // Arrange
    let manifest = Manifest {
        segments: vec![
            create_test_segment(1, 0, b"aaa", b"bbb", 100, SegmentState::Mutable),
            create_test_segment(2, 0, b"bbb", b"ccc", 200, SegmentState::Sealed),
            create_test_segment(3, 0, b"ccc", b"ddd", 300, SegmentState::Promoted),
        ],
        ..Default::default()
    };

    // Act
    let mutable_count = manifest
        .segments
        .iter()
        .filter(|s| s.state == SegmentState::Mutable)
        .count();
    let sealed_count = manifest
        .segments
        .iter()
        .filter(|s| s.state == SegmentState::Sealed)
        .count();
    let promoted_count = manifest
        .segments
        .iter()
        .filter(|s| s.state == SegmentState::Promoted)
        .count();

    // Assert
    assert_eq!(mutable_count, 1);
    assert_eq!(sealed_count, 1);
    assert_eq!(promoted_count, 1);
}

#[test]
fn should_preserve_segment_metadata_through_state_transitions() {
    // Arrange
    let mut segment = create_test_segment(42, 5, b"start", b"end", 12345, SegmentState::Mutable);
    let original_id = segment.id;
    let original_cf_id = segment.cf_id;
    let original_min_key = segment.min_key.clone();
    let original_max_key = segment.max_key.clone();

    // Act
    let _ = segment.seal();
    let _ = segment.promote("sst_042.db".to_string(), 1);

    // Assert
    assert_eq!(segment.id, original_id);
    assert_eq!(segment.cf_id, original_cf_id);
    assert_eq!(segment.min_key, original_min_key);
    assert_eq!(segment.max_key, original_max_key);
    assert_eq!(segment.state, SegmentState::Promoted);
}

#[test]
fn should_handle_flush_triggers_segment_sealing_conceptually() {
    // Arrange: Simulate manifest state before flush
    let mut manifest = Manifest {
        segments: vec![
            create_test_segment(1, 0, b"aaa", b"bbb", 100, SegmentState::Mutable),
            create_test_segment(2, 0, b"bbb", b"ccc", 200, SegmentState::Sealed),
        ],
        ..Default::default()
    };

    // Act: On flush trigger, seal the mutable segment and promote the sealed one
    // Phase 5.3 will implement this in flush_manager.rs
    let segments_to_seal: Vec<_> = manifest
        .segments
        .iter_mut()
        .filter(|s| s.cf_id == 0 && s.state == SegmentState::Mutable)
        .collect();

    for segment in segments_to_seal {
        let _ = segment.seal();
    }

    let segments_to_promote: Vec<_> = manifest
        .segments
        .iter_mut()
        .filter(|s| s.cf_id == 0 && s.state == SegmentState::Sealed && s.sst_name.is_none())
        .collect();

    for segment in segments_to_promote {
        let _ = segment.promote("sst_promoted.db".to_string(), 0);
    }

    // Assert
    let mutable_count = manifest
        .segments
        .iter()
        .filter(|s| s.state == SegmentState::Mutable)
        .count();
    let promoted_count = manifest
        .segments
        .iter()
        .filter(|s| s.state == SegmentState::Promoted)
        .count();

    assert_eq!(mutable_count, 0); // All mutable segments sealed and promoted
    assert_eq!(promoted_count, 2); // Both mutable and sealed segments promoted
}

#[test]
fn should_assign_sst_name_on_promotion() {
    // Arrange
    let mut segment = create_test_segment(1, 0, b"aaa", b"bbb", 100, SegmentState::Sealed);

    // Act
    let sst_name = "sst_000001.db";
    let assigned_level = 0;
    let result = segment.promote(sst_name.to_string(), assigned_level);

    // Assert
    assert!(result.is_ok());
    assert_eq!(segment.sst_name, Some(sst_name.to_string()));
    assert_eq!(segment.promoted_level, Some(assigned_level));
}

#[test]
fn should_track_promotion_timestamp() {
    // Arrange
    let mut segment = create_test_segment(1, 0, b"aaa", b"bbb", 100, SegmentState::Sealed);

    // Act
    let _ = segment.promote("sst_001.db".to_string(), 0);

    // Assert
    assert!(segment.promoted_at.is_some());
    assert!(segment.promoted_at.unwrap() > segment.sealed_at.unwrap());
}
