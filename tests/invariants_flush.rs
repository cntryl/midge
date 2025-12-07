//! Flush subsystem invariant tests.
//!
//! These tests verify the correctness invariants of the flush subsystem:
//!
//! 1. **WAL prune never exceeds durable sequence** - WAL files are only deleted
//!    after their data is safely persisted to SST (disk or cloud).
//!
//! 2. **Manifest is updated before pruning** - No pruning happens if manifest
//!    persistence fails.
//!
//! 3. **Largest sequence wins over WAL rotation sequence** - The manifest's
//!    `last_persisted_sequence` uses the largest entry sequence, not the WAL
//!    rotation sequence, which is critical for MVCC correctness.
//!
//! 4. **Cloud checkpoint must not regress** - In cloud mode, WAL pruning uses
//!    the cloud checkpoint sequence, ensuring WAL is only deleted after SSTs
//!    are uploaded to cloud storage.

use cntryl_midge::core::manifest::{CloudCheckpoint, Manifest};
use cntryl_midge::core::persistence::flush::{
    compute_bounds, determine_safe_prune_sequence, prune_old_wal_files, FlushStats,
};
use cntryl_midge::core::skiplist::OpType;
use cntryl_midge::core::EntryMeta;
use std::time::SystemTime;
use tempfile::TempDir;

/// Helper to create a test entry with specified sequence
fn make_entry(key: &[u8], value: &[u8], sequence: u64) -> EntryMeta {
    EntryMeta {
        key: key.to_vec(),
        value: Some(value.to_vec()),
        sequence,
        is_tombstone: false,
        expiration_millis: None,
        op_type: OpType::Put,
    }
}

/// Helper to create WAL files with given sequences
fn create_wal_files(dir: &std::path::Path, sequences: &[u64]) {
    for seq in sequences {
        let filename = format!("{}.wal", seq);
        std::fs::write(dir.join(&filename), b"dummy").unwrap();
    }
}

// ============================================================================
// Invariant 1: WAL prune never exceeds durable sequence
// ============================================================================

#[test]
fn should_never_prune_wal_above_safe_sequence() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path();

    // Create WAL files at sequences 10, 20, 30, 40, 50
    create_wal_files(wal_dir, &[10, 20, 30, 40, 50]);

    // Safe sequence is 25 (only 10 and 20 should be pruned)
    let safe_sequence = 25;

    // Act
    let result = prune_old_wal_files(wal_dir, safe_sequence);

    // Assert
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2); // Only 10.wal and 20.wal should be pruned

    // Verify remaining files
    assert!(!wal_dir.join("10.wal").exists(), "10.wal should be pruned");
    assert!(!wal_dir.join("20.wal").exists(), "20.wal should be pruned");
    assert!(wal_dir.join("30.wal").exists(), "30.wal should remain");
    assert!(wal_dir.join("40.wal").exists(), "40.wal should remain");
    assert!(wal_dir.join("50.wal").exists(), "50.wal should remain");
}

#[test]
fn should_prune_wal_at_exact_safe_sequence() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path();
    create_wal_files(wal_dir, &[10, 20, 30]);

    // Safe sequence is exactly 20
    let safe_sequence = 20;

    // Act
    let result = prune_old_wal_files(wal_dir, safe_sequence);

    // Assert - WAL at safe_sequence should be pruned (it's <= safe)
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2); // 10.wal and 20.wal
    assert!(!wal_dir.join("20.wal").exists(), "20.wal should be pruned");
    assert!(wal_dir.join("30.wal").exists(), "30.wal should remain");
}

#[test]
fn should_not_prune_any_wal_when_safe_sequence_is_zero() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path();
    create_wal_files(wal_dir, &[1, 5, 10]);

    // Act
    let result = prune_old_wal_files(wal_dir, 0);

    // Assert - only sequence 0 or below would be pruned, but none exist
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
    assert!(wal_dir.join("1.wal").exists());
    assert!(wal_dir.join("5.wal").exists());
    assert!(wal_dir.join("10.wal").exists());
}

// ============================================================================
// Invariant 2: Manifest is updated before pruning (tested via sequence logic)
// ============================================================================

#[test]
fn should_use_manifest_persisted_sequence_for_local_mode() {
    // Arrange - local disk mode (no cloud checkpoint)
    let manifest = Manifest {
        last_persisted_sequence: 100,
        cloud_checkpoint: None,
        ..Default::default()
    };

    // Act
    let safe_seq = determine_safe_prune_sequence(&manifest);

    // Assert
    assert_eq!(safe_seq, 100, "Should use manifest's persisted sequence");
}

#[test]
fn should_use_cloud_checkpoint_for_cloud_mode() {
    // Arrange - cloud mode with checkpoint
    let manifest = Manifest {
        last_persisted_sequence: 100, // Local sequence is ahead
        cloud_checkpoint: Some(CloudCheckpoint {
            checkpoint_sequence: 50, // Cloud is behind
            covering_ssts: vec![],
            checkpoint_time: SystemTime::UNIX_EPOCH,
        }),
        ..Default::default()
    };

    // Act
    let safe_seq = determine_safe_prune_sequence(&manifest);

    // Assert - must use cloud sequence, not local
    assert_eq!(
        safe_seq, 50,
        "Should use cloud checkpoint sequence, not local"
    );
}

// ============================================================================
// Invariant 3: Largest sequence wins over WAL rotation sequence
// ============================================================================

#[test]
fn should_compute_largest_sequence_from_entries() {
    // Arrange - entries with sequences 5, 15, 10
    // (out of order to test proper max finding)
    let entries = vec![
        make_entry(b"key1", b"val1", 5),
        make_entry(b"key2", b"val2", 15),
        make_entry(b"key3", b"val3", 10),
    ];

    // Act
    let (_smallest_key, _largest_key, smallest_seq, largest_seq) = compute_bounds(&entries, &[]);

    // Assert
    assert_eq!(smallest_seq, Some(5));
    assert_eq!(largest_seq, Some(15), "Largest sequence must be 15");
}

#[test]
fn should_include_tombstones_in_sequence_range() {
    // Arrange - mix of entries and range tombstones
    let entries = vec![make_entry(b"key1", b"val1", 10)];
    let range_tombstones = vec![
        (b"a".to_vec(), b"m".to_vec(), 5),  // Earlier
        (b"n".to_vec(), b"z".to_vec(), 20), // Later - should be largest
    ];

    // Act
    let (_, _, smallest_seq, largest_seq) = compute_bounds(&entries, &range_tombstones);

    // Assert
    assert_eq!(smallest_seq, Some(5));
    assert_eq!(
        largest_seq,
        Some(20),
        "Range tombstone sequence must be included"
    );
}

#[test]
fn should_return_none_for_empty_flush() {
    // Arrange
    let entries: Vec<EntryMeta> = vec![];
    let range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)> = vec![];

    // Act
    let (smallest_key, largest_key, smallest_seq, largest_seq) =
        compute_bounds(&entries, &range_tombstones);

    // Assert - empty flush should return None for all bounds
    assert!(smallest_key.is_none());
    assert!(largest_key.is_none());
    assert!(smallest_seq.is_none());
    assert!(largest_seq.is_none());
}

// ============================================================================
// Invariant 4: Cloud checkpoint must not regress
// ============================================================================

#[test]
fn should_prefer_cloud_checkpoint_even_when_lower_than_manifest() {
    // Arrange
    // Scenario: Local manifest updated to seq 200, but cloud only at 100
    // This can happen if cloud upload is slow
    let manifest = Manifest {
        last_persisted_sequence: 200,
        cloud_checkpoint: Some(CloudCheckpoint {
            checkpoint_sequence: 100, // Cloud is behind
            covering_ssts: vec!["00000001/00000001.sst".to_string()],
            checkpoint_time: SystemTime::now(),
        }),
        ..Default::default()
    };

    // Act
    let safe_seq = determine_safe_prune_sequence(&manifest);

    // Assert - must use cloud sequence to prevent data loss
    assert_eq!(
        safe_seq, 100,
        "Must use cloud checkpoint even when lower than local"
    );
}

#[test]
fn should_use_manifest_sequence_when_cloud_checkpoint_is_none() {
    // Arrange - explicit None (not cloud mode)
    let manifest = Manifest {
        last_persisted_sequence: 150,
        cloud_checkpoint: None,
        ..Default::default()
    };

    // Act
    let safe_seq = determine_safe_prune_sequence(&manifest);

    // Assert
    assert_eq!(safe_seq, 150);
}

#[test]
fn should_handle_cloud_checkpoint_at_zero() {
    // Arrange - cloud checkpoint at sequence 0 (fresh database)
    let manifest = Manifest {
        last_persisted_sequence: 50,
        cloud_checkpoint: Some(CloudCheckpoint {
            checkpoint_sequence: 0,
            covering_ssts: vec![],
            checkpoint_time: SystemTime::UNIX_EPOCH,
        }),
        ..Default::default()
    };

    // Act
    let safe_seq = determine_safe_prune_sequence(&manifest);

    // Assert - must use 0, preventing all WAL pruning until cloud catches up
    assert_eq!(safe_seq, 0, "Cloud at zero should prevent all WAL pruning");
}

// ============================================================================
// FlushStats invariant tests
// ============================================================================

#[test]
fn should_compute_correct_byte_count() {
    // Arrange
    let entries = vec![
        make_entry(b"key1", b"value1", 1), // 4 + 6 = 10 bytes
        make_entry(b"key2", b"val", 2),    // 4 + 3 = 7 bytes
    ];
    let range_tombstones = vec![
        (b"start".to_vec(), b"end".to_vec(), 1), // 5 + 3 = 8 bytes
    ];

    // Act
    let stats = FlushStats::compute(&entries, &range_tombstones);

    // Assert
    assert_eq!(stats.total_bytes, 25); // 10 + 7 + 8
    assert_eq!(stats.total_entries, 2);
    assert_eq!(stats.range_tombstone_count, 1);
}

#[test]
fn should_count_tombstones_separately() {
    // Arrange
    let entries = vec![
        make_entry(b"key1", b"val1", 1),
        EntryMeta {
            key: b"key2".to_vec(),
            value: None,
            sequence: 2,
            is_tombstone: true,
            expiration_millis: None,
            op_type: OpType::Delete,
        },
        EntryMeta {
            key: b"key3".to_vec(),
            value: None,
            sequence: 3,
            is_tombstone: true,
            expiration_millis: None,
            op_type: OpType::Delete,
        },
    ];

    // Act
    let stats = FlushStats::compute(&entries, &[]);

    // Assert
    assert_eq!(stats.point_tombstone_count, 2);
    assert_eq!(stats.total_entries, 3);
}

// ============================================================================
// Key bounds ordering invariant tests
// ============================================================================

#[test]
fn should_compute_bounds_correctly_given_unordered_keys_when_flushing() {
    // Arrange - keys intentionally out of order
    let entries = vec![
        make_entry(b"key_m", b"val", 1), // middle
        make_entry(b"key_a", b"val", 2), // smallest
        make_entry(b"key_z", b"val", 3), // largest
    ];

    // Act
    let (smallest_key, largest_key, _, _) = compute_bounds(&entries, &[]);

    // Assert
    assert_eq!(smallest_key, Some(b"key_a".to_vec()));
    assert_eq!(largest_key, Some(b"key_z".to_vec()));
}

#[test]
fn should_expand_bounds_with_range_tombstones() {
    // Arrange
    let entries = vec![make_entry(b"key_m", b"val", 1)];
    let range_tombstones = vec![
        (b"aaa".to_vec(), b"zzz".to_vec(), 2), // expands both bounds
    ];

    // Act
    let (smallest_key, largest_key, _, _) = compute_bounds(&entries, &range_tombstones);

    // Assert - range tombstone should expand bounds
    assert_eq!(smallest_key, Some(b"aaa".to_vec()));
    assert_eq!(largest_key, Some(b"zzz".to_vec()));
}
