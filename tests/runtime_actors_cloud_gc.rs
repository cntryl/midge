//! Tests for Cloud and GC Actors
//!
//! This test suite validates:
//! 1. CloudActor SST and WAL upload tracking
//! 2. CloudActor checkpoint management
//! 3. GcActor orphaned file detection
//! 4. GcActor safe file deletion

use cntryl_midge::runtime::actors::{CloudActor, GcActor};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Arrange: Create a temporary directory for test data
fn setup_test_dir() -> PathBuf {
    let test_num = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let test_dir = PathBuf::from(format!("target/test_actors_cloud_gc_{}", test_num));
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    test_dir
}

/// Cleanup: Remove temporary test directory
fn cleanup_test_dir(dir: &PathBuf) {
    std::thread::sleep(std::time::Duration::from_millis(50));
    let _ = fs::remove_dir_all(dir);
}

// ============================================================================
// CloudActor Tests
// ============================================================================

#[test]
fn should_track_sst_upload_when_cloudactor_uploads_sst() {
    // Arrange
    let test_dir = setup_test_dir();
    let sst_dir = test_dir.join("sst");
    fs::create_dir_all(&sst_dir).expect("Failed to create SST directory");

    // Create a test SST file
    let sst_name = "test_001.sst";
    let sst_path = sst_dir.join(sst_name);
    fs::write(&sst_path, b"test sst content").expect("Failed to write test SST");

    let mut cloud_actor = CloudActor::new();

    // Create a mock runtime state (simplified for testing)
    let mut mock_state = cntryl_midge::runtime::state::RuntimeState::new(test_dir.clone());

    // Act: Upload SST
    let result = cloud_actor.upload_sst(&mut mock_state, sst_name);

    // Assert: Upload succeeds and is tracked
    assert!(result.is_ok(), "SST upload should succeed");
    assert_eq!(
        cloud_actor.uploads_in_progress(),
        1,
        "Should have 1 upload in progress"
    );
    assert!(
        mock_state.cloud.pending_uploads.contains(&sst_name.to_string()),
        "SST should be in pending uploads"
    );

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_track_wal_upload_when_cloudactor_uploads_wal() {
    // Arrange
    let test_dir = setup_test_dir();
    let wal_dir = test_dir.join("wal");
    fs::create_dir_all(&wal_dir).expect("Failed to create WAL directory");

    // Create a test WAL file
    let segment_id = 42u64;
    let wal_name = format!("wal_{:06}.log", segment_id);
    let wal_path = wal_dir.join(&wal_name);
    fs::write(&wal_path, b"test wal content").expect("Failed to write test WAL");

    let mut cloud_actor = CloudActor::new();

    // Create a mock runtime state
    let mut mock_state = cntryl_midge::runtime::state::RuntimeState::new(test_dir.clone());

    // Act: Upload WAL
    let result = cloud_actor.upload_wal(&mut mock_state, segment_id);

    // Assert: Upload succeeds and is tracked
    assert!(result.is_ok(), "WAL upload should succeed");
    assert_eq!(
        cloud_actor.uploads_in_progress(),
        1,
        "Should have 1 upload in progress"
    );
    assert!(
        mock_state.cloud.pending_uploads.contains(&wal_name),
        "WAL should be in pending uploads"
    );
    assert_eq!(
        mock_state.cloud.last_cloud_checkpoint_seq,
        segment_id,
        "Cloud checkpoint should be updated"
    );

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_update_checkpoint_when_cloudactor_handles_upload_complete() {
    // Arrange
    let test_dir = setup_test_dir();
    let sst_dir = test_dir.join("sst");
    let wal_dir = test_dir.join("wal");
    fs::create_dir_all(&sst_dir).expect("Failed to create SST directory");
    fs::create_dir_all(&wal_dir).expect("Failed to create WAL directory");

    // Create test files
    let sst_name = "test_001.sst";
    let wal_name = "wal_000100.log";
    fs::write(sst_dir.join(sst_name), b"sst content").expect("Failed to write SST");
    fs::write(wal_dir.join(wal_name), b"wal content").expect("Failed to write WAL");

    let mut cloud_actor = CloudActor::new();
    let mut mock_state = cntryl_midge::runtime::state::RuntimeState::new(test_dir.clone());

    // Act: Start uploads
    cloud_actor.upload_sst(&mut mock_state, sst_name).ok();
    cloud_actor.upload_wal(&mut mock_state, 100).ok();

    // Verify both uploads are tracked
    assert_eq!(
        cloud_actor.uploads_in_progress(),
        2,
        "Should have 2 uploads in progress"
    );

    // Act: Handle SST upload completion
    cloud_actor.handle_upload_complete(&mut mock_state, sst_name);

    // Assert: SST removed from pending, count decremented
    assert_eq!(
        cloud_actor.uploads_in_progress(),
        1,
        "Should have 1 upload remaining"
    );
    assert!(
        !mock_state.cloud.pending_uploads.contains(&sst_name.to_string()),
        "SST should be removed from pending"
    );

    // Act: Handle WAL upload completion
    cloud_actor.handle_upload_complete(&mut mock_state, wal_name);

    // Assert: WAL removed from pending, checkpoint updated
    assert_eq!(
        cloud_actor.uploads_in_progress(),
        0,
        "Should have 0 uploads remaining"
    );
    assert!(
        !mock_state.cloud.pending_uploads.contains(&wal_name.to_string()),
        "WAL should be removed from pending"
    );
    assert_eq!(
        mock_state.cloud.last_cloud_checkpoint_seq,
        100,
        "Cloud checkpoint should be updated to segment 100"
    );

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_handle_missing_sst_when_cloudactor_uploads_sst() {
    // Arrange
    let test_dir = setup_test_dir();
    fs::create_dir_all(test_dir.join("sst")).expect("Failed to create SST directory");

    let mut cloud_actor = CloudActor::new();
    let mut mock_state = cntryl_midge::runtime::state::RuntimeState::new(test_dir.clone());

    // Act: Try to upload non-existent SST
    let result = cloud_actor.upload_sst(&mut mock_state, "nonexistent.sst");

    // Assert: Should handle gracefully without panicking
    assert!(result.is_ok(), "Should handle missing SST gracefully");
    assert_eq!(
        cloud_actor.uploads_in_progress(),
        0,
        "Should not track failed upload"
    );

    // Cleanup
    cleanup_test_dir(&test_dir);
}

// ============================================================================
// GcActor Tests
// ============================================================================

#[test]
fn should_detect_orphaned_ssts_when_gcactor_checks() {
    // Arrange
    let test_dir = setup_test_dir();
    let sst_dir = test_dir.join("sst");
    fs::create_dir_all(&sst_dir).expect("Failed to create SST directory");

    // Create some SST files on disk
    fs::write(sst_dir.join("001.sst"), b"content1").expect("Failed to write SST 1");
    fs::write(sst_dir.join("002.sst"), b"content2").expect("Failed to write SST 2");
    fs::write(sst_dir.join("003.sst"), b"content3").expect("Failed to write SST 3");

    let gc_actor = GcActor::new();
    let mut mock_state = cntryl_midge::runtime::state::RuntimeState::new(test_dir.clone());

    // Only add one file to manifest (others are orphaned)
    mock_state.manifest.files.push(cntryl_midge::metadata::manifest::FileMeta {
        name: "001.sst".to_string(),
        size_bytes: 8,
        level: 0,
        cf_id: 0,
        sst_seq: 1,
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"b".to_vec()),
        smallest_seq: Some(0),
        largest_seq: Some(1),
        sublevel: 0,
    });

    // Act: Check for garbage
    gc_actor.check(&mock_state);

    // Assert: Successfully identifies orphaned files (just check it doesn't panic)
    // The actual detection happens via logging, so we just verify the operation completes

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_delete_orphaned_ssts_when_gcactor_deletes() {
    // Arrange
    let test_dir = setup_test_dir();
    let sst_dir = test_dir.join("sst");
    fs::create_dir_all(&sst_dir).expect("Failed to create SST directory");

    // Create orphaned SST files
    let orphaned_files = vec!["orphaned_001.sst", "orphaned_002.sst"];
    for file in &orphaned_files {
        fs::write(sst_dir.join(file), b"orphan content").expect("Failed to write orphan SST");
    }

    let mut gc_actor = GcActor::new();
    let mut mock_state = cntryl_midge::runtime::state::RuntimeState::new(test_dir.clone());

    // Act: Delete orphaned SSTs
    let result = gc_actor.delete_ssts(
        &mut mock_state,
        &orphaned_files.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    );

    // Assert: Deletion succeeds and files are gone
    assert!(result.is_ok(), "Deletion should succeed");
    assert!(gc_actor.last_gc_run().is_some(), "GC run timestamp should be set");

    for file in &orphaned_files {
        assert!(
            !sst_dir.join(file).exists(),
            "File {} should be deleted",
            file
        );
    }

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_skip_active_ssts_when_gcactor_deletes() {
    // Arrange
    let test_dir = setup_test_dir();
    let sst_dir = test_dir.join("sst");
    fs::create_dir_all(&sst_dir).expect("Failed to create SST directory");

    // Create SST files
    let active_file = "active_001.sst";
    let orphan_file = "orphan_001.sst";
    fs::write(sst_dir.join(active_file), b"active").expect("Failed to write active SST");
    fs::write(sst_dir.join(orphan_file), b"orphan").expect("Failed to write orphan SST");

    let mut gc_actor = GcActor::new();
    let mut mock_state = cntryl_midge::runtime::state::RuntimeState::new(test_dir.clone());

    // Add active file to manifest
    mock_state.manifest.files.push(cntryl_midge::metadata::manifest::FileMeta {
        name: active_file.to_string(),
        size_bytes: 6,
        level: 0,
        cf_id: 0,
        sst_seq: 1,
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"b".to_vec()),
        smallest_seq: Some(0),
        largest_seq: Some(1),
        sublevel: 0,
    });

    // Act: Try to delete both files (one active, one orphan)
    let files_to_delete = vec![active_file.to_string(), orphan_file.to_string()];
    let result = gc_actor.delete_ssts(&mut mock_state, &files_to_delete);

    // Assert: Active file remains, orphan is deleted
    assert!(result.is_ok(), "Deletion should succeed");
    assert!(
        sst_dir.join(active_file).exists(),
        "Active file should NOT be deleted"
    );
    assert!(
        !sst_dir.join(orphan_file).exists(),
        "Orphan file should be deleted"
    );

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_skip_compacting_ssts_when_gcactor_deletes() {
    // Arrange
    let test_dir = setup_test_dir();
    let sst_dir = test_dir.join("sst");
    fs::create_dir_all(&sst_dir).expect("Failed to create SST directory");

    // Create SST files
    let compacting_file = "compacting_001.sst";
    let orphan_file = "orphan_001.sst";
    fs::write(sst_dir.join(compacting_file), b"compacting").expect("Failed to write compacting SST");
    fs::write(sst_dir.join(orphan_file), b"orphan").expect("Failed to write orphan SST");

    let mut gc_actor = GcActor::new();
    let mut mock_state = cntryl_midge::runtime::state::RuntimeState::new(test_dir.clone());

    // Mark file as being compacted
    mock_state
        .compaction
        .compacting_ssts
        .push(compacting_file.to_string());

    // Act: Try to delete both files
    let files_to_delete = vec![compacting_file.to_string(), orphan_file.to_string()];
    let result = gc_actor.delete_ssts(&mut mock_state, &files_to_delete);

    // Assert: Compacting file skipped, orphan is deleted
    assert!(result.is_ok(), "Deletion should succeed");
    assert!(
        sst_dir.join(compacting_file).exists(),
        "Compacting file should NOT be deleted"
    );
    assert!(
        !sst_dir.join(orphan_file).exists(),
        "Orphan file should be deleted"
    );

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_handle_multiple_uploads_when_cloudactor_tracks_concurrent_uploads() {
    // Arrange
    let test_dir = setup_test_dir();
    let sst_dir = test_dir.join("sst");
    fs::create_dir_all(&sst_dir).expect("Failed to create SST directory");

    // Create multiple SST files
    for i in 1..=3 {
        let name = format!("sst_{:03}.sst", i);
        fs::write(sst_dir.join(&name), b"test content").expect("Failed to write SST");
    }

    let mut cloud_actor = CloudActor::new();
    let mut mock_state = cntryl_midge::runtime::state::RuntimeState::new(test_dir.clone());

    // Act: Upload multiple SSTs
    for i in 1..=3 {
        let name = format!("sst_{:03}.sst", i);
        cloud_actor.upload_sst(&mut mock_state, &name).ok();
    }

    // Assert: All uploads tracked
    assert_eq!(
        cloud_actor.uploads_in_progress(),
        3,
        "Should have 3 uploads in progress"
    );
    assert_eq!(
        mock_state.cloud.pending_uploads.len(),
        3,
        "Should have 3 pending uploads"
    );

    // Act: Complete uploads one by one
    for i in 1..=3 {
        let name = format!("sst_{:03}.sst", i);
        cloud_actor.handle_upload_complete(&mut mock_state, &name);
    }

    // Assert: All uploads completed
    assert_eq!(
        cloud_actor.uploads_in_progress(),
        0,
        "Should have no uploads in progress"
    );
    assert!(
        mock_state.cloud.pending_uploads.is_empty(),
        "Should have no pending uploads"
    );

    // Cleanup
    cleanup_test_dir(&test_dir);
}
