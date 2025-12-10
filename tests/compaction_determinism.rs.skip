//! Integration tests for deterministic compaction planning and replay.
//!
//! These tests validate the complete Phase 2 compaction pipeline:
//! - Deterministic plan generation from manifest
//! - Durable persistence to log
//! - Crash recovery and replay
//! - Consistency validation

use cntryl_midge::core::compaction::{
    CompactionLogManager, CompactionPlan, CompactionTask, Planner,
};
use cntryl_midge::core::manifest::{FileMeta, Manifest};
use tempfile::TempDir;

/// Helper to create a test manifest with files at specified levels
fn create_test_manifest(files: Vec<(u32, u32, u64)>) -> Manifest {
    let mut manifest = Manifest::default();

    for (cf_id, level, size_bytes) in files {
        manifest.files.push(FileMeta {
            name: format!("sst_{:06}.blob", manifest.files.len()),
            cf_id,
            level,
            sst_seq: manifest.files.len() as u64,
            size_bytes,
            smallest_key: Some(format!("key_{}_min", manifest.files.len()).into_bytes()),
            largest_key: Some(format!("key_{}_max", manifest.files.len()).into_bytes()),
            smallest_seq: None,
            largest_seq: None,
            sublevel: 0,
            cloud_location: None,
            cloud_checksum: None,
            cloud_uploaded_at: None,
            cloud_state: None,
            point_tombstone_count: 0,
            range_tombstone_count: 0,
            total_entries: 0,
        });
    }

    manifest
}

/// Test: Deterministic plan generation from same manifest
#[test]
fn should_generate_deterministic_plans_given_same_manifest() {
    // Arrange
    let planner = Planner::new();
    let manifest = create_test_manifest(vec![
        (0, 0, 2 * 1024 * 1024),  // CF 0, L0, 2MB
        (0, 0, 2 * 1024 * 1024),  // CF 0, L0, 2MB (total 4MB > 4MB threshold)
        (0, 1, 10 * 1024 * 1024), // CF 0, L1, 10MB
    ]);

    // Act
    let plans1 = planner.plan(&manifest);
    let plans2 = planner.plan(&manifest);

    // Assert
    assert_eq!(
        plans1.len(),
        plans2.len(),
        "Plan count must be deterministic"
    );

    for (p1, p2) in plans1.iter().zip(plans2.iter()) {
        assert_eq!(p1.source_level, p2.source_level);
        assert_eq!(p1.target_level, p2.target_level);
        assert_eq!(p1.cf_id, p2.cf_id);
        assert_eq!(p1.input_files.len(), p2.input_files.len());
    }
}

/// Test: Compaction log persists and recovers tasks
#[test]
fn should_persist_compaction_tasks_to_log() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let log_manager = CompactionLogManager::new(temp_dir.path());

    let plan1 = CompactionPlan {
        source_level: 0,
        target_level: 1,
        cf_id: 0,
        input_files: vec!["sst_001.blob".to_string(), "sst_002.blob".to_string()],
        output_files: Vec::new(),
    };

    let plan2 = CompactionPlan {
        source_level: 1,
        target_level: 2,
        cf_id: 0,
        input_files: vec!["sst_003.blob".to_string()],
        output_files: Vec::new(),
    };

    let task1 = CompactionTask::new(1, &plan1);
    let task2 = CompactionTask::new(2, &plan2);

    // Act
    log_manager.append(&task1).unwrap();
    log_manager.append(&task2).unwrap();
    let recovered = log_manager.load().unwrap();

    // Assert
    assert_eq!(recovered.len(), 2);
    assert_eq!(recovered[0].task_id, 1);
    assert_eq!(recovered[0].cf_id, 0);
    assert_eq!(recovered[0].source_level, 0);
    assert_eq!(recovered[0].target_level, 1);
    assert_eq!(recovered[0].input_files.len(), 2);

    assert_eq!(recovered[1].task_id, 2);
    assert_eq!(recovered[1].source_level, 1);
    assert_eq!(recovered[1].target_level, 2);
}

/// Test: Log can be cleared after successful checkpoint
#[test]
fn should_clear_log_after_successful_checkpoint() {
    // Arrange
    let temp_dir = TempDir::new().unwrap();
    let log_manager = CompactionLogManager::new(temp_dir.path());

    let plan = CompactionPlan {
        source_level: 0,
        target_level: 1,
        cf_id: 0,
        input_files: vec!["sst_001.blob".to_string()],
        output_files: Vec::new(),
    };
    let task = CompactionTask::new(1, &plan);

    // Act
    log_manager.append(&task).unwrap();
    let recovered_before = log_manager.load().unwrap();
    log_manager.clear().unwrap();
    let recovered_after = log_manager.load().unwrap();

    // Assert
    assert_eq!(recovered_before.len(), 1);
    assert_eq!(recovered_after.len(), 0);
}

/// Test: Multiple column families generate ordered plans
#[test]
fn should_generate_plans_in_cf_id_order_for_multi_cf_engine() {
    // Arrange
    let planner = Planner::new();
    let manifest = create_test_manifest(vec![
        // CF 2 files (should be processed last)
        (2, 0, 2 * 1024 * 1024),
        (2, 0, 2 * 1024 * 1024),
        // CF 0 files (should be processed first)
        (0, 0, 2 * 1024 * 1024),
        (0, 0, 2 * 1024 * 1024),
        // CF 1 files (should be processed second)
        (1, 0, 2 * 1024 * 1024),
        (1, 0, 2 * 1024 * 1024),
    ]);

    // Act
    let plans = planner.plan(&manifest);
    let cf_order: Vec<u32> = plans.iter().map(|p| p.cf_id).collect();
    let mut cf_order_sorted = cf_order.clone();
    cf_order_sorted.sort();

    // Assert
    assert_eq!(cf_order, cf_order_sorted, "Plans must be ordered by CF ID");
}

/// Test: Empty manifest produces empty plan
#[test]
fn should_return_empty_plan_for_empty_manifest() {
    // Arrange
    let planner = Planner::new();
    let manifest = Manifest::default();

    // Act
    let plans = planner.plan(&manifest);

    // Assert
    assert_eq!(plans.len(), 0);
}

/// Test: Plans below threshold produce no compaction
#[test]
fn should_not_plan_compaction_when_below_thresholds() {
    // Arrange
    let planner = Planner::new();
    let manifest = create_test_manifest(vec![
        (0, 0, 1024 * 1024), // CF 0, L0, 1MB (below threshold)
        (0, 0, 1024 * 1024), // CF 0, L0, 1MB (total 2MB < 4MB)
    ]);

    // Act
    let plans = planner.plan(&manifest);

    // Assert
    assert_eq!(
        plans.len(),
        0,
        "Should not plan L0 compaction when below threshold"
    );
}
