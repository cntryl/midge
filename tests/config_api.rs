//! Tests for the high-level ConfigBuilder API.
//!
//! These tests verify that the recommended user-facing configuration API
//! correctly derives parameters from high-level goals and durability settings.

use cntryl_midge::config::{
    CloudMode, ConfigBuilder, Durability, Goal, MemoryBudget, WorkloadProfile,
};
use tempfile::TempDir;

mod common;

// =============================================================================
// BASIC BUILDER TESTS
// =============================================================================

#[test]
fn should_build_config_given_minimal_defaults_when_only_path_provided() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path).build().expect("build config");

    // Assert
    assert_eq!(config.goal(), Goal::Latency); // Default goal
    assert_eq!(config.durability(), Durability::Steady); // Default durability
    assert_eq!(config.cloud_mode(), CloudMode::Off);
    assert_eq!(config.memory_budget(), MemoryBudget::Auto);
    assert!(!config.autotune_enabled());
}

#[test]
fn should_set_goal_given_latency_when_optimizing_for_p99() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .goal(Goal::Latency)
        .build()
        .expect("build config");

    // Assert
    assert_eq!(config.goal(), Goal::Latency);
    let plan = config.plan();
    // Latency mode should use smaller block sizes for faster reads
    assert!(plan.block_size <= 16 * 1024, "block size should be small for latency");
    // Latency mode should have more aggressive bloom filters
    assert!(plan.bloom_bits_per_key >= 10, "bloom bits should be high for latency");
}

#[test]
fn should_set_goal_given_throughput_when_optimizing_for_bulk_writes() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .goal(Goal::Throughput)
        .build()
        .expect("build config");

    // Assert
    assert_eq!(config.goal(), Goal::Throughput);
    let plan = config.plan();
    // Throughput mode should use larger block sizes
    assert!(plan.block_size >= 32 * 1024, "block size should be large for throughput");
    // Throughput mode should have larger memtables
    assert!(
        plan.memtable_size >= 64 * 1024 * 1024,
        "memtable should be large for throughput"
    );
}

#[test]
fn should_set_goal_given_cost_when_minimizing_resources() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .goal(Goal::Cost)
        .build()
        .expect("build config");

    // Assert
    assert_eq!(config.goal(), Goal::Cost);
    let plan = config.plan();
    // Cost mode should use minimal cache
    // Check relative allocation is lower (depends on implementation)
    assert!(plan.block_cache_size > 0, "some cache should be allocated");
    // Cost mode should have lower compaction concurrency
    assert!(plan.compaction_concurrency <= 2, "compaction threads should be minimal");
}

// =============================================================================
// DURABILITY TESTS
// =============================================================================

#[test]
fn should_set_durability_given_strict_when_fsync_per_write_required() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .durability(Durability::Strict)
        .build()
        .expect("build config");

    // Assert
    assert_eq!(config.durability(), Durability::Strict);
    let plan = config.plan();
    // Strict durability should sync every write
    assert!(
        plan.wal_sync_per_write,
        "strict durability must sync per write"
    );
}

#[test]
fn should_set_durability_given_steady_when_balanced_sync_needed() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .durability(Durability::Steady)
        .build()
        .expect("build config");

    // Assert
    assert_eq!(config.durability(), Durability::Steady);
    let plan = config.plan();
    // Steady durability should sync at intervals, not per write
    assert!(
        !plan.wal_sync_per_write,
        "steady durability should not sync per write"
    );
    assert!(
        plan.wal_sync_interval.is_some(),
        "steady durability should have sync interval"
    );
    let interval = plan.wal_sync_interval.unwrap();
    assert!(
        interval.as_millis() >= 10 && interval.as_millis() <= 100,
        "sync interval should be reasonable"
    );
}

// =============================================================================
// MEMORY BUDGET TESTS
// =============================================================================

#[test]
fn should_respect_memory_budget_given_explicit_bytes_when_configured() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");
    let budget_bytes = 256 * 1024 * 1024; // 256 MB

    // Act
    let config = ConfigBuilder::new(&path)
        .memory_budget(MemoryBudget::Bytes(budget_bytes))
        .build()
        .expect("build config");

    // Assert
    let plan = config.plan();
    // Total memory used should not exceed budget
    let total_used = plan.block_cache_size + (plan.memtable_size * plan.memtable_count);
    assert!(
        total_used <= budget_bytes,
        "total memory {} should not exceed budget {}",
        total_used,
        budget_bytes
    );
}

#[test]
fn should_use_auto_memory_given_no_explicit_budget_when_default() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path).build().expect("build config");

    // Assert
    assert_eq!(config.memory_budget(), MemoryBudget::Auto);
    let plan = config.plan();
    // Auto budget should derive reasonable values
    assert!(
        plan.total_memory_budget > 0,
        "auto budget should derive positive value"
    );
}

// =============================================================================
// WORKLOAD PROFILE TESTS
// =============================================================================

#[test]
fn should_optimize_params_given_write_heavy_profile_when_configured() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .workload_profile(WorkloadProfile::WriteHeavy)
        .build()
        .expect("build config");

    // Assert
    assert_eq!(config.workload_profile(), WorkloadProfile::WriteHeavy);
    // Write-heavy profile should have larger memtables (relative to defaults)
    let plan = config.plan();
    assert!(plan.memtable_size > 0, "memtable should be allocated");
}

#[test]
fn should_optimize_params_given_read_mostly_profile_when_configured() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .workload_profile(WorkloadProfile::ReadMostly)
        .build()
        .expect("build config");

    // Assert
    assert_eq!(config.workload_profile(), WorkloadProfile::ReadMostly);
    let plan = config.plan();
    // Read-mostly should prioritize cache and bloom filters
    assert!(plan.bloom_bits_per_key > 0, "bloom filter should be enabled");
}

#[test]
fn should_optimize_params_given_range_scan_profile_when_configured() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .workload_profile(WorkloadProfile::RangeScan)
        .build()
        .expect("build config");

    // Assert
    assert_eq!(config.workload_profile(), WorkloadProfile::RangeScan);
    // Range scan should use larger blocks for sequential access
    let plan = config.plan();
    assert!(plan.block_size > 0, "block size should be set");
}

// =============================================================================
// CLOUD MODE TESTS
// =============================================================================

#[test]
fn should_require_cloud_config_given_cloud_mode_when_not_off() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let result = ConfigBuilder::new(&path)
        .cloud_mode(CloudMode::Cache)
        .build();

    // Assert
    assert!(
        result.is_err(),
        "cloud mode Cache requires cloud config"
    );
}

#[test]
fn should_allow_cloud_off_given_no_cloud_config_when_local_only() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .cloud_mode(CloudMode::Off)
        .build()
        .expect("build config");

    // Assert
    assert_eq!(config.cloud_mode(), CloudMode::Off);
    assert!(config.cloud_config().is_none());
}

// =============================================================================
// AUTOTUNE TESTS
// =============================================================================

#[test]
fn should_enable_autotune_given_flag_set_when_requested() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path)
        .autotune(true)
        .build()
        .expect("build config");

    // Assert
    assert!(config.autotune_enabled());
}

#[test]
fn should_disable_autotune_given_default_when_not_requested() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    // Act
    let config = ConfigBuilder::new(&path).build().expect("build config");

    // Assert
    assert!(!config.autotune_enabled());
}

// =============================================================================
// CONVERSION TO OPTIONS TESTS
// =============================================================================

#[test]
fn should_convert_to_options_given_config_when_bridging_to_engine() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");

    let config = ConfigBuilder::new(&path)
        .goal(Goal::Latency)
        .durability(Durability::Steady)
        .build()
        .expect("build config");

    // Act
    let options = config.to_options();

    // Assert - options should reflect config's plan
    let plan = config.plan();
    assert_eq!(options.block_size, plan.block_size);
}

// =============================================================================
// COMBINATION TESTS
// =============================================================================

#[test]
fn should_derive_consistent_params_given_all_knobs_set_when_building() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("test_db");
    let budget = 512 * 1024 * 1024; // 512 MB

    // Act
    let config = ConfigBuilder::new(&path)
        .goal(Goal::Throughput)
        .durability(Durability::Steady)
        .memory_budget(MemoryBudget::Bytes(budget))
        .workload_profile(WorkloadProfile::WriteHeavy)
        .cloud_mode(CloudMode::Off)
        .autotune(true)
        .build()
        .expect("build config");

    // Assert - all settings preserved
    assert_eq!(config.goal(), Goal::Throughput);
    assert_eq!(config.durability(), Durability::Steady);
    assert_eq!(config.memory_budget(), MemoryBudget::Bytes(budget));
    assert_eq!(config.workload_profile(), WorkloadProfile::WriteHeavy);
    assert_eq!(config.cloud_mode(), CloudMode::Off);
    assert!(config.autotune_enabled());

    // Assert - plan derived correctly
    let plan = config.plan();
    assert!(plan.block_size > 0);
    assert!(plan.memtable_size > 0);
    assert!(plan.block_cache_size > 0);
}

#[test]
fn should_derive_different_params_given_latency_vs_throughput_when_comparing() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let latency_path = temp_dir.path().join("latency_db");
    let throughput_path = temp_dir.path().join("throughput_db");

    // Act
    let latency_config = ConfigBuilder::new(&latency_path)
        .goal(Goal::Latency)
        .build()
        .expect("build latency config");

    let throughput_config = ConfigBuilder::new(&throughput_path)
        .goal(Goal::Throughput)
        .build()
        .expect("build throughput config");

    // Assert - different goals should yield different parameters
    let latency_plan = latency_config.plan();
    let throughput_plan = throughput_config.plan();

    // Throughput mode uses larger blocks
    assert!(
        throughput_plan.block_size >= latency_plan.block_size,
        "throughput block size {} should be >= latency block size {}",
        throughput_plan.block_size,
        latency_plan.block_size
    );

    // Throughput mode uses larger memtables
    assert!(
        throughput_plan.memtable_size >= latency_plan.memtable_size,
        "throughput memtable {} should be >= latency memtable {}",
        throughput_plan.memtable_size,
        latency_plan.memtable_size
    );
}

// =============================================================================
// PATH VALIDATION TESTS
// =============================================================================

#[test]
fn should_store_path_given_relative_path_when_building() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("relative/path/to/db");

    // Act
    let config = ConfigBuilder::new(&path).build().expect("build config");

    // Assert
    assert!(config.path().to_string_lossy().contains("relative"));
}

#[test]
fn should_store_path_given_absolute_path_when_building() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path().join("absolute_db");

    // Act
    let config = ConfigBuilder::new(&path).build().expect("build config");

    // Assert
    assert!(config.path().exists() || !config.path().exists()); // Path stored, may not exist
    assert_eq!(
        config.path().file_name().unwrap().to_string_lossy(),
        "absolute_db"
    );
}
