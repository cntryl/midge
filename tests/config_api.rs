//! Config API Integration Tests
//!
//! Tests the builder-based configuration system that derives low-level parameters
//! from high-level optimization goals (latency/throughput/cost), durability
//! requirements, memory budgets, and workload profiles.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! These tests validate the config builder's behavior without requiring an
//! engine instance, since configuration is orthogonal to storage modes.

use cntryl_midge::{Durability, Goal, MemoryBudget, OpenOptions, WorkloadProfile};
use std::path::PathBuf;

// ============================================================================
// BUILDER INITIALIZATION TESTS
// ============================================================================

#[test]
fn should_build_config_given_minimal_defaults_when_only_path_provided() {
    // Arrange & Act
    let opts = OpenOptions::new().path("./test_db").build();

    // Assert
    assert_eq!(opts.path, PathBuf::from("./test_db"));
    assert_eq!(opts.goal, Goal::Latency);
    assert_eq!(opts.durability, Durability::Steady);
    assert_eq!(opts.memory_budget, MemoryBudget::Auto);
    assert_eq!(opts.workload, WorkloadProfile::Mixed);
}

// ============================================================================
// GOAL SETTING TESTS
// ============================================================================

#[test]
fn should_set_goal_given_latency_when_optimizing_for_p99() {
    // Arrange & Act
    let opts = OpenOptions::new().goal(Goal::Latency).build();

    // Assert
    assert_eq!(opts.goal, Goal::Latency);
    assert!(
        opts.block_size() <= 32 * 1024,
        "Latency goal should use small blocks"
    );
}

#[test]
fn should_set_goal_given_throughput_when_optimizing_for_bulk_writes() {
    // Arrange & Act
    let opts = OpenOptions::new().goal(Goal::Throughput).build();

    // Assert
    assert_eq!(opts.goal, Goal::Throughput);
    assert!(
        opts.block_size() >= 64 * 1024,
        "Throughput goal should use larger blocks"
    );
}

#[test]
fn should_set_goal_given_cost_when_minimizing_resources() {
    // Arrange & Act
    let opts = OpenOptions::new().goal(Goal::Cost).build();

    // Assert
    assert_eq!(opts.goal, Goal::Cost);
    // Cost should allocate less to cache and memtables
    assert!(
        opts.block_cache_size() <= 256 * 1024 * 1024,
        "Cost should limit cache"
    );
}

// ============================================================================
// DURABILITY SETTING TESTS
// ============================================================================

#[test]
fn should_set_durability_given_strict_when_fsync_per_write_required() {
    // Arrange & Act
    let opts = OpenOptions::new().durability(Durability::Strict).build();

    // Assert
    assert_eq!(opts.durability, Durability::Strict);
    assert!(
        opts.wal_sync_on_write(),
        "Strict durability must sync on every write"
    );
}

#[test]
fn should_set_durability_given_steady_when_balanced_sync_needed() {
    // Arrange & Act
    let opts = OpenOptions::new().durability(Durability::Steady).build();

    // Assert
    assert_eq!(opts.durability, Durability::Steady);
    assert!(
        !opts.wal_sync_on_write(),
        "Steady durability should not sync every write"
    );
}

// ============================================================================
// MEMORY BUDGET TESTS
// ============================================================================

#[test]
fn should_respect_memory_budget_given_explicit_bytes_when_configured() {
    // Arrange
    let budget = MemoryBudget::Bytes(256 * 1024 * 1024); // 256MB

    // Act
    let opts = OpenOptions::new().memory_budget(budget).build();

    // Assert
    assert_eq!(opts.memory_budget, budget);
    assert!(
        opts.block_cache_size() > 0,
        "Cache should be allocated from budget"
    );
}

#[test]
fn should_use_auto_memory_given_no_explicit_budget_when_default() {
    // Arrange & Act
    let opts = OpenOptions::new().build();

    // Assert
    assert_eq!(opts.memory_budget, MemoryBudget::Auto);
    // Auto should pick a sensible default
    assert!(
        opts.block_cache_size() > 0,
        "Auto budget should still allocate cache"
    );
}

// ============================================================================
// WORKLOAD PROFILE OPTIMIZATION TESTS
// ============================================================================

#[test]
fn should_optimize_params_given_write_heavy_profile_when_configured() {
    // Arrange
    let normal = OpenOptions::new().workload(WorkloadProfile::Mixed).build();

    // Act
    let write_heavy = OpenOptions::new()
        .workload(WorkloadProfile::WriteHeavy)
        .build();

    // Assert
    assert_eq!(write_heavy.workload, WorkloadProfile::WriteHeavy);
    assert!(
        write_heavy.memtable_size_limit() >= normal.memtable_size_limit(),
        "Write-heavy should have larger memtables"
    );
}

#[test]
fn should_optimize_params_given_read_mostly_profile_when_configured() {
    // Arrange
    let normal = OpenOptions::new().workload(WorkloadProfile::Mixed).build();

    // Act
    let read_mostly = OpenOptions::new()
        .workload(WorkloadProfile::ReadMostly)
        .build();

    // Assert
    assert_eq!(read_mostly.workload, WorkloadProfile::ReadMostly);
    assert!(
        read_mostly.block_cache_size() >= normal.block_cache_size(),
        "Read-mostly should prioritize cache"
    );
}

#[test]
fn should_optimize_params_given_range_scan_profile_when_configured() {
    // Arrange
    let normal = OpenOptions::new().build();

    // Act
    let range_scan = OpenOptions::new()
        .workload(WorkloadProfile::RangeScan)
        .build();

    // Assert
    assert_eq!(range_scan.workload, WorkloadProfile::RangeScan);
    assert!(
        range_scan.block_size() >= normal.block_size(),
        "Range scan should use larger blocks"
    );
}

// ============================================================================
// INTERACTION TESTS (Multiple Knobs)
// ============================================================================

#[test]
fn should_derive_consistent_params_given_all_knobs_set_when_building() {
    // Arrange & Act
    let opts = OpenOptions::new()
        .path("./integrated_db")
        .goal(Goal::Throughput)
        .durability(Durability::Strict)
        .memory_budget(MemoryBudget::Bytes(1024 * 1024 * 1024)) // 1GB
        .workload(WorkloadProfile::WriteHeavy)
        .build();

    // Assert
    assert_eq!(opts.goal, Goal::Throughput);
    assert_eq!(opts.durability, Durability::Strict);
    assert!(opts.wal_sync_on_write());
    assert!(
        opts.memtable_size_limit() > 64 * 1024 * 1024,
        "Write-heavy + throughput should have large memtables"
    );
    assert_eq!(opts.path, PathBuf::from("./integrated_db"));
}

#[test]
fn should_derive_different_params_given_latency_vs_throughput_when_comparing() {
    // Arrange
    let latency_opts = OpenOptions::new().goal(Goal::Latency).build();

    // Act
    let throughput_opts = OpenOptions::new().goal(Goal::Throughput).build();

    // Assert
    assert_ne!(
        latency_opts.block_size(),
        throughput_opts.block_size(),
        "Latency and throughput should use different block sizes"
    );
    assert_ne!(
        latency_opts.memtable_size_limit(),
        throughput_opts.memtable_size_limit(),
        "Latency and throughput should use different memtable sizes"
    );
    assert_ne!(
        latency_opts.target_sst_size(),
        throughput_opts.target_sst_size(),
        "Latency and throughput should use different target SST sizes"
    );
}

// ============================================================================
// GETTER TESTS
// ============================================================================

#[test]
fn should_provide_getter_access_given_derived_params_when_querying() {
    // Arrange & Act
    let opts = OpenOptions::new().build();

    // Assert - all getters should return positive values
    assert!(opts.block_size() > 0);
    assert!(opts.memtable_size_limit() > 0);
    assert!(opts.target_sst_size() > 0);
    assert!(opts.block_cache_size() > 0);
    assert!(opts.wal_buffer_size() > 0);
    assert!(opts.l0_compaction_trigger() > 0);
}

// ============================================================================
// PATH HANDLING TESTS
// ============================================================================

#[test]
fn should_store_path_given_relative_path_when_building() {
    // Arrange & Act
    let opts = OpenOptions::new().path("./relative/path").build();

    // Assert
    assert_eq!(opts.path, PathBuf::from("./relative/path"));
}

#[test]
fn should_store_path_given_absolute_path_when_building() {
    // Arrange & Act
    let opts = OpenOptions::new().path("/absolute/path/to/db").build();

    // Assert
    assert_eq!(opts.path, PathBuf::from("/absolute/path/to/db"));
}

// ============================================================================
// CLONE AND DEFAULT TESTS
// ============================================================================

#[test]
fn should_clone_options_preserving_all_settings_given_configured_opts_when_cloning() {
    // Arrange
    let original = OpenOptions::new()
        .path("./db")
        .goal(Goal::Throughput)
        .durability(Durability::Strict)
        .workload(WorkloadProfile::WriteHeavy)
        .build();

    // Act
    let cloned = original.clone();

    // Assert
    assert_eq!(cloned.path, original.path);
    assert_eq!(cloned.goal, original.goal);
    assert_eq!(cloned.durability, original.durability);
    assert_eq!(cloned.workload, original.workload);
    assert_eq!(cloned.block_size(), original.block_size());
    assert_eq!(cloned.memtable_size_limit(), original.memtable_size_limit());
}

#[test]
fn should_use_sensible_defaults_given_no_configuration_when_using_default() {
    // Arrange & Act
    let opts = OpenOptions::default();

    // Assert
    assert_eq!(opts.goal, Goal::Latency);
    assert_eq!(opts.durability, Durability::Steady);
    assert_eq!(opts.memory_budget, MemoryBudget::Auto);
    assert_eq!(opts.workload, WorkloadProfile::Mixed);
}
