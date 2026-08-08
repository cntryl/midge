//! Config API Integration Tests
//!
//! Tests the builder-based configuration system that derives low-level parameters
//! from high-level optimization goals (latency/throughput/cost), memory budgets,
//! and workload profiles.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! These tests validate the config builder's behavior without requiring an
//! engine instance, since configuration is orthogonal to storage modes.

use cntryl_midge::{
    BlockCachePolicy, Goal, MemoryBudget, MidgeError, MidgeResult, OpenOptions, OpenOptionsBuilder,
    Storage, WorkloadProfile,
};
use std::path::PathBuf;
use std::time::Duration;

// ============================================================================
// BUILDER INITIALIZATION TESTS
// ============================================================================

#[test]
fn should_require_finalization_when_constructing_open_options() {
    // Arrange

    // Act
    let builder: OpenOptionsBuilder = OpenOptions::in_memory();
    let finalized: MidgeResult<OpenOptions> = builder.build();

    // Assert
    assert!(finalized.is_ok());
}

#[test]
fn should_derive_same_pools_when_builder_calls_are_reordered() {
    // Arrange
    let budget = MemoryBudget::Bytes(512 * 1024 * 1024);

    // Act
    let first = OpenOptions::in_memory()
        .goal(Goal::Throughput)
        .workload(WorkloadProfile::WriteHeavy)
        .memory_budget(budget)
        .build()
        .expect("build first order");
    let second = OpenOptions::in_memory()
        .memory_budget(budget)
        .workload(WorkloadProfile::WriteHeavy)
        .goal(Goal::Throughput)
        .build()
        .expect("build second order");

    // Assert
    assert_eq!(first.memtable_size_limit(), second.memtable_size_limit());
    assert_eq!(first.block_cache_size(), second.block_cache_size());
    assert_eq!(
        first.transaction_memory_pool_size(),
        second.transaction_memory_pool_size()
    );
}

#[test]
fn should_keep_accounted_pools_within_total_given_tiny_and_normal_budgets() {
    // Arrange
    let budgets = [3usize, 9, 10, 31, 1024, 128 * 1024, 512 * 1024 * 1024];

    for budget in budgets {
        // Act
        let opts = OpenOptions::in_memory()
            .memory_budget(MemoryBudget::Bytes(budget))
            .build()
            .expect("derive bounded pools");
        let accounted = opts
            .memtable_size_limit()
            .saturating_mul(2)
            .saturating_add(opts.block_cache_size())
            .saturating_add(opts.transaction_memory_pool_size());

        // Assert
        assert!(
            accounted <= opts.memory_budget_bytes(),
            "budget={budget} accounted={accounted}"
        );
        assert!(
            opts.transaction_memory_pool_size() > 0,
            "budget={budget} must retain a nonzero transaction pool"
        );
    }
}

#[test]
fn should_allocate_ten_percent_to_transactions_given_default_pool() {
    // Arrange
    let budget = 1_000usize;

    // Act
    let opts = OpenOptions::in_memory()
        .memory_budget(MemoryBudget::Bytes(budget))
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.transaction_memory_pool_size(), 100);
}

#[test]
fn should_reject_transaction_pool_given_override_exceeds_total_budget() {
    // Arrange

    // Act
    let error = OpenOptions::in_memory()
        .memory_budget(MemoryBudget::Bytes(1024))
        .transaction_memory_pool_size(1025)
        .build()
        .expect_err("oversized pool must fail");

    // Assert
    assert!(matches!(error, MidgeError::ResourceLimit(_)));
}

#[test]
fn should_preserve_storage_io_timeout_given_valid_override() {
    // Arrange
    let timeout = Duration::from_millis(17);

    // Act
    let opts = OpenOptions::in_memory()
        .storage_io_timeout(timeout)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.storage_io_timeout(), timeout);
}

#[test]
fn should_reject_zero_storage_io_timeout_when_building() {
    // Arrange

    // Act
    let error = OpenOptions::in_memory()
        .storage_io_timeout(Duration::ZERO)
        .build()
        .expect_err("zero timeout must fail");

    // Assert
    assert!(matches!(error, MidgeError::InvalidArgument(_)));
}

#[test]
fn should_build_config_given_minimal_defaults_when_only_path_provided() {
    // Arrange

    // Act
    let opts = OpenOptions::in_memory().build().expect("build options");

    // Assert
    assert_eq!(opts.storage(), &Storage::InMemory);
    assert_eq!(opts.goal(), Goal::Latency);
    assert_eq!(opts.memory_budget(), MemoryBudget::Auto);
    assert_eq!(opts.workload(), WorkloadProfile::Mixed);
    assert_eq!(opts.block_cache_policy_value(), BlockCachePolicy::Lru);
}

#[test]
fn should_set_block_cache_policy_given_override_when_building() {
    // Arrange

    // Act
    let opts = OpenOptions::in_memory()
        .block_cache_policy(BlockCachePolicy::TinyLfu)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.block_cache_policy_value(), BlockCachePolicy::TinyLfu);
}

// ============================================================================
// GOAL SETTING TESTS
// ============================================================================

#[test]
fn should_set_goal_given_latency_when_optimizing_for_p99() {
    // Arrange

    // Act
    let opts = OpenOptions::in_memory()
        .goal(Goal::Latency)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.goal(), Goal::Latency);
    assert!(
        opts.block_size() <= 32 * 1024,
        "Latency goal should use small blocks"
    );
}

#[test]
fn should_set_goal_given_throughput_when_optimizing_for_bulk_writes() {
    // Arrange

    // Act
    let opts = OpenOptions::in_memory()
        .goal(Goal::Throughput)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.goal(), Goal::Throughput);
    assert!(
        opts.block_size() >= 64 * 1024,
        "Throughput goal should use larger blocks"
    );
}

#[test]
fn should_set_goal_given_cost_when_minimizing_resources() {
    // Arrange

    // Act
    let opts = OpenOptions::in_memory()
        .goal(Goal::Economy)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.goal(), Goal::Economy);
    // Cost should allocate less to cache and memtables
    assert!(
        opts.block_cache_size() <= 256 * 1024 * 1024,
        "Cost should limit cache"
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
    let opts = OpenOptions::in_memory()
        .memory_budget(budget)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.memory_budget(), budget);
    assert!(
        opts.block_cache_size() > 0,
        "Cache should be allocated from budget"
    );
}

#[test]
fn should_use_auto_memory_given_no_explicit_budget_when_default() {
    // Arrange

    // Act
    let opts = OpenOptions::in_memory().build().expect("build options");

    // Assert
    assert_eq!(opts.memory_budget(), MemoryBudget::Auto);
    // Auto should pick a sensible default
    assert!(
        opts.block_cache_size() > 0,
        "Auto budget should still allocate cache"
    );
}

#[test]
fn should_preserve_default_memtable_size_given_no_override_when_building() {
    // Arrange

    // Act
    let opts = OpenOptions::in_memory().build().expect("build options");

    // Assert
    assert_eq!(opts.memtable_size_limit(), 64 * 1024 * 1024);
}

#[test]
fn should_override_memtable_size_given_explicit_builder_when_building() {
    // Arrange
    let memtable_size = 128 * 1024;

    // Act
    let opts = OpenOptions::in_memory()
        .with_memtable_size_limit(memtable_size)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.memtable_size_limit(), memtable_size);
}

// ============================================================================
// WORKLOAD PROFILE OPTIMIZATION TESTS
// ============================================================================

#[test]
fn should_optimize_params_given_write_heavy_profile_when_configured() {
    // Arrange
    let normal = OpenOptions::in_memory()
        .workload(WorkloadProfile::Mixed)
        .build()
        .expect("build normal options");

    // Act
    let write_heavy = OpenOptions::in_memory()
        .workload(WorkloadProfile::WriteHeavy)
        .build()
        .expect("build write-heavy options");

    // Assert
    assert_eq!(write_heavy.workload(), WorkloadProfile::WriteHeavy);
    assert!(
        write_heavy.memtable_size_limit() >= normal.memtable_size_limit(),
        "Write-heavy should have larger memtables"
    );
}

#[test]
fn should_optimize_params_given_read_mostly_profile_when_configured() {
    // Arrange
    let normal = OpenOptions::in_memory()
        .workload(WorkloadProfile::Mixed)
        .build()
        .expect("build normal options");

    // Act
    let read_mostly = OpenOptions::in_memory()
        .workload(WorkloadProfile::ReadMostly)
        .build()
        .expect("build read-mostly options");

    // Assert
    assert_eq!(read_mostly.workload(), WorkloadProfile::ReadMostly);
    assert!(
        read_mostly.block_cache_size() >= normal.block_cache_size(),
        "Read-mostly should prioritize cache"
    );
}

#[test]
fn should_optimize_params_given_range_scan_profile_when_configured() {
    // Arrange
    let normal = OpenOptions::in_memory()
        .build()
        .expect("build normal options");

    // Act
    let range_scan = OpenOptions::in_memory()
        .workload(WorkloadProfile::RangeScan)
        .build()
        .expect("build range-scan options");

    // Assert
    assert_eq!(range_scan.workload(), WorkloadProfile::RangeScan);
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
    // Arrange

    // Act
    let opts = OpenOptions::in_memory()
        .goal(Goal::Throughput)
        .memory_budget(MemoryBudget::Bytes(1024 * 1024 * 1024)) // 1GB
        .workload(WorkloadProfile::WriteHeavy)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.goal(), Goal::Throughput);
    assert!(
        opts.memtable_size_limit() > 64 * 1024 * 1024,
        "Write-heavy + throughput should have large memtables"
    );
}

#[test]
fn should_derive_different_params_given_latency_vs_throughput_when_comparing() {
    // Arrange
    let latency_opts = OpenOptions::in_memory()
        .goal(Goal::Latency)
        .build()
        .expect("build latency options");

    // Act
    let throughput_opts = OpenOptions::in_memory()
        .goal(Goal::Throughput)
        .build()
        .expect("build throughput options");

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
    // Arrange

    // Act
    let opts = OpenOptions::in_memory().build().expect("build options");

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
    // Arrange

    // Act
    let opts = OpenOptions::local("./relative/path")
        .build()
        .expect("build options");

    // Assert
    assert_eq!(
        opts.storage(),
        &Storage::Local {
            path: PathBuf::from("./relative/path")
        }
    );
}

#[test]
fn should_store_path_given_absolute_path_when_building() {
    // Arrange

    // Act
    let opts = OpenOptions::local("/absolute/path/to/db")
        .build()
        .expect("build options");

    // Assert
    assert_eq!(
        opts.storage(),
        &Storage::Local {
            path: PathBuf::from("/absolute/path/to/db")
        }
    );
}

// ============================================================================
// CLONE AND DEFAULT TESTS
// ============================================================================

#[test]
fn should_clone_options_preserving_all_settings_given_configured_opts_when_cloning() {
    // Arrange
    let original = OpenOptions::in_memory()
        .goal(Goal::Throughput)
        .workload(WorkloadProfile::WriteHeavy)
        .build()
        .expect("build options");

    // Act
    let cloned = original.clone();

    // Assert
    assert_eq!(cloned.storage(), original.storage());
    assert_eq!(cloned.goal(), original.goal());
    assert_eq!(cloned.workload(), original.workload());
    assert_eq!(cloned.block_size(), original.block_size());
    assert_eq!(cloned.memtable_size_limit(), original.memtable_size_limit());
    assert_eq!(
        cloned.block_cache_policy_value(),
        original.block_cache_policy_value()
    );
}

#[test]
fn should_use_sensible_defaults_given_no_configuration_when_using_default() {
    // Arrange

    // Act
    let opts = OpenOptions::in_memory().build().expect("build options");

    // Assert
    assert_eq!(opts.goal(), Goal::Latency);
    assert_eq!(opts.memory_budget(), MemoryBudget::Auto);
    assert_eq!(opts.workload(), WorkloadProfile::Mixed);
}
