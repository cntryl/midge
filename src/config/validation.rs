//! Configuration validation and guardrails.
//!
//! Implements the validation rules from the configuration specification.

use std::time::Duration;

use super::{derivation::DerivedParams, CloudMode, ConfigError, ConfigResult};

/// Minimum memory budget (64 MB).
const MIN_MEMORY_BUDGET: usize = 64 * 1024 * 1024;

/// Maximum safe WAL sync interval (250 ms).
const MAX_SAFE_WAL_INTERVAL_MS: u64 = 250;

/// Maximum memory utilization (90%).
const MAX_MEMORY_UTILIZATION: f64 = 0.90;

/// Validate derived parameters against guardrails.
pub fn validate(params: &DerivedParams, cloud_mode: CloudMode) -> ConfigResult<()> {
    // Validate memory budget
    validate_memory_budget(params)?;

    // Validate memory utilization
    validate_memory_utilization(params)?;

    // Validate WAL sync interval
    validate_wal_interval(params)?;

    // Validate cloud configuration
    validate_cloud_requirements(cloud_mode)?;

    Ok(())
}

/// Validate minimum memory budget.
fn validate_memory_budget(params: &DerivedParams) -> ConfigResult<()> {
    if params.total_memory_budget < MIN_MEMORY_BUDGET {
        return Err(ConfigError::InvalidMemoryBudget {
            budget: params.total_memory_budget,
            minimum: MIN_MEMORY_BUDGET,
        });
    }
    Ok(())
}

/// Validate memory utilization doesn't exceed safe threshold.
fn validate_memory_utilization(params: &DerivedParams) -> ConfigResult<()> {
    let cache = params.block_cache_size;
    let memtables = params.memtable_size * params.memtable_count;
    let overhead = params.overhead_budget;
    let total_allocated = cache + memtables + overhead;

    let utilization = total_allocated as f64 / params.total_memory_budget as f64;

    if utilization > MAX_MEMORY_UTILIZATION {
        return Err(ConfigError::MemoryOvercommit {
            requested: total_allocated,
            budget: params.total_memory_budget,
            utilization: utilization * 100.0,
        });
    }

    Ok(())
}

/// Validate WAL sync interval is within safe limits.
fn validate_wal_interval(params: &DerivedParams) -> ConfigResult<()> {
    if let Some(interval) = params.wal_sync_interval {
        let interval_ms = interval.as_millis() as u64;
        if interval_ms > MAX_SAFE_WAL_INTERVAL_MS {
            return Err(ConfigError::UnsafeWalInterval { interval_ms });
        }
    }
    Ok(())
}

/// Validate cloud mode requirements.
fn validate_cloud_requirements(cloud_mode: CloudMode) -> ConfigResult<()> {
    // Note: Actual bucket/provider validation happens in CloudConfig
    // This just checks that cloud mode is consistent
    match cloud_mode {
        CloudMode::Off => Ok(()),
        CloudMode::Cache | CloudMode::Tiered | CloudMode::Replicated => {
            // Cloud modes validated when cloud_config is built
            Ok(())
        }
    }
}

/// Validate that WAL interval is auto-tunable (within bounds).
pub fn validate_autotune_bounds(interval: Duration) -> ConfigResult<()> {
    let interval_ms = interval.as_millis() as u64;

    // Autotune bounds: 10-40 ms
    if !(10..=40).contains(&interval_ms) {
        return Err(ConfigError::ValidationFailed {
            reason: format!(
                "WAL interval {}ms outside autotune bounds (10-40ms)",
                interval_ms
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Durability, Goal, MemoryBudget, WorkloadProfile};

    #[test]
    fn should_accept_valid_configuration() {
        // Arrange
        let params = DerivedParams::derive(
            Goal::Latency,
            Durability::Steady,
            MemoryBudget::Bytes(512 * 1024 * 1024),
            WorkloadProfile::Mixed,
        );

        // Act & Assert
        assert!(validate(&params, CloudMode::Off).is_ok());
    }

    #[test]
    fn should_reject_insufficient_memory_budget() {
        // Arrange
        let params = DerivedParams::derive(
            Goal::Latency,
            Durability::Steady,
            MemoryBudget::Bytes(32 * 1024 * 1024), // 32 MB - too small
            WorkloadProfile::Mixed,
        );

        // Act
        let result = validate(&params, CloudMode::Off);

        // Assert
        assert!(matches!(
            result,
            Err(ConfigError::InvalidMemoryBudget { .. })
        ));
    }

    #[test]
    fn should_validate_wal_interval_bounds() {
        // Arrange - Create params with unsafe interval
        let mut params = DerivedParams::derive(
            Goal::Latency,
            Durability::Steady,
            MemoryBudget::Bytes(512 * 1024 * 1024),
            WorkloadProfile::Mixed,
        );
        // Override with unsafe interval
        params.wal_sync_interval = Some(Duration::from_millis(300));

        // Act
        let result = validate(&params, CloudMode::Off);

        // Assert
        assert!(matches!(result, Err(ConfigError::UnsafeWalInterval { .. })));
    }

    #[test]
    fn should_validate_autotune_bounds() {
        // Arrange
        let valid1 = Duration::from_millis(10);
        let valid2 = Duration::from_millis(20);
        let valid3 = Duration::from_millis(40);

        // Act
        let result1 = validate_autotune_bounds(valid1);
        let result2 = validate_autotune_bounds(valid2);
        let result3 = validate_autotune_bounds(valid3);

        // Assert
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());
    }

    #[test]
    fn should_reject_autotune_bounds_outside_range() {
        // Arrange
        let too_low = Duration::from_millis(5);
        let too_high = Duration::from_millis(50);

        // Act
        let result_low = validate_autotune_bounds(too_low);
        let result_high = validate_autotune_bounds(too_high);

        // Assert
        assert!(result_low.is_err());
        assert!(result_high.is_err());
    }
}
