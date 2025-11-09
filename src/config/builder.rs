//! Configuration builder with ergonomic API.
//!
//! Implements the builder pattern for constructing validated configurations.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{
    cloud::CloudConfig, derivation::DerivedParams, profile::ProfileAdjustments, validation,
    CloudMode, Config, ConfigError, ConfigResult, Durability, Goal, MemoryBudget, WorkloadProfile,
};
use crate::wal::cloud::CloudStorageBackend;

/// Builder for Midge configuration.
///
/// # Example
///
/// ```rust,no_run
/// use cntryl_midge::config::{ConfigBuilder, Goal, Durability, WorkloadProfile};
///
/// let config = ConfigBuilder::new("./my_db")
///     .goal(Goal::Latency)
///     .durability(Durability::Steady)
///     .workload_profile(WorkloadProfile::ReadMostly)
///     .memory_budget_mb(512)
///     .build()
///     .expect("valid configuration");
/// ```
pub struct ConfigBuilder {
    path: PathBuf,
    goal: Goal,
    durability: Durability,
    memory_budget: MemoryBudget,
    workload_profile: WorkloadProfile,
    cloud_mode: CloudMode,
    autotune_enabled: bool,

    // Cloud-specific
    cloud_backend: Option<Arc<dyn CloudStorageBackend>>,
    cloud_bucket: Option<String>,
    cloud_prefix: Option<String>,
}

impl ConfigBuilder {
    /// Create a new configuration builder.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database directory (local or cache path for cloud mode)
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            goal: Goal::default(),
            durability: Durability::default(),
            memory_budget: MemoryBudget::default(),
            workload_profile: WorkloadProfile::default(),
            cloud_mode: CloudMode::default(),
            autotune_enabled: false,
            cloud_backend: None,
            cloud_bucket: None,
            cloud_prefix: None,
        }
    }

    /// Set the performance goal.
    pub fn goal(mut self, goal: Goal) -> Self {
        self.goal = goal;
        self
    }

    /// Set the durability level.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Set explicit memory budget in bytes.
    pub fn memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget = MemoryBudget::Bytes(bytes);
        self
    }

    /// Set memory budget in megabytes (convenience method).
    pub fn memory_budget_mb(mut self, mb: usize) -> Self {
        self.memory_budget = MemoryBudget::Bytes(mb * 1024 * 1024);
        self
    }

    /// Use automatic memory budget (default).
    pub fn memory_budget_auto(mut self) -> Self {
        self.memory_budget = MemoryBudget::Auto;
        self
    }

    /// Set the workload profile.
    pub fn workload_profile(mut self, profile: WorkloadProfile) -> Self {
        self.workload_profile = profile;
        self
    }

    /// Set cloud storage mode.
    pub fn cloud_mode(mut self, mode: CloudMode) -> Self {
        self.cloud_mode = mode;
        self
    }

    /// Configure cloud storage backend.
    pub fn cloud_backend(
        mut self,
        backend: Arc<dyn CloudStorageBackend>,
        bucket: impl Into<String>,
    ) -> Self {
        self.cloud_backend = Some(backend);
        self.cloud_bucket = Some(bucket.into());
        self
    }

    /// Set cloud object prefix.
    pub fn cloud_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.cloud_prefix = Some(prefix.into());
        self
    }

    /// Enable autotuning (adaptive parameter adjustment).
    pub fn enable_autotune(mut self) -> Self {
        self.autotune_enabled = true;
        self
    }

    /// Build and validate the configuration.
    pub fn build(self) -> ConfigResult<Config> {
        // Validate path
        if self.path.as_os_str().is_empty() {
            return Err(ConfigError::InvalidPath {
                path: self.path.display().to_string(),
            });
        }

        // Derive base parameters
        let mut params = DerivedParams::derive(
            self.goal,
            self.durability,
            self.memory_budget,
            self.workload_profile,
        );

        // Apply workload profile adjustments
        let adjustments = ProfileAdjustments::for_profile(self.workload_profile);
        adjustments.apply(&mut params);

        // Validate derived parameters
        validation::validate(&params, self.cloud_mode)?;

        // Build cloud configuration if needed
        let cloud_config = if self.cloud_mode != CloudMode::Off {
            let backend = self.cloud_backend.ok_or(ConfigError::CloudBucketRequired {
                mode: self.cloud_mode,
            })?;

            let bucket = self.cloud_bucket.ok_or(ConfigError::CloudBucketRequired {
                mode: self.cloud_mode,
            })?;

            Some(CloudConfig::new(
                self.cloud_mode,
                backend,
                bucket,
                self.cloud_prefix,
                self.goal,
            )?)
        } else {
            None
        };

        // Convert to plan with cloud parameters
        let mut plan = params.into_plan(true);

        if let Some(ref cc) = cloud_config {
            plan.upload_concurrency = Some(cc.upload_concurrency());
            plan.multipart_chunk_size = Some(cc.multipart_chunk_size());
            plan.prefetch_depth = Some(cc.prefetch_depth());
        }

        Ok(Config {
            path: self.path,
            goal: self.goal,
            durability: self.durability,
            memory_budget: self.memory_budget,
            workload_profile: self.workload_profile,
            cloud_mode: self.cloud_mode,
            plan,
            autotune_enabled: self.autotune_enabled,
            cloud_config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_build_config_with_basic_settings() {
        // Arrange
        let builder = ConfigBuilder::new("./test_db")
            .goal(Goal::Latency)
            .durability(Durability::Steady)
            .memory_budget_mb(512);

        // Act
        let config = builder.build().unwrap();

        // Assert
        assert_eq!(config.goal(), Goal::Latency);
        assert_eq!(config.durability(), Durability::Steady);
        assert_eq!(config.plan().total_memory_budget, 512 * 1024 * 1024);
    }

    #[test]
    fn should_apply_workload_profile_multipliers() {
        // Arrange
        let builder = ConfigBuilder::new("./test_db")
            .goal(Goal::Throughput)
            .workload_profile(WorkloadProfile::WriteHeavy)
            .memory_budget_mb(1024);

        // Act
        let config = builder.build().unwrap();

        // Assert
        assert_eq!(config.workload_profile(), WorkloadProfile::WriteHeavy);
        // WriteHeavy should have larger memtables
        assert!(config.plan().memtable_size > 128 * 1024 * 1024);
    }

    #[test]
    fn should_enable_autotune_when_requested() {
        // Arrange
        let builder = ConfigBuilder::new("./test_db").enable_autotune();

        // Act
        let config = builder.build().unwrap();

        // Assert
        assert!(config.autotune_enabled());
    }

    #[test]
    fn should_reject_cloud_mode_without_backend() {
        // Arrange
        let builder = ConfigBuilder::new("./test_db").cloud_mode(CloudMode::Cache);

        // Act
        let result = builder.build();

        // Assert
        assert!(matches!(
            result,
            Err(ConfigError::CloudBucketRequired { .. })
        ));
    }

    #[test]
    fn should_reject_empty_path() {
        // Arrange
        let builder = ConfigBuilder::new("");

        // Act
        let result = builder.build();

        // Assert
        assert!(matches!(result, Err(ConfigError::InvalidPath { .. })));
    }

    #[test]
    fn should_use_default_values_when_not_specified() {
        // Arrange
        let builder = ConfigBuilder::new("./test_db");

        // Act
        let config = builder.build().unwrap();

        // Assert
        assert_eq!(config.goal(), Goal::Latency);
        assert_eq!(config.durability(), Durability::Steady);
        assert_eq!(config.cloud_mode(), CloudMode::Off);
        assert!(!config.autotune_enabled());
    }

    #[test]
    fn should_auto_derive_memory_budget() {
        // Arrange
        let builder = ConfigBuilder::new("./test_db").memory_budget_auto();

        // Act
        let config = builder.build().unwrap();

        // Assert
        assert!(config.plan().total_memory_budget > 0);
    }
}
