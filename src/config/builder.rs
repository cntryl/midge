//! Builder pattern for constructing Midge configurations.
//!
//! Provides a fluent API for setting high-level configuration knobs
//! and automatically deriving low-level parameters.

use std::path::Path;

use super::{
    cloud::CloudConfig,
    derivation::DerivedParams,
    validation,
    CloudMode,
    Config,
    ConfigError,
    ConfigResult,
    Durability,
    Goal,
    MemoryBudget,
    WorkloadProfile,
};

/// Builder for Midge configuration.
///
/// Allows fluent construction of `Config` with validation and automatic
/// parameter derivation.
///
/// # Example
///
/// ```rust,no_run
/// use cntryl_midge::config::{ConfigBuilder, Goal, Durability};
///
/// let config = ConfigBuilder::new("./my_db")
///     .goal(Goal::Latency)
///     .durability(Durability::Steady)
///     .build()
///     .expect("valid configuration");
/// ```
#[derive(Debug, Clone)]
pub struct ConfigBuilder {
    path: String,
    goal: Goal,
    durability: Durability,
    memory_budget: MemoryBudget,
    workload_profile: WorkloadProfile,
    cloud_mode: CloudMode,
    autotune_enabled: bool,
    cloud_config: Option<CloudConfig>,
}

impl ConfigBuilder {
    /// Create a new configuration builder with default settings.
    ///
    /// # Arguments
    ///
    /// * `path` - Database directory path
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_string_lossy().to_string(),
            goal: Goal::default(),
            durability: Durability::default(),
            memory_budget: MemoryBudget::default(),
            workload_profile: WorkloadProfile::default(),
            cloud_mode: CloudMode::default(),
            autotune_enabled: false,
            cloud_config: None,
        }
    }

    /// Set the performance optimization goal.
    pub fn goal(mut self, goal: Goal) -> Self {
        self.goal = goal;
        self
    }

    /// Set the durability guarantee level.
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Set the memory budget specification.
    pub fn memory_budget(mut self, budget: MemoryBudget) -> Self {
        self.memory_budget = budget;
        self
    }

    /// Set the workload profile for optimization.
    pub fn workload_profile(mut self, profile: WorkloadProfile) -> Self {
        self.workload_profile = profile;
        self
    }

    /// Set the cloud storage mode.
    pub fn cloud_mode(mut self, mode: CloudMode) -> Self {
        self.cloud_mode = mode;
        self
    }

    /// Enable or disable automatic tuning.
    pub fn autotune(mut self, enabled: bool) -> Self {
        self.autotune_enabled = enabled;
        self
    }

    /// Set cloud configuration for cloud-enabled modes.
    pub fn cloud_config(mut self, config: CloudConfig) -> Self {
        self.cloud_config = Some(config);
        self
    }

    /// Build the configuration with validation.
    ///
    /// Derives all low-level parameters from high-level knobs and validates
    /// the configuration against safety guardrails.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if validation fails or if cloud configuration
    /// is required but not provided.
    pub fn build(self) -> ConfigResult<Config> {
        // Validate cloud requirements
        if matches!(self.cloud_mode, CloudMode::Cache | CloudMode::Tiered | CloudMode::Replicated)
            && self.cloud_config.is_none()
        {
            return Err(ConfigError::CloudBucketRequired { mode: self.cloud_mode });
        }

        // Derive parameters
        let derived = DerivedParams::derive(
            self.goal,
            self.durability,
            self.memory_budget,
            self.workload_profile,
        );

        // Validate derived parameters
        validation::validate(&derived, self.cloud_mode)?;

        // Create validated plan
        let plan = derived.into_plan(true);

        Ok(Config {
            path: self.path.into(),
            goal: self.goal,
            durability: self.durability,
            memory_budget: self.memory_budget,
            workload_profile: self.workload_profile,
            cloud_mode: self.cloud_mode,
            plan,
            autotune_enabled: self.autotune_enabled,
            cloud_config: self.cloud_config,
        })
    }
}