//! Database Configuration Options
//!
//! Smart configuration system with **automatic parameter derivation**.
//!
//! # Design Philosophy
//!
//! Instead of exposing hundreds of low-level tuning knobs, Midge asks **two core questions**:
//!
//! 1. **What's the performance goal?** (`Goal::Latency` | `Goal::Throughput` | `Goal::Economy`)
//! 2. **How much memory?** (`MemoryBudget::Auto` | `MemoryBudget::Bytes(n)`)
//!
//! All other parameters (block sizes, buffer sizes, compaction triggers, cache allocation, etc.)
//! are **derived automatically** from these three inputs plus optional workload hints.
//! Advanced callers can also override runtime memtable sizing explicitly while
//! leaving `MemoryBudget` semantics unchanged.
//!
//! # Example
//!
//! ```rust,no_run
//! use cntryl_midge::{MidgeEngine, OpenOptions};
//!
//! // Open a database with default options
//! let opts = OpenOptions::local("./my_db").build()?;
//! let engine = MidgeEngine::open(opts)?;
//! # Ok::<(), cntryl_midge::MidgeError>(())
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct LeaseLossHook(pub(crate) std::sync::Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for LeaseLossHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LeaseLossHook(..)")
    }
}

use crate::common::{MidgeError, MidgeResult};
#[cfg(test)]
use crate::config::{
    AzureCredentialSource, CloudProviderConfig, GcsApiStyle, GcsCredentialSource,
    S3CredentialSource,
};
pub use crate::config::{RecoveryPolicy, Storage};
pub use crate::sst::cache::BlockCachePolicy;
use crate::sst::cache::{CachePolicyConfig, CachePolicyType};
use crate::sst::compression::{CompressionAlgo, CompressionPolicy};
pub use crate::storage::cloud::CloudWritePolicy;
use crate::storage::cloud::CloudWritePolicyConfig;

/// Performance optimization goal.
///
/// Determines the primary optimization target for derived parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Goal {
    /// Optimize for low latency (p99 < 10ms for point queries).
    ///
    /// - Smaller block sizes (16 KiB)
    /// - More aggressive bloom filters
    /// - Lower compaction trigger thresholds
    /// - Higher cache allocation
    #[default]
    Latency,

    /// Optimize for high throughput (MB/s for bulk operations).
    ///
    /// - Larger block sizes (64 KiB)
    /// - Larger memtables (256 MB)
    /// - Higher compaction concurrency
    /// - Larger SST files
    Throughput,

    /// Optimize for cost (minimize memory/CPU usage).
    ///
    /// - Minimal cache allocation
    /// - Lower compaction concurrency
    /// - Smaller bloom filters
    /// - Higher compression
    Economy,
}

/// Memory budget specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MemoryBudget {
    /// Automatically determine memory budget from available system memory.
    ///
    /// Uses ~50% of the effective memory limit (cgroup-aware when possible).
    #[default]
    Auto,

    /// Explicit memory budget in bytes.
    ///
    /// All allocations (cache + memtables + overhead) must fit within this budget.
    Bytes(usize),
}

/// Workload profile for optimizing derived parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkloadProfile {
    /// Balanced read/write workload (default).
    #[default]
    Mixed,

    /// Write-heavy workload (>70% writes).
    ///
    /// - Larger memtables
    /// - More aggressive compaction
    /// - Lower bloom filter priority
    WriteHeavy,

    /// Read-mostly workload (>70% reads).
    ///
    /// - More aggressive bloom filters
    /// - Higher cache allocation
    /// - Lower compaction priority
    ReadMostly,

    /// Range scan workload.
    ///
    /// - Larger block sizes
    /// - Sequential access optimization
    /// - Lower bloom filter priority (not useful for ranges)
    RangeScan,

    /// TTL-heavy workload with frequent expirations.
    ///
    /// - More aggressive compaction
    /// - Higher tombstone cleanup priority
    TtlHeavy,
}

/// Immutable, validated database open options.
///
/// Values of this type can only be produced by [`OpenOptionsBuilder::build`].
/// All fields are private so derived values cannot become stale after
/// finalization.
///
/// ```compile_fail
/// use cntryl_midge::{Goal, OpenOptions};
/// let mut options = OpenOptions::in_memory().build().unwrap();
/// options.goal = Goal::Economy;
/// ```
#[derive(Debug, Clone)]
pub struct OpenOptions {
    storage: Storage,
    goal: Goal,
    memory_budget: MemoryBudget,
    workload: WorkloadProfile,
    recovery_policy: RecoveryPolicy,
    derived_memory_budget: usize,
    memtable_size_limit: usize,
    memtable_flush_threshold: usize,
    transaction_memory_pool_size: usize,
    cache: CachePolicyConfig,
    compaction: crate::compaction::OpenCompactionConfig,
    cloud: CloudWritePolicyConfig,
    wal: crate::wal::WalBatchingConfig,
    lease_loss_hook: Option<LeaseLossHook>,
    lease_clock_skew_tolerance: Duration,
    ttl_clock: crate::common::time::ClockHandle,
}

/// Mutable input state for constructing [`OpenOptions`].
///
/// Setters only record caller intent. Memory derivation and validation happen
/// once, in [`Self::build`], so setter order cannot stale a derived value.
#[derive(Debug, Clone)]
pub struct OpenOptionsBuilder {
    storage: Storage,
    goal: Goal,
    memory_budget: MemoryBudget,
    workload: WorkloadProfile,
    recovery_policy: RecoveryPolicy,
    explicit_memtable_size_limit: Option<usize>,
    explicit_memtable_flush_threshold: Option<usize>,
    transaction_memory_pool_size: Option<usize>,
    cache: CachePolicyConfig,
    compaction: crate::compaction::OpenCompactionConfig,
    cloud: CloudWritePolicyConfig,
    wal: crate::wal::WalBatchingConfig,
    lease_loss_hook: Option<LeaseLossHook>,
    lease_clock_skew_tolerance: Duration,
    ttl_clock: crate::common::time::ClockHandle,
}

struct DerivedMemoryPools {
    memtable_size_limit: usize,
    memtable_flush_threshold: usize,
    block_cache_size: usize,
    transaction_memory_pool_size: usize,
}

impl OpenOptions {
    /// Create an in-memory database builder.
    #[must_use]
    pub fn in_memory() -> OpenOptionsBuilder {
        OpenOptionsBuilder::new(Storage::InMemory)
    }

    /// Create a local filesystem database builder.
    #[must_use]
    pub fn local<P: Into<PathBuf>>(path: P) -> OpenOptionsBuilder {
        OpenOptionsBuilder::new(Storage::Local { path: path.into() })
    }

    /// Create a real cloud-backed database using one shared location.
    #[must_use]
    pub fn cloud<P: Into<PathBuf>>(
        local_cache_path: P,
        location: crate::config::CloudStorageLocation,
    ) -> OpenOptionsBuilder {
        Self::cloud_multi(
            local_cache_path,
            crate::config::CloudStorageTopology::new(location),
        )
    }

    /// Create a real cloud-backed database with per-class location overrides.
    #[must_use]
    pub fn cloud_multi<P: Into<PathBuf>>(
        local_cache_path: P,
        topology: crate::config::CloudStorageTopology,
    ) -> OpenOptionsBuilder {
        OpenOptionsBuilder::new(Storage::Cloud {
            local_cache_path: local_cache_path.into(),
            topology: Box::new(topology),
        })
    }

    /// Create a filesystem-backed cloud simulation builder.
    #[must_use]
    pub fn cloud_simulated<P: Into<PathBuf>, S: Into<String>>(
        local_cache_path: P,
        bucket: S,
        prefix: S,
    ) -> OpenOptionsBuilder {
        OpenOptionsBuilder::new(Storage::CloudSimulated {
            local_cache_path: local_cache_path.into(),
            bucket: bucket.into(),
            prefix: prefix.into(),
        })
    }

    /// Return the configured storage backend.
    #[must_use]
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Return the configured WAL, SST, and control stores for cloud mode.
    #[must_use]
    pub fn cloud_storage_topology(&self) -> Option<&crate::config::CloudStorageTopology> {
        match &self.storage {
            Storage::Cloud { topology, .. } => Some(topology),
            _ => None,
        }
    }

    /// Return the configured optimization goal.
    #[must_use]
    pub fn goal(&self) -> Goal {
        self.goal
    }

    /// Return the caller-facing memory-budget selection.
    #[must_use]
    pub fn memory_budget(&self) -> MemoryBudget {
        self.memory_budget
    }

    /// Return the resolved total memory budget in bytes.
    #[must_use]
    pub fn memory_budget_bytes(&self) -> usize {
        self.derived_memory_budget
    }

    /// Return the configured workload profile.
    #[must_use]
    pub fn workload(&self) -> WorkloadProfile {
        self.workload
    }

    /// Return the configured recovery policy.
    #[must_use]
    pub fn recovery_policy(&self) -> RecoveryPolicy {
        self.recovery_policy
    }

    /// Return the derived block size.
    #[must_use]
    pub fn block_size(&self) -> usize {
        self.cache.block_size
    }

    /// Return the derived memtable size limit.
    #[must_use]
    pub fn memtable_size_limit(&self) -> usize {
        self.memtable_size_limit
    }

    /// Return the memtable flush threshold.
    #[must_use]
    pub fn memtable_flush_threshold(&self) -> usize {
        self.memtable_flush_threshold
    }

    /// Return the target SST size.
    #[must_use]
    pub fn target_sst_size(&self) -> usize {
        self.compaction.target_sst_size
    }

    /// Return the block-cache allocation.
    #[must_use]
    pub fn block_cache_size(&self) -> usize {
        self.cache.capacity_bytes
    }

    /// Return the shared transaction-memory-pool allocation.
    #[must_use]
    pub fn transaction_memory_pool_size(&self) -> usize {
        self.transaction_memory_pool_size
    }

    /// Return the configured block-cache eviction policy.
    #[must_use]
    pub fn block_cache_policy_value(&self) -> BlockCachePolicy {
        self.cache.policy
    }

    /// Return the configured cloud write policy.
    #[must_use]
    pub fn cloud_write_policy_value(&self) -> &CloudWritePolicy {
        &self.cloud.policy
    }

    /// Return the maximum wait for a storage I/O acknowledgement.
    #[must_use]
    pub fn storage_io_timeout(&self) -> Duration {
        self.cloud.storage_io_timeout
    }

    /// Return the derived WAL buffer size.
    #[must_use]
    pub fn wal_buffer_size(&self) -> usize {
        self.wal.buffer_size
    }

    /// Return the derived L0 compaction trigger.
    #[must_use]
    pub fn l0_compaction_trigger(&self) -> usize {
        self.compaction.l0_trigger
    }

    /// Return the derived compression policy.
    #[must_use]
    pub fn compression_policy(&self) -> &CompressionPolicy {
        &self.compaction.compression
    }

    pub(crate) fn runtime_memtable_size_limit(&self) -> usize {
        self.memtable_size_limit
    }

    pub(crate) fn runtime_memtable_flush_threshold(&self) -> usize {
        self.memtable_flush_threshold
    }

    pub(crate) fn block_cache_policy_type(&self) -> CachePolicyType {
        self.cache.policy.into()
    }

    pub(crate) fn cloud_runtime_policy(&self) -> crate::runtime::CloudRuntimePolicy {
        crate::runtime::CloudRuntimePolicy {
            eventual_flush_segment_gap: self.cloud.policy.eventual_flush_segment_gap,
            wal_seal: crate::runtime::CloudWalSealPolicy {
                min_segment_bytes: self.cloud.policy.wal_seal_min_segment_bytes,
                max_flush_delay: self.cloud.policy.wal_seal_max_flush_delay,
                max_pending_writes: self.cloud.policy.wal_seal_max_pending_writes,
            },
        }
    }

    pub(crate) fn background_compaction_enabled(&self) -> bool {
        self.compaction.background_enabled
    }

    pub(crate) fn shutdown_cloud_drain_timeout(&self) -> Duration {
        self.cloud.shutdown_drain_timeout
    }

    pub(crate) fn wal_batch_config(&self) -> Option<crate::wal::policy::BatchConfig> {
        self.wal.batch
    }

    pub(crate) fn simulated_cloud_local_storage_budget_bytes(&self) -> Option<u64> {
        self.cloud.simulated_local_budget_bytes
    }

    pub(crate) fn lease_loss_hook(&self) -> Option<std::sync::Arc<dyn Fn() + Send + Sync>> {
        self.lease_loss_hook
            .as_ref()
            .map(|hook| std::sync::Arc::clone(&hook.0))
    }

    pub(crate) fn lease_clock_skew_tolerance(&self) -> Duration {
        self.lease_clock_skew_tolerance
    }

    pub(crate) fn ttl_clock(&self) -> Arc<crate::common::time::ObservedClock> {
        Arc::clone(&self.ttl_clock.0)
    }
}

impl OpenOptionsBuilder {
    fn new(storage: Storage) -> Self {
        Self {
            storage,
            goal: Goal::default(),
            memory_budget: MemoryBudget::default(),
            workload: WorkloadProfile::default(),
            recovery_policy: RecoveryPolicy::default(),
            explicit_memtable_size_limit: None,
            explicit_memtable_flush_threshold: None,
            transaction_memory_pool_size: None,
            cache: CachePolicyConfig::new(0, 0, BlockCachePolicy::default()),
            compaction: crate::compaction::OpenCompactionConfig::new(
                0,
                0,
                true,
                Self::derive_compression_policy(Goal::default()),
            ),
            cloud: CloudWritePolicyConfig::default(),
            wal: crate::wal::WalBatchingConfig::new(0, None),
            lease_loss_hook: None,
            // Half a lease TTL tolerates ordinary NTP/VM clock correction while
            // bounding additional failover latency.
            lease_clock_skew_tolerance: Duration::from_secs(15),
            ttl_clock: crate::common::time::ClockHandle(Arc::new(
                crate::common::time::ObservedClock::default(),
            )),
        }
    }

    /// Set the performance goal.
    #[must_use]
    pub fn goal(mut self, goal: Goal) -> Self {
        self.goal = goal;
        self
    }

    /// Set the total memory budget.
    #[must_use]
    pub fn memory_budget(mut self, budget: MemoryBudget) -> Self {
        self.memory_budget = budget;
        self
    }

    /// Override the derived memtable size limit in bytes.
    #[must_use]
    pub fn with_memtable_size_limit(mut self, bytes: usize) -> Self {
        self.explicit_memtable_size_limit = Some(bytes);
        self
    }

    /// Override the runtime memtable flush threshold in bytes.
    #[must_use]
    pub fn with_memtable_flush_threshold(mut self, bytes: usize) -> Self {
        self.explicit_memtable_flush_threshold = Some(bytes);
        self
    }

    /// Override the shared transaction-memory-pool allocation.
    #[must_use]
    pub fn transaction_memory_pool_size(mut self, bytes: usize) -> Self {
        self.transaction_memory_pool_size = Some(bytes);
        self
    }

    /// Set the workload profile hint.
    #[must_use]
    pub fn workload(mut self, profile: WorkloadProfile) -> Self {
        self.workload = profile;
        self
    }

    /// Set the block-cache eviction policy.
    #[must_use]
    pub fn block_cache_policy(mut self, policy: BlockCachePolicy) -> Self {
        self.cache.policy = policy;
        self
    }

    /// Set the cloud write policy.
    #[must_use]
    pub fn cloud_write_policy(mut self, policy: CloudWritePolicy) -> Self {
        self.cloud.set_policy(policy);
        self
    }

    /// Enable or disable automatic background compaction scheduling.
    #[must_use]
    pub fn background_compaction(mut self, enabled: bool) -> Self {
        self.compaction.set_background_enabled(enabled);
        self
    }

    /// Set the recovery policy.
    #[must_use]
    pub fn recovery_policy(mut self, policy: RecoveryPolicy) -> Self {
        self.recovery_policy = policy;
        self
    }

    /// Set the maximum wait for append, flush, and sync acknowledgements.
    #[must_use]
    pub fn storage_io_timeout(mut self, timeout: Duration) -> Self {
        self.cloud.storage_io_timeout = timeout;
        self
    }

    /// Override the CloudAsync drain budget used by deterministic shutdown
    /// fault-injection tests.
    #[cfg(feature = "failpoints")]
    #[doc(hidden)]
    #[must_use]
    pub fn shutdown_cloud_drain_timeout_for_testing(mut self, timeout: Duration) -> Self {
        self.cloud.shutdown_drain_timeout = timeout;
        self
    }

    /// Register a process-local notification invoked exactly once when the
    /// primary lease transitions to fenced. The engine remains open for reads
    /// and rejects writes; embedders should begin orderly shutdown from the
    /// notification without blocking the callback.
    #[must_use]
    pub fn on_lease_loss(mut self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        self.lease_loss_hook = Some(LeaseLossHook(std::sync::Arc::new(hook)));
        self
    }

    /// Set the wall-clock skew allowance used before a persisted lease may be
    /// taken over. Values are bounded to one 30-second lease TTL.
    #[must_use]
    pub fn lease_clock_skew_tolerance(mut self, tolerance: Duration) -> Self {
        self.lease_clock_skew_tolerance = tolerance;
        self
    }

    /// Override the engine TTL wall clock, primarily for deterministic hosts
    /// and tests. Midge applies a process-local nondecreasing floor to it.
    #[must_use]
    pub fn ttl_clock(mut self, clock: Arc<dyn crate::common::time::Clock>) -> Self {
        self.ttl_clock = crate::common::time::ClockHandle(Arc::new(
            crate::common::time::ObservedClock::new(clock),
        ));
        self
    }

    /// Override the simulated-cloud local storage budget.
    #[doc(hidden)]
    #[must_use]
    pub fn with_simulated_cloud_local_storage_budget(mut self, bytes: u64) -> Self {
        self.cloud.simulated_local_budget_bytes = Some(bytes);
        self
    }

    /// Build immutable options and derive every dependent value once.
    ///
    /// # Errors
    ///
    /// Returns [`MidgeError::InvalidArgument`] for zero-valued required limits
    /// and [`MidgeError::ResourceLimit`] when an override cannot fit inside the
    /// total memory budget.
    pub fn build(self) -> MidgeResult<OpenOptions> {
        if self.cloud.storage_io_timeout.is_zero() {
            return Err(MidgeError::InvalidArgument(
                "storage I/O timeout must be greater than zero".to_string(),
            ));
        }
        if self.cloud.shutdown_drain_timeout.is_zero() {
            return Err(MidgeError::InvalidArgument(
                "cloud shutdown drain timeout must be greater than zero".to_string(),
            ));
        }
        if self.lease_clock_skew_tolerance > Duration::from_secs(30) {
            return Err(MidgeError::InvalidArgument(
                "lease clock-skew tolerance must not exceed the 30-second lease TTL".to_string(),
            ));
        }
        self.cloud.policy.validate()?;

        let total_memory = self.resolve_total_memory()?;
        let pools = self.derive_memory_pools(total_memory)?;

        let block_size = match (self.goal, self.workload) {
            (Goal::Latency, _) => 16 * 1024,
            (Goal::Economy, _) => 32 * 1024,
            (Goal::Throughput, WorkloadProfile::RangeScan) => 128 * 1024,
            (Goal::Throughput, _) => 64 * 1024,
        };
        let target_sst_size = match self.goal {
            Goal::Latency => 128 * 1024 * 1024,
            Goal::Throughput => 512 * 1024 * 1024,
            Goal::Economy => 256 * 1024 * 1024,
        };
        let wal_buffer_size = match self.goal {
            Goal::Latency => 128 * 1024,
            Goal::Throughput => 1024 * 1024,
            Goal::Economy => 256 * 1024,
        }
        .min(total_memory)
        .max(1);
        let l0_compaction_trigger = match (self.goal, self.workload) {
            (Goal::Latency, _) => 3,
            (_, WorkloadProfile::WriteHeavy) => 8,
            (Goal::Throughput, _) => 6,
            _ => 4,
        };
        let compression_policy = Self::derive_compression_policy(self.goal);

        Ok(OpenOptions {
            storage: self.storage,
            goal: self.goal,
            memory_budget: self.memory_budget,
            workload: self.workload,
            recovery_policy: self.recovery_policy,
            derived_memory_budget: total_memory,
            memtable_size_limit: pools.memtable_size_limit,
            memtable_flush_threshold: pools.memtable_flush_threshold,
            transaction_memory_pool_size: pools.transaction_memory_pool_size,
            cache: CachePolicyConfig::new(block_size, pools.block_cache_size, self.cache.policy),
            compaction: crate::compaction::OpenCompactionConfig::new(
                target_sst_size,
                l0_compaction_trigger,
                self.compaction.background_enabled,
                compression_policy,
            ),
            cloud: self.cloud,
            wal: crate::wal::WalBatchingConfig::new(wal_buffer_size, self.wal.batch),
            lease_loss_hook: self.lease_loss_hook,
            lease_clock_skew_tolerance: self.lease_clock_skew_tolerance,
            ttl_clock: self.ttl_clock,
        })
    }

    fn resolve_total_memory(&self) -> MidgeResult<usize> {
        let total_memory = match self.memory_budget {
            MemoryBudget::Auto => memory::auto_memory_budget_bytes().unwrap_or(512 * 1024 * 1024),
            MemoryBudget::Bytes(bytes) => bytes,
        };
        if total_memory < 3 {
            return Err(MidgeError::ResourceLimit(
                "memory budget must hold two memtable generations and a transaction pool"
                    .to_string(),
            ));
        }
        Ok(total_memory)
    }

    fn derive_memory_pools(&self, total_memory: usize) -> MidgeResult<DerivedMemoryPools> {
        let transaction_memory_pool_size = self
            .transaction_memory_pool_size
            .unwrap_or_else(|| (total_memory / 10).max(1));
        if transaction_memory_pool_size == 0 {
            return Err(MidgeError::InvalidArgument(
                "transaction memory pool size must be greater than zero".to_string(),
            ));
        }
        if transaction_memory_pool_size > total_memory {
            return Err(MidgeError::ResourceLimit(format!(
                "transaction memory pool ({transaction_memory_pool_size} bytes) exceeds total budget ({total_memory} bytes)"
            )));
        }

        let max_memtable_size = total_memory.saturating_sub(transaction_memory_pool_size) / 2;
        if max_memtable_size == 0 {
            return Err(MidgeError::ResourceLimit(
                "memory budget leaves no capacity for memtables".to_string(),
            ));
        }

        let memtable_size_limit = self.derive_memtable_size(total_memory, max_memtable_size)?;
        let memtable_flush_threshold = self.derive_flush_threshold(memtable_size_limit)?;
        crate::config::validate_memtable_limits(memtable_size_limit, memtable_flush_threshold)?;
        let mut block_cache_size = total_memory
            .saturating_sub(transaction_memory_pool_size)
            .saturating_sub(memtable_size_limit.saturating_mul(2));
        if self.goal == Goal::Economy {
            block_cache_size = block_cache_size.min(256 * 1024 * 1024);
        }

        Ok(DerivedMemoryPools {
            memtable_size_limit,
            memtable_flush_threshold,
            block_cache_size,
            transaction_memory_pool_size,
        })
    }

    fn derive_memtable_size(
        &self,
        total_memory: usize,
        max_memtable_size: usize,
    ) -> MidgeResult<usize> {
        let base_memtable: usize = match self.goal {
            Goal::Latency => 64 * 1024 * 1024,
            Goal::Throughput => 256 * 1024 * 1024,
            Goal::Economy => 32 * 1024 * 1024,
        };
        let desired_memtable = match self.workload {
            WorkloadProfile::WriteHeavy => base_memtable.saturating_mul(2),
            WorkloadProfile::ReadMostly => base_memtable / 2,
            _ => base_memtable,
        };
        match self.explicit_memtable_size_limit {
            Some(0) => Err(MidgeError::InvalidArgument(
                "memtable size limit must be greater than zero".to_string(),
            )),
            Some(bytes) if bytes > max_memtable_size => Err(MidgeError::ResourceLimit(format!(
                "two {bytes}-byte memtables plus transaction memory exceed the {total_memory}-byte budget"
            ))),
            Some(bytes) => Ok(bytes),
            None => Ok(desired_memtable.min(max_memtable_size).max(1)),
        }
    }

    fn derive_flush_threshold(&self, memtable_size_limit: usize) -> MidgeResult<usize> {
        match self.explicit_memtable_flush_threshold {
            Some(0) => Err(MidgeError::InvalidArgument(
                "memtable flush threshold must be greater than zero".to_string(),
            )),
            Some(bytes) if bytes > memtable_size_limit => Err(MidgeError::InvalidArgument(
                format!(
                    "memtable flush threshold ({bytes} bytes) exceeds size limit ({memtable_size_limit} bytes)"
                ),
            )),
            Some(bytes) => Ok(bytes),
            None => Ok(memtable_size_limit),
        }
    }

    fn derive_compression_policy(goal: Goal) -> CompressionPolicy {
        match goal {
            Goal::Latency => CompressionPolicy::Fixed(CompressionAlgo::Lz4),
            Goal::Throughput => CompressionPolicy::Adaptive {
                min_savings_bytes: 256,
                min_ratio: 0.95,
                check_algorithms: vec![CompressionAlgo::Lz4, CompressionAlgo::Zstd3],
            },
            Goal::Economy => CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
        }
    }
}

mod memory;

#[cfg(test)]
#[path = "options/tests.rs"]
mod tests;
