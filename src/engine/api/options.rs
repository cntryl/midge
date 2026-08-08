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
use crate::sst::cache::CachePolicyType;
use crate::sst::compression::{CompressionAlgo, CompressionPolicy};

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

/// Eviction policy for the block cache used by SST reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlockCachePolicy {
    /// Least-recently-used eviction. This is the stable default.
    #[default]
    Lru,
    /// Window TinyLFU-style admission/eviction for frequency-biased workloads.
    TinyLfu,
    /// CLOCK-Pro eviction for scan-resistant workloads.
    ClockPro,
}

impl From<BlockCachePolicy> for CachePolicyType {
    fn from(policy: BlockCachePolicy) -> Self {
        match policy {
            BlockCachePolicy::Lru => Self::Lru,
            BlockCachePolicy::TinyLfu => Self::TinyLfu,
            BlockCachePolicy::ClockPro => Self::ClockPro,
        }
    }
}

/// Cloud-backed write tuning used by `OpenOptions`.
///
/// Defaults preserve the engine's production behavior:
/// - flush a WAL-backed memtable after a segment gap of 128
/// - seal cloud WAL segments after 16 MiB, 500 ms, or 10,000 pending writes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudWritePolicy {
    pub eventual_flush_segment_gap: u64,
    pub wal_seal_min_segment_bytes: usize,
    pub wal_seal_max_flush_delay: Duration,
    pub wal_seal_max_pending_writes: usize,
}

impl Default for CloudWritePolicy {
    fn default() -> Self {
        Self {
            eventual_flush_segment_gap: 128,
            wal_seal_min_segment_bytes: 16 * 1024 * 1024,
            wal_seal_max_flush_delay: Duration::from_millis(500),
            wal_seal_max_pending_writes: 10_000,
        }
    }
}

impl From<CloudWritePolicy> for crate::runtime::CloudRuntimePolicy {
    fn from(policy: CloudWritePolicy) -> Self {
        Self {
            eventual_flush_segment_gap: policy.eventual_flush_segment_gap,
            wal_seal: crate::runtime::CloudWalSealPolicy {
                min_segment_bytes: policy.wal_seal_min_segment_bytes,
                max_flush_delay: policy.wal_seal_max_flush_delay,
                max_pending_writes: policy.wal_seal_max_pending_writes,
            },
        }
    }
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
    block_size: usize,
    memtable_size_limit: usize,
    memtable_flush_threshold: usize,
    target_sst_size: usize,
    block_cache_size: usize,
    transaction_memory_pool_size: usize,
    block_cache_policy: BlockCachePolicy,
    cloud_write_policy: CloudWritePolicy,
    background_compaction: bool,
    storage_io_timeout: Duration,
    wal_buffer_size: usize,
    l0_compaction_trigger: usize,
    compression_policy: CompressionPolicy,
    wal_batch_config: Option<crate::wal::policy::BatchConfig>,
    simulated_cloud_local_storage_budget_bytes: Option<u64>,
    lease_loss_hook: Option<LeaseLossHook>,
    lease_clock_skew_tolerance: Duration,
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
    block_cache_policy: BlockCachePolicy,
    cloud_write_policy: CloudWritePolicy,
    background_compaction: bool,
    storage_io_timeout: Duration,
    wal_batch_config: Option<crate::wal::policy::BatchConfig>,
    simulated_cloud_local_storage_budget_bytes: Option<u64>,
    lease_loss_hook: Option<LeaseLossHook>,
    lease_clock_skew_tolerance: Duration,
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
        self.block_size
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
        self.target_sst_size
    }

    /// Return the block-cache allocation.
    #[must_use]
    pub fn block_cache_size(&self) -> usize {
        self.block_cache_size
    }

    /// Return the shared transaction-memory-pool allocation.
    #[must_use]
    pub fn transaction_memory_pool_size(&self) -> usize {
        self.transaction_memory_pool_size
    }

    /// Return the configured block-cache eviction policy.
    #[must_use]
    pub fn block_cache_policy_value(&self) -> BlockCachePolicy {
        self.block_cache_policy
    }

    /// Return the configured cloud write policy.
    #[must_use]
    pub fn cloud_write_policy_value(&self) -> &CloudWritePolicy {
        &self.cloud_write_policy
    }

    /// Return the maximum wait for a storage I/O acknowledgement.
    #[must_use]
    pub fn storage_io_timeout(&self) -> Duration {
        self.storage_io_timeout
    }

    /// Return the derived WAL buffer size.
    #[must_use]
    pub fn wal_buffer_size(&self) -> usize {
        self.wal_buffer_size
    }

    /// Return the derived L0 compaction trigger.
    #[must_use]
    pub fn l0_compaction_trigger(&self) -> usize {
        self.l0_compaction_trigger
    }

    /// Return the derived compression policy.
    #[must_use]
    pub fn compression_policy(&self) -> &CompressionPolicy {
        &self.compression_policy
    }

    pub(crate) fn runtime_memtable_size_limit(&self) -> usize {
        self.memtable_size_limit
    }

    pub(crate) fn runtime_memtable_flush_threshold(&self) -> usize {
        self.memtable_flush_threshold
    }

    pub(crate) fn block_cache_policy_type(&self) -> CachePolicyType {
        self.block_cache_policy.into()
    }

    pub(crate) fn cloud_runtime_policy(&self) -> crate::runtime::CloudRuntimePolicy {
        self.cloud_write_policy.clone().into()
    }

    pub(crate) fn background_compaction_enabled(&self) -> bool {
        self.background_compaction
    }

    pub(crate) fn wal_batch_config(&self) -> Option<crate::wal::policy::BatchConfig> {
        self.wal_batch_config
    }

    pub(crate) fn simulated_cloud_local_storage_budget_bytes(&self) -> Option<u64> {
        self.simulated_cloud_local_storage_budget_bytes
    }

    pub(crate) fn lease_loss_hook(&self) -> Option<std::sync::Arc<dyn Fn() + Send + Sync>> {
        self.lease_loss_hook
            .as_ref()
            .map(|hook| std::sync::Arc::clone(&hook.0))
    }

    pub(crate) fn lease_clock_skew_tolerance(&self) -> Duration {
        self.lease_clock_skew_tolerance
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
            block_cache_policy: BlockCachePolicy::default(),
            cloud_write_policy: CloudWritePolicy::default(),
            background_compaction: true,
            storage_io_timeout: crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
            wal_batch_config: None,
            simulated_cloud_local_storage_budget_bytes: None,
            lease_loss_hook: None,
            // Half a lease TTL tolerates ordinary NTP/VM clock correction while
            // bounding additional failover latency.
            lease_clock_skew_tolerance: Duration::from_secs(15),
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
        self.block_cache_policy = policy;
        self
    }

    /// Set the cloud write policy.
    #[must_use]
    pub fn cloud_write_policy(mut self, policy: CloudWritePolicy) -> Self {
        self.cloud_write_policy = policy;
        self
    }

    /// Enable or disable automatic background compaction scheduling.
    #[must_use]
    pub fn background_compaction(mut self, enabled: bool) -> Self {
        self.background_compaction = enabled;
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
        self.storage_io_timeout = timeout;
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

    /// Override the simulated-cloud local storage budget.
    #[doc(hidden)]
    #[must_use]
    pub fn with_simulated_cloud_local_storage_budget(mut self, bytes: u64) -> Self {
        self.simulated_cloud_local_storage_budget_bytes = Some(bytes);
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
        if self.storage_io_timeout.is_zero() {
            return Err(MidgeError::InvalidArgument(
                "storage I/O timeout must be greater than zero".to_string(),
            ));
        }
        if self.lease_clock_skew_tolerance > Duration::from_secs(30) {
            return Err(MidgeError::InvalidArgument(
                "lease clock-skew tolerance must not exceed the 30-second lease TTL".to_string(),
            ));
        }

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
            block_size,
            memtable_size_limit: pools.memtable_size_limit,
            memtable_flush_threshold: pools.memtable_flush_threshold,
            target_sst_size,
            block_cache_size: pools.block_cache_size,
            transaction_memory_pool_size: pools.transaction_memory_pool_size,
            block_cache_policy: self.block_cache_policy,
            cloud_write_policy: self.cloud_write_policy,
            background_compaction: self.background_compaction,
            storage_io_timeout: self.storage_io_timeout,
            wal_buffer_size,
            l0_compaction_trigger,
            compression_policy,
            wal_batch_config: self.wal_batch_config,
            simulated_cloud_local_storage_budget_bytes: self
                .simulated_cloud_local_storage_budget_bytes,
            lease_loss_hook: self.lease_loss_hook,
            lease_clock_skew_tolerance: self.lease_clock_skew_tolerance,
        })
    }

    fn resolve_total_memory(&self) -> MidgeResult<usize> {
        let total_memory = match self.memory_budget {
            MemoryBudget::Auto => memory::auto_memory_budget_bytes().unwrap_or(512 * 1024 * 1024),
            MemoryBudget::Bytes(bytes) => bytes,
        };
        if total_memory < 2 {
            return Err(MidgeError::ResourceLimit(
                "memory budget must hold two memtable generations".to_string(),
            ));
        }
        Ok(total_memory)
    }

    fn derive_memory_pools(&self, total_memory: usize) -> MidgeResult<DerivedMemoryPools> {
        let transaction_memory_pool_size = self
            .transaction_memory_pool_size
            .unwrap_or(total_memory / 10);
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

mod memory {
    #[cfg(target_os = "linux")]
    use std::fs;
    #[cfg(target_os = "linux")]
    use std::path::Path;

    pub fn auto_memory_budget_bytes() -> Option<usize> {
        let limit = effective_memory_limit_bytes()?;
        Some(budget_from_limit_bytes(limit))
    }

    fn budget_from_limit_bytes(limit: u64) -> usize {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let mut budget = limit / 2;
        let min_budget: usize = 64 * 1024 * 1024;
        if limit >= min_budget.saturating_mul(2) {
            budget = budget.max(min_budget);
        } else {
            budget = budget.max(1024 * 1024);
        }
        budget.min(limit).max(1)
    }

    fn effective_memory_limit_bytes() -> Option<u64> {
        let host_total = host_total_memory_bytes();
        let cgroup_limit = cgroup_memory_limit_bytes();

        match (host_total, cgroup_limit) {
            (Some(host), Some(limit)) => Some(host.min(limit)),
            (Some(host), None) => Some(host),
            (None, Some(limit)) => Some(limit),
            (None, None) => None,
        }
    }

    fn host_total_memory_bytes() -> Option<u64> {
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        let total_bytes = system.total_memory();
        if total_bytes == 0 {
            return None;
        }
        Some(total_bytes)
    }

    #[cfg(target_os = "linux")]
    fn cgroup_memory_limit_bytes() -> Option<u64> {
        let v2_limit = cgroup_v2_limit_bytes();
        if v2_limit.is_some() {
            return v2_limit;
        }
        cgroup_v1_limit_bytes()
    }

    #[cfg(not(target_os = "linux"))]
    fn cgroup_memory_limit_bytes() -> Option<u64> {
        None
    }

    #[cfg(target_os = "linux")]
    fn cgroup_v2_limit_bytes() -> Option<u64> {
        let controllers = Path::new("/sys/fs/cgroup/cgroup.controllers");
        if !controllers.exists() {
            return None;
        }
        let max_path = Path::new("/sys/fs/cgroup/memory.max");
        let value = fs::read_to_string(max_path).ok()?;
        let trimmed = value.trim();
        if trimmed.eq_ignore_ascii_case("max") {
            return None;
        }
        trimmed.parse::<u64>().ok().filter(|v| *v > 0)
    }

    #[cfg(target_os = "linux")]
    fn cgroup_v1_limit_bytes() -> Option<u64> {
        let max_path = Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes");
        let value = fs::read_to_string(max_path).ok()?;
        let limit = value.trim().parse::<u64>().ok()?;
        if limit == 0 {
            return None;
        }
        Some(limit)
    }

    #[cfg(test)]
    mod tests {
        use super::host_total_memory_bytes;

        #[test]
        fn should_treat_sysinfo_total_memory_as_bytes() {
            // Arrange
            let mut system = sysinfo::System::new();
            system.refresh_memory();
            let expected_bytes = system.total_memory();

            // Act
            let detected_bytes = host_total_memory_bytes();

            // Assert
            if expected_bytes == 0 {
                assert_eq!(detected_bytes, None);
            } else {
                assert_eq!(detected_bytes, Some(expected_bytes));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Goal Enum Tests ==========

    #[test]
    fn should_have_latency_as_default_goal() {
        assert_eq!(Goal::default(), Goal::Latency);
    }

    #[test]
    fn should_create_throughput_goal() {
        assert_eq!(Goal::Throughput, Goal::Throughput);
    }

    #[test]
    fn should_create_cost_goal() {
        assert_eq!(Goal::Economy, Goal::Economy);
    }

    #[test]
    fn should_distinguish_different_goals() {
        assert_ne!(Goal::Latency, Goal::Throughput);
        assert_ne!(Goal::Throughput, Goal::Economy);
        assert_ne!(Goal::Economy, Goal::Latency);
    }

    // ========== MemoryBudget Enum Tests ==========

    #[test]
    fn should_have_auto_as_default_memory_budget() {
        assert_eq!(MemoryBudget::default(), MemoryBudget::Auto);
    }

    #[test]
    fn should_create_explicit_memory_budget() {
        let budget = MemoryBudget::Bytes(4 * 1024 * 1024 * 1024);
        assert_eq!(budget, MemoryBudget::Bytes(4 * 1024 * 1024 * 1024));
    }

    #[test]
    fn should_distinguish_memory_budgets() {
        assert_ne!(MemoryBudget::Auto, MemoryBudget::Bytes(1000));
    }

    // ========== WorkloadProfile Enum Tests ==========

    #[test]
    fn should_have_mixed_as_default_workload() {
        assert_eq!(WorkloadProfile::default(), WorkloadProfile::Mixed);
    }

    #[test]
    fn should_create_write_heavy_workload() {
        assert_eq!(WorkloadProfile::WriteHeavy, WorkloadProfile::WriteHeavy);
    }

    #[test]
    fn should_create_read_mostly_workload() {
        assert_eq!(WorkloadProfile::ReadMostly, WorkloadProfile::ReadMostly);
    }

    #[test]
    fn should_create_range_scan_workload() {
        assert_eq!(WorkloadProfile::RangeScan, WorkloadProfile::RangeScan);
    }

    #[test]
    fn should_create_ttl_heavy_workload() {
        assert_eq!(WorkloadProfile::TtlHeavy, WorkloadProfile::TtlHeavy);
    }

    #[test]
    fn should_distinguish_workload_profiles() {
        // Arrange
        // (no setup required)

        // Act
        // (compare variants)

        // Assert
        assert_ne!(WorkloadProfile::Mixed, WorkloadProfile::WriteHeavy);
        assert_ne!(WorkloadProfile::WriteHeavy, WorkloadProfile::ReadMostly);
        assert_ne!(WorkloadProfile::ReadMostly, WorkloadProfile::RangeScan);
        assert_ne!(WorkloadProfile::RangeScan, WorkloadProfile::TtlHeavy);
    }

    // ========== Cloud Provider Constructor Tests ==========

    #[test]
    fn should_create_aws_s3_with_default_chain() {
        // Arrange
        let provider = CloudProviderConfig::aws_s3("bucket", "us-east-1");

        // Act
        // Assert
        assert_eq!(
            provider,
            CloudProviderConfig::AwsS3 {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                credentials: S3CredentialSource::AwsDefaultChain,
            }
        );
    }

    #[test]
    fn should_create_s3_compatible_env_with_safe_defaults() {
        // Arrange
        let provider = CloudProviderConfig::s3_compatible_env("bucket", "http://localhost:9000");

        // Act
        // Assert
        assert_eq!(
            provider,
            CloudProviderConfig::S3Compatible {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: "http://localhost:9000".to_string(),
                path_style: true,
                credentials: S3CredentialSource::Environment,
            }
        );
    }

    #[test]
    fn should_create_s3_compatible_config_with_explicit_overrides() {
        // Arrange
        let provider = CloudProviderConfig::s3_compatible(
            "bucket",
            "us-phoenix-1",
            "https://object.example.com",
            "key",
            "secret",
        )
        .with_path_style(false)
        .expect("path-style override");

        // Act
        // Assert
        assert_eq!(
            provider,
            CloudProviderConfig::S3Compatible {
                bucket: "bucket".to_string(),
                region: "us-phoenix-1".to_string(),
                endpoint: "https://object.example.com".to_string(),
                path_style: false,
                credentials: S3CredentialSource::access_key("key", "secret"),
            }
        );
    }

    #[test]
    fn should_create_azure_configs_for_supported_credentials() {
        // Arrange
        let identity = CloudProviderConfig::azure_blob("account", "container");
        let shared_key =
            CloudProviderConfig::azure_blob_shared_key("account", "container", "account-key");
        let sas = CloudProviderConfig::azure_blob_sas("account", "container", "?sig=token");
        let conn = CloudProviderConfig::azure_blob_connection_string(
            "container",
            "AccountName=account;AccountKey=key",
        );

        // Act
        // Assert
        assert!(matches!(
            identity,
            CloudProviderConfig::AzureBlob {
                credential: AzureCredentialSource::LightweightDefaultChain,
                ..
            }
        ));
        assert!(matches!(
            shared_key,
            CloudProviderConfig::AzureBlob {
                credential: AzureCredentialSource::SharedKey { .. },
                ..
            }
        ));
        assert!(matches!(
            sas,
            CloudProviderConfig::AzureBlob {
                credential: AzureCredentialSource::SasToken { .. },
                ..
            }
        ));
        assert!(matches!(
            conn,
            CloudProviderConfig::AzureBlob {
                account,
                credential: AzureCredentialSource::ConnectionString { .. },
                ..
            } if account == "account"
        ));
    }

    #[test]
    fn should_create_gcs_configs_with_matching_api_styles() {
        // Arrange
        let adc = CloudProviderConfig::gcs("bucket");
        let hmac = CloudProviderConfig::gcs_hmac("bucket", "access", "secret");
        let bearer = CloudProviderConfig::gcs_bearer_token("bucket", "token");

        // Act
        // Assert
        assert!(matches!(
            adc,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Json,
                credential: GcsCredentialSource::ApplicationDefault,
                ..
            }
        ));
        assert!(matches!(
            hmac,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Xml,
                credential: GcsCredentialSource::HmacKey { .. },
                ..
            }
        ));
        assert!(matches!(
            bearer,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Json,
                credential: GcsCredentialSource::BearerToken { .. },
                ..
            }
        ));
    }

    #[test]
    fn should_resolve_three_cloud_storage_locations() {
        // Arrange
        let wal = crate::config::CloudStorageLocation::new(
            CloudProviderConfig::aws_s3("wal-bucket", "us-east-1"),
            "database-a",
        );
        let sst = crate::config::CloudStorageLocation::new(
            CloudProviderConfig::aws_s3("sst-bucket", "us-east-1"),
            "database-a",
        );
        let control = CloudProviderConfig::aws_s3("control-bucket", "us-east-1");
        let topology = crate::config::CloudStorageTopology::new(wal.clone())
            .with_sst(sst.clone())
            .with_control(crate::config::CloudStorageLocation::new(
                control.clone(),
                "database-a",
            ));

        // Act
        let options = OpenOptions::cloud_multi("/tmp/midge-control-options", topology)
            .build()
            .expect("build separated cloud options");

        // Assert
        let configured = options
            .cloud_storage_topology()
            .expect("cloud topology should be retained");
        assert_eq!(configured.wal(), &wal);
        assert_eq!(configured.sst(), &sst);
        assert_eq!(configured.control().provider(), &control);
        assert_eq!(configured.control().prefix(), "database-a");
    }

    #[test]
    fn should_resolve_shared_cloud_topology() {
        // Arrange
        let shared = crate::config::CloudStorageLocation::new(
            CloudProviderConfig::aws_s3("shared-bucket", "us-east-1"),
            "database-a",
        );
        // Act
        let shared_options = OpenOptions::cloud("/tmp/midge-shared-options", shared.clone())
            .build()
            .expect("build shared cloud options");

        // Assert
        let shared_topology = shared_options
            .cloud_storage_topology()
            .expect("shared topology");
        assert_eq!(shared_topology.wal(), &shared);
        assert_eq!(shared_topology.sst(), &shared);
        assert_eq!(shared_topology.control(), &shared);
    }

    #[test]
    fn should_resolve_two_location_cloud_topology() {
        // Arrange
        let shared = crate::config::CloudStorageLocation::new(
            CloudProviderConfig::aws_s3("shared-bucket", "us-east-1"),
            "database-a",
        );
        let control = crate::config::CloudStorageLocation::new(
            CloudProviderConfig::aws_s3("control-bucket", "us-east-1"),
            "database-a",
        );

        // Act
        let two_location_options = OpenOptions::cloud_multi(
            "/tmp/midge-two-location-options",
            crate::config::CloudStorageTopology::new(shared.clone()).with_control(control.clone()),
        )
        .build()
        .expect("build two-location cloud options");

        // Assert
        let two_location_topology = two_location_options
            .cloud_storage_topology()
            .expect("two-location topology");
        assert_eq!(two_location_topology.wal(), &shared);
        assert_eq!(two_location_topology.sst(), &shared);
        assert_eq!(two_location_topology.control(), &control);
    }

    #[test]
    fn should_apply_fluent_cloud_modifiers() {
        // Arrange
        let s3 = CloudProviderConfig::s3_compatible_env("bucket", "http://old")
            .with_endpoint("http://new")
            .expect("endpoint override")
            .with_s3_region("eu-west-1")
            .expect("region override")
            .with_path_style(false)
            .expect("path-style override")
            .with_s3_credentials(S3CredentialSource::access_key("key", "secret"))
            .expect("s3 credentials");
        let gcs = CloudProviderConfig::gcs_hmac("bucket", "access", "secret")
            .with_gcs_credentials(GcsCredentialSource::application_default())
            .expect("gcs credentials");

        // Act
        // Assert
        assert_eq!(
            s3,
            CloudProviderConfig::S3Compatible {
                bucket: "bucket".to_string(),
                region: "eu-west-1".to_string(),
                endpoint: "http://new".to_string(),
                path_style: false,
                credentials: S3CredentialSource::access_key("key", "secret"),
            }
        );
        assert!(matches!(
            gcs,
            CloudProviderConfig::Gcs {
                api: GcsApiStyle::Json,
                credential: GcsCredentialSource::ApplicationDefault,
                ..
            }
        ));
    }

    #[test]
    fn should_reject_credential_provider_mismatch_given_provider_config_when_building() {
        // Arrange
        let result = CloudProviderConfig::gcs("bucket")
            .try_with_credentials(S3CredentialSource::access_key("key", "secret"));

        // Act
        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_unsupported_cloud_modifiers() {
        // Arrange
        // Act
        // Assert
        assert!(CloudProviderConfig::aws_s3("bucket", "us-east-1")
            .with_endpoint("http://localhost:9000")
            .is_err());
        assert!(CloudProviderConfig::gcs("bucket")
            .with_path_style(true)
            .is_err());
        assert!(CloudProviderConfig::azure_blob("account", "container")
            .with_s3_region("us-west-2")
            .is_err());
    }

    #[test]
    fn should_parse_azure_account_from_connection_string_config() {
        // Arrange
        let provider = CloudProviderConfig::azure_blob_connection_string(
            "container",
            "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key",
        );

        // Act
        // Assert
        assert!(matches!(
            provider,
            CloudProviderConfig::AzureBlob { account, .. } if account == "myaccount"
        ));
    }

    #[test]
    fn should_reject_connection_string_credential_override_without_account() {
        // Arrange
        let provider = CloudProviderConfig::AzureBlob {
            account: String::new(),
            container: "container".to_string(),
            endpoint: None,
            credential: AzureCredentialSource::connection_string("AccountKey=key"),
        };

        let result = provider.with_azure_credentials(AzureCredentialSource::shared_key("key"));

        // Act
        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_create_workload_identity_credentials_without_none_annotations() {
        // Arrange
        let client = AzureCredentialSource::workload_identity_for_client("client-id");
        let file = AzureCredentialSource::workload_identity_from_file("/var/run/token");
        let full = AzureCredentialSource::workload_identity_with(
            Some("tenant-id".to_string()),
            None,
            Some(PathBuf::from("/var/run/token")),
        );

        // Act
        // Assert
        assert!(matches!(
            client,
            AzureCredentialSource::WorkloadIdentity {
                client_id: Some(_),
                ..
            }
        ));
        assert!(matches!(
            file,
            AzureCredentialSource::WorkloadIdentity {
                token_file: Some(_),
                ..
            }
        ));
        assert!(matches!(
            full,
            AzureCredentialSource::WorkloadIdentity {
                tenant_id: Some(_),
                client_id: None,
                token_file: Some(_),
            }
        ));
    }

    #[test]
    fn should_clamp_memtable_for_small_explicit_budget() {
        // Arrange
        let budget = 64 * 1024 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .memory_budget(MemoryBudget::Bytes(budget))
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.memory_budget_bytes(), budget);
        assert!(opts.memtable_size_limit() <= budget / 2);
        assert!(opts.block_cache_size() <= budget);
    }

    #[test]
    fn should_use_explicit_memtable_size_for_flush_threshold_when_only_size_override_is_set() {
        // Arrange
        let size_limit = 128 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .with_memtable_size_limit(size_limit)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.memtable_size_limit(), size_limit);
        assert_eq!(opts.memtable_flush_threshold(), size_limit);
    }

    #[test]
    fn should_preserve_explicit_memtable_limits_when_both_are_set() {
        // Arrange
        let size_limit = 256 * 1024;
        let flush_threshold = 128 * 1024;

        // Act
        let opts = OpenOptions::in_memory()
            .with_memtable_size_limit(size_limit)
            .with_memtable_flush_threshold(flush_threshold)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(opts.memtable_size_limit(), size_limit);
        assert_eq!(opts.memtable_flush_threshold(), flush_threshold);
    }

    #[test]
    fn should_reject_zero_memtable_overrides_when_building() {
        // Arrange
        // (no setup required)

        // Act
        let result = OpenOptions::in_memory()
            .with_memtable_size_limit(0)
            .with_memtable_flush_threshold(0)
            .build();

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    }

    // ========== OpenOptions Builder Tests ==========

    #[test]
    fn should_create_in_memory_options() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory();

        // Assert
        assert_eq!(opts.storage, Storage::InMemory);
        assert_eq!(opts.goal, Goal::Latency);
        assert_eq!(opts.memory_budget, MemoryBudget::Auto);
        assert_eq!(opts.workload, WorkloadProfile::Mixed);
    }

    #[test]
    fn should_create_local_options_with_path() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::local("./test_db");

        // Assert
        assert_eq!(
            opts.storage,
            Storage::Local {
                path: PathBuf::from("./test_db")
            }
        );
    }

    #[test]
    fn should_set_goal_when_calling_goal() {
        // Arrange
        // Act
        let opts = OpenOptions::in_memory().goal(Goal::Throughput);

        // Assert
        assert_eq!(opts.goal, Goal::Throughput);
    }

    #[test]
    fn should_use_inclusive_worth_compressing_ratio_for_throughput_goal() {
        // Arrange
        let options = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .build()
            .expect("build throughput options");

        // Act
        let policy = options.compression_policy();

        // Assert
        assert!(matches!(
            policy,
            CompressionPolicy::Adaptive {
                min_savings_bytes: 256,
                min_ratio,
                check_algorithms,
            } if (*min_ratio - 0.95).abs() < f32::EPSILON
                && check_algorithms
                    == &[CompressionAlgo::Lz4, CompressionAlgo::Zstd3]
        ));
    }

    #[test]
    fn should_set_memory_budget_when_calling_memory_budget() {
        // Arrange
        let budget = MemoryBudget::Bytes(2 * 1024 * 1024 * 1024);

        // Act
        let opts = OpenOptions::in_memory().memory_budget(budget);

        // Assert
        assert_eq!(opts.memory_budget, budget);
    }

    #[test]
    fn should_set_workload_when_calling_workload() {
        let opts = OpenOptions::in_memory().workload(WorkloadProfile::WriteHeavy);
        assert_eq!(opts.workload, WorkloadProfile::WriteHeavy);
    }

    #[test]
    fn should_support_fluent_builder_chain() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::local("./db")
            .goal(Goal::Latency)
            .workload(WorkloadProfile::ReadMostly)
            .build()
            .expect("build options");

        // Assert
        assert_eq!(
            opts.storage,
            Storage::Local {
                path: PathBuf::from("./db")
            }
        );
        assert_eq!(opts.goal, Goal::Latency);
        assert_eq!(opts.workload, WorkloadProfile::ReadMostly);
    }

    #[test]
    fn should_derive_parameters_when_building() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory()
            .goal(Goal::Latency)
            .build()
            .expect("build options");

        // Assert
        assert!(opts.block_size() > 0);
        assert!(opts.memtable_size_limit() > 0);
        assert!(opts.target_sst_size() > 0);
        assert!(opts.block_cache_size() > 0);
    }

    #[test]
    fn should_use_different_block_sizes_for_different_goals() {
        // Arrange
        // (no setup required)

        // Act
        let latency_opts = OpenOptions::in_memory()
            .goal(Goal::Latency)
            .build()
            .expect("build latency options");
        let throughput_opts = OpenOptions::in_memory()
            .goal(Goal::Throughput)
            .build()
            .expect("build throughput options");

        // Assert
        assert_ne!(latency_opts.block_size(), throughput_opts.block_size());
    }

    #[test]
    fn should_use_different_memtable_sizes_for_different_workloads() {
        // Arrange
        // (no setup required)

        // Act
        let normal = OpenOptions::in_memory()
            .workload(WorkloadProfile::Mixed)
            .build()
            .expect("build normal options");
        let write_heavy = OpenOptions::in_memory()
            .workload(WorkloadProfile::WriteHeavy)
            .build()
            .expect("build write-heavy options");

        // Assert
        assert!(write_heavy.memtable_size_limit() >= normal.memtable_size_limit());
    }

    #[test]
    fn should_provide_getter_methods() {
        // Arrange
        // (no setup required)

        // Act
        let opts = OpenOptions::in_memory().build().expect("build options");

        // Assert - getters should be callable
        let _ = opts.block_size();
        let _ = opts.memtable_size_limit();
        let _ = opts.target_sst_size();
        let _ = opts.block_cache_size();
        let _ = opts.wal_buffer_size();
        let _ = opts.l0_compaction_trigger();
    }

    #[test]
    fn should_respect_explicit_memory_budget() {
        // Arrange
        // Use a realistic budget larger than 2x memtable size to have cache allocation
        let budget = MemoryBudget::Bytes(512 * 1024 * 1024); // 512MB

        // Act
        let opts = OpenOptions::in_memory()
            .memory_budget(budget)
            .build()
            .expect("build options");

        // Assert
        assert!(opts.block_cache_size() > 0);
    }

    #[test]
    fn should_clone_options() {
        // Arrange
        let original = OpenOptions::in_memory().goal(Goal::Throughput);

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.goal, original.goal);
    }

    #[test]
    fn should_use_half_ttl_clock_skew_tolerance_by_default() {
        // Arrange
        // (default configuration)

        // Act
        let options = OpenOptions::in_memory().build().expect("build options");

        // Assert
        assert_eq!(
            options.lease_clock_skew_tolerance(),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn should_reject_clock_skew_tolerance_larger_than_lease_ttl() {
        // Arrange
        let tolerance = Duration::from_secs(31);

        // Act
        let result = OpenOptions::in_memory()
            .lease_clock_skew_tolerance(tolerance)
            .build();

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    }
}
