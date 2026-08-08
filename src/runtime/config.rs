//! Runtime and cloud durability policy configuration.

use crate::wal::policy::BatchConfig;
use crate::wal::DurabilityPolicy;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoveredCloudWalSegment {
    pub max_sequence: u64,
    pub writer_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoveredCloudActiveWal {
    pub max_sequence: u64,
    pub writer_epoch: u64,
    pub record_count: usize,
    pub valid_bytes: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct CloudWalSealPolicy {
    pub min_segment_bytes: usize,
    pub max_flush_delay: Duration,
    pub max_pending_writes: usize,
}

impl Default for CloudWalSealPolicy {
    fn default() -> Self {
        Self {
            min_segment_bytes: 16 * 1024 * 1024,
            max_flush_delay: Duration::from_millis(500),
            max_pending_writes: 10_000,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CloudRuntimePolicy {
    pub eventual_flush_segment_gap: u64,
    pub wal_seal: CloudWalSealPolicy,
}

impl Default for CloudRuntimePolicy {
    fn default() -> Self {
        Self {
            eventual_flush_segment_gap: 128,
            wal_seal: CloudWalSealPolicy::default(),
        }
    }
}

/// Runtime configuration for wiring durability/storage behavior.
#[derive(Clone)]
pub struct RuntimeConfig {
    pub wal_durability_policy: DurabilityPolicy,
    pub wal_batch_config: BatchConfig,
    pub storage_io_timeout: Duration,
    /// Maximum time shutdown waits for `CloudAsync` uploads to drain before it
    /// reports an incomplete durability outcome. Production callers keep the
    /// default; deterministic shutdown tests inject a shorter budget.
    pub(crate) shutdown_cloud_drain_timeout: Duration,
    pub cloud_runtime_policy: CloudRuntimePolicy,
    pub hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    pub hybrid_storage_events: Option<crossbeam::channel::Receiver<crate::storage::StorageEvent>>,
    pub cloud_metadata_storage: Option<Arc<crate::storage::cloud::CloudStorage>>,
    /// Remote WAL segments replayed during startup and still eligible for
    /// manifest-covered cleanup.
    pub recovered_cloud_wal_segments: BTreeMap<u64, u64>,
    /// Writer epochs paired with `recovered_cloud_wal_segments` by segment id.
    /// Kept crate-private so the existing public runtime configuration remains
    /// source-compatible while engine startup retains recovery fencing proof.
    pub(crate) recovered_cloud_wal_segment_epochs: BTreeMap<u64, u64>,
    /// Locally recovered sealed WAL segments that were not present in remote
    /// storage and must resume upload before later segments advance the cloud
    /// durability frontier.
    pub(crate) recovered_local_wal_segments: BTreeMap<u64, u64>,
    /// Writer epochs paired with `recovered_local_wal_segments` by segment id.
    pub(crate) recovered_local_wal_segment_epochs: BTreeMap<u64, u64>,
    /// A recovered active WAL file that must be sealed and enqueued after the
    /// runtime has installed its cloud upload pipeline.
    pub(crate) recovered_cloud_active_wal: Option<RecoveredCloudActiveWal>,
    pub compression_policy: crate::sst::compression::CompressionPolicy,
    pub block_cache_size: usize,
    pub block_cache_policy: crate::sst::cache::CachePolicyType,
    pub l0_compaction_trigger: usize,
    pub background_compaction: bool,
    /// Fencing epoch from leader election.  Stamped on every WAL record.
    pub writer_epoch: u64,
    /// Shared lease-health flag.  Set to `false` by the heartbeat thread when
    /// renewal fails.  The event loop checks this before accepting writes.
    pub lease_healthy: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Leader store for epoch validation at WAL sync boundaries.
    /// Injected into the WAL actor so it can verify the epoch is still
    /// valid before flushing to durable storage.
    pub leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            wal_durability_policy: DurabilityPolicy::Batched,
            wal_batch_config: BatchConfig::default(),
            storage_io_timeout: crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
            shutdown_cloud_drain_timeout: Duration::from_secs(30),
            cloud_runtime_policy: CloudRuntimePolicy::default(),
            hybrid_storage: None,
            hybrid_storage_events: None,
            cloud_metadata_storage: None,
            recovered_cloud_wal_segments: BTreeMap::new(),
            recovered_cloud_wal_segment_epochs: BTreeMap::new(),
            recovered_local_wal_segments: BTreeMap::new(),
            recovered_local_wal_segment_epochs: BTreeMap::new(),
            recovered_cloud_active_wal: None,
            compression_policy: crate::sst::compression::CompressionPolicy::default(),
            block_cache_size: 128 * 1024 * 1024,
            block_cache_policy: crate::sst::cache::CachePolicyType::Lru,
            l0_compaction_trigger: 4,
            background_compaction: true,
            writer_epoch: 0,
            lease_healthy: None,
            leader_store: None,
        }
    }
}
