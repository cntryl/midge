//! Where a campaign runs and what its store does: modes, configurations
//! and store fault schedules.

use crate::workload::Durability;
use cntryl_midge::__internal::simulation::{
    Failure, Fate, Operation, RequestId, Schedule, SimulatedObjectStore,
};
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, CloudWritePolicy, OpenOptions, OpenOptionsBuilder,
    WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// The lease TTL in milliseconds: long enough that a live process keeps
/// renewing its lease, short enough that a crashed process's engine, whose
/// threads outlive the simulated crash, stops retrying its lease release
/// within a second.
const LEASE_TTL_MILLIS: u64 = 1_000;
/// A TTL short enough that lease renewals interleave with crashes.
pub(crate) const RENEWING_LEASE_TTL_MILLIS: u64 = 60;
/// The shortest cloud lease TTL the engine accepts, about: two thirds of it
/// must exceed the provider write margin. Both cloud modes hold the cloud
/// lease.
const CLOUD_LEASE_TTL_MILLIS: u64 = 18_000;
/// Every cloud campaign names its objects below this prefix.
const PREFIX: &str = "campaign";
/// Cloud campaigns seal an asynchronous WAL segment every few writes, so
/// several segments upload at once.
const SEAL_EVERY_WRITES: usize = 2;
/// What a faulting store does to WAL and SST uploads and deletes: mostly
/// answer, else fail before or after the effect, lose the answer, or land
/// late. Reads stay eager: a refused read only makes an open fail closed.
const STORE_FATES: &[Fate] = &[
    Fate::Normal,
    Fate::Normal,
    Fate::Normal,
    Fate::Normal,
    Fate::Normal,
    Fate::Normal,
    Fate::FailBefore(Failure::Server),
    Fate::FailBefore(Failure::Transport),
    Fate::FailAfter(Failure::Timeout),
    Fate::Late,
];

/// Where the database lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Mode {
    Local,
    /// Objects in the store; the lease and shared metadata in the cache, so
    /// a fresh cache would lose this mode's whole cloud.
    SimulatedCloud,
    /// Objects, the lease and shared metadata in the store.
    ProviderCloud,
}

impl Mode {
    /// Options for a database named `named`, whose cloud objects, if any,
    /// live in `store`.
    pub(crate) fn open_builder(
        self,
        named: &Path,
        store: &SimulatedObjectStore,
    ) -> OpenOptionsBuilder {
        let cloud = |builder: OpenOptionsBuilder| {
            builder
                .object_store_for_testing(Arc::new(store.clone()))
                .cloud_write_policy(CloudWritePolicy {
                    wal_seal_max_pending_writes: SEAL_EVERY_WRITES,
                    ..CloudWritePolicy::default()
                })
        };
        match self {
            Self::Local => OpenOptions::local(named),
            Self::SimulatedCloud => cloud(OpenOptions::cloud_simulated(named, "bucket", PREFIX)),
            Self::ProviderCloud => cloud(OpenOptions::cloud(
                named,
                CloudStorageLocation::new(
                    CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
                    format!("{PREFIX}/"),
                ),
            )),
        }
    }

    /// What `durability` asks of the engine here.
    pub(crate) fn write_options(self, durability: Durability) -> WriteOptions {
        match (durability, self) {
            (Durability::Sync, Self::Local) => WriteOptions::sync(),
            (Durability::Buffered, Self::Local) => WriteOptions::buffered(),
            (Durability::Sync, Self::SimulatedCloud | Self::ProviderCloud) => {
                WriteOptions::cloud_strict()
            }
            (Durability::Buffered, Self::SimulatedCloud | Self::ProviderCloud) => {
                WriteOptions::cloud_async()
            }
            (Durability::BestEffort, _) => WriteOptions::best_effort(),
        }
    }

    /// The lease TTL campaigns use here.
    fn lease_ttl_millis(self) -> u64 {
        match self {
            Self::Local => LEASE_TTL_MILLIS,
            Self::SimulatedCloud | Self::ProviderCloud => CLOUD_LEASE_TTL_MILLIS,
        }
    }

    /// Whether a new process with an empty cache can take this database
    /// over: only when everything but the cache lives in the store.
    pub(crate) fn has_fresh_cache_takeovers(self) -> bool {
        self == Self::ProviderCloud
    }

    /// Whether the database uses the object store at all.
    fn uses_store(self) -> bool {
        self != Self::Local
    }
}

/// How a campaign runs: where the database lives, the lease TTL, whether
/// the object store faults, which known #808 bug classes it reports, and how
/// it copes with #811.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct Config {
    /// Report a reused WAL segment name (`restarted-segment-id`).
    pub(crate) segment_reuse: bool,
    /// After a power loss, remove a resurrected lease or writer-effect lock
    /// as an operator would (#811) instead of reporting the refused open.
    pub(crate) clear_stale_lease_lock: bool,
    /// The lease TTL, which sets how often renewals interleave with crashes.
    pub(crate) lease_ttl_millis: u64,
    pub(crate) mode: Mode,
    pub(crate) store_faults: StoreFaults,
}

/// What the object store does to WAL and SST uploads and deletes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StoreFaults {
    None,
    /// Draw each request's fate from [`STORE_FATES`] by the workload's seed.
    Seeded,
    /// The first `attempts` uploads of WAL segment `segment` fail, in every
    /// writer epoch.
    FailSegment {
        segment: u64,
        attempts: u64,
    },
}

impl Default for Config {
    fn default() -> Self {
        Self {
            segment_reuse: true,
            clear_stale_lease_lock: true,
            lease_ttl_millis: LEASE_TTL_MILLIS,
            mode: Mode::Local,
            store_faults: StoreFaults::None,
        }
    }
}

impl Config {
    /// A campaign against `mode`'s cloud.
    pub(crate) fn cloud(mode: Mode) -> Self {
        Self {
            lease_ttl_millis: mode.lease_ttl_millis(),
            mode,
            ..Self::default()
        }
    }

    /// Refuse a configuration whose parts cannot work together: store
    /// faults need a mode that uses the store.
    pub(crate) fn validate(self) {
        assert!(
            !self.faulting() || self.mode.uses_store(),
            "store faults need a cloud mode: {self:?}"
        );
    }

    pub(crate) fn lease_ttl(self) -> Duration {
        Duration::from_millis(self.lease_ttl_millis)
    }

    /// How the store decides fates for a workload drawn from `seed`.
    pub(crate) fn schedule(self, seed: u64) -> Schedule {
        match self.store_faults {
            StoreFaults::None => Schedule::Eager,
            StoreFaults::Seeded => Schedule::Seeded {
                seed,
                fates: STORE_FATES.to_vec(),
                scope: vec![format!("{PREFIX}/wal/epochs/"), format!("{PREFIX}/sst/")],
                operations: vec![Operation::Put, Operation::Delete],
            },
            StoreFaults::FailSegment { segment, attempts } => Schedule::Scripted(
                (1..=8u64)
                    .flat_map(|epoch| {
                        (0..attempts).map(move |occurrence| {
                            let key = format!("{PREFIX}/wal/epochs/{epoch:020}/{segment:020}.wal");
                            (
                                RequestId {
                                    operation: Operation::Put,
                                    key,
                                    occurrence,
                                },
                                Fate::FailBefore(Failure::Server),
                            )
                        })
                    })
                    .collect(),
            ),
        }
    }

    pub(crate) fn faulting(self) -> bool {
        self.store_faults != StoreFaults::None
    }
}
