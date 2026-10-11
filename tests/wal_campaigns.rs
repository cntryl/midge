//! Engine campaigns (#831, #838).
//!
//! Seeded workloads drive an engine on a simulated device through clean
//! restarts, process crashes, power losses and injected faults. Cloud
//! campaigns add a simulated object store, which can fail, lose answers and
//! land requests late, and, in provider mode, takeovers by a process with a
//! fresh cache. After
//! every reopen the oracle judges every key the workload can touch, in every
//! family, against the history of commits, and a WAL invariant checks that
//! no segment name is durably given twice. What recovery serves becomes the
//! verified base for the next history: the engine may not lose a value it has
//! already returned after a crash.
//!
//! A finding carries a crash image: the device files and store objects of
//! the last recovery point before it, with the history, the names given so
//! far and the remaining workload, so replaying it needs nothing else. Images are written
//! below `MIDGE_CAMPAIGN_IMAGES` when that variable names a directory, and
//! load back for replay.
//!
//! Control campaigns, which must find nothing, run a seed budget: the
//! default per pull request, and `MIDGE_CAMPAIGN_PROFILE=nightly` for the
//! nightly run, which also moves its seeds to `MIDGE_CAMPAIGN_SEED_BASE`.
//! Detection campaigns keep fixed seeds, so they prove the same findings on
//! every run. `MIDGE_CAMPAIGN_REPLAY` names a saved image to replay.

use cntryl_midge::__internal::io::{Fs, FsPath};
use cntryl_midge::__internal::simulation::{
    Commit, CrashDevice, Failure, Fate, Fault, FaultEffect, FaultError, FaultId, FaultOp, History,
    Key, Logging, Operation, Schedule, SimulatedClock, SimulatedObjectStore, StoreImage, Verdict,
    Violation,
};
use cntryl_midge::{
    Clock, CloudProviderConfig, CloudStorageLocation, CloudWritePolicy, Engine, OpenOptions,
    TransactionMode, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// The lease TTL in milliseconds: long enough that a live process keeps
/// renewing its lease, short enough that a crashed process's engine, whose
/// threads outlive the simulated crash, stops retrying its lease release
/// within a second.
const LEASE_TTL_MILLIS: u64 = 1_000;
/// A TTL short enough that lease renewals interleave with crashes.
const RENEWING_LEASE_TTL_MILLIS: u64 = 60;
/// The shortest provider lease TTL the engine accepts, about: two thirds of it
/// must exceed the provider write margin.
const PROVIDER_LEASE_TTL_MILLIS: u64 = 18_000;
/// Every cloud campaign names its objects below this prefix.
const PREFIX: &str = "campaign";
/// Cloud campaigns seal an asynchronous WAL segment every few writes, so
/// several segments upload at once.
const SEAL_EVERY_WRITES: usize = 2;
/// How many times a faulting store's campaign restarts a process whose
/// open, or whose reads, the store's faults refused, as an operator would.
const ATTEMPTS: usize = 8;
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
/// The local lease's mutation lock. #811: a completed release's unlink is not
/// made durable, so a power loss can bring the lock back and block every
/// later open.
const LEASE_LOCK: &str = ".midge_leader.lock";
/// Workloads write keys `0..KEYS` in every family; recovery reads them all.
const KEYS: u8 = 6;
const DEFAULT: &str = "default";
/// How many times more seeds the nightly profile explores per control.
const NIGHTLY_SCALE: u64 = 100;
/// The seeds a detection campaign searches. Whether a seed reaches a bug can
/// depend on background timing, so the search is wide; it stops at the first
/// finding, which usually comes within the first few seeds.
const DETECTION_SEEDS: std::ops::Range<u64> = 0..200;

/// The seeds a control campaign explores: `per_pr` of them, or
/// [`NIGHTLY_SCALE`] times as many from the nightly seed base.
fn control_seeds(per_pr: u64) -> std::ops::Range<u64> {
    let nightly = std::env::var("MIDGE_CAMPAIGN_PROFILE").is_ok_and(|profile| profile == "nightly");
    let base = std::env::var("MIDGE_CAMPAIGN_SEED_BASE").map_or(0, |base| {
        base.parse()
            .expect("MIDGE_CAMPAIGN_SEED_BASE is a seed number")
    });
    if nightly {
        base..base + per_pr * NIGHTLY_SCALE
    } else {
        base..base + per_pr
    }
}

/// Where the database lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Mode {
    Local,
    /// Objects in the store; the lease and shared metadata in the cache, so
    /// a fresh cache would lose this mode's whole cloud.
    SimulatedCloud,
    /// Objects, the lease and shared metadata in the store.
    ProviderCloud,
}

/// What a commit is promised when the engine acknowledges it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Durability {
    /// Durable when acknowledged: `sync` locally, `cloud_strict` in cloud
    /// modes.
    Sync,
    /// Logged but not yet durable: `buffered` locally, `cloud_async` in cloud
    /// modes.
    Buffered,
    BestEffort,
}

impl Durability {
    fn options(self, mode: Mode) -> WriteOptions {
        match (self, mode) {
            (Self::Sync, Mode::Local) => WriteOptions::sync(),
            (Self::Buffered, Mode::Local) => WriteOptions::buffered(),
            (Self::Sync, Mode::SimulatedCloud | Mode::ProviderCloud) => {
                WriteOptions::cloud_strict()
            }
            (Self::Buffered, Mode::SimulatedCloud | Mode::ProviderCloud) => {
                WriteOptions::cloud_async()
            }
            (Self::BestEffort, _) => WriteOptions::best_effort(),
        }
    }

    fn logging(self) -> Logging {
        match self {
            Self::BestEffort => Logging::Unlogged,
            Self::Sync | Self::Buffered => Logging::Wal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Op {
    Put {
        family: usize,
        key: u8,
        durability: Durability,
    },
    Delete {
        family: usize,
        key: u8,
        durability: Durability,
    },
    Flush {
        family: usize,
    },
    Compact,
    CreateFamily,
    CleanRestart,
    ProcessCrash,
    PowerLoss {
        seed: u64,
    },
    /// Fail the `nth` later removal of a file below `wal/`.
    FailWalRemoval {
        nth: u64,
    },
    /// The process crashes and a new one, on a new host with an empty cache,
    /// takes over. Provider mode only.
    FreshCache,
    /// Every request the store holds back lands now, including those of
    /// processes that have since crashed.
    LandLate,
}

/// `SplitMix64`, so a seed fixes a workload on every platform.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[usize::try_from(self.below(items.len() as u64)).expect("index fits usize")]
    }

    fn key(&mut self) -> u8 {
        u8::try_from(self.below(u64::from(KEYS))).expect("key fits u8")
    }
}

/// Which operations a workload draws, by weight.
#[derive(Debug, Clone, Copy)]
enum Profile {
    /// Synced writes through process crashes and power losses.
    CrashControl,
    /// Synced and buffered writes, flushes, compaction, clean restarts and
    /// power losses, without the triggers of known #808 bugs: no best-effort
    /// writes, no buffered deletes (`lost-buffered-delete`), no faults, and
    /// no process crash before a power loss.
    RetentionControl,
    /// Buffered writes, a process crash, then a power loss.
    BufferedCrashes,
    /// Families created and written, a process crash, then a power loss:
    /// the review's manifest-journal directory-sync lead (#810).
    FamilyCrashes,
    /// Best-effort deletes around flushes and clean restarts.
    BestEffortDelete,
    /// An idle family, flushes, compaction and failing WAL removals.
    FailedUnlink,
    /// Flushes that retire every sealed segment, then restarts.
    SegmentRetirement,
    /// Strict and asynchronous cloud writes, strict deletes, flushes,
    /// compaction, clean restarts, power losses and, in provider mode,
    /// fresh-cache takeovers, without the triggers of known #808 bugs: no
    /// best-effort writes, no asynchronous deletes, no process crash before
    /// a power loss.
    CloudControl,
    /// Mostly asynchronous cloud writes with frequent flushes, so several
    /// segments upload at once, fresh-cache takeovers and late requests
    /// landing, against a faulting store.
    CloudFaults,
}

impl Profile {
    #[allow(clippy::too_many_lines)] // One weighted table per profile.
    fn op(self, rng: &mut Rng, families: usize, mode: Mode) -> Op {
        let family = usize::try_from(rng.below(families as u64)).expect("family fits usize");
        let key = rng.key();
        let logged = rng.pick(&[Durability::Sync, Durability::Buffered]);
        let put = |family, durability| Op::Put {
            family,
            key,
            durability,
        };
        let delete = |family, durability| Op::Delete {
            family,
            key,
            durability,
        };
        match self {
            Self::CrashControl => match rng.below(10) {
                0..=4 => put(0, Durability::Sync),
                5..=6 => delete(0, Durability::Sync),
                7 => Op::ProcessCrash,
                _ => Op::PowerLoss { seed: rng.next() },
            },
            Self::RetentionControl => match rng.below(14) {
                0..=4 => put(0, logged),
                5..=6 => delete(0, Durability::Sync),
                7..=8 => Op::Flush { family: 0 },
                9 => Op::Compact,
                10 => Op::CleanRestart,
                _ => Op::PowerLoss { seed: rng.next() },
            },
            Self::BufferedCrashes => match rng.below(8) {
                0..=4 => put(0, Durability::Buffered),
                5 => Op::ProcessCrash,
                _ => Op::PowerLoss { seed: rng.next() },
            },
            Self::FamilyCrashes => match rng.below(10) {
                0..=1 => Op::CreateFamily,
                2..=5 => put(family, Durability::Sync),
                6..=7 => Op::ProcessCrash,
                _ => Op::PowerLoss { seed: rng.next() },
            },
            Self::BestEffortDelete => match rng.below(10) {
                0..=3 => put(0, Durability::Sync),
                4..=5 => delete(0, rng.pick(&[Durability::Sync, Durability::BestEffort])),
                6..=7 => Op::Flush { family: 0 },
                _ => Op::CleanRestart,
            },
            Self::FailedUnlink => match rng.below(15) {
                0..=4 => put(family, Durability::Sync),
                5..=6 => delete(family, Durability::Sync),
                7..=8 => Op::Flush { family },
                9 => Op::Compact,
                10 => Op::CreateFamily,
                11..=12 => Op::FailWalRemoval {
                    nth: 1 + rng.below(2),
                },
                _ => Op::CleanRestart,
            },
            Self::SegmentRetirement => match rng.below(8) {
                0..=3 => put(0, logged),
                4..=5 => Op::Flush { family: 0 },
                _ => Op::CleanRestart,
            },
            Self::CloudControl => match rng.below(16) {
                0..=5 => put(0, logged),
                6..=7 => delete(0, Durability::Sync),
                8..=9 => Op::Flush { family: 0 },
                10 => Op::Compact,
                11 => Op::CleanRestart,
                12..=13 => Op::PowerLoss { seed: rng.next() },
                _ if mode == Mode::ProviderCloud => Op::FreshCache,
                _ => Op::CleanRestart,
            },
            Self::CloudFaults => match rng.below(16) {
                0..=6 => put(0, Durability::Buffered),
                7 => put(0, Durability::Sync),
                8..=11 => Op::Flush { family: 0 },
                12..=13 => Op::FreshCache,
                14 => Op::LandLate,
                _ => Op::CleanRestart,
            },
        }
    }

    fn workload(self, seed: u64, len: usize, mode: Mode) -> Vec<Op> {
        let mut rng = Rng(seed);
        let mut families = 1;
        (0..len)
            .map(|_| {
                let op = self.op(&mut rng, families, mode);
                if op == Op::CreateFamily {
                    families += 1;
                }
                op
            })
            .collect()
    }
}

/// What a campaign found.
#[derive(Debug, Clone)]
enum FindingKind {
    /// Recovered data no allowed history explains.
    Data(Violation),
    /// This WAL segment name was durably given to a file a second time.
    SegmentReuse(String),
    /// The engine refused to open after a crash it must survive, or to read
    /// what it recovered.
    Refused(String),
}

impl FindingKind {
    fn same_class(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }

    /// Whether a key came back that only an absent value explains.
    fn resurrects(&self) -> bool {
        matches!(self, Self::Data(violation) if violation.keys.iter().any(|mismatch| {
            mismatch.observed.is_some() && mismatch.allowed == [None]
        }))
    }

    /// Whether a write survived that no allowed history keeps: a later
    /// write outlived an earlier one it should not have.
    fn keeps_unallowed_write(&self) -> bool {
        matches!(self, Self::Data(violation) if violation.keys.iter().any(|mismatch| {
            mismatch.observed.is_some() && !mismatch.allowed.contains(&mismatch.observed)
        }))
    }

    /// Whether a value recovery had to keep came back absent.
    fn loses_required_value(&self) -> bool {
        matches!(self, Self::Data(violation) if violation.keys.iter().any(|mismatch| {
            mismatch.observed.is_none() && mismatch.allowed.iter().all(Option::is_some)
        }))
    }
}

/// What recovery may keep, judged two ways (#838). The local history is
/// rebased on what every recovery serves: the engine may not lose a value it
/// has returned from a cache that survived. A takeover with a fresh cache
/// keeps only what the cloud promised, which an asynchronous cloud write
/// served from a surviving cache never was, so the cloud history is rebased
/// only by fresh-cache takeovers and judges them alone. A recovery from a
/// surviving cache still removes the WAL commits it lost from the cloud
/// history: they left the WAL, so no later promise covers them.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Histories {
    local: History,
    cloud: History,
    /// Where the local history's commits start in the cloud history.
    offset: usize,
}

impl Histories {
    fn new(base: BTreeMap<Key, Vec<u8>>, families: BTreeSet<String>) -> Self {
        Self {
            local: History::new(base.clone(), families.clone()),
            cloud: History::new(base, families),
            offset: 0,
        }
    }

    /// Record a commit; returns its index in each history.
    fn commit(&mut self, commit: Commit) -> [usize; 2] {
        [self.local.commit(commit.clone()), self.cloud.commit(commit)]
    }

    fn synced(&mut self, [local, cloud]: [usize; 2]) {
        self.local.synced(local);
        self.cloud.synced(cloud);
    }

    fn flushed(&mut self, family: &str) {
        self.local.flushed(family);
        self.cloud.flushed(family);
    }

    fn shut_down(&mut self) {
        self.local.shut_down();
        self.cloud.shut_down();
    }

    fn create_family(&mut self, family: &str) {
        self.local.create_family(family);
        self.cloud.create_family(family);
    }

    /// Judge a recovery after `stop`, then rebase on what it served.
    fn check(
        &mut self,
        stop: Stop,
        observed: BTreeMap<Key, Vec<u8>>,
        families: BTreeSet<String>,
    ) -> Result<(), Violation> {
        if stop == Stop::FreshCache {
            if let Verdict::Violation(violation) = self.cloud.check(&observed, &families) {
                return Err(violation);
            }
            self.cloud = History::new(observed.clone(), families.clone());
        } else {
            match self.local.check(&observed, &families) {
                Verdict::Violation(violation) => return Err(violation),
                Verdict::Consistent { prefix } => self.cloud.forget_wal_from(self.offset + prefix),
            }
        }
        self.offset = self.cloud.len();
        self.local = History::new(observed, families);
        Ok(())
    }
}

/// A recovery point: everything a replay needs.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Image {
    #[serde(skip)]
    files: BTreeMap<String, Vec<u8>>,
    history: Histories,
    families: Vec<String>,
    names: BTreeMap<String, u64>,
    /// Values written so far, so replayed writes stay distinct.
    values: u64,
    /// The operations after this point, after the recovery itself.
    remaining: Vec<Op>,
    /// Whether this point follows a crash, rather than a clean shutdown or
    /// the start of the workload.
    crashed: bool,
    /// The lease clock's reading, so a replay never sees a lease record
    /// stamped in its future. Images without one were stamped by the system
    /// clock.
    #[serde(default)]
    lease_clock_millis: u64,
    /// The object store's contents and request counts.
    store: StoreImage,
    /// The workload's seed, which also draws the store's fates.
    seed: u64,
    /// How the campaign ran, so a replay runs and judges the same way.
    config: Config,
}

#[derive(Debug, Clone)]
struct Finding {
    kind: FindingKind,
    seed: u64,
    workload: Vec<Op>,
    image: Image,
    /// Injected faults that fired before the finding.
    faults_fired: usize,
}

/// How a campaign runs: where the database lives, the lease TTL, whether
/// the object store faults, which known #808 bug classes it reports, and how
/// it copes with #811.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Config {
    /// Report a reused WAL segment name (`restarted-segment-id`).
    segment_reuse: bool,
    /// After a power loss, remove a resurrected lease lock as an operator
    /// would (#811) instead of reporting the refused open.
    clear_stale_lease_lock: bool,
    /// The lease TTL, which sets how often renewals interleave with crashes.
    lease_ttl_millis: u64,
    mode: Mode,
    /// Fail, lose and delay WAL and SST requests by the workload's seed.
    store_faults: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            segment_reuse: true,
            clear_stale_lease_lock: true,
            lease_ttl_millis: LEASE_TTL_MILLIS,
            mode: Mode::Local,
            store_faults: false,
        }
    }
}

impl Config {
    /// A campaign against `mode`'s cloud.
    fn cloud(mode: Mode) -> Self {
        Self {
            lease_ttl_millis: if mode == Mode::ProviderCloud {
                PROVIDER_LEASE_TTL_MILLIS
            } else {
                LEASE_TTL_MILLIS
            },
            mode,
            ..Self::default()
        }
    }

    fn lease_ttl(self) -> Duration {
        Duration::from_millis(self.lease_ttl_millis)
    }

    /// How the store decides fates for a workload drawn from `seed`.
    fn schedule(self, seed: u64) -> Schedule {
        if self.store_faults {
            Schedule::Seeded {
                seed,
                fates: STORE_FATES.to_vec(),
                scope: vec![format!("{PREFIX}/wal/epochs/"), format!("{PREFIX}/sst/")],
                operations: vec![Operation::Put, Operation::Delete],
            }
        } else {
            Schedule::Eager
        }
    }
}

/// How the engine stopped before a recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    Clean,
    ProcessCrash,
    PowerLoss,
    /// A process crash, then a takeover by a process with an empty cache.
    FreshCache,
}

struct Campaign {
    device: CrashDevice,
    /// The object store every process of this campaign shares.
    store: SimulatedObjectStore,
    /// The current process's view of the store.
    view: SimulatedObjectStore,
    /// The workload's seed, which draws the store's fates.
    seed: u64,
    /// The lease clock every process of this campaign reads.
    clock: SimulatedClock,
    named: tempfile::TempDir,
    engine: Option<Engine>,
    history: Histories,
    families: Vec<String>,
    /// Names given before this device, when replaying an image.
    names_before: BTreeMap<String, u64>,
    /// Names the device gave while restoring an image's files.
    restored_names: BTreeMap<String, u64>,
    values: u64,
    images: Vec<Image>,
    faults: Vec<FaultId>,
    /// Resurrected lease locks removed after power losses (#811).
    stale_lease_locks: usize,
    config: Config,
}

impl Campaign {
    fn new(config: Config, seed: u64) -> Self {
        Self::from_image(&Image {
            files: BTreeMap::new(),
            history: Histories::new(BTreeMap::new(), BTreeSet::from([DEFAULT.to_string()])),
            families: vec![DEFAULT.to_string()],
            names: BTreeMap::new(),
            values: 0,
            remaining: Vec::new(),
            crashed: false,
            lease_clock_millis: 0,
            store: StoreImage::default(),
            seed,
            config,
        })
    }

    fn from_image(image: &Image) -> Self {
        let device = CrashDevice::with_files(&image.files);
        let restored_names = device.names_ever();
        let clock = SimulatedClock::new();
        clock.advance_to(image.lease_clock_millis);
        let store =
            SimulatedObjectStore::from_image(image.config.schedule(image.seed), &image.store);
        Self {
            device,
            view: store.process(),
            store,
            seed: image.seed,
            clock,
            named: tempfile::tempdir().expect("named directory"),
            engine: None,
            history: image.history.clone(),
            families: image.families.clone(),
            names_before: image.names.clone(),
            restored_names,
            values: image.values,
            images: Vec::new(),
            faults: Vec::new(),
            stale_lease_locks: 0,
            config: image.config,
        }
    }

    fn options(&self) -> OpenOptions {
        let root: Arc<dyn Fs> = Arc::new(self.device.fs());
        let named = self.named.path();
        let builder = match self.config.mode {
            Mode::Local => OpenOptions::local(named),
            Mode::SimulatedCloud => OpenOptions::cloud_simulated(named, "bucket", PREFIX),
            Mode::ProviderCloud => OpenOptions::cloud(
                named,
                CloudStorageLocation::new(
                    CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
                    format!("{PREFIX}/"),
                ),
            ),
        };
        let builder = match self.config.mode {
            Mode::Local => builder,
            Mode::SimulatedCloud | Mode::ProviderCloud => builder
                .object_store_for_testing(Arc::new(self.view.clone()))
                .cloud_write_policy(CloudWritePolicy {
                    wal_seal_max_pending_writes: SEAL_EVERY_WRITES,
                    ..CloudWritePolicy::default()
                }),
        };
        builder
            .background_compaction(false)
            .lease_ttl(self.config.lease_ttl())
            .lease_clock_for_testing(Arc::new(self.clock.clone()))
            .filesystem_for_testing(root)
            .build()
            .expect("open options")
    }

    fn engine(&self) -> &Engine {
        self.engine.as_ref().expect("engine is open")
    }

    /// Names durably given so far, over every device this history used.
    fn names(&self) -> BTreeMap<String, u64> {
        let mut names = self.names_before.clone();
        for (name, count) in self.device.names_ever() {
            let restored = self.restored_names.get(&name).copied().unwrap_or(0);
            *names.entry(name).or_default() += count - restored;
        }
        names
    }

    fn reused_segment(&self) -> Option<String> {
        if !self.config.segment_reuse {
            return None;
        }
        self.names().into_iter().find_map(|(name, count)| {
            let id = name.strip_prefix("wal/")?.strip_suffix(".wal")?;
            (count > 1 && id.bytes().all(|byte| byte.is_ascii_digit())).then_some(name)
        })
    }

    fn faults_fired(&self) -> usize {
        self.faults
            .iter()
            .filter(|fault| self.device.fired(**fault))
            .count()
    }

    /// Record a recovery point for replay.
    fn snapshot(&mut self, remaining: &[Op], crashed: bool) {
        let image = Image {
            files: self.device.files(),
            history: self.history.clone(),
            families: self.families.clone(),
            names: self.names(),
            values: self.values,
            remaining: remaining.to_vec(),
            crashed,
            lease_clock_millis: self.clock.now_millis(),
            store: self.store.image(),
            seed: self.seed,
            config: self.config,
        };
        self.images.push(image);
    }

    /// The recovery point a replay of `kind` starts from: the latest one, or
    /// for a reused segment name the latest before the reuse. The first
    /// point precedes the workload, so one always exists.
    fn image_for(&self, kind: &FindingKind) -> Image {
        self.images
            .iter()
            .rev()
            .find(|image| match kind {
                FindingKind::SegmentReuse(name) => image.names.get(name).copied().unwrap_or(0) <= 1,
                FindingKind::Data(_) | FindingKind::Refused(_) => true,
            })
            .cloned()
            .expect("the first recovery point precedes every finding")
    }

    /// Reopen after `stop`, then judge what recovery kept.
    fn recover(&mut self, stop: Stop) -> Result<(), FindingKind> {
        self.device.disarm_faults();
        if stop != Stop::Clean {
            // The restarted process's clock runs past the dead one's lease,
            // beyond the TTL and the default skew allowance of half the TTL,
            // so it takes the lease over at once (#835).
            self.clock.advance(self.config.lease_ttl() * 2);
            self.view = self.store.process();
        }
        let mut opened = self.open(stop);
        let mut attempts = 1;
        while opened.is_err() && self.config.store_faults && attempts < ATTEMPTS {
            // The refused process is gone and may have left its lease
            // behind; the next one takes it over.
            attempts += 1;
            self.view.process_crash();
            self.view = self.store.process();
            self.clock.advance(self.config.lease_ttl() * 2);
            opened = self.open(stop);
        }
        let engine = opened.map_err(|error| FindingKind::Refused(error.to_string()))?;
        self.engine = Some(engine);
        self.verify(stop)?;
        self.reused_segment()
            .map_or(Ok(()), |reuse| Err(FindingKind::SegmentReuse(reuse)))
    }

    /// Open the engine once, clearing a lease lock #811 resurrected.
    fn open(&mut self, stop: Stop) -> Result<Engine, cntryl_midge::MidgeError> {
        match Engine::open(self.options()) {
            Err(error)
                if stop == Stop::PowerLoss
                    && self.config.clear_stale_lease_lock
                    && error.to_string().contains(LEASE_LOCK) =>
            {
                self.stale_lease_locks += 1;
                self.device
                    .fs()
                    .remove_file(&FsPath::new(LEASE_LOCK))
                    .expect("remove the stale lease lock");
                Engine::open(self.options())
            }
            opened => opened,
        }
    }

    /// Read `key`; a faulting store's refused reads are retried, as reads
    /// change nothing.
    fn read(
        &self,
        tx: &cntryl_midge::Transaction,
        key: u8,
    ) -> Result<Option<Vec<u8>>, FindingKind> {
        let mut attempts = 1;
        loop {
            match tx.get(&[key]) {
                Ok(value) => return Ok(value.map(|value| value.to_vec())),
                Err(error) if !self.config.store_faults || attempts == ATTEMPTS => {
                    return Err(FindingKind::Refused(format!("read: {error}")));
                }
                Err(_) => attempts += 1,
            }
        }
    }

    /// Read every key the workload can write, in every family recovery
    /// holds, and judge it.
    fn verify(&mut self, stop: Stop) -> Result<(), FindingKind> {
        let engine = self.engine();
        let handles = engine.list_column_families().expect("list families");
        let families: BTreeSet<String> = handles
            .iter()
            .map(|family| family.name().to_string())
            .collect();
        let mut observed = BTreeMap::new();
        for handle in &handles {
            let tx = engine
                .begin_tx(handle.id(), TransactionMode::ReadOnly)
                .expect("read");
            for key in 0..KEYS {
                if let Some(value) = self.read(&tx, key)? {
                    observed.insert((handle.name().to_string(), vec![key]), value);
                }
            }
        }
        self.history
            .check(stop, observed, families)
            .map_err(FindingKind::Data)
    }

    fn family(&self, family: usize) -> String {
        self.families[family.min(self.families.len() - 1)].clone()
    }

    fn write(&mut self, family: usize, key: u8, delete: bool, durability: Durability) {
        let family = self.family(family);
        self.values += 1;
        let value = (!delete).then(|| format!("v{}", self.values).into_bytes());
        let engine = self.engine();
        let handle = engine.get_column_family(&family).expect("family exists");
        let committed = engine
            .begin_tx(handle.id(), TransactionMode::ReadWrite)
            .and_then(|mut tx| {
                match &value {
                    Some(value) => tx.put(vec![key], value.clone(), None)?,
                    None => tx.delete(vec![key])?,
                }
                tx.commit(durability.options(self.config.mode))
            });
        let indices = self.history.commit(Commit {
            family,
            writes: vec![(vec![key], value)],
            logging: durability.logging(),
            acknowledged: committed.is_ok(),
        });
        if committed.is_ok() && durability == Durability::Sync {
            self.history.synced(indices);
        }
    }

    /// Stop the engine by `stop`, record the recovery point, and recover.
    fn restart(
        &mut self,
        stop: Stop,
        remaining: &[Op],
        power_seed: u64,
    ) -> Result<(), FindingKind> {
        if stop == Stop::Clean {
            let mut engine = self.engine.take().expect("engine is open");
            if engine.shutdown(Duration::from_secs(10)).is_ok() {
                self.history.shut_down();
            }
        } else {
            self.device.process_crash();
            self.view.process_crash();
            if stop == Stop::PowerLoss {
                self.device.power_loss(power_seed);
            }
            // The fenced process can no longer reach the device or the store.
            drop(self.engine.take());
            if stop == Stop::FreshCache {
                self.names_before = self.names();
                self.device = CrashDevice::new();
                self.restored_names = BTreeMap::new();
            }
        }
        self.snapshot(remaining, stop != Stop::Clean);
        self.recover(stop)
    }

    fn apply(&mut self, op: &Op, remaining: &[Op]) -> Result<(), FindingKind> {
        match op {
            Op::Put {
                family,
                key,
                durability,
            } => self.write(*family, *key, false, *durability),
            Op::Delete {
                family,
                key,
                durability,
            } => self.write(*family, *key, true, *durability),
            Op::Flush { family } => {
                let name = self.family(*family);
                let engine = self.engine();
                let handle = engine.get_column_family(&name).expect("family exists");
                if engine.flush_cf(&handle).is_ok() {
                    self.history.flushed(&name);
                }
            }
            Op::Compact => {
                let _ = self.engine().compact_all();
            }
            Op::CreateFamily => {
                let name = format!("family-{}", self.families.len());
                if self.engine().create_column_family(&name).is_ok() {
                    self.history.create_family(&name);
                    self.families.push(name);
                }
            }
            Op::CleanRestart => self.restart(Stop::Clean, remaining, 0)?,
            Op::ProcessCrash => self.restart(Stop::ProcessCrash, remaining, 0)?,
            Op::PowerLoss { seed } => self.restart(Stop::PowerLoss, remaining, *seed)?,
            Op::FreshCache => self.restart(Stop::FreshCache, remaining, 0)?,
            Op::LandLate => {
                let _ = self.store.land_late_effects();
            }
            Op::FailWalRemoval { nth } => {
                let fault = self.device.inject(Fault {
                    op: FaultOp::RemoveFile,
                    path: "wal".into(),
                    nth: *nth,
                    effect: FaultEffect::Before,
                    error: FaultError::Io,
                });
                self.faults.push(fault);
            }
        }
        self.reused_segment()
            .map_or(Ok(()), |reuse| Err(FindingKind::SegmentReuse(reuse)))
    }

    /// Run `workload` and a final clean restart from the recovery point
    /// `campaign` holds; return the first finding.
    fn drive(&mut self, workload: &[Op], stop: Stop) -> Result<(), FindingKind> {
        self.recover(stop)?;
        let mut full = workload.to_vec();
        full.push(Op::CleanRestart);
        for (step, op) in full.iter().enumerate() {
            self.apply(op, &full[step + 1..])?;
        }
        let mut engine = self.engine.take().expect("engine is open");
        let shutdown = engine.shutdown(Duration::from_secs(10));
        // A faulting store may fail a shutdown's final upload.
        assert!(
            shutdown.is_ok() || self.config.store_faults,
            "final shutdown: {shutdown:?}"
        );
        Ok(())
    }

    /// Run `workload` on a fresh device; return the first finding.
    fn run(seed: u64, workload: &[Op], config: Config) -> Result<(), Box<Finding>> {
        let mut campaign = Self::new(config, seed);
        campaign.snapshot(workload, false);
        campaign.drive(workload, Stop::Clean).map_err(|kind| {
            Box::new(Finding {
                image: campaign.image_for(&kind),
                faults_fired: campaign.faults_fired(),
                kind,
                seed,
                workload: workload.to_vec(),
            })
        })
    }

    /// Replay an image from its recovery point; return what it finds.
    fn replay(image: &Image) -> Result<(), FindingKind> {
        let mut campaign = Self::from_image(image);
        let stop = if image.crashed {
            Stop::PowerLoss
        } else {
            Stop::Clean
        };
        campaign.drive(&image.remaining, stop)
    }
}

/// Run `profile` over `seeds`; return the first finding, shrunk.
fn campaign(
    profile: Profile,
    seeds: std::ops::Range<u64>,
    len: usize,
    config: Config,
) -> Option<Finding> {
    let found = seeds
        .map(|seed| (seed, profile.workload(seed, len, config.mode)))
        .find_map(|(seed, workload)| Campaign::run(seed, &workload, config).err())?;
    Some(shrink(*found, config))
}

/// Drop operations one at a time while the same class of finding remains.
fn shrink(mut finding: Finding, config: Config) -> Finding {
    let mut index = 0;
    let mut attempts = 0;
    while index < finding.workload.len() && attempts < 80 {
        attempts += 1;
        let mut candidate = finding.workload.clone();
        candidate.remove(index);
        match Campaign::run(finding.seed, &candidate, config) {
            Err(smaller) if smaller.kind.same_class(&finding.kind) => finding = *smaller,
            _ => index += 1,
        }
    }
    finding
}

/// Write `image` below `directory`: its files under `files/`, the rest as
/// JSON. An earlier image there is replaced.
fn save_image(directory: &Path, image: &Image) {
    if directory.exists() {
        std::fs::remove_dir_all(directory).expect("replace earlier image");
    }
    for (path, bytes) in &image.files {
        let target = directory.join("files").join(path);
        std::fs::create_dir_all(target.parent().expect("image file parent"))
            .expect("image directory");
        std::fs::write(target, bytes).expect("write image file");
    }
    std::fs::create_dir_all(directory).expect("image directory");
    std::fs::write(
        directory.join("image.json"),
        serde_json::to_vec_pretty(image).expect("encode image"),
    )
    .expect("write image");
}

/// Read an image `save_image` wrote.
fn load_image(directory: &Path) -> Image {
    let mut image: Image =
        serde_json::from_slice(&std::fs::read(directory.join("image.json")).expect("read image"))
            .expect("decode image");
    let root = directory.join("files");
    let mut pending = vec![root.clone()];
    while let Some(folder) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&folder) else {
            continue;
        };
        for entry in entries {
            let path = entry.expect("image entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let relative = path
                    .strip_prefix(&root)
                    .expect("image file below root")
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                image
                    .files
                    .insert(relative, std::fs::read(&path).expect("read image file"));
            }
        }
    }
    image
}

/// Where `finding`'s image is kept, below `MIDGE_CAMPAIGN_IMAGES`.
fn kept_image_directory(case: &str, finding: &Finding) -> Option<PathBuf> {
    std::env::var_os("MIDGE_CAMPAIGN_IMAGES")
        .map(|root| Path::new(&root).join(format!("{case}-seed-{}", finding.seed)))
}

/// Write `finding`'s image to `directory`, with a readable account of it.
fn save_finding(directory: &Path, finding: &Finding) {
    save_image(directory, &finding.image);
    std::fs::write(
        directory.join("finding.txt"),
        format!("{:#?}\n\nworkload: {:#?}\n", finding.kind, finding.workload),
    )
    .expect("write finding");
}

/// Save `finding`'s image below `MIDGE_CAMPAIGN_IMAGES`, or a temporary
/// directory, and load it back: replays run from what was saved.
fn saved_and_loaded(case: &str, finding: &Finding) -> Image {
    let temporary = tempfile::tempdir().expect("image directory");
    let directory =
        kept_image_directory(case, finding).unwrap_or_else(|| temporary.path().join(case));
    save_finding(&directory, finding);
    load_image(&directory)
}

/// Assert a control campaign found nothing. A finding's image is kept below
/// `MIDGE_CAMPAIGN_IMAGES` first, so CI uploads it for replay.
fn assert_clean(case: &str, finding: Option<Finding>) {
    let Some(finding) = finding else {
        return;
    };
    let directory = kept_image_directory(case, &finding);
    if let Some(directory) = &directory {
        save_finding(directory, &finding);
    }
    panic!("{case}: the control found {finding:#?}\nimage: {directory:?}");
}

/// Assert `finding` matches `signature`, save its image, and prove the saved
/// image replays it.
fn assert_reproduced(case: &str, finding: Option<Finding>, signature: impl Fn(&Finding) -> bool) {
    let finding = finding.unwrap_or_else(|| panic!("{case}: the campaign found nothing"));
    assert!(signature(&finding), "{case}: found {finding:#?}");
    let image = saved_and_loaded(case, &finding);
    let replayed = Campaign::replay(&image);
    assert!(
        replayed
            .as_ref()
            .is_err_and(|kind| kind.same_class(&finding.kind)),
        "{case}: the saved image did not reproduce {:?}: {replayed:?}",
        finding.kind
    );
}

#[test]
fn should_find_no_violation_when_synced_writes_survive_crashes() {
    // Arrange: lease renewals interleave with the crashes.
    let workloads = control_seeds(32);
    let config = Config {
        lease_ttl_millis: RENEWING_LEASE_TTL_MILLIS,
        ..Config::default()
    };

    // Act
    let finding = campaign(Profile::CrashControl, workloads, 24, config);

    // Assert
    assert_clean("crash-control", finding);
}

#[test]
fn should_find_no_violation_when_buffered_writes_retire_through_power_loss() {
    // Arrange: segment-name reuse after restarts is the known
    // `restarted-segment-id`, reported by its own campaign.
    let workloads = control_seeds(32);
    let config = Config {
        segment_reuse: false,
        ..Config::default()
    };

    // Act
    let finding = campaign(Profile::RetentionControl, workloads, 30, config);

    // Assert
    assert_clean("retention-control", finding);
}

#[test]
fn should_keep_created_families_when_process_crash_precedes_power_loss() {
    // Arrange: the review's lead is that a manifest journal created but not
    // dir-synced before a process crash loses its name at the next power
    // loss.
    let workloads = control_seeds(32);

    // Act
    let finding = campaign(Profile::FamilyCrashes, workloads, 24, Config::default());

    // Assert
    assert_clean("family-crashes", finding);
}

#[test]
fn should_reproduce_best_effort_delete_when_campaigning() {
    // Arrange
    let config = Config {
        segment_reuse: false,
        ..Config::default()
    };

    // Act
    let finding = campaign(Profile::BestEffortDelete, DETECTION_SEEDS, 16, config);

    // Assert: a key came back after a best-effort delete.
    assert_reproduced("best-effort-delete", finding, |finding| {
        finding.kind.resurrects()
            && finding.workload.iter().any(|op| {
                matches!(
                    op,
                    Op::Delete {
                        durability: Durability::BestEffort,
                        ..
                    }
                )
            })
    });
}

#[test]
fn should_reproduce_failed_local_unlink_when_campaigning() {
    // Arrange
    let config = Config {
        segment_reuse: false,
        ..Config::default()
    };

    // Act
    let finding = campaign(Profile::FailedUnlink, DETECTION_SEEDS, 30, config);

    // Assert: a deleted key came back after a WAL removal failed.
    assert_reproduced("failed-local-unlink", finding, |finding| {
        finding.kind.resurrects() && finding.faults_fired > 0
    });
}

#[test]
fn should_reproduce_restarted_segment_id_when_campaigning() {
    // Arrange
    let workloads = DETECTION_SEEDS;

    // Act
    let finding = campaign(Profile::SegmentRetirement, workloads, 16, Config::default());

    // Assert
    assert_reproduced(
        "restarted-segment-id",
        finding,
        |finding| matches!(&finding.kind, FindingKind::SegmentReuse(name) if name.starts_with("wal/")),
    );
}

#[test]
fn should_reproduce_recovered_tail_loss_when_campaigning() {
    // Arrange
    let workloads = DETECTION_SEEDS;

    // Act
    let finding = campaign(Profile::BufferedCrashes, workloads, 12, Config::default());

    // Assert: a value recovery served after a process crash was lost.
    assert_reproduced("recovered-tail-not-durable", finding, |finding| {
        finding.kind.loses_required_value()
            && finding.workload.contains(&Op::ProcessCrash)
            && finding
                .workload
                .iter()
                .any(|op| matches!(op, Op::PowerLoss { .. }))
    });
}

#[test]
fn should_reproduce_stale_lease_lock_when_power_is_lost() {
    // Arrange: #811, found by power losses; the operator's workaround is off.
    let config = Config {
        clear_stale_lease_lock: false,
        ..Config::default()
    };

    // Act
    let found = DETECTION_SEEDS
        .map(|seed| (seed, Profile::CrashControl.workload(seed, 24, Mode::Local)))
        .find_map(|(seed, workload)| Campaign::run(seed, &workload, config).err())
        .map(|finding| *finding);

    // Assert
    assert_reproduced(
        "stale-lease-lock",
        found,
        |finding| matches!(&finding.kind, FindingKind::Refused(error) if error.contains(LEASE_LOCK)),
    );
}

/// Replay the image `MIDGE_CAMPAIGN_REPLAY` names, as a campaign saved it
/// below `MIDGE_CAMPAIGN_IMAGES`; CI uploads those as artifacts. Without the
/// variable there is nothing to replay.
#[test]
fn should_replay_saved_image_when_one_is_named() {
    // Arrange
    let Some(directory) = std::env::var_os("MIDGE_CAMPAIGN_REPLAY") else {
        return;
    };
    let image = load_image(Path::new(&directory));

    // Act
    let replayed = Campaign::replay(&image);

    // Assert
    assert!(replayed.is_ok(), "the image reproduces {replayed:#?}");
}

/// Registered RED case `recovered-tail-not-durable`, found by the
/// buffered-crash campaign: recovery replays a buffered WAL tail from the page
/// cache and serves it, but never makes it durable, so a later power loss
/// loses data the engine already returned.
#[test]
#[ignore = "#808 stage 2: recovered-tail-not-durable"]
fn should_keep_recovered_tail_when_power_is_lost_after_a_process_crash() {
    // Arrange: a buffered put, then the process dies and a restarted process
    // recovers and serves the replayed tail.
    let mut campaign = Campaign::new(Config::default(), 0);
    campaign.recover(Stop::Clean).expect("first open");
    campaign.write(0, 1, false, Durability::Buffered);
    campaign.device.process_crash();
    drop(campaign.engine.take());
    campaign
        .recover(Stop::ProcessCrash)
        .expect("recover from the process crash");
    let served = campaign
        .history
        .local
        .keys()
        .contains(&(DEFAULT.to_string(), vec![1]));

    // Act: power is lost with nothing unsynced reaching the device.
    campaign.device.power_loss_with(|_| 0);
    drop(campaign.engine.take());
    let recovered = campaign.recover(Stop::PowerLoss);

    // Assert
    assert!(served, "recovery did not serve the buffered tail");
    assert!(
        recovered.is_ok(),
        "recovered WAL tail was served, then lost after power loss: {recovered:?}"
    );
}

/// Registered RED case `lost-buffered-delete`, found by the retention
/// control's nightly budget: a buffered delete is flushed into an SST, a power
/// loss drops its unsynced WAL record, and compaction then drops the flushed
/// tombstone together with the put it shadowed, so recovery replays the put.
#[test]
#[ignore = "#808 stage 3: lost-buffered-delete"]
fn should_not_resurrect_flushed_delete_when_power_loss_drops_its_wal_record() {
    // Arrange: a synced put, a buffered delete of it and a completed flush,
    // then power is lost with the WAL page from before the delete.
    let mut campaign = Campaign::new(Config::default(), 0);
    campaign.recover(Stop::Clean).expect("first open");
    campaign.write(0, 1, false, Durability::Sync);
    campaign.write(0, 1, true, Durability::Buffered);
    campaign
        .apply(&Op::Flush { family: 0 }, &[])
        .expect("flush");
    campaign.device.process_crash();
    campaign.device.power_loss_with(|_| 0);
    drop(campaign.engine.take());
    let recovered = campaign.recover(Stop::PowerLoss);

    // Act
    campaign.apply(&Op::Compact, &[]).expect("compact");
    let after_compaction = campaign.apply(&Op::ProcessCrash, &[]);

    // Assert
    assert!(
        recovered.is_ok(),
        "recovery after the power loss: {recovered:?}"
    );
    assert!(
        after_compaction.is_ok(),
        "flushed delete resurrected after compaction: {after_compaction:?}"
    );
}

#[test]
fn should_find_no_violation_when_provider_writes_survive_crashes_and_takeovers() {
    // Arrange: segment-name reuse after restarts is the known
    // `restarted-segment-id`, reported by its own campaign.
    let workloads = control_seeds(16);
    let config = Config {
        segment_reuse: false,
        ..Config::cloud(Mode::ProviderCloud)
    };

    // Act
    let finding = campaign(Profile::CloudControl, workloads, 24, config);

    // Assert
    assert_clean("provider-control", finding);
}

#[test]
fn should_find_no_violation_when_simulated_cloud_writes_survive_crashes() {
    // Arrange: segment-name reuse after restarts is the known
    // `restarted-segment-id`, reported by its own campaign.
    let workloads = control_seeds(16);
    let config = Config {
        segment_reuse: false,
        ..Config::cloud(Mode::SimulatedCloud)
    };

    // Act
    let finding = campaign(Profile::CloudControl, workloads, 24, config);

    // Assert
    assert_clean("simulated-cloud-control", finding);
}

#[test]
fn should_reproduce_out_of_order_publication_when_cloud_uploads_fail() {
    // Arrange: WAL and SST uploads fail before or after their effect, lose
    // their answer or land late; `out-of-order-publication` (#818).
    let config = Config {
        segment_reuse: false,
        store_faults: true,
        ..Config::cloud(Mode::ProviderCloud)
    };

    // Act
    let finding = campaign(Profile::CloudFaults, DETECTION_SEEDS, 12, config);

    // Assert: after a fresh-cache takeover, a later write survived an
    // earlier one the catalog skipped.
    assert_reproduced("out-of-order-publication", finding, |finding| {
        finding.kind.keeps_unallowed_write() && finding.workload.contains(&Op::FreshCache)
    });
}
