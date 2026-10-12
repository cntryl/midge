//! A campaign: one simulated world, the engine running in it, the ops that
//! drive it, and the recovery points a finding replays from.

use crate::config::Config;
use crate::workload::{Durability, Op, DEFAULT, KEYS};
use cntryl_midge::__internal::io::{Fs, FsPath};
use cntryl_midge::__internal::simulation::{
    Commit, CrashDevice, Decision, Fault, FaultEffect, FaultError, FaultOp, Histories, Key,
    Recovery, SimulatedClock, SimulatedObjectStore, StoreImage, Violation, FAILURE_MESSAGE,
};
use cntryl_midge::{Clock, Engine, OpenOptions, TransactionMode};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

/// How many times a faulting store's campaign restarts a process whose open
/// the store's faults refused, as an operator would.
const ATTEMPTS: usize = 8;
/// How long a store must stay quiet before a settled crash, and the longest
/// a campaign waits for that.
const SETTLE_QUIET: Duration = Duration::from_millis(100);
const SETTLE_LIMIT: Duration = Duration::from_secs(5);
/// The local lease's mutation lock. #811: a completed release's unlink is not
/// made durable, so a power loss can bring the lock back and block every
/// later open.
pub(crate) const LEASE_LOCK: &str = ".midge_leader.lock";

/// What a campaign found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FindingKind {
    /// Recovered data no allowed history explains.
    Data(Violation),
    /// This WAL segment name was durably given to a file a second time.
    SegmentReuse(String),
    /// The engine refused to open after a crash it must survive, or to read
    /// what it recovered.
    Refused(String),
}

impl FindingKind {
    pub(crate) fn same_class(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// A recovery point: everything a replay needs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Image {
    #[serde(skip)]
    pub(crate) files: BTreeMap<String, Vec<u8>>,
    pub(crate) history: Histories,
    families: Vec<String>,
    names: BTreeMap<String, u64>,
    /// Values written so far, so replayed writes stay distinct.
    values: u64,
    /// The operations after this point, after the recovery itself.
    pub(crate) remaining: Vec<Op>,
    /// How the engine stopped before this point, so a replay recovers and
    /// judges the same way.
    pub(crate) stop: Stop,
    /// The lease clock's reading, so a replay never sees a lease record
    /// stamped in its future.
    lease_clock_millis: u64,
    /// The object store's contents and request counts.
    store: StoreImage,
    /// The workload's seed, which also draws the store's fates.
    seed: u64,
    /// How the campaign ran, so a replay runs and judges the same way.
    config: Config,
}

#[derive(Debug, Clone)]
pub(crate) struct Finding {
    pub(crate) kind: FindingKind,
    pub(crate) seed: u64,
    pub(crate) workload: Vec<Op>,
    pub(crate) image: Image,
}

/// How the engine stopped before a recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Stop {
    Clean,
    ProcessCrash,
    PowerLoss,
    /// A process crash, then a takeover by a process with an empty cache.
    FreshCache,
    /// A clean shutdown, then a power loss that keeps nothing unsynced.
    PowerLossAfterShutdown,
}

impl Stop {
    /// Whether the stopping process died rather than shut down.
    fn crashes_process(self) -> bool {
        !matches!(self, Self::Clean | Self::PowerLossAfterShutdown)
    }

    /// Whether the device lost power, dropping what was not durable.
    fn loses_power(self) -> bool {
        matches!(self, Self::PowerLoss | Self::PowerLossAfterShutdown)
    }

    /// What the next process recovers from.
    pub(crate) fn recovery(self) -> Recovery {
        if self == Self::FreshCache {
            Recovery::FreshCache
        } else {
            Recovery::SurvivingCache
        }
    }
}

pub(crate) struct Campaign {
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
    /// Resurrected lease locks removed after power losses (#811).
    stale_lease_locks: usize,
    config: Config,
}

impl Campaign {
    pub(crate) fn new(config: Config, seed: u64) -> Self {
        Self::from_image(&Image {
            files: BTreeMap::new(),
            history: Histories::new(BTreeMap::new(), BTreeSet::from([DEFAULT.to_string()])),
            families: vec![DEFAULT.to_string()],
            names: BTreeMap::new(),
            values: 0,
            remaining: Vec::new(),
            stop: Stop::Clean,
            lease_clock_millis: 0,
            store: StoreImage::default(),
            seed,
            config,
        })
    }

    fn from_image(image: &Image) -> Self {
        image.config.validate();
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
            stale_lease_locks: 0,
            config: image.config,
        }
    }

    fn options(&self) -> OpenOptions {
        let root: Arc<dyn Fs> = Arc::new(self.device.fs());
        self.config
            .mode
            .open_builder(self.named.path(), &self.view)
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

    /// Record a recovery point for replay.
    fn snapshot(&mut self, remaining: &[Op], stop: Stop) {
        let image = Image {
            files: self.device.files(),
            history: self.history.clone(),
            families: self.families.clone(),
            names: self.names(),
            values: self.values,
            remaining: remaining.to_vec(),
            stop,
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
    pub(crate) fn recover(&mut self, stop: Stop) -> Result<(), FindingKind> {
        self.device.disarm_faults();
        if stop.crashes_process() {
            // The restarted process's clock runs past the dead one's lease,
            // beyond the TTL and the default skew allowance of half the TTL,
            // so it takes the lease over at once (#835).
            self.clock.advance(self.config.lease_ttl() * 2);
            self.view = self.store.process();
        }
        let mut opened = self.open(stop);
        let mut attempts = 1;
        let mut refused_by_store = false;
        while let Err(error) = &opened {
            // A refusal the store's faults caused is retried by a new
            // process; one it left holding the lease is taken over.
            let message = error.to_string();
            refused_by_store |= message.contains(FAILURE_MESSAGE);
            let retry = message.contains(FAILURE_MESSAGE)
                || (refused_by_store && matches!(error, cntryl_midge::MidgeError::LeaseHeld(_)));
            if !self.config.faulting() || !retry || attempts == ATTEMPTS {
                break;
            }
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

    /// Wait until the store's trace has stopped growing for
    /// [`SETTLE_QUIET`], or [`SETTLE_LIMIT`] has passed.
    fn settle(&self) {
        let deadline = std::time::Instant::now() + SETTLE_LIMIT;
        let mut seen = self.store.trace().len();
        while std::time::Instant::now() < deadline {
            std::thread::sleep(SETTLE_QUIET);
            let now = self.store.trace().len();
            if now == seen {
                return;
            }
            seen = now;
        }
    }

    /// Open the engine once, clearing a lease lock #811 resurrected.
    fn open(&mut self, stop: Stop) -> Result<Engine, cntryl_midge::MidgeError> {
        match Engine::open(self.options()) {
            Err(error)
                if stop.loses_power()
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
                let value = tx
                    .get(&[key])
                    .map_err(|error| FindingKind::Refused(format!("read: {error}")))?;
                if let Some(value) = value {
                    observed.insert((handle.name().to_string(), vec![key]), value.to_vec());
                }
            }
        }
        self.history
            .check(stop.recovery(), observed, families)
            .map_err(FindingKind::Data)
    }

    fn family(&self, family: usize) -> String {
        self.families[family.min(self.families.len() - 1)].clone()
    }

    pub(crate) fn write(&mut self, family: usize, key: u8, delete: bool, durability: Durability) {
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
                tx.commit(self.config.mode.write_options(durability))
            });
        let id = self.history.commit(Commit {
            family,
            writes: vec![(vec![key], value)],
            logging: durability.logging(),
            acknowledged: committed.is_ok(),
        });
        if committed.is_ok() && durability == Durability::Sync {
            self.history.synced(id);
        }
    }

    /// Flush `family`; the history counts it flushed only if the flush
    /// succeeded.
    pub(crate) fn flush(&mut self, family: usize) -> cntryl_midge::MidgeResult<()> {
        let name = self.family(family);
        let engine = self.engine();
        let handle = engine.get_column_family(&name).expect("family exists");
        engine.flush_cf(&handle)?;
        self.history.flushed(&name);
        Ok(())
    }

    /// Fail a later device call; see [`Fault`].
    pub(crate) fn inject(&self, fault: Fault) {
        let _ = self.device.inject(fault);
    }

    /// The process dies: it reaches neither its device nor the store again.
    pub(crate) fn crash_process(&mut self) {
        self.device.process_crash();
        self.view.process_crash();
        drop(self.engine.take());
    }

    /// The process dies and the device loses power, keeping the
    /// alternatives `choose` picks.
    pub(crate) fn lose_power_with(&mut self, choose: impl FnMut(&Decision) -> usize) {
        self.crash_process();
        self.device.power_loss_with(choose);
    }

    /// Whether the last recovery served `key`.
    pub(crate) fn served(&self, key: &Key) -> bool {
        self.history.local_keys().contains(key)
    }

    /// Stop the engine by `stop`, record the recovery point, and recover.
    fn restart(
        &mut self,
        stop: Stop,
        remaining: &[Op],
        power_seed: u64,
    ) -> Result<(), FindingKind> {
        assert!(
            stop.recovery() != Recovery::FreshCache || self.config.mode.has_fresh_cache_takeovers(),
            "{:?} has no fresh-cache takeovers",
            self.config.mode
        );
        if stop.crashes_process() {
            self.crash_process();
            if stop.loses_power() {
                self.device.power_loss(power_seed);
            }
            if stop.recovery() == Recovery::FreshCache {
                // A new host's disk has given no names yet.
                self.names_before = BTreeMap::new();
                self.device = CrashDevice::new();
                self.restored_names = BTreeMap::new();
            }
        } else {
            let mut engine = self.engine.take().expect("engine is open");
            if engine.shutdown(Duration::from_secs(10)).is_ok() {
                self.history.shut_down();
            }
            drop(engine);
            if stop.loses_power() {
                self.device.process_crash();
                self.device.power_loss_with(|_| 0);
            }
        }
        self.snapshot(remaining, stop);
        self.recover(stop)
    }

    pub(crate) fn apply(&mut self, op: &Op, remaining: &[Op]) -> Result<(), FindingKind> {
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
                let _ = self.flush(*family);
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
            Op::PowerLossAfterShutdown => {
                self.restart(Stop::PowerLossAfterShutdown, remaining, 0)?;
            }
            Op::LandLate => {
                let _ = self.store.land_late_effects();
            }
            Op::Settle => self.settle(),
            Op::FailWalRemoval { nth } => self.inject(Fault {
                op: FaultOp::RemoveFile,
                path: "wal".into(),
                nth: *nth,
                effect: FaultEffect::Before,
                error: FaultError::Io,
            }),
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
            shutdown.is_ok() || self.config.faulting(),
            "final shutdown: {shutdown:?}"
        );
        Ok(())
    }

    /// Run `workload` on a fresh device; return the first finding.
    pub(crate) fn run(seed: u64, workload: &[Op], config: Config) -> Result<(), Box<Finding>> {
        let mut campaign = Self::new(config, seed);
        campaign.snapshot(workload, Stop::Clean);
        campaign.drive(workload, Stop::Clean).map_err(|kind| {
            Box::new(Finding {
                image: campaign.image_for(&kind),
                kind,
                seed,
                workload: workload.to_vec(),
            })
        })
    }

    /// Replay an image from its recovery point; return what it finds.
    pub(crate) fn replay(image: &Image) -> Result<(), FindingKind> {
        let mut campaign = Self::from_image(image);
        campaign.drive(&image.remaining, image.stop)
    }
}
