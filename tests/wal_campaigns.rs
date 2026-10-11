//! Local engine campaigns (#831).
//!
//! Seeded workloads drive a local engine on a simulated device through
//! clean restarts, process crashes, power losses and injected faults. After
//! every reopen the oracle judges the recovered data against the history of
//! commits, and a WAL invariant checks that no segment name is ever given
//! twice. A finding carries a crash image: the device files of the last
//! recovery point before it, with the history, the names given so far and
//! the remaining workload, so replaying it needs nothing else. Images are
//! written below `MIDGE_CAMPAIGN_IMAGES` when that variable names a
//! directory.

use cntryl_midge::__internal::io::{Fs, FsPath};
use cntryl_midge::__internal::simulation::{
    Commit, CrashDevice, Fault, FaultEffect, FaultError, FaultOp, History, Logging, Verdict,
};
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

/// Short, so a restart after a simulated crash takes the dead process's
/// lease over quickly.
const LEASE_TTL: Duration = Duration::from_millis(40);
/// The local lease's mutation lock. #811: a completed release's unlink is not
/// made durable, so a power loss can bring the lock back and block every
/// later open.
const LEASE_LOCK: &str = ".midge_leader.lock";
const KEYS: u8 = 6;
const DEFAULT: &str = "default";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Durability {
    Sync,
    Buffered,
    BestEffort,
}

impl Durability {
    fn options(self) -> WriteOptions {
        match self {
            Self::Sync => WriteOptions::sync(),
            Self::Buffered => WriteOptions::buffered(),
            Self::BestEffort => WriteOptions::best_effort(),
        }
    }

    fn logging(self) -> Logging {
        match self {
            Self::BestEffort => Logging::Unlogged,
            Self::Sync | Self::Buffered => Logging::Wal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Synced WAL writes through crashes and power losses only: no trigger
    /// of a known #808 bug, so any finding is new.
    Control,
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
}

impl Profile {
    fn op(self, rng: &mut Rng, families: usize) -> Op {
        let family = usize::try_from(rng.below(families as u64)).expect("family fits usize");
        let key = rng.key();
        let durable = rng.pick(&[Durability::Sync, Durability::Buffered]);
        match self {
            Self::Control => match rng.below(10) {
                0..=4 => Op::Put {
                    family: 0,
                    key,
                    durability: Durability::Sync,
                },
                5..=6 => Op::Delete {
                    family: 0,
                    key,
                    durability: Durability::Sync,
                },
                7 => Op::ProcessCrash,
                _ => Op::PowerLoss { seed: rng.next() },
            },
            Self::FamilyCrashes => match rng.below(10) {
                0..=1 => Op::CreateFamily,
                2..=5 => Op::Put {
                    family,
                    key,
                    durability: Durability::Sync,
                },
                6..=7 => Op::ProcessCrash,
                _ => Op::PowerLoss { seed: rng.next() },
            },
            Self::BufferedCrashes => match rng.below(8) {
                0..=4 => Op::Put {
                    family: 0,
                    key,
                    durability: Durability::Buffered,
                },
                5 => Op::ProcessCrash,
                _ => Op::PowerLoss { seed: rng.next() },
            },
            Self::BestEffortDelete => match rng.below(10) {
                0..=3 => Op::Put {
                    family: 0,
                    key,
                    durability: Durability::Sync,
                },
                4..=5 => Op::Delete {
                    family: 0,
                    key,
                    durability: rng.pick(&[Durability::Sync, Durability::BestEffort]),
                },
                6..=7 => Op::Flush { family: 0 },
                _ => Op::CleanRestart,
            },
            Self::FailedUnlink => match rng.below(15) {
                0..=4 => Op::Put {
                    family,
                    key,
                    durability: Durability::Sync,
                },
                5..=6 => Op::Delete {
                    family,
                    key,
                    durability: Durability::Sync,
                },
                7..=8 => Op::Flush { family },
                9 => Op::Compact,
                10 => Op::CreateFamily,
                11..=12 => Op::FailWalRemoval {
                    nth: 1 + rng.below(2),
                },
                _ => Op::CleanRestart,
            },
            Self::SegmentRetirement => match rng.below(8) {
                0..=3 => Op::Put {
                    family: 0,
                    key,
                    durability: durable,
                },
                4..=5 => Op::Flush { family: 0 },
                _ => Op::CleanRestart,
            },
        }
    }

    fn workload(self, seed: u64, len: usize) -> Vec<Op> {
        let mut rng = Rng(seed);
        let mut families = 1;
        (0..len)
            .map(|_| {
                let op = self.op(&mut rng, families);
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
    Data(String),
    /// This WAL segment name was given to a file a second time.
    SegmentReuse(String),
    /// The engine refused to open after a crash it must survive.
    OpenRefused(String),
}

impl FindingKind {
    fn same_class(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }

    fn detail(&self) -> &str {
        match self {
            Self::Data(detail) | Self::SegmentReuse(detail) | Self::OpenRefused(detail) => detail,
        }
    }
}

/// A recovery point: everything a replay needs.
#[derive(Debug, Clone)]
struct Image {
    files: BTreeMap<String, Vec<u8>>,
    history: History,
    families: Vec<String>,
    names: BTreeMap<String, u64>,
    /// The operations after this point, starting with the recovery itself.
    remaining: Vec<Recovery>,
}

/// A replay step: the recovery the image was taken for, then the workload.
#[derive(Debug, Clone)]
enum Recovery {
    Reopen,
    Run(Op),
}

#[derive(Debug, Clone)]
struct Finding {
    kind: FindingKind,
    seed: u64,
    workload: Vec<Op>,
    image: Image,
}

/// Which known #808 bug classes a campaign reports.
#[derive(Debug, Clone, Copy)]
struct Checks {
    /// Report a reused WAL segment name (`restarted-segment-id`).
    segment_reuse: bool,
    /// Remove a stale lease lock as an operator would (#811) instead of
    /// reporting the refused open.
    clear_stale_lease_lock: bool,
}

impl Default for Checks {
    fn default() -> Self {
        Self {
            segment_reuse: true,
            clear_stale_lease_lock: true,
        }
    }
}

struct Campaign {
    device: CrashDevice,
    named: tempfile::TempDir,
    engine: Option<Engine>,
    history: History,
    families: Vec<String>,
    /// Names given before this device, when replaying an image.
    names_before: BTreeMap<String, u64>,
    /// Names the device gave while restoring an image's files.
    restored_names: BTreeMap<String, u64>,
    values: u64,
    images: Vec<Image>,
    /// Report WAL segment names given twice (`restarted-segment-id`).
    check_segment_reuse: bool,
    /// Remove a stale lease lock as an operator would (#811) instead of
    /// reporting the refused open.
    clear_stale_lease_lock: bool,
}

impl Campaign {
    fn new() -> Self {
        Self::from_image(&Image {
            files: BTreeMap::new(),
            history: History::new(BTreeMap::new(), BTreeSet::from([DEFAULT.to_string()])),
            families: vec![DEFAULT.to_string()],
            names: BTreeMap::new(),
            remaining: Vec::new(),
        })
    }

    fn from_image(image: &Image) -> Self {
        let device = CrashDevice::with_files(&image.files);
        let restored_names = device.names_ever();
        Self {
            device,
            named: tempfile::tempdir().expect("named directory"),
            engine: None,
            history: image.history.clone(),
            families: image.families.clone(),
            names_before: image.names.clone(),
            restored_names,
            values: 0,
            images: Vec::new(),
            check_segment_reuse: true,
            clear_stale_lease_lock: true,
        }
    }

    fn options(&self) -> OpenOptions {
        let root: Arc<dyn Fs> = Arc::new(self.device.fs());
        OpenOptions::local(self.named.path())
            .background_compaction(false)
            .lease_ttl(LEASE_TTL)
            .lease_clock_skew_tolerance(Duration::ZERO)
            .filesystem_for_testing(root)
            .build()
            .expect("open options")
    }

    fn engine(&self) -> &Engine {
        self.engine.as_ref().expect("engine is open")
    }

    /// Names given so far, over every device this history used.
    fn names(&self) -> BTreeMap<String, u64> {
        let mut names = self.names_before.clone();
        for (name, count) in self.device.names_ever() {
            let restored = self.restored_names.get(&name).copied().unwrap_or(0);
            *names.entry(name).or_default() += count - restored;
        }
        names
    }

    fn image(&self, remaining: Vec<Recovery>) -> Image {
        Image {
            files: self.device.files(),
            history: self.history.clone(),
            families: self.families.clone(),
            names: self.names(),
            remaining,
        }
    }

    fn reused_segment(&self) -> Option<String> {
        if !self.check_segment_reuse {
            return None;
        }
        self.names().into_iter().find_map(|(name, count)| {
            let id = name.strip_prefix("wal/")?.strip_suffix(".wal")?;
            (count > 1 && id.bytes().all(|byte| byte.is_ascii_digit())).then_some(name)
        })
    }

    /// Reopen after a restart or crash, then judge what recovery kept.
    fn recover(&mut self, crashed: bool) -> Result<(), FindingKind> {
        self.device.disarm_faults();
        if crashed {
            // A restarted process takes the dead one's lease over once it
            // is stale.
            std::thread::sleep(LEASE_TTL * 3);
        }
        let engine = match Engine::open(self.options()) {
            Err(error) if self.clear_stale_lease_lock && error.to_string().contains(LEASE_LOCK) => {
                self.device
                    .fs()
                    .remove_file(&FsPath::new(LEASE_LOCK))
                    .expect("remove the stale lease lock");
                Engine::open(self.options())
            }
            opened => opened,
        }
        .map_err(|error| FindingKind::OpenRefused(error.to_string()))?;
        self.engine = Some(engine);
        self.verify()
    }

    fn verify(&mut self) -> Result<(), FindingKind> {
        let engine = self.engine();
        let families: BTreeSet<String> = engine
            .list_column_families()
            .expect("list families")
            .iter()
            .map(|family| family.name().to_string())
            .collect();
        let mut observed = BTreeMap::new();
        for (family, key) in self.history.keys() {
            let Some(handle) = engine.get_column_family(&family) else {
                continue;
            };
            let tx = engine
                .begin_tx(handle.id(), TransactionMode::ReadOnly)
                .expect("read");
            if let Some(value) = tx.get(&key).expect("get") {
                observed.insert((family, key), value.to_vec());
            }
        }
        match self.history.check(&observed, &families) {
            Verdict::Consistent { .. } => {
                self.history = History::new(observed, families);
                Ok(())
            }
            Verdict::Violation(violation) => Err(FindingKind::Data(format!("{violation:?}"))),
        }
    }

    fn write(&mut self, family: usize, key: u8, delete: bool, durability: Durability) {
        let family = self.families[family.min(self.families.len() - 1)].clone();
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
                tx.commit(durability.options())
            });
        let index = self.history.commit(Commit {
            family,
            writes: vec![(vec![key], value)],
            logging: durability.logging(),
            acknowledged: committed.is_ok(),
        });
        if committed.is_ok() && durability == Durability::Sync {
            self.history.synced(index);
        }
    }

    /// The process dies; reopen from what the device kept.
    fn crash(
        &mut self,
        step: usize,
        workload: &[Op],
        power_loss: Option<u64>,
    ) -> Result<(), FindingKind> {
        self.device.process_crash();
        if let Some(seed) = power_loss {
            self.device.power_loss(seed);
        }
        // The fenced process can no longer reach the device.
        drop(self.engine.take());
        self.snapshot(step, workload);
        self.recover(true)
    }

    fn snapshot(&mut self, step: usize, workload: &[Op]) {
        let remaining = std::iter::once(Recovery::Reopen)
            .chain(workload[step + 1..].iter().cloned().map(Recovery::Run))
            .collect();
        let image = self.image(remaining);
        self.images.push(image);
    }

    /// The recovery point a replay of `kind` starts from: the latest one,
    /// or for a reused segment name the latest before the reuse.
    fn image_for(&self, kind: &FindingKind) -> Image {
        let latest = |image: &&Image| match kind {
            FindingKind::SegmentReuse(name) => image.names.get(name).copied().unwrap_or(0) <= 1,
            FindingKind::Data(_) | FindingKind::OpenRefused(_) => true,
        };
        self.images
            .iter()
            .rev()
            .find(latest)
            .cloned()
            .unwrap_or_else(|| self.image(Vec::new()))
    }

    fn apply(&mut self, step: usize, op: &Op, workload: &[Op]) -> Result<(), FindingKind> {
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
                let name = self.families[(*family).min(self.families.len() - 1)].clone();
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
            Op::CleanRestart => {
                let mut engine = self.engine.take().expect("engine is open");
                if engine.shutdown(Duration::from_secs(10)).is_ok() {
                    self.history.shut_down();
                }
                drop(engine);
                self.snapshot(step, workload);
                self.recover(false)?;
            }
            Op::ProcessCrash => self.crash(step, workload, None)?,
            Op::PowerLoss { seed } => self.crash(step, workload, Some(*seed))?,
            Op::FailWalRemoval { nth } => {
                let _ = self.device.inject(Fault {
                    op: FaultOp::RemoveFile,
                    path: "wal".into(),
                    nth: *nth,
                    effect: FaultEffect::Before,
                    error: FaultError::Io,
                });
            }
        }
        self.reused_segment()
            .map_or(Ok(()), |reuse| Err(FindingKind::SegmentReuse(reuse)))
    }

    /// Run `workload`, ending with a clean restart; return the first finding.
    fn run_with(seed: u64, workload: &[Op], checks: Checks) -> Result<(), Box<Finding>> {
        let mut campaign = Self::new();
        campaign.clear_stale_lease_lock = checks.clear_stale_lease_lock;
        campaign.check_segment_reuse = checks.segment_reuse;
        let finding = |campaign: &Campaign, kind: FindingKind| {
            Box::new(Finding {
                image: campaign.image_for(&kind),
                kind,
                seed,
                workload: workload.to_vec(),
            })
        };
        if let Err(kind) = campaign.recover(false) {
            return Err(finding(&campaign, kind));
        }
        let mut full = workload.to_vec();
        full.push(Op::CleanRestart);
        for (step, op) in full.iter().enumerate() {
            if let Err(kind) = campaign.apply(step, op, &full) {
                return Err(finding(&campaign, kind));
            }
        }
        let mut engine = campaign.engine.take().expect("engine is open");
        engine
            .shutdown(Duration::from_secs(10))
            .expect("final shutdown");
        Ok(())
    }

    /// Replay an image from its recovery point; return what it finds.
    fn replay(image: &Image, checks: Checks) -> Result<(), FindingKind> {
        let mut campaign = Self::from_image(image);
        campaign.clear_stale_lease_lock = checks.clear_stale_lease_lock;
        campaign.check_segment_reuse = checks.segment_reuse;
        let workload: Vec<Op> = image
            .remaining
            .iter()
            .filter_map(|step| match step {
                Recovery::Run(op) => Some(op.clone()),
                Recovery::Reopen => None,
            })
            .collect();
        campaign.recover(true)?;
        if let Some(reuse) = campaign.reused_segment() {
            return Err(FindingKind::SegmentReuse(reuse));
        }
        for (step, op) in workload.iter().enumerate() {
            campaign.apply(step, op, &workload)?;
        }
        if let Some(mut engine) = campaign.engine.take() {
            let _ = engine.shutdown(Duration::from_secs(10));
        }
        Ok(())
    }
}

/// Run `profile` over `seeds`; return the first finding, shrunk.
fn campaign(
    profile: Profile,
    seeds: std::ops::Range<u64>,
    len: usize,
    checks: Checks,
) -> Option<Finding> {
    let found = seeds
        .map(|seed| (seed, profile.workload(seed, len)))
        .find_map(|(seed, workload)| Campaign::run_with(seed, &workload, checks).err())?;
    let found = *found;
    Some(shrink(found, checks))
}

/// Drop operations one at a time while the same class of finding remains.
fn shrink(mut finding: Finding, checks: Checks) -> Finding {
    let mut index = 0;
    let mut attempts = 0;
    while index < finding.workload.len() && attempts < 80 {
        attempts += 1;
        let mut candidate = finding.workload.clone();
        candidate.remove(index);
        match Campaign::run_with(finding.seed, &candidate, checks) {
            Err(smaller) if smaller.kind.same_class(&finding.kind) => finding = *smaller,
            _ => index += 1,
        }
    }
    finding
}

/// Write `finding`'s image below `MIDGE_CAMPAIGN_IMAGES`, if set.
fn save(case: &str, finding: &Finding) {
    let Some(root) = std::env::var_os("MIDGE_CAMPAIGN_IMAGES") else {
        return;
    };
    let directory = std::path::Path::new(&root).join(format!("{case}-seed-{}", finding.seed));
    for (path, bytes) in &finding.image.files {
        let target = directory.join("files").join(path);
        std::fs::create_dir_all(target.parent().expect("image file parent"))
            .expect("image directory");
        std::fs::write(target, bytes).expect("write image file");
    }
    std::fs::write(
        directory.join("history.json"),
        serde_json::to_vec_pretty(&finding.image.history).expect("encode history"),
    )
    .expect("write history");
    std::fs::write(
        directory.join("finding.txt"),
        format!(
            "{}\n\nworkload: {:#?}\n\nremaining after image: {:#?}\n\nnames: {:#?}\n",
            finding.kind.detail(),
            finding.workload,
            finding.image.remaining,
            finding.image.names
        ),
    )
    .expect("write finding");
}

/// Assert `finding` is of `class`, save it, and prove its image replays it.
fn assert_reproduced(case: &str, finding: Option<Finding>, class: &FindingKind, checks: Checks) {
    let finding = finding.unwrap_or_else(|| panic!("{case}: the campaign found nothing"));
    assert!(
        finding.kind.same_class(class),
        "{case}: found {:?} instead",
        finding.kind
    );
    save(case, &finding);
    let replayed = Campaign::replay(&finding.image, checks);
    assert!(
        replayed
            .as_ref()
            .is_err_and(|kind| kind.same_class(&finding.kind)),
        "{case}: the image did not reproduce {:?}: {replayed:?}",
        finding.kind
    );
}

#[test]
fn should_find_no_violation_when_control_workload_survives_crashes() {
    // Arrange
    let workloads = 0..8;

    // Act
    let finding = campaign(Profile::Control, workloads, 24, Checks::default());

    // Assert
    assert!(finding.is_none(), "{finding:#?}");
}

#[test]
fn should_keep_created_families_when_process_crash_precedes_power_loss() {
    // Arrange: the review's lead is that a manifest journal created but not
    // dir-synced before a process crash loses its name at the next power
    // loss.
    let workloads = 0..8;

    // Act
    let finding = campaign(Profile::FamilyCrashes, workloads, 24, Checks::default());

    // Assert
    assert!(finding.is_none(), "{finding:#?}");
}

#[test]
fn should_reproduce_best_effort_delete_when_campaigning() {
    // Arrange
    let checks = Checks {
        segment_reuse: false,
        ..Checks::default()
    };

    // Act
    let finding = campaign(Profile::BestEffortDelete, 0..20, 16, checks);

    // Assert
    assert_reproduced(
        "best-effort-delete",
        finding,
        &FindingKind::Data(String::new()),
        checks,
    );
}

#[test]
fn should_reproduce_failed_local_unlink_when_campaigning() {
    // Arrange
    let checks = Checks {
        segment_reuse: false,
        ..Checks::default()
    };

    // Act
    let finding = campaign(Profile::FailedUnlink, 0..40, 30, checks);

    // Assert
    assert_reproduced(
        "failed-local-unlink",
        finding,
        &FindingKind::Data(String::new()),
        checks,
    );
}

#[test]
fn should_reproduce_restarted_segment_id_when_campaigning() {
    // Arrange
    let workloads = 0..20;

    // Act
    let finding = campaign(Profile::SegmentRetirement, workloads, 16, Checks::default());

    // Assert
    assert_reproduced(
        "restarted-segment-id",
        finding,
        &FindingKind::SegmentReuse(String::new()),
        Checks::default(),
    );
}

#[test]
fn should_reproduce_recovered_tail_loss_when_campaigning() {
    // Arrange
    let workloads = 0..10;

    // Act
    let finding = campaign(Profile::BufferedCrashes, workloads, 12, Checks::default());

    // Assert
    assert_reproduced(
        "recovered-tail-not-durable",
        finding,
        &FindingKind::Data(String::new()),
        Checks::default(),
    );
}

#[test]
fn should_reproduce_stale_lease_lock_when_power_is_lost() {
    // Arrange: #811, found by power losses; the operator's workaround is off.
    let checks = Checks {
        clear_stale_lease_lock: false,
        ..Checks::default()
    };

    // Act
    let found = (0..8)
        .map(|seed| (seed, Profile::Control.workload(seed, 24)))
        .find_map(|(seed, workload)| Campaign::run_with(seed, &workload, checks).err())
        .map(|finding| *finding);

    // Assert
    assert_reproduced(
        "stale-lease-lock",
        found.filter(|finding| finding.kind.detail().contains(LEASE_LOCK)),
        &FindingKind::OpenRefused(String::new()),
        checks,
    );
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
    let mut campaign = Campaign::new();
    campaign.recover(false).expect("first open");
    campaign.write(0, 1, false, Durability::Buffered);
    campaign.device.process_crash();
    drop(campaign.engine.take());
    campaign
        .recover(true)
        .expect("recover from the process crash");
    let served = campaign
        .history
        .keys()
        .contains(&(DEFAULT.to_string(), vec![1]));

    // Act: power is lost with nothing unsynced reaching the device.
    campaign.device.power_loss_with(|_| 0);
    drop(campaign.engine.take());
    let recovered = campaign.recover(true);

    // Assert
    assert!(served, "recovery did not serve the buffered tail");
    assert!(
        recovered.is_ok(),
        "recovered WAL tail was served, then lost after power loss: {recovered:?}"
    );
}
