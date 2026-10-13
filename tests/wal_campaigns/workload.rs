//! What campaigns do: seeded workloads of operations, by profile.

use crate::config::Mode;
use cntryl_midge::__internal::simulation::Logging;
use serde::{Deserialize, Serialize};

/// Workloads write keys `0..KEYS` in every family; recovery reads them all.
pub(crate) const KEYS: u8 = 6;
pub(crate) const DEFAULT: &str = "default";
/// How many times more seeds the nightly profile explores per control.
pub(crate) const NIGHTLY_SCALE: u64 = 100;
/// The seeds a detection campaign searches. Whether a seed reaches a bug can
/// depend on background timing, so the search is wide; it stops at the first
/// finding, which usually comes within the first few seeds.
pub(crate) const DETECTION_SEEDS: std::ops::Range<u64> = 0..200;

/// The seeds a control campaign explores: `per_pr` of them, or
/// [`NIGHTLY_SCALE`] times as many from the nightly seed base.
pub(crate) fn control_seeds(per_pr: u64) -> std::ops::Range<u64> {
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

/// What a commit is promised when the engine acknowledges it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Durability {
    /// Durable when acknowledged: `sync` locally, `cloud_strict` in cloud
    /// modes.
    Sync,
    /// Logged but not yet durable: `buffered` locally, `cloud_async` in cloud
    /// modes.
    Buffered,
    BestEffort,
}

impl Durability {
    pub(crate) fn logging(self) -> Logging {
        match self {
            Self::BestEffort => Logging::Unlogged,
            Self::Sync | Self::Buffered => Logging::Wal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Op {
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
    /// A clean shutdown, then a power loss that keeps nothing unsynced: the
    /// shutdown's completed lease release unlinked its lock last (#811).
    PowerLossAfterShutdown,
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
    /// Wait until the store has been quiet for a while: background uploads
    /// and publications have finished or given up.
    Settle,
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
pub(crate) enum Profile {
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
    /// Mostly asynchronous cloud writes, flushes, waits for the store to
    /// settle, fresh-cache takeovers and late requests landing, against a
    /// faulting store.
    CloudFaults,
}

impl Profile {
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
            Self::CloudControl => cloud_control_op(rng, key, logged, mode),
            Self::CloudFaults => cloud_fault_op(rng, key, mode),
        }
    }

    pub(crate) fn workload(self, seed: u64, len: usize, mode: Mode) -> Vec<Op> {
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

fn cloud_control_op(rng: &mut Rng, key: u8, logged: Durability, mode: Mode) -> Op {
    match rng.below(16) {
        0..=5 => Op::Put {
            family: 0,
            key,
            durability: logged,
        },
        6..=7 => Op::Delete {
            family: 0,
            key,
            durability: Durability::Sync,
        },
        8..=9 => Op::Flush { family: 0 },
        10 => Op::Compact,
        11 => Op::CleanRestart,
        12..=13 => Op::PowerLoss { seed: rng.next() },
        _ if mode.has_fresh_cache_takeovers() => Op::FreshCache,
        _ => Op::CleanRestart,
    }
}

fn cloud_fault_op(rng: &mut Rng, key: u8, mode: Mode) -> Op {
    match rng.below(16) {
        0..=6 => Op::Put {
            family: 0,
            key,
            durability: Durability::Buffered,
        },
        7 => Op::Put {
            family: 0,
            key,
            durability: Durability::Sync,
        },
        8..=9 => Op::Flush { family: 0 },
        10..=11 => Op::Settle,
        12..=13 if mode.has_fresh_cache_takeovers() => Op::FreshCache,
        14 => Op::LandLate,
        _ => Op::CleanRestart,
    }
}
