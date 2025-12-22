use crate::bench_common::BenchEngineConfig;
use cntryl_midge::MidgeEngine;
use std::path::PathBuf;
use std::time::Duration;

/// Tier-3 single-shot case backed by a seed directory.
/// Ownership + `FnOnce` prevents reuse and looping of the timed body.
#[derive(Clone, Debug)]
pub struct Tier3Case {
    seed_path: PathBuf,
    config: BenchEngineConfig,
}

impl Tier3Case {
    pub fn from_seed(seed_path: PathBuf, config: BenchEngineConfig) -> Self {
        Self { seed_path, config }
    }

    pub fn run<F>(self, f: F) -> Duration
    where
        F: FnOnce(MidgeEngine),
    {
        crate::bench_common::run_single_shot_from_seed(&self.seed_path, &self.config, f)
    }
}

#[derive(Clone, Debug)]
pub struct Tier3RestoreCase {
    seed_path: PathBuf,
    config: BenchEngineConfig,
}

impl Tier3RestoreCase {
    pub fn new(seed_path: PathBuf, config: BenchEngineConfig) -> Self {
        Self { seed_path, config }
    }

    pub fn run<R, T>(self, restore: R, timed: T) -> Duration
    where
        R: FnOnce(&MidgeEngine),
        T: FnOnce(&MidgeEngine),
    {
        crate::bench_common::run_single_shot_with_restore(
            &self.seed_path,
            &self.config,
            restore,
            timed,
        )
    }
}

/// Macro to enforce the Tier-3 contract and prevent calling `b.iter` directly.
/// Usage: `tier3_bench!(b, case, |engine| { ... })`
#[macro_export]
macro_rules! tier3_bench {
    ($b:expr, $case:expr, $body:expr) => {
        $b.iter_custom(|_| {
            let case = $case.clone();
            case.run($body)
        })
    };
}

/// Variant that performs a restore step (pre-timed) then a single timed op.
#[macro_export]
macro_rules! tier3_bench_restore {
    ($b:expr, $case:expr, $restore:expr, $timed:expr) => {
        $b.iter_custom(|_| {
            let case = $case.clone();
            case.run($restore, $timed)
        })
    };
}
