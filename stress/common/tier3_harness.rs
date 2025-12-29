//! Tier-3 Criterion harness helpers.
//!
//! These helpers wrap the single-shot seed/restore utilities from
//! `cntryl_midge::testkit::bench` so Tier-3 benchmarks can:
//! - build expensive datasets once (seed)
//! - clone a seed per-sample (restore)
//! - time only the critical section (or include open)

use std::path::PathBuf;

use cntryl_midge::testkit::bench::{
    reopen_engine_at_path, run_single_shot_from_seed, run_single_shot_open_from_seed,
    run_single_shot_with_restore, BenchEngineConfig,
};

#[derive(Clone)]
pub struct Tier3Case {
    seed_path: PathBuf,
    config: BenchEngineConfig,
}

impl Tier3Case {
    pub fn from_seed(seed_path: PathBuf, config: BenchEngineConfig) -> Self {
        Self { seed_path, config }
    }

    pub fn run<F>(&self, f: F) -> std::time::Duration
    where
        F: FnOnce(cntryl_midge::MidgeEngine),
    {
        run_single_shot_from_seed(&self.seed_path, &self.config, f)
    }
}

#[derive(Clone)]
pub struct Tier3OpenCase {
    seed_path: PathBuf,
    config: BenchEngineConfig,
}

impl Tier3OpenCase {
    pub fn from_seed(seed_path: PathBuf, config: BenchEngineConfig) -> Self {
        Self { seed_path, config }
    }

    pub fn run_open<F>(&self, f: F) -> std::time::Duration
    where
        F: FnOnce(cntryl_midge::MidgeEngine),
    {
        run_single_shot_open_from_seed(&self.seed_path, &self.config, |path, cfg| {
            let engine = reopen_engine_at_path(path, cfg);
            f(engine)
        })
    }
}

#[macro_export]
macro_rules! tier3_bench {
    ($b:expr, $case:expr, $body:expr) => {{
        $b.iter_custom(|iters| {
            let mut total = std::time::Duration::from_nanos(0);
            for _ in 0..iters {
                total += $case.run($body);
            }
            total
        });
    }};
}

#[macro_export]
macro_rules! tier3_bench_restore {
    ($b:expr, $seed_path:expr, $config:expr, $restore_fn:expr, $timed_fn:expr) => {{
        $b.iter_custom(|iters| {
            let mut total = std::time::Duration::from_nanos(0);
            for _ in 0..iters {
                total += run_single_shot_with_restore($seed_path, $config, $restore_fn, $timed_fn);
            }
            total
        });
    }};
}

#[macro_export]
macro_rules! tier3_bench_open {
    ($b:expr, $case:expr, $body:expr) => {{
        $b.iter_custom(|iters| {
            let mut total = std::time::Duration::from_nanos(0);
            for _ in 0..iters {
                total += $case.run_open($body);
            }
            total
        });
    }};
}
