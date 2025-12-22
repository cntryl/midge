//! Long-running stress harness (scaffold)
//!
//! Minimal scaffolding for running long-running workloads. This file intentionally
//! contains only small, well-commented stubs — we'll wire CLI/env parsing and
//! dispatch logic next.

use std::time::Duration;
use std::sync::Arc;
use anyhow::{Result, bail};
use tempfile::TempDir;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

/// Simple config: workload name + duration.
#[derive(Debug, Clone)]
pub struct Config {
    pub workload: String,
    pub duration: Duration,
}

impl Config {
    /// Construct from env or args.
    ///
    /// Usage:
    ///   stress <workload> [--duration SECS]
    ///
    /// Environment variables:
    ///   STRESS_WORKLOAD
    ///   STRESS_DURATION_SECS
    pub fn from_env_or_args() -> Self {
        let mut args = std::env::args().skip(1);
        let mut workload: Option<String> = None;
        let mut duration = std::time::Duration::from_secs(60);

        while let Some(arg) = args.next() {
            if arg.starts_with("--duration=") {
                if let Some(v) = arg.splitn(2, '=').nth(1) {
                    if let Ok(s) = v.parse::<u64>() {
                        duration = Duration::from_secs(s);
                    }
                }
            } else if arg == "--duration" {
                if let Some(v) = args.next() {
                    if let Ok(s) = v.parse::<u64>() {
                        duration = Duration::from_secs(s);
                    }
                }
            } else if workload.is_none() {
                workload = Some(arg);
            } else {
                // ignore additional args
            }
        }

        if workload.is_none() {
            if let Ok(w) = std::env::var("STRESS_WORKLOAD") {
                if !w.is_empty() {
                    workload = Some(w);
                }
            }
        }

        if let Ok(s) = std::env::var("STRESS_DURATION_SECS") {
            if let Ok(sec) = s.parse::<u64>() {
                duration = Duration::from_secs(sec);
            }
        }

        let workload = workload.unwrap_or_else(|| {
            eprintln!("Usage: stress <workload> [--duration SECS] \nExample workloads: ycsb_a, ycsb_b, soak_compaction");
            std::process::exit(2);
        });

        Self { workload, duration }
    }
}

/// Run a named workload by creating a temporary engine, optionally performing a
/// load phase (bench workloads will perform their own load), and dispatching to
/// the selected workload implementation.
pub fn run(workload: &str, cfg: &Config) -> Result<()> {
    // Temporary DB directory for the run
    let tmp = TempDir::new()?;
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };

    let engine = Arc::new(MidgeEngine::open(opts)?);

    match workload {
        // YCSB workloads
        "ycsb_a" | "workload_a" => {
            crate::ycsb::workload_a::run(Arc::clone(&engine), cfg.duration);
        }
        "ycsb_b" | "workload_b" => {
            crate::ycsb::workload_b::run(engine.as_ref(), cfg.duration);
        }
        "ycsb_c" | "workload_c" => {
            crate::ycsb::workload_c::run(engine.as_ref(), cfg.duration);
        }
        "ycsb_d" | "workload_d" => {
            crate::ycsb::workload_d::run(engine.as_ref(), cfg.duration);
        }
        "ycsb_e" | "workload_e" => {
            crate::ycsb::workload_e::run(engine.as_ref(), cfg.duration);
        }
        "ycsb_f" | "workload_f" => {
            crate::ycsb::workload_f::run(engine.as_ref(), cfg.duration);
        }

        // Soak workloads
        "soak_compaction" => {
            crate::soak::compaction::run(engine.as_ref(), cfg.duration);
        }
        "soak_level_drift" => {
            crate::soak::level_drift::run(engine.as_ref(), cfg.duration);
        }
        "soak_space_amplification" => {
            crate::soak::space_amplification::run(engine.as_ref(), cfg.duration);
        }

        other => bail!("unknown workload: {}", other),
    }

    Ok(())
}
