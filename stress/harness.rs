//! Long-running stress harness (scaffold)
//!
//! Minimal scaffolding for running long-running workloads. This file intentionally
//! contains only small, well-commented stubs — we'll wire CLI/env parsing and
//! dispatch logic next.

use anyhow::{bail, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

/// Simple config: workload name + duration.
#[derive(Debug, Clone)]
pub struct Config {
    pub workload: String,
    pub duration: Duration,
    /// Verbose output, enabled via `--verbose` / `-v` or `STRESS_VERBOSE=1`.
    pub verbose: bool,
    /// Skip the load phase (STRESS_SKIP_LOAD=1 or --skip-load)
    pub skip_load: bool,
    /// Optional comma-separated list of scenarios to run (e.g., "fs_nosync,fs_sync").
    pub scenarios: Option<Vec<String>>,
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
        let mut verbose = false;
        let mut skip_load = false;
        let mut scenarios: Option<Vec<String>> = None;

        while let Some(arg) = args.next() {
            if arg.starts_with("--duration=") {
                if let Some(v) = arg.split_once('=').map(|x| x.1) {
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
            } else if arg == "--verbose" || arg == "-v" {
                verbose = true;
            } else if arg == "--skip-load" {
                skip_load = true;
            } else if arg.starts_with("--scenarios=") {
                if let Some(v) = arg.split_once('=').map(|x| x.1) {
                    let list = v
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<_>>();
                    if !list.is_empty() {
                        scenarios = Some(list);
                    }
                }
            } else if arg == "--scenarios" {
                if let Some(v) = args.next() {
                    let list = v
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<_>>();
                    if !list.is_empty() {
                        scenarios = Some(list);
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

        if let Ok(s) = std::env::var("STRESS_VERBOSE") {
            if s == "1" || s.eq_ignore_ascii_case("true") {
                verbose = true;
            }
        }

        if let Ok(s) = std::env::var("STRESS_SKIP_LOAD") {
            if s == "1" || s.eq_ignore_ascii_case("true") {
                skip_load = true;
            }
        }

        if let Ok(s) = std::env::var("STRESS_SCENARIOS") {
            let list = s
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>();
            if !list.is_empty() {
                scenarios = Some(list);
            }
        }

        let workload = workload.unwrap_or_else(|| {
            eprintln!("Usage: stress <workload> [--duration SECS] [--verbose] [--scenarios sc1,sc2] \nExample workloads: ycsb_a, ycsb_b, soak_compaction");
            std::process::exit(2);
        });

        Self {
            workload,
            duration,
            verbose,
            skip_load,
            scenarios,
        }
    }
}

/// Run a named workload by creating a temporary engine, optionally performing a
/// load phase (bench workloads will perform their own load), and dispatching to
/// the selected workload implementation.
pub fn run(workload: &str, cfg: &Config) -> Result<()> {
    let default_scenarios = ["fs_nosync", "fs_sync", "cloud_nosync", "cloud_sync"];
    let scenarios_to_run: Vec<String> = cfg
        .scenarios
        .clone()
        .unwrap_or_else(|| default_scenarios.iter().map(|s| s.to_string()).collect());

    for scenario in scenarios_to_run {
        eprintln!("--- workload '{}' on scenario '{}' ---", workload, scenario);

        if cfg.verbose {
            eprintln!("scenario start: {:?}", Instant::now());
        }

        let base_dir = std::env::var("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::current_dir().unwrap().join("target"));
        let stress_dir = base_dir.join("stress").join(workload).join(&scenario);
        std::fs::create_dir_all(&stress_dir)?;
        if cfg.verbose {
            eprintln!("creating temp DB in: {}", stress_dir.display());
        }
        let tmp = TempDir::new_in(&stress_dir)?;

        // Create loader options for fast, non-durable bulk ingest: disable compaction and WAL sync, increase memtable
        let _loader_opts = match scenario.as_str() {
            "fs_nosync" => MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: tmp.path().to_path_buf(),
                },
                memtable_size: 256 * 1024 * 1024,
                enable_compaction: false,
                wal_sync: false,
                ..Default::default()
            },
            "fs_sync" => MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: tmp.path().to_path_buf(),
                },
                memtable_size: 256 * 1024 * 1024,
                enable_compaction: false,
                wal_sync: false,
                ..Default::default()
            },
            "cloud_nosync" => MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: tmp.path().to_path_buf(),
                },
                memtable_size: 256 * 1024 * 1024,
                enable_compaction: false,
                wal_sync: false,
                ..Default::default()
            },
            "cloud_sync" => MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: tmp.path().to_path_buf(),
                },
                memtable_size: 256 * 1024 * 1024,
                enable_compaction: false,
                wal_sync: false,
                ..Default::default()
            },
            other => bail!("unknown scenario: {}", other),
        };

        // Open a runner engine with normal durability/compaction settings for the run.
        // We'll use the engine itself to enter a temporary ingest mode for bulk load
        let runner_opts = match scenario.as_str() {
            "fs_nosync" => MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: tmp.path().to_path_buf(),
                },
                memtable_size: 64 * 1024 * 1024,
                enable_compaction: true,
                wal_sync: false,
                ..Default::default()
            },
            "fs_sync" => MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: tmp.path().to_path_buf(),
                },
                memtable_size: 64 * 1024 * 1024,
                enable_compaction: true,
                wal_sync: true,
                ..Default::default()
            },
            "cloud_nosync" => MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: tmp.path().to_path_buf(),
                },
                memtable_size: 64 * 1024 * 1024,
                enable_compaction: true,
                wal_sync: false,
                ..Default::default()
            },
            "cloud_sync" => MidgeOptions {
                storage_mode: StorageMode::CloudBacked {
                    local_cache_path: tmp.path().to_path_buf(),
                },
                memtable_size: 64 * 1024 * 1024,
                enable_compaction: true,
                wal_sync: true,
                ..Default::default()
            },
            other => bail!("unknown scenario: {}", other),
        };

        let engine = Arc::new(MidgeEngine::open_with_options(runner_opts)?);

        // Optionally perform a single, once-per-engine load by entering ingest mode on the engine.
        let mut loaded_here = false;
        if !cfg.skip_load {
            if cfg.verbose {
                eprintln!(
                    "entering ingest mode and performing probe+load for scenario {}",
                    scenario
                );
            }
            // Warm up WAL/filesystem to avoid first-write stalls (must be done BEFORE entering ingest)
            crate::ycsb::workload_a::warmup_wal(&engine, cfg.verbose);
            // Probe to detect first-write / WAL behaviour (after warmup)
            crate::ycsb::workload_a::probe_batch_writes(&engine, cfg.verbose, 3);

            // Enter ingest mode (returns previous snapshot) and load dataset
            let prev = engine.enter_ingest_mode()?;
            // Prepare CFs and load dataset once across CFs
            crate::ycsb::workload_a::prepare_for_load(&engine, cfg.verbose);
            // Restore previous runtime settings
            engine.exit_ingest_mode(prev)?;
            loaded_here = true;
        } else if cfg.verbose {
            eprintln!(
                "skip-load enabled; not loading dataset for scenario {}",
                scenario
            );
        }

        if cfg.verbose {
            eprintln!(
                "Starting workload '{}' at {:?} (duration={:?})",
                workload,
                Instant::now(),
                cfg.duration
            );
        }

        let run_start = Instant::now();

        let effective_skip_load = cfg.skip_load || loaded_here;

        match workload {
            // YCSB workloads
            "ycsb_a" | "workload_a" => {
                crate::ycsb::workload_a::run(
                    Arc::clone(&engine),
                    cfg.duration,
                    effective_skip_load,
                    cfg.verbose,
                );
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
        } // end match

        let run_elapsed = run_start.elapsed();
        if cfg.verbose {
            eprintln!(
                "Workload '{}' finished in {:.3}s",
                workload,
                run_elapsed.as_secs_f64()
            );
        }

        // Ensure engine is dropped before attempting to remove DB files
        drop(engine);

        // Capture the path before we take ownership via `close()` which consumes `tmp`.
        let tmp_path = tmp.path().to_path_buf();
        if cfg.verbose {
            eprintln!("removing temp DB dir: {}", tmp_path.display());
        }

        if let Err(e) = tmp.close() {
            eprintln!(
                "warning: failed to remove temp DB dir {}: {}",
                tmp_path.display(),
                e
            );
        }
    } // end for scenario

    Ok(())
}
