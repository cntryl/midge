//! Tier 5 - Memtable sizing sweep
//!
//! Exploratory local-storage sweep for explicit `OpenOptions` memtable sizes.
//! Writes one JSON object per scenario/size row to stdout and to JSONL.

use std::fs::{self, OpenOptions as FsOpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use cntryl_midge::testkit::bench::{
    init_benchmark_telemetry, kick_runtime_compaction_once, parse_memtable_sweep_sizes,
    set_runtime_compaction_enabled, unique_bench_path, MemtableSweepSize, RuntimeCounterDeltas,
    RuntimeCounterSnapshot,
};
use cntryl_midge::{
    ColumnFamilyHandle, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions,
};
use hdrhistogram::Histogram;
use serde::Serialize;

const DEFAULT_WRITES: usize = 100_000;
const DEFAULT_VALUE_BYTES: usize = 1024;
const DEFAULT_PROACTIVE_FLUSH_PERCENT: usize = 80;
const DEFAULT_OUTPUT_PATH: &str = "target/midge-perf/memtable_sweep.jsonl";
const MAX_STALL_RETRIES_PER_WRITE: usize = 100;
const COMPACTION_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum Scenario {
    FlushOnly,
    SystemPressure,
}

impl Scenario {
    fn all() -> [Self; 2] {
        [Self::FlushOnly, Self::SystemPressure]
    }

    fn name(self) -> &'static str {
        match self {
            Self::FlushOnly => "flush_only",
            Self::SystemPressure => "system_pressure",
        }
    }

    fn compaction_enabled(self) -> bool {
        match self {
            Self::FlushOnly => false,
            Self::SystemPressure => true,
        }
    }
}

#[derive(Debug)]
struct SweepConfig {
    writes: usize,
    value_bytes: usize,
    proactive_flush_percent: usize,
    sizes: Vec<MemtableSweepSize>,
    output_path: PathBuf,
}

#[derive(Debug, Serialize)]
struct SweepResult {
    scenario: &'static str,
    memtable_size_label: String,
    memtable_size_bytes: usize,
    writes: usize,
    value_bytes: usize,
    elapsed_ms: u128,
    write_elapsed_ms: u128,
    final_flush_ms: u128,
    compaction_drain_ms: u128,
    ops_per_sec: f64,
    bytes_per_sec: f64,
    commit_p50_us: u64,
    commit_p95_us: u64,
    commit_p99_us: u64,
    commit_max_us: u64,
    runtime_memtable_size_limit: usize,
    runtime_memtable_flush_threshold: usize,
    sst_count: usize,
    sst_bytes: u64,
    proactive_flushes: usize,
    compaction_kicks: usize,
    observed_write_stalls: u64,
    pre_compaction_sst_count: usize,
    pre_compaction_sst_bytes: u64,
    final_sst_count: usize,
    final_sst_bytes: u64,
    write_stalls_total: u64,
    write_stalls_memory_total: u64,
    compactions_run: u64,
    compaction_bytes_rewritten: u64,
    compaction_failures: u64,
    wal_append_count: u64,
    wal_flush_count: u64,
    wal_fsync_count: u64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("tier5_memtable_sizing_sweep failed: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    init_benchmark_telemetry()?;
    let config = SweepConfig::from_env()?;
    if let Some(parent) = config.output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = FsOpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&config.output_path)?;
    let mut writer = BufWriter::new(file);

    eprintln!(
        "writing memtable sizing sweep results to {}",
        config.output_path.display()
    );

    for scenario in Scenario::all() {
        for size in &config.sizes {
            let result = run_sweep_case(&config, scenario, size)?;
            let line = serde_json::to_string(&result)?;
            writeln!(writer, "{line}")?;
            println!("{line}");
        }
    }

    writer.flush()?;
    Ok(())
}

impl SweepConfig {
    fn from_env() -> Result<Self> {
        let writes = read_positive_usize_env("MIDGE_MEMTABLE_SWEEP_WRITES", DEFAULT_WRITES)?;
        let value_bytes =
            read_positive_usize_env("MIDGE_MEMTABLE_SWEEP_VALUE_BYTES", DEFAULT_VALUE_BYTES)?;
        let proactive_flush_percent = read_percent_env(
            "MIDGE_MEMTABLE_SWEEP_PROACTIVE_FLUSH_PERCENT",
            DEFAULT_PROACTIVE_FLUSH_PERCENT,
        )?;
        let size_env = std::env::var("MIDGE_MEMTABLE_SWEEP_SIZES").ok();
        let sizes = parse_memtable_sweep_sizes(size_env.as_deref()).map_err(anyhow::Error::msg)?;
        let output_path = std::env::var("MIDGE_MEMTABLE_SWEEP_OUTPUT")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map_or_else(|| PathBuf::from(DEFAULT_OUTPUT_PATH), PathBuf::from);

        Ok(Self {
            writes,
            value_bytes,
            proactive_flush_percent,
            sizes,
            output_path,
        })
    }
}

fn read_positive_usize_env(name: &str, default: usize) -> Result<usize> {
    let Some(raw_value) = std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return Ok(default);
    };

    let value = raw_value
        .parse::<usize>()
        .map_err(|_| anyhow!("{name} must be a positive integer, got `{raw_value}`"))?;
    if value == 0 {
        bail!("{name} must be greater than zero");
    }

    Ok(value)
}

fn read_percent_env(name: &str, default: usize) -> Result<usize> {
    let Some(raw_value) = std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return Ok(default);
    };

    let value = raw_value
        .parse::<usize>()
        .map_err(|_| anyhow!("{name} must be an integer percent, got `{raw_value}`"))?;
    if value > 100 {
        bail!("{name} must be between 0 and 100");
    }

    Ok(value)
}

fn run_sweep_case(
    config: &SweepConfig,
    scenario: Scenario,
    size: &MemtableSweepSize,
) -> Result<SweepResult> {
    let db_path = unique_bench_path(&format!(
        "memtable_sweep_{}_{}",
        scenario.name(),
        size.label
    ));
    let _ = fs::remove_dir_all(&db_path);

    let result = run_sweep_case_at_path(config, scenario, size, &db_path);
    let _ = fs::remove_dir_all(&db_path);
    result
}

fn run_sweep_case_at_path(
    config: &SweepConfig,
    scenario: Scenario,
    size: &MemtableSweepSize,
    db_path: &PathBuf,
) -> Result<SweepResult> {
    let mut open_options = OpenOptions::local(db_path);
    if let Some(bytes) = size.bytes {
        open_options = open_options.with_memtable_size_limit(bytes);
    }

    let engine = Engine::open(open_options.build())?;
    set_runtime_compaction_enabled(&engine, scenario.compaction_enabled())?;
    let cf = engine.create_column_family("sweep")?;
    let cf_id = cf.id();

    let value = vec![0xA5; config.value_bytes];
    let initial_metrics = engine.get_runtime_metrics()?;
    let initial_counter_snapshot = RuntimeCounterSnapshot::from_runtime_metrics(&initial_metrics);
    let runtime_flush_trigger = initial_metrics
        .memtable_size_limit
        .min(initial_metrics.memtable_flush_threshold)
        .max(1);
    let proactive_flush_at = if config.proactive_flush_percent == 0 {
        None
    } else {
        Some(
            runtime_flush_trigger
                .saturating_mul(config.proactive_flush_percent)
                .checked_div(100)
                .unwrap_or(1)
                .max(1),
        )
    };
    let mut commit_latencies = Histogram::<u64>::new(3)?;
    let total_start = Instant::now();
    let write_start = Instant::now();

    let mut committed = 0usize;
    let mut bytes_since_flush = 0usize;
    let mut stall_retries_for_current_write = 0usize;
    let mut proactive_flushes = 0usize;
    let mut compaction_kicks = 0usize;
    let mut observed_write_stalls = 0u64;
    while committed < config.writes {
        let key = format!("key_{committed:016}").into_bytes();
        let estimated_write_bytes = key.len().saturating_add(value.len());
        if let Some(proactive_flush_at) = proactive_flush_at {
            if bytes_since_flush > 0
                && bytes_since_flush.saturating_add(estimated_write_bytes) >= proactive_flush_at
            {
                flush_and_maybe_kick_compaction(&engine, &cf, scenario, &mut compaction_kicks)?;
                proactive_flushes += 1;
                bytes_since_flush = 0;
            }
        }

        let commit_start = Instant::now();
        let mut tx = match engine.begin_tx(cf_id, TransactionMode::ReadWrite) {
            Ok(tx) => tx,
            Err(MidgeError::WriteStall(_)) => {
                retry_after_write_stall(
                    &engine,
                    &cf,
                    committed,
                    &mut stall_retries_for_current_write,
                    scenario,
                    &mut compaction_kicks,
                    &mut observed_write_stalls,
                )?;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        match tx.put(key, value.clone(), None) {
            Ok(()) => {}
            Err(MidgeError::WriteStall(_)) => {
                retry_after_write_stall(
                    &engine,
                    &cf,
                    committed,
                    &mut stall_retries_for_current_write,
                    scenario,
                    &mut compaction_kicks,
                    &mut observed_write_stalls,
                )?;
                continue;
            }
            Err(error) => return Err(error.into()),
        }

        match tx.commit(WriteOptions::buffered()) {
            Ok(()) => {
                let latency_us = commit_start.elapsed().as_micros().max(1) as u64;
                commit_latencies.record(latency_us)?;
                committed += 1;
                bytes_since_flush = bytes_since_flush.saturating_add(estimated_write_bytes);
                stall_retries_for_current_write = 0;
            }
            Err(MidgeError::WriteStall(_)) => {
                retry_after_write_stall(
                    &engine,
                    &cf,
                    committed,
                    &mut stall_retries_for_current_write,
                    scenario,
                    &mut compaction_kicks,
                    &mut observed_write_stalls,
                )?;
            }
            Err(error) => return Err(error.into()),
        }
    }

    let write_elapsed = write_start.elapsed();
    let final_flush_start = Instant::now();
    flush_and_maybe_kick_compaction(&engine, &cf, scenario, &mut compaction_kicks)?;
    let final_flush_elapsed = final_flush_start.elapsed();
    let pre_compaction_metrics = engine.get_runtime_metrics()?;

    let compaction_drain_elapsed = drain_compaction_pressure(&engine, scenario)?;
    let elapsed = total_start.elapsed();
    let metrics = engine.get_runtime_metrics()?;
    let counter_deltas = RuntimeCounterDeltas::between(
        initial_counter_snapshot,
        RuntimeCounterSnapshot::from_runtime_metrics(&metrics),
    );
    let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
    let written_bytes = config.writes.saturating_mul(config.value_bytes) as f64;

    Ok(SweepResult {
        scenario: scenario.name(),
        memtable_size_label: size.label.clone(),
        memtable_size_bytes: size.bytes.unwrap_or(metrics.memtable_size_limit),
        writes: config.writes,
        value_bytes: config.value_bytes,
        elapsed_ms: elapsed.as_millis(),
        write_elapsed_ms: write_elapsed.as_millis(),
        final_flush_ms: final_flush_elapsed.as_millis(),
        compaction_drain_ms: compaction_drain_elapsed.as_millis(),
        ops_per_sec: config.writes as f64 / elapsed_secs,
        bytes_per_sec: written_bytes / elapsed_secs,
        commit_p50_us: commit_latencies.value_at_percentile(50.0),
        commit_p95_us: commit_latencies.value_at_percentile(95.0),
        commit_p99_us: commit_latencies.value_at_percentile(99.0),
        commit_max_us: commit_latencies.max(),
        runtime_memtable_size_limit: metrics.memtable_size_limit,
        runtime_memtable_flush_threshold: metrics.memtable_flush_threshold,
        sst_count: metrics.sst_count,
        sst_bytes: metrics.sst_bytes,
        proactive_flushes,
        compaction_kicks,
        observed_write_stalls,
        pre_compaction_sst_count: pre_compaction_metrics.sst_count,
        pre_compaction_sst_bytes: pre_compaction_metrics.sst_bytes,
        final_sst_count: metrics.sst_count,
        final_sst_bytes: metrics.sst_bytes,
        write_stalls_total: counter_deltas.write_stalls_total,
        write_stalls_memory_total: counter_deltas.write_stalls_memory_total,
        compactions_run: counter_deltas.compactions_run,
        compaction_bytes_rewritten: counter_deltas.compaction_bytes_rewritten,
        compaction_failures: counter_deltas.compaction_failures,
        wal_append_count: counter_deltas.wal_append_count,
        wal_flush_count: counter_deltas.wal_flush_count,
        wal_fsync_count: counter_deltas.wal_fsync_count,
    })
}

fn flush_and_maybe_kick_compaction(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    scenario: Scenario,
    compaction_kicks: &mut usize,
) -> Result<()> {
    engine.flush_cf(cf)?;
    if scenario.compaction_enabled() {
        kick_runtime_compaction_once(engine)?;
        *compaction_kicks += 1;
    }
    Ok(())
}

fn drain_compaction_pressure(engine: &Engine, scenario: Scenario) -> Result<Duration> {
    if !scenario.compaction_enabled() {
        return Ok(Duration::default());
    }

    let start = Instant::now();
    wait_for_compactions_to_idle(engine, COMPACTION_IDLE_TIMEOUT)?;
    engine.compact_all()?;
    wait_for_compactions_to_idle(engine, COMPACTION_IDLE_TIMEOUT)?;
    Ok(start.elapsed())
}

fn wait_for_compactions_to_idle(engine: &Engine, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    loop {
        let metrics = engine.get_runtime_metrics()?;
        if metrics.active_compactions == 0
            && metrics.compacting_ssts == 0
            && metrics.pending_compactions == 0
        {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            bail!(
                "compactions did not become idle within {:?}; active={}, compacting_ssts={}, pending={}",
                timeout,
                metrics.active_compactions,
                metrics.compacting_ssts,
                metrics.pending_compactions
            );
        }

        std::thread::sleep(Duration::from_millis(10));
    }
}

fn retry_after_write_stall(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    write_index: usize,
    stall_retries_for_current_write: &mut usize,
    scenario: Scenario,
    compaction_kicks: &mut usize,
    observed_write_stalls: &mut u64,
) -> Result<()> {
    *observed_write_stalls = (*observed_write_stalls).saturating_add(1);
    *stall_retries_for_current_write += 1;
    if *stall_retries_for_current_write > MAX_STALL_RETRIES_PER_WRITE {
        bail!("write {write_index} stalled more than {MAX_STALL_RETRIES_PER_WRITE} times");
    }

    match engine.flush_cf(cf) {
        Ok(()) => {
            if scenario.compaction_enabled() {
                kick_runtime_compaction_once(engine)?;
                *compaction_kicks += 1;
            }
        }
        Err(MidgeError::WriteStall(_)) => {}
        Err(error) => return Err(error.into()),
    }

    if !engine.wait_for_write_stall_clear(cf.id(), Duration::from_millis(250))? {
        std::thread::sleep(Duration::from_millis(5));
    }

    Ok(())
}
