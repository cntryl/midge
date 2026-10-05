//! Release checkpoint measurement: one fixed cell per fresh process.
//! Uses the private accounting bridge without changing checkpoint cadence.

#[path = "./bench_support/checkpoint_boundary.rs"]
mod checkpoint_boundary;
#[path = "./bench_support/checkpoint_commit.rs"]
mod checkpoint_commit;
#[path = "./bench_support/checkpoint_gate.rs"]
mod checkpoint_gate;
#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::__internal::checkpoint::{metrics_handle, MetricsHandle, Snapshot};
use cntryl_midge::{
    ColumnFamilyHandle, Engine, MemoryBudget, MidgeResult, OpenOptions, Query, RecoveryPolicy,
    RuntimeMetricsSnapshot, TransactionMode, WriteOptions,
};
use cntryl_stress::{
    stress, stress_main, LogicalUnit, OperationOutcome, ProgressHandle, StressContext,
};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const VALUE_BYTES: usize = 1024;
const MEMTABLE_BYTES: usize = 8 * 1024 * 1024;
const CELL_TIMEOUT: Duration = Duration::from_mins(15);
static EXECUTED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy)]
struct Cell {
    workload: &'static str,
    id: &'static str,
    cycles: usize,
    rows: usize,
    families: usize,
}

impl Cell {
    fn warmup(self) -> usize {
        self.cycles.div_ceil(10)
    }
    fn measured(self) -> usize {
        self.cycles - self.warmup()
    }
}

fn record(family: usize, ordinal: usize) -> (Vec<u8>, Vec<u8>) {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&u64::try_from(family).expect("fixed family").to_be_bytes());
    key.extend_from_slice(&u64::try_from(ordinal).expect("fixed row").to_be_bytes());
    let mut state = u64::try_from(ordinal)
        .expect("fixed row")
        .wrapping_add(1)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ u64::try_from(family)
            .expect("fixed family")
            .wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let mut value = Vec::with_capacity(VALUE_BYTES);
    for _ in 0..VALUE_BYTES / 8 {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        value.extend_from_slice(&state.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes());
    }
    (key, value)
}

fn atomic_json(path: &Path, value: &Value) -> Result<(), String> {
    let temporary = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    std::fs::write(&temporary, bytes).map_err(|error| error.to_string())?;
    std::fs::rename(temporary, path).map_err(|error| error.to_string())
}

fn actual_sha() -> Result<String, String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err("git rev-parse HEAD failed".into());
    }
    let sha = String::from_utf8(output.stdout)
        .map_err(|error| error.to_string())?
        .trim()
        .to_string();
    if sha.len() != 40 || !sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid git SHA".into());
    }
    if std::env::var("STRESS_GIT_SHA").is_ok_and(|expected| expected != sha) {
        return Err("declared SHA differs from actual checkout".into());
    }
    Ok(sha)
}

struct Attempt {
    directory: PathBuf,
    status: Value,
    last_save: Instant,
    started: Instant,
    measured_started: Option<Instant>,
    measured_elapsed: Duration,
    measured_acknowledged_rows: u64,
    commit_observations: checkpoint_commit::Observations,
    boundary_before: checkpoint_boundary::Observation,
    boundary_after: checkpoint_boundary::Observation,
}

impl Attempt {
    fn begin(cell: Cell) -> Result<Self, String> {
        let sha = actual_sha()?;
        let repeat =
            std::env::var("MIDGE_CHECKPOINT_REPEAT").map_err(|_| "missing repeat identity")?;
        if !matches!(repeat.as_str(), "1" | "2" | "3") {
            return Err("repeat must be 1, 2 or 3".into());
        }
        let root = std::env::var_os("MIDGE_STRESS_ARTIFACT_DIR").ok_or("missing artifact root")?;
        let directory =
            PathBuf::from(root).join(format!("{}-{}", cell.workload, std::process::id()));
        std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
        let status = json!({"schema_version":"midge-checkpoint.v1", "benchmark_workload":cell.workload,
            "git_commit":sha, "process_id":std::process::id(), "cell":cell.id, "repeat":repeat,
            "status":"running", "phase":"setup", "stage_index":null, "terminal_error":null,
            "total_cycles":cell.cycles, "warmup_cycles":cell.warmup(), "measured_cycles":cell.measured(),
            "rows_per_cycle":cell.rows, "value_bytes":VALUE_BYTES, "families":cell.families,
            "completed_cycles":0, "acknowledged_rows":0, "verified":false, "reopened_verified":false,
            "commit_backpressure_policy":{"schema_version":"midge-checkpoint-commit-policy.v1",
                "retry_error":"write_stall_only", "retry_budget_ns":30_000_000_000u64,
                "wait_slice_ns":1_000_000_000u64, "cell_budget_ns":900_000_000_000u64,
                "required_no_progress_timeout_ns":60_000_000_000u64},
            "metadata_boundary_policy":{"schema_version":"midge-checkpoint-boundary-policy.v1",
                "metadata_scope":"persistent_only", "boundary_budget_ns":30_000_000_000u64,
                "pause_slice_ns":1_000_000u64, "cell_budget_ns":900_000_000_000u64,
                "sample_order":"runtime_metrics_then_metadata_snapshot", "zero_persistent_active_required":true,
                "before_clock_scope":"warmup_wait_outside_measured_final_query_inside",
                "after_clock_scope":"end_inside_measured", "progress_advanced":false}});
        atomic_json(&directory.join("workload-status.json"), &status)?;
        Ok(Self {
            directory,
            status,
            last_save: Instant::now(),
            started: Instant::now(),
            measured_started: None,
            measured_elapsed: Duration::ZERO,
            measured_acknowledged_rows: 0,
            commit_observations: checkpoint_commit::Observations::default(),
            boundary_before: checkpoint_boundary::Observation::default(),
            boundary_after: checkpoint_boundary::Observation::default(),
        })
    }

    fn save(&mut self, force: bool) -> Result<(), String> {
        self.status["commit_backpressure"] =
            serde_json::to_value(&self.commit_observations).map_err(|error| error.to_string())?;
        self.status["metadata_boundary_before"] =
            serde_json::to_value(&self.boundary_before).map_err(|error| error.to_string())?;
        self.status["metadata_boundary_after"] =
            serde_json::to_value(&self.boundary_after).map_err(|error| error.to_string())?;
        if force || self.last_save.elapsed() >= Duration::from_secs(1) {
            atomic_json(&self.directory.join("workload-status.json"), &self.status)?;
            self.last_save = Instant::now();
        }
        Ok(())
    }

    fn check_budget(&self) -> Result<(), String> {
        if self.started.elapsed() >= CELL_TIMEOUT {
            Err("fixed workload budget expired".into())
        } else {
            Ok(())
        }
    }
}

fn options(path: &Path) -> Result<OpenOptions, String> {
    OpenOptions::local(path)
        .recovery_policy(RecoveryPolicy::Strict)
        .memory_budget(MemoryBudget::Bytes(512 * 1024 * 1024))
        .transaction_memory_pool_size(32 * 1024 * 1024)
        .with_memtable_size_limit(MEMTABLE_BYTES)
        .with_memtable_flush_threshold(MEMTABLE_BYTES)
        .background_compaction(true)
        .build()
        .map_err(|error| error.to_string())
}

fn open_families(engine: &Engine, count: usize) -> Result<Vec<ColumnFamilyHandle>, String> {
    let mut families = vec![engine
        .get_column_family("default")
        .ok_or("missing default CF")?];
    for index in 1..count {
        families.push(
            engine
                .create_column_family(&format!("checkpoint-{index:02}"))
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(families)
}

fn commit_cycle(
    engine: &Engine,
    family: &ColumnFamilyHandle,
    cell: Cell,
    cycle: usize,
) -> MidgeResult<()> {
    let mut transaction = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
    for index in 0..cell.rows {
        let (key, value) = record(cycle % cell.families, cycle * cell.rows + index);
        transaction.put(key, value, None)?;
    }
    // Actual local strict acknowledgement; no terminal/backpressure error is hidden.
    transaction.commit(WriteOptions::sync())
}

fn verify(engine: &Engine, cell: Cell, progress: &ProgressHandle) -> Result<(), String> {
    for family_index in 0..cell.families {
        let name = if family_index == 0 {
            "default".to_string()
        } else {
            format!("checkpoint-{family_index:02}")
        };
        let family = engine
            .get_column_family(&name)
            .ok_or("missing verification CF")?;
        let transaction = engine
            .begin_tx(family.id(), TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let mut expected_count = 0_usize;
        for cycle in (family_index..cell.cycles).step_by(cell.families) {
            for index in 0..cell.rows {
                let (key, expected) = record(family_index, cycle * cell.rows + index);
                let actual = transaction
                    .get(&key)
                    .map_err(|error| error.to_string())?
                    .ok_or("missing acknowledged key")?;
                if actual.as_ref() != expected.as_slice() {
                    return Err("point value mismatch".into());
                }
                expected_count += 1;
                if expected_count.is_multiple_of(64) {
                    progress.advance_by(64);
                }
            }
        }
        let mut seen = 0_usize;
        for result in transaction
            .scan(&Query::new())
            .map_err(|error| error.to_string())?
        {
            let (key, actual) = result.map_err(|error| error.to_string())?;
            if seen >= expected_count {
                return Err("extra scan row".into());
            }
            let cycle = family_index + (seen / cell.rows) * cell.families;
            let ordinal = cycle * cell.rows + seen % cell.rows;
            let (expected_key, expected_value) = record(family_index, ordinal);
            if key.as_ref() != expected_key.as_slice()
                || actual.as_ref() != expected_value.as_slice()
            {
                return Err("scan key/order/value mismatch".into());
            }
            seen += 1;
            if seen.is_multiple_of(64) {
                progress.advance_by(64);
            }
        }
        if seen != expected_count {
            return Err("full scan row count mismatch".into());
        }
    }
    Ok(())
}

fn accounting(handle: &MetricsHandle, path: &Path) -> Result<Snapshot, String> {
    let snapshot = handle.snapshot();
    atomic_json(
        path,
        &serde_json::to_value(&snapshot).map_err(|error| error.to_string())?,
    )?;
    Ok(snapshot)
}

struct Window {
    before: Snapshot,
    before_compactions: u64,
    before_commit_observations: checkpoint_commit::Observations,
    flush_latencies_ns: Vec<u64>,
    pressure_samples: Vec<Value>,
}

fn capture_boundary(
    engine: &Engine,
    handle: &MetricsHandle,
    attempt: &mut Attempt,
    before: bool,
) -> Result<checkpoint_boundary::Boundary<RuntimeMetricsSnapshot>, String> {
    let original_deadline = attempt.started + CELL_TIMEOUT;
    let observation = if before {
        &mut attempt.boundary_before
    } else {
        &mut attempt.boundary_after
    };
    checkpoint_boundary::capture_metadata_boundary(
        original_deadline,
        Duration::from_secs(30),
        observation,
        Instant::now,
        |remaining| {
            let runtime = engine
                .metrics()
                .get_runtime_metrics_with_timeout(remaining)?;
            Ok((runtime, handle.snapshot()))
        },
        std::thread::sleep,
    )
    .map_err(|error| format!("metadata boundary capture: {error:?}"))
}

fn begin_window(
    engine: &Engine,
    handle: &MetricsHandle,
    cell: Cell,
    attempt: &mut Attempt,
) -> Result<Window, String> {
    let boundary = capture_boundary(engine, handle, attempt, true)?;
    let before = boundary.snapshot;
    let runtime_before = boundary.runtime;
    // Earlier warmup active candidates and pauses are outside measurement.
    // The accepted query/snapshot pair begins inside the measured clock.
    attempt.measured_started = Some(boundary.query_started);
    atomic_json(
        &attempt.directory.join("accounting-before.json"),
        &serde_json::to_value(&before).map_err(|error| error.to_string())?,
    )?;
    attempt.status["phase"] = json!("measured_ingestion");
    attempt.save(true)?;
    let before_compactions = runtime_before.compactions_run;
    atomic_json(
        &attempt.directory.join("runtime-before.json"),
        &serde_json::to_value(&runtime_before).map_err(|error| error.to_string())?,
    )?;
    Ok(Window {
        before,
        before_compactions,
        before_commit_observations: attempt.commit_observations.clone(),
        flush_latencies_ns: Vec::with_capacity(cell.measured()),
        pressure_samples: Vec::with_capacity(5),
    })
}

fn record_measured_flush(
    window: &mut Window,
    engine: &Engine,
    cell: Cell,
    cycle: usize,
    elapsed: Duration,
    attempt: &Attempt,
) -> Result<(), String> {
    window
        .flush_latencies_ns
        .push(u64::try_from(elapsed.as_nanos()).map_err(|_| "flush latency overflow")?);
    if (cycle - cell.warmup()).is_multiple_of(cell.measured().div_ceil(4)) {
        window.pressure_samples.push(json!({"completed_cycle":cycle + 1,
            "measured_acknowledged_rows":attempt.measured_acknowledged_rows,
            "measured_elapsed_ns":attempt.measured_started.ok_or("missing clock")?.elapsed().as_nanos(),
            "runtime":engine.metrics().get_runtime_metrics().map_err(|error| error.to_string())?}));
    }
    Ok(())
}

fn finish_window(
    mut window: Window,
    engine: &Engine,
    handle: &MetricsHandle,
    cell: Cell,
    attempt: &mut Attempt,
) -> Result<(Snapshot, u64), String> {
    // The whole end-boundary query/wait stays inside the measured clock.
    // Sampling does not advance progress or require global runtime idleness.
    let boundary = capture_boundary(engine, handle, attempt, false)?;
    let runtime_after = boundary.runtime;
    let after = boundary.snapshot;
    attempt.measured_elapsed = attempt
        .measured_started
        .ok_or("no measured interval")?
        .elapsed();
    window
        .pressure_samples
        .push(json!({"completed_cycle":cell.cycles,
        "measured_acknowledged_rows":attempt.measured_acknowledged_rows,
        "measured_elapsed_ns":attempt.measured_elapsed.as_nanos(), "runtime":runtime_after}));
    let compactions = runtime_after
        .compactions_run
        .checked_sub(window.before_compactions)
        .ok_or("compaction counter decreased")?;
    atomic_json(
        &attempt.directory.join("runtime-after.json"),
        &serde_json::to_value(&runtime_after).map_err(|error| error.to_string())?,
    )?;
    atomic_json(
        &attempt.directory.join("ingestion-observations.json"),
        &json!({"flush_latencies_ns":window.flush_latencies_ns,"pressure_samples":window.pressure_samples, "commit_backpressure_before":window.before_commit_observations, "commit_backpressure_after":attempt.commit_observations, "metadata_boundary_before":attempt.boundary_before, "metadata_boundary_after":attempt.boundary_after}),
    )?;
    atomic_json(
        &attempt.directory.join("accounting-after.json"),
        &serde_json::to_value(&after).map_err(|error| error.to_string())?,
    )?;
    let delta = after.delta(&window.before).map_err(str::to_string)?;
    Ok((delta, compactions))
}

fn run_ingestion(
    engine: &Engine,
    families: &[ColumnFamilyHandle],
    handle: &MetricsHandle,
    cell: Cell,
    progress: &ProgressHandle,
    attempt: &mut Attempt,
) -> Result<(Snapshot, u64), String> {
    let mut window = None;
    for cycle in 0..cell.cycles {
        attempt.check_budget()?;
        if cycle == cell.warmup() {
            window = Some(begin_window(engine, handle, cell, attempt)?);
        }
        checkpoint_commit::commit_with_backpressure(
            attempt.started + CELL_TIMEOUT,
            Duration::from_secs(30),
            &mut attempt.commit_observations,
            Instant::now,
            || commit_cycle(engine, &families[cycle % cell.families], cell, cycle),
            |timeout| {
                engine.wait_for_write_stall_clear(families[cycle % cell.families].id(), timeout)
            },
        )
        .map_err(|error| format!("strict commit: {error:?}"))?;
        progress.advance_by(u64::try_from(cell.rows).expect("fixed row count"));
        if window.is_some() {
            attempt.measured_acknowledged_rows +=
                u64::try_from(cell.rows).expect("fixed row count");
        }
        attempt.status["acknowledged_rows"] = json!((cycle + 1) * cell.rows);
        attempt.status["flush_in_flight"] = json!(true);
        // Companion artifacts live on tmpfs, never on the measured DB filesystem.
        // Retain the true acknowledgement before a flush can outlive its caller.
        attempt.save(true)?;
        let flush_started = Instant::now();
        engine
            .flush_cf(&families[cycle % cell.families])
            .map_err(|error| format!("explicit flush: {error:?}"))?;
        if let Some(window) = &mut window {
            record_measured_flush(
                window,
                engine,
                cell,
                cycle,
                flush_started.elapsed(),
                attempt,
            )?;
        }
        progress.advance();
        attempt.status["completed_cycles"] = json!(cycle + 1);
        attempt.status["flush_in_flight"] = json!(false);
        attempt.save(false)?;
    }
    finish_window(
        window.ok_or("no warmup boundary")?,
        engine,
        handle,
        cell,
        attempt,
    )
}

fn persist_window(
    attempt: &Attempt,
    delta: &Snapshot,
    verdict: &checkpoint_gate::Verdict,
    compactions: u64,
    finalized_after_shutdown: bool,
) -> Result<(), String> {
    atomic_json(
        &attempt.directory.join("accounting-window.json"),
        &json!({"delta":delta,"gate":verdict,"completed_compactions":compactions,
            "measured_elapsed_ns":attempt.measured_elapsed.as_nanos(),
            "measured_acknowledged_rows":attempt.measured_acknowledged_rows,
            "finalized_after_shutdown":finalized_after_shutdown}),
    )
}

fn execute(cell: Cell, progress: &ProgressHandle, attempt: &mut Attempt) -> Result<(), String> {
    if cfg!(debug_assertions) {
        return Err(
            "measurement requires release optimization and disabled debug assertions".into(),
        );
    }
    if EXECUTED.swap(true, Ordering::Relaxed) {
        return Err("fixed cell was invoked twice in one process".into());
    }
    // Never remove a database under accepted work if a failing caller exits.
    // Its fresh-runner lifetime supplies cleanup; the data is retained locally.
    let database = tempfile::tempdir()
        .map_err(|error| error.to_string())?
        .keep();
    attempt.status["database_path"] = json!(database);
    attempt.save(true)?;
    let options = options(&database)?;
    let resolved = json!({"memory_budget_bytes":options.memory_budget_bytes(),
        "memtable_size_limit":options.memtable_size_limit(), "memtable_flush_threshold":options.memtable_flush_threshold(),
        "target_sst_size":options.target_sst_size(), "l0_compaction_trigger":options.l0_compaction_trigger(),
        "background_compaction":true, "transaction_memory_pool_bytes":32 * 1024 * 1024});
    atomic_json(&attempt.directory.join("resolved-options.json"), &resolved)?;
    let mut engine = Engine::open(options.clone()).map_err(|error| format!("open: {error:?}"))?;
    let handle = metrics_handle(&engine);
    let families = open_families(&engine, cell.families)?;
    let (delta, compactions) = run_ingestion(&engine, &families, &handle, cell, progress, attempt)?;
    let mut verdict = checkpoint_gate::evaluate(
        &delta,
        u64::try_from(cell.measured()).expect("fixed cycles"),
        compactions,
    );
    persist_window(attempt, &delta, &verdict, compactions, false)?;
    attempt.status["phase"] = json!("verification");
    attempt.save(true)?;
    verify(&engine, cell, progress)?;
    attempt.status["verified"] = json!(true);
    engine
        .shutdown(Duration::from_secs(30))
        .map_err(|error| format!("shutdown: {error:?}"))?;
    accounting(
        &handle,
        &attempt.directory.join("accounting-after-shutdown.json"),
    )?;
    let mut reopened = Engine::open(options).map_err(|error| format!("reopen: {error:?}"))?;
    verify(&reopened, cell, progress)?;
    attempt.status["reopened_verified"] = json!(true);
    let reopened_handle = metrics_handle(&reopened);
    let reopened_owner = reopened_handle.snapshot().owner_id;
    reopened
        .shutdown(Duration::from_secs(30))
        .map_err(|error| format!("reopened shutdown: {error:?}"))?;
    let settled_reopened = accounting(
        &reopened_handle,
        &attempt
            .directory
            .join("accounting-reopened-after-shutdown.json"),
    )?;
    let settled_original = accounting(
        &handle,
        &attempt.directory.join("accounting-original-final.json"),
    )?;
    checkpoint_gate::seal_after_shutdown(&mut verdict, reopened_owner, &settled_reopened);
    checkpoint_gate::seal_after_shutdown(&mut verdict, delta.owner_id, &settled_original);
    persist_window(attempt, &delta, &verdict, compactions, true)?;
    if !verdict.valid {
        return Err(format!(
            "invalid measurement construction: {:?}",
            verdict.invalid_reasons
        ));
    }
    Ok(())
}

fn run_cell(ctx: &mut StressContext, cell: Cell) {
    stress_config::init_benchmark_telemetry().expect("initialize benchmark telemetry");
    let mut attempt = Attempt::begin(cell).expect("create immutable attempt identity");
    let result = execute(cell, &ctx.progress_handle(), &mut attempt);
    if attempt.measured_elapsed.is_zero() {
        attempt.measured_elapsed = attempt
            .measured_started
            .map_or(attempt.started.elapsed(), |started| started.elapsed());
    }
    attempt.status["status"] = json!(if result.is_ok() { "passed" } else { "failed" });
    attempt.status["phase"] = json!("complete");
    attempt.status["terminal_error"] = json!(result.as_ref().err());
    attempt
        .save(true)
        .expect("retain actual failed/successful attempt");
    ctx.parameter("cell", cell.id)
        .parameter("total_cycles", cell.cycles)
        .parameter("warmup_cycles", cell.warmup())
        .parameter("measured_cycles", cell.measured())
        .parameter("families", cell.families)
        .parameter("value_bytes", VALUE_BYTES)
        .parameter("logical_payload_per_cycle", cell.rows * VALUE_BYTES)
        .parameter("background_compaction", true)
        .parameter("one_fresh_process", true)
        .parameter("commit_stall_budget_ms", 30_000)
        .parameter("commit_wait_slice_ms", 1_000)
        .parameter("device_write_amplification_measured", false);
    let outcome = OperationOutcome {
        attempted: attempt.measured_acknowledged_rows,
        completed: attempt.measured_acknowledged_rows,
        failures: u64::from(result.is_err()),
        ..OperationOutcome::default()
    };
    ctx.record_external_outcome(
        cell.workload,
        attempt.measured_elapsed,
        LogicalUnit::new("acknowledged_row"),
        outcome,
    );
    if let Err(error) = result {
        panic!("fixed checkpoint measurement failed: {error}");
    }
}

#[stress(
    tier = 4,
    role = "diagnostic",
    metadata(
        component = "checkpoint_write_amplification",
        measurement_shape = "fixed_workload"
    )
)]
fn checkpoint_local_256x1mib_1cf(ctx: &mut StressContext) {
    run_cell(
        ctx,
        Cell {
            workload: "checkpoint_local_256x1mib_1cf",
            id: "A",
            cycles: 256,
            rows: 1024,
            families: 1,
        },
    );
}

#[stress(
    tier = 4,
    role = "diagnostic",
    metadata(
        component = "checkpoint_write_amplification",
        measurement_shape = "fixed_workload"
    )
)]
fn checkpoint_local_512x256kib_16cf(ctx: &mut StressContext) {
    run_cell(
        ctx,
        Cell {
            workload: "checkpoint_local_512x256kib_16cf",
            id: "B",
            cycles: 512,
            rows: 256,
            families: 16,
        },
    );
}

#[stress(
    tier = 4,
    role = "diagnostic",
    metadata(
        component = "checkpoint_write_amplification",
        measurement_shape = "fixed_workload"
    )
)]
fn checkpoint_local_1024x64kib_1cf(ctx: &mut StressContext) {
    run_cell(
        ctx,
        Cell {
            workload: "checkpoint_local_1024x64kib_1cf",
            id: "C",
            cycles: 1024,
            rows: 64,
            families: 1,
        },
    );
}

stress_main!();
