//! Actual final-flush callers under the external watchdog in isolated children.

use super::{
    append_generated_row, enable_phase_tracing, flush_acknowledged_data_with, shutdown_engine,
    verify_database, write_atomic_json, CheckpointAccounting, WorkloadArtifacts, WorkloadCase,
    WRITE_BATCH_ROWS,
};
use cntryl_midge::{
    ColumnFamilyHandle, Engine, MemoryBudget, MidgeError, OpenOptions, TransactionMode,
    WriteOptions,
};
use cntryl_stress::{
    LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome, ProgressHandle,
    StressContext,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const PUBLICATION_FAILPOINT: &str = "midge::flush_worker::before_publication";
const CALLER_SLICE: Duration = Duration::from_millis(50);
// Keep real publication beyond the 50ms caller slice with enough headroom for
// healthy completion under the unchanged one-second native watchdog.
const PUBLICATION_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FlushFixtureKind {
    Delayed,
    Held,
    TerminalPolicy,
}

impl FlushFixtureKind {
    fn case(self) -> WorkloadCase {
        WorkloadCase {
            benchmark: match self {
                Self::Delayed => "delayed_final_flush_publication",
                Self::Held => "held_final_flush_publication",
                Self::TerminalPolicy => "terminal_final_flush_retry_policy",
            },
            scenario: if self == Self::TerminalPolicy {
                "watchdog-final-flush-policy"
            } else {
                "watchdog-final-flush"
            },
            backend: "cloud-simulated",
            tier: 5,
            workload: if self == Self::TerminalPolicy {
                "retry-policy-control"
            } else {
                "write-heavy"
            },
            stages: &[],
        }
    }
}

type PreparedFlush = (
    WorkloadArtifacts,
    Engine,
    ColumnFamilyHandle,
    Duration,
    FlushFixtureKind,
);
static PREPARED_FLUSH: super::watchdog_preparation::PreparedSlot<PreparedFlush> =
    super::watchdog_preparation::PreparedSlot::new();

struct PublicationPause {
    release: Arc<AtomicBool>,
}

impl Drop for PublicationPause {
    fn drop(&mut self) {
        self.release.store(true, Ordering::Release);
        fail::remove(PUBLICATION_FAILPOINT);
    }
}

fn open_fixture_engine(
    artifacts: &mut WorkloadArtifacts,
    setup_delay: Duration,
) -> (Engine, ColumnFamilyHandle, Duration) {
    let started = Instant::now();
    // Moving real engine setup under the watchdog must carry this delay too.
    thread::sleep(setup_delay);
    let database = artifacts.path.join("fixture-database");
    let options = OpenOptions::cloud_simulated(&database, "flush-fixture", "actual-publication/")
        .memory_budget(MemoryBudget::Bytes(128 * 1024 * 1024))
        .with_memtable_size_limit(4 * 1024 * 1024)
        .with_memtable_flush_threshold(2 * 1024 * 1024)
        .background_compaction(false)
        .build()
        .expect("build actual simulated-cloud fixture options");
    let engine = Engine::open(options).expect("open actual simulated-cloud fixture");
    let family = engine
        .create_column_family("workload")
        .expect("create actual fixture column family");
    let mut transaction = engine
        .begin_tx(family.id(), TransactionMode::ReadWrite)
        .expect("begin acknowledged fixture write");
    for row in 0..WRITE_BATCH_ROWS {
        append_generated_row(&mut transaction, 0, 0, 0, 0, row)
            .expect("append real deterministic fixture row");
    }
    transaction
        .commit(WriteOptions::cloud_async())
        .expect("acknowledge actual fixture transaction");
    artifacts.attempts = 1;
    artifacts.acknowledged = 1;
    artifacts.acknowledged_rows =
        u64::try_from(WRITE_BATCH_ROWS).expect("fixture row count fits u64");
    assert!(engine.is_primary_lease_healthy());
    (engine, family, started.elapsed())
}

#[allow(
    dead_code,
    reason = "Only the watchdog integration target calls this fixture"
)]
pub(crate) fn prepare_final_flush_watchdog_fixture(
    kind: FlushFixtureKind,
    samples: usize,
    setup_delay: Duration,
) -> impl super::PreparationGuard {
    enable_phase_tracing();
    let scope = PREPARED_FLUSH.install(Vec::new(), |(mut artifacts, mut engine, _, _, _)| {
        let shutdown = engine.shutdown(Duration::from_secs(5));
        artifacts.terminal_error = Some(format!(
            "prepared fixture was not invoked; shutdown={shutdown:?}"
        ));
        // Seal the cancellation receipt before the fallback Drop marks unfinished work failed.
        artifacts.complete = true;
        artifacts.persist("canceled");
    });
    for _ in 0..samples {
        let mut artifacts = WorkloadArtifacts::begin(kind.case(), Duration::from_secs(5));
        let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            open_fixture_engine(&mut artifacts, setup_delay)
        }));
        let (engine, family, setup_elapsed) = prepared.unwrap_or_else(|panic| {
            artifacts.terminal_error = Some("engine fixture preparation panicked".into());
            artifacts.persist("failed");
            std::panic::resume_unwind(panic);
        });
        write_atomic_json(
            &artifacts.path.join("fixture-preparation.json"),
            &json!({
                "setup_elapsed_ms": setup_elapsed.as_millis(),
                "kind": format!("{kind:?}"),
            }),
        )
        .expect("retain actual engine preparation time");
        scope.push((artifacts, engine, family, setup_elapsed, kind));
    }
    scope
}

#[allow(
    dead_code,
    reason = "Only the watchdog integration target calls this fixture"
)]
pub(crate) fn run_final_flush_watchdog_fixture(ctx: &mut StressContext, hold_publication: bool) {
    let (mut artifacts, mut engine, family, setup_elapsed, kind) = PREPARED_FLUSH.take();
    assert_eq!(
        kind,
        if hold_publication {
            FlushFixtureKind::Held
        } else {
            FlushFixtureKind::Delayed
        }
    );
    let progress = ctx.progress_handle();
    let scenario = fail::FailScenario::setup();
    let (pause, worker_entries) = pause_actual_publication(&artifacts, hold_publication);
    let started = Instant::now();
    write_atomic_json(
        &artifacts.path.join("active-work-start.json"),
        &json!({ "setup_elapsed_ms": setup_elapsed.as_millis(), "completed_units_before": progress.completed_units() }),
    )
    .expect("retain start of active flush work");
    let (attempts, flush_progress_units) =
        flush_fixture_data(&engine, &family, &progress, &mut artifacts);
    let metrics = engine
        .metrics()
        .get_runtime_metrics()
        .expect("observe actual completed publication");
    assert_eq!(metrics.flush_publish_count, 1);
    assert_eq!(metrics.sst_count, 1);
    assert_eq!(metrics.immutable_memtables, 0);
    // Disarm before any verification or orderly teardown. On panic the pause
    // also releases first, before Engine drop joins accepted workers.
    drop(pause);
    scenario.teardown();
    let expected = BTreeMap::from([((0, 0, 0), 1)]);
    verify_database(
        &engine,
        &family,
        0,
        &expected,
        &progress,
        &mut artifacts,
        "flushed",
    );
    let verified_rows = artifacts
        .checks
        .iter()
        .find(|check| check["phase"] == "flushed-workload")
        .expect("actual acknowledged-row verification")["actual_rows"]
        .as_u64()
        .expect("actual verified row count");
    let mismatches: u64 = artifacts
        .checks
        .iter()
        .map(|check| check["value_mismatches"].as_u64().expect("mismatch count"))
        .sum();
    write_atomic_json(
        &artifacts.path.join("flush-result.json"),
        &json!({
            "attempts": attempts,
            "flush_progress_units": flush_progress_units,
            "worker_entries": worker_entries.load(Ordering::Acquire),
            "flush_publish_count": metrics.flush_publish_count,
            "authoritative_sst_count": metrics.sst_count,
            "verified_rows": verified_rows,
            "mismatches": mismatches,
            "active_elapsed_ms": started.elapsed().as_millis(),
        }),
    )
    .expect("retain actual flush and verification result");
    ctx.metadata("fixture_flush_progress_units", flush_progress_units);
    ctx.record_external_outcome(
        "actual final flush publication",
        started.elapsed(),
        LogicalUnit::new("flush"),
        OperationOutcome::success(1),
    );
    let checkpoint_accounting =
        CheckpointAccounting::attach(&engine, &artifacts, "final-flush-fixture", None);
    shutdown_engine(
        &mut engine,
        "shutdown-after-final-flush-fixture",
        &checkpoint_accounting,
        &progress,
        &mut artifacts,
    );
    artifacts.finish(&progress);
}

fn pause_actual_publication(
    artifacts: &WorkloadArtifacts,
    hold_publication: bool,
) -> (PublicationPause, Arc<AtomicU64>) {
    let publication_observations = artifacts.path.join("publication-observations.json");
    write_atomic_json(
        &publication_observations,
        &json!({ "worker_entries": 0, "worker_released": false }),
    )
    .expect("retain publication setup");
    let worker_entries = Arc::new(AtomicU64::new(0));
    let observed_entries = Arc::clone(&worker_entries);
    let release = Arc::new(AtomicBool::new(false));
    let worker_release = Arc::clone(&release);
    let origin = artifacts.started;
    // This callback is process-global only inside a dedicated child with one
    // Engine. No test gate can block the worker's read-side failpoint guard.
    // Its only effect is to hold real accepted publication before execution.
    fail::cfg_callback(PUBLICATION_FAILPOINT, move || {
        let publication_started = Instant::now();
        let entries = observed_entries.fetch_add(1, Ordering::AcqRel) + 1;
        write_atomic_json(
            &publication_observations,
            &json!({
                "worker_entries": entries,
                "worker_released": false,
                "entered_elapsed_ms": origin.elapsed().as_millis(),
            }),
        )
        .expect("retain actual publication worker entry");
        if hold_publication {
            while !worker_release.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(20));
            }
        } else {
            thread::sleep(PUBLICATION_DELAY);
        }
        write_atomic_json(
            &publication_observations,
            &json!({
                "worker_entries": entries,
                "worker_released": true,
                "released_elapsed_ms": origin.elapsed().as_millis(),
                "publication_elapsed_ms": publication_started.elapsed().as_millis(),
            }),
        )
        .expect("retain actual publication release");
    })
    .expect("pause actual publication in the isolated child");
    (PublicationPause { release }, worker_entries)
}

fn flush_fixture_data(
    engine: &Engine,
    family: &ColumnFamilyHandle,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
) -> (usize, u64) {
    let before = progress.completed_units();
    let mut attempts = Vec::new();
    let attempt_report = artifacts.path.join("flush-attempt-observations.json");
    flush_acknowledged_data_with(
        engine,
        family,
        true,
        Some(CALLER_SLICE),
        progress,
        artifacts,
        |engine, family| {
            let attempt_started = Instant::now();
            let result = cntryl_midge::__internal::maintenance::flush_cf_with_timeout(
                engine,
                family,
                CALLER_SLICE,
            );
            let error_kind = result.as_ref().err().map(|error| match error {
                cntryl_midge::MidgeError::Timeout(_) => "timeout",
                cntryl_midge::MidgeError::Fenced(_) => "fenced",
                _ => "error",
            });
            attempts.push(json!({
                "elapsed_ms": attempt_started.elapsed().as_millis(),
                "caller_budget_ms": CALLER_SLICE.as_millis(),
                "success": result.is_ok(),
                "error_kind": error_kind,
                "error": result.as_ref().err().map(|error| format!("{error:?}")),
                "observed_lease_healthy": engine.is_primary_lease_healthy(),
                "progress_units": progress.completed_units() - before,
            }));
            write_atomic_json(&attempt_report, &json!({ "attempts": attempts }))
                .expect("retain actual short caller outcomes");
            result
        },
    )
    .expect("actual final flush must finish before verification");
    (attempts.len(), progress.completed_units() - before)
}

#[allow(
    dead_code,
    reason = "Only the watchdog integration target calls this policy fixture"
)]
pub(crate) fn run_final_flush_terminal_policy_fixture(ctx: &mut StressContext) {
    let (mut artifacts, mut engine, family, setup_elapsed, kind) = PREPARED_FLUSH.take();
    assert_eq!(kind, FlushFixtureKind::TerminalPolicy);
    let progress = ctx.progress_handle();
    ctx.record_observation(
        "fixture_setup_elapsed_ns",
        setup_elapsed.as_secs_f64() * 1e9,
        ObservationUnit::Nanoseconds,
        ObservationDirection::Informational,
    );
    let started = Instant::now();
    let mut cases = Vec::new();
    for (kind, error) in terminal_policy_errors() {
        cases.push(assert_terminal_policy_case(
            &engine,
            &family,
            &progress,
            &mut artifacts,
            kind,
            error,
        ));
    }
    write_atomic_json(
        &artifacts.path.join("retry-policy-results.json"),
        &json!({
            "fixture_scope": "benchmark_retry_policy",
            "error_source": "callback_injected",
            "provider_error_behavior_proved": false,
            "cases": cases,
            "verification_checks": artifacts.checks.len(),
        }),
    )
    .expect("retain explicitly scoped policy results");
    ctx.metadata("fixture_scope", "benchmark_retry_policy");
    ctx.metadata("error_source", "callback_injected");
    ctx.record_external_outcome(
        "terminal final-flush retry-policy controls",
        started.elapsed(),
        LogicalUnit::new("policy_case"),
        OperationOutcome::success(u64::try_from(cases.len()).expect("policy case count fits u64")),
    );
    // All terminal results above were expected policy outcomes, preserved in
    // the control report. Orderly cleanup now uses the real Engine shutdown.
    artifacts.terminal_error = None;
    let checkpoint_accounting =
        CheckpointAccounting::attach(&engine, &artifacts, "retry-policy-fixture", None);
    shutdown_engine(
        &mut engine,
        "shutdown-after-retry-policy-controls",
        &checkpoint_accounting,
        &progress,
        &mut artifacts,
    );
    artifacts.enter_phase("complete", "terminal-retry-policy-controls", &progress);
    artifacts.complete = true;
    artifacts.persist("passed");
}

fn terminal_policy_errors() -> [(&'static str, MidgeError); 8] {
    // These errors are injected at the benchmark callback boundary. They prove
    // retry policy on a healthy actual Engine, not provider error behavior.
    [
        (
            "resource_limit",
            MidgeError::ResourceLimit("policy resource limit".into()),
        ),
        ("fenced", MidgeError::Fenced("policy fence".into())),
        (
            "corruption",
            MidgeError::Corruption("policy corruption".into()),
        ),
        (
            "internal",
            MidgeError::Internal("policy internal defect".into()),
        ),
        (
            "io",
            MidgeError::Io(std::io::Error::other("policy I/O failure")),
        ),
        ("no_space", MidgeError::NoSpace("policy no space".into())),
        (
            "lease_unavailable",
            MidgeError::LeaseUnavailable("policy unavailable lease".into()),
        ),
        ("aborted", MidgeError::Aborted("policy abort".into())),
    ]
}

fn assert_terminal_policy_case(
    engine: &Engine,
    family: &ColumnFamilyHandle,
    progress: &ProgressHandle,
    artifacts: &mut WorkloadArtifacts,
    kind: &str,
    error: MidgeError,
) -> serde_json::Value {
    assert!(engine.is_primary_lease_healthy());
    let expected_variant = std::mem::discriminant(&error);
    let expected_error = format!("{error:?}");
    let mut injected_error = Some(error);
    let before = progress.completed_units();
    let previous_attempts = artifacts.final_flush.attempts;
    let mut calls = 0;
    let result = flush_acknowledged_data_with(
        engine,
        family,
        true,
        Some(CALLER_SLICE),
        progress,
        artifacts,
        |engine, family| {
            calls += 1;
            if let Some(error) = injected_error.take() {
                return Err(error);
            }
            // A policy regression reaches a genuine flush, so it fails
            // visibly; no fabricated Ok can turn the callback into work.
            engine.flush_cf(family)
        },
    );
    let returned = result.as_ref().expect_err("terminal error must propagate");
    assert_eq!(std::mem::discriminant(returned), expected_variant);
    assert_eq!(format!("{returned:?}"), expected_error);
    assert_eq!(calls, 1);
    assert_eq!(progress.completed_units() - before, 0);
    assert_eq!(artifacts.final_flush.attempts - previous_attempts, 1);
    assert!(!artifacts.final_flush.completed);
    assert!(!artifacts.final_flush.operation_in_flight);
    assert_eq!(artifacts.final_flush.backoff_ms, 0);
    assert!(engine.is_primary_lease_healthy());
    assert!(!artifacts.path.join("verification-summary.json").exists());
    let report = fs::read_to_string(artifacts.path.join("final-flush-attempts.jsonl"))
        .expect("persisted actual retry-policy outcomes");
    let persisted: serde_json::Value = serde_json::from_str(
        report
            .lines()
            .last()
            .expect("one terminal result was appended"),
    )
    .expect("complete policy result JSON");
    assert_eq!(persisted["retry"], false);
    assert_eq!(persisted["success"], false);
    assert_eq!(persisted["error"], expected_error);
    assert_eq!(persisted["lease_healthy_before"], true);
    assert_eq!(persisted["lease_healthy_after"], true);
    json!({
        "kind": kind,
        "error_source": "callback_injected",
        "calls": calls,
        "original_error": expected_error,
        "returned_error": format!("{returned:?}"),
        "progress_units": progress.completed_units() - before,
        "completed": artifacts.final_flush.completed,
        "operation_in_flight": artifacts.final_flush.operation_in_flight,
        "observed_primary_lease_healthy": engine.is_primary_lease_healthy(),
        "persisted": persisted,
    })
}
