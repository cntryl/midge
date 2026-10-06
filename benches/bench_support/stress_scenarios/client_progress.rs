//! Client-local liveness, failure results and durable partial statistics.
use super::{ClientConfig, OperationResult, SaturationBackoff, StageStats};
use cntryl_midge::MidgeError;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub(super) const CLIENT_REPORT_PERIOD: Duration = Duration::from_secs(1);

#[derive(Default)]
pub(super) struct WorkerResult {
    pub(super) stats: StageStats,
    pub(super) kind_counts: [u64; 11],
    pub(super) account_updates: u64,
    pub(super) terminal_error: Option<MidgeError>,
    pub(super) observed_lease_healthy: Option<bool>,
    pub(super) last_success_elapsed_ms: Option<u128>,
    pub(super) stopped_by_peer: bool,
}

pub(super) struct ClientClock<N, S> {
    pub(super) now: N,
    pub(super) sleep: S,
}

pub(super) struct ClientControl<'a> {
    pub(super) stop: &'a AtomicBool,
    pub(super) lease_health: Option<&'a dyn Fn() -> bool>,
    pub(super) origin: Instant,
}

#[derive(Clone, Copy)]
pub(super) struct ClientReport {
    pub(super) elapsed_ms: u128,
    pub(super) operation_in_flight: bool,
    pub(super) force: bool,
}

pub(super) struct ClientReporter {
    path: PathBuf,
    config: ClientConfig,
    stage: String,
    origin: Instant,
    interval_ms: u128,
    next_due_ms: u128,
}

impl ClientReporter {
    pub(super) fn new(
        directory: &Path,
        config: ClientConfig,
        origin: Instant,
        interval: Duration,
        stage: &str,
    ) -> Self {
        Self {
            path: directory.join(format!("client-{:02}.json", config.client)),
            config,
            stage: stage.to_string(),
            origin,
            interval_ms: interval.as_millis().max(1),
            next_due_ms: 0,
        }
    }

    pub(super) fn origin(&self) -> Instant {
        self.origin
    }

    pub(super) fn report(&mut self, result: &WorkerResult, report: ClientReport) -> bool {
        if !report.force && report.elapsed_ms < self.next_due_ms {
            return false;
        }
        write_atomic_json(&self.path, &self.snapshot(result, report))
            .expect("publish atomic client snapshot");
        self.next_due_ms = report.elapsed_ms.saturating_add(self.interval_ms);
        true
    }

    fn snapshot(&self, result: &WorkerResult, report: ClientReport) -> Value {
        json!({
            "phase": "workload",
            "stage": self.stage,
            "stage_index": self.config.stage,
            "client_index": self.config.client,
            "process_id": std::process::id(),
            "workload": self.config.workload,
            "attempted_transactions": result.stats.attempts,
            "acknowledged_transactions": result.stats.acknowledged,
            "acknowledged_rows": result.stats.acknowledged_rows,
            "logical_operations": result.stats.logical_operations,
            "resource_limit_responses": result.stats.saturation.resource_limit,
            "write_stall_responses": result.stats.saturation.write_stall,
            "saturation_backoff_ms": result.stats.saturation.backoff_ms,
            "latency": result.stats.latency.summary(&result.stats.latency_us),
            "elapsed_ms": report.elapsed_ms,
            "last_success_elapsed_ms": result.last_success_elapsed_ms,
            "terminal_error": result.terminal_error.as_ref().map(ToString::to_string),
            "observed_lease_healthy": result.observed_lease_healthy,
            "operation_in_flight": report.operation_in_flight,
            "stopped_by_peer": result.stopped_by_peer,
        })
    }
}

pub(super) fn write_atomic_json(path: &Path, value: &Value) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?;
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes)?;
    fs::rename(temporary, path)
}

enum ClientAttempt {
    Success,
    Retry { error: MidgeError, backoff_ms: u64 },
    Terminal,
}

impl WorkerResult {
    fn record_operation(
        &mut self,
        outcome: OperationResult,
        latency: Duration,
        elapsed_ms: u128,
        backoff: &mut SaturationBackoff,
    ) -> ClientAttempt {
        self.stats.record_latency(latency);
        match outcome {
            Ok((kind, operations, rows, account_updated)) => {
                self.stats.acknowledged = self.stats.acknowledged.saturating_add(1);
                self.stats.logical_operations =
                    self.stats.logical_operations.saturating_add(operations);
                self.stats.acknowledged_rows = self.stats.acknowledged_rows.saturating_add(rows);
                if let Some(kind) = kind {
                    self.kind_counts[kind] = self.kind_counts[kind].saturating_add(1);
                }
                self.account_updates = self
                    .account_updates
                    .saturating_add(u64::from(account_updated));
                self.last_success_elapsed_ms = Some(elapsed_ms);
                backoff.on_success();
                ClientAttempt::Success
            }
            Err(error @ MidgeError::ResourceLimit(_)) => {
                self.stats.saturation.resource_limit =
                    self.stats.saturation.resource_limit.saturating_add(1);
                ClientAttempt::Retry {
                    error,
                    backoff_ms: backoff.on_saturation(),
                }
            }
            Err(error @ MidgeError::WriteStall(_)) => {
                self.stats.saturation.write_stall =
                    self.stats.saturation.write_stall.saturating_add(1);
                ClientAttempt::Retry {
                    error,
                    backoff_ms: backoff.on_saturation(),
                }
            }
            Err(error) => {
                self.terminal_error = Some(error);
                ClientAttempt::Terminal
            }
        }
    }

    fn retry_lease_healthy(
        &mut self,
        error: &MidgeError,
        workload: &str,
        control: &ClientControl<'_>,
    ) -> bool {
        let Some(check_health) = control.lease_health else {
            return true;
        };
        let healthy = check_health();
        self.observed_lease_healthy = Some(healthy);
        if !healthy {
            self.terminal_error = Some(MidgeError::Fenced(format!(
                "{workload} lost primary lease while receiving {error}"
            )));
        }
        healthy
    }
}

pub(super) fn run_client_with<N, S>(
    config: ClientConfig,
    budget: Duration,
    control: &ClientControl<'_>,
    mut clock: ClientClock<N, S>,
    mut operation: impl FnMut(u64, &mut [u64; 11], &mut u64) -> OperationResult,
    mut advance: impl FnMut(),
    mut report: impl FnMut(&WorkerResult, ClientReport),
) -> WorkerResult
where
    N: FnMut() -> Instant,
    S: FnMut(Duration),
{
    let loop_started = (clock.now)();
    let deadline = loop_started + budget;
    let mut last_ack = loop_started;
    let mut result = WorkerResult::default();
    let mut sequences = [0_u64; 11];
    let mut account = 0_u64;
    let mut attempt = 0_u64;
    let mut backoff = SaturationBackoff::default();
    while !control.stop.load(Ordering::Acquire) && (clock.now)() < deadline {
        let report_at = (clock.now)();
        result.stats.attempts = result.stats.attempts.saturating_add(1);
        report(
            &result,
            ClientReport {
                elapsed_ms: report_at
                    .saturating_duration_since(control.origin)
                    .as_millis(),
                operation_in_flight: true,
                force: attempt == 0,
            },
        );
        let started = (clock.now)();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            operation(attempt, &mut sequences, &mut account)
        }))
        .unwrap_or_else(|payload| Err(operation_panic_error(payload.as_ref())));
        let ended = (clock.now)();
        match result.record_operation(
            outcome,
            ended.saturating_duration_since(started),
            ended.saturating_duration_since(control.origin).as_millis(),
            &mut backoff,
        ) {
            ClientAttempt::Success => {
                result.stats.latency.successful(
                    ended.saturating_duration_since(started),
                    ended.saturating_duration_since(last_ack),
                );
                last_ack = ended;
                advance();
            }
            ClientAttempt::Retry { error, backoff_ms } => {
                if !result.retry_lease_healthy(&error, config.workload, control) {
                    control.stop.store(true, Ordering::Release);
                    break;
                }
                result.stats.saturation.backoff_ms = result
                    .stats
                    .saturation
                    .backoff_ms
                    .saturating_add(backoff_ms);
                let sleep_started = (clock.now)();
                (clock.sleep)(Duration::from_millis(backoff_ms));
                result
                    .stats
                    .latency
                    .slept((clock.now)().saturating_duration_since(sleep_started));
            }
            ClientAttempt::Terminal => {
                control.stop.store(true, Ordering::Release);
                break;
            }
        }
        attempt = attempt.saturating_add(1);
    }
    result.stopped_by_peer =
        control.stop.load(Ordering::Acquire) && result.terminal_error.is_none();
    result
        .stats
        .latency
        .censor((clock.now)().saturating_duration_since(last_ack));
    report(
        &result,
        ClientReport {
            elapsed_ms: (clock.now)()
                .saturating_duration_since(control.origin)
                .as_millis(),
            operation_in_flight: false,
            force: true,
        },
    );
    result
}

fn operation_panic_error(payload: &(dyn std::any::Any + Send)) -> MidgeError {
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload");
    MidgeError::Internal(format!("client operation panicked: {message}"))
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Harness-free benches also compile cfg(test) helpers"
)]
mod progress_tests {
    use super::*;

    fn write_config() -> ClientConfig {
        ClientConfig {
            workload: "write-heavy",
            stage: 0,
            client: 0,
            seed_count: 0,
            cloud: false,
        }
    }

    #[test]
    fn should_measure_retry_sleep_and_inter_ack_delay_when_attempts_are_rejected() {
        use std::cell::Cell;
        // Arrange
        let origin = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let stop = AtomicBool::new(false);
        let directory = tempfile::tempdir().unwrap();
        let reporter = ClientReporter::new(
            directory.path(),
            write_config(),
            origin,
            Duration::from_secs(1),
            "scripted",
        );
        // Act: two rejected 100us calls, requested sleeps of 1ms and 2ms,
        // each overshooting by 500us, then one successful 100us call.
        let result = run_client_with(
            write_config(),
            Duration::from_secs(1),
            &ClientControl {
                stop: &stop,
                lease_health: None,
                origin,
            },
            ClientClock {
                now: || origin + elapsed.get(),
                sleep: |duration| {
                    elapsed.set(elapsed.get() + duration + Duration::from_micros(500));
                },
            },
            |attempt, _, _| {
                elapsed.set(elapsed.get() + Duration::from_micros(100));
                match attempt {
                    0 => Err(MidgeError::WriteStall("scripted".into())),
                    1 => Err(MidgeError::ResourceLimit("scripted".into())),
                    _ => {
                        stop.store(true, Ordering::Release);
                        Ok((Some(0), 1, 1, false))
                    }
                }
            },
            || {},
            |_, report| {
                if report.operation_in_flight {
                    elapsed.set(elapsed.get() + Duration::from_micros(10));
                }
            },
        );
        let snapshot = reporter.snapshot(
            &result,
            ClientReport {
                elapsed_ms: elapsed.get().as_millis(),
                operation_in_flight: false,
                force: true,
            },
        );
        // Assert: first inter-ACK sample starts at this client's loop start,
        // includes all reports/rejections/actual sleep; no final report time.
        assert_eq!(snapshot["latency"]["attempt_samples"], 3);
        assert_eq!(snapshot["latency"]["successful_call_samples"], 1);
        assert_eq!(snapshot["latency"]["inter_ack_samples"], 1);
        assert_eq!(snapshot["latency"]["successful_call_p99_us"], 100);
        assert!((4_330..4_340).contains(&snapshot["latency"]["inter_ack_p99_us"].as_u64().unwrap()));
        assert_eq!(snapshot["latency"]["actual_sleep_ns"], 4_000_000);
        assert_eq!(snapshot["latency"]["censored_inter_ack_samples"], 0);
        assert_eq!(result.stats.acknowledged, 1);
        assert_eq!(result.stats.saturation.backoff_ms, 3);
    }

    #[test]
    fn should_censor_pending_interval_when_backoff_expires_without_an_ack() {
        use std::cell::Cell;
        // Arrange
        let origin = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let stop = AtomicBool::new(false);
        let directory = tempfile::tempdir().unwrap();
        let reporter = ClientReporter::new(
            directory.path(),
            write_config(),
            origin,
            Duration::from_secs(1),
            "scripted",
        );
        // Act
        let result = run_client_with(
            write_config(),
            Duration::from_micros(500),
            &ClientControl {
                stop: &stop,
                lease_health: None,
                origin,
            },
            ClientClock {
                now: || origin + elapsed.get(),
                sleep: |duration| elapsed.set(elapsed.get() + duration),
            },
            |_, _, _| {
                elapsed.set(elapsed.get() + Duration::from_micros(100));
                Err(MidgeError::WriteStall("scripted".into()))
            },
            || panic!("rejection cannot advance progress"),
            |_, _| {},
        );
        let snapshot = reporter.snapshot(
            &result,
            ClientReport {
                elapsed_ms: 1,
                operation_in_flight: false,
                force: true,
            },
        );
        // Assert
        assert_eq!(snapshot["latency"]["successful_call_samples"], 0);
        assert_eq!(snapshot["latency"]["inter_ack_samples"], 0);
        assert_eq!(snapshot["latency"]["censored_inter_ack_samples"], 1);
        assert_eq!(snapshot["latency"]["censored_inter_ack_ns"], 1_100_000);
    }

    #[test]
    fn should_exclude_snapshot_publication_from_transaction_latency() {
        use std::cell::Cell;

        // Arrange
        let started = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let stop = AtomicBool::new(false);

        // Act: the actual loop spends three seconds publishing its initial
        // snapshot, followed by a one-second terminal database operation.
        let result = run_client_with(
            write_config(),
            Duration::from_secs(5),
            &ClientControl {
                stop: &stop,
                lease_health: None,
                origin: started,
            },
            ClientClock {
                now: || started + elapsed.get(),
                sleep: |_duration| panic!("terminal operation cannot retry"),
            },
            |_attempt, _sequences, _account| {
                elapsed.set(elapsed.get() + Duration::from_secs(1));
                Err(MidgeError::Fenced("scripted terminal failure".into()))
            },
            || panic!("no successful operation"),
            |_result, report| {
                if report.operation_in_flight {
                    elapsed.set(elapsed.get() + Duration::from_secs(3));
                }
            },
        );

        // Assert: the overall stage pays reporting overhead, the transaction
        // histogram measures only the database operation (within HDR precision).
        assert_eq!(elapsed.get(), Duration::from_secs(4));
        assert!((1_000_000..1_001_000).contains(&result.stats.latency_us.value_at_quantile(0.99)));
    }

    #[test]
    fn should_preserve_partial_result_and_stop_peers_when_operation_panics() {
        use std::cell::Cell;

        // Arrange
        let origin = Instant::now();
        let stop = AtomicBool::new(false);
        let final_reported = Cell::new(false);
        let control = ClientControl {
            stop: &stop,
            lease_health: None,
            origin,
        };

        // Act
        let result = run_client_with(
            write_config(),
            Duration::from_mins(1),
            &control,
            ClientClock {
                now: Instant::now,
                sleep: |_duration| {},
            },
            |_attempt, _sequences, _account| panic!("scripted account invariant"),
            || panic!("a panic cannot acknowledge an operation"),
            |result, report| {
                if report.force && !report.operation_in_flight {
                    final_reported.set(result.terminal_error.is_some());
                }
            },
        );
        let peer = run_client_with(
            write_config(),
            Duration::from_mins(1),
            &control,
            ClientClock {
                now: Instant::now,
                sleep: |_duration| {},
            },
            |_attempt, _sequences, _account| panic!("stopped peer must not enter operation"),
            || panic!("stopped peer cannot acknowledge an operation"),
            |_result, _report| {},
        );

        // Assert
        assert_eq!(result.stats.attempts, 1);
        assert_eq!(result.stats.acknowledged, 0);
        assert_eq!(result.stats.latency.successful_us.len(), 0);
        assert_eq!(result.stats.latency.inter_ack_us.len(), 0);
        assert!(result
            .terminal_error
            .as_ref()
            .unwrap()
            .to_string()
            .contains("scripted account invariant"));
        assert!(final_reported.get());
        assert!(stop.load(Ordering::Acquire));
        assert_eq!(peer.stats.attempts, 0);
        assert!(peer.stopped_by_peer);
    }

    #[test]
    fn should_count_only_successful_worker_operations_as_progress_after_retries() {
        use std::cell::Cell;
        use std::collections::VecDeque;

        // Arrange
        let started = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let progress_units = Cell::new(0_u64);
        let mut outcomes: VecDeque<OperationResult> = VecDeque::from([
            Err(MidgeError::WriteStall("held pressure".into())),
            Err(MidgeError::ResourceLimit("held budget".into())),
            Ok((Some(0), 32, 32, false)),
            Err(MidgeError::WriteStall("temporary pressure".into())),
            Ok((Some(0), 32, 32, false)),
        ]);

        let stop = AtomicBool::new(false);

        // Act: the same loop used by real clients runs a scripted workload;
        // backoff and operation time advance its injected monotonic clock.
        let result = run_client_with(
            write_config(),
            Duration::from_secs(5),
            &ClientControl {
                stop: &stop,
                lease_health: None,
                origin: started,
            },
            ClientClock {
                now: || started + elapsed.get(),
                sleep: |duration| elapsed.set(elapsed.get() + duration),
            },
            |_attempt, _sequences, _account| {
                elapsed.set(elapsed.get() + Duration::from_secs(1));
                outcomes.pop_front().expect("scripted operation")
            },
            || progress_units.set(progress_units.get() + 1),
            |_result, _report| {},
        );

        // Assert
        assert!(outcomes.is_empty());
        assert_eq!(result.stats.attempts, 5);
        assert_eq!(result.stats.acknowledged, 2);
        assert_eq!(result.stats.acknowledged_rows, 64);
        assert_eq!(result.stats.logical_operations, 64);
        assert_eq!(result.kind_counts[0], 2);
        assert_eq!(result.stats.saturation.write_stall, 2);
        assert_eq!(result.stats.saturation.resource_limit, 1);
        assert_eq!(result.stats.saturation.backoff_ms, 7);
        assert_eq!(
            progress_units.get(),
            2,
            "only successful operations reset liveness"
        );
    }

    #[test]
    fn should_leave_success_heartbeat_unchanged_when_rejections_continue_past_a_minute() {
        use std::cell::Cell;

        // Arrange
        let started = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let progress_units = Cell::new(0_u64);

        let stop = AtomicBool::new(false);

        // Act
        let result = run_client_with(
            write_config(),
            Duration::from_secs(61),
            &ClientControl {
                stop: &stop,
                lease_health: None,
                origin: started,
            },
            ClientClock {
                now: || started + elapsed.get(),
                sleep: |duration| elapsed.set(elapsed.get() + duration),
            },
            |attempt, _sequences, _account| {
                elapsed.set(elapsed.get() + Duration::from_secs(1));
                if attempt.is_multiple_of(2) {
                    Err(MidgeError::WriteStall("held pressure".into()))
                } else {
                    Err(MidgeError::ResourceLimit("held budget".into()))
                }
            },
            || progress_units.set(progress_units.get() + 1),
            |_result, _report| {},
        );

        // Assert: saturation remains diagnostic data even after the existing
        // external watchdog's sixty-second successful-work limit would expire.
        assert!(elapsed.get() >= Duration::from_secs(61));
        assert!(result.stats.attempts > 0);
        assert_eq!(
            result.stats.attempts,
            result.stats.saturation.write_stall + result.stats.saturation.resource_limit
        );
        assert!(result.stats.saturation.write_stall > 0);
        assert!(result.stats.saturation.resource_limit > 0);
        assert!(result.stats.saturation.backoff_ms > 0);
        assert_eq!(result.stats.acknowledged, 0);
        assert_eq!(result.stats.acknowledged_rows, 0);
        assert_eq!(result.stats.logical_operations, 0);
        assert_eq!(
            progress_units.get(),
            0,
            "rejected attempts cannot reset liveness"
        );
    }

    #[test]
    fn should_publish_initial_periodic_and_fatal_snapshots_with_partial_counts() {
        use std::cell::Cell;
        use std::collections::VecDeque;

        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let started = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let stop = AtomicBool::new(false);
        let progress = Cell::new(0_u64);
        let mut reporter = ClientReporter::new(
            directory.path(),
            write_config(),
            started,
            Duration::from_secs(1),
            "1-clients",
        );
        let mut snapshots = Vec::new();
        let mut outcomes = VecDeque::from([
            Ok((Some(0), 32, 32, false)),
            Err(MidgeError::ResourceLimit("temporary pressure".into())),
            Err(MidgeError::Internal("fatal fixture".into())),
        ]);

        // Act
        let result = run_client_with(
            write_config(),
            Duration::from_secs(4),
            &ClientControl {
                stop: &stop,
                lease_health: None,
                origin: started,
            },
            ClientClock {
                now: || started + elapsed.get(),
                sleep: |duration| elapsed.set(elapsed.get() + duration),
            },
            |_attempt, _sequences, _account| {
                elapsed.set(elapsed.get() + Duration::from_secs(1));
                outcomes.pop_front().expect("scripted result")
            },
            || progress.set(progress.get() + 1),
            |result, report| {
                if reporter.report(result, report) {
                    snapshots.push(
                        serde_json::from_slice::<Value>(
                            &fs::read(directory.path().join("client-00.json")).unwrap(),
                        )
                        .unwrap(),
                    );
                }
            },
        );

        // Assert
        assert_eq!(snapshots.len(), 4);
        assert_eq!(snapshots[0]["attempted_transactions"], 1);
        assert_eq!(snapshots[0]["acknowledged_transactions"], 0);
        assert_eq!(snapshots[0]["operation_in_flight"], true);
        assert_eq!(snapshots[1]["acknowledged_transactions"], 1);
        assert_eq!(snapshots[1]["last_success_elapsed_ms"], 1_000);
        let final_snapshot = snapshots.last().unwrap();
        assert_eq!(final_snapshot["attempted_transactions"], 3);
        assert_eq!(final_snapshot["acknowledged_rows"], 32);
        assert_eq!(final_snapshot["latency"]["attempt_samples"], 3);
        assert_eq!(final_snapshot["latency"]["successful_call_samples"], 1);
        assert_eq!(final_snapshot["latency"]["inter_ack_samples"], 1);
        assert_eq!(final_snapshot["latency"]["censored_inter_ack_samples"], 1);
        assert_eq!(
            final_snapshot["latency"]["censored_inter_ack_ns"],
            2_001_000_000_u64
        );
        assert_eq!(final_snapshot["resource_limit_responses"], 1);
        assert_eq!(final_snapshot["saturation_backoff_ms"], 1);
        assert_eq!(final_snapshot["operation_in_flight"], false);
        assert!(final_snapshot["terminal_error"]
            .as_str()
            .unwrap()
            .contains("fatal fixture"));
        assert!(matches!(
            result.terminal_error,
            Some(MidgeError::Internal(_))
        ));
        assert_eq!(result.stats.attempts, 3);
        assert_eq!(result.stats.acknowledged, 1);
        assert_eq!(result.stats.latency_us.len(), 3);
        assert_eq!(progress.get(), 1);
        assert!(stop.load(Ordering::Acquire));
        assert!(!directory.path().join("client-00.tmp").exists());
    }

    #[test]
    fn should_preserve_saturation_before_fencing_a_client_with_unhealthy_lease() {
        use std::cell::Cell;

        // Arrange
        let started = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let stop = AtomicBool::new(false);
        let health_checks = Cell::new(0);
        let check_health = || {
            health_checks.set(health_checks.get() + 1);
            false
        };
        let mut config = write_config();
        config.cloud = true;

        // Act
        let result = run_client_with(
            config,
            Duration::from_secs(10),
            &ClientControl {
                stop: &stop,
                lease_health: Some(&check_health),
                origin: started,
            },
            ClientClock {
                now: || started + elapsed.get(),
                sleep: |_duration| panic!("terminal retry cannot consume backoff"),
            },
            |_attempt, _sequences, _account| {
                elapsed.set(elapsed.get() + Duration::from_secs(1));
                Err(MidgeError::ResourceLimit("held resource allowance".into()))
            },
            || panic!("a fenced retry cannot advance successful progress"),
            |_result, _report| {},
        );

        // Assert
        assert_eq!(result.stats.attempts, 1);
        assert_eq!(result.stats.saturation.resource_limit, 1);
        assert_eq!(result.stats.saturation.backoff_ms, 0);
        assert_eq!(result.stats.latency_us.len(), 1);
        assert_eq!(result.observed_lease_healthy, Some(false));
        assert_eq!(health_checks.get(), 1);
        assert!(matches!(result.terminal_error, Some(MidgeError::Fenced(_))));
        assert!(stop.load(Ordering::Acquire));
    }

    #[test]
    fn should_keep_lease_health_checks_off_successful_client_operations() {
        use std::cell::Cell;

        // Arrange
        let started = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let stop = AtomicBool::new(false);
        let progress = Cell::new(0);
        let check_health = || panic!("successful operation cannot lock lease health");

        // Act
        let result = run_client_with(
            write_config(),
            Duration::from_secs(2),
            &ClientControl {
                stop: &stop,
                lease_health: Some(&check_health),
                origin: started,
            },
            ClientClock {
                now: || started + elapsed.get(),
                sleep: |_duration| panic!("successful operation cannot back off"),
            },
            |_attempt, _sequences, _account| {
                elapsed.set(elapsed.get() + Duration::from_secs(1));
                Ok((Some(0), 32, 32, false))
            },
            || progress.set(progress.get() + 1),
            |_result, _report| {},
        );

        // Assert
        assert_eq!(result.stats.attempts, 2);
        assert_eq!(result.stats.acknowledged, 2);
        assert_eq!(progress.get(), 2);
        assert_eq!(result.last_success_elapsed_ms, Some(2_000));
        assert_eq!(result.observed_lease_healthy, None);
        assert!(result.terminal_error.is_none());
        assert!(!stop.load(Ordering::Acquire));
    }

    #[test]
    fn should_publish_zero_attempts_when_peer_failure_stops_a_client_before_admission() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let started = Instant::now();
        let stop = AtomicBool::new(true);
        let mut reporter = ClientReporter::new(
            directory.path(),
            write_config(),
            started,
            Duration::from_secs(1),
            "1-clients",
        );

        // Act
        let result = run_client_with(
            write_config(),
            Duration::from_secs(2),
            &ClientControl {
                stop: &stop,
                lease_health: None,
                origin: started,
            },
            ClientClock {
                now: Instant::now,
                sleep: |_duration| {},
            },
            |_attempt, _sequences, _account| panic!("peer failure must stop admission"),
            || panic!("no operation acknowledged"),
            |result, report| {
                reporter.report(result, report);
            },
        );
        let snapshot: Value =
            serde_json::from_slice(&fs::read(directory.path().join("client-00.json")).unwrap())
                .unwrap();

        // Assert
        assert_eq!(result.stats.attempts, 0);
        assert!(result.stopped_by_peer);
        assert!(result.terminal_error.is_none());
        assert_eq!(snapshot["attempted_transactions"], 0);
        assert_eq!(snapshot["stopped_by_peer"], true);
        assert_eq!(snapshot["operation_in_flight"], false);
    }

    #[test]
    fn should_write_only_initial_and_final_snapshots_before_report_cadence_is_due() {
        use std::cell::Cell;

        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let started = Instant::now();
        let elapsed = Cell::new(Duration::ZERO);
        let stop = AtomicBool::new(false);
        let writes = Cell::new(0);
        let mut reporter = ClientReporter::new(
            directory.path(),
            write_config(),
            started,
            Duration::from_secs(1),
            "1-clients",
        );

        // Act
        let result = run_client_with(
            write_config(),
            Duration::from_secs(1),
            &ClientControl {
                stop: &stop,
                lease_health: None,
                origin: started,
            },
            ClientClock {
                now: || started + elapsed.get(),
                sleep: |_duration| panic!("successful operation cannot back off"),
            },
            |_attempt, _sequences, _account| {
                elapsed.set(elapsed.get() + Duration::from_millis(50));
                Ok((Some(0), 32, 32, false))
            },
            || {},
            |result, report| {
                if reporter.report(result, report) {
                    writes.set(writes.get() + 1);
                }
            },
        );
        let snapshot: Value =
            serde_json::from_slice(&fs::read(directory.path().join("client-00.json")).unwrap())
                .unwrap();

        // Assert
        assert_eq!(result.stats.attempts, 20);
        assert_eq!(writes.get(), 2);
        assert_eq!(snapshot["acknowledged_transactions"], 20);
        assert_eq!(snapshot["last_success_elapsed_ms"], 1_000);
        assert_eq!(snapshot["operation_in_flight"], false);
    }
}
