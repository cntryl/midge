//! A repeatable outage and mixed-workload campaign against the public API.

use super::fixture::{self, Campaign};
use cntryl_midge::{
    ColumnFamilyHandle, Engine, Query, RuntimeMetricsSnapshot, TransactionMode, WriteOptions,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ROUNDS: u64 = 12;
const KEYS_PER_ROUND: u64 = 8;
const PROGRESS_REPORT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MaintenanceProgress {
    sequence: u64,
    persisted_sequence: u64,
    sst_count: usize,
    compactions: u64,
    rewritten_bytes: u64,
    publications: u64,
}

pub(super) struct WorkloadProgress {
    started: Instant,
    deadline: Instant,
    next_report: Instant,
    changed_at: Instant,
    flush_changed_at: Instant,
    last: Option<MaintenanceProgress>,
}

impl WorkloadProgress {
    fn new(started: Instant, timeout: Duration) -> Self {
        Self {
            started,
            deadline: started + timeout,
            next_report: started,
            changed_at: started,
            flush_changed_at: started,
            last: None,
        }
    }

    fn remaining(&self, now: Instant) -> Duration {
        self.deadline.saturating_duration_since(now)
    }

    pub(super) fn require_time(&self, stage: &str) {
        let now = Instant::now();
        assert!(
            !self.remaining(now).is_zero(),
            "{stage} exceeded the fixed workload deadline after {:?}; observed progress unchanged for {:?}: {:?}",
            now.duration_since(self.started),
            now.duration_since(self.changed_at),
            self.last,
        );
    }

    fn observe(&mut self, progress: MaintenanceProgress, now: Instant) {
        if self
            .last
            .is_none_or(|previous| previous.publications != progress.publications)
        {
            self.flush_changed_at = now;
        }
        if self.last != Some(progress) {
            self.last = Some(progress);
            self.changed_at = now;
        }
    }

    fn report(&mut self, stage: &str, metrics: &RuntimeMetricsSnapshot, now: Instant) {
        self.observe(
            MaintenanceProgress {
                sequence: metrics.current_sequence,
                persisted_sequence: metrics.manifest_last_persisted_sequence,
                sst_count: metrics.sst_count,
                compactions: metrics.compactions_run,
                rewritten_bytes: metrics.compaction_bytes_rewritten,
                publications: metrics.flush_publish_count,
            },
            now,
        );
        if now < self.next_report {
            return;
        }
        self.next_report = now + PROGRESS_REPORT_INTERVAL;
        eprintln!(
            "MIDGE_WORKLOAD_PROGRESS {}",
            serde_json::json!({
                "stage": stage,
                "elapsed_ms": now.duration_since(self.started).as_millis(),
                "remaining_ms": self.remaining(now).as_millis(),
                "observed_progress_age_ms": now.duration_since(self.changed_at).as_millis(),
                "flush_publication_age_ms": now.duration_since(self.flush_changed_at).as_millis(),
                "sst_count": metrics.sst_count,
                "compactions_completed": metrics.compactions_run,
                "compaction_bytes_rewritten": metrics.compaction_bytes_rewritten,
                "active_compactions": metrics.active_compactions,
                "pending_compactions": metrics.pending_compactions,
                "compacting_ssts": metrics.compacting_ssts,
                "flush_queue": metrics.flush_queue_depth,
                "flush_inflight": metrics.flush_inflight,
                "flush_publications": metrics.flush_publish_count,
                "flush_retries": metrics.flush_retries_total,
                "write_stalled": metrics.write_stalled,
                "write_stall_active_ns": metrics.write_stall_active_ns,
                "sequence": metrics.current_sequence,
                "persisted_sequence": metrics.manifest_last_persisted_sequence,
                "cloud_durable_sequence": metrics.wal_cloud_durable_seq,
                "pending_cloud_uploads": metrics.pending_cloud_uploads,
                "local_storage": metrics.local_storage,
            })
        );
    }

    pub(super) fn report_wait(&mut self, engine: &Engine, stage: &str) {
        let now = Instant::now();
        if now < self.next_report || self.remaining(now).is_zero() {
            return;
        }
        match engine
            .get_runtime_metrics_with_timeout(self.remaining(now).min(PROGRESS_REPORT_INTERVAL))
        {
            Ok(metrics) => self.report(stage, &metrics, Instant::now()),
            Err(error) => {
                // Diagnostic requests cannot extend the workload or turn a
                // busy publication gate into a different write failure.
                self.next_report = Instant::now() + PROGRESS_REPORT_INTERVAL;
                eprintln!("MIDGE_WORKLOAD_PROGRESS stage={stage} metrics_unavailable={error}");
            }
        }
    }
}

pub(super) fn expected_keys() -> impl Iterator<Item = Vec<u8>> {
    (0..ROUNDS).flat_map(|round| {
        (0..KEYS_PER_ROUND).map(move |index| format!("pressure-{round:04}-{index:04}").into_bytes())
    })
}

struct StopReader(Arc<AtomicBool>);
impl Drop for StopReader {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) fn exercise(engine: &Engine, campaign: &Campaign) -> WorkloadProgress {
    let cf = super::super::default_cf(engine);
    let reads = AtomicU64::new(0);
    let stopped = Arc::new(AtomicBool::new(false));
    let mut progress = WorkloadProgress::new(
        Instant::now(),
        Duration::from_secs(campaign.profile.timeout_seconds),
    );
    std::thread::scope(|scope| {
        let _stop = StopReader(Arc::clone(&stopped));
        scope.spawn(|| read_during_maintenance(engine, campaign, &stopped, &reads));
        for round in 0..ROUNDS {
            write_round(engine, &cf, round, &mut progress);
            if round == 0 {
                fail_uploads_then_resume(engine, &cf, campaign, &mut progress);
            } else {
                engine
                    .flush_cf(&cf)
                    .expect("flush mixed-workload generation");
            }
            if round % 4 == 3 {
                compact_all(engine, &mut progress);
            }
        }
        verify(engine);
    });
    assert!(
        reads.load(Ordering::Relaxed) > 0,
        "foreground reads must progress during maintenance"
    );
    progress
}

pub(super) fn compact_all(engine: &Engine, progress: &mut WorkloadProgress) {
    progress.require_time("bulk compaction");
    eprintln!("MIDGE_WORKLOAD_COMPACTION started");
    std::thread::scope(|scope| {
        let (completed, result) = std::sync::mpsc::sync_channel(1);
        scope.spawn(move || {
            let _ = completed.send(engine.compact_all());
        });
        loop {
            progress.require_time("bulk compaction");
            let wait = progress
                .remaining(Instant::now())
                .min(PROGRESS_REPORT_INTERVAL);
            match result.recv_timeout(wait) {
                Ok(result) => {
                    result.expect("mixed-workload compaction");
                    progress.require_time("bulk compaction");
                    break;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    progress.report_wait(engine, "compact_all");
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("compaction worker terminated without a result");
                }
            }
        }
    });
    eprintln!("MIDGE_WORKLOAD_COMPACTION completed");
}

fn write_round(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    round: u64,
    progress: &mut WorkloadProgress,
) {
    loop {
        progress.require_time("cloud-strict write admission");
        match try_write_round(engine, cf, round) {
            Ok(()) => return,
            Err(cntryl_midge::MidgeError::WriteStall(_)) => {
                progress.report_wait(engine, "write_stall");
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => panic!("mixed cloud-strict acknowledgment failed: {error}"),
        }
    }
}

fn try_write_round(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    round: u64,
) -> cntryl_midge::MidgeResult<()> {
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("mixed write transaction");
    for index in 0..KEYS_PER_ROUND {
        let key = format!("pressure-{round:04}-{index:04}").into_bytes();
        tx.put(
            key,
            fixture::value(round * KEYS_PER_ROUND + index, 2048),
            None,
        )
        .expect("mixed write");
    }
    tx.commit(WriteOptions::cloud_strict())
}

fn fail_uploads_then_resume(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    campaign: &Campaign,
    progress: &mut WorkloadProgress,
) {
    let before = engine.get_runtime_metrics().expect("pre-outage metrics");
    fail::cfg("midge::cloud::inject_fail_sst_upload", "return").expect("SST upload outage");
    let initial_error = engine
        .flush_cf(cf)
        .expect_err("outage must reject SST publication");
    assert!(
        initial_error
            .to_string()
            .contains("cloud SST upload failed"),
        "unexpected outage: {initial_error}"
    );
    let duration = std::env::var("MIDGE_QUALIFICATION_OUTAGE_SECONDS")
        .map_or(2, |value| value.parse::<u64>().expect("outage duration"));
    let until = Instant::now() + Duration::from_secs(duration);
    while Instant::now() < until {
        progress.require_time("injected SST upload outage");
        let metrics = engine.get_runtime_metrics().expect("outage metrics");
        assert!(metrics.hybrid_total_committed_bytes <= campaign.profile.local_bytes);
        std::thread::sleep(Duration::from_millis(25));
    }
    let failed = engine
        .get_runtime_metrics()
        .expect("retained outage metrics");
    assert!(failed.flush_failures_total > before.flush_failures_total);
    assert!(
        failed
            .local_storage
            .as_ref()
            .expect("hybrid pressure snapshot")
            .usage
            .flush_staging_reserved_bytes
            > 0,
        "failed staging remains charged"
    );
    fail::remove("midge::cloud::inject_fail_sst_upload");
    let settled = loop {
        progress.require_time("SST publication after upload outage");
        let metrics = engine
            .get_runtime_metrics()
            .expect("retry progress metrics");
        progress.report("flush_retry", &metrics, Instant::now());
        if metrics.flush_publish_count > before.flush_publish_count
            && metrics.flush_inflight == 0
            && metrics.flush_queue_depth == 0
        {
            break metrics;
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    std::fs::write(
        campaign.artifacts.join("pressure.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "outage_seconds": duration, "before": before, "failed": failed, "settled": settled,
        }))
        .expect("pressure JSON"),
    )
    .expect("pressure evidence");
}

fn read_during_maintenance(
    engine: &Engine,
    campaign: &Campaign,
    stopped: &AtomicBool,
    reads: &AtomicU64,
) {
    let cf = super::super::default_cf(engine);
    let query = Query::new().prefix(b"data-".as_slice().into()).limit(4);
    while !stopped.load(Ordering::Acquire) {
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("concurrent read transaction");
        assert_eq!(
            tx.get(&fixture::key(0))
                .expect("concurrent point read")
                .as_deref(),
            Some(fixture::value(0, campaign.profile.value_bytes).as_slice())
        );
        let mut count = 0;
        for (index, entry) in tx.scan(&query).expect("concurrent scan").enumerate() {
            let (key, value) = entry.expect("scan item");
            assert_eq!(key.as_ref(), fixture::key(index as u64));
            assert_eq!(
                value.as_ref(),
                fixture::value(index as u64, campaign.profile.value_bytes)
            );
            count += 1;
        }
        assert_eq!(count, campaign.records.min(4));
        reads.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub(super) fn verify(engine: &Engine) {
    let cf = super::super::default_cf(engine);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("mixed workload verification");
    for round in 0..ROUNDS {
        for index in 0..KEYS_PER_ROUND {
            let key = format!("pressure-{round:04}-{index:04}");
            assert_eq!(
                tx.get(key.as_bytes())
                    .expect("acknowledged mixed value")
                    .as_deref(),
                Some(fixture::value(round * KEYS_PER_ROUND + index, 2048).as_slice())
            );
        }
    }
}

#[test]
fn should_preserve_profile_timeout_when_maintenance_progress_changes() {
    // Arrange
    let started = Instant::now();
    let mut progress = WorkloadProgress::new(started, Duration::from_secs(120));
    let completed = MaintenanceProgress {
        sequence: 10,
        persisted_sequence: 10,
        sst_count: 100,
        compactions: 1,
        rewritten_bytes: 1024,
        publications: 1,
    };

    // Act
    let after_one_minute = started + Duration::from_secs(61);
    progress.observe(completed, after_one_minute);
    progress.observe(
        MaintenanceProgress {
            compactions: 2,
            ..completed
        },
        started + Duration::from_secs(119),
    );

    // Assert
    assert_eq!(
        progress.remaining(after_one_minute),
        Duration::from_secs(59)
    );
    assert_eq!(
        progress.remaining(started + Duration::from_secs(120)),
        Duration::ZERO
    );
    assert_eq!(progress.deadline, started + Duration::from_secs(120));
    assert_eq!(
        (started + Duration::from_secs(119)).duration_since(progress.flush_changed_at),
        Duration::from_secs(58),
        "unrelated compaction must not hide a waiting flush"
    );
}

#[test]
fn should_preserve_observed_stall_age_when_maintenance_counters_do_not_change() {
    // Arrange
    let started = Instant::now();
    let mut progress = WorkloadProgress::new(started, Duration::from_secs(10));
    let unchanged = MaintenanceProgress {
        sequence: 10,
        persisted_sequence: 10,
        sst_count: 640,
        compactions: 0,
        rewritten_bytes: 0,
        publications: 0,
    };
    progress.observe(unchanged, started);

    // Act
    let later = started + Duration::from_secs(9);
    progress.observe(unchanged, later);

    // Assert
    assert_eq!(
        later.duration_since(progress.changed_at),
        Duration::from_secs(9)
    );
    assert_eq!(progress.remaining(later), Duration::from_secs(1));
    assert_eq!(
        progress.remaining(started + Duration::from_secs(11)),
        Duration::ZERO
    );
}
