//! Distinguish active maintenance charges from unexplained idle reservations.

use cntryl_midge::{Engine, RuntimeMetricsSnapshot};
use std::time::{Duration, Instant};

pub(super) fn wait(engine: &Engine, deadline: Instant) -> RuntimeMetricsSnapshot {
    let mut next_report = Instant::now();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "maintenance did not become idle before the campaign deadline"
        );
        let metrics =
            match engine.get_runtime_metrics_with_timeout(remaining.min(Duration::from_secs(5))) {
                Ok(metrics) => metrics,
                Err(cntryl_midge::MidgeError::Timeout(_)) => continue,
                Err(error) => panic!("quiescence metrics failed: {error}"),
            };
        if inspect(&metrics).unwrap_or_else(|error| panic!("{error}: {metrics:?}")) {
            return metrics;
        }
        if Instant::now() >= next_report {
            eprintln!("MIDGE_QUIESCENCE waiting: compactions={} flushes={} queued_flushes={} reservations={}",
                metrics.active_compactions, metrics.flush_inflight, metrics.flush_queue_depth,
                metrics.local_storage.as_ref().unwrap().usage.reservations);
            next_report = Instant::now() + Duration::from_secs(5);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn inspect(metrics: &RuntimeMetricsSnapshot) -> Result<bool, &'static str> {
    if metrics.active_compactions != 0
        || metrics.compacting_ssts != 0
        || metrics.pending_compactions != 0
        || metrics.flush_inflight != 0
        || metrics.flush_queue_depth != 0
        || metrics.pending_cloud_uploads != 0
        || metrics.wal_pending_writes != 0
        || metrics.hybrid_pending_evictions != 0
    {
        return Ok(false);
    }
    let storage = metrics
        .local_storage
        .as_ref()
        .ok_or("missing hybrid resource snapshot")?;
    if storage.usage.reservations != 0 {
        return Err("storage reservation leak in idle runtime");
    }
    if metrics.pinned_ssts != 0 {
        return Err("reader pin leak after verification");
    }
    Ok(true)
}

fn empty_snapshot() -> RuntimeMetricsSnapshot {
    let directory = tempfile::tempdir().expect("snapshot database");
    let options =
        cntryl_midge::OpenOptions::cloud_simulated(directory.path(), "bucket", "quiescence")
            .background_compaction(false)
            .build()
            .expect("snapshot options");
    let mut engine = Engine::open(options).expect("snapshot engine");
    let metrics = engine.get_runtime_metrics().expect("empty runtime metrics");
    engine
        .shutdown(Duration::from_secs(5))
        .expect("close snapshot engine");
    metrics
}

#[test]
fn should_wait_for_compaction_completion_before_classifying_its_reservation() {
    // Arrange: reproduce the full campaign's pre-shutdown sample.
    let clean = empty_snapshot();
    let mut active = clean.clone();
    active.active_compactions = 1;
    active.compacting_ssts = 4;
    let usage = &mut active.local_storage.as_mut().unwrap().usage;
    usage.compaction_staging_reserved_bytes = 961_196_442;
    usage.reservations = 1;

    // Act
    let before_completion = inspect(&active);
    let after_completion = inspect(&clean);

    // Assert
    assert_eq!(
        before_completion,
        Ok(false),
        "a live job still owns this charge"
    );
    assert_eq!(after_completion, Ok(true));
}

#[test]
fn should_reject_unexplained_reservations_when_runtime_is_idle() {
    // Arrange
    let mut leaked = empty_snapshot();
    leaked.local_storage.as_mut().unwrap().usage.reservations = 1;

    // Act
    let result = inspect(&leaked);

    // Assert
    assert!(
        result.is_err(),
        "waiting must not hide an idle reservation leak"
    );
}
