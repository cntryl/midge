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
        let metrics = match engine
            .metrics()
            .get_runtime_metrics_with_timeout(remaining.min(Duration::from_secs(5)))
        {
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
