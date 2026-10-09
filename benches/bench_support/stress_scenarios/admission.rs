//! Optional maintenance samples; observations never advance the workload watchdog.

use cntryl_midge::Engine;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub(super) struct MaintenanceSampler {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MaintenanceSampler {
    pub(super) fn start(engine: &Engine, directory: &Path, started: Instant) -> Option<Self> {
        let configured = std::env::var("MIDGE_STRESS_ADMISSION_SAMPLE_MS").ok()?;
        let milliseconds = configured
            .parse::<u64>()
            .expect("admission sample interval");
        assert!(
            (100..=60_000).contains(&milliseconds),
            "admission samples must be 100..60000 ms apart"
        );
        let interval = Duration::from_millis(milliseconds);
        let metrics = engine.metrics();
        let mut file = std::fs::File::create(directory.join("maintenance-samples.jsonl"))
            .expect("create maintenance samples");
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                let requested = started.elapsed().as_nanos();
                let result = metrics.get_runtime_metrics_with_timeout(Duration::from_millis(250));
                let completed = started.elapsed().as_nanos();
                let row = match result {
                    Ok(runtime) => serde_json::json!({
                        "schema_version": 1, "interval_ms": milliseconds,
                        "requested_ns": requested, "completed_ns": completed,
                        "runtime": runtime, "error": null,
                    }),
                    Err(error) => serde_json::json!({
                        "schema_version": 1, "interval_ms": milliseconds,
                        "requested_ns": requested, "completed_ns": completed,
                        "runtime": null, "error": error.to_string(),
                    }),
                };
                serde_json::to_writer(&mut file, &row).expect("write maintenance sample");
                writeln!(file).expect("terminate maintenance sample");
                thread::park_timeout(interval);
            }
        });
        Some(Self {
            stop,
            thread: Some(thread),
        })
    }

    pub(super) fn finish(mut self) {
        self.join().expect("maintenance sampler completed");
    }

    fn join(&mut self) -> thread::Result<()> {
        self.stop.store(true, Ordering::Release);
        self.thread.take().map_or(Ok(()), |thread| {
            thread.thread().unpark();
            thread.join()
        })
    }
}

impl Drop for MaintenanceSampler {
    fn drop(&mut self) {
        let _ = self.join();
    }
}
