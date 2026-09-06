//! External observations supplement internal reservations at publication boundaries.

use super::fixture::Campaign;
use cntryl_midge::RuntimeMetricsSnapshot;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub(super) struct Observation {
    state: Arc<State>,
    worker: Option<JoinHandle<()>>,
    telemetry: super::telemetry::Recorder,
}

struct State {
    cache: PathBuf,
    limit: u64,
    peak: AtomicU64,
    checkpoints: AtomicU64,
    stopped: AtomicBool,
}

impl State {
    fn sample(&self) {
        let bytes = file_bytes(&self.cache);
        self.peak.fetch_max(bytes, Ordering::Relaxed);
        assert!(
            bytes <= self.limit,
            "observed file bytes {bytes} exceeded local budget {}",
            self.limit
        );
    }
}

impl Observation {
    pub fn start(campaign: &Campaign, _phase: &str) -> Self {
        let state = Arc::new(State {
            cache: campaign.cache.clone(),
            limit: campaign.profile.local_bytes,
            peak: AtomicU64::new(0),
            checkpoints: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        });
        let sampler = Arc::clone(&state);
        let worker = std::thread::spawn(move || {
            while !sampler.stopped.load(Ordering::Acquire) {
                sampler.sample();
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        Self {
            state,
            worker: Some(worker),
            telemetry: super::telemetry::Recorder::install(),
        }
    }

    pub fn install_publication_probes(&self, campaign: &Campaign, phase: &str) {
        for name in [
            "midge::flush::after_sst_write_before_publish",
            "midge::flush_worker::after_cloud_sst_upload",
        ] {
            let state = Arc::clone(&self.state);
            fail::cfg_callback(name, move || state.sample()).expect("publication observation");
        }
        let state = Arc::clone(&self.state);
        let artifact = campaign.artifacts.clone();
        let report = self.report(campaign, phase, 0, None);
        let interrupt = phase == "interrupted";
        let telemetry = self.telemetry.clone();
        fail::cfg_callback("midge::recovery::after_checkpoint", move || {
            state.sample();
            let checkpoints = state.checkpoints.fetch_add(1, Ordering::Relaxed) + 1;
            if checkpoints == 1 || checkpoints.is_multiple_of(64) {
                eprintln!("MIDGE_OPERATIONAL_CHECKPOINT {checkpoints}");
            }
            if interrupt {
                let mut report = report.clone();
                report["peak_local_file_bytes"] = state.peak.load(Ordering::Relaxed).into();
                report["checkpoints"] = state.checkpoints.load(Ordering::Relaxed).into();
                report["process_peak_rss_bytes"] = process_peak_rss_bytes().into();
                report["costs"] =
                    serde_json::to_value(telemetry.snapshot()).expect("cost snapshot");
                save(&artifact.join("interrupted.json"), &report);
                std::fs::write(
                    artifact.join("checkpoint-reached"),
                    b"durable recovery checkpoint",
                )
                .expect("exact crash boundary");
                std::process::exit(73);
            }
        })
        .expect("checkpoint observation");
    }

    pub fn record_opened(&self, campaign: &Campaign, phase: &str, recovery_ms: u128) {
        self.state.sample();
        let stage = format!("{phase}-opened");
        save(
            &campaign.artifacts.join(format!("{stage}.json")),
            &self.report(campaign, &stage, recovery_ms, None),
        );
    }

    pub fn finish(
        mut self,
        campaign: &Campaign,
        phase: &str,
        recovery_ms: u128,
        metrics: Option<&RuntimeMetricsSnapshot>,
    ) {
        self.state.stopped.store(true, Ordering::Release);
        self.worker
            .take()
            .expect("sampler")
            .join()
            .expect("disk sampler must not fail");
        self.state.sample();
        save(
            &campaign.artifacts.join(format!("{phase}.json")),
            &self.report(campaign, phase, recovery_ms, metrics),
        );
    }

    fn report(
        &self,
        campaign: &Campaign,
        phase: &str,
        recovery_ms: u128,
        metrics: Option<&RuntimeMetricsSnapshot>,
    ) -> serde_json::Value {
        let verified = matches!(phase, "recovered" | "verified");
        json!({
            "schema_version": 2, "provider": "Sqrzl S3 protocol", "phase": phase,
            "profile": campaign.profile, "cloud_wal_bytes": campaign.actual_wal_bytes,
            "source_records": campaign.records,
            "verified_records": if verified { campaign.records } else { 0 },
            "verification_complete": verified,
            "recovery_ms": if phase == "interrupted" { None } else { Some(recovery_ms) },
            "revision": std::env::var("MIDGE_QUALIFICATION_REVISION").ok(),
            "debug_assertions": cfg!(debug_assertions),
            "peak_local_file_bytes": self.state.peak.load(Ordering::Relaxed),
            "checkpoints": self.state.checkpoints.load(Ordering::Relaxed),
            "process_peak_rss_bytes": process_peak_rss_bytes(),
            "runtime_metrics": metrics,
            "costs": self.telemetry.snapshot(),
        })
    }
}

#[cfg(unix)]
fn process_peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes this correctly sized rusage on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized the complete value.
    let bytes = u64::try_from(unsafe { usage.assume_init() }.ru_maxrss).ok()?;
    #[cfg(target_os = "macos")]
    return Some(bytes);
    #[cfg(not(target_os = "macos"))]
    Some(bytes.saturating_mul(1024))
}

#[cfg(not(unix))]
fn process_peak_rss_bytes() -> Option<u64> {
    None
}

impl Drop for Observation {
    fn drop(&mut self) {
        self.state.stopped.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn save(path: &Path, report: &serde_json::Value) {
    use std::io::Write as _;
    let mut file = std::fs::File::create(path).expect("create report");
    file.write_all(&serde_json::to_vec_pretty(report).expect("report JSON"))
        .expect("report write");
    file.sync_all().expect("sync report");
}

fn file_bytes(path: &Path) -> u64 {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return 0,
        Err(error) => panic!("observe {}: {error}", path.display()),
    };
    entries
        .map(|entry| {
            let entry = entry.expect("observed directory entry");
            match entry.metadata() {
                Ok(meta) if meta.is_dir() => file_bytes(&entry.path()),
                Ok(meta) => meta.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => panic!("observe file metadata: {error}"),
            }
        })
        .sum()
}
