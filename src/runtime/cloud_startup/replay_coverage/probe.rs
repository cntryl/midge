//! Unsupported bounded experiments and per-owner attribution.
use std::cell::Cell;
use std::time::Instant;

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryProbeVariant {
    #[default]
    Baseline,
    KeyIndex,
    SingleReader,
    TimersOff,
}

#[derive(Default)]
pub(super) struct Facts {
    pub point_probes: Cell<u64>,
    pub exact_hits: Cell<u64>,
    pub predicate_rejections: Cell<u64>,
    pub index_build_attempts: Cell<u64>,
    pub index_builds: Cell<u64>,
    pub index_build_ns: Cell<u64>,
    pub candidate_ns: Cell<u64>,
    pub identity_ns: Cell<u64>,
    pub reader_ns: Cell<u64>,
    pub point_ns: Cell<u64>,
    pub proof_inclusive_ns: Cell<u64>,
    pub checkpoint_releases: Cell<u64>,
    pub peak_readers: Cell<usize>,
}

pub(super) fn add(counter: &Cell<u64>, value: u64) {
    counter.set(counter.get().saturating_add(value));
}

pub(super) fn clock(variant: RecoveryProbeVariant) -> Option<Instant> {
    (variant != RecoveryProbeVariant::TimersOff).then(Instant::now)
}

pub(super) fn elapsed(start: Option<Instant>) -> u64 {
    start.map_or(0, |start| {
        u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    })
}

pub(super) struct Phase<'a> {
    counter: &'a Cell<u64>,
    started: Option<Instant>,
}
impl<'a> Phase<'a> {
    pub fn start(counter: &'a Cell<u64>, variant: RecoveryProbeVariant) -> Self {
        Self {
            counter,
            started: clock(variant),
        }
    }
}
impl Drop for Phase<'_> {
    fn drop(&mut self) {
        add(self.counter, elapsed(self.started));
    }
}

#[derive(Default)]
pub(super) struct Reads {
    started: std::sync::atomic::AtomicU64,
    completed: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
    failures: std::sync::atomic::AtomicU64,
    elapsed_ns: std::sync::atomic::AtomicU64,
}
impl crate::io::traits::ReadObserver for Reads {
    fn remote_range_started(&self) {
        self.started
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn remote_range_completed(&self, bytes: u64, elapsed: std::time::Duration, failed: bool) {
        use std::sync::atomic::Ordering::Relaxed;
        self.completed.fetch_add(1, Relaxed);
        self.bytes.fetch_add(bytes, Relaxed);
        self.failures.fetch_add(u64::from(failed), Relaxed);
        self.elapsed_ns.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Relaxed,
        );
        crate::io::traits::ReadObserver::remote_range_completed(
            &crate::telemetry::recovery_progress::RecoveryReadObserver,
            bytes,
            elapsed,
            failed,
        );
    }
}
impl Reads {
    pub(super) fn snapshot(&self) -> serde_json::Value {
        use std::sync::atomic::Ordering::Relaxed;
        serde_json::json!({"attempts": self.started.load(Relaxed), "completed": self.completed.load(Relaxed),
            "bytes": self.bytes.load(Relaxed), "failures": self.failures.load(Relaxed), "elapsed_ns": self.elapsed_ns.load(Relaxed)})
    }
}
