//! Advisory caller slowdown. Runtime hard admission remains authoritative.

use crate::config::WriteBackpressureConfig;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

#[derive(Default)]
pub(crate) struct WriteAdmission {
    config: OnceLock<WriteBackpressureConfig>,
    l0_slots: dashmap::DashMap<u32, usize>,
    hard_families: dashmap::DashMap<u32, ()>,
    draining: AtomicBool,
    memory_bytes: AtomicUsize,
    pending_bytes: AtomicU64,
    pub(crate) urgent: Arc<AtomicBool>,
    pub(crate) slowdown_ns: AtomicU64,
    pub(crate) slowdown_count: AtomicU64,
    pub(crate) limited_bytes: Arc<AtomicU64>,
    limiter: OnceLock<Arc<crate::common::write_rate::BackgroundWriteLimiter>>,
}

impl WriteAdmission {
    pub(crate) fn configure(&self, config: &WriteBackpressureConfig) {
        if let Some(rate) = config.background_write_bytes_per_second {
            let _ = self.limiter.set(Arc::new(
                crate::common::write_rate::BackgroundWriteLimiter::new(
                    rate,
                    self.urgent.clone(),
                    self.limited_bytes.clone(),
                ),
            ));
        }
        let _ = self.config.set(config.clone());
    }

    pub(crate) fn limiter(&self) -> Option<Arc<crate::common::write_rate::BackgroundWriteLimiter>> {
        self.limiter.get().cloned()
    }

    pub(crate) fn enabled(&self) -> bool {
        self.config
            .get()
            .is_some_and(WriteBackpressureConfig::has_soft_limits)
    }

    pub(crate) fn tracks_pending_bytes(&self) -> bool {
        self.config.get().is_some_and(|config| {
            config.pending_compaction_soft_limit.is_some()
                || config.pending_compaction_hard_limit.is_some()
        })
    }

    pub(crate) fn tracks_background_io(&self) -> bool {
        self.config
            .get()
            .is_some_and(|config| config.background_write_bytes_per_second.is_some())
    }

    pub(crate) fn update_family(&self, cf: u32, l0_slots: usize, memory: usize) {
        if self.enabled() {
            self.l0_slots.insert(cf, l0_slots);
            self.memory_bytes.store(memory, Ordering::Release);
        }
    }

    pub(crate) fn update_manifest(&self, pending_bytes: u64) {
        self.pending_bytes.store(pending_bytes, Ordering::Release);
    }

    pub(crate) fn update_urgent(&self, cf: u32, hard: bool) {
        if !self.tracks_background_io() || self.draining.load(Ordering::Acquire) {
            return;
        }
        if hard {
            self.hard_families.insert(cf, ());
        } else {
            self.hard_families.remove(&cf);
        }
        self.urgent
            .store(!self.hard_families.is_empty(), Ordering::Release);
    }

    pub(crate) fn begin_drain(&self) {
        self.draining.store(true, Ordering::Release);
        self.urgent.store(true, Ordering::Release);
    }

    pub(crate) fn retain_families(&self, mut exists: impl FnMut(u32) -> bool) {
        self.l0_slots.retain(|cf, _| exists(*cf));
        self.hard_families.retain(|cf, _| exists(*cf));
        if !self.draining.load(Ordering::Acquire) {
            self.urgent
                .store(!self.hard_families.is_empty(), Ordering::Release);
        }
    }

    pub(crate) fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn hard_pending_limit(&self) -> bool {
        self.config.get().is_some_and(|config| {
            config
                .pending_compaction_hard_limit
                .is_some_and(|limit| self.pending_bytes() >= limit)
        })
    }

    pub(crate) fn delay(&self, cf: u32, queue_soft: bool) -> Duration {
        let Some(config) = self.config.get().filter(|_| self.enabled()) else {
            return Duration::ZERO;
        };
        let l0_soft = config
            .l0_soft_limit
            .is_some_and(|limit| self.l0_slots.get(&cf).is_some_and(|slots| *slots >= limit));
        let memory_soft = config
            .memory_soft_limit
            .is_some_and(|limit| self.memory_bytes.load(Ordering::Acquire) >= limit);
        let pending_soft = config
            .pending_compaction_soft_limit
            .is_some_and(|limit| self.pending_bytes() >= limit);
        if queue_soft || l0_soft || memory_soft || pending_soft {
            config.max_admission_delay
        } else {
            Duration::ZERO
        }
    }

    pub(crate) fn record_delay(&self, elapsed: Duration) {
        self.slowdown_count.fetch_add(1, Ordering::Relaxed);
        self.slowdown_ns.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}
