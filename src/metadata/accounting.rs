//! Bounded metadata and publication accounting for one runtime owner.
//! Issued payload is the size passed to a delegated filesystem write, not device I/O.

use parking_lot::Mutex;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ORIGINS: usize = 9;
const MEDIA: usize = 2;
const PAYLOADS: usize = 3;
const HISTOGRAM_BUCKETS: usize = 80;
const LINEAR_BUCKETS: usize = 64;
const STEP_NS: u64 = 250_000;
static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    OrdinaryLocalFlush,
    CloudFlush,
    Recovery,
    Bootstrap,
    Ddl,
    CompactionBeforeGc,
    Administration,
    Shutdown,
    Unclassified,
}

impl Origin {
    const ALL: [Self; ORIGINS] = [
        Self::OrdinaryLocalFlush,
        Self::CloudFlush,
        Self::Recovery,
        Self::Bootstrap,
        Self::Ddl,
        Self::CompactionBeforeGc,
        Self::Administration,
        Self::Shutdown,
        Self::Unclassified,
    ];

    const fn index(self) -> usize {
        match self {
            Self::OrdinaryLocalFlush => 0,
            Self::CloudFlush => 1,
            Self::Recovery => 2,
            Self::Bootstrap => 3,
            Self::Ddl => 4,
            Self::CompactionBeforeGc => 5,
            Self::Administration => 6,
            Self::Shutdown => 7,
            Self::Unclassified => 8,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Medium {
    Persistent,
    MemoryOnly,
}

impl Medium {
    const ALL: [Self; MEDIA] = [Self::Persistent, Self::MemoryOnly];
    const fn index(self) -> usize {
        match self {
            Self::Persistent => 0,
            Self::MemoryOnly => 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Payload {
    Snapshot,
    Journal,
    OtherMetadata,
}

impl Payload {
    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Snapshot => 0,
            Self::Journal => 1,
            Self::OtherMetadata => 2,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum OperationKind {
    Checkpoint,
    JournalAppend,
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct Counters {
    pub operation_attempts: u64,
    pub operation_failures: u64,
    pub abandoned_operations: u64,
    pub issued_bytes: [u64; PAYLOADS],
    pub returned_write_bytes: [u64; PAYLOADS],
    pub snapshot_durable_bytes: u64,
    pub journal_durable_bytes: u64,
    pub snapshot_durable_count: u64,
    pub checkpoint_complete_count: u64,
    pub checkpoint_attempts: u64,
    pub checkpoint_elapsed_ns: u64,
    pub journal_append_attempts: u64,
    pub journal_elapsed_ns: u64,
    pub publication_attempts: u64,
    pub publication_failures: u64,
    pub publication_attempt_elapsed_ns: u64,
    pub flush_committed_count: u64,
    pub flush_committed_sst_bytes: u64,
    pub flush_full_publication_elapsed_ns: u64,
    pub compaction_committed_count: u64,
    pub compaction_committed_sst_bytes: u64,
}

fn add(target: &mut u64, value: u64) -> bool {
    let next = target.checked_add(value);
    *target = next.unwrap_or(u64::MAX);
    next.is_none()
}

impl Counters {
    fn fold(&mut self, input: &Self) -> bool {
        let mut overflow = false;
        for index in 0..PAYLOADS {
            overflow |= add(&mut self.issued_bytes[index], input.issued_bytes[index]);
            overflow |= add(
                &mut self.returned_write_bytes[index],
                input.returned_write_bytes[index],
            );
        }
        macro_rules! fields {
            ($($field:ident),+ $(,)?) => { $(overflow |= add(&mut self.$field, input.$field);)+ };
        }
        fields!(
            operation_attempts,
            operation_failures,
            abandoned_operations,
            snapshot_durable_bytes,
            journal_durable_bytes,
            snapshot_durable_count,
            checkpoint_complete_count,
            checkpoint_attempts,
            checkpoint_elapsed_ns,
            journal_append_attempts,
            journal_elapsed_ns,
            publication_attempts,
            publication_failures,
            publication_attempt_elapsed_ns,
            flush_committed_count,
            flush_committed_sst_bytes,
            flush_full_publication_elapsed_ns,
            compaction_committed_count,
            compaction_committed_sst_bytes
        );
        overflow
    }

    #[cfg(any(test, feature = "internal-testing"))]
    fn subtract(&self, before: &Self) -> Result<Self, &'static str> {
        let mut result = Self::default();
        for index in 0..PAYLOADS {
            result.issued_bytes[index] = self.issued_bytes[index]
                .checked_sub(before.issued_bytes[index])
                .ok_or("issued byte counter decreased")?;
            result.returned_write_bytes[index] = self.returned_write_bytes[index]
                .checked_sub(before.returned_write_bytes[index])
                .ok_or("returned byte counter decreased")?;
        }
        macro_rules! fields {
            ($($field:ident),+ $(,)?) => { $(result.$field = self.$field.checked_sub(before.$field)
                .ok_or(concat!(stringify!($field), " counter decreased"))?;)+ };
        }
        fields!(
            operation_attempts,
            operation_failures,
            abandoned_operations,
            snapshot_durable_bytes,
            journal_durable_bytes,
            snapshot_durable_count,
            checkpoint_complete_count,
            checkpoint_attempts,
            checkpoint_elapsed_ns,
            journal_append_attempts,
            journal_elapsed_ns,
            publication_attempts,
            publication_failures,
            publication_attempt_elapsed_ns,
            flush_committed_count,
            flush_committed_sst_bytes,
            flush_full_publication_elapsed_ns,
            compaction_committed_count,
            compaction_committed_sst_bytes
        );
        Ok(result)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct LatencyHistogram {
    // Exactly 80 fixed bins, regardless of operation count. Vec is serde-compatible.
    pub counts: Vec<u64>,
}

fn upper_bound(index: usize) -> u64 {
    if index < LINEAR_BUCKETS {
        u64::try_from(index + 1).expect("fixed histogram index") * STEP_NS - 1
    } else {
        (u64::try_from(LINEAR_BUCKETS).expect("fixed histogram index") * STEP_NS)
            .checked_shl(u32::try_from(index - LINEAR_BUCKETS + 1).expect("fixed shift"))
            .unwrap_or(u64::MAX)
            .saturating_sub(1)
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: vec![0; HISTOGRAM_BUCKETS],
        }
    }
}

impl LatencyHistogram {
    fn record(&mut self, ns: u64) -> bool {
        let index = (0..HISTOGRAM_BUCKETS).find(|index| ns <= upper_bound(*index));
        let overflow = index.is_none();
        overflow | add(&mut self.counts[index.unwrap_or(HISTOGRAM_BUCKETS - 1)], 1)
    }

    #[cfg(any(test, feature = "internal-testing"))]
    fn subtract(&self, before: &Self) -> Result<Self, &'static str> {
        if self.counts.len() != HISTOGRAM_BUCKETS || before.counts.len() != HISTOGRAM_BUCKETS {
            return Err("invalid histogram size");
        }
        let counts = self
            .counts
            .iter()
            .zip(&before.counts)
            .map(|(after, before)| {
                after
                    .checked_sub(*before)
                    .ok_or("histogram counter decreased")
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { counts })
    }

    #[cfg(any(test, feature = "internal-testing"))]
    #[must_use]
    pub fn p95_bounds_ns(&self) -> Option<(u64, u64)> {
        let total: u128 = self.counts.iter().map(|count| u128::from(*count)).sum();
        if total == 0 || self.counts.len() != HISTOGRAM_BUCKETS {
            return None;
        }
        let rank = (total * 95).div_ceil(100);
        let mut seen = 0_u128;
        for (index, count) in self.counts.iter().enumerate() {
            seen += u128::from(*count);
            if seen >= rank {
                let lower = if index == 0 {
                    0
                } else {
                    upper_bound(index - 1) + 1
                };
                return Some((lower, upper_bound(index)));
            }
        }
        None
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Bucket {
    pub origin: Origin,
    pub medium: Medium,
    pub counters: Counters,
    pub checkpoint_latency: LatencyHistogram,
    pub full_publication_latency: LatencyHistogram,
    pub committed_sst_size_log2: Vec<u64>,
    pub active_operations: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub owner_id: u64,
    pub buckets: Vec<Bucket>,
    pub overflow: bool,
    /// Sealed-operation payload or filesystem-mutation observations, including
    /// truncate/sync/namespace work that offers no payload bytes.
    pub late_operation_writes: u64,
    pub incomplete_observations: u64,
}

impl Snapshot {
    #[cfg(any(test, feature = "internal-testing"))]
    #[must_use]
    pub fn bucket(&self, origin: Origin, medium: Medium) -> &Bucket {
        &self.buckets[origin.index() * MEDIA + medium.index()]
    }

    /// Subtract counters and histogram bins belonging to the same owner.
    ///
    /// # Errors
    ///
    /// Rejects owner or bucket mismatches, decreasing counters, malformed
    /// histograms, overflow, escaped mutations and incomplete observations.
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn delta(&self, before: &Self) -> Result<Self, &'static str> {
        if self.owner_id != before.owner_id
            || self.buckets.len() != ORIGINS * MEDIA
            || before.buckets.len() != ORIGINS * MEDIA
        {
            return Err("owner or bucket identity mismatch");
        }
        if self.overflow
            || before.overflow
            || self.late_operation_writes > 0
            || self.incomplete_observations > 0
        {
            return Err("invalid accounting overflow or escaped operation");
        }
        let mut result = self.clone();
        for ((after, before), output) in self
            .buckets
            .iter()
            .zip(&before.buckets)
            .zip(&mut result.buckets)
        {
            if after.origin != before.origin || after.medium != before.medium {
                return Err("bucket origin mismatch");
            }
            output.counters = after.counters.subtract(&before.counters)?;
            output.checkpoint_latency = after
                .checkpoint_latency
                .subtract(&before.checkpoint_latency)?;
            output.full_publication_latency = after
                .full_publication_latency
                .subtract(&before.full_publication_latency)?;
            if after.committed_sst_size_log2.len() != 64
                || before.committed_sst_size_log2.len() != 64
            {
                return Err("invalid SST size histogram");
            }
            output.committed_sst_size_log2 = after
                .committed_sst_size_log2
                .iter()
                .zip(&before.committed_sst_size_log2)
                .map(|(after, before)| {
                    after
                        .checked_sub(*before)
                        .ok_or("SST size histogram decreased")
                })
                .collect::<Result<Vec<_>, _>>()?;
            // Gauges are not subtracted. Persistent metadata boundaries must be idle.
        }
        Ok(result)
    }
}

/// Holds only bounded counters; retaining this after shutdown retains no Fs/runtime/lease.
#[derive(Clone)]
pub struct MetricsHandle(Arc<Mutex<Snapshot>>, Arc<AtomicU64>);

impl MetricsHandle {
    #[cfg(any(test, feature = "internal-testing"))]
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        let mut snapshot = self.0.lock().clone();
        snapshot.late_operation_writes = self.1.load(Ordering::Relaxed);
        snapshot
    }
}

#[derive(Clone)]
pub(crate) struct Owner(MetricsHandle);

impl Owner {
    pub(crate) fn new() -> Self {
        let owner_id = NEXT_OWNER.fetch_add(1, Ordering::Relaxed);
        let mut buckets = Vec::with_capacity(ORIGINS * MEDIA);
        for origin in Origin::ALL {
            for medium in Medium::ALL {
                buckets.push(Bucket {
                    origin,
                    medium,
                    counters: Counters::default(),
                    checkpoint_latency: LatencyHistogram::default(),
                    full_publication_latency: LatencyHistogram::default(),
                    committed_sst_size_log2: vec![0; 64],
                    active_operations: 0,
                });
            }
        }
        Self(MetricsHandle(
            Arc::new(Mutex::new(Snapshot {
                owner_id,
                buckets,
                overflow: owner_id == 0,
                late_operation_writes: 0,
                incomplete_observations: 0,
            })),
            Arc::new(AtomicU64::new(0)),
        ))
    }

    #[cfg(any(test, feature = "internal-testing"))]
    pub(crate) fn handle(&self) -> MetricsHandle {
        self.0.clone()
    }

    pub(crate) fn invalidate_missing_publication_start(&self) {
        let mut snapshot = self.0 .0.lock();
        let overflow = add(&mut snapshot.incomplete_observations, 1);
        snapshot.overflow |= overflow;
    }

    pub(crate) fn begin(&self, kind: OperationKind, origin: Origin, medium: Medium) -> Operation {
        let started = Instant::now();
        self.update(origin, medium, |bucket| {
            add(&mut bucket.active_operations, 1)
        });
        Operation {
            owner: self.clone(),
            kind,
            origin,
            medium,
            started,
            ledger: Arc::new(Mutex::new(Ledger {
                late: Arc::clone(&self.0 .1),
                ..Ledger::default()
            })),
            finished: false,
        }
    }

    fn update(&self, origin: Origin, medium: Medium, update: impl FnOnce(&mut Bucket) -> bool) {
        let mut snapshot = self.0 .0.lock();
        let overflow = update(&mut snapshot.buckets[origin.index() * MEDIA + medium.index()]);
        snapshot.overflow |= overflow;
    }

    pub(crate) fn publication_attempt(
        &self,
        origin: Origin,
        medium: Medium,
        elapsed: Duration,
        failed: bool,
    ) {
        let (ns, conversion_overflow) = nanos(elapsed);
        self.update(origin, medium, |bucket| {
            let counters = Counters {
                publication_attempts: 1,
                publication_failures: u64::from(failed),
                publication_attempt_elapsed_ns: ns,
                ..Counters::default()
            };
            conversion_overflow | bucket.counters.fold(&counters)
        });
    }

    /// Credit encoded SST bytes after this output's successful publication and installation.
    /// Runtime requires actual immutable removal; startup recovery requires its
    /// metadata commit, mirror and checkpoint installation to have all succeeded.
    pub(crate) fn flush_committed(
        &self,
        origin: Origin,
        medium: Medium,
        bytes: u64,
        elapsed: Duration,
    ) {
        let (ns, conversion_overflow) = nanos(elapsed);
        self.update(origin, medium, |bucket| {
            let counters = Counters {
                flush_committed_count: 1,
                flush_committed_sst_bytes: bytes,
                flush_full_publication_elapsed_ns: ns,
                ..Counters::default()
            };
            let index =
                usize::try_from(bytes.max(1).bit_width() - 1).expect("bounded byte exponent");
            let invalid = conversion_overflow || bytes == 0;
            invalid
                | bucket.counters.fold(&counters)
                | bucket.full_publication_latency.record(ns)
                | add(&mut bucket.committed_sst_size_log2[index], 1)
        });
    }

    pub(crate) fn compaction_committed(&self, medium: Medium, bytes: u64) {
        self.update(Origin::CompactionBeforeGc, medium, |bucket| {
            bucket.counters.fold(&Counters {
                compaction_committed_count: 1,
                compaction_committed_sst_bytes: bytes,
                ..Counters::default()
            })
        });
    }
}

fn nanos(elapsed: Duration) -> (u64, bool) {
    let value = u64::try_from(elapsed.as_nanos());
    (value.unwrap_or(u64::MAX), value.is_err())
}

#[derive(Default)]
pub(crate) struct Ledger {
    pub(crate) counters: Counters,
    pub(crate) overflow: bool,
    sealed: bool,
    late: Arc<AtomicU64>,
}

impl Ledger {
    pub(crate) fn observe_mutation(&self) {
        if self.sealed {
            let _ = self
                .late
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |late| {
                    Some(late.saturating_add(1))
                });
        }
    }

    pub(crate) fn issued(&mut self, payload: Payload, bytes: usize) {
        self.observe_mutation();
        let value = u64::try_from(bytes);
        self.overflow |= value.is_err();
        self.overflow |= add(
            &mut self.counters.issued_bytes[payload.index()],
            value.unwrap_or(u64::MAX),
        );
    }

    pub(crate) fn returned(&mut self, payload: Payload, bytes: usize) {
        self.observe_mutation();
        let value = u64::try_from(bytes);
        self.overflow |= value.is_err();
        self.overflow |= add(
            &mut self.counters.returned_write_bytes[payload.index()],
            value.unwrap_or(u64::MAX),
        );
    }
}

pub(crate) struct Operation {
    owner: Owner,
    kind: OperationKind,
    origin: Origin,
    medium: Medium,
    started: Instant,
    ledger: Arc<Mutex<Ledger>>,
    finished: bool,
}

impl Operation {
    pub(crate) fn ledger(&self) -> Arc<Mutex<Ledger>> {
        Arc::clone(&self.ledger)
    }

    pub(crate) fn snapshot_durable(&self, bytes: u64) {
        let mut ledger = self.ledger.lock();
        ledger.counters.snapshot_durable_count = 1;
        ledger.counters.snapshot_durable_bytes = bytes;
    }

    pub(crate) fn journal_durable(&self, bytes: u64) {
        self.ledger.lock().counters.journal_durable_bytes = bytes;
    }

    pub(crate) fn checkpoint_complete(&self) {
        self.ledger.lock().counters.checkpoint_complete_count = 1;
    }

    pub(crate) fn finish(mut self, succeeded: bool) {
        self.fold(succeeded, false);
        self.finished = true;
    }

    fn fold(&self, succeeded: bool, abandoned: bool) {
        let (ns, conversion_overflow) = nanos(self.started.elapsed());
        let mut ledger = self.ledger.lock();
        ledger.sealed = true;
        ledger.counters.operation_attempts = 1;
        ledger.counters.operation_failures = u64::from(!succeeded);
        ledger.counters.abandoned_operations = u64::from(abandoned);
        match self.kind {
            OperationKind::Checkpoint => {
                ledger.counters.checkpoint_attempts = 1;
                ledger.counters.checkpoint_elapsed_ns = ns;
            }
            OperationKind::JournalAppend => {
                ledger.counters.journal_append_attempts = 1;
                ledger.counters.journal_elapsed_ns = ns;
            }
        }
        self.owner.update(self.origin, self.medium, |bucket| {
            let active = bucket.active_operations.checked_sub(1);
            bucket.active_operations = active.unwrap_or(0);
            conversion_overflow
                | ledger.overflow
                | active.is_none()
                | bucket.counters.fold(&ledger.counters)
                | matches!(self.kind, OperationKind::Checkpoint)
                    .then(|| bucket.checkpoint_latency.record(ns))
                    .unwrap_or(false)
        });
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        if !self.finished {
            self.fold(false, true);
        }
    }
}

#[cfg(test)]
mod tests;
