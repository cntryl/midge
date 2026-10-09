//! Preserve exact manifest coverage without retaining complete SST bytes.

use crate::io::{Fs, FsPath};
use crate::sst::SstStateReader;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

mod candidate_index;
#[cfg(feature = "internal-testing")]
pub(crate) mod cost_probe;
#[cfg(feature = "internal-testing")]
mod key_index;
#[cfg(feature = "internal-testing")]
pub(crate) mod probe;

pub(crate) struct ReplayCoverage {
    #[cfg(feature = "internal-testing")]
    variant: probe::RecoveryProbeVariant,
    #[cfg(feature = "internal-testing")]
    facts: probe::Facts,
    #[cfg(feature = "internal-testing")]
    key_index: RefCell<Option<key_index::KeyIndex>>,
    #[cfg(feature = "internal-testing")]
    reads: Arc<probe::Reads>,
    #[cfg(feature = "internal-testing")]
    reads_attached: bool,
    manifest: crate::metadata::Manifest,
    fs: Arc<dyn Fs>,
    // Keep immutable identities plus a small LRU of budgeted readers.
    // Invariants: at most `MAX_READERS` readers, each retaining at most
    // `MAX_BLOCKS_PER_READER` decoded blocks and `block_cap_per_reader` bytes,
    // every byte charged to `read_budget`. All readers and proofs are dropped
    // before a checkpoint, so no reader outlives the manifest snapshot it was
    // verified against, and eviction under budget pressure only ever turns a
    // proof into a replay.
    verified: RefCell<HashMap<String, VerifiedProof>>,
    readers: RefCell<Vec<CachedReader>>,
    block_cap_per_reader: usize,
    read_budget: crate::common::resource_budget::ResourceBudget,
    candidate_index: RefCell<Option<candidate_index::CandidateIndex>>,
    probes: Cell<u64>,
    reader_opens: Cell<u64>,
    verified_bytes: Cell<u64>,
    elapsed_ns: Cell<u64>,
    block_hits: Cell<u64>,
    block_misses: Cell<u64>,
    block_peak: Cell<usize>,
    decode_steps: Cell<u64>,
    reconstruction_allocations: Cell<u64>,
    reader_hits: Cell<u64>,
    reader_evictions: Cell<u64>,
    manifest_scanned: Cell<u64>,
    manifest_candidates: Cell<u64>,
    progress: RefCell<crate::telemetry::recovery_progress::WorkProgress>,
}

/// Readers retained for alternating-SST replay locality.
const MAX_READERS: usize = 4;
/// Decoded blocks retained per reader.
const MAX_BLOCKS_PER_READER: usize = 4;

struct VerifiedProof {
    fs: Option<Arc<dyn Fs>>,
    _reservation: crate::common::resource_budget::ResourceReservation,
}

fn proof_metadata_bytes(name: &str) -> usize {
    // Cover hash-table growth (including overlapping old/new tables), keys,
    // and the immutable-view handle. Backend and allocator overhead remain
    // visible in process-memory qualification rather than hidden by pool sizes.
    std::mem::size_of::<(String, VerifiedProof)>()
        .saturating_mul(4)
        .saturating_add(name.len().saturating_mul(2))
        .saturating_add(256)
}

struct CachedReader {
    name: String,
    reader: crate::sst::fs::SstFileIo,
}

fn observe_budgeted(
    proof: &mut crate::runtime::hybrid_persistence::ExactCoverageState,
    retained_value: &mut Option<crate::common::resource_budget::ResourceReservation>,
    observed: crate::types::KeyState,
    budget: &crate::common::resource_budget::ResourceBudget,
) -> bool {
    use crate::types::KeyState;

    let (observed, replacement) = if proof.supersedes(&observed) {
        match observed {
            // Do not let the winning value pin an entire decoded block while
            // the next SST reader is constructed. Keep the old value charged
            // until observe replaces and drops its copied bytes.
            KeyState::Value(value, sequence, expiration, operation) => {
                let Ok(reservation) = budget.reserve(value.len(), "recovery coverage value") else {
                    return false;
                };
                (
                    KeyState::Value(
                        bytes::Bytes::copy_from_slice(&value),
                        sequence,
                        expiration,
                        operation,
                    ),
                    Some(reservation),
                )
            }
            other => (other, None),
        }
    } else {
        (observed, None)
    };
    if proof.observe(observed) {
        *retained_value = replacement;
    }
    true
}

fn check_scope(scope: Option<&crate::common::DeadlineScope>) -> crate::common::MidgeResult<()> {
    scope.map_or(Ok(()), |scope| scope.check("WAL SST coverage"))
}

fn conservative<T>(result: crate::common::MidgeResult<T>) -> crate::common::MidgeResult<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error @ crate::common::MidgeError::Timeout(_)) => Err(error),
        Err(_) => Ok(None),
    }
}

impl ReplayCoverage {
    pub(crate) fn new(
        manifest: crate::metadata::Manifest,
        fs: Arc<dyn Fs>,
        memory_bytes: usize,
    ) -> Self {
        let fs = crate::telemetry::recovery_progress::observe_reads(fs);
        #[cfg(feature = "internal-testing")]
        let reads = Arc::new(probe::Reads::default());
        #[cfg(feature = "internal-testing")]
        let observed = fs.with_read_observer(reads.clone());
        #[cfg(feature = "internal-testing")]
        let reads_attached = observed.is_some();
        #[cfg(feature = "internal-testing")]
        let fs = observed.unwrap_or(fs);
        Self {
            #[cfg(feature = "internal-testing")]
            variant: probe::RecoveryProbeVariant::default(),
            #[cfg(feature = "internal-testing")]
            facts: probe::Facts::default(),
            #[cfg(feature = "internal-testing")]
            key_index: RefCell::new(None),
            #[cfg(feature = "internal-testing")]
            reads,
            #[cfg(feature = "internal-testing")]
            reads_attached,
            manifest,
            fs,
            verified: RefCell::new(HashMap::new()),
            readers: RefCell::new(Vec::new()),
            // Retained decoded blocks may use at most a quarter of the budget
            // in total, leaving room for indexes, proofs and verification.
            block_cap_per_reader: memory_bytes / 4 / MAX_READERS,
            read_budget: crate::common::resource_budget::ResourceBudget::new(memory_bytes),
            candidate_index: RefCell::new(None),
            probes: Cell::new(0),
            reader_opens: Cell::new(0),
            verified_bytes: Cell::new(0),
            elapsed_ns: Cell::new(0),
            block_hits: Cell::new(0),
            block_misses: Cell::new(0),
            block_peak: Cell::new(0),
            decode_steps: Cell::new(0),
            reconstruction_allocations: Cell::new(0),
            reader_hits: Cell::new(0),
            reader_evictions: Cell::new(0),
            manifest_scanned: Cell::new(0),
            manifest_candidates: Cell::new(0),
            progress: RefCell::new(crate::telemetry::recovery_progress::WorkProgress::new(
                "coverage",
            )),
        }
    }

    #[cfg(feature = "internal-testing")]
    pub(crate) fn with_probe_variant(mut self, variant: probe::RecoveryProbeVariant) -> Self {
        self.variant = variant;
        self
    }

    fn reader_limit(&self) -> usize {
        #[cfg(feature = "internal-testing")]
        if self.variant == probe::RecoveryProbeVariant::SingleReader {
            return 1;
        }
        MAX_READERS
    }

    pub(crate) fn release_for_checkpoint(&self) {
        #[cfg(feature = "internal-testing")]
        probe::add(&self.facts.checkpoint_releases, 1);
        self.release_reader();
    }

    pub(crate) fn contains(&self, record: &crate::wal::WalRecord) -> bool {
        self.contains_core(record, None).unwrap_or(false)
    }

    pub(crate) fn contains_within(
        &self,
        record: &crate::wal::WalRecord,
        scope: &crate::common::DeadlineScope,
    ) -> crate::common::MidgeResult<bool> {
        self.contains_core(record, Some(scope))
    }

    fn contains_core(
        &self,
        record: &crate::wal::WalRecord,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> crate::common::MidgeResult<bool> {
        let started = std::time::Instant::now();
        check_scope(scope)?;
        self.probes.set(self.probes.get().saturating_add(1));
        let result = self.contains_record(record, scope)?;
        #[cfg(feature = "internal-testing")]
        if result {
            probe::add(&self.facts.exact_hits, 1);
        }
        check_scope(scope)?;
        self.elapsed_ns.set(
            self.elapsed_ns
                .get()
                .saturating_add(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)),
        );
        self.progress.borrow_mut().completed_operation();
        Ok(result)
    }

    fn contains_record(
        &self,
        record: &crate::wal::WalRecord,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> crate::common::MidgeResult<bool> {
        use crate::runtime::hybrid_persistence::{
            file_covers_wal_point_record, ExactCoverageState,
        };
        use crate::wal::types::WalOpRole;
        if !matches!(record.op.role(), WalOpRole::ValueWrite) {
            return Ok(false);
        }
        #[cfg(feature = "internal-testing")]
        probe::add(&self.facts.point_probes, 1);
        let mut retained_value = None;
        let mut proof = ExactCoverageState::default();
        let mut visit = |file: &crate::metadata::FileMeta| {
            check_scope(scope)?;
            if !file_covers_wal_point_record(file, record) {
                #[cfg(feature = "internal-testing")]
                probe::add(&self.facts.predicate_rejections, 1);
                self.progress.borrow_mut().completed_operation();
                return Ok(true);
            }
            self.manifest_candidates
                .set(self.manifest_candidates.get().saturating_add(1));
            let Some(observed) = self.file_state(file, record.key.as_ref(), scope)? else {
                return Ok(false);
            };
            if !observe_budgeted(&mut proof, &mut retained_value, observed, &self.read_budget) {
                return Ok(false);
            }
            check_scope(scope)?;
            self.progress.borrow_mut().completed_operation();
            Ok(true)
        };
        #[cfg(feature = "internal-testing")]
        let clock = probe::clock(self.variant);
        #[cfg(feature = "internal-testing")]
        let nested_before = self
            .facts
            .proof_inclusive_ns
            .get()
            .saturating_add(self.facts.index_build_ns.get());
        let complete = self.visit_candidates(record, scope, &mut visit);
        #[cfg(feature = "internal-testing")]
        {
            let nested = self
                .facts
                .proof_inclusive_ns
                .get()
                .saturating_add(self.facts.index_build_ns.get())
                .saturating_sub(nested_before);
            probe::add(
                &self.facts.candidate_ns,
                probe::elapsed(clock).saturating_sub(nested),
            );
        }
        if !complete? {
            return Ok(false);
        }
        Ok(proof.exactly_covers_wal_point(record))
    }

    fn visit_candidates(
        &self,
        record: &crate::wal::WalRecord,
        scope: Option<&crate::common::DeadlineScope>,
        visitor: &mut impl FnMut(&crate::metadata::FileMeta) -> crate::common::MidgeResult<bool>,
    ) -> crate::common::MidgeResult<bool> {
        #[cfg(feature = "internal-testing")]
        if self.variant == probe::RecoveryProbeVariant::KeyIndex {
            let mut index = self.key_index.borrow_mut();
            if index.is_none() {
                probe::add(&self.facts.index_build_attempts, 1);
                let _clock = probe::Phase::start(&self.facts.index_build_ns, self.variant);
                *index = key_index::KeyIndex::new(&self.manifest.files, &self.read_budget, scope)?;
                if index.is_some() {
                    probe::add(&self.facts.index_builds, 1);
                }
            }
            if let Some(index) = index.as_ref() {
                return index.visit(
                    &self.manifest.files,
                    record,
                    scope,
                    &self.manifest_scanned,
                    visitor,
                );
            }
            for file in &self.manifest.files {
                self.manifest_scanned
                    .set(self.manifest_scanned.get().saturating_add(1));
                if !visitor(file)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        let mut index = self.candidate_index.borrow_mut();
        if index.is_none() {
            #[cfg(feature = "internal-testing")]
            probe::add(&self.facts.index_build_attempts, 1);
            #[cfg(feature = "internal-testing")]
            let _clock = probe::Phase::start(&self.facts.index_build_ns, self.variant);
            *index = candidate_index::CandidateIndex::new(
                &self.manifest.files,
                &self.read_budget,
                scope,
            )?;
            #[cfg(feature = "internal-testing")]
            if index.is_some() {
                probe::add(&self.facts.index_builds, 1);
            }
        }
        if let Some(index) = index.as_ref() {
            if !index.visit(
                &self.manifest.files,
                record,
                scope,
                &self.manifest_scanned,
                visitor,
            )? {
                return Ok(false);
            }
        } else {
            // Tight budgets retain the old exact proof, without an uncharged
            // index or a newly inferred coverage decision.
            for file in &self.manifest.files {
                self.manifest_scanned
                    .set(self.manifest_scanned.get().saturating_add(1));
                if !visitor(file)? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn file_state(
        &self,
        file: &crate::metadata::FileMeta,
        key: &[u8],
        scope: Option<&crate::common::DeadlineScope>,
    ) -> crate::common::MidgeResult<Option<crate::types::KeyState>> {
        #[cfg(feature = "internal-testing")]
        let _clock = probe::Phase::start(&self.facts.proof_inclusive_ns, self.variant);
        check_scope(scope)?;
        let observed = self.try_file_state(file, key)?;
        check_scope(scope)?;
        if observed.is_none() && !self.readers.borrow().is_empty() {
            self.release_cached_all();
            let result = self.try_file_state(file, key)?;
            check_scope(scope)?;
            return Ok(result);
        }
        Ok(observed)
    }

    fn proof_fs(
        &self,
        file: &crate::metadata::FileMeta,
        path: &FsPath,
    ) -> crate::common::MidgeResult<Option<Arc<dyn Fs>>> {
        #[cfg(feature = "internal-testing")]
        let _clock = probe::Phase::start(&self.facts.identity_ns, self.variant);
        let Some(crc) = file.content_crc32c else {
            return Ok(None);
        };
        let Some(pinned) = conservative(
            self.fs
                .immutable_read_view(path)
                .map_err(crate::io::FsError::into_midge),
        )?
        .flatten() else {
            return Ok(None);
        };
        let window = self
            .read_budget
            .limit()
            .saturating_sub(self.read_budget.used())
            .min(usize::try_from(file.size_bytes).unwrap_or(usize::MAX));
        let Ok(_verification) = self
            .read_budget
            .reserve(window, "recovery SST verification")
        else {
            return Ok(None);
        };
        if conservative(super::streaming_wal_fs::validate_wal_source(
            pinned.as_ref(),
            path,
            file.size_bytes,
            crc,
            window,
        ))?
        .is_none()
        {
            return Ok(None);
        }
        self.verified_bytes
            .set(self.verified_bytes.get().saturating_add(file.size_bytes));
        Ok(Some(pinned))
    }

    fn try_file_state(
        &self,
        file: &crate::metadata::FileMeta,
        key: &[u8],
    ) -> crate::common::MidgeResult<Option<crate::types::KeyState>> {
        let mut readers = self.readers.borrow_mut();
        if let Some(position) = readers.iter().position(|cached| cached.name == file.name) {
            let cached = readers.remove(position);
            self.reader_hits
                .set(self.reader_hits.get().saturating_add(1));
            #[cfg(feature = "internal-testing")]
            let _clock = probe::Phase::start(&self.facts.point_ns, self.variant);
            let result = conservative(cached.reader.get_state_at_with_time(key, u64::MAX, 0));
            readers.push(cached);
            return result;
        }
        while readers.len() >= self.reader_limit() {
            let evicted = readers.remove(0);
            self.record_reader(&evicted);
            self.reader_evictions
                .set(self.reader_evictions.get().saturating_add(1));
        }
        let path = FsPath::new(crate::cloud_layout::object_key(&file.name));
        let mut verified = self.verified.borrow_mut();
        if !verified.contains_key(&file.name) {
            let Ok(reservation) = self.read_budget.reserve(
                proof_metadata_bytes(&file.name),
                "recovery immutable proof metadata",
            ) else {
                return Ok(None);
            };
            // A timed proof is not cached as a negative; cancellation must escape.
            let fs = self.proof_fs(file, &path)?;
            verified.insert(
                file.name.clone(),
                VerifiedProof {
                    fs,
                    _reservation: reservation,
                },
            );
        }
        let Some(fs) = verified.get(&file.name).and_then(|proof| proof.fs.as_ref()) else {
            return Ok(None);
        };
        self.reader_opens
            .set(self.reader_opens.get().saturating_add(1));
        let reader = {
            #[cfg(feature = "internal-testing")]
            let _clock = probe::Phase::start(&self.facts.reader_ns, self.variant);
            let Some(reader) = conservative(crate::sst::fs::SstFileIo::open_for_recovery(
                &path.0,
                Arc::clone(fs),
                self.read_budget.clone(),
                MAX_BLOCKS_PER_READER,
                self.block_cap_per_reader,
            ))?
            else {
                return Ok(None);
            };
            reader
        };
        #[cfg(feature = "internal-testing")]
        let _clock = probe::Phase::start(&self.facts.point_ns, self.variant);
        let result = conservative(reader.get_state_at_with_time(key, u64::MAX, 0));
        readers.push(CachedReader {
            name: file.name.clone(),
            reader,
        });
        #[cfg(feature = "internal-testing")]
        self.facts
            .peak_readers
            .set(self.facts.peak_readers.get().max(readers.len()));
        result
    }

    #[cfg(test)]
    fn block_stats(&self) -> (u64, u64, usize) {
        let mut totals = (
            self.block_hits.get(),
            self.block_misses.get(),
            self.block_peak.get(),
        );
        for cached in self.readers.borrow().iter() {
            let (hits, misses, peak) = cached.reader.recovery_block_stats();
            totals = (totals.0 + hits, totals.1 + misses, totals.2.max(peak));
        }
        totals
    }

    pub(crate) fn release_reader(&self) {
        self.release_cached_all();
        *self.verified.borrow_mut() = HashMap::new();
        *self.candidate_index.borrow_mut() = None;
        #[cfg(feature = "internal-testing")]
        {
            *self.key_index.borrow_mut() = None;
        }
    }

    fn release_cached_all(&self) {
        let drained: Vec<_> = self.readers.borrow_mut().drain(..).collect();
        for cached in drained {
            self.record_reader(&cached);
        }
    }

    fn record_reader(&self, cached: &CachedReader) {
        let (steps, allocations) = cached.reader.recovery_decode_stats();
        self.decode_steps
            .set(self.decode_steps.get().saturating_add(steps));
        self.reconstruction_allocations.set(
            self.reconstruction_allocations
                .get()
                .saturating_add(allocations),
        );
        let (hits, misses, peak) = cached.reader.recovery_block_stats();
        self.block_hits
            .set(self.block_hits.get().saturating_add(hits));
        self.block_misses
            .set(self.block_misses.get().saturating_add(misses));
        self.block_peak.set(self.block_peak.get().max(peak));
    }
}

impl Drop for ReplayCoverage {
    fn drop(&mut self) {
        self.release_reader();
        #[cfg(feature = "internal-testing")]
        {
            let facts = serde_json::json!({
                "variant": self.variant, "manifest_files": self.manifest.files.len(),
                "point_probes": self.facts.point_probes.get(), "exact_hits": self.facts.exact_hits.get(),
                "predicate_rejections": self.facts.predicate_rejections.get(), "index_build_attempts": self.facts.index_build_attempts.get(),
                "index_builds": self.facts.index_builds.get(), "index_build_ns": self.facts.index_build_ns.get(),
                "candidate_ns": self.facts.candidate_ns.get(), "identity_ns": self.facts.identity_ns.get(),
                "reader_ns": self.facts.reader_ns.get(), "point_ns": self.facts.point_ns.get(),
                "proof_inclusive_ns": self.facts.proof_inclusive_ns.get(), "checkpoint_releases": self.facts.checkpoint_releases.get(),
                "peak_readers": self.facts.peak_readers.get(), "reader_limit": self.reader_limit(),
                "budget_limit": self.read_budget.limit(), "budget_peak": self.read_budget.peak(), "budget_final": self.read_budget.used(),
                "remote_observer_attached": self.reads_attached, "remote_ranges": self.reads.snapshot(),
            });
            tracing::info!(target: "midge::recovery", phase = "coverage_attribution", attribution = %facts, "bounded recovery attribution");
        }
        tracing::info!(target: "midge::recovery", phase = "coverage",
            probes = self.probes.get(), reader_opens = self.reader_opens.get(),
            verified_sst_bytes = self.verified_bytes.get(), elapsed_ns = self.elapsed_ns.get(),
            reader_hits = self.reader_hits.get(), reader_evictions = self.reader_evictions.get(),
            manifest_files_scanned = self.manifest_scanned.get(),
            manifest_candidates = self.manifest_candidates.get(),
            block_hits = self.block_hits.get(), block_misses = self.block_misses.get(),
            retained_block_bytes_peak = self.block_peak.get() as u64,
            decoded_entries = self.decode_steps.get(),
            key_reconstruction_allocations = self.reconstruction_allocations.get(),
            "recovery coverage work completed");
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "internal-testing")]
    mod probe_variants;
    use super::*;
    use crate::sst::SstFactory;
    use crate::wal::{WalOpKind, WalRecord};
    use bytes::Bytes;

    mod block_decode_work;
    mod streaming_scale;

    #[derive(Default)]
    struct RangeCounter(std::sync::atomic::AtomicUsize);

    impl crate::io::traits::ReadObserver for RangeCounter {
        fn remote_range_started(&self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        fn remote_range_completed(
            &self,
            _bytes: u64,
            _elapsed: std::time::Duration,
            _failed: bool,
        ) {
        }
    }

    #[test]
    fn should_reject_cancelled_startup_scope_when_exact_coverage_is_cached() {
        // Arrange: warm real remote bytes, CRC proof and the exact SST reader.
        let (directory, coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let record = put(7, None);
        let healthy =
            crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded());
        assert!(coverage.contains_within(&record, &healthy).unwrap());
        let reads_before = coverage.reader_opens.get();
        let verified_before = coverage.verified_bytes.get();
        let names_before: Vec<_> = std::fs::read_dir(directory.path().join("cloud/sst"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        let scope =
            crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded());
        scope.cancel();

        // Act: cancellation must be checked even when no filesystem call occurs.
        let result = coverage.contains_within(&record, &scope);

        // Assert: a cached true proof cannot swallow Timeout or trigger fallback.
        assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
        assert_eq!(coverage.reader_opens.get(), reads_before);
        assert_eq!(coverage.verified_bytes.get(), verified_before);
        assert!(coverage.contains_within(&record, &healthy).unwrap());
        assert_eq!(
            std::fs::read_dir(directory.path().join("cloud/sst"))
                .unwrap()
                .count(),
            names_before.len()
        );
    }

    struct ExpiringSuccessfulRange {
        completed: std::sync::atomic::AtomicUsize,
        completed_bytes: std::sync::atomic::AtomicU64,
        deadline: crate::common::OperationDeadline,
    }

    impl crate::io::traits::ReadObserver for ExpiringSuccessfulRange {
        fn remote_range_started(&self) {}

        fn remote_range_completed(&self, bytes: u64, _elapsed: std::time::Duration, failed: bool) {
            if !failed && bytes > 0 {
                self.completed
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                self.completed_bytes
                    .fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
                // Delay delivery after an actual successful range. This is a
                // controlled adapter timing test, not a provider timeout claim.
                while !self.deadline.is_expired() {
                    std::thread::sleep(self.deadline.remaining());
                }
            }
        }
    }

    #[test]
    fn should_preserve_typed_coverage_timeout_without_caching_a_negative_proof() {
        // Arrange: the genuine remote SST remains available and unchanged.
        let (directory, mut coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let original_fs = Arc::clone(&coverage.fs);
        let path = directory
            .path()
            .join("cloud/sst")
            .join(&coverage.manifest.files[0].name);
        let before = std::fs::read(path.clone()).unwrap();
        let record = put(7, None);
        let deadline =
            crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5));
        let scope = crate::common::DeadlineScope::new(deadline);
        let observer = Arc::new(ExpiringSuccessfulRange {
            completed: std::sync::atomic::AtomicUsize::new(0),
            completed_bytes: std::sync::atomic::AtomicU64::new(0),
            deadline,
        });
        let delayed_fs = original_fs.with_read_observer(observer.clone()).unwrap();
        coverage.fs = crate::io::scope_fs(delayed_fs, scope.clone());

        // Act: a completed range arrives after the shared budget expires.
        let result = coverage.contains_within(&record, &scope);

        // Assert: the typed error escapes rather than becoming cached false.
        assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
        assert!(deadline.is_expired());
        assert_eq!(
            observer
                .completed
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        assert_eq!(
            observer
                .completed_bytes
                .load(std::sync::atomic::Ordering::Acquire),
            u64::try_from(before.len()).unwrap()
        );
        assert!(coverage.verified.borrow().is_empty());
        assert_eq!(std::fs::read(path).unwrap(), before);
        coverage.fs = original_fs;
        let healthy =
            crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded());
        assert!(coverage.contains_within(&record, &healthy).unwrap());
        assert!(coverage.verified_bytes.get() > 0);
    }

    #[test]
    fn should_reuse_bounded_reader_when_repeated_records_probe_same_sst() {
        // Arrange
        let (_dir, mut coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let counter = Arc::new(RangeCounter::default());
        coverage.fs = coverage
            .fs
            .with_read_observer(counter.clone())
            .expect("observed reads");
        let record = put(7, None);
        assert!(coverage.contains(&record));
        let initial = counter.0.load(std::sync::atomic::Ordering::Relaxed);

        // Act
        for _ in 0..100 {
            assert!(coverage.contains(&record));
        }
        let subsequent = counter.0.load(std::sync::atomic::Ordering::Relaxed) - initial;

        // Assert
        assert!(initial > 0, "must exercise real remote range reads");
        assert!(
            subsequent == 0,
            "validated data block must stay resident: {subsequent} ranges for 100 probes"
        );
    }

    #[test]
    fn should_release_coverage_reservations_when_recovery_yields_to_checkpoint() {
        // Arrange
        let (_dir, coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let record = put(7, None);
        assert!(coverage.contains(&record));
        assert!(coverage.read_budget.used() > 0);
        let verified_bytes = coverage.verified_bytes.get();

        // Act
        coverage.release_reader();
        let checkpoint_charge = coverage.read_budget.used();
        let covered_again = coverage.contains(&record);

        // Assert
        assert_eq!(
            checkpoint_charge, 0,
            "checkpoint must have no retained reader charge"
        );
        assert!(covered_again);
        assert_eq!(
            coverage.verified_bytes.get(),
            verified_bytes * 2,
            "checkpoint release must discard charged proof metadata"
        );
        assert_eq!(coverage.reader_opens.get(), 2);
    }

    #[test]
    fn should_release_previous_reader_when_overlapping_ssts_share_one_reader_budget() {
        // Arrange
        let (_single_dir, single) = fixture(&[(Some(b"value"), 7, None)]);
        let record = put(7, None);
        assert!(single.contains(&record));
        let one_reader_peak = single.read_budget.peak();
        assert!(one_reader_peak > 0);
        let (_dir, mut coverage) = fixture(&[(Some(b"value"), 7, None), (Some(b"value"), 7, None)]);
        // The incumbent comparison value remains live across SST readers;
        // its five bytes are charged separately from either decoded block.
        let budget = one_reader_peak
            + b"value".len()
            + proof_metadata_bytes(&coverage.manifest.files[1].name)
            // The single-reader peak already includes its one-file index.
            + candidate_index::CandidateIndex::allocation_bytes(coverage.manifest.files.len())
                .expect("small fixture index")
            - candidate_index::CandidateIndex::allocation_bytes(single.manifest.files.len())
                .expect("single fixture index");
        coverage.read_budget = crate::common::resource_budget::ResourceBudget::new(budget);

        // Act
        for _ in 0..10 {
            assert!(
                coverage.contains(&record),
                "each overlapping SST must fit sequentially"
            );
        }
        coverage.release_reader();

        // Assert
        assert_eq!(coverage.read_budget.used(), 0);
        assert!(coverage.read_budget.peak() <= budget);
        assert!(
            coverage.reader_opens.get() > 2,
            "budget pressure must evict retained readers rather than exceed the budget"
        );
    }

    #[test]
    fn should_keep_prior_value_charged_until_replacement_is_installed() {
        // Arrange
        let budget = crate::common::resource_budget::ResourceBudget::new(15);
        let mut proof = crate::runtime::hybrid_persistence::ExactCoverageState::default();
        let mut retained_value = None;
        let value = |sequence| {
            crate::types::KeyState::Value(
                Bytes::from_static(b"0123456789"),
                sequence,
                None,
                crate::types::EntryType::Put,
            )
        };
        assert!(observe_budgeted(
            &mut proof,
            &mut retained_value,
            value(7),
            &budget,
        ));
        assert_eq!(budget.used(), 10);

        // Act
        let replaced = observe_budgeted(&mut proof, &mut retained_value, value(9), &budget);

        // Assert
        assert!(
            !replaced,
            "both live copied values must fit before replacement"
        );
        assert_eq!(budget.used(), 10, "the prior value remains charged");
        assert!(
            proof.supersedes(&value(9)),
            "the prior state remains installed"
        );
    }

    #[test]
    fn should_replay_when_aggregate_proof_metadata_exhausts_shared_recovery_budget() {
        // Arrange
        let entries = vec![(Some(b"value".as_slice()), 7, None); 100];
        let (_dir, mut coverage) = fixture(&entries);
        coverage.read_budget = crate::common::resource_budget::ResourceBudget::new(8 * 1024);

        // Act
        let covered = coverage.contains(&put(7, None));
        let retained = coverage.read_budget.used();
        coverage.release_reader();

        // Assert
        assert!(
            !covered,
            "aggregate immutable proof metadata must share the configured budget"
        );
        assert!(retained <= 8 * 1024);
        assert_eq!(coverage.read_budget.used(), 0);
    }

    #[test]
    fn should_retain_locality_when_probes_alternate_between_ssts() {
        // Arrange
        let (_dir, mut coverage) = multi_block_fixture(2, &[b"a", b"z"]);
        let counter = Arc::new(RangeCounter::default());
        coverage.fs = coverage
            .fs
            .with_read_observer(counter.clone())
            .expect("observed reads");
        let records = [put_key(b"a", 7), put_key(b"z", 7)];
        for record in &records {
            assert!(coverage.contains(record));
        }
        let warm = counter.0.load(std::sync::atomic::Ordering::Relaxed);
        let warm_misses = coverage.block_stats().1;

        // Act
        for _ in 0..50 {
            for record in &records {
                assert!(coverage.contains(record));
            }
        }
        let ranges = counter.0.load(std::sync::atomic::Ordering::Relaxed) - warm;
        let (hits, misses, _) = coverage.block_stats();

        // Assert
        assert_eq!(coverage.reader_opens.get(), 2, "one open per SST");
        assert_eq!(ranges, 0, "alternation must not reissue range reads");
        assert_eq!(misses, warm_misses, "every block stays resident");
        assert!(hits >= 200, "block hits must be counted: {hits}");
        assert!(coverage.read_budget.peak() <= coverage.read_budget.limit());
    }

    #[test]
    fn should_replay_when_unreadable_interval_overlaps_verified_exact_sst() {
        // Arrange: a valid first proof must not conceal a second overlapping
        // manifest candidate whose authoritative object cannot be read.
        let (directory, coverage) =
            fixture(&[(Some(b"value"), 7, None), (Some(b"value"), 7, None)]);
        std::fs::remove_file(
            directory
                .path()
                .join("cloud/sst")
                .join(&coverage.manifest.files[1].name),
        )
        .expect("remove overlapping authoritative SST");

        // Act
        let covered = coverage.contains(&put(7, None));

        // Assert
        assert!(!covered, "every candidate must supply an exact proof");
        assert_eq!(coverage.reader_opens.get(), 1, "first SST was verified");
        assert_eq!(coverage.manifest_candidates.get(), 2);
        coverage.release_reader();
        assert_eq!(coverage.read_budget.used(), 0);
    }

    #[test]
    fn should_preserve_exact_proof_when_candidate_index_cannot_fit_read_budget() {
        // Arrange: metadata indexing exceeds this budget, but one real SST
        // proof fits. Disjoint intervals may not force a new replay decision.
        let (_directory, mut coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let candidate = coverage.manifest.files[0].clone();
        for index in 0..8192_u64 {
            coverage.manifest.files.push(crate::metadata::FileMeta {
                name: format!("irrelevant-{index}.sst"),
                smallest_seq: Some(10 + index * 10),
                largest_seq: Some(19 + index * 10),
                ..candidate.clone()
            });
        }

        // Act
        let covered = coverage.contains(&put(7, None));

        // Assert
        assert!(covered);
        assert!(coverage.candidate_index.borrow().is_none());
        assert_eq!(coverage.manifest_scanned.get(), 8193);
        assert_eq!(coverage.reader_opens.get(), 1);
        assert!(coverage.read_budget.peak() <= coverage.read_budget.limit());
        coverage.release_reader();
        assert_eq!(coverage.read_budget.used(), 0);
    }

    #[test]
    fn should_bound_manifest_work_when_replay_probes_sparse_sequence_intervals() {
        // Arrange: the candidate is a real checksummed SST. The other entries
        // describe disjoint later sequence intervals and must never be read.
        let (_directory, mut coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let candidate = coverage.manifest.files[0].clone();
        for index in 0..8192_u64 {
            coverage.manifest.files.push(crate::metadata::FileMeta {
                name: format!("irrelevant-{index}.sst"),
                smallest_seq: Some(10 + index * 10),
                largest_seq: Some(19 + index * 10),
                ..candidate.clone()
            });
        }
        let record = put(7, None);

        coverage.read_budget = crate::common::resource_budget::ResourceBudget::new(512 * 1024);

        // Act: count the production candidate-selection work, rather than
        // asserting a machine-dependent elapsed time.
        for _ in 0..100 {
            assert!(coverage.contains(&record));
        }

        // Assert: exact coverage is retained without walking all 8193 entries
        // for each record in a multi-million-row recovery backlog.
        assert_eq!(coverage.probes.get(), 100);
        assert_eq!(coverage.manifest_candidates.get(), 100);
        assert!(
            coverage.manifest_scanned.get() <= 64 * 100,
            "sparse candidate lookup scanned {} entries for 100 probes",
            coverage.manifest_scanned.get()
        );
        assert_eq!(coverage.reader_opens.get(), 1);
        eprintln!(
            "actual sparse coverage work: probes={}, scanned={}, candidates={}, reader_opens={}",
            coverage.probes.get(),
            coverage.manifest_scanned.get(),
            coverage.manifest_candidates.get(),
            coverage.reader_opens.get()
        );
    }

    #[test]
    fn should_report_manifest_candidate_work_when_records_probe_manifest() {
        // Arrange
        let (_dir, coverage) = multi_block_fixture(3, &[b"a"]);

        // Act
        let covered = coverage.contains(&put_key(b"a", 7));

        // Assert
        assert!(covered);
        assert_eq!(coverage.manifest_scanned.get(), 3);
        assert_eq!(coverage.manifest_candidates.get(), 3);
    }

    #[test]
    fn should_stay_within_budget_when_retained_readers_exceed_recovery_memory() {
        // Arrange
        let (_dir, mut coverage) = multi_block_fixture(4, &[b"a", b"z"]);
        let (_one_dir, one) = multi_block_fixture(1, &[b"a", b"z"]);
        assert!(one.contains(&put_key(b"a", 7)));
        let budget = one.read_budget.peak()
            + 12 * 1024
            + coverage
                .manifest
                .files
                .iter()
                .map(|f| proof_metadata_bytes(&f.name))
                .sum::<usize>();
        coverage.read_budget = crate::common::resource_budget::ResourceBudget::new(budget);

        // Act
        let covered = [b"a", b"z", b"a", b"z"]
            .iter()
            .all(|key| coverage.contains(&put_key(*key, 7)));
        let peak = coverage.read_budget.peak();
        coverage.release_reader();

        // Assert
        assert!(covered, "eviction must keep exact coverage provable");
        assert!(peak <= budget);
        assert!(
            coverage.reader_opens.get() > 4,
            "the budget must be tight enough to force eviction"
        );
        assert_eq!(coverage.read_budget.used(), 0);
    }

    fn put_key(key: &'static [u8], sequence: u64) -> WalRecord {
        WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(key),
            Some(Bytes::from(vec![7u8; 4096])),
            sequence,
            1,
        )
    }

    /// `files` identical SSTs, each with one 4 KiB value per key at sequence 7
    /// and a block size small enough that every key lands in its own block.
    fn multi_block_fixture(files: usize, keys: &[&[u8]]) -> (tempfile::TempDir, ReplayCoverage) {
        let dir = tempfile::tempdir().expect("coverage directory");
        let fs = Arc::new(crate::io::RealFs::new(dir.path()).expect("local filesystem"));
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 128);
        let mut manifest = crate::metadata::Manifest::default();
        std::fs::create_dir_all(dir.path().join("cloud/sst")).expect("remote SST directory");
        for index in 0..files {
            let mut writer = factory.create().expect("SST writer");
            for key in keys {
                writer
                    .add_with_meta(
                        key,
                        Some(&[7u8; 4096]),
                        7,
                        crate::types::EntryType::Put,
                        None,
                    )
                    .expect("SST entry");
            }
            let bytes = writer.finish_bytes().expect("SST bytes");
            let name = crate::cloud_layout::file_name(0, 0, index as u64 + 1);
            std::fs::write(dir.path().join("cloud/sst").join(&name), &bytes).expect("remote SST");
            manifest.files.push(crate::metadata::FileMeta {
                name,
                cf_id: 0,
                level: 0,
                size_bytes: bytes.len() as u64,
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                smallest_key: keys.first().map(|k| k.to_vec()),
                largest_key: keys.last().map(|k| k.to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(7),
                ..Default::default()
            });
        }
        let cloud = Arc::new(
            crate::storage::filesystem::FileSystem::new(dir.path().join("cloud"))
                .expect("cloud filesystem"),
        );
        let remote = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
            fs,
            cloud,
            std::time::Duration::from_secs(5),
        ));
        (dir, ReplayCoverage::new(manifest, remote, 512 * 1024))
    }

    type PersistedEntry<'a> = (Option<&'a [u8]>, u64, Option<u64>);

    fn fixture(entries: &[PersistedEntry<'_>]) -> (tempfile::TempDir, ReplayCoverage) {
        let dir = tempfile::tempdir().expect("coverage directory");
        let fs = Arc::new(crate::io::RealFs::new(dir.path()).expect("local filesystem"));
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 4096);
        let mut manifest = crate::metadata::Manifest::default();
        std::fs::create_dir_all(dir.path().join("cloud/sst")).expect("remote SST directory");
        for (index, (value, sequence, expiration)) in entries.iter().enumerate() {
            let mut writer = factory.create().expect("SST writer");
            let operation = if value.is_some() {
                crate::types::EntryType::Put
            } else {
                crate::types::EntryType::Delete
            };
            writer
                .add_with_meta(b"key", *value, *sequence, operation, *expiration)
                .expect("SST entry");
            let bytes = writer.finish_bytes().expect("SST bytes");
            let name = crate::cloud_layout::file_name(0, 0, index as u64 + 1);
            std::fs::write(dir.path().join("cloud/sst").join(&name), &bytes).expect("remote SST");
            manifest.files.push(crate::metadata::FileMeta {
                name,
                cf_id: 0,
                level: 0,
                size_bytes: bytes.len() as u64,
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                smallest_key: Some(b"key".to_vec()),
                largest_key: Some(b"key".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(*sequence),
                ..Default::default()
            });
        }
        let cloud = Arc::new(
            crate::storage::filesystem::FileSystem::new(dir.path().join("cloud"))
                .expect("cloud filesystem"),
        );
        let remote = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
            fs,
            cloud,
            std::time::Duration::from_secs(5),
        ));
        (dir, ReplayCoverage::new(manifest, remote, 128 * 1024))
    }

    fn put(sequence: u64, expiration: Option<u64>) -> WalRecord {
        let mut record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            sequence,
            1,
        );
        record.expiration = expiration;
        record
    }

    #[test]
    fn should_agree_with_shared_wal_coverage_rule_given_same_manifest_and_record() {
        // Arrange
        type Scenario<'a> = (&'a str, Vec<PersistedEntry<'a>>, WalRecord);
        let scenarios: Vec<Scenario<'_>> = vec![
            (
                "exact version",
                vec![(Some(b"value"), 7, None)],
                put(7, None),
            ),
            (
                "newer version",
                vec![(Some(b"newer"), 9, None)],
                put(7, None),
            ),
            ("newer tombstone", vec![(None, 9, None)], put(7, None)),
            (
                "expired exact version",
                vec![(Some(b"value"), 7, Some(1))],
                put(7, Some(1)),
            ),
            (
                "expiration differs",
                vec![(Some(b"value"), 7, Some(u64::MAX))],
                put(7, None),
            ),
            (
                "equal-sequence tombstone",
                vec![(None, 7, None)],
                put(7, None),
            ),
            (
                "disagreeing equal-sequence files",
                vec![(Some(b"value"), 7, None), (Some(b"conflicting"), 7, None)],
                put(7, None),
            ),
            (
                "older version only",
                vec![(Some(b"value"), 5, None)],
                put(7, None),
            ),
        ];

        for (name, entries, record) in scenarios {
            let (_dir, startup) = fixture(&entries);
            let mut proven = crate::runtime::hybrid_persistence::ProvenSstIdentities::default();
            let shared = crate::runtime::hybrid_persistence::VerifiedManifestWalCoverage::open(
                Arc::clone(&startup.fs),
                crate::cloud_layout::CloudObjectLayout::SST_PREFIX,
                &startup.manifest,
                &mut proven,
            );

            // Act
            let startup_covered = startup.contains(&record);
            let shared_covered = shared.covers_wal_record(&record);

            // Assert
            assert_eq!(
                startup_covered, shared_covered,
                "{name}: cloud startup replay and the shared WAL coverage rule must agree"
            );
        }
    }

    #[test]
    fn should_replay_put_when_equal_sequence_sst_expiration_differs() {
        // Arrange
        let (_dir, coverage) = fixture(&[(Some(b"value"), 7, Some(u64::MAX))]);
        let record = put(7, None);
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            !covered,
            "equal value bytes cannot replace persisted TTL metadata"
        );
    }

    #[test]
    fn should_replay_put_when_equal_sequence_sst_contains_tombstone() {
        // Arrange
        let (_dir, coverage) = fixture(&[(None, 7, None)]);
        let record = put(7, None);
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            !covered,
            "a contradictory same-sequence tombstone is not proof of the WAL put"
        );
    }

    #[test]
    fn should_replay_put_when_verified_ssts_disagree_at_same_latest_sequence() {
        // Arrange
        let (_dir, coverage) =
            fixture(&[(Some(b"value"), 7, None), (Some(b"conflicting"), 7, None)]);
        let record = put(7, None);
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            !covered,
            "one matching file must not hide contradictory authority"
        );
    }

    #[test]
    fn should_prove_matching_expired_value_without_using_recovery_wall_clock() {
        // Arrange
        let (_dir, coverage) = fixture(&[(Some(b"value"), 7, Some(1))]);
        let record = put(7, Some(1));
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            covered,
            "raw value plus expiration remain authoritative across clock changes"
        );
    }

    #[test]
    fn should_replay_deletes_even_when_same_sequence_state_is_persisted() {
        // Arrange
        let (_dir, coverage) = fixture(&[(None, 7, None)]);
        let mut record = put(7, None);
        record.op = WalOpKind::Delete;
        record.value = None;
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(!covered, "delete replay stays conservative");
    }
    #[test]
    fn should_retain_wal_when_exact_sst_proof_cannot_fit_recovery_memory_budget() {
        // Arrange
        let (_dir, mut coverage) = fixture(&[(Some(b"value"), 7, None)]);
        coverage.read_budget = crate::common::resource_budget::ResourceBudget::new(32);
        let record = put(7, None);
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(!covered, "proof exhaustion must preserve replayable WAL");
    }

    #[test]
    fn should_retain_wal_when_pinned_sst_identity_changes_before_block_reload() {
        // Arrange
        let (dir, coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let record = put(7, None);
        assert!(coverage.contains(&record));
        let path = dir
            .path()
            .join("cloud/sst")
            .join(&coverage.manifest.files[0].name);
        let mut bytes = std::fs::read(&path).expect("original SST");
        bytes[0] ^= 1;
        std::fs::write(path, bytes).expect("replace immutable object");
        // Act
        let cached = coverage.contains(&record);
        coverage.release_reader();
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            cached,
            "validated bytes remain tied to the original immutable identity"
        );
        assert!(
            !covered,
            "cached identity cannot prove replacement contents"
        );
    }
}
