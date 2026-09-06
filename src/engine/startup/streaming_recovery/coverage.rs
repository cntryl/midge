//! Preserve exact manifest coverage without retaining complete SST bytes.

use crate::io::{Fs, FsPath};
use crate::sst::SstStateReader;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct ReplayCoverage {
    manifest: crate::metadata::Manifest,
    fs: Arc<dyn Fs>,
    // Keep immutable identities and at most one budgeted reader. Release the
    // reader before verifying/opening a different file or building a checkpoint.
    verified: RefCell<HashMap<String, Option<Arc<dyn Fs>>>>,
    reader: RefCell<Option<CachedReader>>,
    read_budget: crate::common::resource_budget::ResourceBudget,
    probes: Cell<u64>,
    reader_opens: Cell<u64>,
    verified_bytes: Cell<u64>,
    elapsed_ns: Cell<u64>,
    block_hits: Cell<u64>,
    block_misses: Cell<u64>,
    block_peak: Cell<usize>,
}

struct CachedReader {
    name: String,
    reader: crate::sst::fs::SstFileIo,
}

impl ReplayCoverage {
    pub(super) fn new(
        manifest: crate::metadata::Manifest,
        fs: Arc<dyn Fs>,
        memory_bytes: usize,
    ) -> Self {
        Self {
            manifest,
            fs,
            verified: RefCell::new(HashMap::new()),
            reader: RefCell::new(None),
            read_budget: crate::common::resource_budget::ResourceBudget::new(memory_bytes),
            probes: Cell::new(0),
            reader_opens: Cell::new(0),
            verified_bytes: Cell::new(0),
            elapsed_ns: Cell::new(0),
            block_hits: Cell::new(0),
            block_misses: Cell::new(0),
            block_peak: Cell::new(0),
        }
    }

    pub(super) fn contains(&self, record: &crate::wal::WalRecord) -> bool {
        let started = std::time::Instant::now();
        self.probes.set(self.probes.get().saturating_add(1));
        let result = self.contains_record(record);
        self.elapsed_ns.set(
            self.elapsed_ns
                .get()
                .saturating_add(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)),
        );
        result
    }

    fn contains_record(&self, record: &crate::wal::WalRecord) -> bool {
        use crate::sst::types::KeyState;
        use crate::wal::types::WalOpRole;
        // Keep the existing conservative rule: tombstones are always replayed.
        if !matches!(record.op.role(), WalOpRole::ValueWrite) {
            return false;
        }
        let mut highest: Option<KeyState> = None;
        let mut highest_reservation = None;
        let mut ambiguous = false;
        for file in self
            .manifest
            .files
            .iter()
            .filter(|file| candidate(file, record))
        {
            let Some(observed) = self.file_state(file, record.key.as_ref()) else {
                return false;
            };
            let Some(sequence) = state_sequence(&observed) else {
                continue;
            };
            match highest.as_ref().and_then(state_sequence) {
                Some(current) if sequence < current => {}
                Some(current) if sequence == current => {
                    ambiguous |= highest.as_ref() != Some(&observed);
                }
                _ => {
                    // Do not let the winning value pin an entire decoded block
                    // while the next SST reader is constructed.
                    let (observed, reservation) = match observed {
                        KeyState::Value(value, sequence, expiration, operation) => {
                            let Ok(reservation) = self
                                .read_budget
                                .reserve(value.len(), "recovery coverage value")
                            else {
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
                    };
                    highest = Some(observed);
                    highest_reservation = reservation;
                    ambiguous = false;
                }
            }
        }
        if ambiguous {
            return false;
        }
        let covered = match highest {
            Some(KeyState::Value(value, sequence, expiration, operation)) => {
                sequence > record.seq
                    || sequence == record.seq
                        && record.value.as_ref() == Some(&value)
                        && record.expiration == expiration
                        && crate::wal::WalOpKind::from_wire_format(operation)
                            .is_ok_and(|op| matches!(op.role(), WalOpRole::ValueWrite))
            }
            Some(KeyState::Tombstone(sequence)) => sequence > record.seq,
            Some(KeyState::Absent) | None => false,
        };
        drop(highest_reservation);
        covered
    }

    fn file_state(
        &self,
        file: &crate::metadata::FileMeta,
        key: &[u8],
    ) -> Option<crate::sst::types::KeyState> {
        let mut cached = self.reader.borrow_mut();
        if let Some(cached) = cached.as_ref().filter(|cached| cached.name == file.name) {
            return cached.reader.get_state_at_with_time(key, u64::MAX, 0).ok();
        }
        // The old reader's reservations must be released before even the
        // next full-object verification buffer is allocated.
        self.release_cached(&mut cached);
        let path = FsPath::new(crate::sst::object_key(&file.name));
        let mut verified = self.verified.borrow_mut();
        let fs = verified.entry(file.name.clone()).or_insert_with(|| {
            let crc = file.content_crc32c?;
            let pinned = self.fs.immutable_read_view(&path).ok()??;
            let window = self
                .read_budget
                .limit()
                .saturating_sub(self.read_budget.used())
                .min(usize::try_from(file.size_bytes).unwrap_or(usize::MAX));
            let _verification = self
                .read_budget
                .reserve(window, "recovery SST verification")
                .ok()?;
            super::super::streaming_wal_fs::validate_wal_source(
                pinned.as_ref(),
                &path,
                file.size_bytes,
                crc,
                window,
            )
            .ok()?;
            self.verified_bytes
                .set(self.verified_bytes.get().saturating_add(file.size_bytes));
            Some(pinned)
        });
        let fs = fs.as_ref()?;
        let budget = self.read_budget.clone();
        self.reader_opens
            .set(self.reader_opens.get().saturating_add(1));
        let reader =
            crate::sst::fs::SstFileIo::open_for_recovery(&path.0, Arc::clone(fs), budget).ok()?;
        // Recovery compares persisted expiration metadata. A forward wall-clock
        // jump must not turn an unrelated expired value into tombstone proof.
        // Expiration zero remains conservatively replayed at equal sequence.
        let result = reader.get_state_at_with_time(key, u64::MAX, 0).ok();
        *cached = Some(CachedReader {
            name: file.name.clone(),
            reader,
        });
        result
    }

    pub(super) fn release_reader(&self) {
        self.release_cached(&mut self.reader.borrow_mut());
    }

    fn release_cached(&self, cached: &mut Option<CachedReader>) {
        if let Some(cached) = cached.take() {
            let (hits, misses, peak) = cached.reader.recovery_block_stats();
            self.block_hits
                .set(self.block_hits.get().saturating_add(hits));
            self.block_misses
                .set(self.block_misses.get().saturating_add(misses));
            self.block_peak.set(self.block_peak.get().max(peak));
        }
    }
}

impl Drop for ReplayCoverage {
    fn drop(&mut self) {
        self.release_reader();
        tracing::info!(target: "midge::recovery", phase = "coverage",
            probes = self.probes.get(), reader_opens = self.reader_opens.get(),
            verified_sst_bytes = self.verified_bytes.get(), elapsed_ns = self.elapsed_ns.get(),
            block_hits = self.block_hits.get(), block_misses = self.block_misses.get(),
            retained_block_bytes_peak = self.block_peak.get() as u64,
            "recovery coverage work completed");
    }
}

fn state_sequence(state: &crate::sst::types::KeyState) -> Option<u64> {
    match state {
        crate::sst::types::KeyState::Value(_, sequence, _, _)
        | crate::sst::types::KeyState::Tombstone(sequence) => Some(*sequence),
        crate::sst::types::KeyState::Absent => None,
    }
}

fn candidate(file: &crate::metadata::FileMeta, record: &crate::wal::WalRecord) -> bool {
    file.cf_id == record.cf_id
        && file
            .smallest_seq
            .is_some_and(|sequence| sequence <= record.seq)
        && file
            .largest_seq
            .is_some_and(|sequence| sequence >= record.seq)
        && file
            .smallest_key
            .as_ref()
            .is_some_and(|key| key.as_slice() <= record.key.as_ref())
        && file
            .largest_key
            .as_ref()
            .is_some_and(|key| key.as_slice() >= record.key.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::SstFactory;
    use crate::wal::{WalOpKind, WalRecord};
    use bytes::Bytes;

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
    fn should_release_reader_reservations_when_recovery_yields_to_checkpoint() {
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
            verified_bytes,
            "reuse pinned full-object proof"
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
        let budget = one_reader_peak + b"value".len();
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
        assert_eq!(coverage.reader_opens.get(), 20);
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
                WalOpKind::Put
            } else {
                WalOpKind::Delete
            };
            writer
                .add_with_meta(
                    b"key",
                    *value,
                    *sequence,
                    operation.to_wire_format(),
                    *expiration,
                )
                .expect("SST entry");
            let bytes = writer.finish_bytes().expect("SST bytes");
            let name = crate::sst::file_name(0, 0, index as u64 + 1);
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
