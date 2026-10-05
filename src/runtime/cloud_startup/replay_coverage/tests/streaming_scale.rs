//! Seeded, encoded recovery work; this is not a public-ACK or network soak.

use super::super::ReplayCoverage;
use crate::common::{DeadlineScope, MidgeResult, OperationDeadline};
use crate::config::RecoveryPolicy;
use crate::io::{Fs, FsError, FsPath};
use crate::memtable::SkipListMemtable;
use crate::metadata::{FileMeta, Manifest};
use crate::runtime::cloud_startup::streaming_wal_plan::StreamingCloudWalRecovery;
use crate::sst::{FsSstFactoryIo, SstFactory};
use crate::storage::remote_sst::RemoteSstFs;
use crate::storage::StorageBackend;
use crate::types::{EntryType, KeyState};
use crate::wal::cloud_catalog::{PublishedWalSegment, WalPublicationCatalog, OBJECT_KEY};
use crate::wal::recovery::streaming::{
    replay_wal_with_options, ReplayOptions, StreamingReplayLimits,
};
use crate::wal::recovery::{RecoveryStats, ReplayPolicy};
use crate::wal::{WalOpKind, WalRecord};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SST_COUNT: usize = 64;
const COVERED_PER_SST: usize = 128;
const SLOTS_PER_SST: usize = COVERED_PER_SST + 1;
const HOLE_SLOT: usize = COVERED_PER_SST / 2;
const BATCH_OPERATIONS: usize = 32;
const WRITER_EPOCH: u64 = 7;
const CATALOG_EPOCH: u64 = 9;
const COVERAGE_BUDGET: usize = 2 * 1_024 * 1_024;

#[derive(Default)]
struct RangeCounts {
    started: AtomicU64,
    completed: AtomicU64,
    bytes: AtomicU64,
    failed: AtomicU64,
}

impl crate::io::traits::ReadObserver for RangeCounts {
    fn remote_range_started(&self) {
        self.started.fetch_add(1, Ordering::Relaxed);
    }

    fn remote_range_completed(&self, returned_bytes: u64, elapsed: Duration, failed: bool) {
        if failed {
            self.failed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.completed.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(returned_bytes, Ordering::Relaxed);
        }
        crate::io::traits::ReadObserver::remote_range_completed(
            &crate::telemetry::recovery_progress::RecoveryReadObserver,
            returned_bytes,
            elapsed,
            failed,
        );
    }
}

struct SavedObject {
    path: PathBuf,
    bytes: Vec<u8>,
}

struct ScaleFixture {
    directory: tempfile::TempDir,
    fs: Arc<dyn Fs>,
    cloud: Arc<dyn StorageBackend>,
    catalog: WalPublicationCatalog,
    manifest: Manifest,
    records: Vec<WalRecord>,
    objects: Vec<SavedObject>,
}

impl ScaleFixture {
    fn new() -> MidgeResult<Self> {
        let directory = tempfile::tempdir()?;
        std::fs::create_dir_all(directory.path().join("local/wal"))?;
        std::fs::create_dir_all(directory.path().join("cloud/sst"))?;
        let fs: Arc<dyn Fs> =
            Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?);
        let cloud: Arc<dyn StorageBackend> = Arc::new(crate::storage::filesystem::FileSystem::new(
            directory.path().join("cloud"),
        )?);
        let mut fixture = Self {
            directory,
            fs,
            cloud,
            catalog: WalPublicationCatalog::empty(CATALOG_EPOCH).expect("valid catalog epoch"),
            manifest: Manifest::default(),
            records: seeded_records(),
            objects: Vec::new(),
        };
        fixture.seed_ssts()?;
        fixture.seed_wal()?;
        Ok(fixture)
    }

    fn save_object(&mut self, key: &str, bytes: Vec<u8>) -> MidgeResult<()> {
        let path = self.directory.path().join("cloud").join(key);
        std::fs::create_dir_all(path.parent().expect("object parent"))?;
        std::fs::write(&path, &bytes)?;
        self.objects.push(SavedObject { path, bytes });
        Ok(())
    }

    fn seed_ssts(&mut self) -> MidgeResult<()> {
        let factory = FsSstFactoryIo::new(Arc::clone(&self.fs), 4_096);
        for sst in 0..SST_COUNT {
            let mut writer = factory.create()?;
            let records = &self.records[sst * SLOTS_PER_SST..(sst + 1) * SLOTS_PER_SST];
            for (slot, record) in records.iter().enumerate() {
                if slot != HOLE_SLOT {
                    writer.add_with_meta(
                        &record.key,
                        record.value.as_deref(),
                        record.seq,
                        EntryType::Put,
                        None,
                    )?;
                }
            }
            let expected_first = records.first().unwrap().clone();
            let expected_last = records.last().unwrap().clone();
            let bytes = writer.finish_bytes()?;
            let crc = crc32c::crc32c(&bytes);
            let sequence = u64::try_from(sst + 1).expect("bounded SST count");
            let name = crate::cloud_layout::file_name(0, 0, sequence);
            let object_key = crate::cloud_layout::object_key(&name);
            self.save_object(&object_key, bytes)?;
            let summary = crate::sst::fs::SstFileIo::summarize_with_fs(
                &format!("cloud/{object_key}"),
                Arc::clone(&self.fs),
            )?;
            assert_eq!(summary.smallest_key, expected_first.key.as_ref());
            assert_eq!(summary.largest_key, expected_last.key.as_ref());
            assert_eq!(summary.smallest_seq, expected_first.seq);
            assert_eq!(summary.largest_seq, expected_last.seq);
            self.manifest.files.push(FileMeta {
                name,
                cf_id: 0,
                sst_seq: sequence,
                level: 0,
                size_bytes: summary.size_bytes,
                content_crc32c: Some(crc),
                smallest_key: Some(summary.smallest_key),
                largest_key: Some(summary.largest_key),
                smallest_seq: Some(summary.smallest_seq),
                largest_seq: Some(summary.largest_seq),
                key_bounds_complete: true,
                ..Default::default()
            });
        }
        Ok(())
    }

    fn seed_wal(&mut self) -> MidgeResult<()> {
        let bytes = encoded_batches(&self.records)?;
        let publication = PublishedWalSegment::from_validated_bytes(
            1,
            final_sequence(&self.records),
            WRITER_EPOCH,
            &bytes,
        );
        publication.validate_bytes(&bytes).expect("real sealed WAL");
        self.save_object(&publication.object_key, bytes)?;
        assert!(self.catalog.publish(CATALOG_EPOCH, publication).unwrap());
        self.save_object(
            OBJECT_KEY,
            self.catalog.encode().expect("validated catalog"),
        )?;
        Ok(())
    }

    fn recovery(&self) -> MidgeResult<StreamingCloudWalRecovery> {
        StreamingCloudWalRecovery::build(
            &self.directory.path().join("local"),
            &self.cloud,
            &self.catalog,
            RecoveryPolicy::Strict,
            Duration::from_secs(5),
            64 * 1_024,
            replay_limits(),
        )
    }

    fn coverage(&self, ranges: Arc<RangeCounts>) -> ReplayCoverage {
        let remote = RemoteSstFs::new(
            Arc::clone(&self.fs),
            Arc::clone(&self.cloud),
            Duration::from_secs(5),
        );
        let mut coverage =
            ReplayCoverage::new(self.manifest.clone(), Arc::new(remote), COVERAGE_BUDGET);
        // Keep genuine range receipts alongside the normal recovery observer.
        coverage.fs = coverage
            .fs
            .with_read_observer(ranges)
            .expect("observed remote SST view");
        coverage
    }

    fn assert_preserved(&self) -> MidgeResult<()> {
        for object in &self.objects {
            assert_eq!(std::fs::read(&object.path)?, object.bytes);
        }
        assert_eq!(
            std::fs::read_dir(self.directory.path().join("local/wal"))?.count(),
            0,
        );
        assert!(!self.directory.path().join("local/cloud_recovery").exists());
        Ok(())
    }
}

fn seeded_records() -> Vec<WalRecord> {
    (0..SST_COUNT * SLOTS_PER_SST)
        .map(|ordinal| {
            let sst = ordinal / SLOTS_PER_SST;
            let slot = ordinal % SLOTS_PER_SST;
            let batch = u64::try_from(ordinal / BATCH_OPERATIONS).unwrap();
            let sequence = u64::try_from(ordinal).unwrap() + 2 * batch + 2;
            let mut record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from(format!("key-{sst:03}-{slot:03}")),
                Some(seeded_value(ordinal)),
                sequence,
                WRITER_EPOCH,
            );
            record.txn_id = Some(batch + 1);
            record
        })
        .collect()
}

fn seeded_value(ordinal: usize) -> Bytes {
    let mut state = u64::try_from(ordinal).unwrap() + 0x9e37_79b9_7f4a_7c15;
    let bytes = (0..64 + ordinal % 192)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect::<Vec<_>>();
    Bytes::from(bytes)
}

fn encoded_batches(records: &[WalRecord]) -> MidgeResult<Vec<u8>> {
    assert!(records.len().is_multiple_of(BATCH_OPERATIONS));
    let mut bytes = Vec::new();
    for batch in records.as_chunks::<BATCH_OPERATIONS>().0 {
        let first = batch.first().unwrap();
        let commit = batch.last().unwrap().seq + 1;
        let txn_id = first.txn_id.expect("seeded batch identity");
        let payload = crate::wal::encoding::encode_txn_batch_payload(
            txn_id,
            first.seq - 1,
            commit,
            WRITER_EPOCH,
            batch,
        )?;
        let mut outer = WalRecord::new(
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            commit,
            WRITER_EPOCH,
        );
        outer.txn_id = Some(txn_id);
        let payload = crate::wal::encoding::encode(&outer)?;
        crate::wal::frame::append_frame(&mut bytes, &payload)?;
    }
    Ok(bytes)
}

fn final_sequence(records: &[WalRecord]) -> u64 {
    records.last().expect("seeded records").seq + 1
}

fn replay_limits() -> StreamingReplayLimits {
    StreamingReplayLimits {
        max_frame_bytes: 128 * 1_024,
        max_pending_txn_bytes: 256 * 1_024,
        max_memtable_encoded_bytes: 512 * 1_024,
        target_memtable_encoded_bytes: 256 * 1_024,
    }
}

fn assert_rows(fixture: &ScaleFixture, memtables: &HashMap<u32, Arc<SkipListMemtable>>) {
    assert_eq!(memtables.len(), 1);
    let table = memtables.get(&0).expect("genuine uncovered rows replayed");
    assert_eq!(table.iter_all().len(), SST_COUNT);
    for (ordinal, record) in fixture.records.iter().enumerate() {
        let state = table.get_raw_key_state_at(&record.key, u64::MAX);
        if ordinal % SLOTS_PER_SST == HOLE_SLOT {
            assert!(
                matches!(state, KeyState::Value(value, sequence, None, EntryType::Put)
                if Some(&value) == record.value.as_ref() && sequence == record.seq)
            );
        } else {
            assert!(matches!(state, KeyState::Absent));
        }
    }
}

fn assert_frontier(fixture: &ScaleFixture, stats: &RecoveryStats) {
    assert_eq!(
        stats.record_count,
        u64::try_from(fixture.records.len() / BATCH_OPERATIONS).unwrap()
    );
    assert_eq!(stats.max_sequence, Some(final_sequence(&fixture.records)));
    assert_eq!(stats.max_epoch_seen, WRITER_EPOCH);
    assert_eq!(stats.stale_records_skipped, 0);
    assert!(!stats.had_corruption);
    assert!(stats.tolerated_active_tail.is_none());
    assert!(stats.salvage_stop.is_none());
}

fn print_diagnostics(coverage: &ReplayCoverage, ranges: &RangeCounts, elapsed: Duration) {
    let (block_hits, block_misses, block_peak) = coverage.block_stats();
    println!(
        "seeded_streaming_scale replay_elapsed_ns={} logical_operations={} probes={} \
         manifest_visits={} candidates={} reader_opens={} verified_sst_bytes={} \
         successful_sst_remote_ranges={} successful_sst_remote_bytes={} \
         block_hits={} block_misses={} block_peak_bytes={} coverage_peak_bytes={}",
        elapsed.as_nanos(),
        SST_COUNT * SLOTS_PER_SST,
        coverage.probes.get(),
        coverage.manifest_scanned.get(),
        coverage.manifest_candidates.get(),
        coverage.reader_opens.get(),
        coverage.verified_bytes.get(),
        ranges.completed.load(Ordering::Relaxed),
        ranges.bytes.load(Ordering::Relaxed),
        block_hits,
        block_misses,
        block_peak,
        coverage.read_budget.peak(),
    );
}

#[test]
fn should_bound_manifest_lookup_work_when_streaming_real_transaction_batches_at_scale(
) -> MidgeResult<()> {
    // Arrange: 8,192 real persisted rows and 64 interior absent keys, not client ACKs.
    let fixture = ScaleFixture::new()?;
    let recovered = fixture.recovery()?;
    assert_eq!(recovered.plan.remote_segments.len(), 1);
    assert!(recovered.plan.local_segments.is_empty());
    let ranges = Arc::new(RangeCounts::default());
    let coverage = fixture.coverage(Arc::clone(&ranges));
    let budget = coverage.read_budget.clone();
    let scope = DeadlineScope::new(OperationDeadline::unbounded());
    let should_apply = |record: &WalRecord| {
        coverage
            .contains_within(record, &scope)
            .map(|covered| !covered)
    };
    let mut memtables = HashMap::new();
    let mut checkpoints = 0;

    // Act: the actual planner validated the sealed bytes; replay uses bounded atomic apply.
    let started = Instant::now();
    let stats = replay_wal_with_options(
        recovered.fs.as_ref(),
        &FsPath::new("wal"),
        &mut memtables,
        ReplayPolicy::Strict,
        None,
        replay_limits(),
        ReplayOptions {
            fallible_should_apply: Some(&should_apply),
            ..ReplayOptions::default()
        },
        &mut |_, _| {
            checkpoints += 1;
            Ok(())
        },
    )?;
    let elapsed = started.elapsed();

    // Assert: preserve the encoded frontier, exact coverage decisions and immutable bodies.
    assert_frontier(&fixture, &stats);
    assert_rows(&fixture, &memtables);
    fixture.assert_preserved()?;
    assert_eq!(checkpoints, 0);
    let probes = 2 * u64::try_from(fixture.records.len()).unwrap();
    assert_eq!(coverage.probes.get(), probes);
    assert_eq!(coverage.manifest_candidates.get(), probes);
    assert_eq!(
        coverage.reader_opens.get(),
        u64::try_from(SST_COUNT).unwrap()
    );
    assert_eq!(
        coverage.verified_bytes.get(),
        fixture
            .manifest
            .files
            .iter()
            .map(|file| file.size_bytes)
            .sum::<u64>()
    );
    assert_eq!(ranges.failed.load(Ordering::Relaxed), 0);
    assert_eq!(
        ranges.started.load(Ordering::Relaxed),
        ranges.completed.load(Ordering::Relaxed)
    );
    assert!(ranges.completed.load(Ordering::Relaxed) > 0);
    assert!(budget.peak() > 0 && budget.peak() <= budget.limit());
    print_diagnostics(&coverage, &ranges, elapsed);
    coverage.release_reader();
    assert_eq!(budget.used(), 0);
    // A disjoint 64-interval tree needs at most two logarithmic paths per probe.
    // The original whole-manifest scan visits 64 entries per probe and fails here.
    let maximum_visits_per_probe = 2 * (u64::from(SST_COUNT.ilog2()) + 1) + 2;
    assert!(
        coverage.manifest_scanned.get() <= probes * maximum_visits_per_probe,
        "actual manifest visits {} exceed the logarithmic bound {} for {probes} real probes",
        coverage.manifest_scanned.get(),
        probes * maximum_visits_per_probe
    );
    Ok(())
}
