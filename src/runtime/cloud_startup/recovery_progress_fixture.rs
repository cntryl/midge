//! Actual recovery fixtures for the external watchdog's isolated child tests.

use super::replay_coverage::ReplayCoverage;
use super::streaming_wal_fs::StreamingWalFs;
use super::streaming_wal_plan::StreamingCloudWalRecovery;
use crate::common::{MidgeError, MidgeResult};
use crate::config::RecoveryPolicy;
use crate::io::{Fs, FsError, FsPath, OpenMode, OpenOptions};
use crate::memtable::SkipListMemtable;
use crate::storage::StorageBackend;
use crate::types::{EntryType, KeyState};
use crate::wal::cloud_catalog::{PublishedWalSegment, WalPublicationCatalog};
use crate::wal::recovery::streaming::{
    replay_wal_with_options, ReplayOptions, StreamingReplayLimits,
};
use crate::wal::recovery::{RecoveryStats, ReplayPolicy};
use crate::wal::{WalOpKind, WalRecord};
use bytes::Bytes;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod backend;
use backend::{FixtureObservations, ObservedBackend};

const READ_WINDOW: usize = 64 * 1024;
const WRITER_EPOCH: u64 = 7;
const PROVIDER_TIMEOUT: Duration = Duration::from_secs(5);
const COVERAGE_PAUSE: Duration = Duration::from_millis(75);

/// Fault policy owned by a fixture child, outside the supported public API.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryProgressFixtureMode {
    DelayedRanges,
    HeldFirstRange,
    CachedCoverage,
    MetadataInventory,
    HeldInventory,
}

/// Independent evidence returned only after the selected recovery path finishes.
#[doc(hidden)]
#[derive(Debug, Serialize)]
pub struct RecoveryProgressFixtureResult {
    pub expected_records: u64,
    pub verified_records: u64,
    pub mismatches: u64,
    pub max_sequence: u64,
    pub max_epoch: u64,
    pub completed_range_reads: u64,
    pub completed_range_bytes: u64,
    pub maximum_range_bytes: u64,
    pub replay_completed_range_reads: u64,
    pub local_wal_bytes: u64,
    pub staged_wal_count: u64,
    pub elapsed_ms: u64,
    pub coverage_checks: u64,
    pub expected_inventory_entries: u64,
    pub retained_inventory_entries: u64,
    pub completed_inventory_heads: u64,
    pub completed_inventory_size_validations: u64,
}

struct FixtureWal {
    records: Vec<WalRecord>,
    bytes: Vec<u8>,
    catalog: WalPublicationCatalog,
}

/// Seed fixture input before the external watchdog begins observing recovery.
#[doc(hidden)]
pub struct PreparedRecoveryProgressFixture {
    root: PathBuf,
    mode: RecoveryProgressFixtureMode,
    observations: Arc<FixtureObservations>,
    wal: Option<FixtureWal>,
    #[cfg(any(test, feature = "cloud-common"))]
    inventory: Vec<crate::metadata::FileMeta>,
}

/// Prepare actual startup inventory or catalog planner/replay input.
///
/// # Errors
///
/// Returns any setup, proof, replay, or independent-evidence persistence error.
#[doc(hidden)]
pub fn prepare_recovery_progress_fixture(
    root: &Path,
    mode: RecoveryProgressFixtureMode,
) -> MidgeResult<PreparedRecoveryProgressFixture> {
    let local = root.join("local");
    std::fs::create_dir_all(local.join("wal"))?;
    if local_wal_state(&local)? != (0, 0) {
        return Err(MidgeError::InvalidArgument(
            "recovery fixture requires an empty local WAL directory".into(),
        ));
    }
    let observations = Arc::new(FixtureObservations::new(root, mode, 0)?);
    let inventory_mode = matches!(
        mode,
        RecoveryProgressFixtureMode::MetadataInventory | RecoveryProgressFixtureMode::HeldInventory
    );
    #[cfg(any(test, feature = "cloud-common"))]
    let inventory = if inventory_mode {
        let inventory = seed_inventory(root)?;
        observations.set_expected_inventory(inventory.len())?;
        inventory
    } else {
        Vec::new()
    };
    #[cfg(not(any(test, feature = "cloud-common")))]
    if inventory_mode {
        return Err(MidgeError::InvalidArgument(
            "metadata inventory fixture requires cloud-common".into(),
        ));
    }
    let wal = if inventory_mode {
        None
    } else {
        let wal = seed_wal(root, mode)?;
        observations.set_expected_records(wal.records.len())?;
        Some(wal)
    };
    Ok(PreparedRecoveryProgressFixture {
        root: root.to_path_buf(),
        mode,
        observations,
        wal,
        #[cfg(any(test, feature = "cloud-common"))]
        inventory,
    })
}

impl PreparedRecoveryProgressFixture {
    /// Execute recovery of the prepared input, with no fixture seeding.
    ///
    /// # Errors
    ///
    /// Returns any proof, replay, or independent-evidence persistence error.
    pub fn run(self) -> MidgeResult<RecoveryProgressFixtureResult> {
        let started = Instant::now();
        let local = self.root.join("local");
        if let Some(wal) = self.wal {
            return wal_fixture(
                &self.root,
                self.mode,
                &local,
                &self.observations,
                &wal,
                started,
            );
        }
        #[cfg(any(test, feature = "cloud-common"))]
        return inventory_fixture(
            &self.root,
            self.mode,
            &local,
            &self.observations,
            self.inventory,
            started,
        );
        #[cfg(not(any(test, feature = "cloud-common")))]
        Err(MidgeError::InvalidArgument(
            "metadata inventory fixture requires cloud-common".into(),
        ))
    }
}

fn wal_fixture(
    root: &Path,
    mode: RecoveryProgressFixtureMode,
    local: &Path,
    observations: &Arc<FixtureObservations>,
    wal: &FixtureWal,
    started: Instant,
) -> MidgeResult<RecoveryProgressFixtureResult> {
    let remote: Arc<dyn StorageBackend> = Arc::new(ObservedBackend::new(
        root.join("cloud"),
        mode,
        Arc::clone(observations),
    )?);
    observations.set_phase("planner")?;
    let recovered = StreamingCloudWalRecovery::build(
        local,
        &remote,
        &wal.catalog,
        RecoveryPolicy::Strict,
        PROVIDER_TIMEOUT,
        READ_WINDOW,
        StreamingReplayLimits::local(),
    )?;
    if recovered.plan.remote_segments.len() != 1
        || !recovered.plan.local_segments.is_empty()
        || recovered.plan.active_wal.is_some()
        || recovered.plan.opened_in_salvage_mode
    {
        return Err(MidgeError::Internal(
            "fixture planner did not select exactly its cataloged remote WAL".into(),
        ));
    }
    let coverage = if mode == RecoveryProgressFixtureMode::CachedCoverage {
        Some(cached_coverage(root, &wal.records, &remote)?)
    } else {
        None
    };
    let replay_fs = if coverage.is_some() {
        buffered_wal(&wal.bytes)?
    } else {
        recovered.fs
    };
    let before_replay = observations.snapshot().completed_range_reads;
    observations.set_phase("replay")?;
    let mut memtables = HashMap::new();
    let stats = replay_fixture(
        replay_fs.as_ref(),
        &mut memtables,
        coverage.as_ref(),
        observations,
    )?;
    observations.check_failure()?;
    let (verified_records, row_mismatches) =
        verify_rows(&wal.records, &memtables, coverage.is_some());
    let observed = observations.snapshot();
    let (local_wal_bytes, staged_wal_count) = local_wal_state(local)?;
    let mismatches = row_mismatches
        .saturating_add(observed.mismatches)
        .saturating_add(u64::from(stats.record_count != observed.expected_records))
        .saturating_add(u64::from(
            stats.max_sequence != Some(observed.expected_records),
        ))
        .saturating_add(u64::from(stats.max_epoch_seen != WRITER_EPOCH))
        .saturating_add(u64::from(
            stats.had_corruption || stats.stale_records_skipped != 0,
        ))
        .saturating_add(u64::from(local.join("cloud_recovery").exists()));
    observations.set_phase("complete")?;
    Ok(RecoveryProgressFixtureResult {
        expected_records: observed.expected_records,
        verified_records,
        mismatches,
        max_sequence: stats.max_sequence.unwrap_or(0),
        max_epoch: stats.max_epoch_seen,
        completed_range_reads: observed.completed_range_reads,
        completed_range_bytes: observed.completed_range_bytes,
        maximum_range_bytes: observed.maximum_range_bytes,
        replay_completed_range_reads: observed.completed_range_reads.saturating_sub(before_replay),
        local_wal_bytes,
        staged_wal_count,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        coverage_checks: observed.coverage_checks,
        expected_inventory_entries: 0,
        retained_inventory_entries: 0,
        completed_inventory_heads: 0,
        completed_inventory_size_validations: 0,
    })
}

#[cfg(any(test, feature = "cloud-common"))]
fn inventory_fixture(
    root: &Path,
    mode: RecoveryProgressFixtureMode,
    local: &Path,
    observations: &Arc<FixtureObservations>,
    files: Vec<crate::metadata::FileMeta>,
    started: Instant,
) -> MidgeResult<RecoveryProgressFixtureResult> {
    let expected_inventory = serde_json::to_vec(&files)
        .map_err(|error| MidgeError::Internal(format!("inventory fixture metadata: {error}")))?;
    let backend = backend::InventoryCloudBackend::new(
        root.join("cloud"),
        mode,
        &files,
        Arc::clone(observations),
    )?;
    let cloud = crate::storage::cloud::CloudStorage::new_with_timeout(
        Arc::new(backend),
        String::new(),
        PROVIDER_TIMEOUT,
    );
    // In-memory runtime metadata keeps the sampled local database completely
    // flat; the actual cloud inventory reconciliation and HEADs still execute.
    let mut state =
        crate::runtime::RuntimeState::try_new(local.to_path_buf(), true, RecoveryPolicy::Strict)?;
    state.manifest.replace_files(files);
    observations.set_phase("metadata_inventory")?;
    super::CloudStartupRecovery::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)?;
    observations.check_failure()?;
    let retained_inventory = serde_json::to_vec(&state.manifest.files)
        .map_err(|error| MidgeError::Internal(format!("retained fixture inventory: {error}")))?;
    observations.record_retained_inventory(
        state.manifest.files.len(),
        retained_inventory != expected_inventory,
    )?;
    let observed = observations.snapshot();
    let (local_wal_bytes, staged_wal_count) = local_wal_state(local)?;
    let mismatches = observed.mismatches
        + u64::from(observed.completed_inventory_heads != observed.expected_inventory_entries)
        + u64::from(
            observed.completed_inventory_size_validations != observed.expected_inventory_entries,
        )
        + u64::from(state.opened_in_salvage_mode());
    observations.set_phase("complete")?;
    Ok(RecoveryProgressFixtureResult {
        expected_records: 0,
        verified_records: 0,
        mismatches,
        max_sequence: 0,
        max_epoch: 0,
        completed_range_reads: observed.completed_range_reads,
        completed_range_bytes: observed.completed_range_bytes,
        maximum_range_bytes: observed.maximum_range_bytes,
        replay_completed_range_reads: 0,
        local_wal_bytes,
        staged_wal_count,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        coverage_checks: observed.coverage_checks,
        expected_inventory_entries: observed.expected_inventory_entries,
        retained_inventory_entries: observed.retained_inventory_entries,
        completed_inventory_heads: observed.completed_inventory_heads,
        completed_inventory_size_validations: observed.completed_inventory_size_validations,
    })
}

#[cfg(any(test, feature = "cloud-common"))]
fn seed_inventory(root: &Path) -> MidgeResult<Vec<crate::metadata::FileMeta>> {
    let directory = root.join("inventory-sst");
    std::fs::create_dir_all(directory.join("sst"))?;
    let fs: Arc<dyn Fs> =
        Arc::new(crate::io::RealFs::new(&directory).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(fs, READ_WINDOW);
    let mut writer = factory.create_for_flush(
        crate::common::resource_budget::ResourceBudget::new(1024 * 1024),
    )?;
    writer.add_sorted_with_meta(
        b"inventory-key",
        Some(b"inventory-value"),
        1,
        EntryType::Put,
        None,
    )?;
    let path = directory.join("sst/inventory.sst");
    writer.finish_to_path(&path)?;
    let bytes = std::fs::read(path)?;
    let mut files = Vec::with_capacity(16);
    for sequence in 1..=16 {
        let name = crate::cloud_layout::file_name(0, 0, sequence);
        let cloud_path = root
            .join("cloud")
            .join(crate::cloud_layout::object_key(&name));
        if let Some(parent) = cloud_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(cloud_path, &bytes)?;
        files.push(crate::metadata::FileMeta {
            name,
            cf_id: 0,
            sst_seq: sequence,
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            content_crc32c: Some(crc32c::crc32c(&bytes)),
            smallest_key: Some(b"inventory-key".to_vec()),
            largest_key: Some(b"inventory-key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            key_bounds_complete: true,
            ..Default::default()
        });
    }
    Ok(files)
}

fn seed_wal(root: &Path, mode: RecoveryProgressFixtureMode) -> MidgeResult<FixtureWal> {
    let mut records = Vec::new();
    let mut bytes = Vec::new();
    let cached = mode == RecoveryProgressFixtureMode::CachedCoverage;
    while if cached {
        records.len() < 32
    } else {
        bytes.len() < 16 * READ_WINDOW
    } {
        let sequence = u64::try_from(records.len() + 1)
            .map_err(|_| MidgeError::Internal("fixture sequence exceeds u64".into()))?;
        let value = if cached {
            incompressible_value(sequence)
        } else {
            sequence.to_le_bytes().repeat(16)
        };
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from(format!("key-{sequence:08}")),
            Some(Bytes::from(value)),
            sequence,
            WRITER_EPOCH,
        );
        let payload = crate::wal::encoding::encode(&record)?;
        if cached && payload.len() < READ_WINDOW {
            return Err(MidgeError::Internal(
                "cached fixture requires genuinely large encoded WAL frames".into(),
            ));
        }
        crate::wal::frame::append_frame(&mut bytes, &payload)?;
        records.push(record);
    }
    let publication = PublishedWalSegment::from_validated_bytes(
        1,
        u64::try_from(records.len()).unwrap_or(u64::MAX),
        WRITER_EPOCH,
        &bytes,
    );
    let remote_path = root.join("cloud").join(&publication.object_key);
    if let Some(parent) = remote_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(remote_path, &bytes)?;
    let mut catalog = WalPublicationCatalog::empty(9).map_err(MidgeError::Internal)?;
    catalog
        .publish(9, publication)
        .map_err(MidgeError::Internal)?;
    Ok(FixtureWal {
        records,
        bytes,
        catalog,
    })
}

fn incompressible_value(sequence: u64) -> Vec<u8> {
    let mut state = sequence ^ 0x9e37_79b9_7f4a_7c15;
    let mut value = Vec::with_capacity(READ_WINDOW);
    while value.len() < READ_WINDOW {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        value.extend_from_slice(&state.to_le_bytes());
    }
    value
}

fn buffered_wal(bytes: &[u8]) -> MidgeResult<Arc<dyn Fs>> {
    let memory: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    let name = crate::wal::segment_file_name(1);
    memory
        .open(
            &FsPath::new(&name),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: true,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?
        .write_at(0, Bytes::copy_from_slice(bytes))
        .map_err(FsError::into_midge)?;
    let mut buffered = StreamingWalFs::new(READ_WINDOW)?;
    buffered.insert(name.clone(), memory, FsPath::new(name))?;
    Ok(Arc::new(buffered))
}

fn cached_coverage(
    root: &Path,
    records: &[WalRecord],
    remote: &Arc<dyn StorageBackend>,
) -> MidgeResult<ReplayCoverage> {
    let directory = root.join("cache-coverage-sst");
    std::fs::create_dir_all(directory.join("sst"))?;
    let fs: Arc<dyn Fs> =
        Arc::new(crate::io::RealFs::new(&directory).map_err(FsError::into_midge)?);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), READ_WINDOW)
        .with_compression_policy(crate::codec::CompressionPolicy::Fixed(
            crate::codec::CompressionAlgo::None,
        ));
    let mut writer = factory.create_for_flush(
        crate::common::resource_budget::ResourceBudget::new(16 * 1024 * 1024),
    )?;
    for record in records.iter().filter(|record| record.seq.is_multiple_of(2)) {
        writer.add_sorted_with_meta(
            &record.key,
            record.value.as_deref(),
            record.seq,
            EntryType::Put,
            None,
        )?;
    }
    let name = crate::cloud_layout::file_name(0, 0, 1);
    let path = directory.join("sst").join(&name);
    writer.finish_to_path(&path)?;
    let bytes = std::fs::read(path)?;
    let mut manifest = crate::metadata::Manifest::default();
    manifest.files.push(crate::metadata::FileMeta {
        name: name.clone(),
        cf_id: 0,
        size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        content_crc32c: Some(crc32c::crc32c(&bytes)),
        smallest_key: records.first().map(|record| record.key.to_vec()),
        largest_key: records.last().map(|record| record.key.to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(u64::try_from(records.len()).unwrap_or(u64::MAX)),
        ..Default::default()
    });
    let pinned: Arc<dyn Fs> = Arc::new(
        crate::storage::remote_sst::RemoteSstFs::new(fs, Arc::clone(remote), PROVIDER_TIMEOUT)
            .with_verified_local_overrides(HashSet::from([name])),
    );
    let coverage = ReplayCoverage::new(manifest, pinned, 16 * 1024 * 1024);
    for record in records {
        if coverage.contains(record) != record.seq.is_multiple_of(2) {
            return Err(MidgeError::Internal(
                "cached fixture warmup did not establish exact SST coverage".into(),
            ));
        }
    }
    Ok(coverage)
}

fn replay_fixture(
    fs: &dyn Fs,
    memtables: &mut HashMap<u32, Arc<SkipListMemtable>>,
    coverage: Option<&ReplayCoverage>,
    observations: &FixtureObservations,
) -> MidgeResult<RecoveryStats> {
    let should_apply = |record: &WalRecord| {
        let covered = coverage.is_some_and(|coverage| coverage.contains(record));
        if coverage.is_some() {
            observations.record_coverage(covered != record.seq.is_multiple_of(2));
            std::thread::sleep(COVERAGE_PAUSE);
        }
        !covered
    };
    replay_wal_with_options(
        fs,
        &FsPath::new("wal"),
        memtables,
        ReplayPolicy::Strict,
        Some(&should_apply),
        StreamingReplayLimits::local(),
        ReplayOptions::default(),
        &mut |_, _| Ok(()),
    )
}

fn verify_rows(
    records: &[WalRecord],
    memtables: &HashMap<u32, Arc<SkipListMemtable>>,
    cached: bool,
) -> (u64, u64) {
    let mut verified = 0;
    let mut mismatches = 0;
    for record in records {
        let state = memtables.get(&0).map_or(KeyState::Absent, |table| {
            table.get_raw_key_state_at(&record.key, u64::MAX)
        });
        let matches = if cached && record.seq.is_multiple_of(2) {
            matches!(state, KeyState::Absent)
        } else {
            matches!(state, KeyState::Value(ref value, sequence, None, EntryType::Put)
                if Some(value) == record.value.as_ref() && sequence == record.seq)
        };
        if matches {
            verified += 1;
        } else {
            mismatches += 1;
        }
    }
    (verified, mismatches)
}

fn local_wal_state(local: &Path) -> MidgeResult<(u64, u64)> {
    let mut bytes = 0_u64;
    let mut files = 0_u64;
    for entry in std::fs::read_dir(local.join("wal"))? {
        let metadata = entry?.metadata()?;
        if metadata.is_file() {
            files += 1;
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok((bytes, files))
}
