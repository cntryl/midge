//! Actual recovery fixtures for the external watchdog's isolated child tests.

use super::streaming_wal_plan::StreamingCloudWalRecovery;
use crate::common::{MidgeError, MidgeResult};
use crate::config::RecoveryPolicy;
use crate::io::{Fs, FsError, ReadFs};
use crate::memtable::SkipListMemtable;
use crate::storage::StorageBackend;
use crate::types::{EntryType, KeyState};
use crate::wal::cloud_catalog::{CatalogEntry, WalPublicationCatalog};
use crate::wal::recovery::streaming::{
    replay_wal_with_options, ReplayOptions, StreamingReplayLimits,
};
use crate::wal::recovery::{RecoveryStats, ReplayPolicy};
use crate::wal::{WalOpKind, WalRecord};
use bytes::Bytes;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod backend;
use backend::{FixtureObservations, ObservedBackend};

const READ_WINDOW: usize = 64 * 1024;
const SEGMENT_EPOCH: u64 = 7;
const PROVIDER_TIMEOUT: Duration = Duration::from_secs(5);

/// Fault policy owned by a fixture child, outside the supported public API.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryProgressFixtureMode {
    DelayedRanges,
    HeldFirstRange,
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
    pub expected_inventory_entries: u64,
    pub retained_inventory_entries: u64,
    pub completed_inventory_heads: u64,
    pub completed_inventory_size_validations: u64,
}

struct FixtureWal {
    records: Vec<WalRecord>,
    catalog: WalPublicationCatalog,
}

enum PreparedInput {
    Wal {
        wal: Box<FixtureWal>,
    },
    #[cfg(any(test, feature = "cloud-common"))]
    Inventory(Vec<crate::metadata::FileMeta>),
}

/// Seed fixture input before the external watchdog begins observing recovery.
#[doc(hidden)]
pub struct PreparedRecoveryProgressFixture {
    root: PathBuf,
    mode: RecoveryProgressFixtureMode,
    observations: Arc<FixtureObservations>,
    remote: Arc<dyn StorageBackend>,
    input: PreparedInput,
    setup_elapsed: Duration,
}

/// Prepare actual startup inventory or catalog planner/replay input.
///
/// # Errors
///
/// Returns any setup, proof, or independent-evidence persistence error.
#[doc(hidden)]
pub fn prepare_recovery_progress_fixture(
    root: &Path,
    mode: RecoveryProgressFixtureMode,
    setup_delay: Duration,
) -> MidgeResult<PreparedRecoveryProgressFixture> {
    let started = Instant::now();
    // Keep the regression delay in the component that actually seeds input.
    // Moving this component back under the watchdog must time out.
    std::thread::sleep(setup_delay);
    let local = root.join("local");
    std::fs::create_dir_all(local.join("wal"))?;
    if local_wal_state(&local)? != (0, 0) {
        return Err(MidgeError::InvalidArgument(
            "recovery fixture requires an empty local WAL directory".into(),
        ));
    }
    let observations = Arc::new(FixtureObservations::new(root, mode, 0)?);
    let remote: Arc<dyn StorageBackend> = Arc::new(ObservedBackend::new(
        root.join("cloud"),
        mode,
        Arc::clone(&observations),
    )?);
    let input = match mode {
        RecoveryProgressFixtureMode::MetadataInventory
        | RecoveryProgressFixtureMode::HeldInventory => {
            #[cfg(any(test, feature = "cloud-common"))]
            {
                let inventory = seed_inventory(root)?;
                observations.set_expected_inventory(inventory.len())?;
                PreparedInput::Inventory(inventory)
            }
            #[cfg(not(any(test, feature = "cloud-common")))]
            return Err(MidgeError::InvalidArgument(
                "metadata inventory fixture requires cloud-common".into(),
            ));
        }
        _ => {
            let wal = seed_wal(root)?;
            // The cache's ledger exists before recovery starts, as it does for
            // a real cache, so the measured recovery never writes locally.
            let local_root: Arc<dyn Fs> =
                Arc::new(crate::io::RealFs::new(&local).map_err(FsError::into_midge)?);
            // A cache that joined the catalog: its first segment follows the
            // catalog's head, at its next id.
            let local_wal = crate::io::subdirectory(local_root, crate::wal::WAL_DIR);
            if crate::runtime::wal_history::WalHistory::load(Arc::clone(&local_wal), SEGMENT_EPOCH)?
                .is_none()
            {
                drop(crate::runtime::wal_history::WalHistory::genesis(
                    local_wal,
                    SEGMENT_EPOCH,
                    wal.catalog.head(),
                    wal.catalog.next_segment_id(),
                )?);
            }
            observations.set_expected_records(wal.records.len())?;
            PreparedInput::Wal { wal: Box::new(wal) }
        }
    };
    Ok(PreparedRecoveryProgressFixture {
        root: root.to_path_buf(),
        mode,
        observations,
        remote,
        input,
        setup_elapsed: started.elapsed(),
    })
}

impl PreparedRecoveryProgressFixture {
    /// Return the mode whose input this fixture owns.
    #[must_use]
    pub const fn mode(&self) -> RecoveryProgressFixtureMode {
        self.mode
    }

    /// Return time spent inside actual fixture preparation.
    #[must_use]
    pub const fn setup_elapsed(&self) -> Duration {
        self.setup_elapsed
    }

    /// Execute recovery of the prepared input, with no fixture seeding.
    ///
    /// # Errors
    ///
    /// Returns any proof, replay, or independent-evidence persistence error.
    pub fn run(self) -> MidgeResult<RecoveryProgressFixtureResult> {
        let started = Instant::now();
        let local = self.root.join("local");
        match self.input {
            PreparedInput::Wal { wal } => {
                wal_fixture(&local, &self.observations, &wal, &self.remote, started)
            }
            #[cfg(any(test, feature = "cloud-common"))]
            PreparedInput::Inventory(inventory) => inventory_fixture(
                &self.root,
                self.mode,
                &local,
                &self.observations,
                inventory,
                started,
            ),
        }
    }
}

fn wal_fixture(
    local: &Path,
    observations: &Arc<FixtureObservations>,
    wal: &FixtureWal,
    remote: &Arc<dyn StorageBackend>,
    started: Instant,
) -> MidgeResult<RecoveryProgressFixtureResult> {
    observations.set_phase("planner")?;
    let local_root: Arc<dyn Fs> =
        Arc::new(crate::io::RealFs::new(local).map_err(FsError::into_midge)?);
    let wal_dir: Arc<dyn ReadFs> =
        crate::io::subdirectory(Arc::clone(&local_root), crate::wal::WAL_DIR);
    let ledger = crate::runtime::wal_history::WalHistory::read(&wal_dir)?
        .ok_or_else(|| MidgeError::Internal("the prepared cache has no WAL ledger".into()))?;
    let recovered = StreamingCloudWalRecovery::build(
        &local_root,
        Some(&ledger),
        remote,
        &wal.catalog,
        RecoveryPolicy::Strict,
        PROVIDER_TIMEOUT,
        READ_WINDOW,
        StreamingReplayLimits::local(),
    )?;
    if recovered.plan.remote_segments.len() != 1
        || !recovered.plan.local_segments.is_empty()
        || recovered.plan.opened_in_salvage_mode
    {
        return Err(MidgeError::Internal(
            "fixture planner did not select exactly its cataloged remote WAL".into(),
        ));
    }
    let replay_fs = recovered.fs;
    let before_replay = observations.snapshot().completed_range_reads;
    observations.set_phase("replay")?;
    let mut memtables = HashMap::new();
    let stats = replay_fixture(replay_fs.as_ref(), &recovered.segments, &mut memtables)?;
    observations.check_failure()?;
    let (verified_records, row_mismatches) = verify_rows(&wal.records, &memtables);
    let observed = observations.snapshot();
    let (local_wal_bytes, staged_wal_count) = local_wal_state(local)?;
    let mismatches = row_mismatches
        .saturating_add(observed.mismatches)
        .saturating_add(u64::from(stats.record_count != observed.expected_records))
        .saturating_add(u64::from(
            stats.max_sequence != Some(observed.expected_records),
        ))
        .saturating_add(u64::from(stats.had_corruption))
        .saturating_add(u64::from(local.join("cloud_recovery").exists()));
    observations.set_phase("complete")?;
    Ok(RecoveryProgressFixtureResult {
        expected_records: observed.expected_records,
        verified_records,
        mismatches,
        max_sequence: stats.max_sequence.unwrap_or(0),
        // Replay checked the segment header against the catalog's epoch.
        max_epoch: wal
            .catalog
            .entries()
            .iter()
            .map(CatalogEntry::epoch)
            .max()
            .unwrap_or_default(),
        completed_range_reads: observed.completed_range_reads,
        completed_range_bytes: observed.completed_range_bytes,
        maximum_range_bytes: observed.maximum_range_bytes,
        replay_completed_range_reads: observed.completed_range_reads.saturating_sub(before_replay),
        local_wal_bytes,
        staged_wal_count,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
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
    let mut state = crate::runtime::RuntimeState::try_new(local, true, RecoveryPolicy::Strict)?;
    state.manifest.replace_files(files);
    observations.set_phase("metadata_inventory")?;
    super::CloudStartupRecovery::validate_manifest_ssts_in_store(&mut state, &cloud)?;
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

fn seed_wal(root: &Path) -> MidgeResult<FixtureWal> {
    let mut records = Vec::new();
    let mut bytes = Vec::new();
    while bytes.len() < 16 * READ_WINDOW {
        let sequence = u64::try_from(records.len() + 1)
            .map_err(|_| MidgeError::Internal("fixture sequence exceeds u64".into()))?;
        let value = sequence.to_le_bytes().repeat(16);
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from(format!("key-{sequence:08}")),
            Some(Bytes::from(value)),
            sequence,
        );
        let payload = crate::wal::encoding::encode(&record)?;
        crate::wal::frame::append_frame(&mut bytes, &payload)?;
        records.push(record);
    }
    // One sealed segment written by `SEGMENT_EPOCH`, as a writer publishes it.
    let (bytes, _) = crate::wal::format::segment::SegmentBuilder::new(
        1,
        SEGMENT_EPOCH,
        crate::wal::format::ChainLink::GENESIS,
    )
    .bytes(&bytes)
    .seal();
    let publication = CatalogEntry::from_sealed_bytes(&bytes).map_err(MidgeError::Corruption)?;
    let remote_path = root.join("cloud").join(publication.object_key());
    if let Some(parent) = remote_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(remote_path, &bytes)?;
    let mut catalog = WalPublicationCatalog::empty(9).map_err(MidgeError::Internal)?;
    catalog
        .append_run(9, &[publication])
        .map_err(MidgeError::from)?;
    Ok(FixtureWal { records, catalog })
}

fn replay_fixture(
    fs: &dyn ReadFs,
    segments: &crate::wal::recovery::ReplaySegments,
    memtables: &mut HashMap<u32, Arc<SkipListMemtable>>,
) -> MidgeResult<RecoveryStats> {
    let frontiers = crate::metadata::frontiers::FlushFrontiers::default();
    replay_wal_with_options(
        fs,
        segments,
        memtables,
        ReplayPolicy::Strict,
        StreamingReplayLimits::local(),
        ReplayOptions {
            selection: crate::wal::replay::ReplaySelection::Frontiers(&frontiers),
            ..ReplayOptions::default()
        },
        &mut |_, _| Ok(()),
    )
}

fn verify_rows(
    records: &[WalRecord],
    memtables: &HashMap<u32, Arc<SkipListMemtable>>,
) -> (u64, u64) {
    let mut verified = 0;
    let mut mismatches = 0;
    for record in records {
        let state = memtables.get(&0).map_or(KeyState::Absent, |table| {
            table.get_raw_key_state_at(&record.key, u64::MAX)
        });
        let matches = matches!(state, KeyState::Value(ref value, sequence, None, EntryType::Put)
            if Some(value) == record.value.as_ref() && sequence == record.seq);
        if matches {
            verified += 1;
        } else {
            mismatches += 1;
        }
    }
    (verified, mismatches)
}

/// Bytes and files staged in `wal/`: everything but the ledger and segments
/// holding no record (at most a header), such as a new ledger's open
/// segment.
fn local_wal_state(local: &Path) -> MidgeResult<(u64, u64)> {
    let mut bytes = 0_u64;
    let mut files = 0_u64;
    for entry in std::fs::read_dir(local.join("wal"))? {
        let entry = entry?;
        if entry.file_name() == crate::wal::ledger_store::LEDGER_FILE {
            continue;
        }
        let metadata = entry.metadata()?;
        if metadata.is_file() && metadata.len() > crate::wal::format::HEADER_LEN as u64 {
            files += 1;
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok((bytes, files))
}
