//! Actual local journal replay for the external watchdog's isolated child.

use super::{Manifest, ManifestEdit, ManifestPersistence};
use crate::common::{MidgeError, MidgeResult};
use crate::config::RecoveryPolicy;
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, Fs, FsError, FsPath, FsResult, OpenOptions};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const EDIT_COUNT: u32 = 16;
const READ_DELAY: Duration = Duration::from_millis(20);

/// Independent scalar evidence from the actual manifest journal load.
#[doc(hidden)]
#[derive(Clone, Debug, Serialize)]
pub struct JournalRecoveryProgressFixtureResult {
    pub expected_edits: u64,
    pub verified_edits: u64,
    pub max_edit_id: u64,
    pub manifest_edit_checkpoint_id: u64,
    pub restored_cf_count: u64,
    pub mismatches: u64,
    pub completed_local_reads: u64,
    pub completed_local_read_bytes: u64,
    pub journal_bytes: u64,
    pub local_wal_bytes: u64,
    pub staged_wal_count: u64,
    pub elapsed_ms: u64,
}

#[derive(Serialize)]
struct ObservationSnapshot {
    phase: &'static str,
    #[serde(flatten)]
    result: JournalRecoveryProgressFixtureResult,
}

struct Observations {
    fs: Arc<dyn Fs>,
    started: Instant,
    snapshot: Mutex<ObservationSnapshot>,
}

impl Observations {
    fn persist(&self) -> MidgeResult<()> {
        let mut snapshot = self.snapshot.lock();
        snapshot.result.elapsed_ms =
            u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let bytes = serde_json::to_vec(&*snapshot).map_err(|error| {
            MidgeError::Internal(format!("journal fixture observations: {error}"))
        })?;
        crate::io::staging::stage_bytes(
            &self.fs,
            &FsPath::new(".fixture-observations.tmp"),
            &FsPath::new("fixture-observations.json"),
            &bytes,
            MidgeError::Internal,
        )
    }

    fn completed_read(&self, bytes: usize) -> FsResult<()> {
        let mut snapshot = self.snapshot.lock();
        snapshot.result.completed_local_reads += 1;
        snapshot.result.completed_local_read_bytes = snapshot
            .result
            .completed_local_read_bytes
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        drop(snapshot);
        self.persist()
            .map_err(|error| FsError::Io(error.to_string()))
    }
}

/// Seed durable journal edits, then load them through the actual parser with
/// bounded successful-read delays. The sampled local directory stays empty.
///
/// # Errors
///
/// Returns setup, journal, manifest-load, or observation-persistence failures.
#[doc(hidden)]
pub fn run_journal_recovery_progress_fixture(
    root: &Path,
) -> MidgeResult<JournalRecoveryProgressFixtureResult> {
    // Arrange: append real edits and their durability markers without delays.
    let started = Instant::now();
    let local_wal = root.join("local/wal");
    std::fs::create_dir_all(&local_wal)?;
    let (local_wal_bytes, staged_wal_count) = local_wal_state(&local_wal)?;
    if local_wal_bytes != 0 || staged_wal_count != 0 {
        return Err(MidgeError::InvalidArgument(
            "journal fixture requires an empty sampled local WAL directory".into(),
        ));
    }
    let memory: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    let max_edit_id = seed_journal(&memory)?;
    let journal_bytes = memory
        .metadata(&FsPath::new(super::files::JOURNAL))
        .map_err(FsError::into_midge)?
        .len;
    let observations = Arc::new(Observations {
        fs: Arc::new(crate::io::RealFs::new(root).map_err(FsError::into_midge)?),
        started,
        snapshot: Mutex::new(ObservationSnapshot {
            phase: "journal_replay",
            result: JournalRecoveryProgressFixtureResult {
                expected_edits: u64::from(EDIT_COUNT),
                verified_edits: 0,
                max_edit_id,
                manifest_edit_checkpoint_id: 0,
                restored_cf_count: 0,
                mismatches: 0,
                completed_local_reads: 0,
                completed_local_read_bytes: 0,
                journal_bytes,
                local_wal_bytes,
                staged_wal_count,
                elapsed_ms: 0,
            },
        }),
    });
    observations.persist()?;
    let delayed: Arc<dyn Fs> = Arc::new(DelayedReadFs {
        inner: memory,
        observations: Arc::clone(&observations),
    });

    // Act: this is the same manifest load used by startup and hydration.
    let manifest = ManifestPersistence::load_with_fs_and_policy(&delayed, RecoveryPolicy::Strict)
        .map_err(MidgeError::Internal)?;

    // Assert: independent evidence checks exact CF metadata and edit frontier.
    let verified_edits = verified_edits(&manifest);
    let (local_wal_bytes, staged_wal_count) = local_wal_state(&local_wal)?;
    let mut snapshot = observations.snapshot.lock();
    snapshot.result.verified_edits = verified_edits;
    snapshot.result.manifest_edit_checkpoint_id = manifest.edit_checkpoint_id;
    snapshot.result.restored_cf_count =
        u64::try_from(manifest.column_families.len()).unwrap_or(u64::MAX);
    snapshot.result.mismatches = u64::from(EDIT_COUNT).saturating_sub(verified_edits)
        + u64::from(manifest.edit_checkpoint_id != max_edit_id)
        + u64::from(snapshot.result.restored_cf_count != u64::from(EDIT_COUNT))
        + u64::from(!manifest.files.is_empty());
    snapshot.result.local_wal_bytes = local_wal_bytes;
    snapshot.result.staged_wal_count = staged_wal_count;
    snapshot.phase = "complete";
    drop(snapshot);
    observations.persist()?;
    let result = observations.snapshot.lock().result.clone();
    Ok(result)
}

fn seed_journal(fs: &Arc<dyn Fs>) -> MidgeResult<u64> {
    let store = super::store::ManifestStore::new(Arc::clone(fs));
    let mut max_edit_id = 0;
    for id in 1..=EDIT_COUNT {
        max_edit_id = store.append(&ManifestEdit::CreateColumnFamily {
            id,
            name: format!("journal-fixture-{id}"),
            created_at: u64::from(id),
        })?;
        if max_edit_id != u64::from(id) {
            return Err(MidgeError::Internal(
                "journal fixture append did not allocate contiguous edit identities".into(),
            ));
        }
    }
    Ok(max_edit_id)
}

fn verified_edits(manifest: &Manifest) -> u64 {
    (1..=EDIT_COUNT)
        .filter(|&id| {
            manifest.column_families.iter().any(|family| {
                family.id == id
                    && family.name == format!("journal-fixture-{id}")
                    && family.created_at == u64::from(id)
                    && family.deleted_at.is_none()
                    && family.drop_sequence.is_none()
                    && family.dropped_sst_names.is_empty()
                    && !family.reclaimed
            })
        })
        .count() as u64
}

fn local_wal_state(path: &Path) -> MidgeResult<(u64, u64)> {
    let mut bytes = 0_u64;
    let mut files = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let metadata = entry?.metadata()?;
        if metadata.is_file() {
            files += 1;
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok((bytes, files))
}

struct DelayedReadFile<'a> {
    inner: Box<dyn File + 'a>,
    observations: Arc<Observations>,
}

impl File for DelayedReadFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        let bytes = self.inner.read_at(offset, len)?;
        std::thread::sleep(READ_DELAY);
        self.observations.completed_read(bytes.len())?;
        Ok(bytes)
    }

    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        self.inner.write_at(offset, data)
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        self.inner.truncate(len)
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        self.inner.append(data)
    }

    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }

    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        self.inner.sync(durability)
    }
}

struct DelayedReadFs {
    inner: Arc<dyn Fs>,
    observations: Arc<Observations>,
}

impl Fs for DelayedReadFs {
    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }

    fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        Ok(Box::new(DelayedReadFile {
            inner: self.inner.open(path, options)?,
            observations: Arc::clone(&self.observations),
        }))
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(path)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(path)
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(path)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_dir_all(path)
    }

    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        self.inner.sync_dir(path, durability)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}
