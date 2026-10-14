use crate::common::MidgeResult;
use crate::io::FsError;
use crate::metadata::manifest::FileMeta;
use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::path::Path;
use std::sync::LazyLock;

const MANIFEST_WRITER_LOCK_STRIPES: usize = 64;
static MANIFEST_WRITER_LOCKS: LazyLock<Vec<parking_lot::Mutex<()>>> = LazyLock::new(|| {
    (0..MANIFEST_WRITER_LOCK_STRIPES)
        .map(|_| parking_lot::Mutex::new(()))
        .collect()
});

pub(crate) fn with_manifest_writer_lock<T>(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    operation: impl FnOnce() -> T,
) -> T {
    crate::failpoints::with_read_gate(|| {
        let stripe_count = u64::try_from(MANIFEST_WRITER_LOCK_STRIPES).unwrap_or(64);
        let stripe = usize::try_from(fs.coordination_key() % stripe_count).unwrap_or(0);
        let _guard = MANIFEST_WRITER_LOCKS[stripe].lock();
        operation()
    })
}

pub(crate) fn with_manifest_writer_lock_within<T>(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    scope: &crate::common::DeadlineScope,
    operation: impl FnOnce() -> MidgeResult<T>,
) -> MidgeResult<T> {
    crate::failpoints::with_read_gate(|| {
        scope.check("manifest writer lock")?;
        let stripe_count = u64::try_from(MANIFEST_WRITER_LOCK_STRIPES).unwrap_or(64);
        let stripe = usize::try_from(fs.coordination_key() % stripe_count).unwrap_or(0);
        let lock = &MANIFEST_WRITER_LOCKS[stripe];
        let deadline = scope.deadline();
        let _guard = if deadline.is_bounded() {
            lock.try_lock_for(deadline.remaining()).ok_or_else(|| {
                crate::common::MidgeError::Timeout("manifest writer lock timed out".into())
            })?
        } else {
            lock.lock()
        };
        scope.check("manifest writer lock")?;
        let result = operation()?;
        scope.check("manifest writer completion")?;
        Ok(result)
    })
}

fn journal_serialize<T: ?Sized + serde::Serialize>(value: &T) -> MidgeResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| crate::common::MidgeError::Internal(e.to_string()))
}

fn journal_deserialize<T: serde::de::DeserializeOwned>(payload: &[u8]) -> MidgeResult<T> {
    serde_json::from_slice(payload).map_err(|e| crate::common::MidgeError::Internal(e.to_string()))
}

fn millis_since_epoch() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn payload_len_u32(payload: &[u8]) -> MidgeResult<u32> {
    u32::try_from(payload.len())
        .map_err(|_| crate::common::MidgeError::Internal("journal payload too large".to_string()))
}

/// TLV-encoded manifest edit record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManifestEdit {
    AddSst(FileMeta),
    RemoveSst {
        name: String,
    },
    CreateColumnFamily {
        id: u32,
        name: String,
        created_at: u64,
    },
    /// Durable drop intent carrying the snapshot frontier and exact SST set.
    DropColumnFamilyAt {
        id: u32,
        drop_sequence: u64,
        #[serde(default)]
        dropped_sst_names: Vec<String>,
    },
    /// Remove the captured SST set after the minimum snapshot passed the drop
    /// frontier. The CF tombstone itself remains to prevent ID reuse.
    ReclaimColumnFamily {
        id: u32,
        #[serde(default)]
        names: Vec<String>,
    },
    AdvanceFlushFrontier {
        cf_id: u32,
        freeze_sequence: u64,
    },
    BumpNextSstSeq {
        cf_id: u32,
        next_seq: u64,
    },
    /// Batch of edits emitted atomically as a single journal TLV record
    Batch(Vec<ManifestEdit>),
}

/// Every journal edit record's payload: the edit with a durable monotonic
/// identity, so checkpoint recovery can ignore edits the snapshot already
/// holds even when journal truncation did not complete. Replay refuses a
/// record without it as corruption.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalEditEnvelope {
    edit_id: u64,
    edit: ManifestEdit,
}

#[derive(Debug, Clone)]
struct JournalEdit {
    edit_id: u64,
    edit: ManifestEdit,
}

impl ManifestEdit {
    pub(crate) fn validate_for_append(&self) -> MidgeResult<()> {
        self.validate_persisted_sst_names()
    }

    pub fn record_type(&self) -> u8 {
        match self {
            ManifestEdit::AddSst(_) => 1,
            ManifestEdit::RemoveSst { .. } => 2,
            ManifestEdit::CreateColumnFamily { .. } => 3,
            ManifestEdit::AdvanceFlushFrontier { .. } => 12,
            ManifestEdit::BumpNextSstSeq { .. } => 6,
            ManifestEdit::Batch(_) => 8,
            ManifestEdit::DropColumnFamilyAt { .. } => 10,
            ManifestEdit::ReclaimColumnFamily { .. } => 11,
        }
    }

    fn validate_persisted_sst_names(&self) -> MidgeResult<()> {
        fn validate(name: &str) -> MidgeResult<()> {
            crate::cloud_layout::PersistedSstName::parse(name).map(|_| ())
        }

        match self {
            ManifestEdit::AddSst(meta) => validate(&meta.name),
            ManifestEdit::RemoveSst { name } => validate(name),
            ManifestEdit::DropColumnFamilyAt {
                dropped_sst_names, ..
            }
            | ManifestEdit::ReclaimColumnFamily {
                names: dropped_sst_names,
                ..
            } => {
                for name in dropped_sst_names {
                    validate(name)?;
                }
                Ok(())
            }
            ManifestEdit::Batch(edits) => {
                for edit in edits {
                    edit.validate_persisted_sst_names()?;
                }
                Ok(())
            }
            ManifestEdit::CreateColumnFamily { .. }
            | ManifestEdit::AdvanceFlushFrontier { .. }
            | ManifestEdit::BumpNextSstSeq { .. } => Ok(()),
        }
    }
}

// Special TLV record type used to mark a durable sync point in the journal.
// When replaying, only edits up to the last marker are considered durable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsyncMarker {
    pub last_persisted_sequence: u64,
    pub ts_millis: u64,
}

const BATCH_RECORD_TYPE: u8 = 8;
const FSYNC_MARKER_TYPE: u8 = 9;
const DROP_COLUMN_FAMILY_AT_RECORD_TYPE: u8 = 10;
const RECLAIM_COLUMN_FAMILY_RECORD_TYPE: u8 = 11;
const ADVANCE_FLUSH_FRONTIER_RECORD_TYPE: u8 = 12;

const JOURNAL_FILE: &str = super::files::JOURNAL;
const JOURNAL_REPAIR_TEMP_FILE: &str = "manifest.journal.repair.tmp";

fn encode_journal_record<T: ?Sized + serde::Serialize>(
    record_type: u8,
    value: &T,
) -> MidgeResult<Vec<u8>> {
    let payload = journal_serialize(value)?;
    let mut hasher = Crc32::new();
    hasher.update(&payload);
    let crc = hasher.finalize();

    let mut record = Vec::with_capacity(1 + 4 + payload.len() + 4);
    record.push(record_type);
    record.extend_from_slice(&payload_len_u32(&payload)?.to_le_bytes());
    record.extend_from_slice(&payload);
    record.extend_from_slice(&crc.to_le_bytes());
    Ok(record)
}

pub(crate) fn checkpoint_edit_id_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<u64> {
    use crate::io::traits::{FsPath, OpenMode, OpenOptions};

    let path = FsPath::new(super::files::MANIFEST_SNAPSHOT);
    let snapshot_exists = match fs.exists(&path) {
        Ok(exists) => exists,
        Err(crate::io::traits::FsError::NotFound(_)) => false,
        Err(error) => return Err(FsError::into_midge(error)),
    };
    if !snapshot_exists {
        return Ok(0);
    }

    let file = fs
        .open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    let len = file.len().map_err(FsError::into_midge)?;
    let bytes = file.read_at(0, len).map_err(FsError::into_midge)?;
    let manifest: crate::metadata::Manifest = serde_json::from_slice(&bytes)
        .map_err(|error| crate::common::MidgeError::Internal(error.to_string()))?;
    Ok(manifest.edit_checkpoint_id)
}

#[cfg(test)]
pub(crate) fn next_edit_id_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<u64> {
    next_edit_id_after(highest_edit_id_with_repair_with_fs_unlocked(fs)?)
}

include!("journal/recovery.rs");
include!("journal/publication.rs");

/// Append `edit` through a `RealFs` rooted at `db_path`.
#[cfg(test)]
pub fn append_edit(db_path: &Path, edit: &ManifestEdit) -> MidgeResult<u64> {
    let fs: std::sync::Arc<dyn crate::io::traits::Fs> =
        std::sync::Arc::new(crate::io::real::RealFs::new(db_path).map_err(|e| {
            crate::common::MidgeError::Internal(format!("failed to create RealFs: {e:?}"))
        })?);
    append_edit_with_fs(&fs, edit)
}

/// Replay the journal through a `RealFs` rooted at `db_path`.
#[cfg(test)]
pub fn replay_journal(db_path: &Path) -> MidgeResult<Vec<ManifestEdit>> {
    let fs: std::sync::Arc<dyn crate::io::traits::Fs> =
        std::sync::Arc::new(crate::io::real::RealFs::new(db_path).map_err(|e| {
            crate::common::MidgeError::Internal(format!("failed to create RealFs: {e:?}"))
        })?);
    replay_journal_with_fs(&fs)
}

/// Replay a journal file at `db_path`. Returns durable edits in order.
/// A genuine incomplete EOF append is ignored; complete corrupt records are rejected.
#[cfg(test)]
pub fn replay_journal_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<Vec<ManifestEdit>> {
    replay_journal_with_ids_with_fs(fs)
        .map(|edits| edits.into_iter().map(|edit| edit.edit).collect())
}

#[cfg(test)]
fn replay_journal_with_ids_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<Vec<JournalEdit>> {
    replay_journal_with_state(fs).map(|replay| replay.edits)
}

include!("journal/replay.rs");

pub(crate) fn truncate_journal_with_fs_unlocked(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<()> {
    use crate::io::traits::{FsPath, OpenMode, OpenOptions};

    // Opening with truncate=true will set length to 0
    let mut f = fs
        .open(
            &FsPath::new(JOURNAL_FILE),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: true,
            },
        )
        .map_err(FsError::into_midge)?;

    // Sync file to durable state
    f.sync(crate::io::traits::Durability::Durable)
        .map_err(FsError::into_midge)?;

    Ok(())
}

#[cfg(test)]
include!("journal/tests.rs");
