use crate::common::MidgeResult;
use crate::metadata::manifest::{CloudCheckpoint, FileMeta};
use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};
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
    DropColumnFamily {
        id: u32,
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
    BumpWalSeq {
        seq: u64,
    },
    BumpNextSstSeq {
        cf_id: u32,
        next_seq: u64,
    },
    SetCloudCheckpoint(CloudCheckpoint),
    /// Batch of edits emitted atomically as a single journal TLV record
    Batch(Vec<ManifestEdit>),
}

/// Journal payload wrapper used by new writers.
///
/// The record framing and edit enum remain unchanged so older journals can be
/// replayed. The wrapper adds a durable monotonic identity to each record,
/// allowing checkpoint recovery to ignore edits already represented by the
/// snapshot even when journal truncation did not complete.
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
    pub fn record_type(&self) -> u8 {
        match self {
            ManifestEdit::AddSst(_) => 1,
            ManifestEdit::RemoveSst { .. } => 2,
            ManifestEdit::CreateColumnFamily { .. } => 3,
            ManifestEdit::DropColumnFamily { .. } => 4,
            ManifestEdit::BumpWalSeq { .. } => 5,
            ManifestEdit::BumpNextSstSeq { .. } => 6,
            ManifestEdit::SetCloudCheckpoint(_) => 7,
            ManifestEdit::Batch(_) => 8,
            ManifestEdit::DropColumnFamilyAt { .. } => 10,
            ManifestEdit::ReclaimColumnFamily { .. } => 11,
        }
    }

    fn validate_persisted_sst_names(&self) -> MidgeResult<()> {
        fn validate(name: &str) -> MidgeResult<()> {
            crate::sst::PersistedSstName::parse(name).map(|_| ())
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
            ManifestEdit::SetCloudCheckpoint(checkpoint) => {
                for name in &checkpoint.covering_ssts {
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
            | ManifestEdit::DropColumnFamily { .. }
            | ManifestEdit::BumpWalSeq { .. }
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

const JOURNAL_FILE: &str = "manifest.journal";
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

fn checkpoint_edit_id_with_fs(fs: &std::sync::Arc<dyn crate::io::traits::Fs>) -> MidgeResult<u64> {
    use crate::io::traits::{FsPath, OpenMode, OpenOptions};

    let path = FsPath::new("manifest.snapshot.json");
    let snapshot_exists = match fs.exists(&path) {
        Ok(exists) => exists,
        Err(crate::io::traits::FsError::NotFound(_)) => false,
        Err(error) => return Err(crate::common::MidgeError::from(error)),
    };
    if !snapshot_exists {
        return Ok(0);
    }

    let file = fs.open(
        &path,
        OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    )?;
    let len = file.len().map_err(crate::common::MidgeError::from)?;
    let bytes = file
        .read_at(0, len)
        .map_err(crate::common::MidgeError::from)?;
    let manifest: crate::metadata::Manifest = serde_json::from_slice(&bytes)
        .map_err(|error| crate::common::MidgeError::Internal(error.to_string()))?;
    Ok(manifest.edit_checkpoint_id)
}

fn next_edit_id_with_fs(fs: &std::sync::Arc<dyn crate::io::traits::Fs>) -> MidgeResult<u64> {
    let checkpoint = checkpoint_edit_id_with_fs(fs)?;
    let replay = replay_journal_with_state(fs)?;
    if let JournalReplayTail::PartialEof {
        last_valid_offset,
        record_start,
        reason,
    } = replay.tail
    {
        tracing::warn!(
            last_valid_offset,
            record_start,
            file_len = replay.file_len,
            reason,
            "repairing incomplete manifest journal tail before append"
        );
        repair_partial_journal_tail(fs, last_valid_offset)?;
    }
    let journal_max = replay.max_edit_id;
    Ok(checkpoint.max(journal_max).saturating_add(1).max(1))
}

pub(crate) fn highest_edit_id_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<u64> {
    with_manifest_writer_lock(fs, || highest_edit_id_with_fs_unlocked(fs))
}

pub(crate) fn highest_edit_id_with_fs_unlocked(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<u64> {
    let checkpoint = checkpoint_edit_id_with_fs(fs)?;
    let journal_max = replay_journal_with_state(fs)?.max_edit_id;
    Ok(checkpoint.max(journal_max))
}

pub(crate) fn replay_edits_after_with_fs_unlocked(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    checkpoint: u64,
) -> MidgeResult<Vec<ManifestEdit>> {
    Ok(replay_journal_with_ids_with_fs(fs)?
        .into_iter()
        .filter(|edit| edit.edit_id > checkpoint)
        .map(|edit| edit.edit)
        .collect())
}

/// Append an edit to the manifest journal using a provided Fs (preferred).
pub fn append_edit_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    edit: &ManifestEdit,
) -> MidgeResult<()> {
    edit.validate_persisted_sst_names()?;
    with_manifest_writer_lock(fs, || append_validated_edit_with_fs(fs, edit))
}

fn append_validated_edit_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    edit: &ManifestEdit,
) -> MidgeResult<()> {
    let edit_id = next_edit_id_with_fs(fs)?;
    let record = encode_journal_record(
        edit.record_type(),
        &JournalEditEnvelope {
            edit_id,
            edit: edit.clone(),
        },
    )?;

    crate::failpoints::fail_point!("midge::manifest::inject_no_space_on_append_edit", |_| Err(
        crate::common::MidgeError::NoSpace(
            "failpoint: no space on manifest journal append".to_string()
        )
    ));
    let (write_ns, fsync_ns) = append_record_and_marker_with_fs(fs, record, edit_id)?;
    tracing::info!(
        write_ns,
        fsync_ns,
        "manifest journal append: edit and marker durably synced (ns)"
    );
    Ok(())
}

fn append_record_and_marker_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    record: Vec<u8>,
    edit_id: u64,
) -> MidgeResult<(u128, u128)> {
    use crate::io::traits::{Durability, FsPath, OpenMode, OpenOptions};

    let marker = encode_journal_record(
        FSYNC_MARKER_TYPE,
        &FsyncMarker {
            last_persisted_sequence: edit_id,
            ts_millis: millis_since_epoch(),
        },
    )?;

    let mut file = fs.open(
        &FsPath::new(JOURNAL_FILE),
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        },
    )?;

    let write_start = std::time::Instant::now();
    file.append(bytes::Bytes::from(record))
        .map_err(crate::common::MidgeError::from)?;

    crate::failpoints::fail_point!("midge::manifest::inject_fsync_marker_write_failure", |_| {
        Err(crate::common::MidgeError::Io(std::io::Error::other(
            "injected manifest fsync marker write failure",
        )))
    });
    file.append(bytes::Bytes::from(marker))
        .map_err(crate::common::MidgeError::from)?;
    let write_ns = write_start.elapsed().as_nanos();

    crate::failpoints::fail_point!("midge::manifest::before_required_sync");
    crate::failpoints::fail_point!("midge::manifest::inject_required_sync_failure", |_| Err(
        crate::common::MidgeError::Io(std::io::Error::other(
            "injected required manifest journal sync failure"
        ))
    ));
    let fsync_start = std::time::Instant::now();
    file.sync(Durability::Durable)
        .map_err(crate::common::MidgeError::from)?;
    let fsync_ns = fsync_start.elapsed().as_nanos();

    Ok((write_ns, fsync_ns))
}

fn repair_partial_journal_tail(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    last_valid_offset: u64,
) -> MidgeResult<()> {
    use crate::io::traits::{FsPath, OpenMode, OpenOptions};

    let journal_path = FsPath::new(JOURNAL_FILE);
    let file = fs.open(
        &journal_path,
        OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    )?;
    let file_len = file.len().map_err(crate::common::MidgeError::from)?;
    if last_valid_offset > file_len {
        return Err(crate::common::MidgeError::Corruption(format!(
            "manifest journal repair offset {last_valid_offset} exceeds file length {file_len}"
        )));
    }
    let valid_prefix = file
        .read_at(0, last_valid_offset)
        .map_err(crate::common::MidgeError::from)?;
    drop(file);

    crate::io::staging::stage_bytes(
        fs,
        &FsPath::new(JOURNAL_REPAIR_TEMP_FILE),
        &journal_path,
        &valid_prefix,
        crate::common::MidgeError::Internal,
    )
}

/// Convenience wrapper: append via a `RealFs` created from `db_path` (backwards compatible)
pub fn append_edit(db_path: &Path, edit: &ManifestEdit) -> MidgeResult<()> {
    let fs: std::sync::Arc<dyn crate::io::traits::Fs> =
        std::sync::Arc::new(crate::io::real::RealFs::new(db_path).map_err(|e| {
            crate::common::MidgeError::Internal(format!("failed to create RealFs: {e:?}"))
        })?);
    append_edit_with_fs(&fs, edit)
}

/// Convenience wrapper: replay journal via a `RealFs` created from `db_path` (backwards compatible)
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

fn replay_journal_with_ids_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<Vec<JournalEdit>> {
    replay_journal_with_state(fs).map(|replay| replay.edits)
}

fn replay_journal_with_state(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<JournalReplay> {
    let Some(file) = open_journal_for_replay(fs)? else {
        return Ok(JournalReplay::empty());
    };

    let file_len = file.len().map_err(crate::common::MidgeError::from)?;
    let mut state = JournalReplayState::default();
    let mut offset: u64 = 0;
    let mut tail = JournalReplayTail::Clean;

    while offset < file_len {
        match read_journal_record(&*file, offset, file_len)? {
            JournalRecordStatus::Record(record) => {
                offset = record.next_offset;
                validate_journal_record_crc(&record)?;
                handle_journal_record(&record, &mut state)?;
            }
            JournalRecordStatus::PartialHeader { record_start } => {
                tail = JournalReplayTail::PartialEof {
                    last_valid_offset: state.last_marker_offset.unwrap_or(0),
                    record_start,
                    reason: "partial record header at EOF",
                };
                break;
            }
            JournalRecordStatus::PartialPayload { record_start } => {
                tail = JournalReplayTail::PartialEof {
                    last_valid_offset: state.last_marker_offset.unwrap_or(0),
                    record_start,
                    reason: "partial record payload or CRC at EOF",
                };
                break;
            }
        }
    }

    // Windows does not permit replacing a file while a read handle is still
    // open. Release the replay handle before a caller repairs a partial tail
    // with an atomic replacement.
    drop(file);

    let durable_edit_count = state.last_marker_edit_idx.unwrap_or(0);
    if matches!(tail, JournalReplayTail::Clean) && state.edits.len() > durable_edit_count {
        tail = JournalReplayTail::PartialEof {
            last_valid_offset: state.last_marker_offset.unwrap_or(0),
            record_start: state.last_marker_offset.unwrap_or(0),
            reason: "edit record at EOF is missing its durability marker",
        };
    }

    state.edits.truncate(durable_edit_count);
    let max_edit_id = state
        .edits
        .iter()
        .map(|edit| edit.edit_id)
        .max()
        .unwrap_or(0);

    Ok(JournalReplay {
        edits: state.edits,
        max_edit_id,
        file_len,
        tail,
    })
}

struct JournalReplay {
    edits: Vec<JournalEdit>,
    max_edit_id: u64,
    file_len: u64,
    tail: JournalReplayTail,
}

impl JournalReplay {
    fn empty() -> Self {
        Self {
            edits: Vec::new(),
            max_edit_id: 0,
            file_len: 0,
            tail: JournalReplayTail::Clean,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum JournalReplayTail {
    Clean,
    PartialEof {
        last_valid_offset: u64,
        record_start: u64,
        reason: &'static str,
    },
}

#[derive(Default)]
struct JournalReplayState {
    edits: Vec<JournalEdit>,
    last_marker_edit_idx: Option<usize>,
    last_marker_offset: Option<u64>,
    max_edit_id: u64,
}

struct JournalRecord {
    record_start: u64,
    typ: u8,
    payload: Vec<u8>,
    got_crc: u32,
    next_offset: u64,
}

enum JournalRecordStatus {
    Record(JournalRecord),
    PartialHeader { record_start: u64 },
    PartialPayload { record_start: u64 },
}

fn open_journal_for_replay(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<Option<Box<dyn crate::io::traits::File + '_>>> {
    use crate::io::traits::FsPath;

    match fs.open(
        &FsPath::new(JOURNAL_FILE),
        crate::io::traits::OpenOptions {
            mode: crate::io::traits::OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    ) {
        Ok(file) => Ok(Some(file)),
        Err(crate::io::traits::FsError::NotFound(_)) => Ok(None),
        Err(crate::io::traits::FsError::Io(message))
            if message.contains("No such file") || message.contains("no such file") =>
        {
            Ok(None)
        }
        Err(e) => Err(crate::common::MidgeError::from(e)),
    }
}

fn read_journal_record(
    file: &dyn crate::io::traits::File,
    offset: u64,
    file_len: u64,
) -> MidgeResult<JournalRecordStatus> {
    let record_start = offset;
    let typ = file
        .read_at(offset, 1)
        .map_err(crate::common::MidgeError::from)?[0];
    validate_journal_record_type(typ, record_start)?;
    if offset + 5 > file_len {
        return Ok(JournalRecordStatus::PartialHeader { record_start });
    }

    let len_offset = offset + 1;
    let len_bytes = file
        .read_at(len_offset, 4)
        .map_err(crate::common::MidgeError::from)?;
    let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
    let payload_offset = len_offset + 4;

    if payload_offset + (len as u64) + 4 > file_len {
        return Ok(JournalRecordStatus::PartialPayload { record_start });
    }

    let payload = file
        .read_at(payload_offset, len as u64)
        .map_err(crate::common::MidgeError::from)?;
    let crc_offset = payload_offset + len as u64;
    let crc_bytes = file
        .read_at(crc_offset, 4)
        .map_err(crate::common::MidgeError::from)?;
    let got_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);

    Ok(JournalRecordStatus::Record(JournalRecord {
        record_start,
        typ,
        payload: payload.to_vec(),
        got_crc,
        next_offset: crc_offset + 4,
    }))
}

fn validate_journal_record_type(record_type: u8, record_start: u64) -> MidgeResult<()> {
    if (1..=RECLAIM_COLUMN_FAMILY_RECORD_TYPE).contains(&record_type) {
        return Ok(());
    }

    Err(crate::common::MidgeError::Corruption(format!(
        "manifest journal has unknown record type {record_type} at byte {record_start}"
    )))
}

fn validate_journal_record_crc(record: &JournalRecord) -> MidgeResult<()> {
    let mut hasher = Crc32::new();
    hasher.update(&record.payload);
    let calc = hasher.finalize();
    if calc == record.got_crc {
        return Ok(());
    }

    Err(crate::common::MidgeError::Corruption(format!(
        "manifest journal CRC mismatch at byte {}",
        record.record_start
    )))
}

fn handle_journal_record(
    record: &JournalRecord,
    state: &mut JournalReplayState,
) -> MidgeResult<()> {
    let typ = record.typ;
    if typ == FSYNC_MARKER_TYPE {
        return handle_fsync_marker_record(record, state);
    }
    if typ == BATCH_RECORD_TYPE {
        return handle_batch_record(record, state);
    }
    handle_manifest_edit_record(record, state)
}

fn handle_fsync_marker_record(
    record: &JournalRecord,
    state: &mut JournalReplayState,
) -> MidgeResult<()> {
    let marker = journal_deserialize::<FsyncMarker>(&record.payload).map_err(|error| {
        crate::common::MidgeError::Corruption(format!(
            "manifest journal fsync marker at byte {} is invalid: {error}",
            record.record_start
        ))
    })?;
    tracing::info!(
        last_seq = marker.last_persisted_sequence,
        ts = marker.ts_millis,
        "journal fsync marker encountered"
    );
    state.last_marker_edit_idx = Some(state.edits.len());
    state.last_marker_offset = Some(record.next_offset);
    Ok(())
}

fn handle_batch_record(record: &JournalRecord, state: &mut JournalReplayState) -> MidgeResult<()> {
    if let Ok(envelope) = journal_deserialize::<JournalEditEnvelope>(&record.payload) {
        if !matches!(envelope.edit, ManifestEdit::Batch(_)) {
            return Err(journal_record_type_mismatch(record, &envelope.edit));
        }
        push_journal_edit(
            state,
            JournalEdit {
                edit_id: envelope.edit_id,
                edit: envelope.edit,
            },
        )?;
        return Ok(());
    }

    let batch = journal_deserialize::<Vec<ManifestEdit>>(&record.payload).map_err(|error| {
        crate::common::MidgeError::Corruption(format!(
            "manifest journal batch at byte {} is invalid: {error}",
            record.record_start
        ))
    })?;
    let edit_id = next_replay_edit_id(state);
    push_journal_edit(
        state,
        JournalEdit {
            edit_id,
            edit: ManifestEdit::Batch(batch),
        },
    )?;
    Ok(())
}

fn handle_manifest_edit_record(
    record: &JournalRecord,
    state: &mut JournalReplayState,
) -> MidgeResult<()> {
    let is_legacy_edit = (1..BATCH_RECORD_TYPE).contains(&record.typ);
    let is_reclamation_edit = matches!(
        record.typ,
        DROP_COLUMN_FAMILY_AT_RECORD_TYPE | RECLAIM_COLUMN_FAMILY_RECORD_TYPE
    );
    if !is_legacy_edit && !is_reclamation_edit {
        return Err(crate::common::MidgeError::Corruption(format!(
            "manifest journal has unknown record type {} at byte {}",
            record.typ, record.record_start
        )));
    }

    if let Ok(envelope) = journal_deserialize::<JournalEditEnvelope>(&record.payload) {
        if envelope.edit.record_type() != record.typ {
            return Err(journal_record_type_mismatch(record, &envelope.edit));
        }
        push_journal_edit(
            state,
            JournalEdit {
                edit_id: envelope.edit_id,
                edit: envelope.edit,
            },
        )?;
        return Ok(());
    }

    let edit = journal_deserialize::<ManifestEdit>(&record.payload).map_err(|error| {
        crate::common::MidgeError::Corruption(format!(
            "manifest journal edit at byte {} is invalid: {error}",
            record.record_start
        ))
    })?;
    if edit.record_type() != record.typ {
        return Err(journal_record_type_mismatch(record, &edit));
    }
    let edit_id = next_replay_edit_id(state);
    push_journal_edit(state, JournalEdit { edit_id, edit })?;
    Ok(())
}

fn journal_record_type_mismatch(
    record: &JournalRecord,
    edit: &ManifestEdit,
) -> crate::common::MidgeError {
    crate::common::MidgeError::Corruption(format!(
        "manifest journal record type {} at byte {} does not match payload type {}",
        record.typ,
        record.record_start,
        edit.record_type()
    ))
}

fn push_journal_edit(state: &mut JournalReplayState, edit: JournalEdit) -> MidgeResult<()> {
    edit.edit.validate_persisted_sst_names()?;
    state.max_edit_id = state.max_edit_id.max(edit.edit_id);
    state.edits.push(edit);
    Ok(())
}

fn next_replay_edit_id(state: &JournalReplayState) -> u64 {
    state.max_edit_id.saturating_add(1).max(1)
}

/// Append a batch of edits as a single TLV record using the provided Fs (preferred).
pub fn append_edit_batch_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    batch: &[ManifestEdit],
) -> MidgeResult<()> {
    for edit in batch {
        edit.validate_persisted_sst_names()?;
    }
    with_manifest_writer_lock(fs, || append_validated_edit_batch_with_fs(fs, batch))
}

fn append_validated_edit_batch_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    batch: &[ManifestEdit],
) -> MidgeResult<()> {
    let edit_id = next_edit_id_with_fs(fs)?;
    let record = encode_journal_record(
        BATCH_RECORD_TYPE,
        &JournalEditEnvelope {
            edit_id,
            edit: ManifestEdit::Batch(batch.to_vec()),
        },
    )?;

    crate::failpoints::fail_point!("midge::manifest::inject_no_space_on_append_edit", |_| Err(
        crate::common::MidgeError::NoSpace(
            "failpoint: no space on manifest journal append".to_string()
        )
    ));
    if batch
        .iter()
        .any(|edit| matches!(edit, ManifestEdit::AddSst(_)))
    {
        crate::failpoints::fail_point!(
            "midge::manifest::inject_no_space_on_add_sst_edit",
            |_| Err(crate::common::MidgeError::NoSpace(
                "failpoint: no space on manifest add_sst append".to_string()
            ))
        );
    }
    crate::failpoints::fail_point!(
        "midge::manifest::inject_no_space_on_append_edit_batch",
        |_| Err(crate::common::MidgeError::NoSpace(
            "failpoint: no space on manifest journal batch append".to_string()
        ))
    );
    let (write_ns, fsync_ns) = append_record_and_marker_with_fs(fs, record, edit_id)?;
    tracing::info!(
        batch_size = batch.len(),
        write_ns,
        fsync_ns,
        "manifest journal batch append: edit and marker durably synced (ns)"
    );
    Ok(())
}

/// Convenience wrapper: append batch via a `RealFs` created from `db_path` (backwards compatible)
pub fn append_edit_batch(db_path: &Path, batch: &[ManifestEdit]) -> MidgeResult<()> {
    let fs: std::sync::Arc<dyn crate::io::traits::Fs> =
        std::sync::Arc::new(crate::io::real::RealFs::new(db_path).map_err(|e| {
            crate::common::MidgeError::Internal(format!("failed to create RealFs: {e:?}"))
        })?);
    append_edit_batch_with_fs(&fs, batch)
}

pub(crate) fn truncate_journal_with_fs_unlocked(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<()> {
    use crate::io::traits::{FsPath, OpenMode, OpenOptions};

    // Opening with truncate=true will set length to 0
    let mut f = fs.open(
        &FsPath::new(JOURNAL_FILE),
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: true,
        },
    )?;

    // Sync file to durable state
    f.sync(crate::io::traits::Durability::Durable)
        .map_err(crate::common::MidgeError::from)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::traits::{
        DirEntry, Durability, File, Fs, FsPath, FsResult, Metadata, OpenOptions,
    };
    use crate::metadata::FileMeta;
    use crate::metadata::{Manifest, ManifestPersistence};
    use proptest::prelude::*;
    use std::cell::Cell;
    use std::io::Write;
    use std::sync::{Arc, Condvar, Mutex};
    use tempfile::tempdir;

    thread_local! {
        static OBSERVE_REQUIRED_MANIFEST_SYNC: Cell<bool> = const { Cell::new(false) };
    }

    #[derive(Default)]
    struct JournalOpenGate {
        state: Mutex<(usize, bool)>,
        changed: Condvar,
    }

    impl JournalOpenGate {
        fn wait_in_open(&self) {
            let mut state = self.state.lock().expect("lock journal-open gate");
            state.0 += 1;
            self.changed.notify_all();
            while !state.1 {
                state = self
                    .changed
                    .wait(state)
                    .expect("wait for journal-open release");
            }
        }

        fn wait_for_arrivals(&self, expected: usize, timeout: std::time::Duration) -> bool {
            let state = self.state.lock().expect("lock journal-open gate");
            let (state, _) = self
                .changed
                .wait_timeout_while(state, timeout, |state| state.0 < expected)
                .expect("wait for journal-open arrivals");
            state.0 >= expected
        }

        fn release(&self) {
            let mut state = self.state.lock().expect("lock journal-open gate");
            state.1 = true;
            self.changed.notify_all();
        }
    }

    struct GatedJournalFs {
        inner: crate::io::real::RealFs,
        open_gate: Arc<JournalOpenGate>,
    }

    impl Fs for GatedJournalFs {
        fn coordination_key(&self) -> u64 {
            self.inner.coordination_key()
        }

        fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            if path.0 == JOURNAL_FILE && opts.create && !opts.truncate {
                self.open_gate.wait_in_open();
            }
            self.inner.open(path, opts)
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

    fn encode_test_record(payload_edit: &ManifestEdit, crc_edit: &ManifestEdit) -> Vec<u8> {
        let payload = serde_json::to_vec(payload_edit).expect("serialize test journal payload");
        let crc_payload = serde_json::to_vec(crc_edit).expect("serialize test CRC payload");
        assert_eq!(
            payload.len(),
            crc_payload.len(),
            "test payloads must have equal lengths"
        );
        let mut hasher = Crc32::new();
        hasher.update(&crc_payload);

        let mut record = Vec::with_capacity(1 + 4 + payload.len() + 4);
        record.push(payload_edit.record_type());
        record.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("test payload length fits in u32")
                .to_le_bytes(),
        );
        record.extend_from_slice(&payload);
        record.extend_from_slice(&hasher.finalize().to_le_bytes());
        record
    }

    fn write_and_sync_test_journal(db: &Path, bytes: &[u8]) {
        let path = db.join(JOURNAL_FILE);
        std::fs::write(&path, bytes).expect("write test manifest journal");
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open test manifest journal")
            .sync_all()
            .expect("sync test manifest journal");
    }

    #[test]
    fn should_replay_journal_when_valid_records_exist() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();
        let sst_name = crate::sst::file_name(0, 0, 1);

        let file = FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 1024,
            cf_id: 0,
            smallest_key: Some(vec![1, 2]),
            largest_key: Some(vec![9, 9]),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        };

        // Act
        let edit = ManifestEdit::AddSst(file.clone());
        append_edit(db, &edit).expect("append_edit failed");

        // Assert
        let edits = replay_journal(db).expect("replay_journal failed");
        assert_eq!(edits.len(), 1);
        match &edits[0] {
            ManifestEdit::AddSst(m) => {
                assert_eq!(m.name, sst_name);
                assert_eq!(m.size_bytes, 1024);
            }
            _ => panic!("unexpected edit variant"),
        }
    }

    #[test]
    fn should_assign_distinct_edit_ids_given_concurrent_manifest_appends() {
        // Arrange
        let td = tempdir().expect("create manifest directory");
        let open_gate = Arc::new(JournalOpenGate::default());
        let first_fs: Arc<dyn Fs> = Arc::new(GatedJournalFs {
            inner: crate::io::real::RealFs::new(td.path()).expect("open first filesystem handle"),
            open_gate: Arc::clone(&open_gate),
        });
        let second_fs: Arc<dyn Fs> = Arc::new(GatedJournalFs {
            inner: crate::io::real::RealFs::new(td.path()).expect("open second filesystem handle"),
            open_gate: Arc::clone(&open_gate),
        });
        let first = std::thread::spawn(move || {
            append_edit_with_fs(&first_fs, &ManifestEdit::BumpWalSeq { seq: 1 })
        });
        assert!(
            open_gate.wait_for_arrivals(1, std::time::Duration::from_secs(1)),
            "first writer must reach the journal open"
        );
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let second = std::thread::spawn(move || {
            started_tx.send(()).expect("signal second writer start");
            append_edit_with_fs(&second_fs, &ManifestEdit::BumpWalSeq { seq: 2 })
        });
        started_rx.recv().expect("observe second writer start");

        // Act
        let concurrent_open = open_gate.wait_for_arrivals(2, std::time::Duration::from_millis(500));
        open_gate.release();
        first
            .join()
            .expect("join first writer")
            .expect("append first manifest edit");
        second
            .join()
            .expect("join second writer")
            .expect("append second manifest edit");
        let replay_fs: Arc<dyn Fs> = Arc::new(
            crate::io::real::RealFs::open_existing(td.path()).expect("open replay filesystem"),
        );
        let replayed =
            replay_journal_with_ids_with_fs(&replay_fs).expect("replay concurrent appends");
        let edit_ids: Vec<_> = replayed.iter().map(|edit| edit.edit_id).collect();

        // Assert
        assert!(
            !concurrent_open,
            "a second manifest writer entered the journal append transaction"
        );
        assert_eq!(edit_ids, vec![1, 2]);
    }

    #[test]
    fn should_stop_replay_when_partial_tail() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        // create a valid edit to precede the partial record
        let file = FileMeta {
            name: "a.sst".to_string(),
            ..Default::default()
        };
        let edit = ManifestEdit::AddSst(file);
        append_edit(db, &edit).expect("append_edit failed");

        // Act: append a truncated record to simulate crash
        let path = db.join(JOURNAL_FILE);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("failed to open temp journal file");

        let fake = ManifestEdit::RemoveSst {
            name: "missing.sst".to_string(),
        };
        let payload = serde_json::to_vec(&fake).unwrap();
        let mut hasher = Crc32::new();
        hasher.update(&payload);
        let _crc = hasher.finalize();

        let mut buf = Vec::new();
        buf.push(fake.record_type());
        buf.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("payload length fits in u32")
                .to_le_bytes(),
        );
        buf.extend_from_slice(&payload[..10.min(payload.len())]); // partial
                                                                  // do NOT write crc

        f.write_all(&buf)
            .expect("failed to write partial journal record");
        f.sync_all().expect("failed to sync partial journal file");

        // Assert: replay should only return the first valid record
        let edits = replay_journal(db).expect("replay_journal failed");
        assert_eq!(edits.len(), 1);
    }

    #[test]
    fn should_fail_when_corrupt_record_precedes_valid_record() {
        // Arrange
        let td = tempdir().expect("create temp directory");
        let first = ManifestEdit::BumpWalSeq { seq: 7 };
        append_edit(td.path(), &first).expect("append durable first edit");

        let corrupted_middle = ManifestEdit::RemoveSst {
            name: "avoid.sst".to_string(),
        };
        let stale_crc_source = ManifestEdit::RemoveSst {
            name: "apply.sst".to_string(),
        };
        let valid_later = ManifestEdit::BumpWalSeq { seq: 9 };
        let mut suffix = encode_test_record(&corrupted_middle, &stale_crc_source);
        suffix.extend_from_slice(&encode_test_record(&valid_later, &valid_later));
        let mut journal = std::fs::OpenOptions::new()
            .append(true)
            .open(td.path().join(JOURNAL_FILE))
            .expect("open journal for corrupt suffix");
        journal
            .write_all(&suffix)
            .expect("append corrupt record followed by valid record");
        journal.sync_all().expect("sync corrupt test journal");

        // Act
        let error = replay_journal(td.path())
            .expect_err("mid-file corruption before a valid record must fail");

        // Assert
        assert!(matches!(error, crate::common::MidgeError::Corruption(_)));
    }

    #[test]
    fn should_fail_when_first_journal_record_crc_is_stale() {
        // Arrange
        let td = tempdir().expect("create temp directory");
        let corrupted_first = ManifestEdit::RemoveSst {
            name: "avoid.sst".to_string(),
        };
        let stale_crc_source = ManifestEdit::RemoveSst {
            name: "apply.sst".to_string(),
        };
        let journal = encode_test_record(&corrupted_first, &stale_crc_source);
        write_and_sync_test_journal(td.path(), &journal);

        // Act
        let error = replay_journal(td.path()).expect_err("first CRC mismatch must fail");

        // Assert
        assert!(matches!(error, crate::common::MidgeError::Corruption(_)));
    }

    #[test]
    fn should_truncate_partial_eof_tail_before_next_append() {
        // Arrange
        let td = tempdir().expect("create temp directory");
        let first = ManifestEdit::BumpWalSeq { seq: 1 };
        append_edit(td.path(), &first).expect("append first edit");

        let partial = ManifestEdit::BumpWalSeq { seq: 2 };
        let encoded = encode_test_record(&partial, &partial);
        let mut journal = std::fs::OpenOptions::new()
            .append(true)
            .open(td.path().join(JOURNAL_FILE))
            .expect("open journal for partial tail");
        journal
            .write_all(&encoded[..encoded.len() - 3])
            .expect("append partial EOF record");
        journal.sync_all().expect("sync partial EOF record");
        drop(journal);

        let after_repair = ManifestEdit::BumpWalSeq { seq: 3 };

        // Act
        append_edit(td.path(), &after_repair).expect("repair tail and append next edit");
        let edits = replay_journal(td.path()).expect("replay repaired journal");

        // Assert
        assert_eq!(edits.len(), 2);
        assert!(matches!(edits[0], ManifestEdit::BumpWalSeq { seq: 1 }));
        assert!(matches!(edits[1], ManifestEdit::BumpWalSeq { seq: 3 }));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_return_error_when_fsync_marker_write_fails() {
        // Arrange
        let td = tempdir().expect("create temp directory");
        let _test_guard = crate::failpoints::test_failpoint_guard();
        append_edit(td.path(), &ManifestEdit::BumpWalSeq { seq: 1 })
            .expect("append durable prefix");
        let marker_failure = fail::FailGuard::new(
            "midge::manifest::inject_fsync_marker_write_failure",
            "return",
        )
        .expect("configure marker-write failpoint");

        // Act
        let error = append_edit(td.path(), &ManifestEdit::BumpWalSeq { seq: 2 })
            .expect_err("marker write failure must be returned");
        drop(marker_failure);
        append_edit(td.path(), &ManifestEdit::BumpWalSeq { seq: 3 })
            .expect("repair incomplete append and write next edit");
        let replayed = replay_journal(td.path()).expect("replay repaired journal");

        // Assert
        assert!(error.to_string().contains("marker"));
        assert_eq!(replayed.len(), 2);
        assert!(matches!(replayed[0], ManifestEdit::BumpWalSeq { seq: 1 }));
        assert!(matches!(replayed[1], ManifestEdit::BumpWalSeq { seq: 3 }));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_return_error_when_required_manifest_sync_fails() {
        // Arrange
        let td = tempdir().expect("create temp directory");
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let _guard =
            fail::FailGuard::new("midge::manifest::inject_required_sync_failure", "return")
                .expect("configure sync failpoint");

        // Act
        let error = append_edit(td.path(), &ManifestEdit::BumpWalSeq { seq: 1 })
            .expect_err("required sync failure must be returned");

        // Assert
        assert!(error.to_string().contains("sync"));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_issue_one_required_sync_for_manifest_append() {
        // Arrange
        let td = tempdir().expect("create temp directory");
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let syncs = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_syncs = std::sync::Arc::clone(&syncs);
        let _guard =
            fail::FailGuard::with_callback("midge::manifest::before_required_sync", move || {
                OBSERVE_REQUIRED_MANIFEST_SYNC.with(|observe| {
                    if observe.get() {
                        observed_syncs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                });
            })
            .expect("configure sync observer");

        // Act
        OBSERVE_REQUIRED_MANIFEST_SYNC.with(|observe| observe.set(true));
        append_edit(td.path(), &ManifestEdit::BumpWalSeq { seq: 1 }).expect("append manifest edit");
        OBSERVE_REQUIRED_MANIFEST_SYNC.with(|observe| observe.set(false));

        // Assert
        assert_eq!(syncs.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn should_save_snapshot_to_disk_when_saved() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "one.sst".to_string(),
            level: 0,
            size_bytes: 100,
            ..Default::default()
        });

        // Act
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
            .expect("save snapshot failed");

        // Assert
        let snap = ManifestPersistence::manifest_snapshot_path(db);
        assert!(snap.exists(), "snapshot file should exist after saving");
    }

    #[test]
    fn should_truncate_journal_after_snapshot_saved() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "one.sst".to_string(),
            level: 0,
            size_bytes: 100,
            ..Default::default()
        });

        // Act
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
            .expect("save snapshot failed");

        // Assert: journal should be truncated (zero-length) if it exists
        let journal = db.join(JOURNAL_FILE);
        if journal.exists() {
            let meta = std::fs::metadata(&journal).unwrap();
            assert_eq!(
                meta.len(),
                0,
                "journal should be zero-length after snapshot/truncate"
            );
        }
    }

    #[test]
    fn should_load_snapshot_when_snapshot_exists() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        // Create snapshot
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "base.sst".to_string(),
            level: 0,
            size_bytes: 10,
            ..Default::default()
        });
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
            .expect("save snapshot failed");

        // Act
        let loaded = ManifestPersistence::load(db).expect("load failed");

        // Assert
        assert!(
            loaded.files.iter().any(|f| f.name == "base.sst"),
            "loaded manifest should include snapshot entries"
        );
    }

    #[test]
    fn should_replay_journal_on_top_of_snapshot_when_present() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        // Create snapshot
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "base.sst".to_string(),
            level: 0,
            size_bytes: 10,
            ..Default::default()
        });
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
            .expect("save snapshot failed");

        // Act: append an edit that should be replayed on load
        let edit = ManifestEdit::AddSst(FileMeta {
            name: "new.sst".to_string(),
            level: 1,
            size_bytes: 20,
            ..Default::default()
        });
        append_edit(db, &edit).expect("append_edit failed");

        // Assert
        let loaded = ManifestPersistence::load(db).expect("load failed");
        assert!(
            loaded.files.iter().any(|f| f.name == "base.sst"),
            "snapshot entry missing"
        );
        assert!(
            loaded.files.iter().any(|f| f.name == "new.sst"),
            "journal replay entry missing"
        );
    }

    #[test]
    fn should_replay_batch_record() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        let e1 = ManifestEdit::AddSst(FileMeta {
            name: "b1.sst".to_string(),
            level: 0,
            size_bytes: 10,
            ..Default::default()
        });
        let e2 = ManifestEdit::RemoveSst {
            name: "missing.sst".to_string(),
        };

        // Act
        append_edit_batch(db, &[e1.clone(), e2.clone()]).expect("append batch failed");

        // Assert: replay should yield a single Batch record
        let edits = replay_journal(db).expect("replay_journal failed");
        assert_eq!(edits.len(), 1);
        match &edits[0] {
            ManifestEdit::Batch(b) => {
                assert_eq!(b.len(), 2);
            }
            _ => panic!("expected batch record"),
        }
    }

    #[test]
    fn should_emit_fsync_marker_for_batch_append() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        // Act
        let e = ManifestEdit::BumpWalSeq { seq: 1 };
        append_edit_batch(db, std::slice::from_ref(&e)).expect("append batch failed");

        // Assert: journal file contains an FSYNC_MARKER_TYPE record
        let data = std::fs::read(db.join(JOURNAL_FILE)).expect("read journal");
        assert!(data.contains(&FSYNC_MARKER_TYPE));
    }

    proptest! {
        #[test]
        fn should_preserve_batch_order_when_replaying_bump_wal_seq_edits(
            seqs in proptest::collection::vec(0u64..10_000, 1..32)
        ) {
            // Arrange
            let td = tempdir().unwrap();
            let db = td.path();
            let batch: Vec<ManifestEdit> = seqs
                .iter()
                .copied()
                .map(|seq| ManifestEdit::BumpWalSeq { seq })
                .collect();

            // Act
            append_edit_batch(db, &batch).expect("append batch failed");

            let replayed = replay_journal(db).expect("replay_journal failed");
            prop_assert_eq!(replayed.len(), 1);

            // Assert
            match &replayed[0] {
                ManifestEdit::Batch(inner) => {
                    prop_assert_eq!(inner.len(), batch.len());
                    for (observed, expected) in inner.iter().zip(batch.iter()) {
                        match (observed, expected) {
                            (
                                ManifestEdit::BumpWalSeq { seq: observed_seq },
                                ManifestEdit::BumpWalSeq { seq: expected_seq },
                            ) => prop_assert_eq!(observed_seq, expected_seq),
                            other => prop_assert!(false, "unexpected replayed edit pair: {:?}", other),
                        }
                    }
                }
                other => prop_assert!(false, "expected batch replay, got {:?}", other),
            }

            let mut direct_manifest = Manifest::default();
            direct_manifest.apply_edit(&ManifestEdit::Batch(batch.clone()));
            let mut replayed_manifest = Manifest::default();
            replayed_manifest.apply_edit(&replayed[0]);

            prop_assert_eq!(
                replayed_manifest.last_persisted_sequence,
                direct_manifest.last_persisted_sequence
            );
        }
    }
}
