use super::{
    journal_deserialize, FsError, FsyncMarker, JournalEdit, JournalEditEnvelope, ManifestEdit,
    MidgeResult, ADVANCE_FLUSH_FRONTIER_RECORD_TYPE, BATCH_RECORD_TYPE,
    DROP_COLUMN_FAMILY_AT_RECORD_TYPE, FSYNC_MARKER_TYPE, JOURNAL_FILE,
    RECLAIM_COLUMN_FAMILY_RECORD_TYPE,
};

pub(super) fn replay_journal_with_state(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<JournalReplay> {
    replay_journal_with_mode(fs, JournalReplayMode::Strict, None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JournalReplayMode {
    /// Any corrupt record fails replay.
    Strict,
    /// Stop at the first corrupt record and keep the durable prefix.
    SalvagePrefix,
}

pub(super) fn replay_journal_with_mode(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    mode: JournalReplayMode,
    scope: Option<&crate::common::DeadlineScope>,
) -> MidgeResult<JournalReplay> {
    check_replay_scope(scope)?;
    let Some(file) = open_journal_for_replay(fs)? else {
        return Ok(JournalReplay::empty());
    };

    replay_journal_file(&*file, mode, scope)
}

fn replay_journal_file(
    file: &dyn crate::io::ReadFile,
    mode: JournalReplayMode,
    scope: Option<&crate::common::DeadlineScope>,
) -> MidgeResult<JournalReplay> {
    let file_len = file.len().map_err(FsError::into_midge)?;
    let mut state = JournalReplayState::default();

    let mut offset: u64 = 0;
    let mut tail = JournalReplayTail::Clean;
    let mut corruption = None;
    let mut progress = crate::telemetry::recovery_progress::WorkProgress::new("manifest_journal");

    while offset < file_len {
        check_replay_scope(scope)?;
        let status = match read_journal_record(file, offset, file_len) {
            Ok(status) => status,
            // A read error is not evidence of corruption: salvaging past it
            // would drop durable edits whose bytes are fine.
            Err(error) if mode == JournalReplayMode::SalvagePrefix && error.is_salvageable() => {
                corruption = Some(format!("at byte {offset}: {error}"));
                break;
            }
            Err(error) => return Err(error),
        };
        check_replay_scope(scope)?;
        match status {
            JournalRecordStatus::Record(record) => {
                offset = record.next_offset;
                let applied = validate_journal_record_crc(&record)
                    .and_then(|()| handle_journal_record(&record, &mut state));
                check_replay_scope(scope)?;
                match applied {
                    Ok(()) => {
                        progress.completed(record.next_offset - record.record_start, 1);
                    }
                    Err(error @ crate::common::MidgeError::Timeout(_)) => return Err(error),
                    Err(error) if mode == JournalReplayMode::SalvagePrefix => {
                        corruption = Some(format!("at byte {}: {error}", record.record_start));
                        break;
                    }
                    Err(error) => return Err(error),
                }
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

    let durable_edit_count = state.last_marker_edit_idx.unwrap_or(0);
    if matches!(tail, JournalReplayTail::Clean) && state.edits.len() > durable_edit_count {
        tail = JournalReplayTail::PartialEof {
            last_valid_offset: state.last_marker_offset.unwrap_or(0),
            record_start: state.last_marker_offset.unwrap_or(0),
            reason: "edit record at EOF is missing its durability marker",
        };
    }

    check_replay_scope(scope)?;
    state.edits.truncate(durable_edit_count);
    let mut max_edit_id = 0;
    for edit in &state.edits {
        check_replay_scope(scope)?;
        // Semantic invalidity is not a salvageable byte-tail error. Validate
        // only durable edits, outside the prefix decoder's salvage boundary.
        edit.edit.validate_for_append()?;
        max_edit_id = max_edit_id.max(edit.edit_id);
    }
    check_replay_scope(scope)?;

    progress.finish();
    Ok(JournalReplay {
        edits: state.edits,
        max_edit_id,
        file_len,
        tail,
        corruption,
    })
}

/// Decode immutable journal bytes with the same framing and durability rules.
pub(crate) fn replay_identified_edits_from_bytes(
    bytes: &[u8],
    checkpoint: u64,
    scope: &crate::common::DeadlineScope,
) -> MidgeResult<Vec<(u64, ManifestEdit)>> {
    struct SliceFile<'a>(&'a [u8]);
    impl crate::io::ReadFile for SliceFile<'_> {
        fn read_at(&self, offset: u64, len: u64) -> crate::io::FsResult<bytes::Bytes> {
            let start = usize::try_from(offset)
                .map_err(|_| FsError::Corruption("journal offset overflow".into()))?;
            let length = usize::try_from(len)
                .map_err(|_| FsError::Corruption("journal length overflow".into()))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| FsError::Corruption("journal extent overflow".into()))?;
            let data = self.0.get(start..end).ok_or_else(|| {
                FsError::Corruption("journal extent exceeds immutable body".into())
            })?;
            Ok(bytes::Bytes::copy_from_slice(data))
        }
        fn len(&self) -> crate::io::FsResult<u64> {
            Ok(self.0.len() as u64)
        }
    }
    let replay = replay_journal_file(&SliceFile(bytes), JournalReplayMode::Strict, Some(scope))?;
    Ok(replay
        .edits
        .into_iter()
        .filter(|edit| edit.edit_id > checkpoint)
        .map(|edit| (edit.edit_id, edit.edit))
        .collect())
}

pub(super) fn check_replay_scope(scope: Option<&crate::common::DeadlineScope>) -> MidgeResult<()> {
    scope.map_or(Ok(()), |scope| scope.check("manifest journal replay"))
}

pub(super) struct JournalReplay {
    pub(super) edits: Vec<JournalEdit>,
    pub(super) max_edit_id: u64,
    pub(super) file_len: u64,
    pub(super) tail: JournalReplayTail,
    /// First corrupt record, when replay ran in salvage mode and stopped.
    pub(super) corruption: Option<String>,
}

impl JournalReplay {
    fn empty() -> Self {
        Self {
            edits: Vec::new(),
            max_edit_id: 0,
            file_len: 0,
            tail: JournalReplayTail::Clean,
            corruption: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum JournalReplayTail {
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
}

pub(super) struct JournalRecord {
    pub(super) record_start: u64,
    pub(super) typ: u8,
    pub(super) payload: Vec<u8>,
    pub(super) got_crc: u32,
    pub(super) next_offset: u64,
}

pub(super) enum JournalRecordStatus {
    Record(JournalRecord),
    PartialHeader { record_start: u64 },
    PartialPayload { record_start: u64 },
}

pub(super) fn open_journal_for_replay(
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
        Err(e) => Err(FsError::into_midge(e)),
    }
}

pub(super) fn read_journal_record(
    file: &dyn crate::io::ReadFile,
    offset: u64,
    file_len: u64,
) -> MidgeResult<JournalRecordStatus> {
    let record_start = offset;
    let typ = file.read_at(offset, 1).map_err(FsError::into_midge)?[0];
    if typ == 0 {
        // An unsynced append can persist the new file size but not its bytes,
        // leaving a zero-filled end. That is a torn tail (the WAL treats it
        // the same way); zeros followed by data are still corruption.
        let rest = file
            .read_at(offset, file_len - offset)
            .map_err(FsError::into_midge)?;
        if rest.iter().all(|byte| *byte == 0) {
            return Ok(JournalRecordStatus::PartialHeader { record_start });
        }
    }
    validate_journal_record_type(typ, record_start)?;
    if offset + 5 > file_len {
        return Ok(JournalRecordStatus::PartialHeader { record_start });
    }

    let len_offset = offset + 1;
    let len_bytes = file.read_at(len_offset, 4).map_err(FsError::into_midge)?;
    let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
    let payload_offset = len_offset + 4;

    if payload_offset + (len as u64) + 4 > file_len {
        // A torn final append leaves only garbage after its header. If the
        // bytes this length would swallow hold a verified fsync marker, the
        // length itself is corrupt and truncating here would erase durable
        // edits (the same shape the WAL rejects after #153).
        let rest = file
            .read_at(payload_offset, file_len - payload_offset)
            .map_err(FsError::into_midge)?;
        if contains_verified_fsync_marker(&rest) {
            return Err(crate::common::MidgeError::Corruption(format!(
                "manifest journal record length at byte {record_start} overruns EOF and hides a verified later record (len={len}, file_len={file_len})"
            )));
        }
        return Ok(JournalRecordStatus::PartialPayload { record_start });
    }

    let payload = file
        .read_at(payload_offset, len as u64)
        .map_err(FsError::into_midge)?;
    let crc_offset = payload_offset + len as u64;
    let crc_bytes = file.read_at(crc_offset, 4).map_err(FsError::into_midge)?;
    let got_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);

    Ok(JournalRecordStatus::Record(JournalRecord {
        record_start,
        typ,
        payload: payload.to_vec(),
        got_crc,
        next_offset: crc_offset + 4,
    }))
}

/// Whether `bytes` contain a complete, CRC-valid fsync marker record. Every
/// durable append ends with one, so finding one proves durable data follows.
fn contains_verified_fsync_marker(bytes: &[u8]) -> bool {
    const HEADER_LEN: usize = 5;
    const CRC_LEN: usize = 4;
    let mut start = 0;
    while let Some(found) = bytes[start..]
        .iter()
        .position(|byte| *byte == FSYNC_MARKER_TYPE)
    {
        let record_start = start + found;
        start = record_start + 1;
        let Some(len_bytes) = bytes.get(record_start + 1..record_start + HEADER_LEN) else {
            return false;
        };
        let len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        let payload_start = record_start + HEADER_LEN;
        let Some(crc_start) = payload_start.checked_add(len) else {
            continue;
        };
        let Some(crc_bytes) = crc_start
            .checked_add(CRC_LEN)
            .and_then(|crc_end| bytes.get(crc_start..crc_end))
        else {
            continue;
        };
        let payload = &bytes[payload_start..crc_start];
        let expected = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        if crc32c::crc32c(payload) == expected
            && journal_deserialize::<FsyncMarker>(payload).is_ok()
        {
            return true;
        }
    }
    false
}

fn validate_journal_record_type(record_type: u8, record_start: u64) -> MidgeResult<()> {
    if matches!(record_type, 1 | 2 | 3 | 6 | 8 | 9 | 10 | 11 | 12) {
        return Ok(());
    }

    Err(crate::common::MidgeError::Corruption(format!(
        "manifest journal has unknown record type {record_type} at byte {record_start}"
    )))
}

fn validate_journal_record_crc(record: &JournalRecord) -> MidgeResult<()> {
    let calc = crc32c::crc32c(&record.payload);
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
    let envelope = decode_edit_envelope(record)?;
    if !matches!(envelope.edit, ManifestEdit::Batch(_)) {
        return Err(journal_record_type_mismatch(record, &envelope.edit));
    }
    state.edits.push(JournalEdit {
        edit_id: envelope.edit_id,
        edit: envelope.edit,
    });
    Ok(())
}

fn handle_manifest_edit_record(
    record: &JournalRecord,
    state: &mut JournalReplayState,
) -> MidgeResult<()> {
    let is_single_edit_record = matches!(record.typ, 1 | 2 | 3 | 6)
        || matches!(
            record.typ,
            DROP_COLUMN_FAMILY_AT_RECORD_TYPE
                | RECLAIM_COLUMN_FAMILY_RECORD_TYPE
                | ADVANCE_FLUSH_FRONTIER_RECORD_TYPE
        );
    if !is_single_edit_record {
        return Err(crate::common::MidgeError::Corruption(format!(
            "manifest journal has unknown record type {} at byte {}",
            record.typ, record.record_start
        )));
    }

    let envelope = decode_edit_envelope(record)?;
    if envelope.edit.record_type() != record.typ {
        return Err(journal_record_type_mismatch(record, &envelope.edit));
    }
    state.edits.push(JournalEdit {
        edit_id: envelope.edit_id,
        edit: envelope.edit,
    });
    Ok(())
}

/// Decode an edit record's payload, which must carry the edit envelope.
fn decode_edit_envelope(record: &JournalRecord) -> MidgeResult<JournalEditEnvelope> {
    journal_deserialize::<JournalEditEnvelope>(&record.payload).map_err(|error| {
        crate::common::MidgeError::Corruption(format!(
            "manifest journal record at byte {} is not an edit envelope: {error}",
            record.record_start
        ))
    })
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
