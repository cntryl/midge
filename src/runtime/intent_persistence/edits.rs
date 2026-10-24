//! Append-only intent suffix edits with a checksummed predecessor chain.

use super::{IntentLogEntry, MidgeError, MidgeResult};
use crate::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions};
use crate::metadata::codec;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const COMPACT_BYTES: usize = 1024 * 1024;
const COMPACT_RECORDS: u64 = 1024;

#[derive(Serialize, Deserialize)]
struct IntentEdit {
    sequence: u64,
    previous_checksum: u32,
    keep: usize,
    append: Vec<IntentLogEntry>,
}

#[derive(Default)]
struct IntentState {
    intents: Vec<IntentLogEntry>,
    sequence: u64,
    checksum: u32,
    valid_bytes: usize,
    bytes: bytes::Bytes,
}

pub(super) fn load(fs: &Arc<dyn Fs>) -> MidgeResult<Vec<IntentLogEntry>> {
    read_state(fs).map(|state| state.intents)
}

fn read_state(fs: &Arc<dyn Fs>) -> MidgeResult<IntentState> {
    let path = FsPath::new(super::IntentPersistence::INTENT_FILE);
    let file = match fs.open(&path, OpenOptions::READ_ONLY) {
        Ok(file) => file,
        Err(FsError::NotFound(_)) => return Ok(IntentState::default()),
        Err(error) => return Err(super::recovery_io_error(error, "open intent edits")),
    };
    let length = file.len().map_err(FsError::into_midge)?;
    let bytes = file.read_at(0, length).map_err(FsError::into_midge)?;
    let mut state = IntentState {
        bytes,
        ..IntentState::default()
    };
    while let Some(frame) = codec::frame_len(&state.bytes[state.valid_bytes..])? {
        let end = state.valid_bytes + frame;
        let encoded = &state.bytes[state.valid_bytes..end];
        let edit: IntentEdit = codec::decode(codec::INTENT, encoded)?;
        if state.sequence.checked_add(1) != Some(edit.sequence)
            || edit.previous_checksum != state.checksum
            || edit.keep > state.intents.len()
        {
            return Err(MidgeError::Corruption(
                "intent predecessor or prefix mismatch".into(),
            ));
        }
        state.intents.truncate(edit.keep);
        state.intents.extend(edit.append);
        state.sequence = edit.sequence;
        state.checksum = frame_checksum(encoded);
        state.valid_bytes = end;
    }
    Ok(state)
}

fn frame_checksum(bytes: &[u8]) -> u32 {
    let mut checksum = [0_u8; 4];
    checksum.copy_from_slice(&bytes[bytes.len() - 4..]);
    u32::from_be_bytes(checksum)
}

fn encode_edit(state: &IntentState, intents: &[IntentLogEntry]) -> MidgeResult<Vec<u8>> {
    let mut keep = 0;
    for (old, new) in state.intents.iter().zip(intents) {
        if codec::encode(codec::INTENT, old)? != codec::encode(codec::INTENT, new)? {
            break;
        }
        keep += 1;
    }
    let sequence = state
        .sequence
        .checked_add(1)
        .ok_or_else(|| MidgeError::ResourceLimit("intent edit identity exhausted".into()))?;
    codec::encode(
        codec::INTENT,
        &IntentEdit {
            sequence,
            previous_checksum: state.checksum,
            keep,
            append: intents[keep..].to_vec(),
        },
    )
}

pub(super) fn save(fs: &Arc<dyn Fs>, intents: &[IntentLogEntry]) -> MidgeResult<()> {
    crate::metadata::journal::with_manifest_writer_lock(fs, || save_locked(fs, intents))
}

fn save_locked(fs: &Arc<dyn Fs>, intents: &[IntentLogEntry]) -> MidgeResult<()> {
    crate::failpoints::fail_point!("midge::intent::before_save");
    crate::failpoints::fail_point!("midge::intent::inject_no_space_on_save", |_| Err(
        MidgeError::NoSpace("failpoint: no space while saving intent log".into())
    ));
    let mut state = read_state(fs)?;
    if state.valid_bytes < state.bytes.len() {
        replace(fs, &state.bytes[..state.valid_bytes])?;
    }
    if state.valid_bytes >= COMPACT_BYTES || state.sequence >= COMPACT_RECORDS {
        let bytes = encode_edit(&IntentState::default(), &state.intents)?;
        replace(fs, &bytes)?;
        state.sequence = 1;
        state.checksum = frame_checksum(&bytes);
    }
    let encoded = encode_edit(&state, intents)?;
    let path = FsPath::new(super::IntentPersistence::INTENT_FILE);
    let creating = !fs.exists(&path).map_err(FsError::into_midge)?;
    let mut file = fs
        .open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    file.append(bytes::Bytes::from(encoded))
        .map_err(FsError::into_midge)?;
    file.sync(Durability::Durable)
        .map_err(FsError::into_midge)?;
    if creating {
        fs.sync_dir(&FsPath::new("."), Durability::Durable)
            .map_err(FsError::into_midge)?;
    }
    Ok(())
}

fn replace(fs: &Arc<dyn Fs>, bytes: &[u8]) -> MidgeResult<()> {
    crate::io::staging::stage_bytes_typed(
        fs,
        &FsPath::new(super::IntentPersistence::INTENT_FILE_TEMP),
        &FsPath::new(super::IntentPersistence::INTENT_FILE),
        bytes,
        || Ok(()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(sequence: u64) -> IntentLogEntry {
        IntentLogEntry::SeqnoAllocated {
            seqno: sequence,
            cf_id: 0,
        }
    }

    #[test]
    fn should_append_intent_edits_when_clearing_and_recording_publication_state() -> MidgeResult<()>
    {
        // Arrange
        let directory = tempfile::tempdir()?;
        let fs: Arc<dyn Fs> =
            Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?);
        save(&fs, &[entry(1), entry(2)])?;
        let path = directory
            .path()
            .join(super::super::IntentPersistence::INTENT_FILE);
        let before = std::fs::read(&path)?;

        // Act
        save(&fs, &[entry(1), entry(3)])?;
        save(&fs, &[entry(3)])?;
        let after = std::fs::read(&path)?;
        let recovered = load(&fs)?;

        // Assert: the old durable records remain, while replay returns exactly
        // the current intent set rather than resurrecting cleared entries.
        assert!(after.starts_with(&before));
        assert_eq!(recovered.len(), 1);
        assert!(matches!(
            recovered[0],
            IntentLogEntry::SeqnoAllocated { seqno: 3, cf_id: 0 }
        ));
        Ok(())
    }

    #[test]
    fn should_retain_prior_intent_and_repair_tail_when_final_append_is_torn() -> MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let fs: Arc<dyn Fs> =
            Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?);
        save(&fs, &[entry(1)])?;
        let path = directory
            .path()
            .join(super::super::IntentPersistence::INTENT_FILE);
        let prefix = std::fs::read(&path)?;
        save(&fs, &[entry(1), entry(2)])?;
        let full = std::fs::read(&path)?;
        let added = full.len() - prefix.len();

        // Act / Assert
        for extent in [1, 8, 16, added - 1] {
            std::fs::write(&path, &full[..prefix.len() + extent])?;
            let recovered = load(&fs)?;
            assert_eq!(recovered.len(), 1);
            assert!(matches!(
                recovered[0],
                IntentLogEntry::SeqnoAllocated { seqno: 1, cf_id: 0 }
            ));
            save(&fs, &[entry(1), entry(3)])?;
            let repaired = load(&fs)?;
            assert_eq!(repaired.len(), 2);
            assert!(matches!(
                repaired[1],
                IntentLogEntry::SeqnoAllocated { seqno: 3, cf_id: 0 }
            ));
        }
        Ok(())
    }
}
