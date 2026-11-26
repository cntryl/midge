//! Append-only intent suffix edits with a checksummed predecessor chain.

use super::{IntentUpgradeState, MidgeError, MidgeResult};
use crate::metadata::codec;
use crate::metadata::intent::IntentLogEntry;
use cntryl_midge_core::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const COMPACT_BYTES: usize = 1024 * 1024;
const COMPACT_RECORDS: u64 = 1024;
mod workspace;
pub use workspace::IntentDecodeWorkspace;

pub(super) fn decoded_workspace_bound(
    bytes: &[u8],
    deadline: &cntryl_midge_core::common::OperationDeadline,
) -> MidgeResult<IntentDecodeWorkspace> {
    workspace::decoded_workspace_bound(bytes, deadline)
}

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

pub(super) fn decode_for_upgrade(
    bytes: bytes::Bytes,
    scope: &cntryl_midge_core::common::DeadlineScope,
) -> MidgeResult<IntentUpgradeState> {
    let state = replay(bytes, Some(scope))?;
    if state.valid_bytes != state.bytes.len() {
        return Err(MidgeError::Corruption(format!(
            "FORMAT 9 upgrade requires a complete intent log; unverified bytes after byte {}",
            state.valid_bytes
        )));
    }
    Ok(IntentUpgradeState {
        entries: state.intents,
        sequence: state.sequence,
        checksum: state.checksum,
    })
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
    replay(bytes, None)
}

fn replay(
    bytes: bytes::Bytes,
    scope: Option<&cntryl_midge_core::common::DeadlineScope>,
) -> MidgeResult<IntentState> {
    let mut state = IntentState {
        bytes,
        ..IntentState::default()
    };
    while let Some(frame) = codec::tail_frame_len(&state.bytes[state.valid_bytes..], codec::INTENT)?
    {
        if let Some(scope) = scope {
            scope.check("intent edit replay")?;
        }
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
    if let Some(scope) = scope {
        scope.check("intent edit replay")?;
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
    cntryl_midge_core::failpoints::fail_point!("midge::intent::before_save");
    cntryl_midge_core::failpoints::fail_point!("midge::intent::inject_no_space_on_save", |_| Err(
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
    cntryl_midge_core::io::staging::stage_bytes_typed(
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
        let fs: Arc<dyn Fs> = Arc::new(
            cntryl_midge_core::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?,
        );
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
    fn should_recover_the_durable_intent_prefix_when_final_append_is_torn() -> MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let fs: Arc<dyn Fs> = Arc::new(
            cntryl_midge_core::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?,
        );
        save(&fs, &[entry(1)])?;
        let path = directory
            .path()
            .join(super::super::IntentPersistence::INTENT_FILE);
        let prefix = std::fs::read(&path)?;
        save(&fs, &[entry(1), entry(2)])?;
        let full = std::fs::read(&path)?;
        let added = full.len() - prefix.len();

        // Act: replay each interrupted append from the actual on-disk prefix.
        for extent in [1, 8, 16, added - 1] {
            std::fs::write(&path, &full[..prefix.len() + extent])?;
            let recovered = load(&fs)?;
            // Assert: an incomplete append cannot replace its durable prefix.
            assert_eq!(recovered.len(), 1);
            assert!(matches!(
                recovered[0],
                IntentLogEntry::SeqnoAllocated { seqno: 1, cf_id: 0 }
            ));
            // Act: persist a new edit after loading the torn final append.
            save(&fs, &[entry(1), entry(3)])?;
            let repaired = load(&fs)?;
            // Assert: the repaired durable prefix accepts the next intent.
            assert_eq!(repaired.len(), 2);
            assert!(matches!(
                repaired[1],
                IntentLogEntry::SeqnoAllocated { seqno: 3, cf_id: 0 }
            ));
        }
        Ok(())
    }

    struct Log {
        _directory: tempfile::TempDir,
        fs: Arc<dyn Fs>,
        path: std::path::PathBuf,
        /// Start offset of each frame, plus the final file length.
        starts: Vec<usize>,
    }

    fn strict_load(fs: &Arc<dyn Fs>) -> MidgeResult<Vec<IntentLogEntry>> {
        super::super::IntentPersistence::load_with_fs_and_policy_typed(
            fs,
            crate::RecoveryPolicy::Strict,
        )
    }

    fn log_with_edits(count: u64) -> MidgeResult<Log> {
        let directory = tempfile::tempdir()?;
        let fs: Arc<dyn Fs> = Arc::new(
            cntryl_midge_core::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?,
        );
        let path = directory
            .path()
            .join(super::super::IntentPersistence::INTENT_FILE);
        let mut starts = vec![0];
        let mut intents = Vec::new();
        for sequence in 1..=count {
            intents.push(entry(sequence));
            save(&fs, &intents)?;
            starts.push(usize::try_from(std::fs::metadata(&path)?.len()).unwrap());
        }
        Ok(Log {
            _directory: directory,
            fs,
            path,
            starts,
        })
    }

    fn assert_corruption(result: &MidgeResult<Vec<IntentLogEntry>>) {
        assert!(
            matches!(result, Err(MidgeError::Corruption(_))),
            "{result:?}"
        );
    }

    #[test]
    fn should_fail_when_mid_file_frame_length_overruns_and_hides_later_frames() -> MidgeResult<()> {
        // Arrange: inflate the second frame's length past EOF but under the cap.
        let log = log_with_edits(3)?;
        let mut bytes = std::fs::read(&log.path)?;
        bytes[log.starts[1] + 9..log.starts[1] + 13].copy_from_slice(&1_000_000_u32.to_le_bytes());
        std::fs::write(&log.path, &bytes)?;

        // Act
        let loaded = strict_load(&log.fs);
        let saved = save(&log.fs, &[entry(1), entry(9)]);

        // Assert: durable history follows, so neither load nor save may
        // treat this as a torn tail or rewrite the file to its prefix.
        assert_corruption(&loaded);
        assert!(saved.is_err());
        assert_eq!(std::fs::read(&log.path)?, bytes);
        Ok(())
    }

    #[test]
    fn should_load_durable_prefix_when_tail_is_zero_filled() -> MidgeResult<()> {
        // Arrange: an unsynced append that persisted its length but not bytes.
        for zeros in [1, 8, 12, 13, 64] {
            let log = log_with_edits(1)?;
            let mut bytes = std::fs::read(&log.path)?;
            bytes.extend(std::iter::repeat_n(0, zeros));
            std::fs::write(&log.path, &bytes)?;

            // Act
            let loaded = strict_load(&log.fs)?;
            save(&log.fs, &[entry(1), entry(2)])?;
            let repaired = strict_load(&log.fs)?;

            // Assert
            assert_eq!(loaded.len(), 1, "zeros={zeros}");
            assert_eq!(repaired.len(), 2, "zeros={zeros}");
            assert_eq!(
                std::fs::read(&log.path)?[..log.starts[1]],
                bytes[..log.starts[1]]
            );
        }
        Ok(())
    }

    #[test]
    fn should_fail_when_zero_tail_is_followed_by_data() -> MidgeResult<()> {
        // Arrange
        let log = log_with_edits(1)?;
        let mut bytes = std::fs::read(&log.path)?;
        bytes.extend([0; 16]);
        bytes.push(7);
        std::fs::write(&log.path, &bytes)?;

        // Act
        let loaded = strict_load(&log.fs);
        let saved = save(&log.fs, &[entry(1), entry(2)]);

        // Assert: still an error, and the file is untouched.
        assert!(
            matches!(loaded, Err(MidgeError::CompatibilityError(_))),
            "{loaded:?}"
        );
        assert!(saved.is_err());
        assert_eq!(std::fs::read(&log.path)?, bytes);
        Ok(())
    }

    #[test]
    fn should_keep_compatibility_error_when_magic_at_offset_zero_is_wrong() -> MidgeResult<()> {
        // Arrange
        let log = log_with_edits(2)?;
        let mut bytes = std::fs::read(&log.path)?;
        bytes[0] ^= 1;
        std::fs::write(&log.path, &bytes)?;

        // Act
        let loaded = strict_load(&log.fs);

        // Assert
        assert!(
            matches!(loaded, Err(MidgeError::CompatibilityError(_))),
            "{loaded:?}"
        );
        Ok(())
    }

    #[test]
    fn should_load_prefix_when_torn_final_frame_has_inflated_length_and_nothing_follows(
    ) -> MidgeResult<()> {
        // Arrange: a torn final append whose header is intact and whose body is
        // partial; nothing valid follows it.
        let log = log_with_edits(2)?;
        let bytes = std::fs::read(&log.path)?;
        let mut torn = bytes[..log.starts[1] + 20].to_vec();
        torn[log.starts[1] + 9..log.starts[1] + 13].copy_from_slice(&5_000_u32.to_le_bytes());
        std::fs::write(&log.path, &torn)?;

        // Act
        let loaded = strict_load(&log.fs)?;
        save(&log.fs, &[entry(1), entry(5)])?;

        // Assert
        assert_eq!(loaded.len(), 1);
        assert_eq!(strict_load(&log.fs)?.len(), 2);
        Ok(())
    }

    #[test]
    fn should_fail_when_final_durable_frame_length_is_inflated() -> MidgeResult<()> {
        // Arrange: only the length field of the final frame is damaged, so the
        // frame still runs to EOF with a valid CRC once its length is corrected.
        let log = log_with_edits(3)?;
        let mut bytes = std::fs::read(&log.path)?;
        bytes[log.starts[2] + 9..log.starts[2] + 13].copy_from_slice(&1_000_000_u32.to_le_bytes());
        std::fs::write(&log.path, &bytes)?;

        // Act
        let loaded = strict_load(&log.fs);
        let saved = save(&log.fs, &[entry(1), entry(9)]);

        // Assert: an acknowledged frame is reported, never dropped or rewritten.
        assert_corruption(&loaded);
        assert!(saved.is_err());
        assert_eq!(std::fs::read(&log.path)?, bytes);
        Ok(())
    }

    #[test]
    fn should_decode_complete_intent_log_with_committed_identity_for_upgrade() -> MidgeResult<()> {
        // Arrange
        let log = log_with_edits(2)?;
        let bytes = std::fs::read(&log.path)?;
        let checksum = frame_checksum(&bytes[log.starts[1]..]);
        let scope = cntryl_midge_core::common::DeadlineScope::new(
            cntryl_midge_core::common::OperationDeadline::from_budget(
                std::time::Duration::from_secs(5),
            ),
        );

        // Act
        let state = super::super::IntentPersistence::decode_for_upgrade(bytes.into(), &scope)?;

        // Assert
        assert_eq!(state.entries.len(), 2);
        assert!(matches!(
            state.entries[1],
            IntentLogEntry::SeqnoAllocated { seqno: 2, cf_id: 0 }
        ));
        assert_eq!(state.sequence, 2);
        assert_eq!(state.checksum, checksum);
        Ok(())
    }

    #[test]
    fn should_reject_upgrade_when_final_intent_length_overruns_and_crc_is_corrupt(
    ) -> MidgeResult<()> {
        // Arrange: damage two framing fields in an otherwise complete edit.
        let log = log_with_edits(2)?;
        let mut bytes = std::fs::read(&log.path)?;
        let tail = log.starts[1];
        let length = tail + 9;
        let payload_len = u32::from_le_bytes(bytes[length..length + 4].try_into().unwrap());
        bytes[length..length + 4].copy_from_slice(&(payload_len + 1).to_le_bytes());
        *bytes.last_mut().expect("intent checksum byte") ^= 1;
        std::fs::write(&log.path, &bytes)?;
        let scope = cntryl_midge_core::common::DeadlineScope::new(
            cntryl_midge_core::common::OperationDeadline::from_budget(
                std::time::Duration::from_secs(5),
            ),
        );

        // Act
        let legacy_recovery = strict_load(&log.fs);
        let upgrade = super::super::IntentPersistence::decode_for_upgrade(
            bytes::Bytes::copy_from_slice(&bytes),
            &scope,
        );

        // Assert: ordinary recovery accepts the prefix, but migration declines
        // to certify it while leaving the legacy input intact.
        assert_eq!(legacy_recovery?.len(), 1);
        assert!(matches!(
            upgrade,
            Err(MidgeError::Corruption(message)) if message.contains("unverified bytes")
        ));
        assert_eq!(std::fs::read(&log.path)?, bytes);
        Ok(())
    }

    #[test]
    fn should_load_prefix_when_torn_final_frame_is_zero_extended() -> MidgeResult<()> {
        // Arrange: a strict prefix of the final frame, extended with zeros to
        // the persisted file length.
        let log = log_with_edits(2)?;
        let bytes = std::fs::read(&log.path)?;
        let added = log.starts[2] - log.starts[1];
        // Zero extension stays short of the frame's full extent; one that
        // reaches it is a complete frame with a bad CRC and is reported.
        for (kept, zeros) in [(13, 5), (20, 5), (added - 5, 4)] {
            let mut torn = bytes[..log.starts[1] + kept].to_vec();
            torn.extend(std::iter::repeat_n(0, zeros));
            std::fs::write(&log.path, &torn)?;

            // Act
            let loaded = strict_load(&log.fs)?;

            // Assert
            assert_eq!(loaded.len(), 1, "kept={kept} zeros={zeros}");
        }
        Ok(())
    }

    #[test]
    fn should_fail_when_mid_file_frame_is_entirely_zeroed() -> MidgeResult<()> {
        // Arrange
        let log = log_with_edits(3)?;
        let mut bytes = std::fs::read(&log.path)?;
        bytes[log.starts[1]..log.starts[2]].fill(0);
        std::fs::write(&log.path, &bytes)?;

        // Act
        let loaded = strict_load(&log.fs);
        let saved = save(&log.fs, &[entry(1), entry(9)]);

        // Assert
        assert!(loaded.is_err(), "{loaded:?}");
        assert!(saved.is_err());
        assert_eq!(std::fs::read(&log.path)?, bytes);
        Ok(())
    }

    #[test]
    fn should_load_empty_when_file_is_entirely_zeros() -> MidgeResult<()> {
        // Arrange: a first append that persisted its length but not its bytes.
        let log = log_with_edits(1)?;
        std::fs::write(&log.path, [0_u8; 64])?;

        // Act
        let loaded = strict_load(&log.fs)?;
        save(&log.fs, &[entry(4)])?;

        // Assert
        assert!(loaded.is_empty());
        assert_eq!(strict_load(&log.fs)?.len(), 1);
        Ok(())
    }

    #[test]
    fn should_report_corruption_when_single_field_of_a_complete_frame_is_damaged() -> MidgeResult<()>
    {
        // Arrange: magic (8), kind (8 = byte 8), deflated length, payload, crc.
        let log = log_with_edits(3)?;
        let original = std::fs::read(&log.path)?;
        for frame in [1, 2] {
            let start = log.starts[frame];
            let end = log.starts[frame + 1];
            for offset in [start + 3, start + 8, start + 9, start + 14, end - 1] {
                let mut bytes = original.clone();
                bytes[offset] ^= 1;
                std::fs::write(&log.path, &bytes)?;

                // Act
                let loaded = strict_load(&log.fs);
                let saved = save(&log.fs, &[entry(1), entry(9)]);

                // Assert: damage to a complete durable frame, mid-file or
                // final, is reported and never silently dropped or rewritten.
                assert!(loaded.is_err(), "frame={frame} offset={offset}: {loaded:?}");
                assert!(saved.is_err(), "frame={frame} offset={offset}");
                assert_eq!(std::fs::read(&log.path)?, bytes);
            }
        }
        Ok(())
    }
}
