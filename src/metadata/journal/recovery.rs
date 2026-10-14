pub(crate) fn next_edit_id_after(highest: u64) -> MidgeResult<u64> {
    highest.checked_add(1).ok_or_else(|| {
        crate::common::MidgeError::ResourceLimit("manifest journal edit IDs exhausted".into())
    })
}

/// Read the current position and repair a genuine partial tail. Reading or
/// checkpointing the last valid identity remains legal after allocation ends.
pub(crate) fn highest_edit_id_with_repair_with_fs_unlocked(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<u64> {
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
    Ok(checkpoint.max(journal_max))
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
    Ok(
        replay_identified_edits_after_with_fs_unlocked(fs, checkpoint)?
            .into_iter()
            .map(|(_, edit)| edit)
            .collect(),
    )
}

/// Journal edits newer than `checkpoint`, each with its edit id.
pub(crate) fn replay_identified_edits_after_with_fs_unlocked(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    checkpoint: u64,
) -> MidgeResult<Vec<(u64, ManifestEdit)>> {
    replay_identified_edits_after_with_fs_unlocked_within(fs, checkpoint, None)
}

pub(crate) fn replay_identified_edits_after_with_fs_unlocked_within(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    checkpoint: u64,
    scope: Option<&crate::common::DeadlineScope>,
) -> MidgeResult<Vec<(u64, ManifestEdit)>> {
    let replay = replay_journal_with_mode(fs, JournalReplayMode::Strict, scope)?;
    let mut edits = Vec::new();
    for edit in replay.edits {
        check_replay_scope(scope)?;
        if edit.edit_id > checkpoint {
            edits.push((edit.edit_id, edit.edit));
        }
    }
    check_replay_scope(scope)?;
    Ok(edits)
}

/// Durable journal prefix recovered by salvage replay.
pub(crate) struct SalvagedJournal {
    /// Durable edits newer than the checkpoint, up to the last fsync marker
    /// before any corrupt record.
    pub(crate) edits: Vec<ManifestEdit>,
    /// Highest edit id in the durable prefix.
    pub(crate) max_edit_id: u64,
    /// The first corrupt record, if replay stopped early.
    pub(crate) corruption: Option<String>,
}

/// Replay the journal, stopping at the first corrupt record instead of
/// failing, and keep every durable edit before it.
#[cfg(test)]
pub(crate) fn salvage_edits_after_with_fs_unlocked(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    checkpoint: u64,
) -> MidgeResult<SalvagedJournal> {
    salvage_edits_after_with_fs_unlocked_within(fs, checkpoint, None)
}

pub(crate) fn salvage_edits_after_with_fs_unlocked_within(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    checkpoint: u64,
    scope: Option<&crate::common::DeadlineScope>,
) -> MidgeResult<SalvagedJournal> {
    let replay = replay_journal_with_mode(fs, JournalReplayMode::SalvagePrefix, scope)?;
    let mut edits = Vec::new();
    for edit in replay.edits {
        check_replay_scope(scope)?;
        if edit.edit_id > checkpoint {
            edits.push(edit.edit);
        }
    }
    check_replay_scope(scope)?;
    Ok(SalvagedJournal {
        edits,
        max_edit_id: replay.max_edit_id,
        corruption: replay.corruption,
    })
}

/// Durably copy the current journal to `manifest.journal.corrupt.<millis>`
/// so an operator can inspect it after salvage rewrites the journal.
pub(crate) fn preserve_corrupt_journal_with_fs_unlocked(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<String> {
    use crate::io::traits::FsPath;

    let Some(file) = open_journal_for_replay(fs)? else {
        return Err(crate::common::MidgeError::Internal(
            "manifest journal disappeared before it could be preserved".to_string(),
        ));
    };
    let len = file.len().map_err(FsError::into_midge)?;
    let bytes = file.read_at(0, len).map_err(FsError::into_midge)?;
    drop(file);
    let preserved = format!("{JOURNAL_FILE}.corrupt.{}", millis_since_epoch());
    crate::io::staging::stage_bytes(
        fs,
        &FsPath::new(format!("{preserved}.tmp")),
        &FsPath::new(&preserved),
        &bytes,
        crate::common::MidgeError::Internal,
    )?;
    Ok(preserved)
}
