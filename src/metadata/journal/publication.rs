use super::{
    encode_journal_record, millis_since_epoch, FsError, FsyncMarker, JournalEditEnvelope,
    ManifestEdit, MidgeResult, BATCH_RECORD_TYPE, FSYNC_MARKER_TYPE, JOURNAL_FILE,
    JOURNAL_REPAIR_TEMP_FILE,
};

/// Append an edit to the manifest journal using a provided Fs (preferred).
/// Returns the journal edit id assigned to the record.
#[cfg(test)]
pub fn append_edit_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    edit: &ManifestEdit,
) -> MidgeResult<u64> {
    edit.validate_for_append()?;
    with_manifest_writer_lock(fs, || append_validated_edit_with_fs(fs, edit))
}

#[cfg(test)]
fn append_validated_edit_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    edit: &ManifestEdit,
) -> MidgeResult<u64> {
    let edit_id = next_edit_id_with_fs(fs)?;
    append_validated_edit_with_id(fs, edit, edit_id)
}

/// Appends a validated edit under `edit_id`. The caller holds the manifest
/// writer lock and has chosen an id above every durable one.
#[cfg(test)]
pub(crate) fn append_validated_edit_with_id(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    edit: &ManifestEdit,
    edit_id: u64,
) -> MidgeResult<u64> {
    append_validated_edit_with_id_observed(fs, edit, edit_id, None, None)
}

pub(crate) fn append_validated_edit_with_id_observed(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    edit: &ManifestEdit,
    edit_id: u64,
    observation: Option<&crate::metadata::accounting::Operation>,
    write_started: Option<&std::cell::Cell<bool>>,
) -> MidgeResult<u64> {
    // A batch writes the same record either way; route it through the batch
    // path so its storage-fault boundaries stay reachable.
    if let ManifestEdit::Batch(batch) = edit {
        return append_validated_edit_batch_with_id_observed(
            fs,
            batch,
            edit_id,
            observation,
            write_started,
        );
    }
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
    let (write_ns, fsync_ns) =
        append_record_and_marker_with_fs(fs, record, edit_id, observation, write_started)?;
    tracing::info!(
        write_ns,
        fsync_ns,
        "manifest journal append: edit and marker durably synced (ns)"
    );
    Ok(edit_id)
}

fn append_record_and_marker_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    record: Vec<u8>,
    edit_id: u64,
    observation: Option<&crate::metadata::accounting::Operation>,
    write_started: Option<&std::cell::Cell<bool>>,
) -> MidgeResult<(u128, u128)> {
    use crate::io::traits::{Durability, FsPath, OpenMode, OpenOptions};

    let marker = encode_journal_record(
        FSYNC_MARKER_TYPE,
        &FsyncMarker {
            last_persisted_sequence: edit_id,
            ts_millis: millis_since_epoch(),
        },
    )?;

    let framed_bytes = u64::try_from(record.len())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(marker.len()).unwrap_or(u64::MAX));
    let journal_path = FsPath::new(JOURNAL_FILE);
    // A missing or still-empty journal has never had its directory entry
    // made durable, whether this append creates it or a crash left it empty.
    let creating_journal = !fs.exists(&journal_path).map_err(FsError::into_midge)?
        || fs.metadata(&journal_path).map_err(FsError::into_midge)?.len == 0;
    let mut file = fs
        .open(
            &journal_path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;

    let write_start = std::time::Instant::now();
    if let Some(write_started) = write_started {
        write_started.set(true);
    }
    file.append(bytes::Bytes::from(record))
        .map_err(FsError::into_midge)?;

    crate::failpoints::fail_point!("midge::manifest::inject_fsync_marker_write_failure", |_| {
        Err(crate::common::MidgeError::Io(std::io::Error::other(
            "injected manifest fsync marker write failure",
        )))
    });
    file.append(bytes::Bytes::from(marker))
        .map_err(FsError::into_midge)?;
    let write_ns = write_start.elapsed().as_nanos();

    crate::failpoints::fail_point!("midge::manifest::before_required_sync");
    crate::failpoints::fail_point!("midge::manifest::inject_required_sync_failure", |_| Err(
        crate::common::MidgeError::Io(std::io::Error::other(
            "injected required manifest journal sync failure"
        ))
    ));
    let fsync_start = std::time::Instant::now();
    file.sync(Durability::Durable)
        .map_err(FsError::into_midge)?;
    if creating_journal {
        // An fsynced file is still lost after a crash if the directory entry
        // naming it was never synced. The acknowledged edit (for example a
        // column-family create) must survive, so sync the directory once.
        fs.sync_dir(&FsPath::new("."), Durability::Durable)
            .map_err(FsError::into_midge)?;
    }
    let fsync_ns = fsync_start.elapsed().as_nanos();
    if let Some(observation) = observation {
        observation.journal_durable(framed_bytes);
    }

    Ok((write_ns, fsync_ns))
}

pub(super) fn repair_partial_journal_tail(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    last_valid_offset: u64,
) -> MidgeResult<()> {
    use crate::io::traits::{FsPath, OpenMode, OpenOptions};

    let journal_path = FsPath::new(JOURNAL_FILE);
    let file = fs
        .open(
            &journal_path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    let file_len = file.len().map_err(FsError::into_midge)?;
    if last_valid_offset > file_len {
        return Err(crate::common::MidgeError::Corruption(format!(
            "manifest journal repair offset {last_valid_offset} exceeds file length {file_len}"
        )));
    }
    let valid_prefix = file
        .read_at(0, last_valid_offset)
        .map_err(FsError::into_midge)?;
    drop(file);

    crate::io::staging::stage_bytes(
        fs,
        &FsPath::new(JOURNAL_REPAIR_TEMP_FILE),
        &journal_path,
        &valid_prefix,
        crate::common::MidgeError::Internal,
    )
}

/// Append a batch of edits as a single TLV record using the provided Fs (preferred).
/// Returns the journal edit id assigned to the record.
#[cfg(test)]
pub fn append_edit_batch_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    batch: &[ManifestEdit],
) -> MidgeResult<u64> {
    ManifestEdit::Batch(batch.to_vec()).validate_for_append()?;
    with_manifest_writer_lock(fs, || append_validated_edit_batch_with_fs(fs, batch))
}

#[cfg(test)]
fn append_validated_edit_batch_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    batch: &[ManifestEdit],
) -> MidgeResult<u64> {
    let edit_id = next_edit_id_with_fs(fs)?;
    append_validated_edit_batch_with_id(fs, batch, edit_id)
}

/// Appends a validated batch under `edit_id`; see
/// [`append_validated_edit_with_id_observed`].
#[cfg(test)]
pub(crate) fn append_validated_edit_batch_with_id(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    batch: &[ManifestEdit],
    edit_id: u64,
) -> MidgeResult<u64> {
    append_validated_edit_batch_with_id_observed(fs, batch, edit_id, None, None)
}

pub(crate) fn append_validated_edit_batch_with_id_observed(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    batch: &[ManifestEdit],
    edit_id: u64,
    observation: Option<&crate::metadata::accounting::Operation>,
    write_started: Option<&std::cell::Cell<bool>>,
) -> MidgeResult<u64> {
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
    let (write_ns, fsync_ns) =
        append_record_and_marker_with_fs(fs, record, edit_id, observation, write_started)?;
    tracing::info!(
        batch_size = batch.len(),
        write_ns,
        fsync_ns,
        "manifest journal batch append: edit and marker durably synced (ns)"
    );
    Ok(edit_id)
}

/// Append `batch` as one record through a `RealFs` rooted at `db_path`.
#[cfg(test)]
pub fn append_edit_batch(db_path: &Path, batch: &[ManifestEdit]) -> MidgeResult<u64> {
    let fs: std::sync::Arc<dyn crate::io::traits::Fs> =
        std::sync::Arc::new(crate::io::real::RealFs::new(db_path).map_err(|e| {
            crate::common::MidgeError::Internal(format!("failed to create RealFs: {e:?}"))
        })?);
    append_edit_batch_with_fs(&fs, batch)
}
#[cfg(test)]
use super::{next_edit_id_with_fs, with_manifest_writer_lock, Path};
