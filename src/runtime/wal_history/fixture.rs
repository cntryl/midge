//! Test fixtures that give a WAL a segment-ledger history (#842). Recovery
//! replays only what `wal/LEDGER` names, so a test that writes segment files
//! by hand must also record them. Every test seeds ledgers through this one
//! module, and only through [`WalHistory`], so the ledger's write API can
//! change in one place.

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions};
use crate::runtime::wal_history::WalHistory;
use crate::wal::ledger::SegmentId;
use std::sync::Arc;

/// Rotate `history`: create the segment after its open one and record it
/// open, sealing the previous one. Returns the new open segment.
pub(crate) fn rotate(history: &mut WalHistory) -> MidgeResult<SegmentId> {
    let prepared = history.prepare_rotation()?;
    history.record_rotation(prepared)?;
    Ok(history.ledger().open_segment())
}

/// Bring `history` to where a writer that sealed `segment_id` left it:
/// `segment_id` sealed at the log start and the segment after it open.
pub(crate) fn seal_through(history: &mut WalHistory, segment_id: SegmentId) -> MidgeResult<()> {
    while history.ledger().open_segment() <= segment_id {
        rotate(history)?;
    }
    if history.ledger().log_start() < segment_id {
        history.retire_below(segment_id)?;
    }
    Ok(())
}

/// Record a history for the WAL directory `wal`, which has no ledger yet:
/// `sealed`, oldest first, then `open`. The ids must be consecutive.
/// Segment files already in `wal` keep their bytes; a member without a
/// file is created empty.
///
/// # Panics
///
/// Panics if the ids are not consecutive.
pub(crate) fn record(wal: &Arc<dyn Fs>, sealed: &[SegmentId], open: SegmentId) -> MidgeResult<()> {
    let ids: Vec<SegmentId> = sealed.iter().copied().chain([open]).collect();
    assert!(
        ids.windows(2).all(|pair| pair[0] + 1 == pair[1]),
        "a recorded WAL history is consecutive: {ids:?}"
    );
    if WalHistory::read(wal)?.is_some() {
        return Err(MidgeError::InvalidArgument(
            "the WAL directory already has a ledger".into(),
        ));
    }
    // A new history starts over no segment file, so set the fixture's bytes
    // aside while recording and put them back after.
    let stashed = stash_segments(wal.as_ref())?;
    let mut history = WalHistory::open(Arc::clone(wal), ids[0])?;
    for _ in &ids[1..] {
        rotate(&mut history)?;
    }
    drop(history);
    for (name, bytes) in stashed {
        let mut file = wal
            .open(
                &FsPath::new(name),
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )
            .map_err(FsError::into_midge)?;
        file.write_at(0, bytes.into())
            .map_err(FsError::into_midge)?;
        file.sync(Durability::Durable)
            .map_err(FsError::into_midge)?;
    }
    Ok(())
}

/// Drop the ledger of the WAL directory `wal`, as if it had never been
/// opened, then [`record`] the given history over its segment files.
pub(crate) fn replace(wal: &Arc<dyn Fs>, sealed: &[SegmentId], open: SegmentId) -> MidgeResult<()> {
    match wal.remove_file(&FsPath::new(crate::wal::ledger_store::LEDGER_FILE)) {
        Ok(()) | Err(FsError::NotFound(_)) => {}
        Err(error) => return Err(error.into_midge()),
    }
    record(wal, sealed, open)
}

/// [`record`] for the `wal/` directory of the database at `db_path`.
pub(crate) fn record_at(
    db_path: &std::path::Path,
    sealed: &[SegmentId],
    open: SegmentId,
) -> MidgeResult<()> {
    let wal: Arc<dyn Fs> = Arc::new(
        crate::io::RealFs::new(db_path.join(crate::wal::WAL_DIR)).map_err(FsError::into_midge)?,
    );
    record(&wal, sealed, open)
}

/// Remove every segment file in `wal`, returning each one's name and bytes.
fn stash_segments(wal: &dyn Fs) -> MidgeResult<Vec<(String, Vec<u8>)>> {
    let entries = match wal.list_dir(&FsPath::new("")) {
        Ok(entries) => entries,
        Err(FsError::NotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into_midge()),
    };
    let mut stashed = Vec::new();
    for entry in entries {
        if entry.is_dir || crate::wal::parse_segment_id(&entry.name).is_none() {
            continue;
        }
        let path = FsPath::new(&entry.name);
        let bytes = crate::io::read_all(wal, &path).map_err(FsError::into_midge)?;
        wal.remove_file(&path).map_err(FsError::into_midge)?;
        stashed.push((entry.name, bytes.to_vec()));
    }
    Ok(stashed)
}
