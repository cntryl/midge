//! Test fixtures that give a WAL a segment-ledger history (#842, #843).
//! Recovery replays only what `wal/LEDGER` names, and only segments whose
//! headers chain, so a test that writes segment files by hand must also
//! record them. Every test seeds ledgers through this one module, and only
//! through [`WalHistory`], so the ledger's write API can change in one place.
//!
//! A segment file a test wrote is adopted as it is when it is already a
//! v2 segment (`wal::format`); otherwise its bytes are a body the fixture
//! wraps: the real header before it and, once sealed, a footer whose facts
//! come from whatever frames decode, so a deliberately corrupt body still
//! seals.

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions};
use crate::runtime::wal_history::WalHistory;
use crate::wal::format::{ChainLink, Footer, SealedFacts, SegmentHeader, HEADER_LEN};
use crate::wal::ledger::SegmentId;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Rotate `history` as a writer does: seal the open segment with a footer
/// for whatever it holds, create the next one linked to it, and record the
/// rotation. Returns the new open segment.
pub(crate) fn rotate(history: &mut WalHistory) -> MidgeResult<SegmentId> {
    let open = history.ledger().open_member().open;
    let (facts, footer) = match history.segment_files() {
        Some(wal) => seal_facts(wal.as_ref(), open.id)?,
        None => (empty_facts(open.id, open.prev), None),
    };
    let first_seq = facts.link.last_seq.saturating_add(1).max(open.first_seq);
    let prepared = history.prepare_rotation(facts, first_seq)?;
    if let (Some(wal), Some((offset, frame))) = (history.segment_files(), footer) {
        write_at(wal.as_ref(), open.id, offset, &frame)?;
    }
    history.record_rotation(prepared)?;
    Ok(history.ledger().open_segment())
}

/// The facts sealing `history`'s open segment as it stands, counted
/// leniently, without writing anything.
pub(crate) fn lenient_seal(history: &WalHistory) -> MidgeResult<SealedFacts> {
    let open = history.ledger().open_member().open;
    match history.segment_files() {
        Some(wal) => Ok(seal_facts(wal.as_ref(), open.id)?.0),
        None => Ok(empty_facts(open.id, open.prev)),
    }
}

/// Write and sync the footer of `history`'s open segment, as a rotation
/// does before the ledger records it, and return its facts. The ledger is
/// left as it was.
pub(crate) fn seal_open_for_test(history: &mut WalHistory) -> MidgeResult<SealedFacts> {
    let wal = history
        .segment_files()
        .ok_or_else(|| MidgeError::Internal("a memory-mode WAL writes no footer".into()))?;
    let open = history.ledger().open_member().open;
    let (facts, footer) = seal_facts(wal.as_ref(), open.id)?;
    if let Some((offset, frame)) = footer {
        write_at(wal.as_ref(), open.id, offset, &frame)?;
    }
    Ok(facts)
}

/// Rotate `history` until `open` is its open segment.
///
/// # Panics
///
/// Panics if `history` is already past `open`.
pub(crate) fn rotate_to(history: &mut WalHistory, open: SegmentId) -> MidgeResult<()> {
    while history.ledger().open_segment() < open {
        rotate(history)?;
    }
    assert_eq!(
        history.ledger().open_segment(),
        open,
        "the WAL history is already past segment {open}"
    );
    Ok(())
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
/// Segment files already in `wal` keep their bytes (see the module docs); a
/// member without a file holds no record.
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
    if wal
        .exists(&FsPath::new(crate::wal::ledger_store::LEDGER_FILE))
        .map_err(FsError::into_midge)?
    {
        return Err(MidgeError::InvalidArgument(
            "the WAL directory already has a ledger".into(),
        ));
    }
    // A new history starts over no segment file, so set the fixture's bytes
    // aside while recording and put them back as each member is created.
    let stashed = stash_segments(wal.as_ref())?;
    let adopted = |id: SegmentId| {
        stashed
            .get(&id)
            .and_then(|bytes| SegmentHeader::decode(bytes).ok())
    };
    let (epoch, anchor) = adopted(ids[0]).map_or((0, ChainLink::GENESIS), |header| {
        (header.epoch, header.prev)
    });
    let mut history = WalHistory::genesis(Arc::clone(wal), epoch, anchor, ids[0])?;
    for (index, id) in ids.iter().enumerate() {
        if let Some(bytes) = stashed.get(id) {
            if adopted(*id).is_some() {
                replace_file(wal.as_ref(), *id, bytes)?;
            } else {
                write_at(wal.as_ref(), *id, HEADER_LEN as u64, bytes)?;
            }
        }
        if let Some(next) = ids.get(index + 1) {
            if let Some(header) = adopted(*next) {
                history.epoch = header.epoch;
            }
            rotate(&mut history)?;
        }
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

/// A footer frame and the offset it is written at.
type FooterWrite = (u64, Vec<u8>);

/// The facts sealing open segment `id` of `wal`, and the footer frame to
/// write at its offset, unless the segment already ends in one.
fn seal_facts(wal: &dyn Fs, id: SegmentId) -> MidgeResult<(SealedFacts, Option<FooterWrite>)> {
    let bytes = crate::io::read_all(wal, &segment_path(id)).map_err(FsError::into_midge)?;
    if let Ok(Some(link)) = crate::wal::format::segment::footer_link(&bytes) {
        let footer = existing_footer(&bytes, link)?;
        return Ok((footer.sealed_facts(), None));
    }
    let footer = crate::wal::format::segment::lenient_footer(&bytes)?;
    let frame = footer.encode_frame()?;
    Ok((footer.sealed_facts(), Some((bytes.len() as u64, frame))))
}

/// The footer of a sealed segment, decoded from its final frame.
fn existing_footer(bytes: &[u8], link: ChainLink) -> MidgeResult<Footer> {
    let mut pos = HEADER_LEN as u64;
    loop {
        let step = crate::wal::frame::next_frame(
            &bytes,
            &"fixture",
            pos,
            crate::wal::frame::FrameLimits::default(),
        )
        .map_err(crate::wal::frame::FrameError::into_error)?;
        let crate::wal::frame::FrameStep::Frame { payload, next_pos } = step else {
            return Err(MidgeError::Internal(format!(
                "segment {} has no footer frame",
                link.id
            )));
        };
        if crate::wal::format::segment::is_footer_payload(&payload) {
            return Footer::decode_payload(&payload);
        }
        pos = next_pos;
    }
}

/// The facts sealing a memory-mode segment, which holds nothing.
fn empty_facts(id: SegmentId, prev: ChainLink) -> SealedFacts {
    SealedFacts {
        link: ChainLink {
            id,
            last_seq: prev.last_seq,
            digest: [0; 16],
        },
        record_count: 0,
        data_len: 0,
        max_seq_per_cf: Vec::new(),
    }
}

fn segment_path(id: SegmentId) -> FsPath {
    FsPath::new(crate::wal::segment_file_name(id))
}

fn write_at(wal: &dyn Fs, id: SegmentId, offset: u64, bytes: &[u8]) -> MidgeResult<()> {
    let mut file = wal
        .open(
            &segment_path(id),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    file.write_at(offset, bytes::Bytes::copy_from_slice(bytes))
        .map_err(FsError::into_midge)?;
    file.sync(Durability::Durable).map_err(FsError::into_midge)
}

fn replace_file(wal: &dyn Fs, id: SegmentId, bytes: &[u8]) -> MidgeResult<()> {
    let mut file = wal
        .open(
            &segment_path(id),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: true,
            },
        )
        .map_err(FsError::into_midge)?;
    file.write_at(0, bytes::Bytes::copy_from_slice(bytes))
        .map_err(FsError::into_midge)?;
    file.sync(Durability::Durable).map_err(FsError::into_midge)
}

/// Remove every segment file in `wal`, returning each one's bytes by id.
fn stash_segments(wal: &dyn Fs) -> MidgeResult<BTreeMap<SegmentId, Vec<u8>>> {
    let entries = match wal.list_dir(&FsPath::new("")) {
        Ok(entries) => entries,
        Err(FsError::NotFound(_)) => return Ok(BTreeMap::new()),
        Err(error) => return Err(error.into_midge()),
    };
    let mut stashed = BTreeMap::new();
    for entry in entries {
        if entry.is_dir {
            continue;
        }
        let Some(id) = crate::wal::parse_segment_file_name(&entry.name) else {
            continue;
        };
        let path = FsPath::new(&entry.name);
        let bytes = crate::io::read_all(wal, &path).map_err(FsError::into_midge)?;
        wal.remove_file(&path).map_err(FsError::into_midge)?;
        stashed.insert(id, bytes.to_vec());
    }
    Ok(stashed)
}
