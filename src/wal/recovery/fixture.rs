//! Replay lists for WAL test fixtures: the segments a fixture wrote under its
//! WAL directory, named by id the way the segment ledger names them, sealed
//! ids in order and then the open one. Tests state their own lists so they
//! do not depend on how the runtime decides membership. A list expects of
//! each file the header and footer it holds, which [`wrap_chain`] settles;
//! replay then checks that the segments chain.

use super::{Expectation, ReplaySegments};
use crate::io::FsPath;

/// Replay `sealed` in order, then `open` as the final active segment, all
/// under `wal_dir`, after any anchor. Pass the list through [`wrap_chain`]
/// before replaying it.
pub(crate) fn segments(wal_dir: &FsPath, sealed: &[u64], open: Option<u64>) -> ReplaySegments {
    segments_in_epoch(wal_dir, sealed, open, 0)
}

/// [`segments`] whose files are written by `epoch`; [`wrap_chain`] settles
/// what each file is expected to hold.
pub(crate) fn segments_in_epoch(
    wal_dir: &FsPath,
    sealed: &[u64],
    open: Option<u64>,
    epoch: u64,
) -> ReplaySegments {
    let path = |id: u64| {
        let name = crate::wal::segment_file_name(id);
        let base = wal_dir.0.trim_end_matches('/');
        FsPath::new(if base.is_empty() {
            name
        } else {
            format!("{base}/{name}")
        })
    };
    let mut segments = ReplaySegments::new(None);
    for id in sealed {
        segments.push_sealed(*id, path(*id), unread(*id, epoch));
    }
    if let Some(id) = open {
        segments.push_open(id, path(id), unread(id, epoch));
    }
    segments
}

/// What a fixture expects of segment `id` by `epoch` before it read the
/// file: its header facts as a writer would leave them after genesis. A
/// file [`wrap_chain`] cannot read never reaches the expectation.
fn unread(id: u64, epoch: u64) -> Expectation {
    Expectation::Member {
        member: crate::wal::ledger::Member {
            open: crate::wal::format::OpenFacts {
                id,
                epoch,
                first_seq: 1,
                prev: crate::wal::format::ChainLink::GENESIS,
            },
            sealed: None,
            durable_bytes: None,
        },
        holder: crate::wal::format::segment::FIXTURE_HOLDER,
    }
}

/// What a fixture expects of a file whose bytes are `bytes`: exactly the
/// header it holds and, when sealed, the footer that seals it.
fn holding(bytes: &[u8]) -> Option<Expectation> {
    let header = crate::wal::format::segment::SegmentHeader::decode(bytes).ok()?;
    let sealed = crate::wal::format::segment::sealed_footer(bytes)
        .ok()
        .flatten()
        .map(|footer| footer.sealed_facts());
    Some(Expectation::Member {
        member: crate::wal::ledger::Member {
            open: header.open_facts(),
            sealed,
            durable_bytes: None,
        },
        holder: header.holder,
    })
}

/// Make every file `segments` names that a test wrote as a bare body of
/// frames into a v2 segment, chained in replay order: the header, linked to
/// the file before, then the body, then, for a sealed file, a footer whose
/// facts come from whatever frames decode (`SegmentBuilder::seal`), so a
/// deliberately corrupt body still seals. Files that already start with a
/// header are left as they are. Every wrapped segment is written by `epoch`.
/// Returns `segments` expecting of each file the epoch its header names.
///
/// # Panics
///
/// Panics if a file cannot be read or written.
pub(crate) fn wrap_chain(
    storage: &dyn crate::io::Fs,
    segments: &ReplaySegments,
    epoch: u64,
) -> ReplaySegments {
    use crate::wal::format::segment::{footer_link, lenient_footer, SegmentHeader};
    let mut settled = segments.clone();
    let mut prev = segments
        .anchor
        .unwrap_or(crate::wal::format::ChainLink::GENESIS);
    for file in &mut settled.files {
        let Ok(bytes) = crate::io::read_all(storage, &file.path) else {
            continue;
        };
        if let Some(expect) = holding(&bytes) {
            file.expect = expect;
            if let Ok(Some(link)) = footer_link(&bytes) {
                prev = link;
            }
            continue;
        }
        let header = SegmentHeader {
            id: file.id,
            epoch,
            holder: crate::wal::format::segment::FIXTURE_HOLDER,
            first_seq: prev.last_seq.saturating_add(1),
            prev,
        };
        let mut wrapped = header.encode().to_vec();
        wrapped.extend_from_slice(&bytes);
        if file.kind == super::ReplayFileKind::Sealed {
            let footer = lenient_footer(&wrapped).expect("a written header decodes");
            prev = footer.sealed_facts().link;
            wrapped.extend(footer.encode_frame().expect("encode fixture footer"));
        }
        file.expect = holding(&wrapped).unwrap_or_else(|| unread(file.id, epoch));
        let mut handle = storage
            .open(
                &file.path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: true,
                },
            )
            .expect("open fixture segment");
        handle
            .write_at(0, bytes::Bytes::from(wrapped))
            .expect("write fixture segment");
        handle
            .sync(crate::io::Durability::Durable)
            .expect("sync fixture segment");
    }
    settled
}
