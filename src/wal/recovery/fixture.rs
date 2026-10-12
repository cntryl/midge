//! Replay lists for WAL test fixtures: the segments a fixture wrote under its
//! WAL directory, named by id the way the segment ledger names them, sealed
//! ids in order and then the open one. Tests state their own lists so they
//! do not depend on how the runtime decides membership; replay checks only
//! that the segments chain.

use super::{Expectation, ReplaySegments};
use crate::io::FsPath;

/// Replay `sealed` in order, then `open` as the final active segment, all
/// under `wal_dir`, after any anchor.
pub(crate) fn segments(wal_dir: &FsPath, sealed: &[u64], open: Option<u64>) -> ReplaySegments {
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
        segments.push_sealed(*id, path(*id), Expectation::Chain);
    }
    if let Some(id) = open {
        segments.push_open(id, path(id), Expectation::Chain);
    }
    segments
}

/// Make every file `segments` names that a test wrote as a bare body of
/// frames into a v2 segment, chained in replay order: the header, linked to
/// the file before, then the body, then, for a sealed file, a footer whose
/// facts come from whatever frames decode (`SegmentBuilder::seal`), so a
/// deliberately corrupt body still seals. Files that already start with a
/// header are left as they are. Every wrapped segment is written by `epoch`.
///
/// # Panics
///
/// Panics if a file cannot be read or written.
pub(crate) fn wrap_chain(storage: &dyn crate::io::Fs, segments: &ReplaySegments, epoch: u64) {
    use crate::wal::format::segment::{footer_link, lenient_footer, SegmentHeader};
    let mut prev = segments
        .anchor
        .unwrap_or(crate::wal::format::ChainLink::GENESIS);
    for file in &segments.files {
        let Ok(bytes) = crate::io::read_all(storage, &file.path) else {
            continue;
        };
        if SegmentHeader::decode(&bytes).is_ok() {
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
}
