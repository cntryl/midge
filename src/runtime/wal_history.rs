//! Which WAL files are history (#808). Today it is whatever `wal/` holds;
//! #842 replaces the listing with the segment ledger. This is the only place
//! that decides which files recovery replays.

use crate::common::MidgeResult;
use crate::io::{Fs, FsError, FsPath};
use crate::wal::recovery::ReplayFile;

/// The WAL files under `wal_dir` to replay, in order: rotated segments by
/// ascending id, preferring each id's canonical name, then the active file.
///
/// # Errors
///
/// Returns an error if `wal_dir` cannot be listed.
pub(crate) fn listed_segments(storage: &dyn Fs, wal_dir: &FsPath) -> MidgeResult<Vec<ReplayFile>> {
    if !storage.exists(wal_dir).map_err(FsError::into_midge)? {
        return Ok(Vec::new());
    }
    let entries = match storage.list_dir(wal_dir) {
        Ok(entries) => entries,
        Err(FsError::NotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into_midge()),
    };
    let mut sealed: std::collections::BTreeMap<u64, String> = std::collections::BTreeMap::new();
    let mut active = None;
    for entry in entries {
        if entry.is_dir {
            continue;
        }
        let name = entry.name;
        if name == crate::wal::ACTIVE_FILE_NAME {
            active = Some(join(wal_dir, crate::wal::ACTIVE_FILE_NAME));
            continue;
        }
        if let Some(segment_id) = crate::wal::parse_segment_id(&name) {
            let canonical = crate::wal::cloud_segment::file_name(segment_id);
            let prefer = sealed
                .get(&segment_id)
                .is_none_or(|existing| *existing != canonical && name == canonical);
            if prefer {
                sealed.insert(segment_id, name);
            }
        }
    }
    let mut segments: Vec<ReplayFile> = sealed
        .into_values()
        .map(|name| ReplayFile::sealed(join(wal_dir, &name)))
        .collect();
    segments.extend(active.map(ReplayFile::active));
    Ok(segments)
}

fn join(dir: &FsPath, leaf: &str) -> FsPath {
    let base = dir.0.trim_end_matches('/');
    if base.is_empty() {
        FsPath::new(leaf)
    } else {
        FsPath::new(format!("{base}/{leaf}"))
    }
}
