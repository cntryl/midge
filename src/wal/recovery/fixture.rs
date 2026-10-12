//! Replay lists for WAL test fixtures: the segments a fixture wrote under its
//! WAL directory, named by id the way the segment ledger names them, sealed
//! ids in order and then the open one. Tests state their own lists so they
//! do not depend on how the runtime decides membership.

use super::ReplaySegments;
use crate::io::FsPath;

/// Replay `sealed` in order, then `open` as the final active segment, all
/// under `wal_dir`.
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
    ReplaySegments::new(sealed.iter().copied().map(path), open.map(path))
}
