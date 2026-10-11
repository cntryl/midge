//! Replay lists for WAL test fixtures: the files a fixture wrote under its
//! WAL directory, sealed segments by id, then `wal.log`. Tests build their
//! own lists so they do not depend on how the runtime decides membership.

use super::ReplaySegments;
use crate::io::{Fs, FsPath};

pub(crate) fn segments(storage: &dyn Fs, wal_dir: &FsPath) -> ReplaySegments {
    let Ok(entries) = storage.list_dir(wal_dir) else {
        return ReplaySegments::default();
    };
    let path = |name: &str| {
        let base = wal_dir.0.trim_end_matches('/');
        FsPath::new(if base.is_empty() {
            name.to_string()
        } else {
            format!("{base}/{name}")
        })
    };
    let mut sealed: Vec<(u64, String)> = entries
        .iter()
        .filter(|entry| !entry.is_dir)
        .filter_map(|entry| {
            Some((
                crate::wal::parse_segment_id(&entry.name)?,
                entry.name.clone(),
            ))
        })
        .collect();
    sealed.sort();
    let active = entries
        .iter()
        .any(|entry| entry.name == crate::wal::ACTIVE_FILE_NAME)
        .then(|| path(crate::wal::ACTIVE_FILE_NAME));
    ReplaySegments::new(sealed.iter().map(|(_, name)| path(name)), active)
}
