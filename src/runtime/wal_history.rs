//! Which WAL files are history (#808). Today it is whatever `wal/` holds;
//! #842 replaces the listing with the segment ledger. This is the only place
//! that decides which files recovery replays.

use crate::common::MidgeResult;
use crate::io::{Fs, FsError, FsPath};
use crate::wal::recovery::ReplaySegments;

/// The WAL files under `wal_dir` to replay, in order: rotated segments by
/// ascending id, preferring each id's canonical name, then the active file.
///
/// # Errors
///
/// Returns an error if `wal_dir` cannot be listed.
pub(crate) fn listed_segments(storage: &dyn Fs, wal_dir: &FsPath) -> MidgeResult<ReplaySegments> {
    if !storage.exists(wal_dir).map_err(FsError::into_midge)? {
        return Ok(ReplaySegments::default());
    }
    let entries = match storage.list_dir(wal_dir) {
        Ok(entries) => entries,
        Err(FsError::NotFound(_)) => return Ok(ReplaySegments::default()),
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
    Ok(ReplaySegments::new(
        sealed.into_values().map(|name| join(wal_dir, &name)),
        active,
    ))
}

fn join(dir: &FsPath, leaf: &str) -> FsPath {
    let base = dir.0.trim_end_matches('/');
    if base.is_empty() {
        FsPath::new(leaf)
    } else {
        FsPath::new(format!("{base}/{leaf}"))
    }
}

#[cfg(test)]
mod tests {
    use super::listed_segments;
    use crate::io::{Fs, FsPath, MockFs, OpenMode, OpenOptions};

    fn write(fs: &MockFs, name: &str, bytes: &[u8]) {
        let mut file = fs
            .open(
                &FsPath::new(format!("wal/{name}")),
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )
            .expect("create WAL file");
        file.append(bytes::Bytes::copy_from_slice(bytes))
            .expect("write WAL file");
    }

    #[test]
    fn should_list_sealed_segments_by_id_then_active_file_when_listing() {
        // Arrange: segments written out of order, an alias of segment 1, and
        // the active file.
        let fs = MockFs::new();
        fs.create_dir_all(&FsPath::new("wal")).expect("WAL dir");
        write(&fs, &crate::wal::segment_file_name(2), b"two");
        write(&fs, crate::wal::ACTIVE_FILE_NAME, b"active");
        write(&fs, "wal_1.log", b"alias");
        write(&fs, &crate::wal::segment_file_name(1), b"one");

        // Act
        let segments = listed_segments(&fs, &FsPath::new("wal")).expect("list");

        // Assert: the canonical name of segment 1 wins, and the active file
        // is replayed last.
        assert_eq!(
            format!("{segments:?}"),
            format!(
                "{:?}",
                crate::wal::recovery::ReplaySegments::new(
                    [1, 2].map(|id| FsPath::new(format!(
                        "wal/{}",
                        crate::wal::segment_file_name(id)
                    ))),
                    Some(FsPath::new(format!("wal/{}", crate::wal::ACTIVE_FILE_NAME))),
                )
            )
        );
    }

    #[test]
    fn should_list_nothing_when_wal_directory_is_absent() {
        // Arrange
        let fs = MockFs::new();

        // Act
        let segments = listed_segments(&fs, &FsPath::new("wal")).expect("list");

        // Assert
        assert_eq!(segments.len(), 0);
    }
}
