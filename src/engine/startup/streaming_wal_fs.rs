//! Read-only replay views over individually authorized WAL sources.
//!
//! The startup planner owns catalog, epoch, and alias selection. This view
//! exposes its canonical names without copying WAL objects onto local disk.

use crate::common::{MidgeError, MidgeResult};
use crate::io::buffered_read::BufferedReadFile;
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, Fs, FsError, FsPath, FsResult, OpenMode, OpenOptions};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

const READ_ONLY: OpenOptions = OpenOptions {
    mode: OpenMode::ReadOnly,
    create: false,
    create_new: false,
    truncate: false,
};

struct WalSource {
    fs: Arc<dyn Fs>,
    path: FsPath,
}

pub(super) struct StreamingWalFs {
    sources: BTreeMap<String, WalSource>,
    range_buffer_bytes: usize,
}

impl StreamingWalFs {
    pub(super) fn new(range_buffer_bytes: usize) -> MidgeResult<Self> {
        validate_buffer_size(range_buffer_bytes)?;
        Ok(Self {
            sources: BTreeMap::new(),
            range_buffer_bytes,
        })
    }

    pub(super) fn insert(
        &mut self,
        canonical_name: String,
        fs: Arc<dyn Fs>,
        backing_path: FsPath,
    ) -> MidgeResult<()> {
        if !canonical_wal_name(&canonical_name) {
            return Err(MidgeError::InvalidArgument(format!(
                "non-canonical replay WAL filename: {canonical_name}"
            )));
        }
        match self.sources.entry(canonical_name) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(WalSource {
                    fs,
                    path: backing_path,
                });
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(entry) => Err(MidgeError::RecoveryFailed(
                format!("duplicate replay WAL identity: {}", entry.key()),
            )),
        }
    }

    fn source(&self, path: &FsPath) -> FsResult<&WalSource> {
        let name = path.0.strip_prefix("wal/").unwrap_or(&path.0);
        if !canonical_wal_name(name) {
            return Err(FsError::NotFound(path.0.clone()));
        }
        self.sources
            .get(name)
            .ok_or_else(|| FsError::NotFound(path.0.clone()))
    }
}

fn canonical_wal_name(name: &str) -> bool {
    name == crate::wal::ACTIVE_FILE_NAME
        || crate::wal::parse_segment_id(name)
            .is_some_and(|segment_id| crate::wal::segment_file_name(segment_id) == name)
}

fn replay_directory(path: &FsPath) -> bool {
    matches!(path.0.as_str(), "" | "." | "wal")
}

fn read_only_error() -> FsError {
    FsError::Unsupported("streaming WAL replay views are read only".into())
}

fn require_read_only(options: OpenOptions) -> FsResult<()> {
    if options == READ_ONLY {
        Ok(())
    } else {
        Err(read_only_error())
    }
}

impl Fs for StreamingWalFs {
    fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        require_read_only(options)?;
        let source = self.source(path)?;
        Ok(Box::new(BufferedReadFile::new(
            source.fs.open(&source.path, READ_ONLY)?,
            self.range_buffer_bytes,
        )?))
    }

    fn open_persistent_handle(
        &self,
        path: &FsPath,
        options: OpenOptions,
    ) -> FsResult<Box<dyn File>> {
        require_read_only(options)?;
        let source = self.source(path)?;
        Ok(Box::new(BufferedReadFile::new(
            source.fs.open_persistent_handle(&source.path, READ_ONLY)?,
            self.range_buffer_bytes,
        )?))
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        if replay_directory(path) {
            return Ok(Metadata { len: 0 });
        }
        let source = self.source(path)?;
        source.fs.metadata(&source.path)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        if replay_directory(path) {
            return Ok(true);
        }
        match self.source(path) {
            Ok(source) => source.fs.exists(&source.path),
            Err(FsError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        if !replay_directory(path) {
            return Err(FsError::NotFound(path.0.clone()));
        }
        Ok(self
            .sources
            .keys()
            .map(|name| DirEntry {
                name: name.clone(),
                is_dir: false,
            })
            .collect())
    }

    fn remove_file(&self, _path: &FsPath) -> FsResult<()> {
        Err(read_only_error())
    }
    fn create_dir_all(&self, _path: &FsPath) -> FsResult<()> {
        Err(read_only_error())
    }
    fn remove_dir_all(&self, _path: &FsPath) -> FsResult<()> {
        Err(read_only_error())
    }
    fn sync_dir(&self, _path: &FsPath, _durability: Durability) -> FsResult<()> {
        Err(read_only_error())
    }
    fn rename_atomic(&self, _from: &FsPath, _to: &FsPath) -> FsResult<()> {
        Err(read_only_error())
    }
}

fn validate_buffer_size(bytes: usize) -> MidgeResult<()> {
    if bytes == 0 {
        Err(MidgeError::InvalidArgument(
            "streaming WAL range buffer must be positive".into(),
        ))
    } else {
        Ok(())
    }
}

/// Check the catalog's complete-object proof while retaining only one chunk.
pub(super) fn validate_wal_source(
    fs: &dyn Fs,
    path: &FsPath,
    expected_len: u64,
    expected_crc: u32,
    range_buffer_bytes: usize,
) -> MidgeResult<()> {
    validate_buffer_size(range_buffer_bytes)?;
    let file = fs.open(path, READ_ONLY)?;
    if file.len()? != expected_len {
        return Err(MidgeError::RecoveryFailed(format!(
            "WAL source {path} does not match catalog length {expected_len}"
        )));
    }
    let mut offset = 0_u64;
    let mut crc = 0;
    while offset < expected_len {
        let length = (range_buffer_bytes as u64).min(expected_len - offset);
        let bytes = read_exact_range(file.as_ref(), offset, length)?;
        crc = crc32c::crc32c_append(crc, &bytes);
        offset += length;
    }
    if file.len()? != expected_len || crc != expected_crc {
        return Err(MidgeError::RecoveryFailed(format!(
            "WAL source {path} does not match its catalog content checksum"
        )));
    }
    Ok(())
}

/// Prove exact alias equality without materializing either complete segment.
pub(super) fn wal_sources_equal(
    left: (&dyn Fs, &FsPath),
    right: (&dyn Fs, &FsPath),
    range_buffer_bytes: usize,
) -> MidgeResult<bool> {
    validate_buffer_size(range_buffer_bytes)?;
    let left_file = left.0.open(left.1, READ_ONLY)?;
    let right_file = right.0.open(right.1, READ_ONLY)?;
    let size = left_file.len()?;
    if right_file.len()? != size {
        return Ok(false);
    }
    let mut offset = 0_u64;
    while offset < size {
        let length = (range_buffer_bytes as u64).min(size - offset);
        if read_exact_range(left_file.as_ref(), offset, length)?
            != read_exact_range(right_file.as_ref(), offset, length)?
        {
            return Ok(false);
        }
        offset += length;
    }
    Ok(left_file.len()? == size && right_file.len()? == size)
}

fn read_exact_range(file: &dyn File, offset: u64, length: u64) -> MidgeResult<Bytes> {
    let bytes = file.read_at(offset, length)?;
    if bytes.len() as u64 != length {
        return Err(MidgeError::RecoveryFailed(
            "WAL source changed or returned a truncated range".into(),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{StorageBackend, StorageEvent, StorageObjectMetadata, StorageOutcome};
    use std::time::Duration;

    struct RecordingBackend {
        inner: crate::storage::filesystem::FileSystem,
        ranges: parking_lot::Mutex<Vec<(u64, u64)>>,
    }

    impl StorageBackend for RecordingBackend {
        fn submit_read(&self, _key: &str, _callback: crate::storage::StorageCallback) {
            panic!("whole-object recovery GET is forbidden");
        }
        fn submit_write(
            &self,
            _key: &str,
            _bytes: Vec<u8>,
            _callback: crate::storage::StorageCallback,
        ) {
            panic!("replay cannot write cloud objects");
        }
        fn submit_delete(&self, _key: &str, _callback: crate::storage::StorageCallback) {
            panic!("replay cannot delete cloud objects");
        }
        fn submit_list(&self, _prefix: &str, _callback: crate::storage::StorageCallback) {
            panic!("replay must use catalog-authorized objects");
        }
        fn submit_range_head(
            &self,
            key: &str,
            timeout: Duration,
            callback: crate::storage::StorageCallback,
        ) {
            self.inner.submit_range_head(key, timeout, callback);
        }
        fn submit_read_range(
            &self,
            key: &str,
            start: u64,
            end: u64,
            expected: StorageObjectMetadata,
            timeout: Duration,
            callback: crate::storage::RangeReadCallback,
        ) {
            self.ranges.lock().push((start, end));
            self.inner
                .submit_read_range(key, start, end, expected, timeout, callback);
        }
    }

    struct Fixture {
        directory: tempfile::TempDir,
        backend: Arc<RecordingBackend>,
        source: Arc<dyn Fs>,
        bytes: Vec<u8>,
        path: FsPath,
    }

    impl Fixture {
        fn new() -> MidgeResult<Self> {
            let directory = tempfile::tempdir()?;
            let cloud_root = directory.path().join("cloud");
            let local = Arc::new(crate::io::RealFs::new(directory.path().join("local"))?);
            let backend = Arc::new(RecordingBackend {
                inner: crate::storage::filesystem::FileSystem::new(&cloud_root)?,
                ranges: parking_lot::Mutex::new(Vec::new()),
            });
            let key = crate::wal::segment_object_key(41, 7);
            let full_path = cloud_root.join(&key);
            std::fs::create_dir_all(full_path.parent().expect("object parent"))?;
            let bytes: Vec<u8> = (0..65_537_u32)
                .map(|index| index.to_le_bytes()[0])
                .collect();
            std::fs::write(&full_path, &bytes)?;
            let (tx, rx) = std::sync::mpsc::channel();
            backend.submit_range_head(&key, Duration::from_secs(5), tx);
            let metadata = match rx.recv_timeout(Duration::from_secs(5)).expect("range HEAD") {
                StorageEvent::HeadComplete {
                    result: StorageOutcome::Ok(metadata),
                    ..
                } => metadata,
                other => panic!("unexpected HEAD {other:?}"),
            };
            let source = Arc::new(crate::storage::remote_sst::RemoteSstFs::for_object(
                local,
                backend.clone(),
                key,
                metadata,
                Duration::from_secs(5),
            ));
            Ok(Self {
                directory,
                backend,
                source,
                bytes,
                path: FsPath::new("authorized-source"),
            })
        }
    }

    #[test]
    fn should_read_large_cloud_wal_frames_through_bounded_ranges_without_local_copies(
    ) -> MidgeResult<()> {
        // Arrange
        let fixture = Fixture::new()?;
        let mut fs = StreamingWalFs::new(127)?;
        let name = crate::wal::segment_file_name(41);
        fs.insert(
            name.clone(),
            Arc::clone(&fixture.source),
            fixture.path.clone(),
        )?;
        let path = FsPath::new(format!("wal/{name}"));
        let file = fs.open(&path, READ_ONLY)?;

        // Act
        let bytes = file.read_at(9, 9_973)?;
        let requests = fixture.backend.ranges.lock().len();
        let cached = file.read_at(9_981, 1)?;

        // Assert
        assert_eq!(bytes.as_ref(), &fixture.bytes[9..9_982]);
        assert_eq!(cached.as_ref(), &fixture.bytes[9_981..9_982]);
        assert_eq!(fixture.backend.ranges.lock().len(), requests);
        assert!(fixture
            .backend
            .ranges
            .lock()
            .iter()
            .all(|(start, end)| end - start <= 127));
        assert_eq!(
            std::fs::read_dir(fixture.directory.path().join("local"))?.count(),
            0
        );
        assert_eq!(fs.list_dir(&FsPath::new("wal"))?[0].name, name);
        Ok(())
    }

    #[test]
    fn should_verify_complete_wal_catalog_checksum_with_bounded_reads() -> MidgeResult<()> {
        // Arrange
        let fixture = Fixture::new()?;
        let crc = crc32c::crc32c(&fixture.bytes);

        // Act
        validate_wal_source(
            fixture.source.as_ref(),
            &fixture.path,
            fixture.bytes.len() as u64,
            crc,
            1_024,
        )?;
        let incorrect_crc = validate_wal_source(
            fixture.source.as_ref(),
            &fixture.path,
            fixture.bytes.len() as u64,
            crc ^ 1,
            1_024,
        );
        let incorrect_size = validate_wal_source(
            fixture.source.as_ref(),
            &fixture.path,
            fixture.bytes.len() as u64 + 1,
            crc,
            1_024,
        );

        // Assert
        assert!(matches!(incorrect_crc, Err(MidgeError::RecoveryFailed(_))));
        assert!(matches!(incorrect_size, Err(MidgeError::RecoveryFailed(_))));
        assert!(fixture
            .backend
            .ranges
            .lock()
            .iter()
            .all(|(start, end)| end - start <= 1_024));
        Ok(())
    }

    #[test]
    fn should_compare_local_aliases_to_pinned_cloud_wal_without_copying_segments() -> MidgeResult<()>
    {
        // Arrange
        let fixture = Fixture::new()?;
        let local = crate::io::RealFs::new(fixture.directory.path().join("aliases"))?;
        std::fs::write(
            fixture.directory.path().join("aliases/equal.wal"),
            &fixture.bytes,
        )?;
        let mut different = fixture.bytes.clone();
        *different.last_mut().expect("nonempty WAL") ^= 1;
        std::fs::write(
            fixture.directory.path().join("aliases/different.wal"),
            different,
        )?;
        let remote = (fixture.source.as_ref(), &fixture.path);

        // Act
        let equal = wal_sources_equal(remote, (&local, &FsPath::new("equal.wal")), 257)?;
        let divergent = wal_sources_equal(remote, (&local, &FsPath::new("different.wal")), 257)?;
        let missing = wal_sources_equal(remote, (&local, &FsPath::new("missing.wal")), 257);

        // Assert
        assert!(equal);
        assert!(!divergent);
        assert!(matches!(missing, Err(MidgeError::NotFound)));
        assert!(fixture
            .backend
            .ranges
            .lock()
            .iter()
            .all(|(start, end)| end - start <= 257));
        Ok(())
    }

    #[test]
    fn should_reject_unsupported_access_in_streaming_wal_view() -> MidgeResult<()> {
        // Arrange
        let fixture = Fixture::new()?;
        let mut fs = StreamingWalFs::new(1_024)?;
        let name = crate::wal::segment_file_name(41);
        fs.insert(
            name.clone(),
            Arc::clone(&fixture.source),
            fixture.path.clone(),
        )?;
        let path = FsPath::new(name.clone());
        let mut file = fs.open(&path, READ_ONLY)?;

        // Act
        let mutation = file.write_at(0, Bytes::from_static(b"invalid"));
        let remove = fs.remove_file(&path);
        let escape = fs.open(&FsPath::new(format!("../{name}")), READ_ONLY);
        let alias = fs.open(&FsPath::new("41.wal"), READ_ONLY);

        // Assert
        assert!(matches!(mutation, Err(FsError::Unsupported(_))));
        assert!(matches!(remove, Err(FsError::Unsupported(_))));
        assert!(matches!(escape, Err(FsError::NotFound(_))));
        assert!(matches!(alias, Err(FsError::NotFound(_))));
        drop(file);
        drop(escape);
        drop(alias);
        assert!(fs
            .insert(name, Arc::clone(&fixture.source), fixture.path.clone())
            .is_err());
        assert!(fs
            .insert(
                "wal/00000000000000000042.wal".into(),
                fixture.source,
                fixture.path
            )
            .is_err());
        assert!(StreamingWalFs::new(0).is_err());
        Ok(())
    }
}
