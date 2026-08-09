//! Atomic staging helpers for filesystem-backed persistence.
//!
//! These helpers centralize the common pattern of:
//! write -> file fsync -> atomic rename -> parent-directory fsync -> best-effort temp cleanup.
//!
//! They are intentionally small and generic so metadata, intent logs, leader
//! records, and cloud recovery bootstrap writes all follow the same lifecycle.

use crate::io::traits::{Durability, Fs, FsPath, OpenMode, OpenOptions};
use bytes::Bytes;
use std::io::Read;
use std::sync::Arc;

fn cleanup_temp_file(fs: &Arc<dyn Fs>, temp_path: &FsPath) {
    if let Err(error) = fs.remove_file(temp_path) {
        tracing::debug!(
            path = ?temp_path,
            error = ?error,
            "staging temp cleanup skipped or failed"
        );
    }
}

/// Stage a byte buffer to `temp_path`, sync it, rename it into `target_path`,
/// sync the target's parent directory, and remove the temp file on failure.
pub(crate) fn stage_bytes<E, M>(
    fs: &Arc<dyn Fs>,
    temp_path: &FsPath,
    target_path: &FsPath,
    data: &[u8],
    map_error: M,
) -> Result<(), E>
where
    M: Fn(String) -> E,
{
    stage_bytes_with_hook(fs, temp_path, target_path, data, || Ok(()), map_error)
}

/// Stage a byte stream without reconstructing it into one large allocation.
///
/// The stream is copied into a temporary file in bounded chunks, followed by
/// the same file-sync, atomic-rename, and parent-directory-sync sequence used
/// by [`stage_bytes`].
pub(crate) fn stage_stream<E, M>(
    fs: &Arc<dyn Fs>,
    temp_path: &FsPath,
    target_path: &FsPath,
    source: &mut dyn Read,
    map_error: M,
) -> Result<u64, E>
where
    M: Fn(String) -> E,
{
    let result = (|| {
        let mut file = fs
            .open(
                temp_path,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )
            .map_err(|error| {
                map_error(format!(
                    "failed to open staging file {temp_path:?}: {error:?}"
                ))
            })?;

        let mut offset = 0_u64;
        let mut chunk = vec![0_u8; 64 * 1024];
        loop {
            let count = source.read(&mut chunk).map_err(|error| {
                map_error(format!(
                    "failed to read staging source for {target_path:?}: {error}"
                ))
            })?;
            if count == 0 {
                break;
            }
            file.write_at(offset, Bytes::copy_from_slice(&chunk[..count]))
                .map_err(|error| {
                    map_error(format!(
                        "failed to write staging file {temp_path:?}: {error:?}"
                    ))
                })?;
            offset = offset.saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        }

        file.sync(Durability::Durable).map_err(|error| {
            map_error(format!(
                "failed to sync staging file {temp_path:?}: {error:?}"
            ))
        })?;
        drop(file);

        fs.rename_atomic(temp_path, target_path).map_err(|error| {
            map_error(format!(
                "failed to rename staging file {temp_path:?} -> {target_path:?}: {error:?}"
            ))
        })?;

        let parent_path = std::path::Path::new(&target_path.0)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map_or_else(
                || FsPath::new("."),
                |parent| FsPath::new(parent.to_string_lossy()),
            );
        fs.sync_dir(&parent_path, Durability::Durable)
            .map_err(|error| {
                map_error(format!(
                    "failed to sync staging target directory {parent_path:?}: {error:?}"
                ))
            })?;

        Ok(offset)
    })();

    if result.is_err() {
        cleanup_temp_file(fs, temp_path);
    }

    result
}

/// Stage a byte buffer with a hook that runs after the temp file is synced and
/// before the atomic rename.
pub(crate) fn stage_bytes_with_hook<E, F, M>(
    fs: &Arc<dyn Fs>,
    temp_path: &FsPath,
    target_path: &FsPath,
    data: &[u8],
    before_rename: F,
    map_error: M,
) -> Result<(), E>
where
    F: FnOnce() -> Result<(), E>,
    M: Fn(String) -> E,
{
    let result = (|| {
        let mut file = fs
            .open(
                temp_path,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )
            .map_err(|error| {
                map_error(format!(
                    "failed to open staging file {temp_path:?}: {error:?}"
                ))
            })?;

        file.write_at(0, Bytes::copy_from_slice(data))
            .map_err(|error| {
                map_error(format!(
                    "failed to write staging file {temp_path:?}: {error:?}"
                ))
            })?;
        file.sync(Durability::Durable).map_err(|error| {
            map_error(format!(
                "failed to sync staging file {temp_path:?}: {error:?}"
            ))
        })?;
        drop(file);

        before_rename()?;

        fs.rename_atomic(temp_path, target_path).map_err(|error| {
            map_error(format!(
                "failed to rename staging file {temp_path:?} -> {target_path:?}: {error:?}"
            ))
        })?;

        let parent_path = std::path::Path::new(&target_path.0)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map_or_else(
                || FsPath::new("."),
                |parent| FsPath::new(parent.to_string_lossy()),
            );
        fs.sync_dir(&parent_path, Durability::Durable)
            .map_err(|error| {
                map_error(format!(
                    "failed to sync staging target directory {parent_path:?}: {error:?}"
                ))
            })?;

        Ok(())
    })();

    if result.is_err() {
        cleanup_temp_file(fs, temp_path);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::traits::{DirEntry, File, FileCaps, FsError, FsResult, Metadata};
    use parking_lot::Mutex;

    struct RecordingFile<'a> {
        inner: Box<dyn File + 'a>,
        events: Arc<Mutex<Vec<String>>>,
    }

    impl File for RecordingFile<'_> {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
            self.inner.read_at(offset, len)
        }

        fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
            self.inner.write_at(offset, data)?;
            self.events.lock().push("write".to_string());
            Ok(())
        }

        fn append(&mut self, data: Bytes) -> FsResult<u64> {
            self.inner.append(data)
        }

        fn len(&self) -> FsResult<u64> {
            self.inner.len()
        }

        fn sync(&mut self, durability: Durability) -> FsResult<()> {
            self.inner.sync(durability)?;
            self.events.lock().push("file sync".to_string());
            Ok(())
        }

        fn close(self: Box<Self>) -> FsResult<()> {
            self.inner.close()
        }

        fn caps(&self) -> FileCaps {
            self.inner.caps()
        }
    }

    struct RecordingFs {
        inner: crate::io::MockFs,
        events: Arc<Mutex<Vec<String>>>,
        fail_directory_sync: bool,
    }

    impl RecordingFs {
        fn new(fail_directory_sync: bool) -> Self {
            Self {
                inner: crate::io::MockFs::new(),
                events: Arc::new(Mutex::new(Vec::new())),
                fail_directory_sync,
            }
        }
    }

    impl Fs for RecordingFs {
        fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            Ok(Box::new(RecordingFile {
                inner: self.inner.open(path, options)?,
                events: Arc::clone(&self.events),
            }))
        }

        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_file(path)
        }

        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.inner.metadata(path)
        }

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.create_dir_all(path)
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.inner.list_dir(path)
        }

        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_dir_all(path)
        }

        fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
            self.events
                .lock()
                .push(format!("directory sync {}", path.0));
            if self.fail_directory_sync {
                return Err(FsError::Unavailable(
                    "injected directory sync failure".to_string(),
                ));
            }
            self.inner.sync_dir(path, durability)
        }

        fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
            self.inner.rename_atomic(from, to)?;
            self.events.lock().push("rename".to_string());
            Ok(())
        }
    }

    #[test]
    fn should_sync_parent_directory_after_atomic_rename() -> Result<(), String> {
        // Arrange
        let fs = Arc::new(RecordingFs::new(false));
        let events = Arc::clone(&fs.events);
        let fs: Arc<dyn Fs> = fs;

        // Act
        stage_bytes(
            &fs,
            &FsPath::new("metadata/manifest.json.tmp"),
            &FsPath::new("metadata/manifest.json"),
            b"manifest",
            |message| message,
        )?;

        // Assert
        assert_eq!(
            *events.lock(),
            ["write", "file sync", "rename", "directory sync metadata"]
        );
        Ok(())
    }

    #[test]
    fn should_sync_current_directory_for_root_level_target() -> Result<(), String> {
        // Arrange
        let fs = Arc::new(RecordingFs::new(false));
        let events = Arc::clone(&fs.events);
        let fs: Arc<dyn Fs> = fs;

        // Act
        stage_bytes(
            &fs,
            &FsPath::new("FORMAT.tmp"),
            &FsPath::new("FORMAT"),
            b"2",
            |message| message,
        )?;

        // Assert
        assert_eq!(
            events.lock().last().map(String::as_str),
            Some("directory sync .")
        );
        Ok(())
    }

    #[test]
    fn should_preserve_target_atomically_given_parent_directory_sync_failure_when_publishing() {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(RecordingFs::new(true));

        // Act
        let result = stage_bytes(
            &fs,
            &FsPath::new("manifest.json.tmp"),
            &FsPath::new("manifest.json"),
            b"manifest",
            |message| message,
        );

        // Assert
        assert!(result
            .expect_err("directory sync failure must be returned")
            .contains("injected directory sync failure"));
    }

    #[test]
    fn should_stream_then_sync_parent_directory_after_atomic_rename() -> Result<(), String> {
        // Arrange
        let fs = Arc::new(RecordingFs::new(false));
        let events = Arc::clone(&fs.events);
        let fs: Arc<dyn Fs> = fs;
        let mut source = std::io::Cursor::new(vec![b'x'; 128 * 1024 + 7]);

        // Act
        let written = stage_stream(
            &fs,
            &FsPath::new("sst/output.sst.tmp"),
            &FsPath::new("sst/output.sst"),
            &mut source,
            |message| message,
        )?;

        // Assert
        assert_eq!(written, 128 * 1024 + 7);
        assert_eq!(
            *events.lock(),
            [
                "write",
                "write",
                "write",
                "file sync",
                "rename",
                "directory sync sst"
            ]
        );
        Ok(())
    }

    #[test]
    fn should_return_error_when_stream_parent_directory_sync_fails() {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(RecordingFs::new(true));
        let mut source = std::io::Cursor::new(b"sst-data");

        // Act
        let result = stage_stream(
            &fs,
            &FsPath::new("sst/output.sst.tmp"),
            &FsPath::new("sst/output.sst"),
            &mut source,
            |message| message,
        );

        // Assert
        assert!(result
            .expect_err("directory sync failure must be returned")
            .contains("injected directory sync failure"));
    }

    #[cfg(unix)]
    #[test]
    fn should_reject_rename_through_symlinked_parent_given_staging_filesystem_when_renaming() {
        // Arrange
        let root = tempfile::tempdir().expect("create rooted filesystem directory");
        let outside = tempfile::tempdir().expect("create outside directory");
        std::fs::write(outside.path().join("manifest.json"), b"outside-original")
            .expect("seed outside target");
        std::os::unix::fs::symlink(outside.path(), root.path().join("linked"))
            .expect("create symlinked target parent");
        let fs: Arc<dyn Fs> =
            Arc::new(crate::io::RealFs::new(root.path()).expect("create rooted real filesystem"));

        // Act
        let result = stage_bytes(
            &fs,
            &FsPath::new("manifest.json.tmp"),
            &FsPath::new("linked/manifest.json"),
            b"replacement",
            |message| message,
        );

        // Assert
        assert!(result
            .expect_err("symlinked rename parent must be rejected")
            .contains("symlink"));
        assert_eq!(
            std::fs::read(outside.path().join("manifest.json"))
                .expect("read retained outside target"),
            b"outside-original"
        );
        assert!(!root.path().join("manifest.json.tmp").exists());
    }
}
