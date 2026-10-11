//! Explicit cleanup proof for SST build scratch owned by one factory.
//!
//! Scratch lives in the factory's output filesystem, so an injected root sees
//! every scratch byte, and its faults reach the build, like any other engine
//! write. Scratch carries no durability and is never synced.

use crate::common::{MidgeError, MidgeResult};
use crate::io::{File, Fs, FsError, FsPath, OpenMode, OpenOptions};
use bytes::Bytes;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(super) struct TrackedScratch {
    fs: Arc<dyn Fs>,
    path: FsPath,
    file: Option<Box<dyn File>>,
    len: u64,
    /// Whether this scratch still holds its count in `outstanding`.
    counted: bool,
    outstanding: Arc<AtomicUsize>,
}

impl TrackedScratch {
    /// Create scratch in `directory`, a location in `fs`, or at its root.
    pub(super) fn new(
        fs: Arc<dyn Fs>,
        outstanding: Arc<AtomicUsize>,
        directory: Option<&Path>,
    ) -> MidgeResult<Self> {
        let name = format!(".compaction-{}", uuid::Uuid::new_v4().simple());
        let path = super::fs_relative_sst_path(
            &fs,
            &directory.unwrap_or_else(|| Path::new("")).join(name),
        )?;
        if let Some(parent) = Path::new(&path.0)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs.create_dir_all(&FsPath::new(parent.to_string_lossy()))
                .map_err(FsError::into_midge)?;
        }
        // Count the scratch before creating it. A failed creation can leave a
        // file behind; only a confirmed removal establishes that none remain.
        outstanding.fetch_add(1, Ordering::AcqRel);
        let mut scratch = Self {
            fs,
            path,
            file: None,
            len: 0,
            counted: true,
            outstanding,
        };
        let file = scratch
            .fs
            .open_persistent_handle(
                &scratch.path,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: false,
                    create_new: true,
                    truncate: false,
                },
            )
            .map_err(FsError::into_midge)?;
        scratch.file = Some(file);
        Ok(scratch)
    }

    pub(super) fn append(&mut self, bytes: &[u8]) -> MidgeResult<()> {
        let file = self.file.as_mut().expect("scratch stays open until closed");
        file.write_at(self.len, Bytes::copy_from_slice(bytes))
            .map_err(FsError::into_midge)?;
        self.len = self
            .len
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| MidgeError::ResourceLimit("SST scratch size overflow".into()))?;
        Ok(())
    }

    /// Read the scratch back from its first byte.
    pub(super) fn reader(&self) -> ScratchReader<'_> {
        ScratchReader {
            file: self
                .file
                .as_deref()
                .expect("scratch stays open until closed"),
            len: self.len,
            position: 0,
        }
    }

    pub(super) fn close(mut self) -> MidgeResult<()> {
        self.cleanup()
    }

    fn cleanup(&mut self) -> MidgeResult<()> {
        if !self.counted {
            return Ok(());
        }
        // Close before unlinking: Windows cannot remove an open file.
        self.file = None;
        match self.fs.remove_file(&self.path) {
            Ok(()) | Err(FsError::NotFound(_)) => {
                self.counted = false;
                self.outstanding.fetch_sub(1, Ordering::AcqRel);
                Ok(())
            }
            Err(error) => Err(error.into_midge()),
        }
    }
}

impl Drop for TrackedScratch {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            tracing::warn!(%error, "retaining compaction disk admission because scratch cleanup failed");
        }
    }
}

/// Sequential reader over finished scratch.
pub(super) struct ScratchReader<'a> {
    file: &'a dyn File,
    len: u64,
    position: u64,
}

impl std::io::Read for ScratchReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = (self.len - self.position).min(buffer.len() as u64);
        if count == 0 {
            return Ok(0);
        }
        let bytes = self
            .file
            .read_at(self.position, count)
            .map_err(|error| std::io::Error::other(error.into_midge()))?;
        let read = bytes.len();
        if read as u64 != count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "SST scratch shrank while it was read back",
            ));
        }
        buffer[..read].copy_from_slice(&bytes);
        self.position += count;
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read as _;

    #[test]
    fn should_confirm_scratch_cleanup_only_after_owned_file_is_removed() -> MidgeResult<()> {
        for cleanup_fails in [false, true] {
            // Arrange
            let directory = tempfile::tempdir()?;
            let fs = crate::io::real::test_root(directory.path());
            let outstanding = Arc::new(AtomicUsize::new(0));
            let mut scratch = TrackedScratch::new(
                Arc::clone(&fs),
                Arc::clone(&outstanding),
                Some(Path::new("scratch")),
            )?;
            scratch.append(b"partially encoded blocks")?;
            let path = directory.path().join(&scratch.path.0);
            assert!(path.starts_with(directory.path().join("scratch")));
            if cleanup_fails {
                std::fs::remove_file(&path)?;
                std::fs::create_dir(&path)?;
            }
            // Act
            let result = scratch.close();
            // Assert
            assert_eq!(
                result.is_err(),
                cleanup_fails,
                "successful finalization must report failed scratch cleanup"
            );
            assert_eq!(
                outstanding.load(Ordering::Acquire),
                usize::from(cleanup_fails)
            );
            assert_eq!(path.exists(), cleanup_fails);
        }
        Ok(())
    }

    #[test]
    fn should_keep_scratch_inside_filesystem_when_root_is_in_memory() -> MidgeResult<()> {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let outstanding = Arc::new(AtomicUsize::new(0));
        let mut scratch = TrackedScratch::new(
            fs.clone(),
            Arc::clone(&outstanding),
            Some(Path::new("sst/.flush-staging")),
        )?;

        // Act
        scratch.append(b"first block,")?;
        scratch.append(b" second block")?;
        let mut read_back = Vec::new();
        scratch.reader().read_to_end(&mut read_back)?;
        let path = scratch.path.0.clone();
        let stored = fs.get_file(&path);
        scratch.close()?;

        // Assert
        assert_eq!(read_back, b"first block, second block");
        assert_eq!(stored.as_deref(), Some(&b"first block, second block"[..]));
        assert!(path.starts_with("sst/.flush-staging/.compaction-"));
        assert_eq!(fs.get_file(&path), None);
        assert_eq!(outstanding.load(Ordering::Acquire), 0);
        Ok(())
    }
}
