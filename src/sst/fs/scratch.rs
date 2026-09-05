//! Explicit cleanup proof for compaction scratch owned by one SST factory.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(super) struct TrackedScratch {
    file: Option<tempfile::NamedTempFile>,
    outstanding: Arc<AtomicUsize>,
}

impl TrackedScratch {
    pub(super) fn new(
        outstanding: Arc<AtomicUsize>,
        directory: Option<&Path>,
    ) -> std::io::Result<Self> {
        if let Some(directory) = directory {
            std::fs::create_dir_all(directory)?;
        }
        // A failed creation can include an unsuccessful internal cleanup.
        // Only a successful close below establishes absence of all its bytes.
        outstanding.fetch_add(1, Ordering::AcqRel);
        let file = match directory {
            Some(directory) => tempfile::Builder::new()
                .prefix(".compaction-")
                .tempfile_in(directory)?,
            None => tempfile::NamedTempFile::new()?,
        };
        Ok(Self {
            file: Some(file),
            outstanding,
        })
    }

    pub(super) fn as_file_mut(&mut self) -> &mut std::fs::File {
        self.file
            .as_mut()
            .expect("scratch remains owned")
            .as_file_mut()
    }

    pub(super) fn reopen(&self) -> std::io::Result<std::fs::File> {
        self.file.as_ref().expect("scratch remains owned").reopen()
    }

    pub(super) fn close(mut self) -> std::io::Result<()> {
        self.cleanup()
    }

    fn cleanup(&mut self) -> std::io::Result<()> {
        if let Some(file) = self.file.take() {
            file.close()?;
            self.outstanding.fetch_sub(1, Ordering::AcqRel);
        }
        Ok(())
    }
}

impl Drop for TrackedScratch {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            tracing::warn!(%error, "retaining compaction disk admission because scratch cleanup failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn should_confirm_scratch_cleanup_only_after_owned_file_is_removed() -> std::io::Result<()> {
        for cleanup_fails in [false, true] {
            // Arrange
            let directory = tempfile::tempdir()?;
            let outstanding = Arc::new(AtomicUsize::new(0));
            let mut scratch =
                TrackedScratch::new(Arc::clone(&outstanding), Some(directory.path()))?;
            scratch
                .as_file_mut()
                .write_all(b"partially encoded blocks")?;
            let path = scratch.file.as_ref().expect("scratch").path().to_path_buf();
            assert!(path.starts_with(directory.path()));
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
}
