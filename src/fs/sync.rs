//! Platform-specific filesystem sync operations
//!
//! Provides optimized data-only sync that's faster than full fsync on Unix.

use std::io;

/// Platform-specific data-only sync (faster than fsync, skips metadata)
///
/// On Unix: uses `fdatasync()` which syncs data but not metadata (atime, mtime, etc.)
/// On Windows: uses `FlushFileBuffers()` (equivalent to fsync, no fdatasync available)
///
/// Performance impact:
/// - Unix: ~20-30% faster than fsync() for WAL/SST writes
/// - Windows: Same as fsync() (no performance difference)
///
/// # Examples
///
/// ```rust,no_run
/// use std::fs::File;
/// # use cntryl_midge::fs::sync_data_only;
///
/// let file = File::create("data.db").unwrap();
/// // ... write data ...
/// sync_data_only(&file).unwrap(); // Fast data-only sync
/// ```
#[inline]
pub fn sync_data_only(file: &std::fs::File) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        let result = unsafe { libc::fdatasync(fd) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(unix))]
    {
        // Windows and other platforms: fall back to sync_all()
        file.sync_all()
    }
}

/// Ensure the parent directory entry for `path` is durable.
///
/// This opens the parent directory (when possible) and calls `sync_all()` on it
/// so that the directory entry for a recently-created or renamed file is
/// persisted to disk. On platforms where opening the directory is not
/// permitted, this function will log a warning and return Ok(()) so callers
/// can proceed without failing tests.
pub fn sync_parent(path: &std::path::Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        // Try to open parent directory for syncing. This succeeds on Unix.
        match std::fs::OpenOptions::new().read(true).open(parent) {
            Ok(f) => f.sync_all(),
            Err(e) => {
                // Opening directories may be restricted on some platforms (Windows).
                // Log and continue — this is a best-effort durability step.
                tracing::warn!(
                    "failed to open parent dir {} for sync: {}",
                    parent.display(),
                    e
                );
                Ok(())
            }
        }
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn should_sync_data_without_error() {
        // Arrange
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.dat");
        let mut file = File::create(&path).unwrap();

        // Act
        file.write_all(b"test data").unwrap();
        let result = sync_data_only(&file);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_persist_data_after_sync() {
        // Arrange
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("test.dat");
        let data = b"persistent data";

        // Act
        {
            let mut file = File::create(&path).unwrap();
            file.write_all(data).unwrap();
            sync_data_only(&file).unwrap();
        }

        // Assert
        let read_data = std::fs::read(&path).unwrap();
        assert_eq!(read_data, data);
    }
}
