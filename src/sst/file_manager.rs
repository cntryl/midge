use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// File manager for tracking and controlling database file lifecycle.
///
/// Provides centralized management of:
/// - File handle pooling
/// - Deletion tracking (pending deletes)
/// - Disk space monitoring
/// - File quotas
#[derive(Debug, Clone)]
pub struct FileManager {
    inner: Arc<Mutex<FileManagerState>>,
}

#[derive(Debug)]
struct FileManagerState {
    /// Files pending deletion (marked but not yet deleted)
    pending_deletes: HashMap<PathBuf, PendingDelete>,
    /// Maximum total file size in bytes (0 = unlimited)
    max_total_bytes: u64,
    /// Current total file size
    current_total_bytes: u64,
    /// Maximum number of open files (0 = unlimited)
    max_open_files: usize,
    /// Currently open file count (tracked manually)
    open_file_count: usize,
}

#[derive(Debug, Clone)]
struct PendingDelete {
    size_bytes: u64,
    marked_at: std::time::Instant,
}

impl FileManager {
    /// Create a new file manager
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FileManagerState {
                pending_deletes: HashMap::new(),
                max_total_bytes: 0,
                current_total_bytes: 0,
                max_open_files: 0,
                open_file_count: 0,
            })),
        }
    }

    /// Create a file manager with limits
    pub fn with_limits(max_total_bytes: u64, max_open_files: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FileManagerState {
                pending_deletes: HashMap::new(),
                max_total_bytes,
                current_total_bytes: 0,
                max_open_files,
                open_file_count: 0,
            })),
        }
    }

    /// Register a new file with the manager
    ///
    /// Returns Ok(()) if file can be created within quota, Err if quota exceeded
    pub fn register_file(&self, _path: &Path, size_bytes: u64) -> Result<(), FileManagerError> {
        let mut state = self.inner.lock();

        // Check quota
        if state.max_total_bytes > 0 {
            let new_total = state.current_total_bytes + size_bytes;
            if new_total > state.max_total_bytes {
                return Err(FileManagerError::QuotaExceeded {
                    requested: size_bytes,
                    available: state
                        .max_total_bytes
                        .saturating_sub(state.current_total_bytes),
                });
            }
        }

        state.current_total_bytes += size_bytes;
        Ok(())
    }

    /// Unregister a file (when deleted or moved)
    pub fn unregister_file(&self, _path: &Path, size_bytes: u64) {
        let mut state = self.inner.lock();
        state.current_total_bytes = state.current_total_bytes.saturating_sub(size_bytes);
        state.pending_deletes.remove(_path);
    }

    /// Mark a file for deletion (but don't delete it yet)
    ///
    /// This is useful for tracking files that should be deleted but might still
    /// be in use by readers (e.g., SST files being compacted away)
    pub fn mark_for_deletion(&self, path: PathBuf, size_bytes: u64) {
        let mut state = self.inner.lock();
        state.pending_deletes.insert(
            path,
            PendingDelete {
                size_bytes,
                marked_at: std::time::Instant::now(),
            },
        );
    }

    /// Execute pending deletions that are older than the grace period
    ///
    /// # Arguments
    /// * `grace_period` - Minimum age before deletion
    ///
    /// # Returns
    /// Number of files deleted
    pub fn execute_pending_deletions(&self, grace_period: std::time::Duration) -> usize {
        let mut state = self.inner.lock();
        let now = std::time::Instant::now();

        let mut to_delete = Vec::new();

        for (path, pending) in &state.pending_deletes {
            if now.duration_since(pending.marked_at) >= grace_period {
                to_delete.push((path.clone(), pending.size_bytes));
            }
        }

        let mut deleted_count = 0;

        for (path, size_bytes) in to_delete {
            if std::fs::remove_file(&path).is_ok() {
                state.pending_deletes.remove(&path);
                state.current_total_bytes = state.current_total_bytes.saturating_sub(size_bytes);
                deleted_count += 1;
            }
        }

        deleted_count
    }

    /// Get list of pending deletions
    pub fn pending_deletions(&self) -> Vec<PathBuf> {
        let state = self.inner.lock();
        state.pending_deletes.keys().cloned().collect()
    }

    /// Track opening a file
    ///
    /// Returns Ok(()) if within limit, Err if too many files open
    pub fn track_open(&self) -> Result<FileHandle, FileManagerError> {
        let mut state = self.inner.lock();

        if state.max_open_files > 0 && state.open_file_count >= state.max_open_files {
            return Err(FileManagerError::TooManyOpenFiles {
                limit: state.max_open_files,
                current: state.open_file_count,
            });
        }

        state.open_file_count += 1;

        Ok(FileHandle {
            manager: self.clone(),
        })
    }

    /// Track closing a file (called automatically by FileHandle drop)
    fn track_close(&self) {
        let mut state = self.inner.lock();
        state.open_file_count = state.open_file_count.saturating_sub(1);
    }

    /// Get current statistics
    pub fn stats(&self) -> FileManagerStats {
        let state = self.inner.lock();
        FileManagerStats {
            current_total_bytes: state.current_total_bytes,
            max_total_bytes: state.max_total_bytes,
            pending_delete_count: state.pending_deletes.len(),
            open_file_count: state.open_file_count,
            max_open_files: state.max_open_files,
        }
    }

    /// Set maximum total file size
    pub fn set_max_total_bytes(&self, max_bytes: u64) {
        let mut state = self.inner.lock();
        state.max_total_bytes = max_bytes;
    }

    /// Set maximum open files
    pub fn set_max_open_files(&self, max_files: usize) {
        let mut state = self.inner.lock();
        state.max_open_files = max_files;
    }
}

impl Default for FileManager {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII handle for tracking file opens/closes
#[derive(Debug)]
pub struct FileHandle {
    manager: FileManager,
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        self.manager.track_close();
    }
}

/// File manager statistics
#[derive(Debug, Clone, Copy)]
pub struct FileManagerStats {
    pub current_total_bytes: u64,
    pub max_total_bytes: u64,
    pub pending_delete_count: usize,
    pub open_file_count: usize,
    pub max_open_files: usize,
}

impl FileManagerStats {
    /// Calculate space utilization percentage (0.0 to 1.0)
    #[inline]
    pub fn space_utilization(&self) -> f64 {
        if self.max_total_bytes == 0 {
            0.0
        } else {
            self.current_total_bytes as f64 / self.max_total_bytes as f64
        }
    }

    /// Calculate open file utilization percentage (0.0 to 1.0)
    #[inline]
    pub fn file_handle_utilization(&self) -> f64 {
        if self.max_open_files == 0 {
            0.0
        } else {
            self.open_file_count as f64 / self.max_open_files as f64
        }
    }

    /// Format statistics as human-readable string
    pub fn format(&self) -> String {
        let space_pct = if self.max_total_bytes > 0 {
            format!("{:.1}%", self.space_utilization() * 100.0)
        } else {
            "unlimited".to_string()
        };

        let files_pct = if self.max_open_files > 0 {
            format!("{:.1}%", self.file_handle_utilization() * 100.0)
        } else {
            "unlimited".to_string()
        };

        format!(
            "File Manager Stats:\n\
             Space: {:.2} MB / {} MB ({})\n\
             Open Files: {} / {} ({})\n\
             Pending Deletes: {}",
            self.current_total_bytes as f64 / 1_048_576.0,
            if self.max_total_bytes > 0 {
                format!("{:.2}", self.max_total_bytes as f64 / 1_048_576.0)
            } else {
                "∞".to_string()
            },
            space_pct,
            self.open_file_count,
            if self.max_open_files > 0 {
                self.max_open_files.to_string()
            } else {
                "∞".to_string()
            },
            files_pct,
            self.pending_delete_count
        )
    }
}

/// File manager errors
#[derive(Debug, Clone)]
pub enum FileManagerError {
    QuotaExceeded { requested: u64, available: u64 },
    TooManyOpenFiles { limit: usize, current: usize },
}

impl std::fmt::Display for FileManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileManagerError::QuotaExceeded {
                requested,
                available,
            } => {
                write!(
                    f,
                    "File quota exceeded: requested {} bytes, only {} bytes available",
                    requested, available
                )
            }
            FileManagerError::TooManyOpenFiles { limit, current } => {
                write!(
                    f,
                    "Too many open files: limit is {}, currently have {}",
                    limit, current
                )
            }
        }
    }
}

impl std::error::Error for FileManagerError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn should_track_file_sizes_given_registrations_when_register_and_unregister() {
        // Arrange
        let fm = FileManager::new();

        // Act
        fm.register_file(Path::new("file1.sst"), 1000).unwrap();
        fm.register_file(Path::new("file2.sst"), 2000).unwrap();
        let stats = fm.stats();
        fm.unregister_file(Path::new("file1.sst"), 1000);
        let stats2 = fm.stats();

        // Assert
        assert_eq!(stats.current_total_bytes, 3000);
        assert_eq!(stats2.current_total_bytes, 2000);
    }

    #[test]
    fn should_enforce_quota_given_disk_limit_when_registering_files() {
        // Arrange
        let fm = FileManager::with_limits(5000, 0);

        // Act
        fm.register_file(Path::new("file1.sst"), 2000).unwrap();
        fm.register_file(Path::new("file2.sst"), 2000).unwrap();
        let result = fm.register_file(Path::new("file3.sst"), 2000);

        // Assert
        assert!(result.is_err());
        if let Err(FileManagerError::QuotaExceeded {
            requested,
            available,
        }) = result
        {
            assert_eq!(requested, 2000);
            assert_eq!(available, 1000);
        }
    }

    #[test]
    fn should_delay_deletion_given_pending_file_when_grace_period_not_expired() {
        // Arrange
        let fm = FileManager::new();

        // Act
        fm.mark_for_deletion(PathBuf::from("old1.sst"), 1000);
        fm.mark_for_deletion(PathBuf::from("old2.sst"), 2000);

        // Assert
        let stats = fm.stats();
        assert_eq!(stats.pending_delete_count, 2);
        let pending = fm.pending_deletions();
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn should_respect_grace_period_given_pending_deletion_when_cleanup_called() {
        // Arrange
        let fm = FileManager::new();
        let dir = tempfile::tempdir().unwrap();
        let file1 = dir.path().join("file1.txt");
        let file2 = dir.path().join("file2.txt");
        std::fs::write(&file1, b"test1").unwrap();
        std::fs::write(&file2, b"test2").unwrap();
        fm.mark_for_deletion(file1.clone(), 5);
        fm.mark_for_deletion(file2.clone(), 5);

        // Initial deletion attempt (before grace period)
        let deleted = fm.execute_pending_deletions(Duration::from_millis(100));
        assert_eq!(deleted, 0);
        assert!(file1.exists());
        assert!(file2.exists());

        // Act - Wait for grace period and execute again
        std::thread::sleep(Duration::from_millis(150));
        let deleted = fm.execute_pending_deletions(Duration::from_millis(100));

        // Assert
        assert_eq!(deleted, 2);
        assert!(!file1.exists());
        assert!(!file2.exists());
    }

    #[test]
    fn should_track_single_open_file_given_open_when_counted() {
        // Arrange
        let fm = FileManager::with_limits(0, 3);

        // Act
        let _h1 = fm.track_open().unwrap();

        // Assert
        let stats = fm.stats();
        assert_eq!(stats.open_file_count, 1);
    }

    #[test]
    fn should_track_multiple_open_files_given_multiple_opens_when_counted() {
        // Arrange
        let fm = FileManager::with_limits(0, 3);
        let _h1 = fm.track_open().unwrap();

        // Act
        let _h2 = fm.track_open().unwrap();
        let _h3 = fm.track_open().unwrap();

        // Assert
        let stats = fm.stats();
        assert_eq!(stats.open_file_count, 3);
    }

    #[test]
    fn should_fail_open_given_at_limit_when_track_open_called() {
        // Arrange
        let fm = FileManager::with_limits(0, 3);
        let _h1 = fm.track_open().unwrap();
        let _h2 = fm.track_open().unwrap();
        let _h3 = fm.track_open().unwrap();

        // Act
        let result = fm.track_open();

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_decrease_count_given_handle_dropped_when_counted() {
        // Arrange
        let fm = FileManager::with_limits(0, 3);
        let h1 = fm.track_open().unwrap();
        let _h2 = fm.track_open().unwrap();
        let _h3 = fm.track_open().unwrap();

        // Act
        drop(h1);

        // Assert
        let stats = fm.stats();
        assert_eq!(stats.open_file_count, 2);
    }

    #[test]
    fn should_allow_new_open_given_handle_dropped_when_at_limit() {
        // Arrange
        let fm = FileManager::with_limits(0, 3);
        let h1 = fm.track_open().unwrap();
        let _h2 = fm.track_open().unwrap();
        let _h3 = fm.track_open().unwrap();
        drop(h1); // Free up a slot

        // Act
        let _h4 = fm.track_open().unwrap();

        // Assert
        let stats = fm.stats();
        assert_eq!(stats.open_file_count, 3);
    }

    #[test]
    fn should_calculate_utilization_given_limits_when_stats_requested() {
        // Arrange
        let fm = FileManager::with_limits(10000, 10); // 10000 bytes, 10 file handles

        // Act
        fm.register_file(Path::new("file1.sst"), 5000).unwrap();
        let _h1 = fm.track_open().unwrap();
        let _h2 = fm.track_open().unwrap();

        // Assert
        let stats = fm.stats();
        assert_eq!(stats.space_utilization(), 0.5); // 5000 / 10000
        assert_eq!(stats.file_handle_utilization(), 0.2); // 2 / 10
    }

    #[test]
    fn should_apply_new_limits_given_set_limits_when_changed() {
        // Arrange
        let fm = FileManager::new();
        fm.register_file(Path::new("file1.sst"), 3000).unwrap();

        // Set quota lower than current usage but allow some room
        fm.set_max_total_bytes(5000);

        // Register within quota (setup)
        fm.register_file(Path::new("file2.sst"), 1000).unwrap();

        // Act - Try to register beyond new limit
        let result = fm.register_file(Path::new("file3.sst"), 2000);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_mark_file_for_deletion() {
        use std::path::PathBuf;

        // Arrange
        let fm = FileManager::new();
        let path = PathBuf::from("test.sst");
        fm.register_file(&path, 1024).unwrap();

        // Act
        fm.mark_for_deletion(path.clone(), 1024);

        // Assert - Should track pending deletion
        assert_eq!(fm.stats().pending_delete_count, 1);
        assert_eq!(fm.pending_deletions().len(), 1);
    }

    #[test]
    fn should_execute_pending_deletions_after_grace_period() {
        use std::fs::File;
        use std::io::Write;
        use std::time::Duration;

        // Arrange
        let tmp = tempfile::tempdir().unwrap();
        let fm = FileManager::new();
        let path = tmp.path().join("test.sst");
        let mut file = File::create(&path).unwrap();
        file.write_all(b"test data").unwrap();
        drop(file);
        fm.register_file(&path, 1024).unwrap();
        fm.mark_for_deletion(path.clone(), 1024);
        assert_eq!(fm.stats().pending_delete_count, 1);

        // Act - Execute with zero grace period (delete immediately)
        std::thread::sleep(Duration::from_millis(10));
        let deleted = fm.execute_pending_deletions(Duration::from_millis(0));

        // Assert
        assert_eq!(deleted, 1);
        assert_eq!(fm.stats().pending_delete_count, 0);
        assert!(!path.exists());
    }

    #[test]
    fn should_not_delete_files_before_grace_period() {
        use std::fs::File;
        use std::io::Write;
        use std::time::Duration;

        // Arrange
        let tmp = tempfile::tempdir().unwrap();
        let fm = FileManager::new();
        let path = tmp.path().join("test.sst");
        let mut file = File::create(&path).unwrap();
        file.write_all(b"test data").unwrap();
        drop(file);
        fm.mark_for_deletion(path.clone(), 1024);

        // Act - Try to delete with a far-future grace period
        let deleted = fm.execute_pending_deletions(Duration::from_secs(3600));

        // Assert
        assert_eq!(deleted, 0);
        assert_eq!(fm.stats().pending_delete_count, 1);
        assert!(path.exists());
    }

    #[test]
    fn should_handle_concurrent_operations() {
        use std::path::PathBuf;
        use std::sync::Arc;
        use std::thread;

        // Arrange
        let fm = Arc::new(FileManager::new());
        let mut handles = vec![];

        // Act - Spawn threads to register files concurrently
        for i in 0..5 {
            let fm_clone = fm.clone();
            handles.push(thread::spawn(move || {
                let path = PathBuf::from(format!("file{}.sst", i));
                fm_clone.register_file(&path, 1000).unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        assert_eq!(fm.stats().current_total_bytes, 5000);
    }
}
