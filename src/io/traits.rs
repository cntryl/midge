//! Base filesystem abstraction traits
//!
//! Domain-agnostic synchronous file I/O with platform-optimizable vectorized operations.
//! This is the foundation for all filesystem interactions in Midge (SST, WAL, etc.).
//!
//! Designed for:
//! - Fast, blocking synchronous I/O
//! - Vectorized operations (`readv_at`, `writev_at`, `appendv`) for bulk efficiency
//! - Platform-specific optimizations (preadv/pwritev, direct I/O, etc.)
//! - Explicit durability control (fsync vs unsafe)
//! - Swappable real and mock implementations

use bytes::Bytes;
use std::fmt;
use std::io::{IoSlice, IoSliceMut};
use std::ops::{BitOr, BitOrAssign};
use thiserror::Error;

/// Filesystem errors
#[derive(Error, Debug)]
pub enum FsError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("corruption: {0}")]
    Corruption(String),
    #[error("io: {0}")]
    Io(String),
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type FsResult<T> = Result<T, FsError>;

/// Durability semantics for filesystem operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// No durability guarantee - fast but may be lost on crash
    Unsafe,
    /// Durable - data synced to storage device (fsync/similar)
    Durable,
}

/// Portable path type - avoids `std::path` pollution in abstraction
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FsPath(pub String);

impl FsPath {
    pub fn new(s: impl Into<String>) -> Self {
        FsPath(s.into())
    }
}

impl fmt::Display for FsPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// File open mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    ReadOnly,
    ReadWrite,
}

/// Options for file operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    pub mode: OpenMode,
    pub create: bool,     // Create if not exists
    pub create_new: bool, // Fail if exists
    pub truncate: bool,   // Truncate if exists
}

/// Range specification for read operations
#[derive(Debug, Clone, Copy)]
pub struct ReadRange {
    pub offset: u64,
    pub len: u32,
}

/// File capability flags - for implementations to advertise support
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileCaps(u32);

impl FileCaps {
    pub const READV_AT: FileCaps = FileCaps(1 << 0); // Vectored read
    pub const WRITEV_AT: FileCaps = FileCaps(1 << 1); // Vectored write
    pub const APPENDV: FileCaps = FileCaps(1 << 2); // Vectored append
    pub const READ_RANGES: FileCaps = FileCaps(1 << 3); // Multi-range read

    #[must_use]
    pub const fn empty() -> Self {
        FileCaps(0)
    }

    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }
}

impl BitOr for FileCaps {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        FileCaps(self.0 | rhs.0)
    }
}

impl BitOrAssign for FileCaps {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Metadata for a file or directory
#[derive(Debug, Clone)]
pub struct Metadata {
    pub len: u64,
}

/// Directory entry
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

/// Individual file handle with read/write operations
pub trait File: Send {
    /// Read at specific offset
    ///
    /// # Errors
    ///
    /// Returns an error when the read cannot be satisfied by the backing
    /// filesystem or storage backend.
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes>;

    /// Write at specific offset
    ///
    /// # Errors
    ///
    /// Returns an error when the write cannot be completed by the backing
    /// filesystem or storage backend.
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()>;

    /// Truncate the file to an exact length.
    ///
    /// Writers use this to roll back a positional append that may have written
    /// a partial durability frame before returning an error.
    ///
    /// # Errors
    ///
    /// Returns an error when the backing file cannot be truncated or does not
    /// support truncation.
    fn truncate(&mut self, len: u64) -> FsResult<()> {
        Err(FsError::Unsupported(format!(
            "file handle does not support truncation to {len} bytes"
        )))
    }

    /// Append to end of file, return starting offset
    ///
    /// # Errors
    ///
    /// Returns an error when the append cannot be completed by the backing
    /// filesystem or storage backend.
    fn append(&mut self, data: Bytes) -> FsResult<u64>;

    /// Get file length
    ///
    /// # Errors
    ///
    /// Returns an error when the file length cannot be queried.
    fn len(&self) -> FsResult<u64>;

    /// Check if file is empty
    ///
    /// # Errors
    ///
    /// Returns an error when the file length cannot be queried.
    fn is_empty(&self) -> FsResult<bool> {
        Ok(self.len()? == 0)
    }

    /// Sync to storage device according to durability level
    ///
    /// # Errors
    ///
    /// Returns an error when the requested durability cannot be reached.
    fn sync(&mut self, dur: Durability) -> FsResult<()>;

    /// Close file (called on drop)
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be closed cleanly.
    fn close(self: Box<Self>) -> FsResult<()>;

    /// Vectored read at explicit offset (implementations can optimize)
    ///
    /// # Errors
    ///
    /// Returns an error when the vectored read cannot be completed.
    fn readv_at(&self, offset: u64, bufs: &mut [IoSliceMut<'_>]) -> FsResult<u64> {
        // Default: fall back to scalar read then copy
        let need: usize = bufs.iter().map(|b| b.len()).sum();
        let data = self.read_at(offset, need as u64)?;
        let mut written = 0usize;
        let mut cursor = &data[..];
        for b in bufs {
            let n = b.len().min(cursor.len());
            b[..n].copy_from_slice(&cursor[..n]);
            cursor = &cursor[n..];
            written += n;
        }
        Ok(written as u64)
    }

    /// Vectored write at explicit offset (implementations can optimize)
    ///
    /// # Errors
    ///
    /// Returns an error when the vectored write cannot be completed.
    fn writev_at(&mut self, offset: u64, bufs: &[IoSlice<'_>]) -> FsResult<u64> {
        // Default: coalesce then write
        let mut total = 0usize;
        for b in bufs {
            total += b.len();
        }
        let mut tmp = Vec::with_capacity(total);
        for b in bufs {
            tmp.extend_from_slice(b);
        }
        self.write_at(offset, Bytes::from(tmp))?;
        Ok(total as u64)
    }

    /// Vectored append (WAL hot path - implementations can optimize)
    ///
    /// # Errors
    ///
    /// Returns an error when the append cannot be completed.
    fn appendv(&mut self, bufs: &[IoSlice<'_>]) -> FsResult<u64> {
        // Default: coalesce then append
        let mut total = 0usize;
        for b in bufs {
            total += b.len();
        }
        let mut tmp = Vec::with_capacity(total);
        for b in bufs {
            tmp.extend_from_slice(b);
        }
        self.append(Bytes::from(tmp))
    }

    /// Read multiple ranges in one operation (implementations can optimize)
    ///
    /// # Errors
    ///
    /// Returns an error when any requested range cannot be read.
    fn read_ranges(&self, ranges: &[ReadRange]) -> FsResult<Vec<Bytes>> {
        // Default: N scalar reads
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.read_at(r.offset, u64::from(r.len))?);
        }
        Ok(out)
    }

    /// Report capabilities for this file
    fn caps(&self) -> FileCaps {
        FileCaps::empty()
    }

    /// Try to acquire an exclusive lock on this file (non-blocking).
    /// Used for instance exclusivity (primary lease).
    /// Returns Ok(()) if lock acquired, Err if already locked or unsupported.
    ///
    /// # Errors
    ///
    /// Returns an error when the lock is already held or the backend does not
    /// support file locking.
    fn try_lock_exclusive(&self) -> FsResult<()> {
        Err(FsError::Unsupported(
            "exclusive locking not supported".to_string(),
        ))
    }

    /// Release an exclusive lock on this file.
    /// Idempotent - safe to call even if not locked.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend does not support file locking.
    fn unlock(&self) -> FsResult<()> {
        Err(FsError::Unsupported(
            "exclusive locking not supported".to_string(),
        ))
    }
}

/// Filesystem abstraction - agnostic of domain
pub trait Fs: Send + Sync + 'static {
    /// Pin an immutable read view when opening one SST requires several handles.
    /// Remote implementations bind every later range to one object version.
    ///
    /// # Errors
    ///
    /// Returns an error when the immutable object identity cannot be resolved.
    fn immutable_read_view(&self, _path: &FsPath) -> FsResult<Option<std::sync::Arc<dyn Fs>>> {
        Ok(None)
    }

    /// Return a stable key for coordinating multi-call filesystem transactions.
    ///
    /// Handles that address the same logical filesystem root should return the
    /// same key within a process. Collisions are safe and only add contention.
    /// The conservative default serializes implementations that cannot expose
    /// a stable root identity.
    fn coordination_key(&self) -> u64 {
        0
    }

    // --- Files ---

    /// Open a file
    ///
    /// # Errors
    ///
    /// Returns an error when the path cannot be opened with the requested options.
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>>;

    /// Optional optimized method: open a persistent append handle with 'static lifetime.
    /// Implementations that can return a 'static file (e.g., real OS-backed files) may
    /// provide an implementation. Default: return Unsupported.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot create the handle or does not support it.
    fn open_persistent_handle(
        &self,
        _path: &FsPath,
        _opts: OpenOptions,
    ) -> FsResult<Box<dyn File>> {
        Err(FsError::Unsupported(
            "open_persistent_handle not supported by this backend".to_string(),
        ))
    }

    /// Remove a file
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be removed.
    fn remove_file(&self, path: &FsPath) -> FsResult<()>;

    /// Check if file exists
    ///
    /// # Errors
    ///
    /// Returns an error when the existence check cannot be completed.
    fn exists(&self, path: &FsPath) -> FsResult<bool>;

    /// Get file metadata
    ///
    /// # Errors
    ///
    /// Returns an error when metadata cannot be read.
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata>;

    // --- Directories ---

    /// Create directory and all parents
    ///
    /// # Errors
    ///
    /// Returns an error when the directory hierarchy cannot be created.
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()>;

    /// List directory contents
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be enumerated.
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>>;

    /// Remove directory and all contents
    ///
    /// # Errors
    ///
    /// Returns an error when the directory tree cannot be removed.
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()>;

    /// Sync directory metadata to storage
    ///
    /// # Errors
    ///
    /// Returns an error when directory metadata cannot be durably synced.
    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()>;

    // --- Atomicity ---

    /// Atomic rename (same filesystem/volume)
    ///
    /// # Errors
    ///
    /// Returns an error when the atomic rename cannot be completed.
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()>;
}

impl From<FsError> for crate::common::MidgeError {
    fn from(err: FsError) -> Self {
        match err {
            FsError::NotFound(_msg) => crate::common::MidgeError::NotFound,
            FsError::AlreadyExists(msg) => crate::common::MidgeError::InvalidArgument(msg),
            FsError::Corruption(msg) => crate::common::MidgeError::Corruption(msg),
            FsError::Io(msg) => {
                let lowered = msg.to_ascii_lowercase();
                if lowered.contains("no space") || lowered.contains("disk full") {
                    crate::common::MidgeError::NoSpace(msg)
                } else {
                    crate::common::MidgeError::Io(std::io::Error::other(msg))
                }
            }
            FsError::Unavailable(msg) => crate::common::MidgeError::Internal(msg),
            FsError::Timeout(msg) => crate::common::MidgeError::Timeout(msg),
            FsError::Unsupported(msg) => crate::common::MidgeError::NotSupported(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filesystem_error_coverage(error: &FsError) -> &'static str {
        match error {
            FsError::NotFound(_) => "missing SST and filesystem recovery tests",
            FsError::AlreadyExists(_) => "immutable publication and create-new tests",
            FsError::Corruption(_) => "SST integrity and conditional object-version tests",
            FsError::Io(_) => "filesystem failure injection and retained-data tests",
            FsError::Unavailable(_) => "backend availability and retry tests",
            FsError::Timeout(_) => "filesystem timeout classification regression",
            FsError::Unsupported(_) => "unsupported filesystem capability tests",
        }
    }

    #[test]
    fn should_cover_every_internal_filesystem_error_variant() {
        // Arrange
        let errors = [
            FsError::NotFound(String::new()),
            FsError::AlreadyExists(String::new()),
            FsError::Corruption(String::new()),
            FsError::Io(String::new()),
            FsError::Unavailable(String::new()),
            FsError::Timeout(String::new()),
            FsError::Unsupported(String::new()),
        ];

        // Act
        let notes = errors.each_ref().map(filesystem_error_coverage);
        let unique = notes.iter().collect::<std::collections::HashSet<_>>();

        // Assert
        assert!(notes.iter().all(|note| !note.is_empty()));
        assert_eq!(unique.len(), errors.len());
    }

    #[test]
    fn should_preserve_timeout_classification_when_filesystem_operation_expires() {
        // Arrange
        let error = FsError::Timeout("remote SST range deadline expired".into());

        // Act
        let public_error = crate::common::MidgeError::from(error);

        // Assert
        assert!(
            matches!(public_error, crate::common::MidgeError::Timeout(message)
            if message == "remote SST range deadline expired")
        );
    }

    #[test]
    fn should_have_empty_capabilities() {
        let caps = FileCaps::empty();
        assert!(!caps.contains(FileCaps::READV_AT));
    }

    #[test]
    fn should_check_capability() {
        let caps = FileCaps::READV_AT;
        assert!(caps.contains(FileCaps::READV_AT));
        assert!(!caps.contains(FileCaps::WRITEV_AT));
    }

    #[test]
    fn should_combine_capabilities() {
        // Arrange
        let caps = FileCaps::READV_AT;

        // Act
        let combined = FileCaps(caps.0 | FileCaps::WRITEV_AT.0);

        // Assert
        assert!(combined.contains(FileCaps::READV_AT));
        assert!(combined.contains(FileCaps::WRITEV_AT));
    }
}
