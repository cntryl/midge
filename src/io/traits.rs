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
//! - Multiple swappable implementations (Real, Mock, Chaos)

use bytes::Bytes;
use std::io::{IoSlice, IoSliceMut};
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

/// Portable path type - avoids std::path pollution in abstraction
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FsPath(pub String);

impl FsPath {
    pub fn new(s: impl Into<String>) -> Self {
        FsPath(s.into())
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

    pub const fn empty() -> Self {
        FileCaps(0)
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) != 0
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
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes>;

    /// Write at specific offset
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()>;

    /// Append to end of file, return starting offset
    fn append(&mut self, data: Bytes) -> FsResult<u64>;

    /// Get file length
    fn len(&self) -> FsResult<u64>;

    /// Check if file is empty
    fn is_empty(&self) -> FsResult<bool> {
        Ok(self.len()? == 0)
    }

    /// Sync to storage device according to durability level
    fn sync(&mut self, dur: Durability) -> FsResult<()>;

    /// Close file (called on drop)
    fn close(self: Box<Self>) -> FsResult<()>;

    /// Vectored read at explicit offset (implementations can optimize)
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
    fn read_ranges(&self, ranges: &[ReadRange]) -> FsResult<Vec<Bytes>> {
        // Default: N scalar reads
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.read_at(r.offset, r.len as u64)?);
        }
        Ok(out)
    }

    /// Report capabilities for this file
    fn caps(&self) -> FileCaps {
        FileCaps::empty()
    }
}

/// Filesystem abstraction - agnostic of domain
pub trait Fs: Send + Sync + 'static {
    // --- Files ---

    /// Open a file
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>>;

    /// Optional optimized method: open a persistent append handle with 'static lifetime.
    /// Implementations that can return a 'static file (e.g., real OS-backed files) may
    /// provide an implementation. Default: return Unsupported.
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
    fn remove_file(&self, path: &FsPath) -> FsResult<()>;

    /// Check if file exists
    fn exists(&self, path: &FsPath) -> FsResult<bool>;

    /// Get file metadata
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata>;

    // --- Directories ---

    /// Create directory and all parents
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()>;

    /// List directory contents
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>>;

    /// Remove directory and all contents
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()>;

    /// Sync directory metadata to storage
    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()>;

    // --- Atomicity ---

    /// Atomic rename (same filesystem/volume)
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()>;
}

impl From<FsError> for crate::common::MidgeError {
    fn from(err: FsError) -> Self {
        match err {
            FsError::NotFound(_msg) => crate::common::MidgeError::NotFound,
            FsError::AlreadyExists(msg) => crate::common::MidgeError::InvalidArgument(msg),
            FsError::Corruption(msg) => crate::common::MidgeError::Corruption(msg),
            FsError::Io(msg) => crate::common::MidgeError::Io(std::io::Error::other(msg)),
            FsError::Unavailable(msg) => crate::common::MidgeError::Internal(msg),
            FsError::Unsupported(msg) => crate::common::MidgeError::NotSupported(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_fs_path() {
        let path = FsPath::new("test/file.txt");
        assert_eq!(path.0, "test/file.txt");
    }

    #[test]
    fn should_compare_paths() {
        let p1 = FsPath::new("test");
        let p2 = FsPath::new("test");
        assert_eq!(p1, p2);
    }

    #[test]
    fn should_use_path_as_key() {
        // Arrange
        use std::collections::HashMap;
        let mut map = HashMap::new();
        let path = FsPath::new("key");

        // Act
        map.insert(path, "value");

        // Assert
        assert_eq!(map.get(&FsPath::new("key")), Some(&"value"));
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
