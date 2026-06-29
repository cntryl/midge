//! Stable, filesystem-oriented storage abstraction.
//!
//! This module is intentionally small and conservative. It is designed to
//! abstract local filesystems, cloud object stores, and hybrid backends.
//!
//! ## Execution model
//! Implementations may complete operations synchronously or asynchronously.
//! The public API does not require an async runtime and does not guarantee
//! non-blocking behavior.
//!
//! ## Durability model
//! - Mutating operations (`write_at`, `append`, `rename`) are **unsafe by default**.
//! - Durability boundaries are explicit via `sync_file` and `sync_dir`.
//! - Backends must not perform implicit sync/flush unless explicitly requested.

use std::fmt;
use std::io::{IoSlice, IoSliceMut};

/// Result alias for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

/// A small, portable path type.
///
/// Semantics:
/// - UTF-8 string.
/// - Uses `/` as a separator (even on Windows).
/// - Backends may treat paths as hierarchical (filesystem) or as key prefixes
///   (object storage). Callers must not assume POSIX normalization rules.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StoragePath(String);

impl StoragePath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for StoragePath {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for StoragePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Explicit durability boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Durability {
    /// No durability required.
    Unsafe,
    /// Durable with respect to crash/restart *to the extent supported by the backend*.
    Durable,
}

/// Sync flavor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SyncMode {
    /// Sync file data only.
    Data,
    /// Sync data and metadata.
    DataAndMetadata,
}

/// Error taxonomy for storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageErrorKind {
    /// Data corruption detected or strongly suspected.
    Corruption,
    /// Transient or permanent I/O failure.
    Io,
    /// Backend is unavailable (e.g. network partition, credentials invalid, service down).
    Unavailable,
    /// Operation is not supported by this backend or configuration.
    Unsupported,
    /// Path does not exist.
    NotFound,
    /// Path already exists.
    AlreadyExists,
    /// Permission denied / authorization failure.
    PermissionDenied,
    /// Invalid input (e.g. malformed path, invalid offsets).
    InvalidInput,
    /// Conflict due to concurrent writers or conditional operation failure.
    Conflict,
    /// Catch-all.
    Other,
}

/// Storage error with stable taxonomy.
#[derive(Debug)]
pub struct StorageError {
    pub kind: StorageErrorKind,
    pub message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl StorageError {
    pub fn new(kind: StorageErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(
        kind: StorageErrorKind,
        message: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(source.into()),
        }
    }

    pub fn corruption(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Corruption, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Unsupported, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Unavailable, message)
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Io, message)
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as _)
    }
}

/// Capability discovery for `Storage`.
///
/// This struct is `#[non_exhaustive]` so we can add fields without breaking callers.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct StorageCapabilities {
    /// Directory operations exist as distinct primitives.
    pub supports_directories: bool,
    /// `sync_dir()` is supported.
    pub supports_dir_sync: bool,
    /// Backend can guarantee atomic rename within some scope (see `rename()` docs).
    pub supports_atomic_rename: bool,
    /// Backend supports an append primitive.
    pub supports_append: bool,
}

/// Capability discovery for `StorageFile`.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct FileCapabilities {
    pub supports_readv_at: bool,
    pub supports_writev_at: bool,
    pub supports_appendv: bool,
    pub supports_read_ranges: bool,
}

/// File open mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OpenMode {
    ReadOnly,
    ReadWrite,
}

/// Options for opening a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpenOptions {
    pub mode: OpenMode,
    pub create: bool,
    pub create_new: bool,
    pub truncate: bool,
    /// If true, open intends to use append operations.
    pub append: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
            append: false,
        }
    }
}

/// Entry returned by `list_dir()`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

/// Range for batched reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReadRange {
    pub offset: u64,
    pub len: u32,
}

/// Best-effort atomicity classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Atomicity {
    /// Backend guarantees atomic replacement of `from` with `to` in the stated scope.
    Guaranteed,
    /// Backend attempts atomicity but cannot guarantee it (e.g. cross-volume, object store copy+delete).
    BestEffort,
    /// Backend knows the operation is not atomic.
    NonAtomic,
}

/// Options controlling rename behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RenameOptions {
    /// If true, the operation must be atomic or fail with `Unsupported`.
    pub require_atomic: bool,
    /// If true, destination may be replaced.
    pub overwrite: bool,
    /// Whether the backend should attempt to make the rename durable (e.g. directory sync).
    pub durability: Durability,
}

impl Default for RenameOptions {
    fn default() -> Self {
        Self {
            require_atomic: false,
            overwrite: false,
            durability: Durability::Unsafe,
        }
    }
}

/// Report returned by `rename()` describing guarantees actually achieved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RenameReport {
    pub atomicity: Atomicity,
    /// Whether the requested durability level was fully achieved.
    pub durable: bool,
}

/// A file handle with explicit durability boundaries.
///
/// Robustness notes:
/// - Writes may be partial; callers must check the returned byte count.
/// - Backends may reorder or buffer unsafe writes until `sync()`.
/// - `sync()` is the only durability boundary.
pub trait StorageFile: Send {
    /// Implementation-specific capability discovery.
    fn capabilities(&self) -> FileCapabilities {
        FileCapabilities::default()
    }

    /// Read up to `len` bytes starting at `offset`.
    fn read_at(&self, offset: u64, len: u64) -> StorageResult<Vec<u8>>;

    /// Read into multiple caller-provided buffers.
    ///
    /// Returns the number of bytes read across all buffers.
    fn readv_at(&self, offset: u64, bufs: &mut [IoSliceMut<'_>]) -> StorageResult<u64> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        let data = self.read_at(offset, total as u64)?;
        let mut copied = 0usize;
        let mut cursor = data.as_slice();
        for dst in bufs {
            let n = dst.len().min(cursor.len());
            dst[..n].copy_from_slice(&cursor[..n]);
            cursor = &cursor[n..];
            copied += n;
            if cursor.is_empty() {
                break;
            }
        }
        Ok(copied as u64)
    }

    /// Read a batch of independent ranges.
    fn read_ranges(&self, ranges: &[ReadRange]) -> StorageResult<Vec<Vec<u8>>> {
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.read_at(r.offset, u64::from(r.len))?);
        }
        Ok(out)
    }

    /// Write bytes at `offset`.
    ///
    /// Returns the number of bytes written (may be less than `data.len()`).
    fn write_at(&mut self, offset: u64, data: &[u8]) -> StorageResult<u64>;

    /// Write from multiple buffers at `offset`.
    ///
    /// Default fallback: sequential writes (portable, potentially slow).
    fn writev_at(&mut self, mut offset: u64, bufs: &[IoSlice<'_>]) -> StorageResult<u64> {
        let mut total = 0u64;
        for buf in bufs {
            let n = self.write_at(offset, buf)?;
            total += n;
            offset = offset.saturating_add(n);
            if n < buf.len() as u64 {
                break;
            }
        }
        Ok(total)
    }

    /// Append bytes, returning the starting offset and bytes appended.
    fn append(&mut self, data: &[u8]) -> StorageResult<(u64, u64)>;

    /// Append from multiple buffers.
    fn appendv(&mut self, bufs: &[IoSlice<'_>]) -> StorageResult<(u64, u64)> {
        let mut merged = Vec::new();
        let mut total = 0usize;
        for b in bufs {
            total += b.len();
        }
        merged.reserve(total);
        for b in bufs {
            merged.extend_from_slice(b);
        }
        self.append(&merged)
    }

    /// Returns the current file length.
    fn len(&self) -> StorageResult<u64>;

    /// Returns true if the file is empty (length is 0).
    #[inline]
    fn is_empty(&self) -> StorageResult<bool> {
        Ok(self.len()? == 0)
    }

    /// Explicit durability boundary for this file.
    fn sync(&mut self, mode: SyncMode) -> StorageResult<()>;

    /// Close this handle and surface close-time errors.
    fn close(self: Box<Self>) -> StorageResult<()>;
}

/// A stable, filesystem-oriented storage interface.
///
/// Notes:
/// - This trait is intentionally small. Add new behaviors via capabilities first.
/// - Operations may be backed by local disk, object storage, or a hybrid backend.
/// - `sync_*` methods define durability boundaries; other methods are unsafe.
pub trait Storage: Send + Sync {
    /// Query coarse backend capabilities.
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::default()
    }

    /// Open a file.
    fn open_file(
        &self,
        path: &StoragePath,
        options: OpenOptions,
    ) -> StorageResult<Box<dyn StorageFile>>;

    /// List a directory/prefix.
    fn list_dir(&self, path: &StoragePath) -> StorageResult<Vec<DirEntry>>;

    /// Create a directory and any missing parents.
    fn create_dir_all(&self, path: &StoragePath) -> StorageResult<()>;

    /// Attempt to make directory metadata durable.
    ///
    /// Backends that do not have durable directories must return `Unsupported`.
    fn sync_dir(&self, path: &StoragePath, mode: SyncMode) -> StorageResult<()>;

    /// Best-effort rename.
    ///
    /// Guarantees:
    /// - If `options.require_atomic == true`, implementations must either perform an
    ///   atomic rename within their supported scope or fail with `Unsupported`.
    /// - If `options.require_atomic == false`, implementations may fall back to
    ///   non-atomic emulation (copy+delete, multipart rewrite, etc.).
    /// - If `options.durability == Durable`, implementations must *attempt* to make
    ///   the rename durable, and report whether it was achieved.
    fn rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
        options: RenameOptions,
    ) -> StorageResult<RenameReport>;
}
