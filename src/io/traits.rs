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
use std::path::Path;
use thiserror::Error;

/// Filesystem errors
///
/// Only `internal-testing` builds can name this type. Internal code converts it
/// with the crate-private [`FsError::into_midge`] rather than a `From` impl, so
/// `MidgeError`'s public API does not expose it.
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
    /// The device or quota is full.
    #[error("no space: {0}")]
    NoSpace(String),
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
}

pub type FsResult<T> = Result<T, FsError>;

/// An acquired, non-durable exclusive filesystem lock.
///
/// Locks are coordination state, not durable data. Dropping the guard releases
/// the lock, and the operating system releases host locks when a process exits.
pub trait FsLock: Send + Sync {}

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

impl OpenOptions {
    /// Open an existing file for reading only.
    pub const READ_ONLY: Self = Self {
        mode: OpenMode::ReadOnly,
        create: false,
        create_new: false,
        truncate: false,
    };
}

/// Metadata for a file or directory
#[derive(Debug, Clone)]
pub struct Metadata {
    pub len: u64,
    /// Whether the path names a directory rather than a file.
    pub is_dir: bool,
}

/// Directory entry
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

/// Receives transport observations without coupling filesystems to runtime metrics.
/// Local file reads and reads rejected before submission do not emit events.
pub trait ReadObserver: Send + Sync {
    fn remote_range_started(&self);
    fn remote_range_completed(
        &self,
        returned_bytes: u64,
        elapsed: std::time::Duration,
        failed: bool,
    );
}

/// The read-only operations of an individual file handle.
pub trait ReadFile: Send {
    /// Read at specific offset
    ///
    /// # Errors
    ///
    /// Returns an error when the read cannot be satisfied by the backing
    /// filesystem or storage backend.
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes>;

    /// Get file length.
    ///
    /// # Errors
    ///
    /// Returns an error when the file length cannot be queried.
    fn len(&self) -> FsResult<u64>;

    /// Whether the file contains no bytes.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::len`].
    #[cfg(feature = "internal-testing")]
    fn is_empty(&self) -> FsResult<bool> {
        self.len().map(|len| len == 0)
    }
}

/// Individual file handle with read/write operations.
pub trait File: ReadFile {
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

    /// Sync to storage device according to durability level
    ///
    /// # Errors
    ///
    /// Returns an error when the requested durability cannot be reached.
    fn sync(&mut self, dur: Durability) -> FsResult<()>;
}

/// The host path frame used to address a rooted filesystem.
///
/// A host-rooted backend records the canonical root it serves and the same
/// directory as its caller named it. Keeping them in one value makes a
/// wrapper either delegate the complete frame or expose no host addressing
/// capability at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostAddressing<'a> {
    /// Canonical host directory the filesystem serves.
    pub root: &'a Path,
    /// The root as its caller named it, made absolute without resolving
    /// symlinks (`/var/...` where `root` is `/private/var/...` on macOS).
    pub named_root: &'a Path,
}

impl HostAddressing<'_> {
    /// The part of an absolute host `path` below this root, reached through
    /// either spelling of the root. This compares paths without touching the
    /// host, so every component below the root stays exactly as written.
    #[must_use]
    pub fn relative<'p>(&self, path: &'p Path) -> Option<&'p Path> {
        path.strip_prefix(self.root)
            .or_else(|_| path.strip_prefix(self.named_root))
            .ok()
    }
}

/// The read-only surface of a filesystem: open for reading, check existence,
/// read metadata and list directories.
///
/// A view that only serves reads (WAL replay over authorized sources)
/// implements this alone, so the type system rather than a runtime error
/// keeps it from mutating anything. Every [`Fs`] is also a `ReadFs`.
pub trait ReadFs: Send + Sync + 'static {
    /// Open an existing file for reading. For an [`Fs`] this is
    /// [`Fs::open`] with [`OpenOptions::READ_ONLY`].
    ///
    /// # Errors
    ///
    /// Returns an error when the path cannot be opened for reading; a missing
    /// file is `FsError::NotFound`.
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn ReadFile + '_>>;

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

    /// List directory contents
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be enumerated.
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>>;
}

/// Filesystem abstraction - agnostic of domain
///
/// Extends [`ReadFs`] with opening for writing, the capability hooks that
/// views of the same storage share, and every mutator.
pub trait Fs: ReadFs {
    /// The filesystem that receives writes when this view also reads through
    /// a remote fallback. Fresh SST outputs must be read back from this view
    /// before their remote publication exists.
    fn local_output_view(&self) -> Option<std::sync::Arc<dyn Fs>> {
        None
    }

    /// Return an equivalent view whose remote range reads report to this owner.
    /// Implementations without remote I/O keep their existing view.
    fn with_read_observer(
        &self,
        _observer: std::sync::Arc<dyn ReadObserver>,
    ) -> Option<std::sync::Arc<dyn Fs>> {
        None
    }

    /// Pin an immutable read view when opening one SST requires several handles.
    /// Remote implementations bind every later range to one object version.
    ///
    /// # Errors
    ///
    /// Returns an error when the immutable object identity cannot be resolved.
    fn immutable_read_view(&self, _path: &FsPath) -> FsResult<Option<std::sync::Arc<dyn Fs>>> {
        Ok(None)
    }

    /// Return the host path frame for this filesystem, when it has one.
    ///
    /// Backends that address a host directory expose both the canonical root
    /// and the frame for relative host paths. Backends without host paths
    /// (in-memory mocks and object stores) return `None`; their path strings
    /// are their own key space. Wrappers forwarding writes must delegate this
    /// complete capability to the backend they forward to.
    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        None
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

    /// Try to acquire a non-durable exclusive lock for `path`.
    ///
    /// Backends with process-scoped locking should override this. Returning
    /// `AlreadyExists` means another process currently holds the lock;
    /// `Unsupported` asks callers to use their portable fallback. The lock is
    /// released when the returned guard is dropped.
    ///
    /// # Errors
    ///
    /// Returns `AlreadyExists` when another owner holds the lock, `Unsupported`
    /// when the backend has no native locking support, or another filesystem
    /// error when lock acquisition fails.
    fn try_lock_exclusive(&self, path: &FsPath) -> FsResult<Box<dyn FsLock>> {
        Err(FsError::Unsupported(format!(
            "exclusive filesystem locks are unavailable for {path}"
        )))
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

    // --- Directories ---

    /// Create directory and all parents
    ///
    /// # Errors
    ///
    /// Returns an error when the directory hierarchy cannot be created.
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()>;

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

impl FsError {
    /// Carry a filesystem error through a `std::io` interface. The error rides
    /// along as the payload, so converting back to [`crate::common::MidgeError`]
    /// classifies it exactly as [`Self::into_midge`] does.
    pub(crate) fn into_io(self) -> std::io::Error {
        let kind = match &self {
            FsError::NotFound(_) => std::io::ErrorKind::NotFound,
            FsError::AlreadyExists(_) => std::io::ErrorKind::AlreadyExists,
            FsError::Corruption(_) => std::io::ErrorKind::InvalidData,
            FsError::NoSpace(_) => std::io::ErrorKind::StorageFull,
            FsError::Timeout(_) => std::io::ErrorKind::TimedOut,
            FsError::Unsupported(_) => std::io::ErrorKind::Unsupported,
            FsError::Io(_) | FsError::Unavailable(_) => std::io::ErrorKind::Other,
        };
        std::io::Error::new(kind, self)
    }

    /// Map a filesystem error onto the public error type.
    pub(crate) fn into_midge(self) -> crate::common::MidgeError {
        match self {
            FsError::NotFound(_msg) => crate::common::MidgeError::NotFound,
            FsError::AlreadyExists(msg) => crate::common::MidgeError::InvalidArgument(msg),
            FsError::Corruption(msg) => crate::common::MidgeError::Corruption(msg),
            FsError::Io(msg) => crate::common::MidgeError::Io(std::io::Error::other(msg)),
            FsError::NoSpace(msg) => crate::common::MidgeError::NoSpace(msg),
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
            FsError::NoSpace(_) => "disk-full classification regression",
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
            FsError::NoSpace(String::new()),
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
        let public_error = FsError::into_midge(error);

        // Assert
        assert!(
            matches!(public_error, crate::common::MidgeError::Timeout(message)
            if message == "remote SST range deadline expired")
        );
    }
}
