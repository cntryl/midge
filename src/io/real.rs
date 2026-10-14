//! Real filesystem implementation
//!
//! Direct mapping to `std::fs` with path sanitization.
//! Suitable for production use.
//!
//! Notes:
//! - `read_at` / `write_at` use true positional IO when available (no shared cursor):
//!   - Unix: `std::os::unix::fs::FileExt::{read_at`, `write_at`}
//!   - Windows: `std::os::windows::fs::FileExt::{seek_read`, `seek_write`}
//! - `sync_dir` uses a directory durability barrier on Unix and Windows.

use super::traits::{
    DirEntry, Durability, File, Fs, FsError, FsLock, FsPath, FsResult, HostAddressing, Metadata,
    OpenOptions, ReadFs,
};
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Weak};

/// NFS can implement advisory locks with process-scoped record locks, where
/// closing any descriptor for the lock file releases the process's lock.
/// Keep one local owner per canonical path so a contending thread never opens
/// and closes another descriptor while an effect is in progress.
static LOCAL_FILE_LOCKS: LazyLock<parking_lot::Mutex<HashMap<PathBuf, Weak<AtomicBool>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

fn local_file_lock(path: &Path) -> Arc<AtomicBool> {
    let mut locks = LOCAL_FILE_LOCKS.lock();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(AtomicBool::new(false));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

struct RealFsLock {
    file: Option<fs::File>,
    local: Arc<AtomicBool>,
}

impl Drop for RealFsLock {
    fn drop(&mut self) {
        // Release the OS/NFS lock before allowing another local descriptor to
        // open the same path.
        self.file.take();
        self.local.store(false, Ordering::Release);
    }
}

impl FsLock for RealFsLock {}

fn lock_error(path: &Path, error: &io::Error) -> FsError {
    let unsupported = error.kind() == io::ErrorKind::Unsupported || {
        #[cfg(unix)]
        {
            error.raw_os_error().is_some_and(|code| {
                code == libc::EOPNOTSUPP || code == libc::ENOTSUP || code == libc::ENOSYS
            })
        }
        #[cfg(not(unix))]
        {
            false
        }
    };
    if unsupported {
        FsError::Unsupported(format!(
            "exclusive file locks are unsupported for {}: {error}",
            path.display()
        ))
    } else {
        io_err("lock_exclusive", path, error)
    }
}

/// Real filesystem backend
pub struct RealFs {
    base_path: PathBuf,
    /// `base_path` as the caller named it, made absolute at construction
    /// without resolving symlinks.
    named_root: PathBuf,
}

/// A host-backed database root for tests, created if missing.
///
/// # Panics
///
/// Panics when the directory cannot be created or resolved.
#[cfg(test)]
pub fn test_root(path: impl AsRef<Path>) -> std::sync::Arc<dyn Fs> {
    std::sync::Arc::new(RealFs::new(path).expect("test database root"))
}

impl RealFs {
    /// Create a new real filesystem rooted at `base_path`
    ///
    /// # Note
    /// Callers are responsible for ensuring this is not called in memory-only mode.
    /// Higher-level code (`EventLoop`, lease creation, etc.) should check `memory_mode`
    /// and use `MockFs` instead when appropriate.
    ///
    /// # Errors
    ///
    /// Returns an error when the base directory cannot be created.
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn new(base_path: impl AsRef<Path>) -> FsResult<Self> {
        let path = base_path.as_ref().to_path_buf();
        super::durable_dir::create_path_durably(&path)
            .map_err(|error| io_err("create_path_durably", &path, &error))?;
        Self::resolved(&path)
    }

    /// Open a filesystem root that must already exist, without creating it.
    ///
    /// This is the entry point for offline inspection and verification paths,
    /// where observing a missing directory must never mutate disk state.
    ///
    /// # Errors
    ///
    /// Returns an error when the root is missing, inaccessible, or not a directory.
    pub fn open_existing(base_path: impl AsRef<Path>) -> FsResult<Self> {
        let path = base_path.as_ref().to_path_buf();
        let metadata = fs::metadata(&path).map_err(|error| io_err("metadata", &path, &error))?;
        if !metadata.is_dir() {
            return Err(FsError::Io(format!(
                "filesystem root is not a directory: {}",
                path.display()
            )));
        }
        Self::resolved(&path)
    }

    fn resolved(path: &Path) -> FsResult<Self> {
        let named_root =
            std::path::absolute(path).map_err(|error| io_err("absolute", path, &error))?;
        let base_path =
            fs::canonicalize(path).map_err(|error| io_err("canonicalize", path, &error))?;
        Ok(Self {
            base_path,
            named_root,
        })
    }

    /// Compute sanitized full path, preventing directory traversal.
    ///
    /// Current policy (drop-in compatible with your tests):
    /// - keeps `Normal` components
    /// - ignores `.` and any attempts to traverse (`..`, roots, prefixes)
    fn full_path(&self, rel: &FsPath) -> FsResult<PathBuf> {
        let mut out = self.base_path.clone();
        for component in Path::new(&rel.0).components() {
            match component {
                Component::Normal(part) => out.push(part),
                Component::CurDir
                | Component::ParentDir
                | Component::RootDir
                | Component::Prefix(_) => {}
            }
        }
        let relative = out
            .strip_prefix(&self.base_path)
            .map_err(|error| FsError::Io(format!("filesystem path escaped root: {error}")))?;
        let mut current = self.base_path.clone();
        for component in relative.components() {
            let Component::Normal(part) = component else {
                continue;
            };
            current.push(part);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(FsError::Io(format!(
                        "filesystem path contains a symlink: {}",
                        current.display()
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => break,
                Err(error) => return Err(io_err("symlink_metadata", &current, &error)),
            }
        }
        Ok(out)
    }

    /// Best-effort parent directory extraction for directory fsync barriers.
    fn parent_dir(full_path: &Path) -> Option<&Path> {
        full_path.parent().filter(|p| !p.as_os_str().is_empty())
    }

    /// Shared internal helper used by `Fs` implementations to open an OS file.
    fn open_inner(
        &self,
        path: &FsPath,
        opts: super::traits::OpenOptions,
    ) -> super::traits::FsResult<Box<dyn super::traits::File>> {
        // Forward to the `Fs`-level implementation so the code is colocated and
        // reusable when `RealFs` is used as a backend for in-memory tests.
        // Note: this helper returns a `'static` file handle.
        let full = self.full_path(path)?;

        if opts.create || opts.create_new {
            if let Some(parent) = Self::parent_dir(&full) {
                super::durable_dir::create_dir_all_durably(&self.base_path, parent)
                    .map_err(|e| io_error("create_dir_all", &e))?;
            }
        }

        let mut std_opts = std::fs::OpenOptions::new();
        match opts.mode {
            super::traits::OpenMode::ReadOnly => std_opts.read(true),
            super::traits::OpenMode::ReadWrite => std_opts.read(true).write(true),
        };
        if opts.create {
            std_opts.create(true);
        }
        if opts.create_new {
            std_opts.create_new(true);
        }
        if opts.truncate {
            std_opts.truncate(true);
        }

        let file = std_opts
            .open(&full)
            .map_err(|error| file_op_err("open", &full, &error))?;
        Ok(Box::new(RealFile { file }))
    }
}

impl ReadFs for RealFs {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
        self.open(path, OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn crate::io::ReadFile>)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        let full = self.full_path(path)?;
        match fs::metadata(&full) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(io_err("metadata(exists)", &full, &error)),
        }
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let full = self.full_path(path)?;
        let meta = fs::metadata(&full).map_err(|error| file_op_err("metadata", &full, &error))?;
        Ok(Metadata {
            len: meta.len(),
            is_dir: meta.is_dir(),
        })
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let full = self.full_path(path)?;
        let entries = fs::read_dir(&full)
            .map_err(|error| file_op_err("read_dir", &full, &error))?
            .map(|entry| {
                let entry = entry.map_err(|e| io_err("read_dir_entry", &full, &e))?;
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry
                    .file_type()
                    .map_err(|e| io_err("file_type", &full, &e))?
                    .is_dir();
                Ok(DirEntry { name, is_dir })
            })
            .collect::<Result<Vec<_>, FsError>>()?;
        Ok(entries)
    }
}

impl Fs for RealFs {
    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        Some(HostAddressing {
            root: &self.base_path,
            named_root: &self.named_root,
        })
    }

    fn coordination_key(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.base_path.hash(&mut hasher);
        hasher.finish()
    }

    fn try_lock_exclusive(&self, path: &FsPath) -> FsResult<Box<dyn FsLock>> {
        let full = self.full_path(path)?;
        let local = local_file_lock(&full);
        local
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .map_err(|_| FsError::AlreadyExists(path.0.clone()))?;
        let file = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&full)
        {
            Ok(file) => file,
            Err(error) => {
                local.store(false, Ordering::Release);
                return Err(file_op_err("open_lock", &full, &error));
            }
        };
        match file.try_lock() {
            Ok(()) => Ok(Box::new(RealFsLock {
                file: Some(file),
                local,
            })),
            Err(std::fs::TryLockError::WouldBlock) => {
                local.store(false, Ordering::Release);
                Err(FsError::AlreadyExists(path.0.clone()))
            }
            Err(std::fs::TryLockError::Error(error)) => {
                local.store(false, Ordering::Release);
                Err(lock_error(&full, &error))
            }
        }
    }

    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        // Delegate to shared implementation that returns a 'static file handle.
        self.open_inner(path, opts)
    }

    fn open_persistent_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        // For real FS, a file handle is independently owned and therefore 'static.
        // Delegate to the shared implementation which returns a `'static` handle.
        self.open_inner(path, opts)
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        let full = self.full_path(path)?;
        fs::remove_file(&full).map_err(|error| file_op_err("remove_file", &full, &error))
    }

    /// Creates missing directories durably: each new entry's parent is
    /// fsynced before this returns (#519).
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let full = self.full_path(path)?;
        super::durable_dir::create_dir_all_durably(&self.base_path, &full)
            .map_err(|e| io_err("create_dir_all", &full, &e))
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let full = self.full_path(path)?;
        super::durable_dir::remove_dir_all_and_forget(&full, || fs::remove_dir_all(&full))
            .map_err(|error| file_op_err("remove_dir_all", &full, &error))
    }

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        if dur == Durability::Unsafe {
            return Ok(());
        }

        let full = self.full_path(path)?;

        // Unix: directory fsync barrier.
        #[cfg(unix)]
        {
            // fs::File::open works for directories on Unix.
            let dir = fs::File::open(&full).map_err(|e| io_err("open_dir", &full, &e))?;
            dir.sync_all().map_err(|e| io_err("fsync_dir", &full, &e))?;
            Ok(())
        }

        // Windows: open the directory with backup semantics, then flush the handle.
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;

            // FlushFileBuffers requires GENERIC_WRITE even for a directory
            // handle opened with backup semantics.
            let mut options = fs::OpenOptions::new();
            options
                .read(true)
                .write(true)
                .custom_flags(winapi::um::winbase::FILE_FLAG_BACKUP_SEMANTICS);
            let dir = options
                .open(&full)
                .map_err(|e| io_err("open_dir", &full, &e))?;
            dir.sync_all().map_err(|e| io_err("flush_dir", &full, &e))?;
            Ok(())
        }

        #[cfg(not(any(unix, windows)))]
        {
            Err(FsError::Unsupported(format!(
                "durable directory sync is not supported for {}",
                full.display()
            )))
        }
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let from_full = self.full_path(from)?;
        let to_full = self.full_path(to)?;

        // Ensure destination parent exists (helps callers that assume it).
        if let Some(parent) = Self::parent_dir(&to_full) {
            super::durable_dir::create_dir_all_durably(&self.base_path, parent)
                .map_err(|e| io_err("create_dir_all", parent, &e))?;
        }

        #[cfg(windows)]
        {
            use winapi::um::winbase::{
                MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
            };

            const ERROR_ACCESS_DENIED: i32 = 5;

            let from_wide: Vec<u16> = from_full
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();
            let to_wide: Vec<u16> = to_full
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();
            // SAFETY: both paths are valid, NUL-terminated UTF-16 buffers for
            // the duration of the call. The flags request atomic replacement
            // and a write-through durability barrier.
            let result = unsafe {
                MoveFileExW(
                    from_wide.as_ptr(),
                    to_wide.as_ptr(),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            };
            if result == 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(ERROR_ACCESS_DENIED) {
                    // Rust's Windows rename uses FileRenameInfoEx with POSIX
                    // replacement semantics here: existing readers retain the
                    // old file, while new opens see the replacement. Keep the
                    // write-through fast path above for ordinary WAL sealing;
                    // staged replacements also sync their parent directory.
                    return fs::rename(&from_full, &to_full)
                        .map_err(|error| rename_err(&from_full, &to_full, &error));
                }
                return Err(rename_err(&from_full, &to_full, &error));
            }
            Ok(())
        }

        #[cfg(not(windows))]
        {
            fs::rename(&from_full, &to_full)
                .map_err(|error| rename_err(&from_full, &to_full, &error))
        }
    }
}

pub struct RealFile {
    file: fs::File,
}

impl RealFile {
    fn len_usize_u64(len: u64) -> FsResult<usize> {
        usize::try_from(len).map_err(|_| FsError::Io(format!("len too large: {len}")))
    }
}

impl crate::io::ReadFile for RealFile {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<bytes::Bytes> {
        let len = Self::len_usize_u64(len)?;
        let mut buf = vec![0u8; len];

        // Prefer true positional IO (no shared cursor).
        #[cfg(unix)]
        {
            read_exact_at_unix(&self.file, offset, &mut buf)?;
            Ok(bytes::Bytes::from(buf))
        }

        #[cfg(windows)]
        {
            read_exact_at_windows(&self.file, offset, &mut buf)?;
            Ok(bytes::Bytes::from(buf))
        }

        #[cfg(not(any(unix, windows)))]
        {
            // Fallback: cursor-based.
            use std::io::{Read, Seek, SeekFrom};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| io_error(format!("seek(read_at) offset={offset}"), &e))?;
            file.read_exact(&mut buf)
                .map_err(|e| io_error(format!("read_exact(read_at) len={len}"), &e))?;
            Ok(bytes::Bytes::from(buf))
        }
    }

    fn len(&self) -> FsResult<u64> {
        let meta = self
            .file
            .metadata()
            .map_err(|e| io_error("metadata(len)", &e))?;
        Ok(meta.len())
    }
}

impl File for RealFile {
    fn write_at(&mut self, offset: u64, data: bytes::Bytes) -> FsResult<()> {
        let bytes = data.as_ref();

        // Prefer true positional IO (no shared cursor).
        #[cfg(unix)]
        {
            write_all_at_unix(&self.file, offset, bytes)?;
            Ok(())
        }

        #[cfg(windows)]
        {
            write_all_at_windows(&self.file, offset, bytes)?;
            Ok(())
        }

        #[cfg(not(any(unix, windows)))]
        {
            use std::io::{Seek, SeekFrom, Write};
            self.file
                .seek(SeekFrom::Start(offset))
                .map_err(|e| io_error(format!("seek(write_at) offset={offset}"), &e))?;
            self.file
                .write_all(bytes)
                .map_err(|e| io_error(format!("write_all(write_at) len={}", bytes.len()), &e))?;
            Ok(())
        }
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        self.file
            .set_len(len)
            .map_err(|error| io_error(format!("truncate len={len}"), &error))
    }

    fn append(&mut self, data: bytes::Bytes) -> FsResult<u64> {
        // Keep as cursor-based: append implies a shared logical end anyway.
        use std::io::{Seek, SeekFrom, Write};

        let pos = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| io_error("seek(append)", &e))?;

        self.file
            .write_all(data.as_ref())
            .map_err(|e| io_error(format!("write_all(append) len={}", data.len()), &e))?;

        Ok(pos)
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        match dur {
            Durability::Unsafe => Ok(()),
            Durability::Durable => self.file.sync_all().map_err(|e| io_error("sync_all", &e)),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers

fn io_err(op: &str, path: &Path, e: &io::Error) -> FsError {
    io_error(format!("{op} {}", path.display()), e)
}

fn file_op_err(op: &str, path: &Path, error: &io::Error) -> FsError {
    let message = format!("{op} {}: {error}", path.display());
    match error.kind() {
        io::ErrorKind::NotFound => FsError::NotFound(message),
        io::ErrorKind::AlreadyExists => FsError::AlreadyExists(message),
        _ => io_error(format!("{op} {}", path.display()), error),
    }
}

fn rename_err(from: &Path, to: &Path, error: &io::Error) -> FsError {
    if error.kind() != io::ErrorKind::NotFound {
        return file_op_err("rename", to, error);
    }

    let destination_parent = to.parent().unwrap_or_else(|| Path::new("."));
    match fs::metadata(destination_parent) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return FsError::Io(format!(
                "rename destination parent is not a directory: {}",
                destination_parent.display()
            ));
        }
        Err(parent_error) => {
            return io_err(
                "rename destination parent",
                destination_parent,
                &parent_error,
            );
        }
    }

    match fs::symlink_metadata(from) {
        Err(source_error) if source_error.kind() == io::ErrorKind::NotFound => {
            FsError::NotFound(format!("rename source {}: {source_error}", from.display()))
        }
        Err(source_error) => io_err("inspect rename source", from, &source_error),
        Ok(_) => io_err("rename", to, error),
    }
}

/// The one place `RealFs` turns an OS error into an `FsError`, keeping the
/// class callers act on (a full disk) as a variant rather than message text.
pub(super) fn io_error(context: impl std::fmt::Display, error: &io::Error) -> FsError {
    let message = format!("{context}: {error}");
    if crate::common::is_no_space(error.kind()) {
        FsError::NoSpace(message)
    } else {
        FsError::Io(message)
    }
}

#[cfg(unix)]
fn read_exact_at_unix(file: &fs::File, mut offset: u64, mut dst: &mut [u8]) -> FsResult<()> {
    use std::os::unix::fs::FileExt;

    while !dst.is_empty() {
        let n = file
            .read_at(dst, offset)
            .map_err(|e| io_error(format!("pread offset={offset}"), &e))?;
        if n == 0 {
            return Err(FsError::Io(format!(
                "pread offset={offset}: unexpected EOF"
            )));
        }
        offset += n as u64;
        dst = &mut dst[n..];
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at_unix(file: &fs::File, mut offset: u64, mut src: &[u8]) -> FsResult<()> {
    use std::os::unix::fs::FileExt;

    while !src.is_empty() {
        let n = file
            .write_at(src, offset)
            .map_err(|e| io_error(format!("pwrite offset={offset}"), &e))?;
        if n == 0 {
            return Err(FsError::Io(format!(
                "pwrite offset={offset}: wrote 0 bytes"
            )));
        }
        offset += n as u64;
        src = &src[n..];
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at_windows(file: &fs::File, mut offset: u64, mut dst: &mut [u8]) -> FsResult<()> {
    use std::os::windows::fs::FileExt;

    while !dst.is_empty() {
        let n = file
            .seek_read(dst, offset)
            .map_err(|e| io_error(format!("seek_read offset={offset}"), &e))?;
        if n == 0 {
            return Err(FsError::Io(format!(
                "seek_read offset={offset}: unexpected EOF"
            )));
        }
        offset += n as u64;
        dst = &mut dst[n..];
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at_windows(file: &fs::File, mut offset: u64, mut src: &[u8]) -> FsResult<()> {
    use std::os::windows::fs::FileExt;

    while !src.is_empty() {
        let n = file
            .seek_write(src, offset)
            .map_err(|e| io_error(format!("seek_write offset={offset}"), &e))?;
        if n == 0 {
            return Err(FsError::Io(format!(
                "seek_write offset={offset}: wrote 0 bytes"
            )));
        }
        offset += n as u64;
        src = &src[n..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::io::FsPath;

    #[test]
    fn should_sync_parent_of_each_new_directory_when_creating_nested_directories() {
        // Arrange (#519)
        let temp = tempfile::tempdir().expect("temp dir");
        let fs = RealFs::new(temp.path()).expect("real fs");
        let root = std::fs::canonicalize(temp.path()).expect("canonical root");
        crate::io::durable_dir::take_synced_dirs();

        // Act
        crate::io::Fs::create_dir_all(&fs, &FsPath::new("wal/epochs/3")).expect("create");

        // Assert
        assert_eq!(
            crate::io::durable_dir::take_synced_dirs(),
            [root.clone(), root.join("wal"), root.join("wal/epochs")]
        );
    }

    #[test]
    fn should_classify_storage_full_as_no_space_when_realfs_error_is_converted() {
        // Arrange: neither message says "no space" or "disk full".
        let errors = [
            io::Error::from(io::ErrorKind::StorageFull),
            io::Error::new(io::ErrorKind::StorageFull, "x"),
            io::Error::from(io::ErrorKind::QuotaExceeded),
        ];

        // Act
        let converted: Vec<_> = errors
            .iter()
            .map(|error| FsError::into_midge(super::io_error("write", error)))
            .collect();

        // Assert
        assert!(
            converted
                .iter()
                .all(|error| matches!(error, crate::common::MidgeError::NoSpace(_))),
            "{converted:?}"
        );
    }

    use super::*;
    use crate::io::OpenMode;
    use tempfile::TempDir;

    #[test]
    fn should_use_portable_lock_fallback_when_os_lock_is_unsupported() {
        // Arrange
        let error = io::Error::new(io::ErrorKind::Unsupported, "lock operation unsupported");

        // Act
        let mapped = lock_error(Path::new(".midge_writer_effect.v2.lock"), &error);

        // Assert
        assert!(matches!(mapped, FsError::Unsupported(_)), "{mapped:?}");
    }

    #[test]
    fn should_keep_process_lock_held_when_another_real_fs_view_contends() -> FsResult<()> {
        // Arrange: two independently opened roots point at the same lock file.
        let temp = TempDir::new().map_err(|error| FsError::Io(error.to_string()))?;
        let first = RealFs::new(temp.path())?;
        let second = RealFs::new(temp.path())?;
        let path = FsPath::new(".midge_writer_effect.v2.lock");
        let held = first.try_lock_exclusive(&path)?;

        // Act: a contending view must not open and close another descriptor.
        let contention = second.try_lock_exclusive(&path);

        // Assert: the first view still owns the lock until its guard drops.
        assert!(matches!(contention, Err(FsError::AlreadyExists(_))));
        assert!(matches!(
            second.try_lock_exclusive(&path),
            Err(FsError::AlreadyExists(_))
        ));
        drop(held);
        second.try_lock_exclusive(&path)?;
        Ok(())
    }

    #[test]
    fn should_create_real_fs() -> FsResult<()> {
        // Arrange: point at a nested directory that does not exist yet, to
        // exercise RealFs::new's create_dir_all behavior concretely.
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let nested = temp.path().join("a").join("b").join("c");
        assert!(
            !nested.exists(),
            "precondition: nested dir must not exist yet"
        );

        // Act
        let _fs = RealFs::new(&nested)?;

        // Assert: RealFs::new created the full directory chain
        assert!(nested.is_dir());
        Ok(())
    }

    #[test]
    fn should_make_new_filesystem_root_entry_durable() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let root = std::fs::canonicalize(temp.path()).expect("canonical temp dir");
        let database = root.join("nested/database");
        crate::io::durable_dir::take_synced_dirs();

        // Act
        let _fs = RealFs::new(&database).expect("create filesystem root");

        // Assert: creating the database root must sync its parent chain before
        // a successful write can rely on files beneath it.
        let synced = crate::io::durable_dir::take_synced_dirs();
        assert!(synced.contains(&root));
        assert!(synced.contains(&root.join("nested")));
    }

    #[test]
    fn should_sync_parent_when_opening_an_existing_filesystem_root() {
        // Arrange: another component created the root, but its parent entry
        // may not yet have been synced.
        let temp = TempDir::new().expect("temp dir");
        let root = std::fs::canonicalize(temp.path()).expect("canonical temp dir");
        let database = root.join("database");
        std::fs::create_dir(&database).expect("create database root");
        crate::io::durable_dir::take_synced_dirs();

        // Act
        let _fs = RealFs::new(&database).expect("open filesystem root");

        // Assert
        assert!(crate::io::durable_dir::take_synced_dirs().contains(&root));
    }

    #[cfg(unix)]
    #[test]
    fn should_sync_configured_parent_when_filesystem_root_is_a_symlink() {
        use std::os::unix::fs::symlink;

        // Arrange
        let link_parent = TempDir::new().expect("link parent");
        let target_parent = TempDir::new().expect("target parent");
        let link_parent = std::fs::canonicalize(link_parent.path()).expect("canonical link parent");
        let target = target_parent.path().join("database");
        std::fs::create_dir(&target).expect("database target");
        let configured_root = link_parent.join("database");
        symlink(&target, &configured_root).expect("database root symlink");
        crate::io::durable_dir::take_synced_dirs();

        // Act
        let _fs = RealFs::new(&configured_root).expect("open symlinked root");

        // Assert
        assert!(crate::io::durable_dir::take_synced_dirs().contains(&link_parent));
    }

    #[test]
    fn should_read_written_file_when_writing() -> FsResult<()> {
        // Arrange
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        let path = FsPath::new("test.txt");

        // Act
        let mut file = fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )?;

        file.append(bytes::Bytes::from("hello"))?;
        drop(file);

        let file = fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;

        let data = file.read_at(0, 5)?;

        // Assert
        assert_eq!(data, bytes::Bytes::from("hello"));
        Ok(())
    }

    #[test]
    fn should_sanitize_path_traversal() -> FsResult<()> {
        // Arrange
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        let path = FsPath::new("../escape.txt");

        // Act
        let mut file = fs.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )?;

        file.append(bytes::Bytes::from("data"))?;
        drop(file);

        // Assert
        // File should be in temp dir, not parent
        assert!(fs.exists(&FsPath::new("escape.txt"))?);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn should_reject_open_through_symlinked_existing_component_given_real_filesystem_when_opening(
    ) -> FsResult<()> {
        // Arrange
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let outside = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        std::os::unix::fs::symlink(outside.path(), temp.path().join("link"))
            .map_err(|e| FsError::Io(e.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        // Act
        let result = fs.open(
            &FsPath::new("link/escaped.txt"),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        );

        // Assert
        assert!(result.is_err());
        assert!(!outside.path().join("escaped.txt").exists());
        Ok(())
    }

    #[test]
    fn should_atomically_replace_existing_target_when_renaming() -> FsResult<()> {
        // Arrange
        let temp = TempDir::new().map_err(|error| FsError::Io(error.to_string()))?;
        let fs = RealFs::new(temp.path())?;
        std::fs::write(temp.path().join("source.tmp"), b"new")
            .map_err(|error| FsError::Io(error.to_string()))?;
        std::fs::write(temp.path().join("target"), b"old")
            .map_err(|error| FsError::Io(error.to_string()))?;

        // Act
        fs.rename_atomic(&FsPath::new("source.tmp"), &FsPath::new("target"))?;

        // Assert
        let contents = std::fs::read(temp.path().join("target"))
            .map_err(|error| FsError::Io(error.to_string()))?;
        assert_eq!(contents, b"new");
        assert!(!temp.path().join("source.tmp").exists());
        Ok(())
    }

    #[test]
    fn should_distinguish_missing_rename_source_from_destination_parent() {
        // Arrange
        let temp = TempDir::new().expect("temp dir");
        let source = temp.path().join("source.tmp");
        std::fs::write(&source, b"source").expect("write source");
        let missing_destination_parent = temp.path().join("missing/target");
        let not_found = io::Error::new(io::ErrorKind::NotFound, "component missing");

        // Act
        let missing_parent = rename_err(&source, &missing_destination_parent, &not_found);
        let missing_source = rename_err(
            &temp.path().join("missing-source"),
            &temp.path().join("target"),
            &not_found,
        );

        // Assert
        assert!(
            matches!(missing_parent, FsError::Io(_)),
            "{missing_parent:?}"
        );
        assert!(
            matches!(missing_source, FsError::NotFound(_)),
            "{missing_source:?}"
        );
    }

    #[test]
    fn should_preserve_open_reader_when_atomically_replacing_target() -> FsResult<()> {
        // Arrange
        let temp = TempDir::new().map_err(|error| FsError::Io(error.to_string()))?;
        let fs = RealFs::new(temp.path())?;
        let target = FsPath::new("leader-記録");
        std::fs::write(temp.path().join(&target.0), b"old leader")
            .map_err(|error| FsError::Io(error.to_string()))?;
        let reader = fs.open(
            &target,
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        let original_len = reader.len()?;

        // Act
        for replacement in [b"new leader with longer contents".as_slice(), b"latest"] {
            std::fs::write(temp.path().join("source.tmp"), replacement)
                .map_err(|error| FsError::Io(error.to_string()))?;
            fs.rename_atomic(&FsPath::new("source.tmp"), &target)?;

            // Assert
            assert_eq!(reader.len()?, original_len);
            assert_eq!(reader.read_at(0, original_len)?.as_ref(), b"old leader");
            assert_eq!(
                std::fs::read(temp.path().join(&target.0))
                    .map_err(|error| FsError::Io(error.to_string()))?,
                replacement
            );
            assert!(!temp.path().join("source.tmp").exists());
        }
        Ok(())
    }

    #[test]
    fn should_classify_missing_file_as_not_found() -> FsResult<()> {
        // Arrange
        let temp = TempDir::new().map_err(|error| FsError::Io(error.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        // Act
        let result = fs.open(
            &FsPath::new("missing.txt"),
            OpenOptions {
                mode: OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        );

        // Assert
        assert!(matches!(result, Err(FsError::NotFound(_))));
        Ok(())
    }

    #[test]
    fn should_classify_missing_metadata_as_not_found() -> FsResult<()> {
        // Arrange
        let temp = TempDir::new().map_err(|error| FsError::Io(error.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        // Act
        let result = fs.metadata(&FsPath::new("missing.txt"));

        // Assert
        assert!(matches!(result, Err(FsError::NotFound(_))));
        Ok(())
    }

    #[test]
    fn should_classify_missing_removal_as_not_found() -> FsResult<()> {
        // Arrange
        let temp = TempDir::new().map_err(|error| FsError::Io(error.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        // Act
        let result = fs.remove_file(&FsPath::new("missing.txt"));

        // Assert
        assert!(matches!(result, Err(FsError::NotFound(_))));
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn should_sync_directory_with_a_writable_backup_handle() -> FsResult<()> {
        // Arrange
        let temp = TempDir::new().map_err(|error| FsError::Io(error.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        // Assert: syncing a directory that does not exist must fail, proving
        // sync_dir genuinely opens a handle to the target (with
        // FILE_FLAG_BACKUP_SEMANTICS) rather than being a stubbed no-op.
        let missing = fs.sync_dir(&FsPath::new("does-not-exist"), Durability::Durable);
        assert!(missing.is_err());

        // Act
        fs.sync_dir(&FsPath::new("."), Durability::Durable)?;

        // Assert
        Ok(())
    }
}
