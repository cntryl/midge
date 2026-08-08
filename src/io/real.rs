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
    DirEntry, Durability, File, FileCaps, Fs, FsError, FsPath, FsResult, Metadata, OpenOptions,
};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

/// Real filesystem backend
pub struct RealFs {
    base_path: PathBuf,
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
    pub fn new(base_path: impl AsRef<Path>) -> FsResult<Self> {
        let path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&path).map_err(|e| io_err("create_dir_all", &path, &e))?;
        let path =
            fs::canonicalize(path).map_err(|e| io_err("canonicalize", base_path.as_ref(), &e))?;
        Ok(Self { base_path: path })
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
        let path =
            fs::canonicalize(path).map_err(|e| io_err("canonicalize", base_path.as_ref(), &e))?;
        Ok(Self { base_path: path })
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
                std::fs::create_dir_all(parent)
                    .map_err(|e| super::traits::FsError::Io(format!("create_dir_all: {e}")))?;
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

        let file = std_opts.open(&full).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                super::traits::FsError::NotFound(format!("{}: {error}", full.display()))
            } else {
                super::traits::FsError::Io(format!("open {}: {error}", full.display()))
            }
        })?;
        Ok(Box::new(RealFile { file }))
    }
}

impl Fs for RealFs {
    fn coordination_key(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.base_path.hash(&mut hasher);
        hasher.finish()
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
        Ok(Metadata { len: meta.len() })
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let full = self.full_path(path)?;
        fs::create_dir_all(&full).map_err(|e| io_err("create_dir_all", &full, &e))
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let full = self.full_path(path)?;
        let entries = fs::read_dir(&full)
            .map_err(|e| io_err("read_dir", &full, &e))?
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

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let full = self.full_path(path)?;
        fs::remove_dir_all(&full).map_err(|e| io_err("remove_dir_all", &full, &e))
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
            fs::create_dir_all(parent).map_err(|e| io_err("create_dir_all", parent, &e))?;
        }

        #[cfg(windows)]
        {
            use winapi::um::winbase::{
                MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
            };

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
                return Err(io_err("rename", &to_full, &error));
            }
            Ok(())
        }

        #[cfg(not(windows))]
        {
            fs::rename(&from_full, &to_full).map_err(|e| io_err("rename", &to_full, &e))
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

impl File for RealFile {
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
                .map_err(|e| FsError::Io(format!("seek(read_at) offset={offset}: {e}")))?;
            file.read_exact(&mut buf)
                .map_err(|e| FsError::Io(format!("read_exact(read_at) len={len}: {e}")))?;
            Ok(bytes::Bytes::from(buf))
        }
    }

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
                .map_err(|e| FsError::Io(format!("seek(write_at) offset={offset}: {e}")))?;
            self.file.write_all(bytes).map_err(|e| {
                FsError::Io(format!("write_all(write_at) len={}: {e}", bytes.len()))
            })?;
            Ok(())
        }
    }

    fn append(&mut self, data: bytes::Bytes) -> FsResult<u64> {
        // Keep as cursor-based: append implies a shared logical end anyway.
        use std::io::{Seek, SeekFrom, Write};

        let pos = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|e| FsError::Io(format!("seek(append): {e}")))?;

        self.file
            .write_all(data.as_ref())
            .map_err(|e| FsError::Io(format!("write_all(append) len={}: {e}", data.len())))?;

        Ok(pos)
    }

    fn len(&self) -> FsResult<u64> {
        let meta = self
            .file
            .metadata()
            .map_err(|e| FsError::Io(format!("metadata(len): {e}")))?;
        Ok(meta.len())
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        match dur {
            Durability::Unsafe => Ok(()),
            Durability::Durable => self
                .file
                .sync_all()
                .map_err(|e| FsError::Io(format!("sync_all: {e}"))),
        }
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        Ok(())
    }

    fn caps(&self) -> FileCaps {
        // Keep conservative. If your FileCaps has flags, add them here.
        FileCaps::empty()
    }

    fn try_lock_exclusive(&self) -> FsResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
            if result != 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    return Err(FsError::AlreadyExists("file is already locked".to_string()));
                }
                return Err(FsError::Io(format!("flock failed: {err}")));
            }
            Ok(())
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use winapi::um::fileapi::LockFileEx;

            const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;
            const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x0000_0001;

            let handle = self.file.as_raw_handle();
            let mut overlapped: winapi::um::minwinbase::OVERLAPPED = unsafe { std::mem::zeroed() };
            let result = unsafe {
                LockFileEx(
                    handle.cast(),
                    LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                    0,
                    !0,
                    !0,
                    std::ptr::addr_of_mut!(overlapped),
                )
            };
            if result == 0 {
                return Err(FsError::AlreadyExists("file is already locked".to_string()));
            }
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            Err(FsError::Unsupported(
                "file locking not supported on this platform".to_string(),
            ))
        }
    }

    fn unlock(&self) -> FsResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let result = unsafe { libc::flock(fd, libc::LOCK_UN) };
            if result != 0 {
                return Err(FsError::Io(format!(
                    "unlock failed: {}",
                    std::io::Error::last_os_error()
                )));
            }
            Ok(())
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use winapi::um::fileapi::UnlockFileEx;

            let handle = self.file.as_raw_handle();
            let mut overlapped: winapi::um::minwinbase::OVERLAPPED = unsafe { std::mem::zeroed() };
            let result = unsafe {
                UnlockFileEx(handle.cast(), 0, !0, !0, std::ptr::addr_of_mut!(overlapped))
            };
            if result == 0 {
                return Err(FsError::Io("unlock failed".to_string()));
            }
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            Ok(()) // No-op on unsupported platforms
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers

fn io_err(op: &str, path: &Path, e: &io::Error) -> FsError {
    FsError::Io(format!("{op} {}: {e}", path.display()))
}

fn file_op_err(op: &str, path: &Path, error: &io::Error) -> FsError {
    let message = format!("{op} {}: {error}", path.display());
    match error.kind() {
        io::ErrorKind::NotFound => FsError::NotFound(message),
        io::ErrorKind::AlreadyExists => FsError::AlreadyExists(message),
        _ => FsError::Io(message),
    }
}

#[cfg(unix)]
fn read_exact_at_unix(file: &fs::File, mut offset: u64, mut dst: &mut [u8]) -> FsResult<()> {
    use std::os::unix::fs::FileExt;

    while !dst.is_empty() {
        let n = file
            .read_at(dst, offset)
            .map_err(|e| FsError::Io(format!("pread offset={offset}: {e}")))?;
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
            .map_err(|e| FsError::Io(format!("pwrite offset={offset}: {e}")))?;
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
            .map_err(|e| FsError::Io(format!("seek_read offset={offset}: {e}")))?;
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
            .map_err(|e| FsError::Io(format!("seek_write offset={offset}: {e}")))?;
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
    use super::*;
    use crate::io::OpenMode;
    use tempfile::TempDir;

    #[test]
    fn should_create_real_fs() -> FsResult<()> {
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let _fs = RealFs::new(temp.path())?;
        Ok(())
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

        // Act
        fs.sync_dir(&FsPath::new("."), Durability::Durable)?;

        // Assert
        Ok(())
    }
}
