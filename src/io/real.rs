//! Real filesystem implementation
//!
//! Direct mapping to std::fs with path sanitization.
//! Suitable for production use.
//!
//! Notes:
//! - `read_at` / `write_at` use true positional IO when available (no shared cursor):
//!   - Unix: std::os::unix::fs::FileExt::{read_at, write_at}
//!   - Windows: std::os::windows::fs::FileExt::{seek_read, seek_write}
//! - `sync_dir` is implemented on Unix (fsync dir). On Windows it remains best-effort/no-op.

use super::traits::*;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

/// Real filesystem backend
pub struct RealFs {
    base_path: PathBuf,
}

impl RealFs {
    /// Create a new real filesystem rooted at `base_path`
    pub fn new(base_path: impl AsRef<Path>) -> FsResult<Self> {
        let path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&path).map_err(|e| io_err("create_dir_all", &path, e))?;
        Ok(Self { base_path: path })
    }

    /// Compute sanitized full path, preventing directory traversal.
    ///
    /// Current policy (drop-in compatible with your tests):
    /// - keeps `Normal` components
    /// - ignores `.` and any attempts to traverse (`..`, roots, prefixes)
    fn full_path(&self, rel: &FsPath) -> PathBuf {
        let mut out = self.base_path.clone();
        for component in Path::new(&rel.0).components() {
            match component {
                Component::Normal(part) => out.push(part),
                Component::CurDir => {}
                Component::ParentDir => {}
                Component::RootDir => {}
                Component::Prefix(_) => {}
            }
        }
        out
    }

    /// Best-effort parent directory extraction for directory fsync barriers.
    fn parent_dir(full_path: &Path) -> Option<&Path> {
        full_path.parent().filter(|p| !p.as_os_str().is_empty())
    }
}

impl Fs for RealFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        let full = self.full_path(path);

        // Ensure parent directory exists when creating.
        if opts.create || opts.create_new {
            if let Some(parent) = Self::parent_dir(&full) {
                fs::create_dir_all(parent).map_err(|e| io_err("create_dir_all", parent, e))?;
            }
        }

        let mut std_opts = fs::OpenOptions::new();

        match opts.mode {
            OpenMode::ReadOnly => {
                std_opts.read(true);
            }
            OpenMode::ReadWrite => {
                std_opts.read(true).write(true);
            }
        }

        if opts.create {
            std_opts.create(true);
        }
        if opts.create_new {
            std_opts.create_new(true);
        }
        if opts.truncate {
            std_opts.truncate(true);
        }

        let file = std_opts.open(&full).map_err(|e| io_err("open", &full, e))?;

        Ok(Box::new(RealFile { file }))
    }

    fn open_persistent_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        // For real FS, a file handle is independently owned and therefore 'static.
        let full = self.full_path(path);

        // Ensure parent dir
        if opts.create || opts.create_new {
            if let Some(parent) = Self::parent_dir(&full) {
                fs::create_dir_all(parent).map_err(|e| io_err("create_dir_all", parent, e))?;
            }
        }

        let mut std_opts = fs::OpenOptions::new();
        match opts.mode {
            OpenMode::ReadOnly => std_opts.read(true),
            OpenMode::ReadWrite => std_opts.read(true).write(true),
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
        let file = std_opts.open(&full).map_err(|e| io_err("open", &full, e))?;
        Ok(Box::new(RealFile { file }))
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        let full = self.full_path(path);
        fs::remove_file(&full).map_err(|e| io_err("remove_file", &full, e))
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        Ok(self.full_path(path).exists())
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let full = self.full_path(path);
        let meta = fs::metadata(&full).map_err(|e| io_err("metadata", &full, e))?;
        Ok(Metadata { len: meta.len() })
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let full = self.full_path(path);
        fs::create_dir_all(&full).map_err(|e| io_err("create_dir_all", &full, e))
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let full = self.full_path(path);
        let entries = fs::read_dir(&full)
            .map_err(|e| io_err("read_dir", &full, e))?
            .map(|entry| {
                let entry = entry.map_err(|e| io_err("read_dir_entry", &full, e))?;
                let name = entry.file_name().to_string_lossy().to_string();
                let is_dir = entry
                    .file_type()
                    .map_err(|e| io_err("file_type", &full, e))?
                    .is_dir();
                Ok(DirEntry { name, is_dir })
            })
            .collect::<Result<Vec<_>, FsError>>()?;
        Ok(entries)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let full = self.full_path(path);
        fs::remove_dir_all(&full).map_err(|e| io_err("remove_dir_all", &full, e))
    }

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        if dur == Durability::Unsafe {
            return Ok(());
        }

        let full = self.full_path(path);

        // Unix: directory fsync barrier.
        #[cfg(unix)]
        {
            // fs::File::open works for directories on Unix.
            let dir = fs::File::open(&full).map_err(|e| io_err("open_dir", &full, e))?;
            dir.sync_all().map_err(|e| io_err("fsync_dir", &full, e))?;
            Ok(())
        }

        // Windows: std doesn't reliably support directory handles with fsync semantics.
        #[cfg(windows)]
        {
            let _ = full;
            Ok(())
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = full;
            Ok(())
        }
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let from_full = self.full_path(from);
        let to_full = self.full_path(to);

        // Ensure destination parent exists (helps callers that assume it).
        if let Some(parent) = Self::parent_dir(&to_full) {
            fs::create_dir_all(parent).map_err(|e| io_err("create_dir_all", parent, e))?;
        }

        fs::rename(&from_full, &to_full).map_err(|e| io_err("rename", &to_full, e))
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
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers

fn io_err(op: &str, path: &Path, e: io::Error) -> FsError {
    FsError::Io(format!("{op} {}: {e}", path.display()))
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
    use tempfile::TempDir;

    #[test]
    fn should_create_real_fs() -> FsResult<()> {
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let _fs = RealFs::new(temp.path())?;
        Ok(())
    }

    #[test]
    fn should_read_written_file_when_writing() -> FsResult<()> {
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        let path = FsPath::new("test.txt");
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
        assert_eq!(data, bytes::Bytes::from("hello"));
        Ok(())
    }

    #[test]
    fn should_sanitize_path_traversal() -> FsResult<()> {
        let temp = TempDir::new().map_err(|e| FsError::Io(e.to_string()))?;
        let fs = RealFs::new(temp.path())?;

        let path = FsPath::new("../escape.txt");
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

        // File should be in temp dir, not parent
        assert!(fs.exists(&FsPath::new("escape.txt"))?);
        Ok(())
    }
}
