//! Local filesystem implementation of the `Storage` trait (abstraction layer).
//!
//! **NOT on the hot path.** This implementation is used only by WAL recovery tests
//! and kept for contract compatibility with the `Storage` trait.
//!
//! Design is intentionally conservative and portable:
//! - Uses a per-handle mutex for correctness (slow).
//! - Does not assume POSIX directory fsync semantics.
//! - Provides explicit durability via `sync()`.
//!
//! For actual hot-path I/O, use `FileSystem` which implements `StorageBackend`.

use crate::storage::abstraction::{
    Atomicity, DirEntry, DirectorySupport, Durability, FileCapabilities, OpenDisposition, OpenMode,
    OpenOptions, ReadRange, RenameOptions, RenameReport, Storage, StorageCapabilities,
    StorageError, StorageErrorKind, StorageFile, StoragePath, StorageResult, SyncMode,
    VectoredIoCapabilities,
};
use parking_lot::Mutex;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

/// Local filesystem-backed `Storage` rooted at `root`.
#[derive(Debug, Clone)]
pub(crate) struct LocalFsStorage {
    root: PathBuf,
}

impl LocalFsStorage {
    pub(crate) fn new(root: impl AsRef<Path>) -> StorageResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|e| {
            StorageError::with_source(StorageErrorKind::Io, "create_dir_all(root)", e)
        })?;
        Ok(Self { root })
    }

    fn full_path(&self, rel: &StoragePath) -> PathBuf {
        // Treat `StoragePath` as a relative, forward-slash-friendly path.
        // Prevent absolute paths and traversal outside root.
        let mut out = self.root.clone();
        for component in Path::new(rel.as_str()).components() {
            match component {
                Component::Normal(part) => out.push(part),
                Component::CurDir
                | Component::ParentDir
                | Component::RootDir
                | Component::Prefix(_) => {}
            }
        }
        out
    }

    fn io_err(kind: StorageErrorKind, msg: impl Into<String>, err: std::io::Error) -> StorageError {
        StorageError::with_source(kind, msg, err)
    }

    fn map_fs_err(err: std::io::Error, ctx: &str) -> StorageError {
        use std::io::ErrorKind;
        match err.kind() {
            ErrorKind::NotFound => StorageError::new(StorageErrorKind::NotFound, ctx),
            ErrorKind::AlreadyExists => StorageError::new(StorageErrorKind::AlreadyExists, ctx),
            ErrorKind::PermissionDenied => {
                StorageError::new(StorageErrorKind::PermissionDenied, ctx)
            }
            _ => Self::io_err(StorageErrorKind::Io, ctx, err),
        }
    }
}

struct LocalFsFile {
    _path: PathBuf,
    file: Mutex<fs::File>,
}

impl LocalFsFile {
    fn new(path: PathBuf, file: fs::File) -> Self {
        Self {
            _path: path,
            file: Mutex::new(file),
        }
    }
}

impl StorageFile for LocalFsFile {
    fn capabilities(&self) -> FileCapabilities {
        // Portable slow path; optimized backends can override.
        FileCapabilities {
            vectored_io: VectoredIoCapabilities {
                read: false,
                write: false,
                append: false,
            },
            supports_read_ranges: false,
        }
    }

    fn read_at(&self, offset: u64, len: u64) -> StorageResult<Vec<u8>> {
        let mut file = self.file.lock();
        let meta = file
            .metadata()
            .map_err(|e| LocalFsStorage::map_fs_err(e, "metadata"))?;
        if offset > meta.len() {
            return Err(StorageError::new(
                StorageErrorKind::InvalidInput,
                "offset beyond file length",
            ));
        }

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| LocalFsStorage::map_fs_err(e, "seek"))?;

        let len = usize::try_from(len).map_err(|_| {
            StorageError::new(StorageErrorKind::InvalidInput, "read length exceeds usize")
        })?;
        let mut buf = vec![0u8; len];
        let mut filled = 0usize;
        while filled < buf.len() {
            let n = file
                .read(&mut buf[filled..])
                .map_err(|e| LocalFsStorage::map_fs_err(e, "read"))?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        buf.truncate(filled);
        Ok(buf)
    }

    fn read_ranges(&self, ranges: &[ReadRange]) -> StorageResult<Vec<Vec<u8>>> {
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.read_at(r.offset, u64::from(r.len))?);
        }
        Ok(out)
    }

    fn write_at(&mut self, offset: u64, data: &[u8]) -> StorageResult<u64> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| LocalFsStorage::map_fs_err(e, "seek"))?;

        let mut written = 0usize;
        while written < data.len() {
            let n = file
                .write(&data[written..])
                .map_err(|e| LocalFsStorage::map_fs_err(e, "write"))?;
            if n == 0 {
                return Err(StorageError::new(
                    StorageErrorKind::Io,
                    "write returned 0 bytes",
                ));
            }
            written += n;
        }
        Ok(written as u64)
    }

    fn append(&mut self, data: &[u8]) -> StorageResult<(u64, u64)> {
        let mut file = self.file.lock();
        let start = file
            .seek(SeekFrom::End(0))
            .map_err(|e| LocalFsStorage::map_fs_err(e, "seek_end"))?;

        let mut written = 0usize;
        while written < data.len() {
            let n = file
                .write(&data[written..])
                .map_err(|e| LocalFsStorage::map_fs_err(e, "write"))?;
            if n == 0 {
                return Err(StorageError::new(
                    StorageErrorKind::Io,
                    "write returned 0 bytes",
                ));
            }
            written += n;
        }
        Ok((start, written as u64))
    }

    fn len(&self) -> StorageResult<u64> {
        let file = self.file.lock();
        let meta = file
            .metadata()
            .map_err(|e| LocalFsStorage::map_fs_err(e, "metadata"))?;
        Ok(meta.len())
    }

    fn sync(&mut self, mode: SyncMode) -> StorageResult<()> {
        let file = self.file.lock();
        match mode {
            SyncMode::Data => file
                .sync_data()
                .map_err(|e| LocalFsStorage::map_fs_err(e, "sync_data")),
            SyncMode::DataAndMetadata => file
                .sync_all()
                .map_err(|e| LocalFsStorage::map_fs_err(e, "sync_all")),
        }
    }

    fn close(self: Box<Self>) -> StorageResult<()> {
        drop(self);
        Ok(())
    }
}

impl Storage for LocalFsStorage {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            directory_support: DirectorySupport::ListOnly,
            supports_atomic_rename: true,
            supports_append: true,
        }
    }

    fn open_file(
        &self,
        path: &StoragePath,
        options: OpenOptions,
    ) -> StorageResult<Box<dyn StorageFile>> {
        let full = self.full_path(path);

        if matches!(
            options.disposition,
            OpenDisposition::CreateIfMissing
                | OpenDisposition::CreateNew
                | OpenDisposition::TruncateExisting
        ) {
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| LocalFsStorage::map_fs_err(e, "create_dir_all(parent)"))?;
            }
        }

        let mut opts = fs::OpenOptions::new();
        match options.mode {
            OpenMode::ReadOnly => {
                opts.read(true);
            }
            OpenMode::ReadWrite => {
                opts.read(true).write(true);
            }
        }
        match options.disposition {
            OpenDisposition::OpenExisting => {}
            OpenDisposition::CreateIfMissing => {
                opts.create(true);
            }
            OpenDisposition::CreateNew => {
                opts.create_new(true);
            }
            OpenDisposition::TruncateExisting => {
                opts.truncate(true);
            }
        }
        opts.append(options.append);

        let file = opts
            .open(&full)
            .map_err(|e| LocalFsStorage::map_fs_err(e, "open"))?;
        Ok(Box::new(LocalFsFile::new(full, file)))
    }

    fn list_dir(&self, path: &StoragePath) -> StorageResult<Vec<DirEntry>> {
        let full = self.full_path(path);
        let iter = fs::read_dir(&full).map_err(|e| LocalFsStorage::map_fs_err(e, "read_dir"))?;
        let mut out = Vec::new();
        for entry in iter {
            let entry = entry.map_err(|e| LocalFsStorage::map_fs_err(e, "read_dir(entry)"))?;
            let file_type = entry
                .file_type()
                .map_err(|e| LocalFsStorage::map_fs_err(e, "file_type"))?;
            let name = entry.file_name().to_string_lossy().to_string();
            out.push(DirEntry {
                name,
                is_dir: file_type.is_dir(),
            });
        }
        Ok(out)
    }

    fn create_dir_all(&self, path: &StoragePath) -> StorageResult<()> {
        let full = self.full_path(path);
        fs::create_dir_all(&full).map_err(|e| LocalFsStorage::map_fs_err(e, "create_dir_all"))
    }

    fn sync_dir(&self, _path: &StoragePath, _mode: SyncMode) -> StorageResult<()> {
        Err(StorageError::unsupported(
            "directory sync is not supported by LocalFsStorage",
        ))
    }

    fn rename(
        &self,
        from: &StoragePath,
        to: &StoragePath,
        options: RenameOptions,
    ) -> StorageResult<RenameReport> {
        if options.require_atomic {
            // We assume within-root renames are atomic on local filesystems.
            // If a caller needs stronger guarantees (dir fsync), it must use storage-specific policy.
        }

        let from_full = self.full_path(from);
        let to_full = self.full_path(to);

        if options.overwrite {
            // Best-effort: remove destination first.
            let _ = fs::remove_file(&to_full);
        }

        fs::rename(&from_full, &to_full).map_err(|e| LocalFsStorage::map_fs_err(e, "rename"))?;

        Ok(RenameReport {
            atomicity: Atomicity::Guaranteed,
            durable: matches!(options.durability, Durability::Unsafe),
        })
    }
}
