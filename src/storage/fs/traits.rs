use bytes::Bytes;
use thiserror::Error;

pub use std::io::{IoSlice, IoSliceMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    Unsafe,
    Durable,
}

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

/// Your own tiny path type helps keep this portable (vs std::path leaking everywhere).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FsPath(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    pub mode: OpenMode,
    pub create: bool,
    pub create_new: bool,
    pub truncate: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct ReadRange {
    pub offset: u64,
    pub len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileCaps(u32);

impl FileCaps {
    pub const READV_AT: FileCaps = FileCaps(1 << 0);
    pub const WRITEV_AT: FileCaps = FileCaps(1 << 1);
    pub const APPENDV: FileCaps = FileCaps(1 << 2);
    pub const READ_RANGES: FileCaps = FileCaps(1 << 3);

    pub const fn empty() -> Self {
        FileCaps(0)
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }
}

pub trait Fs: Send + Sync + 'static {
    // --- Files ---
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>>;
    fn remove_file(&self, path: &FsPath) -> FsResult<()>;
    fn exists(&self, path: &FsPath) -> FsResult<bool>;
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata>;

    // --- Directories ---
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()>;
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>>;
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()>;
    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()>;

    // --- Atomicity ---
    /// Must be atomic within the same filesystem/volume when supported.
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()>;
}

#[derive(Debug, Clone)]
pub struct Metadata {
    pub len: u64,
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

pub trait File: Send {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes>;
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()>;

    /// Common fast-path write (append-only logs).
    fn append(&mut self, data: Bytes) -> FsResult<u64>; // returns starting offset

    fn len(&self) -> FsResult<u64>;

    fn is_empty(&self) -> FsResult<bool> {
        Ok(self.len()? == 0)
    }

    /// The durability boundary (fsync / flush / multipart commit / etc.)
    fn sync(&mut self, dur: Durability) -> FsResult<()>;

    fn close(self: Box<Self>) -> FsResult<()>;

    // NEW: vectored write at an explicit offset
    fn writev_at(&mut self, offset: u64, bufs: &[IoSlice<'_>]) -> FsResult<u64> {
        // default: coalesce into one buffer (slow but correct)
        let mut total = 0usize;
        for b in bufs { total += b.len(); }
        let mut tmp = Vec::with_capacity(total);
        for b in bufs { tmp.extend_from_slice(b); }
        self.write_at(offset, Bytes::from(tmp))?;
        Ok(total as u64)
    }

    // NEW: vectored append (WAL hot path)
    fn appendv(&mut self, bufs: &[IoSlice<'_>]) -> FsResult<u64> {
        // default: coalesce
        let mut total = 0usize;
        for b in bufs { total += b.len(); }
        let mut tmp = Vec::with_capacity(total);
        for b in bufs { tmp.extend_from_slice(b); }
        self.append(Bytes::from(tmp))
    }

    // NEW: vectored read into caller-provided buffers
    fn readv_at(&self, offset: u64, bufs: &mut [IoSliceMut<'_>]) -> FsResult<u64> {
        // default: fall back to scalar read then copy out
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

    fn read_ranges(&self, ranges: &[ReadRange]) -> FsResult<Vec<Bytes>> {
        // default: N scalar reads
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.read_at(r.offset, r.len as u64)?);
        }
        Ok(out)
    }

    fn caps(&self) -> FileCaps { FileCaps::empty() }
}