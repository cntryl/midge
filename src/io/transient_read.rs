//! Test filesystem that fails reads past an offset with a transient I/O error.

use super::traits::{
    DirEntry, Durability, File, Fs, FsError, FsPath, FsResult, Metadata, OpenOptions,
};
use bytes::Bytes;

/// Wraps a real filesystem and fails every read that reaches `fail_from`.
pub(crate) struct TransientReadFs {
    pub(crate) inner: super::RealFs,
    pub(crate) fail_from: u64,
}

struct TransientReadFile<'a> {
    inner: Box<dyn File + 'a>,
    fail_from: u64,
}

impl File for TransientReadFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        if offset + len > self.fail_from {
            return Err(FsError::Io("transient EIO".into()));
        }
        self.inner.read_at(offset, len)
    }
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        self.inner.write_at(offset, data)
    }
    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        self.inner.append(data)
    }
    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }
    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        self.inner.sync(durability)
    }
    fn close(self: Box<Self>) -> FsResult<()> {
        self.inner.close()
    }
}

impl Fs for TransientReadFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        Ok(Box::new(TransientReadFile {
            inner: self.inner.open(path, opts)?,
            fail_from: self.fail_from,
        }))
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(path)
    }
    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(path)
    }
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(path)
    }
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_dir_all(path)
    }
    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        self.inner.sync_dir(path, durability)
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}
