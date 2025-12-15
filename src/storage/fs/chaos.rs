use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;

use super::{DirEntry, Durability, File, Fs, FsError, FsPath, FsResult, FileCaps, IoSlice, IoSliceMut, Metadata, OpenOptions, ReadRange};

#[derive(Debug)]
pub struct ChaosFs<F> {
    inner: Arc<F>,
    fail_every: usize, // fail every N operations
    counter: AtomicUsize,
}

impl<F> ChaosFs<F> {
    pub fn new(inner: Arc<F>, fail_every: usize) -> Self {
        Self {
            inner,
            fail_every,
            counter: AtomicUsize::new(0),
        }
    }

    fn should_fail(&self) -> bool {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        self.fail_every != 0 && count.is_multiple_of(self.fail_every)
    }
}

impl<F: Fs> Fs for ChaosFs<F> {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        if self.should_fail() {
            return Err(FsError::Unavailable("chaos injection".to_string()));
        }
        let file = self.inner.open(path, opts)?;
        Ok(Box::new(ChaosFile {
            inner: file,
            fail_every: self.fail_every,
            counter: AtomicUsize::new(0),
        }))
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        if self.should_fail() {
            return Err(FsError::Unavailable("chaos injection".to_string()));
        }
        self.inner.remove_file(path)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        if self.should_fail() {
            return Err(FsError::Unavailable("chaos injection".to_string()));
        }
        self.inner.exists(path)
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        if self.should_fail() {
            return Err(FsError::Unavailable("chaos injection".to_string()));
        }
        self.inner.metadata(path)
    }

    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        if self.should_fail() {
            return Err(FsError::Unavailable("chaos injection".to_string()));
        }
        self.inner.create_dir_all(path)
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        if self.should_fail() {
            return Err(FsError::Unavailable("chaos injection".to_string()));
        }
        self.inner.list_dir(path)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        if self.should_fail() {
            return Err(FsError::Unavailable("chaos injection".to_string()));
        }
        self.inner.remove_dir_all(path)
    }

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        if self.should_fail() {
            return Err(FsError::Unavailable("chaos injection".to_string()));
        }
        self.inner.sync_dir(path, dur)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        if self.should_fail() {
            return Err(FsError::Unavailable("chaos injection".to_string()));
        }
        self.inner.rename_atomic(from, to)
    }
}

pub struct ChaosFile<'a> {
    inner: Box<dyn File + 'a>,
    fail_every: usize,
    counter: AtomicUsize,
}

impl<'a> ChaosFile<'a> {
    fn should_fail(&self) -> bool {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        self.fail_every != 0 && count.is_multiple_of(self.fail_every)
    }
}

impl<'a> File for ChaosFile<'a> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        if self.should_fail() {
            return Err(FsError::Corruption("chaos injection".to_string()));
        }
        self.inner.read_at(offset, len)
    }

    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        if self.should_fail() {
            return Err(FsError::Io("chaos injection".to_string()));
        }
        self.inner.write_at(offset, data)
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        if self.should_fail() {
            return Err(FsError::Io("chaos injection".to_string()));
        }
        self.inner.append(data)
    }

    fn len(&self) -> FsResult<u64> {
        if self.should_fail() {
            return Err(FsError::Io("chaos injection".to_string()));
        }
        self.inner.len()
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        if self.should_fail() {
            return Err(FsError::Io("chaos injection".to_string()));
        }
        self.inner.sync(dur)
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        self.inner.close()
    }

    fn writev_at(&mut self, offset: u64, bufs: &[IoSlice<'_>]) -> FsResult<u64> {
        if self.should_fail() {
            return Err(FsError::Io("chaos injection".to_string()));
        }
        self.inner.writev_at(offset, bufs)
    }

    fn appendv(&mut self, bufs: &[IoSlice<'_>]) -> FsResult<u64> {
        if self.should_fail() {
            return Err(FsError::Io("chaos injection".to_string()));
        }
        self.inner.appendv(bufs)
    }

    fn readv_at(&self, offset: u64, bufs: &mut [IoSliceMut<'_>]) -> FsResult<u64> {
        if self.should_fail() {
            return Err(FsError::Corruption("chaos injection".to_string()));
        }
        self.inner.readv_at(offset, bufs)
    }

    fn read_ranges(&self, ranges: &[ReadRange]) -> FsResult<Vec<bytes::Bytes>> {
        if self.should_fail() {
            return Err(FsError::Corruption("chaos injection".to_string()));
        }
        self.inner.read_ranges(ranges)
    }

    fn caps(&self) -> FileCaps { self.inner.caps() }
}