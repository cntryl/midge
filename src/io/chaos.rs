//! Chaos-injection wrapper for filesystem implementations
//!
//! Wraps any `Fs` implementation and injects failures for testing resilience.

use super::traits::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Chaos-injecting wrapper around an `Fs` implementation
pub struct ChaosFs {
    inner: Arc<dyn Fs>,
    fail_every: usize,
    counters: ChaosCounters,
}

#[derive(Debug)]
struct ChaosCounters {
    open: AtomicUsize,
    read: AtomicUsize,
    write: AtomicUsize,
    delete: AtomicUsize,
    list: AtomicUsize,
    rename: AtomicUsize,
}

impl ChaosFs {
    /// Create a new chaos wrapper
    ///
    /// # Arguments
    /// - `inner`: wrapped filesystem
    /// - `fail_every`: inject failure every N operations (0 = never fail)
    pub fn new(inner: Arc<dyn Fs>, fail_every: usize) -> Self {
        Self {
            inner,
            fail_every,
            counters: ChaosCounters {
                open: AtomicUsize::new(0),
                read: AtomicUsize::new(0),
                write: AtomicUsize::new(0),
                delete: AtomicUsize::new(0),
                list: AtomicUsize::new(0),
                rename: AtomicUsize::new(0),
            },
        }
    }

    fn should_fail_open(&self) -> bool {
        self.fail_every != 0
            && self.counters.open.fetch_add(1, Ordering::Relaxed).is_multiple_of(self.fail_every)
    }

    fn should_fail_read(&self) -> bool {
        self.fail_every != 0
            && self.counters.read.fetch_add(1, Ordering::Relaxed).is_multiple_of(self.fail_every)
    }

    fn should_fail_write(&self) -> bool {
        self.fail_every != 0
            && self.counters.write.fetch_add(1, Ordering::Relaxed).is_multiple_of(self.fail_every)
    }

    fn should_fail_delete(&self) -> bool {
        self.fail_every != 0
            && self.counters.delete.fetch_add(1, Ordering::Relaxed).is_multiple_of(self.fail_every)
    }

    fn should_fail_list(&self) -> bool {
        self.fail_every != 0
            && self.counters.list.fetch_add(1, Ordering::Relaxed).is_multiple_of(self.fail_every)
    }

    fn should_fail_rename(&self) -> bool {
        self.fail_every != 0
            && self.counters.rename.fetch_add(1, Ordering::Relaxed).is_multiple_of(self.fail_every)
    }

    /// Reset all failure counters
    #[allow(dead_code)]
    pub fn reset_counters(&self) {
        self.counters.open.store(0, Ordering::Relaxed);
        self.counters.read.store(0, Ordering::Relaxed);
        self.counters.write.store(0, Ordering::Relaxed);
        self.counters.delete.store(0, Ordering::Relaxed);
        self.counters.list.store(0, Ordering::Relaxed);
        self.counters.rename.store(0, Ordering::Relaxed);
    }
}

impl Fs for ChaosFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        if self.should_fail_open() {
            return Err(FsError::Unavailable("chaos: open failed".into()));
        }
        let file = self.inner.open(path, opts)?;
        Ok(Box::new(ChaosFile {
            inner: file,
            chaos: self,
        }))
    }

    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        if self.should_fail_delete() {
            return Err(FsError::Unavailable("chaos: delete failed".into()));
        }
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
        if self.should_fail_list() {
            return Err(FsError::Unavailable("chaos: list failed".into()));
        }
        self.inner.list_dir(path)
    }

    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        if self.should_fail_delete() {
            return Err(FsError::Unavailable("chaos: rmdir failed".into()));
        }
        self.inner.remove_dir_all(path)
    }

    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        self.inner.sync_dir(path, dur)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        if self.should_fail_rename() {
            return Err(FsError::Unavailable("chaos: rename failed".into()));
        }
        self.inner.rename_atomic(from, to)
    }
}

pub struct ChaosFile<'a> {
    inner: Box<dyn File + 'a>,
    chaos: &'a ChaosFs,
}

impl<'a> File for ChaosFile<'a> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<bytes::Bytes> {
        if self.chaos.should_fail_read() {
            return Err(FsError::Unavailable("chaos: read failed".into()));
        }
        self.inner.read_at(offset, len)
    }

    fn write_at(&mut self, offset: u64, data: bytes::Bytes) -> FsResult<()> {
        if self.chaos.should_fail_write() {
            return Err(FsError::Unavailable("chaos: write failed".into()));
        }
        self.inner.write_at(offset, data)
    }

    fn append(&mut self, data: bytes::Bytes) -> FsResult<u64> {
        if self.chaos.should_fail_write() {
            return Err(FsError::Unavailable("chaos: append failed".into()));
        }
        self.inner.append(data)
    }

    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }

    fn sync(&mut self, dur: Durability) -> FsResult<()> {
        self.inner.sync(dur)
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        self.inner.close()
    }

    fn caps(&self) -> FileCaps {
        self.inner.caps()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::mock::MockFs;

    #[test]
    fn should_inject_open_failure() -> FsResult<()> {
        let inner = Arc::new(MockFs::new());
        let chaos = ChaosFs::new(inner, 1); // fail every 1st operation

        let path = FsPath::new("test.txt");
        let result = chaos.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        );

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn should_pass_through_when_no_fail() -> FsResult<()> {
        let inner = Arc::new(MockFs::new());
        let chaos = ChaosFs::new(inner, 0); // never fail

        let path = FsPath::new("test.txt");
        let result = chaos.open(
            &path,
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        );

        assert!(result.is_ok());
        Ok(())
    }
}
