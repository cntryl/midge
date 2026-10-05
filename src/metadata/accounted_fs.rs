//! Operation-scoped observation of delegated metadata filesystem mutations.
//! A per-operation view; capability delegation preserves deadline/root identity.

use super::accounting::{Ledger, Payload};
use crate::io::traits::{DirEntry, Metadata, ReadObserver};
use crate::io::{Durability, File, Fs, FsPath, FsResult, HostAddressing, OpenOptions};
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;

pub(crate) fn account_fs(inner: Arc<dyn Fs>, ledger: Arc<Mutex<Ledger>>) -> Arc<dyn Fs> {
    Arc::new(AccountedFs { inner, ledger })
}

struct AccountedFs {
    inner: Arc<dyn Fs>,
    ledger: Arc<Mutex<Ledger>>,
}
struct AccountedFile<'a> {
    inner: Box<dyn File + 'a>,
    ledger: Arc<Mutex<Ledger>>,
    payload: Payload,
}

// Guard integrity without attributing namespace/barrier work as payload bytes.
// Checking both sides also observes finish racing a live delegated mutation.
// No ledger lock is retained while the underlying filesystem executes.
fn observe_mutation<T>(
    ledger: &Mutex<Ledger>,
    mutation: impl FnOnce() -> FsResult<T>,
) -> FsResult<T> {
    ledger.lock().observe_mutation();
    let result = mutation();
    ledger.lock().observe_mutation();
    result
}

fn mutating_open(options: OpenOptions) -> bool {
    options.create || options.create_new || options.truncate
}

fn payload(path: &FsPath) -> Payload {
    if path.0.strip_suffix(".tmp") == Some(crate::metadata::files::MANIFEST_SNAPSHOT) {
        Payload::Snapshot
    } else if path.0 == crate::metadata::files::JOURNAL {
        Payload::Journal
    } else {
        Payload::OtherMetadata
    }
}

impl Fs for AccountedFs {
    fn local_output_view(&self) -> Option<Arc<dyn Fs>> {
        self.inner
            .local_output_view()
            .map(|inner| account_fs(inner, Arc::clone(&self.ledger)))
    }
    fn with_read_observer(&self, observer: Arc<dyn ReadObserver>) -> Option<Arc<dyn Fs>> {
        self.inner
            .with_read_observer(observer)
            .map(|inner| account_fs(inner, Arc::clone(&self.ledger)))
    }
    fn immutable_read_view(&self, path: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
        self.inner
            .immutable_read_view(path)
            .map(|view| view.map(|inner| account_fs(inner, Arc::clone(&self.ledger))))
    }
    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        self.inner.host_addressing()
    }
    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }
    fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let inner = if mutating_open(options) {
            observe_mutation(&self.ledger, || self.inner.open(path, options))
        } else {
            self.inner.open(path, options)
        }?;
        Ok(Box::new(AccountedFile {
            inner,
            ledger: Arc::clone(&self.ledger),
            payload: payload(path),
        }))
    }
    fn open_persistent_handle(
        &self,
        path: &FsPath,
        options: OpenOptions,
    ) -> FsResult<Box<dyn File>> {
        let inner = if mutating_open(options) {
            observe_mutation(&self.ledger, || {
                self.inner.open_persistent_handle(path, options)
            })
        } else {
            self.inner.open_persistent_handle(path, options)
        }?;
        Ok(Box::new(AccountedFile {
            inner,
            ledger: Arc::clone(&self.ledger),
            payload: payload(path),
        }))
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        observe_mutation(&self.ledger, || self.inner.remove_file(path))
    }
    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        observe_mutation(&self.ledger, || self.inner.create_dir_all(path))
    }
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(path)
    }
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        observe_mutation(&self.ledger, || self.inner.remove_dir_all(path))
    }
    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        observe_mutation(&self.ledger, || self.inner.sync_dir(path, durability))
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        observe_mutation(&self.ledger, || self.inner.rename_atomic(from, to))
    }
}

impl File for AccountedFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.inner.read_at(offset, len)
    }
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        let bytes = data.len();
        self.ledger.lock().issued(self.payload, bytes);
        let result = self.inner.write_at(offset, data);
        if result.is_ok() {
            self.ledger.lock().returned(self.payload, bytes);
        } else {
            self.ledger.lock().observe_mutation();
        }
        result
    }
    fn truncate(&mut self, len: u64) -> FsResult<()> {
        observe_mutation(&self.ledger, || self.inner.truncate(len))
    }
    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        let bytes = data.len();
        self.ledger.lock().issued(self.payload, bytes);
        let result = self.inner.append(data);
        if result.is_ok() {
            self.ledger.lock().returned(self.payload, bytes);
        } else {
            self.ledger.lock().observe_mutation();
        }
        result
    }
    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }
    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        observe_mutation(&self.ledger, || self.inner.sync(durability))
    }
}
