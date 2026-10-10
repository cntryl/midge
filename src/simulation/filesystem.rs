//! Schedule synchronous filesystem calls without changing the writer protocol.

use super::scheduler::Scheduler;
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, Fs, FsError, FsPath, FsResult, HostAddressing, OpenOptions};
use bytes::Bytes;
use std::sync::{mpsc, Arc, Mutex};

type Handle = Arc<Mutex<Box<dyn File>>>;

/// Workers block on the normal `Fs` call while the controller chooses when its
/// storage effect and response occur. Lost responses return an unavailable error;
/// they never undo the storage effect. Pending effects retain their file handles.
#[derive(Clone)]
pub struct ScheduledFs {
    inner: Arc<dyn Fs>,
    scheduler: Scheduler,
    epoch: u64,
}

impl ScheduledFs {
    #[must_use]
    pub fn new(inner: Arc<dyn Fs>, scheduler: Scheduler, epoch: u64) -> Self {
        Self {
            inner,
            scheduler,
            epoch,
        }
    }

    fn call<T: Clone + Send + 'static>(
        &self,
        kind: &str,
        path: &str,
        operation: impl FnOnce() -> FsResult<T> + Send + 'static,
    ) -> FsResult<T> {
        let (tx, rx) = mpsc::channel();
        self.scheduler.submit(
            self.epoch,
            kind,
            path.into(),
            None,
            Box::new(move || {
                let result = operation();
                let summary = match &result {
                    Ok(_) => "Ok".into(),
                    Err(error) => error.to_string(),
                };
                (
                    Box::new(move || {
                        let _ = tx.send(result.clone());
                    }),
                    summary,
                )
            }),
        );
        rx.recv().map_err(|_| {
            FsError::Unavailable(
                "simulated filesystem response lost; effect may have applied".into(),
            )
        })?
    }

    fn open_file(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        let inner = self.inner.clone();
        let name = path.clone();
        let handle = self.call("open", &path.0, move || {
            inner
                .open_persistent_handle(&name, opts)
                .map(|handle| Arc::new(Mutex::new(handle)))
        })?;
        Ok(Box::new(ScheduledFile {
            handle,
            fs: self.clone(),
            path: path.0.clone(),
        }))
    }
}

impl Fs for ScheduledFs {
    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        self.inner.host_addressing()
    }
    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }
    fn local_output_view(&self) -> Option<Arc<dyn Fs>> {
        self.inner.local_output_view().map(|inner| {
            Arc::new(Self::new(inner, self.scheduler.clone(), self.epoch)) as Arc<dyn Fs>
        })
    }
    fn immutable_read_view(&self, path: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
        let inner = self.inner.clone();
        let name = path.clone();
        self.call("immutable_view", &path.0, move || {
            inner.immutable_read_view(&name)
        })
        .map(|view| {
            view.map(|inner| {
                Arc::new(Self::new(inner, self.scheduler.clone(), self.epoch)) as Arc<dyn Fs>
            })
        })
    }
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.open_file(path, opts)
    }
    fn open_persistent_handle(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File>> {
        self.open_file(path, opts)
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        let inner = self.inner.clone();
        let name = path.clone();
        self.call("unlink", &path.0, move || inner.remove_file(&name))
    }
    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        let inner = self.inner.clone();
        let name = path.clone();
        self.call("exists", &path.0, move || inner.exists(&name))
    }
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        let inner = self.inner.clone();
        let name = path.clone();
        self.call("metadata", &path.0, move || inner.metadata(&name))
    }
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let inner = self.inner.clone();
        let name = path.clone();
        self.call("mkdir", &path.0, move || inner.create_dir_all(&name))
    }
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        let inner = self.inner.clone();
        let name = path.clone();
        self.call("list", &path.0, move || inner.list_dir(&name))
    }
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        let inner = self.inner.clone();
        let name = path.clone();
        self.call("remove_tree", &path.0, move || inner.remove_dir_all(&name))
    }
    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        let inner = self.inner.clone();
        let name = path.clone();
        self.call("sync_dir", &path.0, move || {
            inner.sync_dir(&name, durability)
        })
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        let inner = self.inner.clone();
        let source = from.clone();
        let target = to.clone();
        self.call("rename", &from.0, move || {
            inner.rename_atomic(&source, &target)
        })
    }
}

struct ScheduledFile {
    handle: Handle,
    fs: ScheduledFs,
    path: String,
}

impl ScheduledFile {
    fn call<T: Clone + Send + 'static>(
        &self,
        kind: &str,
        operation: impl FnOnce(&mut dyn File) -> FsResult<T> + Send + 'static,
    ) -> FsResult<T> {
        let handle = self.handle.clone();
        self.fs.call(kind, &self.path, move || {
            operation(
                handle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_mut(),
            )
        })
    }
}

impl File for ScheduledFile {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.call("read", move |file| file.read_at(offset, len))
    }
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        self.call("write", move |file| file.write_at(offset, data))
    }
    fn truncate(&mut self, len: u64) -> FsResult<()> {
        self.call("truncate", move |file| file.truncate(len))
    }
    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        self.call("append", move |file| file.append(data))
    }
    fn len(&self) -> FsResult<u64> {
        self.call("length", |file| file.len())
    }
    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        self.call("sync_file", move |file| file.sync(durability))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{
        crash_fs::{CrashFs, CrashMode},
        OpenMode,
    };
    use crate::simulation::scheduler::Action;
    use std::time::{Duration, Instant};

    fn wait_for_command(scheduler: &Scheduler) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some((command, _)) = scheduler.pending().first() {
                return command.id;
            }
            assert!(Instant::now() < deadline, "worker did not submit command");
            std::thread::yield_now();
        }
    }

    #[test]
    fn should_keep_synced_bytes_when_worker_loses_fsync_response() {
        // Arrange
        let fs = CrashFs::new();
        let inner: Arc<dyn Fs> = Arc::new(fs.clone());
        let eager = ScheduledFs::new(inner.clone(), Scheduler::eager(), 1);
        let opts = OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        };
        let mut file = eager
            .open_persistent_handle(&FsPath::new("wal"), opts)
            .unwrap();
        file.append(Bytes::from_static(b"committed")).unwrap();
        fs.sync_dir(&FsPath::new(""), Durability::Durable).unwrap();
        // Open the same inode under manual scheduling before starting the sync.
        let scheduler = Scheduler::manual();
        let mut file = ScheduledFile {
            handle: Arc::new(Mutex::new(
                fs.open_persistent_handle(&FsPath::new("wal"), opts)
                    .unwrap(),
            )),
            fs: ScheduledFs::new(inner, scheduler.clone(), 1),
            path: "wal".into(),
        };
        let worker = std::thread::spawn(move || file.sync(Durability::Durable));
        let id = wait_for_command(&scheduler);
        // Act
        scheduler.apply(Action::Effect(id)).unwrap();
        scheduler.apply(Action::LoseResponse(id)).unwrap();
        let failure = worker.join().unwrap().unwrap_err();
        fs.crash(CrashMode::DiscardVolatile);
        // Assert
        assert!(matches!(failure, FsError::Unavailable(_)));
        let recovered = fs
            .open(
                &FsPath::new("wal"),
                OpenOptions {
                    mode: OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .unwrap();
        assert_eq!(recovered.read_at(0, 9).unwrap().as_ref(), b"committed");
        assert_eq!(scheduler.pending(), Vec::new());
    }
}
