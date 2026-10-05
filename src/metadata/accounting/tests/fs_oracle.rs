//! Independent delegated-RealFs oracle; test-only, not producer counters.

use crate::io::traits::{DirEntry, Metadata, ReadObserver};
use crate::io::{Durability, File, Fs, FsError, FsPath, FsResult, HostAddressing, OpenOptions};
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    OpenSnapshot,
    WriteSnapshot,
    SyncJournal,
    RenameSnapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PayloadKind {
    Append,
    WriteAt,
}

#[derive(Clone, Debug)]
pub(super) struct OfferedWrite {
    pub path: String,
    pub bytes: u64,
    pub returned: bool,
}

#[derive(Default)]
pub(super) struct Observations {
    writes: Mutex<Vec<OfferedWrite>>,
    fault: Mutex<Option<Fault>>,
    sync_hold: Mutex<Option<SyncHold>>,
    payload_error_gate: Mutex<Option<PayloadErrorGate>>,
}

struct SyncHold {
    entered: crossbeam::channel::Sender<()>,
    release: crossbeam::channel::Receiver<()>,
}

struct PayloadErrorGate {
    kind: PayloadKind,
    entered: crossbeam::channel::Sender<()>,
    release: crossbeam::channel::Receiver<()>,
}

impl Observations {
    pub fn arm(&self, fault: Fault) {
        *self.fault.lock() = Some(fault);
    }

    pub fn clear_writes(&self) {
        self.writes.lock().clear();
    }

    pub fn writes(&self) -> Vec<OfferedWrite> {
        self.writes.lock().clone()
    }

    pub fn hold_next_successful_sync(
        &self,
        entered: crossbeam::channel::Sender<()>,
        release: crossbeam::channel::Receiver<()>,
    ) {
        *self.sync_hold.lock() = Some(SyncHold { entered, release });
    }

    fn complete_sync(&self) {
        let hold = self.sync_hold.lock().take();
        if let Some(hold) = hold {
            // Actual delegated sync succeeded before the positive handshake.
            // A dropped control or finite timeout always releases the worker.
            let _ = hold.entered.send(());
            let _ = hold
                .release
                .recv_timeout(std::time::Duration::from_secs(10));
        }
    }

    pub fn hold_next_successful_payload_then_error(
        &self,
        kind: PayloadKind,
        entered: crossbeam::channel::Sender<()>,
        release: crossbeam::channel::Receiver<()>,
    ) {
        *self.payload_error_gate.lock() = Some(PayloadErrorGate {
            kind,
            entered,
            release,
        });
    }

    fn complete_payload(&self, kind: PayloadKind) -> FsResult<()> {
        let gate = {
            let mut slot = self.payload_error_gate.lock();
            if slot.as_ref().is_some_and(|gate| gate.kind == kind) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            // The real underlying append/write_at succeeded before this signal.
            // Disconnect or finite timeout releases the held error unconditionally.
            let _ = gate.entered.send(());
            let _ = gate
                .release
                .recv_timeout(std::time::Duration::from_secs(10));
            return Err(FsError::Io(
                "actual oracle error after held payload completion".into(),
            ));
        }
        Ok(())
    }

    pub fn bytes_for(&self, path: &str, returned_only: bool) -> u64 {
        self.writes
            .lock()
            .iter()
            .filter(|write| write.path == path && (!returned_only || write.returned))
            .map(|write| write.bytes)
            .sum()
    }

    fn take_fault(&self, expected: Fault) -> bool {
        let mut fault = self.fault.lock();
        if *fault == Some(expected) {
            *fault = None;
            true
        } else {
            false
        }
    }

    fn record(&self, path: &FsPath, bytes: usize, returned: bool) {
        self.writes.lock().push(OfferedWrite {
            path: path.0.clone(),
            bytes: u64::try_from(bytes).expect("actual offered payload length fits u64"),
            returned,
        });
    }
}

pub(super) fn observe_fs(inner: Arc<dyn Fs>, observations: Arc<Observations>) -> Arc<dyn Fs> {
    Arc::new(ObservedFs {
        inner,
        observations,
    })
}

struct ObservedFs {
    inner: Arc<dyn Fs>,
    observations: Arc<Observations>,
}

struct ObservedFile<'a> {
    inner: Box<dyn File + 'a>,
    path: FsPath,
    observations: Arc<Observations>,
}

fn snapshot_temporary(path: &FsPath) -> bool {
    path.0 == super::fs_tests::SNAPSHOT_STAGE
}

impl Fs for ObservedFs {
    fn local_output_view(&self) -> Option<Arc<dyn Fs>> {
        self.inner
            .local_output_view()
            .map(|inner| observe_fs(inner, Arc::clone(&self.observations)))
    }

    fn with_read_observer(&self, observer: Arc<dyn ReadObserver>) -> Option<Arc<dyn Fs>> {
        self.inner
            .with_read_observer(observer)
            .map(|inner| observe_fs(inner, Arc::clone(&self.observations)))
    }

    fn immutable_read_view(&self, path: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
        self.inner
            .immutable_read_view(path)
            .map(|view| view.map(|inner| observe_fs(inner, Arc::clone(&self.observations))))
    }

    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        self.inner.host_addressing()
    }

    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }

    fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        if snapshot_temporary(path) && self.observations.take_fault(Fault::OpenSnapshot) {
            return Err(FsError::NoSpace(
                "actual oracle snapshot open boundary".into(),
            ));
        }
        self.inner.open(path, options).map(|inner| {
            Box::new(ObservedFile {
                inner,
                path: path.clone(),
                observations: Arc::clone(&self.observations),
            }) as Box<dyn File + '_>
        })
    }

    fn open_persistent_handle(
        &self,
        path: &FsPath,
        options: OpenOptions,
    ) -> FsResult<Box<dyn File>> {
        self.inner
            .open_persistent_handle(path, options)
            .map(|inner| {
                Box::new(ObservedFile {
                    inner,
                    path: path.clone(),
                    observations: Arc::clone(&self.observations),
                }) as Box<dyn File>
            })
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
        if snapshot_temporary(from) && self.observations.take_fault(Fault::RenameSnapshot) {
            return Err(FsError::Io("actual oracle snapshot rename boundary".into()));
        }
        self.inner.rename_atomic(from, to)
    }
}

impl File for ObservedFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.inner.read_at(offset, len)
    }

    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        let offered = data.len();
        let result = if snapshot_temporary(&self.path)
            && self.observations.take_fault(Fault::WriteSnapshot)
        {
            Err(FsError::NoSpace(
                "actual oracle snapshot write boundary".into(),
            ))
        } else {
            self.inner.write_at(offset, data)
        };
        self.observations
            .record(&self.path, offered, result.is_ok());
        result.and_then(|()| self.observations.complete_payload(PayloadKind::WriteAt))
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        self.inner.truncate(len)
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        let offered = data.len();
        let result = self.inner.append(data);
        self.observations
            .record(&self.path, offered, result.is_ok());
        result.and_then(|offset| {
            self.observations.complete_payload(PayloadKind::Append)?;
            Ok(offset)
        })
    }

    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }

    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        if self.path.0 == crate::metadata::files::JOURNAL
            && self.observations.take_fault(Fault::SyncJournal)
        {
            return Err(FsError::Io("actual oracle journal sync boundary".into()));
        }
        let result = self.inner.sync(durability);
        if result.is_ok() {
            self.observations.complete_sync();
        }
        result
    }
}
