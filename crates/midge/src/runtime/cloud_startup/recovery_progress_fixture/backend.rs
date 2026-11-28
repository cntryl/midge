use super::{local_wal_state, RecoveryProgressFixtureMode, PROVIDER_TIMEOUT};
use cntryl_midge_core::common::{MidgeError, MidgeResult};
use cntryl_midge_core::io::{Fs, FsError, FsPath};
use cntryl_midge_storage::{
    MetadataReadCallback, RangeReadCallback, StorageBackend, StorageCallback, StorageError,
    StorageRequest,
};
use parking_lot::Mutex;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::time::Duration;

#[derive(Clone, Serialize)]
pub(super) struct ObservationSnapshot {
    mode: RecoveryProgressFixtureMode,
    phase: &'static str,
    pub(super) expected_records: u64,
    held_requests: u64,
    pub(super) completed_range_reads: u64,
    pub(super) completed_range_bytes: u64,
    pub(super) maximum_range_bytes: u64,
    local_wal_bytes: u64,
    staged_wal_count: u64,
    pub(super) mismatches: u64,
    pub(super) expected_inventory_entries: u64,
    pub(super) retained_inventory_entries: u64,
    pub(super) completed_inventory_heads: u64,
    pub(super) completed_inventory_size_validations: u64,
}

pub(super) struct FixtureObservations {
    fs: Arc<dyn Fs>,
    local: PathBuf,
    snapshot: Mutex<ObservationSnapshot>,
    failure: Mutex<Option<String>>,
}

impl FixtureObservations {
    pub(super) fn new(root: &Path, mode: RecoveryProgressFixtureMode) -> MidgeResult<Self> {
        let observations = Self {
            fs: Arc::new(cntryl_midge_core::io::RealFs::new(root).map_err(FsError::into_midge)?),
            local: root.join("local"),
            snapshot: Mutex::new(ObservationSnapshot {
                mode,
                phase: "setup",
                expected_records: 0,
                held_requests: 0,
                completed_range_reads: 0,
                completed_range_bytes: 0,
                maximum_range_bytes: 0,
                local_wal_bytes: 0,
                staged_wal_count: 0,
                mismatches: 0,
                expected_inventory_entries: 0,
                retained_inventory_entries: 0,
                completed_inventory_heads: 0,
                completed_inventory_size_validations: 0,
            }),
            failure: Mutex::new(None),
        };
        observations.persist()?;
        Ok(observations)
    }

    pub(super) fn snapshot(&self) -> ObservationSnapshot {
        self.snapshot.lock().clone()
    }

    pub(super) fn set_phase(&self, phase: &'static str) -> MidgeResult<()> {
        self.snapshot.lock().phase = phase;
        self.persist()
    }

    pub(super) fn set_expected_records(&self, records: usize) -> MidgeResult<()> {
        self.snapshot.lock().expected_records = u64::try_from(records).unwrap_or(u64::MAX);
        self.persist()
    }

    #[cfg(any(test, feature = "cloud-common"))]
    pub(super) fn set_expected_inventory(&self, entries: usize) -> MidgeResult<()> {
        self.snapshot.lock().expected_inventory_entries =
            u64::try_from(entries).unwrap_or(u64::MAX);
        self.persist()
    }

    #[cfg(any(test, feature = "cloud-common"))]
    pub(super) fn record_retained_inventory(
        &self,
        entries: usize,
        mismatch: bool,
    ) -> MidgeResult<()> {
        let mut snapshot = self.snapshot.lock();
        snapshot.retained_inventory_entries = u64::try_from(entries).unwrap_or(u64::MAX);
        snapshot.mismatches += u64::from(mismatch);
        drop(snapshot);
        self.persist()
    }

    fn persist(&self) -> MidgeResult<()> {
        let mut snapshot = self.snapshot.lock();
        (snapshot.local_wal_bytes, snapshot.staged_wal_count) = local_wal_state(&self.local)?;
        let bytes = serde_json::to_vec(&*snapshot)
            .map_err(|error| MidgeError::Internal(format!("fixture observations: {error}")))?;
        cntryl_midge_core::io::staging::stage_bytes(
            &self.fs,
            &FsPath::new(".fixture-observations.tmp"),
            &FsPath::new("fixture-observations.json"),
            &bytes,
            MidgeError::Internal,
        )
    }

    fn remember_persistence(&self) {
        if let Err(error) = self.persist() {
            *self.failure.lock() = Some(error.to_string());
        }
    }

    pub(super) fn check_failure(&self) -> MidgeResult<()> {
        self.failure.lock().as_ref().map_or(Ok(()), |failure| {
            Err(MidgeError::Internal(format!(
                "recovery fixture evidence persistence failed: {failure}"
            )))
        })
    }

    pub(super) fn hold_request(&self) -> MidgeResult<()> {
        self.snapshot.lock().held_requests += 1;
        self.persist()
    }

    fn completed_range(&self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        let mut snapshot = self.snapshot.lock();
        snapshot.completed_range_reads += 1;
        snapshot.completed_range_bytes = snapshot.completed_range_bytes.saturating_add(bytes);
        snapshot.maximum_range_bytes = snapshot.maximum_range_bytes.max(bytes);
        drop(snapshot);
        self.remember_persistence();
    }

    #[cfg(any(test, feature = "cloud-common"))]
    pub(super) fn completed_inventory_head(&self, expected_size: bool, persist_progress: bool) {
        let mut snapshot = self.snapshot.lock();
        snapshot.completed_inventory_heads += 1;
        snapshot.completed_inventory_size_validations += u64::from(expected_size);
        snapshot.mismatches += u64::from(!expected_size);
        drop(snapshot);
        if persist_progress {
            self.remember_persistence();
        }
    }
}

pub(super) struct ObservedBackend {
    inner: cntryl_midge_storage::filesystem::FileSystem,
    mode: RecoveryProgressFixtureMode,
    observations: Arc<FixtureObservations>,
    held: Mutex<Option<RangeReadCallback>>,
}

impl ObservedBackend {
    pub(super) fn new(
        remote_root: PathBuf,
        mode: RecoveryProgressFixtureMode,
        observations: Arc<FixtureObservations>,
    ) -> MidgeResult<Self> {
        Ok(Self {
            inner: cntryl_midge_storage::filesystem::FileSystem::new(remote_root)?,
            mode,
            observations,
            held: Mutex::new(None),
        })
    }
}

impl StorageBackend for ObservedBackend {
    fn submit_range_read_request(
        &self,
        request: StorageRequest,
        range: std::ops::Range<u64>,
        callback: RangeReadCallback,
    ) {
        if self.mode == RecoveryProgressFixtureMode::HeldFirstRange {
            let mut held = self.held.lock();
            if held.is_none() {
                if let Err(error) = self.observations.hold_request() {
                    let _ = callback.send(Err(StorageError::io(error)));
                    return;
                }
                *held = Some(callback);
                return;
            }
        }
        let (sender, receiver) = mpsc::channel();
        self.inner.submit_range_read_request(request, range, sender);
        let result = receiver
            .recv_timeout(PROVIDER_TIMEOUT)
            .unwrap_or_else(|error| {
                Err(StorageError::io(format!("fixture range callback: {error}")))
            });
        match result {
            Ok(bytes) => {
                if self.mode == RecoveryProgressFixtureMode::DelayedRanges {
                    std::thread::sleep(Duration::from_millis(100));
                }
                let returned = bytes.len();
                if callback.send(Ok(bytes)).is_ok() {
                    self.observations.completed_range(returned);
                }
            }
            Err(error) => {
                let _ = callback.send(Err(error));
            }
        }
    }

    fn submit_range_head_request(&self, request: StorageRequest, callback: StorageCallback) {
        self.inner.submit_range_head_request(request, callback);
    }

    fn submit_metadata_read_request(
        &self,
        request: StorageRequest,
        callback: MetadataReadCallback,
    ) {
        self.inner.submit_metadata_read_request(request, callback);
    }

    fn submit_head_request(&self, request: StorageRequest, callback: StorageCallback) {
        self.inner.submit_head_request(request, callback);
    }

    fn submit_delete_request(&self, request: StorageRequest, callback: StorageCallback) {
        self.inner.submit_delete_request(request, callback);
    }

    fn submit_write_request(
        &self,
        request: StorageRequest,
        data: Vec<u8>,
        callback: StorageCallback,
    ) {
        self.inner.submit_write_request(request, data, callback);
    }
}
