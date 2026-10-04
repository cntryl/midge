use super::*;
use crate::storage::{StorageBackend, StorageCallback, StorageRequest};

struct WalReadGate {
    reached: crossbeam::channel::Sender<()>,
    release: crossbeam::channel::Receiver<()>,
}

pub(super) struct ProviderRelease(Option<crossbeam::channel::Sender<()>>);

impl ProviderRelease {
    pub(super) fn release(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for ProviderRelease {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) struct ControlledStorage {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    wal_gate: Mutex<Option<WalReadGate>>,
    fail_upload: AtomicBool,
    pub(super) uploads: Mutex<Vec<String>>,
}

pub(super) fn install(
    el: &mut EventLoop,
    hold_wal: bool,
    fail_upload: bool,
) -> MidgeResult<(
    Arc<ControlledStorage>,
    ProviderRelease,
    crossbeam::channel::Receiver<()>,
)> {
    let (reached, arrived) = crossbeam::channel::bounded(1);
    let (release, resumed) = crossbeam::channel::bounded(1);
    let cloud = Arc::new(ControlledStorage {
        inner: Arc::new(crate::storage::filesystem::FileSystem::new(
            el.state.db_path.join("cloud_store"),
        )?),
        wal_gate: Mutex::new(hold_wal.then_some(WalReadGate {
            reached,
            release: resumed,
        })),
        fail_upload: AtomicBool::new(fail_upload),
        uploads: Mutex::new(Vec::new()),
    });
    let local = Arc::new(crate::storage::filesystem::FileSystem::new(
        el.state.db_path.join("hybrid_local"),
    )?);
    let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    hybrid.enable_ephemeral_sst_cache(64 * 1024 * 1024);
    el.set_hybrid_storage(hybrid);
    Ok((cloud, ProviderRelease(Some(release)), arrived))
}

impl StorageBackend for ControlledStorage {
    fn submit_range_read_request(
        &self,
        request: StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        if request.key.starts_with("wal/") {
            let gate = self
                .wal_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(gate) = gate {
                assert!(std::thread::current()
                    .name()
                    .is_some_and(|name| name.starts_with("midge-wal-prune-preflight-")));
                let _ = gate.reached.send(());
                let _ = gate.release.recv();
            }
        }
        self.inner
            .submit_range_read_request(request, range, callback);
    }

    fn submit_range_head_request(&self, request: StorageRequest, callback: StorageCallback) {
        self.inner.submit_range_head_request(request, callback);
    }

    fn submit_metadata_read_request(
        &self,
        request: StorageRequest,
        callback: crate::storage::MetadataReadCallback,
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
        if request.key.starts_with("sst/") {
            self.uploads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request.key.clone());
            if self.fail_upload.swap(false, Ordering::AcqRel) {
                let _ = callback.send(crate::storage::StorageEvent::WriteComplete {
                    key: request.key,
                    result: crate::storage::StorageOutcome::Err(
                        crate::storage::StorageError::timeout(
                            "one fixture SST upload timeout before provider write",
                        ),
                    ),
                });
                return;
            }
        }
        self.inner.submit_write_request(request, data, callback);
    }
}
