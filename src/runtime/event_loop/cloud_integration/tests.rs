use super::super::tests::{create_test_cloud_event_loop, create_test_event_loop};
use super::super::wal::{ApplyTransactionRequest, WalCoordinator};
use super::super::EventLoop;
use crate::runtime::durability::DurabilityWaiter;
use crate::runtime::hybrid_persistence::HybridPersistence;
use crate::runtime::{
    state::RuntimeState, ConflictPolicy, KeyAssertion, ResponseRouter, RuntimeMsg, RuntimeResponse,
};
use crate::sst::Memtable;
use crate::wal::DurabilityPolicy;
use bytes::Bytes;
use std::path::{Path, PathBuf};
#[cfg(feature = "failpoints")]
use std::sync::OnceLock;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

#[cfg(feature = "failpoints")]
static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[cfg(feature = "failpoints")]
fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

fn assertion_only_cloud_request(request_id: u64, start_sequence: u64) -> ApplyTransactionRequest {
    ApplyTransactionRequest {
        request_id,
        ops: Vec::new(),
        assertions: vec![KeyAssertion {
            cf_id: 0,
            key: Bytes::from_static(b"cloud-assertion-only"),
        }],
        durability_policy: Some(DurabilityPolicy::CloudAsync),
        start_sequence: Some(start_sequence),
        conflict_policy: ConflictPolicy::LastWriteWins,
    }
}

fn apply_assertion_only_cloud_request(
    event_loop: &mut EventLoop,
    request_id: u64,
    start_sequence: u64,
) -> RuntimeResponse {
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let (response_tx, response_rx) = crossbeam::channel::bounded(1);
    let outcome = WalCoordinator::apply_transaction(
        event_loop,
        &msg_rx,
        assertion_only_cloud_request(request_id, start_sequence),
        Some(response_tx),
    );
    assert_eq!(outcome, super::super::HandleOutcome::Continue);
    response_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("assertion-only cloud response")
}

#[test]
fn should_not_queue_confirmation_waiter_when_cloud_assertion_only_sequence_is_already_durable(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    assert_eq!(event_loop.state.sequence, 0);
    assert_eq!(event_loop.state.wal.cloud_durable_seq, 0);

    // Act
    let response = apply_assertion_only_cloud_request(&mut event_loop, 41, 0);

    // Assert
    assert!(matches!(
        response,
        RuntimeResponse::TransactionApplied {
            request_id: 41,
            last_sequence: 0,
            op_count: 0,
            ..
        }
    ));
    assert!(
        !event_loop.durability.has_pending_waiters(),
        "an assertion-only request must not leave a waiter without a WAL generation"
    );
    Ok(())
}

#[test]
fn should_not_add_confirmation_waiter_when_cloud_assertion_only_sequence_has_pending_upload(
) -> crate::common::MidgeResult<()> {
    // Arrange: model an earlier write whose generation has not reached the
    // cloud frontier. The assertion-only commit adds no WAL record of its own.
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    event_loop.state.sequence = 1;
    event_loop.state.wal.cloud_durable_seq = 0;
    event_loop
        .durability
        .queue_waiter(DurabilityWaiter::ConfirmWalAppend { request_id: 40 });

    // Act
    let response = apply_assertion_only_cloud_request(&mut event_loop, 42, 1);
    let waiters = event_loop.durability.drain_all_waiters();

    // Assert
    assert!(matches!(
        response,
        RuntimeResponse::TransactionApplied {
            request_id: 42,
            last_sequence: 1,
            op_count: 0,
            ..
        }
    ));
    assert_eq!(waiters.len(), 1, "only the earlier write should remain");
    assert!(matches!(
        &waiters[0],
        DurabilityWaiter::ConfirmWalAppend { request_id: 40 }
    ));
    Ok(())
}

#[test]
fn should_fail_all_generation_waiters_given_terminal_cloud_upload_error(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let direct_request = 43;
    let pending_request = 44;
    let direct_response = event_loop.router.register(direct_request, "TestRequest");
    let pending_response = event_loop.router.register(pending_request, "TestRequest");
    event_loop.durability.queue_waiter_for_key(
        0,
        DurabilityWaiter::CloudDurability {
            request_id: direct_request,
        },
    );
    event_loop
        .durability
        .queue_waiter(DurabilityWaiter::CloudDurability {
            request_id: pending_request,
        });

    // Act
    event_loop.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id: 0,
        error: "terminal upload failure".to_string(),
        terminal: true,
        failure_kind: crate::storage::CloudUploadFailureKind::Other,
    });

    // Assert
    for (request_id, receiver) in [
        (direct_request, direct_response),
        (pending_request, pending_response),
    ] {
        let response = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("terminal cloud failure response");
        assert!(matches!(
            response,
            RuntimeResponse::Error {
                request_id: response_id,
                error: crate::common::MidgeError::Internal(message),
            } if response_id == request_id && message.contains("terminal upload failure")
        ));
    }
    assert!(!event_loop.durability.has_pending_waiters());
    Ok(())
}

#[test]
fn should_preserve_timeout_variant_given_terminal_cloud_upload_timeout(
) -> crate::common::MidgeResult<()> {
    // Arrange: a strict durability caller is still attached when the storage
    // queue exhausts three callback-timeout attempts.
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let request_id = 46;
    let response = event_loop.router.register(request_id, "TestRequest");
    event_loop
        .durability
        .queue_waiter_for_key(0, DurabilityWaiter::CloudDurability { request_id });

    // Act
    event_loop.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id: 0,
        error: "cloud WAL upload callback timed out".to_string(),
        terminal: true,
        failure_kind: crate::storage::CloudUploadFailureKind::Timeout,
    });

    // Assert
    let response = response
        .recv_timeout(Duration::from_secs(1))
        .expect("terminal cloud timeout response");
    assert!(matches!(
        response,
        RuntimeResponse::Error {
            request_id: response_id,
            error: crate::common::MidgeError::Timeout(message),
        } if response_id == request_id && message.contains("timed out")
    ));
    Ok(())
}

#[test]
fn should_retain_cloud_retry_state_given_storage_owned_upload_failure(
) -> crate::common::MidgeResult<()> {
    // Arrange: the storage queue still owns this segment because its internal
    // retry budget has not been exhausted.
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    append_cloud_async_put(&mut event_loop)?;
    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut event_loop)?;
    let request_id = 45;
    let response = event_loop.router.register(request_id, "TestRequest");
    event_loop
        .durability
        .queue_waiter_for_key(segment_id, DurabilityWaiter::CloudDurability { request_id });
    event_loop
        .state
        .sequence_idempotency_cache
        .insert(request_id, (max_sequence, 1, 0));

    // Act
    event_loop.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id,
        error: "retryable upload failure".to_string(),
        terminal: false,
        failure_kind: crate::storage::CloudUploadFailureKind::Other,
    });

    // Assert
    assert!(matches!(
        response.try_recv(),
        Err(crossbeam::channel::TryRecvError::Empty)
    ));
    assert_eq!(
        event_loop
            .durability
            .cloud_durability_request_ids_at(segment_id),
        vec![request_id]
    );
    assert_eq!(
        event_loop.durability.cloud_segment_max_sequence(segment_id),
        Some(max_sequence)
    );
    assert!(event_loop
        .state
        .sequence_idempotency_cache
        .contains_key(&request_id));
    assert!(!event_loop.state.persistence_anomaly_detected());
    Ok(())
}

struct FailThirdIntentPutBackend {
    inner: Arc<crate::storage::cloud::MockCloudBackend>,
    intent_puts: AtomicUsize,
}

impl FailThirdIntentPutBackend {
    fn new(inner: Arc<crate::storage::cloud::MockCloudBackend>) -> Self {
        Self {
            inner,
            intent_puts: AtomicUsize::new(0),
        }
    }
}

impl crate::storage::cloud::CloudBackend for FailThirdIntentPutBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        if key.ends_with("metadata/intent_log.json")
            && self.intent_puts.fetch_add(1, Ordering::SeqCst) >= 2
        {
            let key = key.to_string();
            let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
                key,
                result: crate::storage::cloud::CloudOutcome::Err(
                    crate::storage::cloud::CloudError::ServerError(
                        "injected intent metadata put failure".to_string(),
                    ),
                ),
            });
            return;
        }

        crate::storage::cloud::CloudBackend::submit_put(
            self.inner.as_ref(),
            key,
            data,
            headers,
            callback,
        );
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_get(self.inner.as_ref(), key, callback);
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_get_range(
            self.inner.as_ref(),
            key,
            start,
            end,
            callback,
        );
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_delete(
            self.inner.as_ref(),
            key,
            headers,
            callback,
        );
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_list(self.inner.as_ref(), prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_head(self.inner.as_ref(), key, callback);
    }
}

struct ObserveIntentBeforeRemoteSstBackend {
    inner: Arc<crate::storage::cloud::MockCloudBackend>,
    remote_sst_path: PathBuf,
    saw_compaction_intent: Arc<AtomicBool>,
    remote_existed_at_intent_publish: Arc<AtomicBool>,
}

impl crate::storage::cloud::CloudBackend for ObserveIntentBeforeRemoteSstBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        if key.ends_with("metadata/intent_log.json")
            && data
                .windows(b"CompactionPublish".len())
                .any(|window| window == b"CompactionPublish")
            && !self.saw_compaction_intent.swap(true, Ordering::SeqCst)
        {
            self.remote_existed_at_intent_publish
                .store(self.remote_sst_path.exists(), Ordering::SeqCst);
        }
        crate::storage::cloud::CloudBackend::submit_put(
            self.inner.as_ref(),
            key,
            data,
            headers,
            callback,
        );
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_get(self.inner.as_ref(), key, callback);
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_get_range(
            self.inner.as_ref(),
            key,
            start,
            end,
            callback,
        );
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_delete(
            self.inner.as_ref(),
            key,
            headers,
            callback,
        );
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_list(self.inner.as_ref(), prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_head(self.inner.as_ref(), key, callback);
    }
}

struct BlockingDeleteStorageBackend {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    block_key: String,
    delete_started: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    release_delete: Arc<AtomicBool>,
}

struct ArmedDelayedHeadStorageBackend {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    delay: Duration,
    delay_next_head: AtomicBool,
}

struct CommitThenBlockCatalogReadbackBackend {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    arm_catalog_write: AtomicBool,
    block_next_catalog_head: AtomicBool,
    retained_callbacks: Mutex<Vec<crate::storage::StorageCallback>>,
}

struct BudgetConsumingDdlBackend {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    registry_head_calls: AtomicUsize,
    registry_cas_timeouts: Mutex<Vec<Duration>>,
}

struct DelayedCommitDdlBackend {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    delay_first_registry_cas: AtomicBool,
    commit_complete: Arc<AtomicBool>,
}

impl DelayedCommitDdlBackend {
    fn new(inner: Arc<crate::storage::filesystem::FileSystem>) -> Self {
        Self {
            inner,
            delay_first_registry_cas: AtomicBool::new(true),
            commit_complete: Arc::new(AtomicBool::new(false)),
        }
    }

    fn commit_complete(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.commit_complete)
    }
}

impl BudgetConsumingDdlBackend {
    fn new(inner: Arc<crate::storage::filesystem::FileSystem>) -> Self {
        Self {
            inner,
            registry_head_calls: AtomicUsize::new(0),
            registry_cas_timeouts: Mutex::new(Vec::new()),
        }
    }

    fn registry_cas_timeouts(&self) -> Vec<Duration> {
        self.registry_cas_timeouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl CommitThenBlockCatalogReadbackBackend {
    fn new(inner: Arc<crate::storage::filesystem::FileSystem>) -> Self {
        Self {
            inner,
            arm_catalog_write: AtomicBool::new(false),
            block_next_catalog_head: AtomicBool::new(false),
            retained_callbacks: Mutex::new(Vec::new()),
        }
    }

    fn arm(&self) {
        self.arm_catalog_write.store(true, Ordering::SeqCst);
    }
}

impl ArmedDelayedHeadStorageBackend {
    fn new(inner: Arc<crate::storage::filesystem::FileSystem>, delay: Duration) -> Self {
        Self {
            inner,
            delay,
            delay_next_head: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.delay_next_head.store(true, Ordering::SeqCst);
    }
}

impl crate::storage::StorageBackend for ArmedDelayedHeadStorageBackend {
    fn submit_read(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_read(self.inner.as_ref(), key, callback);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_write(self.inner.as_ref(), key, data, callback);
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_write_with_headers(
            self.inner.as_ref(),
            key,
            data,
            headers,
            callback,
        );
    }

    fn submit_delete(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_delete(self.inner.as_ref(), key, callback);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_delete_with_headers(
            self.inner.as_ref(),
            key,
            headers,
            callback,
        );
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_list(self.inner.as_ref(), prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::StorageCallback) {
        if self.delay_next_head.swap(false, Ordering::SeqCst) {
            let inner = Arc::clone(&self.inner);
            let key = key.to_string();
            let delay = self.delay;
            std::thread::spawn(move || {
                std::thread::sleep(delay);
                crate::storage::StorageBackend::submit_head(inner.as_ref(), &key, callback);
            });
            return;
        }
        crate::storage::StorageBackend::submit_head(self.inner.as_ref(), key, callback);
    }
}

impl crate::storage::StorageBackend for CommitThenBlockCatalogReadbackBackend {
    fn submit_read(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_read(self.inner.as_ref(), key, callback);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_write(self.inner.as_ref(), key, data, callback);
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        if key == crate::wal::cloud_catalog::OBJECT_KEY
            && self.arm_catalog_write.swap(false, Ordering::SeqCst)
        {
            let (inner_tx, inner_rx) = std::sync::mpsc::channel();
            crate::storage::StorageBackend::submit_write_with_headers(
                self.inner.as_ref(),
                key,
                data,
                headers,
                inner_tx,
            );
            let event = inner_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("catalog CAS fixture completion");
            if matches!(
                event,
                crate::storage::StorageEvent::WriteComplete {
                    result: crate::storage::StorageOutcome::Ok(()),
                    ..
                }
            ) {
                self.block_next_catalog_head.store(true, Ordering::SeqCst);
            }
            let _ = callback.send(event);
            return;
        }

        crate::storage::StorageBackend::submit_write_with_headers(
            self.inner.as_ref(),
            key,
            data,
            headers,
            callback,
        );
    }

    fn submit_delete(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_delete(self.inner.as_ref(), key, callback);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_delete_with_headers(
            self.inner.as_ref(),
            key,
            headers,
            callback,
        );
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_list(self.inner.as_ref(), prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::StorageCallback) {
        if key == crate::wal::cloud_catalog::OBJECT_KEY
            && self.block_next_catalog_head.swap(false, Ordering::SeqCst)
        {
            self.retained_callbacks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(callback);
            return;
        }
        crate::storage::StorageBackend::submit_head(self.inner.as_ref(), key, callback);
    }
}

impl crate::storage::StorageBackend for BudgetConsumingDdlBackend {
    fn submit_read(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_read(self.inner.as_ref(), key, callback);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_write(self.inner.as_ref(), key, data, callback);
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        if key == crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY {
            let inner = Arc::clone(&self.inner);
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(250));
                crate::storage::StorageBackend::submit_write_with_headers(
                    inner.as_ref(),
                    &key,
                    data,
                    headers,
                    callback,
                );
            });
            return;
        }
        crate::storage::StorageBackend::submit_write_with_headers(
            self.inner.as_ref(),
            key,
            data,
            headers,
            callback,
        );
    }

    fn submit_write_with_headers_and_timeout(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        timeout: Duration,
        callback: crate::storage::StorageCallback,
    ) {
        if key == crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY {
            self.registry_cas_timeouts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(timeout);
            let _ = callback.send(crate::storage::StorageEvent::WriteComplete {
                key: key.to_string(),
                result: crate::storage::StorageOutcome::Err(
                    "remote request timed out before mutation".to_string(),
                ),
            });
            return;
        }
        self.submit_write_with_headers(key, data, headers, callback);
    }

    fn submit_delete(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_delete(self.inner.as_ref(), key, callback);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_delete_with_headers(
            self.inner.as_ref(),
            key,
            headers,
            callback,
        );
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_list(self.inner.as_ref(), prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::StorageCallback) {
        if key == crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY
            && self.registry_head_calls.fetch_add(1, Ordering::SeqCst) == 0
        {
            let inner = Arc::clone(&self.inner);
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(300));
                crate::storage::StorageBackend::submit_head(inner.as_ref(), &key, callback);
            });
            return;
        }
        crate::storage::StorageBackend::submit_head(self.inner.as_ref(), key, callback);
    }
}

impl crate::storage::StorageBackend for DelayedCommitDdlBackend {
    fn submit_read(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_read(self.inner.as_ref(), key, callback);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_write(self.inner.as_ref(), key, data, callback);
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_write_with_headers(
            self.inner.as_ref(),
            key,
            data,
            headers,
            callback,
        );
    }

    fn submit_write_with_headers_and_timeout(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        timeout: Duration,
        callback: crate::storage::StorageCallback,
    ) {
        if key == crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY
            && self.delay_first_registry_cas.swap(false, Ordering::SeqCst)
        {
            let inner = Arc::clone(&self.inner);
            let key_for_worker = key.to_string();
            let commit_complete = Arc::clone(&self.commit_complete);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(100));
                let (tx, rx) = std::sync::mpsc::channel();
                crate::storage::StorageBackend::submit_write_with_headers(
                    inner.as_ref(),
                    &key_for_worker,
                    data,
                    headers,
                    tx,
                );
                let committed = matches!(
                    rx.recv_timeout(Duration::from_secs(1)),
                    Ok(crate::storage::StorageEvent::WriteComplete {
                        result: crate::storage::StorageOutcome::Ok(()),
                        ..
                    })
                );
                commit_complete.store(committed, Ordering::SeqCst);
            });
            let _ = callback.send(crate::storage::StorageEvent::WriteComplete {
                key: key.to_string(),
                result: crate::storage::StorageOutcome::Err(format!(
                    "remote request timed out after submission (budget {timeout:?})"
                )),
            });
            return;
        }
        crate::storage::StorageBackend::submit_write_with_headers_and_timeout(
            self.inner.as_ref(),
            key,
            data,
            headers,
            timeout,
            callback,
        );
    }

    fn submit_delete(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_delete(self.inner.as_ref(), key, callback);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_delete_with_headers(
            self.inner.as_ref(),
            key,
            headers,
            callback,
        );
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_list(self.inner.as_ref(), prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_head(self.inner.as_ref(), key, callback);
    }
}

impl BlockingDeleteStorageBackend {
    fn new(
        inner: Arc<crate::storage::filesystem::FileSystem>,
        block_key: String,
        delete_started: std::sync::mpsc::Sender<()>,
        release_delete: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner,
            block_key,
            delete_started: Mutex::new(Some(delete_started)),
            release_delete,
        }
    }
}

impl crate::storage::StorageBackend for BlockingDeleteStorageBackend {
    fn submit_read(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_read(self.inner.as_ref(), key, callback);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_write(self.inner.as_ref(), key, data, callback);
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_write_with_headers(
            self.inner.as_ref(),
            key,
            data,
            headers,
            callback,
        );
    }

    fn submit_delete(&self, key: &str, callback: crate::storage::StorageCallback) {
        if key == self.block_key {
            if let Ok(mut started) = self.delete_started.lock() {
                if let Some(tx) = started.take() {
                    let _ = tx.send(());
                }
            }
            while !self.release_delete.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        crate::storage::StorageBackend::submit_delete(self.inner.as_ref(), key, callback);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_delete_with_headers(
            self.inner.as_ref(),
            key,
            headers,
            callback,
        );
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_list(self.inner.as_ref(), prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_head(self.inner.as_ref(), key, callback);
    }
}

struct FailOnceDeleteStorageBackend {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    fail_key: String,
    failed: AtomicBool,
    delete_attempts: AtomicUsize,
}

impl FailOnceDeleteStorageBackend {
    fn new(inner: Arc<crate::storage::filesystem::FileSystem>, fail_key: String) -> Self {
        Self {
            inner,
            fail_key,
            failed: AtomicBool::new(false),
            delete_attempts: AtomicUsize::new(0),
        }
    }

    fn delete_attempts(&self) -> usize {
        self.delete_attempts.load(Ordering::SeqCst)
    }
}

impl crate::storage::StorageBackend for FailOnceDeleteStorageBackend {
    fn submit_read(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_read(self.inner.as_ref(), key, callback);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_write(self.inner.as_ref(), key, data, callback);
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_write_with_headers(
            self.inner.as_ref(),
            key,
            data,
            headers,
            callback,
        );
    }

    fn submit_delete(&self, key: &str, callback: crate::storage::StorageCallback) {
        self.delete_attempts.fetch_add(1, Ordering::SeqCst);
        if key == self.fail_key && !self.failed.swap(true, Ordering::SeqCst) {
            let _ = callback.send(crate::storage::StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: crate::storage::StorageOutcome::Err(
                    "injected first cloud SST delete failure".to_string(),
                ),
            });
            return;
        }

        crate::storage::StorageBackend::submit_delete(self.inner.as_ref(), key, callback);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::StorageBackend::submit_delete_with_headers(
            self.inner.as_ref(),
            key,
            headers,
            callback,
        );
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_list(self.inner.as_ref(), prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::StorageCallback) {
        crate::storage::StorageBackend::submit_head(self.inner.as_ref(), key, callback);
    }
}

fn seal_segment_for_test(el: &mut EventLoop) -> crate::common::MidgeResult<(u64, u64)> {
    let (seg_id, max_sequence) = seal_segment_without_remote_proof_for_test(el)?;
    if let Some(storage) = el.hybrid_storage.as_ref() {
        let local_path = el.state.wal_dir.join(crate::wal::segment_file_name(seg_id));
        storage
            .publish_remote_wal_segment(
                seg_id,
                max_sequence,
                &local_path,
                el.state.writer_epoch,
                &crate::common::OperationDeadline::unbounded(),
            )
            .expect("publish remote WAL for test CloudAck");
    }
    Ok((seg_id, max_sequence))
}

fn seal_segment_without_remote_proof_for_test(
    el: &mut EventLoop,
) -> crate::common::MidgeResult<(u64, u64)> {
    let seg_id = el.state.wal.current_segment_id;
    let max_sequence = el.wal_actor.flush_for_cloud_upload(&mut el.state)?;
    el.wal_actor.rotate(&mut el.state)?;
    el.durability
        .rotate_from_to(seg_id, el.state.wal.current_segment_id)?;
    copy_local_segment_to_remote_wal_for_test(el, seg_id);
    el.wal_actor.complete_cloud_upload_seal(&mut el.state);
    el.durability
        .record_cloud_segment_inflight(seg_id, max_sequence);
    el.durability.record_cloud_flush();
    Ok((seg_id, max_sequence))
}

#[test]
fn should_notify_all_waiters_given_multiple_seal_requests_for_same_cloud_segment(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sequence = append_cloud_async_put(&mut el)?;
    let first_id = 18_101;
    let second_id = 18_102;
    let first_rx = el.router.register(first_id, "TestRequest");
    let second_rx = el.router.register(second_id, "TestRequest");
    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;

    // Act
    for request_id in [first_id, second_id] {
        assert_eq!(
            el.handle_runtime_msg(
                RuntimeMsg::SealWalForCloud {
                    request_id,
                    sequence,
                    wait_for_ack: true,
                },
                &msg_rx,
            ),
            super::super::HandleOutcome::Continue
        );
    }
    let segment_id = el
        .durability
        .inflight_segment_for_sequence(sequence)
        .expect("shared inflight segment");
    copy_local_segment_to_remote_wal_for_test(&el, segment_id);
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence: sequence,
    });

    // Assert
    for (request_id, receiver) in [(first_id, first_rx), (second_id, second_rx)] {
        assert!(matches!(
            receiver.recv().expect("cloud durability response"),
            RuntimeResponse::Ok { request_id: actual } if actual == request_id
        ));
    }
    let metrics_id = 18_103;
    let metrics_rx = el.router.register(metrics_id, "TestRequest");
    el.handle_runtime_msg(
        RuntimeMsg::GetRuntimeMetrics {
            request_id: metrics_id,
        },
        &msg_rx,
    );
    let RuntimeResponse::RuntimeMetricsSnapshot { snapshot, .. } =
        metrics_rx.recv().expect("runtime metrics response")
    else {
        panic!("unexpected runtime metrics response");
    };
    assert_eq!(snapshot.durability_waiters_fanned_out_total, 2);
    Ok(())
}

fn remote_wal_path_for_test(el: &EventLoop, segment_id: u64) -> PathBuf {
    el.state
        .db_path
        .join("cloud_store")
        .join(crate::wal::cloud_segment_object_key(
            segment_id,
            el.state.writer_epoch,
        ))
}

fn remote_sst_path_for_test(el: &EventLoop, sst_name: &str) -> PathBuf {
    el.state
        .db_path
        .join("cloud_store")
        .join("sst")
        .join(sst_name)
}

fn write_test_file(path: PathBuf, data: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create test file parent");
    }
    std::fs::write(path, data).expect("write test file");
}

fn copy_local_segment_to_remote_wal_for_test(el: &EventLoop, segment_id: u64) {
    let local_path = el
        .state
        .wal_dir
        .join(crate::wal::segment_file_name(segment_id));
    let remote_path = remote_wal_path_for_test(el, segment_id);
    if let Some(parent) = remote_path.parent() {
        std::fs::create_dir_all(parent).expect("create remote WAL parent");
    }
    std::fs::copy(&local_path, &remote_path).unwrap_or_else(|error| {
        panic!(
            "copy local WAL '{}' to remote WAL '{}': {error}",
            local_path.display(),
            remote_path.display()
        )
    });
}

fn publish_remote_wal_bytes_for_test(
    el: &EventLoop,
    segment_id: u64,
    max_sequence: u64,
    bytes: &[u8],
) {
    let local_path = el
        .state
        .wal_dir
        .join(crate::wal::segment_file_name(segment_id));
    write_test_file(local_path.clone(), bytes);
    write_test_file(remote_wal_path_for_test(el, segment_id), bytes);
    if let Some(storage) = el.hybrid_storage.as_ref() {
        storage
            .publish_remote_wal_segment(
                segment_id,
                max_sequence,
                &local_path,
                el.state.writer_epoch,
                &crate::common::OperationDeadline::unbounded(),
            )
            .expect("publish authoritative remote WAL for test");
    }
}

fn seed_cloud_prune_candidate(el: &mut EventLoop, segment_id: u64, max_sequence: u64) {
    el.state.wal.current_segment_id = segment_id + 1;
    el.state.manifest.last_persisted_sequence = max_sequence;
    el.cloud_wal.acked_segments.insert(segment_id, max_sequence);
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"prune-candidate"),
        Some(Bytes::from_static(b"value")),
        max_sequence,
        el.state.writer_epoch,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode test WAL record");
    let mut bytes = Vec::new();
    crate::wal::frame::append_frame(&mut bytes, &payload).expect("append test WAL frame");
    publish_remote_wal_bytes_for_test(el, segment_id, max_sequence, &bytes);
}

fn seed_cloud_prune_candidate_with_records(
    el: &mut EventLoop,
    segment_id: u64,
    max_sequence: u64,
    records: Vec<crate::wal::WalRecord>,
) {
    el.state.wal.current_segment_id = segment_id + 1;
    el.state.manifest.last_persisted_sequence = max_sequence;
    el.cloud_wal.acked_segments.insert(segment_id, max_sequence);

    let mut bytes = Vec::new();
    for mut record in records {
        record.writer_epoch = el.state.writer_epoch;
        let payload = crate::wal::encoding::encode(&record).expect("encode test WAL record");
        crate::wal::frame::append_frame(&mut bytes, &payload).expect("append test WAL frame");
    }
    publish_remote_wal_bytes_for_test(el, segment_id, max_sequence, &bytes);
}

fn add_manifest_sst_for_test(el: &mut EventLoop, sst_name: &str, max_sequence: u64) {
    el.state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.to_string(),
        level: 0,
        size_bytes: 128,
        cf_id: 0,
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"z".to_vec()),
        smallest_seq: Some(1),
        largest_seq: Some(max_sequence),
        ..Default::default()
    });
}

fn add_manifest_sst_meta_for_test(
    el: &mut EventLoop,
    sst_name: &str,
    cf_id: u32,
    key: &[u8],
    smallest_seq: u64,
    largest_seq: u64,
) {
    let bytes = valid_sst_bytes_for_test(key, b"value", largest_seq);
    el.state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.to_string(),
        level: 0,
        size_bytes: bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&bytes)),
        cf_id,
        smallest_key: Some(key.to_vec()),
        largest_key: Some(key.to_vec()),
        smallest_seq: Some(smallest_seq),
        largest_seq: Some(largest_seq),
        ..Default::default()
    });
    write_test_file(remote_sst_path_for_test(el, sst_name), &bytes);
}

fn valid_sst_bytes_for_test(key: &[u8], value: &[u8], seq: u64) -> Vec<u8> {
    use crate::sst::SstFactory;

    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("create test SST writer");
    writer
        .add_with_meta(key, Some(value), seq, 0, None)
        .expect("add test SST entry");
    writer.finish_bytes().expect("finish test SST bytes")
}

fn valid_range_tombstone_sst_bytes_for_test(start: &[u8], end: &[u8], seq: u64) -> Vec<u8> {
    use crate::sst::SstFactory;

    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("create test SST writer");
    writer
        .add_range_tombstone(start, end, seq)
        .expect("add test range tombstone");
    writer
        .finish_bytes()
        .expect("finish range tombstone test SST bytes")
}

fn add_valid_manifest_sst_for_test(
    el: &mut EventLoop,
    sst_name: &str,
    max_sequence: u64,
) -> Vec<u8> {
    let bytes = valid_sst_bytes_for_test(b"prune-candidate", b"value", max_sequence);
    el.state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.to_string(),
        level: 0,
        size_bytes: bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&bytes)),
        cf_id: 0,
        smallest_key: Some(b"prune-candidate".to_vec()),
        largest_key: Some(b"prune-candidate".to_vec()),
        smallest_seq: Some(max_sequence),
        largest_seq: Some(max_sequence),
        ..Default::default()
    });
    write_test_file(remote_sst_path_for_test(el, sst_name), &bytes);
    bytes
}

fn add_valid_range_tombstone_manifest_sst_for_test(
    el: &mut EventLoop,
    sst_name: &str,
    start: &[u8],
    end: &[u8],
    seq: u64,
) -> Vec<u8> {
    let bytes = valid_range_tombstone_sst_bytes_for_test(start, end, seq);
    el.state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.to_string(),
        level: 0,
        size_bytes: bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&bytes)),
        cf_id: 0,
        smallest_key: Some(start.to_vec()),
        largest_key: Some(end.to_vec()),
        smallest_seq: Some(seq),
        largest_seq: Some(seq),
        ..Default::default()
    });
    write_test_file(remote_sst_path_for_test(el, sst_name), &bytes);
    bytes
}

fn drain_prune_completion_for_test(el: &mut EventLoop) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        el.tick_hybrid_storage();
        el.drain_hybrid_storage_events();
        if el.cloud_wal.prune_inflight.is_empty() && el.cloud_wal_prune_worker.is_none() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn put_cloud_metadata_for_test(
    cloud: &crate::storage::cloud::CloudStorage,
    file_name: &str,
    data: Vec<u8>,
) {
    let key = crate::storage::cloud::cloud_metadata_key(file_name);
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(&key, data, vec![], tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(crate::storage::cloud::CloudEvent::Put {
            result: crate::storage::cloud::CloudOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("metadata put for '{key}' failed: {other:?}"),
    }
}

fn get_cloud_metadata_for_test(
    cloud: &crate::storage::cloud::CloudStorage,
    file_name: &str,
) -> Vec<u8> {
    let key = crate::storage::cloud::cloud_metadata_key(file_name);
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get(&key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(crate::storage::cloud::CloudEvent::Get {
            result: crate::storage::cloud::CloudOutcome::Ok(data),
            ..
        }) => data,
        other => panic!("metadata get for '{key}' failed: {other:?}"),
    }
}

fn put_all_cloud_metadata_for_test(cloud: &crate::storage::cloud::CloudStorage, db_path: &Path) {
    for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
        let local_path = db_path.join(file_name);
        if !local_path.exists() {
            continue;
        }
        let data = std::fs::read(&local_path).expect("read local metadata");
        put_cloud_metadata_for_test(cloud, file_name, data);
    }
}

fn delete_cloud_metadata_for_test(cloud: &crate::storage::cloud::CloudStorage, file_name: &str) {
    let key = crate::storage::cloud::cloud_metadata_key(file_name);
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_delete(&key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(crate::storage::cloud::CloudEvent::Delete {
            result: crate::storage::cloud::CloudOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("metadata delete for '{key}' failed: {other:?}"),
    }
}

struct BudgetConsumingMetadataBackend {
    inner: crate::storage::cloud::MockCloudBackend,
    get_calls: AtomicUsize,
    retained_callbacks: Mutex<Vec<crate::storage::cloud::CloudCallback>>,
}

struct BudgetConsumingMetadataProofBackend {
    inner: Arc<crate::storage::cloud::MockCloudBackend>,
    get_calls: AtomicUsize,
    retained_callbacks: Mutex<Vec<crate::storage::cloud::CloudCallback>>,
}

struct ProviderTimeoutMetadataBackend {
    inner: crate::storage::cloud::MockCloudBackend,
}

impl ProviderTimeoutMetadataBackend {
    fn new() -> Self {
        Self {
            inner: crate::storage::cloud::MockCloudBackend::new(),
        }
    }
}

impl crate::storage::cloud::CloudBackend for ProviderTimeoutMetadataBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_put(key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        let _ = callback.send(crate::storage::cloud::CloudEvent::Get {
            key: key.to_string(),
            result: crate::storage::cloud::CloudOutcome::Err(
                crate::storage::cloud::CloudError::Transport(
                    "request timed out after 30 ms".to_string(),
                ),
            ),
        });
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_delete(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}

impl BudgetConsumingMetadataProofBackend {
    fn new(inner: Arc<crate::storage::cloud::MockCloudBackend>) -> Self {
        Self {
            inner,
            get_calls: AtomicUsize::new(0),
            retained_callbacks: Mutex::new(Vec::new()),
        }
    }
}

impl crate::storage::cloud::CloudBackend for BudgetConsumingMetadataProofBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_put(key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        if self.get_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let inner = Arc::clone(&self.inner);
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(80));
                inner.submit_get(&key, callback);
            });
        } else {
            self.inner.submit_get(key, callback);
        }
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_delete(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, _key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.retained_callbacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(callback);
    }
}

impl BudgetConsumingMetadataBackend {
    fn new() -> Self {
        Self {
            inner: crate::storage::cloud::MockCloudBackend::new(),
            get_calls: AtomicUsize::new(0),
            retained_callbacks: Mutex::new(Vec::new()),
        }
    }
}

impl crate::storage::cloud::CloudBackend for BudgetConsumingMetadataBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_put(key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        if self.get_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(80));
                let _ = callback.send(crate::storage::cloud::CloudEvent::Get {
                    key,
                    result: crate::storage::cloud::CloudOutcome::Err(
                        crate::storage::cloud::CloudError::NotFound(
                            "delayed metadata miss".to_string(),
                        ),
                    ),
                });
            });
        } else {
            self.retained_callbacks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(callback);
        }
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_delete(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}

struct AdvanceManifestBeforeHeadBackend {
    inner: crate::storage::cloud::MockCloudBackend,
    advanced_manifest: Vec<u8>,
    advanced: AtomicBool,
}

impl AdvanceManifestBeforeHeadBackend {
    fn new(advanced_manifest: Vec<u8>) -> Self {
        Self {
            inner: crate::storage::cloud::MockCloudBackend::new(),
            advanced_manifest,
            advanced: AtomicBool::new(false),
        }
    }
}

impl crate::storage::cloud::CloudBackend for AdvanceManifestBeforeHeadBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_put(key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_get(key, callback);
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_get_range(key, start, end, callback);
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        self.inner.submit_delete(key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_list(prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        if key.ends_with("metadata/manifest.json") && !self.advanced.swap(true, Ordering::SeqCst) {
            let (tx, rx) = std::sync::mpsc::channel();
            self.inner
                .submit_put(key, self.advanced_manifest.clone(), vec![], tx);
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(crate::storage::cloud::CloudEvent::Put {
                    result: crate::storage::cloud::CloudOutcome::Ok(()),
                    ..
                }) => {}
                other => panic!("advance remote manifest before HEAD failed: {other:?}"),
            }
        }
        self.inner.submit_head(key, callback);
    }
}

#[test]
fn should_bound_create_metadata_mirror_by_runtime_response_deadline(
) -> crate::common::MidgeResult<()> {
    // Arrange: the first metadata GET consumes most of the caller budget and
    // the next callback never arrives. The committed DDL must still succeed,
    // but its auxiliary mirror must not receive a fresh storage timeout.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_millis(300);
    el.cloud_metadata_storage = Some(Arc::new(
        crate::storage::cloud::CloudStorage::new_with_timeout(
            Arc::new(BudgetConsumingMetadataBackend::new()),
            "metadata-deadline".to_string(),
            Duration::from_secs(1),
        ),
    ));
    let request_id = 9_601;
    let response_rx = el.router.register(request_id, "ManifestCreateColumnFamily");
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    let started = Instant::now();
    el.handle_runtime_msg(
        RuntimeMsg::ManifestCreateColumnFamily {
            request_id,
            name: "deadline-bounded".to_string(),
        },
        &msg_rx,
    );
    let elapsed = started.elapsed();

    // Assert
    assert!(
        elapsed < Duration::from_millis(350),
        "metadata mirror exceeded the caller's shared deadline: {elapsed:?}"
    );
    assert!(matches!(
        response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("committed create response"),
        RuntimeResponse::ColumnFamilyCreated {
            request_id: 9_601,
            ..
        }
    ));
    assert!(
        el.state.persistence_anomaly_detected(),
        "an incomplete auxiliary metadata mirror must degrade persistence health"
    );
    Ok(())
}

#[test]
fn should_share_create_deadline_with_remote_ddl_registry_cas() -> crate::common::MidgeResult<()> {
    // Arrange: the registry existence check consumes most of the request
    // budget. The following CAS must receive only the remaining allowance,
    // rather than a fresh storage timeout.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_secs(1);
    let cloud_fs = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("open deadline-bounded DDL cloud backend"),
    );
    let ddl_cloud = Arc::new(BudgetConsumingDdlBackend::new(cloud_fs));
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local"))
            .expect("open deadline-bounded DDL local backend"),
    );
    el.hybrid_storage = Some(Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        ddl_cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let request_id = 9_603;
    let response_rx = el.router.register(request_id, "ManifestCreateColumnFamily");
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    let started = Instant::now();
    el.handle_runtime_msg(
        RuntimeMsg::ManifestCreateColumnFamily {
            request_id,
            name: "ddl-deadline-bounded".to_string(),
        },
        &msg_rx,
    );
    let elapsed = started.elapsed();

    // Assert
    assert!(
        elapsed < Duration::from_millis(1_200),
        "remote DDL registry work escaped the shared deadline: {elapsed:?}"
    );
    let response = response_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("DDL deadline response");
    assert!(
        matches!(
            response,
            RuntimeResponse::Error {
                error: crate::common::MidgeError::Timeout(_),
                ..
            }
        ),
        "unexpected DDL deadline response: {response:?}"
    );
    let cas_timeouts = ddl_cloud.registry_cas_timeouts();
    assert_eq!(
        cas_timeouts.len(),
        1,
        "the DDL registry CAS must be attempted once"
    );
    assert!(
        cas_timeouts[0] < Duration::from_millis(900),
        "DDL CAS received a fresh timeout instead of the remaining request budget: {:?}",
        cas_timeouts[0]
    );
    assert!(
        el.state
            .manifest
            .get_column_family_by_name("ddl-deadline-bounded")
            .is_none(),
        "a registry CAS that did not start within budget must not commit locally"
    );
    Ok(())
}

#[test]
fn should_keep_ddl_fenced_until_delayed_cas_commit_is_observed() -> crate::common::MidgeResult<()> {
    // Arrange: the provider reports a timeout after admitting the CAS, an
    // immediate authority reread still sees the old registry, and the mutation
    // commits later on its provider worker.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_secs(1);
    let cloud_fs = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("open delayed DDL cloud backend"),
    );
    let delayed_cloud = Arc::new(DelayedCommitDdlBackend::new(cloud_fs));
    let commit_complete = delayed_cloud.commit_complete();
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local"))
            .expect("open delayed DDL local backend"),
    );
    el.hybrid_storage = Some(Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        delayed_cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act: the first request becomes ambiguous; a second DDL must not clear the
    // prepare merely because its early reread is negative.
    let first_request = 9_604;
    let first_response = el
        .router
        .register(first_request, "ManifestCreateColumnFamily");
    el.handle_runtime_msg(
        RuntimeMsg::ManifestCreateColumnFamily {
            request_id: first_request,
            name: "delayed-authority".to_string(),
        },
        &msg_rx,
    );
    assert!(matches!(
        first_response.recv_timeout(Duration::from_secs(1)),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Fenced(_),
            ..
        })
    ));
    assert!(el.ddl_authority_ambiguous);
    assert!(el.state.db_path.join("ddl.prepare.json").exists());

    let blocked_request = 9_605;
    let blocked_response = el
        .router
        .register(blocked_request, "ManifestCreateColumnFamily");
    el.handle_runtime_msg(
        RuntimeMsg::ManifestCreateColumnFamily {
            request_id: blocked_request,
            name: "must-stay-fenced".to_string(),
        },
        &msg_rx,
    );
    assert!(matches!(
        blocked_response.recv_timeout(Duration::from_secs(1)),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Fenced(_),
            ..
        })
    ));
    assert!(el.ddl_authority_ambiguous);
    assert!(el.state.db_path.join("ddl.prepare.json").exists());

    let commit_deadline = Instant::now() + Duration::from_secs(1);
    while !commit_complete.load(Ordering::SeqCst) && Instant::now() < commit_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(commit_complete.load(Ordering::SeqCst));

    let reconcile_request = 9_606;
    let reconcile_response = el
        .router
        .register(reconcile_request, "ManifestCreateColumnFamily");
    el.handle_runtime_msg(
        RuntimeMsg::ManifestCreateColumnFamily {
            request_id: reconcile_request,
            name: "delayed-authority".to_string(),
        },
        &msg_rx,
    );

    // Assert: positive operation-id readback applies the committed edit and is
    // the only event that clears the in-process authority fence.
    assert!(matches!(
        reconcile_response.recv_timeout(Duration::from_secs(1)),
        Ok(RuntimeResponse::ColumnFamilyCreated { .. })
    ));
    assert!(!el.ddl_authority_ambiguous);
    assert!(!el.state.db_path.join("ddl.prepare.json").exists());
    assert!(el
        .state
        .manifest
        .get_column_family_by_name("delayed-authority")
        .is_some());
    Ok(())
}

#[test]
fn should_preserve_provider_timeout_from_manifest_metadata_mirror() -> crate::common::MidgeResult<()>
{
    // Arrange: provider executors report their own request deadline through a
    // typed transport error before the outer runtime deadline expires.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.cloud_metadata_storage = Some(Arc::new(crate::storage::cloud::CloudStorage::new(
        Arc::new(ProviderTimeoutMetadataBackend::new()),
        "metadata-provider-timeout".to_string(),
    )));
    let request_id = 9_602;
    let response_rx = el.router.register(request_id, "ManifestPersist");
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    el.handle_runtime_msg(RuntimeMsg::ManifestPersist { request_id }, &msg_rx);

    // Assert
    match response_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("manifest persistence response")
    {
        RuntimeResponse::Error {
            error: crate::common::MidgeError::Timeout(message),
            ..
        } => assert!(
            message.contains("metadata"),
            "unexpected timeout: {message}"
        ),
        other => panic!("expected typed metadata timeout, got {other:?}"),
    }
    Ok(())
}

#[test]
fn should_not_overwrite_newer_remote_manifest_metadata_when_mirroring(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.state.manifest.last_persisted_sequence = 10;
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(crate::common::MidgeError::Internal)?;

    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend,
        "metadata-test".to_string(),
    ));
    let remote_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 11,
        ..Default::default()
    };
    put_cloud_metadata_for_test(
        &metadata_storage,
        "manifest.json",
        serde_json::to_vec_pretty(&remote_manifest).expect("serialize remote manifest"),
    );
    el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));

    let error = el
        .mirror_metadata_to_authoritative_cloud()
        .expect_err("newer remote manifest metadata must reject stale local mirror");

    // Act
    // Assert
    assert!(
        error.to_string().contains("newer")
            || error.to_string().contains("ahead")
            || error.to_string().contains("stale"),
        "unexpected stale metadata mirror error: {error}"
    );
    let retained: crate::metadata::Manifest = serde_json::from_slice(&get_cloud_metadata_for_test(
        &metadata_storage,
        "manifest.json",
    ))
    .expect("parse retained remote manifest");
    assert_eq!(
        retained.last_persisted_sequence, 11,
        "stale metadata mirror must not overwrite newer remote manifest"
    );

    Ok(())
}

#[test]
fn should_not_rewrite_unchanged_cloud_metadata_when_mirroring() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(crate::common::MidgeError::Internal)?;
    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend.clone(),
        "metadata-idempotence".to_string(),
    ));
    el.cloud_metadata_storage = Some(metadata_storage);
    el.mirror_metadata_to_authoritative_cloud()?;
    metadata_backend.clear_history();

    // Act
    el.mirror_metadata_to_authoritative_cloud()?;

    // Assert
    assert!(
        metadata_backend.get_uploads().is_empty(),
        "unchanged metadata must not create new object versions"
    );

    Ok(())
}

#[test]
fn should_not_overwrite_manifest_metadata_advanced_after_preflight(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.state.manifest.last_persisted_sequence = 30;
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(crate::common::MidgeError::Internal)?;

    let advanced_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 31,
        ..Default::default()
    };
    let metadata_backend = Arc::new(AdvanceManifestBeforeHeadBackend::new(
        serde_json::to_vec_pretty(&advanced_manifest).expect("serialize advanced manifest"),
    ));
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend,
        "metadata-race".to_string(),
    ));
    let initial_manifest = crate::metadata::Manifest {
        last_persisted_sequence: 30,
        ..Default::default()
    };
    put_cloud_metadata_for_test(
        &metadata_storage,
        "manifest.json",
        serde_json::to_vec_pretty(&initial_manifest).expect("serialize initial manifest"),
    );
    el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));

    let error = el
        .mirror_metadata_to_authoritative_cloud()
        .expect_err("manifest advancing after preflight must reject stale metadata mirror");

    // Act
    // Assert
    assert!(
        error.to_string().contains("ahead") || error.to_string().contains("stale"),
        "unexpected metadata mirror race error: {error}"
    );
    let retained: crate::metadata::Manifest = serde_json::from_slice(&get_cloud_metadata_for_test(
        &metadata_storage,
        "manifest.json",
    ))
    .expect("parse retained race manifest");
    assert_eq!(
        retained.last_persisted_sequence, 31,
        "metadata mirror must not overwrite a manifest that advanced after preflight"
    );

    Ok(())
}

#[test]
fn should_start_with_no_hybrid_storage() {
    // Arrange
    // Act
    let event_loop = create_test_event_loop().expect("Should create event loop");

    // Assert
    assert!(event_loop.hybrid_storage.is_none());
}

#[test]
fn should_set_hybrid_storage() {
    // Arrange
    let mut event_loop = create_test_event_loop().expect("Should create event loop");
    assert!(event_loop.hybrid_storage.is_none());

    let db_path = event_loop.state.db_path.clone();
    let local = std::sync::Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("hybrid_local"))
            .expect("create local backend"),
    );
    let cloud = std::sync::Arc::new(
        crate::storage::filesystem::FileSystem::new(db_path.join("cloud_store"))
            .expect("create cloud backend"),
    );
    let hybrid_storage = std::sync::Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));

    // Act - the real setter, not just a construction check
    event_loop.set_hybrid_storage(std::sync::Arc::clone(&hybrid_storage));

    // Assert
    let stored = event_loop
        .hybrid_storage
        .as_ref()
        .expect("hybrid storage should now be set");
    assert!(std::sync::Arc::ptr_eq(stored, &hybrid_storage));
}

#[test]
fn should_not_prune_remote_wal_when_manifest_sst_is_missing_from_cloud(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    add_manifest_sst_for_test(&mut el, "missing.sst", max_sequence);

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must be retained when a manifest-referenced cloud SST is missing"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

    Ok(())
}

#[test]
fn should_keep_event_loop_responsive_while_callerless_wal_prune_finishes(
) -> crate::common::MidgeResult<()> {
    // Arrange: seed a valid prune candidate while the backend is responsive,
    // then make one healthy provider proof slower than the response budget.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_millis(100);
    let cloud_fs = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("open delayed prune cloud backend"),
    );
    let delayed_cloud = Arc::new(ArmedDelayedHeadStorageBackend::new(
        cloud_fs,
        Duration::from_millis(250),
    ));
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local"))
            .expect("open delayed prune local backend"),
    );
    el.hybrid_storage = Some(Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        delayed_cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));

    let segment_id = 61;
    let max_sequence = 61;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    add_valid_manifest_sst_for_test(&mut el, "deadline-prune.sst", max_sequence);
    delayed_cloud.arm();

    // Act
    let started = Instant::now();
    el.prune_cloud_wal_segments_covered_by_manifest();
    let elapsed = started.elapsed();

    // Assert
    assert!(
        elapsed < Duration::from_millis(150),
        "callerless WAL prune monopolized the event loop: {elapsed:?}"
    );
    drain_prune_completion_for_test(&mut el);
    assert!(
        !remote_wal_path_for_test(&el, segment_id).exists(),
        "slow-but-valid callerless cleanup should eventually finish"
    );
    assert!(!el.cloud_wal.acked_segments.contains_key(&segment_id));
    assert!(!el.cloud_wal.prune_inflight.contains(&segment_id));
    Ok(())
}

#[test]
fn should_keep_event_loop_responsive_when_cloud_metadata_proof_times_out(
) -> crate::common::MidgeResult<()> {
    // Arrange: all WAL/SST proofs are responsive. The metadata GET completes,
    // but its following HEAD never answers.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_millis(100);
    let segment_id = 62;
    let max_sequence = 62;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    add_valid_manifest_sst_for_test(&mut el, "metadata-deadline-prune.sst", max_sequence);
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(crate::common::MidgeError::Internal)?;

    let inner = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new_with_timeout(
        Arc::new(BudgetConsumingMetadataProofBackend::new(Arc::clone(&inner))),
        "metadata-prune-deadline".to_string(),
        Duration::from_secs(1),
    ));
    put_all_cloud_metadata_for_test(&metadata_storage, &el.state.db_path);
    el.cloud_metadata_storage = Some(metadata_storage);

    // Act
    let started = Instant::now();
    el.prune_cloud_wal_segments_covered_by_manifest();
    let elapsed = started.elapsed();

    // Assert
    assert!(
        elapsed < Duration::from_millis(150),
        "metadata proof monopolized the event loop: {elapsed:?}"
    );
    drain_prune_completion_for_test(&mut el);
    assert_eq!(
        el.cloud_wal.acked_segments.get(&segment_id),
        Some(&max_sequence),
        "metadata timeout must retain WAL authority for retry"
    );
    assert!(!el.cloud_wal.prune_inflight.contains(&segment_id));
    Ok(())
}

#[test]
fn should_reconcile_wal_prune_when_catalog_retirement_commits_before_timeout(
) -> crate::common::MidgeResult<()> {
    // Arrange: the provider commits the catalog retirement, then withholds the
    // first readback HEAD until the maintenance deadline expires.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_millis(100);
    let cloud_fs = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("open ambiguous prune cloud backend"),
    );
    let ambiguous_cloud = Arc::new(CommitThenBlockCatalogReadbackBackend::new(cloud_fs));
    let local: Arc<dyn crate::storage::StorageBackend> = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local"))
            .expect("open ambiguous prune local backend"),
    );
    let ambiguous_backend: Arc<dyn crate::storage::StorageBackend> = ambiguous_cloud.clone();
    let (storage_event_tx, storage_event_rx) = crossbeam::channel::unbounded();
    el.hybrid_storage = Some(Arc::new(
        crate::storage::HybridStorage::new_with_class_stores_and_event_sender(
            local,
            Arc::clone(&ambiguous_backend),
            Arc::clone(&ambiguous_backend),
            ambiguous_backend,
            storage_event_tx,
            Duration::from_millis(100),
        ),
    ));
    el.hybrid_storage_events = Some(storage_event_rx);

    let segment_id = 63;
    let max_sequence = 63;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    add_valid_manifest_sst_for_test(&mut el, "ambiguous-retirement.sst", max_sequence);
    ambiguous_cloud.arm();

    // Act: the first pass times out after the authority update. A later pass
    // must recognize that committed state and finish local bookkeeping.
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);
    assert_eq!(
        el.cloud_wal.acked_segments.get(&segment_id),
        Some(&max_sequence),
        "ambiguous completion should remain retryable until readback"
    );
    let catalog_proof = el
        .hybrid_storage
        .as_ref()
        .expect("hybrid storage")
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read catalog after ambiguous retirement");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode catalog after ambiguous retirement");
    assert!(
        !catalog.segments.contains_key(&segment_id),
        "the first catalog CAS must have committed before its readback timed out"
    );
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Assert
    assert!(
        !el.cloud_wal.acked_segments.contains_key(&segment_id),
        "a confirmed-absent catalog entry must settle the prior ambiguous retirement"
    );
    assert!(!el.cloud_wal.prune_inflight.contains(&segment_id));
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "ambiguous retirement may leak the ignored WAL object but must not delete it without its proof"
    );
    Ok(())
}

#[test]
fn should_retain_reclaimed_sst_when_salvage_metadata_mirror_exceeds_deadline(
) -> crate::common::MidgeResult<()> {
    // Arrange: salvage mode may keep serving through metadata degradation, but
    // destructive GC still needs proof that the remote manifest was updated.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.state
        .set_recovery_policy_for_test(crate::config::RecoveryPolicy::Salvage);
    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    el.cloud_metadata_storage = Some(Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend,
        "salvage-gc-deadline".to_string(),
    )));
    let sst_name = "salvage-retained-after-mirror-timeout.sst";
    let sst_bytes = valid_sst_bytes_for_test(b"salvage", b"value", 64);
    el.hybrid_storage
        .as_ref()
        .expect("hybrid storage")
        .write_sst_object(sst_name, sst_bytes)?;
    el.gc_actor
        .queue_manifest_reclamation([sst_name.to_string()]);
    let expired = crate::common::OperationDeadline::from_budget(Duration::ZERO);

    // Act
    el.retry_gc_within(&expired);

    // Assert
    assert!(
        el.gc_actor.has_manifest_reclamation(),
        "metadata timeout must keep destructive reclamation queued even in salvage mode"
    );
    assert!(
        remote_sst_path_for_test(&el, sst_name).exists(),
        "remote SST must remain while remote metadata may still reference it"
    );
    Ok(())
}

#[test]
fn should_retry_manifest_reclamation_after_metadata_publication_timeout(
) -> crate::common::MidgeResult<()> {
    // Arrange: the first attempt has no budget left, so the SST must remain
    // queued until callerless maintenance can publish the manifest safely.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    el.cloud_metadata_storage = Some(Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend,
        "gc-publication-retry".to_string(),
    )));
    let sst_name = "reclaimed-after-metadata-retry.sst";
    let sst_bytes = valid_sst_bytes_for_test(b"retry", b"value", 65);
    el.hybrid_storage
        .as_ref()
        .expect("hybrid storage")
        .write_sst_object(sst_name, sst_bytes)?;
    el.gc_actor
        .queue_manifest_reclamation([sst_name.to_string()]);
    let expired = crate::common::OperationDeadline::from_budget(Duration::ZERO);
    el.retry_gc_within(&expired);
    assert!(el.gc_actor.has_manifest_reclamation());
    assert!(remote_sst_path_for_test(&el, sst_name).exists());

    // Act: no new GC request arrives. A normal event-loop progress pass after
    // the retry backoff must resume the retained database obligation.
    std::thread::sleep(Duration::from_millis(25));
    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;
    el.progress_pass(&msg_rx);
    el.gc_actor.shutdown_workers();

    // Assert
    assert!(
        !el.gc_actor.has_manifest_reclamation(),
        "timed-out manifest reclamation must retry without unrelated activity"
    );
    assert!(
        !remote_sst_path_for_test(&el, sst_name).exists(),
        "the retained SST should be reclaimed after metadata publication succeeds"
    );
    Ok(())
}

#[test]
fn should_retry_manifest_reclamation_under_continuous_request_load(
) -> crate::common::MidgeResult<()> {
    // Arrange: an earlier bounded publication attempt retained the obligation,
    // and more than one ordinary request is already waiting when retry becomes
    // due.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.cloud_metadata_storage = Some(Arc::new(crate::storage::cloud::CloudStorage::new(
        Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        "gc-publication-busy-retry".to_string(),
    )));
    el.gc_actor
        .queue_manifest_reclamation(["busy-retry-reclamation.sst".to_string()]);
    let expired = crate::common::OperationDeadline::from_budget(Duration::ZERO);
    el.retry_gc_within(&expired);
    std::thread::sleep(Duration::from_millis(25));
    assert!(el.gc_actor.manifest_reclamation_retry_due());
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded::<RuntimeMsg>();
    msg_tx
        .send(RuntimeMsg::Noop { request_id: 90_301 })
        .expect("queue first request");
    msg_tx
        .send(RuntimeMsg::Noop { request_id: 90_302 })
        .expect("queue continuing request load");

    // Act: process one normal request while another remains queued.
    let first = msg_rx.recv().expect("receive first queued request");
    el.process_one(first, &msg_rx);
    el.gc_actor.shutdown_workers();

    // Assert
    assert!(
        !msg_rx.is_empty(),
        "the fixture must keep request pressure present during the retry slot"
    );
    assert!(
        !el.gc_actor.has_manifest_reclamation(),
        "bounded maintenance must receive a fairness slot under sustained load"
    );
    Ok(())
}

#[test]
fn should_bound_retry_gc_metadata_publication_by_maintenance_deadline(
) -> crate::common::MidgeResult<()> {
    // Arrange: the metadata provider consumes most of the maintenance budget,
    // then withholds the next callback. A fire-and-forget RetryGc message must
    // not grant the provider an unbounded event-loop wait.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_millis(100);
    el.cloud_metadata_storage = Some(Arc::new(
        crate::storage::cloud::CloudStorage::new_with_timeout(
            Arc::new(BudgetConsumingMetadataBackend::new()),
            "gc-publication-bounded-retry".to_string(),
            Duration::from_millis(250),
        ),
    ));
    el.gc_actor
        .queue_manifest_reclamation(["bounded-retry-reclamation.sst".to_string()]);
    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;

    // Act
    let started = Instant::now();
    el.handle_runtime_msg(RuntimeMsg::RetryGc, &msg_rx);
    let elapsed = started.elapsed();

    // Assert
    assert!(
        elapsed < Duration::from_millis(160),
        "RetryGc exceeded its bounded maintenance attempt: {elapsed:?}"
    );
    assert!(
        el.gc_actor.has_manifest_reclamation(),
        "a bounded retry timeout must retain the reclamation obligation"
    );
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retry_reclamation_discovery_after_journal_append_failure(
) -> crate::common::MidgeResult<()> {
    // Arrange: the reclaimable drop exists, but the first durable journal
    // append fails before any SST names can enter the actor-owned queue.
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let cf_id = el
        .state
        .manifest
        .create_column_family("journal-retry-drop".to_string());
    assert!(el
        .state
        .manifest
        .delete_column_family_with_reclamation(cf_id, 0, Vec::new()));
    fail::cfg(
        "midge::manifest::inject_no_space_on_append_edit_batch",
        "return",
    )
    .expect("configure reclaim journal failure");

    // Act: fail once, clear the storage fault, then let callerless maintenance
    // rediscover the still-reclaimable manifest state.
    el.retry_gc();
    fail::remove("midge::manifest::inject_no_space_on_append_edit_batch");
    assert!(
        el.gc_actor.manifest_reclamation_retry_due()
            || el.gc_actor.retry_deadline_timeout().is_some(),
        "journal failure must install an owned retry even before names are queued"
    );
    std::thread::sleep(Duration::from_millis(25));
    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;
    el.progress_pass(&msg_rx);

    // Assert
    assert!(
        el.state
            .manifest
            .column_families
            .iter()
            .any(|cf| cf.id == cf_id && cf.reclaimed),
        "idle retry must rediscover and durably apply the reclamation edit"
    );
    scenario.teardown();
    Ok(())
}

#[test]
fn should_not_prune_remote_wal_when_manifest_sst_is_corrupt_in_cloud(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    let sst_name = "corrupt.sst";
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    add_manifest_sst_for_test(&mut el, sst_name, max_sequence);
    write_test_file(remote_sst_path_for_test(&el, sst_name), b"not a valid sst");

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must be retained when a manifest-referenced cloud SST is unreadable"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

    Ok(())
}

#[test]
fn should_not_prune_remote_wal_when_cloud_metadata_is_missing() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(crate::common::MidgeError::Internal)?;
    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    el.cloud_metadata_storage = Some(Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend,
        "metadata-test".to_string(),
    )));

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must be retained when committed cloud metadata is missing"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

    Ok(())
}

#[test]
fn should_not_retire_wal_authority_without_cloud_manifest_base() -> crate::common::MidgeResult<()> {
    // Arrange: FORMAT is readable in both places, but neither recovery manifest
    // base exists. Non-manifest metadata alone must never authorize cleanup.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 64;
    let max_sequence = 64;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    add_valid_manifest_sst_for_test(&mut el, "missing-manifest-base.sst", max_sequence);
    for file_name in ["manifest.snapshot.json", "manifest.json"] {
        let path = el.state.db_path.join(file_name);
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(crate::common::MidgeError::Io(error)),
        }
    }
    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend,
        "metadata-no-manifest-base".to_string(),
    ));
    let format_bytes = std::fs::read(el.state.db_path.join("FORMAT"))?;
    put_cloud_metadata_for_test(&metadata_storage, "FORMAT", format_bytes);
    el.cloud_metadata_storage = Some(metadata_storage);

    // Act
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Assert
    assert!(remote_wal_path_for_test(&el, segment_id).exists());
    assert_eq!(
        el.cloud_wal.acked_segments.get(&segment_id),
        Some(&max_sequence)
    );
    let catalog_proof = el
        .hybrid_storage
        .as_ref()
        .expect("hybrid storage")
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read WAL catalog after refused cleanup");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode WAL catalog after refused cleanup");
    assert!(catalog.segments.contains_key(&segment_id));
    Ok(())
}

#[test]
fn should_retain_remote_wal_when_intent_metadata_needs_convergence(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    add_valid_manifest_sst_for_test(&mut el, "coverage.sst", max_sequence);
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(crate::common::MidgeError::Internal)?;

    el.state
        .append_intent(crate::runtime::IntentLogEntry::WalSynced {
            segment_id: 1,
            seqno: 1,
        })?;

    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend.clone(),
        "metadata-test".to_string(),
    ));
    put_all_cloud_metadata_for_test(&metadata_storage, &el.state.db_path);
    el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));
    el.verify_cloud_metadata_for_wal_cleanup()
        .expect("initial cloud metadata validation should cache proofs");

    el.state
        .append_intent(crate::runtime::IntentLogEntry::WalSynced {
            segment_id: 2,
            seqno: max_sequence,
        })?;
    let remote_intent_before = get_cloud_metadata_for_test(&metadata_storage, "intent_log.json");
    metadata_backend.clear_history();

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must remain while intent metadata needs normal publisher convergence"
    );
    assert_eq!(
        get_cloud_metadata_for_test(&metadata_storage, "intent_log.json"),
        remote_intent_before,
        "cleanup must never overwrite cloud metadata from a potentially stale snapshot"
    );
    assert!(
        !metadata_backend
            .get_uploads()
            .iter()
            .any(|(key, _)| key.ends_with("metadata/intent_log.json")),
        "cleanup verification must remain read-only"
    );
    assert_eq!(
        el.cloud_wal.acked_segments.get(&segment_id),
        Some(&max_sequence)
    );
    assert!(!el.cloud_wal.prune_inflight.contains(&segment_id));

    Ok(())
}

#[test]
fn should_not_prune_remote_wal_when_cloud_manifest_metadata_is_ahead(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    add_valid_manifest_sst_for_test(&mut el, "coverage.sst", max_sequence);
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(crate::common::MidgeError::Internal)?;

    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend,
        "metadata-test".to_string(),
    ));
    put_all_cloud_metadata_for_test(&metadata_storage, &el.state.db_path);
    let remote_manifest = crate::metadata::Manifest {
        last_persisted_sequence: max_sequence + 1,
        ..Default::default()
    };
    put_cloud_metadata_for_test(
        &metadata_storage,
        "manifest.json",
        serde_json::to_vec_pretty(&remote_manifest).expect("serialize remote manifest"),
    );
    el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must be retained when cloud manifest metadata is ahead of local state"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

    Ok(())
}

#[test]
fn should_prune_remote_wal_when_flush_intent_clear_is_mirrored() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    let sst_name = "flush-covered.sst";
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;

    let sst_bytes = valid_sst_bytes_for_test(b"prune-candidate", b"value", max_sequence);
    write_test_file(el.state.sst_dir.join(sst_name), &sst_bytes);
    write_test_file(remote_sst_path_for_test(&el, sst_name), &sst_bytes);
    let file_meta = crate::runtime::FileMeta {
        name: sst_name.to_string(),
        level: 0,
        size_bytes: sst_bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&sst_bytes)),
        cf_id: 0,
        smallest_key: Some(b"prune-candidate".to_vec()),
        largest_key: Some(b"prune-candidate".to_vec()),
        smallest_seq: Some(max_sequence),
        largest_seq: Some(max_sequence),
    };

    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend.clone(),
        "metadata-test".to_string(),
    ));
    el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));

    el.state
        .append_intent(crate::runtime::IntentLogEntry::FlushPublish {
            phase: crate::runtime::PublicationPhase::OutputDurable,
            cf_id: 0,
            sequence: max_sequence,
            file_meta: file_meta.clone(),
        })?;

    // Act
    el.publish_flushed_sst(0, sst_name, max_sequence, Some(file_meta), None)?;
    drain_prune_completion_for_test(&mut el);

    // Assert
    let local_intent =
        std::fs::read(el.state.db_path.join("intent_log.json")).expect("read local intent log");
    let remote_intent = get_cloud_metadata_for_test(&metadata_storage, "intent_log.json");
    assert_eq!(
        remote_intent, local_intent,
        "cloud intent metadata must reflect the committed local intent clear before WAL prune"
    );
    let metadata_uploads = metadata_backend.get_uploads();
    let expected_metadata_uploads = crate::storage::cloud::CLOUD_METADATA_FILES
        .iter()
        .filter(|file_name| el.state.db_path.join(file_name).exists())
        .count();
    assert_eq!(
            metadata_uploads.len(),
            expected_metadata_uploads,
            "flush publication should use the existing metadata mirror, not perform a second full mirror: {metadata_uploads:?}"
        );
    assert_eq!(
        metadata_uploads
            .iter()
            .filter(|(key, _)| key.ends_with("metadata/intent_log.json"))
            .count(),
        1,
        "intent metadata should be uploaded once as part of the existing mirror"
    );
    assert!(
        !remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL should prune after flush coverage and current cloud metadata are both verified"
    );

    Ok(())
}

#[test]
fn should_publish_control_intent_before_remote_compaction_sst() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.state.set_compaction_enabled(false);
    let input_sst = "ordered-control-input.sst";
    add_valid_manifest_sst_for_test(&mut el, input_sst, 10);
    let output_sst = "ordered-control-output.sst";
    let output_bytes = valid_sst_bytes_for_test(b"ordered", b"value", 11);
    write_test_file(el.state.sst_dir.join(output_sst), &output_bytes);

    let saw_compaction_intent = Arc::new(AtomicBool::new(false));
    let remote_existed_at_intent_publish = Arc::new(AtomicBool::new(false));
    let metadata_backend = Arc::new(ObserveIntentBeforeRemoteSstBackend {
        inner: Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        remote_sst_path: remote_sst_path_for_test(&el, output_sst),
        saw_compaction_intent: Arc::clone(&saw_compaction_intent),
        remote_existed_at_intent_publish: Arc::clone(&remote_existed_at_intent_publish),
    });
    el.cloud_metadata_storage = Some(Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend,
        "separate-control".to_string(),
    )));
    el.compaction_actor
        .prepare_for_completion_test(&mut el.state, &[input_sst.to_string()])?;
    let request_id = 4141;
    let response_rx = el.router.register(request_id, "TestRequest");
    let (_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    el.handle_runtime_msg(
        RuntimeMsg::CompactionComplete {
            request_id,
            input_ssts: vec![input_sst.to_string()],
            output_ssts: vec![output_sst.to_string()],
            cf_id: 0,
            target_level: 1,
            succeeded: true,
        },
        &msg_rx,
    );

    // Assert
    assert!(matches!(
        response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("compaction completion response"),
        RuntimeResponse::Ok { .. }
    ));
    assert!(saw_compaction_intent.load(Ordering::SeqCst));
    assert!(
        !remote_existed_at_intent_publish.load(Ordering::SeqCst),
        "control-cloud cleanup intent must be authoritative before SST upload"
    );
    Ok(())
}

#[test]
fn should_mirror_cleared_compaction_intent_after_cloud_sst_publish(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.state.set_compaction_enabled(false);

    let input_sst = "compaction-input.sst";
    add_valid_manifest_sst_for_test(&mut el, input_sst, 10);

    let output_sst = "compaction-output.sst";
    let output_bytes = valid_sst_bytes_for_test(b"prune-candidate", b"value", 10);
    write_test_file(el.state.sst_dir.join(output_sst), &output_bytes);

    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        "metadata-test".to_string(),
    ));
    el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));

    el.compaction_actor
        .prepare_for_completion_test(&mut el.state, &[input_sst.to_string()])?;
    let request_id = 4242;
    let response_rx = el.router.register(request_id, "TestRequest");
    let (_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    el.handle_runtime_msg(
        RuntimeMsg::CompactionComplete {
            request_id,
            input_ssts: vec![input_sst.to_string()],
            output_ssts: vec![output_sst.to_string()],
            cf_id: 0,
            target_level: 1,
            succeeded: true,
        },
        &msg_rx,
    );

    // Assert
    assert!(matches!(
        response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("compaction completion response"),
        RuntimeResponse::Ok { .. }
    ));
    let local_intent =
        std::fs::read(el.state.db_path.join("intent_log.json")).expect("read local intent log");
    let remote_intent = get_cloud_metadata_for_test(&metadata_storage, "intent_log.json");
    assert_eq!(
        remote_intent, local_intent,
        "cloud intent metadata must reflect the cleared compaction publication intent"
    );
    assert!(
        el.state.intent_log.is_empty(),
        "compaction publication intent should be cleared locally after completion"
    );

    Ok(())
}

#[test]
fn should_unblock_compaction_waiters_when_cleared_compaction_intent_mirror_fails(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.state.set_compaction_enabled(false);

    let input_sst = "mirror-fail-input.sst";
    add_valid_manifest_sst_for_test(&mut el, input_sst, 10);

    let output_sst = "mirror-fail-output.sst";
    let output_bytes = valid_sst_bytes_for_test(b"prune-candidate", b"value", 10);
    write_test_file(el.state.sst_dir.join(output_sst), &output_bytes);

    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let failing_backend = Arc::new(FailThirdIntentPutBackend::new(metadata_backend));
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        failing_backend,
        "metadata-test".to_string(),
    ));
    el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));

    el.compaction_actor
        .prepare_for_completion_test(&mut el.state, &[input_sst.to_string()])?;
    let completion_request_id = 4343;
    let completion_rx = el.router.register(completion_request_id, "TestRequest");
    let waiter_request_id = 4344;
    let waiter_rx = el.router.register(waiter_request_id, "TestRequest");
    el.state
        .pending_compaction_waits
        .lock()
        .insert(waiter_request_id, "CompactAll".to_string());
    let (_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    el.handle_runtime_msg(
        RuntimeMsg::CompactionComplete {
            request_id: completion_request_id,
            input_ssts: vec![input_sst.to_string()],
            output_ssts: vec![output_sst.to_string()],
            cf_id: 0,
            target_level: 1,
            succeeded: true,
        },
        &msg_rx,
    );

    // Assert
    match completion_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("compaction completion response")
    {
        RuntimeResponse::Error { error, .. } => {
            assert!(
                error
                    .to_string()
                    .contains("failed to mirror cleared compaction publication intent"),
                "unexpected compaction completion error: {error}"
            );
        }
        other => panic!("expected mirror failure response, got {other:?}"),
    }
    assert!(matches!(
        waiter_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("pending compaction waiter response"),
        RuntimeResponse::Ok { .. }
    ));
    assert_eq!(
        el.state
            .active_compactions
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "compaction completion should still drain active count after mirror failure"
    );
    assert!(
        el.state.pending_compaction_waits.lock().is_empty(),
        "mirror failure must not leave pending compaction waiters stuck"
    );

    Ok(())
}

#[test]
fn should_delete_obsolete_cloud_sst_objects_after_compaction() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.state.set_compaction_enabled(false);

    let input_sst = "cloud-gc-input.sst";
    let input_bytes = valid_sst_bytes_for_test(b"obsolete", b"value", 10);
    el.state.manifest.files.push(crate::metadata::FileMeta {
        name: input_sst.to_string(),
        level: 0,
        size_bytes: input_bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&input_bytes)),
        cf_id: 0,
        smallest_key: Some(b"obsolete".to_vec()),
        largest_key: Some(b"obsolete".to_vec()),
        smallest_seq: Some(10),
        largest_seq: Some(10),
        ..Default::default()
    });
    write_test_file(el.state.sst_dir.join(input_sst), &input_bytes);
    el.hybrid_storage
        .as_ref()
        .expect("hybrid storage")
        .write_sst_object(input_sst, input_bytes)?;
    assert!(
        remote_sst_path_for_test(&el, input_sst).exists(),
        "test setup should create the obsolete provider SST object"
    );

    let output_sst = "cloud-gc-output.sst";
    let output_bytes = valid_sst_bytes_for_test(b"obsolete", b"new-value", 11);
    write_test_file(el.state.sst_dir.join(output_sst), &output_bytes);

    el.compaction_actor
        .prepare_for_completion_test(&mut el.state, &[input_sst.to_string()])?;
    let request_id = 4545;
    let response_rx = el.router.register(request_id, "TestRequest");
    let (_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    el.handle_runtime_msg(
        RuntimeMsg::CompactionComplete {
            request_id,
            input_ssts: vec![input_sst.to_string()],
            output_ssts: vec![output_sst.to_string()],
            cf_id: 0,
            target_level: 1,
            succeeded: true,
        },
        &msg_rx,
    );

    // Assert
    assert!(matches!(
        response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("compaction completion response"),
        RuntimeResponse::Ok { .. }
    ));
    for _ in 0..100 {
        if !el.state.sst_dir.join(input_sst).exists()
            && !remote_sst_path_for_test(&el, input_sst).exists()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !el.state.sst_dir.join(input_sst).exists(),
        "obsolete input SST should be removed from the local runtime cache"
    );
    assert!(
        !remote_sst_path_for_test(&el, input_sst).exists(),
        "obsolete input SST should be removed from the cloud provider namespace"
    );
    assert!(
        remote_sst_path_for_test(&el, output_sst).exists(),
        "compaction output SST should remain in the cloud provider namespace"
    );

    Ok(())
}

#[test]
fn should_not_block_runtime_when_cloud_sst_delete_is_slow() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sst_name = "slow-cloud-gc.sst";
    let sst_bytes = valid_sst_bytes_for_test(b"slow-delete", b"value", 12);
    write_test_file(el.state.sst_dir.join(sst_name), &sst_bytes);

    let local_backend: Arc<dyn crate::storage::StorageBackend> = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local_slow"))
            .expect("create slow local backend"),
    );
    let cloud_backend_inner = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("create slow cloud backend"),
    );
    let (delete_started_tx, delete_started_rx) = std::sync::mpsc::channel();
    let release_delete = Arc::new(AtomicBool::new(false));
    let cloud_backend: Arc<dyn crate::storage::StorageBackend> =
        Arc::new(BlockingDeleteStorageBackend::new(
            cloud_backend_inner,
            crate::sst::object_key(sst_name),
            delete_started_tx,
            Arc::clone(&release_delete),
        ));
    let hybrid_storage = Arc::new(crate::storage::HybridStorage::with_policy(
        local_backend,
        cloud_backend,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    el.set_hybrid_storage(Arc::clone(&hybrid_storage));
    hybrid_storage.write_sst_object(sst_name, sst_bytes)?;

    let request_id = 4546;
    let response_rx = el.router.register(request_id, "TestRequest");
    let (_tx, msg_rx) = crossbeam::channel::unbounded();
    let release_delete_for_thread = Arc::clone(&release_delete);
    let release_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(250));
        release_delete_for_thread.store(true, Ordering::SeqCst);
    });

    // Act
    let started_at = Instant::now();
    el.handle_runtime_msg(
        RuntimeMsg::DeleteObsoleteSsts {
            request_id,
            sst_names: vec![sst_name.to_string()],
        },
        &msg_rx,
    );
    let elapsed = started_at.elapsed();

    // Assert
    assert!(
        elapsed < Duration::from_millis(150),
        "runtime GC handler blocked on provider delete for {elapsed:?}"
    );
    assert!(matches!(
        response_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("GC response"),
        RuntimeResponse::Ok { .. }
    ));
    delete_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("background cloud delete should start");
    release_thread
        .join()
        .expect("release blocked delete thread should finish");
    for _ in 0..100 {
        if !el.state.sst_dir.join(sst_name).exists()
            && !remote_sst_path_for_test(&el, sst_name).exists()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !el.state.sst_dir.join(sst_name).exists(),
        "runtime-local orphan should be deleted after provider cleanup completes"
    );
    assert!(
        !remote_sst_path_for_test(&el, sst_name).exists(),
        "provider object should be deleted after blocked cloud delete is released"
    );

    Ok(())
}

#[test]
fn should_retry_failed_cloud_sst_delete_without_runtime_restart() -> crate::common::MidgeResult<()>
{
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sst_name = "retry-cloud-gc.sst";
    let sst_bytes = valid_sst_bytes_for_test(b"retry-delete", b"value", 14);
    write_test_file(el.state.sst_dir.join(sst_name), &sst_bytes);

    let local_backend: Arc<dyn crate::storage::StorageBackend> = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local_retry"))
            .expect("create retry local backend"),
    );
    let cloud_backend_inner = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("create retry cloud backend"),
    );
    let failing_cloud = Arc::new(FailOnceDeleteStorageBackend::new(
        cloud_backend_inner,
        crate::sst::object_key(sst_name),
    ));
    let cloud_backend: Arc<dyn crate::storage::StorageBackend> =
        Arc::clone(&failing_cloud) as Arc<dyn crate::storage::StorageBackend>;
    let hybrid_storage = Arc::new(crate::storage::HybridStorage::with_policy(
        local_backend,
        cloud_backend,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    el.set_hybrid_storage(Arc::clone(&hybrid_storage));
    hybrid_storage.write_sst_object(sst_name, sst_bytes)?;

    let (retry_tx, retry_rx) = crossbeam::channel::unbounded();
    el.gc_actor.set_retry_notifier(Some(retry_tx));
    let (_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act: the first background deletion fails, then the event loop arms
    // and executes its bounded retry without being restarted.
    let hybrid_storage_for_delete = el.hybrid_storage.clone();
    el.gc_actor.delete_ssts(
        &mut el.state,
        &[sst_name.to_string()],
        hybrid_storage_for_delete,
    );
    assert!(matches!(
        retry_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("failed cloud delete should wake the event loop"),
        RuntimeMsg::RetryGc
    ));
    el.handle_runtime_msg(RuntimeMsg::RetryGc, &msg_rx);

    for _ in 0..100 {
        el.progress_pass(&msg_rx);
        if !el.state.sst_dir.join(sst_name).exists()
            && !remote_sst_path_for_test(&el, sst_name).exists()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    assert!(
        failing_cloud.delete_attempts() >= 2,
        "the failed cloud delete should be attempted again"
    );
    assert!(
        !el.state.sst_dir.join(sst_name).exists(),
        "the runtime-local orphan should be deleted after retry"
    );
    assert!(
        !remote_sst_path_for_test(&el, sst_name).exists(),
        "the cloud SST should be deleted after retry"
    );

    Ok(())
}

#[test]
fn should_join_cloud_gc_worker_before_runtime_shutdown() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sst_name = "shutdown-cloud-gc.sst";
    let sst_bytes = valid_sst_bytes_for_test(b"shutdown-delete", b"value", 13);
    write_test_file(el.state.sst_dir.join(sst_name), &sst_bytes);

    let local_backend: Arc<dyn crate::storage::StorageBackend> = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local_shutdown"))
            .expect("create local backend"),
    );
    let cloud_backend_inner = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("create cloud backend"),
    );
    let (delete_started_tx, delete_started_rx) = std::sync::mpsc::channel();
    let release_delete = Arc::new(AtomicBool::new(false));
    let cloud_backend: Arc<dyn crate::storage::StorageBackend> =
        Arc::new(BlockingDeleteStorageBackend::new(
            cloud_backend_inner,
            crate::sst::object_key(sst_name),
            delete_started_tx,
            Arc::clone(&release_delete),
        ));
    let hybrid_storage = Arc::new(crate::storage::HybridStorage::with_policy(
        local_backend,
        cloud_backend,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    el.set_hybrid_storage(Arc::clone(&hybrid_storage));
    hybrid_storage.write_sst_object(sst_name, sst_bytes)?;

    let request_id = 4547;
    let (_response_tx, msg_rx) = crossbeam::channel::unbounded();
    el.handle_runtime_msg(
        RuntimeMsg::DeleteObsoleteSsts {
            request_id,
            sst_names: vec![sst_name.to_string()],
        },
        &msg_rx,
    );
    delete_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("background cloud delete should start");

    let release_delete_for_thread = Arc::clone(&release_delete);
    let release_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(250));
        release_delete_for_thread.store(true, Ordering::SeqCst);
    });

    // Act
    let shutdown_started = Instant::now();
    el.handle_runtime_msg(RuntimeMsg::Shutdown, &msg_rx);
    let shutdown_elapsed = shutdown_started.elapsed();
    release_thread
        .join()
        .expect("release blocked delete thread should finish");

    // Assert: shutdown must wait for the tracked storage mutation.
    assert!(
        shutdown_elapsed >= Duration::from_millis(150),
        "shutdown returned before the cloud GC worker completed: {shutdown_elapsed:?}"
    );
    assert!(!el.state.sst_dir.join(sst_name).exists());
    assert!(!remote_sst_path_for_test(&el, sst_name).exists());
    Ok(())
}

#[test]
fn should_not_prune_remote_wal_when_segment_is_not_cloud_durable() -> crate::common::MidgeResult<()>
{
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence - 1;

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must be retained until the cloud durable frontier covers its max sequence"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

    Ok(())
}

#[test]
fn should_not_prune_remote_wal_when_manifest_sequence_advances_without_sst_coverage(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    // Act
    // Assert
    assert!(
        el.state.manifest.files.is_empty(),
        "test requires no manifest SSTs to prove sequence-only metadata is insufficient"
    );

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must be retained when manifest sequence is advanced but no SST covers it"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

    Ok(())
}

#[test]
fn should_not_prune_remote_wal_when_high_sequence_sst_does_not_cover_wal_record_cf(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate_with_records(
        &mut el,
        segment_id,
        max_sequence,
        vec![
            crate::wal::WalRecord::new_cf(
                0,
                crate::wal::WalOpKind::Put,
                Bytes::from_static(b"default-only-in-wal"),
                Some(Bytes::from_static(b"default-value")),
                5,
                0,
            ),
            crate::wal::WalRecord::new_cf(
                1,
                crate::wal::WalOpKind::Put,
                Bytes::from_static(b"other-covered"),
                Some(Bytes::from_static(b"other-value")),
                max_sequence,
                0,
            ),
        ],
    );
    el.state.wal.cloud_durable_seq = max_sequence;
    add_manifest_sst_meta_for_test(
        &mut el,
        "other-high-seq.sst",
        1,
        b"other-covered",
        max_sequence,
        max_sequence,
    );

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must be retained when manifest SST coverage is only for a different CF"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

    Ok(())
}

#[test]
fn should_not_prune_remote_wal_when_manifest_sst_metadata_does_not_match_actual_sst(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    let sst_name = "lying-summary.sst";
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;

    let bytes = valid_sst_bytes_for_test(b"other-key", b"value", max_sequence);
    el.state.manifest.files.push(crate::metadata::FileMeta {
        name: sst_name.to_string(),
        level: 0,
        size_bytes: bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&bytes)),
        cf_id: 0,
        smallest_key: Some(b"prune-candidate".to_vec()),
        largest_key: Some(b"prune-candidate".to_vec()),
        smallest_seq: Some(max_sequence),
        largest_seq: Some(max_sequence),
        ..Default::default()
    });
    write_test_file(remote_sst_path_for_test(&el, sst_name), &bytes);

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must be retained when manifest SST metadata does not match actual SST contents"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

    Ok(())
}

#[test]
fn should_not_prune_remote_wal_when_segment_max_sequence_exceeds_manifest_coverage(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    el.state.manifest.last_persisted_sequence = max_sequence - 1;

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL must be retained when its max sequence exceeds manifest coverage"
    );

    Ok(())
}

#[test]
fn should_prune_remote_wal_when_segment_max_sequence_equals_manifest_coverage(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    add_valid_manifest_sst_for_test(&mut el, "coverage.sst", max_sequence);

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
            !remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL may be pruned when cloud durability and manifest coverage both include its max sequence"
        );

    Ok(())
}

#[test]
fn should_prune_remote_wal_when_delete_range_record_is_manifest_covered(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    let mut delete_range = crate::wal::WalRecord::new_cf(
        0,
        crate::wal::WalOpKind::DeleteRange,
        Bytes::from_static(b"k10"),
        None,
        max_sequence,
        0,
    );
    delete_range.range_end = Some(Bytes::from_static(b"k20"));
    seed_cloud_prune_candidate_with_records(&mut el, segment_id, max_sequence, vec![delete_range]);
    el.state.wal.cloud_durable_seq = max_sequence;
    add_valid_range_tombstone_manifest_sst_for_test(
        &mut el,
        "delete-range-covered.sst",
        b"k10",
        b"k20",
        max_sequence,
    );

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
            !remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL may be pruned when a delete-range record is physically covered by a manifest SST"
        );

    Ok(())
}

#[test]
fn should_not_prune_remote_wal_when_delete_range_record_exceeds_manifest_range(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    let mut delete_range = crate::wal::WalRecord::new_cf(
        0,
        crate::wal::WalOpKind::DeleteRange,
        Bytes::from_static(b"k10"),
        None,
        max_sequence,
        0,
    );
    delete_range.range_end = Some(Bytes::from_static(b"k20"));
    seed_cloud_prune_candidate_with_records(&mut el, segment_id, max_sequence, vec![delete_range]);
    el.state.wal.cloud_durable_seq = max_sequence;
    add_valid_range_tombstone_manifest_sst_for_test(
        &mut el,
        "delete-range-partial.sst",
        b"k10",
        b"k19",
        max_sequence,
    );

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
            remote_wal_path_for_test(&el, segment_id).exists(),
            "remote WAL must be retained when manifest range tombstone coverage is narrower than the WAL record"
        );

    Ok(())
}

#[test]
fn should_prune_remote_wal_when_only_transaction_marker_exceeds_data_coverage(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate_with_records(
        &mut el,
        segment_id,
        max_sequence,
        vec![
            crate::wal::WalRecord::new_cf(
                0,
                crate::wal::WalOpKind::TxnBegin,
                Bytes::from_static(b"txn"),
                None,
                8,
                0,
            ),
            crate::wal::WalRecord::new_cf(
                0,
                crate::wal::WalOpKind::Put,
                Bytes::from_static(b"txn-data"),
                Some(Bytes::from_static(b"value")),
                9,
                0,
            ),
            crate::wal::WalRecord::new_cf(
                0,
                crate::wal::WalOpKind::TxnCommit,
                Bytes::from_static(b"txn"),
                None,
                max_sequence,
                0,
            ),
        ],
    );
    el.state.wal.cloud_durable_seq = max_sequence;
    add_manifest_sst_meta_for_test(&mut el, "txn-data.sst", 0, b"txn-data", 9, 9);

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        !remote_wal_path_for_test(&el, segment_id).exists(),
        "transaction marker records must not force retention when all data records are covered"
    );

    Ok(())
}

#[test]
fn should_retry_prune_after_preflight_failure_clears_inflight() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    let sst_name = "guard-retry.sst";
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    let sst_bytes = add_valid_manifest_sst_for_test(&mut el, sst_name, max_sequence);
    std::fs::remove_file(remote_sst_path_for_test(&el, sst_name))
        .expect("delete remote SST after initial validation");
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        el.cloud_wal.prune_inflight.is_empty(),
        "preflight failure must not leave prune inflight state"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&segment_id),
        "failed guarded prune must keep the WAL eligible for retry"
    );
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "failed guarded prune must retain the remote WAL"
    );

    write_test_file(remote_sst_path_for_test(&el, sst_name), &sst_bytes);
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    assert!(
        !remote_wal_path_for_test(&el, segment_id).exists(),
        "restored manifest SST should allow a later guarded prune"
    );

    Ok(())
}

#[test]
fn should_not_starve_later_wal_prune_when_earlier_segment_is_unverifiable(
) -> crate::common::MidgeResult<()> {
    // Arrange: the lower segment belongs to a CF with no manifest coverage;
    // the following segment is independently covered and safe to retire.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let blocked_segment = 71;
    let covered_segment = 72;
    seed_cloud_prune_candidate_with_records(
        &mut el,
        blocked_segment,
        blocked_segment,
        vec![crate::wal::WalRecord::new_cf(
            1,
            crate::wal::WalOpKind::Put,
            Bytes::from_static(b"uncovered-earlier-segment"),
            Some(Bytes::from_static(b"value")),
            blocked_segment,
            0,
        )],
    );
    seed_cloud_prune_candidate(&mut el, covered_segment, covered_segment);
    el.state.wal.current_segment_id = covered_segment + 1;
    el.state.wal.cloud_durable_seq = covered_segment;
    add_valid_manifest_sst_for_test(&mut el, "later-covered.sst", covered_segment);

    // Act: the first pass fails closed on segment 71. Round-robin selection
    // must allow the next pass to make progress on segment 72.
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);
    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Assert
    assert!(remote_wal_path_for_test(&el, blocked_segment).exists());
    assert!(el.cloud_wal.acked_segments.contains_key(&blocked_segment));
    assert!(!remote_wal_path_for_test(&el, covered_segment).exists());
    assert!(!el.cloud_wal.acked_segments.contains_key(&covered_segment));
    Ok(())
}

#[test]
fn should_ignore_listing_only_ssts_when_deciding_remote_wal_cleanup(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let max_sequence = 10;
    seed_cloud_prune_candidate(&mut el, segment_id, max_sequence);
    el.state.wal.cloud_durable_seq = max_sequence;
    el.state.manifest.last_persisted_sequence = 0;
    write_test_file(
        remote_sst_path_for_test(&el, "uploaded-but-uncommitted.sst"),
        b"listing-only object",
    );

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        remote_wal_path_for_test(&el, segment_id).exists(),
        "uploaded but uncommitted SST objects must not establish WAL cleanup coverage"
    );

    Ok(())
}

#[test]
fn should_keep_local_wal_when_remote_wal_readback_fails_after_cloud_ack(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let segment_id = 1;
    let local_wal = el
        .state
        .wal_dir
        .join(crate::wal::segment_file_name(segment_id));
    write_test_file(local_wal.clone(), b"local wal still needed");

    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence: 1,
    });

    // Act
    // Assert
    assert!(
        local_wal.exists(),
        "local WAL must be retained when the remote WAL cannot be read back after CloudAck"
    );

    Ok(())
}

#[test]
fn should_validate_uncached_cloud_ack_before_local_wal_removal() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let request_id = 501u64;
    let (seq, deferred) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id,
            cf_id: 0,
            key: Bytes::from_static(b"unproven-ack"),
            value: Some(Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    // Act
    // Assert
    assert!(deferred, "CloudAsync append should wait for CloudAck");
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id,
            sequence: seq,
        });

    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    let local_wal = el
        .state
        .wal_dir
        .join(crate::wal::segment_file_name(segment_id));
    assert!(
        local_wal.exists(),
        "sealed local WAL should exist before ack"
    );

    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence,
    });

    assert!(
        !local_wal.exists(),
        "valid remote WAL should be proven directly before local removal"
    );

    Ok(())
}

#[test]
fn should_reject_cloud_ack_given_remote_wal_from_different_writer_epoch(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let (sequence, _) = event_loop.wal_actor.append(
        &mut event_loop.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 502,
            cf_id: 0,
            key: Bytes::from_static(b"current-writer"),
            value: Some(Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    let (segment_id, max_sequence) = seal_segment_for_test(&mut event_loop)?;
    assert_eq!(sequence, max_sequence);
    let local_wal = event_loop
        .state
        .wal_dir
        .join(crate::wal::segment_file_name(segment_id));
    let stale_record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"stale-writer"),
        Some(Bytes::from_static(b"value")),
        max_sequence,
        99,
    );
    let payload = crate::wal::encoding::encode(&stale_record).expect("encode stale WAL record");
    let mut stale_bytes = Vec::new();
    crate::wal::frame::append_frame(&mut stale_bytes, &payload).expect("frame stale WAL record");
    write_test_file(
        remote_wal_path_for_test(&event_loop, segment_id),
        &stale_bytes,
    );

    // Act
    event_loop.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence,
    });

    // Assert
    assert_eq!(event_loop.state.wal.cloud_durable_seq, 0);
    assert!(local_wal.exists(), "unmatched local WAL must be retained");
    assert!(event_loop.state.persistence_anomaly_detected());
    Ok(())
}

#[test]
fn should_reject_cloud_ack_given_writer_fenced_after_upload_was_enqueued(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let healthy = Arc::new(AtomicBool::new(true));
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    event_loop.lease_healthy = Some(Arc::clone(&healthy));
    let (sequence, _) = event_loop.wal_actor.append(
        &mut event_loop.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 503,
            cf_id: 0,
            key: Bytes::from_static(b"fenced-before-ack"),
            value: Some(Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    let (segment_id, max_sequence) = seal_segment_for_test(&mut event_loop)?;
    assert_eq!(sequence, max_sequence);
    let local_path = event_loop
        .state
        .wal_dir
        .join(crate::wal::segment_file_name(segment_id));
    healthy.store(false, Ordering::Release);

    // Act
    event_loop.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence,
    });

    // Assert
    assert_eq!(event_loop.state.wal.cloud_durable_seq, 0);
    assert!(local_path.exists(), "fenced ACK must retain the local WAL");
    assert!(event_loop.state.persistence_anomaly_detected());
    Ok(())
}

#[test]
fn should_not_advance_cloud_durability_across_unacked_segment_gap() -> crate::common::MidgeResult<()>
{
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;

    let first_request = 601u64;
    let (first_seq, first_deferred) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: first_request,
            cf_id: 0,
            key: Bytes::from_static(b"gap-first"),
            value: Some(Bytes::from_static(b"value-1")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    // Act
    // Assert
    assert!(first_deferred, "CloudAsync first append should defer");
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id: first_request,
            sequence: first_seq,
        });
    let (first_segment, first_max_sequence) = seal_segment_for_test(&mut el)?;

    let second_request = 602u64;
    let (second_seq, second_deferred) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: second_request,
            cf_id: 0,
            key: Bytes::from_static(b"gap-second"),
            value: Some(Bytes::from_static(b"value-2")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    assert!(second_deferred, "CloudAsync second append should defer");
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id: second_request,
            sequence: second_seq,
        });
    let (second_segment, second_max_sequence) = seal_segment_for_test(&mut el)?;
    assert!(second_segment > first_segment);
    let second_local_wal = el
        .state
        .wal_dir
        .join(crate::wal::segment_file_name(second_segment));

    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: second_segment,
        max_sequence: second_max_sequence,
    });

    assert_eq!(
        el.state.wal.cloud_durable_seq, 0,
        "cloud durable frontier must not jump across an unacked segment"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&second_segment),
        "later acknowledgement must remain buffered until the earlier gap closes"
    );
    assert!(
        second_local_wal.exists(),
        "local WAL for an out-of-order ack must remain until earlier segments are durable"
    );

    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: first_segment,
        max_sequence: first_max_sequence,
    });

    assert_eq!(
        el.state.wal.cloud_durable_seq, second_max_sequence,
        "frontier should advance through the contiguous acked segment range once the gap closes"
    );
    assert!(
        el.durability
            .cloud_segment_max_sequence(second_segment)
            .is_none(),
        "contiguous inflight acknowledgement bookkeeping must drain after the gap closes"
    );
    assert!(
        !second_local_wal.exists(),
        "local WAL can be removed after the contiguous cloud durable frontier covers it"
    );

    Ok(())
}

#[test]
fn should_drop_buffered_cloud_acks_when_earlier_segment_fails() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;

    let first_request = 611u64;
    let (first_seq, first_deferred) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: first_request,
            cf_id: 0,
            key: Bytes::from_static(b"fail-gap-first"),
            value: Some(Bytes::from_static(b"value-1")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    // Act
    // Assert
    assert!(first_deferred, "CloudAsync first append should defer");
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id: first_request,
            sequence: first_seq,
        });
    let (first_segment, _) = seal_segment_for_test(&mut el)?;

    let second_request = 612u64;
    let (second_seq, second_deferred) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: second_request,
            cf_id: 0,
            key: Bytes::from_static(b"fail-gap-second"),
            value: Some(Bytes::from_static(b"value-2")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    assert!(second_deferred, "CloudAsync second append should defer");
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id: second_request,
            sequence: second_seq,
        });
    let (second_segment, second_max_sequence) = seal_segment_for_test(&mut el)?;

    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: second_segment,
        max_sequence: second_max_sequence,
    });
    assert!(
        el.cloud_wal.acked_segments.contains_key(&second_segment),
        "later ack should be buffered while an earlier segment is unacked"
    );

    el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id: first_segment,
        error: "injected upload failure".to_string(),
        terminal: true,
        failure_kind: crate::storage::CloudUploadFailureKind::Other,
    });

    assert_eq!(
        el.state.wal.cloud_durable_seq, 0,
        "failure of the earlier segment must not let a buffered later ack advance durability"
    );
    assert!(
        el.cloud_wal.acked_segments.contains_key(&second_segment),
        "later verified ACK bookkeeping must remain buffered behind an earlier gap"
    );
    assert!(el.state.persistence_anomaly_detected());

    Ok(())
}

#[test]
fn should_keep_local_wal_when_cached_remote_wal_proof_becomes_stale_before_cloud_ack(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let request_id = 502u64;
    let (seq, deferred) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id,
            cf_id: 0,
            key: Bytes::from_static(b"stale-proof-ack"),
            value: Some(Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    // Act
    // Assert
    assert!(deferred, "CloudAsync append should wait for CloudAck");
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id,
            sequence: seq,
        });

    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    let local_wal = el
        .state
        .wal_dir
        .join(crate::wal::segment_file_name(segment_id));
    el.hybrid_storage
        .as_ref()
        .expect("hybrid storage")
        .publish_remote_wal_segment(
            segment_id,
            max_sequence,
            &local_wal,
            el.state.writer_epoch,
            &crate::common::OperationDeadline::unbounded(),
        )
        .expect("establish authoritative remote WAL publication");
    std::fs::remove_file(remote_wal_path_for_test(&el, segment_id))
        .expect("delete remote WAL after proof");

    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence,
    });

    assert!(
        local_wal.exists(),
        "local WAL must be retained when cached remote proof becomes stale before CloudAck"
    );

    Ok(())
}

#[test]
fn should_revalidate_verified_cloud_metadata_on_repeated_wal_cleanup_check(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(crate::common::MidgeError::Internal)?;

    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend.clone(),
        "metadata-test".to_string(),
    ));
    put_all_cloud_metadata_for_test(&metadata_storage, &el.state.db_path);
    el.cloud_metadata_storage = Some(metadata_storage);

    metadata_backend.clear_history();
    el.verify_cloud_metadata_for_wal_cleanup()
        .expect("first cloud metadata validation");
    let first_downloads = metadata_backend.get_downloads();
    // Act
    // Assert
    assert!(
        !first_downloads.is_empty(),
        "first validation should read cloud metadata"
    );

    el.verify_cloud_metadata_for_wal_cleanup()
        .expect("second cloud metadata validation");

    let second_downloads = metadata_backend.get_downloads();
    assert_eq!(
        second_downloads.len(),
        first_downloads.len() * 2,
        "unchanged metadata proof should revalidate object bytes and identity"
    );
    assert_eq!(&second_downloads[..first_downloads.len()], &first_downloads);
    assert_eq!(&second_downloads[first_downloads.len()..], &first_downloads);

    Ok(())
}

#[test]
fn should_reject_cached_cloud_metadata_proof_when_remote_metadata_is_deleted(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    crate::metadata::ManifestPersistence::save(&el.state.db_path, &el.state.manifest)
        .map_err(crate::common::MidgeError::Internal)?;

    let metadata_backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        metadata_backend,
        "metadata-test".to_string(),
    ));
    put_all_cloud_metadata_for_test(&metadata_storage, &el.state.db_path);
    el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));

    el.verify_cloud_metadata_for_wal_cleanup()
        .expect("initial cloud metadata validation");
    delete_cloud_metadata_for_test(&metadata_storage, "manifest.json");

    let error = el
        .verify_cloud_metadata_for_wal_cleanup()
        .expect_err("deleted metadata must invalidate cached cleanup proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation")
            || error.contains("unreadable")
            || error.contains("disappeared")
            || error.contains("is missing"),
        "unexpected stale metadata proof error: {error}"
    );

    Ok(())
}

#[test]
fn should_retry_auto_flush_when_backpressure_releases() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.state.set_write_stalled(true);
    el.state.memtable_flush_threshold = 1024;
    el.state.memtable_size_limit = 1024 * 1024;
    el.state.sequence = 1;
    {
        let cf = el.state.get_cf(0).expect("default cf");
        cf.memtable
            .put_with_seq(b"retry-key".to_vec(), vec![0xA5; 2048], 1, None)
            .expect("seed memtable");
    }
    el.state.total_memtable_bytes = el
        .state
        .get_cf(0)
        .expect("default cf")
        .memtable
        .size_bytes();

    el.handle_storage_event(crate::storage::StorageEvent::BackpressureOff);

    // Act
    // Assert
    assert!(!el.state.write_stalled());
    assert!(
        el.state.manifest.files.iter().any(|file| file.cf_id == 0),
        "backpressure release should retry the pending auto-flush"
    );

    Ok(())
}

#[test]
fn should_cloud_async_ack_confirm_idempotent_request() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;

    // Act

    // Add a wal append with a specific request_id
    let request_id = 123u64;
    let cf_id = 0u32;

    let (seq, deferred) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id,
            cf_id,
            key: bytes::Bytes::from("k1"),
            value: Some(bytes::Bytes::from("v1")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;

    assert!(
        deferred,
        "CloudAsync append should be deferred waiting for CloudAck"
    );

    // Queue waiter for this append (simulates EventLoop behavior)
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id,
            sequence: seq,
        });

    // Simulate sealing & uploading segment for CloudAsync as EventLoop would do
    let (seg_id, max_sequence) = seal_segment_for_test(&mut el)?;

    // Now simulate the storage CloudAck for that segment
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: seg_id,
        max_sequence,
    });

    // Assert: After handling, the idempotency entry for request_id should be confirmed at cloud frontier
    assert!(
        el.state
            .sequence_idempotency_cache
            .contains_key(&request_id),
        "idempotency entry missing"
    );
    if let Some(entry) = el.state.sequence_idempotency_cache.get(&request_id) {
        assert!(entry.2 >= el.state.wal.cloud_durable_seq);
    }

    Ok(())
}

#[test]
fn should_cloud_async_retry_after_ack_return_same_sequence_without_queueing(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;

    // Act

    // Add a wal append with a specific request_id
    let request_id = 124u64;
    let cf_id = 0u32;

    let (seq1, deferred1) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id,
            cf_id,
            key: bytes::Bytes::from("k1"),
            value: Some(bytes::Bytes::from("v1")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;

    assert!(
        deferred1,
        "CloudAsync append should be deferred waiting for CloudAck"
    );

    // Queue waiter for this append (simulates EventLoop behavior)
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id,
            sequence: seq1,
        });

    // Simulate sealing & uploading segment for CloudAsync as EventLoop would do
    let (seg_id, max_sequence) = seal_segment_for_test(&mut el)?;

    // Now simulate the storage CloudAck for that segment
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: seg_id,
        max_sequence,
    });

    // After handling, the idempotency entry for request_id should be confirmed at cloud frontier
    assert!(
        el.state
            .sequence_idempotency_cache
            .contains_key(&request_id),
        "idempotency entry missing"
    );
    if let Some(entry) = el.state.sequence_idempotency_cache.get(&request_id) {
        assert!(entry.2 >= el.state.wal.cloud_durable_seq);
    }

    // Assert: Retry the same request_id: should return the same sequence and NOT be deferred
    // Retry the same request_id: should return the same sequence and NOT be deferred
    let (seq2, deferred2) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id,
            cf_id,
            key: bytes::Bytes::from("k1"),
            value: Some(bytes::Bytes::from("v1")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;

    assert_eq!(seq1, seq2, "retry should return same sequence");
    assert!(
        !deferred2,
        "retry after confirmation should not be deferred"
    );
    assert_eq!(el.state.wal.pending_writes, 0);

    Ok(())
}

#[test]
fn should_preserve_idempotency_allocation_when_failed_cloud_wal_remains_retryable(
) -> crate::common::MidgeResult<()> {
    // Arrange: create state and event loop with CloudAsync policy
    let tmp = tempfile::tempdir().expect("create tmpdir");
    let state = RuntimeState::new(tmp.path().to_path_buf(), false);
    let router = Arc::new(ResponseRouter::new());
    let config = crate::runtime::RuntimeConfig {
        wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
        ..Default::default()
    };
    let mut el = EventLoop::new(state, false, router, config, None)?;

    // Act

    // Add a wal append with a specific request_id
    let request_id = 200u64;
    let cf_id = 0u32;

    let (seq1, deferred1) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id,
            cf_id,
            key: bytes::Bytes::from("k2"),
            value: Some(bytes::Bytes::from("v2")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;

    assert!(
        deferred1,
        "CloudAsync append should be deferred waiting for CloudAck"
    );

    // Queue waiter for this append (simulates EventLoop behavior)
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id,
            sequence: seq1,
        });

    // Simulate sealing & uploading segment for CloudAsync as EventLoop would do
    let (seg_id, _max_sequence) = seal_segment_for_test(&mut el)?;

    // Now simulate the storage CloudFail for that segment
    el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id: seg_id,
        error: "upload_failed".to_string(),
        terminal: true,
        failure_kind: crate::storage::CloudUploadFailureKind::Other,
    });

    // Assert: the original allocation remains the identity of the accepted
    // mutation while its local WAL is still owned for callerless publication.
    assert_eq!(
        el.state.get_cached_sequences(request_id),
        Some((seq1, 1)),
        "requeued WAL publication must retain its original sequence allocation"
    );

    Ok(())
}

#[test]
fn should_not_advance_cloud_frontier_across_failed_segment_gap() -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let (first_seq, first_deferred) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 701,
            cf_id: 0,
            key: Bytes::from_static(b"frontier-gap-first"),
            value: Some(Bytes::from_static(b"value-1")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    assert!(first_deferred);
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id: 701,
            sequence: first_seq,
        });
    let (first_segment, first_max_sequence) = seal_segment_for_test(&mut el)?;

    let (second_seq, second_deferred) = el.wal_actor.append(
        &mut el.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 702,
            cf_id: 0,
            key: Bytes::from_static(b"frontier-gap-second"),
            value: Some(Bytes::from_static(b"value-2")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    assert!(second_deferred);
    el.durability
        .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
            request_id: 702,
            sequence: second_seq,
        });
    let (second_segment, second_max_sequence) = seal_segment_for_test(&mut el)?;

    // Act
    el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id: first_segment,
        error: "injected upload failure".to_string(),
        terminal: true,
        failure_kind: crate::storage::CloudUploadFailureKind::Other,
    });
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: second_segment,
        max_sequence: second_max_sequence,
    });

    // Assert
    assert_eq!(
        el.state.wal.cloud_durable_seq, 0,
        "a later cloud ACK must not skip an earlier failed segment"
    );

    // A retry ACK for the failed segment closes the gap and may advance
    // the contiguous frontier through the already-verified later segment.
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: first_segment,
        max_sequence: first_max_sequence,
    });
    assert_eq!(
        el.state.wal.cloud_durable_seq, second_max_sequence,
        "the frontier should advance only after the failed segment is durable"
    );

    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retry_background_cloud_seal_after_failpoint_before_rotate(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;

    let ops = vec![crate::runtime::TransactionOp::Put {
        cf_id: 0,
        key: bytes::Bytes::from_static(b"buffered-seal-key"),
        value: bytes::Bytes::from_static(b"buffered-seal-value"),
        ttl_seconds: None,
        insert_only: false,
    }];
    let (last_sequence, _op_count, deferred) = el.wal_actor.append_transaction(
        &mut el.state,
        crate::runtime::actors::wal::TransactionAppendParams {
            request_id: 301,
            ops,
            assertions: Vec::new(),
            durability_policy: Some(crate::wal::DurabilityPolicy::CloudAsync),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    // Act
    // Assert
    assert!(
        deferred,
        "buffered transaction should defer cloud durability"
    );

    let failed_segment = el.state.wal.current_segment_id;
    fail::cfg(
        "midge::cloud::inject_fail_after_wal_flush_before_rotate",
        "return",
    )
    .expect("configure cloud seal failpoint");

    let first_error = el
        .seal_current_cloud_segment()
        .expect_err("first seal should fail before rotate");
    match first_error {
        crate::common::MidgeError::Internal(message) => {
            assert!(
                message.contains("cloud seal failed after WAL flush before rotate"),
                "unexpected failpoint error: {message}"
            );
        }
        other => panic!("unexpected seal failure: {other:?}"),
    }
    assert_eq!(
        el.state.wal.current_segment_id, failed_segment,
        "failed seal must not advance the current WAL segment"
    );
    assert!(
        el.state.wal.pending_writes > 0,
        "failed seal must preserve buffered WAL accounting for retry"
    );
    assert!(
        el.wal_actor.bytes_since_sync() > 0,
        "failed seal must preserve buffered byte accounting for retry"
    );
    assert!(!el.has_actionable_work(), "seal retry must back off");
    std::thread::sleep(Duration::from_millis(15));
    assert!(el.has_actionable_work(), "seal retry must become due");

    fail::remove("midge::cloud::inject_fail_after_wal_flush_before_rotate");

    let sealed = el
        .seal_current_cloud_segment()?
        .expect("retry should seal and enqueue the same WAL segment");
    assert_eq!(
        sealed.0, failed_segment,
        "retry should seal the same WAL segment after the failpoint clears"
    );
    assert_eq!(
        sealed.1, last_sequence,
        "retry should preserve the original max sequence for the segment"
    );
    assert_eq!(
        el.state.wal.current_segment_id,
        failed_segment + 1,
        "successful retry should advance to the next WAL segment"
    );
    assert_eq!(
        el.state.wal.pending_writes, 0,
        "successful retry should clear buffered WAL accounting"
    );
    assert_eq!(
        el.wal_actor.bytes_since_sync(),
        0,
        "successful retry should clear buffered byte accounting"
    );
    assert!(
        el.hybrid_storage
            .as_ref()
            .expect("hybrid storage")
            .pending_upload_count()
            > 0,
        "successful retry should enqueue the sealed segment for upload"
    );

    scenario.teardown();
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retain_upload_obligation_given_failure_after_cloud_wal_rotation(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let (sequence, deferred) = event_loop.wal_actor.append(
        &mut event_loop.state,
        crate::runtime::actors::wal::AppendParams {
            request_id: 302,
            cf_id: 0,
            key: Bytes::from_static(b"post-rotate-failure"),
            value: Some(Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;
    let segment_id = event_loop.state.wal.current_segment_id;
    fail::cfg(
        "midge::cloud::inject_fail_after_wal_rotate_before_enqueue",
        "return",
    )
    .expect("configure post-rotate failpoint");

    // Act
    let error = event_loop
        .seal_current_cloud_segment()
        .expect_err("post-rotate failpoint should interrupt enqueue");

    // Assert
    assert!(deferred);
    assert!(matches!(
        error,
        crate::common::MidgeError::Internal(message)
            if message.contains("after WAL rotate before enqueue")
    ));
    assert_eq!(
        event_loop.cloud_wal.upload_backlog.get(&segment_id),
        Some(&sequence)
    );
    assert_eq!(
        event_loop.durability.cloud_segment_max_sequence(segment_id),
        Some(sequence),
        "the rotated segment must occupy the frontier gap before enqueue"
    );
    assert_eq!(event_loop.state.wal.pending_writes, 0);

    fail::remove("midge::cloud::inject_fail_after_wal_rotate_before_enqueue");
    event_loop.drain_cloud_wal_upload_backlog();
    assert!(!event_loop
        .cloud_wal
        .upload_backlog
        .contains_key(&segment_id));
    assert_eq!(
        event_loop
            .hybrid_storage
            .as_ref()
            .map_or(0, |storage| storage.pending_upload_count()),
        1
    );
    drop(scenario);
    Ok(())
}

#[test]
fn should_seal_cloud_wal_with_segment_max_sequence_not_global_sequence(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;

    let ops = vec![crate::runtime::TransactionOp::Put {
        cf_id: 0,
        key: bytes::Bytes::from_static(b"segment-max-key"),
        value: bytes::Bytes::from_static(b"segment-max-value"),
        ttl_seconds: None,
        insert_only: false,
    }];
    let (last_wal_sequence, _op_count, deferred) = el.wal_actor.append_transaction(
        &mut el.state,
        crate::runtime::actors::wal::TransactionAppendParams {
            request_id: 303,
            ops,
            assertions: Vec::new(),
            durability_policy: Some(crate::wal::DurabilityPolicy::CloudAsync),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    // Act
    // Assert
    assert!(
        deferred,
        "CloudAsync transaction should defer cloud durability"
    );

    let global_sequence_without_wal_records = last_wal_sequence + 551;
    el.state.sequence = global_sequence_without_wal_records;

    let (segment_id, sealed_max_sequence) = el
        .seal_current_cloud_segment()?
        .expect("pending CloudAsync WAL records should seal a segment");

    assert_eq!(
            sealed_max_sequence, last_wal_sequence,
            "sealed WAL max sequence must come from records appended to the segment, not global runtime sequence"
        );
    assert_eq!(
        el.state.wal.local_durable_seq, last_wal_sequence,
        "local durable frontier for the sealed segment should not advance past WAL contents"
    );

    let storage = el.hybrid_storage.as_ref().expect("hybrid storage");
    for _ in 0..100 {
        if storage
                .process_uploads()
                .iter()
                .any(|event| matches!(event, crate::storage::StorageEvent::CloudAck { segment_id: acked, max_sequence } if *acked == segment_id && *max_sequence == last_wal_sequence))
            {
                break;
            }
        std::thread::sleep(Duration::from_millis(10));
    }

    storage
        .publish_remote_wal_segment(
            segment_id,
            last_wal_sequence,
            &el.state
                .wal_dir
                .join(crate::wal::segment_file_name(segment_id)),
            el.state.writer_epoch,
            &crate::common::OperationDeadline::unbounded(),
        )
        .expect("publish the actual segment frontier");
    storage
        .verify_remote_wal_segment(segment_id, last_wal_sequence)
        .expect("remote WAL readback should prove the actual segment max sequence");
    let overproof = storage
        .verify_remote_wal_segment(segment_id, global_sequence_without_wal_records)
        .expect_err("remote WAL readback must reject a frontier above the segment contents");
    assert!(
        overproof.contains("does not match expected") || overproof.contains("below expected"),
        "unexpected overproof validation error: {overproof}"
    );

    Ok(())
}

#[test]
fn should_not_enqueue_cloud_wal_segment_given_lease_unhealthy_when_sealing(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let mut event_loop = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let ops = vec![crate::runtime::TransactionOp::Put {
        cf_id: 0,
        key: Bytes::from_static(b"fenced-seal-key"),
        value: Bytes::from_static(b"fenced-seal-value"),
        ttl_seconds: None,
        insert_only: false,
    }];
    event_loop.wal_actor.append_transaction(
        &mut event_loop.state,
        crate::runtime::actors::wal::TransactionAppendParams {
            request_id: 304,
            ops,
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::CloudAsync),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    event_loop.lease_healthy = Some(Arc::new(AtomicBool::new(false)));
    let segment_id = event_loop.state.wal.current_segment_id;

    // Act
    let result = event_loop.seal_current_cloud_segment();

    // Assert
    assert!(matches!(result, Err(crate::common::MidgeError::Fenced(_))));
    assert_eq!(event_loop.state.wal.current_segment_id, segment_id);
    assert_eq!(
        event_loop
            .hybrid_storage
            .as_ref()
            .expect("hybrid storage")
            .pending_upload_count(),
        0,
        "a fenced writer must not enqueue a cloud WAL mutation"
    );
    Ok(())
}

fn append_cloud_async_put(el: &mut EventLoop) -> crate::common::MidgeResult<u64> {
    let ops = vec![crate::runtime::TransactionOp::Put {
        cf_id: 0,
        key: bytes::Bytes::from_static(b"strict-seal-key"),
        value: bytes::Bytes::from_static(b"strict-seal-value"),
        ttl_seconds: None,
        insert_only: false,
    }];
    let (last_sequence, _op_count, deferred) = el.wal_actor.append_transaction(
        &mut el.state,
        crate::runtime::actors::wal::TransactionAppendParams {
            request_id: 302,
            ops,
            assertions: Vec::new(),
            durability_policy: Some(crate::wal::DurabilityPolicy::CloudAsync),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    assert!(
        deferred,
        "cloud-backed transaction should defer cloud durability"
    );
    Ok(last_sequence)
}

#[cfg(feature = "failpoints")]
fn expect_failed_seal_response(fail_rx: &crossbeam::channel::Receiver<RuntimeResponse>) {
    match fail_rx.recv().expect("failed strict response") {
        RuntimeResponse::Error { error, .. } => match error {
            crate::common::MidgeError::Internal(message) => {
                assert!(
                    message.contains("cloud seal failed after WAL flush before rotate"),
                    "unexpected strict failure: {message}"
                );
            }
            other => panic!("unexpected strict failure: {other:?}"),
        },
        other => panic!("unexpected strict failure response: {other:?}"),
    }
}

#[cfg(feature = "failpoints")]
fn complete_retry_and_ack(
    el: &mut EventLoop,
    last_sequence: u64,
    retry_request_id: u64,
    retry_rx: &crossbeam::channel::Receiver<RuntimeResponse>,
) {
    let seg_id = el
        .durability
        .inflight_segment_for_sequence(last_sequence)
        .expect("inflight segment for strict retry");
    copy_local_segment_to_remote_wal_for_test(el, seg_id);
    el.hybrid_storage
        .as_ref()
        .expect("hybrid storage")
        .publish_remote_wal_segment(
            seg_id,
            last_sequence,
            &el.state.wal_dir.join(crate::wal::segment_file_name(seg_id)),
            el.state.writer_epoch,
            &crate::common::OperationDeadline::unbounded(),
        )
        .expect("publish retry remote WAL for test CloudAck");
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: seg_id,
        max_sequence: last_sequence,
    });

    match retry_rx.recv().expect("strict retry response") {
        RuntimeResponse::Ok { request_id } => assert_eq!(request_id, retry_request_id),
        other => panic!("unexpected strict retry response: {other:?}"),
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retry_seal_wal_for_cloud_after_failpoint_before_rotate() -> crate::common::MidgeResult<()>
{
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let last_sequence = append_cloud_async_put(&mut el)?;

    let fail_request_id = 401u64;
    let fail_rx = el.router.register(fail_request_id, "TestRequest");
    fail::cfg(
        "midge::cloud::inject_fail_after_wal_flush_before_rotate",
        "return",
    )
    .expect("configure cloud seal failpoint");

    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;
    let outcome = el.handle_runtime_msg(
        RuntimeMsg::SealWalForCloud {
            request_id: fail_request_id,
            sequence: last_sequence,
            wait_for_ack: true,
        },
        &msg_rx,
    );
    // Act
    // Assert
    assert_eq!(outcome, super::super::HandleOutcome::Continue);
    expect_failed_seal_response(&fail_rx);
    assert!(
        el.state.wal.pending_writes > 0,
        "failed strict seal must preserve buffered WAL accounting for retry"
    );
    assert!(
        el.durability
            .inflight_segment_for_sequence(last_sequence)
            .is_none(),
        "failed strict seal must not invent an inflight segment before rotate succeeds"
    );

    fail::remove("midge::cloud::inject_fail_after_wal_flush_before_rotate");

    let retry_request_id = 402u64;
    let retry_rx = el.router.register(retry_request_id, "TestRequest");
    let outcome = el.handle_runtime_msg(
        RuntimeMsg::SealWalForCloud {
            request_id: retry_request_id,
            sequence: last_sequence,
            wait_for_ack: true,
        },
        &msg_rx,
    );
    assert_eq!(outcome, super::super::HandleOutcome::Continue);
    assert!(
            el.durability
                .inflight_segment_for_sequence(last_sequence)
                .is_some(),
            "successful retry should install an inflight segment instead of falling through to a missing-cover error"
        );
    complete_retry_and_ack(&mut el, last_sequence, retry_request_id, &retry_rx);
    assert_eq!(
        el.state.wal.pending_writes, 0,
        "successful strict retry should clear buffered WAL accounting"
    );

    scenario.teardown();
    Ok(())
}

/// Counts `read_current` calls so a test can prove how many lease round trips a
/// single drain pass performs. Under `ProviderLeaderStore` each one is a cloud
/// GET, so the count is the cost being measured.
#[derive(Debug, Default)]
struct CountingLeaderStore {
    reads: std::sync::atomic::AtomicUsize,
    holder_id: String,
    epoch: u64,
}

#[derive(Debug)]
struct DelayedLeaderStore {
    delay: Duration,
    holder_id: String,
    epoch: u64,
}

#[derive(Debug)]
struct FailAfterFirstLeaderStore {
    reads: AtomicUsize,
    failure_delay: Duration,
    holder_id: String,
    epoch: u64,
}

impl FailAfterFirstLeaderStore {
    fn new(failure_delay: Duration, holder_id: &str, epoch: u64) -> Self {
        Self {
            reads: AtomicUsize::new(0),
            failure_delay,
            holder_id: holder_id.to_string(),
            epoch,
        }
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl DelayedLeaderStore {
    fn new(delay: Duration, holder_id: &str, epoch: u64) -> Self {
        Self {
            delay,
            holder_id: holder_id.to_string(),
            epoch,
        }
    }
}

impl crate::lease::LeaderStore for DelayedLeaderStore {
    fn acquire_leadership(
        &self,
        _holder_id: &str,
    ) -> Result<crate::lease::LeaderRecord, crate::lease::LeaseError> {
        Err(crate::lease::LeaseError::AcquisitionFailed(
            "delayed test store does not acquire".to_string(),
        ))
    }

    fn read_current(&self) -> Result<Option<crate::lease::LeaderRecord>, crate::lease::LeaseError> {
        std::thread::sleep(self.delay);
        Ok(Some(crate::lease::LeaderRecord {
            epoch: self.epoch,
            holder_id: self.holder_id.clone(),
            acquired_at: "2026-08-25T00:00:00Z".to_string(),
        }))
    }

    fn validate_epoch_with_timeout(
        &self,
        expected_holder_id: &str,
        expected_epoch: u64,
        timeout: Duration,
    ) -> Result<(), crate::lease::LeaseError> {
        std::thread::sleep(self.delay.min(timeout));
        if timeout < self.delay {
            return Err(crate::lease::LeaseError::RenewalFailed(format!(
                "delayed leader validation timed out after {timeout:?}"
            )));
        }
        if self.holder_id == expected_holder_id && self.epoch == expected_epoch {
            Ok(())
        } else {
            Err(crate::lease::LeaseError::RenewalFailed(format!(
                "epoch/holder mismatch: expected holder={expected_holder_id} epoch={expected_epoch}, found holder={} epoch={}",
                self.holder_id, self.epoch
            )))
        }
    }
}

impl crate::lease::LeaderStore for FailAfterFirstLeaderStore {
    fn acquire_leadership(
        &self,
        _holder_id: &str,
    ) -> Result<crate::lease::LeaderRecord, crate::lease::LeaseError> {
        Err(crate::lease::LeaseError::AcquisitionFailed(
            "failing test store does not acquire".to_string(),
        ))
    }

    fn read_current(&self) -> Result<Option<crate::lease::LeaderRecord>, crate::lease::LeaseError> {
        if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(Some(crate::lease::LeaderRecord {
                epoch: self.epoch,
                holder_id: self.holder_id.clone(),
                acquired_at: "2026-08-25T00:00:00Z".to_string(),
            }));
        }
        std::thread::sleep(self.failure_delay);
        Err(crate::lease::LeaseError::RenewalFailed(
            "persistent delayed lease validation failure".to_string(),
        ))
    }
}

impl CountingLeaderStore {
    fn new(holder_id: &str, epoch: u64) -> Self {
        Self {
            reads: std::sync::atomic::AtomicUsize::new(0),
            holder_id: holder_id.to_string(),
            epoch,
        }
    }

    fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl crate::lease::LeaderStore for CountingLeaderStore {
    fn acquire_leadership(
        &self,
        _holder_id: &str,
    ) -> Result<crate::lease::LeaderRecord, crate::lease::LeaseError> {
        Err(crate::lease::LeaseError::AcquisitionFailed(
            "counting store does not acquire".to_string(),
        ))
    }

    fn read_current(&self) -> Result<Option<crate::lease::LeaderRecord>, crate::lease::LeaseError> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(crate::lease::LeaderRecord {
            epoch: self.epoch,
            holder_id: self.holder_id.clone(),
            acquired_at: "2026-08-25T00:00:00Z".to_string(),
        }))
    }
}

#[test]
fn should_validate_writer_lease_once_given_multi_segment_backlog_when_draining_uploads(
) -> crate::common::MidgeResult<()> {
    // Arrange: a CloudAsync loop holding several sealed WAL segments that still
    // need uploading, with a leader store that counts its lease round trips.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let leader_store = std::sync::Arc::new(CountingLeaderStore::new("writer-1", 1));
    el.leader_store =
        Some(std::sync::Arc::clone(&leader_store) as std::sync::Arc<dyn crate::lease::LeaderStore>);
    el.leader_holder_id = Some("writer-1".to_string());

    let mut sealed = Vec::new();
    for _ in 0..4 {
        append_cloud_async_put(&mut el)?;
        sealed.push(seal_segment_without_remote_proof_for_test(&mut el)?);
    }
    el.cloud_wal.upload_backlog.clear();
    for (segment_id, max_sequence) in &sealed {
        el.cloud_wal
            .upload_backlog
            .insert(*segment_id, *max_sequence);
    }
    let before = leader_store.reads();

    // Act
    el.drain_cloud_wal_upload_backlog();

    // Assert
    assert!(
        el.cloud_wal.upload_backlog.is_empty(),
        "the pass must actually drain every segment, or the count below is vacuous"
    );
    let lease_reads = leader_store.reads() - before;
    assert_eq!(
        lease_reads, 1,
        "one drain pass must validate the writer lease once, not once per segment"
    );
    Ok(())
}

#[test]
fn should_back_off_runtime_wal_admission_when_storage_queue_is_full(
) -> crate::common::MidgeResult<()> {
    // Arrange: one sealed segment occupies the only storage queue slot while a
    // second accepted segment remains in the runtime-owned backlog.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    append_cloud_async_put(&mut el)?;
    let (first_segment, first_max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    append_cloud_async_put(&mut el)?;
    let (second_segment, second_max_sequence) =
        seal_segment_without_remote_proof_for_test(&mut el)?;
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("bounded-retry-local"))
            .expect("create bounded retry local storage"),
    );
    let cloud = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("bounded-retry-cloud"))
            .expect("create bounded retry cloud storage"),
    );
    let storage = Arc::new(crate::storage::HybridStorage::with_test_upload_limits(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        1,
        u64::MAX,
    ));
    storage.fence_cloud_wal_catalog(1)?;
    storage.enqueue_wal_segment(
        first_segment,
        &el.state
            .wal_dir
            .join(crate::wal::segment_file_name(first_segment)),
        first_max_sequence,
    )?;
    el.set_hybrid_storage(Arc::clone(&storage));
    el.cloud_wal.upload_backlog.clear();
    el.cloud_wal
        .upload_backlog
        .insert(second_segment, second_max_sequence);
    let leader_store = Arc::new(CountingLeaderStore::new("writer-1", 1));
    el.leader_store = Some(Arc::clone(&leader_store) as Arc<dyn crate::lease::LeaderStore>);
    el.leader_holder_id = Some("writer-1".to_string());

    // Act: the first pass discovers capacity pressure; an immediate second
    // pass must respect runtime backoff instead of repeating lease and WAL I/O.
    el.drain_cloud_wal_upload_backlog();
    let reads_after_full_queue = leader_store.reads();
    el.drain_cloud_wal_upload_backlog();

    // Assert
    assert_eq!(storage.pending_upload_count(), 1);
    assert_eq!(
        el.cloud_wal.upload_backlog.get(&second_segment),
        Some(&second_max_sequence)
    );
    assert!(el.cloud_wal.upload_retry_deadline_timeout().is_some());
    assert_eq!(
        leader_store.reads(),
        reads_after_full_queue,
        "a full queue must not trigger another provider lease read before backoff expires"
    );
    Ok(())
}

#[test]
fn should_derive_ack_deadline_from_waiting_caller_when_verifying_remote_segment(
) -> crate::common::MidgeResult<()> {
    // Arrange: a cloud-durability waiter that registered long enough ago that
    // its response budget is already spent.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sequence = append_cloud_async_put(&mut el)?;
    let request_id = 90_101;
    let _rx = el.router.register(request_id, "SealWalForCloud");
    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;
    assert_eq!(
        el.handle_runtime_msg(
            RuntimeMsg::SealWalForCloud {
                request_id,
                sequence,
                wait_for_ack: true,
            },
            &msg_rx,
        ),
        super::super::HandleOutcome::Continue
    );
    let segment_id = el
        .durability
        .inflight_segment_for_sequence(sequence)
        .expect("inflight segment");

    // Act
    let request_ids = el.durability.cloud_durability_request_ids_at(segment_id);

    // Assert: the ack path can see which caller it is serving, which is what
    // lets it bound its storage work by that caller's remaining budget.
    assert_eq!(
        request_ids,
        vec![request_id],
        "the ack path must be able to find the caller whose budget it shares"
    );
    assert!(
        el.router.registered_at(request_id).is_some(),
        "the caller's start instant must still be resolvable"
    );
    Ok(())
}

#[test]
fn should_complete_accepted_wal_publication_given_abandoned_caller_when_every_waiter_gave_up(
) -> crate::common::MidgeResult<()> {
    // Arrange: a caller waits for a cloud-strict seal, then gives up.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sequence = append_cloud_async_put(&mut el)?;
    let request_id = 90_201;
    let _rx = el.router.register(request_id, "SealWalForCloud");
    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;
    assert_eq!(
        el.handle_runtime_msg(
            RuntimeMsg::SealWalForCloud {
                request_id,
                sequence,
                wait_for_ack: true,
            },
            &msg_rx,
        ),
        super::super::HandleOutcome::Continue
    );
    let segment_id = el
        .durability
        .inflight_segment_for_sequence(sequence)
        .expect("inflight segment");
    copy_local_segment_to_remote_wal_for_test(&el, segment_id);

    // Act: the caller times out and abandons before the upload is acked.
    el.router.abandon(request_id, Duration::from_millis(20));
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence: sequence,
    });

    // Assert: caller abandonment only stops response delivery. The sealed WAL
    // is already an accepted durability obligation and must still close the
    // inflight gap so later strict waiters cannot stall behind it.
    assert!(
        !el.state.persistence_anomaly_detected(),
        "caller abandonment must not turn valid publication into a persistence failure"
    );
    assert_eq!(el.state.wal.cloud_durable_seq, sequence);
    assert!(
        el.durability
            .inflight_segment_for_sequence(sequence)
            .is_none(),
        "callerless completion must retire the accepted inflight segment"
    );
    Ok(())
}

#[test]
fn should_use_latest_surviving_waiter_deadline_given_older_waiter_already_expired() {
    // Arrange: the first waiter has exhausted its response budget while a
    // newer waiter on the same segment still has substantial time remaining.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )
    .expect("create cloud event loop");
    el.runtime_response_timeout = Duration::from_millis(200);
    let segment_id = 91_001;
    let old_request_id = 91_002;
    let new_request_id = 91_003;
    let _old_rx = el.router.register(old_request_id, "SealWalForCloud");
    el.durability.queue_waiter_for_key(
        segment_id,
        DurabilityWaiter::CloudDurability {
            request_id: old_request_id,
        },
    );
    std::thread::sleep(Duration::from_millis(150));
    let _new_rx = el.router.register(new_request_id, "SealWalForCloud");
    el.durability.queue_waiter_for_key(
        segment_id,
        DurabilityWaiter::CloudDurability {
            request_id: new_request_id,
        },
    );
    std::thread::sleep(Duration::from_millis(75));

    // Act
    let deadline = el
        .cloud_ack_deadline(segment_id)
        .expect("a surviving waiter supplies a deadline");

    // Assert
    assert!(
        !deadline.is_expired(),
        "the expired older waiter must not cancel a newer caller's remaining budget"
    );
}

#[test]
fn should_preserve_later_segment_waiter_when_earlier_gap_waiter_expired(
) -> crate::common::MidgeResult<()> {
    // Arrange: segment two cannot become durable until segment one closes its
    // frontier gap. Its newer caller therefore contributes budget to the
    // acknowledgement work for that earlier segment.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_millis(200);
    append_cloud_async_put(&mut el)?;
    let (first_segment, first_max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    append_cloud_async_put(&mut el)?;
    let (second_segment, second_max_sequence) =
        seal_segment_without_remote_proof_for_test(&mut el)?;

    let first_request_id = 91_011;
    let second_request_id = 91_012;
    let first_rx = el.router.register(first_request_id, "SealWalForCloud");
    el.durability.queue_waiter_for_key(
        first_segment,
        DurabilityWaiter::CloudDurability {
            request_id: first_request_id,
        },
    );
    std::thread::sleep(Duration::from_millis(150));
    let second_rx = el.router.register(second_request_id, "SealWalForCloud");
    el.durability.queue_waiter_for_key(
        second_segment,
        DurabilityWaiter::CloudDurability {
            request_id: second_request_id,
        },
    );
    std::thread::sleep(Duration::from_millis(75));

    // Act: the first caller's budget is exhausted, but publication of its
    // segment is also required to serve the newer second caller.
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: first_segment,
        max_sequence: first_max_sequence,
    });

    // Assert: closing the first gap succeeds and does not prematurely drain
    // the still-live waiter attached to the dependent segment.
    assert!(matches!(
        first_rx.try_recv(),
        Ok(RuntimeResponse::Ok {
            request_id
        }) if request_id == first_request_id
    ));
    assert!(matches!(
        second_rx.try_recv(),
        Err(crossbeam::channel::TryRecvError::Empty)
    ));
    assert_eq!(el.state.wal.cloud_durable_seq, first_max_sequence);

    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: second_segment,
        max_sequence: second_max_sequence,
    });
    assert!(matches!(
        second_rx.try_recv(),
        Ok(RuntimeResponse::Ok {
            request_id
        }) if request_id == second_request_id
    ));
    assert_eq!(el.state.wal.cloud_durable_seq, second_max_sequence);
    Ok(())
}

#[test]
fn should_preserve_earlier_waiter_when_later_segment_upload_fails() -> crate::common::MidgeResult<()>
{
    // Arrange: two independently addressable inflight generations. A failure
    // in the later one does not prevent the earlier frontier from completing.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    append_cloud_async_put(&mut el)?;
    let (first_segment, first_max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    append_cloud_async_put(&mut el)?;
    let (second_segment, second_max_sequence) =
        seal_segment_without_remote_proof_for_test(&mut el)?;
    let first_request_id = 91_021;
    let second_request_id = 91_022;
    let first_rx = el.router.register(first_request_id, "SealWalForCloud");
    let second_rx = el.router.register(second_request_id, "SealWalForCloud");
    el.durability.queue_waiter_for_key(
        first_segment,
        DurabilityWaiter::CloudDurability {
            request_id: first_request_id,
        },
    );
    el.durability.queue_waiter_for_key(
        second_segment,
        DurabilityWaiter::CloudDurability {
            request_id: second_request_id,
        },
    );
    el.state
        .sequence_idempotency_cache
        .insert(first_request_id, (first_max_sequence, 1, 0));
    el.state
        .sequence_idempotency_cache
        .insert(second_request_id, (second_max_sequence, 1, 0));

    // Act: storage reports the later upload failure before the first
    // acknowledgement arrives.
    el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id: second_segment,
        error: "injected later-segment failure".to_string(),
        terminal: true,
        failure_kind: crate::storage::CloudUploadFailureKind::Other,
    });

    // Assert: only the failed generation and its dependents fail. Segment one
    // can still close normally when its valid acknowledgement arrives.
    assert!(matches!(
        second_rx.try_recv(),
        Ok(RuntimeResponse::Error {
            request_id,
            ..
        }) if request_id == second_request_id
    ));
    assert!(matches!(
        first_rx.try_recv(),
        Err(crossbeam::channel::TryRecvError::Empty)
    ));
    assert!(
        el.state
            .sequence_idempotency_cache
            .contains_key(&first_request_id),
        "later upload failure must preserve the earlier request's retry identity"
    );
    assert!(
        el.state
            .sequence_idempotency_cache
            .contains_key(&second_request_id),
        "the requeued segment must preserve its request identity while publication remains owned"
    );

    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id: first_segment,
        max_sequence: first_max_sequence,
    });
    assert!(matches!(
        first_rx.try_recv(),
        Ok(RuntimeResponse::Ok { request_id }) if request_id == first_request_id
    ));
    Ok(())
}

#[test]
fn should_bound_strict_seal_lease_validation_given_request_deadline_is_exhausted(
) -> crate::common::MidgeResult<()> {
    // Arrange: a strict request has no response budget left. No provider lease
    // GET may start after that point, including the validations performed by
    // sealing and upload admission.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sequence = append_cloud_async_put(&mut el)?;
    let leader_store = std::sync::Arc::new(CountingLeaderStore::new("writer-1", 1));
    el.leader_store =
        Some(std::sync::Arc::clone(&leader_store) as std::sync::Arc<dyn crate::lease::LeaderStore>);
    el.leader_holder_id = Some("writer-1".to_string());
    el.runtime_response_timeout = Duration::ZERO;
    let request_id = 91_101;
    let response_rx = el.router.register(request_id, "SealWalForCloud");
    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;

    // Act
    el.handle_runtime_msg(
        RuntimeMsg::SealWalForCloud {
            request_id,
            sequence,
            wait_for_ack: true,
        },
        &msg_rx,
    );

    // Assert
    assert_eq!(
        leader_store.reads(),
        0,
        "an exhausted caller budget must prevent every strict-seal lease read"
    );
    assert!(matches!(
        response_rx.try_recv(),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Timeout(_),
            ..
        })
    ));
    assert!(
        el.durability
            .inflight_segment_for_sequence(sequence)
            .is_none(),
        "deadline expiry before sealing must leave the active WAL retryable"
    );
    Ok(())
}

#[test]
fn should_not_start_wal_flush_when_lease_check_leaves_less_than_storage_budget(
) -> crate::common::MidgeResult<()> {
    // Arrange: the first provider lease read consumes enough of the shared
    // caller deadline that a WAL flush with its configured I/O timeout can no
    // longer safely begin.
    let tmp = tempfile::tempdir().expect("create bounded strict-seal directory");
    let state = RuntimeState::new(tmp.path().to_path_buf(), false);
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("hybrid-local"))
            .expect("create bounded strict-seal local backend"),
    );
    let cloud = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("cloud-store"))
            .expect("create bounded strict-seal cloud backend"),
    );
    let storage = Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ));
    storage.fence_cloud_wal_catalog(1)?;
    let leader_store: Arc<dyn crate::lease::LeaderStore> = Arc::new(DelayedLeaderStore::new(
        Duration::from_millis(80),
        "writer-1",
        1,
    ));
    let router = Arc::new(ResponseRouter::new());
    let config = crate::runtime::RuntimeConfig {
        wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
        storage_io_timeout: Duration::from_millis(150),
        runtime_response_timeout: Duration::from_millis(200),
        hybrid_storage: Some(storage),
        writer_epoch: 1,
        leader_store: Some(leader_store),
        leader_holder_id: Some("writer-1".to_string()),
        ..crate::runtime::RuntimeConfig::default()
    };
    let mut el = EventLoop::new(state, false, Arc::clone(&router), config, None)?;
    let sequence = append_cloud_async_put(&mut el)?;
    let active_segment = el.state.wal.current_segment_id;
    let request_id = 91_102;
    let response_rx = router.register(request_id, "SealWalForCloud");
    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;

    // Act
    let started = Instant::now();
    el.handle_runtime_msg(
        RuntimeMsg::SealWalForCloud {
            request_id,
            sequence,
            wait_for_ack: true,
        },
        &msg_rx,
    );
    let elapsed = started.elapsed();

    // Assert
    assert!(matches!(
        response_rx.try_recv(),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Timeout(_),
            ..
        })
    ));
    assert!(
        elapsed < Duration::from_millis(150),
        "strict seal started additional I/O outside the remaining budget: {elapsed:?}"
    );
    assert_eq!(
        el.state.wal.current_segment_id, active_segment,
        "deadline refusal before flush must leave the active WAL segment intact"
    );
    assert!(el.state.wal.pending_writes > 0);
    assert!(el.durability.cloud_seal_retry_needed());
    Ok(())
}

#[test]
fn should_back_off_failed_cloud_seal_while_normal_requests_make_progress(
) -> crate::common::MidgeResult<()> {
    // Arrange: the first lease validation permits the WAL flush, then every
    // post-flush validation fails slowly. The active segment remains retryable.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    append_cloud_async_put(&mut el)?;
    let leader_store = Arc::new(FailAfterFirstLeaderStore::new(
        Duration::from_millis(75),
        "writer-1",
        1,
    ));
    el.leader_store = Some(Arc::clone(&leader_store) as Arc<dyn crate::lease::LeaderStore>);
    el.leader_holder_id = Some("writer-1".to_string());
    el.seal_current_cloud_segment()
        .expect_err("post-flush lease validation must fail");
    assert!(el.durability.cloud_seal_retry_needed());
    let reads_after_failure = leader_store.reads();
    assert_eq!(reads_after_failure, 2);
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded::<RuntimeMsg>();
    let mut response_receivers = Vec::new();
    for request_id in 91_301..91_304 {
        response_receivers.push((request_id, el.router.register(request_id, "Noop")));
        msg_tx
            .send(RuntimeMsg::Noop { request_id })
            .expect("queue unrelated request");
    }

    // Act: process unrelated requests immediately after the failed attempt,
    // then drive two maintenance passes after the retry becomes due.
    let request_start = Instant::now();
    for _ in 0..response_receivers.len() {
        let message = msg_rx.recv().expect("receive unrelated request");
        el.process_one(message, &msg_rx);
    }
    let request_elapsed = request_start.elapsed();
    std::thread::sleep(Duration::from_millis(25));
    el.progress_pass(&msg_rx);
    let reads_after_due_retry = leader_store.reads();
    el.progress_pass(&msg_rx);

    // Assert
    assert!(
        request_elapsed < Duration::from_millis(50),
        "seal maintenance delayed normal request progress: {request_elapsed:?}"
    );
    for (request_id, response_rx) in response_receivers {
        assert!(matches!(
            response_rx.try_recv(),
            Ok(RuntimeResponse::Ok {
                request_id: response_id
            }) if response_id == request_id
        ));
    }
    assert_eq!(
        reads_after_due_retry,
        reads_after_failure + 1,
        "exactly one seal retry should run once the backoff expires"
    );
    assert_eq!(
        leader_store.reads(),
        reads_after_due_retry,
        "a failed due retry must re-arm backoff before another maintenance pass"
    );
    Ok(())
}

#[test]
fn should_requeue_publication_with_timeout_given_ack_deadline_expires_before_lease_check(
) -> crate::common::MidgeResult<()> {
    // Arrange: the upload completed, but its strict caller's shared deadline is
    // exhausted before acknowledgement validation begins.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sequence = append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    let leader_store = std::sync::Arc::new(CountingLeaderStore::new("writer-1", 1));
    el.leader_store =
        Some(std::sync::Arc::clone(&leader_store) as std::sync::Arc<dyn crate::lease::LeaderStore>);
    el.leader_holder_id = Some("writer-1".to_string());
    el.runtime_response_timeout = Duration::ZERO;
    let request_id = 91_201;
    let response_rx = el.router.register(request_id, "SealWalForCloud");
    el.durability
        .queue_waiter_for_key(segment_id, DurabilityWaiter::CloudDurability { request_id });

    // Act
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence,
    });

    // Assert
    assert_eq!(sequence, max_sequence);
    assert_eq!(
        leader_store.reads(),
        0,
        "acknowledgement lease validation must not start outside the caller deadline"
    );
    assert!(matches!(
        response_rx.try_recv(),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Timeout(_),
            ..
        })
    ));
    assert_eq!(
        el.cloud_wal.upload_backlog.get(&segment_id),
        Some(&max_sequence),
        "deadline expiry must requeue the accepted WAL obligation"
    );
    assert_eq!(
        el.durability.inflight_segment_for_sequence(max_sequence),
        Some(segment_id),
        "failed acknowledgement must retain the inflight frontier gap"
    );
    Ok(())
}

#[test]
fn should_requeue_known_wal_publication_when_sequence_range_is_inconsistent(
) -> crate::common::MidgeResult<()> {
    // Arrange: recovery drift has already advanced the visible cloud frontier
    // to this inflight segment's maximum. The scoped idempotency interval is
    // therefore inconsistent, but ownership of the accepted local WAL remains
    // sufficient to retry publication.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    el.state.wal.cloud_durable_seq = max_sequence;
    let leader_store = std::sync::Arc::new(CountingLeaderStore::new("different-writer", 1));
    el.leader_store =
        Some(std::sync::Arc::clone(&leader_store) as std::sync::Arc<dyn crate::lease::LeaderStore>);
    el.leader_holder_id = Some("writer-1".to_string());
    let request_id = 91_202;
    let response_rx = el.router.register(request_id, "SealWalForCloud");
    el.durability
        .queue_waiter_for_key(segment_id, DurabilityWaiter::CloudDurability { request_id });

    // Act: lease validation fails before acknowledgement settlement.
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence,
    });

    // Assert
    assert_eq!(
        el.cloud_wal.upload_backlog.get(&segment_id),
        Some(&max_sequence),
        "known accepted WAL must requeue even when its cache interval cannot be derived"
    );
    assert_eq!(
        el.durability.inflight_segment_for_sequence(max_sequence),
        Some(segment_id)
    );
    assert!(matches!(
        response_rx.try_recv(),
        Ok(RuntimeResponse::Error {
            error: crate::common::MidgeError::Fenced(_),
            ..
        })
    ));
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_resume_wal_upload_after_storage_retry_budget_is_exhausted(
) -> crate::common::MidgeResult<()> {
    // Arrange: the storage-owned queue exhausts all three upload attempts while
    // the sealed local WAL remains an accepted runtime obligation.
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let sequence = append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = el
        .seal_current_cloud_segment()?
        .expect("seal cloud WAL for terminal retry test");
    fail::cfg("midge::cloud::inject_fail_wal_upload", "return")
        .expect("configure WAL upload failures");

    // Act: exhaust storage retries, restore the provider, and continue driving
    // the event loop without reopening the database.
    for _ in 0..8 {
        el.tick_hybrid_storage();
        std::thread::sleep(Duration::from_millis(5));
    }
    fail::remove("midge::cloud::inject_fail_wal_upload");
    // The recovered upload runs on the storage worker. Release the test's
    // process-global failpoint write lock before asking that worker to evaluate
    // its (now disabled) failpoint hooks.
    drop(test_guard);
    assert_eq!(
        el.hybrid_storage
            .as_ref()
            .expect("hybrid storage")
            .pending_upload_count(),
        0,
        "the storage queue must actually exhaust its attempt budget"
    );
    assert_eq!(
        el.cloud_wal.upload_backlog.get(&segment_id),
        Some(&max_sequence),
        "terminal queue failure must transfer ownership back to the runtime"
    );

    std::thread::sleep(Duration::from_millis(25));
    let msg_rx = crossbeam::channel::unbounded::<RuntimeMsg>().1;
    let recovery_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < recovery_deadline && el.state.wal.cloud_durable_seq < max_sequence {
        el.progress_pass(&msg_rx);
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    assert_eq!(sequence, max_sequence);
    assert_eq!(
        el.state.wal.cloud_durable_seq, max_sequence,
        "callerless retry must close the frontier after provider recovery"
    );
    assert!(el.cloud_wal.upload_backlog.is_empty());
    scenario.teardown();
    Ok(())
}

#[test]
fn should_retry_runtime_owned_wal_upload_under_continuous_request_load(
) -> crate::common::MidgeResult<()> {
    // Arrange: storage exhausted its attempt budget and transferred a sealed
    // segment back to the runtime while unrelated requests remain queued.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id,
        error: "storage retry budget exhausted".to_string(),
        terminal: true,
        failure_kind: crate::storage::CloudUploadFailureKind::Other,
    });
    std::thread::sleep(Duration::from_millis(25));
    assert_eq!(
        el.cloud_wal.upload_backlog.get(&segment_id),
        Some(&max_sequence)
    );
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded::<RuntimeMsg>();
    msg_tx
        .send(RuntimeMsg::Noop { request_id: 90_401 })
        .expect("queue first request");
    msg_tx
        .send(RuntimeMsg::Noop { request_id: 90_402 })
        .expect("queue continuing request load");

    // Act: process one normal request while another remains queued.
    let first = msg_rx.recv().expect("receive first queued request");
    el.process_one(first, &msg_rx);

    // Assert
    assert!(
        !msg_rx.is_empty(),
        "the fixture must keep request pressure present during the retry slot"
    );
    assert!(
        !el.cloud_wal.upload_backlog.contains_key(&segment_id),
        "runtime-owned WAL publication must receive a fairness slot under sustained load"
    );
    Ok(())
}

#[test]
fn should_drain_runtime_owned_wal_retry_before_shutdown_succeeds() -> crate::common::MidgeResult<()>
{
    // Arrange: a terminal storage failure has transferred an accepted segment
    // into the runtime backlog immediately before shutdown begins.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.shutdown_cloud_drain_timeout = Duration::from_secs(2);
    append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id,
        error: "storage retry budget exhausted".to_string(),
        terminal: true,
        failure_kind: crate::storage::CloudUploadFailureKind::Other,
    });
    let request_id = 90_403;
    let response_rx = el.router.register(request_id, "Shutdown");

    // Act
    let outcome = el.handle_shutdown(Some(request_id));

    // Assert
    assert_eq!(outcome, super::super::HandleOutcome::Break);
    assert!(matches!(
        response_rx.try_recv(),
        Ok(RuntimeResponse::Ok {
            request_id: response_id
        }) if response_id == request_id
    ));
    assert!(el.cloud_wal.upload_backlog.is_empty());
    assert_eq!(
        el.hybrid_storage
            .as_ref()
            .expect("hybrid storage")
            .pending_upload_count(),
        0
    );
    assert_eq!(el.state.wal.cloud_durable_seq, max_sequence);
    Ok(())
}

#[test]
fn should_bound_final_cloud_wal_seal_by_shutdown_deadline() -> crate::common::MidgeResult<()> {
    // Arrange: shutdown owns an active CloudAsync WAL that still needs sealing,
    // while the first provider lease read is slower than the entire drain budget.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    append_cloud_async_put(&mut el)?;
    let active_segment = el.state.wal.current_segment_id;
    el.leader_store = Some(Arc::new(DelayedLeaderStore::new(
        Duration::from_millis(250),
        "writer-1",
        1,
    )));
    el.leader_holder_id = Some("writer-1".to_string());
    el.shutdown_cloud_drain_timeout = Duration::from_millis(40);
    let request_id = 90_406;
    let response_rx = el.router.register(request_id, "Shutdown");

    // Act
    let started = Instant::now();
    let outcome = el.handle_shutdown(Some(request_id));
    let elapsed = started.elapsed();

    // Assert
    assert_eq!(outcome, super::super::HandleOutcome::Break);
    assert!(
        elapsed < Duration::from_millis(150),
        "final cloud WAL seal exceeded the aggregate shutdown deadline: {elapsed:?}"
    );
    assert!(matches!(
        response_rx.try_recv(),
        Ok(RuntimeResponse::Error {
            request_id: response_id,
            error: crate::common::MidgeError::Timeout(_),
        }) if response_id == request_id
    ));
    assert_eq!(
        el.state.wal.current_segment_id, active_segment,
        "timed-out final seal must retain the active WAL for recovery"
    );
    assert!(
        el.state.wal.pending_writes > 0,
        "timed-out final seal must retain pending WAL accounting"
    );
    Ok(())
}

#[test]
fn should_bound_runtime_owned_wal_admission_by_shutdown_deadline() -> crate::common::MidgeResult<()>
{
    // Arrange: terminal upload failure transfers a sealed WAL back to the
    // runtime just before shutdown, and lease validation is slower than the
    // entire shutdown drain budget.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id,
        error: "storage retry budget exhausted".to_string(),
        terminal: true,
        failure_kind: crate::storage::CloudUploadFailureKind::Other,
    });
    std::thread::sleep(Duration::from_millis(25));
    el.leader_store = Some(Arc::new(DelayedLeaderStore::new(
        Duration::from_millis(250),
        "writer-1",
        1,
    )));
    el.leader_holder_id = Some("writer-1".to_string());
    el.runtime_response_timeout = Duration::from_millis(500);
    el.shutdown_cloud_drain_timeout = Duration::from_millis(40);
    let request_id = 90_404;
    let response_rx = el.router.register(request_id, "Shutdown");

    // Act
    let started = Instant::now();
    let outcome = el.handle_shutdown(Some(request_id));
    let elapsed = started.elapsed();

    // Assert
    assert_eq!(outcome, super::super::HandleOutcome::Break);
    assert!(
        elapsed < Duration::from_millis(150),
        "shutdown WAL admission exceeded its shared drain deadline: {elapsed:?}"
    );
    assert!(matches!(
        response_rx.try_recv(),
        Ok(RuntimeResponse::Error {
            request_id: response_id,
            ..
        }) if response_id == request_id
    ));
    assert_eq!(
        el.cloud_wal.upload_backlog.get(&segment_id),
        Some(&max_sequence),
        "timed-out shutdown must retain the runtime-owned WAL obligation"
    );
    Ok(())
}

#[test]
fn should_bound_pending_cloud_ack_given_shutdown_deadline() -> crate::common::MidgeResult<()> {
    // Arrange: the upload worker has completed and queued its acknowledgement,
    // but the runtime has not yet settled that ACK. Its readback proof is slower
    // than the entire shutdown drain budget.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_millis(500);
    el.shutdown_cloud_drain_timeout = Duration::from_millis(40);
    let cloud_fs = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("open delayed shutdown acknowledgement cloud backend"),
    );
    let delayed_cloud = Arc::new(ArmedDelayedHeadStorageBackend::new(
        cloud_fs,
        Duration::from_millis(250),
    ));
    let local: Arc<dyn crate::storage::StorageBackend> = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local"))
            .expect("open shutdown acknowledgement local backend"),
    );
    let cloud: Arc<dyn crate::storage::StorageBackend> = delayed_cloud.clone();
    let (storage_event_tx, storage_event_rx) = crossbeam::channel::unbounded();
    let storage = Arc::new(crate::storage::HybridStorage::new_with_event_sender(
        local,
        cloud,
        storage_event_tx,
    ));
    el.set_hybrid_storage(Arc::clone(&storage));
    el.hybrid_storage_events = Some(storage_event_rx.clone());

    append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    let local_path = el
        .state
        .wal_dir
        .join(crate::wal::segment_file_name(segment_id));
    storage.enqueue_wal_segment(segment_id, &local_path, max_sequence)?;
    assert!(storage.process_uploads().is_empty());
    let ack_wait_started = Instant::now();
    while storage_event_rx.is_empty() && ack_wait_started.elapsed() < Duration::from_secs(2) {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        !storage_event_rx.is_empty(),
        "upload worker must queue the acknowledgement before shutdown begins"
    );
    assert_eq!(
        storage.pending_upload_count(),
        1,
        "the unprocessed ACK must still own one storage queue entry"
    );
    delayed_cloud.arm();
    let request_id = 90_405;
    let response_rx = el.router.register(request_id, "Shutdown");

    // Act
    let started = Instant::now();
    let outcome = el.handle_shutdown(Some(request_id));
    let elapsed = started.elapsed();

    // Assert
    assert_eq!(outcome, super::super::HandleOutcome::Break);
    assert!(
        elapsed < Duration::from_millis(150),
        "pending CloudAck settlement exceeded the shared shutdown deadline: {elapsed:?}"
    );
    assert!(matches!(
        response_rx.try_recv(),
        Ok(RuntimeResponse::Error {
            request_id: response_id,
            ..
        }) if response_id == request_id
    ));
    assert_eq!(
        el.cloud_wal.upload_backlog.get(&segment_id),
        Some(&max_sequence),
        "timed-out ACK settlement must retain the accepted WAL for retry"
    );
    assert_eq!(el.state.wal.cloud_durable_seq, 0);
    Ok(())
}

#[test]
fn should_poll_inflight_cloud_upload_on_interval_without_busy_spin(
) -> crate::common::MidgeResult<()> {
    // Arrange: the storage worker owns an upload whose provider proof is slow.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let cloud_fs = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("open delayed upload cloud backend"),
    );
    let delayed_cloud = Arc::new(ArmedDelayedHeadStorageBackend::new(
        cloud_fs,
        Duration::from_millis(250),
    ));
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local"))
            .expect("open delayed upload local backend"),
    );
    el.set_hybrid_storage(Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        delayed_cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    append_cloud_async_put(&mut el)?;
    el.seal_current_cloud_segment()?
        .expect("seal slow cloud WAL upload");
    delayed_cloud.arm();
    el.tick_hybrid_storage();
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(
        el.hybrid_storage
            .as_ref()
            .expect("hybrid storage")
            .pending_upload_count(),
        1
    );

    // Act
    let actionable = el.has_actionable_work();
    let idle_timeout = el.idle_progress_timeout();

    // Assert
    assert!(
        !actionable,
        "an in-flight provider callback must not drive the 50-microsecond actionable loop"
    );
    assert!(
        idle_timeout.is_some_and(|timeout| {
            timeout > Duration::ZERO && timeout <= Duration::from_millis(10)
        }),
        "slow storage work still needs a bounded polling wakeup: {idle_timeout:?}"
    );
    Ok(())
}

#[test]
fn should_bound_callerless_ack_when_provider_exceeds_maintenance_budget(
) -> crate::common::MidgeResult<()> {
    // Arrange: a CloudAsync segment has no waiting caller, but its synchronous
    // acknowledgement proof must still yield the event loop after one bounded
    // maintenance attempt.
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    el.runtime_response_timeout = Duration::from_millis(100);
    let cloud_fs = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("cloud_store"))
            .expect("open delayed acknowledgement cloud backend"),
    );
    let delayed_cloud = Arc::new(ArmedDelayedHeadStorageBackend::new(
        cloud_fs,
        Duration::from_millis(250),
    ));
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(el.state.db_path.join("hybrid_local"))
            .expect("open delayed acknowledgement local backend"),
    );
    el.set_hybrid_storage(Arc::new(crate::storage::HybridStorage::with_policy(
        local,
        delayed_cloud.clone(),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    append_cloud_async_put(&mut el)?;
    let (segment_id, max_sequence) = seal_segment_without_remote_proof_for_test(&mut el)?;
    copy_local_segment_to_remote_wal_for_test(&el, segment_id);
    assert!(
        el.durability
            .cloud_durability_request_ids_at(segment_id)
            .is_empty(),
        "background seal must have no caller attached"
    );
    delayed_cloud.arm();

    // Act
    let started = Instant::now();
    el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
        segment_id,
        max_sequence,
    });
    let elapsed = started.elapsed();

    // Assert
    assert!(
        elapsed < Duration::from_millis(160),
        "callerless acknowledgement monopolized the event loop: {elapsed:?}"
    );
    assert!(el.state.persistence_anomaly_detected());
    assert_eq!(
        el.cloud_wal.upload_backlog.get(&segment_id),
        Some(&max_sequence),
        "bounded maintenance expiry must retain the accepted WAL for retry"
    );
    assert_eq!(el.state.wal.cloud_durable_seq, 0);
    Ok(())
}
