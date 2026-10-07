//! Corrupt bytes and changing identity must both fail admitted publication.

use super::*;
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudStorage, MockCloudBackend,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

struct PanicLocalBackend;

impl StorageBackend for PanicLocalBackend {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        let _ = (request, range, callback);
        panic!("test backend received undeclared range-read capability");
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, callback);
        panic!("ephemeral lookup called the retired local backend");
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared metadata-read capability");
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, callback);
        panic!("test backend received undeclared HEAD capability");
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, callback);
        panic!("ephemeral deletion called the retired local backend");
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        let _ = (request, data, callback);
        panic!("ephemeral publication called the retired local backend");
    }
}

#[test]
fn should_not_call_local_backend_when_publishing_with_ephemeral_cache() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("source");
    let bytes = b"immutable bytes";
    std::fs::write(&path, bytes)?;
    let storage = HybridStorage::with_policy(
        Arc::new(PanicLocalBackend),
        Arc::new(CloudStorage::new(
            Arc::new(MockCloudBackend::new()),
            String::new(),
        )),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    storage.enable_ephemeral_sst_cache(1024 * 1024);
    // Startup has already swept any legacy copy before runtime publication.
    storage.retire_legacy_local_store();
    let budget = ResourceBudget::new(2 * 1024 * 1024);

    // Act
    storage.publish_immutable_file(
        "sst/retired.sst",
        &path,
        bytes.len() as u64,
        crc32c::crc32c(bytes),
        &budget,
    )?;
    assert!(storage.local_object_cache_is_absent("sst/retired.sst")?);
    storage.evict_local_object_cache("sst/retired.sst")?;
    storage.delete_immutable_object_blocking("sst/retired.sst")?;

    // Assert: the local backend panics on every operation.
    Ok(())
}

#[test]
fn should_leave_three_quarters_of_maintenance_pool_for_live_compaction_inputs() {
    // Arrange
    let pool = 32 * 1024 * 1024;
    let variable = pool - FIXED_WORKSPACE;

    // Act
    let partition = HybridStorage::immutable_file_partition_target(pool);

    // Assert
    assert_eq!(partition, variable / (COPY_FACTOR * 4));
    assert!(partition * COPY_FACTOR <= variable / 4);
}

struct ChangingReadback {
    inner: MockCloudBackend,
    bytes: Vec<u8>,
    replace_identity: bool,
    injected: AtomicBool,
}

impl CloudBackend for ChangingReadback {
    crate::storage::cloud::unsupported_cloud_backend!(
        submit_get,
        submit_get_with_metadata,
        submit_delete,
        submit_list,
    );

    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.inner.submit_put(key, bytes, headers, callback);
    }

    fn submit_get_range(
        &self,
        _key: &str,
        _start: u64,
        _end: Option<u64>,
        _callback: CloudCallback,
    ) {
        unreachable!("publication requires pinned ranges")
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }

    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: StorageObjectMetadata,
        timeout: Duration,
        callback: CloudCallback,
    ) {
        let (tx, rx) = mpsc::channel();
        self.inner
            .submit_get_range_with_identity(key, start, end, expected, timeout, tx);
        let mut event = rx.recv().unwrap();
        if !self.injected.swap(true, Ordering::AcqRel) {
            if self.replace_identity {
                let (tx, rx) = mpsc::channel();
                self.inner
                    .submit_put(key, self.bytes.clone(), Vec::new(), tx);
                rx.recv().unwrap();
            } else if let CloudEvent::GetRange {
                result: Ok(bytes), ..
            } = &mut event
            {
                bytes[0] ^= 1;
            }
        }
        callback.send(event).unwrap();
    }
}

#[test]
fn should_reject_admitted_publication_when_readback_bytes_or_identity_changes() -> MidgeResult<()> {
    for (replace_identity, kib) in [(false, 32), (false, 130), (true, 32), (true, 130)] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("source");
        let bytes = vec![7; kib * 1024];
        std::fs::write(&path, &bytes)?;
        let backend = Arc::new(ChangingReadback {
            inner: MockCloudBackend::new(),
            bytes: bytes.clone(),
            replace_identity,
            injected: AtomicBool::new(false),
        });
        let storage = HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(
                directory.path().join("local"),
            )?),
            Arc::new(CloudStorage::new(backend.clone(), String::new())),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        storage.enable_ephemeral_sst_cache(1024 * 1024);
        let budget = ResourceBudget::new(2 * 1024 * 1024);

        // Act
        let result = storage.publish_immutable_file(
            "sst/object",
            &path,
            bytes.len() as u64,
            crc32c::crc32c(&bytes),
            &budget,
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while budget.used() != 0 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }

        // Assert
        assert!(
            result.is_err(),
            "changed bytes or identity cannot establish publication"
        );
        assert!(backend.injected.load(Ordering::Acquire));
        assert_eq!(std::fs::read(&path)?, bytes, "retain recoverable source");
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

struct HeldReadbackCompletion {
    event: CloudEvent,
    callback: CloudCallback,
    reservation: Option<Arc<ResourceReservation>>,
}

struct HeldReadback {
    inner: MockCloudBackend,
    pending: parking_lot::Mutex<Option<HeldReadbackCompletion>>,
}

impl CloudBackend for HeldReadback {
    crate::storage::cloud::unsupported_cloud_backend!(
        submit_get,
        submit_get_with_metadata,
        submit_delete,
        submit_list,
    );

    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.inner.submit_put(key, bytes, headers, callback);
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }

    fn submit_get_range(
        &self,
        _key: &str,
        _start: u64,
        _end: Option<u64>,
        _callback: CloudCallback,
    ) {
        unreachable!("publication must use identity-pinned readback");
    }

    fn submit_get_range_with_reservation(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        expected: StorageObjectMetadata,
        timeout: Duration,
        reservation: Option<Arc<ResourceReservation>>,
        callback: CloudCallback,
    ) {
        let (tx, rx) = mpsc::channel();
        self.inner.submit_get_range_with_identity(
            key,
            range.start,
            range.end,
            expected,
            timeout,
            tx,
        );
        *self.pending.lock() = Some(HeldReadbackCompletion {
            event: rx.recv().unwrap(),
            callback,
            reservation,
        });
    }
}

#[test]
fn should_preserve_timeout_when_admitted_pinned_range_readback_exhausts_deadline() -> MidgeResult<()>
{
    // Arrange: actual HEAD and pinned range bytes are valid; only delivery is held.
    #[cfg(feature = "failpoints")]
    let _failpoint_guard = crate::failpoints::test_failpoint_guard();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("source");
    let key = "sst/object";
    let bytes = vec![7; READBACK_BYTES];
    let size = u64::try_from(bytes.len()).unwrap();
    std::fs::write(&path, &bytes)?;
    let backend = Arc::new(HeldReadback {
        inner: MockCloudBackend::new(),
        pending: parking_lot::Mutex::new(None),
    });
    let (tx, rx) = mpsc::channel();
    backend.inner.submit_put(key, bytes.clone(), Vec::new(), tx);
    assert!(matches!(
        rx.recv().unwrap(),
        CloudEvent::Put { result: Ok(()), .. }
    ));
    backend.inner.clear_history();
    let (tx, rx) = mpsc::channel();
    backend.inner.submit_head(key, tx);
    let original_head = rx.recv().unwrap();
    let cloud: Arc<dyn StorageBackend> =
        Arc::new(CloudStorage::new(backend.clone(), String::new()));
    let (tx, _rx) = crossbeam::channel::unbounded();
    let storage = HybridStorage::new_with_class_stores_and_event_sender(
        Arc::new(PanicLocalBackend),
        cloud.clone(),
        cloud.clone(),
        cloud,
        tx,
        Duration::from_millis(100),
    );
    storage.enable_ephemeral_sst_cache(1024 * 1024);
    storage.retire_legacy_local_store();
    let budget = ResourceBudget::new(2 * 1024 * 1024);

    // Act: the real cloud callback adapter expires while the provider owns readback.
    let result = storage.publish_immutable_file(key, &path, size, crc32c::crc32c(&bytes), &budget);
    let charge_while_held = budget.used();
    let completion = backend.pending.lock().take();
    let valid_range = completion.as_ref().is_some_and(|held| {
        matches!(&held.event, CloudEvent::GetRange {
            key: actual_key, start: 0, end: Some(end), result: Ok(actual),
        } if actual_key == key && *end == size && actual == &bytes)
    });
    let retained_reservation = completion
        .as_ref()
        .is_some_and(|held| held.reservation.is_some());
    // Release every accepted payload before asserting even on the red baseline.
    if let Some(HeldReadbackCompletion {
        event,
        callback,
        reservation,
    }) = completion
    {
        drop(callback.send(event));
        drop(reservation);
    }
    let (tx, rx) = mpsc::channel();
    backend.inner.submit_get(key, tx);
    let remote_bytes = rx.recv().unwrap();
    let (tx, rx) = mpsc::channel();
    backend.inner.submit_head(key, tx);
    let unchanged_identity = matches!((original_head, rx.recv().unwrap()), (
        CloudEvent::Head { result: Ok(before), .. },
        CloudEvent::Head { result: Ok(after), .. },
    ) if before.same_version(&after) && before.size == size);

    // Assert: typed timeout cannot install proof, overwrite bytes, or release live charges.
    assert!(
        valid_range,
        "a successful actual identity-pinned range must be held"
    );
    assert!(retained_reservation);
    assert!(
        charge_while_held > 0,
        "accepted provider readback owns its workspace"
    );
    assert_eq!(backend.inner.get_range_downloads(), vec![key.to_string()]);
    assert_eq!(backend.inner.get_uploads(), [] as [(String, u64); 0]);
    assert!(matches!(remote_bytes, CloudEvent::Get { result: Ok(actual), .. } if actual == bytes));
    assert!(unchanged_identity);
    assert_eq!(std::fs::read(&path)?, bytes);
    assert_eq!(budget.used(), 0);
    assert!(
        matches!(&result, Err(MidgeError::Timeout(_))),
        "{:?}",
        result.as_ref().err()
    );
    Ok(())
}
