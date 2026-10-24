//! Corrupt bytes and changing identity must both fail admitted publication.

use super::*;
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudStorage, MockCloudBackend,
};
use crate::storage::cloud::{CloudHeadCapability, CloudReadCapability, CloudWriteCapability};
use crate::storage::StorageBackend;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Publish the file named `source` in `directory` through a host root.
fn publish_source(
    storage: &HybridStorage,
    key: &str,
    directory: &std::path::Path,
    bytes: &[u8],
    budget: &ResourceBudget,
) -> MidgeResult<super::super::GuardedObjectProof> {
    let fs = crate::io::RealFs::new(directory).map_err(FsError::into_midge)?;
    storage.publish_immutable_file(
        ImmutableFileSource {
            key,
            fs: &fs,
            path: &FsPath::new("source"),
            size: bytes.len() as u64,
            checksum: crc32c::crc32c(bytes),
        },
        budget,
    )
}

#[test]
fn should_leave_three_quarters_of_maintenance_pool_for_live_compaction_inputs() {
    // Arrange: the mock bounds no copies of its own.
    let storage = HybridStorage::with_policy(
        Arc::new(CloudStorage::with_mock()),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    let pool = 32 * 1024 * 1024;
    let variable = pool - FIXED_WORKSPACE;
    let copies = 1 + crate::storage::CONSERVATIVE_WRITE_PAYLOAD_COPIES;

    // Act
    let partition = storage.immutable_file_partition_target(pool);

    // Assert
    assert_eq!(partition, variable / (copies * 4));
    assert!(partition * copies <= variable / 4);
}

struct ChangingReadback {
    inner: MockCloudBackend,
    bytes: Vec<u8>,
    replace_identity: bool,
    injected: AtomicBool,
}

impl crate::storage::cloud::CloudReadCapability for ChangingReadback {
    crate::storage::cloud::unsupported_cloud_backend!(submit_get, submit_get_with_metadata);

    fn submit_get_range(
        &self,
        _key: &str,
        _start: u64,
        _end: Option<u64>,
        _callback: CloudCallback,
    ) {
        unreachable!("publication requires pinned ranges")
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

impl crate::storage::cloud::CloudWriteCapability for ChangingReadback {
    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.inner.submit_put(key, bytes, headers, callback);
    }
}

impl crate::storage::cloud::CloudDeleteCapability for ChangingReadback {
    crate::storage::cloud::unsupported_cloud_backend!(submit_delete);
}

impl crate::storage::cloud::CloudListCapability for ChangingReadback {
    crate::storage::cloud::unsupported_cloud_backend!(submit_list);
}

impl crate::storage::cloud::CloudHeadCapability for ChangingReadback {
    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}

impl CloudBackend for ChangingReadback {}

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
            Arc::new(CloudStorage::new(backend.clone(), String::new())),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        storage.enable_ephemeral_sst_cache(1024 * 1024);
        let budget = ResourceBudget::new(2 * 1024 * 1024);

        // Act
        let result = publish_source(&storage, "sst/object", directory.path(), &bytes, &budget);
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

impl crate::storage::cloud::CloudReadCapability for HeldReadback {
    crate::storage::cloud::unsupported_cloud_backend!(submit_get, submit_get_with_metadata);

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

impl crate::storage::cloud::CloudWriteCapability for HeldReadback {
    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.inner.submit_put(key, bytes, headers, callback);
    }
}

impl crate::storage::cloud::CloudDeleteCapability for HeldReadback {
    crate::storage::cloud::unsupported_cloud_backend!(submit_delete);
}

impl crate::storage::cloud::CloudListCapability for HeldReadback {
    crate::storage::cloud::unsupported_cloud_backend!(submit_list);
}

impl crate::storage::cloud::CloudHeadCapability for HeldReadback {
    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}

impl CloudBackend for HeldReadback {}

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
        cloud.clone(),
        cloud.clone(),
        cloud,
        tx,
        Duration::from_millis(100),
    );
    storage.enable_ephemeral_sst_cache(1024 * 1024);
    let budget = ResourceBudget::new(2 * 1024 * 1024);

    // Act: the real cloud callback adapter expires while the provider owns readback.
    let result = publish_source(&storage, key, directory.path(), &bytes, &budget);
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

#[derive(Clone, Copy)]
enum ReceiptFault {
    None,
    WrongChecksum,
    Missing,
    TransientBeforeWrite,
    LostResponseAfterWrite,
}

struct ReceiptPublication {
    inner: MockCloudBackend,
    fault: ReceiptFault,
    attempts: std::sync::atomic::AtomicUsize,
}

impl CloudReadCapability for ReceiptPublication {
    crate::storage::cloud::forward_cloud_backend!(inner;
        submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity);
}

impl CloudWriteCapability for ReceiptPublication {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_put);

    fn write_with_receipt(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: crate::storage::cloud::CloudRequestOptions,
        callback: CloudCallback,
    ) {
        use crate::storage::cloud::CloudError;
        let first = self.attempts.fetch_add(1, Ordering::AcqRel) == 0;
        if first && matches!(self.fault, ReceiptFault::TransientBeforeWrite) {
            callback
                .send(CloudEvent::PutReceipt {
                    key: key.into(),
                    result: Err(CloudError::ServerError("injected 503".into())),
                })
                .unwrap();
            return;
        }
        let checksum = crc32c::crc32c(&bytes);
        let (tx, rx) = mpsc::channel();
        self.inner.submit_put(key, bytes, options.headers, tx);
        assert!(matches!(
            rx.recv().unwrap(),
            CloudEvent::Put { result: Ok(()), .. }
        ));
        if first && matches!(self.fault, ReceiptFault::LostResponseAfterWrite) {
            callback
                .send(CloudEvent::PutReceipt {
                    key: key.into(),
                    result: Err(CloudError::Transport("injected lost response".into())),
                })
                .unwrap();
            return;
        }
        let (tx, rx) = mpsc::channel();
        self.inner.submit_head(key, tx);
        let CloudEvent::Head {
            result: Ok(metadata),
            ..
        } = rx.recv().unwrap()
        else {
            panic!("written object must have an identity");
        };
        let receipt = (!matches!(self.fault, ReceiptFault::Missing)).then_some(
            crate::storage::StorageWriteReceipt {
                metadata,
                content_crc32c: checksum
                    ^ u32::from(matches!(self.fault, ReceiptFault::WrongChecksum)),
            },
        );
        callback
            .send(CloudEvent::PutReceipt {
                key: key.into(),
                result: Ok(receipt),
            })
            .unwrap();
    }
}

impl crate::storage::cloud::CloudDeleteCapability for ReceiptPublication {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_delete);
}

impl crate::storage::cloud::CloudListCapability for ReceiptPublication {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_list);
}

impl CloudHeadCapability for ReceiptPublication {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_head);
}

impl CloudBackend for ReceiptPublication {}

#[test]
fn should_validate_receipts_and_reconcile_retries_when_publishing_immutable_files(
) -> MidgeResult<()> {
    for fault in [
        ReceiptFault::None,
        ReceiptFault::WrongChecksum,
        ReceiptFault::Missing,
        ReceiptFault::TransientBeforeWrite,
        ReceiptFault::LostResponseAfterWrite,
    ] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let bytes = vec![7; 256 * 1024];
        std::fs::write(directory.path().join("source"), &bytes)?;
        let backend = Arc::new(ReceiptPublication {
            inner: MockCloudBackend::new(),
            fault,
            attempts: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut storage = HybridStorage::with_policy(
            Arc::new(CloudStorage::new(backend.clone(), String::new())),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        storage.set_immutable_retry_policy(crate::storage::cloud::CloudRetryPolicy {
            max_retries: 2,
            base_delay: Duration::ZERO,
        });
        let budget = ResourceBudget::new(4 * 1024 * 1024);

        // Act
        let result = publish_source(&storage, "sst/receipt", directory.path(), &bytes, &budget);

        // Assert
        if matches!(fault, ReceiptFault::WrongChecksum) {
            assert!(matches!(result, Err(MidgeError::Corruption(_))));
        } else {
            assert!(result.is_ok(), "{:?}", result.err());
        }
        assert_eq!(backend.inner.get_uploads().len(), 1);
        let readback = matches!(
            fault,
            ReceiptFault::Missing | ReceiptFault::LostResponseAfterWrite
        );
        assert_eq!(!backend.inner.get_range_downloads().is_empty(), readback);
        let attempts = 1 + usize::from(matches!(fault, ReceiptFault::TransientBeforeWrite));
        assert_eq!(backend.attempts.load(Ordering::Acquire), attempts);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}
