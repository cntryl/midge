//! Corrupt bytes and changing identity must both fail admitted publication.

use super::*;
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudStorage, MockCloudBackend,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

struct PanicLocalBackend;

impl StorageBackend for PanicLocalBackend {
    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        crate::storage::test_support::forward_typed_metadata_read_to_legacy(
            self, request, callback,
        );
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_head_to_legacy(self, request, callback);
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_delete_to_legacy(self, request, callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_write_to_legacy(self, request, data, callback);
    }

    fn submit_write(&self, _key: &str, _data: Vec<u8>, _callback: crate::storage::StorageCallback) {
        panic!("ephemeral publication called the retired local backend");
    }

    fn submit_delete(&self, _key: &str, _callback: crate::storage::StorageCallback) {
        panic!("ephemeral deletion called the retired local backend");
    }

    fn submit_range_head(
        &self,
        _key: &str,
        _timeout: Duration,
        _callback: crate::storage::StorageCallback,
    ) {
        panic!("ephemeral lookup called the retired local backend");
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
