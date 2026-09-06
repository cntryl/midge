//! Corrupt bytes and changing identity must both fail admitted publication.

use super::*;
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudStorage, MockCloudBackend,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

struct ChangingReadback {
    inner: MockCloudBackend,
    bytes: Vec<u8>,
    replace_identity: bool,
    injected: AtomicBool,
}

impl CloudBackend for ChangingReadback {
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
    for replace_identity in [false, true] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("source");
        let bytes = vec![7; 130 * 1024];
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
