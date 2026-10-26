use super::*;
use crate::lease::PrimaryLease;
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudOutcome, CloudRequestOptions, MockCloudBackend,
};
use crate::storage::cloud::{CloudReadCapability, CloudWriteCapability};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

type RequestHeaders = Vec<(String, String)>;
type PutRequest = (String, RequestHeaders);
type PutRequestHistory = std::sync::Mutex<Vec<PutRequest>>;

struct ReceiptBackend {
    inner: MockCloudBackend,
    reads: AtomicUsize,
    writes: AtomicUsize,
    generations: std::sync::Mutex<HashMap<String, u64>>,
    put_requests: PutRequestHistory,
}

impl CloudReadCapability for ReceiptBackend {
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.submit_get(key, callback);
    }
    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.submit_get_with_metadata(key, callback);
    }
    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: crate::storage::StorageObjectMetadata,
        timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner
            .submit_get_range_with_identity(key, start, end, expected, timeout, callback);
    }
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.submit_get_range(key, start, end, callback);
    }
}

impl CloudWriteCapability for ReceiptBackend {
    fn submit_put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.put_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((key.to_string(), headers.clone()));
        let (tx, rx) = std::sync::mpsc::channel();
        self.inner.submit_put(key, bytes, headers, tx);
        let event = rx.recv().expect("mock PUT completion");
        match event {
            CloudEvent::Put {
                result: CloudOutcome::Ok(()),
                ..
            } => {
                let mut generations = self
                    .generations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let generation = generations.entry(key.to_string()).or_default();
                *generation = generation.saturating_add(1);
                let _ = callback.send(CloudEvent::Put {
                    key: key.into(),
                    result: CloudOutcome::Ok(()),
                });
            }
            event => {
                let _ = callback.send(event);
            }
        }
    }
    fn write_with_receipt(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_put(key, bytes.clone(), options.headers_with_deadline(), tx);
        match rx.recv().expect("mock PUT completion") {
            CloudEvent::Put {
                result: CloudOutcome::Ok(()),
                ..
            } => {
                let generations = self
                    .generations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let generation = generations
                    .get(key)
                    .copied()
                    .expect("successful mock PUT has a generation");
                let _ = callback.send(CloudEvent::PutReceipt {
                    key: key.into(),
                    result: CloudOutcome::Ok(Some(crate::storage::StorageWriteReceipt {
                        metadata: crate::storage::StorageObjectMetadata {
                            size: bytes.len() as u64,
                            etag: format!("mock-gen-{generation}"),
                            generation: None,
                        },
                        content_crc32c: crc32c::crc32c(&bytes),
                    })),
                });
            }
            event => {
                let _ = callback.send(event);
            }
        }
    }
}
impl crate::storage::cloud::CloudDeleteCapability for ReceiptBackend {
    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.submit_delete(key, headers, callback);
    }
}
impl crate::storage::cloud::CloudListCapability for ReceiptBackend {
    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.submit_list(prefix, callback);
    }
}
impl crate::storage::cloud::CloudHeadCapability for ReceiptBackend {
    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.submit_head(key, callback);
    }
}
impl CloudBackend for ReceiptBackend {}

fn assert_metadata_publication_request_history(backend: &ReceiptBackend) {
    assert_eq!(backend.writes.load(Ordering::Relaxed), 2);
    assert_eq!(backend.reads.load(Ordering::Relaxed), 0);
    let put_requests = backend
        .put_requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(put_requests.len(), 2);
    let edit_put = put_requests
        .iter()
        .find(|(key, _)| key.starts_with("metadata-cache/manifest/"))
        .expect("one immutable manifest edit PUT");
    assert!(edit_put
        .1
        .iter()
        .any(|(name, value)| { name.eq_ignore_ascii_case("if-none-match") && value == "*" }));
    let pointer_cas = put_requests
        .iter()
        .find(|(key, _)| key == "metadata-cache/metadata/committed-pointer.v1")
        .expect("one committed-pointer CAS");
    assert!(pointer_cas.1.iter().any(|(name, _)| {
        name.eq_ignore_ascii_case("if-match")
            || name.eq_ignore_ascii_case("x-goog-if-generation-match")
    }));
    assert!(
        backend.inner.get_downloads().is_empty(),
        "mock transport observed unexpected GETs: {:?}",
        backend.inner.get_downloads()
    );
    assert!(
        backend.inner.get_range_downloads().is_empty(),
        "mock transport observed unexpected range GETs: {:?}",
        backend.inner.get_range_downloads()
    );
}

#[test]
fn should_bound_changed_metadata_publication_to_two_requests_when_receipts_are_cached() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(ReceiptBackend {
        inner: MockCloudBackend::new(),
        reads: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
        generations: std::sync::Mutex::new(HashMap::new()),
        put_requests: std::sync::Mutex::new(Vec::new()),
    });
    let cloud = Arc::new(CloudStorage::new(backend.clone(), "metadata-cache".into()));
    let lease = Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
        crate::lease::CloudLeaseConfig {
            bucket: "midge".into(),
            prefix: "metadata-cache".into(),
        },
        directory.path().to_path_buf(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease).try_acquire().unwrap();
    let store = lease.get_leader_store().unwrap();
    let fs = crate::io::real::test_root(directory.path());
    crate::metadata::ensure_or_create_format_marker(&fs, directory.path()).unwrap();
    ManifestPersistence::save_with_fs(&fs, &Manifest::default()).unwrap();
    let lock = crate::runtime::MetadataPublicationLock::default();
    let mirror = || {
        mirror_control_metadata_within(
            CloudMetadataMirrorContext {
                cloud: &cloud,
                fs: fs.as_ref(),
                publication_lock: &lock,
                lock_wait_budget: std::time::Duration::from_secs(5),
                deadline: &OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                authority: CloudMetadataMirrorAuthority {
                    store: store.as_ref(),
                    holder_id: &lease.holder_id(),
                    writer_epoch: lease.epoch(),
                },
            },
            |_| Ok(()),
        )
    };
    mirror().unwrap();
    crate::runtime::IntentPersistence::save_with_fs(
        &fs,
        &[crate::runtime::IntentLogEntry::SeqnoAllocated {
            seqno: 11,
            cf_id: 0,
        }],
    )
    .unwrap();
    backend.inner.clear_history();
    backend.reads.store(0, Ordering::Relaxed);
    backend.writes.store(0, Ordering::Relaxed);
    backend
        .put_requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();

    // Act
    mirror().unwrap();

    // Assert: count provider calls, then independently read the selected chain.
    assert_metadata_publication_request_history(&backend);
    let CloudMetadataHead::Committed(generation) = store
        .read_committed_metadata(std::time::Duration::from_secs(5))
        .unwrap()
    else {
        panic!("committed metadata")
    };
    let bodies = read_committed_metadata_bodies_within(
        &cloud,
        &generation,
        &OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
    )
    .unwrap();
    assert_eq!(
        bodies[files::INTENT_LOG],
        crate::io::read_all(fs.as_ref(), &FsPath::new(files::INTENT_LOG)).unwrap()
    );
}
