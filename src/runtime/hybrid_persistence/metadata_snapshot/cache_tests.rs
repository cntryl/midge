use super::*;
use crate::lease::PrimaryLease;
use crate::storage::cloud::{
    CloudBackend, CloudCallback, CloudEvent, CloudOutcome, CloudRequestOptions, MockCloudBackend,
};
use crate::storage::cloud::{CloudReadCapability, CloudWriteCapability};
use std::sync::atomic::{AtomicUsize, Ordering};

struct ReceiptBackend {
    inner: MockCloudBackend,
    reads: AtomicUsize,
    writes: AtomicUsize,
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
    crate::storage::cloud::forward_cloud_backend!(inner; submit_get_range);
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
        self.inner.submit_put(key, bytes, headers, callback);
    }
    fn write_with_receipt(
        &self,
        key: &str,
        bytes: Vec<u8>,
        options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = std::sync::mpsc::channel();
        self.inner
            .submit_put(key, bytes.clone(), options.headers_with_deadline(), tx);
        match rx.recv().expect("mock PUT completion") {
            CloudEvent::Put {
                result: CloudOutcome::Ok(()),
                ..
            } => {
                // The mock's in-memory state lookup supplies the receipt for
                // this logical PUT; it is not another transport request.
                let (tx, rx) = std::sync::mpsc::channel();
                self.inner.submit_get_with_metadata(key, tx);
                let CloudEvent::GetWithMetadata {
                    result: CloudOutcome::Ok((actual, metadata)),
                    ..
                } = rx.recv().expect("mock receipt state")
                else {
                    panic!("mock receipt object")
                };
                assert_eq!(actual, bytes);
                let _ = callback.send(CloudEvent::PutReceipt {
                    key: key.into(),
                    result: CloudOutcome::Ok(Some(crate::storage::StorageWriteReceipt {
                        metadata,
                        content_crc32c: crc32c::crc32c(&actual),
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

#[test]
fn should_bound_changed_metadata_publication_to_two_requests_when_receipts_are_cached() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(ReceiptBackend {
        inner: MockCloudBackend::new(),
        reads: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
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
    backend.reads.store(0, Ordering::Relaxed);
    backend.writes.store(0, Ordering::Relaxed);

    // Act
    mirror().unwrap();

    // Assert: count calls at the provider boundary after first-publication
    // admission, then independently read the actual selected immutable chain.
    assert_eq!(backend.writes.load(Ordering::Relaxed), 2);
    assert_eq!(backend.reads.load(Ordering::Relaxed), 0);
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
