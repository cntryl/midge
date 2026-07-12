use super::super::tests::{create_test_cloud_event_loop, create_test_event_loop};
use super::super::EventLoop;
use crate::runtime::hybrid_persistence::HybridPersistence;
use crate::runtime::{state::RuntimeState, ResponseRouter, RuntimeMsg, RuntimeResponse};
use crate::sst::Memtable;
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

struct FailSecondIntentPutBackend {
    inner: Arc<crate::storage::cloud::MockCloudBackend>,
    intent_puts: AtomicUsize,
}

impl FailSecondIntentPutBackend {
    fn new(inner: Arc<crate::storage::cloud::MockCloudBackend>) -> Self {
        Self {
            inner,
            intent_puts: AtomicUsize::new(0),
        }
    }
}

impl crate::storage::cloud::CloudBackend for FailSecondIntentPutBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        if key.ends_with("metadata/intent_log.json")
            && self.intent_puts.fetch_add(1, Ordering::SeqCst) >= 1
        {
            let key = key.to_string();
            let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
                key,
                result: crate::storage::cloud::CloudOutcome::Err(
                    "injected intent metadata put failure".to_string(),
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

struct BlockingDeleteStorageBackend {
    inner: Arc<crate::storage::filesystem::FileSystem>,
    block_key: String,
    delete_started: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    release_delete: Arc<AtomicBool>,
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
        storage
            .verify_remote_wal_segment(seg_id, max_sequence)
            .expect("verify remote WAL for test CloudAck");
    }
    Ok((seg_id, max_sequence))
}

fn seal_segment_without_remote_proof_for_test(
    el: &mut EventLoop,
) -> crate::common::MidgeResult<(u64, u64)> {
    let seg_id = el.state.wal.current_segment_id;
    let max_sequence = el.wal_actor.flush_for_cloud_upload(&mut el.state)?;
    el.wal_actor.rotate(&mut el.state)?;
    el.durability.rotate_to(el.state.wal.current_segment_id);
    copy_local_segment_to_remote_wal_for_test(el, seg_id);
    el.wal_actor.complete_cloud_upload_seal(&mut el.state);
    el.durability
        .record_cloud_segment_inflight(seg_id, max_sequence);
    el.durability.record_cloud_flush();
    Ok((seg_id, max_sequence))
}

fn remote_wal_path_for_test(el: &EventLoop, segment_id: u64) -> PathBuf {
    el.state
        .db_path
        .join("cloud_store")
        .join("wal")
        .join(crate::wal::cloud_segment_file_name(segment_id))
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

fn seed_cloud_prune_candidate(el: &mut EventLoop, segment_id: u64, max_sequence: u64) {
    el.state.wal.current_segment_id = segment_id + 1;
    el.state.manifest.last_persisted_sequence = max_sequence;
    el.cloud_acked_wal_segments.insert(segment_id, max_sequence);
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"prune-candidate"),
        Some(Bytes::from_static(b"value")),
        max_sequence,
        0,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode test WAL record");
    let mut bytes = Vec::new();
    crate::wal::frame::append_frame(&mut bytes, &payload).expect("append test WAL frame");
    write_test_file(remote_wal_path_for_test(el, segment_id), &bytes);
    if let Some(storage) = el.hybrid_storage.as_ref() {
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("verify remote WAL prune candidate");
    }
}

fn seed_cloud_prune_candidate_with_records(
    el: &mut EventLoop,
    segment_id: u64,
    max_sequence: u64,
    records: Vec<crate::wal::WalRecord>,
) {
    el.state.wal.current_segment_id = segment_id + 1;
    el.state.manifest.last_persisted_sequence = max_sequence;
    el.cloud_acked_wal_segments.insert(segment_id, max_sequence);

    let mut bytes = Vec::new();
    for record in records {
        let payload = crate::wal::encoding::encode(&record).expect("encode test WAL record");
        crate::wal::frame::append_frame(&mut bytes, &payload).expect("append test WAL frame");
    }
    write_test_file(remote_wal_path_for_test(el, segment_id), &bytes);
    if let Some(storage) = el.hybrid_storage.as_ref() {
        storage
            .verify_remote_wal_segment(segment_id, max_sequence)
            .expect("verify remote WAL prune candidate");
    }
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
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        el.tick_hybrid_storage();
        if el.cloud_wal_prune_inflight.is_empty() {
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
    let event_loop = create_test_event_loop().expect("Should create event loop");

    // Act
    // Create a mock hybrid storage (we need to use a real one or skip this test)
    // For now, we'll skip detailed testing of set_hybrid_storage since it requires
    // complex setup with actual HybridStorage instance

    // Assert - Method exists and is callable
    drop(event_loop);
}

#[test]
fn should_support_hybrid_storage_optional_field() {
    // Arrange
    let event_loop = create_test_event_loop().expect("Should create event loop");

    // Act

    // Assert
    // hybrid_storage starts as None
    assert!(event_loop.hybrid_storage.is_none());
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
        el.cloud_acked_wal_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

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
        el.cloud_acked_wal_segments.contains_key(&segment_id),
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
        el.cloud_acked_wal_segments.contains_key(&segment_id),
        "retained WAL should remain eligible for a future conservative retry"
    );

    Ok(())
}

#[test]
fn should_converge_stale_intent_metadata_before_remote_wal_prune() -> crate::common::MidgeResult<()>
{
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
    let local_intent =
        std::fs::read(el.state.db_path.join("intent_log.json")).expect("read local intent log");
    metadata_backend.clear_history();

    el.prune_cloud_wal_segments_covered_by_manifest();
    drain_prune_completion_for_test(&mut el);

    // Act
    // Assert
    assert!(
        !remote_wal_path_for_test(&el, segment_id).exists(),
        "remote WAL should prune after stale intent metadata is mirrored and revalidated"
    );
    assert_eq!(
        get_cloud_metadata_for_test(&metadata_storage, "intent_log.json"),
        local_intent,
        "cloud intent metadata should converge to committed local metadata before WAL prune"
    );
    let proof = el
        .cloud_metadata_cleanup_proofs
        .get("intent_log.json")
        .expect("retry validation should refresh the intent metadata proof");
    assert_eq!(proof.len, local_intent.len() as u64);
    assert_eq!(proof.crc32c, crc32c::crc32c(&local_intent));
    assert!(
        metadata_backend
            .get_uploads()
            .iter()
            .any(|(key, _)| key.ends_with("metadata/intent_log.json")),
        "stale intent metadata should be repaired by an authoritative metadata mirror"
    );

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
        el.cloud_acked_wal_segments.contains_key(&segment_id),
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

    el.state
        .active_compactions
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let request_id = 4242;
    let response_rx = el.router.register(request_id);
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
    let failing_backend = Arc::new(FailSecondIntentPutBackend::new(metadata_backend));
    let metadata_storage = Arc::new(crate::storage::cloud::CloudStorage::new(
        failing_backend,
        "metadata-test".to_string(),
    ));
    el.cloud_metadata_storage = Some(Arc::clone(&metadata_storage));

    el.state
        .active_compactions
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let completion_request_id = 4343;
    let completion_rx = el.router.register(completion_request_id);
    let waiter_request_id = 4344;
    let waiter_rx = el.router.register(waiter_request_id);
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

    el.state
        .active_compactions
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let request_id = 4545;
    let response_rx = el.router.register(request_id);
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
    let response_rx = el.router.register(request_id);
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
        el.cloud_acked_wal_segments.contains_key(&segment_id),
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
        el.cloud_acked_wal_segments.contains_key(&segment_id),
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
        el.cloud_acked_wal_segments.contains_key(&segment_id),
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
        el.cloud_acked_wal_segments.contains_key(&segment_id),
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

    // Act
    // Assert
    assert!(
        el.cloud_wal_prune_inflight.is_empty(),
        "preflight failure must not leave prune inflight state"
    );
    assert!(
        el.cloud_acked_wal_segments.contains_key(&segment_id),
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
    assert_eq!(
        el.wal_actor.pending_cloud_writes_len(),
        2,
        "pending CloudAsync writes must not become visible until the gap is closed"
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
    assert_eq!(
        el.wal_actor.pending_cloud_writes_len(),
        0,
        "pending CloudAsync writes should drain after contiguous durability is proven"
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
        el.cloud_acked_wal_segments.contains_key(&second_segment),
        "later ack should be buffered while an earlier segment is unacked"
    );

    el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
        segment_id: first_segment,
        error: "injected upload failure".to_string(),
    });

    assert_eq!(
        el.state.wal.cloud_durable_seq, 0,
        "failure of the earlier segment must not let a buffered later ack advance durability"
    );
    assert!(
        el.cloud_acked_wal_segments.contains_key(&second_segment),
        "later verified ACK bookkeeping must remain buffered behind an earlier gap"
    );
    assert_eq!(
        el.wal_actor.pending_cloud_writes_len(),
        0,
        "pending CloudAsync writes should be cleared after cloud upload failure"
    );

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
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("establish remote WAL proof");
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
            || error.contains("disappeared"),
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
    assert_eq!(el.wal_actor.pending_cloud_writes_len(), 0);

    Ok(())
}

#[test]
fn should_cloud_async_fail_invalidates_idempotency_then_retry_allocates_new_seq(
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
    });

    // Retry the same request_id: since the previous allocation failed, we expect a new sequence
    let (seq2, deferred2) = el.wal_actor.append(
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

    // Assert: retry should allocate a new sequence and be deferred
    assert_ne!(
        seq1, seq2,
        "retry after cloud fail should allocate a new sequence"
    );
    assert!(
        deferred2,
        "retry should be deferred when retried after fail (CloudAsync)"
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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
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
        301,
        ops,
        Some(crate::wal::DurabilityPolicy::CloudAsync),
        None,
        crate::runtime::ConflictPolicy::LastWriteWins,
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
    assert!(
        el.has_actionable_work(),
        "failed seal should leave the runtime actionable for retry"
    );

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
        303,
        ops,
        Some(crate::wal::DurabilityPolicy::CloudAsync),
        None,
        crate::runtime::ConflictPolicy::LastWriteWins,
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

#[cfg(feature = "failpoints")]
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
        302,
        ops,
        Some(crate::wal::DurabilityPolicy::CloudAsync),
        None,
        crate::runtime::ConflictPolicy::LastWriteWins,
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
        .verify_remote_wal_segment(seg_id, last_sequence)
        .expect("verify retry remote WAL for test CloudAck");
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
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut el = create_test_cloud_event_loop(
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )?;
    let last_sequence = append_cloud_async_put(&mut el)?;

    let fail_request_id = 401u64;
    let fail_rx = el.router.register(fail_request_id);
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
    let retry_rx = el.router.register(retry_request_id);
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
