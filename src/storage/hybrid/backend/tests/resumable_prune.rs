use super::*;
use crate::metadata::Manifest;
use crate::runtime::hybrid_persistence::CloudWalPruneProgress;
use crate::storage::cloud::{CloudCallback, CloudError, CloudEvent};

struct LimitedRanges {
    inner: MockCloudBackend,
    allowance: AtomicUsize,
    reads: Mutex<Vec<(String, u64)>>,
    delay_micros: AtomicUsize,
}
impl CloudBackend for LimitedRanges {
    crate::storage::cloud::forward_cloud_backend!(inner; submit_put, submit_get, submit_get_with_metadata, submit_get_range, submit_delete, submit_list, submit_head);
    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: crate::storage::StorageObjectMetadata,
        timeout: Duration,
        callback: CloudCallback,
    ) {
        let delay = Duration::from_micros(self.delay_micros.load(Ordering::SeqCst) as u64);
        if !delay.is_zero() {
            std::thread::sleep(delay.min(timeout));
        }
        if delay >= timeout
            || self
                .allowance
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_err()
        {
            let _ = callback.send(CloudEvent::GetRange {
                key: key.into(),
                start,
                end: Some(end),
                result: Err(CloudError::Timeout(
                    "injected attempt range budget exhausted".into(),
                )),
            });
            return;
        }
        self.reads.lock().push((key.into(), start));
        self.inner
            .submit_get_range_with_identity(key, start, end, expected, timeout, callback);
    }
}

fn fixture(
    records: u64,
) -> (
    tempfile::TempDir,
    Arc<LimitedRanges>,
    HybridStorage,
    Manifest,
) {
    let directory = tempfile::tempdir().expect("directory");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(directory.path().join("local")).expect("local"),
    );
    let backend = Arc::new(LimitedRanges {
        inner: MockCloudBackend::new(),
        allowance: AtomicUsize::new(usize::MAX),
        reads: Mutex::new(Vec::new()),
        delay_micros: AtomicUsize::new(0),
    });
    let cloud = Arc::new(CloudStorage::new(backend.clone(), String::new()));
    let storage = HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    storage.enable_ephemeral_sst_cache(1024 * 1024);
    storage.fence_cloud_wal_catalog(2).expect("catalog");
    let value = (0..8192_u32)
        .map(|index| index.wrapping_mul(37).to_le_bytes()[0])
        .collect::<Vec<_>>();
    let mut wal = Vec::new();
    for sequence in 1..=records {
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::copy_from_slice(&value)),
            sequence,
            1,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode");
        crate::wal::frame::append_frame(&mut wal, &payload).expect("frame");
    }
    if records > 0 {
        write_authoritative_cloud_wal(&storage, 1, records, wal);
    }
    let sst = valid_sst_bytes(b"k", &value, records);
    let manifest =
        manifest_covering_wal("resumable.sst", &sst, records, Some(crc32c::crc32c(&sst)));
    write_cloud_object(&storage, &crate::sst::object_key("resumable.sst"), sst);
    backend.reads.lock().clear();
    (directory, backend, storage, manifest)
}

#[test]
fn should_finish_oldest_wal_proof_across_repeated_provider_timeouts() {
    // Arrange
    let (_directory, backend, storage, manifest) = fixture(80);
    let progress = CloudWalPruneProgress::default();
    let mut retired = false;
    // Act
    for _attempt in 0..100 {
        backend.allowance.store(16, Ordering::SeqCst);
        let guard = CloudWalPruneGuard::new(manifest.clone(), None)
            .with_memory_limit(256 * 1024)
            .with_progress(progress.clone());
        if storage
            .prune_cloud_wal_segment_within(
                1,
                80,
                guard,
                2,
                &crate::common::OperationDeadline::from_budget(Duration::from_secs(1)),
            )
            .is_ok()
        {
            retired = true;
            break;
        }
        assert!(
            assert_wal_catalog_copies_match(&storage)
                .segments
                .contains_key(&1),
            "incomplete proof cannot retire authority"
        );
    }
    // Assert
    assert!(
        retired,
        "bounded retries must preserve completed CRC and exact-record work"
    );
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
    assert!(!assert_wal_catalog_copies_match(&storage)
        .segments
        .contains_key(&1));
}

fn attempt(
    storage: &HybridStorage,
    manifest: &Manifest,
    progress: &CloudWalPruneProgress,
    sequence: u64,
) -> crate::MidgeResult<()> {
    storage.prune_cloud_wal_segment_within(
        1,
        sequence,
        CloudWalPruneGuard::new(manifest.clone(), None)
            .with_memory_limit(256 * 1024)
            .with_progress(progress.clone()),
        2,
        &crate::common::OperationDeadline::from_budget(Duration::from_secs(1)),
    )
}

#[test]
fn should_restart_crc_progress_when_wal_provider_identity_changes() {
    // Arrange
    let (_directory, backend, storage, manifest) = fixture(80);
    let progress = CloudWalPruneProgress::default();
    backend.allowance.store(3, Ordering::SeqCst);
    assert!(attempt(&storage, &manifest, &progress, 80).is_err());
    let key = crate::wal::cloud_segment::object_key(1, 1);
    let bytes = read_cloud_object(&storage, &key);
    write_cloud_object(&storage, &key, bytes);
    backend.reads.lock().clear();
    backend.allowance.store(1, Ordering::SeqCst);
    // Act
    let result = attempt(&storage, &manifest, &progress, 80);
    // Assert
    assert!(result.is_err());
    assert_eq!(backend.reads.lock().first(), Some(&(key, 0)));
    assert!(assert_wal_catalog_copies_match(&storage)
        .segments
        .contains_key(&1));
}

#[test]
fn should_restart_semantic_progress_when_sst_provider_identity_changes() {
    // Arrange
    let (_directory, backend, storage, manifest) = fixture(80);
    let progress = CloudWalPruneProgress::default();
    backend.allowance.store(80, Ordering::SeqCst);
    assert!(attempt(&storage, &manifest, &progress, 80).is_err());
    assert!(backend
        .reads
        .lock()
        .iter()
        .any(|(key, _)| key.contains("resumable.sst")));
    let key = crate::sst::object_key("resumable.sst");
    let bytes = read_cloud_object(&storage, &key);
    write_cloud_object(&storage, &key, bytes);
    backend.reads.lock().clear();
    backend.allowance.store(1, Ordering::SeqCst);
    // Act
    let result = attempt(&storage, &manifest, &progress, 80);
    // Assert
    assert!(result.is_err());
    assert_eq!(
        backend.reads.lock().first(),
        Some(&(crate::wal::cloud_segment::object_key(1, 1), 0))
    );
    assert!(assert_wal_catalog_copies_match(&storage)
        .segments
        .contains_key(&1));
}

#[test]
fn should_revalidate_from_start_when_process_local_proof_state_is_lost() {
    // Arrange
    let (_directory, backend, storage, manifest) = fixture(80);
    let progress = CloudWalPruneProgress::default();
    backend.allowance.store(3, Ordering::SeqCst);
    assert!(attempt(&storage, &manifest, &progress, 80).is_err());
    drop(progress);
    let restarted = CloudWalPruneProgress::default();
    backend.reads.lock().clear();
    backend.allowance.store(1, Ordering::SeqCst);
    // Act
    let result = attempt(&storage, &manifest, &restarted, 80);
    // Assert
    assert!(result.is_err());
    assert_eq!(
        backend.reads.lock().first(),
        Some(&(crate::wal::cloud_segment::object_key(1, 1), 0))
    );
    assert!(assert_wal_catalog_copies_match(&storage)
        .segments
        .contains_key(&1));
}

#[test]
fn should_resume_legacy_sst_summary_across_timeouts_with_many_versions_of_one_key() {
    // Arrange
    let (_directory, backend, storage, _) = fixture(1);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("writer");
    for sequence in (1..=100).rev() {
        writer
            .add_with_meta(b"k", Some(&vec![b'x'; 8192]), sequence, 0, None)
            .expect("historical version");
    }
    let bytes = writer.finish_bytes().expect("historical SST");
    let mut manifest = manifest_covering_wal("resumable.sst", &bytes, 100, None);
    manifest.files[0].smallest_seq = Some(1);
    write_cloud_object(&storage, &crate::sst::object_key("resumable.sst"), bytes);
    let progress = CloudWalPruneProgress::default();
    let mut retired = false;
    // Act
    for _attempt in 0..100 {
        backend.allowance.store(16, Ordering::SeqCst);
        if attempt(&storage, &manifest, &progress, 1).is_ok() {
            retired = true;
            break;
        }
        assert!(assert_wal_catalog_copies_match(&storage)
            .segments
            .contains_key(&1));
    }
    // Assert
    assert!(
        retired,
        "legacy summary and exact-version cursor must resume within their pinned SST"
    );
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
}

#[test]
fn should_finish_oldest_wal_proof_across_short_attempt_deadlines() {
    // Arrange
    let (_directory, backend, storage, manifest) = fixture(40);
    let progress = CloudWalPruneProgress::default();
    backend.delay_micros.store(2_000, Ordering::SeqCst);
    let mut attempts = 0;
    let mut retired = false;
    // Act
    for _attempt in 0..200 {
        attempts += 1;
        let guard = CloudWalPruneGuard::new(manifest.clone(), None)
            .with_memory_limit(256 * 1024)
            .with_progress(progress.clone());
        if storage
            .prune_cloud_wal_segment_within(
                1,
                40,
                guard,
                2,
                &crate::common::OperationDeadline::from_budget(Duration::from_millis(20)),
            )
            .is_ok()
        {
            retired = true;
            break;
        }
        assert!(assert_wal_catalog_copies_match(&storage)
            .segments
            .contains_key(&1));
    }
    // Assert
    assert!(attempts > 1);
    assert!(retired, "short deadlines must accumulate proven progress");
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
}

#[test]
fn should_resume_cross_family_transaction_proof_without_retiring_partial_coverage() {
    // Arrange
    let (_directory, backend, storage, _) = fixture(0);
    let mut records = Vec::new();
    for sequence in 2..=41 {
        let mut record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            sequence,
            1,
        );
        record.cf_id = u32::try_from(sequence % 2).expect("family");
        record.txn_id = Some(1);
        records.push(record);
    }
    let payload =
        crate::wal::encoding::encode_txn_batch_payload(1, 1, 42, 1, &records).expect("batch");
    let mut record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::TxnBatch,
        Bytes::new(),
        Some(payload),
        42,
        1,
    );
    record.txn_id = Some(1);
    let payload = crate::wal::encoding::encode(&record).expect("encode");
    let mut wal = Vec::new();
    crate::wal::frame::append_frame(&mut wal, &payload).expect("frame");
    write_authoritative_cloud_wal(&storage, 1, 42, wal);
    let mut manifest = Manifest::default();
    for (name, family, sequence) in [("first.sst", 0, 40), ("second.sst", 1, 41)] {
        let bytes = valid_sst_bytes(b"k", b"v", sequence);
        let mut file = manifest_covering_wal(name, &bytes, sequence, Some(crc32c::crc32c(&bytes)))
            .files
            .remove(0);
        file.cf_id = family;
        manifest.files.push(file);
        write_cloud_object(&storage, &crate::sst::object_key(name), bytes);
    }
    let progress = CloudWalPruneProgress::default();
    let mut retired = false;
    // Act
    for _attempt in 0..100 {
        backend.allowance.store(16, Ordering::SeqCst);
        if attempt(&storage, &manifest, &progress, 42).is_ok() {
            retired = true;
            break;
        }
        assert!(
            assert_wal_catalog_copies_match(&storage)
                .segments
                .contains_key(&1),
            "batch must remain authoritative until both CFs and all operations are covered"
        );
    }
    // Assert
    assert!(
        retired,
        "completed operations inside a large transaction must survive retries"
    );
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
}

#[test]
fn should_retain_catalog_authority_when_resumed_wal_identity_contains_corrupt_bytes() {
    // Arrange
    let (_directory, backend, storage, manifest) = fixture(80);
    let progress = CloudWalPruneProgress::default();
    backend.allowance.store(3, Ordering::SeqCst);
    assert!(attempt(&storage, &manifest, &progress, 80).is_err());
    let key = crate::wal::cloud_segment::object_key(1, 1);
    let mut bytes = read_cloud_object(&storage, &key);
    *bytes.last_mut().expect("WAL payload") ^= 1;
    write_cloud_object(&storage, &key, bytes);
    backend.allowance.store(usize::MAX, Ordering::SeqCst);
    // Act
    let result = attempt(&storage, &manifest, &progress, 80);
    // Assert
    assert!(
        matches!(result, Err(crate::MidgeError::Corruption(_))),
        "replacement must restart full checksum validation"
    );
    assert!(assert_wal_catalog_copies_match(&storage)
        .segments
        .contains_key(&1));
    assert_cloud_object_exists(&storage, &key);
}

#[test]
fn should_revalidate_exact_coverage_when_manifest_family_assignment_changes() {
    // Arrange
    let (_directory, backend, storage, mut manifest) = fixture(80);
    let progress = CloudWalPruneProgress::default();
    backend.allowance.store(80, Ordering::SeqCst);
    assert!(attempt(&storage, &manifest, &progress, 80).is_err());
    manifest.files[0].cf_id = 1;
    backend.reads.lock().clear();
    backend.allowance.store(usize::MAX, Ordering::SeqCst);
    // Act
    let result = attempt(&storage, &manifest, &progress, 80);
    // Assert
    assert!(
        result.is_err(),
        "another family's persisted value cannot cover the WAL"
    );
    assert_eq!(
        backend.reads.lock().first(),
        Some(&(crate::wal::cloud_segment::object_key(1, 1), 0))
    );
    assert!(assert_wal_catalog_copies_match(&storage)
        .segments
        .contains_key(&1));
}

#[test]
fn should_preserve_oldest_proof_progress_while_newer_ssts_are_appended() {
    // Arrange
    let (_directory, backend, storage, mut manifest) = fixture(80);
    let progress = CloudWalPruneProgress::default();
    let mut retired = false;
    // Act
    for index in 0..100_u64 {
        backend.allowance.store(16, Ordering::SeqCst);
        if attempt(&storage, &manifest, &progress, 80).is_ok() {
            retired = true;
            break;
        }
        assert!(assert_wal_catalog_copies_match(&storage)
            .segments
            .contains_key(&1));
        let sequence = 1000 + index;
        let name = format!("newer-{index}.sst");
        let bytes = valid_sst_bytes(b"z", b"later", sequence);
        let mut file = manifest_covering_wal(&name, &bytes, sequence, Some(crc32c::crc32c(&bytes)))
            .files
            .remove(0);
        file.smallest_key = Some(b"z".to_vec());
        file.largest_key = Some(b"z".to_vec());
        file.key_bounds_complete = true;
        manifest.files.push(file);
        write_cloud_object(&storage, &crate::sst::object_key(&name), bytes);
    }
    // Assert
    assert!(
        retired,
        "newer flush publications must not discard already-proven older frame coverage"
    );
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
}
