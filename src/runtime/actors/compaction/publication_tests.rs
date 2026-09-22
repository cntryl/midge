//! Exercise real compaction publication, provider ownership and installation proofs.

use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::sst::SstReader;
use crate::types::EntryType;

#[test]
fn should_retain_compaction_partition_when_upload_workspace_cannot_be_admitted() -> MidgeResult<()>
{
    // Arrange
    #[cfg(feature = "failpoints")]
    let _failpoint_guard = crate::failpoints::test_failpoint_guard();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("partition.sst");
    let factory =
        crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::RealFs::new(directory.path())?), 4096)
            .with_compression_policy(crate::codec::CompressionPolicy::Fixed(
                crate::codec::CompressionAlgo::None,
            ));
    let mut writer = factory.create()?;
    for key in 0_u64..64 {
        writer.add_with_meta(
            &key.to_be_bytes(),
            Some(&vec![7; 4096]),
            key + 1,
            EntryType::Put,
            None,
        )?;
    }
    crate::sst::fs::finish_writer_to_path(writer, &path)?;
    let original = std::fs::read(&path)?;
    let cloud_path = directory.path().join("cloud");
    let hybrid = crate::storage::HybridStorage::with_policy(
        Arc::new(crate::storage::filesystem::FileSystem::new(
            directory.path().join("local"),
        )?),
        Arc::new(crate::storage::filesystem::FileSystem::new(&cloud_path)?),
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    hybrid.enable_ephemeral_sst_cache(1024 * 1024);
    let prepared = PreparedCompactionOutputs::default();
    let budget = ResourceBudget::new(1024 * 1024);
    let name = "000000_01_00000000000000000002.sst";

    // Act
    let result =
        record_staged_output_partition(Some(&hybrid), &prepared, 0, 1, name, &path, &budget);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::ResourceLimit(_))),
        "upload copies must be admitted before publication"
    );
    assert_eq!(std::fs::read(path)?, original);
    assert!(prepared.lock().is_empty());
    assert!(!cloud_path
        .join(crate::cloud_layout::object_key(name))
        .exists());
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[derive(Default)]
struct PendingUpload {
    inner: crate::storage::cloud::MockCloudBackend,
    head_delay: std::time::Duration,
    pending: parking_lot::Mutex<Option<(Vec<u8>, crate::storage::cloud::CloudCallback)>>,
}

impl crate::storage::cloud::CloudBackend for PendingUpload {
    crate::storage::cloud::unsupported_cloud_backend!(submit_get, submit_delete, submit_list);

    fn submit_put(
        &self,
        _key: &str,
        data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        *self.pending.lock() = Some((data, callback));
    }
    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        std::thread::sleep(self.head_delay);
        self.inner.submit_head(key, callback);
    }
    fn submit_get_with_metadata(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        self.inner.submit_get_with_metadata(key, callback);
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
}

#[test]
fn should_retain_compaction_upload_charge_after_timeout_until_provider_releases_body(
) -> MidgeResult<()> {
    // Arrange
    #[cfg(feature = "failpoints")]
    let _failpoint_guard = crate::failpoints::test_failpoint_guard();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("partition.sst");
    let factory =
        crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::RealFs::new(directory.path())?), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"retained value"), 1, EntryType::Put, None)?;
    crate::sst::fs::finish_writer_to_path(writer, &path)?;
    let backend = Arc::new(PendingUpload::default());
    let cloud: Arc<dyn crate::storage::StorageBackend> = Arc::new(
        crate::storage::cloud::CloudStorage::new(backend.clone(), String::new()),
    );
    let (tx, _rx) = crossbeam::channel::unbounded();
    let hybrid = crate::storage::HybridStorage::new_with_class_stores_and_event_sender(
        Arc::new(crate::storage::filesystem::FileSystem::new(
            directory.path().join("local"),
        )?),
        cloud.clone(),
        cloud.clone(),
        cloud,
        tx,
        std::time::Duration::from_millis(200),
    );
    hybrid.enable_ephemeral_sst_cache(1024 * 1024);
    let budget = ResourceBudget::new(2 * 1024 * 1024);
    let prepared = PreparedCompactionOutputs::default();

    // Act
    let result = record_staged_output_partition(
        Some(&hybrid),
        &prepared,
        0,
        1,
        "000000_01_00000000000000000002.sst",
        &path,
        &budget,
    );
    let retained = budget.used();
    let pending = backend.pending.lock().take().expect("provider owns upload");
    let payload_bytes = pending.0.len();
    drop(pending);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while budget.used() != 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }

    // Assert
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert!(
        retained >= payload_bytes,
        "provider-owned memory cannot become uncharged after caller timeout"
    );
    assert!(path.exists());
    assert!(prepared.lock().is_empty());
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn should_roll_over_remote_compaction_outputs_to_leave_room_for_upload_workspace() -> MidgeResult<()>
{
    for ephemeral in [true, false] {
        // Arrange
        #[cfg(feature = "failpoints")]
        let _failpoint_guard = crate::failpoints::test_failpoint_guard();
        let directory = tempfile::tempdir()?;
        let factory = crate::sst::FsSstFactoryIo::new(
            Arc::new(crate::io::RealFs::new(directory.path())?),
            4096,
        )
        .with_compression_policy(crate::codec::CompressionPolicy::Fixed(
            crate::codec::CompressionAlgo::None,
        ));
        let mut writer = factory.create()?;
        for key in 0_u64..64 {
            writer.add_with_meta(
                &key.to_be_bytes(),
                Some(&vec![7; 4096]),
                key + 1,
                EntryType::Put,
                None,
            )?;
        }
        crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("input.sst"))?;
        let cloud_path = directory.path().join("cloud");
        let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(
                directory.path().join("local"),
            )?),
            Arc::new(crate::storage::filesystem::FileSystem::new(&cloud_path)?),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        if ephemeral {
            hybrid.enable_ephemeral_sst_cache(1024 * 1024);
        }
        let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1).with_output_seq(2);
        plan.input_files.push("input.sst".into());
        plan.compaction_memory_limit = 1024 * 1024;
        plan.target_sst_size = 1024 * 1024;
        let prepared = PreparedCompactionOutputs::default();
        let compaction_storage: Arc<dyn CompactionStorage> = hybrid.clone();

        // Act
        let outputs = CompactionActor::execute_with_storage(
            &plan,
            &factory,
            directory.path(),
            None,
            Some(&compaction_storage),
            &prepared,
        )?;

        // Assert
        assert!(
            outputs.len() > 1,
            "publication workspace requires smaller partitions"
        );
        let mut actual = Vec::new();
        for name in outputs {
            assert_eq!(directory.path().join(&name).exists(), !ephemeral);
            let reader = crate::sst::fs::SstFileIo::open_with_real_fs(
                &cloud_path.join(crate::cloud_layout::object_key(&name)),
            )?;
            actual.extend(reader.scan_range(None, None)?);
            let proof = prepared
                .lock()
                .get(&name)
                .cloned()
                .expect("prepared output")
                .proof
                .expect("an uploaded partition carries its proof");
            hybrid.verify_remote_object_guards_within(
                std::slice::from_ref(&proof),
                &crate::common::OperationDeadline::unbounded(),
            )?;
            drop(reader);
            std::fs::write(
                cloud_path.join(crate::cloud_layout::object_key(&name)),
                b"replacement",
            )?;
            assert!(
                hybrid
                    .verify_remote_object_guards_within(
                        &[proof],
                        &crate::common::OperationDeadline::unbounded()
                    )
                    .is_err(),
                "replacement cannot authorize input retirement"
            );
        }
        assert_eq!(actual.len(), 64);
        for (index, (key, value)) in actual.into_iter().enumerate() {
            assert_eq!(key.as_ref(), (index as u64).to_be_bytes());
            assert_eq!(value.as_ref(), vec![7; 4096]);
        }
        assert!(directory.path().join("input.sst").exists());
    }
    Ok(())
}

#[test]
fn should_not_start_upload_after_publication_deadline_is_spent_on_head() -> MidgeResult<()> {
    // Arrange
    #[cfg(feature = "failpoints")]
    let _failpoint_guard = crate::failpoints::test_failpoint_guard();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("source");
    std::fs::write(&path, b"payload")?;
    let backend = Arc::new(PendingUpload {
        head_delay: std::time::Duration::from_millis(30),
        ..PendingUpload::default()
    });
    let cloud: Arc<dyn crate::storage::StorageBackend> = Arc::new(
        crate::storage::cloud::CloudStorage::new(backend.clone(), String::new()),
    );
    let (tx, _rx) = crossbeam::channel::unbounded();
    let hybrid = crate::storage::HybridStorage::new_with_class_stores_and_event_sender(
        Arc::new(crate::storage::filesystem::FileSystem::new(
            directory.path().join("local"),
        )?),
        cloud.clone(),
        cloud.clone(),
        cloud,
        tx,
        std::time::Duration::from_millis(20),
    );
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let result =
        hybrid.publish_immutable_file("sst/object", &path, 7, crc32c::crc32c(b"payload"), &budget);
    let started_upload = backend.pending.lock().take().is_some();

    // Assert
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert!(
        !started_upload,
        "sequential storage calls must consume one attempt budget"
    );
    assert_eq!(budget.used(), 0);
    assert!(path.exists());
    Ok(())
}

#[test]
fn should_summarize_compaction_output_on_the_worker_when_there_is_no_cloud_storage(
) -> MidgeResult<()> {
    // Arrange: a finished partition and no cloud storage at all, which is the
    // local-only compaction shape.
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("partition.sst");
    let factory =
        crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::RealFs::new(directory.path())?), 4096);
    let mut writer = factory.create()?;
    for key in 0_u64..8 {
        writer.add_with_meta(
            &key.to_be_bytes(),
            Some(b"value"),
            key + 1,
            EntryType::Put,
            None,
        )?;
    }
    crate::sst::fs::finish_writer_to_path(writer, &path)?;
    let prepared = PreparedCompactionOutputs::default();
    let budget = ResourceBudget::new(1024 * 1024);
    let name = "000000_01_00000000000000000002.sst";

    // Act
    record_staged_output_partition(None, &prepared, 0, 1, name, &path, &budget)?;

    // Assert: the worker, not the event loop, paid for the re-read and CRC.
    let output = prepared
        .lock()
        .get(name)
        .cloned()
        .expect("summarized output");
    assert_eq!(output.metadata.name, name);
    assert_eq!(output.metadata.level, 1);
    assert_eq!(output.metadata.cf_id, 0);
    assert_eq!(output.metadata.smallest_seq, Some(1));
    assert_eq!(output.metadata.largest_seq, Some(8));
    assert!(output.metadata.content_crc32c.is_some());
    assert!(output.metadata.key_bounds_complete);
    assert!(
        output.proof.is_none(),
        "a local-only partition has nothing to prove remotely"
    );
    // Nothing uploaded, so the local file must remain the only copy.
    assert!(path.exists());
    Ok(())
}
