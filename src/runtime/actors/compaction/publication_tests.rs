//! Exercise real compaction publication, provider ownership and installation proofs.

use super::*;
use cntryl_midge_core::common::resource_budget::ResourceBudget;
use cntryl_midge_core::io::FsError;
use cntryl_midge_table::sst::SstStateReader;
use cntryl_midge_core::types::EntryType;

struct ReadCountingFs {
    inner: cntryl_midge_core::io::MockFs,
    read_opens: std::sync::atomic::AtomicUsize,
}

impl cntryl_midge_core::io::ReadFs for ReadCountingFs {
    fn open_read(
        &self,
        path: &cntryl_midge_core::io::FsPath,
    ) -> cntryl_midge_core::io::FsResult<Box<dyn cntryl_midge_core::io::ReadFile + '_>> {
        self.read_opens
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.open_read(path)
    }

    fn exists(&self, path: &cntryl_midge_core::io::FsPath) -> cntryl_midge_core::io::FsResult<bool> {
        self.inner.exists(path)
    }

    fn metadata(
        &self,
        path: &cntryl_midge_core::io::FsPath,
    ) -> cntryl_midge_core::io::FsResult<cntryl_midge_core::io::traits::Metadata> {
        self.inner.metadata(path)
    }

    fn list_dir(
        &self,
        path: &cntryl_midge_core::io::FsPath,
    ) -> cntryl_midge_core::io::FsResult<Vec<cntryl_midge_core::io::traits::DirEntry>> {
        self.inner.list_dir(path)
    }
}

impl cntryl_midge_core::io::Fs for ReadCountingFs {
    fn persistent_handle_capability(&self) -> Option<&dyn cntryl_midge_core::io::PersistentHandleFs> {
        Some(self)
    }

    fn open(
        &self,
        path: &cntryl_midge_core::io::FsPath,
        options: cntryl_midge_core::io::OpenOptions,
    ) -> cntryl_midge_core::io::FsResult<Box<dyn cntryl_midge_core::io::File + '_>> {
        if options.mode == cntryl_midge_core::io::OpenMode::ReadOnly {
            self.read_opens
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.open(path, options)
    }

    fn remove_file(&self, path: &cntryl_midge_core::io::FsPath) -> cntryl_midge_core::io::FsResult<()> {
        self.inner.remove_file(path)
    }

    fn create_dir_all(&self, path: &cntryl_midge_core::io::FsPath) -> cntryl_midge_core::io::FsResult<()> {
        self.inner.create_dir_all(path)
    }

    fn remove_dir_all(&self, path: &cntryl_midge_core::io::FsPath) -> cntryl_midge_core::io::FsResult<()> {
        self.inner.remove_dir_all(path)
    }

    fn sync_dir(
        &self,
        path: &cntryl_midge_core::io::FsPath,
        durability: cntryl_midge_core::io::Durability,
    ) -> cntryl_midge_core::io::FsResult<()> {
        self.inner.sync_dir(path, durability)
    }

    fn rename_atomic(
        &self,
        from: &cntryl_midge_core::io::FsPath,
        to: &cntryl_midge_core::io::FsPath,
    ) -> cntryl_midge_core::io::FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

impl cntryl_midge_core::io::PersistentHandleFs for ReadCountingFs {
    fn open_persistent(
        &self,
        path: &cntryl_midge_core::io::FsPath,
        options: cntryl_midge_core::io::OpenOptions,
    ) -> cntryl_midge_core::io::FsResult<Box<dyn cntryl_midge_core::io::File>> {
        if options.mode == cntryl_midge_core::io::OpenMode::ReadOnly {
            self.read_opens
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        cntryl_midge_core::io::PersistentHandleFs::open_persistent(&self.inner, path, options)
    }
}

struct DeadlineInputFactory {
    delegate: Arc<dyn SstFactory>,
    deadline: parking_lot::Mutex<Option<OperationDeadline>>,
    opened: std::sync::atomic::AtomicUsize,
    writers_created: std::sync::atomic::AtomicUsize,
    hold_on_open: usize,
    held_valid: AtomicBool,
}

impl SstFactory for DeadlineInputFactory {
    fn output_fs(&self) -> Arc<dyn cntryl_midge_core::io::Fs> {
        self.delegate.output_fs()
    }

    fn compaction_scratch_cleanup_verified(&self) -> bool {
        self.delegate.compaction_scratch_cleanup_verified()
    }

    fn create(&self) -> MidgeResult<Box<dyn cntryl_midge_table::sst::traits::DynSstWriter>> {
        self.delegate.create()
    }

    fn create_for_compaction(
        &self,
        budget: ResourceBudget,
    ) -> MidgeResult<Box<dyn cntryl_midge_table::sst::traits::DynSstWriter>> {
        self.writers_created.fetch_add(1, Ordering::AcqRel);
        self.delegate.create_for_compaction(budget)
    }

    fn open(
        &self,
        path: &std::path::Path,
    ) -> MidgeResult<Box<dyn cntryl_midge_table::sst::traits::SstReaderExt>> {
        self.delegate.open(path)
    }

    fn open_for_compaction(
        &self,
        path: &std::path::Path,
        budget: ResourceBudget,
    ) -> MidgeResult<Box<dyn cntryl_midge_table::sst::traits::SstReaderExt>> {
        let reader = self.delegate.open_for_compaction(path, budget)?;
        if self.opened.fetch_add(1, Ordering::AcqRel) == self.hold_on_open {
            let (key, expected) = if self.hold_on_open == 0 {
                (b"first".as_slice(), b"first-value".as_slice())
            } else {
                (b"second".as_slice(), b"second-value".as_slice())
            };
            self.held_valid.store(
                matches!(reader.get_state_at(key, u64::MAX)?,
                    cntryl_midge_core::types::KeyState::Value(value, 10, None, EntryType::Put)
                    if value.as_ref() == expected),
                Ordering::Release,
            );
            let deadline = self.deadline.lock().take().expect("captured manual origin");
            // Hold only a completed actual first read. No result/error is
            // fabricated; the next cooperative input admission owns the check.
            while !deadline.is_expired() {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
        Ok(reader)
    }
}

struct InputEvidence {
    held_valid: bool,
    opened: usize,
    writers_created: usize,
    error: MidgeError,
}

fn deadline_input_case(asynchronous: bool, hold_on_open: usize) -> MidgeResult<InputEvidence> {
    cntryl_midge_core::failpoints::with_read_gate(|| deadline_input_case_inner(asynchronous, hold_on_open))
}

fn deadline_input_case_inner(
    asynchronous: bool,
    hold_on_open: usize,
) -> MidgeResult<InputEvidence> {
    let directory = tempfile::tempdir()?;
    let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
    let delegate = Arc::new(cntryl_midge_table::sst::FsSstFactoryIo::new(
        Arc::new(cntryl_midge_core::io::RealFs::new(state.host_sst_dir()).map_err(FsError::into_midge)?),
        4096,
    ));
    let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1).with_output_seq(3);
    let mut originals = Vec::new();
    for (index, (key, value)) in [
        (b"first".as_slice(), b"first-value".as_slice()),
        (b"second".as_slice(), b"second-value".as_slice()),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("input-{index}.sst");
        let mut writer = delegate.create()?;
        writer.add_with_meta(key, Some(value), 10, EntryType::Put, None)?;
        cntryl_midge_table::sst::fs::finish_writer_to_path(writer, &state.host_sst_dir().join(&name))?;
        let bytes = std::fs::read(state.host_sst_dir().join(&name))?;
        state
            .manifest
            .test_mut()
            .files
            .push(cntryl_midge_durability::metadata::FileMeta {
                name: name.clone(),
                cf_id: 0,
                level: 0,
                size_bytes: bytes.len() as u64,
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                ..Default::default()
            });
        plan.add_test_source(name.clone());
        originals.push((name, bytes));
    }
    let factory = Arc::new(DeadlineInputFactory {
        delegate,
        deadline: parking_lot::Mutex::new(None),
        opened: std::sync::atomic::AtomicUsize::new(0),
        writers_created: std::sync::atomic::AtomicUsize::new(0),
        hold_on_open,
        held_valid: AtomicBool::new(false),
    });
    let mut actor = CompactionActor::new(factory.clone());
    let (tx, rx) = crossbeam::channel::unbounded();
    let deadline = OperationDeadline::from_budget(std::time::Duration::from_secs(2));
    *factory.deadline.lock() = Some(deadline);
    let result = actor.run_compaction(
        &mut state,
        &plan,
        None,
        asynchronous.then_some(tx),
        Some(deadline),
    );
    let error = if asynchronous {
        result?;
        let completion = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("genuine accepted compute completion");
        actor.join_completed_worker();
        assert!(matches!(
            completion,
            RuntimeMsg::CompactionComplete {
                succeeded: false,
                ..
            }
        ));
        actor.take_worker_error().expect("actual worker error")
    } else {
        result.expect_err("actual synchronous merge must honor the original budget")
    };
    actor.cancel_and_join_worker(&mut state, None);
    for (name, original) in originals {
        assert_eq!(
            std::fs::read(state.host_sst_dir().join(name))?,
            original,
            "accepted timeout preserves each exact authoritative input"
        );
    }
    assert_eq!(state.active_compactions.load(Ordering::Acquire), 0);
    Ok(InputEvidence {
        held_valid: factory.held_valid.load(Ordering::Acquire),
        opened: factory.opened.load(Ordering::Acquire),
        writers_created: factory.writers_created.load(Ordering::Acquire),
        error,
    })
}

#[test]
fn should_stop_input_admission_when_manual_deadline_expires_during_first_open() -> MidgeResult<()> {
    // Arrange: two real SSTs; the first successful open spans the actual budget.
    for asynchronous in [false, true] {
        // Act: use actual synchronous and owned-worker actor entry points.
        let evidence = deadline_input_case(asynchronous, 0)?;

        // Assert: all owned work joins and exact inputs survive before this check.
        assert!(
            evidence.held_valid,
            "the first actual SST read must complete successfully"
        );
        assert!(
            matches!(evidence.error, MidgeError::Timeout(_)),
            "actual error: {:?}",
            evidence.error
        );
        assert_eq!(
            evidence.opened, 1,
            "no later input admission after the original deadline; async={asynchronous}"
        );
    }
    Ok(())
}

#[test]
fn should_reject_output_writer_when_manual_deadline_expires_during_last_input() -> MidgeResult<()> {
    // Arrange: both input opens are genuine; hold the last successful result.
    for asynchronous in [false, true] {
        // Act: the actual input collection returns only after original expiry.
        let evidence = deadline_input_case(asynchronous, 1)?;

        // Assert: preserve exact inputs and join the accepted owner before checks.
        assert!(evidence.held_valid);
        assert_eq!(evidence.opened, 2);
        assert!(
            matches!(evidence.error, MidgeError::Timeout(_)),
            "actual error: {:?}",
            evidence.error
        );
        assert_eq!(evidence.writers_created, 0,
            "no new output-writer admission after the last input returned expired; async={asynchronous}");
    }
    Ok(())
}

#[test]
fn should_summarize_compaction_output_through_injected_mock_fs() -> MidgeResult<()> {
    // Arrange
    let mock = Arc::new(cntryl_midge_core::io::MockFs::new());
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(mock.clone(), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"value"), 7, EntryType::Put, None)?;
    let name = "mock-summary.sst";
    let path = std::path::Path::new(name);
    writer.finish_to_path(path)?;

    // Act
    let prepared = stage_local_output_partition(
        &factory.output_fs(),
        0,
        1,
        name,
        path,
        CompactionOutputAdmission {
            finalized: None,
            budget: &ResourceBudget::new(1024 * 1024),
            manual_deadline: None,
        },
    )?;

    // Assert
    assert_eq!(prepared.metadata.name, name);
    assert_eq!(prepared.metadata.largest_seq, Some(7));
    assert_eq!(
        usize::try_from(prepared.metadata.size_bytes).unwrap(),
        mock.get_file(name).unwrap().len()
    );
    assert!(prepared.metadata.content_crc32c.is_some());
    Ok(())
}

#[test]
fn should_open_finalized_compaction_output_only_for_cloud_upload() -> MidgeResult<()> {
    // Arrange
    let counting_fs = Arc::new(ReadCountingFs {
        inner: cntryl_midge_core::io::MockFs::new(),
        read_opens: std::sync::atomic::AtomicUsize::new(0),
    });
    let mock_cloud = Arc::new(cntryl_midge_storage::cloud::MockCloudBackend::new());
    let cloud: Arc<dyn cntryl_midge_storage::StorageBackend> = Arc::new(
        cntryl_midge_storage::cloud::CloudStorage::new(mock_cloud.clone(), "compaction-test".into()),
    );
    let hybrid = cntryl_midge_storage::HybridStorage::with_policy(
        cloud,
        cntryl_midge_storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(counting_fs.clone(), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"source", Some(b"value"), 7, EntryType::Put, None)?;
    writer.finish_to_path(std::path::Path::new("input.sst"))?;
    counting_fs
        .read_opens
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1).with_output_seq(949);
    plan.add_test_source("input.sst");
    let output_fs = factory.output_fs();
    let prepared: PreparedCompactionOutputs =
        Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
    let sink = |name: &str,
                path: &std::path::Path,
                budget: &ResourceBudget,
                finalized: Option<&cntryl_midge_table::sst::traits::FinalizedSst>| {
        // The writer has finalized the output at this callback boundary.
        counting_fs
            .read_opens
            .store(0, std::sync::atomic::Ordering::SeqCst);
        record_staged_output_partition(
            Some(&hybrid),
            &output_fs,
            &prepared,
            0,
            1,
            name,
            path,
            CompactionOutputAdmission {
                budget,
                manual_deadline: None,
                finalized,
            },
        )?;
        assert_eq!(
            counting_fs
                .read_opens
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the finalized output must be opened once for cloud upload, without a prior metadata reread"
        );
        Ok(())
    };

    // Act
    let outputs = crate::compaction::execute_compaction_with_output_sink(
        &plan,
        &factory,
        std::path::Path::new("output"),
        None,
        Some(&sink),
        None,
    )?;

    // Assert
    assert_eq!(outputs.len(), 1);
    assert_eq!(mock_cloud.get_uploads().len(), 1);
    let published = prepared.lock();
    let metadata = &published.values().next().expect("staged output").metadata;
    assert_eq!(metadata.smallest_key.as_deref(), Some(b"source".as_slice()));
    assert!(metadata.content_crc32c.is_some());
    Ok(())
}

#[test]
fn should_retain_compaction_partition_when_upload_workspace_cannot_be_admitted() -> MidgeResult<()>
{
    // Arrange
    #[cfg(feature = "failpoints")]
    let _failpoint_guard = cntryl_midge_core::failpoints::test_failpoint_guard();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("partition.sst");
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(
        Arc::new(cntryl_midge_core::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?),
        4096,
    )
    .with_compression_policy(cntryl_midge_core::codec::CompressionPolicy::Fixed(
        cntryl_midge_core::codec::CompressionAlgo::None,
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
    cntryl_midge_table::sst::fs::finish_writer_to_path(writer, &path)?;
    let original = std::fs::read(&path)?;
    let cloud_path = directory.path().join("cloud");
    let hybrid = cntryl_midge_storage::HybridStorage::with_policy(
        Arc::new(cntryl_midge_storage::filesystem::FileSystem::new(&cloud_path)?),
        cntryl_midge_storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    hybrid.enable_ephemeral_sst_cache(1024 * 1024);
    let prepared = PreparedCompactionOutputs::default();
    let budget = ResourceBudget::new(1024 * 1024);
    let name = "000000_01_00000000000000000002.sst";

    // Act
    let result = record_staged_output_partition(
        Some(&hybrid),
        &factory.output_fs(),
        &prepared,
        0,
        1,
        name,
        &path,
        CompactionOutputAdmission {
            finalized: None,
            budget: &budget,
            manual_deadline: None,
        },
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::ResourceLimit(_))),
        "upload copies must be admitted before publication"
    );
    assert_eq!(std::fs::read(path)?, original);
    assert!(prepared.lock().is_empty());
    assert!(!cloud_path
        .join(cntryl_midge_core::cloud_layout::object_key(name))
        .exists());
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[derive(Default)]
struct PendingUpload {
    inner: cntryl_midge_storage::cloud::MockCloudBackend,
    head_delay: std::time::Duration,
    pending: parking_lot::Mutex<Option<(Vec<u8>, cntryl_midge_storage::cloud::CloudCallback)>>,
}

impl cntryl_midge_storage::cloud::CloudReadCapability for PendingUpload {
    cntryl_midge_storage::cloud::unsupported_cloud_backend!(submit_get);

    fn submit_get_with_metadata(&self, key: &str, callback: cntryl_midge_storage::cloud::CloudCallback) {
        self.inner.submit_get_with_metadata(key, callback);
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: cntryl_midge_storage::cloud::CloudCallback,
    ) {
        self.inner.submit_get_range(key, start, end, callback);
    }
}

impl cntryl_midge_storage::cloud::CloudWriteCapability for PendingUpload {
    fn submit_put(
        &self,
        _key: &str,
        data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: cntryl_midge_storage::cloud::CloudCallback,
    ) {
        *self.pending.lock() = Some((data, callback));
    }
}

impl cntryl_midge_storage::cloud::CloudDeleteCapability for PendingUpload {
    cntryl_midge_storage::cloud::unsupported_cloud_backend!(submit_delete);
}

impl cntryl_midge_storage::cloud::CloudListCapability for PendingUpload {
    cntryl_midge_storage::cloud::unsupported_cloud_backend!(submit_list);
}

impl cntryl_midge_storage::cloud::CloudHeadCapability for PendingUpload {
    fn submit_head(&self, key: &str, callback: cntryl_midge_storage::cloud::CloudCallback) {
        std::thread::sleep(self.head_delay);
        self.inner.submit_head(key, callback);
    }
}

impl cntryl_midge_storage::cloud::CloudBackend for PendingUpload {}

#[test]
fn should_retain_compaction_upload_charge_after_timeout_until_provider_releases_body(
) -> MidgeResult<()> {
    // Arrange: retain a shared failpoint scope before setup or either clock.
    cntryl_midge_core::failpoints::with_read_gate(|| {
        for manual in [false, true] {
            // Act: exercise actual provider ownership for both origin policies.
            upload_charge_case(manual)?;
            // Assert: the case checks charge, exact bytes and joined release.
        }
        Ok(())
    })
}

fn upload_charge_case(manual: bool) -> MidgeResult<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("partition.sst");
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(
        Arc::new(cntryl_midge_core::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?),
        4096,
    );
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"retained value"), 1, EntryType::Put, None)?;
    cntryl_midge_table::sst::fs::finish_writer_to_path(writer, &path)?;
    let original = std::fs::read(&path)?;
    let backend = Arc::new(PendingUpload::default());
    let cloud: Arc<dyn cntryl_midge_storage::StorageBackend> = Arc::new(
        cntryl_midge_storage::cloud::CloudStorage::new(backend.clone(), String::new()),
    );
    let (tx, _rx) = crossbeam::channel::unbounded();
    let hybrid = cntryl_midge_storage::HybridStorage::new_with_class_stores_and_event_sender(
        cloud.clone(),
        cloud.clone(),
        cloud,
        tx,
        std::time::Duration::from_secs(2),
    );
    hybrid.enable_ephemeral_sst_cache(1024 * 1024);
    let budget = ResourceBudget::new(2 * 1024 * 1024);
    let prepared = PreparedCompactionOutputs::default();

    // Capture only after actual SST/store/budget setup. The optional manual
    // origin is shorter than the ordinary whole-file provider cap.
    let manual_deadline =
        manual.then(|| OperationDeadline::from_budget(std::time::Duration::from_secs(1)));
    let result = record_staged_output_partition(
        Some(&hybrid),
        &factory.output_fs(),
        &prepared,
        0,
        1,
        "000000_01_00000000000000000002.sst",
        &path,
        CompactionOutputAdmission {
            finalized: None,
            budget: &budget,
            manual_deadline,
        },
    );
    let retained = budget.used();
    let pending = backend.pending.lock().take().expect("provider owns upload");
    let payload_bytes = pending.0.len();
    let payload_matches = pending.0 == original;
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
    assert!(
        payload_matches,
        "the provider owns the actual immutable bytes"
    );
    assert_eq!(std::fs::read(&path)?, original);
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
        let _failpoint_guard = cntryl_midge_core::failpoints::test_failpoint_guard();
        let directory = tempfile::tempdir()?;
        let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(
            Arc::new(cntryl_midge_core::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?),
            4096,
        )
        .with_compression_policy(cntryl_midge_core::codec::CompressionPolicy::Fixed(
            cntryl_midge_core::codec::CompressionAlgo::None,
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
        cntryl_midge_table::sst::fs::finish_writer_to_path(writer, &directory.path().join("input.sst"))?;
        let cloud_path = directory.path().join("cloud");
        let hybrid = Arc::new(cntryl_midge_storage::HybridStorage::with_policy(
            Arc::new(cntryl_midge_storage::filesystem::FileSystem::new(&cloud_path)?),
            cntryl_midge_storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        if ephemeral {
            hybrid.enable_ephemeral_sst_cache(1024 * 1024);
        }
        let mut plan = crate::compaction::CompactionPlan::new(0, 0, 1).with_output_seq(2);
        plan.add_test_source("input.sst");
        plan.compaction_memory_limit = 1024 * 1024;
        plan.target_sst_size = 1024 * 1024;
        let prepared = PreparedCompactionOutputs::default();
        let compaction_storage: Arc<dyn CompactionStorage> = hybrid.clone();
        let grandparent_overlap_rolls = Arc::new(std::sync::atomic::AtomicU64::default());

        // Act
        let outputs = CompactionJobActor::execute_with_storage(
            &plan,
            crate::compaction::LeveledCompactionConfig::default().max_compaction_input_files,
            &factory,
            directory.path(),
            None,
            Some(&compaction_storage),
            CompactionOutputWork {
                prepared: &prepared,
                manual_deadline: None,
                budget: None,
                grandparent_overlap_rolls: &grandparent_overlap_rolls,
            },
        )?;

        // Assert
        assert!(
            outputs.len() > 1,
            "publication workspace requires smaller partitions"
        );
        verify_rollover_partitions(
            &outputs,
            directory.path(),
            &cloud_path,
            ephemeral,
            &prepared,
            &hybrid,
        )?;
        assert!(directory.path().join("input.sst").exists());
    }
    Ok(())
}

fn verify_rollover_partitions(
    outputs: &[String],
    directory: &std::path::Path,
    cloud_path: &std::path::Path,
    ephemeral: bool,
    prepared: &PreparedCompactionOutputs,
    hybrid: &cntryl_midge_storage::HybridStorage,
) -> MidgeResult<()> {
    let mut actual = Vec::new();
    for name in outputs {
        assert_eq!(directory.join(name).exists(), !ephemeral);
        let reader = cntryl_midge_table::sst::fs::SstFileIo::open(
            &cntryl_midge_core::cloud_layout::object_key(name),
            cntryl_midge_core::io::real::test_root(cloud_path),
        )?;
        actual.extend(reader.scan_range_state(None, None)?.into_iter().filter_map(
            |(key, state)| match state {
                cntryl_midge_core::types::KeyState::Value(value, _, _, _) => Some((key, value)),
                cntryl_midge_core::types::KeyState::Absent | cntryl_midge_core::types::KeyState::Tombstone(_) => None,
            },
        ));
        let proof = prepared
            .lock()
            .get(name)
            .cloned()
            .expect("prepared output")
            .proof
            .expect("an uploaded partition carries its proof");
        hybrid.verify_remote_object_guards_within(
            std::slice::from_ref(&proof),
            &cntryl_midge_core::common::OperationDeadline::unbounded(),
        )?;
        drop(reader);
        std::fs::write(
            cloud_path.join(cntryl_midge_core::cloud_layout::object_key(name)),
            b"replacement",
        )?;
        assert!(
            hybrid
                .verify_remote_object_guards_within(
                    &[proof],
                    &cntryl_midge_core::common::OperationDeadline::unbounded()
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
    Ok(())
}

#[test]
fn should_not_start_upload_after_publication_deadline_is_spent_on_head() -> MidgeResult<()> {
    // Arrange
    #[cfg(feature = "failpoints")]
    let _failpoint_guard = cntryl_midge_core::failpoints::test_failpoint_guard();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("source");
    std::fs::write(&path, b"payload")?;
    let backend = Arc::new(PendingUpload {
        head_delay: std::time::Duration::from_millis(30),
        ..PendingUpload::default()
    });
    let cloud: Arc<dyn cntryl_midge_storage::StorageBackend> = Arc::new(
        cntryl_midge_storage::cloud::CloudStorage::new(backend.clone(), String::new()),
    );
    let (tx, _rx) = crossbeam::channel::unbounded();
    let hybrid = cntryl_midge_storage::HybridStorage::new_with_class_stores_and_event_sender(
        cloud.clone(),
        cloud.clone(),
        cloud,
        tx,
        std::time::Duration::from_millis(20),
    );
    let budget = ResourceBudget::new(1024 * 1024);

    let source =
        cntryl_midge_core::io::RealFs::new(directory.path()).map_err(cntryl_midge_core::io::FsError::into_midge)?;

    // Act
    let result = hybrid.publish_immutable_file(
        cntryl_midge_storage::hybrid::backend::ImmutableFileSource {
            key: "sst/object",
            fs: &source,
            path: &cntryl_midge_core::io::FsPath::new("source"),
            size: 7,
            checksum: crc32c::crc32c(b"payload"),
        },
        &budget,
    );
    let started_upload = backend.pending.lock().take().is_some();

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::Timeout(_))),
        "{:?}",
        result.as_ref().err()
    );
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
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(
        Arc::new(cntryl_midge_core::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?),
        4096,
    );
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
    cntryl_midge_table::sst::fs::finish_writer_to_path(writer, &path)?;
    let prepared = PreparedCompactionOutputs::default();
    let budget = ResourceBudget::new(1024 * 1024);
    let name = "000000_01_00000000000000000002.sst";

    // Act
    record_staged_output_partition(
        None,
        &factory.output_fs(),
        &prepared,
        0,
        1,
        name,
        &path,
        CompactionOutputAdmission {
            finalized: None,
            budget: &budget,
            manual_deadline: None,
        },
    )?;

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
