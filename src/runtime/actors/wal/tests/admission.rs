use super::*;
use crate::io::FsError;

#[test]
fn should_reject_spilled_transaction_before_wal_write_when_preflight_finds_duplicate_sequence(
) -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    actor.install_filesystem_for_test(
        Arc::clone(&fs),
        FsWalFactoryIo::new(fs).create_writer(&crate::wal::segment_file_name(1))?,
    );
    let key = Bytes::from_static(b"duplicate-sequence");
    state
        .get_cf(0)
        .expect("default CF")
        .memtable
        .put_bytes_with_seq(key.clone(), Bytes::from_static(b"old"), 2, None)?;
    let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(16));
    let mut write_set = crate::runtime::transaction_spill::TransactionWriteSet::new(
        pool,
        &crate::io::real::test_root(temp.path()),
        false,
        1,
    );
    write_set.push(crate::runtime::TransactionOp::Put {
        cf_id: 0,
        key,
        value: Bytes::from_static(b"new"),
        ttl_seconds: None,
        insert_only: false,
    })?;
    let source = write_set.take_source();
    let original_pos = actor.writer().expect("WAL writer").current_pos();

    // Act
    let result = actor.append_spilled_transaction(
        &mut state,
        &source,
        &SpilledTransactionAppendParams {
            request_id: 1,
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::Strict),
            start_sequence: 0,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::Corruption(_))),
        "{result:?}"
    );
    assert_eq!(
        actor.writer().expect("WAL writer").current_pos(),
        original_pos
    );
    assert!(actor.is_open());
    assert!(!actor.is_fenced());
    Ok(())
}

/// Filesystem whose next write through a persistent handle (a WAL
/// writer's) lands half its bytes and fails.
#[derive(Default)]
pub(super) struct PartialAppendFs {
    inner: MockFs,
    pub(super) fail_next: Arc<std::sync::atomic::AtomicBool>,
}

struct PartialAppendFile {
    inner: Box<dyn File>,
    fail_next: Arc<std::sync::atomic::AtomicBool>,
}

impl crate::io::ReadFile for PartialAppendFile {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.inner.read_at(offset, len)
    }

    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }
}

impl File for PartialAppendFile {
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        if self
            .fail_next
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            let partial = data.slice(..data.len().div_ceil(2));
            self.inner.write_at(offset, partial)?;
            return Err(FsError::NoSpace("after partial test append".into()));
        }
        self.inner.write_at(offset, data)
    }
    fn truncate(&mut self, len: u64) -> FsResult<()> {
        self.inner.truncate(len)
    }
    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        self.inner.append(data)
    }

    fn sync(&mut self, durability: FsDurability) -> FsResult<()> {
        self.inner.sync(durability)
    }
}

impl ReadFs for PartialAppendFs {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
        self.open(path, FsOpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn crate::io::ReadFile>)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(path)
    }
}

impl crate::io::PersistentHandleFs for PartialAppendFs {
    fn open_persistent(&self, path: &FsPath, opts: FsOpenOptions) -> FsResult<Box<dyn File>> {
        Ok(Box::new(PartialAppendFile {
            inner: crate::io::open_persistent_handle(&self.inner, path, opts)?,
            fail_next: Arc::clone(&self.fail_next),
        }))
    }
}

impl Fs for PartialAppendFs {
    fn try_lock_exclusive(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<Box<dyn crate::io::FsLock>> {
        self.inner.try_lock_exclusive(path)
    }

    fn persistent_handle_capability(&self) -> Option<&dyn crate::io::PersistentHandleFs> {
        self.inner
            .persistent_handle_capability()
            .map(|_| self as &dyn crate::io::PersistentHandleFs)
    }

    fn open(&self, path: &FsPath, opts: FsOpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.inner.open(path, opts)
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(path)
    }
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(path)
    }
    fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_dir_all(path)
    }
    fn sync_dir(&self, path: &FsPath, durability: FsDurability) -> FsResult<()> {
        self.inner.sync_dir(path, durability)
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

fn append_admission_test_transaction(
    actor: &mut WalActor,
    state: &mut RuntimeState,
    path: &std::path::Path,
    spilled: bool,
) -> MidgeResult<()> {
    if spilled {
        let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(8192));
        let mut write_set = crate::runtime::transaction_spill::TransactionWriteSet::new(
            pool,
            &crate::io::real::test_root(path),
            true,
            1,
        );
        write_set.push(crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(b"failed-key"),
            value: Bytes::from_static(b"value"),
            ttl_seconds: None,
            insert_only: false,
        })?;
        actor
            .append_spilled_transaction(
                state,
                &write_set.take_source(),
                &SpilledTransactionAppendParams {
                    request_id: 2,
                    assertions: Vec::new(),
                    durability_policy: Some(DurabilityPolicy::CloudAsync),
                    start_sequence: 0,
                    conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
                },
            )
            .map(|_| ())
    } else {
        let prepared = prepare_put_transaction(
            actor,
            state,
            2,
            b"failed-key",
            b"value",
            DurabilityPolicy::CloudAsync,
        )?;
        actor
            .append_prepared_transactions(state, vec![prepared])
            .map(|_| ())
    }
}

#[test]
fn should_keep_spilled_append_retryable_when_wal_admission_fails_before_first_frame(
) -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let setup = crate::storage::simulated::host_simulated_cloud(temp.path(), Some(1024))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(1024);
    let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    actor.install_filesystem_for_test(
        Arc::clone(&fs),
        FsWalFactoryIo::new(fs).create_writer(&crate::wal::segment_file_name(1))?,
    );
    actor.set_storage_budget(Arc::clone(&setup.hybrid_storage));
    let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(8192));
    let mut write_set = crate::runtime::transaction_spill::TransactionWriteSet::new(
        pool,
        &crate::io::real::test_root(temp.path()),
        true,
        1,
    );
    write_set.push(crate::runtime::TransactionOp::Put {
        cf_id: 0,
        key: Bytes::from_static(b"retryable-key"),
        value: Bytes::from_static(b"value"),
        ttl_seconds: None,
        insert_only: false,
    })?;
    let source = write_set.take_source();
    setup.hybrid_storage.admit_local_wal_bytes(1024)?;

    // Act
    let first_result = actor.append_spilled_transaction(
        &mut state,
        &source,
        &SpilledTransactionAppendParams {
            request_id: 1,
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::CloudAsync),
            start_sequence: 0,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    );

    // Assert
    assert!(matches!(first_result, Err(MidgeError::NoSpace(_))));
    assert!(actor.is_open());
    assert!(!actor.is_fenced());
    assert!(!state.persistence_anomaly_detected());
    assert_eq!(state.wal.pending_writes, 0);
    assert_eq!(
        state
            .get_cf(0)
            .expect("default CF")
            .memtable
            .iter_all()
            .len(),
        0
    );

    setup.hybrid_storage.release_local_wal_bytes(1024);
    actor.append_spilled_transaction(
        &mut state,
        &source,
        &SpilledTransactionAppendParams {
            request_id: 2,
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::CloudAsync),
            start_sequence: 0,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    assert!(actor.is_open());
    assert_eq!(state.wal.pending_writes, 3);
    assert_eq!(
        state
            .get_cf(0)
            .expect("default CF")
            .memtable
            .iter_all()
            .len(),
        1
    );
    Ok(())
}

#[test]
fn should_release_wal_admission_when_failed_append_is_durably_rolled_back() -> MidgeResult<()> {
    for spilled in [false, true] {
        // Arrange
        let temp = tempfile::tempdir()?;
        let setup = crate::storage::simulated::host_simulated_cloud(temp.path(), Some(8192))?;
        setup.hybrid_storage.enable_ephemeral_sst_cache(8192);
        let fs = Arc::new(PartialAppendFs::default());
        let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
        let mut actor = WalActor::open(
            &state,
            DurabilityPolicy::CloudAsync,
            BatchConfig::default(),
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        actor.install_filesystem_for_test(
            fs.clone(),
            FsWalFactoryIo::new(fs.clone()).create_writer(&crate::wal::segment_file_name(1))?,
        );
        actor.set_storage_budget(setup.hybrid_storage.clone());
        let seed = prepare_put_transaction(
            &mut actor,
            &mut state,
            1,
            b"seed",
            b"retained",
            DurabilityPolicy::CloudAsync,
        )?;
        actor.append_prepared_transactions(&mut state, vec![seed])?;
        let physical_before = fs
            .metadata(&FsPath::new(crate::wal::segment_file_name(1)))
            .map_err(FsError::into_midge)?
            .len;
        fs.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Act
        let result =
            append_admission_test_transaction(&mut actor, &mut state, temp.path(), spilled);

        // Assert
        assert!(matches!(result, Err(MidgeError::NoSpace(_))));
        assert!(physical_before > 0);
        assert_eq!(
            fs.metadata(&FsPath::new(crate::wal::segment_file_name(1)))
                .map_err(FsError::into_midge)?
                .len,
            physical_before
        );
        // The header is written when the segment is created, outside WAL
        // admission.
        assert_eq!(
            setup.hybrid_storage.budget_snapshot().total_committed_bytes,
            physical_before - crate::wal::format::HEADER_LEN as u64,
            "durably rolled-back WAL bytes must return their admission (spilled={spilled})"
        );
        assert!(state
            .get_cf(0)
            .expect("default CF")
            .memtable
            .iter_all()
            .iter()
            .all(|(key, _, _)| key.as_slice() != b"failed-key"));
    }
    Ok(())
}

#[test]
fn should_preserve_physical_wal_when_trait_append_claims_unchanged() -> MidgeResult<()> {
    for (wrapped, batch) in [(false, false), (false, true), (true, false), (true, true)] {
        // Arrange: seed the real writer, then exercise both accounted entry
        // points through the trait, including a delegating test writer.
        let fs = Arc::new(PartialAppendFs::default());
        let name = crate::wal::segment_file_name(1);
        let writer = FsWalFactoryIo::new(fs.clone()).create_writer(&name)?;
        writer
            .append_record_accounted(&WalRecord::new(
                crate::wal::WalOpKind::Put,
                Bytes::from_static(b"seed"),
                Some(Bytes::from_static(b"retained")),
                1,
            ))
            .unwrap();
        let writer: Box<dyn WalWriter> = if wrapped {
            Box::new(super::FsyncFailingWriter { inner: writer })
        } else {
            writer
        };
        let before = fs.inner.get_file(&name).unwrap();
        let before_pos = writer.current_pos();
        assert_eq!(writer.append_batch_accounted(&[]).unwrap(), before_pos);
        assert_eq!(fs.inner.get_file(&name).unwrap(), before);
        fs.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let record = WalRecord::new(
            crate::wal::WalOpKind::Put,
            Bytes::from_static(b"failed"),
            Some(Bytes::from_static(b"partial frame")),
            2,
        );

        // Act: the physical write lands partially before the injected failure.
        let failure = if batch {
            let mut later = record.clone();
            later.seq = 3;
            writer.append_batch_accounted(&[record, later])
        } else {
            writer.append_record_accounted(&record)
        }
        .expect_err("injected partial append");
        let after_pos = writer.current_pos();
        drop(writer); // Join the worker so no late effect can escape the check.

        // Assert: unchanged is a physical proof, not just a logical position.
        assert!(matches!(failure.error, MidgeError::NoSpace(_)));
        assert!(failure.unchanged, "the fixture's rollback must be durable");
        assert_eq!(fs.inner.get_file(&name).unwrap(), before);
        assert_eq!(after_pos, before_pos);
    }
    Ok(())
}

struct UncertainAppendWriter {
    timeout: bool,
}

impl WalWriter for UncertainAppendWriter {
    fn append_record_accounted(
        &self,
        _record: &WalRecord,
    ) -> Result<u64, crate::wal::traits::WalAppendError> {
        let error = if self.timeout {
            MidgeError::Timeout("append remains in flight".into())
        } else {
            MidgeError::NoSpace("partial append could not roll back".into())
        };
        Err(crate::wal::traits::WalAppendError::unknown(error))
    }

    fn append_batch_accounted(
        &self,
        records: &[WalRecord],
    ) -> Result<u64, crate::wal::traits::WalAppendError> {
        records.first().map_or_else(
            || Ok(self.current_pos()),
            |record| self.append_record_accounted(record),
        )
    }

    fn current_pos(&self) -> u64 {
        0
    }
    fn sync_with_timeout(&self, _timeout: std::time::Duration) -> MidgeResult<()> {
        Ok(())
    }

    fn tail(&self) -> crate::wal::format::SegmentTail {
        crate::wal::traits::unsealable::tail()
    }

    fn seal_facts(
        &self,
        _timeout: std::time::Duration,
    ) -> crate::common::MidgeResult<crate::wal::format::SealedFacts> {
        crate::wal::traits::unsealable::seal()
    }

    fn write_footer(
        &self,
        _facts: &crate::wal::format::SealedFacts,
        _timeout: std::time::Duration,
    ) -> crate::common::MidgeResult<()> {
        crate::wal::traits::unsealable::write_footer()
    }
}

#[test]
fn should_retain_wal_admission_when_failed_append_has_no_rollback_proof() -> MidgeResult<()> {
    for (spilled, timeout) in [(false, false), (false, true), (true, false), (true, true)] {
        // Arrange
        let temp = tempfile::tempdir()?;
        let setup = crate::storage::simulated::host_simulated_cloud(temp.path(), Some(8192))?;
        setup.hybrid_storage.enable_ephemeral_sst_cache(8192);
        let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
        let mut actor = WalActor::open(
            &state,
            DurabilityPolicy::CloudAsync,
            BatchConfig::default(),
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        actor.replace_writer_for_test(Box::new(UncertainAppendWriter { timeout }));
        actor.set_storage_budget(setup.hybrid_storage.clone());

        // Act
        let result =
            append_admission_test_transaction(&mut actor, &mut state, temp.path(), spilled);

        // Assert
        assert!(matches!(result, Err(MidgeError::OutcomeUnknown(_))));
        assert!(
            actor.is_fenced(),
            "an ambiguous append failure must make writer loss explicit"
        );
        assert!(setup.hybrid_storage.budget_snapshot().total_committed_bytes > 0,
            "unchanged logical position cannot release ambiguous bytes (spilled={spilled}, timeout={timeout})");
        assert_eq!(
            state
                .get_cf(0)
                .expect("default CF")
                .memtable
                .iter_all()
                .len(),
            0
        );
    }
    Ok(())
}

#[test]
fn should_report_recovery_required_not_fenced_when_append_follows_poisoning_io_failure(
) -> MidgeResult<()> {
    // Arrange: the disk fills mid-append, which poisons the local WAL.
    let temp = tempfile::tempdir()?;
    let fs = Arc::new(PartialAppendFs::default());
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let dyn_fs: Arc<dyn Fs> = fs.clone();
    actor.install_filesystem_for_test(
        Arc::clone(&dyn_fs),
        FsWalFactoryIo::new(dyn_fs).create_writer(&crate::wal::segment_file_name(1))?,
    );
    fs.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let put = |request_id| crate::runtime::actors::wal::AppendParams {
        request_id,
        cf_id: 0,
        key: Bytes::from_static(b"key"),
        value: Some(Bytes::from_static(b"value")),
        insert_only: false,
        ttl_seconds: None,
    };

    // Act
    let first = actor.append_single_op(&mut state, put(1));
    let second = actor.append_single_op(&mut state, put(2));

    // Assert: the cause is reported first, then a restart requirement. Local
    // disk trouble is never reported as lost lease authority.
    assert!(matches!(first, Err(MidgeError::NoSpace(_))), "{first:?}");
    let Err(second) = second else {
        panic!("a poisoned WAL must reject later appends");
    };
    assert!(
        matches!(second, MidgeError::RecoveryFailed(_)),
        "{second:?}"
    );
    assert_eq!(second.severity(), crate::common::Severity::Fatal);
    Ok(())
}

#[test]
fn should_keep_wal_open_when_spilled_transaction_exceeds_remaining_wal_budget() -> MidgeResult<()> {
    // Arrange: ephemeral-cache WAL budget with room for the TxnBegin frame
    // but not for the 500-byte op frame that follows it.
    let temp = tempfile::tempdir()?;
    let setup = crate::storage::simulated::host_simulated_cloud(temp.path(), Some(1024))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(1024);
    let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    actor.install_filesystem_for_test(
        Arc::clone(&fs),
        FsWalFactoryIo::new(Arc::clone(&fs)).create_writer(&crate::wal::segment_file_name(1))?,
    );
    actor.set_storage_budget(Arc::clone(&setup.hybrid_storage));
    let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(8192));
    let mut write_set = crate::runtime::transaction_spill::TransactionWriteSet::new(
        pool,
        &crate::io::real::test_root(temp.path()),
        true,
        1,
    );
    write_set.push(crate::runtime::TransactionOp::Put {
        cf_id: 0,
        key: Bytes::from_static(b"big-key"),
        value: Bytes::from(vec![b'v'; 500]),
        ttl_seconds: None,
        insert_only: false,
    })?;
    let source = write_set.take_source();
    let begin = crate::wal::WalRecord::new_cf(
        0,
        crate::wal::WalOpKind::TxnBegin,
        Bytes::from_static(b"txn"),
        None,
        u64::MAX,
    );
    let begin_bound = crate::wal::encoding::record_frame_size_bound(&begin)? as u64;
    // Largest admissible amount right now.
    let (mut low, mut high) = (0_u64, 4096_u64);
    while low < high {
        let mid = (low + high).div_ceil(2);
        if setup.hybrid_storage.admit_local_wal_bytes(mid).is_ok() {
            setup.hybrid_storage.release_local_wal_bytes(mid);
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    let free = low;
    assert!(
        free > begin_bound + 16,
        "test setup: free {free}, begin bound {begin_bound}"
    );
    let pre_admitted = free - begin_bound - 16;
    setup.hybrid_storage.admit_local_wal_bytes(pre_admitted)?;
    let params = |request_id| SpilledTransactionAppendParams {
        request_id,
        assertions: Vec::new(),
        durability_policy: Some(DurabilityPolicy::CloudAsync),
        start_sequence: 0,
        conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
    };

    let wal_len_before = fs
        .metadata(&FsPath::new(crate::wal::segment_file_name(1)))
        .map_err(crate::io::FsError::into_midge)?
        .len;
    let budget_before = setup.hybrid_storage.budget_snapshot().total_committed_bytes;

    // Act: the whole unit is rejected before its begin frame can be written.
    let first_result = actor.append_spilled_transaction(&mut state, &source, &params(1));
    let wal_len_after_first = fs
        .metadata(&FsPath::new(crate::wal::segment_file_name(1)))
        .map_or(0, |m| m.len);
    let fenced_after_first = actor.is_fenced();
    let budget_after_rejection = setup.hybrid_storage.budget_snapshot().total_committed_bytes;
    // The budget frees up (e.g. uploads retire WAL); the caller retries.
    setup.hybrid_storage.release_local_wal_bytes(pre_admitted);
    let retry = actor.append_spilled_transaction(&mut state, &source, &params(2));

    // Assert
    assert!(
        matches!(first_result, Err(MidgeError::NoSpace(_))),
        "{first_result:?}"
    );
    assert!(!fenced_after_first, "a budget rejection fenced the WAL");
    assert_eq!(
        wal_len_after_first, wal_len_before,
        "rejected unit wrote WAL bytes"
    );
    assert_eq!(
        budget_after_rejection, budget_before,
        "rejected unit leaked its reservation"
    );
    assert!(
        retry.is_ok(),
        "retry after the budget freed failed: {retry:?}"
    );
    assert!(actor.is_open());
    Ok(())
}
