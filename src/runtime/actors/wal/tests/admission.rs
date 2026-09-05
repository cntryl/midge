use super::*;

#[derive(Default)]
struct PartialAppendFs {
    inner: MockFs,
    fail_next: Arc<std::sync::atomic::AtomicBool>,
}

struct PartialAppendFile {
    inner: Box<dyn File>,
    fail_next: Arc<std::sync::atomic::AtomicBool>,
}

impl File for PartialAppendFile {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.inner.read_at(offset, len)
    }
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        if self
            .fail_next
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            let partial = data.slice(..data.len().div_ceil(2));
            self.inner.write_at(offset, partial)?;
            return Err(FsError::Io("no space after partial test append".into()));
        }
        self.inner.write_at(offset, data)
    }
    fn truncate(&mut self, len: u64) -> FsResult<()> {
        self.inner.truncate(len)
    }
    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        self.inner.append(data)
    }
    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }
    fn sync(&mut self, durability: FsDurability) -> FsResult<()> {
        self.inner.sync(durability)
    }
    fn close(self: Box<Self>) -> FsResult<()> {
        self.inner.close()
    }
}

impl Fs for PartialAppendFs {
    fn open(&self, path: &FsPath, opts: FsOpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.inner.open(path, opts)
    }
    fn open_persistent_handle(
        &self,
        path: &FsPath,
        opts: FsOpenOptions,
    ) -> FsResult<Box<dyn File>> {
        Ok(Box::new(PartialAppendFile {
            inner: self.inner.open_persistent_handle(path, opts)?,
            fail_next: Arc::clone(&self.fail_next),
        }))
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(path)
    }
    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }
    fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
        self.inner.create_dir_all(path)
    }
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(path)
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
        let mut write_set =
            crate::runtime::transaction_spill::TransactionWriteSet::new(pool, path, true, 1);
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
                SpilledTransactionAppendParams {
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
fn should_release_wal_admission_when_failed_append_is_durably_rolled_back() -> MidgeResult<()> {
    for spilled in [false, true] {
        // Arrange
        let temp = tempfile::tempdir()?;
        let setup = crate::storage::test_support::build_cloud_backed_filesystem_simulation(
            temp.path(),
            Some(8192),
        )?;
        setup.hybrid_storage.enable_ephemeral_sst_cache(8192);
        let fs = Arc::new(PartialAppendFs::default());
        let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
        let mut actor = WalActor::new(
            temp.path().join("wal"),
            DurabilityPolicy::CloudAsync,
            BatchConfig::default(),
            true,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        actor.writer = Some(FsWalFactoryIo::new(fs.clone()).create_writer("wal.log")?);
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
        let physical_before = fs.metadata(&FsPath::new("wal.log"))?.len;
        fs.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Act
        let result =
            append_admission_test_transaction(&mut actor, &mut state, temp.path(), spilled);

        // Assert
        assert!(matches!(result, Err(MidgeError::NoSpace(_))));
        assert!(physical_before > 0);
        assert_eq!(fs.metadata(&FsPath::new("wal.log"))?.len, physical_before);
        assert_eq!(
            setup.hybrid_storage.budget_snapshot().total_committed_bytes,
            physical_before,
            "durably rolled-back WAL bytes must return their admission (spilled={spilled})"
        );
        assert!(state
            .get_cf(0)
            .expect("default CF")
            .memtable
            .iter_all(u64::MAX)
            .iter()
            .all(|(key, _, _)| key.as_slice() != b"failed-key"));
    }
    Ok(())
}

struct UncertainAppendWriter {
    timeout: bool,
}

impl WalWriter for UncertainAppendWriter {
    fn append_record(&self, _record: &WalRecord) -> MidgeResult<u64> {
        if self.timeout {
            Err(MidgeError::Timeout("append remains in flight".into()))
        } else {
            Err(MidgeError::NoSpace(
                "partial append could not roll back".into(),
            ))
        }
    }

    fn current_pos(&self) -> u64 {
        0
    }
    fn flush(&self) -> MidgeResult<()> {
        Ok(())
    }
    fn sync(&self) -> MidgeResult<()> {
        Ok(())
    }
    fn close(&self) -> MidgeResult<()> {
        Ok(())
    }
}

#[test]
fn should_retain_wal_admission_when_failed_append_has_no_rollback_proof() -> MidgeResult<()> {
    for (spilled, timeout) in [(false, false), (false, true), (true, false), (true, true)] {
        // Arrange
        let temp = tempfile::tempdir()?;
        let setup = crate::storage::test_support::build_cloud_backed_filesystem_simulation(
            temp.path(),
            Some(8192),
        )?;
        setup.hybrid_storage.enable_ephemeral_sst_cache(8192);
        let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
        let mut actor = WalActor::new(
            temp.path().join("unused-wal"),
            DurabilityPolicy::CloudAsync,
            BatchConfig::default(),
            true,
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        actor.writer = Some(Box::new(UncertainAppendWriter { timeout }));
        actor.set_storage_budget(setup.hybrid_storage.clone());

        // Act
        let result =
            append_admission_test_transaction(&mut actor, &mut state, temp.path(), spilled);

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::NoSpace(_) | MidgeError::Timeout(_))
        ));
        assert_eq!(actor.writer.as_ref().expect("writer").current_pos(), 0);
        assert!(setup.hybrid_storage.budget_snapshot().total_committed_bytes > 0,
            "unchanged logical position cannot release ambiguous bytes (spilled={spilled}, timeout={timeout})");
        assert!(state
            .get_cf(0)
            .expect("default CF")
            .memtable
            .iter_all(u64::MAX)
            .is_empty());
    }
    Ok(())
}
