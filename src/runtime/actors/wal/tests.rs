use super::*;
use crate::io::traits::{DirEntry, FsError, Metadata};
use crate::io::{
    Durability as FsDurability, File, Fs, FsPath, FsResult, MockFs, OpenOptions as FsOpenOptions,
};
use crate::runtime::RuntimeState;
use bytes::Bytes;
use std::path::PathBuf;
#[cfg(feature = "failpoints")]
use std::sync::{Mutex, OnceLock};

#[cfg(feature = "failpoints")]
static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[cfg(feature = "failpoints")]
fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(feature = "failpoints")]
struct TxnAppendBatchNoSpaceFailpointGuard {
    _test_guard: crate::failpoints::TestFailpointGuard,
}

#[cfg(feature = "failpoints")]
impl TxnAppendBatchNoSpaceFailpointGuard {
    fn setup(request_id: u64) -> Self {
        let test_guard = crate::failpoints::test_failpoint_guard();
        set_txn_append_batch_no_space_failpoint_request_id(Some(request_id));
        fail::cfg("midge::wal::inject_no_space_on_txn_append_batch", "return")
            .expect("configure txn append batch no-space failpoint");
        Self {
            _test_guard: test_guard,
        }
    }
}

#[cfg(feature = "failpoints")]
impl Drop for TxnAppendBatchNoSpaceFailpointGuard {
    fn drop(&mut self) {
        fail::remove("midge::wal::inject_no_space_on_txn_append_batch");
        set_txn_append_batch_no_space_failpoint_request_id(None);
    }
}

struct RenameFailingFs {
    inner: MockFs,
}

impl RenameFailingFs {
    fn new() -> Self {
        Self {
            inner: MockFs::new(),
        }
    }
}

impl Fs for RenameFailingFs {
    fn open(&self, path: &FsPath, opts: FsOpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.inner.open(path, opts)
    }

    fn open_persistent_handle(
        &self,
        path: &FsPath,
        opts: FsOpenOptions,
    ) -> FsResult<Box<dyn File>> {
        self.inner.open_persistent_handle(path, opts)
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

    fn sync_dir(&self, path: &FsPath, dur: FsDurability) -> FsResult<()> {
        self.inner.sync_dir(path, dur)
    }

    fn rename_atomic(&self, from: &FsPath, _to: &FsPath) -> FsResult<()> {
        Err(FsError::Unavailable(format!(
            "rename failed for {}",
            from.0
        )))
    }
}

#[test]
fn should_apply_wal_sequence_to_memtable() -> MidgeResult<()> {
    // Arrange: start with a large sequence so memtable's local seq would differ
    let mut state = RuntimeState::new(PathBuf::from("/tmp/test"), true);
    state.sequence = 100; // ensure WAL sequence is distinct from memtable's internal counter

    let mut wal_actor = WalActor::new(
        PathBuf::from("/tmp/test"),
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        true,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    // Act: append a single put
    let (seq, deferred) = wal_actor.append(
        &mut state,
        AppendParams {
            request_id: 1,
            cf_id: 0,
            key: Bytes::from("k"),
            value: Some(Bytes::from("v")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;

    // Assert: memtable contains one entry and its seq equals WAL seq
    assert!(!deferred);
    let cf_state = state.get_cf(0).expect("cf exists");
    let entries = cf_state.memtable.iter_all(u64::MAX);
    assert_eq!(entries.len(), 1);
    let (key, value, m_seq) = &entries[0];
    assert_eq!(key.as_slice(), b"k");
    assert_eq!(value.as_ref().unwrap().as_slice(), b"v");
    assert_eq!(*m_seq, seq);

    Ok(())
}

#[test]
fn should_not_force_sync_immediately() -> MidgeResult<()> {
    // Arrange: WAL actor with a long batch window so batching is possible
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let wal_dir = temp.path().to_path_buf();
    let batch_cfg = BatchConfig {
        max_delay_ms: 10_000,
        max_bytes: 1024 * 1024,
    };
    let mut wal_actor = WalActor::new(
        wal_dir.clone(),
        DurabilityPolicy::Batched,
        batch_cfg,
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    // Prepare a runtime state
    let mut state = RuntimeState::new(wal_dir, false);

    // Act: append a small batch (deferred in Batched mode)
    let ops = vec![
        crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(b"k1"),
            value: Bytes::from_static(b"v1"),
            ttl_seconds: None,
            insert_only: false,
        },
        crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(b"k2"),
            value: Bytes::from_static(b"v2"),
            ttl_seconds: None,
            insert_only: false,
        },
    ];

    let (_last_seq, _count, deferred) = wal_actor.append_transaction(
        &mut state,
        1,
        ops,
        None,
        None,
        crate::runtime::ConflictPolicy::LastWriteWins,
    )?;
    assert!(deferred);

    // Assert
    // Should not request an immediate sync (time/bytes thresholds not met)
    assert!(
        !wal_actor.should_sync_batch(),
        "should_sync_batch should be false immediately after small append"
    );
    assert_eq!(
        wal_actor.sync_calls(),
        0,
        "no syncs should have been performed yet"
    );
    assert!(
        wal_actor.sync_deadline_timeout().is_some(),
        "pending data should expose a sync deadline timeout"
    );

    Ok(())
}

#[test]
fn should_append_multiple_prepared_transactions_with_one_physical_call() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let wal_dir = db_path.join("wal");
    let mut state = RuntimeState::new(db_path, false);
    let mut wal_actor = WalActor::new(
        wal_dir,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    let first = wal_actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 10,
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"coalesce-a"),
                value: Bytes::from_static(b"value-a"),
                ttl_seconds: None,
                insert_only: false,
            }],
            durability_policy: Some(DurabilityPolicy::Batched),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    let second = wal_actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 11,
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"coalesce-b"),
                value: Bytes::from_static(b"value-b"),
                ttl_seconds: Some(60),
                insert_only: true,
            }],
            durability_policy: Some(DurabilityPolicy::Batched),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;

    // Act
    let results = wal_actor.append_prepared_transactions(&mut state, vec![first, second])?;

    // Assert
    assert_eq!(results.len(), 2);
    assert_eq!(wal_actor.append_calls(), 1);
    assert_eq!(state.wal.pending_writes, 2);
    assert_eq!(wal_actor.pending_sync_count(), 2);
    assert!(state.pending_txn_min_seq.is_some());

    let cf_state = state.get_cf(0).expect("cf exists");
    let entries = cf_state.memtable.iter_all(u64::MAX);
    assert!(entries.iter().any(|(key, value, _sequence)| {
        key.as_slice() == b"coalesce-a"
            && value
                .as_ref()
                .is_some_and(|bytes| bytes.as_slice() == b"value-a")
    }));
    assert!(entries.iter().any(|(key, value, _sequence)| {
        key.as_slice() == b"coalesce-b"
            && value
                .as_ref()
                .is_some_and(|bytes| bytes.as_slice() == b"value-b")
    }));

    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fail_all_prepared_transactions_when_batch_append_hits_no_space() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let wal_dir = db_path.join("wal");
    let mut state = RuntimeState::new(db_path, false);
    let mut wal_actor = WalActor::new(
        wal_dir,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    let first = wal_actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 20,
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"failed-a"),
                value: Bytes::from_static(b"value-a"),
                ttl_seconds: None,
                insert_only: false,
            }],
            durability_policy: Some(DurabilityPolicy::Batched),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    let second = wal_actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 21,
            ops: vec![crate::runtime::TransactionOp::Delete {
                cf_id: 0,
                key: Bytes::from_static(b"failed-b"),
            }],
            durability_policy: Some(DurabilityPolicy::Batched),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    assert!(
        state.sequence > 0,
        "preparation should preserve existing sequence allocation behavior"
    );

    {
        let scenario = fail::FailScenario::setup();
        let failpoint_guard = TxnAppendBatchNoSpaceFailpointGuard::setup(20);

        // Act
        let result = wal_actor.append_prepared_transactions(&mut state, vec![first, second]);

        // Assert
        assert!(result.is_err(), "coalesced append should fail");
        let error = result.err().expect("coalesced append error");
        assert!(matches!(error, MidgeError::NoSpace(_)));
        assert_eq!(wal_actor.append_calls(), 0);
        assert_eq!(state.wal.pending_writes, 0);
        assert_eq!(wal_actor.pending_sync_count(), 0);
        assert_eq!(wal_actor.bytes_since_sync(), 0);
        assert_eq!(state.pending_txn_min_seq, None);
        assert!(
            state
                .get_cf(0)
                .expect("cf exists")
                .memtable
                .iter_all(u64::MAX)
                .is_empty(),
            "failed coalesced append must not publish partial memtable state"
        );

        drop(failpoint_guard);
        scenario.teardown();
    }
    let recovery = wal_actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 22,
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"recovered"),
                value: Bytes::from_static(b"value"),
                ttl_seconds: None,
                insert_only: false,
            }],
            durability_policy: Some(DurabilityPolicy::Batched),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    wal_actor.append_prepared_transactions(&mut state, vec![recovery])?;
    assert!(state
        .get_cf(0)
        .expect("cf exists")
        .memtable
        .iter_all(u64::MAX)
        .iter()
        .any(|(key, value, _sequence)| {
            key.as_slice() == b"recovered"
                && value
                    .as_ref()
                    .is_some_and(|bytes| bytes.as_slice() == b"value")
        }));

    Ok(())
}

#[test]
fn should_not_report_sync_deadline_without_pending_data() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let wal_actor = WalActor::new(
        temp.path().to_path_buf(),
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    // Act
    let deadline = wal_actor.sync_deadline_timeout();

    // Assert
    assert_eq!(deadline, None);
    Ok(())
}

#[test]
fn should_only_coalesce_local_batched_transaction_appends() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let batched_actor = WalActor::new(
        temp.path().join("batched"),
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let strict_actor = WalActor::new(
        temp.path().join("strict"),
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let cloud_actor = WalActor::new(
        temp.path().join("cloud"),
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let memory_actor = WalActor::new(
        temp.path().join("memory"),
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        true,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    // Act
    // Assert
    assert!(batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Batched)));
    assert!(!batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Strict)));
    assert!(!batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::BestEffort)));
    assert!(!batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::CloudAsync)));
    assert!(!strict_actor.can_coalesce_transaction_append(None));
    assert!(!cloud_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Batched)));
    assert!(!memory_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Batched)));

    Ok(())
}

#[test]
fn should_rotate_to_lex_sortable_wal_segment_name() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let wal_dir = db_path.join("wal");
    let mut state = RuntimeState::new(db_path, false);
    let mut wal_actor = WalActor::new(
        wal_dir.clone(),
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    // Act
    wal_actor.rotate(&mut state)?;

    // Assert
    assert!(
        wal_dir
            .join(crate::wal::cloud_segment_file_name(1))
            .exists(),
        "rotated WAL segments should use the canonical lex-sortable segment name"
    );
    assert!(
        !wal_dir.join("1.wal").exists(),
        "newly rotated WAL segments should not use the legacy non-padded name"
    );

    Ok(())
}

#[test]
fn should_fail_rotate_without_advancing_segment_when_rename_fails() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let fs: Arc<dyn Fs> = Arc::new(RenameFailingFs::new());
    let writer =
        FsWalFactoryIo::new(Arc::clone(&fs)).create_writer(crate::wal::ACTIVE_FILE_NAME)?;
    let mut state = RuntimeState::new(db_path.clone(), true);
    state.wal.current_segment_id = 7;

    let mut wal_actor = WalActor::new(
        db_path.join("unused-wal"),
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        true,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    wal_actor.wal_fs = Some(fs);
    wal_actor.writer = Some(writer);
    wal_actor.segment_max_sequence = 42;

    // Act
    let result = wal_actor.rotate(&mut state);

    // Assert
    assert!(result.is_err());
    assert_eq!(state.wal.current_segment_id, 7);
    assert_eq!(wal_actor.segment_max_sequence, 42);
    assert!(wal_actor.writer.is_some());

    Ok(())
}
