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

fn prepare_put_transaction(
    wal_actor: &mut WalActor,
    state: &mut RuntimeState,
    request_id: u64,
    key: &'static [u8],
    value: &'static [u8],
    durability_policy: DurabilityPolicy,
) -> MidgeResult<PreparedTransactionAppend> {
    wal_actor.prepare_transaction_append(
        state,
        TransactionAppendParams {
            request_id,
            assertions: Vec::new(),
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(key),
                value: Bytes::from_static(value),
                ttl_seconds: None,
                insert_only: false,
            }],
            durability_policy: Some(durability_policy),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )
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
        TransactionAppendParams {
            request_id: 1,
            ops,
            assertions: Vec::new(),
            durability_policy: None,
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
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
            assertions: Vec::new(),
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
            assertions: Vec::new(),
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

#[test]
fn should_build_one_read_snapshot_when_checking_multiple_assertions_in_one_column_family(
) -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let state = RuntimeState::new(temp.path().to_path_buf(), false);
    let assertions = vec![
        crate::runtime::KeyAssertion {
            cf_id: 0,
            key: Bytes::from_static(b"first"),
        },
        crate::runtime::KeyAssertion {
            cf_id: 0,
            key: Bytes::from_static(b"second"),
        },
        crate::runtime::KeyAssertion {
            cf_id: 0,
            key: Bytes::from_static(b"third"),
        },
    ];
    WalActor::reset_assertion_snapshot_build_count();

    // Act
    WalActor::ensure_no_assertion_conflicts(&state, &assertions, 0)?;

    // Assert
    assert_eq!(
        WalActor::assertion_snapshot_build_count(),
        1,
        "assertion conflict checks must reuse one read snapshot per column family"
    );
    Ok(())
}

#[test]
fn should_durably_append_strict_transaction_group_once_before_memtable_apply() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path.clone(), false);
    let mut wal_actor = WalActor::new(
        db_path.join("wal"),
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let first = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        12,
        b"strict-a",
        b"value-a",
        DurabilityPolicy::Strict,
    )?;
    let second = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        13,
        b"strict-b",
        b"value-b",
        DurabilityPolicy::Strict,
    )?;

    // Act
    let results = wal_actor.append_prepared_transactions(&mut state, vec![first, second])?;

    // Assert
    assert_eq!(
        results
            .iter()
            .map(|result| result.request_id)
            .collect::<Vec<_>>(),
        vec![12, 13]
    );
    assert_eq!(wal_actor.append_calls(), 1);
    assert_eq!(wal_actor.sync_calls(), 1);
    assert_eq!(state.wal.pending_writes, 0);
    assert_eq!(state.wal.local_durable_seq, state.sequence);
    let entries = state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .iter_all(u64::MAX);
    assert_eq!(entries.len(), 2);

    Ok(())
}

#[test]
fn should_reject_whole_group_before_wal_append_given_duplicate_key_sequence() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path.clone(), false);
    let mut wal_actor = WalActor::new(
        db_path.join("wal"),
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let first = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        120,
        b"same-key",
        b"first",
        DurabilityPolicy::Batched,
    )?;
    let mut second = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        121,
        b"same-key",
        b"second",
        DurabilityPolicy::Batched,
    )?;
    let duplicate_sequence = match &first.apply_ops[0] {
        TransactionApplyOp::Put { sequence, .. } => *sequence,
        _ => unreachable!("prepared put"),
    };
    match &mut second.apply_ops[0] {
        TransactionApplyOp::Put { sequence, .. } => *sequence = duplicate_sequence,
        _ => unreachable!("prepared put"),
    }

    // Act
    let result = wal_actor.append_prepared_transactions(&mut state, vec![first, second]);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(wal_actor.append_calls(), 0);
    assert!(state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .iter_all(u64::MAX)
        .is_empty());
    Ok(())
}

#[test]
fn should_reject_whole_group_before_wal_append_given_cf_missing_after_prepare() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path.clone(), false);
    let mut wal_actor = WalActor::new(
        db_path.join("wal"),
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        122,
        b"must-not-apply",
        b"value",
        DurabilityPolicy::Batched,
    )?;
    state.column_families.remove(&0);

    // Act
    let result = wal_actor.append_prepared_transactions(&mut state, vec![prepared]);

    // Assert
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    assert_eq!(wal_actor.append_calls(), 0);
    Ok(())
}

#[test]
fn should_fail_loudly_given_column_family_missing_at_direct_memtable_apply() {
    // Arrange
    let mut state = RuntimeState::new(PathBuf::from("/tmp/missing-cf-apply"), true);

    // Act
    let point = WalActor::apply_to_memtable(
        &mut state,
        1,
        99,
        Bytes::from_static(b"key"),
        Some(Bytes::from_static(b"value")),
        None,
    );
    let range = WalActor::apply_delete_range_to_memtable(&mut state, 2, 99, b"a", b"z");

    // Assert
    assert!(matches!(point, Err(MidgeError::Internal(_))));
    assert!(matches!(range, Err(MidgeError::Internal(_))));
}

#[cfg(feature = "failpoints")]
#[test]
fn should_apply_no_strict_group_member_when_shared_sync_fails() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path.clone(), false);
    let mut wal_actor = WalActor::new(
        db_path.join("wal"),
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let first = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        14,
        b"sync-failed-a",
        b"value-a",
        DurabilityPolicy::Strict,
    )?;
    let second = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        15,
        b"sync-failed-b",
        b"value-b",
        DurabilityPolicy::Strict,
    )?;
    fail::cfg("midge::wal::inject_no_space_on_sync", "return").expect("configure WAL sync failure");

    // Act
    let result = wal_actor.append_prepared_transactions(&mut state, vec![first, second]);

    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))));
    assert_eq!(wal_actor.append_calls(), 1);
    assert_eq!(wal_actor.sync_calls(), 0);
    assert!(state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .iter_all(u64::MAX)
        .is_empty());

    fail::remove("midge::wal::inject_no_space_on_sync");
    scenario.teardown();
    drop(test_guard);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fail_all_prepared_transactions_when_batch_append_hits_no_space() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    let first = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        20,
        b"failed-a",
        b"value-a",
        DurabilityPolicy::Batched,
    )?;
    let second = wal_actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 21,
            assertions: Vec::new(),
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
    let recovery = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        22,
        b"recovered",
        b"value",
        DurabilityPolicy::Batched,
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
fn should_not_report_local_batch_work_given_pending_cloud_async_wal() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path.clone(), false);
    let mut wal_actor = WalActor::new(
        db_path.join("wal"),
        DurabilityPolicy::CloudAsync,
        BatchConfig {
            max_delay_ms: 0,
            max_bytes: 1,
        },
        false,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let (_, deferred) = wal_actor.append(
        &mut state,
        AppendParams {
            request_id: 581,
            cf_id: 0,
            key: Bytes::from_static(b"cloud-pending"),
            value: Some(Bytes::from_static(b"value")),
            insert_only: false,
            ttl_seconds: None,
        },
    )?;

    // Act
    let should_run_local_batch = wal_actor.should_sync_batch();
    let local_batch_deadline = wal_actor.sync_deadline_timeout();

    // Assert
    assert!(deferred);
    assert!(wal_actor.has_pending_data());
    assert!(!should_run_local_batch);
    assert_eq!(local_batch_deadline, None);
    Ok(())
}

#[test]
fn should_only_coalesce_supported_local_transaction_appends() -> MidgeResult<()> {
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
    assert!(batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Strict)));
    assert!(!batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::BestEffort)));
    assert!(!batched_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::CloudAsync)));
    assert!(strict_actor.can_coalesce_transaction_append(None));
    assert_eq!(
        strict_actor.coalesced_transaction_durability(None),
        Some(DurabilityPolicy::Strict)
    );
    assert!(!cloud_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Batched)));
    assert!(!memory_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Batched)));
    assert!(!memory_actor.can_coalesce_transaction_append(Some(DurabilityPolicy::Strict)));

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
