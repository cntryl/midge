use super::*;
use crate::io::traits::{DirEntry, FsError, Metadata};
use crate::io::{
    Durability as FsDurability, File, Fs, FsPath, FsResult, MockFs, OpenOptions as FsOpenOptions,
    ReadFs,
};
use crate::runtime::wal_transition::{WalSealTicket, WalSyncTicket};
#[cfg(feature = "failpoints")]
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use crate::runtime::RuntimeState;
use crate::types::EntryType;
use bytes::Bytes;
use std::path::PathBuf;
#[cfg(feature = "failpoints")]
use std::sync::{Mutex, OnceLock};

#[cfg(feature = "failpoints")]
static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn seal_ticket_for_test(actor: &WalActor, state: &RuntimeState) -> WalSealTicket {
    let segment_id = state.wal.open_segment();
    WalSealTicket::for_test(
        segment_id,
        segment_id
            .checked_add(1)
            .expect("test segment has successor"),
        actor.open_segment_max_sequence(),
    )
}

#[test]
fn should_reject_wal_append_before_disk_growth_when_ephemeral_budget_is_exhausted(
) -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let setup = crate::storage::simulated::host_simulated_cloud(temp.path(), Some(1))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(1);
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    actor.set_storage_budget(setup.hybrid_storage.clone());
    let prepared = prepare_put_transaction(
        &mut actor,
        &mut state,
        1,
        b"key",
        b"value",
        DurabilityPolicy::CloudAsync,
    )?;
    // Act
    let result = actor.append_prepared_transactions(&mut state, vec![prepared]);
    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))));
    assert_eq!(
        std::fs::metadata(
            temp.path()
                .join("wal")
                .join(crate::wal::segment_file_name(1))
        )?
        .len(),
        crate::wal::format::HEADER_LEN as u64
    );
    assert_eq!(
        setup.hybrid_storage.budget_snapshot().total_committed_bytes,
        0
    );
    Ok(())
}

#[test]
fn should_charge_actual_wal_bytes_when_successful_append_compresses_payload() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let setup = crate::storage::simulated::host_simulated_cloud(temp.path(), Some(8192))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(8192);
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    actor.set_storage_budget(setup.hybrid_storage.clone());
    let prepared = actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 1,
            assertions: Vec::new(),
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"key"),
                value: Bytes::from(vec![b'a'; 4096]),
                ttl_seconds: None,
                insert_only: false,
            }],
            durability_policy: Some(DurabilityPolicy::CloudAsync),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    // Act
    actor.append_prepared_transactions(&mut state, vec![prepared])?;
    let physical = std::fs::metadata(
        temp.path()
            .join("wal")
            .join(crate::wal::segment_file_name(1)),
    )?
    .len();
    // Assert: the segment header is written when the segment is created,
    // outside WAL admission; appends are charged what they wrote.
    let appended = physical - crate::wal::format::HEADER_LEN as u64;
    assert!(appended > 0 && appended < 4096);
    assert_eq!(
        setup.hybrid_storage.budget_snapshot().total_committed_bytes,
        appended
    );
    Ok(())
}

#[test]
fn should_check_transaction_assertions_against_remote_sst_when_local_cache_is_empty(
) -> MidgeResult<()> {
    use crate::sst::SstFactory;
    // Arrange
    let local = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    let mut state = RuntimeState::new(local.path().to_path_buf(), false);
    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"asserted", Some(b"value"), 9, EntryType::Put, None)?;
    let bytes = writer.finish_bytes()?;
    let name = crate::cloud_layout::file_name(0, 0, 1);
    std::fs::create_dir_all(remote.path().join("sst"))?;
    std::fs::write(remote.path().join("sst").join(&name), &bytes)?;
    state
        .manifest
        .test_mut()
        .files
        .push(crate::metadata::FileMeta {
            name,
            cf_id: 0,
            size_bytes: bytes.len() as u64,
            content_crc32c: Some(crc32c::crc32c(&bytes)),
            smallest_key: Some(b"asserted".to_vec()),
            largest_key: Some(b"asserted".to_vec()),
            smallest_seq: Some(9),
            largest_seq: Some(9),
            ..Default::default()
        });
    // Validation reads through the event loop's resources, which in
    // ephemeral-cache cloud mode sit over the remote SST store.
    let remote_fs: Arc<dyn crate::io::Fs> = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        Arc::clone(&state.fs),
        Arc::new(crate::storage::filesystem::FileSystem::new(remote.path())?),
        Duration::from_secs(5),
    ));
    let resources = Arc::new(
        crate::runtime::read_resources::ReadResources::new_with_diagnostics(
            remote_fs,
            std::path::PathBuf::from("sst"),
            64 * 1024,
            crate::sst::cache::CachePolicyType::Lru,
            Arc::new(crate::diagnostics::RuntimeDiagnostics::default()),
        ),
    );
    let mut snapshots = super::transaction_state::ValidationSnapshots::new(&state, Some(resources));
    // Act
    let result = WalActor::ensure_no_assertion_conflicts(
        &state,
        &mut snapshots,
        &[crate::runtime::KeyAssertion {
            cf_id: 0,
            key: Bytes::from_static(b"asserted"),
        }],
        5,
    );
    // Assert
    assert!(
        matches!(result, Err(MidgeError::WriteConflict(_))),
        "remote sequence must conflict: {result:?}"
    );
    Ok(())
}

#[test]
fn should_assign_identical_expiration_given_multiple_ttl_puts_in_one_transaction() {
    // Arrange
    let ops = vec![
        crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(b"a"),
            value: Bytes::from_static(b"a"),
            ttl_seconds: Some(3),
            insert_only: false,
        },
        crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(b"b"),
            value: Bytes::from_static(b"b"),
            ttl_seconds: Some(3),
            insert_only: false,
        },
    ];

    // Act
    let apply_ops = WalActor::build_apply_ops(ops, 10, 1, 50_000);

    // Assert
    let expirations = apply_ops
        .iter()
        .map(|op| match op {
            super::TransactionApplyOp::Put { expiration, .. } => *expiration,
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(expirations, vec![Some(53_000), Some(53_000)]);
}

#[test]
fn should_reject_transaction_without_mutation_when_sequence_space_is_exhausted() -> MidgeResult<()>
{
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge_sequence_exhaustion".into(), true);
    state.sequence = u64::MAX - 1;
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::BestEffort,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    // Act
    let result = prepare_put_transaction(
        &mut actor,
        &mut state,
        99,
        b"overflow",
        b"value",
        DurabilityPolicy::BestEffort,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert_eq!(state.sequence, u64::MAX - 1);
    assert_eq!(state.transaction_id_cursor_for_test(), 0);
    Ok(())
}

#[test]
fn should_reject_transaction_without_mutation_when_commit_would_use_the_dropped_sentinel(
) -> MidgeResult<()> {
    // Arrange
    let mut state = RuntimeState::new("/tmp/test_midge_dropped_sequence_sentinel".into(), true);
    state.sequence = u64::MAX - 3;
    let reserved = state.wal.history.ledger().seq_reserved_through();
    let frontiers = state.manifest.flush_frontiers().clone();
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::BestEffort,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    // Act
    let result = prepare_put_transaction(
        &mut actor,
        &mut state,
        99,
        b"reserved-sentinel",
        b"value",
        DurabilityPolicy::BestEffort,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert_eq!(state.sequence, u64::MAX - 3);
    assert_eq!(state.transaction_id_cursor_for_test(), 0);
    assert_eq!(state.wal.history.ledger().seq_reserved_through(), reserved);
    assert_eq!(state.manifest.flush_frontiers(), &frontiers);
    assert_eq!(state.wal.pending_writes, 0);
    assert_eq!(actor.append_calls(), 0);
    assert_eq!(state.get_cf(0).unwrap().memtable.iter_all(), []);
    Ok(())
}

#[cfg(feature = "failpoints")]
fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(feature = "failpoints")]
struct TxnAppendBatchNoSpaceFailpointGuard {
    // Drop order: the scenario is torn down in `drop` before the gate field
    // is released.
    scenario: Option<fail::FailScenario<'static>>,
    _test_guard: crate::failpoints::TestFailpointGuard,
}

#[cfg(feature = "failpoints")]
impl TxnAppendBatchNoSpaceFailpointGuard {
    /// Take the failpoint gate before the `fail` scenario lock, matching
    /// every other failpoint test; the reverse order deadlocks against them.
    fn setup(request_id: u64) -> Self {
        let test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        set_txn_append_batch_no_space_failpoint_request_id(Some(request_id));
        fail::cfg("midge::wal::inject_no_space_on_txn_append_batch", "return")
            .expect("configure txn append batch no-space failpoint");
        Self {
            scenario: Some(scenario),
            _test_guard: test_guard,
        }
    }
}

#[cfg(feature = "failpoints")]
impl Drop for TxnAppendBatchNoSpaceFailpointGuard {
    fn drop(&mut self) {
        fail::remove("midge::wal::inject_no_space_on_txn_append_batch");
        set_txn_append_batch_no_space_failpoint_request_id(None);
        if let Some(scenario) = self.scenario.take() {
            scenario.teardown();
        }
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

/// A file-backed WAL actor over `fs`, appending to segment `open` of the
/// ledger there, which starts at `open` when `fs` holds none. `state`
/// continues that ledger.
fn actor_over(
    state: &mut RuntimeState,
    fs: Arc<dyn Fs>,
    open: u64,
    policy: DurabilityPolicy,
) -> MidgeResult<WalActor> {
    state.wal.history = crate::runtime::wal_history::WalHistory::open(fs, open, 0)?;
    WalActor::open(
        state,
        policy,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )
}

/// Filesystem that cannot create segment `segment`'s file.
struct SegmentCreateFailingFs {
    inner: MockFs,
    segment: FsPath,
}

impl SegmentCreateFailingFs {
    fn new(segment: u64) -> Self {
        Self {
            inner: MockFs::new(),
            segment: FsPath::new(crate::wal::segment_file_name(segment)),
        }
    }

    fn should_fail_open(&self, path: &FsPath, opts: FsOpenOptions) -> bool {
        (opts.create || opts.create_new) && *path == self.segment
    }
}

impl ReadFs for SegmentCreateFailingFs {
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

impl crate::io::PersistentHandleFs for SegmentCreateFailingFs {
    fn open_persistent(&self, path: &FsPath, opts: FsOpenOptions) -> FsResult<Box<dyn File>> {
        if self.should_fail_open(path, opts) {
            return Err(FsError::Unavailable(format!(
                "segment creation failed for {}",
                path.0
            )));
        }
        crate::io::open_persistent_handle(&self.inner, path, opts)
    }
}

impl Fs for SegmentCreateFailingFs {
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
        if self.should_fail_open(path, opts) {
            return Err(FsError::Unavailable(format!(
                "segment creation failed for {}",
                path.0
            )));
        }
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

    fn sync_dir(&self, path: &FsPath, dur: FsDurability) -> FsResult<()> {
        self.inner.sync_dir(path, dur)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

/// Filesystem that creates segment `segment` but cannot open a writer on it.
struct SegmentWriterOpenFailingFs {
    inner: MockFs,
    segment: FsPath,
}

impl SegmentWriterOpenFailingFs {
    fn new(segment: u64) -> Self {
        Self {
            inner: MockFs::new(),
            segment: FsPath::new(crate::wal::segment_file_name(segment)),
        }
    }

    /// Creating the segment succeeds; opening it again for the writer fails.
    fn should_fail_open(&self, path: &FsPath, opts: FsOpenOptions) -> bool {
        !opts.create && !opts.create_new && *path == self.segment
    }
}

impl ReadFs for SegmentWriterOpenFailingFs {
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

impl crate::io::PersistentHandleFs for SegmentWriterOpenFailingFs {
    fn open_persistent(&self, path: &FsPath, opts: FsOpenOptions) -> FsResult<Box<dyn File>> {
        if self.should_fail_open(path, opts) {
            return Err(FsError::Unavailable(
                "next WAL writer open failed".to_string(),
            ));
        }
        crate::io::open_persistent_handle(&self.inner, path, opts)
    }
}

impl Fs for SegmentWriterOpenFailingFs {
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
        if self.should_fail_open(path, opts) {
            return Err(FsError::Unavailable(
                "next WAL writer open failed".to_string(),
            ));
        }
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

    fn sync_dir(&self, path: &FsPath, dur: FsDurability) -> FsResult<()> {
        self.inner.sync_dir(path, dur)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

/// Filesystem that fails one existence check once armed.
struct RotationPreflightFailingOnceFs {
    inner: MockFs,
    fail_once: std::sync::atomic::AtomicBool,
}

impl ReadFs for RotationPreflightFailingOnceFs {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
        self.open(path, FsOpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn crate::io::ReadFile>)
    }

    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        if self
            .fail_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(FsError::Io(
                "injected sealed-segment preflight failure".to_string(),
            ));
        }
        self.inner.exists(path)
    }

    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        self.inner.metadata(path)
    }

    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
        self.inner.list_dir(path)
    }
}

impl crate::io::PersistentHandleFs for RotationPreflightFailingOnceFs {
    fn open_persistent(&self, path: &FsPath, opts: FsOpenOptions) -> FsResult<Box<dyn File>> {
        crate::io::open_persistent_handle(&self.inner, path, opts)
    }
}

impl Fs for RotationPreflightFailingOnceFs {
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

    fn sync_dir(&self, path: &FsPath, dur: FsDurability) -> FsResult<()> {
        self.inner.sync_dir(path, dur)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

/// Writer whose fsync always fails after appends succeeded.
struct FsyncFailingWriter {
    inner: Box<dyn crate::wal::WalWriter>,
}

impl crate::wal::WalWriter for FsyncFailingWriter {
    fn append_record(&self, record: &crate::wal::WalRecord) -> MidgeResult<u64> {
        self.inner.append_record(record)
    }

    fn append_batch(&self, records: &[crate::wal::WalRecord]) -> MidgeResult<u64> {
        self.inner.append_batch(records)
    }

    fn sync(&self) -> MidgeResult<()> {
        Err(MidgeError::Io(std::io::Error::other(
            "injected fsync failure",
        )))
    }

    fn current_pos(&self) -> u64 {
        self.inner.current_pos()
    }

    fn tail(&self) -> crate::wal::format::SegmentTail {
        self.inner.tail()
    }

    fn seal_facts(
        &self,
        timeout: std::time::Duration,
    ) -> MidgeResult<crate::wal::format::SealedFacts> {
        self.inner.seal_facts(timeout)
    }

    /// Sealing syncs the footer, which fails like every other sync.
    fn write_footer(
        &self,
        _facts: &crate::wal::format::SealedFacts,
        _timeout: std::time::Duration,
    ) -> MidgeResult<()> {
        Err(MidgeError::Io(std::io::Error::other(
            "injected fsync failure",
        )))
    }
}

#[test]
fn should_restore_open_actor_when_cloud_rotation_preflight_read_fails() -> MidgeResult<()> {
    // Arrange
    let preflight = Arc::new(RotationPreflightFailingOnceFs {
        inner: MockFs::new(),
        fail_once: std::sync::atomic::AtomicBool::new(false),
    });
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut wal_actor = actor_over(
        &mut state,
        Arc::clone(&preflight) as Arc<dyn Fs>,
        1,
        DurabilityPolicy::CloudAsync,
    )?;
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        1,
        b"preflight-retry",
        b"value",
        DurabilityPolicy::CloudAsync,
    )?;
    wal_actor.append_prepared_transactions(&mut state, vec![prepared])?;
    let ticket = seal_ticket_for_test(&wal_actor, &state);
    wal_actor.flush_for_cloud_upload(&mut state, &ticket)?;
    let segment_id = state.wal.open_segment();
    preflight
        .fail_once
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Act
    let error = wal_actor
        .rotate(&mut state, &ticket)
        .expect_err("injected existence preflight must fail");

    // Assert
    assert!(matches!(error, MidgeError::Io(_)));
    assert!(wal_actor.is_open());
    assert!(!wal_actor.is_fenced());
    assert!(!state.persistence_anomaly_detected());
    assert_eq!(state.wal.open_segment(), segment_id);
    wal_actor.flush_for_cloud_upload(&mut state, &ticket)?;
    wal_actor.rotate(&mut state, &ticket)?;
    assert_eq!(state.wal.open_segment(), segment_id + 1);
    Ok(())
}

#[test]
fn should_fence_rotation_when_open_segment_is_missing_with_pending_records() -> MidgeResult<()> {
    // Arrange: the open segment vanishes while records appended to it still
    // wait for their fsync.
    let mock = MockFs::new();
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut wal_actor = actor_over(
        &mut state,
        Arc::new(mock.clone()),
        4,
        DurabilityPolicy::Batched,
    )?;
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        1,
        b"pending-before-loss",
        b"value",
        DurabilityPolicy::Batched,
    )?;
    wal_actor.append_prepared_transactions(&mut state, vec![prepared])?;
    assert!(wal_actor.has_pending_data());
    mock.remove_file(&FsPath::new(crate::wal::segment_file_name(4)))
        .map_err(FsError::into_midge)?;
    let ticket = seal_ticket_for_test(&wal_actor, &state);

    // Act
    let error = wal_actor
        .rotate(&mut state, &ticket)
        .expect_err("a missing open segment with pending records must not rotate");

    // Assert
    assert!(matches!(error, MidgeError::RecoveryFailed(_)), "{error:?}");
    assert!(wal_actor.is_fenced());
    assert!(state.persistence_anomaly_detected());
    assert_eq!(state.wal.open_segment(), 4);
    assert_eq!(state.wal.frontiers.local_durable(), 0);
    assert!(matches!(
        begin_sync_for_test(&mut wal_actor, &mut state),
        Err(MidgeError::RecoveryFailed(_))
    ));
    Ok(())
}

#[test]
fn should_fence_rotation_when_seal_ticket_disagrees_with_actor_state() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let wal_dir = temp.path().join("wal");
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let segment_id = state.wal.open_segment();
    let mismatched_ticket = WalSealTicket::for_test(segment_id, segment_id + 1, 99);

    // Act
    let error = wal_actor
        .rotate(&mut state, &mismatched_ticket)
        .expect_err("ticket that disagrees with the actor must not rotate");

    // Assert
    assert!(matches!(error, MidgeError::Internal(message) if message.contains("disagrees")));
    assert!(wal_actor.is_fenced());
    assert_eq!(state.wal.open_segment(), segment_id);
    assert_eq!(Some(state.wal.open_segment()), Some(segment_id));
    assert!(wal_dir
        .join(crate::wal::segment_file_name(segment_id))
        .exists());
    assert!(!wal_dir
        .join(crate::wal::segment_file_name(segment_id + 1))
        .exists());
    Ok(())
}

#[test]
fn should_fence_strict_transaction_when_sync_fails_after_records_are_appended() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
    let inner =
        FsWalFactoryIo::new(Arc::clone(&fs)).create_writer(&crate::wal::segment_file_name(1))?;
    wal_actor.replace_writer_for_test(Box::new(FsyncFailingWriter { inner }));
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        1,
        b"strict-unsynced",
        b"must-not-be-visible",
        DurabilityPolicy::Strict,
    )?;

    // Act
    let Err(error) = wal_actor.append_prepared_transactions(&mut state, vec![prepared]) else {
        panic!("strict transaction must fail when its fsync fails");
    };
    let sequence_after_failure = state.sequence;
    let later = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        2,
        b"later-strict",
        b"rejected",
        DurabilityPolicy::Strict,
    );

    // Assert
    assert!(matches!(error, MidgeError::OutcomeUnknown(_)));
    assert!(wal_actor.is_fenced());
    assert!(state.persistence_anomaly_detected());
    assert_eq!(state.wal.frontiers.local_durable(), 0);
    assert_eq!(
        state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .iter_all()
            .len(),
        0
    );
    assert!(matches!(later, Err(MidgeError::RecoveryFailed(_))));
    assert_eq!(state.sequence, sequence_after_failure);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_keep_unknown_lease_authority_as_authority_loss_when_sync_fails() -> MidgeResult<()> {
    // Arrange: the WAL append is admitted, then the lease store becomes
    // unreachable at the writer thread's fsync boundary.
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let authority_calls = Arc::clone(&calls);
    wal_actor.set_write_authority_for_test(Arc::new(move |operation| {
        if authority_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            operation();
            Ok(())
        } else {
            Err(MidgeError::LeaseUnavailable(
                "injected provider outage during fsync authority check".into(),
            ))
        }
    }));
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        1,
        b"authority-sync",
        b"possibly durable",
        DurabilityPolicy::Strict,
    )?;

    // Act
    let result = wal_actor.append_prepared_transactions(&mut state, vec![prepared]);

    // Assert: the WAL stops because authority is unknown, not because local
    // storage was poisoned.
    assert!(
        matches!(result, Err(MidgeError::OutcomeUnknown(_))),
        "{:?}",
        result.as_ref().err()
    );
    assert!(wal_actor.is_fenced());
    assert!(wal_actor.authority_lost());
    assert!(calls.load(std::sync::atomic::Ordering::SeqCst) >= 2);
    Ok(())
}

fn begin_sync_for_test(
    actor: &mut WalActor,
    state: &mut RuntimeState,
) -> MidgeResult<super::WalSyncReceipt> {
    actor.begin_sync_transition(state, &WalSyncTicket::for_test(0))
}

#[test]
fn should_apply_wal_sequence_to_memtable() -> MidgeResult<()> {
    // Arrange: start with a large sequence so memtable's local seq would differ
    let mut state = RuntimeState::new(PathBuf::from("/tmp/test"), true);
    state.sequence = 100; // ensure WAL sequence is distinct from memtable's internal counter

    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

    // Act: append a single put
    let (seq, deferred) = wal_actor.append_single_op(
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

    // Assert: memtable contains one entry at the transaction's operation
    // sequence, one below its commit sequence.
    assert!(!deferred);
    let cf_state = state.get_cf(0).expect("cf exists");
    let entries = cf_state.memtable.iter_all();
    assert_eq!(entries.len(), 1);
    let (key, value, m_seq) = &entries[0];
    assert_eq!(key.as_slice(), b"k");
    assert_eq!(value.as_ref().unwrap().as_slice(), b"v");
    assert_eq!(*m_seq + 1, seq);

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
    let mut state = RuntimeState::new(wal_dir, false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        batch_cfg,
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;

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
    let mut state = RuntimeState::new(db_path, false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
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
    assert!(state.pending_transaction_started());

    let cf_state = state.get_cf(0).expect("cf exists");
    let entries = cf_state.memtable.iter_all();
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
    WalActor::ensure_no_assertion_conflicts(
        &state,
        &mut super::transaction_state::ValidationSnapshots::new(&state, None),
        &assertions,
        0,
    )?;

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
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
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
    assert_eq!(state.wal.frontiers.local_durable(), state.sequence);
    let entries = state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .iter_all();
    assert_eq!(entries.len(), 2);

    Ok(())
}

#[test]
fn should_reject_whole_group_before_wal_append_given_duplicate_key_sequence() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path.clone(), false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
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
    assert_eq!(
        state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .iter_all()
            .len(),
        0
    );
    Ok(())
}

#[test]
fn should_reject_whole_group_before_wal_append_given_cf_missing_after_prepare() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path.clone(), false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
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
fn should_leave_no_partial_writes_given_later_memtable_preflight_failure() -> MidgeResult<()> {
    // Arrange: a transaction is single-family (#843), so the later
    // operation fails preflight on its own key: the memtable already holds
    // that key at every sequence the transaction could have taken.
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path.clone(), false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let put = |key: &'static [u8]| crate::runtime::TransactionOp::Put {
        cf_id: 0,
        key: Bytes::from_static(key),
        value: Bytes::from_static(b"value"),
        ttl_seconds: None,
        insert_only: false,
    };
    let prepared = wal_actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 123,
            assertions: Vec::new(),
            ops: vec![put(b"first-valid"), put(b"later-invalid")],
            durability_policy: Some(DurabilityPolicy::Batched),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    )?;
    let memtable = state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .clone();
    for sequence in 1..=state.sequence {
        memtable.put_with_seq(b"later-invalid".to_vec(), b"held".to_vec(), sequence, None)?;
    }

    // Act
    let result = wal_actor.append_prepared_transactions(&mut state, vec![prepared]);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(wal_actor.append_calls(), 0);
    assert_eq!(memtable.get(b"first-valid")?, None);
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
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
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
    assert!(matches!(result, Err(MidgeError::OutcomeUnknown(_))));
    assert_eq!(wal_actor.append_calls(), 1);
    assert_eq!(wal_actor.sync_calls(), 0);
    assert_eq!(
        state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .iter_all()
            .len(),
        0
    );

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
    let setup = crate::storage::simulated::host_simulated_cloud(temp.path(), Some(8192))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(8192);
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path, false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    wal_actor.set_storage_budget(setup.hybrid_storage.clone());

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
        assert_eq!(
            setup.hybrid_storage.budget_snapshot().total_committed_bytes,
            0,
            "a rejected append must not consume local disk admission"
        );
        assert!(!state.pending_transaction_started());
        assert!(
            state
                .get_cf(0)
                .expect("cf exists")
                .memtable
                .iter_all()
                .is_empty(),
            "failed coalesced append must not publish partial memtable state"
        );

        drop(failpoint_guard);
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
        .iter_all()
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
fn should_rotate_through_ledger_in_memory_mode() -> MidgeResult<()> {
    // Arrange: memory mode keeps no WAL files, but its ledger still names
    // the open segment (#851).
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let ticket = seal_ticket_for_test(&wal_actor, &state);

    // Act
    let receipt = wal_actor.rotate(&mut state, &ticket)?;

    // Assert
    assert!(state.wal.history.segment_files().is_none());
    assert_eq!((receipt.sealed_segment(), receipt.next_segment()), (1, 2));
    assert_eq!(state.wal.open_segment(), 2);
    assert_eq!(
        state.wal.history.ledger().members().collect::<Vec<_>>(),
        vec![1, 2]
    );
    Ok(())
}

#[test]
fn should_not_report_sync_deadline_without_pending_data() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let state = RuntimeState::new(temp.path().to_path_buf(), false);
    let wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
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
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::CloudAsync,
        BatchConfig {
            max_delay_ms: 0,
            max_bytes: 1,
        },
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let (_, deferred) = wal_actor.append_single_op(
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
    let actor = |directory: &str, policy, memory_mode| {
        let state = RuntimeState::new(temp.path().join(directory), memory_mode);
        WalActor::open(
            &state,
            policy,
            BatchConfig::default(),
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )
    };
    let batched_actor = actor("batched", DurabilityPolicy::Batched, false)?;
    let strict_actor = actor("strict", DurabilityPolicy::Strict, false)?;
    let cloud_actor = actor("cloud", DurabilityPolicy::CloudAsync, false)?;
    let memory_actor = actor("memory", DurabilityPolicy::Batched, true)?;

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
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let ticket = seal_ticket_for_test(&wal_actor, &state);

    // Act
    wal_actor.rotate(&mut state, &ticket)?;

    // Assert
    assert!(
        wal_dir.join(crate::wal::segment_file_name(1)).exists(),
        "rotated WAL segments should use the canonical lex-sortable segment name"
    );
    assert!(
        !wal_dir.join("1.wal").exists(),
        "newly rotated WAL segments should not use the legacy non-padded name"
    );

    Ok(())
}

#[test]
fn should_fail_rotate_without_advancing_segment_when_next_segment_creation_fails() -> MidgeResult<()>
{
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let fs: Arc<dyn Fs> = Arc::new(SegmentCreateFailingFs::new(8));
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut wal_actor = actor_over(&mut state, Arc::clone(&fs), 7, DurabilityPolicy::Strict)?;
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        1,
        b"key",
        b"value",
        DurabilityPolicy::Strict,
    )?;
    wal_actor.append_prepared_transactions(&mut state, vec![prepared])?;
    let max_sequence = wal_actor.open_segment_max_sequence();
    let segment = FsPath::new(crate::wal::segment_file_name(7));
    let before = crate::io::read_all(fs.as_ref(), &segment).map_err(FsError::into_midge)?;
    let ticket = seal_ticket_for_test(&wal_actor, &state);

    // Act
    let result = wal_actor.rotate(&mut state, &ticket);

    // Assert: the digest is taken but no footer is written before the
    // successor exists, so segment 7 stays open and writable.
    assert!(result.is_err());
    assert!(wal_actor.is_open());
    assert_eq!(state.wal.open_segment(), 7);
    assert!(max_sequence > 0);
    assert_eq!(wal_actor.open_segment_max_sequence(), max_sequence);
    assert_eq!(
        crate::io::read_all(fs.as_ref(), &segment).map_err(FsError::into_midge)?,
        before
    );
    assert!(wal_actor.writer().is_some());

    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_leave_rotation_operational_or_fenced_at_every_filesystem_boundary() -> MidgeResult<()> {
    // Arrange
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();

    for boundary in WalTransitionBoundary::LOCAL_ROTATION_BOUNDARIES {
        let temp = tempfile::tempdir()?;
        let db_path = temp.path().to_path_buf();
        let wal_dir = db_path.join("wal");
        let mut state = RuntimeState::new(db_path, false);
        let mut wal_actor = WalActor::open(
            &state,
            DurabilityPolicy::Strict,
            BatchConfig::default(),
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        let segment_id = state.wal.open_segment();
        let ticket = seal_ticket_for_test(&wal_actor, &state);
        fail::cfg(boundary.failpoint_name(), "return").expect("configure WAL rotation boundary");

        // Act
        let error = wal_actor
            .rotate(&mut state, &ticket)
            .expect_err("configured boundary must interrupt rotation");
        fail::remove(boundary.failpoint_name());

        // Assert
        assert!(matches!(error, MidgeError::Internal(_)));
        assert!(wal_dir
            .join(crate::wal::segment_file_name(segment_id))
            .exists());
        let ledger_open = Some(state.wal.open_segment());
        // The footer is the first irreversible step: from it on the actor
        // fences, and only the ledger's rotation moves the open segment.
        if matches!(
            boundary,
            WalTransitionBoundary::AfterFooter | WalTransitionBoundary::AfterLedgerRotate
        ) {
            assert!(wal_actor.is_fenced());
            assert!(state.persistence_anomaly_detected());
            let rotated = boundary == WalTransitionBoundary::AfterLedgerRotate;
            assert_eq!(ledger_open, Some(segment_id + u64::from(rotated)));
            assert!(matches!(
                wal_actor.rotate(&mut state, &ticket),
                Err(MidgeError::RecoveryFailed(_))
            ));
        } else {
            assert!(!wal_actor.is_fenced());
            assert!(!state.persistence_anomaly_detected());
            assert_eq!(ledger_open, Some(segment_id));
            assert_eq!(
                wal_dir
                    .join(crate::wal::segment_file_name(segment_id + 1))
                    .exists(),
                boundary == WalTransitionBoundary::AfterSegmentCreate
            );
            wal_actor.rotate(&mut state, &ticket)?;
            assert_eq!(state.wal.open_segment(), segment_id + 1);
        }
    }

    scenario.teardown();
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fence_each_append_path_after_physical_commit_before_accounting() -> MidgeResult<()> {
    // Arrange
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();

    for spilled in [false, true] {
        let temp = tempfile::tempdir()?;
        let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
        let mut actor = WalActor::open(
            &state,
            DurabilityPolicy::CloudAsync,
            BatchConfig::default(),
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        fail::cfg(
            WalTransitionBoundary::AfterAppendBeforeAccounting.failpoint_name(),
            "return",
        )
        .expect("configure post-append accounting boundary");

        // Act
        let error = if spilled {
            let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(0));
            let mut writes = crate::runtime::transaction_spill::TransactionWriteSet::new(
                pool,
                &crate::io::real::test_root(temp.path()),
                false,
                1,
            );
            writes.push(crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"post-append-spilled"),
                value: Bytes::from_static(b"value"),
                ttl_seconds: None,
                insert_only: false,
            })?;
            actor
                .append_spilled_transaction(
                    &mut state,
                    &writes.take_source(),
                    SpilledTransactionAppendParams {
                        request_id: 2,
                        assertions: Vec::new(),
                        durability_policy: Some(DurabilityPolicy::CloudAsync),
                        start_sequence: 0,
                        conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
                    },
                )
                .expect_err("spilled append must stop after physical commit")
        } else {
            let prepared = prepare_put_transaction(
                &mut actor,
                &mut state,
                1,
                b"post-append-regular",
                b"value",
                DurabilityPolicy::CloudAsync,
            )?;
            match actor.append_prepared_transactions(&mut state, vec![prepared]) {
                Err(error) => error,
                Ok(_) => panic!("regular append must stop after physical commit"),
            }
        };
        fail::remove(WalTransitionBoundary::AfterAppendBeforeAccounting.failpoint_name());
        let sequence_after_failure = state.sequence;

        // Assert
        assert!(matches!(error, MidgeError::OutcomeUnknown(_)), "{error:?}");
        assert!(actor.is_fenced());
        assert!(state.persistence_anomaly_detected());
        assert_eq!(state.wal.pending_writes, 0);
        assert_eq!(state.wal.frontiers.local_durable(), 0);
        assert_eq!(
            state
                .get_cf(0)
                .expect("default column family")
                .memtable
                .iter_all()
                .len(),
            0
        );
        let later = prepare_put_transaction(
            &mut actor,
            &mut state,
            3,
            b"later-durable-write",
            b"must-not-appear",
            DurabilityPolicy::CloudAsync,
        );
        assert!(matches!(later, Err(MidgeError::RecoveryFailed(_))));
        assert_eq!(state.sequence, sequence_after_failure);
    }

    scenario.teardown();
    Ok(())
}

#[test]
fn should_fence_rotation_before_overwriting_a_next_segment_that_holds_data() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let db_path = temp.path().to_path_buf();
    let wal_dir = db_path.join("wal");
    let mut state = RuntimeState::new(db_path, false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let segment_id = state.wal.open_segment();
    let next_path = wal_dir.join(crate::wal::segment_file_name(segment_id + 1));
    // Longer than a segment header, so it may hold records.
    let existing = [
        vec![0xa5; crate::wal::format::HEADER_LEN],
        b"existing-segment-data".to_vec(),
    ]
    .concat();
    std::fs::write(&next_path, &existing)?;
    let ticket = seal_ticket_for_test(&wal_actor, &state);

    // Act
    let error = wal_actor
        .rotate(&mut state, &ticket)
        .expect_err("a next segment holding data must reject rotation");

    // Assert
    assert!(
        matches!(
            error,
            MidgeError::Corruption(_) | MidgeError::RecoveryFailed(_)
        ),
        "{error:?}"
    );
    assert_eq!(std::fs::read(next_path)?, existing);
    assert_eq!(state.wal.open_segment(), segment_id);
    assert_eq!(Some(state.wal.open_segment()), Some(segment_id));
    assert!(wal_actor.is_fenced());
    assert!(state.persistence_anomaly_detected());
    Ok(())
}

#[test]
fn should_keep_rotation_retryable_when_next_segment_directory_sync_fails() -> MidgeResult<()> {
    // Arrange: the next segment is not history until the ledger records it,
    // so failing to make its directory entry durable is reversible.
    let mock = MockFs::new();
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut wal_actor = actor_over(
        &mut state,
        Arc::new(mock.clone()),
        7,
        DurabilityPolicy::Strict,
    )?;
    mock.set_sync_dir_failure(true);
    let ticket = seal_ticket_for_test(&wal_actor, &state);

    // Act
    let error = wal_actor
        .rotate(&mut state, &ticket)
        .expect_err("directory sync failure must interrupt rotation");
    let ledger_open = Some(state.wal.open_segment());
    mock.set_sync_dir_failure(false);
    let retried = wal_actor.rotate(&mut state, &ticket);

    // Assert
    assert!(error.to_string().contains("sync"));
    assert_eq!(ledger_open, Some(7));
    assert!(!state.persistence_anomaly_detected());
    assert!(retried.is_ok(), "{retried:?}");
    assert_eq!(state.wal.open_segment(), 8);
    Ok(())
}

#[test]
fn should_fence_filesystem_wal_when_next_writer_open_fails_after_ledger_rotate() -> MidgeResult<()>
{
    // Arrange
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let fs: Arc<dyn Fs> = Arc::new(SegmentWriterOpenFailingFs::new(8));
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut wal_actor = actor_over(&mut state, Arc::clone(&fs), 7, DurabilityPolicy::CloudAsync)?;
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        1,
        b"accepted-before-rotate",
        b"durable",
        DurabilityPolicy::CloudAsync,
    )?;
    wal_actor.append_prepared_transactions(&mut state, vec![prepared])?;
    let ticket = seal_ticket_for_test(&wal_actor, &state);
    wal_actor.flush_for_cloud_upload(&mut state, &ticket)?;
    let durable_before = state.wal.frontiers.local_durable();
    let pending_before = state.wal.pending_writes;
    let entries_before = state
        .get_cf(0)
        .expect("default column family")
        .memtable
        .iter_all()
        .len();

    // Act
    let rotate_error = wal_actor
        .rotate(&mut state, &ticket)
        .expect_err("next writer open must fail after the ledger rotation");
    let sequence_before_rejected = state.sequence;
    let rejected = wal_actor.append_transaction(
        &mut state,
        TransactionAppendParams {
            request_id: 2,
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"rejected-after-rotate"),
                value: Bytes::from_static(b"must-not-appear"),
                ttl_seconds: None,
                insert_only: false,
            }],
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::CloudAsync),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    );
    let sync_ticket = WalSyncTicket::for_test(0);
    let sync_error = wal_actor
        .begin_sync_transition(&mut state, &sync_ticket)
        .expect_err("fenced WAL must reject sync");
    let flush_error = wal_actor
        .flush_for_cloud_upload(&mut state, &ticket)
        .expect_err("fenced WAL must reject cloud flush");
    let second_rotate_error = wal_actor
        .rotate(&mut state, &ticket)
        .expect_err("fenced WAL must reject further rotation");

    // Assert
    assert!(matches!(
        rotate_error,
        MidgeError::Internal(message) if message.contains("next WAL writer open failed")
    ));
    assert!(matches!(rejected, Err(MidgeError::RecoveryFailed(_))));
    assert!(matches!(sync_error, MidgeError::RecoveryFailed(_)));
    assert!(matches!(flush_error, MidgeError::RecoveryFailed(_)));
    assert!(matches!(second_rotate_error, MidgeError::RecoveryFailed(_)));
    assert!(state.persistence_anomaly_detected());
    assert_eq!(state.sequence, sequence_before_rejected);
    // The ledger recorded the rotation before the next writer failed.
    assert_eq!(state.wal.open_segment(), 8);
    assert_eq!(state.wal.frontiers.local_durable(), durable_before);
    assert_eq!(state.wal.pending_writes, pending_before);
    assert_eq!(
        state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .iter_all()
            .len(),
        entries_before,
        "rejected write must not mutate the memtable"
    );
    assert!(fs
        .exists(&FsPath::new(crate::wal::segment_file_name(7)))
        .map_err(FsError::into_midge)?);
    assert_eq!(
        Some(state.wal.open_segment()),
        Some(8),
        "the ledger already sealed segment 7"
    );

    assert_best_effort_write_after_fencing(&mut wal_actor, &mut state, entries_before);

    Ok(())
}

fn assert_best_effort_write_after_fencing(
    wal_actor: &mut WalActor,
    state: &mut RuntimeState,
    entries_before: usize,
) {
    let best_effort = wal_actor.append_transaction(
        state,
        TransactionAppendParams {
            request_id: 3,
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"explicit-best-effort"),
                value: Bytes::from_static(b"memory-only"),
                ttl_seconds: None,
                insert_only: false,
            }],
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::BestEffort),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    );
    assert!(best_effort.is_ok(), "explicit BestEffort remains available");
    assert_eq!(
        state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .iter_all()
            .len(),
        entries_before + 1
    );
}

mod admission;

mod floor;

struct DurableSyncCountingFile<'a> {
    inner: Box<dyn File + 'a>,
    durable_syncs: Arc<std::sync::atomic::AtomicUsize>,
}

impl crate::io::ReadFile for DurableSyncCountingFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.inner.read_at(offset, len)
    }

    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }
}

impl File for DurableSyncCountingFile<'_> {
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        self.inner.write_at(offset, data)
    }
    fn truncate(&mut self, len: u64) -> FsResult<()> {
        self.inner.truncate(len)
    }
    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        self.inner.append(data)
    }

    fn sync(&mut self, dur: FsDurability) -> FsResult<()> {
        if dur == FsDurability::Durable {
            self.durable_syncs
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.sync(dur)
    }
}

/// Counts durable file fsyncs so a test can prove a code path made bytes
/// power-loss safe, not merely handed them to the OS.
struct DurableSyncCountingFs {
    inner: MockFs,
    durable_syncs: Arc<std::sync::atomic::AtomicUsize>,
}

impl ReadFs for DurableSyncCountingFs {
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

impl crate::io::PersistentHandleFs for DurableSyncCountingFs {
    fn open_persistent(&self, path: &FsPath, opts: FsOpenOptions) -> FsResult<Box<dyn File>> {
        Ok(Box::new(DurableSyncCountingFile {
            inner: crate::io::open_persistent_handle(&self.inner, path, opts)?,
            durable_syncs: Arc::clone(&self.durable_syncs),
        }))
    }
}

impl Fs for DurableSyncCountingFs {
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
        Ok(Box::new(DurableSyncCountingFile {
            inner: self.inner.open(path, opts)?,
            durable_syncs: Arc::clone(&self.durable_syncs),
        }))
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
    fn sync_dir(&self, path: &FsPath, dur: FsDurability) -> FsResult<()> {
        self.inner.sync_dir(path, dur)
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

#[test]
fn should_fsync_active_segment_before_sealing_cloud_async_wal() -> MidgeResult<()> {
    // Arrange: a seal records the segment sealed and advances
    // local_durable_seq, so its bytes must be on disk first or power loss
    // leaves a torn sealed file.
    let durable_syncs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fs: Arc<dyn Fs> = Arc::new(DurableSyncCountingFs {
        inner: MockFs::new(),
        durable_syncs: Arc::clone(&durable_syncs),
    });
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut wal_actor = actor_over(&mut state, fs, 1, DurabilityPolicy::CloudAsync)?;
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        1,
        b"sealed",
        b"value",
        DurabilityPolicy::CloudAsync,
    )?;
    wal_actor.append_prepared_transactions(&mut state, vec![prepared])?;
    let syncs_before_seal = durable_syncs.load(std::sync::atomic::Ordering::SeqCst);
    let ticket = seal_ticket_for_test(&wal_actor, &state);

    // Act
    wal_actor.flush_for_cloud_upload(&mut state, &ticket)?;

    // Assert
    assert!(
        durable_syncs.load(std::sync::atomic::Ordering::SeqCst) > syncs_before_seal,
        "sealing a CloudAsync segment must fsync its bytes before the ledger seals it"
    );
    wal_actor.rotate(&mut state, &ticket)?;
    Ok(())
}

/// Whether `result` reports a logged unit whose outcome is unknown because
/// restart may apply it.
#[cfg(feature = "failpoints")]
fn may_be_applied_on_restart<T>(result: &MidgeResult<T>) -> bool {
    matches!(
        result,
        Err(MidgeError::OutcomeUnknown(message))
            if message.contains("is logged in the WAL")
                && message.contains("may be applied on restart")
    )
}

#[cfg(feature = "failpoints")]
#[test]
fn should_answer_fenced_when_lease_lapses_before_unlogged_unit_applies() -> MidgeResult<()> {
    // Arrange: a BestEffort unit has no WAL record. The lease lapses after
    // its sequences are allocated, at the apply of the group of one.
    let _guard = crate::failpoints::test_failpoint_guard();
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
    let validity = Arc::new(crate::lease::LeaseValidity::new());
    validity.activate(1, Instant::now() + Duration::from_mins(1))?;
    actor.set_lease_validity(Some(Arc::clone(&validity)));
    let sequence_before = state.sequence;
    let observed = Arc::clone(&validity);
    fail::cfg_callback("midge::wal::before_prepared_transaction_apply", move || {
        observed.expire_for_test();
    })
    .expect("lapse the lease before the apply");

    // Act
    let result = actor.append_transaction(
        &mut state,
        TransactionAppendParams {
            request_id: 343,
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"unlogged"),
                value: Bytes::from_static(b"value"),
                ttl_seconds: None,
                insert_only: false,
            }],
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::BestEffort),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    );
    fail::remove("midge::wal::before_prepared_transaction_apply");

    // Assert: sequences were allocated, yet nothing was logged or applied,
    // so the caller is told the unit is not applied.
    assert!(matches!(result, Err(MidgeError::Fenced(_))), "{result:?}");
    assert!(state.sequence > sequence_before);
    assert_eq!(state.get_cf(0).map(|cf| cf.memtable.size_bytes()), Some(0));
    assert!(state
        .get_cf(0)
        .is_some_and(|cf| cf.partially_applied_commit().is_none()));
    assert!(actor.is_fenced());
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fence_when_spilled_apply_fails_after_durable_wal_commit() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
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
    let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(16));
    let mut write_set = crate::runtime::transaction_spill::TransactionWriteSet::new(
        pool,
        &crate::io::real::test_root(temp.path()),
        false,
        1,
    );
    for key in [b"first".as_slice(), b"second".as_slice()] {
        write_set.push(crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: Bytes::copy_from_slice(key),
            value: Bytes::from_static(b"spilled-value"),
            ttl_seconds: None,
            insert_only: false,
        })?;
    }
    let source = write_set.take_source();
    fail::cfg("midge::wal::spilled_apply_op", "1*off->return")
        .expect("fail the second post-commit apply");

    // Act
    let result = actor.append_spilled_transaction(
        &mut state,
        &source,
        SpilledTransactionAppendParams {
            request_id: 339,
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::Strict),
            start_sequence: 0,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    );
    fail::remove("midge::wal::spilled_apply_op");

    // Assert: the WAL already holds the whole transaction, so a partial
    // memtable apply must stop the writer instead of serving it silently.
    assert!(may_be_applied_on_restart(&result), "{result:?}");
    assert!(actor.is_fenced());
    assert!(state.persistence_anomaly_detected());
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_report_discard_on_restart_when_best_effort_spill_partly_applies() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
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
    let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(16));
    let mut write_set = crate::runtime::transaction_spill::TransactionWriteSet::new(
        pool,
        &crate::io::real::test_root(temp.path()),
        false,
        1,
    );
    for key in [b"first".as_slice(), b"second".as_slice()] {
        write_set.push(crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: Bytes::copy_from_slice(key),
            value: Bytes::from_static(b"spilled-value"),
            ttl_seconds: None,
            insert_only: false,
        })?;
    }
    let source = write_set.take_source();
    fail::cfg("midge::wal::spilled_apply_op", "1*off->return").expect("fail the second apply");

    // Act
    let result = actor.append_spilled_transaction(
        &mut state,
        &source,
        SpilledTransactionAppendParams {
            request_id: 340,
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::BestEffort),
            start_sequence: 0,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    );
    fail::remove("midge::wal::spilled_apply_op");

    // Assert: a BestEffort unit has no WAL record, so restart cannot apply
    // it; the error must not promise that it will.
    let Err(MidgeError::RecoveryFailed(message)) = result else {
        panic!("expected RecoveryFailed, got {result:?}");
    };
    assert!(message.contains("restart discards all of it"), "{message}");
    assert!(!message.contains("fully applied on restart"), "{message}");
    assert!(state
        .get_cf(0)
        .is_some_and(|cf| cf.partially_applied_commit().is_some()));
    Ok(())
}

#[cfg(feature = "failpoints")]
const PARTIAL_SPILL_CHILD: &str = "MIDGE_PARTIAL_SPILL_FLUSH_CHILD";

#[cfg(feature = "failpoints")]
fn partial_spill_options(path: &std::path::Path) -> crate::OpenOptions {
    crate::OpenOptions::local(path)
        .transaction_memory_pool_size(1024)
        .background_compaction(false)
        .build()
        .expect("partial spill options")
}

#[cfg(feature = "failpoints")]
fn partial_spill_value() -> Vec<u8> {
    vec![7_u8; 4 * 1024]
}

#[cfg(feature = "failpoints")]
#[test]
fn should_exit_after_flushing_partly_applied_spilled_transaction_in_child() {
    // Arrange: runs only as the child of the restart test below, so the
    // process-global failpoint cannot reach another test.
    let Some(path) = std::env::var_os(PARTIAL_SPILL_CHILD) else {
        return;
    };
    let path = PathBuf::from(path);
    let mut engine = crate::Engine::open(partial_spill_options(&path)).expect("open spill writer");
    let family = engine.get_column_family("default").expect("default family");
    let mut tx = engine
        .begin_tx(family.id(), crate::TransactionMode::ReadWrite)
        .expect("spilled transaction");
    tx.put(b"first".to_vec(), partial_spill_value(), None)
        .expect("spill first");
    tx.put(b"second".to_vec(), partial_spill_value(), None)
        .expect("spill second");
    fail::cfg("midge::wal::spilled_apply_op", "1*off->return")
        .expect("fail the second post-commit apply");

    // Act: the strict commit is durably logged, but only `first` reaches the
    // memtable; a manual flush then persists that partial memtable before restart.
    let commit = tx.commit(crate::WriteOptions::sync());
    fail::remove("midge::wal::spilled_apply_op");
    assert!(may_be_applied_on_restart(&commit), "{commit:?}");
    let flushed = engine.flush_cf(&family);
    let _ = engine.shutdown(std::time::Duration::from_secs(30));

    // Assert
    std::fs::write(path.join("partial-spill-flushed"), format!("{flushed:?}"))
        .expect("flush marker");
    std::process::exit(76);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_apply_whole_spilled_transaction_on_restart_when_flushed_after_partial_apply() {
    // Arrange: a child process logs a spilled transaction, applies only its
    // first op, requests a manual flush of the family, and shuts down.
    let directory = tempfile::tempdir().expect("database directory");
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "runtime::actors::wal::tests::should_exit_after_flushing_partly_applied_spilled_transaction_in_child",
            "--nocapture",
        ])
        .env(PARTIAL_SPILL_CHILD, directory.path())
        .output()
        .expect("run partial spill child");
    assert_eq!(
        output.status.code(),
        Some(76),
        "child stdout: {}\nchild stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        directory.path().join("partial-spill-flushed").exists(),
        "the child must reach its manual flush"
    );

    // Act
    let mut reopened =
        crate::Engine::open(partial_spill_options(directory.path())).expect("restart");

    // Assert: the durably logged transaction is visible whole, never in part.
    {
        let tx = reopened
            .begin_tx(0, crate::TransactionMode::ReadOnly)
            .expect("read transaction");
        assert_eq!(
            tx.get(b"first").expect("read first").as_deref(),
            Some(partial_spill_value().as_slice())
        );
        assert_eq!(
            tx.get(b"second").expect("read second").as_deref(),
            Some(partial_spill_value().as_slice())
        );
    }
    reopened
        .shutdown(std::time::Duration::from_secs(30))
        .expect("shutdown restarted engine");
}

#[cfg(feature = "failpoints")]
const PARTIAL_SPILL_WRITE_CHILD: &str = "MIDGE_PARTIAL_SPILL_WRITE_CHILD";

#[cfg(feature = "failpoints")]
fn put_one(
    engine: &crate::Engine,
    family: &crate::ColumnFamilyHandle,
    key: &[u8],
    options: crate::WriteOptions,
) -> MidgeResult<()> {
    let mut tx = engine.begin_tx(family.id(), crate::TransactionMode::ReadWrite)?;
    tx.put(key.to_vec(), b"after-partial-apply".to_vec(), None)?;
    tx.commit(options)
}

#[cfg(feature = "failpoints")]
#[test]
fn should_exit_after_writing_to_partly_applied_family_in_child() {
    // Arrange: runs only as the child of the rejection test below, so the
    // process-global failpoint cannot reach another test.
    let Some(path) = std::env::var_os(PARTIAL_SPILL_WRITE_CHILD) else {
        return;
    };
    let path = PathBuf::from(path);
    let mut engine = crate::Engine::open(partial_spill_options(&path)).expect("open spill writer");
    let family = engine.get_column_family("default").expect("default family");
    let other = engine.create_column_family("other").expect("other family");
    let mut tx = engine
        .begin_tx(family.id(), crate::TransactionMode::ReadWrite)
        .expect("spilled transaction");
    tx.put(b"first".to_vec(), partial_spill_value(), None)
        .expect("spill first");
    tx.put(b"second".to_vec(), partial_spill_value(), None)
        .expect("spill second");
    fail::cfg("midge::wal::spilled_apply_op", "1*off->return")
        .expect("fail the second post-commit apply");
    let commit = tx.commit(crate::WriteOptions::sync());
    fail::remove("midge::wal::spilled_apply_op");
    assert!(may_be_applied_on_restart(&commit), "{commit:?}");

    // Act: the other family's write goes first, because a write refused
    // while the WAL is fenced may stop the whole runtime from writing.
    let best_effort_other = put_one(
        &engine,
        &other,
        b"best-effort",
        crate::WriteOptions::best_effort(),
    );
    let best_effort_same = put_one(
        &engine,
        &family,
        b"best-effort",
        crate::WriteOptions::best_effort(),
    );
    let sync_same = put_one(&engine, &family, b"sync", crate::WriteOptions::sync());
    let flushed = engine.flush_cf(&family);

    // Assert: the marked family cannot freeze until restart, so it must not
    // acknowledge a write it could never persist; the other family, which
    // holds no partly applied unit, still takes the writes the fenced WAL
    // allows.
    assert!(best_effort_other.is_ok(), "{best_effort_other:?}");
    assert!(
        matches!(best_effort_same, Err(MidgeError::RecoveryFailed(_))),
        "{best_effort_same:?}"
    );
    assert!(
        matches!(
            sync_same,
            Err(MidgeError::RecoveryFailed(_) | MidgeError::Fenced(_))
        ),
        "{sync_same:?}"
    );
    assert!(flushed.is_err(), "{flushed:?}");
    let _ = engine.shutdown(std::time::Duration::from_secs(30));
    std::process::exit(76);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_reject_best_effort_write_when_family_holds_partly_applied_spilled_transaction() {
    // Arrange
    let directory = tempfile::tempdir().expect("database directory");

    // Act: a child process partly applies a durably logged spilled
    // transaction, then writes to the marked family and to another one.
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "runtime::actors::wal::tests::should_exit_after_writing_to_partly_applied_family_in_child",
            "--nocapture",
        ])
        .env(PARTIAL_SPILL_WRITE_CHILD, directory.path())
        .output()
        .expect("run partial spill write child");

    // Assert
    assert_eq!(
        output.status.code(),
        Some(76),
        "child stdout: {}\nchild stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "failpoints")]
const PARTIAL_SPILL_STALL_CHILD: &str = "MIDGE_PARTIAL_SPILL_STALL_CHILD";

#[cfg(feature = "failpoints")]
const PARTIAL_SPILL_STALL_THRESHOLD: usize = 16 * 1024;

#[cfg(feature = "failpoints")]
#[test]
fn should_exit_after_writing_beside_large_partly_applied_family_in_child() {
    // Arrange: runs only as the child of the stall test below, so the
    // process-global failpoint cannot reach another test.
    let Some(path) = std::env::var_os(PARTIAL_SPILL_STALL_CHILD) else {
        return;
    };
    let path = PathBuf::from(path);
    let options = crate::OpenOptions::local(&path)
        .transaction_memory_pool_size(1024)
        .with_memtable_flush_threshold(PARTIAL_SPILL_STALL_THRESHOLD)
        .background_compaction(false)
        .build()
        .expect("stall options");
    let mut engine = crate::Engine::open(options).expect("open spill writer");
    let family = engine.get_column_family("default").expect("default family");
    let other = engine.create_column_family("other").expect("other family");
    let mut tx = engine
        .begin_tx(family.id(), crate::TransactionMode::ReadWrite)
        .expect("spilled transaction");
    for key in [b"first".as_slice(), b"second", b"third", b"fourth"] {
        tx.put(
            key.to_vec(),
            vec![7_u8; PARTIAL_SPILL_STALL_THRESHOLD],
            None,
        )
        .expect("spill op");
    }
    fail::cfg("midge::wal::spilled_apply_op", "3*off->return")
        .expect("fail the fourth post-commit apply");
    let commit = tx.commit(crate::WriteOptions::sync());
    fail::remove("midge::wal::spilled_apply_op");
    assert!(may_be_applied_on_restart(&commit), "{commit:?}");

    // Act: a stall hint on the first reply makes the second check the stall.
    let written = [b"best-effort-1".as_slice(), b"best-effort-2"]
        .map(|key| put_one(&engine, &other, key, crate::WriteOptions::best_effort()));
    let cleared = engine.wait_for_write_stall_clear(other.id(), std::time::Duration::from_secs(1));

    // Assert: the marked family holds more than twice the flush threshold
    // but takes no writes and cannot flush until restart; it must not stall
    // a family that can.
    assert!(written.iter().all(Result::is_ok), "{written:?}");
    assert!(matches!(cleared, Ok(true)), "{cleared:?}");
    let _ = engine.shutdown(std::time::Duration::from_secs(30));
    std::process::exit(76);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_accept_write_beside_family_holding_large_partly_applied_spilled_transaction() {
    // Arrange
    let directory = tempfile::tempdir().expect("database directory");

    // Act: a child process partly applies a spilled transaction larger than
    // twice the flush threshold, then writes to another family.
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "runtime::actors::wal::tests::should_exit_after_writing_beside_large_partly_applied_family_in_child",
            "--nocapture",
        ])
        .env(PARTIAL_SPILL_STALL_CHILD, directory.path())
        .output()
        .expect("run partial spill stall child");

    // Assert
    assert_eq!(
        output.status.code(),
        Some(76),
        "child stdout: {}\nchild stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "failpoints")]
const FAILED_SYNC_CHILD: &str = "MIDGE_FAILED_SYNC_FLUSH_CHILD";

#[cfg(feature = "failpoints")]
const FAILED_SYNC_KEYS: [&[u8]; 2] = [b"failed-first", b"failed-second"];

#[cfg(feature = "failpoints")]
#[test]
fn should_exit_after_flushing_past_failed_strict_sync_in_child() {
    // Arrange: runs only as the child of the flush test below, so the
    // process-global failpoint cannot reach another test.
    let Some(path) = std::env::var_os(FAILED_SYNC_CHILD) else {
        return;
    };
    let path = PathBuf::from(path);
    let mut engine = crate::Engine::open(partial_spill_options(&path)).expect("open writer");
    let family = engine.get_column_family("default").expect("default family");
    put_one(
        &engine,
        &family,
        b"best-effort",
        crate::WriteOptions::best_effort(),
    )
    .expect("best-effort put");
    let mut tx = engine
        .begin_tx(family.id(), crate::TransactionMode::ReadWrite)
        .expect("strict transaction");
    for key in FAILED_SYNC_KEYS {
        tx.put(key.to_vec(), partial_spill_value(), None)
            .expect("stage failed key");
    }
    fail::cfg("midge::wal::inject_no_space_on_sync", "return").expect("fail the strict sync");
    let commit = tx.commit(crate::WriteOptions::sync());
    fail::remove("midge::wal::inject_no_space_on_sync");
    assert!(
        matches!(commit, Err(MidgeError::OutcomeUnknown(_))),
        "{commit:?}"
    );

    // Act
    let flushed = engine.flush_cf(&family);
    let _ = engine.shutdown(std::time::Duration::from_secs(30));

    // Assert
    std::fs::write(path.join("failed-sync-flushed"), format!("{flushed:?}")).expect("flush marker");
    std::process::exit(76);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_publish_best_effort_write_when_flushed_after_failed_strict_sync() {
    // Arrange: a child process writes a best-effort key, has a strict
    // spilled commit fail on its sync after the WAL append, flushes the
    // family and shuts down.
    let directory = tempfile::tempdir().expect("database directory");
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "runtime::actors::wal::tests::should_exit_after_flushing_past_failed_strict_sync_in_child",
            "--nocapture",
        ])
        .env(FAILED_SYNC_CHILD, directory.path())
        .output()
        .expect("run failed sync child");
    assert_eq!(
        output.status.code(),
        Some(76),
        "child stdout: {}\nchild stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let flushed = std::fs::read_to_string(directory.path().join("failed-sync-flushed"))
        .expect("flush marker");

    // Act
    let mut reopened =
        crate::Engine::open(partial_spill_options(directory.path())).expect("restart");

    // Assert: none of the failed unit reached the memtable, so a flush past
    // its commit drops it whole on replay; the best-effort write the flush
    // published survives.
    assert_eq!(flushed, "Ok(())");
    {
        let tx = reopened
            .begin_tx(0, crate::TransactionMode::ReadOnly)
            .expect("read transaction");
        assert_eq!(
            tx.get(b"best-effort").expect("read best-effort").as_deref(),
            Some(b"after-partial-apply".as_slice())
        );
        let present = FAILED_SYNC_KEYS
            .iter()
            .map(|key| tx.get(key).expect("read failed key").is_some())
            .collect::<Vec<_>>();
        assert!(
            present.iter().all(|found| *found) || present.iter().all(|found| !found),
            "{present:?}"
        );
    }
    reopened
        .shutdown(std::time::Duration::from_secs(30))
        .expect("shutdown restarted engine");
}

#[test]
fn should_reject_resident_transaction_when_batch_payload_exceeds_wal_decode_limit(
) -> MidgeResult<()> {
    // Arrange: each value is under the per-entry limit, but the batch payload
    // is not. It compresses well, so the old frame-size check let it through
    // and replay then rejected the acknowledged record.
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let db_path = temp.path().to_path_buf();
    let mut state = RuntimeState::new(db_path.clone(), false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let half = crate::wal::frame::WAL_MAX_VALUE_LEN / 2 + 1;
    let sequence_before = state.sequence;
    let put = |key: &'static [u8]| crate::runtime::TransactionOp::Put {
        cf_id: 0,
        key: Bytes::from_static(key),
        value: Bytes::from(vec![0_u8; half]),
        ttl_seconds: None,
        insert_only: false,
    };

    // Act
    let result = wal_actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 333,
            assertions: Vec::new(),
            ops: vec![put(b"a"), put(b"b")],
            durability_policy: Some(DurabilityPolicy::Batched),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
        },
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::ResourceLimit(_))),
        "an undecodable batch must be rejected before it is written: {:?}",
        result.as_ref().err()
    );
    assert_eq!(
        state.sequence, sequence_before,
        "a rejected batch must not consume sequences"
    );
    prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        334,
        b"small",
        b"value",
        DurabilityPolicy::Batched,
    )?;
    Ok(())
}

#[test]
fn should_build_one_read_snapshot_per_cf_when_validating_insert_only_conflict_checks(
) -> MidgeResult<()> {
    // Arrange: every op used to rebuild a full read snapshot (manifest clone,
    // read view, uncached SST opens) on the event-loop thread.
    let mut state = RuntimeState::new("/tmp/test_midge_validation_snapshots".into(), true);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::BestEffort,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let ops = (0..100_u32)
        .map(|index| crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from(format!("key-{index}")),
            value: Bytes::from_static(b"value"),
            ttl_seconds: None,
            insert_only: true,
        })
        .collect();
    WalActor::reset_assertion_snapshot_build_count();

    // Act
    wal_actor.prepare_transaction_append(
        &mut state,
        TransactionAppendParams {
            request_id: 364,
            assertions: Vec::new(),
            ops,
            durability_policy: Some(DurabilityPolicy::BestEffort),
            start_sequence: Some(0),
            conflict_policy: crate::runtime::ConflictPolicy::AbortOnWriteConflict,
        },
    )?;

    // Assert
    assert_eq!(WalActor::assertion_snapshot_build_count(), 1);
    Ok(())
}

/// Counts SST file opens through the filesystem, whichever read path does them.
struct SstOpenCountingFs {
    inner: crate::io::RealFs,
    sst_opens: std::sync::atomic::AtomicUsize,
}

impl crate::io::ReadFs for SstOpenCountingFs {
    fn open_read(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<Box<dyn crate::io::ReadFile + '_>> {
        crate::io::Fs::open(self, path, crate::io::OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn crate::io::ReadFile>)
    }

    fn exists(&self, path: &crate::io::FsPath) -> crate::io::FsResult<bool> {
        self.inner.exists(path)
    }

    fn metadata(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<crate::io::traits::Metadata> {
        self.inner.metadata(path)
    }

    fn list_dir(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
        self.inner.list_dir(path)
    }
}

impl crate::io::Fs for SstOpenCountingFs {
    fn try_lock_exclusive(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<Box<dyn crate::io::FsLock>> {
        self.inner.try_lock_exclusive(path)
    }

    fn open(
        &self,
        path: &crate::io::FsPath,
        opts: crate::io::OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
        if std::path::Path::new(&path.0)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))
        {
            self.sst_opens
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        self.inner.open(path, opts)
    }
    fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.inner.remove_file(path)
    }
    fn create_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.inner.create_dir_all(path)
    }
    fn remove_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.inner.remove_dir_all(path)
    }
    fn sync_dir(
        &self,
        path: &crate::io::FsPath,
        durability: crate::io::Durability,
    ) -> crate::io::FsResult<()> {
        self.inner.sync_dir(path, durability)
    }
    fn rename_atomic(
        &self,
        from: &crate::io::FsPath,
        to: &crate::io::FsPath,
    ) -> crate::io::FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

/// SST opens to validate a 100-key `AbortOnWriteConflict` transaction that
/// asserts every tenth key, over four published SSTs of 25 keys each,
/// validating `keys_per_file` keys from each file.
fn sst_opens_to_validate(keys_per_file: usize) -> MidgeResult<usize> {
    use crate::sst::SstFactory;
    let temp = tempfile::tempdir()?;
    let counting = Arc::new(SstOpenCountingFs {
        inner: crate::io::RealFs::new(temp.path()).map_err(FsError::into_midge)?,
        sst_opens: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    state.fs = counting.clone();
    std::fs::create_dir_all(temp.path().join("sst"))?;
    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut keys = Vec::new();
    for file in 0..4_u64 {
        let mut writer = factory.create()?;
        let file_keys: Vec<Vec<u8>> = (0..25)
            .map(|index| format!("k{file}{index:02}").into_bytes())
            .collect();
        for key in &file_keys {
            writer.add_with_meta(key, Some(b"value"), 1, EntryType::Put, None)?;
        }
        let bytes = writer.finish_bytes()?;
        let name = crate::cloud_layout::file_name(0, 0, file + 1);
        std::fs::write(temp.path().join("sst").join(&name), &bytes)?;
        state
            .manifest
            .test_mut()
            .files
            .push(crate::metadata::FileMeta {
                name,
                cf_id: 0,
                level: 1,
                size_bytes: bytes.len() as u64,
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                smallest_key: file_keys.first().cloned(),
                largest_key: file_keys.last().cloned(),
                smallest_seq: Some(1),
                largest_seq: Some(1),
                key_bounds_complete: true,
                ..Default::default()
            });
        keys.extend(file_keys.into_iter().take(keys_per_file));
    }
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let counting_fs: Arc<dyn crate::io::Fs> = counting.clone();
    actor.set_read_resources(Some(Arc::new(
        crate::runtime::read_resources::ReadResources::new_with_diagnostics(
            counting_fs,
            std::path::PathBuf::from("sst"),
            64 * 1024,
            crate::sst::cache::CachePolicyType::Lru,
            Arc::new(crate::diagnostics::RuntimeDiagnostics::default()),
        ),
    )));
    state.sequence = 1_000;
    let ops = keys
        .iter()
        .map(|key| crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: Bytes::copy_from_slice(key),
            value: Bytes::from_static(b"new"),
            ttl_seconds: None,
            insert_only: false,
        })
        .collect();
    let assertions = keys
        .iter()
        .step_by(10)
        .map(|key| crate::runtime::KeyAssertion {
            cf_id: 0,
            key: Bytes::copy_from_slice(key),
        })
        .collect();
    actor.append_transaction(
        &mut state,
        TransactionAppendParams {
            request_id: 1,
            assertions,
            ops,
            durability_policy: Some(DurabilityPolicy::BestEffort),
            start_sequence: Some(1_000),
            conflict_policy: crate::runtime::ConflictPolicy::AbortOnWriteConflict,
        },
    )?;
    Ok(counting.sst_opens.load(std::sync::atomic::Ordering::SeqCst))
}

#[test]
fn should_reuse_cached_sst_readers_when_validating_conflicting_transaction_ops() -> MidgeResult<()>
{
    // Arrange: one validated key per file, then 25 per file.
    let (few, many) = (1, 25);

    // Act
    let one_key_per_file = sst_opens_to_validate(few)?;
    let every_key = sst_opens_to_validate(many)?;

    // Assert: SST opens depend on the files read, not on the op count.
    assert_eq!(
        every_key, one_key_per_file,
        "{every_key} SST opens for 100 ops against {one_key_per_file} for 4 ops"
    );
    Ok(())
}

#[test]
fn should_fence_as_local_failure_when_wal_state_changes_under_a_sync_receipt() -> MidgeResult<()> {
    // Arrange: a receipt whose WAL state moved before commit is a local
    // invariant failure, not lost authority (#537).
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut wal_actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        1,
        b"receipt",
        b"value",
        DurabilityPolicy::Batched,
    )?;
    wal_actor.append_prepared_transactions(&mut state, vec![prepared])?;
    let sync_ticket = WalSyncTicket::for_test(0);
    let receipt = wal_actor.begin_sync_transition(&mut state, &sync_ticket)?;
    state.sequence += 1;

    // Act
    let error = wal_actor
        .commit_sync_transition(&mut state, receipt, &sync_ticket)
        .expect_err("a moved WAL state must reject the receipt");

    // Assert
    assert!(matches!(error, MidgeError::Internal(_)), "{error:?}");
    assert!(wal_actor.is_fenced());
    assert!(!wal_actor.authority_lost());
    Ok(())
}

#[test]
fn should_not_regress_local_durable_seq_when_older_sync_receipt_completes_after_newer(
) -> MidgeResult<()> {
    // Arrange: the local frontier has already reached 20.
    let mut state = RuntimeState::new("/tmp/test_midge_frontier_regression".into(), true);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::BestEffort,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    state.wal.frontiers.set_last_synced_for_test(20);
    state.wal.frontiers.set_local_durable_for_test(20);

    // Act: a seal receipt for an older segment completes afterwards.
    actor.complete_cloud_upload_seal(
        &mut state,
        crate::runtime::actors::wal::WalRotationReceipt::for_test(1, 2, 10),
    );

    // Assert
    assert_eq!(state.wal.frontiers.local_durable(), 20);
    assert_eq!(state.wal.frontiers.last_synced(), 20);
    Ok(())
}

#[test]
fn should_reject_prepared_group_when_validity_expires_before_append() -> MidgeResult<()> {
    // Arrange
    for policy in [
        DurabilityPolicy::BestEffort,
        DurabilityPolicy::CloudAsync,
        DurabilityPolicy::Strict,
    ] {
        let temp = tempfile::tempdir()?;
        let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
        let mut actor = WalActor::open(
            &state,
            policy,
            BatchConfig::default(),
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        let validity = Arc::new(crate::lease::LeaseValidity::new());
        validity.activate(1, Instant::now() + Duration::from_mins(1))?;
        actor.set_lease_validity(Some(Arc::clone(&validity)));
        let first = prepare_put_transaction(&mut actor, &mut state, 1, b"first", b"value", policy)?;
        let second =
            prepare_put_transaction(&mut actor, &mut state, 2, b"second", b"value", policy)?;
        validity.expire_for_test();
        // Act
        let result = actor.append_prepared_transactions(&mut state, vec![first, second]);
        // Assert
        assert!(matches!(result, Err(MidgeError::Fenced(_))));
        assert_eq!(actor.append_calls(), 0);
        assert_eq!(state.get_cf(0).unwrap().memtable.iter_all().len(), 0);
        assert_eq!(
            std::fs::metadata(
                temp.path()
                    .join("wal")
                    .join(crate::wal::segment_file_name(1))
            )?
            .len(),
            crate::wal::format::HEADER_LEN as u64
        );
    }
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_reject_write_when_validity_expires_during_preparation() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    for policy in [DurabilityPolicy::BestEffort, DurabilityPolicy::CloudAsync] {
        for spilled in [false, true] {
            let temp = tempfile::tempdir()?;
            let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
            let mut actor = WalActor::open(
                &state,
                policy,
                BatchConfig::default(),
                1,
                crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
            )?;
            let validity = Arc::new(crate::lease::LeaseValidity::new());
            validity.activate(1, Instant::now() + Duration::from_mins(1))?;
            actor.set_lease_validity(Some(Arc::clone(&validity)));
            let trigger = if spilled {
                "midge::wal::after_spilled_preparation"
            } else {
                "midge::wal::after_transaction_preparation"
            };
            let observed = Arc::clone(&validity);
            fail::cfg_callback(trigger, move || observed.expire_for_test()).unwrap();
            // Act
            let result = if spilled {
                let pool =
                    Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(0));
                let mut writes = crate::runtime::transaction_spill::TransactionWriteSet::new(
                    pool,
                    &crate::io::real::test_root(temp.path()),
                    false,
                    1,
                );
                writes.push(crate::runtime::TransactionOp::Put {
                    cf_id: 0,
                    key: Bytes::from_static(b"key"),
                    value: Bytes::from_static(b"value"),
                    ttl_seconds: None,
                    insert_only: false,
                })?;
                actor.append_spilled_transaction(
                    &mut state,
                    &writes.take_source(),
                    SpilledTransactionAppendParams {
                        request_id: 1,
                        assertions: Vec::new(),
                        durability_policy: Some(policy),
                        start_sequence: 0,
                        conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
                    },
                )
            } else {
                prepare_put_transaction(&mut actor, &mut state, 1, b"key", b"value", policy)
                    .map(|_| (0, 0, false))
            };
            fail::remove(trigger);
            // Assert
            assert!(matches!(result, Err(MidgeError::Fenced(_))));
            assert_eq!(actor.append_calls(), 0);
            assert_eq!(state.get_cf(0).unwrap().memtable.iter_all().len(), 0);
            assert_eq!(
                std::fs::metadata(
                    temp.path()
                        .join("wal")
                        .join(crate::wal::segment_file_name(1))
                )?
                .len(),
                crate::wal::format::HEADER_LEN as u64
            );
        }
    }
    scenario.teardown();
    Ok(())
}

#[test]
fn should_retain_physical_writer_validity_after_rotation() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let validity = Arc::new(crate::lease::LeaseValidity::new());
    validity
        .activate(1, Instant::now() + Duration::from_mins(1))
        .unwrap();
    actor.set_lease_validity(Some(Arc::clone(&validity)));
    let ticket = seal_ticket_for_test(&actor, &state);
    actor.rotate(&mut state, &ticket)?;
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(Bytes::from_static(b"value")),
        1,
    );
    // Act
    validity.expire_for_test();
    let result = actor.writer().unwrap().append_record_accounted(&record);
    // Assert
    assert!(matches!(
        result,
        Err(crate::wal::traits::WalAppendError {
            error: MidgeError::Fenced(_),
            unchanged: true
        })
    ));
    assert_eq!(
        std::fs::metadata(
            temp.path()
                .join("wal")
                .join(crate::wal::segment_file_name(2))
        )?
        .len(),
        crate::wal::format::HEADER_LEN as u64
    );
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_preserve_open_segment_when_validity_expires_before_next_segment_create() -> MidgeResult<()>
{
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let validity = Arc::new(crate::lease::LeaseValidity::new());
    validity
        .activate(1, Instant::now() + Duration::from_mins(1))
        .unwrap();
    actor.set_lease_validity(Some(Arc::clone(&validity)));
    let ticket = seal_ticket_for_test(&actor, &state);
    let expire = Arc::clone(&validity);
    fail::cfg_callback("midge::wal::inject_fail_before_segment_create", move || {
        expire.expire_for_test();
    })
    .unwrap();
    let active = temp
        .path()
        .join("wal")
        .join(crate::wal::segment_file_name(1));
    let before = std::fs::read(&active)?;
    // Act
    let result = actor.rotate(&mut state, &ticket);
    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))));
    assert_eq!(std::fs::read(&active)?, before);
    assert!(!temp
        .path()
        .join("wal")
        .join(crate::wal::segment_file_name(2))
        .exists());
    drop(scenario);
    Ok(())
}

#[test]
fn should_settle_cloud_flush_transition_when_validity_expires_before_rotation() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let validity = Arc::new(crate::lease::LeaseValidity::new());
    validity
        .activate(1, Instant::now() + Duration::from_mins(1))
        .unwrap();
    actor.set_lease_validity(Some(Arc::clone(&validity)));
    let ticket = seal_ticket_for_test(&actor, &state);
    actor.flush_for_cloud_upload(&mut state, &ticket)?;
    let active = temp
        .path()
        .join("wal")
        .join(crate::wal::segment_file_name(1));
    let before = std::fs::read(&active)?;
    validity.expire_for_test();
    // Act
    let result = actor.rotate(&mut state, &ticket);
    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))));
    assert!(actor.is_open() || actor.is_fenced());
    assert_eq!(std::fs::read(&active)?, before);
    assert!(!temp
        .path()
        .join("wal")
        .join(crate::wal::segment_file_name(2))
        .exists());
    Ok(())
}

#[test]
fn should_fence_when_footer_write_fails() -> MidgeResult<()> {
    // Arrange: segment 7 holds a record; the footer write lands half its
    // bytes and fails, after the next segment was created.
    let temp = tempfile::tempdir().map_err(crate::common::MidgeError::Io)?;
    let fs = Arc::new(admission::PartialAppendFs::default());
    let mut state = RuntimeState::new(temp.path().to_path_buf(), true);
    let mut wal_actor = actor_over(
        &mut state,
        Arc::clone(&fs) as Arc<dyn Fs>,
        7,
        DurabilityPolicy::Strict,
    )?;
    let prepared = prepare_put_transaction(
        &mut wal_actor,
        &mut state,
        1,
        b"key",
        b"value",
        DurabilityPolicy::Strict,
    )?;
    wal_actor.append_prepared_transactions(&mut state, vec![prepared])?;
    let ticket = seal_ticket_for_test(&wal_actor, &state);
    fs.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);

    // Act
    let result = wal_actor.rotate(&mut state, &ticket);

    // Assert: a torn footer cannot be taken back, so the actor fences and
    // the ledger keeps segment 7 open for restart recovery to seal.
    assert!(result.is_err());
    assert!(wal_actor.is_fenced());
    assert!(state.persistence_anomaly_detected());
    assert_eq!(state.wal.open_segment(), 7);
    assert!(matches!(
        wal_actor.rotate(&mut state, &ticket),
        Err(MidgeError::RecoveryFailed(_))
    ));
    Ok(())
}

/// A file-backed actor over a fresh database with a second family.
fn actor_with_second_family(
    temp: &tempfile::TempDir,
) -> MidgeResult<(RuntimeState, WalActor, u32)> {
    let mut state = RuntimeState::new(temp.path().to_path_buf(), false);
    let family = state.create_cf("second".into())?;
    let actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    Ok((state, actor, family))
}

fn put_in(cf_id: u32, key: &'static [u8]) -> crate::runtime::TransactionOp {
    crate::runtime::TransactionOp::Put {
        cf_id,
        key: Bytes::from_static(key),
        value: Bytes::from_static(b"value"),
        ttl_seconds: None,
        insert_only: false,
    }
}

fn transaction(ops: Vec<crate::runtime::TransactionOp>) -> TransactionAppendParams {
    TransactionAppendParams {
        request_id: 1,
        assertions: Vec::new(),
        ops,
        durability_policy: Some(DurabilityPolicy::Strict),
        start_sequence: None,
        conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
    }
}

#[test]
fn should_stamp_transaction_batch_with_its_family() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let (mut state, mut actor, family) = actor_with_second_family(&temp)?;
    let prepared = actor.prepare_transaction_append(
        &mut state,
        transaction(vec![put_in(family, b"a"), put_in(family, b"b")]),
    )?;

    // Act
    actor.append_prepared_transactions(&mut state, vec![prepared])?;

    // Assert: the batch frame and every operation in it name the family.
    let segment = state
        .host_wal_dir()
        .join(crate::wal::segment_file_name(state.wal.open_segment()));
    let records = crate::wal::format::segment::records(&std::fs::read(segment)?)?;
    let [batch] = records.as_slice() else {
        panic!("one transaction frame: {records:?}");
    };
    assert_eq!(batch.op, crate::wal::WalOpKind::TxnBatch);
    assert_eq!(batch.cf_id, family);
    let payload = batch.value.as_ref().expect("batch payload");
    let decoded = crate::wal::encoding::decode_txn_batch_payload(batch, payload)?;
    assert_eq!(decoded.records.len(), 2);
    assert!(decoded.records.iter().all(|record| record.cf_id == family));
    Ok(())
}

#[test]
fn should_reject_transaction_spanning_families_before_allocating() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let (mut state, mut actor, family) = actor_with_second_family(&temp)?;
    let sequence_before = state.sequence;
    let segment = state
        .host_wal_dir()
        .join(crate::wal::segment_file_name(state.wal.open_segment()));
    let bytes_before = std::fs::read(&segment)?;

    // Act
    let result = actor.prepare_transaction_append(
        &mut state,
        transaction(vec![put_in(0, b"a"), put_in(family, b"b")]),
    );

    // Assert: no sequence was handed out and no frame written.
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    assert_eq!(state.sequence, sequence_before);
    assert_eq!(std::fs::read(&segment)?, bytes_before);
    Ok(())
}
