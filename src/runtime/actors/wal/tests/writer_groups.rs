use super::*;
use crate::lease::{fs_leader_store::FsLeaderStore, LeaderStore};
use crate::runtime::wal_history::{WalHistory, WalHistoryFence};
use std::sync::atomic::{AtomicUsize, Ordering};

struct FenceCountingFs {
    inner: MockFs,
    locks: AtomicUsize,
    leader_reads: AtomicUsize,
}

impl ReadFs for FenceCountingFs {
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

impl Fs for FenceCountingFs {
    fn try_lock_exclusive(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::FsLock>> {
        let lock = self.inner.try_lock_exclusive(path)?;
        self.locks.fetch_add(1, Ordering::SeqCst);
        Ok(lock)
    }
    fn open(&self, path: &FsPath, options: FsOpenOptions) -> FsResult<Box<dyn File + '_>> {
        if path.0 == ".midge_leader" && options.mode == crate::io::OpenMode::ReadOnly {
            self.leader_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.open(path, options)
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

struct FencedActor {
    fs: Arc<FenceCountingFs>,
    store: Arc<FsLeaderStore>,
    wal: Arc<dyn Fs>,
    state: RuntimeState,
    actor: WalActor,
}

fn fenced_actor() -> MidgeResult<FencedActor> {
    let fs = Arc::new(FenceCountingFs {
        inner: MockFs::new(),
        locks: AtomicUsize::new(0),
        leader_reads: AtomicUsize::new(0),
    });
    let root: Arc<dyn Fs> = fs.clone();
    let store = Arc::new(FsLeaderStore::new(
        root.clone(),
        crate::lease::LeaseClock::default(),
    ));
    let lease = store.acquire_leadership("writer").unwrap();
    let fence = WalHistoryFence::new(
        store.clone(),
        "writer".into(),
        lease.epoch,
        Duration::from_secs(2),
    );
    let wal = crate::io::subdirectory(root, crate::wal::WAL_DIR);
    let mut state = RuntimeState::new(PathBuf::from("/writer-group"), true);
    state.wal.history = WalHistory::open_with_fence(wal.clone(), 1, lease.epoch, &fence)?;
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::Strict,
        BatchConfig::default(),
        lease.epoch,
        Duration::from_secs(2),
    )?;
    actor.set_leader_store(store.clone(), "writer".into());
    Ok(FencedActor {
        fs,
        store,
        wal,
        state,
        actor,
    })
}

#[test]
fn should_reuse_one_native_fence_when_strict_group_appends_syncs_and_records_history(
) -> MidgeResult<()> {
    // Arrange: actor and history share the production filesystem authority.
    let FencedActor {
        fs,
        store,
        wal,
        mut state,
        mut actor,
    } = fenced_actor()?;
    let prepared = prepare_put_transaction(
        &mut actor,
        &mut state,
        1,
        b"key",
        b"value",
        DurabilityPolicy::Strict,
    )?;
    fs.locks.store(0, Ordering::SeqCst);
    fs.leader_reads.store(0, Ordering::SeqCst);

    // Act
    let results = actor.append_prepared_transactions(&mut state, vec![prepared])?;

    // Assert: one proof protects all three effects and the acknowledged bytes
    // remain in the authoritative history after reopening with a successor.
    assert_eq!(
        (
            fs.locks.load(Ordering::SeqCst),
            fs.leader_reads.load(Ordering::SeqCst)
        ),
        (1, 1)
    );
    assert_eq!(results.len(), 1);
    assert_eq!(state.get_cf(0).unwrap().memtable.iter_all().len(), 1);
    let durable = state
        .wal
        .history
        .ledger()
        .open_member()
        .durable_bytes
        .unwrap();
    assert!(durable > crate::wal::format::HEADER_LEN as u64);
    drop(actor);
    let successor = store.acquire_leadership("successor").unwrap();
    let successor_fence = WalHistoryFence::new(
        store,
        "successor".into(),
        successor.epoch,
        Duration::from_secs(2),
    );
    let reopened = WalHistory::open_with_fence(wal.clone(), 1, successor.epoch, &successor_fence)?;
    assert_eq!(reopened.ledger().open_member().durable_bytes, Some(durable));
    let bytes = crate::io::read_all(wal.as_ref(), &FsPath::new(crate::wal::segment_file_name(1)))
        .map_err(FsError::into_midge)?;
    let records = crate::wal::format::segment::records(&bytes)?;
    assert_eq!(records.len(), 1);
    let batch = crate::wal::encoding::decode_txn_batch_payload(
        &records[0],
        records[0].value.as_ref().unwrap(),
    )?;
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].key.as_ref(), b"key");
    assert_eq!(batch.records[0].value.as_ref().unwrap().as_ref(), b"value");
    Ok(())
}

#[test]
fn should_fence_strict_actor_when_successor_precedes_native_group_admission() -> MidgeResult<()> {
    // Arrange
    let FencedActor {
        store,
        wal,
        mut state,
        mut actor,
        ..
    } = fenced_actor()?;
    let prepared = prepare_put_transaction(
        &mut actor,
        &mut state,
        1,
        b"key",
        b"value",
        DurabilityPolicy::Strict,
    )?;
    store.acquire_leadership("successor").unwrap();
    let path = FsPath::new(crate::wal::segment_file_name(1));
    let before = crate::io::read_all(wal.as_ref(), &path).map_err(FsError::into_midge)?;

    // Act
    let rejected = actor.append_prepared_transactions(&mut state, vec![prepared]);

    // Assert
    assert!(matches!(rejected, Err(MidgeError::Fenced(_))));
    assert!(actor.is_fenced());
    assert!(actor.authority_lost());
    assert!(state.persistence_anomaly_detected());
    assert_eq!(
        state.get_cf(0).unwrap().memtable.iter_all(),
        Vec::<(Vec<u8>, Option<Vec<u8>>, u64)>::new()
    );
    assert_eq!(
        crate::io::read_all(wal.as_ref(), &path).map_err(FsError::into_midge)?,
        before
    );
    Ok(())
}

#[test]
fn should_remain_retryable_when_native_strict_group_admission_times_out() -> MidgeResult<()> {
    // Arrange: a closed predecessor grant has one already admitted job left.
    let FencedActor {
        store,
        wal,
        mut state,
        mut actor,
        ..
    } = fenced_actor()?;
    let prepared = prepare_put_transaction(
        &mut actor,
        &mut state,
        1,
        b"key",
        b"value",
        DurabilityPolicy::Strict,
    )?;
    actor.storage_io_timeout = Duration::from_millis(10);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let pinned = std::thread::spawn(move || {
        let root = store
            .writer_effect_groups()
            .unwrap()
            .begin_group("writer", 1, Duration::from_secs(2))
            .unwrap();
        let ticket = root.reserve_ticket(Duration::from_secs(2)).unwrap();
        drop(root);
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(ticket);
    });
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let path = FsPath::new(crate::wal::segment_file_name(1));
    let before = crate::io::read_all(wal.as_ref(), &path).map_err(FsError::into_midge)?;

    // Act
    let timed_out = actor.append_prepared_transactions(&mut state, vec![prepared]);
    release_tx.send(()).unwrap();
    pinned.join().unwrap();

    // Assert: no physical effect started, so admission can be retried.
    assert!(matches!(timed_out, Err(MidgeError::Timeout(_))));
    assert!(actor.is_open());
    assert!(!actor.authority_lost());
    assert_eq!(
        state.get_cf(0).unwrap().memtable.iter_all(),
        Vec::<(Vec<u8>, Option<Vec<u8>>, u64)>::new()
    );
    assert_eq!(
        crate::io::read_all(wal.as_ref(), &path).map_err(FsError::into_midge)?,
        before
    );
    let retried = prepare_put_transaction(
        &mut actor,
        &mut state,
        2,
        b"key",
        b"value",
        DurabilityPolicy::Strict,
    )?;
    actor.append_prepared_transactions(&mut state, vec![retried])?;
    assert_eq!(state.get_cf(0).unwrap().memtable.iter_all().len(), 1);
    Ok(())
}
