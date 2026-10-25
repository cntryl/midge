use super::*;
use crate::config::DEFAULT_STORAGE_IO_TIMEOUT;
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, FsError, FsPath, FsResult, OpenMode, ReadFs};
use crate::lease::LeaderStore;
use crate::wal::traits::WalWriter;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

struct RecoveryReadGate {
    entered: Barrier,
    resume: Barrier,
    claimed: AtomicBool,
}

impl RecoveryReadGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Barrier::new(2),
            resume: Barrier::new(2),
            claimed: AtomicBool::new(false),
        })
    }
}

struct SnapshotRecoveryFs {
    inner: crate::io::RealFs,
    blocked_path: String,
    gate: Arc<RecoveryReadGate>,
}

struct SnapshotRecoveryFile<'a> {
    inner: Box<dyn File + 'a>,
    snapshot: Bytes,
    gate: Arc<RecoveryReadGate>,
    block_once: AtomicBool,
}

impl crate::io::ReadFile for SnapshotRecoveryFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        let start = usize::try_from(offset)
            .map_err(|_| FsError::Io("snapshot read offset does not fit usize".into()))?;
        let end = usize::try_from(offset.saturating_add(len))
            .map_err(|_| FsError::Io("snapshot read length does not fit usize".into()))?;
        let bytes = self
            .snapshot
            .get(start..end)
            .ok_or_else(|| FsError::Io("snapshot read exceeds captured WAL length".into()))?;
        if self.block_once.swap(false, Ordering::AcqRel) {
            self.gate.entered.wait();
            self.gate.resume.wait();
        }
        Ok(Bytes::copy_from_slice(bytes))
    }

    fn len(&self) -> FsResult<u64> {
        Ok(self.snapshot.len() as u64)
    }
}

impl File for SnapshotRecoveryFile<'_> {
    fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
        self.inner.write_at(offset, data)
    }

    fn truncate(&mut self, len: u64) -> FsResult<()> {
        self.inner.truncate(len)
    }

    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        self.inner.append(data)
    }

    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        self.inner.sync(durability)
    }
}

impl ReadFs for SnapshotRecoveryFs {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::ReadFile + '_>> {
        self.open(path, crate::io::OpenOptions::READ_ONLY)
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

impl Fs for SnapshotRecoveryFs {
    fn host_addressing(&self) -> Option<crate::io::HostAddressing<'_>> {
        self.inner.host_addressing()
    }

    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }

    fn try_lock_exclusive(&self, path: &FsPath) -> FsResult<Box<dyn crate::io::FsLock>> {
        self.inner.try_lock_exclusive(path)
    }

    fn open(&self, path: &FsPath, options: crate::io::OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let file = self.inner.open(path, options)?;
        if path.0 != self.blocked_path || self.gate.claimed.swap(true, Ordering::AcqRel) {
            return Ok(file);
        }
        let len = file.len()?;
        let snapshot = file.read_at(0, len)?;
        Ok(Box::new(SnapshotRecoveryFile {
            inner: file,
            snapshot,
            gate: Arc::clone(&self.gate),
            block_once: AtomicBool::new(true),
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

    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        self.inner.sync_dir(path, durability)
    }

    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

#[test]
fn should_leave_wal_history_unchanged_when_salvage_lacks_family_authority() {
    // Arrange
    let wal: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    let history = crate::runtime::wal_history::WalHistory::open(Arc::clone(&wal), 1, 1).unwrap();
    let ledger = crate::io::FsPath::new(crate::wal::ledger_store::LEDGER_FILE);
    let before = crate::io::read_all(wal.as_ref(), &ledger).unwrap();
    let root: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    let storage = crate::wal::FsWalFactoryIo::new(Arc::clone(&wal));
    let writer = storage.open_segment(1).unwrap();
    let record = crate::wal::WalRecord::new_cf(
        7,
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from_static(b"key"),
        Some(bytes::Bytes::from_static(b"value")),
        1,
    );
    writer.append_record_accounted(&record).unwrap();
    writer
        .sync_with_timeout(DEFAULT_STORAGE_IO_TIMEOUT)
        .unwrap();

    // Act
    let result = RuntimeState::replay_wal_storage_within(
        std::path::Path::new("wal"),
        &root,
        crate::config::RecoveryPolicy::Salvage,
        &Manifest::default(),
        HashMap::new(),
        None,
        None,
        wal.as_ref(),
        history,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(crate::io::read_all(wal.as_ref(), &ledger).unwrap(), before);
}

struct WindowDamageFixture {
    root: Arc<dyn Fs>,
    wal: Arc<dyn Fs>,
    history: crate::runtime::wal_history::WalHistory,
    prefix_end: u64,
    damaged_bytes: Vec<u8>,
}

fn window_damage_fixture(include_damaged_frame_in_floor: bool) -> MidgeResult<WindowDamageFixture> {
    let root: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    let wal = crate::io::subdirectory(Arc::clone(&root), crate::wal::WAL_DIR);
    let mut history = crate::runtime::wal_history::WalHistory::open(Arc::clone(&wal), 1, 1)?;
    let writer = crate::wal::FsWalFactoryIo::new(Arc::clone(&wal)).open_segment(1)?;
    let record = |key: &'static [u8], sequence| {
        crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            Bytes::from_static(key),
            Some(Bytes::from_static(b"value")),
            sequence,
        )
    };

    writer
        .append_record_accounted(&record(b"verified", 1))
        .map_err(|failure| failure.error)?;
    writer.sync_with_timeout(Duration::from_secs(2))?;
    let prefix_end = writer.current_pos();
    history.record_sync(prefix_end)?;

    writer
        .append_record_accounted(&record(b"damaged", 2))
        .map_err(|failure| failure.error)?;
    writer.sync_with_timeout(Duration::from_secs(2))?;
    let damaged_end = writer.current_pos();
    if include_damaged_frame_in_floor {
        history.record_sync(damaged_end)?;
    }
    writer
        .append_record_accounted(&record(b"later-acknowledged", 3))
        .map_err(|failure| failure.error)?;
    writer.sync_with_timeout(Duration::from_secs(2))?;
    drop(writer);

    let path = FsPath::new(crate::wal::segment_file_name(1));
    let mut bytes = crate::io::read_all(wal.as_ref(), &path)
        .map_err(FsError::into_midge)?
        .to_vec();
    let corrupt_at = prefix_end + crate::wal::frame::WAL_FRAME_HEADER_LEN as u64;
    bytes[usize::try_from(corrupt_at).expect("frame offset fits usize")] ^= 0xff;
    let original_len = bytes.len();
    let mut file = wal
        .open(
            &path,
            crate::io::OpenOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    file.write_at(0, Bytes::from(bytes.clone()))
        .map_err(FsError::into_midge)?;
    file.sync(Durability::Durable)
        .map_err(FsError::into_midge)?;
    drop(file);
    bytes.truncate(original_len);
    Ok(WindowDamageFixture {
        root,
        wal,
        history,
        prefix_end,
        damaged_bytes: bytes,
    })
}

#[test]
fn should_enforce_sync_floor_recovery_policy_for_window_damage() -> MidgeResult<()> {
    // Arrange: the later valid record is synced after the retained floor.
    let above_floor = window_damage_fixture(false)?;
    let below_floor = window_damage_fixture(true)?;
    let replay_dir = std::path::Path::new("wal");

    // Act: corruption after the floor is treated as the open tail and cut to
    // the verified prefix, even though a later valid frame follows it.
    let recovered = RuntimeState::replay_wal_storage_within(
        replay_dir,
        &above_floor.root,
        crate::config::RecoveryPolicy::Strict,
        &Manifest::default(),
        HashMap::new(),
        None,
        None,
        above_floor.wal.as_ref(),
        above_floor.history,
    )?;

    // Assert
    assert_eq!(recovered.records_replayed, 1);
    let path = FsPath::new(crate::wal::segment_file_name(1));
    let bytes =
        crate::io::read_all(above_floor.wal.as_ref(), &path).map_err(FsError::into_midge)?;
    assert_eq!(bytes.len() as u64, above_floor.prefix_end);
    assert_eq!(
        &bytes[..],
        &above_floor.damaged_bytes[..usize::try_from(above_floor.prefix_end).unwrap()]
    );
    drop(recovered);

    let result = RuntimeState::replay_wal_storage_within(
        replay_dir,
        &below_floor.root,
        crate::config::RecoveryPolicy::Strict,
        &Manifest::default(),
        HashMap::new(),
        None,
        None,
        below_floor.wal.as_ref(),
        below_floor.history,
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("strict recovery accepted damage below the retained floor"),
    };
    assert!(matches!(error, MidgeError::Corruption(_)), "{error:?}");
    let bytes =
        crate::io::read_all(below_floor.wal.as_ref(), &path).map_err(FsError::into_midge)?;
    assert_eq!(bytes, below_floor.damaged_bytes);
    Ok(())
}

fn write_torn_recovery_segment(wal: &Arc<dyn Fs>, segment_name: &str) -> MidgeResult<u64> {
    let writer = crate::wal::fs::FsWalWriterIo::new_with_counters(
        segment_name,
        Arc::clone(wal),
        std::time::Duration::from_secs(2),
        crate::telemetry::CounterSink::default(),
    )?;
    writer
        .append_record_accounted(&crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            Bytes::from_static(b"recovery-key"),
            Some(Bytes::from_static(b"recovery-value")),
            1,
        ))
        .map_err(|failure| failure.error)?;
    writer.sync_with_timeout(std::time::Duration::from_secs(2))?;
    drop(writer);
    let segment_path = FsPath::new(segment_name);
    let mut segment = wal
        .open(
            &segment_path,
            crate::io::OpenOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    let valid_prefix_bytes = segment.len().map_err(FsError::into_midge)?;
    segment
        .write_at(valid_prefix_bytes, Bytes::from_static(b"\0"))
        .map_err(FsError::into_midge)?;
    segment
        .sync(Durability::Durable)
        .map_err(FsError::into_midge)?;
    drop(segment);
    Ok(valid_prefix_bytes)
}

#[test]
fn should_fence_stale_recovery_tail_truncation_after_successor_seals_segment() -> MidgeResult<()> {
    // Arrange: recovery captures a torn active tail, then pauses before it
    // attempts the truncation. A successor can take over and seal that segment.
    let directory = tempfile::tempdir().expect("temporary database root");
    let real = Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?);
    let root: Arc<dyn Fs> = real.clone();
    let store = Arc::new(crate::lease::fs_leader_store::FsLeaderStore::new(
        Arc::clone(&root),
        crate::lease::LeaseClock::default(),
    ));
    let predecessor = store.acquire_leadership("predecessor").unwrap();
    let wal = crate::io::subdirectory(Arc::clone(&root), crate::wal::WAL_DIR);
    let predecessor_fence = crate::runtime::wal_history::WalHistoryFence::new(
        Arc::clone(&store) as Arc<dyn crate::lease::LeaderStore>,
        "predecessor".into(),
        predecessor.epoch,
        std::time::Duration::from_secs(2),
    );
    let history = crate::runtime::wal_history::WalHistory::open_with_fence(
        Arc::clone(&wal),
        1,
        predecessor.epoch,
        &predecessor_fence,
    )?;
    drop(history);

    let segment_name = crate::wal::segment_file_name(1);
    let valid_prefix_bytes = write_torn_recovery_segment(&wal, &segment_name)?;
    let segment_path = FsPath::new(segment_name.clone());

    let gate = RecoveryReadGate::new();
    let gated_root: Arc<dyn Fs> = Arc::new(SnapshotRecoveryFs {
        inner: crate::io::RealFs::open_existing(directory.path()).map_err(FsError::into_midge)?,
        blocked_path: format!("{}/{}", crate::wal::WAL_DIR, segment_name),
        gate: Arc::clone(&gate),
    });
    let pred_fence = predecessor_fence.clone();
    let replay_dir = directory.path().to_path_buf();
    let predecessor_recovery = std::thread::spawn(move || {
        RuntimeState::recover_wal_state(
            false,
            &gated_root,
            &replay_dir,
            crate::config::RecoveryPolicy::Strict,
            &Manifest::default(),
            HashMap::new(),
            (predecessor.epoch, None, Some(pred_fence)),
        )
    });
    gate.entered.wait();

    // Act: successor recovers and seals the segment while predecessor replay
    // still holds its pre-takeover snapshot of the torn tail.
    let successor = store.acquire_leadership("successor").unwrap();
    let successor_fence = crate::runtime::wal_history::WalHistoryFence::new(
        Arc::clone(&store) as Arc<dyn crate::lease::LeaderStore>,
        "successor".into(),
        successor.epoch,
        std::time::Duration::from_secs(2),
    );
    let mut successor_recovery = RuntimeState::recover_wal_state(
        false,
        &root,
        directory.path(),
        crate::config::RecoveryPolicy::Strict,
        &Manifest::default(),
        HashMap::new(),
        (successor.epoch, None, Some(successor_fence)),
    )?;
    crate::runtime::wal_history::fixture::rotate(&mut successor_recovery.history)?;
    assert!(successor_recovery
        .history
        .ledger()
        .sealed()
        .any(|segment| segment == 1));
    let successor_bytes =
        crate::io::read_all(wal.as_ref(), &segment_path).map_err(FsError::into_midge)?;
    assert!(successor_bytes.len() as u64 > valid_prefix_bytes);

    // Assert: the stale recovery is refused before it can truncate the
    // successor's footer, and the successor's sealed bytes remain intact.
    gate.resume.wait();
    let predecessor_result = predecessor_recovery
        .join()
        .expect("predecessor recovery thread completed");
    let Err(predecessor_error) = predecessor_result else {
        panic!("stale recovery completed after successor takeover");
    };
    assert!(
        matches!(
            &predecessor_error,
            MidgeError::RecoveryFailed(message) if message.contains("Fenced")
        ),
        "stale recovery error: {predecessor_error:?}"
    );
    let bytes_after_stale_recovery =
        crate::io::read_all(wal.as_ref(), &segment_path).map_err(FsError::into_midge)?;
    assert_eq!(bytes_after_stale_recovery, successor_bytes);
    Ok(())
}
