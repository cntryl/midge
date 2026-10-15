use super::*;
use crate::io::{Durability, FsError, FsPath, FsResult, OpenOptions, ReadFile, ReadFs};
use std::sync::atomic::{AtomicUsize, Ordering};

struct RefuseSstReads {
    inner: crate::io::RealFs,
    attempts: AtomicUsize,
}

impl RefuseSstReads {
    fn check(&self, path: &FsPath) -> FsResult<()> {
        if std::path::Path::new(&path.0)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))
        {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(FsError::Io(
                "SST payload reads are forbidden in this control".into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl ReadFs for RefuseSstReads {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn ReadFile + '_>> {
        self.check(path)?;
        self.inner.open_read(path)
    }
    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }
    fn metadata(&self, path: &FsPath) -> FsResult<crate::io::traits::Metadata> {
        self.inner.metadata(path)
    }
    fn list_dir(&self, path: &FsPath) -> FsResult<Vec<crate::io::traits::DirEntry>> {
        self.inner.list_dir(path)
    }
}

impl crate::io::PersistentHandleFs for RefuseSstReads {
    fn open_persistent(
        &self,
        path: &FsPath,
        options: OpenOptions,
    ) -> FsResult<Box<dyn crate::io::File>> {
        self.check(path)?;
        crate::io::open_persistent_handle(&self.inner, path, options)
    }
}

impl Fs for RefuseSstReads {
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

    fn open(&self, path: &FsPath, options: OpenOptions) -> FsResult<Box<dyn crate::io::File + '_>> {
        self.check(path)?;
        self.inner.open(path, options)
    }
    fn host_addressing(&self) -> Option<crate::io::HostAddressing<'_>> {
        self.inner.host_addressing()
    }
    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
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

fn append_record(state: &RuntimeState, sequence: u64, key: &'static [u8]) {
    let record = crate::wal::WalRecord::new_cf(
        0,
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from_static(key),
        Some(bytes::Bytes::from_static(b"value")),
        sequence,
    );
    let mut frame = Vec::new();
    crate::wal::frame::append_frame(&mut frame, &crate::wal::encoding::encode(&record).unwrap())
        .unwrap();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(state.host_wal_dir().join(crate::wal::segment_file_name(
            state.wal.history.ledger().open_segment(),
        )))
        .unwrap();
    std::io::Write::write_all(&mut file, &frame).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn should_not_read_ssts_to_decide_wal_replay_or_retention() {
    // Arrange: the flushed record is in a sealed segment, the later record
    // remains in its open successor, and the authoritative SST is absent.
    let directory = tempfile::tempdir().unwrap();
    let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
    append_record(&state, 1, b"flushed");
    crate::runtime::wal_history::fixture::rotate(&mut state.wal.history).unwrap();
    append_record(&state, 3, b"replay");
    let output = crate::runtime::FileMeta {
        name: crate::cloud_layout::file_name(0, 0, 1),
        level: 0,
        size_bytes: 10,
        content_crc32c: None,
        cf_id: 0,
        smallest_key: None,
        largest_key: None,
        smallest_seq: Some(1),
        largest_seq: Some(1),
        key_bounds_complete: false,
    };
    state
        .commit_flush_publication(0, 2, &output, 2, false)
        .unwrap();
    assert!(!state.host_sst_dir().join(&output.name).exists());
    drop(state);
    let observed = Arc::new(RefuseSstReads {
        inner: crate::io::RealFs::new(directory.path()).unwrap(),
        attempts: AtomicUsize::new(0),
    });
    let fs: Arc<dyn Fs> = observed.clone();

    // Act: exercise actual startup replay, the production prefix selector,
    // and durable retirement while all SST payload opens are rejected.
    let mut recovered = RuntimeState::recover(
        directory.path(),
        false,
        fs,
        crate::config::RecoveryPolicy::Strict,
        super::super::recovery::RecoveryWal::Replay { writer_epoch: 0 },
        None,
        None,
    )
    .unwrap();
    let below = recovered.wal.history.ledger().retirable_prefix(|cf, sequence| {
        matches!(recovered.manifest.flush_frontiers().get(cf),
            Ok(crate::metadata::FamilyFrontier::Live { through_sequence }) if sequence <= through_sequence)
    });
    recovered.wal.history.retire_below(below.unwrap()).unwrap();

    // Assert: the boundary skips only the flushed record, replays its live
    // successor, and retires exactly the sealed prefix without SST reads.
    let memtable = &recovered.get_cf(0).unwrap().memtable;
    assert_eq!(memtable.get(b"flushed").unwrap(), None);
    assert_eq!(
        memtable.get(b"replay").unwrap().as_deref(),
        Some(b"value".as_slice())
    );
    assert_eq!(below, Some(2));
    assert_eq!(recovered.wal.history.ledger().log_start(), 2);
    assert_eq!(observed.attempts.load(Ordering::SeqCst), 0);
    assert!(!recovered.host_sst_dir().join(output.name).exists());
}
