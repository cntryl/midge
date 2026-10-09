use super::*;
use crate::common::MidgeError;
use crate::io::traits::{DirEntry, Durability, File, FsResult, Metadata, OpenOptions};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Counts snapshot opens and can fail snapshot staging writes with ENOSPC.
struct ObservedFs {
    inner: crate::io::RealFs,
    snapshot_opens: AtomicUsize,
    snapshot_no_space: AtomicBool,
    /// Fail every stat once the journal has been opened for writing.
    fail_stats_after_journal_write: AtomicBool,
    journal_written: AtomicBool,
}

struct NoSpaceFile<'a> {
    inner: Box<dyn File + 'a>,
}

impl File for NoSpaceFile<'_> {
    fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.inner.read_at(offset, len)
    }
    fn write_at(&mut self, _offset: u64, _data: Bytes) -> FsResult<()> {
        Err(FsError::NoSpace("no space left on device".into()))
    }
    fn append(&mut self, data: Bytes) -> FsResult<u64> {
        self.inner.append(data)
    }
    fn len(&self) -> FsResult<u64> {
        self.inner.len()
    }
    fn sync(&mut self, durability: Durability) -> FsResult<()> {
        self.inner.sync(durability)
    }
}

impl Fs for ObservedFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let file = self.inner.open(path, opts)?;
        if path.0 == crate::metadata::files::JOURNAL
            && opts.mode == crate::io::traits::OpenMode::ReadWrite
        {
            self.journal_written.store(true, Ordering::SeqCst);
        }
        if path.0 == crate::metadata::files::MANIFEST_SNAPSHOT {
            self.snapshot_opens.fetch_add(1, Ordering::SeqCst);
        }
        if path.0.starts_with("manifest.snapshot.json.tmp")
            && self.snapshot_no_space.load(Ordering::SeqCst)
        {
            return Ok(Box::new(NoSpaceFile { inner: file }));
        }
        Ok(file)
    }
    fn remove_file(&self, path: &FsPath) -> FsResult<()> {
        self.inner.remove_file(path)
    }
    fn exists(&self, path: &FsPath) -> FsResult<bool> {
        self.inner.exists(path)
    }
    fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
        if self.fail_stats_after_journal_write.load(Ordering::SeqCst)
            && self.journal_written.load(Ordering::SeqCst)
        {
            return Err(FsError::Io("stat failed".into()));
        }
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
    fn sync_dir(&self, path: &FsPath, durability: Durability) -> FsResult<()> {
        self.inner.sync_dir(path, durability)
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
    fn coordination_key(&self) -> u64 {
        self.inner.coordination_key()
    }
}

fn observed(directory: &tempfile::TempDir) -> Arc<ObservedFs> {
    Arc::new(ObservedFs {
        inner: crate::io::RealFs::new(directory.path()).expect("open fs"),
        snapshot_opens: AtomicUsize::new(0),
        snapshot_no_space: AtomicBool::new(false),
        fail_stats_after_journal_write: AtomicBool::new(false),
        journal_written: AtomicBool::new(false),
    })
}

fn add_sst(sequence: u64) -> ManifestEdit {
    ManifestEdit::AddSst(crate::metadata::FileMeta {
        name: crate::cloud_layout::file_name(0, 0, sequence),
        size_bytes: 10,
        ..Default::default()
    })
}

#[test]
fn should_force_checkpoint_when_cached_authority_is_unknown_or_stale() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let fs = observed(&directory);
    let store = ManifestStore::new(fs.clone());
    let mut manifest = Manifest::default();
    assert!(store.local_checkpoint_due(0));
    store
        .save_snapshot(&manifest)
        .unwrap()
        .adopt_into(&mut manifest);
    assert!(!store.local_checkpoint_due(manifest.edit_checkpoint_id));

    // Act
    let edit = ManifestEdit::BumpWalSeq { seq: 42 };
    let id = store.append(&edit).unwrap();
    manifest.apply_edit(&edit);
    manifest.note_applied_journal_edit(id);

    // Assert: neither a stale caller nor a changed file can defer authority.
    assert!(store.local_checkpoint_due(id - 1));
    assert!(!store.local_checkpoint_due(id));
    crate::metadata::journal::append_edit_with_fs(
        &(fs as Arc<dyn Fs>),
        &ManifestEdit::BumpWalSeq { seq: 43 },
    )
    .unwrap();
    assert!(store.local_checkpoint_due(id));
}

#[test]
fn should_reduce_fixed_cardinality_checkpoint_payload_without_losing_deferred_edits() {
    fn measure(force: bool) -> u64 {
        let directory = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(directory.path()).unwrap());
        let owner = Owner::new();
        let store =
            ManifestStore::new_with_accounting(fs.clone(), owner.clone(), Medium::Persistent);
        let mut manifest = Manifest::default();
        for n in 1..=128 {
            manifest.apply_edit(&add_sst(n));
        }
        store
            .save_snapshot_for(Origin::Ddl, &manifest)
            .unwrap()
            .adopt_into(&mut manifest);
        for n in 1..=128 {
            let edit = ManifestEdit::BumpWalSeq { seq: n };
            let id = store.append_for(Origin::OrdinaryLocalFlush, &edit).unwrap();
            manifest.apply_edit(&edit);
            manifest.note_applied_journal_edit(id);
            let recovered = ManifestPersistence::load_with_fs_and_policy_typed(
                &fs,
                crate::config::RecoveryPolicy::Strict,
            )
            .unwrap();
            assert_eq!(recovered.files.len(), 128);
            assert_eq!(recovered.last_persisted_sequence, n);
            assert_eq!(recovered.edit_checkpoint_id, id);
            if force || store.local_checkpoint_due(id) {
                store
                    .save_snapshot_for(Origin::OrdinaryLocalFlush, &manifest)
                    .unwrap()
                    .adopt_into(&mut manifest);
            }
        }
        owner
            .handle()
            .snapshot()
            .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
            .counters
            .issued_bytes[0]
    }

    // Arrange
    let forced_bytes = measure(true);
    // Act
    let deferred_bytes = measure(false);
    // Assert
    assert!(
        deferred_bytes * 10 <= forced_bytes,
        "{deferred_bytes} vs {forced_bytes}"
    );
}

#[test]
fn should_not_read_snapshot_when_appending_journal_edit() {
    // Arrange: a database with a snapshot and a store that has written.
    let directory = tempfile::tempdir().expect("tempdir");
    let fs = observed(&directory);
    let store = ManifestStore::new(fs.clone());
    store.append(&add_sst(1)).expect("first append");
    store
        .save_snapshot(&crate::metadata::ManifestPersistence::load(directory.path()).unwrap())
        .expect("snapshot");
    fs.snapshot_opens.store(0, Ordering::SeqCst);

    // Act
    let first = store.append(&add_sst(2)).expect("append");
    let second = store.append_batch(&[add_sst(3)]).expect("append batch");

    // Assert
    assert_eq!(fs.snapshot_opens.load(Ordering::SeqCst), 0);
    assert_eq!((first, second), (2, 3));
}

#[test]
fn should_not_read_metadata_when_current_caller_saves_snapshot() {
    // Arrange
    let directory = tempfile::tempdir().expect("tempdir");
    let fs = observed(&directory);
    let store = ManifestStore::new(fs.clone());
    let mut manifest = Manifest::default();
    let edit_id = store.append(&add_sst(1)).expect("append");
    manifest.apply_edit(&add_sst(1));
    manifest.edit_checkpoint_id = edit_id;
    store.save_snapshot(&manifest).expect("prime");
    let edit_id = store.append(&add_sst(2)).expect("append");
    manifest.apply_edit(&add_sst(2));
    manifest.edit_checkpoint_id = edit_id;
    fs.snapshot_opens.store(0, Ordering::SeqCst);

    // Act
    let written = store.save_snapshot(&manifest).expect("save");

    // Assert
    assert_eq!(fs.snapshot_opens.load(Ordering::SeqCst), 0);
    assert!(written.caller_was_current);
    assert_eq!(written.edit_checkpoint_id, 2);
    let reloaded = crate::metadata::ManifestPersistence::load(directory.path()).unwrap();
    assert_eq!(reloaded.files.len(), 2);
}

#[test]
fn should_report_no_space_when_snapshot_write_hits_enospc() {
    // Arrange
    let directory = tempfile::tempdir().expect("tempdir");
    let fs = observed(&directory);
    let store = ManifestStore::new(fs.clone());
    fs.snapshot_no_space.store(true, Ordering::SeqCst);

    // Act
    let result = store.save_snapshot(&Manifest::default());

    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))), "{result:?}");
}

#[test]
fn should_require_checkpoint_retry_when_append_refreshes_cache_after_snapshot_failure() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let fs = observed(&directory);
    let store = ManifestStore::new(fs.clone());
    let mut manifest = Manifest::default();
    store
        .save_snapshot(&manifest)
        .unwrap()
        .adopt_into(&mut manifest);
    fs.snapshot_no_space.store(true, Ordering::SeqCst);
    assert!(store.save_snapshot(&manifest).is_err());

    // Act: a durable frontier fallback refreshes the cached position, but
    // must not erase the failed checkpoint's publication backpressure.
    let edit = ManifestEdit::BumpWalSeq { seq: 42 };
    let id = store.append(&edit).unwrap();
    manifest.apply_edit(&edit);
    manifest.note_applied_journal_edit(id);
    let retry_required = store.local_checkpoint_due(id);
    fs.snapshot_no_space.store(false, Ordering::SeqCst);
    store
        .save_snapshot(&manifest)
        .unwrap()
        .adopt_into(&mut manifest);

    // Assert: only a successful checkpoint releases that pressure.
    assert!(retry_required);
    assert!(!store.local_checkpoint_due(id));
    assert!(store.can_use_forced_flush_path(id, 2 * 1024 * 1024));
    let recovered = ManifestPersistence::load(directory.path()).unwrap();
    assert_eq!(recovered.last_persisted_sequence, 42);
}

#[test]
fn should_not_reuse_edit_id_when_journal_changed_outside_store() {
    // Arrange: the store knows the position, then another writer appends.
    let directory = tempfile::tempdir().expect("tempdir");
    let fs = observed(&directory);
    let store = ManifestStore::new(fs.clone());
    store.append(&add_sst(1)).expect("store append");
    let outside: Arc<dyn Fs> = fs.clone();
    let outside_id = crate::metadata::journal::append_edit_with_fs(&outside, &add_sst(2))
        .expect("outside append");

    // Act
    let next = store
        .append(&add_sst(3))
        .expect("store append after outside write");

    // Assert
    assert_eq!(outside_id, 2);
    assert_eq!(next, 3);
    let reloaded = crate::metadata::ManifestPersistence::load(directory.path()).unwrap();
    assert_eq!(reloaded.files.len(), 3);
}

#[test]
fn should_keep_every_edit_when_store_resumes_after_snapshot_and_appends() {
    // Arrange: a snapshot, then appends, then a snapshot from a stale caller.
    let directory = tempfile::tempdir().expect("tempdir");
    let fs = observed(&directory);
    let store = ManifestStore::new(fs.clone());
    for sequence in 1..=3 {
        store.append(&add_sst(sequence)).expect("append");
    }

    // Act: the caller never applied any edit; the snapshot must replay them.
    let written = store.save_snapshot(&Manifest::default()).expect("save");
    let after = store.append(&add_sst(4)).expect("append after snapshot");

    // Assert
    assert!(!written.caller_was_current);
    assert_eq!(written.edit_checkpoint_id, 3);
    assert_eq!(after, 4);
    let reloaded = crate::metadata::ManifestPersistence::load(directory.path()).unwrap();
    assert_eq!(reloaded.files.len(), 4);
}

#[test]
fn should_not_reuse_edit_id_when_snapshot_saved_outside_store() {
    // Arrange: the journal ends empty either way, so only the snapshot shows
    // that another writer journaled and checkpointed an edit.
    let directory = tempfile::tempdir().expect("tempdir");
    let fs = observed(&directory);
    let store = ManifestStore::new(fs.clone());
    store.save_snapshot(&Manifest::default()).expect("prime");
    let outside: Arc<dyn Fs> = fs.clone();
    crate::metadata::journal::append_edit_with_fs(&outside, &add_sst(1)).expect("outside append");
    crate::metadata::ManifestPersistence::save_snapshot_and_truncate_journal_with_fs(
        &outside,
        &Manifest::default(),
    )
    .expect("outside snapshot");

    // Act
    let next = store.append(&add_sst(2)).expect("store append");

    // Assert
    assert_eq!(next, 2);
    let reloaded = crate::metadata::ManifestPersistence::load(directory.path()).unwrap();
    assert_eq!(reloaded.files.len(), 2);
}

#[test]
fn should_report_durable_append_when_stat_after_write_fails() {
    // Arrange: the append reaches disk, then refreshing the cache fails.
    let directory = tempfile::tempdir().expect("tempdir");
    let fs = observed(&directory);
    let store = ManifestStore::new(fs.clone());
    fs.fail_stats_after_journal_write
        .store(true, Ordering::SeqCst);

    // Act
    let appended = store.append(&add_sst(1));
    fs.fail_stats_after_journal_write
        .store(false, Ordering::SeqCst);
    let next = store.append(&add_sst(2)).expect("next append");

    // Assert: a durable edit is reported as written, and the store re-reads
    // its position instead of trusting a cache it could not refresh.
    assert_eq!(appended.expect("durable append reports success"), 1);
    assert_eq!(next, 2);
    assert_eq!(
        *store
            .known
            .lock()
            .map(|known| known.position.highest_edit_id)
            .as_ref()
            .unwrap(),
        2
    );
}
