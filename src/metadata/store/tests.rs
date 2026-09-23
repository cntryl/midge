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
    fn close(self: Box<Self>) -> FsResult<()> {
        self.inner.close()
    }
}

impl Fs for ObservedFs {
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        let file = self.inner.open(path, opts)?;
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
