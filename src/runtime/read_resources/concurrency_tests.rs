use super::*;
use crate::io::traits::{DirEntry, Metadata};
use crate::io::{Durability, File, FsPath, FsResult, OpenOptions};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

struct ControlledFs {
    inner: Arc<dyn Fs>,
    blocked_name: String,
    block_next: AtomicBool,
    next_failure: AtomicU8,
    blocked_opens: AtomicUsize,
    entered: crossbeam::channel::Sender<()>,
    release: crossbeam::channel::Receiver<()>,
}

impl Fs for ControlledFs {
    fn immutable_read_view(&self, path: &FsPath) -> FsResult<Option<Arc<dyn Fs>>> {
        if path.0 == self.blocked_name {
            self.blocked_opens.fetch_add(1, Ordering::SeqCst);
            if self.block_next.swap(false, Ordering::SeqCst) {
                self.entered.send(()).expect("announce blocked open");
                self.release.recv().expect("release blocked open");
            }
            match self.next_failure.swap(0, Ordering::SeqCst) {
                1 => {
                    return Err(crate::io::FsError::Unavailable(
                        "injected open failure".into(),
                    ))
                }
                2 => panic!("injected open cancellation"),
                _ => {}
            }
        }
        self.inner.immutable_read_view(path)
    }
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
        self.inner.open(path, opts)
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
    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        self.inner.sync_dir(path, dur)
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

fn controlled(
    directory: &tempfile::TempDir,
) -> (
    Arc<ControlledFs>,
    crossbeam::channel::Receiver<()>,
    crossbeam::channel::Sender<()>,
) {
    let (entered_tx, entered_rx) = crossbeam::channel::bounded(1);
    let (release_tx, release_rx) = crossbeam::channel::bounded(1);
    let fs = Arc::new(ControlledFs {
        inner: Arc::new(crate::io::RealFs::new(directory.path()).expect("filesystem")),
        blocked_name: "slow.sst".into(),
        block_next: AtomicBool::new(true),
        next_failure: AtomicU8::new(0),
        blocked_opens: AtomicUsize::new(0),
        entered: entered_tx,
        release: release_rx,
    });
    (fs, entered_rx, release_tx)
}

#[test]
fn should_open_unrelated_cold_sst_while_another_open_is_blocked() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let slow = tests::write_test_sst(&directory, "slow.sst")?;
    let other = tests::write_test_sst(&directory, "other.sst")?;
    let hot = tests::write_test_sst(&directory, "hot.sst")?;
    let (fs, entered, release) = controlled(&directory);
    let resources = Arc::new(ReadResources::new(
        fs,
        PathBuf::new(),
        1024 * 1024,
        CachePolicyType::Lru,
    ));
    let hot_reader = resources.reader_for(&hot)?;
    // Act
    let slow_resources = Arc::clone(&resources);
    let slow_thread = std::thread::spawn(move || slow_resources.reader_for(&slow));
    entered
        .recv_timeout(Duration::from_secs(2))
        .expect("slow reader entered filesystem");
    let hot_hit = resources.reader_for(&hot)?;
    let (finished_tx, finished_rx) = crossbeam::channel::bounded(1);
    let other_resources = Arc::clone(&resources);
    let other_thread = std::thread::spawn(move || {
        finished_tx
            .send(other_resources.reader_for(&other))
            .expect("other reader completion");
    });
    let independent = finished_rx.recv_timeout(Duration::from_secs(2));
    release
        .send(())
        .expect("release slow read even on regression");
    slow_thread.join().expect("slow reader thread")?;
    other_thread.join().expect("other reader thread");
    // Assert
    assert!(Arc::ptr_eq(&hot_reader, &hot_hit));
    assert!(
        independent.is_ok_and(|result| result.is_ok()),
        "unrelated cold SST must complete before slow SST is released"
    );
    assert!(resources.metadata_budget.peak() <= resources.metadata_budget.limit());
    Ok(())
}

#[test]
fn should_share_one_reader_when_concurrent_requests_open_same_sst() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let meta = tests::write_test_sst(&directory, "slow.sst")?;
    let (fs, entered, release) = controlled(&directory);
    let resources = Arc::new(ReadResources::new(
        fs.clone(),
        PathBuf::new(),
        1024 * 1024,
        CachePolicyType::Lru,
    ));
    // Act
    let first_resources = Arc::clone(&resources);
    let first_meta = meta.clone();
    let first = std::thread::spawn(move || first_resources.reader_for(&first_meta));
    entered
        .recv_timeout(Duration::from_secs(2))
        .expect("first open entered");
    let second_resources = Arc::clone(&resources);
    let second = std::thread::spawn(move || second_resources.reader_for(&meta));
    release.send(()).expect("release first open");
    let first = first.join().expect("first reader thread")?;
    let second = second.join().expect("second reader thread")?;
    // Assert
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(fs.blocked_opens.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn should_allow_new_open_after_owner_fails_or_unwinds() -> MidgeResult<()> {
    for failure in [1, 2] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let meta = tests::write_test_sst(&directory, "slow.sst")?;
        let (fs, _, _) = controlled(&directory);
        fs.block_next.store(false, Ordering::SeqCst);
        fs.next_failure.store(failure, Ordering::SeqCst);
        let resources = ReadResources::new(
            fs.clone(),
            PathBuf::new(),
            1024 * 1024,
            CachePolicyType::Lru,
        );
        // Act
        let failed =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| resources.reader_for(&meta)));
        let retry = resources.reader_for(&meta);
        // Assert
        assert!(failed.is_err() || failed.is_ok_and(|result| result.is_err()));
        assert!(
            retry.is_ok(),
            "failed open must release ownership, including unwind"
        );
        assert_eq!(fs.blocked_opens.load(Ordering::SeqCst), 2);
        assert!(resources.metadata_budget.peak() <= resources.metadata_budget.limit());
    }
    Ok(())
}
