use cntryl_midge_core::io::traits::{DirEntry, Metadata};
use cntryl_midge_core::io::{
    Durability, File, Fs, FsError, FsPath, FsResult, HostAddressing, OpenMode, OpenOptions, ReadFs,
};
use cntryl_midge_core::types::EntryType;
use cntryl_midge_storage::remote_sst::RemoteSstFs;
use cntryl_midge_storage::test_support::StorageBackendTestExt;
use cntryl_midge_storage::StorageBackend;
use cntryl_midge_table::sst::traits::{SstFactory, SstStateReader};
use std::{path::Path, sync::Arc, time::Duration};

#[test]
fn should_read_repair_scratch_locally_through_hybrid_compaction_factory(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let remote = tempfile::tempdir()?;
    let local = Arc::new(cntryl_midge_core::io::MockFs::new());
    local
        .create_dir_all(&FsPath::new(".compaction-repair"))
        .map_err(FsError::into_midge)?;
    let cloud = Arc::new(cntryl_midge_storage::filesystem::FileSystem::new(
        remote.path(),
    )?);
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(
        Arc::new(RemoteSstFs::new(local, cloud, Duration::from_secs(5))),
        4096,
    );
    let path = Path::new(".compaction-repair/run.sst");
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"local"), 9, EntryType::Put, None)?;
    writer.finish_to_path(path)?;

    // Act
    let reader = factory.open_for_compaction(
        path,
        cntryl_midge_core::common::resource_budget::ResourceBudget::new(1024 * 1024),
    )?;

    // Assert
    assert!(matches!(reader.get_state_at_with_time(b"key", 9, 0)?,
            cntryl_midge_core::types::KeyState::Value(value, 9, None, _) if value.as_ref() == b"local"));
    Ok(())
}

#[test]
fn should_read_back_unpublished_sst_from_local_output_view(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: remote object has not been published when the writer finishes.
    let remote = tempfile::tempdir()?;
    let local = Arc::new(cntryl_midge_core::io::MockFs::new());
    let cloud = Arc::new(cntryl_midge_storage::filesystem::FileSystem::new(
        remote.path(),
    )?);
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(
        Arc::new(RemoteSstFs::new(
            local.clone(),
            cloud,
            Duration::from_secs(5),
        )),
        4096,
    );
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"value"), 9, EntryType::Put, None)?;
    writer.finish_to_path(Path::new("unpublished.sst"))?;

    // Act
    let summary = cntryl_midge_table::sst::fs::SstFileIo::summarize_with_fs_for_compaction(
        "unpublished.sst",
        factory.output_fs(),
        cntryl_midge_core::common::resource_budget::ResourceBudget::new(1024 * 1024),
    )?;

    // Assert
    assert_eq!(summary.largest_seq, 9);
    assert_eq!(
        usize::try_from(summary.size_bytes).unwrap(),
        local.get_file("unpublished.sst").unwrap().len()
    );
    Ok(())
}

struct RecordingBackend {
    inner: cntryl_midge_storage::filesystem::FileSystem,
    ranges: std::sync::Mutex<Vec<(u64, u64)>>,
    next_range_failure: std::sync::atomic::AtomicU8,
}

impl StorageBackend for RecordingBackend {
    fn submit_range_read_request(
        &self,
        request: cntryl_midge_storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: cntryl_midge_storage::RangeReadCallback,
    ) {
        self.ranges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((range.start, range.end));
        match self
            .next_range_failure
            .swap(0, std::sync::atomic::Ordering::SeqCst)
        {
            1 => {
                let _ = callback.send(Err("timeout: injected range timeout".into()));
                return;
            }
            2 => {
                let _ = callback.send(Ok(vec![0]));
                return;
            }
            3 => return,
            _ => {}
        }
        self.inner
            .submit_range_read_request(request, range, callback);
    }

    cntryl_midge_storage::forward_storage_backend!(
        inner;
        submit_write_request, submit_delete_request, submit_head_request, submit_range_head_request, submit_metadata_read_request,

    );
}

#[test]
fn should_record_actual_remote_ranges_in_the_owning_read_diagnostics(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let local = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    let cloud = Arc::new(RecordingBackend {
        inner: cntryl_midge_storage::filesystem::FileSystem::new(remote.path())?,
        ranges: std::sync::Mutex::new(Vec::new()),
        next_range_failure: std::sync::atomic::AtomicU8::new(0),
    });
    let local_fs =
        Arc::new(cntryl_midge_core::io::RealFs::new(local.path()).map_err(FsError::into_midge)?);
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(local_fs.clone(), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"value"), 9, EntryType::Put, None)?;
    let bytes = writer.finish_bytes()?;
    let meta = cntryl_midge_durability::metadata::FileMeta {
        name: "remote.sst".into(),
        size_bytes: bytes.len() as u64,
        ..Default::default()
    };
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.write_for_test("sst/remote.sst", bytes, tx);
    rx.recv().expect("remote write");
    let diagnostics = Arc::new(crate::diagnostics::RuntimeDiagnostics::default());
    let unrelated = crate::diagnostics::RuntimeDiagnostics::default();
    let resources = crate::runtime::read_resources::ReadResources::new_with_diagnostics(
        Arc::new(RemoteSstFs::new(
            local_fs,
            cloud.clone(),
            Duration::from_secs(5),
        )),
        std::path::PathBuf::new(),
        1024 * 1024,
        cntryl_midge_table::sst::cache::CachePolicyType::Lru,
        diagnostics.clone(),
    );
    // Act
    let reader = resources.reader_for(&meta)?;
    reader.get_state_at(b"key", 9)?;
    let cold = diagnostics.snapshot();
    resources.reader_for(&meta)?.get_state_at(b"key", 9)?;
    let warm = diagnostics.snapshot();
    // Assert
    let ranges = cloud
        .ranges
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(ranges.len() > 1);
    assert_eq!(cold.remote_range_requests_total, ranges.len() as u64);
    assert_eq!(
        cold.remote_range_bytes_total,
        ranges.iter().map(|(start, end)| end - start).sum::<u64>()
    );
    assert!(cold.remote_range_latency_ns_total > 0);
    assert!(cold.remote_range_latency_ns_max > 0);
    assert_eq!(cold.remote_range_failures_total, 0);
    assert_eq!(
        warm.remote_range_requests_total,
        cold.remote_range_requests_total
    );
    assert_eq!(warm.remote_range_bytes_total, cold.remote_range_bytes_total);
    assert_eq!(unrelated.snapshot().remote_range_requests_total, 0);
    Ok(())
}

#[test]
fn should_count_failed_range_payloads_without_counting_unsubmitted_reads(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let local = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    std::fs::create_dir_all(remote.path().join("sst"))?;
    std::fs::write(remote.path().join("sst/remote.sst"), b"abcdefgh")?;
    let cloud = Arc::new(RecordingBackend {
        inner: cntryl_midge_storage::filesystem::FileSystem::new(remote.path())?,
        ranges: std::sync::Mutex::new(Vec::new()),
        next_range_failure: std::sync::atomic::AtomicU8::new(0),
    });
    let diagnostics = Arc::new(crate::diagnostics::RuntimeDiagnostics::default());
    let fs = RemoteSstFs::new(
        Arc::new(cntryl_midge_core::io::RealFs::new(local.path()).map_err(FsError::into_midge)?),
        cloud.clone(),
        Duration::from_secs(5),
    )
    .with_read_observer(diagnostics.clone())
    .expect("remote observation view");
    let file = fs
        .open(&FsPath::new("remote.sst"), read_options())
        .map_err(FsError::into_midge)?;
    // Act
    assert_eq!(
        file.read_at(0, 3).map_err(FsError::into_midge)?.as_ref(),
        b"abc"
    );
    assert!(file.read_at(0, 0).map_err(FsError::into_midge)?.is_empty());
    assert!(file.read_at(8, 1).is_err());
    for failure in 1..=3 {
        cloud
            .next_range_failure
            .store(failure, std::sync::atomic::Ordering::SeqCst);
        assert!(file.read_at(0, 3).is_err());
    }
    let snapshot = diagnostics.snapshot();
    // Assert
    assert_eq!(snapshot.remote_range_requests_total, 4);
    assert_eq!(snapshot.remote_range_bytes_total, 4);
    assert_eq!(snapshot.remote_range_failures_total, 3);
    assert!(snapshot.remote_range_latency_ns_max > 0);
    assert!(snapshot.remote_range_latency_ns_total >= snapshot.remote_range_latency_ns_max);
    assert_eq!(
        cloud
            .ranges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len(),
        4
    );
    Ok(())
}

#[test]
fn should_keep_observations_separate_when_reusing_a_pinned_remote_view(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let local = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    std::fs::create_dir_all(remote.path().join("sst"))?;
    std::fs::write(remote.path().join("sst/remote.sst"), b"abcdefgh")?;
    let fs = RemoteSstFs::new(
        Arc::new(cntryl_midge_core::io::RealFs::new(local.path()).map_err(FsError::into_midge)?),
        Arc::new(cntryl_midge_storage::filesystem::FileSystem::new(
            remote.path(),
        )?),
        Duration::from_secs(5),
    );
    let path = FsPath::new("remote.sst");
    let pinned = fs
        .immutable_read_view(&path)
        .map_err(FsError::into_midge)?
        .expect("pinned view");
    let first = Arc::new(crate::diagnostics::RuntimeDiagnostics::default());
    let second = Arc::new(crate::diagnostics::RuntimeDiagnostics::default());
    let first_view = pinned
        .with_read_observer(first.clone())
        .expect("first observer");
    let second_view = first_view
        .with_read_observer(second.clone())
        .expect("second observer");
    // Act
    first_view
        .open(&path, read_options())
        .map_err(FsError::into_midge)?
        .read_at(0, 3)
        .map_err(FsError::into_midge)?;
    second_view
        .open(&path, read_options())
        .map_err(FsError::into_midge)?
        .read_at(3, 5)
        .map_err(FsError::into_midge)?;
    // Assert
    assert_eq!(first.snapshot().remote_range_requests_total, 1);
    assert_eq!(first.snapshot().remote_range_bytes_total, 3);
    assert_eq!(second.snapshot().remote_range_requests_total, 1);
    assert_eq!(second.snapshot().remote_range_bytes_total, 5);
    Ok(())
}

fn read_options() -> OpenOptions {
    OpenOptions {
        mode: OpenMode::ReadOnly,
        create: false,
        create_new: false,
        truncate: false,
    }
}

#[test]
fn should_use_only_verified_salvage_file_when_reader_opens_by_basename(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let local = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    let local_fs =
        Arc::new(cntryl_midge_core::io::RealFs::new(local.path()).map_err(FsError::into_midge)?);
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(local_fs.clone(), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"verified"), 9, EntryType::Put, None)?;
    let bytes = writer.finish_bytes()?;
    std::fs::create_dir_all(local.path().join("sst"))?;
    std::fs::write(local.path().join("sst/allowed.sst"), &bytes)?;
    std::fs::write(local.path().join("sst/other.sst"), &bytes)?;
    std::fs::write(
        local.path().join("allowed.sst"),
        b"wrong same-name local file",
    )?;
    let fs = Arc::new(
        RemoteSstFs::new(
            local_fs,
            Arc::new(cntryl_midge_storage::filesystem::FileSystem::new(
                remote.path(),
            )?),
            Duration::from_secs(5),
        )
        .with_verified_local_overrides(std::collections::HashSet::from(["allowed.sst".into()])),
    );
    // Act
    let reader = cntryl_midge_table::sst::fs::SstFileIo::open("allowed.sst", fs.clone())?;
    let value = reader.get_state_at(b"key", 9)?;
    let unverified = cntryl_midge_table::sst::fs::SstFileIo::open("sst/other.sst", fs);
    // Assert
    assert!(
        matches!(value, cntryl_midge_core::types::KeyState::Value(bytes, 9, None, _) if bytes.as_ref() == b"verified")
    );
    assert!(
        unverified.is_err(),
        "unlisted local files must use remote authority"
    );
    Ok(())
}

#[test]
fn should_stream_remote_sst_via_bounded_ranges_when_local_cache_is_corrupt(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange
    let local = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    let cloud = Arc::new(RecordingBackend {
        inner: cntryl_midge_storage::filesystem::FileSystem::new(remote.path())?,
        ranges: std::sync::Mutex::new(Vec::new()),
        next_range_failure: std::sync::atomic::AtomicU8::new(0),
    });
    let local_fs =
        Arc::new(cntryl_midge_core::io::RealFs::new(local.path()).map_err(FsError::into_midge)?);
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(local_fs.clone(), 4096);
    let mut writer = factory.create()?;
    for index in 0_u32..1024 {
        let mut value = vec![0; 1024];
        for (offset, byte) in value.iter_mut().enumerate() {
            *byte = u8::try_from(offset.wrapping_mul(73).wrapping_add(index as usize) % 251)
                .expect("remainder fits u8");
        }
        writer.add_with_meta(&index.to_be_bytes(), Some(&value), 9, EntryType::Put, None)?;
    }
    let bytes = writer.finish_bytes()?;
    let size = bytes.len() as u64;
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.write_for_test("sst/remote.sst", bytes, tx);
    rx.recv().unwrap();
    std::fs::write(local.path().join("remote.sst"), b"corrupt local cache")?;
    let fs = Arc::new(RemoteSstFs::new(
        local_fs,
        cloud.clone(),
        Duration::from_secs(5),
    ));
    // Act
    let reader = cntryl_midge_table::sst::fs::SstFileIo::open("remote.sst", fs)?;
    let open_bytes: u64 = cloud
        .ranges
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|(start, end)| end - start)
        .sum();
    let state = reader.get_state_at(&500_u32.to_be_bytes(), 9)?;
    let count = Box::new(reader)
        .raw_version_cursor(None, None)?
        .collect::<cntryl_midge_core::common::MidgeResult<Vec<_>>>()?
        .len();
    // Assert
    assert!(
        open_bytes < size / 4,
        "opening read {open_bytes} bytes from {size}-byte object"
    );
    assert!(matches!(
        state,
        cntryl_midge_core::types::KeyState::Value(_, 9, None, _)
    ));
    assert_eq!(count, 1024);
    assert!(cloud
        .ranges
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .all(|(start, end)| end - start < size / 4));
    assert_eq!(
        std::fs::read(local.path().join("remote.sst"))?,
        b"corrupt local cache"
    );
    Ok(())
}

#[test]
fn should_read_remote_sst_without_creating_local_file() -> cntryl_midge_core::common::MidgeResult<()>
{
    // Arrange
    let local = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    let cloud = Arc::new(cntryl_midge_storage::filesystem::FileSystem::new(
        remote.path(),
    )?);
    let fs =
        Arc::new(cntryl_midge_core::io::RealFs::new(local.path()).map_err(FsError::into_midge)?);
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(fs.clone(), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"value"), 9, EntryType::Put, None)?;
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.write_for_test("sst/remote.sst", writer.finish_bytes()?, tx);
    rx.recv().expect("write completion");
    let ranges = Arc::new(RemoteSstFs::new(fs, cloud, Duration::from_secs(5)));
    // Act
    let reader = cntryl_midge_table::sst::fs::SstFileIo::open("remote.sst", ranges)?;
    let actual = reader.get_state_at(b"key", 9)?;
    // Assert
    assert!(
        matches!(actual, cntryl_midge_core::types::KeyState::Value(value, 9, None, _) if value.as_ref() == b"value")
    );
    assert!(!local.path().join("remote.sst").exists());
    Ok(())
}

/// Records the root-relative paths a publication renames into and fsyncs,
/// so a test can observe how an SST target was addressed rather than only
/// where it landed.
struct PublicationRecordingFs {
    inner: cntryl_midge_core::io::RealFs,
    renames: std::sync::Mutex<Vec<String>>,
    directory_syncs: std::sync::Mutex<Vec<String>>,
}

impl PublicationRecordingFs {
    fn new(root: &Path) -> cntryl_midge_core::common::MidgeResult<Self> {
        Ok(Self {
            inner: cntryl_midge_core::io::RealFs::new(root).map_err(FsError::into_midge)?,
            renames: std::sync::Mutex::new(Vec::new()),
            directory_syncs: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn recorded(lock: &std::sync::Mutex<Vec<String>>) -> Vec<String> {
        lock.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ReadFs for PublicationRecordingFs {
    fn open_read(&self, path: &FsPath) -> FsResult<Box<dyn cntryl_midge_core::io::ReadFile + '_>> {
        self.open(path, OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn cntryl_midge_core::io::ReadFile>)
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

impl Fs for PublicationRecordingFs {
    fn try_lock_exclusive(
        &self,
        path: &FsPath,
    ) -> FsResult<Box<dyn cntryl_midge_core::io::FsLock>> {
        self.inner.try_lock_exclusive(path)
    }

    fn host_addressing(&self) -> Option<HostAddressing<'_>> {
        self.inner.host_addressing()
    }
    fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
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
    fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
        self.directory_syncs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(path.0.clone());
        self.inner.sync_dir(path, dur)
    }
    fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
        self.inner.rename_atomic(from, to)?;
        self.renames
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(to.0.clone());
        Ok(())
    }
}

#[test]
fn should_address_compaction_output_relative_to_local_root_when_publishing_through_remote_view(
) -> cntryl_midge_core::common::MidgeResult<()> {
    // Arrange: a cloud-mode SST factory writing through a remote view of
    // the db directory. Without the view exposing its local host root the
    // target would be addressed by absolute host path and re-rooted.
    let db = tempfile::tempdir()?;
    let remote = tempfile::tempdir()?;
    let local = Arc::new(PublicationRecordingFs::new(db.path())?);
    let cloud = Arc::new(cntryl_midge_storage::filesystem::FileSystem::new(
        remote.path(),
    )?);
    let view: Arc<dyn Fs> = Arc::new(RemoteSstFs::new(
        Arc::clone(&local) as Arc<dyn Fs>,
        cloud,
        Duration::from_secs(5),
    ));
    let target = db.path().join("sst").join("000042.sst");
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(Arc::clone(&view), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(b"key", Some(b"value"), 9, EntryType::Put, None)?;

    // Act
    let mapped = cntryl_midge_table::sst::fs::fs_relative_sst_path(&view, &target)?;
    cntryl_midge_table::sst::fs::finish_writer_to_path(writer, &target)?;

    // Assert
    assert_eq!(mapped, FsPath::new("sst/000042.sst"));
    assert_eq!(
        PublicationRecordingFs::recorded(&local.renames),
        vec!["sst/000042.sst".to_string()]
    );
    assert_eq!(
        PublicationRecordingFs::recorded(&local.directory_syncs),
        vec!["sst".to_string()]
    );
    assert!(target.is_file());
    Ok(())
}
