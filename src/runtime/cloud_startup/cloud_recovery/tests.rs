//! Focused cloud recovery tests.

use super::*;

/// A real filesystem that refuses deletions, standing in for a disk that
/// cannot discard a staged file.
struct RemoveRefusingFs {
    inner: crate::io::RealFs,
}

impl crate::io::ReadFs for RemoveRefusingFs {
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

impl crate::io::PersistentHandleFs for RemoveRefusingFs {
    fn open_persistent(
        &self,
        path: &crate::io::FsPath,
        opts: crate::io::OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File>> {
        crate::io::open_persistent_handle(&self.inner, path, opts)
    }
}

impl crate::io::Fs for RemoveRefusingFs {
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

    fn open(
        &self,
        path: &crate::io::FsPath,
        opts: crate::io::OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
        self.inner.open(path, opts)
    }

    fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        Err(FsError::Io(format!(
            "injected removal failure for {}",
            path.0
        )))
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
        dur: crate::io::Durability,
    ) -> crate::io::FsResult<()> {
        self.inner.sync_dir(path, dur)
    }

    fn rename_atomic(
        &self,
        from: &crate::io::FsPath,
        to: &crate::io::FsPath,
    ) -> crate::io::FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

#[test]
fn should_retain_invalid_restored_sst_when_staging_filesystem_cannot_discard_it() -> MidgeResult<()>
{
    // Arrange: salvage restores bytes that satisfy the name-only proof but
    // are not an SST, and the staging filesystem refuses to delete them.
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::try_new(temp.path(), false, RecoveryPolicy::Salvage)?;
    state.fs = Arc::new(RemoveRefusingFs {
        inner: crate::io::RealFs::new(temp.path()).map_err(FsError::into_midge)?,
    });
    let sst_name = crate::cloud_layout::file_name(0, 0, 3);
    let cloud = crate::storage::cloud::CloudStorage::new(
        Arc::new(crate::storage::cloud::MockCloudBackend::new()),
        "midge".to_string(),
    );
    BlockingCloudIo::new(&cloud).put(
        &crate::cloud_layout::object_key(&sst_name),
        b"not an sst".to_vec(),
    )?;

    // Act
    let result = CloudStartupRecovery::ensure_named_sst_cache_from_cloud_storage(
        &mut state,
        &cloud,
        vec![CloudSstRecoveryProof::name_only(sst_name.clone())],
    );

    // Assert
    assert!(result.is_ok(), "salvage keeps opening: replay revalidates");
    assert!(
        state.host_sst_dir().join(&sst_name).exists(),
        "a discard the staging filesystem refused must leave the bytes in place, not bypass it"
    );
    Ok(())
}

struct ConcurrentInventory {
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    released: std::sync::Mutex<bool>,
    release: std::sync::Condvar,
}

impl crate::storage::inventory::ObjectInventory for ConcurrentInventory {
    fn object_size(&self, _key: &str, _scope: Option<&DeadlineScope>) -> MidgeResult<Option<u64>> {
        use std::sync::atomic::Ordering;
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak.fetch_max(active, Ordering::AcqRel);
        let mut released = self.released.lock().unwrap();
        if active == 32 {
            *released = true;
            self.release.notify_all();
        }
        let (mut released, _) = self
            .release
            .wait_timeout_while(released, std::time::Duration::from_secs(1), |released| {
                !*released
            })
            .unwrap();
        // Bound the negative control as well: a serial implementation must
        // fail the concurrency assertion instead of hanging the test.
        *released = true;
        self.release.notify_all();
        self.active.fetch_sub(1, Ordering::AcqRel);
        Ok(Some(64))
    }

    fn keys_under(
        &self,
        _prefix: &str,
        _scope: Option<&DeadlineScope>,
    ) -> MidgeResult<Vec<String>> {
        unreachable!("manifest inventory only probes referenced objects")
    }
}

#[test]
fn should_probe_manifest_ssts_with_bounded_concurrency_when_opening_cloud_state() -> MidgeResult<()>
{
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut state = RuntimeState::try_new(temp.path(), true, RecoveryPolicy::Strict)?;
    let files = (0..100)
        .map(|index| crate::metadata::FileMeta {
            name: format!("inventory-{index}.sst"),
            size_bytes: 64,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    state.manifest.replace_files(files.clone());
    let inventory = ConcurrentInventory {
        active: std::sync::atomic::AtomicUsize::new(0),
        peak: std::sync::atomic::AtomicUsize::new(0),
        released: std::sync::Mutex::new(false),
        release: std::sync::Condvar::new(),
    };

    // Act
    CloudStartupRecovery::validate_manifest_ssts_in_store(&mut state, &inventory)?;

    // Assert
    assert_eq!(
        inventory.peak.load(std::sync::atomic::Ordering::Acquire),
        32
    );
    assert_eq!(state.manifest.files.len(), files.len());
    assert!(state
        .manifest
        .files
        .iter()
        .zip(&files)
        .all(|(actual, expected)| actual.same_identity(expected)));
    Ok(())
}
