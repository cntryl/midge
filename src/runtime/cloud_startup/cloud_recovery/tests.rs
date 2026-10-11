//! Focused local cloud SST loss classification tests.

use super::*;

#[test]
fn should_treat_missing_child_as_indeterminate_when_sst_parent_is_a_file() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let sst_dir = temp.path().join("sst");
    std::fs::write(&sst_dir, b"blocked parent")?;
    let child_error = std::io::Error::from(std::io::ErrorKind::NotFound);

    // Act
    let definitive = local_sst_is_definitively_missing(&sst_dir, &child_error);

    // Assert
    assert!(!definitive);
    Ok(())
}

#[test]
fn should_treat_missing_child_as_definitive_when_sst_parent_is_a_directory() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let sst_dir = temp.path().join("sst");
    std::fs::create_dir(&sst_dir)?;
    let child_error = std::io::Error::from(std::io::ErrorKind::NotFound);

    // Act
    let definitive = local_sst_is_definitively_missing(&sst_dir, &child_error);

    // Assert
    assert!(definitive);
    Ok(())
}

/// A real filesystem that refuses deletions, standing in for a disk that
/// cannot discard a staged file.
struct RemoveRefusingFs {
    inner: crate::io::RealFs,
}

impl crate::io::Fs for RemoveRefusingFs {
    fn open(
        &self,
        path: &crate::io::FsPath,
        opts: crate::io::OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
        self.inner.open(path, opts)
    }

    fn open_persistent_handle(
        &self,
        path: &crate::io::FsPath,
        opts: crate::io::OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File>> {
        self.inner.open_persistent_handle(path, opts)
    }

    fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        Err(FsError::Io(format!(
            "injected removal failure for {}",
            path.0
        )))
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

    fn create_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.inner.create_dir_all(path)
    }

    fn list_dir(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
        self.inner.list_dir(path)
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

/// An in-memory root whose `wal/` cannot be listed, as on a damaged disk.
struct UnlistableWalFs(crate::io::MockFs);

impl crate::io::Fs for UnlistableWalFs {
    fn coordination_key(&self) -> u64 {
        self.0.coordination_key()
    }

    fn open(
        &self,
        path: &crate::io::FsPath,
        options: crate::io::OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
        self.0.open(path, options)
    }

    fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.0.remove_file(path)
    }

    fn exists(&self, path: &crate::io::FsPath) -> crate::io::FsResult<bool> {
        self.0.exists(path)
    }

    fn metadata(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<crate::io::traits::Metadata> {
        self.0.metadata(path)
    }

    fn create_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.0.create_dir_all(path)
    }

    fn list_dir(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
        Err(crate::io::FsError::Io(format!(
            "injected unreadable entry while listing {}",
            path.0
        )))
    }

    fn remove_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.0.remove_dir_all(path)
    }

    fn sync_dir(
        &self,
        path: &crate::io::FsPath,
        durability: crate::io::Durability,
    ) -> crate::io::FsResult<()> {
        self.0.sync_dir(path, durability)
    }

    fn rename_atomic(
        &self,
        from: &crate::io::FsPath,
        to: &crate::io::FsPath,
    ) -> crate::io::FsResult<()> {
        self.0.rename_atomic(from, to)
    }
}

#[test]
fn should_fail_open_when_local_wal_directory_cannot_be_listed() {
    // Arrange: local WAL may hold acknowledged writes that never reached the
    // cloud, so even a salvage open cannot proceed as if it held none.
    let fs = UnlistableWalFs(crate::io::MockFs::new());

    // Act
    let result = CloudStartupRecovery::collect_local_wal_paths(&fs);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::RecoveryFailed(_))),
        "{result:?}"
    );
}
