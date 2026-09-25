//! Durable directory creation (#519).
//!
//! A new directory entry survives a crash only once its parent directory is
//! fsynced. `std::fs::create_dir_all` leaves that to whatever fsync happens
//! next, which made durability depend on startup ordering.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

/// Directories whose entry this process has already made durable.
static DURABLE_DIRS: LazyLock<parking_lot::Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| parking_lot::Mutex::new(HashSet::new()));

#[cfg(test)]
thread_local! {
    /// Directories this thread fsynced through [`sync_dir_path`].
    static SYNCED_DIRS: std::cell::RefCell<Vec<PathBuf>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Take the directories this thread has fsynced since the last call.
#[cfg(test)]
pub(crate) fn take_synced_dirs() -> Vec<PathBuf> {
    SYNCED_DIRS.with(|synced| std::mem::take(&mut *synced.borrow_mut()))
}

/// Fsync the directory at `path`.
///
/// # Errors
///
/// Fails when the directory cannot be opened or synced, or the platform has
/// no durable directory sync.
pub(crate) fn sync_dir_path(path: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    SYNCED_DIRS.with(|synced| synced.borrow_mut().push(path.to_path_buf()));
    #[cfg(unix)]
    {
        std::fs::File::open(path)?.sync_all()
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FlushFileBuffers requires GENERIC_WRITE even for a directory handle
        // opened with backup semantics.
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(winapi::um::winbase::FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)?
            .sync_all()
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!(
                "durable directory sync is not supported for {}",
                path.display()
            ),
        ))
    }
}

/// Create `dir` and every missing ancestor below `root`, and make each entry
/// from `root` down to `dir` durable by fsyncing its parent.
///
/// Each entry is synced once per process and recorded only after its sync
/// completes. A caller that finds a directory another thread has created but
/// not yet synced therefore syncs it itself, rather than trusting that it
/// exists. `root` must already be durable: its own entry is the caller's.
///
/// # Errors
///
/// Fails when a directory cannot be created or synced, or `dir` is not
/// inside `root`.
pub(crate) fn create_dir_all_durably(root: &Path, dir: &Path) -> std::io::Result<()> {
    let relative = dir.strip_prefix(root).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "durable directory {} is outside its root {}",
                dir.display(),
                root.display()
            ),
        )
    })?;
    std::fs::create_dir_all(dir)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let parent = current.clone();
        current.push(component);
        if DURABLE_DIRS.lock().contains(&current) {
            continue;
        }
        sync_dir_path(&parent)?;
        DURABLE_DIRS.lock().insert(current.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_sync_the_parent_of_every_created_directory_below_root() {
        // Arrange
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().to_path_buf();
        take_synced_dirs();

        // Act
        create_dir_all_durably(&root, &root.join("a/b/c")).expect("create durably");

        // Assert
        assert!(root.join("a/b/c").is_dir());
        assert_eq!(
            take_synced_dirs(),
            [root.clone(), root.join("a"), root.join("a/b")]
        );
    }

    #[test]
    fn should_sync_parent_when_directory_exists_but_was_never_made_durable() {
        // Arrange: another thread created the directory and has not yet
        // synced its parent; finding it present proves nothing (#519).
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().to_path_buf();
        std::fs::create_dir(root.join("sst")).expect("plain mkdir");
        take_synced_dirs();

        // Act
        create_dir_all_durably(&root, &root.join("sst")).expect("create durably");

        // Assert
        assert_eq!(take_synced_dirs(), std::slice::from_ref(&root));
    }

    #[test]
    fn should_not_resync_a_directory_already_made_durable() {
        // Arrange
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().to_path_buf();
        create_dir_all_durably(&root, &root.join("wal")).expect("first create");
        take_synced_dirs();

        // Act
        create_dir_all_durably(&root, &root.join("wal")).expect("second create");

        // Assert
        assert!(take_synced_dirs().is_empty());
    }

    #[test]
    fn should_reject_directory_outside_its_root() {
        // Arrange
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().join("root");

        // Act
        let result = create_dir_all_durably(&root, &temp.path().join("elsewhere"));

        // Assert
        assert!(result.is_err());
        assert!(!temp.path().join("elsewhere").exists());
    }
}
