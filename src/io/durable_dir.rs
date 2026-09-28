//! Durable directory creation (#519).
//!
//! A new directory entry survives a crash only once its parent directory is
//! fsynced. `std::fs::create_dir_all` leaves that to whatever fsync happens
//! next, which made durability depend on startup ordering.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

/// Canonical directories whose entries this process has already made durable
/// and has not removed or reinitialized through the rooted filesystem.
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

/// Forget cached directory entries at or below `path` after removing or
/// recreating that part of the rooted filesystem. A path-only cache entry
/// cannot prove that a newly created directory is the same durable entry.
pub(crate) fn forget_durable_dirs_under(path: &Path) {
    DURABLE_DIRS
        .lock()
        .retain(|directory| !directory.starts_with(path));
}

/// Create a filesystem root and persist every new directory entry on the way
/// to it. Existing roots have their parent synced as well, covering roots
/// created by another startup step before Midge begins writing durable state.
///
/// # Errors
///
/// Fails when any directory cannot be created or its parent cannot be synced.
pub(crate) fn create_path_durably(path: &Path) -> std::io::Result<()> {
    let requested = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    let mut missing = Vec::new();
    let mut ancestor = requested.as_path();
    loop {
        match std::fs::metadata(ancestor) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotADirectory,
                    format!(
                        "filesystem root ancestor is not a directory: {}",
                        ancestor.display()
                    ),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = ancestor.file_name().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "no existing ancestor for filesystem root {}",
                            path.display()
                        ),
                    )
                })?;
                missing.push(name.to_os_string());
                ancestor = ancestor.parent().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "no existing ancestor for filesystem root {}",
                            path.display()
                        ),
                    )
                })?;
            }
            Err(error) => return Err(error),
        }
    }

    let mut current = std::fs::canonicalize(ancestor)?;
    for name in missing.into_iter().rev() {
        current.push(name);
        match std::fs::create_dir(&current) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !std::fs::metadata(&current)?.is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotADirectory,
                        format!(
                            "filesystem root component is not a directory: {}",
                            current.display()
                        ),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
        let parent = current.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("filesystem root has no parent: {}", current.display()),
            )
        })?;
        sync_dir_path(parent)?;
    }

    let canonical = std::fs::canonicalize(path)?;
    let parent = canonical.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("filesystem root has no parent: {}", canonical.display()),
        )
    })?;
    sync_dir_path(parent)?;
    forget_durable_dirs_under(&canonical);
    Ok(())
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
/// Each canonical entry is recorded only after its parent sync completes. A
/// caller that finds a directory another thread has created but not yet synced
/// therefore syncs it itself, rather than trusting that it exists. Directory
/// removal and root reinitialization invalidate the affected cache entries.
/// `root` must already be durable: its own entry is the caller's.
///
/// # Errors
///
/// Fails when a directory cannot be created or synced, or `dir` is not
/// inside `root`.
pub(crate) fn create_dir_all_durably(root: &Path, dir: &Path) -> std::io::Result<()> {
    dir.strip_prefix(root).map_err(|_| {
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
    let root = std::fs::canonicalize(root)?;
    let dir = std::fs::canonicalize(dir)?;
    let relative = dir.strip_prefix(&root).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "durable directory {} resolves outside its root {}",
                dir.display(),
                root.display()
            ),
        )
    })?;
    let mut current = root;
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
        let root = std::fs::canonicalize(temp.path()).expect("canonical root");
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
        let root = std::fs::canonicalize(temp.path()).expect("canonical root");
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
        let root = std::fs::canonicalize(temp.path()).expect("canonical root");
        create_dir_all_durably(&root, &root.join("wal")).expect("first create");
        take_synced_dirs();

        // Act
        create_dir_all_durably(&root, &root.join("wal")).expect("second create");

        // Assert
        assert!(take_synced_dirs().is_empty());
    }

    #[test]
    fn should_resync_parent_when_durable_directory_is_removed_and_recreated() {
        // Arrange
        let temp = tempfile::tempdir().expect("temp dir");
        let root = std::fs::canonicalize(temp.path()).expect("canonical root");
        let directory = root.join("db/wal");
        create_dir_all_durably(&root, &directory).expect("initial durable create");
        take_synced_dirs();

        // Act: a database directory can be removed and recreated at the same
        // path while this process remains alive.
        let fs = crate::io::real::RealFs::new(&root).expect("real filesystem");
        take_synced_dirs();
        crate::io::Fs::remove_dir_all(&fs, &crate::io::FsPath::new("db"))
            .expect("remove database directory");
        create_dir_all_durably(&root, &directory).expect("recreate durable directory");

        // Assert: both recreated directory entries need their parent synced.
        assert_eq!(take_synced_dirs(), [root.clone(), root.join("db")]);
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
