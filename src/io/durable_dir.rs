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

/// Remove a directory tree and invalidate its cached entries as one
/// operation relative to durable directory creation.
pub(crate) fn remove_dir_all_and_forget(
    path: &Path,
    remove: impl FnOnce() -> std::io::Result<()>,
) -> std::io::Result<()> {
    let mut durable_dirs = DURABLE_DIRS.lock();
    let result = remove();
    durable_dirs.retain(|directory| !directory.starts_with(path));
    result
}

/// Create a filesystem root and persist every new directory entry on the way
/// to it. Existing roots have their parent synced as well, covering roots
/// created by another startup step before Midge begins writing durable state.
///
/// # Errors
///
/// Fails when any directory cannot be created or its parent cannot be synced.
pub(crate) fn create_path_durably(path: &Path) -> std::io::Result<()> {
    let mut durable_dirs = DURABLE_DIRS.lock();
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

    let canonical = std::fs::canonicalize(&requested)?;
    let parent = canonical.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("filesystem root has no parent: {}", canonical.display()),
        )
    })?;
    let requested_parent = requested.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("filesystem root has no parent: {}", requested.display()),
        )
    })?;
    let requested_parent = std::fs::canonicalize(requested_parent)?;
    if requested_parent != parent {
        // When the configured root is a symlink, its own directory entry lives
        // in the configured parent, not beside the resolved target.
        sync_dir_path(&requested_parent)?;
    }
    sync_dir_path(parent)?;
    durable_dirs.retain(|directory| !directory.starts_with(&canonical));
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
    let mut durable_dirs = DURABLE_DIRS.lock();
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
    if relative
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "durable directory {} traverses above its root {}",
                dir.display(),
                root.display()
            ),
        ));
    }
    let root = std::fs::canonicalize(root)?;
    let mut current = root;
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            continue;
        };
        let candidate = current.join(name);
        match std::fs::symlink_metadata(&candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "durable directory component is a symlink: {}",
                        candidate.display()
                    ),
                ));
            }
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotADirectory,
                    format!(
                        "durable directory component is not a directory: {}",
                        candidate.display()
                    ),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::create_dir(&candidate) {
                    Ok(()) => {}
                    Err(create_error)
                        if create_error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(create_error) => return Err(create_error),
                }
                let metadata = std::fs::symlink_metadata(&candidate)?;
                if metadata.file_type().is_symlink() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "durable directory component is a symlink: {}",
                            candidate.display()
                        ),
                    ));
                }
                if !metadata.is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotADirectory,
                        format!(
                            "durable directory component is not a directory: {}",
                            candidate.display()
                        ),
                    ));
                }
            }
            Err(error) => return Err(error),
        }

        if !durable_dirs.contains(&candidate) {
            sync_dir_path(&current)?;
            durable_dirs.insert(candidate.clone());
        }
        current = candidate;
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
        assert_eq!(take_synced_dirs().len(), 0);
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

    #[cfg(unix)]
    #[test]
    fn should_reject_symlinked_storage_directory_before_creating_outside_root() {
        use std::os::unix::fs::symlink;

        // Arrange
        let root = tempfile::tempdir().expect("database root");
        let outside = tempfile::tempdir().expect("outside directory");
        std::fs::create_dir(root.path().join("sst")).expect("SST directory");
        std::fs::remove_dir(root.path().join("sst")).expect("remove SST directory");
        symlink(outside.path(), root.path().join("sst")).expect("symlink SST outside root");

        // Act
        let result = create_dir_all_durably(root.path(), &root.path().join("sst/created-outside"));

        // Assert
        assert!(result.is_err());
        assert!(!outside.path().join("created-outside").exists());
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
