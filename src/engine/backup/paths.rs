//! Resolve aliases before mutation and publish directories without replacement.
use crate::common::{MidgeError, MidgeResult};
use std::fs;
use std::path::{Component, Path, PathBuf};

pub(super) fn resolve(path: &Path) -> MidgeResult<PathBuf> {
    let absolute = absolute(path)?;
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => resolved.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(name) => {
                resolved.push(name);
                match fs::symlink_metadata(&resolved) {
                    Ok(_) => {
                        resolved = fs::canonicalize(&resolved).map_err(|error| {
                            MidgeError::InvalidArgument(format!(
                                "cannot resolve path '{}': {error}",
                                resolved.display()
                            ))
                        })?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
    Ok(resolved)
}

fn absolute(path: &Path) -> MidgeResult<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    #[cfg(windows)]
    {
        let mut components = path.components();
        if let Some(Component::Prefix(prefix)) = components.next() {
            if !matches!(prefix.kind(), std::path::Prefix::Disk(_)) {
                return Err(MidgeError::InvalidArgument(
                    "unsupported relative Windows path prefix".into(),
                ));
            }
            // C:child is relative to that drive's current directory. Resolve
            // only the drive anchor, preserving symlinks and parent components
            // in the remaining path for component-wise resolution below.
            let anchor = std::path::absolute(Path::new(prefix.as_os_str()))?;
            return Ok(anchor.join(components.as_path()));
        }
    }
    Ok(std::env::current_dir()?.join(path))
}

pub(super) fn disjoint(left: &Path, right: &Path) -> MidgeResult<()> {
    if left.starts_with(right) || right.starts_with(left) {
        return Err(MidgeError::InvalidArgument(format!(
            "backup/restore roots '{}' and '{}' overlap",
            left.display(),
            right.display()
        )));
    }
    Ok(())
}

pub(super) fn prepare_target(source: &Path, target: &Path) -> MidgeResult<PathBuf> {
    let target = resolve(target)?;
    disjoint(source, &target)?;
    if fs::symlink_metadata(&target).is_ok() {
        return Err(MidgeError::InvalidArgument(format!(
            "backup/restore target '{}' already exists",
            target.display()
        )));
    }
    let parent = target
        .parent()
        .ok_or_else(|| MidgeError::InvalidArgument("target requires a parent directory".into()))?;
    fs::create_dir_all(parent)?;
    // Resolve the actual parent again after creation, before choosing a stage.
    let target = fs::canonicalize(parent)?.join(
        target
            .file_name()
            .ok_or_else(|| MidgeError::InvalidArgument("target requires a filename".into()))?,
    );
    disjoint(source, &target)?;
    Ok(target)
}

pub(super) fn publish(stage: &Path, target: &Path) -> MidgeResult<()> {
    // Unlike a check followed by std::fs::rename, the native operation refuses
    // replacement even when another caller creates the target at this boundary.
    let result = publish_native(stage, target);
    result.map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists || fs::symlink_metadata(target).is_ok()
        {
            MidgeError::InvalidArgument(format!(
                "backup/restore target '{}' already exists",
                target.display()
            ))
        } else if error.kind() == std::io::ErrorKind::Unsupported {
            MidgeError::NotSupported(
                "atomic directory publication without replacement is unavailable".into(),
            )
        } else {
            error.into()
        }
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn publish_native(stage: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let stage = std::ffi::CString::new(stage.as_os_str().as_bytes())?;
    let target = std::ffi::CString::new(target.as_os_str().as_bytes())?;
    // SAFETY: both C strings remain alive; AT_FDCWD resolves absolute paths.
    #[cfg(target_os = "linux")]
    let status = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            stage.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(target_os = "macos")]
    let status = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            stage.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if status == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(libc::ENOSYS | libc::EINVAL | libc::EOPNOTSUPP)
    ) {
        return Err(std::io::Error::new(std::io::ErrorKind::Unsupported, error));
    }
    Err(error)
}

#[cfg(windows)]
fn publish_native(stage: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    let stage: Vec<_> = stage.as_os_str().encode_wide().chain(Some(0)).collect();
    let target: Vec<_> = target.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: both null-terminated UTF-16 buffers remain alive. Zero flags do
    // not permit replacement or a non-atomic cross-volume copy fallback.
    if unsafe { winapi::um::winbase::MoveFileExW(stage.as_ptr(), target.as_ptr(), 0) } != 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(50 | 120)) {
        return Err(std::io::Error::new(std::io::ErrorKind::Unsupported, error));
    }
    Err(error)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn publish_native(_stage: &Path, _target: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "native no-replace rename unavailable",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_preserve_existing_target_when_publishing_stage() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let stage = directory.path().join("stage");
        let target = directory.path().join("target");
        fs::create_dir(&stage).unwrap();
        fs::write(stage.join("payload"), b"new").unwrap();
        fs::create_dir(&target).unwrap();

        // Act
        let result = publish(&stage, &target);

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
        assert!(target.read_dir().unwrap().next().is_none());
        assert_eq!(fs::read(stage.join("payload")).unwrap(), b"new");
    }

    #[cfg(unix)]
    #[test]
    fn should_preserve_symlink_target_when_publishing_stage() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let stage = directory.path().join("stage");
        let target = directory.path().join("target");
        let foreign = directory.path().join("foreign");
        fs::create_dir(&stage).unwrap();
        fs::create_dir(&foreign).unwrap();
        std::os::unix::fs::symlink(&foreign, &target).unwrap();

        // Act
        let result = publish(&stage, &target);

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
        assert_eq!(fs::read_link(&target).unwrap(), foreign);
        assert!(stage.is_dir());
    }

    #[test]
    fn should_publish_directory_without_replacement() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let stage = directory.path().join("stage");
        let target = directory.path().join("target");
        fs::create_dir(&stage).unwrap();
        fs::write(stage.join("payload"), b"data").unwrap();

        // Act
        publish(&stage, &target).unwrap();

        // Assert
        assert!(!stage.exists());
        assert_eq!(fs::read(target.join("payload")).unwrap(), b"data");
    }
}
