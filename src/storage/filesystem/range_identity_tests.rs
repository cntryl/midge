use super::*;
use std::sync::mpsc;
use std::time::Duration;

const KEY: &str = "immutable.sst";

fn head(backend: &FileSystem) -> StorageObjectMetadata {
    let (tx, rx) = mpsc::channel();
    backend.submit_range_head(KEY, Duration::from_secs(2), tx);
    match rx.recv().expect("range HEAD response") {
        StorageEvent::HeadComplete {
            result: StorageOutcome::Ok(metadata),
            ..
        } => metadata,
        other => panic!("unexpected range HEAD: {other:?}"),
    }
}

fn read(backend: &FileSystem, metadata: StorageObjectMetadata) -> Result<Vec<u8>, String> {
    let (tx, rx) = mpsc::channel();
    backend.submit_read_range(KEY, 1, 4, metadata, Duration::from_secs(2), tx);
    rx.recv().expect("range read response")
}

fn delete(backend: &FileSystem, metadata: &StorageObjectMetadata) -> StorageOutcome<()> {
    let (tx, rx) = mpsc::channel();
    backend.submit_delete_with_headers(KEY, vec![("If-Match".into(), metadata.etag.clone())], tx);
    match rx.recv().expect("conditional delete response") {
        StorageEvent::DeleteComplete { result, .. } => result,
        other => panic!("unexpected delete: {other:?}"),
    }
}

#[test]
fn should_preserve_range_identity_when_unchanged_file_is_read_repeatedly() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let backend = FileSystem::new(directory.path())?;
    fs::write(directory.path().join(KEY), b"value")?;
    let before = head(&backend);
    // Act
    let first = read(&backend, before.clone()).expect("first pinned range");
    let second = read(&backend, before.clone()).expect("second pinned range");
    let after = head(&backend);
    // Assert
    assert_eq!(first, b"alu");
    assert_eq!(second, first);
    assert!(before.same_version(&after));
    assert!(before.etag.starts_with("fs:"));
    Ok(())
}

#[test]
fn should_reject_stale_range_authority_when_same_size_file_is_replaced() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let backend = FileSystem::new(directory.path())?;
    let path = directory.path().join(KEY);
    fs::write(&path, b"older")?;
    let before = head(&backend);
    let modified = fs::metadata(&path)?.modified()?;
    let replacement = directory.path().join("replacement");
    fs::write(&replacement, b"newer")?;
    fs::File::options()
        .write(true)
        .open(&replacement)?
        .set_modified(modified)?;
    // Act
    fs::remove_file(&path)?;
    fs::rename(&replacement, &path)?;
    let stale_read = read(&backend, before.clone());
    let stale_delete = delete(&backend, &before);
    let after = head(&backend);
    let current_read = read(&backend, after.clone()).expect("replacement range");
    let current_delete = delete(&backend, &after);
    // Assert
    assert_eq!(before.size, after.size);
    assert!(!before.same_version(&after));
    assert!(stale_read
        .expect_err("stale identity must fail")
        .contains("precondition failed"));
    assert!(stale_delete.is_err());
    assert_eq!(current_read, b"ewe");
    assert!(current_delete.is_ok());
    assert!(!path.exists());
    Ok(())
}

#[test]
fn should_reject_stale_range_authority_when_in_place_write_preserves_size_and_modified_time(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let backend = FileSystem::new(directory.path())?;
    let path = directory.path().join(KEY);
    fs::write(&path, b"older")?;
    let modified = fs::metadata(&path)?.modified()?;
    let before = head(&backend);
    let (tx, rx) = mpsc::channel();
    // Act
    backend.submit_write(KEY, b"newer".to_vec(), tx);
    let write = rx.recv().expect("in-place write response");
    fs::File::options()
        .write(true)
        .open(&path)?
        .set_modified(modified)?;
    let after = head(&backend);
    let stale_read = read(&backend, before.clone());
    let stale_delete = delete(&backend, &before);
    // Assert
    assert!(matches!(
        write,
        StorageEvent::WriteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }
    ));
    assert_eq!(before.size, after.size);
    assert_eq!(fs::metadata(&path)?.modified()?, modified);
    assert!(!before.same_version(&after));
    assert!(stale_read.is_err());
    assert!(stale_delete.is_err());
    assert_eq!(fs::read(path)?, b"newer");
    Ok(())
}

#[test]
fn should_exclude_independent_lock_handles_when_conditional_mutation_lock_is_held(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let backend = FileSystem::new(directory.path())?;
    let path = backend.full_path(KEY).expect("object path");
    let guard = backend
        .acquire_process_lock(&path)
        .expect("conditional mutation lock");
    let lock_path = fs::read_dir(directory.path().join(".midge-locks"))?
        .next()
        .expect("one lock file")?
        .path();
    let independent = fs::File::options().read(true).write(true).open(lock_path)?;
    // Act
    let held = independent.try_lock();
    drop(guard);
    let released = independent.try_lock();
    // Assert
    assert!(matches!(held, Err(fs::TryLockError::WouldBlock)));
    assert!(
        released.is_ok(),
        "process guard must release its OS lock: {released:?}"
    );
    independent.unlock()?;
    Ok(())
}
