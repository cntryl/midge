//! Startup inventory of the host-directory object store.

use super::{object_is_definitively_missing, FileSystem};
use crate::storage::inventory::ObjectInventory;

#[test]
fn should_treat_missing_child_as_indeterminate_when_classifying_under_a_file(
) -> crate::common::MidgeResult<()> {
    // Arrange: Unix reports this path as ENOTDIR before the classifier runs,
    // so exercise the classifier directly on every platform.
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("sst"), b"blocked parent")?;
    let not_found = std::io::Error::from(std::io::ErrorKind::NotFound);

    // Act
    let under_file =
        object_is_definitively_missing(&temp.path().join("sst").join("1.sst"), &not_found);
    let under_missing =
        object_is_definitively_missing(&temp.path().join("absent").join("1.sst"), &not_found);

    // Assert
    assert!(!under_file);
    assert!(!under_missing);
    Ok(())
}

#[test]
fn should_treat_missing_child_as_indeterminate_when_parent_is_a_file(
) -> crate::common::MidgeResult<()> {
    // Arrange: Windows reports a path below a regular file as not found.
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("sst"), b"blocked parent")?;
    let store = FileSystem::new(temp.path())?;

    // Act
    let size = store.object_size("sst/1.sst", None);

    // Assert
    assert!(size.is_err(), "{size:?}");
    Ok(())
}

#[test]
fn should_treat_missing_child_as_definitive_when_parent_is_a_directory(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    std::fs::create_dir(temp.path().join("sst"))?;
    let store = FileSystem::new(temp.path())?;

    // Act
    let size = store.object_size("sst/1.sst", None)?;

    // Assert
    assert_eq!(size, None);
    Ok(())
}

#[test]
fn should_report_object_size_when_object_exists() -> crate::common::MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    std::fs::create_dir(temp.path().join("sst"))?;
    std::fs::write(temp.path().join("sst/1.sst"), b"bytes")?;
    let store = FileSystem::new(temp.path())?;

    // Act
    let size = store.object_size("sst/1.sst", None)?;

    // Assert
    assert_eq!(size, Some(5));
    Ok(())
}

#[test]
fn should_list_nested_keys_under_prefix_when_inventorying_host_store(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    std::fs::create_dir_all(temp.path().join("wal/epochs/7"))?;
    std::fs::write(temp.path().join("wal/1.wal"), b"a")?;
    std::fs::write(temp.path().join("wal/epochs/7/2.wal"), b"b")?;
    std::fs::write(temp.path().join("walrus"), b"c")?;
    let store = FileSystem::new(temp.path())?;

    // Act
    let keys = store.keys_under("wal/", None)?;
    let absent = store.keys_under("sst/", None)?;

    // Assert
    assert_eq!(keys, ["wal/1.wal", "wal/epochs/7/2.wal"]);
    assert_eq!(absent, Vec::<String>::new());
    Ok(())
}

#[test]
fn should_fail_listing_when_prefix_directory_is_a_file() -> crate::common::MidgeResult<()> {
    // Arrange: an unlistable WAL prefix must not read as "no WAL objects".
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("wal"), b"not a directory")?;
    let store = FileSystem::new(temp.path())?;

    // Act
    let keys = store.keys_under("wal/", None);

    // Assert
    assert!(keys.is_err(), "{keys:?}");
    Ok(())
}

#[test]
fn should_not_create_store_when_opening_missing_directory() -> crate::common::MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("cloud_store");

    // Act
    let store = FileSystem::open_existing(&path)?;

    // Assert
    assert!(store.is_none());
    assert!(!path.exists());
    Ok(())
}
