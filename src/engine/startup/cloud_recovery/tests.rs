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
