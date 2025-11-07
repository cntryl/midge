//! Numbered file management utilities
//!
//! Common pattern for managing numbered files (e.g., WAL segments, SST files)
//! with zero-padded filenames for proper lexicographic ordering.

use crate::error::MidgeResult;
use std::path::{Path, PathBuf};

/// Generate a numbered file path with zero-padded filename
///
/// Creates paths like `NNNNNNNNNNNNNNNNNNNN.ext` (20-digit zero-padded)
/// to support the full u64 range and ensure proper lexicographic ordering.
///
/// # Examples
///
/// ```rust
/// # use midge::fs::numbered_file_path;
/// use std::path::Path;
///
/// let path = numbered_file_path(Path::new("/data"), 1, "wal");
/// assert_eq!(path.file_name().unwrap(), "00000000000000000001.wal");
///
/// let path = numbered_file_path(Path::new("/data"), 12345, "sst");
/// assert_eq!(path.file_name().unwrap(), "00000000000000012345.sst");
/// ```
pub fn numbered_file_path(dir: &Path, number: u64, extension: &str) -> PathBuf {
    dir.join(format!("{:020}.{}", number, extension))
}

/// Find the highest numbered file in a directory matching a pattern
///
/// Scans for files with format: `NNNNNNNNNNNNNNNNNNNN.ext` (20-digit zero-padded)
/// Returns the highest number found, or 0 if no matching files exist.
///
/// # Examples
///
/// ```rust,no_run
/// # use midge::fs::find_latest_numbered_file;
/// use std::path::Path;
///
/// // Find highest WAL file number
/// let latest = find_latest_numbered_file(Path::new("/wal"), "wal").unwrap();
/// if latest > 0 {
///     println!("Latest WAL file: {}", latest);
/// }
/// ```
pub fn find_latest_numbered_file(dir: &Path, extension: &str) -> MidgeResult<u64> {
    if !dir.exists() {
        return Ok(0); // No files exist yet
    }

    let mut max_num = 0u64;
    let entries = std::fs::read_dir(dir)?;
    let ext_with_dot = format!(".{}", extension);
    let expected_len = 20 + ext_with_dot.len(); // 20 digits + ".ext"

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            // Match pattern: NNNNNNNNNNNNNNNNNNNN.ext (20 digits + extension)
            if filename.ends_with(&ext_with_dot) && filename.len() == expected_len {
                // Extract the numeric part (first 20 characters)
                if let Ok(num) = filename[..20].parse::<u64>() {
                    max_num = max_num.max(num);
                }
            }
        }
    }

    Ok(max_num)
}

/// List all numbered files in a directory matching a pattern, sorted by number
///
/// Returns a vector of (number, path) tuples sorted in ascending order.
///
/// # Examples
///
/// ```rust,no_run
/// # use midge::fs::list_numbered_files;
/// use std::path::Path;
///
/// let files = list_numbered_files(Path::new("/sst"), "sst").unwrap();
/// for (num, path) in files {
///     println!("SST {}: {:?}", num, path);
/// }
/// ```
pub fn list_numbered_files(dir: &Path, extension: &str) -> MidgeResult<Vec<(u64, PathBuf)>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let entries = std::fs::read_dir(dir)?;
    let ext_with_dot = format!(".{}", extension);
    let expected_len = 20 + ext_with_dot.len();

    let mut files = Vec::new();

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            if filename.ends_with(&ext_with_dot) && filename.len() == expected_len {
                if let Ok(num) = filename[..20].parse::<u64>() {
                    files.push((num, path));
                }
            }
        }
    }

    // Sort by number (ascending)
    files.sort_by_key(|(num, _)| *num);

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_generate_zero_padded_filename() {
        // Arrange
        let dir = Path::new("/data");

        // Act
        let path1 = numbered_file_path(dir, 1, "wal");
        let path999 = numbered_file_path(dir, 999, "wal");
        let pathmax = numbered_file_path(dir, u64::MAX, "sst");

        // Assert
        assert_eq!(path1.file_name().unwrap(), "00000000000000000001.wal");
        assert_eq!(path999.file_name().unwrap(), "00000000000000000999.wal");
        assert_eq!(pathmax.file_name().unwrap(), "18446744073709551615.sst");
    }

    #[test]
    fn should_return_zero_when_directory_does_not_exist() {
        // Arrange
        let nonexistent = Path::new("/nonexistent_dir_12345");

        // Act
        let result = find_latest_numbered_file(nonexistent, "wal");

        // Assert
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn should_return_zero_when_directory_is_empty() {
        // Arrange
        let tmp = tempfile::tempdir().unwrap();

        // Act
        let result = find_latest_numbered_file(tmp.path(), "wal");

        // Assert
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn should_find_latest_numbered_file() {
        // Arrange
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("00000000000000000001.wal"), b"").unwrap();
        std::fs::write(tmp.path().join("00000000000000000005.wal"), b"").unwrap();
        std::fs::write(tmp.path().join("00000000000000000003.wal"), b"").unwrap();

        // Act
        let latest = find_latest_numbered_file(tmp.path(), "wal").unwrap();

        // Assert
        assert_eq!(latest, 5);
    }

    #[test]
    fn should_ignore_files_with_wrong_extension() {
        // Arrange
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("00000000000000000001.wal"), b"").unwrap();
        std::fs::write(tmp.path().join("00000000000000000005.sst"), b"").unwrap();
        std::fs::write(tmp.path().join("00000000000000000003.log"), b"").unwrap();

        // Act
        let latest_wal = find_latest_numbered_file(tmp.path(), "wal").unwrap();
        let latest_sst = find_latest_numbered_file(tmp.path(), "sst").unwrap();

        // Assert
        assert_eq!(latest_wal, 1);
        assert_eq!(latest_sst, 5);
    }

    #[test]
    fn should_ignore_files_with_wrong_length() {
        // Arrange
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("00000000000000000001.wal"), b"").unwrap();
        std::fs::write(tmp.path().join("123.wal"), b"").unwrap(); // Too short
        std::fs::write(tmp.path().join("000000000000000000000002.wal"), b"").unwrap(); // Too long

        // Act
        let latest = find_latest_numbered_file(tmp.path(), "wal").unwrap();

        // Assert
        assert_eq!(latest, 1); // Only the properly formatted file
    }

    #[test]
    fn should_list_numbered_files_in_order() {
        // Arrange
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("00000000000000000005.sst"), b"").unwrap();
        std::fs::write(tmp.path().join("00000000000000000001.sst"), b"").unwrap();
        std::fs::write(tmp.path().join("00000000000000000003.sst"), b"").unwrap();

        // Act
        let files = list_numbered_files(tmp.path(), "sst").unwrap();

        // Assert
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].0, 1);
        assert_eq!(files[1].0, 3);
        assert_eq!(files[2].0, 5);
    }

    #[test]
    fn should_return_empty_vec_when_no_matching_files() {
        // Arrange
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("00000000000000000001.wal"), b"").unwrap();

        // Act
        let files = list_numbered_files(tmp.path(), "sst").unwrap();

        // Assert
        assert!(files.is_empty());
    }
}
