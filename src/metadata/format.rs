use crate::common::{MidgeError, MidgeResult};
use std::path::{Path, PathBuf};

pub const CURRENT_FORMAT_VERSION: u32 = 3;
const FORMAT_FILE: &str = "FORMAT";
const FORMAT_PREFIX: &str = "midge-format-version=";

pub fn format_marker_path(db_path: &Path) -> PathBuf {
    db_path.join(FORMAT_FILE)
}

pub fn ensure_or_create_format_marker(db_path: &Path) -> MidgeResult<u32> {
    std::fs::create_dir_all(db_path)?;

    let marker_path = format_marker_path(db_path);
    if marker_path.exists() {
        return validate_format_marker(db_path);
    }

    if has_persisted_state_without_format_marker(db_path)? {
        return Err(MidgeError::CompatibilityError(format!(
            "database at '{}' contains persisted state but no {} marker; on-disk state without an explicit format marker is unsupported and must be rebuilt or re-imported",
            db_path.display(),
            FORMAT_FILE
        )));
    }

    std::fs::write(
        &marker_path,
        format!("{FORMAT_PREFIX}{CURRENT_FORMAT_VERSION}\n"),
    )?;
    Ok(CURRENT_FORMAT_VERSION)
}

pub fn validate_format_marker(db_path: &Path) -> MidgeResult<u32> {
    let marker_path = format_marker_path(db_path);
    let contents = std::fs::read_to_string(&marker_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            MidgeError::CompatibilityError(format!(
                "failed to read {} marker at '{}': {}",
                FORMAT_FILE,
                marker_path.display(),
                error
            ))
        } else {
            MidgeError::Io(error)
        }
    })?;

    let version = contents
        .trim()
        .strip_prefix(FORMAT_PREFIX)
        .ok_or_else(|| {
            MidgeError::CompatibilityError(format!(
                "invalid {} marker at '{}': expected '{}<version>'",
                FORMAT_FILE,
                marker_path.display(),
                FORMAT_PREFIX
            ))
        })?
        .parse::<u32>()
        .map_err(|error| {
            MidgeError::CompatibilityError(format!(
                "invalid {} marker version at '{}': {}",
                FORMAT_FILE,
                marker_path.display(),
                error
            ))
        })?;

    if version != CURRENT_FORMAT_VERSION {
        return Err(MidgeError::CompatibilityError(format!(
            "unsupported on-disk format version {} at '{}'; this build expects version {}",
            version,
            db_path.display(),
            CURRENT_FORMAT_VERSION
        )));
    }

    Ok(version)
}

fn has_persisted_state_without_format_marker(db_path: &Path) -> MidgeResult<bool> {
    const ROOT_STATE_FILES: [&str; 7] = [
        "manifest.json",
        "manifest.snapshot.json",
        "intent_log.json",
        "manifest.yaml",
        "manifest.snapshot",
        "manifest.journal",
        "intent_log.yaml",
    ];

    for file_name in ROOT_STATE_FILES {
        if db_path.join(file_name).exists() {
            return Ok(true);
        }
    }

    for dir_name in ["wal", "sst"] {
        let dir = db_path.join(dir_name);
        if !dir.exists() {
            continue;
        }

        let mut entries = std::fs::read_dir(&dir)?;
        if entries.next().transpose()?.is_some() {
            return Ok(true);
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_format_marker_atomically_given_new_database_when_opening() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");

        // Act
        let version = ensure_or_create_format_marker(temp_dir.path()).expect("create marker");

        // Assert
        assert_eq!(version, CURRENT_FORMAT_VERSION);
        assert!(format_marker_path(temp_dir.path()).exists());
    }

    #[test]
    fn should_fail_given_legacy_state_without_format_marker() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(temp_dir.path().join("manifest.yaml"), "legacy: true\n")
            .expect("write legacy manifest");

        // Act
        let error = ensure_or_create_format_marker(temp_dir.path()).expect_err("legacy format");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[test]
    fn should_fail_given_current_manifest_without_format_marker() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(temp_dir.path().join("manifest.json"), "{}\n")
            .expect("write manifest without marker");

        // Act
        let error =
            ensure_or_create_format_marker(temp_dir.path()).expect_err("missing format marker");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[test]
    fn should_fail_given_current_intent_log_without_format_marker() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(temp_dir.path().join("intent_log.json"), "[]\n")
            .expect("write intent log without marker");

        // Act
        let error =
            ensure_or_create_format_marker(temp_dir.path()).expect_err("missing format marker");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[test]
    fn should_reject_open_given_future_format_version_when_starting() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            format_marker_path(temp_dir.path()),
            format!("{}{}\n", FORMAT_PREFIX, CURRENT_FORMAT_VERSION + 1),
        )
        .expect("write marker");

        // Act
        let error = validate_format_marker(temp_dir.path()).expect_err("unknown version");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[test]
    fn should_reject_open_given_invalid_format_marker_when_starting() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            format_marker_path(temp_dir.path()),
            format!("{}{}\n", FORMAT_PREFIX, CURRENT_FORMAT_VERSION - 1),
        )
        .expect("write previous format marker");

        // Act
        let error = validate_format_marker(temp_dir.path()).expect_err("previous version");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[cfg(unix)]
    #[test]
    fn should_report_io_error_given_inaccessible_format_marker_when_validating() {
        use std::os::unix::fs::PermissionsExt;

        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        ensure_or_create_format_marker(temp_dir.path()).expect("create marker");
        let marker_path = format_marker_path(temp_dir.path());
        let original_permissions = std::fs::metadata(&marker_path)
            .expect("FORMAT metadata")
            .permissions();
        let mut unreadable_permissions = original_permissions.clone();
        unreadable_permissions.set_mode(0o000);
        std::fs::set_permissions(&marker_path, unreadable_permissions)
            .expect("make FORMAT unreadable");

        // Act
        let error = validate_format_marker(temp_dir.path()).expect_err("inaccessible marker");
        std::fs::set_permissions(&marker_path, original_permissions)
            .expect("restore FORMAT access");

        // Assert
        assert!(matches!(error, MidgeError::Io(_)));
    }
}
