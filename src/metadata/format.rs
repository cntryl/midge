use crate::common::{MidgeError, MidgeResult};
use crate::io::{staging, Fs, FsError, FsPath, RealFs};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const CURRENT_FORMAT_VERSION: u32 = 3;
const FORMAT_FILE: &str = "FORMAT";
const FORMAT_FILE_TEMP: &str = "FORMAT.tmp";
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

    let fs: Arc<dyn Fs> = Arc::new(RealFs::new(db_path)?);
    staging::stage_bytes_with_hook(
        &fs,
        &FsPath::new(FORMAT_FILE_TEMP),
        &FsPath::new(FORMAT_FILE),
        format!("{FORMAT_PREFIX}{CURRENT_FORMAT_VERSION}\n").as_bytes(),
        || {
            crate::failpoints::fail_point!("midge::format::before_publish", |_| Err(FsError::Io(
                "failpoint: FORMAT publication failed".to_string()
            )));
            Ok(())
        },
        FsError::Io,
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
        assert_eq!(
            std::fs::read_to_string(format_marker_path(temp_dir.path()))
                .expect("read published format marker"),
            format!("{FORMAT_PREFIX}{CURRENT_FORMAT_VERSION}\n")
        );
        assert!(!temp_dir.path().join(FORMAT_FILE_TEMP).exists());
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_clean_failed_format_publication_given_configured_failpoint_when_opening() {
        // Arrange
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let temp_dir = tempfile::tempdir().expect("temp dir");
        fail::cfg("midge::format::before_publish", "return")
            .expect("configure FORMAT publication failure");

        // Act
        let error = ensure_or_create_format_marker(temp_dir.path())
            .expect_err("injected publication failure must be visible");
        let marker_existed_after_failure = format_marker_path(temp_dir.path()).exists();
        let temp_existed_after_failure = temp_dir.path().join(FORMAT_FILE_TEMP).exists();
        fail::remove("midge::format::before_publish");
        let retry = ensure_or_create_format_marker(temp_dir.path());
        scenario.teardown();

        // Assert
        assert!(matches!(error, MidgeError::Io(_)));
        assert!(!marker_existed_after_failure);
        assert!(!temp_existed_after_failure);
        retry.expect("retry after pre-publish failure should create FORMAT");
        assert_eq!(
            validate_format_marker(temp_dir.path()).expect("validate retried FORMAT"),
            CURRENT_FORMAT_VERSION
        );
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
    fn should_reject_open_given_older_format_version_when_starting() {
        // Arrange: supported databases must match the current format exactly.
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            format_marker_path(temp_dir.path()),
            format!("{FORMAT_PREFIX}{}\n", CURRENT_FORMAT_VERSION - 1),
        )
        .expect("write marker");

        // Act
        let error = validate_format_marker(temp_dir.path()).expect_err("older version");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[test]
    fn should_reject_open_given_invalid_format_marker_when_starting() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(format_marker_path(temp_dir.path()), "not-a-format-marker\n")
            .expect("write malformed format marker");

        // Act
        let error = validate_format_marker(temp_dir.path()).expect_err("malformed marker");

        // Assert
        assert!(matches!(
            error,
            MidgeError::CompatibilityError(message)
                if message.contains("invalid FORMAT marker")
                    && message.contains("expected 'midge-format-version=<version>'")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn should_report_io_error_given_inaccessible_format_marker_when_validating() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        ensure_or_create_format_marker(temp_dir.path()).expect("create marker");
        let marker_path = format_marker_path(temp_dir.path());
        std::fs::remove_file(&marker_path).expect("remove FORMAT file");
        std::fs::create_dir(&marker_path).expect("replace FORMAT with directory");

        // Act
        let error = validate_format_marker(temp_dir.path()).expect_err("inaccessible marker");

        // Assert
        assert!(matches!(error, MidgeError::Io(_)));
    }
}
