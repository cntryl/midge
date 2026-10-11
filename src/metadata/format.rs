use crate::common::{MidgeError, MidgeResult};
use crate::io::{staging, Fs, FsError, FsPath};
use std::path::Path;
use std::sync::Arc;

pub const CURRENT_FORMAT_VERSION: u32 = 4;
/// Oldest on-disk format this build opens. Version 4 changed only the
/// manifest key-bound encoding (#546), which still decodes version 3
/// manifests, so a writable open upgrades a version 3 marker in place.
const MIN_SUPPORTED_FORMAT_VERSION: u32 = 3;
const FORMAT_FILE: &str = "FORMAT";
const FORMAT_FILE_TEMP: &str = "FORMAT.tmp";
const FORMAT_PREFIX: &str = "midge-format-version=";

pub(crate) fn current_format_marker_bytes() -> Vec<u8> {
    format!("{FORMAT_PREFIX}{CURRENT_FORMAT_VERSION}\n").into_bytes()
}

/// Validate the database's FORMAT marker, creating it for a new database.
/// `db_path` names the database in error messages; all I/O goes through `fs`.
///
/// # Errors
///
/// Returns `CompatibilityError` for an unsupported or malformed marker, or
/// for a database that holds state without one, and the filesystem error when
/// the marker cannot be read or published.
pub fn ensure_or_create_format_marker(fs: &Arc<dyn Fs>, db_path: &Path) -> MidgeResult<u32> {
    if fs
        .exists(&FsPath::new(FORMAT_FILE))
        .map_err(FsError::into_midge)?
    {
        // Upgrade before this process writes any newer-format metadata, so
        // an older build refuses the database instead of misreading it.
        if validate_format_marker(fs.as_ref(), db_path)? < CURRENT_FORMAT_VERSION {
            publish_current_marker(fs)?;
        }
        return Ok(CURRENT_FORMAT_VERSION);
    }

    if has_persisted_state_without_format_marker(fs.as_ref())? {
        return Err(MidgeError::CompatibilityError(format!(
            "database at '{}' contains persisted state but no {FORMAT_FILE} marker; on-disk state without an explicit format marker is unsupported and must be rebuilt or re-imported",
            db_path.display()
        )));
    }

    publish_current_marker(fs)?;
    Ok(CURRENT_FORMAT_VERSION)
}

/// Durably publish the current format marker, replacing any older one.
fn publish_current_marker(fs: &Arc<dyn Fs>) -> MidgeResult<()> {
    staging::stage_bytes_with_hook(
        fs,
        &FsPath::new(FORMAT_FILE_TEMP),
        &FsPath::new(FORMAT_FILE),
        &current_format_marker_bytes(),
        || {
            crate::failpoints::fail_point!("midge::format::before_publish", |_| Err(FsError::Io(
                "failpoint: FORMAT publication failed".to_string()
            )));
            Ok(())
        },
        FsError::Io,
    )
    .map_err(FsError::into_midge)?;
    Ok(())
}

/// Read and validate the FORMAT marker without writing anything. `db_path`
/// names the database in error messages; all I/O goes through `fs`.
///
/// # Errors
///
/// Returns `CompatibilityError` for a missing, malformed or unsupported
/// marker, and the filesystem error when it cannot be read.
pub fn validate_format_marker(fs: &dyn Fs, db_path: &Path) -> MidgeResult<u32> {
    let marker_path = db_path.join(FORMAT_FILE);
    let bytes =
        crate::io::read_all(fs, &FsPath::new(FORMAT_FILE)).map_err(|error| match error {
            FsError::NotFound(message) => MidgeError::CompatibilityError(format!(
                "failed to read {FORMAT_FILE} marker at '{}': {message}",
                marker_path.display()
            )),
            error => error.into_midge(),
        })?;
    let invalid = || {
        MidgeError::CompatibilityError(format!(
            "invalid {FORMAT_FILE} marker at '{}': expected '{FORMAT_PREFIX}<version>'",
            marker_path.display()
        ))
    };
    let contents = std::str::from_utf8(&bytes).map_err(|_| invalid())?;

    // The marker is matched verbatim (lsm-spec manifest.md §2): exactly the
    // prefix, ASCII decimal digits, and one trailing newline.
    let version = contents
        .strip_prefix(FORMAT_PREFIX)
        .and_then(|rest| rest.strip_suffix('\n'))
        .filter(|digits| !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(invalid)?
        .parse::<u32>()
        .map_err(|error| {
            MidgeError::CompatibilityError(format!(
                "invalid {FORMAT_FILE} marker version at '{}': {error}",
                marker_path.display()
            ))
        })?;

    if !(MIN_SUPPORTED_FORMAT_VERSION..=CURRENT_FORMAT_VERSION).contains(&version) {
        return Err(MidgeError::CompatibilityError(format!(
            "unsupported on-disk format version {version} at '{}'; this build supports versions {MIN_SUPPORTED_FORMAT_VERSION}..={CURRENT_FORMAT_VERSION}",
            db_path.display()
        )));
    }

    Ok(version)
}

fn has_persisted_state_without_format_marker(fs: &dyn Fs) -> MidgeResult<bool> {
    const ROOT_STATE_FILES: [&str; 7] = [
        super::files::MANIFEST,
        super::files::MANIFEST_SNAPSHOT,
        super::files::INTENT_LOG,
        "manifest.yaml",
        "manifest.snapshot",
        super::files::JOURNAL,
        "intent_log.yaml",
    ];

    for file_name in ROOT_STATE_FILES {
        if fs
            .exists(&FsPath::new(file_name))
            .map_err(FsError::into_midge)?
        {
            return Ok(true);
        }
    }

    for dir_name in ["wal", "sst"] {
        let dir = FsPath::new(dir_name);
        if fs.exists(&dir).map_err(FsError::into_midge)?
            && !fs.list_dir(&dir).map_err(FsError::into_midge)?.is_empty()
        {
            return Ok(true);
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(path: &std::path::Path) -> Arc<dyn Fs> {
        Arc::new(crate::io::RealFs::new(path).expect("database root"))
    }

    #[test]
    fn should_create_format_marker_atomically_given_new_database_when_opening() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");

        // Act
        let version = ensure_or_create_format_marker(&root(temp_dir.path()), temp_dir.path())
            .expect("create marker");

        // Assert
        assert_eq!(version, CURRENT_FORMAT_VERSION);
        assert_eq!(
            std::fs::read_to_string(temp_dir.path().join(FORMAT_FILE))
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
        let error = ensure_or_create_format_marker(&root(temp_dir.path()), temp_dir.path())
            .expect_err("injected publication failure must be visible");
        let marker_existed_after_failure = temp_dir.path().join(FORMAT_FILE).exists();
        let temp_existed_after_failure = temp_dir.path().join(FORMAT_FILE_TEMP).exists();
        fail::remove("midge::format::before_publish");
        let retry = ensure_or_create_format_marker(&root(temp_dir.path()), temp_dir.path());
        scenario.teardown();

        // Assert
        assert!(matches!(error, MidgeError::Io(_)));
        assert!(!marker_existed_after_failure);
        assert!(!temp_existed_after_failure);
        retry.expect("retry after pre-publish failure should create FORMAT");
        assert_eq!(
            validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path())
                .expect("validate retried FORMAT"),
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
        let error = ensure_or_create_format_marker(&root(temp_dir.path()), temp_dir.path())
            .expect_err("legacy format");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[test]
    fn should_upgrade_version_three_marker_when_opening_writable_database() {
        // Arrange: version 4 only changed the manifest key-bound encoding,
        // which still decodes version 3 manifests (#546).
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            temp_dir.path().join(FORMAT_FILE),
            format!("{FORMAT_PREFIX}3\n"),
        )
        .expect("write version 3 marker");

        // Act
        let version = ensure_or_create_format_marker(&root(temp_dir.path()), temp_dir.path())
            .expect("open v3");

        // Assert
        assert_eq!(version, 4);
        assert_eq!(
            std::fs::read_to_string(temp_dir.path().join(FORMAT_FILE)).expect("read marker"),
            format!("{FORMAT_PREFIX}4\n")
        );
    }

    #[test]
    fn should_accept_version_three_marker_without_writing_when_validating() {
        // Arrange: read-only paths such as verification must not mutate disk.
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            temp_dir.path().join(FORMAT_FILE),
            format!("{FORMAT_PREFIX}3\n"),
        )
        .expect("write version 3 marker");

        // Act
        let version = validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path())
            .expect("validate v3");

        // Assert
        assert_eq!(version, 3);
        assert_eq!(
            std::fs::read_to_string(temp_dir.path().join(FORMAT_FILE)).expect("read marker"),
            format!("{FORMAT_PREFIX}3\n")
        );
    }

    #[test]
    fn should_reject_open_given_format_version_older_than_three() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            temp_dir.path().join(FORMAT_FILE),
            format!("{FORMAT_PREFIX}2\n"),
        )
        .expect("write version 2 marker");

        // Act
        let error = validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path())
            .expect_err("too old");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[test]
    fn should_reject_open_given_future_format_version_when_starting() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            temp_dir.path().join(FORMAT_FILE),
            format!("{}{}\n", FORMAT_PREFIX, CURRENT_FORMAT_VERSION + 1),
        )
        .expect("write marker");

        // Act
        let error = validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path())
            .expect_err("unknown version");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[test]
    fn should_reject_open_given_invalid_format_marker_when_starting() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(temp_dir.path().join(FORMAT_FILE), "not-a-format-marker\n")
            .expect("write malformed format marker");

        // Act
        let error = validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path())
            .expect_err("malformed marker");

        // Assert
        assert!(matches!(
            error,
            MidgeError::CompatibilityError(message)
                if message.contains("invalid FORMAT marker")
                    && message.contains("expected 'midge-format-version=<version>'")
        ));
    }

    #[test]
    fn should_reject_open_given_non_verbatim_format_marker_when_starting() {
        // Arrange
        let non_verbatim_markers = [
            format!(" {FORMAT_PREFIX}{CURRENT_FORMAT_VERSION}\n"),
            format!("{FORMAT_PREFIX}{CURRENT_FORMAT_VERSION} \n"),
            format!("{FORMAT_PREFIX}{CURRENT_FORMAT_VERSION}\n\n"),
            format!("{FORMAT_PREFIX}{CURRENT_FORMAT_VERSION}\r\n"),
            format!("{FORMAT_PREFIX}{CURRENT_FORMAT_VERSION}"),
            format!("{FORMAT_PREFIX}+{CURRENT_FORMAT_VERSION}\n"),
            format!("{FORMAT_PREFIX} {CURRENT_FORMAT_VERSION}\n"),
        ];

        for marker in non_verbatim_markers {
            let temp_dir = tempfile::tempdir().expect("temp dir");
            std::fs::write(temp_dir.path().join(FORMAT_FILE), &marker)
                .expect("write non-verbatim format marker");

            // Act
            let result = validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path());

            // Assert
            assert!(
                matches!(result, Err(MidgeError::CompatibilityError(_))),
                "marker {marker:?} must be rejected, got {result:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn should_report_io_error_given_inaccessible_format_marker_when_validating() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        ensure_or_create_format_marker(&root(temp_dir.path()), temp_dir.path())
            .expect("create marker");
        let marker_path = temp_dir.path().join(FORMAT_FILE);
        std::fs::remove_file(&marker_path).expect("remove FORMAT file");
        std::fs::create_dir(&marker_path).expect("replace FORMAT with directory");

        // Act
        let error = validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path())
            .expect_err("inaccessible marker");

        // Assert
        assert!(matches!(error, MidgeError::Io(_)));
    }
}
