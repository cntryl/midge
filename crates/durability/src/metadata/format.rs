use cntryl_midge_core::common::{MidgeError, MidgeResult};
use cntryl_midge_core::io::{staging, Fs, FsError, FsPath};
use std::path::Path;
use std::sync::Arc;

/// Version 9 uses shared binary metadata and append-only intent edits.
/// Version 8 persists per-family flush frontiers and shared cloud retirement
/// generations. Version 7 chains the cloud WAL publication catalog (catalog v2) and drops
/// the metadata fields and encodings older databases carried (#844). Version
/// 6 links WAL segments with headers and sealed footers, writes records
/// without a per-record writer epoch, and reserves sequences in the LEDGER v2
/// (#843). Version 5 named WAL history by the segment ledger (`wal/LEDGER`,
/// #842) instead of a directory listing.
pub const CURRENT_FORMAT_VERSION: u32 = 9;
/// Oldest on-disk format this build opens. Older databases are rejected, not
/// upgraded: their cloud catalogs are unchained.
const MIN_SUPPORTED_FORMAT_VERSION: u32 = CURRENT_FORMAT_VERSION;
const MAX_SUPPORTED_FORMAT_VERSION: u32 = CURRENT_FORMAT_VERSION + 1;
const FORMAT_FILE: &str = "FORMAT";
const FORMAT_FILE_TEMP: &str = "FORMAT.tmp";
const FORMAT_PREFIX: &str = "midge-format-version=";

#[must_use]
pub fn current_format_marker_bytes() -> Vec<u8> {
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
        return validate_format_marker(fs.as_ref(), db_path);
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

/// Durably publish the current format marker for a new database.
fn publish_current_marker(fs: &Arc<dyn Fs>) -> MidgeResult<()> {
    staging::stage_bytes_with_hook(
        fs,
        &FsPath::new(FORMAT_FILE_TEMP),
        &FsPath::new(FORMAT_FILE),
        &current_format_marker_bytes(),
        || {
            cntryl_midge_core::failpoints::fail_point!("midge::format::before_publish", |_| Err(
                FsError::Io("failpoint: FORMAT publication failed".to_string())
            ));
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
    let bytes = read_format_marker(fs).map_err(|error| match error {
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

    if !(MIN_SUPPORTED_FORMAT_VERSION..=MAX_SUPPORTED_FORMAT_VERSION).contains(&version) {
        return Err(MidgeError::CompatibilityError(format!(
            "unsupported on-disk format version {version} at '{}'; this build supports versions {MIN_SUPPORTED_FORMAT_VERSION}..={MAX_SUPPORTED_FORMAT_VERSION}",
            db_path.display()
        )));
    }

    if version == MAX_SUPPORTED_FORMAT_VERSION {
        if bytes.as_ref() != b"midge-format-version=10\n" {
            return Err(invalid());
        }
        super::local_authority::LocalMetadataAuthority::validate(fs)?;
    }

    Ok(version)
}

pub fn is_format_10(fs: &dyn Fs) -> MidgeResult<bool> {
    let path = FsPath::new(FORMAT_FILE);
    if !fs.exists(&path).map_err(FsError::into_midge)? {
        return Ok(false);
    }
    let bytes = read_format_marker(fs).map_err(FsError::into_midge)?;
    let version_9 = format!("{FORMAT_PREFIX}{CURRENT_FORMAT_VERSION}\n");
    let version_10 = format!("{FORMAT_PREFIX}{MAX_SUPPORTED_FORMAT_VERSION}\n");
    if bytes.as_ref() == version_10.as_bytes() {
        return Ok(true);
    }
    if bytes.as_ref() == version_9.as_bytes() {
        return Ok(false);
    }
    // Malformed and unsupported markers must not route through the v9 path.
    validate_format_marker(fs, Path::new("."))?;
    Ok(false)
}

fn read_format_marker(fs: &dyn Fs) -> Result<bytes::Bytes, FsError> {
    const MAX_FORMAT_MARKER_BYTES: u64 = 64;
    let path = FsPath::new(FORMAT_FILE);
    let metadata = fs.metadata(&path)?;
    if metadata.is_dir {
        return Err(FsError::Io(format!("{FORMAT_FILE}: Is a directory")));
    }
    if metadata.len > MAX_FORMAT_MARKER_BYTES {
        return Err(FsError::Corruption(format!(
            "FORMAT marker length {} exceeds bound {MAX_FORMAT_MARKER_BYTES}",
            metadata.len
        )));
    }
    let file = fs.open_read(&path)?;
    let length = file.len()?;
    if length > MAX_FORMAT_MARKER_BYTES || length != metadata.len {
        return Err(FsError::Corruption(
            "FORMAT marker length changed or exceeds bound".into(),
        ));
    }
    file.read_at(0, length)
}

fn has_persisted_state_without_format_marker(fs: &dyn Fs) -> MidgeResult<bool> {
    const ROOT_STATE_FILES: [&str; 9] = [
        super::files::MANIFEST,
        super::files::MANIFEST_SNAPSHOT,
        super::files::INTENT_LOG,
        "metadata/HEAD",
        "metadata/HEAD.tmp",
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

    for dir_name in ["wal", "sst", "metadata/generations"] {
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
        Arc::new(cntryl_midge_core::io::RealFs::new(path).expect("database root"))
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
        let _test_guard = cntryl_midge_core::failpoints::test_failpoint_guard();
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
    fn should_reject_version_four_marker_without_writing_when_opening() {
        // Arrange: version 4 databases have no WAL ledger (#842).
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            temp_dir.path().join(FORMAT_FILE),
            format!("{FORMAT_PREFIX}4\n"),
        )
        .expect("write version 4 marker");

        // Act
        let error = ensure_or_create_format_marker(&root(temp_dir.path()), temp_dir.path())
            .expect_err("version 4 is unsupported");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
        assert_eq!(
            std::fs::read_to_string(temp_dir.path().join(FORMAT_FILE)).expect("read marker"),
            format!("{FORMAT_PREFIX}4\n")
        );
    }

    #[test]
    fn should_reject_open_given_format_version_older_than_nine() {
        for version in [5, 6, 7, 8] {
            // Arrange
            let temp_dir = tempfile::tempdir().expect("temp dir");
            std::fs::write(
                temp_dir.path().join(FORMAT_FILE),
                format!("{FORMAT_PREFIX}{version}\n"),
            )
            .expect("write older marker");

            // Act
            let result = validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path());

            // Assert
            assert!(
                matches!(result, Err(MidgeError::CompatibilityError(_))),
                "format {version} must be rejected, got {result:?}"
            );
        }
    }

    #[test]
    fn should_reject_open_given_future_format_version_when_starting() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            temp_dir.path().join(FORMAT_FILE),
            format!("{}{}\n", FORMAT_PREFIX, CURRENT_FORMAT_VERSION + 2),
        )
        .expect("write marker");

        // Act
        let error = validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path())
            .expect_err("unknown version");

        // Assert
        assert!(matches!(error, MidgeError::CompatibilityError(_)));
    }

    #[test]
    fn should_reject_format_10_marker_without_a_valid_selected_head() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            temp_dir.path().join(FORMAT_FILE),
            format!("{FORMAT_PREFIX}{}\n", CURRENT_FORMAT_VERSION + 1),
        )
        .expect("write v10 marker without head");

        // Act
        let result = validate_format_marker(root(temp_dir.path()).as_ref(), temp_dir.path());

        // Assert
        assert!(
            result.is_err(),
            "FORMAT 10 without a valid HEAD must fail closed"
        );
    }

    #[test]
    fn should_not_create_format_9_marker_when_head_exists_without_format_marker() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        std::fs::create_dir_all(temp_dir.path().join("metadata")).expect("create metadata dir");
        std::fs::write(
            temp_dir.path().join("metadata/HEAD"),
            b"selected v10 authority",
        )
        .expect("write head");

        // Act
        let result = ensure_or_create_format_marker(&root(temp_dir.path()), temp_dir.path());

        // Assert
        assert!(matches!(result, Err(MidgeError::CompatibilityError(_))));
        assert!(!temp_dir.path().join(FORMAT_FILE).exists());
    }

    #[test]
    fn should_not_create_format_9_marker_when_unselected_generation_exists_without_head() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let generation = temp_dir
            .path()
            .join("metadata/generations/unselected/manifest");
        std::fs::create_dir_all(&generation).expect("create unselected generation");
        std::fs::write(generation.join("base"), b"staged base").expect("write staged base");

        // Act
        let result = ensure_or_create_format_marker(&root(temp_dir.path()), temp_dir.path());

        // Assert
        assert!(matches!(result, Err(MidgeError::CompatibilityError(_))));
        assert!(!temp_dir.path().join(FORMAT_FILE).exists());
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
