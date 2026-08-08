use cntryl_midge::{
    Engine, EngineHealth, MidgeError, OpenOptions, Query, RecoveryPolicy, TransactionMode,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use xxhash_rust::xxh3::xxh3_64;

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compatibility")
}

fn copy_fixture_dir(name: &str) -> tempfile::TempDir {
    let source = fixtures_root().join(name);
    assert!(
        source.exists(),
        "missing compatibility fixture: {}",
        source.display()
    );

    let temp = tempfile::tempdir().expect("create temp dir");
    copy_dir_recursive(&source, temp.path()).expect("copy compatibility fixture");
    temp
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;

    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dest_path = destination.join(entry.file_name());

        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &dest_path)?;
        } else {
            fs::copy(entry.path(), &dest_path)?;
        }
    }

    Ok(())
}

fn assert_compatibility_error(error: MidgeError) {
    match error {
        MidgeError::CompatibilityError(message) => {
            assert!(
                message.contains("unsupported on-disk format version"),
                "expected unsupported-format compatibility error, got: {message}"
            );
        }
        other => panic!("expected CompatibilityError, got: {other:?}"),
    }
}

fn logical_rows_digest(rows: &[(bytes::Bytes, bytes::Bytes)]) -> u64 {
    let mut canonical = Vec::new();
    for (key, value) in rows {
        canonical.extend_from_slice(
            &u32::try_from(key.len())
                .expect("fixture key length fits u32")
                .to_le_bytes(),
        );
        canonical.extend_from_slice(key);
        canonical.extend_from_slice(
            &u32::try_from(value.len())
                .expect("fixture value length fits u32")
                .to_le_bytes(),
        );
        canonical.extend_from_slice(value);
    }
    xxh3_64(&canonical)
}

#[test]
fn should_verify_populated_release_v3_v4_fixture_given_supported_format_when_reopening() {
    // Arrange
    let temp = copy_fixture_dir("v3_populated_v4_sst_db");

    // Act
    let report = Engine::verify_path(temp.path()).expect("verify release fixture");
    assert_eq!(report.health, EngineHealth::Healthy);
    assert_eq!(report.manifest_files_verified, 1);
    assert_eq!(report.sst_files_verified, 1);
    assert_eq!(report.bytes_verified, 437);
    assert_eq!(report.data_blocks_verified, 1);
    assert_eq!(report.wal_recovery_records_replayed, 0);
    assert_eq!(report.wal_recovery_bytes_replayed, 0);
    assert_eq!(report.intent_entries_loaded, 0);

    let mut engine = Engine::open(
        OpenOptions::local(temp.path())
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
    )
    .expect("open release fixture");
    let cf = engine
        .get_column_family("default")
        .expect("fixture default column family");
    let read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin fixture read");
    let rows = read
        .scan(&Query::new())
        .expect("scan fixture")
        .try_collect()
        .expect("collect fixture rows");
    let runtime = engine.get_runtime_metrics().expect("runtime metrics");

    // Assert
    assert_eq!(runtime.health, EngineHealth::Healthy);
    assert_eq!(rows.len(), 3);
    assert_eq!(logical_rows_digest(&rows), 14_948_492_731_235_234_299);
    drop(read);
    engine
        .shutdown(Duration::from_secs(5))
        .expect("shutdown fixture engine");
}

#[test]
fn should_reject_v2_empty_fixture_given_breaking_v4_sst_format() {
    // Arrange
    let temp = copy_fixture_dir("v2_empty_db");

    // Act
    let verify_error = Engine::verify_path(temp.path()).expect_err("V2 verify must fail");
    let Err(open_error) = Engine::open(
        OpenOptions::local(temp.path())
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
    ) else {
        panic!("V2 fixture must fail open");
    };

    // Assert
    assert_compatibility_error(verify_error);
    assert_compatibility_error(open_error);
}

#[test]
fn should_reject_future_v4_fixture_given_unsupported_version_when_reopening() {
    // Arrange
    let temp = copy_fixture_dir("future_v4");

    // Act
    let verify_error =
        Engine::verify_path(temp.path()).expect_err("future fixture should fail verify");
    assert_compatibility_error(verify_error);

    let Err(open_error) = Engine::open(
        OpenOptions::local(temp.path())
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
    ) else {
        panic!("future fixture should fail open");
    };
    // Assert
    assert_compatibility_error(open_error);
}
