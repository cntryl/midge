//! Integration tests for SST atomic publish and crash/power-loss scenarios

use cntryl_midge::testkit::*;
use cntryl_midge::runtime::{FileMeta, ManifestActor, RuntimeState};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

// Simulate a leftover .tmp file (crash before rename) and ensure manifest.add_sst rejects it
#[test]
fn should_reject_manifest_add_when_only_tmp_file_exists_integration() {
    // Arrange
    let tmpdir = tempfile::tempdir().expect("create tmpdir");
    let sst_name = "sst_crash_tmp.sst".to_string();
    let mut state = RuntimeState::new(tmpdir.path().to_path_buf(), false);
    let tmp_name = format!("{}.tmp", sst_name);
    let tmp_path = state.sst_dir.join(&tmp_name);

    // Write a partial tmp file (simulate crash before rename)
    fs::write(&tmp_path, b"partial-sst").expect("write tmp sst");

    let file_meta = FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: 0,
        cf_id: 0,
        smallest_key: None,
        largest_key: None,
        smallest_seq: None,
        largest_seq: None,
    };

    let mut state = RuntimeState::new(tmpdir.path().to_path_buf(), false);
    let mut actor = ManifestActor::new();

    // Act
    let result = actor.add_sst(&mut state, file_meta);

    // Assert
    assert!(result.is_err(), "expected manifest.add_sst to fail when only tmp file exists");
}

// Simulate an SST file present on disk (as if writer succeeded but manifest not yet updated)
// Verify that add_sst accepts it and that the file is readable
#[test]
fn should_accept_sst_present_on_disk_and_allow_manual_manifest_add() -> cntryl_midge::common::MidgeResult<()> {
    // Arrange
    let tmpdir = tempfile::tempdir().expect("create tmpdir");
    let sst_name = "sst_crash_present.sst".to_string();
    let mut state = RuntimeState::new(tmpdir.path().to_path_buf(), false);
    let sst_path = state.sst_dir.join(&sst_name);

    // Write a minimal valid SST footer so the reader validation accepts it
    use cntryl_midge::sst::types::SST_FOOTER_MAGIC;
    let mut f = fs::File::create(&sst_path)?;
    let mut buf = vec![0u8; 48];
    buf[40..48].copy_from_slice(&SST_FOOTER_MAGIC.to_le_bytes());
    use std::io::Write;
    f.write_all(&buf)?;
    f.sync_all()?;

    // Build file meta
    let file_meta = FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: fs::metadata(&sst_path)?.len(),
        cf_id: 0,
        smallest_key: None,
        largest_key: None,
        smallest_seq: None,
        largest_seq: None,
    };

    let mut state = RuntimeState::new(tmpdir.path().to_path_buf(), false);
    let mut actor = ManifestActor::new();

    // Sanity
    assert!(state.sst_dir.exists());
    assert!(sst_path.exists());

    // Act
    actor.add_sst(&mut state, file_meta.clone())?;

    // Assert: manifest now references the sst by name
    let found = state.manifest.files.iter().any(|f| f.name == sst_name);
    assert!(found, "manifest should reference the manually added sst file");

    Ok(())
}
