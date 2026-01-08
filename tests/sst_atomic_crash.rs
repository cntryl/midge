//! Integration tests for SST atomic publish and crash/power-loss scenarios
//!
//! NOTE: These tests access private runtime and sst modules.
//! They should be moved to unit tests within those modules.
//! Disabled during API migration.

// use cntryl_midge::runtime::{FileMeta, ManifestActor, RuntimeState};
use std::fs;

// Simulate a leftover .tmp file (crash before rename) and ensure manifest.add_sst rejects it
#[test]
#[ignore = "requires access to private runtime module - move to unit tests"]
fn should_reject_manifest_add_when_only_tmp_file_exists_integration() {
    // Test disabled - move to src/runtime or src/metadata unit tests
}

// Simulate an SST file present on disk (as if writer succeeded but manifest not yet updated)
// Verify that add_sst accepts it and that the file is readable
#[test]
#[ignore = "requires access to private runtime module - move to unit tests"]
fn should_accept_sst_present_on_disk_and_allow_manual_manifest_add(
) -> cntryl_midge::common::MidgeResult<()> {
    // Test disabled - move to src/runtime or src/metadata unit tests
    Ok(())
}
