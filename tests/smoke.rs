//! Smoke tests for Midge.
//!
//! Purpose:
//! - Validate core end-to-end invariants
//! - Exercise real engine wiring with minimal data
//! - Catch “green unit tests, broken database” failures
//!
//! Rules:
//! - No sleeps
//! - No large loops
//! - No timing assumptions
//! - No helpers beyond basic setup

#[test]
fn write_read_in_memory() {
    panic!("stub: write -> get in pure in-memory mode");
}

#[test]
fn write_read_after_flush() {
    panic!("stub: write -> flush -> get preserves visibility");
}

#[test]
fn delete_hides_value() {
    panic!("stub: put -> delete -> get returns not found");
}

#[test]
fn tombstone_survives_flush() {
    panic!("stub: delete -> flush -> get remains not found");
}

#[test]
fn restart_persists_data() {
    panic!("stub: write -> restart engine -> get returns value");
}

#[test]
fn restart_persists_tombstone() {
    panic!("stub: delete -> restart engine -> get remains not found");
}

#[test]
fn snapshot_isolation_holds() {
    panic!("stub: snapshot sees stable view across writes");
}

#[test]
fn compaction_preserves_latest_version() {
    panic!("stub: compaction does not resurrect older versions");
}

#[test]
fn range_scan_respects_visibility_rules() {
    panic!("stub: range scan respects deletes, snapshots, ordering");
}

#[test]
fn sequence_numbers_are_monotonic() {
    panic!("stub: seqnos always increase across operations");
}

#[test]
fn crash_safe_recovery_does_not_corrupt_state() {
    panic!("stub: simulated unclean shutdown recovers safely");
}
