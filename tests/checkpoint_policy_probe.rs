//! Real framed journal/snapshot exploration with no engine cadence changes.
#![cfg(feature = "internal-testing")]
use cntryl_midge::__internal::checkpoint::run_checkpoint_policy_probe;

#[test]
fn should_reconstruct_each_deferred_edit_when_fixed_cardinality_probe_batches_snapshots() {
    // Arrange
    let baseline = tempfile::tempdir().unwrap();
    let batched = tempfile::tempdir().unwrap();
    // Act
    let baseline = run_checkpoint_policy_probe(baseline.path(), 16, 64, 1, 16_384).unwrap();
    let batched = run_checkpoint_policy_probe(batched.path(), 16, 64, 16, 16_384).unwrap();
    // Assert: real reload was checked after every edit, including unsnapshotted ones.
    assert_eq!(baseline["verified_replays"], 64);
    assert_eq!(batched["verified_replays"], 64);
    assert_eq!(baseline["checkpoints"], 64);
    assert_eq!(batched["checkpoints"], 4);
    assert!(
        batched["snapshot_issued_bytes"].as_u64().unwrap() * 10
            < baseline["snapshot_issued_bytes"].as_u64().unwrap()
    );
    assert_eq!(batched["production_policy_accepted"], false);
}

#[test]
fn should_force_snapshot_at_journal_trigger_when_edit_interval_has_not_elapsed() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    // Act: one framed edit exceeds this deliberately tiny post-append trigger.
    let report = run_checkpoint_policy_probe(directory.path(), 4, 16, 64, 1).unwrap();
    // Assert
    assert_eq!(report["checkpoints"], 16);
    assert_eq!(report["verified_replays"], 16);
    assert!(report["journal_peak_bytes"].as_u64().unwrap() > 1);
}
