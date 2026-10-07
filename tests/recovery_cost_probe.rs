#![cfg(feature = "internal-testing")]

use cntryl_midge::__internal::recovery::run_recovery_cost_probe;

#[test]
fn should_preserve_exact_fixture_when_probe_order_changes_reader_locality() {
    // Arrange
    let grouped = tempfile::tempdir().unwrap();
    let interleaved = tempfile::tempdir().unwrap();
    // Act
    let grouped =
        run_recovery_cost_probe(grouped.path(), 8, 32, 2 * 1_024 * 1_024, false, 0).unwrap();
    let interleaved =
        run_recovery_cost_probe(interleaved.path(), 8, 32, 2 * 1_024 * 1_024, true, 0).unwrap();
    // Assert: exact same immutable bytes/probes; interleaving exceeds four-reader locality.
    assert_eq!(grouped["fixture_xxh3_128"], interleaved["fixture_xxh3_128"]);
    assert_eq!(grouped["probes"], 256);
    assert_eq!(interleaved["probes"], 256);
    assert_eq!(grouped["reader_opens"], 8);
    assert!(
        interleaved["reader_opens"].as_u64().unwrap() > grouped["reader_opens"].as_u64().unwrap()
    );
    assert_eq!(grouped["charged_final_bytes"], 0);
    assert_eq!(interleaved["charged_final_bytes"], 0);
    assert_eq!(grouped["remote_range_failures"], 0);
    assert!(grouped["remote_range_calls"].as_u64().unwrap() > 0);
    assert_eq!(
        grouped["remote_range_started"],
        grouped["remote_range_calls"]
    );
    assert!(
        grouped["remote_range_bytes"].as_u64().unwrap()
            >= grouped["verified_sst_bytes"].as_u64().unwrap()
    );
    assert_eq!(interleaved["all_exact_proofs_passed"], true);
}

#[test]
fn should_release_identity_owned_proof_when_probe_models_checkpoint_discard() {
    // Arrange
    let retained = tempfile::tempdir().unwrap();
    let discarded = tempfile::tempdir().unwrap();
    // Act
    let retained = run_recovery_cost_probe(retained.path(), 2, 32, 128 * 1_024, false, 0).unwrap();
    let discarded =
        run_recovery_cost_probe(discarded.path(), 2, 32, 128 * 1_024, false, 8).unwrap();
    // Assert: this models proof discard only, not a durable Engine checkpoint.
    assert_eq!(retained["fixture_xxh3_128"], discarded["fixture_xxh3_128"]);
    assert_eq!(discarded["explicit_proof_releases"], 8);
    assert_eq!(discarded["charged_final_bytes"], 0);
    assert!(
        discarded["verified_sst_bytes"].as_u64().unwrap()
            > retained["verified_sst_bytes"].as_u64().unwrap()
    );
    assert_eq!(discarded["production_optimization_accepted"], false);
}
