#![cfg(all(feature = "internal-testing", feature = "failpoints"))]
#[path = "../examples/recovery_attribution/fixture.rs"]
mod fixture;
use cntryl_midge::__internal::recovery::RecoveryProbeVariant;
use tracing_subscriber::prelude::*;

#[test]
fn should_recover_same_engine_backup_when_bounded_probe_variant_changes() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("fixture");
    let capture = fixture::Capture::default();
    tracing_subscriber::registry()
        .with(capture.clone())
        .try_init()
        .unwrap();
    let facts = fixture::create(&root, 512).unwrap();
    // Act
    let mut results = Vec::new();
    for variant in [
        RecoveryProbeVariant::Baseline,
        RecoveryProbeVariant::KeyIndex,
        RecoveryProbeVariant::SingleReader,
        RecoveryProbeVariant::TimersOff,
    ] {
        let target = directory.path().join(format!("trial-{variant:?}"));
        results.push(fixture::trial(&root, &target, variant, 16 * 1024, &capture).unwrap());
    }
    // Assert
    for result in results {
        assert_eq!(result["fixture_sha256"], facts["inventory"]["sha256"]);
        assert_eq!(result["verification"].as_array().unwrap().len(), 6);
        assert_eq!(result["shutdowns"], 2);
        let attribution = result["native_open_events"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|event| event.get("attribution"))
            .unwrap();
        assert_eq!(attribution["budget_final"], 0);
        assert!(
            attribution["budget_peak"].as_u64().unwrap()
                <= attribution["budget_limit"].as_u64().unwrap()
        );
        assert!(attribution["checkpoint_releases"].as_u64().unwrap() > 0);
        assert!(attribution["index_builds"].as_u64().unwrap() > 1);
        assert!(attribution["exact_hits"].as_u64().unwrap() > 0);
        assert!(
            attribution["peak_readers"].as_u64().unwrap()
                <= attribution["reader_limit"].as_u64().unwrap()
        );
    }
}
