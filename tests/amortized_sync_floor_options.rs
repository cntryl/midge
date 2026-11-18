use cntryl_midge::{AmortizedSyncFloor, MidgeError, OpenOptions};
use std::time::Duration;

fn floor(bytes: u64, delay: Duration) -> AmortizedSyncFloor {
    AmortizedSyncFloor {
        max_unrecorded_bytes: bytes,
        max_unrecorded_delay: delay,
    }
}

#[test]
fn should_reject_amortized_sync_floor_when_bounds_are_out_of_range() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let invalid = [
        floor(0, Duration::from_millis(100)),
        floor(64 * 1024 * 1024 + 1, Duration::from_millis(100)),
        floor(1024, Duration::from_micros(999)),
        floor(1024, Duration::ZERO),
        floor(1024, Duration::from_secs(10) + Duration::from_millis(1)),
    ];

    for config in invalid {
        // Act
        let result = OpenOptions::local(dir.path())
            .amortized_sync_floor(config)
            .build();

        // Assert
        assert!(
            matches!(result, Err(MidgeError::InvalidArgument(_))),
            "{config:?} must be rejected, got {result:?}"
        );
    }
}

#[test]
fn should_accept_amortized_sync_floor_when_bounds_are_inclusive_edges() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let edges = [
        floor(1, Duration::from_millis(1)),
        floor(64 * 1024 * 1024, Duration::from_secs(10)),
        AmortizedSyncFloor::default(),
    ];

    for config in edges {
        // Act
        let options = OpenOptions::local(dir.path())
            .amortized_sync_floor(config)
            .build()
            .unwrap();

        // Assert
        assert_eq!(options.amortized_sync_floor(), Some(config));
    }
}

#[test]
fn should_default_amortized_sync_floor_to_documented_bounds_when_unconfigured() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();

    // Act
    let options = OpenOptions::local(dir.path()).build().unwrap();
    let defaults = AmortizedSyncFloor::default();

    // Assert
    assert_eq!(options.amortized_sync_floor(), None);
    assert_eq!(defaults.max_unrecorded_bytes, 256 * 1024);
    assert_eq!(defaults.max_unrecorded_delay, Duration::from_millis(20));
}

#[test]
fn should_reject_amortized_sync_floor_when_storage_is_cloud() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();

    // Act
    let result = OpenOptions::cloud_simulated(dir.path(), "bucket", "prefix")
        .amortized_sync_floor(AmortizedSyncFloor::default())
        .build();

    // Assert
    assert!(
        matches!(result, Err(MidgeError::InvalidArgument(_))),
        "cloud storage must reject the option, got {result:?}"
    );
}

#[test]
fn should_accept_amortized_sync_floor_when_storage_is_in_memory() {
    // Arrange
    let builder = OpenOptions::in_memory();

    // Act
    let options = builder
        .amortized_sync_floor(AmortizedSyncFloor::default())
        .build()
        .unwrap();

    // Assert
    assert_eq!(
        options.amortized_sync_floor(),
        Some(AmortizedSyncFloor::default())
    );
}
