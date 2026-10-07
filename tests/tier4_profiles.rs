//! Constructed profile-policy controls; these do not claim a ten-minute native run.
#![cfg(feature = "internal-testing")]

#[path = "../benches/stress_config.rs"]
mod stress_config;

use cntryl_midge::MidgeError;
use std::time::Duration;

#[test]
fn should_keep_original_short_window_when_smoke_profile_is_selected() {
    // Arrange: different scenarios retain their existing smoke windows.
    let windows = [5, 8, 12, 15, 16, 20];

    // Act: resolve each original smoke window using the shared policy.
    for seconds in windows {
        let original = Duration::from_secs(seconds);
        // Assert: smoke has no replacement fixed-duration window.
        assert_eq!(
            stress_config::tier4_profile_window(Some("smoke")).unwrap(),
            None
        );
        assert_eq!(
            stress_config::tier4_duration_for_profile(Some("smoke"), original).unwrap(),
            original
        );
    }
}

#[test]
fn should_keep_original_window_when_no_ui_profile_is_supplied() {
    // Arrange: existing direct benchmark invocations have no UI profile.
    let original = Duration::from_secs(5);

    // Act: resolve the same helper used by the YCSB A measured phase.
    let window = stress_config::tier4_duration_for_profile(None, original);

    // Assert: old short-duration CLI behavior is compatible.
    assert_eq!(window.unwrap(), original);
}

#[test]
fn should_use_ten_minutes_when_standard_profile_is_selected() {
    // Arrange: YCSB A's existing five-second window is only the smoke default.
    let original = Duration::from_secs(5);

    // Act: resolve the common policy used by both configuration and execution.
    let window = stress_config::tier4_duration_for_profile(Some("standard"), original);

    // Assert: each registered scenario receives one ten-minute measured window.
    assert_eq!(window.unwrap(), Duration::from_mins(10));
    assert_eq!(
        stress_config::tier4_profile_window(Some("standard")).unwrap(),
        Some(Duration::from_mins(10))
    );
}

#[test]
fn should_use_one_hour_when_full_profile_is_selected() {
    // Arrange: a scenario-specific long smoke plateau must not shorten full.
    let original = Duration::from_secs(20);

    // Act: resolve the same common policy for this existing scenario window.
    let window = stress_config::tier4_duration_for_profile(Some("full"), original);

    // Assert: full changes measured duration without changing dataset size.
    assert_eq!(window.unwrap(), Duration::from_hours(1));
    assert_eq!(
        stress_config::tier4_profile_window(Some("full")).unwrap(),
        Some(Duration::from_hours(1))
    );
}

#[test]
fn should_reject_unknown_profile_when_ui_value_is_invalid() {
    // Arrange: a typo must not silently use the short smoke window.
    let invalid = ["", "default", "lab", "standard\n", "FULL"];

    // Act: resolve each invalid profile using the shared policy.
    for profile in invalid {
        // Assert: only the explicit UI contract is accepted.
        assert!(matches!(
            stress_config::tier4_duration_for_profile(Some(profile), Duration::from_secs(5)),
            Err(MidgeError::InvalidArgument(_))
        ));
    }
}
