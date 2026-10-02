//! Recovery fuzz helpers must finish lease-owned teardown between attempts.
#[path = "../fuzz/fuzz_targets/common.rs"]
mod harness;

use cntryl_midge::{Engine, OpenOptions};
use std::time::Duration;

#[test]
fn should_release_seed_lease_before_returning_to_fuzz_input_mutation() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();

    // Act
    harness::seed_db(directory.path());
    let mut successor = Engine::open(OpenOptions::local(directory.path()).build().unwrap())
        .expect("seed helper must complete teardown before returning");

    // Assert
    successor.shutdown(Duration::from_secs(10)).unwrap();
}

#[test]
fn should_finish_recovery_attempts_before_returning_to_the_next_fuzz_iteration() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(OpenOptions::local(directory.path()).build().unwrap()).unwrap();
    engine.shutdown(Duration::from_secs(10)).unwrap();
    drop(engine);
    harness::write_relative(directory.path(), "probe", b"bounded fixture");

    // Act
    harness::exercise_open_and_verify(directory.path());
    let mut successor = Engine::open(OpenOptions::local(directory.path()).build().unwrap())
        .expect("recovery helpers must complete teardown before returning");

    // Assert
    successor.shutdown(Duration::from_secs(10)).unwrap();
}
