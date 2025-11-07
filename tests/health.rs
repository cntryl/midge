//! Integration tests for health management system

use midge::health::{HealthConfig, HealthManager, LifecycleState};
use midge::manifest::Manifest;
use parking_lot::RwLock;
use std::sync::{Arc, Weak};

// Mock engine for testing
struct MockEngine {
    current_seq: u64,
    checkpoint_seq: u64,
}

impl midge::health::manager::EngineHealth for MockEngine {
    fn current_sequence(&self) -> u64 {
        self.current_seq
    }

    fn flush_memtable(&self) -> midge::error::MidgeResult<()> {
        Ok(())
    }

    fn last_checkpoint_seq(&self) -> u64 {
        self.checkpoint_seq
    }
}

// Helper to create test manifest
fn create_test_manifest() -> Arc<RwLock<Manifest>> {
    Arc::new(RwLock::new(Manifest::default()))
}

#[test]
fn should_start_in_stopped_state_given_new_manager() {
    // Arrange
    let manifest = create_test_manifest();
    let engine = Arc::new(MockEngine {
        current_seq: 0,
        checkpoint_seq: 0,
    });
    let engine_weak: Weak<dyn midge::health::manager::EngineHealth> =
        Arc::downgrade(&engine) as Weak<dyn midge::health::manager::EngineHealth>;
    let config = HealthConfig::default();

    // Act
    let manager = HealthManager::new(engine_weak, manifest, config);

    // Assert
    assert_eq!(manager.get_state(), LifecycleState::Stopped);
    assert!(manager.is_alive());
}

#[test]
fn should_transition_to_starting_given_valid_state() {
    // Arrange
    let manifest = create_test_manifest();
    let engine = Arc::new(MockEngine {
        current_seq: 100,
        checkpoint_seq: 90,
    });
    let engine_weak: Weak<dyn midge::health::manager::EngineHealth> =
        Arc::downgrade(&engine) as Weak<dyn midge::health::manager::EngineHealth>;
    let config = HealthConfig::default();
    let manager = HealthManager::new(engine_weak, manifest, config);

    // Act
    let result = manager.set_state(LifecycleState::Starting);

    // Assert
    assert!(result.is_ok());
    assert_eq!(manager.get_state(), LifecycleState::Starting);
}

#[test]
fn should_reject_invalid_transition_given_stopped_to_ready() {
    // Arrange
    let manifest = create_test_manifest();
    let engine = Arc::new(MockEngine {
        current_seq: 100,
        checkpoint_seq: 90,
    });
    let engine_weak: Weak<dyn midge::health::manager::EngineHealth> =
        Arc::downgrade(&engine) as Weak<dyn midge::health::manager::EngineHealth>;
    let config = HealthConfig::default();
    let manager = HealthManager::new(engine_weak, manifest, config);

    // Act
    let result = manager.set_state(LifecycleState::Ready);

    // Assert
    assert!(result.is_err());
    assert_eq!(manager.get_state(), LifecycleState::Stopped);
}

#[test]
fn should_return_not_ready_given_starting_state() {
    // Arrange
    let manifest = create_test_manifest();
    let engine = Arc::new(MockEngine {
        current_seq: 100,
        checkpoint_seq: 90,
    });
    let engine_weak: Weak<dyn midge::health::manager::EngineHealth> =
        Arc::downgrade(&engine) as Weak<dyn midge::health::manager::EngineHealth>;
    let config = HealthConfig::default();
    let manager = HealthManager::new(engine_weak, manifest, config);
    manager.set_state(LifecycleState::Starting).unwrap();

    // Act
    let status = manager.is_ready();

    // Assert
    assert!(!status.ready);
    assert!(!status.rehydration_complete);
    assert_eq!(status.state, "starting");
    assert!(status.reason.is_some());
}

#[test]
fn should_return_ready_given_complete_rehydration() {
    // Arrange
    let manifest = create_test_manifest();
    let engine = Arc::new(MockEngine {
        current_seq: 100,
        checkpoint_seq: 90,
    });
    let engine_weak: Weak<dyn midge::health::manager::EngineHealth> =
        Arc::downgrade(&engine) as Weak<dyn midge::health::manager::EngineHealth>;
    let config = HealthConfig::default();
    let manager = HealthManager::new(engine_weak, manifest, config);

    manager.set_state(LifecycleState::Starting).unwrap();
    manager.start_rehydration();
    manager.complete_rehydration();
    manager.set_state(LifecycleState::Ready).unwrap();

    // Act
    let status = manager.is_ready();

    // Assert
    assert!(status.ready);
    assert!(status.rehydration_complete);
    assert_eq!(status.state, "ready");
    assert_eq!(status.last_applied_seq, 100);
    assert!(status.reason.is_none());
}

#[test]
fn should_track_rehydration_progress_given_updates() {
    // Arrange
    let manifest = create_test_manifest();
    let engine = Arc::new(MockEngine {
        current_seq: 100,
        checkpoint_seq: 90,
    });
    let engine_weak: Weak<dyn midge::health::manager::EngineHealth> =
        Arc::downgrade(&engine) as Weak<dyn midge::health::manager::EngineHealth>;
    let config = HealthConfig::default();
    let manager = HealthManager::new(engine_weak, manifest, config);

    manager.start_rehydration();
    manager.set_total_wal_segments(10);
    manager.set_total_ssts(20);

    // Act
    manager.update_wal_progress(5);
    manager.update_sst_progress(10);
    let status = manager.get_rehydration_status();

    // Assert
    assert!(!status.complete);
    assert_eq!(status.total_wal_segments, 10);
    assert_eq!(status.replayed_wal_segments, 5);
    assert_eq!(status.total_ssts, 20);
    assert_eq!(status.loaded_ssts, 10);
    assert_eq!(status.progress_pct, 50.0);
}

#[test]
fn should_return_syncpoint_given_engine_state() {
    // Arrange
    let manifest = create_test_manifest();
    let engine = Arc::new(MockEngine {
        current_seq: 1000,
        checkpoint_seq: 900,
    });
    let engine_weak: Weak<dyn midge::health::manager::EngineHealth> =
        Arc::downgrade(&engine) as Weak<dyn midge::health::manager::EngineHealth>;
    let config = HealthConfig::default();
    let manager = HealthManager::new(engine_weak, manifest, config);

    // Act
    let syncpoint = manager.get_syncpoint();

    // Assert
    assert_eq!(syncpoint.current_seq, 1000);
    assert_eq!(syncpoint.checkpoint_seq, 900);
    assert_eq!(syncpoint.state, "stopped");
}

#[test]
fn should_seal_engine_given_drain_called() {
    // Arrange
    let manifest = create_test_manifest();
    let engine = Arc::new(MockEngine {
        current_seq: 1000,
        checkpoint_seq: 900,
    });
    let engine_weak: Weak<dyn midge::health::manager::EngineHealth> =
        Arc::downgrade(&engine) as Weak<dyn midge::health::manager::EngineHealth>;
    let config = HealthConfig::default();
    let manager = HealthManager::new(engine_weak, manifest, config);

    manager.set_state(LifecycleState::Starting).unwrap();
    manager.set_state(LifecycleState::Ready).unwrap();

    // Act
    let result = manager.drain();

    // Assert
    assert_eq!(result.status, "sealed");
    assert_eq!(result.last_committed_seq, 1000);
    assert!(result.error.is_none());
    assert_eq!(manager.get_state(), LifecycleState::Sealed);
}

#[test]
fn should_validate_state_against_cloud_checkpoint() {
    // Arrange
    let manifest = create_test_manifest();
    let engine = Arc::new(MockEngine {
        current_seq: 1000,
        checkpoint_seq: 900,
    });
    let engine_weak: Weak<dyn midge::health::manager::EngineHealth> =
        Arc::downgrade(&engine) as Weak<dyn midge::health::manager::EngineHealth>;
    let config = HealthConfig::default();
    let manager = HealthManager::new(engine_weak, manifest, config);

    // Act
    let validation = manager.validate_state();

    // Assert
    assert!(validation.valid);
    assert_eq!(validation.current_seq, 1000);
    assert_eq!(validation.cloud_seq, 0); // No checkpoint yet
    assert!(validation.missing_segments.is_empty());
}
