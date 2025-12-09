//! Lifecycle state definitions and response types.

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Lifecycle state of the engine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LifecycleState {
    /// Engine is stopped
    Stopped,

    /// Engine is starting up, rehydrating from cloud
    Starting,

    /// Engine is ready to accept traffic
    Ready,

    /// Engine is draining for graceful shutdown
    Draining,

    /// Engine has sealed all data and is safe to terminate
    Sealed,
}

impl LifecycleState {
    /// Check if the engine can accept writes in this state
    pub fn can_accept_writes(&self) -> bool {
        matches!(self, LifecycleState::Ready)
    }

    /// Check if the engine is ready for traffic
    pub fn is_ready(&self) -> bool {
        matches!(self, LifecycleState::Ready)
    }

    /// Check if the engine is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self, LifecycleState::Stopped | LifecycleState::Sealed)
    }
}

impl std::fmt::Display for LifecycleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LifecycleState::Stopped => write!(f, "stopped"),
            LifecycleState::Starting => write!(f, "starting"),
            LifecycleState::Ready => write!(f, "ready"),
            LifecycleState::Draining => write!(f, "draining"),
            LifecycleState::Sealed => write!(f, "sealed"),
        }
    }
}

/// Readiness status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessStatus {
    /// Is the engine ready to accept traffic
    pub ready: bool,

    /// Is rehydration complete
    pub rehydration_complete: bool,

    /// Last applied sequence number
    pub last_applied_seq: u64,

    /// Last checkpoint sequence number
    pub checkpoint_seq: u64,

    /// Current lifecycle state
    pub state: String,

    /// Optional reason if not ready
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Syncpoint status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncpointStatus {
    /// Current sequence number (latest applied)
    pub current_seq: u64,

    /// Checkpoint sequence number (last durable point)
    pub checkpoint_seq: u64,

    /// Current state
    pub state: String,

    /// Timestamp of last update
    pub last_update: SystemTime,
}

/// Result of drain operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainResult {
    /// Status: "sealed", "failed", "in_progress"
    pub status: String,

    /// Last committed sequence number
    pub last_committed_seq: u64,

    /// Duration of drain operation in milliseconds
    pub duration_ms: u64,

    /// Optional error message if failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of state validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Is the state valid and consistent
    pub valid: bool,

    /// Current local sequence
    pub current_seq: u64,

    /// Cloud checkpoint sequence
    pub cloud_seq: u64,

    /// Missing WAL segments
    pub missing_segments: Vec<String>,

    /// Discrepancies found
    pub discrepancies: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_allow_writes_given_ready_state() {
        // Arrange
        let state = LifecycleState::Ready;

        // Act
        let can_write = state.can_accept_writes();

        // Assert
        assert!(can_write);
    }

    #[test]
    fn should_reject_writes_given_starting_state() {
        // Arrange
        let state = LifecycleState::Starting;

        // Act
        let can_write = state.can_accept_writes();

        // Assert
        assert!(!can_write);
    }

    #[test]
    fn should_reject_writes_given_draining_state() {
        // Arrange
        let state = LifecycleState::Draining;

        // Act
        let can_write = state.can_accept_writes();

        // Assert
        assert!(!can_write);
    }

    #[test]
    fn should_return_ready_given_ready_state() {
        // Arrange
        let state = LifecycleState::Ready;

        // Act
        let ready = state.is_ready();

        // Assert
        assert!(ready);
    }

    #[test]
    fn should_return_terminal_given_sealed_state() {
        // Arrange
        let state = LifecycleState::Sealed;

        // Act
        let terminal = state.is_terminal();

        // Assert
        assert!(terminal);
    }

    #[test]
    fn should_return_terminal_given_stopped_state() {
        // Arrange
        let state = LifecycleState::Stopped;

        // Act
        let terminal = state.is_terminal();

        // Assert
        assert!(terminal);
    }

    #[test]
    fn should_display_correct_string_given_each_state() {
        // Arrange
        // Act
        // Assert
        assert_eq!(LifecycleState::Stopped.to_string(), "stopped");
        assert_eq!(LifecycleState::Starting.to_string(), "starting");
        assert_eq!(LifecycleState::Ready.to_string(), "ready");
        assert_eq!(LifecycleState::Draining.to_string(), "draining");
        assert_eq!(LifecycleState::Sealed.to_string(), "sealed");
    }
}
