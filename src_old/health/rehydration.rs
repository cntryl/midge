//! Rehydration progress tracking.

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

use crate::common::timestamp;

/// Tracks rehydration progress during startup
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RehydrationProgress {
    /// Total WAL segments discovered
    pub total_wal_segments: usize,

    /// WAL segments successfully replayed
    pub replayed_wal_segments: usize,

    /// Total SST files discovered
    pub total_ssts: usize,

    /// SST metadata loaded
    pub loaded_ssts: usize,

    /// Current sequence number being replayed
    pub current_seq: u64,

    /// Target sequence number (from manifest)
    pub target_seq: u64,

    /// When rehydration started
    pub started_at: SystemTime,

    /// When rehydration completed (if done)
    pub completed_at: Option<SystemTime>,
}

impl RehydrationProgress {
    /// Create new rehydration progress tracker
    pub fn new() -> Self {
        Self {
            total_wal_segments: 0,
            replayed_wal_segments: 0,
            total_ssts: 0,
            loaded_ssts: 0,
            current_seq: 0,
            target_seq: 0,
            started_at: timestamp::now(),
            completed_at: None,
        }
    }

    /// Check if rehydration is complete
    pub fn is_complete(&self) -> bool {
        self.completed_at.is_some()
    }

    /// Calculate progress percentage (0.0 to 100.0)
    pub fn progress_pct(&self) -> f64 {
        if self.total_wal_segments == 0 && self.total_ssts == 0 {
            return 100.0; // No work to do
        }

        let wal_progress = if self.total_wal_segments > 0 {
            (self.replayed_wal_segments as f64 / self.total_wal_segments as f64) * 50.0
        } else {
            50.0
        };

        let sst_progress = if self.total_ssts > 0 {
            (self.loaded_ssts as f64 / self.total_ssts as f64) * 50.0
        } else {
            50.0
        };

        (wal_progress + sst_progress).min(100.0)
    }

    /// Get elapsed time since start
    pub fn elapsed(&self) -> Duration {
        timestamp::now()
            .duration_since(self.started_at)
            .unwrap_or(Duration::from_secs(0))
    }

    /// Get total duration if complete
    pub fn total_duration(&self) -> Option<Duration> {
        self.completed_at
            .and_then(|completed| completed.duration_since(self.started_at).ok())
    }

    /// Mark rehydration as complete
    pub fn mark_complete(&mut self) {
        self.completed_at = Some(timestamp::now());
    }
}

impl Default for RehydrationProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Rehydration status response for API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RehydrationStatus {
    /// Is rehydration complete
    pub complete: bool,

    /// Progress percentage (0.0 to 100.0)
    pub progress_pct: f64,

    /// Total WAL segments to replay
    pub total_wal_segments: usize,

    /// WAL segments replayed
    pub replayed_wal_segments: usize,

    /// Total SSTs to load
    pub total_ssts: usize,

    /// SSTs loaded
    pub loaded_ssts: usize,

    /// Current sequence being processed
    pub current_seq: u64,

    /// Target sequence to reach
    pub target_seq: u64,

    /// Elapsed time in milliseconds
    pub elapsed_ms: u64,

    /// Total duration in milliseconds (if complete)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_duration_ms: Option<u64>,
}

impl From<&RehydrationProgress> for RehydrationStatus {
    fn from(progress: &RehydrationProgress) -> Self {
        Self {
            complete: progress.is_complete(),
            progress_pct: progress.progress_pct(),
            total_wal_segments: progress.total_wal_segments,
            replayed_wal_segments: progress.replayed_wal_segments,
            total_ssts: progress.total_ssts,
            loaded_ssts: progress.loaded_ssts,
            current_seq: progress.current_seq,
            target_seq: progress.target_seq,
            elapsed_ms: progress.elapsed().as_millis() as u64,
            total_duration_ms: progress.total_duration().map(|d| d.as_millis() as u64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn should_initialize_with_zero_progress() {
        // Arrange
        // Act
        let progress = RehydrationProgress::new();

        // Assert
        assert_eq!(progress.total_wal_segments, 0);
        assert_eq!(progress.replayed_wal_segments, 0);
        assert_eq!(progress.total_ssts, 0);
        assert_eq!(progress.loaded_ssts, 0);
        assert!(!progress.is_complete());
    }

    #[test]
    fn should_return_hundred_percent_given_no_work() {
        // Arrange
        let progress = RehydrationProgress::new();

        // Act
        let pct = progress.progress_pct();

        // Assert
        assert_eq!(pct, 100.0);
    }

    #[test]
    fn should_calculate_progress_given_partial_completion() {
        // Arrange
        let mut progress = RehydrationProgress::new();
        progress.total_wal_segments = 10;
        progress.replayed_wal_segments = 5;
        progress.total_ssts = 20;
        progress.loaded_ssts = 10;

        // Act
        let pct = progress.progress_pct();

        // Assert
        assert_eq!(pct, 50.0); // (5/10 * 50) + (10/20 * 50) = 25 + 25 = 50
    }

    #[test]
    fn should_mark_complete_given_mark_complete_called() {
        // Arrange
        let mut progress = RehydrationProgress::new();
        assert!(!progress.is_complete());

        // Act
        progress.mark_complete();

        // Assert
        assert!(progress.is_complete());
        assert!(progress.completed_at.is_some());
    }

    #[test]
    fn should_track_elapsed_time() {
        // Arrange
        let progress = RehydrationProgress::new();
        thread::sleep(Duration::from_millis(10));

        // Act
        let elapsed = progress.elapsed();

        // Assert
        assert!(elapsed.as_millis() >= 10);
    }

    #[test]
    fn should_convert_to_status_response() {
        // Arrange
        let mut progress = RehydrationProgress::new();
        progress.total_wal_segments = 10;
        progress.replayed_wal_segments = 5;
        progress.total_ssts = 20;
        progress.loaded_ssts = 15;
        progress.current_seq = 1000;
        progress.target_seq = 2000;

        // Act
        let status = RehydrationStatus::from(&progress);

        // Assert
        assert!(!status.complete);
        assert_eq!(status.total_wal_segments, 10);
        assert_eq!(status.replayed_wal_segments, 5);
        assert_eq!(status.current_seq, 1000);
        assert!(status.progress_pct > 0.0);
    }
}
