//! Test hooks for fault injection and instrumentation.
//!
//! This module provides hooks that allow tests to inject failures, intercept
//! operations, and verify internal behavior. These hooks are only available
//! when the `test-hooks` feature is enabled (automatically enabled in test builds).
//!
//! # Example
//!
//! ```rust,ignore
//! use cntryl_midge::test_hooks::{TestHooks, FsyncBehavior};
//!
//! let hooks = TestHooks::new()
//!     .with_fsync_behavior(FsyncBehavior::Skip); // Simulate crash before fsync
//!
//! let opts = MidgeOptions {
//!     test_hooks: Some(hooks),
//!     ..Default::default()
//! };
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Behavior for fsync operations during tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncBehavior {
    /// Normal fsync behavior (default)
    Normal,
    /// Skip fsync (simulate crash before sync completes)
    Skip,
    /// Record fsync calls but skip actual sync
    RecordOnly,
}

/// Behavior for WAL operations during tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalBehavior {
    /// Normal WAL behavior (default)
    Normal,
    /// Truncate WAL after write (simulate torn write)
    TruncateAfterWrite,
}

/// Behavior for manifest operations during tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestBehavior {
    /// Normal manifest behavior (default)
    Normal,
    /// Fail manifest save operations
    FailSave,
    /// Corrupt manifest after save
    CorruptAfterSave,
}

/// Behavior for compaction operations during tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionBehavior {
    /// Normal compaction behavior (default)
    Normal,
    /// Fail compaction midway
    FailMidway,
    /// Crash before output fsync
    CrashBeforeFsync,
}

/// Test hooks for fault injection and instrumentation.
///
/// This allows tests to intercept operations, inject failures, and verify
/// internal behavior without modifying production code paths.
#[derive(Clone, Debug)]
pub struct TestHooks {
    /// Fsync behavior control
    fsync_behavior: Arc<parking_lot::RwLock<FsyncBehavior>>,
    /// WAL behavior control
    wal_behavior: Arc<parking_lot::RwLock<WalBehavior>>,
    /// Manifest behavior control
    manifest_behavior: Arc<parking_lot::RwLock<ManifestBehavior>>,
    /// Compaction behavior control
    compaction_behavior: Arc<parking_lot::RwLock<CompactionBehavior>>,

    // Instrumentation counters
    /// Number of fsync calls made
    fsync_count: Arc<AtomicU64>,
    /// Number of WAL appends
    wal_append_count: Arc<AtomicU64>,
    /// Number of manifest updates
    manifest_update_count: Arc<AtomicU64>,
    /// Number of compactions started
    compaction_start_count: Arc<AtomicU64>,
    /// Number of compactions completed
    compaction_complete_count: Arc<AtomicU64>,
    /// Number of compactions failed
    compaction_failed_count: Arc<AtomicU64>,

    // Verification flags
    /// Whether WAL was truncated after manifest update
    wal_truncated_after_manifest: Arc<AtomicBool>,
    /// Whether manifest was fsynced before WAL truncation
    manifest_fsynced_before_wal_truncate: Arc<AtomicBool>,
}

impl Default for TestHooks {
    fn default() -> Self {
        Self::new()
    }
}

impl TestHooks {
    /// Create new test hooks with default (normal) behavior.
    pub fn new() -> Self {
        Self {
            fsync_behavior: Arc::new(parking_lot::RwLock::new(FsyncBehavior::Normal)),
            wal_behavior: Arc::new(parking_lot::RwLock::new(WalBehavior::Normal)),
            manifest_behavior: Arc::new(parking_lot::RwLock::new(ManifestBehavior::Normal)),
            compaction_behavior: Arc::new(parking_lot::RwLock::new(CompactionBehavior::Normal)),
            fsync_count: Arc::new(AtomicU64::new(0)),
            wal_append_count: Arc::new(AtomicU64::new(0)),
            manifest_update_count: Arc::new(AtomicU64::new(0)),
            compaction_start_count: Arc::new(AtomicU64::new(0)),
            compaction_complete_count: Arc::new(AtomicU64::new(0)),
            compaction_failed_count: Arc::new(AtomicU64::new(0)),
            wal_truncated_after_manifest: Arc::new(AtomicBool::new(false)),
            manifest_fsynced_before_wal_truncate: Arc::new(AtomicBool::new(false)),
        }
    }

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    /// Set fsync behavior for testing.
    pub fn with_fsync_behavior(self, behavior: FsyncBehavior) -> Self {
        *self.fsync_behavior.write() = behavior;
        self
    }

    /// Set WAL behavior for testing.
    pub fn with_wal_behavior(self, behavior: WalBehavior) -> Self {
        *self.wal_behavior.write() = behavior;
        self
    }

    /// Set manifest behavior for testing.
    pub fn with_manifest_behavior(self, behavior: ManifestBehavior) -> Self {
        *self.manifest_behavior.write() = behavior;
        self
    }

    /// Set compaction behavior for testing.
    pub fn with_compaction_behavior(self, behavior: CompactionBehavior) -> Self {
        *self.compaction_behavior.write() = behavior;
        self
    }

    // -------------------------------------------------------------------------
    // Operation Hooks (called by production code)
    // -------------------------------------------------------------------------

    /// Hook called before fsync. Returns whether to actually perform fsync.
    pub fn before_fsync(&self) -> bool {
        self.fsync_count.fetch_add(1, Ordering::SeqCst);
        match *self.fsync_behavior.read() {
            FsyncBehavior::Normal => true,
            FsyncBehavior::Skip | FsyncBehavior::RecordOnly => false,
        }
    }

    /// Hook called before WAL append.
    pub fn before_wal_append(&self) {
        self.wal_append_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Hook called after WAL append. Returns whether to truncate WAL.
    pub fn after_wal_append(&self) -> bool {
        matches!(*self.wal_behavior.read(), WalBehavior::TruncateAfterWrite)
    }

    /// Hook called before manifest update. Returns whether to fail the update.
    pub fn before_manifest_update(&self) -> bool {
        self.manifest_update_count.fetch_add(1, Ordering::SeqCst);
        matches!(*self.manifest_behavior.read(), ManifestBehavior::FailSave)
    }

    /// Hook called after manifest fsync, before WAL truncation.
    pub fn manifest_fsynced_before_wal_truncate(&self) {
        self.manifest_fsynced_before_wal_truncate
            .store(true, Ordering::SeqCst);
    }

    /// Hook called after WAL truncation following manifest update.
    pub fn wal_truncated_after_manifest(&self) {
        self.wal_truncated_after_manifest
            .store(true, Ordering::SeqCst);
    }

    /// Hook called before compaction starts. Returns whether to fail midway.
    pub fn before_compaction(&self) -> bool {
        self.compaction_start_count.fetch_add(1, Ordering::SeqCst);
        matches!(
            *self.compaction_behavior.read(),
            CompactionBehavior::FailMidway
        )
    }

    /// Hook called after compaction completes successfully.
    pub fn after_compaction(&self) {
        self.compaction_complete_count
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Hook called when compaction fails.
    pub fn compaction_failed(&self) {
        self.compaction_failed_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Hook called before compaction output fsync. Returns whether to fail.
    pub fn before_compaction_fsync(&self) -> bool {
        matches!(
            *self.compaction_behavior.read(),
            CompactionBehavior::CrashBeforeFsync
        )
    }

    // -------------------------------------------------------------------------
    // Verification (called by tests)
    // -------------------------------------------------------------------------

    /// Get the number of fsync calls made.
    pub fn fsync_count(&self) -> u64 {
        self.fsync_count.load(Ordering::SeqCst)
    }

    /// Get the number of WAL appends.
    pub fn wal_append_count(&self) -> u64 {
        self.wal_append_count.load(Ordering::SeqCst)
    }

    /// Get the number of manifest updates.
    pub fn manifest_update_count(&self) -> u64 {
        self.manifest_update_count.load(Ordering::SeqCst)
    }

    /// Get the number of compactions started.
    pub fn compaction_start_count(&self) -> u64 {
        self.compaction_start_count.load(Ordering::SeqCst)
    }

    /// Get the number of compactions completed.
    pub fn compaction_complete_count(&self) -> u64 {
        self.compaction_complete_count.load(Ordering::SeqCst)
    }

    /// Get the number of compactions failed.
    pub fn compaction_failed_count(&self) -> u64 {
        self.compaction_failed_count.load(Ordering::SeqCst)
    }

    /// Verify that manifest was fsynced before WAL truncation.
    pub fn verify_manifest_fsynced_before_wal_truncate(&self) -> bool {
        self.manifest_fsynced_before_wal_truncate
            .load(Ordering::SeqCst)
    }

    /// Verify that WAL was truncated after manifest update.
    pub fn verify_wal_truncated_after_manifest(&self) -> bool {
        self.wal_truncated_after_manifest.load(Ordering::SeqCst)
    }

    /// Reset all counters and flags.
    pub fn reset(&self) {
        self.fsync_count.store(0, Ordering::SeqCst);
        self.wal_append_count.store(0, Ordering::SeqCst);
        self.manifest_update_count.store(0, Ordering::SeqCst);
        self.compaction_start_count.store(0, Ordering::SeqCst);
        self.compaction_complete_count.store(0, Ordering::SeqCst);
        self.compaction_failed_count.store(0, Ordering::SeqCst);
        self.wal_truncated_after_manifest
            .store(false, Ordering::SeqCst);
        self.manifest_fsynced_before_wal_truncate
            .store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_hooks_with_normal_behavior() {
        // Arrange & Act
        let hooks = TestHooks::new();

        // Assert
        assert!(hooks.before_fsync(), "Should perform fsync by default");
        assert_eq!(hooks.fsync_count(), 1);
    }

    #[test]
    fn should_skip_fsync_when_configured() {
        // Arrange
        let hooks = TestHooks::new().with_fsync_behavior(FsyncBehavior::Skip);

        // Act
        let should_fsync = hooks.before_fsync();

        // Assert
        assert!(!should_fsync, "Should skip fsync");
        assert_eq!(hooks.fsync_count(), 1, "Should still count the call");
    }

    #[test]
    fn should_record_wal_appends() {
        // Arrange
        let hooks = TestHooks::new();

        // Act
        hooks.before_wal_append();
        hooks.before_wal_append();
        hooks.before_wal_append();

        // Assert
        assert_eq!(hooks.wal_append_count(), 3);
    }

    #[test]
    fn should_track_compaction_lifecycle() {
        // Arrange
        let hooks = TestHooks::new();

        // Act
        hooks.before_compaction();
        hooks.after_compaction();

        // Assert
        assert_eq!(hooks.compaction_start_count(), 1);
        assert_eq!(hooks.compaction_complete_count(), 1);
    }

    #[test]
    fn should_reset_all_counters() {
        // Arrange
        let hooks = TestHooks::new();
        hooks.before_fsync();
        hooks.before_wal_append();
        hooks.before_manifest_update();

        // Act
        hooks.reset();

        // Assert
        assert_eq!(hooks.fsync_count(), 0);
        assert_eq!(hooks.wal_append_count(), 0);
        assert_eq!(hooks.manifest_update_count(), 0);
    }
}
