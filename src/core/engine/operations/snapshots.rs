//! Snapshot operations for MidgeEngine
//!
//! This module contains snapshot creation for consistent point-in-time reads.

use std::sync::atomic::Ordering;

use crate::api::snapshot::Snapshot;

use super::super::MidgeEngine;

impl MidgeEngine {
    /// Create a snapshot capturing the current sequence number for consistent reads.
    ///
    /// The snapshot captures the current state of the database at the moment of creation.
    /// All reads using this snapshot will see a consistent view of the database as it
    /// existed at this point in time, regardless of subsequent writes.
    ///
    /// The snapshot will see all writes with sequence numbers strictly less than
    /// the snapshot's sequence number (not <=).
    ///
    /// # Returns
    ///
    /// Returns a `Snapshot` that can be used with `get_at()` and `scan_at()` methods
    /// to perform consistent reads.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let cf = engine.default_column_family();
    ///
    /// // Write initial value
    /// engine.put(&cf, b"key", b"value1").unwrap();
    ///
    /// // Create snapshot
    /// let snapshot = engine.snapshot();
    ///
    /// // Subsequent writes are not visible in the snapshot
    /// engine.put(&cf, b"key", b"value2").unwrap();
    ///
    /// // Reading from snapshot sees the old value
    /// let value = engine.get_at(&cf, b"key", &snapshot).unwrap();
    /// assert_eq!(value.as_deref(), Some(&b"value1"[..]));
    ///
    /// // Reading without snapshot sees the new value
    /// let value = engine.get(&cf, b"key").unwrap();
    /// assert_eq!(value.as_deref(), Some(&b"value2"[..]));
    /// ```
    pub fn snapshot(&self) -> Snapshot {
        self.metrics.snapshot_created();
        // Load the CURRENT sequence counter value. This is the next sequence that
        // will be assigned to a write. The snapshot will see all writes with
        // seq < this value (strictly less than, not <=).
        let seq = self.seq.load(Ordering::SeqCst);
        self.snapshot_registry.register(seq)
    }
}

#[cfg(test)]
mod tests {
    use crate::{MidgeEngine, MidgeOptions, StorageMode};
    use bytes::Bytes;
    use uuid;

    fn create_test_engine() -> MidgeEngine {
        let temp_dir =
            std::env::temp_dir().join(format!("midge_test_snapshots_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir;
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk { db_path },
            enable_compaction: false,
            ..Default::default()
        };
        MidgeEngine::open(opts).unwrap()
    }

    #[test]
    fn should_create_snapshot_successfully() {
        // Arrange
        let engine = create_test_engine();

        // Act
        let _snapshot = engine.snapshot();

        // Assert
        // Sequence should be non-negative (u64)
    }

    #[test]
    fn should_snapshot_capture_state_before_writes() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();
        engine.put(&cf, b"key1", b"value1").unwrap();

        // Act
        let snapshot = engine.snapshot();
        engine.put(&cf, b"key1", b"value2").unwrap();

        // Assert
        let current_value = engine.get(&cf, b"key1").unwrap();
        let snapshot_value = engine.get_at(&cf, b"key1", &snapshot).unwrap();
        assert_eq!(current_value, Some(Bytes::from("value2")));
        assert_eq!(snapshot_value, Some(Bytes::from("value1")));
    }
}
