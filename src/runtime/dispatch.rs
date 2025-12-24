//! Dispatcher — maps incoming messages to actor task categories.
//!
//! IMPORTANT (Copilot guidance):
//! - Dispatcher does *not* handle per-request routing.
//! - Dispatcher must never touch ResponseRouter or pending responses.
//! - EventLoop is solely responsible for running actors and sending responses.
//! - This component only classifies messages by type.

use super::task::TaskKind;
use super::RuntimeMsg;

/// Message dispatcher — lightweight classifier.
pub struct Dispatcher;

impl Dispatcher {
    pub fn new() -> Self {
        Self
    }

    /// Determine which actor category should handle the message.
    ///
    /// Copilot: DO NOT move response routing here.
    pub fn route(&self, msg: &RuntimeMsg) -> TaskKind {
        use RuntimeMsg::*;

        match msg {
            // Flush
            FlushMemtable { .. } | FlushComplete { .. } => TaskKind::Flush,

            // Compaction
            CheckCompaction { .. } | RunCompaction { .. } | CompactionComplete { .. } => {
                TaskKind::Compaction
            }

            // WAL
            WalAppend { .. }
            | WalMerge { .. }
            | WriteBatch { .. }
            | WalSync { .. }
            | WalRotate { .. }
            | WalSyncComplete { .. } => TaskKind::Wal,

            // Cloud
            CloudUploadSst { .. } | CloudUploadWal { .. } | CloudUploadComplete { .. } => {
                TaskKind::Cloud
            }

            // GC
            CheckGc { .. } | DeleteObsoleteSsts { .. } => TaskKind::Gc,

            // Manifest
            ManifestAddSst { .. }
            | ManifestCompactionComplete { .. }
            | ManifestPersist { .. }
            | ManifestCreateColumnFamily { .. }
            | ManifestDropColumnFamily { .. } => TaskKind::Manifest,

            // User-level (reads, control, registration, observability)
            Read { .. }
            | RangeScan { .. }
            | RegisterMergeOperator { .. }
            | GetReadAmpMetrics { .. }
            | GetCurrentSequence { .. }
            | SetRuntimeConfig { .. }
            | GetRuntimeConfig { .. }
            | GetIngestState { .. }
            | BeginIngest { .. }
            | EndIngest { .. }
            | Shutdown
            | Noop { .. }
            | StartupPing { .. } => TaskKind::User,
        }
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{CompactionPlan, FileMeta};

    fn create_dispatcher() -> Dispatcher {
        Dispatcher::new()
    }

    // =========== Flush Actor Routes ===========

    #[test]
    fn should_route_flush_memtable_to_flush() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::FlushMemtable {
            request_id: 1,
            cf_id: 0,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Flush);
    }

    #[test]
    fn should_route_flush_complete_to_flush() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::FlushComplete {
            request_id: 1,
            cf_id: 0,
            sst_name: "sst_001.sst".to_string(),
            sequence: 100,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Flush);
    }

    // =========== Compaction Actor Routes ===========

    #[test]
    fn should_route_check_compaction_to_compaction() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::CheckCompaction { request_id: 1 };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Compaction);
    }

    #[test]
    fn should_route_run_compaction_to_compaction() {
        // Arrange
        let dispatcher = create_dispatcher();
        let plan = CompactionPlan {
            input_files: vec!["sst_001.sst".to_string()],
            source_level: 0,
            target_level: 1,
            cf_id: 0,
        };
        let msg = RuntimeMsg::RunCompaction {
            request_id: 1,
            plan,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Compaction);
    }

    #[test]
    fn should_route_compaction_complete_to_compaction() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::CompactionComplete {
            request_id: 1,
            input_ssts: vec!["sst_001.sst".to_string()],
            output_ssts: vec!["sst_002.sst".to_string()],
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Compaction);
    }

    // =========== WAL Actor Routes ===========

    #[test]
    fn should_route_wal_append_to_wal() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::WalAppend {
            request_id: 1,
            cf_id: 0,
            key: b"key".to_vec(),
            value: Some(b"value".to_vec()),
            ttl_seconds: None,
            insert_only: false,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Wal);
    }

    #[test]
    fn should_route_wal_merge_to_wal() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::WalMerge {
            request_id: 1,
            cf_id: 0,
            key: b"key".to_vec(),
            operand: b"operand".to_vec(),
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Wal);
    }

    #[test]
    fn should_route_wal_sync_to_wal() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::WalSync { request_id: 1 };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Wal);
    }

    #[test]
    fn should_route_wal_rotate_to_wal() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::WalRotate { request_id: 1 };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Wal);
    }

    #[test]
    fn should_route_wal_sync_complete_to_wal() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::WalSyncComplete {
            request_id: 1,
            segment_id: 1,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Wal);
    }

    // =========== Cloud Actor Routes ===========

    #[test]
    fn should_route_cloud_upload_sst_to_cloud() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::CloudUploadSst {
            request_id: 1,
            sst_name: "sst_001.sst".to_string(),
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Cloud);
    }

    #[test]
    fn should_route_cloud_upload_wal_to_cloud() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::CloudUploadWal {
            request_id: 1,
            segment_id: 1,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Cloud);
    }

    #[test]
    fn should_route_cloud_upload_complete_to_cloud() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::CloudUploadComplete {
            request_id: 1,
            resource: "sst_001.sst".to_string(),
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Cloud);
    }

    // =========== GC Actor Routes ===========

    #[test]
    fn should_route_check_gc_to_gc() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::CheckGc { request_id: 1 };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Gc);
    }

    #[test]
    fn should_route_delete_obsolete_ssts_to_gc() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::DeleteObsoleteSsts {
            request_id: 1,
            sst_names: vec!["sst_001.sst".to_string()],
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Gc);
    }

    // =========== Manifest Actor Routes ===========

    #[test]
    fn should_route_manifest_add_sst_to_manifest() {
        // Arrange
        let dispatcher = create_dispatcher();
        let file_meta = FileMeta {
            name: "sst_001.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            smallest_seq: None,
            largest_seq: None,
        };
        let msg = RuntimeMsg::ManifestAddSst {
            request_id: 1,
            file_meta,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Manifest);
    }

    #[test]
    fn should_route_manifest_compaction_complete_to_manifest() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::ManifestCompactionComplete {
            request_id: 1,
            removed: vec!["sst_001.sst".to_string()],
            added: vec![],
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Manifest);
    }

    #[test]
    fn should_route_manifest_persist_to_manifest() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::ManifestPersist { request_id: 1 };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Manifest);
    }

    #[test]
    fn should_route_manifest_create_column_family_to_manifest() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::ManifestCreateColumnFamily {
            request_id: 1,
            name: "new_cf".to_string(),
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Manifest);
    }

    #[test]
    fn should_route_manifest_drop_column_family_to_manifest() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::ManifestDropColumnFamily {
            request_id: 1,
            cf_id: 1,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::Manifest);
    }

    // =========== User Actor Routes ===========

    #[test]
    fn should_route_read_to_user() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::Read {
            request_id: 1,
            cf_id: 0,
            key: b"key".to_vec(),
            sequence: 1,
            requested_durability: crate::engine::api::Durability::Steady,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::User);
    }

    #[test]
    fn should_route_range_scan_to_user() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::RangeScan {
            request_id: 1,
            cf_id: 0,
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            sequence: 1,
            requested_durability: crate::engine::api::Durability::Steady,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::User);
    }

    #[test]
    fn should_route_register_merge_operator_to_user() {
        // Arrange
        let dispatcher = create_dispatcher();

        // Create a mock merge operator for testing
        use std::sync::Arc;
        #[derive(Debug)]
        struct MockOperator;
        impl crate::engine::MergeOperator for MockOperator {
            fn merge(
                &self,
                _key: &[u8],
                _base_value: Option<&[u8]>,
                _operands: &[Vec<u8>],
            ) -> crate::common::MidgeResult<Option<Vec<u8>>> {
                Ok(None)
            }

            fn name(&self) -> &str {
                "mock"
            }
        }

        let operator = Arc::new(MockOperator) as Arc<dyn crate::engine::MergeOperator>;
        let msg = RuntimeMsg::RegisterMergeOperator {
            request_id: 1,
            cf_id: 0,
            operator,
        };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::User);
    }

    #[test]
    fn should_route_noop_to_user() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::Noop { request_id: 1 };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::User);
    }

    #[test]
    fn should_route_startup_ping_to_user() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::StartupPing { request_id: 1 };

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::User);
    }

    #[test]
    fn should_route_shutdown_to_user() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::Shutdown;

        // Act
        let kind = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind, TaskKind::User);
    }

    // =========== Dispatcher Default ===========

    #[test]
    fn should_create_dispatcher_with_default() {
        // Arrange & Act
        let dispatcher = Dispatcher;

        // Assert - Just verify it's usable
        let msg = RuntimeMsg::Noop { request_id: 1 };
        let kind = dispatcher.route(&msg);
        assert_eq!(kind, TaskKind::User);
    }

    // =========== Routing Consistency ===========

    #[test]
    fn should_route_same_message_consistently() {
        // Arrange
        let dispatcher = create_dispatcher();
        let msg = RuntimeMsg::FlushMemtable {
            request_id: 1,
            cf_id: 0,
        };

        // Act
        let kind1 = dispatcher.route(&msg);
        let kind2 = dispatcher.route(&msg);

        // Assert
        assert_eq!(kind1, kind2);
    }
}
