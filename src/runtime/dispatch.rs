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
            WalAppend { .. } | WalSync { .. } | WalRotate { .. } | WalSyncComplete { .. } => {
                TaskKind::Wal
            }

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

            // User-level (reads, control)
            Read { .. } | RangeScan { .. } | Shutdown | Noop { .. } | StartupPing { .. } => {
                TaskKind::User
            }
        }
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}
