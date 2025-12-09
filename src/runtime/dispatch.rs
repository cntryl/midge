//! Dispatcher - routes messages to appropriate actors
//!
//! Simple message routing based on message type.

use super::task::TaskKind;
use super::RuntimeMsg;

/// Message dispatcher
pub struct Dispatcher;

impl Dispatcher {
    /// Create a new dispatcher
    pub fn new() -> Self {
        Self
    }

    /// Determine which actor should handle a message
    pub fn route(&self, msg: &RuntimeMsg) -> TaskKind {
        match msg {
            RuntimeMsg::FlushMemtable { .. } | RuntimeMsg::FlushComplete { .. } => TaskKind::Flush,

            RuntimeMsg::CheckCompaction
            | RuntimeMsg::RunCompaction { .. }
            | RuntimeMsg::CompactionComplete { .. } => TaskKind::Compaction,

            RuntimeMsg::WalAppend { .. }
            | RuntimeMsg::WalSync
            | RuntimeMsg::WalRotate
            | RuntimeMsg::WalSyncComplete { .. } => TaskKind::Wal,

            RuntimeMsg::CloudUploadSst { .. }
            | RuntimeMsg::CloudUploadWal { .. }
            | RuntimeMsg::CloudUploadComplete { .. } => TaskKind::Cloud,

            RuntimeMsg::CheckGc | RuntimeMsg::DeleteObsoleteSsts { .. } => TaskKind::Gc,

            RuntimeMsg::ManifestAddSst { .. }
            | RuntimeMsg::ManifestCompactionComplete { .. }
            | RuntimeMsg::ManifestPersist
            | RuntimeMsg::ManifestCreateColumnFamily { .. }
            | RuntimeMsg::ManifestDropColumnFamily { .. } => TaskKind::Manifest,

            RuntimeMsg::Read { .. } => TaskKind::User,

            RuntimeMsg::Shutdown | RuntimeMsg::Noop => TaskKind::User,
        }
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}
