//! Backup and restore subsystem for Midge database.
//!
//! Provides full and incremental backup capabilities with verification.
//! Backups are atomic, consistent snapshots of the database state.

mod backup_engine;
mod restore_engine;
mod types;

#[cfg(test)]
mod tests;

// Re-export public API
pub use backup_engine::BackupEngine;
pub use restore_engine::RestoreEngine;
pub use types::{
    BackupInfo, BackupOptions, BackupType, RestoreOptions, SstFileInfo, VerifyResult,
};
