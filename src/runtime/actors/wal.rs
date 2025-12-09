//! WAL Actor - handles write-ahead log operations
//!
//! Responsible for:
//! - Appending records to WAL
//! - Syncing WAL to disk
//! - Rotating WAL segments
//! - Coordinating with cloud actor for WAL uploads

use crate::common::MidgeResult;
use crate::wal::{WalWriter, WalRecord, WalOpKind, FsWalFactory, WalFactory};
use bytes::Bytes;
use super::super::state::RuntimeState;
use std::path::PathBuf;

/// Actor handling WAL operations
pub struct WalActor {
    /// Current WAL writer
    writer: Option<Box<dyn WalWriter>>,
    /// WAL directory
    wal_dir: PathBuf,
    /// Buffered writes pending sync
    pending_sync_count: usize,
}

impl WalActor {
    pub fn new(wal_dir: PathBuf) -> MidgeResult<Self> {
        // Create WAL directory if needed
        std::fs::create_dir_all(&wal_dir)
            .map_err(|e| crate::common::MidgeError::Io(e))?;
        
        // Create writer via factory
        let factory = FsWalFactory;
        let writer = factory.create_writer(&wal_dir)?;
        
        Ok(Self {
            writer: Some(writer),
            wal_dir,
            pending_sync_count: 0,
        })
    }

    /// Append a record to the WAL
    pub fn append(
        &mut self,
        state: &mut RuntimeState,
        cf_id: u32,
        key: Bytes,
        value: Option<Bytes>,
        sequence: u64,
    ) -> MidgeResult<()> {
        // Create WAL record
        let record = WalRecord::new_cf(cf_id, WalOpKind::Put, key, value, sequence);
        
        // Append to WAL
        if let Some(writer) = &self.writer {
            writer.append_record(&record)?;
        }
        
        // Update state tracking
        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;

        tracing::trace!(
            cf_id,
            sequence,
            "WAL append"
        );

        Ok(())
    }

    /// Sync WAL to disk
    pub fn sync(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        let pending = state.wal.pending_writes;

        // Sync writer if present
        if let Some(writer) = &self.writer {
            writer.sync()?;
        }
        
        // Update state
        state.wal.last_synced_seq = state.sequence;
        state.wal.pending_writes = 0;
        self.pending_sync_count = 0;

        tracing::debug!(
            pending_writes = pending,
            synced_seq = state.wal.last_synced_seq,
            "WAL sync"
        );

        Ok(())
    }

    /// Rotate to a new WAL segment
    pub fn rotate(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        let old_segment = state.wal.current_segment_id;
        
        // Rotate via factory
        let factory = FsWalFactory;
        self.writer = Some(factory.rotate_writer(&self.wal_dir, old_segment)?);
        
        state.wal.current_segment_id += 1;

        tracing::info!(
            old_segment,
            new_segment = state.wal.current_segment_id,
            "WAL rotate"
        );

        Ok(())
    }

    /// Handle sync completion notification
    pub fn handle_sync_complete(&mut self, state: &mut RuntimeState, segment_id: u64) {
        tracing::debug!(segment_id, "WAL sync complete");

        // Update last synced info if this is newer
        if segment_id >= state.wal.current_segment_id {
            // This sync covers the current segment
        }
    }
}

impl Default for WalActor {
    fn default() -> Self {
        // Cannot create with default since we need a WAL directory
        panic!("WalActor::default() should not be called, use WalActor::new(wal_dir)")
    }
}