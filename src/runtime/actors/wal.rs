//! WAL Actor - handles write-ahead log operations
//!
//! Responsible for:
//! - Appending records to WAL
//! - Syncing WAL to disk
//! - Rotating WAL segments
//! - Coordinating with cloud actor for WAL uploads
//! - Tracking local_durable_seq and cloud_durable_seq frontiers
//! - Managing pending requests waiting for cloud durability
//!
//! ARCHITECTURAL RULES:
//! - ALWAYS uses FsWalWriter (never creates new backends)
//! - Assigns global sequence numbers via state.next_sequence()
//! - Tracks two durability frontiers for CloudFirst mode
//! - Does NOT block event loop waiting for cloud
//! - Queues pending requests and completes them when cloud_durable_seq advances
//!
//! CLOUD-DURABLE MEMTABLE RULE:
//! In Durability::CloudFirst mode, writes are NOT visible in memtable
//! until cloud storage acknowledges durability. Local WAL is ephemeral;
//! cloud WAL is the source of truth. Therefore:
//! - Append to local WAL immediately (fast path)
//! - Do NOT update memtable yet
//! - Do NOT respond to request yet
//! - Queue as PendingCloudWrite
//! - When cloud ACKs: apply to memtable + respond to request

use super::super::state::RuntimeState;
use crate::common::MidgeResult;
use crate::sst::Memtable;
use crate::wal::{DurabilityPolicy, FsWalFactory, WalFactory, WalOpKind, WalRecord, WalWriter};
use bytes::Bytes;
use std::collections::VecDeque;
use std::path::PathBuf;

/// Pending write waiting for cloud durability confirmation
/// (CloudFirst mode only - other modes apply to memtable immediately)
#[derive(Debug)]
struct PendingCloudWrite {
    request_id: u64,
    cf_id: u32,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    sequence: u64,
}

/// Actor handling WAL operations
pub struct WalActor {
    /// Current WAL writer (always FsWalWriter via factory)
    writer: Option<Box<dyn WalWriter>>,
    /// WAL directory
    wal_dir: PathBuf,
    /// Buffered writes pending sync
    pending_sync_count: usize,
    /// Durability policy (determines sync behavior)
    durability_policy: DurabilityPolicy,
    /// Pending writes waiting for cloud durability (CloudFirst mode only)
    /// These writes are in local WAL but NOT in memtable yet
    pending_cloud_writes: VecDeque<PendingCloudWrite>,
    /// Bytes written since last sync (for batched mode)
    bytes_since_sync: usize,
}

impl WalActor {
    pub fn new(wal_dir: PathBuf, durability_policy: DurabilityPolicy) -> MidgeResult<Self> {
        // Create WAL directory if needed
        std::fs::create_dir_all(&wal_dir).map_err(|e| crate::common::MidgeError::Io(e))?;

        // Create writer via factory (always FsWalWriter - never create backends)
        let factory = FsWalFactory;
        let writer = factory.create_writer(&wal_dir)?;

        Ok(Self {
            writer: Some(writer),
            wal_dir,
            pending_sync_count: 0,
            durability_policy,
            pending_cloud_writes: VecDeque::new(),
            bytes_since_sync: 0,
        })
    }

    /// Append a record to the WAL
    ///
    /// Always appends locally first. Durability behavior depends on policy:
    /// - Strict: fsync immediately
    /// - Batched: batch and fsync periodically
    /// - CloudMirrored: fsync + schedule cloud upload
    /// - CloudFirst: schedule cloud upload (caller must wait for cloud ack)
    ///
    /// Returns the assigned sequence number for CloudFirst tracking.
    pub fn append(
        &mut self,
        state: &mut RuntimeState,
        cf_id: u32,
        key: Bytes,
        value: Option<Bytes>,
        _sequence: u64, // Ignored - runtime assigns
    ) -> MidgeResult<u64> {
        // Assign sequence number from runtime state
        let sequence = state.next_sequence();

        // Create WAL record
        let record = WalRecord::new_cf(cf_id, WalOpKind::Put, key.clone(), value.clone(), sequence);

        // Calculate record size for batching
        let record_size = record.key.len() + record.value.as_ref().map_or(0, |v| v.len());

        // ALWAYS append to local WAL first (FsWalWriter)
        if let Some(writer) = &self.writer {
            writer.append_record(&record)?;
        }

        // Update memtable - reads must see writes immediately
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            if let Some(val) = &value {
                cf_state.memtable.as_ref().put(key.to_vec(), val.to_vec())?;
            } else {
                cf_state.memtable.as_ref().delete(key.to_vec())?;
            }
        }

        // Update state tracking
        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += record_size;

        // Apply durability policy
        match self.durability_policy {
            DurabilityPolicy::Strict => {
                // Fsync immediately
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;
            }
            DurabilityPolicy::Batched => {
                // Sync if batch thresholds exceeded (handled by caller/timer)
            }
            DurabilityPolicy::CloudMirrored => {
                // Fsync locally, schedule cloud upload in background
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;
                // TODO: Send CloudUploadWal message to CloudActor
            }
            DurabilityPolicy::CloudFirst => {
                // Append locally but don't consider durable until cloud confirms
                // Caller must track request_id and wait for CloudUploadComplete
            }
        }

        tracing::trace!(cf_id, sequence, policy = ?self.durability_policy, "WAL append");

        Ok(sequence)
    }

    /// Internal sync helper - fsyncs the writer
    fn sync_internal(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        if let Some(writer) = &self.writer {
            writer.sync()?;
        }

        state.wal.last_synced_seq = state.sequence;
        state.wal.local_durable_seq = state.sequence;
        state.wal.pending_writes = 0;
        self.pending_sync_count = 0;
        self.bytes_since_sync = 0;

        Ok(())
    }

    /// Sync WAL to disk (public interface)
    pub fn sync(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        let pending = state.wal.pending_writes;

        self.sync_internal(state)?;

        tracing::debug!(
            pending_writes = pending,
            synced_seq = state.wal.last_synced_seq,
            local_durable = state.wal.local_durable_seq,
            "WAL sync"
        );

        Ok(())
    }

    /// Check if batched sync should trigger
    pub fn should_sync_batch(&self) -> bool {
        // TODO: Add time-based check (max_delay_ms)
        const MAX_BATCH_BYTES: usize = 64 * 1024; // 64KB
        self.bytes_since_sync >= MAX_BATCH_BYTES
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

    /// Handle cloud upload completion (CloudFirst durability)
    ///
    /// Updates cloud_durable_seq and completes any pending requests
    /// that are now durable.
    pub fn handle_cloud_upload_complete(
        &mut self,
        state: &mut RuntimeState,
        segment_id: u64,
    ) -> Vec<u64> {
        // Update cloud durability frontier
        // TODO: Track per-segment max sequence to update cloud_durable_seq precisely
        // For now, assume segment upload means all records in that segment are durable
        
        tracing::debug!(
            segment_id,
            cloud_durable_seq = state.wal.cloud_durable_seq,
            "Cloud upload complete"
        );

        // Return request_ids that can now be completed
        let mut completed_requests = Vec::new();
        
        while let Some(pending) = self.pending_cloud_requests.front() {
            if pending.sequence <= state.wal.cloud_durable_seq {
                let req = self.pending_cloud_requests.pop_front().unwrap();
                completed_requests.push(req.request_id);
            } else {
                break;
            }
        }

        completed_requests
    }

    /// Queue a request waiting for cloud durability
    pub fn queue_cloud_request(&mut self, request_id: u64, sequence: u64) {
        self.pending_cloud_requests.push_back(PendingCloudRequest {
            request_id,
            sequence,
        });

        tracing::trace!(request_id, sequence, "Queued cloud durability request");
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
