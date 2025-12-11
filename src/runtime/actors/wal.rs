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
use crate::common::MidgeError;
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
    expiration: Option<u64>,
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
    /// - Strict: fsync immediately + apply to memtable + respond
    /// - Batched: batch writes + apply to memtable immediately + respond
    /// - CloudMirrored: fsync + apply to memtable + schedule cloud upload + respond
    /// - CloudFirst: local write (cache) + queue for cloud + DO NOT apply to memtable yet
    ///
    /// In CloudFirst mode, writes are NOT visible until cloud acknowledges.
    /// Returns the assigned sequence number.
    pub fn append(
        &mut self,
        state: &mut RuntimeState,
        cf_id: u32,
        key: Bytes,
        value: Option<Bytes>,
        _sequence: u64, // Ignored - runtime assigns
        insert_only: bool,
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<u64> {
        // Enforce insert-only if requested by checking in-memory state
        if insert_only && self.key_exists(state, cf_id, &key) {
            return Err(MidgeError::InvalidArgument(
                "key already exists".to_string(),
            ));
        }
        // Assign sequence number from runtime state
        let sequence = state.next_sequence();

        // Create WAL record (with expiration if provided)
        let record = match ttl_seconds {
            Some(ttl) if ttl > 0 => WalRecord::new_with_ttl(
                cf_id,
                WalOpKind::Put,
                key.clone(),
                value.clone(),
                sequence,
                ttl,
            ),
            _ => WalRecord::new_cf(cf_id, WalOpKind::Put, key.clone(), value.clone(), sequence),
        };

        // Calculate record size for batching
        let record_size = record.key.len() + record.value.as_ref().map_or(0, |v| v.len());

        // ALWAYS append to local WAL first (FsWalWriter)
        if let Some(writer) = &self.writer {
            writer.append_record(&record)?;
        }

        // Update state tracking
        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += record_size;

        // Apply durability policy
        match self.durability_policy {
            DurabilityPolicy::Strict => {
                // Fsync immediately, then apply to memtable
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply to memtable - write is now visible
                self.apply_to_memtable(state, cf_id, &key, &value, record.expiration)?;
            }
            DurabilityPolicy::Batched => {
                // Apply to memtable immediately (no cloud wait)
                self.apply_to_memtable(state, cf_id, &key, &value, record.expiration)?;
                // Sync if batch thresholds exceeded (handled by caller/timer)
            }
            DurabilityPolicy::CloudMirrored => {
                // Fsync locally, apply to memtable, schedule cloud upload in background
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply to memtable - local durability sufficient
                self.apply_to_memtable(state, cf_id, &key, &value, record.expiration)?;

                // TODO: Send CloudUploadWal message to CloudActor
            }
            DurabilityPolicy::CloudFirst => {
                // DO NOT apply to memtable yet!
                // Queue write for cloud durability confirmation
                // Memtable update happens in handle_cloud_upload_complete
                // No response sent yet (caller must wait for cloud ACK)
            }
        }

        tracing::trace!(cf_id, sequence, policy = ?self.durability_policy, "WAL append");

        Ok(sequence)
    }

    /// Checks current in-memory view (active + immutable memtables) for existence
    fn key_exists(&self, state: &RuntimeState, cf_id: u32, key: &[u8]) -> bool {
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            if let Ok(Some(_)) = cf_state.memtable.get(key) {
                return true;
            }
            for imm in cf_state.immutable_memtables.iter().rev() {
                if let Ok(Some(_)) = imm.get(key) {
                    return true;
                }
            }
        }
        false
    }

    /// Apply a write to the memtable
    fn apply_to_memtable(
        &self,
        state: &RuntimeState,
        cf_id: u32,
        key: &[u8],
        value: &Option<Bytes>,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            if let Some(val) = value {
                cf_state
                    .memtable
                    .as_ref()
                    .put_with_exp(key.to_vec(), val.to_vec(), expiration)?;
            } else {
                cf_state.memtable.as_ref().delete(key.to_vec())?;
            }
        }
        Ok(())
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
    /// Updates cloud_durable_seq and completes any pending writes
    /// by applying them to memtable and returning request_ids to complete.
    pub fn handle_cloud_upload_complete(
        &mut self,
        state: &mut RuntimeState,
        segment_id: u64,
        max_seq_in_segment: u64,
    ) -> MidgeResult<Vec<u64>> {
        // Update cloud durability frontier
        state.wal.cloud_durable_seq = state.wal.cloud_durable_seq.max(max_seq_in_segment);

        tracing::debug!(
            segment_id,
            cloud_durable_seq = state.wal.cloud_durable_seq,
            "Cloud upload complete"
        );

        // Apply pending writes to memtable and collect completed request_ids
        let mut completed_requests = Vec::new();

        while let Some(pending) = self.pending_cloud_writes.front() {
            if pending.sequence <= state.wal.cloud_durable_seq {
                let write = self.pending_cloud_writes.pop_front().unwrap();

                // NOW apply to memtable - write becomes visible
                let key_bytes = Bytes::from(write.key);
                let value_bytes = write.value.map(Bytes::from);
                self.apply_to_memtable(
                    state,
                    write.cf_id,
                    &key_bytes,
                    &value_bytes,
                    write.expiration,
                )?;

                completed_requests.push(write.request_id);

                tracing::trace!(
                    request_id = write.request_id,
                    sequence = write.sequence,
                    "Applied cloud-durable write to memtable"
                );
            } else {
                break;
            }
        }

        Ok(completed_requests)
    }

    /// Queue a write waiting for cloud durability (CloudFirst mode)
    pub fn queue_cloud_write(
        &mut self,
        request_id: u64,
        cf_id: u32,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
        expiration: Option<u64>,
    ) {
        self.pending_cloud_writes.push_back(PendingCloudWrite {
            request_id,
            cf_id,
            key,
            value,
            sequence,
            expiration,
        });

        tracing::trace!(request_id, sequence, "Queued write for cloud durability");
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
