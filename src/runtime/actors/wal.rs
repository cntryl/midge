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
//! NOTE: `DurabilityPolicy::CloudFirst` is not fully wired end-to-end yet.
//! The actor has scaffolding for queuing pending cloud writes, but the runtime
//! event loop currently responds to WAL append requests immediately.
//! Do not enable CloudFirst until response deferral + cloud ACK completion are
//! implemented in the runtime.
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
    /// WAL writer (owned by this actor)
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
    pub fn new(
        wal_dir: PathBuf,
        durability_policy: DurabilityPolicy,
        memory_mode: bool,
    ) -> MidgeResult<Self> {
        let writer = if memory_mode {
            None
        } else {
            let factory = FsWalFactory;
            Some(factory.create_writer(&wal_dir)?)
        };

        Ok(Self {
            writer,
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
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        state: &mut RuntimeState,
        cf_id: u32,
        key: Bytes,
        value: Option<Bytes>,
        insert_only: bool,
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<u64> {
        // Enforce insert-only if requested by checking in-memory state
        if insert_only && self.key_exists(state, cf_id, &key) {
            return Err(MidgeError::InvalidArgument(
                "key already exists".to_string(),
            ));
        }
        // Allocate sequence number at append time to preserve a total order under concurrency.
        let sequence = state.next_sequence();

        // Determine operation kind: Delete if value is None, Put otherwise
        let op_kind = if value.is_none() {
            WalOpKind::Delete
        } else {
            WalOpKind::Put
        };

        // Create WAL record (with expiration if provided)
        let record = match ttl_seconds {
            Some(ttl) if ttl > 0 => {
                WalRecord::new_with_ttl(cf_id, op_kind, key.clone(), value.clone(), sequence, ttl)
            }
            _ => WalRecord::new_cf(cf_id, op_kind, key.clone(), value.clone(), sequence),
        };

        // Calculate record size for batching
        let record_size = record.key.len() + record.value.as_ref().map_or(0, |v| v.len());

        // ALWAYS append to local WAL first (FsWalWriter)
        if let Some(writer) = &mut self.writer {
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

    /// Append a batch of operations to the WAL as a single atomic unit.
    ///
    /// This method:
    /// - allocates a single transaction id
    /// - writes TxnBegin marker
    /// - writes all operation records (in order)
    /// - writes TxnCommit marker
    /// - applies all operations to memtables (in order)
    ///
    /// Returns the last allocated sequence number for the batch.
    pub fn append_batch(
        &mut self,
        state: &mut RuntimeState,
        ops: Vec<crate::runtime::WriteBatchOp>,
    ) -> MidgeResult<u64> {
        if ops.is_empty() {
            return Ok(state.sequence);
        }

        if matches!(self.durability_policy, DurabilityPolicy::CloudFirst) {
            return Err(MidgeError::InvalidArgument(
                "CloudFirst durability is not supported for write batches yet".to_string(),
            ));
        }

        let txn_id = state.next_txn_id();

        // Marker key is unused by semantics but required by the record format.
        let marker_key = Bytes::from_static(b"txn");

        let begin_seq = state.next_sequence();
        let mut begin_record = WalRecord::new_cf(
            0,
            WalOpKind::TxnBegin,
            marker_key.clone(),
            None,
            begin_seq,
        );
        begin_record.txn_id = Some(txn_id);

        if let Some(writer) = &mut self.writer {
            writer.append_record(&begin_record)?;
        }

        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += begin_record.estimated_size();

        // Collect the concrete records we wrote so we can apply in order.
        // (We keep the minimal info needed to apply, rather than replaying WAL.)
        enum ApplyOp {
            Put {
                cf_id: u32,
                key: Bytes,
                value: Bytes,
                expiration: Option<u64>,
            },
            Delete {
                cf_id: u32,
                key: Bytes,
            },
        }

        let mut apply_ops: Vec<ApplyOp> = Vec::with_capacity(ops.len());

        for op in ops {
            match op {
                crate::runtime::WriteBatchOp::Put {
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                } => {
                    let seq = state.next_sequence();
                    let key_b = Bytes::from(key);
                    let value_b = Bytes::from(value);

                    let mut record = match ttl_seconds {
                        Some(ttl) if ttl > 0 => WalRecord::new_with_ttl(
                            cf_id,
                            WalOpKind::Put,
                            key_b.clone(),
                            Some(value_b.clone()),
                            seq,
                            ttl,
                        ),
                        _ => WalRecord::new_cf(
                            cf_id,
                            WalOpKind::Put,
                            key_b.clone(),
                            Some(value_b.clone()),
                            seq,
                        ),
                    };
                    record.txn_id = Some(txn_id);

                    if let Some(writer) = &mut self.writer {
                        writer.append_record(&record)?;
                    }

                    state.wal.pending_writes += 1;
                    self.pending_sync_count += 1;
                    self.bytes_since_sync += record.estimated_size();

                    apply_ops.push(ApplyOp::Put {
                        cf_id,
                        key: key_b,
                        value: value_b,
                        expiration: record.expiration,
                    });
                }
                crate::runtime::WriteBatchOp::Delete { cf_id, key } => {
                    let seq = state.next_sequence();
                    let key_b = Bytes::from(key);

                    let mut record = WalRecord::new_cf(
                        cf_id,
                        WalOpKind::Delete,
                        key_b.clone(),
                        None,
                        seq,
                    );
                    record.txn_id = Some(txn_id);

                    if let Some(writer) = &mut self.writer {
                        writer.append_record(&record)?;
                    }

                    state.wal.pending_writes += 1;
                    self.pending_sync_count += 1;
                    self.bytes_since_sync += record.estimated_size();

                    apply_ops.push(ApplyOp::Delete { cf_id, key: key_b });
                }
            }
        }

        let commit_seq = state.next_sequence();
        let last_sequence = commit_seq;
        let mut commit_record = WalRecord::new_cf(
            0,
            WalOpKind::TxnCommit,
            marker_key,
            None,
            commit_seq,
        );
        commit_record.txn_id = Some(txn_id);

        if let Some(writer) = &mut self.writer {
            writer.append_record(&commit_record)?;
        }

        state.wal.pending_writes += 1;
        self.pending_sync_count += 1;
        self.bytes_since_sync += commit_record.estimated_size();

        // Apply durability policy (single sync for the whole batch, where relevant).
        match self.durability_policy {
            DurabilityPolicy::Strict | DurabilityPolicy::CloudMirrored => {
                self.sync_internal(state)?;
                state.wal.local_durable_seq = last_sequence;
            }
            DurabilityPolicy::Batched => {
                // no-op; background/timer can sync later
            }
            DurabilityPolicy::CloudFirst => {
                // guarded above
            }
        }

        let op_count = apply_ops.len();

        // Apply to memtables in-order (atomic visibility within the actor).
        for apply_op in apply_ops {
            match apply_op {
                ApplyOp::Put {
                    cf_id,
                    key,
                    value,
                    expiration,
                } => {
                    self.apply_to_memtable(state, cf_id, &key, &Some(value), expiration)?;
                }
                ApplyOp::Delete { cf_id, key } => {
                    self.apply_to_memtable(state, cf_id, &key, &None, None)?;
                }
            }
        }

        tracing::trace!(txn_id, last_sequence, op_count, "WAL batch append");

        Ok(last_sequence)
    }

    /// Append a merge operand to the WAL
    pub fn append_merge(
        &mut self,
        state: &mut RuntimeState,
        cf_id: u32,
        key: Bytes,
        operand: Bytes,
    ) -> MidgeResult<u64> {
        // Allocate sequence number at append time to preserve a total order under concurrency.
        let sequence = state.next_sequence();

        // Create WAL record for merge
        let record = WalRecord::new_cf(
            cf_id,
            WalOpKind::Merge,
            key.clone(),
            Some(operand.clone()),
            sequence,
        );

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

        // Apply durability policy (same as regular append)
        match self.durability_policy {
            DurabilityPolicy::Strict => {
                // Fsync immediately, then apply to memtable
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply merge operand to memtable
                self.apply_merge_to_memtable(state, cf_id, &key, &operand)?;
            }
            DurabilityPolicy::Batched => {
                // Apply to memtable immediately (no cloud wait)
                self.apply_merge_to_memtable(state, cf_id, &key, &operand)?;
                // Sync if batch thresholds exceeded (handled by caller/timer)
            }
            DurabilityPolicy::CloudMirrored => {
                // Fsync locally, apply to memtable, schedule cloud upload in background
                self.sync_internal(state)?;
                state.wal.local_durable_seq = sequence;

                // Apply merge operand to memtable
                self.apply_merge_to_memtable(state, cf_id, &key, &operand)?;

                // Queue for async cloud upload (handled by cloud actor)
                self.pending_cloud_writes.push_back(PendingCloudWrite {
                    request_id: 0, // Will be set by cloud upload logic
                    cf_id,
                    key: key.to_vec(),
                    value: Some(operand.to_vec()),
                    sequence,
                    expiration: None,
                });
            }
            DurabilityPolicy::CloudFirst => {
                // Queue for cloud upload without applying to memtable yet
                // Write is NOT visible until cloud confirms
                self.pending_cloud_writes.push_back(PendingCloudWrite {
                    request_id: 0,
                    cf_id,
                    key: key.to_vec(),
                    value: Some(operand.to_vec()),
                    sequence,
                    expiration: None,
                });
            }
        }

        tracing::trace!(cf_id, sequence, policy = ?self.durability_policy, "WAL merge append");

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

    /// Apply a merge operand to the memtable
    fn apply_merge_to_memtable(
        &self,
        state: &RuntimeState,
        cf_id: u32,
        key: &[u8],
        operand: &Bytes,
    ) -> MidgeResult<()> {
        if let Some(cf_state) = state.column_families.get(&cf_id) {
            cf_state
                .memtable
                .as_ref()
                .merge(key.to_vec(), operand.to_vec())?;
        }
        Ok(())
    }

    /// Internal sync helper - fsyncs the writer
    fn sync_internal(&mut self, state: &mut RuntimeState) -> MidgeResult<()> {
        if let Some(writer) = &mut self.writer {
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
                let write = self
                    .pending_cloud_writes
                    .pop_front()
                    .expect("pending write exists after front() check");

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
