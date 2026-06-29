//! `CloudAsync` Write Queue Management
//!
//! Manages pending writes waiting for cloud durability acknowledgment.
//! Isolated from `WalActor` to reduce complexity and improve testability.
//!
//! SEMANTIC CONSTRAINTS (Phase 2 refactor - no behavior changes):
//! - Backpressure thresholds must remain identical (100k writes, 100MB)
//! - Timeout value must remain 30 seconds
//! - Eviction policy (LRU by enqueue time) unchanged
//! - Memory accounting must be byte-for-byte equivalent

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Maximum number of pending cloud writes before returning `WriteStall`
pub const MAX_PENDING_CLOUD_WRITES: usize = 100_000;

/// Approximate memory threshold for pending cloud writes (100MB)
pub const MAX_PENDING_CLOUD_WRITE_BYTES: usize = 100 * 1024 * 1024;

/// Maximum time to wait for cloud upload acknowledgment (30 seconds)
pub const CLOUD_UPLOAD_TIMEOUT: Duration = Duration::from_secs(30);

/// Pending write waiting for cloud durability
#[derive(Debug)]
pub enum PendingCloudWrite {
    Single {
        cf_id: crate::types::ColumnFamilyId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
        expiration: Option<u64>,
        enqueued_at: Instant,
    },
    DeleteRange {
        cf_id: crate::types::ColumnFamilyId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        sequence: u64,
        enqueued_at: Instant,
    },
    Transaction {
        commit_sequence: u64,
        ops: Vec<TransactionApplyOp>,
        enqueued_at: Instant,
    },
}

/// Transaction operation for cloud-pending batch
#[derive(Debug)]
pub enum TransactionApplyOp {
    Put {
        cf_id: crate::types::ColumnFamilyId,
        key: bytes::Bytes,
        value: bytes::Bytes,
        expiration: Option<u64>,
        sequence: u64,
    },
    Delete {
        cf_id: crate::types::ColumnFamilyId,
        key: bytes::Bytes,
        sequence: u64,
    },
    DeleteRange {
        cf_id: crate::types::ColumnFamilyId,
        start_key: bytes::Bytes,
        end_key: bytes::Bytes,
        sequence: u64,
    },
}

/// `CloudAsync` write queue with backpressure management
pub struct CloudWriteQueue {
    /// Queue of pending writes
    queue: VecDeque<PendingCloudWrite>,
    /// Approximate bytes in queue (for backpressure)
    pending_bytes: usize,
}

impl CloudWriteQueue {
    /// Create new empty queue
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            pending_bytes: 0,
        }
    }

    /// Check if backpressure should be applied
    pub fn should_apply_backpressure(&self) -> bool {
        self.queue.len() >= MAX_PENDING_CLOUD_WRITES
            || self.pending_bytes >= MAX_PENDING_CLOUD_WRITE_BYTES
    }

    /// Count timed-out writes exceeding `CLOUD_UPLOAD_TIMEOUT`
    pub fn count_timed_out_writes(&self) -> usize {
        let now = Instant::now();
        self.queue
            .iter()
            .filter(|pw| {
                let enqueued_at = match pw {
                    PendingCloudWrite::Single { enqueued_at, .. }
                    | PendingCloudWrite::DeleteRange { enqueued_at, .. }
                    | PendingCloudWrite::Transaction { enqueued_at, .. } => *enqueued_at,
                };
                now.duration_since(enqueued_at) > CLOUD_UPLOAD_TIMEOUT
            })
            .count()
    }

    /// Enqueue single write
    pub fn enqueue_write(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
        expiration: Option<u64>,
    ) {
        let estimated_bytes = key.len() + value.as_ref().map_or(0, std::vec::Vec::len) + 64;
        self.pending_bytes += estimated_bytes;

        self.queue.push_back(PendingCloudWrite::Single {
            cf_id,
            key,
            value,
            sequence,
            expiration,
            enqueued_at: Instant::now(),
        });

        tracing::trace!(
            sequence,
            pending_count = self.queue.len(),
            pending_bytes = self.pending_bytes,
            "Queued write for cloud durability"
        );
    }

    /// Enqueue delete range
    pub fn enqueue_delete_range(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        sequence: u64,
    ) {
        let estimated_bytes = start_key.len() + end_key.len() + 64;
        self.pending_bytes += estimated_bytes;

        self.queue.push_back(PendingCloudWrite::DeleteRange {
            cf_id,
            start_key,
            end_key,
            sequence,
            enqueued_at: Instant::now(),
        });

        tracing::trace!(
            sequence,
            pending_count = self.queue.len(),
            pending_bytes = self.pending_bytes,
            "Queued delete_range for cloud durability"
        );
    }

    /// Enqueue transaction
    pub fn enqueue_transaction(&mut self, commit_sequence: u64, ops: Vec<TransactionApplyOp>) {
        let batch_estimated_bytes: usize = ops
            .iter()
            .map(|op| match op {
                TransactionApplyOp::Put { key, value, .. } => key.len() + value.len() + 64,
                TransactionApplyOp::Delete { key, .. } => key.len() + 64,
                TransactionApplyOp::DeleteRange {
                    start_key, end_key, ..
                } => start_key.len() + end_key.len() + 64,
            })
            .sum();

        self.pending_bytes += batch_estimated_bytes;

        let op_count = ops.len();
        self.queue.push_back(PendingCloudWrite::Transaction {
            commit_sequence,
            ops,
            enqueued_at: Instant::now(),
        });

        tracing::trace!(
            commit_sequence,
            op_count,
            pending_count = self.queue.len(),
            pending_bytes = self.pending_bytes,
            "Queued transaction for cloud durability"
        );
    }

    /// Drain writes up to `cloud_durable_seq`
    /// Returns iterator of writes that became durable
    pub fn drain_until(&mut self, cloud_durable_seq: u64) -> Vec<PendingCloudWrite> {
        let mut drained = Vec::new();

        while let Some(pending) = self.queue.front() {
            let gate_seq = match pending {
                PendingCloudWrite::Single { sequence, .. } => *sequence,
                PendingCloudWrite::DeleteRange { sequence, .. } => *sequence,
                PendingCloudWrite::Transaction {
                    commit_sequence, ..
                } => *commit_sequence,
            };

            if gate_seq > cloud_durable_seq {
                break;
            }

            let pending = self.queue.pop_front().expect("front() returned Some");

            // Decrement pending bytes
            let dequeued_bytes = match &pending {
                PendingCloudWrite::Single { key, value, .. } => {
                    key.len() + value.as_ref().map_or(0, std::vec::Vec::len) + 64
                }
                PendingCloudWrite::DeleteRange {
                    start_key, end_key, ..
                } => start_key.len() + end_key.len() + 64,
                PendingCloudWrite::Transaction { ops, .. } => ops
                    .iter()
                    .map(|op| match op {
                        TransactionApplyOp::Put { key, value, .. } => key.len() + value.len() + 64,
                        TransactionApplyOp::Delete { key, .. } => key.len() + 64,
                        TransactionApplyOp::DeleteRange {
                            start_key, end_key, ..
                        } => start_key.len() + end_key.len() + 64,
                    })
                    .sum(),
            };

            self.pending_bytes = self.pending_bytes.saturating_sub(dequeued_bytes);
            drained.push(pending);
        }

        drained
    }

    /// Check if queue has pending writes
    pub fn has_pending_writes(&self) -> bool {
        !self.queue.is_empty()
    }

    /// Get current queue length
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Get pending bytes
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Check if a key exists in pending writes (for `insert_only` validation)
    pub fn contains_key(&self, cf_id: crate::types::ColumnFamilyId, key: &[u8]) -> bool {
        for pending in &self.queue {
            match pending {
                PendingCloudWrite::Single {
                    cf_id: p_cf,
                    key: p_key,
                    ..
                } => {
                    if *p_cf == cf_id && p_key.as_slice() == key {
                        return true;
                    }
                }
                PendingCloudWrite::DeleteRange { .. } => {
                    // DeleteRange isn't currently applied to memtable, so it doesn't affect
                    // insert-only existence checks.
                }
                PendingCloudWrite::Transaction { ops, .. } => {
                    for op in ops {
                        match op {
                            TransactionApplyOp::Put {
                                cf_id: p_cf,
                                key: p_key,
                                ..
                            }
                            | TransactionApplyOp::Delete {
                                cf_id: p_cf,
                                key: p_key,
                                sequence: _,
                            } => {
                                if *p_cf == cf_id && &p_key[..] == key {
                                    return true;
                                }
                            }
                            TransactionApplyOp::DeleteRange { .. } => {
                                // DeleteRange doesn't affect insert-only existence checks
                            }
                        }
                    }
                }
            }
        }
        false
    }

    /// Clear all pending writes (used on cloud upload failure)
    pub fn clear(&mut self) {
        self.queue.clear();
        self.pending_bytes = 0;
    }
}

impl Default for CloudWriteQueue {
    fn default() -> Self {
        Self::new()
    }
}
