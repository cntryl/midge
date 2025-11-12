//! WAL replay logic for database recovery
//!
//! This module handles replaying WAL records into memtables during recovery.
//! It depends on `core::memtable` and belongs in the core engine orchestration,
//! not in the WAL subsystem itself.

use crate::core::memtable::MemTable;
use crate::wal::{WalOpKind, WalRecord};

/// Replay WAL records into the appropriate column families based on cf_id.
///
/// This function is called during database recovery to restore state from the WAL.
/// Records for non-existent column families (e.g., dropped CFs) are silently ignored.
///
/// Transactions are handled atomically: records belonging to a transaction are
/// buffered until a TxnCommit is found. If no matching commit is found, the
/// entire transaction is discarded (crash atomicity).
///
/// # Arguments
///
/// * `cf_map` - Map of column family ID to memtable (uses shared refs due to interior mutability)
/// * `records` - Slice of WAL records to replay
///
/// # Returns
///
/// Maximum sequence number seen during replay
#[allow(dead_code)] // Used in tests
pub(crate) fn replay_wal_to_memtables(
    cf_map: &mut std::collections::HashMap<u32, &MemTable>,
    records: &[WalRecord],
) -> u64 {
    replay_wal_to_memtables_after_seq(cf_map, records, 0)
}

/// Replay WAL records to memtables, skipping records with sequence <= skip_before_seq.
/// This is used during recovery to avoid replaying records that were already flushed to SST.
pub(crate) fn replay_wal_to_memtables_after_seq(
    cf_map: &mut std::collections::HashMap<u32, &MemTable>,
    records: &[WalRecord],
    skip_before_seq: u64,
) -> u64 {
    use std::collections::HashMap;

    let mut max_seq = 0u64;
    let mut txn_buffers: HashMap<u64, Vec<WalRecord>> = HashMap::new();
    let mut committed_txns: std::collections::HashSet<u64> = std::collections::HashSet::new();

    // First pass: identify committed transactions
    for rec in records {
        // Skip records already persisted. Special case: skip_before_seq=0 means nothing persisted yet
        // (since sequences start at 0), so don't skip anything. Otherwise skip rec.seq <= skip_before_seq.
        if skip_before_seq > 0 && rec.seq <= skip_before_seq {
            continue;
        }

        if rec.op == WalOpKind::TxnCommit {
            if let Some(txn_id) = rec.txn_id {
                committed_txns.insert(txn_id);
            }
        }
    }

    // Second pass: buffer transactional ops and apply committed ones
    for rec in records {
        // Skip records already persisted. Special case: skip_before_seq=0 means nothing persisted yet
        // (since sequences start at 0), so don't skip anything. Otherwise skip rec.seq <= skip_before_seq.
        if skip_before_seq > 0 && rec.seq <= skip_before_seq {
            continue;
        }

        max_seq = max_seq.max(rec.seq);

        match rec.op {
            WalOpKind::TxnBegin => {
                // Initialize buffer for this transaction
                if let Some(txn_id) = rec.txn_id {
                    txn_buffers.entry(txn_id).or_default();
                }
            }
            WalOpKind::TxnCommit => {
                // Apply buffered ops for this transaction if they exist
                if let Some(txn_id) = rec.txn_id {
                    if let Some(buffered) = txn_buffers.remove(&txn_id) {
                        for op_rec in buffered {
                            apply_record_to_memtable(cf_map, op_rec);
                        }
                    }
                }
            }
            _ => {
                // Regular op: check if it's part of a transaction
                if let Some(txn_id) = rec.txn_id {
                    // Buffer if part of a transaction (clone needed as we're taking from slice)
                    txn_buffers.entry(txn_id).or_default().push(rec.clone());
                } else {
                    // Non-transactional: apply immediately (clone needed as we're taking from slice)
                    apply_record_to_memtable(cf_map, rec.clone());
                }
            }
        }
    }

    max_seq
}

/// Apply a single WAL record to the appropriate memtable.
fn apply_record_to_memtable(
    cf_map: &mut std::collections::HashMap<u32, &MemTable>,
    rec: WalRecord,
) {
    let cf_id = rec.column_family_id().as_u32();

    // Get the CF memtable by ID, skip if CF doesn't exist (was dropped)
    if let Some(memtable) = cf_map.get(&cf_id) {
        match rec.op {
            WalOpKind::Put | WalOpKind::Insert => {
                if let Some(value) = rec.value {
                    memtable.put_with_seq_and_exp(
                        rec.key.as_ref(),
                        value.as_ref(),
                        rec.seq,
                        rec.expiration,
                    );
                }
            }
            WalOpKind::Delete => {
                memtable.delete_with_seq(rec.key.as_ref(), rec.seq);
            }
            WalOpKind::DeleteRange => {
                if let Some(range_end) = rec.range_end {
                    memtable.delete_range_with_seq(rec.key.as_ref(), range_end.as_ref(), rec.seq);
                }
            }
            WalOpKind::Merge => {
                // Merge operations are stored like Put in the memtable
                // Resolution happens at read time
                if let Some(value) = rec.value {
                    memtable.put_with_seq_and_exp(
                        rec.key.as_ref(),
                        value.as_ref(),
                        rec.seq,
                        rec.expiration,
                    );
                }
            }
            WalOpKind::TxnBegin | WalOpKind::TxnCommit => {
                // Markers are not applied to memtable
            }
        }
    }
}

/// WAL replay iterator for database recovery.
pub struct WalReplayIterator;

/// Compute the encoded length of a WAL record in the FS WAL format.
///
/// This is used to predict when a WAL buffer will fill up and needs rotation.
///
/// WAL format v4:
/// ```text
/// CRC32(4) + type(1) + cf_id(4) + seq(8) + varint(keylen) + key + varint(vallen) + val
/// For DeleteRange: also includes varint(range_end_len) + range_end
/// ```
///
/// # Arguments
/// * `kind` - The operation kind (Put, Delete, DeleteRange, etc.)
/// * `key_len` - Length of the key in bytes (or range start for DeleteRange)
/// * `val_len` - Optional length of the value in bytes
/// * `range_end_len` - Optional length of range_end for DeleteRange operations
///
/// # Returns
/// Total encoded size in bytes
pub(crate) fn wal_record_encoded_len(
    kind: WalOpKind,
    key_len: usize,
    val_len: Option<usize>,
    range_end_len: Option<usize>,
) -> u64 {
    let _ = kind; // kind is 1 byte
    let vlen = val_len.unwrap_or(0);
    let mut size = 4
        + 1
        + 4
        + 8
        + varint_len_u32(key_len as u32)
        + key_len
        + varint_len_u32(vlen as u32)
        + vlen;

    // Add range_end if present (for DeleteRange)
    if let Some(rend_len) = range_end_len {
        size += varint_len_u32(rend_len as u32) + rend_len;
    }

    size as u64
}

/// Calculate the encoded length of a u32 varint.
///
/// Varint encoding uses 7 bits per byte with the high bit as a continuation flag.
/// This determines how many bytes will be needed to encode a given integer.
///
/// # Arguments
/// * `v` - The u32 value to encode
///
/// # Returns
/// Number of bytes needed (1-5)
fn varint_len_u32(mut v: u32) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        n += 1;
        v >>= 7;
    }
    n
}
