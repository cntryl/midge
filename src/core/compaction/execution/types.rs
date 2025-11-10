///! Core data types for compaction execution.

use bytes::Bytes;

/// A single version of a key collected during compaction.
///
/// Multiple versions may exist for the same user key at different sequence numbers.
/// During compaction, these are merged according to the LSM merge semantics
/// (newer versions shadow older ones).
#[derive(Debug, Clone)]
pub struct CompactionVersion {
    pub user_key: Vec<u8>,
    pub seq: u64,
    pub tombstone: bool,
    pub value: Option<Bytes>,
    pub expiration: Option<u64>, // TTL: Unix milliseconds when key expires
}
