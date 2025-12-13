//! Fast path write operations
//!
//! This module provides direct write access bypassing actor request-response.
//!
//! Architecture:
//! - FastPathState holds Arc to memtables + shared WAL writer
//! - Writes go to WAL FIRST, then memtable (preserves correctness)
//! - No actor await (no request-response overhead)
//! - Sequence numbers allocated atomically
//!
//! CRITICAL: WAL-first ordering is NON-NEGOTIABLE.
//! Write order: WAL append → WAL sync → memtable update → return
//!
//! Key insight: We can avoid actor request-response while still
//! maintaining correctness by writing directly to shared resources.

use crate::common::{MidgeError, MidgeResult};
use crate::sst::{Memtable, SkipListMemtable};
use crate::wal::{DurabilityPolicy, FsWalFactory, WalFactory, WalOpKind, WalRecord, WalWriter};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc, Mutex};

/// Shared state for fast-path writes
///
/// Provides direct WAL + memtable access for hot-path operations.
/// Preserves WAL-first ordering while avoiding actor overhead.
#[derive(Clone)]
pub struct FastPathState {
    /// Atomic sequence number allocator
    sequence: Arc<AtomicU64>,
    /// WAL writer (Mutex for exclusive write access)
    wal_writer: Arc<Mutex<Option<Box<dyn WalWriter>>>>,
    /// Memtables by CF ID (thread-safe via Arc)
    memtables: Arc<Mutex<HashMap<u32, Arc<SkipListMemtable>>>>,
    /// Durability policy
    durability_policy: DurabilityPolicy,
    /// Memory mode flag
    memory_mode: bool,
}

impl FastPathState {
    /// Create new fast path state (package-private - created by Runtime)
    pub(crate) fn new(
        wal_dir: PathBuf,
        durability_policy: DurabilityPolicy,
        memory_mode: bool,
        initial_sequence: u64,
    ) -> MidgeResult<Self> {
        let wal_writer = if memory_mode {
            None
        } else {
            std::fs::create_dir_all(&wal_dir).map_err(MidgeError::Io)?;
            let factory = FsWalFactory;
            Some(factory.create_writer(&wal_dir)?)
        };

        Ok(Self {
            sequence: Arc::new(AtomicU64::new(initial_sequence)),
            wal_writer: Arc::new(Mutex::new(wal_writer)),
            memtables: Arc::new(Mutex::new(HashMap::new())),
            durability_policy,
            memory_mode,
        })
    }

    /// Register a memtable for a column family (called by EventLoop during setup)
    pub(crate) fn register_memtable(&self, cf_id: u32, memtable: Arc<SkipListMemtable>) {
        let mut tables = self
            .memtables
            .lock()
            .expect("FastPathState memtables lock poisoned");
        tables.insert(cf_id, memtable);
    }

    /// Allocate next sequence number
    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }

    /// Fast path put - WAL-first ordering preserved
    ///
    /// Order: WAL append → WAL sync → memtable update → return
    /// No actor await (bypasses request-response overhead)
    pub fn put(
        &self,
        cf_id: u32,
        key: &[u8],
        value: &[u8],
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<u64> {
        let sequence = self.next_sequence();

        // Create WAL record
        let record = match ttl_seconds {
            Some(ttl) if ttl > 0 => WalRecord::new_with_ttl(
                cf_id,
                WalOpKind::Put,
                Bytes::copy_from_slice(key),
                Some(Bytes::copy_from_slice(value)),
                sequence,
                ttl,
            ),
            _ => WalRecord::new_cf(
                cf_id,
                WalOpKind::Put,
                Bytes::copy_from_slice(key),
                Some(Bytes::copy_from_slice(value)),
                sequence,
            ),
        };

        // STEP 1: Write to WAL (durability)
        if !self.memory_mode {
            let mut writer_guard = self
                .wal_writer
                .lock()
                .expect("FastPathState WAL writer lock poisoned");
            if let Some(writer) = writer_guard.as_mut() {
                writer.append_record(&record)?;

                // STEP 2: Sync if strict durability
                if matches!(self.durability_policy, DurabilityPolicy::Strict) {
                    writer.sync()?;
                }
            }
        }

        // STEP 3: Apply to memtable (visibility) - AFTER WAL
        let tables = self
            .memtables
            .lock()
            .expect("FastPathState memtables lock poisoned");
        if let Some(memtable) = tables.get(&cf_id) {
            memtable.put_with_exp(key.to_vec(), value.to_vec(), record.expiration)?;
        } else {
            return Err(MidgeError::InvalidArgument(format!(
                "column family {} not found",
                cf_id
            )));
        }

        Ok(sequence)
    }

    /// Fast path delete - WAL-first ordering preserved
    pub fn delete(&self, cf_id: u32, key: &[u8]) -> MidgeResult<u64> {
        let sequence = self.next_sequence();

        // Create WAL record
        let record = WalRecord::new_cf(
            cf_id,
            WalOpKind::Delete,
            Bytes::copy_from_slice(key),
            None,
            sequence,
        );

        // STEP 1: Write to WAL (durability)
        if !self.memory_mode {
            let mut writer_guard = self
                .wal_writer
                .lock()
                .expect("FastPathState WAL writer lock poisoned");
            if let Some(writer) = writer_guard.as_mut() {
                writer.append_record(&record)?;

                // STEP 2: Sync if strict durability
                if matches!(self.durability_policy, DurabilityPolicy::Strict) {
                    writer.sync()?;
                }
            }
        }

        // STEP 3: Apply to memtable (visibility) - AFTER WAL
        let tables = self
            .memtables
            .lock()
            .expect("FastPathState memtables lock poisoned");
        if let Some(memtable) = tables.get(&cf_id) {
            memtable.delete(key.to_vec())?;
        } else {
            return Err(MidgeError::InvalidArgument(format!(
                "column family {} not found",
                cf_id
            )));
        }

        Ok(sequence)
    }

    /// Get current sequence number (for observability)
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }
}
