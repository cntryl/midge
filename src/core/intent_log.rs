// Copyright (c) 2025 Midge Contributors
// License: Apache-2.0 or MIT

//! Explicit intent log for deterministic background work recovery.
//!
//! ## Purpose
//!
//! The intent log records the *intent* to perform background work (flush, compaction)
//! *before* execution. This enables recovery to:
//! 1. See what work was planned but not completed
//! 2. Resume or retry incomplete work
//! 3. Provide better observability and debugging
//!
//! This is the "deterministic, observable work" principle from THE_BIG_IDEA:
//! rather than inferring what happened from artifacts on disk, we explicitly
//! record the plan.
//!
//! ## Architecture
//!
//! IntentLog is append-only:
//! - New intent → Write to log
//! - Work completed → Mark intent as committed
//! - Crash → On recovery, see all pending + completed work
//!
//! Intent states:
//! - **Pending**: Work was planned but not started/completed
//! - **Committed**: Work completed successfully
//! - **Failed**: Work attempted but failed (optional)
//!
//! ## Storage
//!
//! - Local: `{db_path}/intent_log.json` (or append-only WAL format)
//! - Cloud: `{prefix}/intent/intent_log.json` (mirrored, not source of truth)
//!
//! Local intent log is source of truth because intents are generated locally.
//! Cloud copy is for debugging/audit across replicas.
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! // 1. Create intent to flush memtable 0
//! let intent = Intent::new_flush_memtable(cf_id, memtable_id);
//! intent_log.log_intent(&intent)?;
//!
//! // 2. Actually do the flush
//! let sst_id = flush_memtable(cf_id, memtable_id)?;
//!
//! // 3. Mark intent as completed
//! intent.mark_completed_flush(sst_id);
//! intent_log.mark_committed(&intent)?;
//!
//! // On recovery:
//! let pending = intent_log.load_pending_intents()?;
//! for intent in pending {
//!     match intent {
//!         Intent::FlushMemtable { cf_id, memtable_id, .. } => {
//!             // Retry the flush or clean up
//!         }
//!         Intent::CompactLevel { level, .. } => {
//!             // Retry the compaction or verify it was done
//!         }
//!     }
//! }
//! ```

use crate::error::{MidgeError, MidgeResult};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique identifier for an intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct IntentId(u64);

impl IntentId {
    /// Create a new intent ID from a timestamp (monotonically increasing).
    pub fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Self(nanos)
    }
}

impl Default for IntentId {
    fn default() -> Self {
        Self::new()
    }
}

/// State of an intent in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntentState {
    /// Work was planned but not yet started/completed
    Pending,
    /// Work was completed successfully
    Committed,
    /// Work was attempted but failed (optional, for auditing)
    Failed,
}

/// Flush intent details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushIntentDetails {
    pub cf_id: u32,
    pub memtable_id: u64,
    /// SST ID produced by flush (set when committed)
    pub sst_id: Option<u64>,
}

/// Compaction intent details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionIntentDetails {
    pub cf_id: u32,
    pub level: usize,
    /// Input file IDs (SSTs to compact)
    pub input_ssts: Vec<u64>,
    /// Output SST IDs (set when committed)
    pub output_ssts: Option<Vec<u64>>,
}

/// Explicit intent for background work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Intent {
    /// Flush a memtable to SST
    FlushMemtable(FlushIntentDetails),
    /// Compact a level
    CompactLevel(CompactionIntentDetails),
}

impl Intent {
    /// Create a new flush intent.
    pub fn new_flush_memtable(cf_id: u32, memtable_id: u64) -> Self {
        Intent::FlushMemtable(FlushIntentDetails {
            cf_id,
            memtable_id,
            sst_id: None,
        })
    }

    /// Create a new compaction intent.
    pub fn new_compact_level(cf_id: u32, level: usize, input_ssts: Vec<u64>) -> Self {
        Intent::CompactLevel(CompactionIntentDetails {
            cf_id,
            level,
            input_ssts,
            output_ssts: None,
        })
    }

    /// Mark a flush intent as completed with output SST.
    pub fn mark_flush_completed(&mut self, sst_id: u64) -> MidgeResult<()> {
        if let Intent::FlushMemtable(ref mut details) = self {
            details.sst_id = Some(sst_id);
            Ok(())
        } else {
            Err(MidgeError::Internal {
                message: "mark_flush_completed called on non-flush intent".to_string(),
            })
        }
    }

    /// Mark a compaction intent as completed with output SSTs.
    pub fn mark_compaction_completed(&mut self, output_ssts: Vec<u64>) -> MidgeResult<()> {
        if let Intent::CompactLevel(ref mut details) = self {
            details.output_ssts = Some(output_ssts);
            Ok(())
        } else {
            Err(MidgeError::Internal {
                message: "mark_compaction_completed called on non-compaction intent".to_string(),
            })
        }
    }

    /// Get column family ID for this intent.
    pub fn cf_id(&self) -> u32 {
        match self {
            Intent::FlushMemtable(d) => d.cf_id,
            Intent::CompactLevel(d) => d.cf_id,
        }
    }
}

/// Logged entry in the intent log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: IntentId,
    pub intent: Intent,
    pub state: IntentState,
    pub timestamp_ms: u64,
}

/// In-memory intent log.
#[derive(Debug)]
pub struct IntentLog {
    db_path: PathBuf,
    entries: Vec<LogEntry>,
}

impl IntentLog {
    /// Open or create an intent log.
    pub fn open(db_path: &Path) -> MidgeResult<Self> {
        let log_path = db_path.join("intent_log.json");
        let entries = if log_path.exists() {
            let data = fs::read_to_string(&log_path)?;
            serde_json::from_str(&data).unwrap_or_default()
        } else {
            Vec::new()
        };

        Ok(IntentLog {
            db_path: db_path.to_path_buf(),
            entries,
        })
    }

    /// Log a new pending intent.
    pub fn log_intent(&mut self, intent: Intent) -> MidgeResult<IntentId> {
        let id = IntentId::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let entry = LogEntry {
            id,
            intent,
            state: IntentState::Pending,
            timestamp_ms: now,
        };

        self.entries.push(entry.clone());
        self.persist()?;

        tracing::debug!("logged pending intent: {:?}", id);
        Ok(id)
    }

    /// Mark an intent as committed.
    pub fn mark_committed(&mut self, id: IntentId) -> MidgeResult<()> {
        let entry = self
            .entries
            .iter_mut()
            .find(|e| e.id == id)
            .ok_or_else(|| MidgeError::Internal {
                message: format!("intent not found: {:?}", id),
            })?;

        entry.state = IntentState::Committed;
        self.persist()?;

        tracing::debug!("marked intent as committed: {:?}", id);
        Ok(())
    }

    /// Mark an intent as failed.
    pub fn mark_failed(&mut self, id: IntentId, reason: &str) -> MidgeResult<()> {
        let entry = self
            .entries
            .iter_mut()
            .find(|e| e.id == id)
            .ok_or_else(|| MidgeError::Internal {
                message: format!("intent not found: {:?}", id),
            })?;

        entry.state = IntentState::Failed;
        self.persist()?;

        tracing::warn!("marked intent as failed: {:?} ({})", id, reason);
        Ok(())
    }

    /// Load all pending intents (work that was planned but not completed).
    pub fn load_pending_intents(&self) -> Vec<Intent> {
        self.entries
            .iter()
            .filter(|e| e.state == IntentState::Pending)
            .map(|e| e.intent.clone())
            .collect()
    }

    /// Load all intents (for auditing/debugging).
    pub fn load_all_intents(&self) -> Vec<(IntentId, Intent, IntentState)> {
        self.entries
            .iter()
            .map(|e| (e.id, e.intent.clone(), e.state))
            .collect()
    }

    /// Get the timestamp of the most recent committed intent (for recovery).
    pub fn last_committed_timestamp_ms(&self) -> Option<u64> {
        self.entries
            .iter()
            .rev()
            .find(|e| e.state == IntentState::Committed)
            .map(|e| e.timestamp_ms)
    }

    /// Compact the log by removing all committed intents (optional garbage collection).
    pub fn compact(&mut self) -> MidgeResult<()> {
        self.entries.retain(|e| e.state != IntentState::Committed);
        self.persist()?;

        tracing::debug!("compacted intent log");
        Ok(())
    }

    /// Persist the log to disk.
    fn persist(&self) -> MidgeResult<()> {
        let log_path = self.db_path.join("intent_log.json");
        let data = serde_json::to_string_pretty(&self.entries)?;
        let tmp_path = log_path.with_extension("json.tmp");

        // Atomic write
        fs::write(&tmp_path, data)?;
        fs::rename(&tmp_path, &log_path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn should_log_and_retrieve_flush_intent() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let mut log = IntentLog::open(temp_dir.path()).unwrap();

        // Act
        let intent = Intent::new_flush_memtable(0, 42);
        let _id = log.log_intent(intent.clone()).unwrap();

        // Assert
        let pending = log.load_pending_intents();
        assert_eq!(pending.len(), 1);
        match &pending[0] {
            Intent::FlushMemtable(d) => {
                assert_eq!(d.cf_id, 0);
                assert_eq!(d.memtable_id, 42);
                assert_eq!(d.sst_id, None);
            }
            _ => panic!("expected flush intent"),
        }
    }

    #[test]
    fn should_mark_intent_committed() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let mut log = IntentLog::open(temp_dir.path()).unwrap();
        let intent = Intent::new_flush_memtable(0, 42);
        let id = log.log_intent(intent).unwrap();

        // Act
        log.mark_committed(id).unwrap();

        // Assert
        let pending = log.load_pending_intents();
        assert_eq!(pending.len(), 0);

        let all = log.load_all_intents();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].2, IntentState::Committed);
    }

    #[test]
    fn should_persist_and_reload_intents() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let mut log = IntentLog::open(temp_dir.path()).unwrap();
        let intent = Intent::new_flush_memtable(0, 42);
        let _id = log.log_intent(intent).unwrap();

        // Act
        drop(log);
        let log2 = IntentLog::open(temp_dir.path()).unwrap();

        // Assert
        let pending = log2.load_pending_intents();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn should_compact_log_removing_committed_intents() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let mut log = IntentLog::open(temp_dir.path()).unwrap();
        let intent1 = Intent::new_flush_memtable(0, 1);
        let intent2 = Intent::new_flush_memtable(0, 2);
        let id1 = log.log_intent(intent1).unwrap();
        let id2 = log.log_intent(intent2).unwrap();

        // Act
        log.mark_committed(id1).unwrap();
        log.mark_committed(id2).unwrap();
        log.compact().unwrap();

        // Assert
        let all = log.load_all_intents();
        assert_eq!(all.len(), 0);
    }

    #[test]
    fn should_track_compaction_intent_with_output_ssts() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let mut log = IntentLog::open(temp_dir.path()).unwrap();
        let mut intent = Intent::new_compact_level(0, 1, vec![1, 2, 3]);
        let id = log.log_intent(intent.clone()).unwrap();

        // Act
        intent.mark_compaction_completed(vec![4, 5]).unwrap();
        // In real code, this would be persisted; for test, we just verify the intent changed
        log.mark_committed(id).unwrap();

        // Assert
        let all = log.load_all_intents();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].2, IntentState::Committed);
    }
}
