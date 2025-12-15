//! Runtime state - centralized mutable state owned by the runtime
//!
//! All engine state that can change at runtime lives here.
//! Actors read from and propose updates to this state.

use crate::common::MidgeResult;
use crate::metadata::Manifest;
use crate::runtime::IntentLogEntry;
use crate::sst::{ReadAmpMetrics, SkipListMemtable};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Column family state
pub struct ColumnFamilyState {
    pub id: u32,
    pub name: String,
    pub memtable: Arc<SkipListMemtable>,
    /// Immutable memtables waiting to be flushed
    pub immutable_memtables: Vec<Arc<SkipListMemtable>>,
    /// Merge operator for this CF (if registered)
    pub merge_operator: Option<std::sync::Arc<dyn crate::engine::MergeOperator>>,
}

impl ColumnFamilyState {
    pub fn new(id: u32, name: String) -> Self {
        Self {
            id,
            name,
            memtable: Arc::new(SkipListMemtable::new()),
            immutable_memtables: Vec::new(),
            merge_operator: None,
        }
    }
}

/// WAL state
pub struct WalState {
    /// Current WAL segment ID
    pub current_segment_id: u64,
    /// Last synced sequence number (local durability)
    pub last_synced_seq: u64,
    /// Pending writes waiting for sync
    pub pending_writes: usize,
    /// Local durability frontier - highest sequence number fsynced locally
    pub local_durable_seq: u64,
    /// Cloud durability frontier - highest sequence number confirmed by cloud
    pub cloud_durable_seq: u64,
}

impl Default for WalState {
    fn default() -> Self {
        Self {
            current_segment_id: 1,
            last_synced_seq: 0,
            pending_writes: 0,
            local_durable_seq: 0,
            cloud_durable_seq: 0,
        }
    }
}

/// Compaction state
#[derive(Default)]
pub struct CompactionState {
    /// SSTs currently being compacted (locked from other compactions)
    pub compacting_ssts: Vec<String>,
    /// Pending compaction tasks
    pub pending_tasks: usize,
}

/// Cloud sync state
#[derive(Default)]
pub struct CloudState {
    /// SSTs pending upload
    pub pending_uploads: Vec<String>,
    /// Last checkpoint sequence uploaded to cloud
    pub last_cloud_checkpoint_seq: u64,
}

/// Centralized runtime state
///
/// This is the single source of truth for all mutable engine state.
/// The runtime owns this and actors propose updates via messages.
///
/// Important:
/// - This type does NOT handle per-request routing.
/// - Response routing is handled exclusively by ResponseRouter
///   (shared between RuntimeHandle and EventLoop).
pub struct RuntimeState {
    // === Paths ===
    pub db_path: PathBuf,
    pub wal_dir: PathBuf,
    pub sst_dir: PathBuf,

    // === Sequence Numbers ===
    /// Global monotonic sequence number
    pub sequence: u64,
    /// Next transaction ID
    pub next_txn_id: u64,

    // === Column Families ===
    pub column_families: HashMap<u32, ColumnFamilyState>,

    // === Metadata ===
    pub manifest: Manifest,

    // === Subsystem State ===
    pub wal: WalState,
    pub compaction: CompactionState,
    pub cloud: CloudState,

    // === Configuration ===
    pub memtable_size_limit: usize,
    pub read_only: bool,
    /// If true, never touch filesystem (pure in-memory mode)
    pub memory_mode: bool,

    // === Intent Log & Determinism ===
    /// Deterministic intent log for recovery and replay
    pub intent_log: Vec<IntentLogEntry>,
    /// Maximum size of any memtable before write stall
    pub memtable_flush_threshold: usize,
    /// Write stall active flag
    pub write_stalled: bool,
    /// Total size of all memtables (in-memory)
    pub total_memtable_bytes: usize,

    // === Observability ===
    /// Read amplification metrics across all reads
    pub read_amp_metrics: ReadAmpMetrics,
}

impl RuntimeState {
    /// Create new runtime state with the given database path.
    /// If memory_mode is true, filesystem is never touched.
    pub fn new(db_path: PathBuf, memory_mode: bool) -> Self {
        Self::new_with_recovery_dir(db_path, memory_mode, None)
    }

    /// Create new runtime state with an optional override for WAL recovery.
    ///
    /// When `recovery_wal_dir` is provided, recovery replays WAL from that
    /// directory (instead of `db_path/wal`). This is used for CloudFirst mode
    /// where cloud WAL is the source of truth.
    pub fn new_with_recovery_dir(
        db_path: PathBuf,
        memory_mode: bool,
        recovery_wal_dir: Option<PathBuf>,
    ) -> Self {
        // Only touch filesystem if not in memory mode
        if !memory_mode {
            if let Err(e) = std::fs::create_dir_all(&db_path) {
                tracing::warn!(error = %e, path = ?db_path, "failed to create database directory");
            }
        }

        let wal_dir = db_path.join("wal");
        if !memory_mode {
            if let Err(e) = std::fs::create_dir_all(&wal_dir) {
                tracing::warn!(error = %e, path = ?wal_dir, "failed to create WAL directory");
            }
        }

        let sst_dir = db_path.join("sst");
        if !memory_mode {
            if let Err(e) = std::fs::create_dir_all(&sst_dir) {
                tracing::warn!(error = %e, path = ?sst_dir, "failed to create SST directory");
            }
        }

        // Load manifest (only if not in memory mode)
        let manifest = if !memory_mode {
            match crate::metadata::ManifestPersistence::load(&db_path) {
                Ok(m) => {
                    tracing::info!("manifest loaded from disk");
                    m
                }
                Err(e) => {
                    tracing::warn!("failed to load manifest, using default: {}", e);
                    Manifest::default()
                }
            }
        } else {
            Manifest::default()
        };

        // Load intent log if present
        let intent_log = if !memory_mode {
            match crate::runtime::IntentPersistence::load(&db_path) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to load intent log, starting empty");
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        let mut column_families = HashMap::new();
        column_families.insert(0, ColumnFamilyState::new(0, "default".into()));

        for cf_meta in &manifest.column_families {
            // Skip deleted column families and default CF (already added)
            if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                column_families.insert(
                    cf_meta.id,
                    ColumnFamilyState::new(cf_meta.id, cf_meta.name.clone()),
                );
            }
        }

        // WAL recovery (skip in memory mode)
        let replay_dir = recovery_wal_dir.as_deref().unwrap_or(&wal_dir);
        let recovered_sequence = if !memory_mode && replay_dir.exists() {
            let mut recovery_memtables = HashMap::new();
            match crate::storage::LocalFsStorage::new(replay_dir) {
                Ok(storage) => match crate::wal::recovery::replay_wal(
                    &storage,
                    &crate::storage::abstraction::StoragePath::new(""),
                    &mut recovery_memtables,
                ) {
                Ok(stats) => {
                    tracing::info!(
                        records_recovered = stats.record_count,
                        bytes_recovered = stats.bytes,
                        max_sequence = ?stats.max_sequence,
                        replay_dir = ?replay_dir,
                        "WAL recovery completed successfully"
                    );
                    for (cf_id, recovered_memtable) in recovery_memtables {
                        if let Some(cf_state) = column_families.get_mut(&cf_id) {
                            cf_state.memtable = recovered_memtable;
                        } else {
                            let name = format!("cf_{}", cf_id);
                            let mut cf_state = ColumnFamilyState::new(cf_id, name);
                            cf_state.memtable = recovered_memtable;
                            column_families.insert(cf_id, cf_state);
                        }
                    }
                    // Restore sequence counter from recovery
                    stats.max_sequence.unwrap_or(0)
                }
                Err(e) => {
                    tracing::error!(error = %e, "WAL recovery failed, continuing without recovered state");
                    0
                }
                },
                Err(e) => {
                    tracing::error!(error = %e, "failed to initialize WAL recovery storage");
                    0
                }
            }
        } else {
            0
        };

        // WAL segment id recovery:
        // On restart, we must continue segment ids beyond the highest existing rotated segment
        // to avoid overwriting previously-uploaded cloud WAL segments (CloudFirst) or local
        // WAL segments (LocalDisk). The source of truth is the replay directory.
        let recovered_next_segment_id = if !memory_mode && replay_dir.exists() {
            let mut max_segment_id: u64 = 0;

            if let Ok(entries) = std::fs::read_dir(replay_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) != Some("wal") {
                        continue;
                    }
                    let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                        continue;
                    };

                    // Skip the active WAL file name if it happens to match the extension.
                    if stem.eq_ignore_ascii_case("wal") {
                        continue;
                    }

                    if let Ok(id) = stem.parse::<u64>() {
                        max_segment_id = max_segment_id.max(id);
                    }
                }
            }

            // Segment ids start at 1.
            max_segment_id.saturating_add(1).max(1)
        } else {
            1
        };

        Self {
            db_path,
            wal_dir,
            sst_dir,
            sequence: recovered_sequence,
            next_txn_id: 0,
            column_families,
            manifest,
            wal: WalState {
                current_segment_id: recovered_next_segment_id,
                ..WalState::default()
            },
            compaction: CompactionState::default(),
            cloud: CloudState::default(),
            memtable_size_limit: 64 * 1024 * 1024, // 64MB
            read_only: false,
            memory_mode,
            intent_log,
            memtable_flush_threshold: 64 * 1024 * 1024, // 64MB
            write_stalled: false,
            total_memtable_bytes: 0,
            read_amp_metrics: ReadAmpMetrics::new(),
        }
    }

    pub fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// Get the next transaction ID
    pub fn next_txn_id(&mut self) -> u64 {
        self.next_txn_id += 1;
        self.next_txn_id
    }

    /// Append an intent entry and persist the intent log to disk.
    pub fn append_intent(&mut self, entry: crate::runtime::IntentLogEntry) -> MidgeResult<()> {
        self.intent_log.push(entry);
        // Persist intent log unless running in memory mode
        if !self.memory_mode {
            crate::runtime::IntentPersistence::save(&self.db_path, &self.intent_log)
                .map_err(crate::common::MidgeError::Internal)?;
        }
        Ok(())
    }

    pub fn get_cf(&self, cf_id: u32) -> Option<&ColumnFamilyState> {
        self.column_families.get(&cf_id)
    }

    pub fn get_cf_mut(&mut self, cf_id: u32) -> Option<&mut ColumnFamilyState> {
        self.column_families.get_mut(&cf_id)
    }

    pub fn create_cf(&mut self, name: String) -> MidgeResult<u32> {
        let id = self.column_families.len() as u32;
        self.column_families
            .insert(id, ColumnFamilyState::new(id, name));
        Ok(id)
    }

    pub fn needs_flush(&self) -> Option<u32> {
        for (cf_id, cf) in &self.column_families {
            // Use the Memtable trait method
            if crate::sst::Memtable::size_bytes(cf.memtable.as_ref()) >= self.memtable_size_limit {
                return Some(*cf_id);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========== ColumnFamilyState Tests ===========

    #[test]
    fn should_create_column_family_with_unique_id() {
        // Arrange
        let id = 42;
        let name = "test_cf".to_string();

        // Act
        let cf = ColumnFamilyState::new(id, name.clone());

        // Assert
        assert_eq!(cf.id, 42);
        assert_eq!(cf.name, "test_cf");
        assert!(cf.merge_operator.is_none());
        assert!(cf.immutable_memtables.is_empty());
    }

    #[test]
    fn should_track_immutable_memtables_in_cf_state() {
        // Arrange
        let mut cf = ColumnFamilyState::new(1, "cf".to_string());
        let imm_memtable = Arc::new(SkipListMemtable::new());

        // Act
        cf.immutable_memtables.push(imm_memtable.clone());
        cf.immutable_memtables.push(imm_memtable.clone());

        // Assert
        assert_eq!(cf.immutable_memtables.len(), 2);
    }

    // =========== WalState Tests ===========

    #[test]
    fn should_initialize_wal_state_with_defaults() {
        // Arrange & Act
        let wal = WalState::default();

        // Assert
        assert_eq!(wal.current_segment_id, 1);
        assert_eq!(wal.last_synced_seq, 0);
        assert_eq!(wal.pending_writes, 0);
        assert_eq!(wal.local_durable_seq, 0);
        assert_eq!(wal.cloud_durable_seq, 0);
    }

    #[test]
    fn should_maintain_wal_durability_frontiers() {
        // Arrange
        let wal = WalState {
            last_synced_seq: 10,
            local_durable_seq: 10,
            pending_writes: 5,
            cloud_durable_seq: 8,
            ..Default::default()
        };

        // Assert - Verify monotonicity constraints
        assert!(wal.cloud_durable_seq <= wal.local_durable_seq);
        assert!(wal.local_durable_seq >= wal.last_synced_seq);
        assert!(wal.pending_writes < u64::MAX as usize);
    }

    #[test]
    fn should_track_segment_rotation() {
        // Arrange
        let mut wal = WalState::default();
        let initial_segment = wal.current_segment_id;

        // Act
        wal.current_segment_id += 1;
        wal.current_segment_id += 1;

        // Assert
        assert_eq!(wal.current_segment_id, initial_segment + 2);
    }

    // =========== CompactionState Tests ===========

    #[test]
    fn should_initialize_compaction_state() {
        // Arrange & Act
        let compaction = CompactionState::default();

        // Assert
        assert!(compaction.compacting_ssts.is_empty());
        assert_eq!(compaction.pending_tasks, 0);
    }

    #[test]
    fn should_track_compacting_ssts() {
        // Arrange
        let mut compaction = CompactionState::default();

        // Act
        compaction.compacting_ssts.push("sst_001.sst".to_string());
        compaction.compacting_ssts.push("sst_002.sst".to_string());
        compaction.pending_tasks = 2;

        // Assert
        assert_eq!(compaction.compacting_ssts.len(), 2);
        assert_eq!(compaction.pending_tasks, 2);
    }

    // =========== CloudState Tests ===========

    #[test]
    fn should_initialize_cloud_state() {
        // Arrange & Act
        let cloud = CloudState::default();

        // Assert
        assert!(cloud.pending_uploads.is_empty());
        assert_eq!(cloud.last_cloud_checkpoint_seq, 0);
    }

    #[test]
    fn should_track_pending_uploads() {
        // Arrange
        let mut cloud = CloudState::default();

        // Act
        cloud.pending_uploads.push("sst_001.sst".to_string());
        cloud.last_cloud_checkpoint_seq = 100;

        // Assert
        assert_eq!(cloud.pending_uploads.len(), 1);
        assert_eq!(cloud.last_cloud_checkpoint_seq, 100);
    }

    // =========== RuntimeState Tests ===========

    #[test]
    fn should_create_runtime_state_in_memory_mode() {
        // Arrange & Act
        let state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Assert
        assert!(state.memory_mode);
        assert_eq!(state.sequence, 0);
        assert_eq!(state.next_txn_id, 0);
        assert!(state.column_families.contains_key(&0)); // Default CF
        assert_eq!(state.column_families.len(), 1);
    }

    #[test]
    fn should_initialize_default_column_family() {
        // Arrange & Act
        let state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Assert
        let cf0 = state.get_cf(0).expect("Default CF should exist");
        assert_eq!(cf0.id, 0);
        assert_eq!(cf0.name, "default");
    }

    #[test]
    fn should_increment_sequence_numbers_monotonically() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        let initial = state.sequence;

        // Act
        let seq1 = state.next_sequence();
        let seq2 = state.next_sequence();
        let seq3 = state.next_sequence();

        // Assert
        assert_eq!(seq1, initial + 1);
        assert_eq!(seq2, initial + 2);
        assert_eq!(seq3, initial + 3);
        assert!(seq1 < seq2 && seq2 < seq3);
    }

    #[test]
    fn should_increment_transaction_ids_monotonically() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        let txn1 = state.next_txn_id();
        let txn2 = state.next_txn_id();
        let txn3 = state.next_txn_id();

        // Assert
        assert_eq!(txn1, 1);
        assert_eq!(txn2, 2);
        assert_eq!(txn3, 3);
        assert!(txn1 < txn2 && txn2 < txn3);
    }

    #[test]
    fn should_create_new_column_family() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        let cf_id = state
            .create_cf("test_cf".to_string())
            .expect("create_cf should succeed");

        // Assert
        assert_eq!(cf_id, 1); // After default (0)
        assert!(state.column_families.contains_key(&cf_id));
        let cf = state.get_cf(cf_id).expect("Created CF should exist");
        assert_eq!(cf.name, "test_cf");
    }

    #[test]
    fn should_get_column_family_by_id() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state
            .create_cf("my_cf".to_string())
            .expect("create_cf should succeed");

        // Act
        let cf = state.get_cf(1);

        // Assert
        assert!(cf.is_some());
        assert_eq!(cf.unwrap().name, "my_cf");
    }

    #[test]
    fn should_return_none_for_nonexistent_column_family() {
        // Arrange
        let state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act & Assert
        assert!(state.get_cf(999).is_none());
    }

    #[test]
    fn should_get_mutable_column_family() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state
            .create_cf("mutable_cf".to_string())
            .expect("create_cf should succeed");

        // Act
        {
            let cf_mut = state.get_cf_mut(1).expect("get_cf_mut should succeed");
            cf_mut
                .immutable_memtables
                .push(Arc::new(SkipListMemtable::new()));
        }

        // Assert
        let cf = state.get_cf(1).expect("CF should exist");
        assert_eq!(cf.immutable_memtables.len(), 1);
    }

    #[test]
    fn should_load_intent_log_on_startup() {
        // Arrange: write an intent file to the test dir and then create RuntimeState
        let test_dir = std::env::temp_dir().join("midge_state_intent_test");
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).expect("create test dir");

        let intents = vec![crate::runtime::IntentLogEntry::WalSynced {
            segment_id: 2,
            seqno: 99,
        }];
        crate::runtime::IntentPersistence::save(&test_dir, &intents).expect("save intents");

        // Act: create runtime state for that path (not memory mode)
        let state = RuntimeState::new(test_dir.clone(), false);

        // Assert: intent log was loaded
        assert!(
            !state.intent_log.is_empty(),
            "intent log should be loaded from disk"
        );
        match &state.intent_log[0] {
            crate::runtime::IntentLogEntry::WalSynced { segment_id, seqno } => {
                assert_eq!(*segment_id, 2);
                assert_eq!(*seqno, 99);
            }
            _ => panic!("unexpected intent variant"),
        }
    }

    #[test]
    fn should_track_wal_state_separately() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        state.wal.current_segment_id = 5;
        state.wal.pending_writes = 10;

        // Assert
        assert_eq!(state.wal.current_segment_id, 5);
        assert_eq!(state.wal.pending_writes, 10);
    }

    #[test]
    fn should_track_compaction_state_separately() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        state
            .compaction
            .compacting_ssts
            .push("sst_001.sst".to_string());
        state.compaction.pending_tasks = 3;

        // Assert
        assert_eq!(state.compaction.compacting_ssts.len(), 1);
        assert_eq!(state.compaction.pending_tasks, 3);
    }

    #[test]
    fn should_track_cloud_state_separately() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        state
            .cloud
            .pending_uploads
            .push("sst_remote.sst".to_string());
        state.cloud.last_cloud_checkpoint_seq = 50;

        // Assert
        assert_eq!(state.cloud.pending_uploads.len(), 1);
        assert_eq!(state.cloud.last_cloud_checkpoint_seq, 50);
    }

    #[test]
    fn should_maintain_memtable_size_limit() {
        // Arrange
        let state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Assert
        assert!(state.memtable_size_limit > 0);
        assert_eq!(state.memtable_size_limit, 64 * 1024 * 1024); // 64MB
    }

    #[test]
    fn should_respect_read_only_flag() {
        // Arrange & Act
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Assert - Initially not read-only
        assert!(!state.read_only);

        // Act - Set read-only
        state.read_only = true;

        // Assert
        assert!(state.read_only);
    }

    #[test]
    fn should_handle_multiple_column_families() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        let cf1 = state
            .create_cf("cf1".to_string())
            .expect("create_cf should succeed");
        let cf2 = state
            .create_cf("cf2".to_string())
            .expect("create_cf should succeed");
        let cf3 = state
            .create_cf("cf3".to_string())
            .expect("create_cf should succeed");

        // Assert
        assert_eq!(state.column_families.len(), 4); // default + 3 created
        assert!(state.get_cf(cf1).is_some());
        assert!(state.get_cf(cf2).is_some());
        assert!(state.get_cf(cf3).is_some());
    }

    #[test]
    fn should_track_all_state_components_independently() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        let seq1 = state.next_sequence();
        let txn1 = state.next_txn_id();
        state.wal.pending_writes = 5;
        state.compaction.pending_tasks = 2;
        state.cloud.last_cloud_checkpoint_seq = 100;

        // Assert
        assert_eq!(seq1, 1);
        assert_eq!(txn1, 1);
        assert_eq!(state.wal.pending_writes, 5);
        assert_eq!(state.compaction.pending_tasks, 2);
        assert_eq!(state.cloud.last_cloud_checkpoint_seq, 100);
    }
}
