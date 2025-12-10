//! Runtime state - centralized mutable state owned by the runtime
//!
//! All engine state that can change at runtime lives here.
//! Actors read from and propose updates to this state.

use crate::common::MidgeResult;
use crate::metadata::Manifest;
use crate::sst::SkipListMemtable;
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
}

impl ColumnFamilyState {
    pub fn new(id: u32, name: String) -> Self {
        Self {
            id,
            name,
            memtable: Arc::new(SkipListMemtable::new()),
            immutable_memtables: Vec::new(),
        }
    }
}

/// WAL state
pub struct WalState {
    /// Current WAL segment ID
    pub current_segment_id: u64,
    /// Last synced sequence number
    pub last_synced_seq: u64,
    /// Pending writes waiting for sync
    pub pending_writes: usize,
}

impl Default for WalState {
    fn default() -> Self {
        Self {
            current_segment_id: 1,
            last_synced_seq: 0,
            pending_writes: 0,
        }
    }
}

/// Compaction state
pub struct CompactionState {
    /// SSTs currently being compacted (locked from other compactions)
    pub compacting_ssts: Vec<String>,
    /// Pending compaction tasks
    pub pending_tasks: usize,
}

impl Default for CompactionState {
    fn default() -> Self {
        Self {
            compacting_ssts: Vec::new(),
            pending_tasks: 0,
        }
    }
}

/// Cloud sync state
pub struct CloudState {
    /// SSTs pending upload
    pub pending_uploads: Vec<String>,
    /// Last checkpoint sequence uploaded to cloud
    pub last_cloud_checkpoint_seq: u64,
}

impl Default for CloudState {
    fn default() -> Self {
        Self {
            pending_uploads: Vec::new(),
            last_cloud_checkpoint_seq: 0,
        }
    }
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
}

impl RuntimeState {
    /// Create new runtime state with the given database path.
    pub fn new(db_path: PathBuf) -> Self {
        if let Err(e) = std::fs::create_dir_all(&db_path) {
            tracing::warn!(error = %e, path = ?db_path, "failed to create database directory");
        }

        let wal_dir = db_path.join("wal");
        if let Err(e) = std::fs::create_dir_all(&wal_dir) {
            tracing::warn!(error = %e, path = ?wal_dir, "failed to create WAL directory");
        }

        let sst_dir = db_path.join("sst");
        if let Err(e) = std::fs::create_dir_all(&sst_dir) {
            tracing::warn!(error = %e, path = ?sst_dir, "failed to create SST directory");
        }

        // Load manifest
        let manifest = match crate::metadata::ManifestPersistence::load(&db_path) {
            Ok(m) => {
                tracing::info!("manifest loaded from disk");
                m
            }
            Err(e) => {
                tracing::warn!("failed to load manifest, using default: {}", e);
                Manifest::default()
            }
        };

        let mut column_families = HashMap::new();
        column_families.insert(0, ColumnFamilyState::new(0, "default".into()));

        for cf_meta in &manifest.column_families {
            if cf_meta.id != 0 {
                column_families.insert(
                    cf_meta.id,
                    ColumnFamilyState::new(cf_meta.id, cf_meta.name.clone()),
                );
            }
        }

        // WAL recovery
        if wal_dir.exists() {
            let mut recovery_memtables = HashMap::new();
            match crate::wal::recovery::replay_wal(&wal_dir, &mut recovery_memtables) {
                Ok(stats) => {
                    tracing::info!(
                        records_recovered = stats.records_recovered,
                        bytes_recovered = stats.bytes_recovered,
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
                }
                Err(e) => {
                    tracing::error!(error = %e, "WAL recovery failed, continuing without recovered state");
                }
            }
        }

        Self {
            db_path,
            wal_dir,
            sst_dir,
            sequence: 0,
            next_txn_id: 0,
            column_families,
            manifest,
            wal: WalState::default(),
            compaction: CompactionState::default(),
            cloud: CloudState::default(),
            memtable_size_limit: 64 * 1024 * 1024, // 64MB
            read_only: false,
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

    pub fn get_cf(&self, cf_id: u32) -> Option<&ColumnFamilyState> {
        self.column_families.get(&cf_id)
    }

    pub fn get_cf_mut(&mut self, cf_id: u32) -> Option<&mut ColumnFamilyState> {
        self.column_families.get_mut(&cf_id)
    }

    pub fn create_cf(&mut self, name: String) -> MidgeResult<u32> {
        let id = self.column_families.len() as u32;
        self.column_families.insert(id, ColumnFamilyState::new(id, name));
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
