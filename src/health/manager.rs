//! System health monitoring and lifecycle management
//!
//! Provides operational visibility into database health, manages graceful
//! startup and shutdown procedures, and coordinates system state transitions.
//! Enables external monitoring systems to check readiness, track rehydration
//! progress, and ensure safe maintenance operations.

use parking_lot::RwLock;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime};

use crate::common::timestamp;
use crate::core::manifest::Manifest;
use crate::error::{MidgeError, MidgeResult};

use super::rehydration::{RehydrationProgress, RehydrationStatus};
use super::state::{
    DrainResult, LifecycleState, ReadinessStatus, SyncpointStatus, ValidationResult,
};

/// Trait to persist a `Manifest` atomically. Implementations may write to disk
/// (e.g. `Manifest::save_atomic`) or upload to cloud. Injected into
/// `HealthManager` so drain logic can request durable persistence without
/// depending on engine internals.
pub trait ManifestPersister: Send + Sync {
    fn persist(&self, manifest: &Manifest) -> MidgeResult<()>;
}

/// Configuration for health management
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Enable health probe functionality
    pub enabled: bool,

    /// Drain timeout duration
    pub drain_timeout: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            drain_timeout: Duration::from_secs(30),
        }
    }
}

/// Manages system health monitoring and lifecycle operations.
///
/// Tracks database readiness, coordinates graceful shutdown procedures,
/// monitors rehydration progress, and provides operational status for
/// external health checks and maintenance operations.
pub struct HealthMonitor {
    /// Current lifecycle state
    state: Arc<RwLock<LifecycleState>>,

    /// Rehydration progress
    rehydration: Arc<RwLock<RehydrationProgress>>,

    /// Reference to engine (weak to avoid circular ref)
    engine: Weak<dyn EngineHealth>,

    /// Reference to manifest
    manifest: Arc<RwLock<Manifest>>,

    /// Optional persister callback used to persist manifest/checkpoint atomically.
    /// Stored as RwLock so it can be injected after construction.
    manifest_persister: Arc<RwLock<Option<Arc<dyn ManifestPersister>>>>,

    /// Configuration
    #[allow(dead_code)]
    config: HealthConfig,

    /// Database path for file system checks
    db_path: std::path::PathBuf,

    /// Last state update time
    last_update: Arc<RwLock<SystemTime>>,
}

/// Trait for engine health queries (avoids circular dependency)
pub trait EngineHealth: Send + Sync {
    /// Get current sequence number
    fn current_sequence(&self) -> u64;

    /// Flush memtable to SST
    fn flush_memtable(&self) -> MidgeResult<()>;

    /// Get last checkpoint sequence
    fn last_checkpoint_seq(&self) -> u64;
}

impl HealthMonitor {
    /// Create a new health manager
    pub fn new(
        engine: Weak<dyn EngineHealth>,
        manifest: Arc<RwLock<Manifest>>,
        config: HealthConfig,
        db_path: std::path::PathBuf,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(LifecycleState::Stopped)),
            rehydration: Arc::new(RwLock::new(RehydrationProgress::new())),
            engine,
            manifest,
            manifest_persister: Arc::new(RwLock::new(None)),
            config,
            db_path,
            last_update: Arc::new(RwLock::new(timestamp::now())),
        }
    }

    /// Set the optional manifest persister. This allows external code (usually
    /// the engine or factory) to provide a component capable of persisting the
    /// manifest to disk or uploading it to cloud. Passing `None` clears it.
    pub fn set_manifest_persister(&self, p: Option<Arc<dyn ManifestPersister>>) {
        *self.manifest_persister.write() = p;
    }

    /// Check if process is alive and responsive (fast check, no I/O)
    pub fn is_alive(&self) -> bool {
        // Simple check: can we acquire the state lock?
        self.state.try_read().is_some()
    }

    /// Get current lifecycle state
    pub fn get_state(&self) -> LifecycleState {
        *self.state.read()
    }

    /// Set lifecycle state
    pub fn set_state(&self, new_state: LifecycleState) -> MidgeResult<()> {
        let mut state = self.state.write();

        // Validate state transition
        self.validate_transition(*state, new_state)?;

        *state = new_state;
        *self.last_update.write() = timestamp::now();

        Ok(())
    }

    /// Validate state transition is legal
    fn validate_transition(&self, from: LifecycleState, to: LifecycleState) -> MidgeResult<()> {
        use LifecycleState::*;

        let valid = matches!(
            (from, to),
            (Stopped, Starting) |
            (Starting, Ready) |
            (Starting, Stopped) | // Failed startup
            (Ready, Draining) |
            (Draining, Sealed) |
            (Draining, Ready) | // Drain cancelled
            (_, Stopped) // Emergency stop always allowed
        );

        if !valid {
            return Err(MidgeError::internal(format!(
                "Invalid state transition: {} -> {}",
                from, to
            )));
        }

        Ok(())
    }

    /// Check if ready to accept traffic
    pub fn is_ready(&self) -> ReadinessStatus {
        let state = self.get_state();
        let rehydration = self.rehydration.read();

        let ready = state.is_ready() && rehydration.is_complete();

        let (last_applied_seq, checkpoint_seq) = if let Some(engine) = self.engine.upgrade() {
            (engine.current_sequence(), engine.last_checkpoint_seq())
        } else {
            (0, 0)
        };

        let reason = if !ready {
            Some(match state {
                LifecycleState::Starting if !rehydration.is_complete() => {
                    format!(
                        "Rehydration in progress: {:.1}%",
                        rehydration.progress_pct()
                    )
                }
                LifecycleState::Draining => "Engine is draining".to_string(),
                LifecycleState::Sealed => "Engine is sealed".to_string(),
                LifecycleState::Stopped => "Engine is stopped".to_string(),
                _ => "Not ready".to_string(),
            })
        } else {
            None
        };

        ReadinessStatus {
            ready,
            rehydration_complete: rehydration.is_complete(),
            last_applied_seq,
            checkpoint_seq,
            state: state.to_string(),
            reason,
        }
    }

    /// Get rehydration progress
    pub fn get_rehydration_status(&self) -> RehydrationStatus {
        let progress = self.rehydration.read();
        RehydrationStatus::from(&*progress)
    }

    /// Get current syncpoint
    pub fn get_syncpoint(&self) -> SyncpointStatus {
        let state = self.get_state();
        let last_update = *self.last_update.read();

        let (current_seq, checkpoint_seq) = if let Some(engine) = self.engine.upgrade() {
            (engine.current_sequence(), engine.last_checkpoint_seq())
        } else {
            (0, 0)
        };

        SyncpointStatus {
            current_seq,
            checkpoint_seq,
            state: state.to_string(),
            last_update,
        }
    }

    /// Initiate graceful drain
    pub fn drain(&self) -> DrainResult {
        let start = Instant::now();

        // Transition to draining state
        if let Err(e) = self.set_state(LifecycleState::Draining) {
            return DrainResult {
                status: "failed".to_string(),
                last_committed_seq: 0,
                duration_ms: start.elapsed().as_millis() as u64,
                error: Some(e.to_string()),
            };
        }

        // Get engine reference
        let engine = match self.engine.upgrade() {
            Some(e) => e,
            None => {
                return DrainResult {
                    status: "failed".to_string(),
                    last_committed_seq: 0,
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: Some("Engine not available".to_string()),
                };
            }
        };

        // Flush memtable
        if let Err(e) = engine.flush_memtable() {
            return DrainResult {
                status: "failed".to_string(),
                last_committed_seq: engine.current_sequence(),
                duration_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("Flush failed: {}", e)),
            };
        }

        // Get final sequence
        let last_committed_seq = engine.current_sequence();

        // Phase 4: Mark WAL checkpoint in manifest so WAL pruning can proceed.
        // We update the in-memory manifest and, if a persister is registered,
        // ask it to persist the manifest atomically. We clone the manifest and
        // release the write lock before calling out to the persister to avoid
        // potential deadlocks.
        let persister_opt = { self.manifest_persister.read().clone() };
        let manifest_clone = {
            let mut manifest = self.manifest.write();
            let covering_ssts: Vec<String> =
                manifest.files.iter().map(|f| f.name.clone()).collect();
            if let Err(e) = manifest.update_cloud_checkpoint(last_committed_seq, covering_ssts) {
                return DrainResult {
                    status: "failed".to_string(),
                    last_committed_seq,
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!("Failed to update manifest checkpoint: {}", e)),
                };
            }
            manifest.clone()
        };

        if let Some(p) = persister_opt {
            if let Err(e) = p.persist(&manifest_clone) {
                return DrainResult {
                    status: "failed".to_string(),
                    last_committed_seq,
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: Some(format!("Manifest persist failed: {}", e)),
                };
            }
        }

        // Transition to sealed state
        if let Err(e) = self.set_state(LifecycleState::Sealed) {
            return DrainResult {
                status: "failed".to_string(),
                last_committed_seq,
                duration_ms: start.elapsed().as_millis() as u64,
                error: Some(e.to_string()),
            };
        }

        DrainResult {
            status: "sealed".to_string(),
            last_committed_seq,
            duration_ms: start.elapsed().as_millis() as u64,
            error: None,
        }
    }

    /// Validate state against cloud
    pub fn validate_state(&self) -> ValidationResult {
        let current_seq = if let Some(engine) = self.engine.upgrade() {
            engine.current_sequence()
        } else {
            0
        };

        // Get cloud checkpoint
        let manifest = self.manifest.read();
        let cloud_seq = manifest
            .cloud_checkpoint
            .as_ref()
            .map(|c| c.checkpoint_sequence)
            .unwrap_or(0);

        // Detailed validation (Phase 5)
        let mut missing_segments = Vec::new();
        let mut discrepancies = Vec::new();

        // Check for missing WAL segments
        // Look for WAL files in wal directory and check for gaps
        let wal_dir = self.db_path.join("wal");
        if wal_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&wal_dir) {
                let mut wal_files = Vec::new();
                for entry in entries.flatten() {
                    if let Some(name) = entry.file_name().to_str() {
                        if name.ends_with(".wal") {
                            wal_files.push(name.to_string());
                        }
                    }
                }
                // Sort and check for sequence gaps (basic check)
                wal_files.sort();
                if wal_files.len() > 1 {
                    for i in 1..wal_files.len() {
                        let prev = &wal_files[i - 1];
                        let curr = &wal_files[i];
                        // Simple check: if names don't follow expected pattern, note it
                        if prev >= curr {
                            discrepancies
                                .push(format!("WAL files out of order: {} >= {}", prev, curr));
                        }
                    }
                }
            }
        }

        // Validate SST continuity
        let sst_dir = self.db_path.join("sst");
        let mut level_files = std::collections::HashMap::new();
        for file_meta in &manifest.files {
            // Check if SST file exists
            let sst_path = sst_dir.join(&file_meta.name);
            if !sst_path.exists() {
                missing_segments.push(format!("SST file missing: {}", file_meta.name));
            }

            // Group by level for continuity check
            level_files
                .entry(file_meta.level)
                .or_insert_with(Vec::new)
                .push(&file_meta.name);
        }

        // Check for level continuity (basic: no gaps in levels)
        let mut levels: Vec<_> = level_files.keys().collect();
        levels.sort();
        if let Some(&max_level) = levels.last() {
            for level in 0..=*max_level {
                if !level_files.contains_key(&level) && level > 0 {
                    // Level 0 can be missing, but higher levels should exist if max_level > 0
                    discrepancies.push(format!("Missing SST files at level {}", level));
                }
            }
        }

        // Check for duplicate SST names
        let mut seen_names = std::collections::HashSet::new();
        for file_meta in &manifest.files {
            if !seen_names.insert(&file_meta.name) {
                discrepancies.push(format!(
                    "Duplicate SST file in manifest: {}",
                    file_meta.name
                ));
            }
        }

        ValidationResult {
            valid: current_seq >= cloud_seq
                && missing_segments.is_empty()
                && discrepancies.is_empty(),
            current_seq,
            cloud_seq,
            missing_segments,
            discrepancies,
        }
    }

    // === Rehydration Progress Tracking ===

    /// Start rehydration tracking
    pub fn start_rehydration(&self) {
        let mut progress = self.rehydration.write();
        *progress = RehydrationProgress::new();
    }

    /// Set total WAL segments to replay
    pub fn set_total_wal_segments(&self, total: usize) {
        self.rehydration.write().total_wal_segments = total;
    }

    /// Update WAL replay progress
    pub fn update_wal_progress(&self, replayed: usize) {
        self.rehydration.write().replayed_wal_segments = replayed;
    }

    /// Set total SSTs to load
    pub fn set_total_ssts(&self, total: usize) {
        self.rehydration.write().total_ssts = total;
    }

    /// Update SST load progress
    pub fn update_sst_progress(&self, loaded: usize) {
        self.rehydration.write().loaded_ssts = loaded;
    }

    /// Update current sequence being processed
    pub fn update_current_seq(&self, seq: u64) {
        self.rehydration.write().current_seq = seq;
    }

    /// Set target sequence
    pub fn set_target_seq(&self, seq: u64) {
        self.rehydration.write().target_seq = seq;
    }

    /// Mark rehydration complete
    pub fn complete_rehydration(&self) {
        self.rehydration.write().mark_complete();
    }
}

// Tests for HealthManager are in tests/health.rs
