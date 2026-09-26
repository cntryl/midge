//! WAL, manifest, and intent-log recovery construction.

use super::{
    Arc, CloudState, ColumnFamilyState, CompactionConfig, CompactionState, Fs, HashMap,
    IntentLogEntry, Manifest, MidgeError, MidgeResult, PathBuf, PublicationPhase,
    RecoveryLoadState, RecoveryStatus, RuntimeDiagnostics, RuntimeMode, RuntimePersistence,
    RuntimeState, SkipListMemtable, SnapshotPinRegistry, SnapshotState, TransactionCoordination,
    WalRecoveryState, WalState, WritePressureState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionManifestState {
    Prepublication,
    Published,
    Partial,
}

impl RuntimeState {
    pub(super) fn manifest_visible_sequence_floor(manifest: &Manifest) -> u64 {
        let file_max_sequence = manifest
            .files
            .iter()
            .filter_map(|file| file.largest_seq.or(file.smallest_seq))
            .max()
            .unwrap_or(0);
        manifest.last_persisted_sequence.max(file_max_sequence)
    }

    pub(super) fn manifest_compaction_output_generation_floor(manifest: &Manifest) -> u64 {
        manifest
            .files
            .iter()
            .filter_map(|file| crate::cloud_layout::parse_compaction_file_name(&file.name))
            .map(|(_, _, generation, _)| generation)
            // The manifest counter reserves names before cloud output
            // publication. An interrupted job can leave no visible SSTs.
            .chain(manifest.next_sst_seqs.values().copied())
            .max()
            .unwrap_or(0)
    }

    /// Create new runtime state with the given database path.
    /// If `memory_mode` is true, filesystem is never touched.
    #[cfg(test)]
    pub fn new(db_path: PathBuf, memory_mode: bool) -> Self {
        Self::try_new(db_path, memory_mode, crate::config::RecoveryPolicy::Strict)
            .expect("runtime state initialization failed")
    }

    /// Create new runtime state with an explicit recovery policy.
    pub fn try_new(
        db_path: PathBuf,
        memory_mode: bool,
        recovery_policy: crate::config::RecoveryPolicy,
    ) -> MidgeResult<Self> {
        Self::try_new_with_recovery_dir(db_path, memory_mode, None, recovery_policy)
    }

    /// Create new runtime state with an optional override for WAL recovery and
    /// an explicit recovery policy.
    /// # Errors
    ///
    /// Returns an error if recovery cannot initialize the filesystem, load the manifest,
    /// replay the intent log, or replay WAL state.
    pub fn try_new_with_recovery_dir(
        db_path: PathBuf,
        memory_mode: bool,
        recovery_wal_dir: Option<&PathBuf>,
        recovery_policy: crate::config::RecoveryPolicy,
    ) -> MidgeResult<Self> {
        Self::initialize_recovery(
            db_path,
            memory_mode,
            recovery_wal_dir,
            recovery_policy,
            true,
        )
    }

    /// Load metadata before cloud WAL replay, whose working set is checkpointed
    /// incrementally by startup after outstanding publication intents are repaired.
    pub(crate) fn try_new_before_cloud_replay(
        db_path: PathBuf,
        recovery_policy: crate::config::RecoveryPolicy,
    ) -> MidgeResult<Self> {
        Self::initialize_recovery(db_path, false, None, recovery_policy, false)
    }

    fn initialize_recovery(
        db_path: PathBuf,
        memory_mode: bool,
        recovery_wal_dir: Option<&PathBuf>,
        recovery_policy: crate::config::RecoveryPolicy,
        replay_wal: bool,
    ) -> MidgeResult<Self> {
        let persistence = RuntimePersistence::from_memory_mode(memory_mode);
        let (wal_dir, sst_dir) = Self::ensure_directories(&db_path, persistence.is_memory());
        let fs = Self::initialize_fs(&db_path, persistence.is_memory())?;
        let RecoveryLoadState {
            opened_in_salvage_mode,
            manifest,
            intent_log,
        } = Self::load_recovery_state(&db_path, persistence.is_memory(), recovery_policy, &fs)?;
        let column_families = Self::bootstrap_column_families(&manifest);
        let wal_recovery = if replay_wal {
            Self::recover_wal_state(
                persistence.is_memory(),
                &wal_dir,
                &sst_dir,
                recovery_wal_dir,
                recovery_policy,
                &manifest,
                column_families,
            )?
        } else {
            Self::empty_wal_recovery(&manifest, column_families)
        };
        let recovered_memtable_bytes = Self::recovered_memtable_bytes(&wal_recovery);
        let recovered_compaction_output_generation =
            Self::manifest_compaction_output_generation_floor(&manifest)
                .max(wal_recovery.recovered_sequence);
        let diagnostics = Self::diagnostics_for_recovered_wal(&wal_recovery);

        let mut state = Self {
            db_path,
            wal_dir,
            sst_dir,
            sequence: wal_recovery.recovered_sequence,
            compaction_output_generation: recovered_compaction_output_generation,
            transaction: TransactionCoordination {
                next_id: 0,
                pending_started_at: None,
            },
            column_families: wal_recovery.column_families,
            manifest,
            fs: fs.clone(),
            manifest_store: Arc::new(crate::metadata::store::ManifestStore::new(fs.clone())),
            sst_names: super::SstNameAllocation::default(),
            recovery_sst_fs: None,
            salvaged_local_ssts: std::collections::HashSet::new(),
            ttl_clock: Arc::new(crate::common::time::ObservedClock::default()),
            wal: WalState {
                current_segment_id: wal_recovery.next_segment_id,
                local_durable_seq: wal_recovery.recovered_sequence,
                cloud_durable_seq: wal_recovery.recovered_sequence,
                ..WalState::default()
            },
            compaction: CompactionState::default(),
            cloud: CloudState::default(),
            snapshots: SnapshotState {
                max_snapshot_lifetime: std::time::Duration::from_hours(1), // 1 hour default
            },
            snapshot_pins: Arc::new(SnapshotPinRegistry::default()),
            diagnostics,
            recent_delete_ranges: Vec::new(),
            memtable_size_limit: 64 * 1024 * 1024, // 64MB
            mode: RuntimeMode {
                #[cfg(test)]
                read_only: false,
                persistence,
            },
            recovery: RecoveryStatus {
                policy: recovery_policy,
                opened_in_salvage_mode: opened_in_salvage_mode
                    || wal_recovery.opened_in_salvage_mode,
                persistence_anomaly_detected: false,
                ddl_authority_ambiguous: false,
                metadata: super::MetadataSync::Current,
            },
            compaction_config: CompactionConfig {
                enabled: persistence.compaction_enabled(),
            },
            intent_log,
            memtable_flush_threshold: 64 * 1024 * 1024, // 64MB
            eventual_flush_segment_gap: crate::runtime::CloudRuntimePolicy::default()
                .eventual_flush_segment_gap,
            max_immutable_memtables: 10, // Hard limit on immutable memtable queue
            l0_compaction_trigger: crate::compaction::LeveledCompactionConfig::default()
                .l0_file_count_threshold,
            next_flush_id: 1,
            writer_epoch: 0,
            flush_metrics: super::FlushRuntimeMetrics::default(),
            write_pressure: WritePressureState { stalled: false },
            total_memtable_bytes: recovered_memtable_bytes,
            wal_recovery_records_replayed: wal_recovery.records_replayed,
            wal_recovery_bytes_replayed: wal_recovery.bytes_replayed,
            intent_log_replay_runs: 0,
            intent_log_entries_replayed: 0,
            ingest_epoch: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            active_compactions: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            ingest_active: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            pending_compaction_waits: parking_lot::Mutex::new(std::collections::HashMap::new()),
        };
        state.reinitialize_active_memtable_segment_tracking();
        Ok(state)
    }

    fn diagnostics_for_recovered_wal(wal_recovery: &WalRecoveryState) -> Arc<RuntimeDiagnostics> {
        let diagnostics = Arc::new(RuntimeDiagnostics::default());
        diagnostics.record(|metrics| {
            metrics.record_wal_recovery(wal_recovery.records_replayed, wal_recovery.bytes_replayed);
        });
        diagnostics
    }

    fn recovered_memtable_bytes(wal_recovery: &WalRecoveryState) -> usize {
        wal_recovery
            .column_families
            .values()
            .map(|cf| {
                cf.immutable_memtables
                    .iter()
                    .fold(cf.memtable.size_bytes(), |total, memtable| {
                        total.saturating_add(memtable.size_bytes())
                    })
            })
            .fold(0_usize, usize::saturating_add)
    }

    fn empty_wal_recovery(
        manifest: &Manifest,
        column_families: HashMap<u32, ColumnFamilyState>,
    ) -> WalRecoveryState {
        WalRecoveryState {
            column_families,
            recovered_sequence: Self::manifest_visible_sequence_floor(manifest),
            next_segment_id: 1,
            records_replayed: 0,
            bytes_replayed: 0,
            opened_in_salvage_mode: false,
        }
    }

    fn initialize_fs(db_path: &std::path::Path, memory_mode: bool) -> MidgeResult<Arc<dyn Fs>> {
        if !memory_mode {
            crate::metadata::ensure_or_create_format_marker(db_path)?;
        }
        if memory_mode {
            Ok(Arc::new(crate::io::MockFs::new()))
        } else {
            Ok(Arc::new(crate::io::real::RealFs::new(db_path).map_err(
                |error| {
                    MidgeError::RecoveryFailed(format!("failed to initialize filesystem: {error}"))
                },
            )?))
        }
    }

    fn load_recovery_state(
        db_path: &std::path::Path,
        memory_mode: bool,
        recovery_policy: crate::config::RecoveryPolicy,
        fs: &Arc<dyn Fs>,
    ) -> MidgeResult<RecoveryLoadState> {
        let mut opened_in_salvage_mode = false;
        let manifest = Self::load_manifest(
            db_path,
            memory_mode,
            recovery_policy,
            fs,
            &mut opened_in_salvage_mode,
        )?;
        let intent_log = Self::load_intent_log(
            memory_mode,
            recovery_policy,
            fs,
            &mut opened_in_salvage_mode,
        )?;
        Ok(RecoveryLoadState {
            opened_in_salvage_mode,
            manifest,
            intent_log,
        })
    }

    fn load_manifest(
        _db_path: &std::path::Path,
        memory_mode: bool,
        recovery_policy: crate::config::RecoveryPolicy,
        fs: &Arc<dyn Fs>,
        opened_in_salvage_mode: &mut bool,
    ) -> MidgeResult<Manifest> {
        if memory_mode {
            return Ok(Manifest::default());
        }
        match crate::metadata::ManifestPersistence::load_with_fs_and_policy(
            fs,
            crate::config::RecoveryPolicy::Strict,
        ) {
            Ok(manifest) => {
                tracing::info!("manifest loaded from disk");
                Ok(manifest)
            }
            Err(error) => {
                if recovery_policy == crate::config::RecoveryPolicy::Strict {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to load manifest: {error}"
                    )));
                }
                *opened_in_salvage_mode = true;
                tracing::warn!(
                    "failed to load manifest strictly, retrying in salvage mode: {}",
                    error
                );
                // A manifest salvage cannot read at all opens empty; the
                // salvage-mode orphan sweep then quarantines every SST rather
                // than deleting it. Telling a transient read error apart
                // from corruption here needs a typed load error (#494).
                Ok(
                    crate::metadata::ManifestPersistence::load_with_fs_and_policy(
                        fs,
                        crate::config::RecoveryPolicy::Salvage,
                    )
                    .unwrap_or_else(|salvage_error| {
                        tracing::warn!(
                            "failed to load manifest in salvage mode, using default: {}",
                            salvage_error
                        );
                        Manifest::default()
                    }),
                )
            }
        }
    }

    fn load_intent_log(
        memory_mode: bool,
        recovery_policy: crate::config::RecoveryPolicy,
        fs: &Arc<dyn Fs>,
        opened_in_salvage_mode: &mut bool,
    ) -> MidgeResult<Vec<IntentLogEntry>> {
        if memory_mode {
            return Ok(Vec::new());
        }
        match crate::runtime::IntentPersistence::load_with_fs_and_policy(
            fs,
            crate::config::RecoveryPolicy::Strict,
        ) {
            Ok(intent_log) => Ok(intent_log),
            Err(error) => {
                if recovery_policy == crate::config::RecoveryPolicy::Strict {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to load intent log: {error}"
                    )));
                }
                *opened_in_salvage_mode = true;
                tracing::warn!(
                    error = %error,
                    "failed to load intent log strictly, retrying in salvage mode"
                );
                Ok(crate::runtime::IntentPersistence::load_with_fs_and_policy(
                    fs,
                    crate::config::RecoveryPolicy::Salvage,
                )
                .unwrap_or_else(|salvage_error| {
                    tracing::warn!(
                        error = %salvage_error,
                        "failed to load intent log in salvage mode, starting empty"
                    );
                    Vec::new()
                }))
            }
        }
    }

    fn bootstrap_column_families(manifest: &Manifest) -> HashMap<u32, ColumnFamilyState> {
        let mut column_families = HashMap::new();
        column_families.insert(0, ColumnFamilyState::new(0, "default".into()));
        for cf_meta in &manifest.column_families {
            if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                column_families.insert(
                    cf_meta.id,
                    ColumnFamilyState::new(cf_meta.id, cf_meta.name.clone()),
                );
            }
        }
        column_families
    }

    fn recover_wal_state(
        memory_mode: bool,
        wal_dir: &std::path::Path,
        sst_dir: &std::path::Path,
        recovery_wal_dir: Option<&PathBuf>,
        recovery_policy: crate::config::RecoveryPolicy,
        manifest: &Manifest,
        column_families: HashMap<u32, ColumnFamilyState>,
    ) -> MidgeResult<WalRecoveryState> {
        let replay_dir = recovery_wal_dir.map_or(wal_dir, PathBuf::as_path);
        let mut wal_recovery = Self::replay_wal(
            memory_mode,
            replay_dir,
            sst_dir,
            recovery_policy,
            manifest,
            column_families,
        )?;
        wal_recovery.recovered_sequence = wal_recovery
            .recovered_sequence
            .max(Self::manifest_visible_sequence_floor(manifest));
        wal_recovery.next_segment_id = Self::recover_next_segment_id(memory_mode, replay_dir)?;
        Ok(wal_recovery)
    }

    fn replay_wal(
        memory_mode: bool,
        replay_dir: &std::path::Path,
        sst_dir: &std::path::Path,
        recovery_policy: crate::config::RecoveryPolicy,
        manifest: &Manifest,
        mut column_families: HashMap<u32, ColumnFamilyState>,
    ) -> MidgeResult<WalRecoveryState> {
        if memory_mode || !replay_dir.exists() {
            return Ok(WalRecoveryState {
                column_families,
                recovered_sequence: 0,
                next_segment_id: 1,
                records_replayed: 0,
                bytes_replayed: 0,
                opened_in_salvage_mode: false,
            });
        }

        let mut recovery_memtables = HashMap::new();
        let storage = match crate::io::RealFs::new(replay_dir) {
            Ok(storage) => storage,
            Err(error) => {
                return Self::handle_wal_recovery_failure(
                    recovery_policy,
                    format!("failed to initialize WAL recovery filesystem: {error}"),
                    column_families,
                );
            }
        };
        let replay_policy = match recovery_policy {
            crate::config::RecoveryPolicy::Strict => crate::wal::recovery::ReplayPolicy::Strict,
            crate::config::RecoveryPolicy::Salvage => {
                crate::wal::recovery::ReplayPolicy::SalvageValidPrefix
            }
        };
        let coverage = crate::runtime::hybrid_persistence::VerifiedManifestWalCoverage::open(
            sst_dir, manifest,
        );
        let should_apply = |record: &crate::wal::WalRecord| !coverage.covers_wal_record(record);
        let stats = match crate::wal::recovery::replay_wal_with_manifest_filter(
            &storage,
            &crate::io::FsPath::new(""),
            &mut recovery_memtables,
            replay_policy,
            &should_apply,
        ) {
            Ok(stats) => stats,
            Err(error) => {
                if recovery_policy == crate::config::RecoveryPolicy::Salvage {
                    // Salvage continues without the WAL's state, so set every
                    // WAL file aside first: new records must not land behind
                    // them or reuse the sequences they hold.
                    let files = Self::replayable_wal_files(&storage).and_then(|files| {
                        Self::quarantine_wal_files(&storage, None, &files).map(|()| files)
                    });
                    if let Err(quarantine_error) = files {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "WAL recovery failed ({error}) and the WAL could not be set aside: {quarantine_error}"
                        )));
                    }
                }
                return Self::handle_wal_recovery_failure(
                    recovery_policy,
                    format!("WAL recovery failed: {error}"),
                    column_families,
                );
            }
        };

        if let Some(tail) = &stats.tolerated_active_tail {
            // Fail the open under either policy: appending past a torn tail
            // would corrupt acknowledged writes, and salvage's fallback would
            // discard the state replay just recovered.
            Self::truncate_tolerated_active_tail(&storage, tail).map_err(|error| {
                MidgeError::RecoveryFailed(format!(
                    "failed to truncate torn active WAL tail: {error}"
                ))
            })?;
        }
        let mut recovered_sequence = stats.max_sequence.unwrap_or(0);
        if let Some(stop) = &stats.salvage_stop {
            Self::quarantine_wal_files(&storage, Some(stop), &stop.unreplayed_paths).map_err(
                |error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to set aside WAL beyond the salvage point: {error}"
                    ))
                },
            )?;
            recovered_sequence = recovered_sequence.max(stop.max_unreplayed_sequence.unwrap_or(0));
        }
        Self::record_wal_recovery_stats(replay_dir, &stats);
        let opened_in_salvage_mode = stats.had_corruption;
        if opened_in_salvage_mode {
            tracing::warn!(
                replay_dir = ?replay_dir,
                "WAL recovery salvaged a valid prefix after corruption"
            );
        }
        Self::apply_recovered_memtables(&mut column_families, recovery_memtables);
        Ok(WalRecoveryState {
            column_families,
            recovered_sequence,
            next_segment_id: 1,
            records_replayed: stats.record_count,
            bytes_replayed: stats.bytes,
            opened_in_salvage_mode,
        })
    }

    /// WAL files that replay would read, relative to the WAL directory.
    fn replayable_wal_files(storage: &dyn crate::io::Fs) -> MidgeResult<Vec<crate::io::FsPath>> {
        let mut files: Vec<_> = storage
            .list_dir(&crate::io::FsPath::new(""))?
            .into_iter()
            .filter(|entry| {
                !entry.is_dir
                    && (entry.name == crate::wal::ACTIVE_FILE_NAME
                        || crate::wal::parse_segment_id(&entry.name).is_some())
            })
            .map(|entry| crate::io::FsPath::new(entry.name))
            .collect();
        files.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(files)
    }

    /// Move WAL that salvage did not replay into `salvaged-<millis>/`, which
    /// replay and segment numbering ignore. With a stop point, its file keeps
    /// the replayed prefix live (a full copy is preserved first) because the
    /// recovered memtables still depend on it. Nothing is deleted.
    fn quarantine_wal_files(
        storage: &dyn crate::io::Fs,
        stop: Option<&crate::wal::recovery::WalSalvageStop>,
        unreplayed: &[crate::io::FsPath],
    ) -> MidgeResult<()> {
        use crate::io::{Durability, FsPath};

        if stop.is_none() && unreplayed.is_empty() {
            return Ok(());
        }
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis());
        let quarantine = FsPath::new(format!("salvaged-{millis}"));
        storage.create_dir_all(&quarantine)?;
        let target = |path: &FsPath| FsPath::new(format!("{}/{}", quarantine.0, path.0));

        if let Some(stop) = stop {
            let file = storage.open(
                &stop.path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )?;
            let bytes = file.read_at(0, file.len()?)?;
            drop(file);
            let mut copy = storage.open(
                &target(&stop.path),
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: true,
                    create_new: true,
                    truncate: false,
                },
            )?;
            copy.write_at(0, bytes)?;
            copy.sync(Durability::Durable)?;
        }
        for path in unreplayed {
            storage.rename_atomic(path, &target(path))?;
        }
        storage.sync_dir(&quarantine, Durability::Durable)?;
        storage.sync_dir(&FsPath::new(""), Durability::Durable)?;

        if let Some(stop) = stop {
            let mut file = storage.open(
                &stop.path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )?;
            file.truncate(stop.valid_bytes)?;
            file.sync(Durability::Durable)?;
        }
        tracing::warn!(
            quarantine = %quarantine.0,
            stop_file = ?stop.map(|stop| &stop.path.0),
            moved = unreplayed.len(),
            "set aside WAL that salvage recovery did not replay"
        );
        Ok(())
    }

    /// Cut the active WAL back to its verified prefix before the writer
    /// reopens it for append. Otherwise new frames land after the torn bytes,
    /// and the next recovery sees mid-file corruption instead of a tail.
    fn truncate_tolerated_active_tail(
        storage: &dyn crate::io::Fs,
        tail: &crate::wal::recovery::ToleratedActiveTail,
    ) -> MidgeResult<()> {
        let mut file = storage.open(
            &tail.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        let length = file.len()?;
        if length <= tail.valid_bytes {
            return Ok(());
        }
        file.truncate(tail.valid_bytes)?;
        file.sync(crate::io::Durability::Durable)?;
        tracing::warn!(
            path = %tail.path,
            discarded_bytes = length - tail.valid_bytes,
            valid_bytes = tail.valid_bytes,
            "truncated torn active WAL tail before reopening for append"
        );
        Ok(())
    }

    fn handle_wal_recovery_failure(
        recovery_policy: crate::config::RecoveryPolicy,
        message: String,
        column_families: HashMap<u32, ColumnFamilyState>,
    ) -> MidgeResult<WalRecoveryState> {
        if recovery_policy == crate::config::RecoveryPolicy::Strict {
            return Err(MidgeError::RecoveryFailed(message));
        }
        tracing::error!("{message}, continuing without recovered state in salvage mode");
        Ok(WalRecoveryState {
            column_families,
            recovered_sequence: 0,
            next_segment_id: 1,
            records_replayed: 0,
            bytes_replayed: 0,
            opened_in_salvage_mode: true,
        })
    }

    fn record_wal_recovery_stats(
        replay_dir: &std::path::Path,
        stats: &crate::wal::recovery::RecoveryStats,
    ) {
        tracing::info!(
            records_recovered = stats.record_count,
            bytes_recovered = stats.bytes,
            max_sequence = ?stats.max_sequence,
            replay_dir = ?replay_dir,
            replay_ms = std::time::Duration::from_nanos(
                u64::try_from(stats.total_replay_ns).unwrap_or(u64::MAX),
            )
            .as_secs_f64()
                * 1_000.0,
            wal_read_ms = std::time::Duration::from_nanos(
                u64::try_from(stats.wal_read_ns).unwrap_or(u64::MAX),
            )
            .as_secs_f64()
                * 1_000.0,
            apply_ms = std::time::Duration::from_nanos(
                u64::try_from(stats.apply_ns).unwrap_or(u64::MAX),
            )
            .as_secs_f64()
                * 1_000.0,
            "WAL recovery completed successfully"
        );
    }

    pub(super) fn apply_recovered_memtables(
        column_families: &mut HashMap<u32, ColumnFamilyState>,
        recovery_memtables: HashMap<u32, Arc<SkipListMemtable>>,
    ) {
        for (cf_id, recovered_memtable) in recovery_memtables {
            if let Some(cf_state) = column_families.get_mut(&cf_id) {
                cf_state.memtable = recovered_memtable;
            } else {
                // WAL records cannot create schema. An unknown column family
                // belongs to a dropped or stale manifest generation and must
                // not be resurrected during recovery.
                tracing::warn!(cf_id, "ignoring WAL records for unknown column family");
            }
        }
    }

    fn recover_next_segment_id(
        memory_mode: bool,
        replay_dir: &std::path::Path,
    ) -> MidgeResult<u64> {
        if memory_mode || !replay_dir.exists() {
            return Ok(1);
        }
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
                if stem.eq_ignore_ascii_case("wal") {
                    continue;
                }
                if let Ok(id) = stem.parse::<u64>() {
                    max_segment_id = max_segment_id.max(id);
                }
            }
        }
        max_segment_id
            .checked_add(1)
            .map(|next| next.max(1))
            .ok_or_else(|| MidgeError::ResourceLimit("WAL segment identity space exhausted".into()))
    }

    fn handle_recovery_issue(&mut self, message: String) -> MidgeResult<bool> {
        if self.recovery_policy() == crate::config::RecoveryPolicy::Strict {
            return Err(MidgeError::RecoveryFailed(message));
        }

        self.mark_opened_in_salvage_mode();
        self.mark_persistence_anomaly();
        tracing::warn!("{}", message);
        Ok(false)
    }

    /// Verify a complete SST publication proof through one immutable file view.
    /// The checksum pass uses fixed-size ranges and never materializes a whole
    /// output locally or in memory. Metadata is decoded through the same pinned
    /// view so a replacement object cannot be mixed into one proof.
    pub(crate) fn validate_sst_fs_proof(
        fs: Arc<dyn Fs>,
        file_meta: &crate::runtime::FileMeta,
    ) -> MidgeResult<()> {
        let path = crate::io::FsPath::new(crate::cloud_layout::object_key(&file_meta.name));
        let pinned = fs.immutable_read_view(&path)?.unwrap_or(fs);
        let file = pinned.open(
            &path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        let size = file.len()?;
        // Recovery keeps readable data an older writer never proved, so a
        // manifest entry with no recorded size or CRC is accepted.
        crate::sst::identity::SstIdentity::of_file(file.as_ref(), size, None)
            .and_then(|identity| {
                identity
                    .verify_against(
                        file_meta.expected_sst(),
                        None,
                        crate::sst::identity::ProofPolicy::Legacy,
                    )
                    .map_err(|mismatch| {
                        MidgeError::RecoveryFailed(format!(
                            "recovery intent SST does not match its publication proof: {mismatch}"
                        ))
                    })
            })
            .map_err(|error| match error {
                MidgeError::Corruption(message) => MidgeError::RecoveryFailed(format!(
                    "recovery intent SST '{}' could not be read in full: {message}",
                    file_meta.name
                )),
                other => other,
            })?;
        drop(file);
        crate::sst::fs::SstFileIo::open(&path.0, pinned)?;
        Ok(())
    }

    fn validate_recovered_sst(
        &mut self,
        file_meta: &crate::runtime::FileMeta,
    ) -> MidgeResult<bool> {
        let fs = self.recovery_sst_fs.as_ref().unwrap_or(&self.fs).clone();
        match Self::validate_sst_fs_proof(fs, file_meta) {
            Ok(()) => Ok(true),
            Err(error) => self.handle_recovery_issue(format!(
                "recovery intent references missing or invalid SST '{}': {error}",
                file_meta.name
            )),
        }
    }

    fn validate_local_recovered_sst(
        &mut self,
        file_meta: &crate::runtime::FileMeta,
    ) -> MidgeResult<bool> {
        match Self::validate_sst_fs_proof(Arc::clone(&self.fs), file_meta) {
            Ok(()) => Ok(true),
            Err(error) => self.handle_recovery_issue(format!(
                "recovery intent references missing or invalid local SST '{}': {error}",
                file_meta.name
            )),
        }
    }

    fn delete_sst_if_exists(&mut self, sst_name: &str) -> MidgeResult<()> {
        let path = self.sst_dir.join(sst_name);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                if self.recovery_policy() == crate::config::RecoveryPolicy::Strict {
                    Err(MidgeError::RecoveryFailed(format!(
                        "failed to delete SST '{sst_name}' during recovery: {error}"
                    )))
                } else {
                    self.mark_opened_in_salvage_mode();
                    self.mark_persistence_anomaly();
                    tracing::warn!(
                        sst_name,
                        error = %error,
                        "failed to delete SST during salvage recovery"
                    );
                    Ok(())
                }
            }
        }
    }

    fn replay_flush_publish_intent(
        &mut self,
        phase: PublicationPhase,
        file_meta: &crate::runtime::FileMeta,
    ) -> MidgeResult<bool> {
        if self.manifest_has_file(&file_meta.name) {
            tracing::info!(sst_name = %file_meta.name, "flush publish intent already reflected in manifest");
            return Ok(false);
        }

        match phase {
            PublicationPhase::OutputDurable => {
                let path = self.sst_dir.join(&file_meta.name);
                if path.exists() {
                    if !self.validate_local_recovered_sst(file_meta)? {
                        return Ok(false);
                    }
                    self.delete_sst_if_exists(&file_meta.name)?;
                    tracing::info!(
                        sst_name = %file_meta.name,
                        "deleted orphan flush SST left behind before manifest publication"
                    );
                } else {
                    tracing::info!(
                        sst_name = %file_meta.name,
                        "flush publication intent references no remaining SST; treating it as an already-cleaned orphan"
                    );
                }
                Ok(false)
            }
            PublicationPhase::ManifestPublished => {
                if !self.validate_recovered_sst(file_meta)? {
                    return Ok(false);
                }

                self.append_manifest_add_sst(file_meta)?;
                let changed = self.insert_manifest_file_if_missing(file_meta);
                tracing::info!(
                    sst_name = %file_meta.name,
                    "replayed manifest-published flush intent"
                );
                Ok(changed)
            }
        }
    }

    fn replay_compaction_publish_intent(
        &mut self,
        phase: PublicationPhase,
        removed: &[String],
        added: &[crate::runtime::FileMeta],
    ) -> MidgeResult<bool> {
        if removed.is_empty() {
            return self
                .handle_recovery_issue("compaction publication intent has no inputs".to_string());
        }

        let manifest_state = self.compaction_manifest_state(removed, added);
        match (phase, manifest_state) {
            (PublicationPhase::OutputDurable, CompactionManifestState::Prepublication) => {
                for file_meta in added {
                    let path = self.sst_dir.join(&file_meta.name);
                    if !path.exists() {
                        continue;
                    }
                    if !self.validate_local_recovered_sst(file_meta)? {
                        return Ok(false);
                    }
                    self.delete_sst_if_exists(&file_meta.name)?;
                }
                tracing::info!(
                    removed_count = removed.len(),
                    added_count = added.len(),
                    "rolled back output-durable compaction before manifest publication"
                );
                Ok(false)
            }
            (
                PublicationPhase::OutputDurable | PublicationPhase::ManifestPublished,
                CompactionManifestState::Published,
            ) => {
                for file_meta in added {
                    if !self.validate_recovered_sst(file_meta)? {
                        return Ok(false);
                    }
                }
                for sst_name in removed {
                    self.delete_sst_if_exists(sst_name)?;
                }
                Ok(false)
            }
            (PublicationPhase::ManifestPublished, CompactionManifestState::Prepublication) => {
                for file_meta in added {
                    if !self.validate_recovered_sst(file_meta)? {
                        return Ok(false);
                    }
                }
                self.append_manifest_compaction_batch(removed, added)?;
                let manifest_changed = self.apply_compaction_to_manifest(removed, added);
                for sst_name in removed {
                    self.delete_sst_if_exists(sst_name)?;
                }
                tracing::info!(
                    removed_count = removed.len(),
                    added_count = added.len(),
                    "replayed manifest-published compaction into manifest"
                );
                Ok(manifest_changed)
            }
            (_, CompactionManifestState::Partial) => self.handle_recovery_issue(format!(
                "compaction publication intent has partial manifest visibility (phase={phase:?}, removed={removed:?})"
            )),
        }
    }

    fn compaction_output_is_superseded_by_published_retry(
        &self,
        current_index: usize,
        cf_id: crate::types::ColumnFamilyId,
        removed: &[String],
        added: &[crate::runtime::FileMeta],
        intents: &[crate::runtime::IntentLogEntry],
    ) -> bool {
        intents.iter().enumerate().any(|(index, intent)| {
            if index == current_index {
                return false;
            }
            let crate::runtime::IntentLogEntry::CompactionPublish {
                phase: retry_phase,
                cf_id: retry_cf_id,
                removed: retry_removed,
                added: retry_added,
            } = intent
            else {
                return false;
            };

            *retry_cf_id == cf_id
                && Self::same_file_name_set(
                    retry_removed.iter().map(String::as_str),
                    removed.iter().map(String::as_str),
                )
                && !Self::same_file_name_set(
                    retry_added.iter().map(|meta| meta.name.as_str()),
                    added.iter().map(|meta| meta.name.as_str()),
                )
                && (*retry_phase == PublicationPhase::ManifestPublished
                    || self.compaction_manifest_state(retry_removed, retry_added)
                        == CompactionManifestState::Published)
        })
    }

    fn compaction_retry_groups_are_unambiguous(
        &mut self,
        intents: &[crate::runtime::IntentLogEntry],
    ) -> MidgeResult<bool> {
        for (left_index, left) in intents.iter().enumerate() {
            let crate::runtime::IntentLogEntry::CompactionPublish {
                phase: left_phase,
                cf_id: left_cf_id,
                removed: left_removed,
                added: left_added,
            } = left
            else {
                continue;
            };
            for right in intents.iter().skip(left_index + 1) {
                let crate::runtime::IntentLogEntry::CompactionPublish {
                    phase: right_phase,
                    cf_id: right_cf_id,
                    removed: right_removed,
                    added: right_added,
                } = right
                else {
                    continue;
                };
                let same_inputs = left_cf_id == right_cf_id
                    && Self::same_file_name_set(
                        left_removed.iter().map(String::as_str),
                        right_removed.iter().map(String::as_str),
                    );
                let different_outputs = !Self::same_file_name_set(
                    left_added.iter().map(|meta| meta.name.as_str()),
                    right_added.iter().map(|meta| meta.name.as_str()),
                );
                if !same_inputs || !different_outputs {
                    continue;
                }

                let both_declared_published = *left_phase == PublicationPhase::ManifestPublished
                    && *right_phase == PublicationPhase::ManifestPublished;
                let both_manifest_visible = self
                    .compaction_manifest_state(left_removed, left_added)
                    == CompactionManifestState::Published
                    && self.compaction_manifest_state(right_removed, right_added)
                        == CompactionManifestState::Published;
                if both_declared_published || both_manifest_visible {
                    return self.handle_recovery_issue(format!(
                        "multiple compaction retries claim publication authority for column family {left_cf_id} and inputs {left_removed:?}"
                    ));
                }
            }
        }
        Ok(true)
    }

    fn roll_back_superseded_compaction_output(
        &mut self,
        added: &[crate::runtime::FileMeta],
    ) -> MidgeResult<()> {
        for file_meta in added {
            if self.manifest_has_file(&file_meta.name)
                || !self.sst_dir.join(&file_meta.name).exists()
            {
                continue;
            }
            if !self.validate_local_recovered_sst(file_meta)? {
                return Ok(());
            }
            self.delete_sst_if_exists(&file_meta.name)?;
        }
        tracing::info!(
            added_count = added.len(),
            "rolled back superseded output-durable compaction retry"
        );
        Ok(())
    }

    fn reject_compaction_for_inactive_column_family(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        added: &[crate::runtime::FileMeta],
    ) -> MidgeResult<()> {
        for file_meta in added {
            if self.manifest_has_file(&file_meta.name)
                || !self.sst_dir.join(&file_meta.name).exists()
            {
                continue;
            }
            if !self.validate_local_recovered_sst(file_meta)? {
                return Ok(());
            }
            self.delete_sst_if_exists(&file_meta.name)?;
        }
        self.mark_persistence_anomaly();
        tracing::warn!(
            cf_id,
            added_count = added.len(),
            "rejected compaction publication intent for inactive column family"
        );
        Ok(())
    }

    fn compaction_manifest_state(
        &self,
        removed: &[String],
        added: &[crate::runtime::FileMeta],
    ) -> CompactionManifestState {
        let all_inputs_present = removed.iter().all(|name| self.manifest_has_file(name));
        let all_inputs_absent = removed.iter().all(|name| !self.manifest_has_file(name));
        let all_outputs_present = added
            .iter()
            .all(|file_meta| self.manifest_has_file(&file_meta.name));
        let all_outputs_absent = added
            .iter()
            .all(|file_meta| !self.manifest_has_file(&file_meta.name));

        if all_inputs_present && all_outputs_absent {
            CompactionManifestState::Prepublication
        } else if all_inputs_absent && all_outputs_present {
            CompactionManifestState::Published
        } else {
            CompactionManifestState::Partial
        }
    }

    /// Return durable, non-authoritative compaction outputs that may have
    /// reached remote storage and must be removed before replay clears their
    /// intent. The caller validates object identity before deletion.
    pub(crate) fn non_authoritative_compaction_outputs_for_remote_cleanup(
        &self,
    ) -> MidgeResult<Vec<crate::runtime::FileMeta>> {
        let intents = &self.intent_log;
        let mut candidates = std::collections::BTreeMap::<String, crate::runtime::FileMeta>::new();

        for (intent_index, intent) in intents.iter().enumerate() {
            let crate::runtime::IntentLogEntry::CompactionPublish {
                phase,
                cf_id,
                removed,
                added,
            } = intent
            else {
                continue;
            };
            if *phase != PublicationPhase::OutputDurable {
                continue;
            }
            // A committed CF drop removes the compaction inputs from the
            // manifest before startup can classify this OutputDurable intent.
            // In that state the usual input-present prepublication proof is
            // unavailable, but an output absent from the manifest cannot be
            // authoritative for an inactive, monotonically identified CF.
            // Keep the object-identity validation at the deletion boundary.
            let inactive_output_set = self.get_cf(*cf_id).is_none()
                && added
                    .iter()
                    .all(|file_meta| !self.manifest_has_file(&file_meta.name));
            let non_authoritative = self.compaction_manifest_state(removed, added)
                == CompactionManifestState::Prepublication
                || self.compaction_output_is_superseded_by_published_retry(
                    intent_index,
                    *cf_id,
                    removed,
                    added,
                    intents,
                )
                || inactive_output_set;
            if !non_authoritative {
                continue;
            }

            for file_meta in added {
                if self.manifest_has_file(&file_meta.name) {
                    continue;
                }
                if let Some(existing) = candidates.get(&file_meta.name) {
                    if existing.size_bytes != file_meta.size_bytes
                        || existing.content_crc32c != file_meta.content_crc32c
                        || existing.cf_id != file_meta.cf_id
                    {
                        return Err(MidgeError::Corruption(format!(
                            "conflicting remote cleanup proofs for compaction output '{}'",
                            file_meta.name
                        )));
                    }
                } else {
                    candidates.insert(file_meta.name.clone(), file_meta.clone());
                }
            }
        }

        Ok(candidates.into_values().collect())
    }

    /// Replay intent log to recover incomplete mutations
    /// Called during startup to apply any interrupted manifest or durability changes
    pub fn replay_intent_log(&mut self) -> MidgeResult<()> {
        if self.intent_log.is_empty() {
            return Ok(());
        }

        self.diagnostics.record(|m| {
            m.record_intent_log_replay(self.intent_log.len() as u64);
        });

        self.intent_log_replay_runs = self.intent_log_replay_runs.saturating_add(1);
        self.intent_log_entries_replayed = self
            .intent_log_entries_replayed
            .saturating_add(self.intent_log.len() as u64);

        tracing::info!(
            intent_count = self.intent_log.len(),
            "replaying intent log during recovery"
        );

        let intents = self.intent_log.clone();
        let compaction_replay_safe = self.compaction_retry_groups_are_unambiguous(&intents)?;
        let mut manifest_changed = false;

        for (intent_index, intent) in intents.iter().enumerate() {
            match intent {
                crate::runtime::IntentLogEntry::FlushPublish {
                    phase, file_meta, ..
                } => {
                    manifest_changed |= self.replay_flush_publish_intent(*phase, file_meta)?;
                }
                crate::runtime::IntentLogEntry::CompactionPublish {
                    phase,
                    cf_id,
                    removed,
                    added,
                } => {
                    if !compaction_replay_safe {
                        continue;
                    }
                    if added.iter().any(|file_meta| file_meta.cf_id != *cf_id) {
                        let _ = self.handle_recovery_issue(format!(
                            "compaction publication intent for column family {cf_id} contains output metadata for another family"
                        ))?;
                        continue;
                    }
                    if self.get_cf(*cf_id).is_none() {
                        self.reject_compaction_for_inactive_column_family(*cf_id, added)?;
                        continue;
                    }
                    if *phase == PublicationPhase::OutputDurable
                        && self.compaction_output_is_superseded_by_published_retry(
                            intent_index,
                            *cf_id,
                            removed,
                            added,
                            &intents,
                        )
                    {
                        self.roll_back_superseded_compaction_output(added)?;
                        continue;
                    }
                    manifest_changed |=
                        self.replay_compaction_publish_intent(*phase, removed, added)?;
                }
                crate::runtime::IntentLogEntry::CompactionApplied { removed, added } => {
                    manifest_changed |= self.replay_compaction_publish_intent(
                        PublicationPhase::ManifestPublished,
                        removed,
                        added,
                    )?;
                }
                crate::runtime::IntentLogEntry::SstAdded { file_meta } => {
                    manifest_changed |= self.replay_flush_publish_intent(
                        PublicationPhase::ManifestPublished,
                        file_meta,
                    )?;
                }
                _ => {
                    // Other intents (WalSynced, DataUploaded, etc.) don't require replay
                    // They are informational and don't affect the recoverable state
                }
            }
        }

        if manifest_changed {
            if let Err(error) = self.persist_manifest_checkpoint() {
                let _ = self.handle_recovery_issue(format!(
                    "failed to persist manifest checkpoint after intent replay: {error}"
                ))?;
            }
        }

        self.restore_sequence_floor_from_manifest();

        // Clear the intent log after successful replay
        // New intents will be written during normal operation
        self.intent_log.clear();
        if !self.is_memory_mode() {
            if let Err(error) =
                crate::runtime::IntentPersistence::save(&self.db_path, &self.intent_log)
            {
                let _ = self.handle_recovery_issue(format!(
                    "failed to clear intent log after replay: {error}"
                ))?;
            }
        }

        tracing::info!("intent log replay complete and cleared");
        Ok(())
    }
}

#[cfg(test)]
mod salvage_quarantine_tests {
    use super::*;
    use std::io::Write as _;

    fn frame(key: &'static [u8], seq: u64) -> Vec<u8> {
        let record = crate::wal::WalRecord::new(
            crate::wal::types::WalOpKind::Put,
            bytes::Bytes::from_static(key),
            Some(bytes::Bytes::from_static(b"value")),
            seq,
            1,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode record");
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &payload).expect("frame record");
        frame
    }

    fn frame_with_value(key: &'static [u8], value: &'static [u8], seq: u64) -> Vec<u8> {
        let record = crate::wal::WalRecord::new(
            crate::wal::types::WalOpKind::Put,
            bytes::Bytes::from_static(key),
            Some(bytes::Bytes::from_static(value)),
            seq,
            1,
        );
        let payload = crate::wal::encoding::encode(&record).expect("encode record");
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &payload).expect("frame record");
        frame
    }

    /// `1.wal` writes `k` at sequence 7; `wal.log` writes a different value
    /// for `k` at the same sequence.
    fn conflicting_repeated_sequence(wal_dir: &std::path::Path) {
        write(
            &wal_dir.join(crate::wal::segment_file_name(1)),
            &[frame_with_value(b"k", b"a", 7)],
        );
        write(
            &wal_dir.join(crate::wal::ACTIVE_FILE_NAME),
            &[frame_with_value(b"k", b"b", 7)],
        );
    }

    #[test]
    fn should_reject_conflicting_repeated_sequence_when_replaying_local_wal() {
        // Arrange: the same sequence carrying two values breaks MVCC; local
        // and cloud replay must refuse it the same way (#524).
        let directory = tempfile::tempdir().expect("temp dir");
        let wal_dir = directory.path().join("wal");
        std::fs::create_dir(&wal_dir).expect("create wal dir");
        conflicting_repeated_sequence(&wal_dir);

        // Act
        let result = RuntimeState::replay_wal(
            false,
            &wal_dir,
            &directory.path().join("sst"),
            crate::config::RecoveryPolicy::Strict,
            &Manifest::default(),
            HashMap::new(),
        );

        // Assert
        let Err(error) = result else {
            panic!("strict replay must reject a conflicting repeated sequence");
        };
        assert!(
            error
                .to_string()
                .contains("conflicting or repeated WAL sequence 7"),
            "{error}"
        );
    }

    #[test]
    fn should_keep_prefix_when_salvage_meets_conflicting_repeated_sequence() {
        // Arrange
        let directory = tempfile::tempdir().expect("temp dir");
        let wal_dir = directory.path().join("wal");
        std::fs::create_dir(&wal_dir).expect("create wal dir");
        conflicting_repeated_sequence(&wal_dir);

        // Act
        let recovered = salvage_replay(&wal_dir, &directory.path().join("sst"));

        // Assert: the first write survives and replay stops at the
        // conflicting frame, which is not counted as replayed.
        assert!(recovered.opened_in_salvage_mode);
        assert_eq!(recovered.records_replayed, 1);
    }

    #[test]
    fn should_raise_sequence_floor_over_frames_after_a_record_level_salvage_stop() {
        // Arrange: all four frames are individually readable. Replay stops
        // at the conflicting second frame, leaving the later two quarantined.
        let directory = tempfile::tempdir().expect("temp dir");
        let wal_dir = directory.path().join("wal");
        std::fs::create_dir(&wal_dir).expect("create wal dir");
        write(
            &wal_dir.join(crate::wal::ACTIVE_FILE_NAME),
            &[
                frame_with_value(b"k", b"a", 7),
                frame_with_value(b"k", b"b", 7),
                frame_with_value(b"x", b"x", 8),
                frame_with_value(b"y", b"y", 9),
            ],
        );

        // Act
        let recovered = salvage_replay(&wal_dir, &directory.path().join("sst"));

        // Assert
        assert!(recovered.opened_in_salvage_mode);
        assert_eq!(recovered.records_replayed, 1);
        assert!(
            recovered.recovered_sequence >= 9,
            "new writes must not reuse quarantined sequences: {}",
            recovered.recovered_sequence
        );
    }

    fn write(path: &std::path::Path, parts: &[Vec<u8>]) {
        let mut file = std::fs::File::create(path).expect("create wal file");
        for part in parts {
            file.write_all(part).expect("write wal bytes");
        }
        file.sync_all().expect("sync wal file");
    }

    /// `3.wal` holds two good frames, then a CRC-corrupt frame, then a good
    /// one; `4.wal` and `wal.log` hold later sequences salvage never reaches.
    fn corrupt_middle_segment(wal_dir: &std::path::Path) -> u64 {
        let prefix = [frame(b"a", 1), frame(b"b", 2)];
        let mut corrupt = frame(b"c", 3);
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xFF;
        let prefix_len: usize = prefix.iter().map(Vec::len).sum();
        write(
            &wal_dir.join(crate::wal::segment_file_name(3)),
            &[
                prefix[0].clone(),
                prefix[1].clone(),
                corrupt,
                frame(b"d", 4),
            ],
        );
        write(
            &wal_dir.join(crate::wal::segment_file_name(4)),
            &[frame(b"e", 5)],
        );
        write(
            &wal_dir.join(crate::wal::ACTIVE_FILE_NAME),
            &[frame(b"f", 6)],
        );
        prefix_len as u64
    }

    fn salvage_replay(wal_dir: &std::path::Path, sst_dir: &std::path::Path) -> WalRecoveryState {
        RuntimeState::replay_wal(
            false,
            wal_dir,
            sst_dir,
            crate::config::RecoveryPolicy::Salvage,
            &Manifest::default(),
            HashMap::new(),
        )
        .expect("salvage replay")
    }

    #[test]
    fn should_set_aside_unreplayed_wal_when_salvage_stops_at_corrupt_segment() {
        // Arrange
        let directory = tempfile::tempdir().expect("temp dir");
        let wal_dir = directory.path().join("wal");
        std::fs::create_dir(&wal_dir).expect("create wal dir");
        let prefix_len = corrupt_middle_segment(&wal_dir);

        // Act
        let recovered = salvage_replay(&wal_dir, &directory.path().join("sst"));

        // Assert
        assert!(recovered.opened_in_salvage_mode);
        assert!(
            recovered.recovered_sequence >= 6,
            "new writes must stay above sequences set aside: {}",
            recovered.recovered_sequence
        );
        assert_eq!(
            std::fs::metadata(wal_dir.join(crate::wal::segment_file_name(3)))
                .expect("stop segment keeps its replayed prefix")
                .len(),
            prefix_len
        );
        assert!(!wal_dir.join(crate::wal::segment_file_name(4)).exists());
        assert!(!wal_dir.join(crate::wal::ACTIVE_FILE_NAME).exists());
        let quarantine = std::fs::read_dir(&wal_dir)
            .expect("read wal dir")
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("salvaged-"))
            .expect("quarantine directory")
            .path();
        for name in [
            crate::wal::segment_file_name(3),
            crate::wal::segment_file_name(4),
            crate::wal::ACTIVE_FILE_NAME.to_string(),
        ] {
            assert!(quarantine.join(&name).exists(), "{name} must be preserved");
        }
    }

    #[test]
    fn should_reopen_strictly_after_salvage_sets_aside_unreplayed_wal() {
        // Arrange
        let directory = tempfile::tempdir().expect("temp dir");
        let wal_dir = directory.path().join("wal");
        std::fs::create_dir(&wal_dir).expect("create wal dir");
        corrupt_middle_segment(&wal_dir);
        salvage_replay(&wal_dir, &directory.path().join("sst"));
        write(
            &wal_dir.join(crate::wal::ACTIVE_FILE_NAME),
            &[frame(b"after", 7)],
        );

        // Act
        let reopened = RuntimeState::replay_wal(
            false,
            &wal_dir,
            &directory.path().join("sst"),
            crate::config::RecoveryPolicy::Strict,
            &Manifest::default(),
            HashMap::new(),
        );

        // Assert
        let reopened = reopened.expect("writes after salvage must not sit behind corruption");
        assert!(!reopened.opened_in_salvage_mode);
        assert_eq!(reopened.recovered_sequence, 7);
    }
}
