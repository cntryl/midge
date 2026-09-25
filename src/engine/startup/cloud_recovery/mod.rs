use super::cloud_io::BlockingCloudIo;
use super::{CloudSstRecoveryProof, CloudStartupRecovery};
use crate::common::{MidgeError, MidgeResult};
use crate::config::RecoveryPolicy;
use crate::io::Fs as _;
use crate::runtime::RuntimeState;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod metadata;
mod sst_proof;

#[cfg(test)]
mod tests;

type LocalWalPaths = (
    std::collections::BTreeMap<u64, Vec<PathBuf>>,
    Option<PathBuf>,
);

/// Why an authoritative cloud SST failed validation during recovery.
enum SstLoss {
    /// The object is missing or does not match the manifest: it is gone for good.
    Definitive(MidgeError),
    /// The check itself failed (timeout, I/O, permissions), so nothing is known
    /// about the object.
    Indeterminate(MidgeError),
}

fn local_sst_is_definitively_missing(sst_dir: &Path, error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
        && std::fs::metadata(sst_dir).is_ok_and(|metadata| metadata.is_dir())
}

/// What salvage recovery does with one manifest SST after validating it.
enum SstDisposition {
    Retain,
    /// Dropped because the object is definitively lost; the drop is made durable.
    DropDefinitive,
    /// Retained because the validation check did not establish object loss.
    RetainIndeterminate,
}

impl CloudStartupRecovery {
    pub(crate) fn cleanup_non_authoritative_compaction_outputs(
        state: &mut RuntimeState,
        storage: &crate::storage::HybridStorage,
        candidates: &[crate::runtime::FileMeta],
    ) -> MidgeResult<std::collections::BTreeSet<String>> {
        let mut cleaned = std::collections::BTreeSet::new();
        for file_meta in candidates {
            let key = crate::cloud_layout::object_key(&file_meta.name);
            let deadline =
                crate::common::OperationDeadline::from_budget(storage.storage_io_timeout());
            let metadata = storage
                .remote_range_metadata_optional_within(&key, &deadline)
                .map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to prove remote compaction cleanup candidate '{}': {error}",
                        file_meta.name
                    ))
                })?;
            if let Some(metadata) = metadata {
                let pinned = Arc::new(
                    crate::storage::remote_sst::RemoteSstFs::for_object(
                        Arc::clone(&state.fs),
                        storage.remote_sst_backend(),
                        key.clone(),
                        metadata.clone(),
                        storage.storage_io_timeout(),
                    )
                    .with_deadline(deadline),
                );
                RuntimeState::validate_sst_fs_proof(pinned, file_meta)?;
                // The intent proves this name is non-authoritative and the
                // remote checksum proves its publication. Discard its local
                // cache before deleting that remote proof, even if the cache
                // is corrupt. A durable directory barrier prevents a crash
                // from resurrecting corrupt local bytes after remote cleanup.
                let local_path = crate::io::FsPath::new(key.clone());
                match state.fs.remove_file(&local_path) {
                    Ok(()) => state
                        .fs
                        .sync_dir(
                            &crate::io::FsPath::new("sst"),
                            crate::io::Durability::Durable,
                        )
                        .map_err(MidgeError::from)?,
                    Err(crate::io::FsError::NotFound(_)) => {}
                    Err(error) => {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to remove proven non-authoritative local compaction output '{}': {error}",
                            file_meta.name
                        )));
                    }
                }
                storage
                    .delete_remote_object_by_identity_blocking_within(&key, &metadata, &deadline)
                    .map_err(|error| {
                        MidgeError::RecoveryFailed(format!(
                            "failed to conditionally delete remote compaction cleanup candidate '{}': {error}",
                            file_meta.name
                        ))
                    })?;
            }
            cleaned.insert(file_meta.name.clone());
        }
        Ok(cleaned)
    }

    /// Validate the authoritative inventory without materializing the local cache.
    /// SST metadata and data blocks are checked when a reader requests them;
    /// ordinary startup must not scan or copy the object-store dataset.
    pub(in crate::engine) fn ensure_local_sst_cache_from_cloud(
        state: &mut RuntimeState,
        cloud_root: &Path,
    ) -> MidgeResult<()> {
        let remote_sst_dir = cloud_root.join("sst");
        let mut retained_files = Vec::with_capacity(state.manifest.files.len());
        let mut manifest_changed = false;
        let mut definitively_lost: Vec<String> = Vec::new();

        for file in state.manifest.files.clone() {
            let remote_path = remote_sst_dir.join(&file.name);
            let validation = std::fs::metadata(&remote_path)
                .map_err(|error| {
                    let loss = MidgeError::RecoveryFailed(format!(
                        "authoritative cloud SST '{}' is unavailable: {error}",
                        file.name
                    ));
                    if local_sst_is_definitively_missing(&remote_sst_dir, &error) {
                        SstLoss::Definitive(loss)
                    } else {
                        SstLoss::Indeterminate(loss)
                    }
                })
                .and_then(|metadata| {
                    Self::validate_manifest_sst_size(&file, metadata.len())
                        .map_err(SstLoss::Definitive)
                });
            match Self::retain_manifest_sst_after_metadata_validation(state, &file, validation)? {
                SstDisposition::Retain | SstDisposition::RetainIndeterminate => {
                    retained_files.push(file);
                }
                SstDisposition::DropDefinitive => {
                    definitively_lost.push(file.name.clone());
                    manifest_changed = true;
                }
            }
        }

        if manifest_changed {
            Self::commit_manifest_removals(state, retained_files, &definitively_lost)?;
            state.restore_sequence_floor_from_manifest();
        }

        Ok(())
    }

    pub(super) fn recovery_staging_fs(
        db_path: &Path,
    ) -> MidgeResult<Arc<dyn crate::io::traits::Fs>> {
        let real = crate::io::real::RealFs::new(db_path).map_err(|error| {
            MidgeError::RecoveryFailed(format!(
                "failed to initialize recovery staging filesystem: {error}"
            ))
        })?;
        Ok(Arc::new(real))
    }

    pub(crate) fn hydrate_cloud_metadata(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        let staging_fs = Self::recovery_staging_fs(db_path)?;
        let mut metadata_objects = Vec::new();
        let mut snapshot_sequence = None;
        let mut manifest_sequence = None;
        let mut has_manifest_journal = false;

        for file_name in crate::metadata::files::CLOUD_MIRRORED {
            let key = crate::cloud_layout::CloudObjectLayout::metadata_key(file_name);
            let data = match BlockingCloudIo::new(cloud).get_optional(&key) {
                Ok(Some(data)) => data,
                Ok(None) => continue,
                Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                    tracing::warn!(%error, key = %key, "skipping cloud metadata object during salvage open");
                    continue;
                }
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to download cloud metadata '{key}': {error}"
                    )))
                }
            };

            if file_name == &crate::metadata::files::JOURNAL {
                has_manifest_journal = true;
            }
            if let Some(sequence) = crate::metadata::files::manifest_sequence(file_name, &data)? {
                match *file_name {
                    crate::metadata::files::MANIFEST_SNAPSHOT => snapshot_sequence = Some(sequence),
                    crate::metadata::files::MANIFEST => manifest_sequence = Some(sequence),
                    _ => {}
                }
            }

            metadata_objects.push((*file_name, data));
        }

        let mut metadata_to_skip = None;
        if !has_manifest_journal {
            if let (Some(snapshot), Some(manifest)) = (snapshot_sequence, manifest_sequence) {
                if snapshot != manifest {
                    if recovery_policy == RecoveryPolicy::Strict {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "mixed cloud manifest metadata without journal: manifest.snapshot.json sequence {snapshot}, manifest.json sequence {manifest}"
                        )));
                    }
                    let skip_metadata = if manifest >= snapshot {
                        "manifest.snapshot.json"
                    } else {
                        "manifest.json"
                    };
                    metadata_to_skip = Some(skip_metadata);
                    tracing::warn!(
                        snapshot_sequence = snapshot,
                        manifest_sequence = manifest,
                        skip = skip_metadata,
                        "skipping mixed cloud manifest metadata during salvage open"
                    );
                }
            }
        }

        for (file_name, data) in metadata_objects {
            if metadata_to_skip == Some(file_name) {
                continue;
            }
            let temp_path = crate::io::traits::FsPath::new(format!("{file_name}.tmp"));
            let target_path = crate::io::traits::FsPath::new(file_name);
            crate::io::staging::stage_bytes(
                &staging_fs,
                &temp_path,
                &target_path,
                &data,
                MidgeError::RecoveryFailed,
            )?;
        }

        Ok(())
    }

    pub(crate) fn mirror_cloud_metadata(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        let local_manifest = match Self::load_local_manifest_for_cloud_metadata_mirror(db_path) {
            Ok(manifest) => manifest,
            Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                tracing::warn!(%error, "skipping metadata mirror during salvage open because local manifest could not be loaded");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let local_manifest_sequence = local_manifest.last_persisted_sequence;

        Self::ensure_remote_manifest_metadata_not_ahead(cloud, local_manifest_sequence)?;

        for file_name in crate::metadata::files::CLOUD_MIRRORED {
            let local_path = db_path.join(file_name);
            if !local_path.exists() {
                continue;
            }

            let data = match std::fs::read(&local_path) {
                Ok(data) => data,
                Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                    tracing::warn!(%error, file = %local_path.display(), "skipping metadata mirror during salvage open");
                    if *file_name == crate::metadata::files::FORMAT {
                        return Ok(());
                    }
                    continue;
                }
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to read local metadata '{}': {}",
                        local_path.display(),
                        error
                    )))
                }
            };

            let key = crate::cloud_layout::CloudObjectLayout::metadata_key(file_name);
            if let Err(error) = Self::blocking_conditional_cloud_metadata_put(
                cloud,
                file_name,
                &key,
                data,
                local_manifest_sequence,
            ) {
                if recovery_policy == RecoveryPolicy::Salvage {
                    tracing::warn!(%error, key = %key, "skipping metadata mirror during salvage open");
                    if *file_name == crate::metadata::files::FORMAT {
                        return Ok(());
                    }
                    continue;
                }
                return Err(MidgeError::RecoveryFailed(format!(
                    "failed to mirror cloud metadata '{key}': {error}"
                )));
            }
        }

        Ok(())
    }

    pub(in crate::engine) fn reject_cloud_wal_without_catalog(
        cloud: &crate::storage::cloud::CloudStorage,
    ) -> MidgeResult<()> {
        let io = BlockingCloudIo::new(cloud);
        if io
            .head_optional(crate::wal::cloud_catalog::OBJECT_KEY)?
            .is_some()
            || io
                .head_optional(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)?
                .is_some()
        {
            return Ok(());
        }
        let untracked = io
            .list(crate::cloud_layout::CloudObjectLayout::WAL_PREFIX)?
            .into_iter()
            .map(|key| cloud.strip_namespace(&key).to_string())
            .find(|key| crate::wal::parse_segment_id(key).is_some());
        if let Some(key) = untracked {
            return Err(Self::cloud_wal_without_catalog_error(&key));
        }
        Ok(())
    }

    pub(in crate::engine) fn reject_simulated_cloud_wal_without_catalog(
        cloud_wal_dir: &Path,
    ) -> MidgeResult<()> {
        let has_catalog_copy = [
            crate::wal::cloud_catalog::OBJECT_KEY,
            crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
        ]
        .into_iter()
        .any(|key| {
            let catalog_name = key
                .strip_prefix(crate::cloud_layout::CloudObjectLayout::WAL_PREFIX)
                .unwrap_or(key);
            cloud_wal_dir.join(catalog_name).exists()
        });
        if has_catalog_copy {
            return Ok(());
        }
        match std::fs::read_dir(cloud_wal_dir) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(MidgeError::RecoveryFailed(format!(
                    "failed to inspect simulated cloud WAL directory '{}': {error}",
                    cloud_wal_dir.display()
                )))
            }
        }
        let mut pending_dirs = vec![cloud_wal_dir.to_path_buf()];
        while let Some(dir) = pending_dirs.pop() {
            let entries = std::fs::read_dir(&dir).map_err(|error| {
                MidgeError::RecoveryFailed(format!(
                    "failed to inspect simulated cloud WAL directory '{}': {error}",
                    dir.display()
                ))
            })?;
            for entry in entries {
                let entry = entry.map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to inspect simulated cloud WAL directory '{}': {error}",
                        dir.display()
                    ))
                })?;
                let file_type = entry.file_type().map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to inspect simulated cloud WAL object '{}': {error}",
                        entry.path().display()
                    ))
                })?;
                if file_type.is_dir() {
                    pending_dirs.push(entry.path());
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let object_path = entry.path();
                let relative = object_path.strip_prefix(cloud_wal_dir).map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to resolve simulated cloud WAL object '{}': {error}",
                        object_path.display()
                    ))
                })?;
                let relative = relative
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                let key = format!(
                    "{}{relative}",
                    crate::cloud_layout::CloudObjectLayout::WAL_PREFIX
                );
                if crate::wal::parse_segment_id(&key).is_some() {
                    return Err(Self::cloud_wal_without_catalog_error(&key));
                }
            }
        }
        Ok(())
    }

    fn cloud_wal_without_catalog_error(key: &str) -> MidgeError {
        let relative = key
            .strip_prefix(crate::cloud_layout::CloudObjectLayout::WAL_PREFIX)
            .unwrap_or(key);
        if relative.contains('/') {
            return MidgeError::RecoveryFailed(format!(
                "epoch-scoped cloud WAL object '{key}' exists without publication catalog format v1; refusing ambiguous recovery because object presence is not authority"
            ));
        }
        MidgeError::RecoveryFailed(format!(
            "legacy segment-only cloud WAL object '{key}' is unsupported by publication catalog format v1; migrate or restore with a compatible Midge release"
        ))
    }

    pub(super) fn collect_local_wal_paths(
        local_wal_dir: &Path,
        recovery_policy: RecoveryPolicy,
        opened_in_salvage_mode: &mut bool,
    ) -> MidgeResult<Option<LocalWalPaths>> {
        let entries = match std::fs::read_dir(local_wal_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                tracing::warn!(
                    %error,
                    path = %local_wal_dir.display(),
                    "could not list intact local WAL during salvage open"
                );
                *opened_in_salvage_mode = true;
                return Ok(None);
            }
            Err(error) => {
                return Err(MidgeError::RecoveryFailed(format!(
                    "failed to list intact local WAL directory '{}': {error}",
                    local_wal_dir.display()
                )))
            }
        };
        let mut segment_paths = std::collections::BTreeMap::<u64, Vec<PathBuf>>::new();
        let mut active_path = None;
        for entry_result in entries {
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                    tracing::warn!(
                        %error,
                        "skipping unreadable local WAL directory entry during salvage"
                    );
                    *opened_in_salvage_mode = true;
                    continue;
                }
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to read intact local WAL directory entry: {error}"
                    )))
                }
            };
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                    tracing::warn!(
                        %error,
                        path = %entry.path().display(),
                        "skipping local WAL entry with unreadable type during salvage"
                    );
                    *opened_in_salvage_mode = true;
                    continue;
                }
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to inspect intact local WAL entry '{}': {error}",
                        entry.path().display()
                    )))
                }
            };
            if !file_type.is_file() {
                continue;
            }
            let source_path = entry.path();
            let Some(source_name) = source_path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if source_name == crate::wal::ACTIVE_FILE_NAME {
                active_path = Some(source_path);
                continue;
            }
            let Some(segment_id) = crate::wal::parse_segment_id(source_name) else {
                continue;
            };
            segment_paths
                .entry(segment_id)
                .or_default()
                .push(source_path);
        }
        Ok(Some((segment_paths, active_path)))
    }

    pub(super) fn quarantine_local_wal_alias(path: &Path) -> MidgeResult<()> {
        let retained_path = Self::unused_retained_path(path)?;
        std::fs::rename(path, &retained_path).map_err(|error| {
            MidgeError::RecoveryFailed(format!(
                "failed to quarantine local WAL alias '{}' as '{}': {error}",
                path.display(),
                retained_path.display()
            ))
        })
    }

    /// Durably copy a local WAL file beside itself before salvage rewrites
    /// it, so the discarded bytes stay available for inspection.
    pub(super) fn retain_local_wal_copy(path: &Path) -> MidgeResult<()> {
        let retained_path = Self::unused_retained_path(path)?;
        (|| {
            // Use ordinary file handles for Windows sharing semantics and a
            // bounded copy, including when the source WAL is open elsewhere.
            let mut source = std::fs::File::open(path)?;
            let mut retained = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&retained_path)?;
            std::io::copy(&mut source, &mut retained)?;
            retained.sync_all()
        })()
        .map_err(|error| {
            MidgeError::RecoveryFailed(format!(
                "failed to retain a copy of local WAL '{}' as '{}': {error}",
                path.display(),
                retained_path.display()
            ))
        })
    }

    fn unused_retained_path(path: &Path) -> MidgeResult<PathBuf> {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                MidgeError::RecoveryFailed(format!(
                    "local WAL alias '{}' has no UTF-8 filename",
                    path.display()
                ))
            })?;
        for suffix in 0_u32..=u32::MAX {
            let retained_name = if suffix == 0 {
                format!("{file_name}.salvage-retained")
            } else {
                format!("{file_name}.salvage-retained.{suffix}")
            };
            let retained_path = path.with_file_name(retained_name);
            if !retained_path.exists() {
                return Ok(retained_path);
            }
        }
        Err(MidgeError::RecoveryFailed(format!(
            "could not allocate quarantine name for local WAL alias '{}'",
            path.display()
        )))
    }

    /// Validate cloud authority using object metadata only. The historical name
    /// is retained for the startup boundary; this no longer populates a cache.
    pub(crate) fn ensure_local_sst_cache_from_cloud_storage(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
    ) -> MidgeResult<()> {
        let mut retained_files = Vec::with_capacity(state.manifest.files.len());
        let mut manifest_changed = false;
        let mut definitively_lost: Vec<String> = Vec::new();

        for file in state.manifest.files.clone() {
            let key = crate::cloud_layout::object_key(&file.name);
            let validation = BlockingCloudIo::new(cloud)
                .head_optional(&key)
                .map_err(SstLoss::Indeterminate)
                .and_then(|metadata| {
                    metadata.ok_or_else(|| {
                        SstLoss::Definitive(MidgeError::RecoveryFailed(format!(
                            "authoritative cloud SST '{}' is missing",
                            file.name
                        )))
                    })
                })
                .and_then(|metadata| {
                    Self::validate_manifest_sst_size(&file, metadata.size)
                        .map_err(SstLoss::Definitive)
                });
            match Self::retain_manifest_sst_after_metadata_validation(state, &file, validation)? {
                SstDisposition::Retain | SstDisposition::RetainIndeterminate => {
                    retained_files.push(file);
                }
                SstDisposition::DropDefinitive => {
                    definitively_lost.push(file.name.clone());
                    manifest_changed = true;
                }
            }
        }

        if manifest_changed {
            Self::commit_manifest_removals(state, retained_files, &definitively_lost)?;
            state.restore_sequence_floor_from_manifest();
        }

        Ok(())
    }

    fn validate_manifest_sst_size(
        file: &crate::metadata::FileMeta,
        actual_size: u64,
    ) -> MidgeResult<()> {
        if actual_size != file.size_bytes {
            return Err(MidgeError::RecoveryFailed(format!(
                "authoritative cloud SST '{}' size {actual_size} does not match manifest {}",
                file.name, file.size_bytes
            )));
        }
        Ok(())
    }

    /// Persist a salvage-mode drop of manifest SSTs.
    ///
    /// Only `definitively_lost` names are journaled and snapshotted: the object is
    /// gone or does not match the manifest, so leaving the entry would make every
    /// later persist resurrect a file that cannot be read. Indeterminate losses stay
    /// in both the running and durable manifests so a later persist cannot erase a
    /// possibly live remote SST.
    fn commit_manifest_removals(
        state: &mut RuntimeState,
        retained_files: Vec<crate::metadata::FileMeta>,
        definitively_lost: &[String],
    ) -> MidgeResult<()> {
        if definitively_lost.is_empty() {
            state.manifest.files = retained_files;
            return crate::metadata::ManifestPersistence::save(&state.db_path, &state.manifest)
                .map_err(MidgeError::Internal);
        }
        let edits: Vec<crate::metadata::ManifestEdit> = definitively_lost
            .iter()
            .map(|name| crate::metadata::ManifestEdit::RemoveSst { name: name.clone() })
            .collect();
        let edit_id = state.manifest_store.append_batch(&edits)?;
        let mut durable = state.manifest.clone();
        durable
            .files
            .retain(|file| !definitively_lost.contains(&file.name));
        durable.note_applied_journal_edit(edit_id);
        state.manifest_store.save_snapshot(&durable)?;
        state.manifest.files = retained_files;
        state.manifest.note_applied_journal_edit(edit_id);
        Ok(())
    }

    fn retain_manifest_sst_after_metadata_validation(
        state: &mut RuntimeState,
        file: &crate::metadata::FileMeta,
        validation: Result<(), SstLoss>,
    ) -> MidgeResult<SstDisposition> {
        let Err(loss) = validation else {
            state.salvaged_local_ssts.remove(&file.name);
            return Ok(SstDisposition::Retain);
        };
        let (error, definitive) = match loss {
            SstLoss::Definitive(error) => (error, true),
            SstLoss::Indeterminate(error) => (error, false),
        };
        if state.recovery_policy() == RecoveryPolicy::Strict {
            return Err(MidgeError::RecoveryFailed(format!(
                "failed to validate authoritative cloud SST '{}': {error}",
                file.name
            )));
        }
        state.mark_opened_in_salvage_mode();
        state.mark_persistence_anomaly();
        // Salvage may retain a fully verified local copy when cloud authority
        // is unavailable. This exceptional path is deliberately conservative;
        // the normal inventory path never reads local or remote SST bodies.
        let retain = Self::retain_verified_local_sst(state, file)?;
        if retain {
            state.salvaged_local_ssts.insert(file.name.clone());
        } else {
            state.salvaged_local_ssts.remove(&file.name);
        }
        tracing::warn!(
            %error,
            sst_name = %file.name,
            retained_local_copy = retain,
            "authoritative cloud SST metadata validation failed during salvage"
        );
        Ok(match (retain, definitive) {
            (true, _) => SstDisposition::Retain,
            (false, true) => SstDisposition::DropDefinitive,
            (false, false) => SstDisposition::RetainIndeterminate,
        })
    }

    pub(super) fn retain_verified_local_sst(
        state: &RuntimeState,
        file: &crate::metadata::FileMeta,
    ) -> MidgeResult<bool> {
        if Self::local_sst_file_matches_manifest(&state.sst_dir.join(&file.name), file) {
            return Ok(true);
        }
        let secondary = state.db_path.join("hybrid_local/sst").join(&file.name);
        if !Self::local_sst_file_matches_manifest(&secondary, file) {
            return Ok(false);
        }
        // Move the verified secondary into the canonical read path without
        // allocating another full SST. A failed rename preserves its source.
        let fs = crate::io::RealFs::open_existing(&state.db_path)?;
        fs.rename_atomic(
            &crate::io::FsPath::new(format!("hybrid_local/sst/{}", file.name)),
            &crate::io::FsPath::new(format!("sst/{}", file.name)),
        )?;
        fs.sync_dir(
            &crate::io::FsPath::new("sst"),
            crate::io::Durability::Durable,
        )?;
        fs.sync_dir(
            &crate::io::FsPath::new("hybrid_local/sst"),
            crate::io::Durability::Durable,
        )?;
        Ok(true)
    }

    pub(crate) fn ensure_named_sst_cache_from_cloud_storage(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
        sst_proofs: impl IntoIterator<Item = CloudSstRecoveryProof>,
    ) -> MidgeResult<()> {
        if state.recovery_sst_fs.is_some() {
            // Intent replay verifies only the publication outputs it needs
            // through immutable, checksummed ranges. It must not stage the
            // complete output set on the ephemeral disk first.
            return Ok(());
        }
        let staging_fs = state.fs.clone();

        for proof in sst_proofs {
            Self::recover_named_sst_from_cloud(state, cloud, &staging_fs, &proof)?;
        }

        Ok(())
    }

    fn recover_named_sst_from_cloud(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
        staging_fs: &Arc<dyn crate::io::traits::Fs>,
        proof: &CloudSstRecoveryProof,
    ) -> MidgeResult<()> {
        let sst_name = proof.name.clone();
        let cloud_key = crate::cloud_layout::object_key(&sst_name);
        let local_path = state.sst_dir.join(&sst_name);
        if Self::local_sst_file_matches_proof(
            &local_path,
            &sst_name,
            proof.expected_size_bytes,
            proof.expected_crc32c,
        ) {
            return Self::validate_named_sst_against_cloud(
                state, cloud, &cloud_key, &sst_name, proof,
            );
        }
        Self::restore_named_sst_from_cloud(
            state,
            cloud,
            staging_fs,
            &cloud_key,
            &local_path,
            &sst_name,
            proof,
        )
    }

    fn validate_named_sst_against_cloud(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
        cloud_key: &str,
        sst_name: &str,
        proof: &CloudSstRecoveryProof,
    ) -> MidgeResult<()> {
        match BlockingCloudIo::new(cloud).object_proof_optional(cloud_key) {
            Ok(Some(cloud_proof)) => {
                if let Err(error) = Self::validate_sst_bytes_against_proof(
                    sst_name,
                    &cloud_proof.bytes,
                    proof.expected_size_bytes,
                    proof.expected_crc32c,
                ) {
                    if state.recovery_policy() == RecoveryPolicy::Strict {
                        return Err(error);
                    }
                    state.mark_opened_in_salvage_mode();
                    state.mark_persistence_anomaly();
                    tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage validation");
                }
            }
            Ok(None) => Self::note_missing_named_sst(state, sst_name)?,
            Err(error) if state.recovery_policy() == RecoveryPolicy::Salvage => {
                state.mark_opened_in_salvage_mode();
                state.mark_persistence_anomaly();
                tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage validation");
            }
            Err(error) => {
                return Err(MidgeError::RecoveryFailed(format!(
                    "failed to validate cloud SST '{sst_name}': {error}"
                )));
            }
        }
        Ok(())
    }

    fn restore_named_sst_from_cloud(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
        staging_fs: &Arc<dyn crate::io::traits::Fs>,
        cloud_key: &str,
        local_path: &Path,
        sst_name: &str,
        proof: &CloudSstRecoveryProof,
    ) -> MidgeResult<()> {
        let cloud_proof = match BlockingCloudIo::new(cloud).object_proof_optional(cloud_key) {
            Ok(Some(proof)) => proof,
            Ok(None) => return Self::note_missing_named_sst(state, sst_name),
            Err(error) if state.recovery_policy() == RecoveryPolicy::Salvage => {
                state.mark_opened_in_salvage_mode();
                state.mark_persistence_anomaly();
                tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage");
                return Ok(());
            }
            Err(error) => {
                return Err(MidgeError::RecoveryFailed(format!(
                    "failed to restore cloud SST '{sst_name}': {error}"
                )));
            }
        };

        if let Err(error) = Self::validate_sst_bytes_against_proof(
            sst_name,
            &cloud_proof.bytes,
            proof.expected_size_bytes,
            proof.expected_crc32c,
        ) {
            if state.recovery_policy() == RecoveryPolicy::Strict {
                return Err(error);
            }
            state.mark_opened_in_salvage_mode();
            state.mark_persistence_anomaly();
            tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage proof validation");
            return Ok(());
        }

        Self::stage_sst_bytes(staging_fs, sst_name, &cloud_proof.bytes)?;
        if let Err(error) = crate::sst::fs::SstFileIo::open_with_real_fs(local_path) {
            if state.recovery_policy() == RecoveryPolicy::Strict {
                return Err(MidgeError::RecoveryFailed(format!(
                    "restored cloud SST '{sst_name}' is invalid: {error}"
                )));
            }
            state.mark_opened_in_salvage_mode();
            state.mark_persistence_anomaly();
            let _ = std::fs::remove_file(local_path);
            tracing::warn!(
                sst_name = %sst_name,
                error = %error,
                "discarding invalid cloud SST during salvage staging"
            );
        }
        Ok(())
    }

    fn note_missing_named_sst(state: &mut RuntimeState, sst_name: &str) -> MidgeResult<()> {
        if state.recovery_policy() == RecoveryPolicy::Strict {
            return Err(MidgeError::RecoveryFailed(format!(
                "authoritative cloud SST '{sst_name}' is missing"
            )));
        }
        state.mark_opened_in_salvage_mode();
        state.mark_persistence_anomaly();
        tracing::warn!(
            sst_name = %sst_name,
            "skipping cloud SST staging because authoritative object is missing"
        );
        Ok(())
    }

    fn stage_sst_bytes(
        staging_fs: &Arc<dyn crate::io::traits::Fs>,
        sst_name: &str,
        data: &[u8],
    ) -> MidgeResult<()> {
        let temp_path =
            crate::io::traits::FsPath::new(crate::cloud_layout::temp_object_key(sst_name));
        let target_path = crate::io::traits::FsPath::new(crate::cloud_layout::object_key(sst_name));
        crate::io::staging::stage_bytes(
            staging_fs,
            &temp_path,
            &target_path,
            data,
            MidgeError::RecoveryFailed,
        )
    }

    pub(crate) fn cloud_recovery_sst_proofs_for_intent_replay(
        state: &RuntimeState,
    ) -> Vec<CloudSstRecoveryProof> {
        let mut proofs = std::collections::BTreeMap::<String, CloudSstRecoveryProof>::new();
        for intent in &state.intent_log {
            match intent {
                crate::runtime::IntentLogEntry::FlushPublish { file_meta, .. }
                | crate::runtime::IntentLogEntry::SstAdded { file_meta } => {
                    proofs
                        .entry(file_meta.name.clone())
                        .and_modify(|proof| {
                            proof.merge_from(&CloudSstRecoveryProof::from_runtime(file_meta));
                        })
                        .or_insert_with(|| CloudSstRecoveryProof::from_runtime(file_meta));
                }
                crate::runtime::IntentLogEntry::CompactionPublish { added, .. }
                | crate::runtime::IntentLogEntry::CompactionApplied { added, .. } => {
                    for file_meta in added {
                        proofs
                            .entry(file_meta.name.clone())
                            .and_modify(|proof| {
                                proof.merge_from(&CloudSstRecoveryProof::from_runtime(file_meta));
                            })
                            .or_insert_with(|| CloudSstRecoveryProof::from_runtime(file_meta));
                    }
                }
                _ => {}
            }
        }
        // Only interrupted publication outputs need full proof validation for
        // intent replay. Merge manifest proof fields for those same files;
        // unrelated immutable SSTs stay remote and are opened on demand.
        for file in &state.manifest.files {
            if let Some(proof) = proofs.get_mut(&file.name) {
                let mut manifest_proof = CloudSstRecoveryProof::from_manifest(file);
                manifest_proof.merge_from(proof);
                *proof = manifest_proof;
            }
        }
        proofs.into_values().collect()
    }
}
