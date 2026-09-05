use super::{CloudSstRecoveryProof, CloudStartupRecovery, CloudWalRecoveryPlan};
use crate::common::{MidgeError, MidgeResult};
use crate::config::RecoveryPolicy;
use crate::io::Fs as _;
use crate::runtime::RuntimeState;
use std::path::{Path, PathBuf};
use std::sync::Arc;

type LocalWalPaths = (
    std::collections::BTreeMap<u64, Vec<PathBuf>>,
    Option<PathBuf>,
);

const CLOUD_WAL_RECOVERY_MAX_IN_FLIGHT: usize = 8;

impl CloudStartupRecovery {
    pub(crate) fn cleanup_non_authoritative_compaction_outputs(
        state: &mut RuntimeState,
        storage: &crate::storage::HybridStorage,
        candidates: &[crate::runtime::FileMeta],
    ) -> MidgeResult<std::collections::BTreeSet<String>> {
        let mut cleaned = std::collections::BTreeSet::new();
        for file_meta in candidates {
            let key = crate::sst::object_key(&file_meta.name);
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
    pub(super) fn ensure_local_sst_cache_from_cloud(
        state: &mut RuntimeState,
        cloud_root: &Path,
    ) -> MidgeResult<()> {
        let remote_sst_dir = cloud_root.join("sst");
        let mut retained_files = Vec::with_capacity(state.manifest.files.len());
        let mut manifest_changed = false;

        for file in state.manifest.files.clone() {
            let remote_path = remote_sst_dir.join(&file.name);
            let validation = std::fs::metadata(&remote_path)
                .map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "authoritative cloud SST '{}' is unavailable: {error}",
                        file.name
                    ))
                })
                .and_then(|metadata| Self::validate_manifest_sst_size(&file, metadata.len()));
            if Self::retain_manifest_sst_after_metadata_validation(state, &file, validation)? {
                retained_files.push(file);
            } else {
                manifest_changed = true;
            }
        }

        if manifest_changed {
            state.manifest.files = retained_files;
            crate::metadata::ManifestPersistence::save(&state.db_path, &state.manifest)
                .map_err(MidgeError::Internal)?;
            state.restore_sequence_floor_from_manifest();
        }

        Ok(())
    }

    pub(super) fn validate_sst_bytes_against_proof(
        sst_name: &str,
        data: &[u8],
        expected_size_bytes: Option<u64>,
        expected_crc32c: Option<u32>,
    ) -> MidgeResult<()> {
        if let Some(expected_size_bytes) = expected_size_bytes {
            if data.len() as u64 != expected_size_bytes {
                return Err(MidgeError::RecoveryFailed(format!(
                    "SST '{}' size {} does not match manifest {}",
                    sst_name,
                    data.len(),
                    expected_size_bytes
                )));
            }
        }

        if let Some(expected_crc32c) = expected_crc32c {
            let actual_crc32c = crc32c::crc32c(data);
            if actual_crc32c != expected_crc32c {
                return Err(MidgeError::RecoveryFailed(format!(
                    "SST '{sst_name}' content crc32c {actual_crc32c:08x} does not match manifest {expected_crc32c:08x}"
                )));
            }
        }

        Ok(())
    }

    pub(super) fn local_sst_file_matches_proof(
        path: &Path,
        _sst_name: &str,
        expected_size_bytes: Option<u64>,
        expected_crc32c: Option<u32>,
    ) -> bool {
        if !path.exists() {
            return false;
        }

        if let Some(expected_size_bytes) = expected_size_bytes {
            match std::fs::metadata(path) {
                Ok(metadata) if metadata.len() == expected_size_bytes => {}
                _ => return false,
            }
        }

        if let Some(expected) = expected_crc32c {
            let Ok(mut file) = std::fs::File::open(path) else {
                return false;
            };
            let mut checksum = 0_u32;
            let mut buffer = vec![0_u8; 1024 * 1024];
            loop {
                let Ok(count) = std::io::Read::read(&mut file, &mut buffer) else {
                    return false;
                };
                if count == 0 {
                    break;
                }
                checksum = crc32c::crc32c_append(checksum, &buffer[..count]);
            }
            if checksum != expected {
                return false;
            }
        }

        let Ok(reader) = crate::sst::fs::SstFileIo::open_with_real_fs(path) else {
            return false;
        };
        expected_crc32c.is_some() || reader.verify_all_blocks().is_ok()
    }

    pub(super) fn local_sst_file_matches_manifest(
        path: &Path,
        file: &crate::metadata::FileMeta,
    ) -> bool {
        Self::local_sst_file_matches_proof(
            path,
            &file.name,
            Some(file.size_bytes),
            file.content_crc32c,
        )
    }

    pub(super) fn blocking_cloud_list(
        cloud: &crate::storage::cloud::CloudStorage,
        prefix: &str,
    ) -> MidgeResult<Vec<String>> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_list(prefix, tx);
        match rx.recv_timeout(cloud.callback_timeout()) {
            Ok(crate::storage::cloud::CloudEvent::List {
                prefix: returned_prefix,
                result,
            }) => {
                let _ = returned_prefix;
                match result {
                    crate::storage::cloud::CloudOutcome::Ok(keys) => Ok(keys),
                    crate::storage::cloud::CloudOutcome::Err(error) => Err(MidgeError::Internal(
                        format!("cloud list '{prefix}': {error}"),
                    )),
                }
            }
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud list response for '{prefix}': {other:?}"
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud list '{prefix}' timed out or failed: {error}"
            ))),
        }
    }

    #[cfg(test)]
    pub(crate) fn blocking_cloud_get(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> MidgeResult<Vec<u8>> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_get(key, tx);
        match rx.recv_timeout(cloud.callback_timeout()) {
            Ok(crate::storage::cloud::CloudEvent::Get { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(data) => Ok(data),
                crate::storage::cloud::CloudOutcome::Err(error) => {
                    Err(MidgeError::Internal(format!("cloud get '{key}': {error}")))
                }
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud get response for '{key}': {other:?}"
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud get '{key}' timed out or failed: {error}"
            ))),
        }
    }

    pub(super) fn blocking_cloud_get_optional(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> MidgeResult<Option<Vec<u8>>> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_get(key, tx);
        match rx.recv_timeout(cloud.callback_timeout()) {
            Ok(crate::storage::cloud::CloudEvent::Get { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(data) => Ok(Some(data)),
                crate::storage::cloud::CloudOutcome::Err(error)
                    if crate::storage::cloud::is_not_found_error(&error) =>
                {
                    Ok(None)
                }
                crate::storage::cloud::CloudOutcome::Err(error) => {
                    Err(MidgeError::Internal(format!("cloud get '{key}': {error}")))
                }
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud get response for '{key}': {other:?}"
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud get '{key}' timed out or failed: {error}"
            ))),
        }
    }

    pub(super) fn blocking_cloud_head_optional(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> MidgeResult<Option<crate::storage::cloud::ObjectMetadata>> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_head(key, tx);
        match rx.recv_timeout(cloud.callback_timeout()) {
            Ok(crate::storage::cloud::CloudEvent::Head { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(metadata) => Ok(Some(metadata)),
                crate::storage::cloud::CloudOutcome::Err(error)
                    if crate::storage::cloud::is_not_found_error(&error) =>
                {
                    Ok(None)
                }
                crate::storage::cloud::CloudOutcome::Err(error) => {
                    Err(MidgeError::Internal(format!("cloud head '{key}': {error}")))
                }
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud head response for '{key}': {other:?}"
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud head '{key}' timed out or failed: {error}"
            ))),
        }
    }

    pub(super) fn blocking_cloud_object_proof_optional(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> MidgeResult<Option<crate::storage::cloud::CloudObjectProof>> {
        crate::storage::cloud::blocking_cloud_object_proof(cloud, key).map_err(MidgeError::Internal)
    }

    #[cfg(test)]
    pub(crate) fn blocking_cloud_put(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
        data: Vec<u8>,
    ) -> MidgeResult<()> {
        Self::blocking_cloud_put_with_headers(cloud, key, data, vec![])
    }

    pub(super) fn blocking_cloud_put_with_headers(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> MidgeResult<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(key, data, headers, tx);
        match rx.recv_timeout(cloud.callback_timeout()) {
            Ok(crate::storage::cloud::CloudEvent::Put { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(()) => Ok(()),
                crate::storage::cloud::CloudOutcome::Err(error) => {
                    Err(MidgeError::Internal(format!("cloud put '{key}': {error}")))
                }
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud put response for '{key}': {other:?}"
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud put '{key}' timed out or failed: {error}"
            ))),
        }
    }

    pub(super) fn remote_manifest_sequence_from_metadata(
        file_name: &str,
        data: &[u8],
    ) -> MidgeResult<Option<u64>> {
        match file_name {
            "manifest.json" | "manifest.snapshot.json" => {
                let manifest: crate::metadata::Manifest =
                    serde_json::from_slice(data).map_err(|error| {
                        MidgeError::Internal(format!(
                            "cloud metadata '{file_name}' is invalid: {error}"
                        ))
                    })?;
                Ok(Some(manifest.last_persisted_sequence))
            }
            _ => Ok(None),
        }
    }

    pub(super) fn load_local_manifest_for_cloud_metadata_mirror(
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<crate::metadata::Manifest> {
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(crate::io::RealFs::new(db_path)?);
        crate::metadata::ManifestPersistence::load_with_fs_and_policy(&fs, recovery_policy)
            .map_err(MidgeError::Internal)
    }

    pub(super) fn ensure_remote_manifest_metadata_not_ahead(
        cloud: &crate::storage::cloud::CloudStorage,
        local_sequence: u64,
    ) -> MidgeResult<()> {
        for file_name in ["manifest.snapshot.json", "manifest.json"] {
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let Some(data) = Self::blocking_cloud_get_optional(cloud, &key)? else {
                continue;
            };
            let Some(remote_sequence) =
                Self::remote_manifest_sequence_from_metadata(file_name, &data)?
            else {
                continue;
            };
            if remote_sequence > local_sequence {
                return Err(MidgeError::Internal(format!(
                    "stale cloud metadata mirror rejected: remote {file_name} is ahead of local manifest ({remote_sequence} > {local_sequence})"
                )));
            }
        }

        Ok(())
    }

    pub(super) fn blocking_conditional_cloud_metadata_put(
        cloud: &crate::storage::cloud::CloudStorage,
        file_name: &str,
        key: &str,
        data: Vec<u8>,
        local_manifest_sequence: u64,
    ) -> MidgeResult<()> {
        let headers = match Self::blocking_cloud_head_optional(cloud, key)? {
            Some(metadata) => {
                let headers = crate::storage::cloud::object_match_precondition_headers(
                    &metadata.etag,
                    metadata.generation.as_deref(),
                )
                .ok_or_else(|| {
                    MidgeError::Internal(format!(
                        "cloud metadata '{key}' cannot be conditionally updated without an identity token"
                    ))
                })?;
                let current = Self::blocking_cloud_get_optional(cloud, key)?.ok_or_else(|| {
                    MidgeError::Internal(format!(
                        "cloud metadata '{key}' disappeared after HEAD precondition"
                    ))
                })?;
                if let Some(remote_sequence) =
                    Self::remote_manifest_sequence_from_metadata(file_name, &current)?
                {
                    if remote_sequence > local_manifest_sequence {
                        return Err(MidgeError::Internal(format!(
                            "stale cloud metadata mirror rejected: remote {file_name} is ahead of local manifest ({remote_sequence} > {local_manifest_sequence})"
                        )));
                    }
                }
                if current == data {
                    return Ok(());
                }
                headers
            }
            None => vec![("If-None-Match".to_string(), "*".to_string())],
        };

        Self::blocking_cloud_put_with_headers(cloud, key, data, headers)
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

        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let data = match Self::blocking_cloud_get_optional(cloud, &key) {
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

            if file_name == &"manifest.journal" {
                has_manifest_journal = true;
            }
            if let Some(sequence) = Self::remote_manifest_sequence_from_metadata(file_name, &data)?
            {
                match *file_name {
                    "manifest.snapshot.json" => snapshot_sequence = Some(sequence),
                    "manifest.json" => manifest_sequence = Some(sequence),
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
        let local_manifest = match Self::load_local_manifest_for_cloud_metadata_mirror(
            db_path,
            recovery_policy,
        ) {
            Ok(manifest) => manifest,
            Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                tracing::warn!(%error, "skipping metadata mirror during salvage open because local manifest could not be loaded");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let local_manifest_sequence = local_manifest.last_persisted_sequence;

        Self::ensure_remote_manifest_metadata_not_ahead(cloud, local_manifest_sequence)?;

        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            let local_path = db_path.join(file_name);
            if !local_path.exists() {
                continue;
            }

            let data = match std::fs::read(&local_path) {
                Ok(data) => data,
                Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                    tracing::warn!(%error, file = %local_path.display(), "skipping metadata mirror during salvage open");
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

            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            if let Err(error) = Self::blocking_conditional_cloud_metadata_put(
                cloud,
                file_name,
                &key,
                data,
                local_manifest_sequence,
            ) {
                if recovery_policy == RecoveryPolicy::Salvage {
                    tracing::warn!(%error, key = %key, "skipping metadata mirror during salvage open");
                    continue;
                }
                return Err(MidgeError::RecoveryFailed(format!(
                    "failed to mirror cloud metadata '{key}': {error}"
                )));
            }
        }

        Ok(())
    }

    pub(in crate::engine) fn materialize_cloud_wal_recovery_dir(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    ) -> MidgeResult<CloudWalRecoveryPlan> {
        let recovery_wal_dir = Self::reset_cloud_wal_recovery_dir(db_path)?;
        let mut plan = CloudWalRecoveryPlan {
            replay_dir: recovery_wal_dir,
            remote_segments: std::collections::BTreeMap::new(),
            local_segments: std::collections::BTreeMap::new(),
            active_wal: None,
            opened_in_salvage_mode: false,
        };

        let staging_fs = Self::recovery_staging_fs(db_path)?;
        let publications = catalog.segments.iter().collect::<Vec<_>>();
        for batch in publications.chunks(CLOUD_WAL_RECOVERY_MAX_IN_FLIGHT) {
            let deadline = std::time::Instant::now() + cloud.callback_timeout();
            let pending = batch
                .iter()
                .map(|(segment_id, publication)| {
                    let (tx, rx) = std::sync::mpsc::channel();
                    cloud.submit_get(&publication.object_key, tx);
                    (**segment_id, *publication, rx)
                })
                .collect::<Vec<_>>();

            for (segment_id, publication, rx) in pending {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                let data = match rx.recv_timeout(remaining) {
                    Ok(crate::storage::cloud::CloudEvent::Get { result, .. }) => match result {
                        crate::storage::cloud::CloudOutcome::Ok(data) => Ok(data),
                        crate::storage::cloud::CloudOutcome::Err(error) => {
                            Err(MidgeError::Internal(format!(
                                "cloud get '{}': {error}",
                                publication.object_key
                            )))
                        }
                    },
                    Ok(other) => Err(MidgeError::Internal(format!(
                        "unexpected cloud get response for '{}': {other:?}",
                        publication.object_key
                    ))),
                    Err(error) => Err(MidgeError::Internal(format!(
                        "cloud get '{}' timed out or failed: {error}",
                        publication.object_key
                    ))),
                };
                let data = match data {
                    Ok(data) => data,
                    Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                        tracing::warn!(
                            %error,
                            key = %publication.object_key,
                            "skipping authoritative cloud WAL object during salvage open"
                        );
                        plan.opened_in_salvage_mode = true;
                        continue;
                    }
                    Err(error) => {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to download authoritative cloud WAL '{}': {error}",
                            publication.object_key
                        )))
                    }
                };
                if let Err(error) = publication.validate_bytes(&data) {
                    if recovery_policy == RecoveryPolicy::Salvage {
                        plan.opened_in_salvage_mode = true;
                        tracing::warn!(
                            %error,
                            key = %publication.object_key,
                            "skipping invalid authoritative cloud WAL during salvage open"
                        );
                        continue;
                    }
                    return Err(MidgeError::RecoveryFailed(format!(
                        "authoritative cloud WAL '{}' failed catalog validation: {error}",
                        publication.object_key
                    )));
                }
                Self::stage_recovery_wal_bytes(
                    &staging_fs,
                    &crate::wal::cloud_segment_file_name(segment_id),
                    &data,
                )?;
                plan.remote_segments.insert(
                    segment_id,
                    crate::runtime::RecoveredCloudWalSegment {
                        max_sequence: publication.max_sequence,
                        writer_epoch: publication.writer_epoch,
                    },
                );
            }
        }

        Self::enforce_recovered_wal_epoch_order(&mut plan, recovery_policy)?;
        Ok(plan)
    }

    pub(in crate::engine) fn materialize_cloud_wal_recovery_dir_with_budget(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        max_local_bytes: u64,
    ) -> MidgeResult<CloudWalRecoveryPlan> {
        Self::validate_cloud_wal_recovery_disk_budget(db_path, catalog, max_local_bytes)?;
        Self::materialize_cloud_wal_recovery_dir(cloud, db_path, recovery_policy, catalog)
    }

    pub(in crate::engine) fn materialize_simulated_cloud_wal_recovery_dir_with_budget(
        cloud_wal_dir: &Path,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        max_local_bytes: u64,
    ) -> MidgeResult<CloudWalRecoveryPlan> {
        Self::validate_cloud_wal_recovery_disk_budget(db_path, catalog, max_local_bytes)?;
        Self::materialize_simulated_cloud_wal_recovery_dir(
            cloud_wal_dir,
            db_path,
            recovery_policy,
            catalog,
        )
    }

    fn validate_cloud_wal_recovery_disk_budget(
        db_path: &Path,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        max_local_bytes: u64,
    ) -> MidgeResult<()> {
        let existing_bytes = Self::local_working_file_bytes(db_path)?;
        let mut replay_sizes = catalog
            .segments
            .iter()
            .map(|(id, publication)| (*id, publication.size_bytes))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut active_bytes = 0_u64;
        match std::fs::read_dir(db_path.join("wal")) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else {
                        continue;
                    };
                    let size = entry.metadata()?.len();
                    if name == crate::wal::ACTIVE_FILE_NAME {
                        active_bytes = active_bytes.max(size);
                    } else if let Some(segment_id) = crate::wal::parse_segment_id(name) {
                        replay_sizes
                            .entry(segment_id)
                            .and_modify(|bytes| {
                                *bytes = (*bytes).max(size);
                            })
                            .or_insert(size);
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let projected = replay_sizes
            .values()
            .copied()
            .chain([active_bytes])
            .try_fold(existing_bytes, |total, bytes| {
                total.checked_add(bytes).ok_or_else(|| {
                    MidgeError::NoSpace("cloud WAL recovery disk requirement overflow".into())
                })
            })?;
        if projected > max_local_bytes {
            return Err(MidgeError::NoSpace(format!(
                "cloud WAL recovery needs at most {projected} local bytes including existing working files, exceeding configured budget {max_local_bytes}"
            )));
        }
        Ok(())
    }

    fn local_working_file_bytes(db_path: &Path) -> MidgeResult<u64> {
        let resettable_wal_staging = db_path.join("cloud_recovery/wal");
        let mut directories = vec![db_path.to_path_buf()];
        let mut bytes = 0_u64;
        while let Some(directory) = directories.pop() {
            let entries = match std::fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            for entry in entries {
                let entry = entry?;
                if (directory == db_path && entry.file_name() == "cloud_store")
                    || entry.path() == resettable_wal_staging
                {
                    // The simulator's cloud store is remote authority. Previous
                    // WAL staging is removed after admission and before GETs.
                    // Other recovery residue survives that reset and counts.
                    continue;
                }
                let file_type = entry.file_type()?;
                if file_type.is_dir() {
                    directories.push(entry.path());
                } else if file_type.is_file() {
                    bytes = bytes.checked_add(entry.metadata()?.len()).ok_or_else(|| {
                        MidgeError::NoSpace("local working-file byte accounting overflow".into())
                    })?;
                } else if file_type.is_symlink() {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "cannot establish local disk budget through symbolic link '{}'",
                        entry.path().display()
                    )));
                }
            }
        }
        Ok(bytes)
    }

    pub(in crate::engine) fn materialize_simulated_cloud_wal_recovery_dir(
        cloud_wal_dir: &Path,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    ) -> MidgeResult<CloudWalRecoveryPlan> {
        let recovery_wal_dir = Self::reset_cloud_wal_recovery_dir(db_path)?;
        let mut plan = CloudWalRecoveryPlan {
            replay_dir: recovery_wal_dir,
            remote_segments: std::collections::BTreeMap::new(),
            local_segments: std::collections::BTreeMap::new(),
            active_wal: None,
            opened_in_salvage_mode: false,
        };
        let staging_fs = Self::recovery_staging_fs(db_path)?;
        for (segment_id, publication) in &catalog.segments {
            let relative = publication
                .object_key
                .strip_prefix(crate::cloud_layout::CloudObjectLayout::WAL_PREFIX)
                .ok_or_else(|| {
                    MidgeError::RecoveryFailed(format!(
                        "cloud WAL catalog key '{}' is outside the WAL namespace",
                        publication.object_key
                    ))
                })?;
            let path = cloud_wal_dir.join(relative);
            let data = match std::fs::read(&path) {
                Ok(data) => data,
                Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                    plan.opened_in_salvage_mode = true;
                    tracing::warn!(
                        %error,
                        path = %path.display(),
                        "skipping authoritative simulated cloud WAL during salvage open"
                    );
                    continue;
                }
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to read authoritative simulated cloud WAL '{}': {error}",
                        path.display()
                    )))
                }
            };
            if let Err(error) = publication.validate_bytes(&data) {
                if recovery_policy == RecoveryPolicy::Salvage {
                    plan.opened_in_salvage_mode = true;
                    tracing::warn!(
                        %error,
                        path = %path.display(),
                        "skipping invalid authoritative simulated cloud WAL during salvage open"
                    );
                    continue;
                }
                return Err(MidgeError::RecoveryFailed(format!(
                    "authoritative simulated cloud WAL '{}' failed catalog validation: {error}",
                    path.display()
                )));
            }
            Self::stage_recovery_wal_bytes(
                &staging_fs,
                &crate::wal::cloud_segment_file_name(*segment_id),
                &data,
            )?;
            plan.remote_segments.insert(
                *segment_id,
                crate::runtime::RecoveredCloudWalSegment {
                    max_sequence: publication.max_sequence,
                    writer_epoch: publication.writer_epoch,
                },
            );
        }

        Self::merge_local_wal_into_recovery_dir(db_path, &mut plan, recovery_policy)?;
        Ok(plan)
    }

    pub(in crate::engine) fn reject_cloud_wal_without_catalog(
        cloud: &crate::storage::cloud::CloudStorage,
    ) -> MidgeResult<()> {
        if Self::blocking_cloud_head_optional(cloud, crate::wal::cloud_catalog::OBJECT_KEY)?
            .is_some()
            || Self::blocking_cloud_head_optional(
                cloud,
                crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
            )?
            .is_some()
        {
            return Ok(());
        }
        let untracked =
            Self::blocking_cloud_list(cloud, crate::cloud_layout::CloudObjectLayout::WAL_PREFIX)?
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

    fn collect_local_wal_paths(
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

    pub(in crate::engine) fn merge_local_wal_into_recovery_dir(
        db_path: &Path,
        plan: &mut CloudWalRecoveryPlan,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        let local_wal_dir = db_path.join("wal");
        let staging_fs = Self::recovery_staging_fs(db_path)?;
        let local_paths = Self::collect_local_wal_paths(
            &local_wal_dir,
            recovery_policy,
            &mut plan.opened_in_salvage_mode,
        )?;
        if let Some((segment_paths, active_path)) = local_paths {
            Self::merge_local_sealed_wal_segments(
                segment_paths,
                &local_wal_dir,
                &staging_fs,
                plan,
                recovery_policy,
            )?;

            if let Some(active_path) = active_path {
                Self::merge_active_local_wal(&active_path, &staging_fs, plan, recovery_policy)?;
            }
        }

        Self::enforce_recovered_wal_epoch_order(plan, recovery_policy)
    }

    fn enforce_recovered_wal_epoch_order(
        plan: &mut CloudWalRecoveryPlan,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        let mut highest_epoch = 0_u64;
        let mut stale_segments = Vec::new();
        let segment_ids = plan
            .remote_segments
            .keys()
            .chain(plan.local_segments.keys())
            .copied()
            .collect::<std::collections::BTreeSet<_>>();

        for segment_id in segment_ids {
            let segment = plan
                .remote_segments
                .get(&segment_id)
                .or_else(|| plan.local_segments.get(&segment_id))
                .expect("recovered segment id came from one recovery map");
            if highest_epoch > 0 && segment.writer_epoch < highest_epoch {
                let message = format!(
                    "recovered WAL writer epoch regression at segment {segment_id}: {} < {highest_epoch}",
                    segment.writer_epoch
                );
                if recovery_policy == RecoveryPolicy::Strict {
                    return Err(MidgeError::RecoveryFailed(message));
                }
                plan.opened_in_salvage_mode = true;
                stale_segments.push(segment_id);
                tracing::warn!(%message, "skipping later stale-epoch WAL segment during salvage open");
                continue;
            }
            highest_epoch = highest_epoch.max(segment.writer_epoch);
        }

        for segment_id in stale_segments {
            let staged_path = plan
                .replay_dir
                .join(crate::wal::cloud_segment_file_name(segment_id));
            match std::fs::remove_file(&staged_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to remove stale-epoch staged WAL '{}': {error}",
                        staged_path.display()
                    )))
                }
            }
            plan.remote_segments.remove(&segment_id);
            plan.local_segments.remove(&segment_id);
        }

        if let Some(active_wal) = plan.active_wal {
            if highest_epoch > 0 && active_wal.writer_epoch < highest_epoch {
                let message = format!(
                    "recovered WAL writer epoch regression at active WAL: {} < {highest_epoch}",
                    active_wal.writer_epoch
                );
                if recovery_policy == RecoveryPolicy::Strict {
                    return Err(MidgeError::RecoveryFailed(message));
                }
                plan.opened_in_salvage_mode = true;
                let staged_path = plan.replay_dir.join(crate::wal::ACTIVE_FILE_NAME);
                match std::fs::remove_file(&staged_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to remove stale-epoch staged active WAL '{}': {error}",
                            staged_path.display()
                        )))
                    }
                }
                plan.active_wal = None;
                tracing::warn!(%message, "skipping later stale-epoch active WAL during salvage open");
            }
        }

        Ok(())
    }

    fn merge_local_sealed_wal_segments(
        segment_paths: std::collections::BTreeMap<u64, Vec<PathBuf>>,
        local_wal_dir: &Path,
        staging_fs: &Arc<dyn crate::io::traits::Fs>,
        plan: &mut CloudWalRecoveryPlan,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        for (segment_id, mut source_paths) in segment_paths {
            let target_name = crate::wal::segment_file_name(segment_id);
            source_paths.sort_by(|left, right| {
                let left_name = left
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("");
                let right_name = right
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("");
                (left_name != target_name, left_name).cmp(&(right_name != target_name, right_name))
            });
            let mut selected: Option<(PathBuf, Vec<u8>, crate::runtime::RecoveredCloudWalSegment)> =
                None;
            let mut other_aliases = Vec::new();
            for source_path in source_paths {
                let local_bytes = match std::fs::read(&source_path) {
                    Ok(bytes) => bytes,
                    Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                        tracing::warn!(
                            %error,
                            path = %source_path.display(),
                            "skipping unreadable local WAL segment during salvage open"
                        );
                        plan.opened_in_salvage_mode = true;
                        other_aliases.push(source_path);
                        continue;
                    }
                    Err(error) => {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to read intact local WAL '{}': {error}",
                            source_path.display()
                        )))
                    }
                };
                let key = source_path.to_string_lossy();
                let Some(segment) = Self::validate_sealed_recovery_wal(
                    &key,
                    &local_bytes,
                    recovery_policy,
                    &mut plan.opened_in_salvage_mode,
                )?
                else {
                    other_aliases.push(source_path);
                    continue;
                };
                if let Some((selected_path, selected_bytes, _)) = &selected {
                    if selected_bytes != &local_bytes {
                        let message = format!(
                            "conflicting duplicate local WAL files for segment {segment_id}: '{}' and '{}'",
                            selected_path.display(),
                            source_path.display()
                        );
                        if recovery_policy == RecoveryPolicy::Strict {
                            return Err(MidgeError::RecoveryFailed(message));
                        }
                        plan.opened_in_salvage_mode = true;
                        tracing::warn!(%message, "retaining canonical local WAL alias during salvage open");
                    }
                    other_aliases.push(source_path);
                    continue;
                }
                selected = Some((source_path, local_bytes, segment));
            }
            let Some((source_path, local_bytes, segment)) = selected else {
                continue;
            };
            let canonical_path = local_wal_dir.join(&target_name);
            Self::canonicalize_local_wal_aliases(
                local_wal_dir,
                &canonical_path,
                &source_path,
                &local_bytes,
                &other_aliases,
            )?;
            let staged_path = plan.replay_dir.join(&target_name);
            if staged_path.exists() {
                let cloud_bytes = std::fs::read(&staged_path).map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to compare staged cloud WAL '{}': {error}",
                        staged_path.display()
                    ))
                })?;
                if cloud_bytes == local_bytes {
                    continue;
                }
                return Err(MidgeError::RecoveryFailed(format!(
                    "validated local and cloud WAL bytes diverge for '{target_name}'; refusing ambiguous recovery"
                )));
            }

            Self::stage_recovery_wal_bytes(staging_fs, &target_name, &local_bytes)?;
            plan.local_segments.insert(segment_id, segment);
        }
        Ok(())
    }

    fn canonicalize_local_wal_aliases(
        local_wal_dir: &Path,
        canonical_path: &Path,
        selected_path: &Path,
        selected_bytes: &[u8],
        other_aliases: &[PathBuf],
    ) -> MidgeResult<()> {
        let target_name = canonical_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                MidgeError::RecoveryFailed("canonical WAL path has no UTF-8 filename".to_string())
            })?;
        let mut directory_changed = false;

        if selected_path != canonical_path {
            if canonical_path.exists() {
                Self::quarantine_local_wal_alias(canonical_path)?;
            }
            let local_fs: Arc<dyn crate::io::traits::Fs> =
                Arc::new(crate::io::RealFs::new(local_wal_dir)?);
            crate::io::staging::stage_bytes(
                &local_fs,
                &crate::io::FsPath::new(format!("{target_name}.recovery.tmp")),
                &crate::io::FsPath::new(target_name),
                selected_bytes,
                MidgeError::RecoveryFailed,
            )?;
            directory_changed = true;
        }

        let mut aliases = other_aliases.to_vec();
        if selected_path != canonical_path {
            aliases.push(selected_path.to_path_buf());
        }
        for alias in aliases {
            if alias == canonical_path || !alias.exists() {
                continue;
            }
            let matches_selected =
                std::fs::read(&alias).is_ok_and(|alias_bytes| alias_bytes == selected_bytes);
            if matches_selected {
                std::fs::remove_file(&alias).map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to remove redundant local WAL alias '{}': {error}",
                        alias.display()
                    ))
                })?;
            } else {
                Self::quarantine_local_wal_alias(&alias)?;
            }
            directory_changed = true;
        }

        if directory_changed {
            let local_fs = crate::io::RealFs::new(local_wal_dir)?;
            local_fs
                .sync_dir(&crate::io::FsPath::new("."), crate::io::Durability::Durable)
                .map_err(MidgeError::from)?;
        }
        Ok(())
    }

    fn quarantine_local_wal_alias(path: &Path) -> MidgeResult<()> {
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
            if retained_path.exists() {
                continue;
            }
            return std::fs::rename(path, &retained_path).map_err(|error| {
                MidgeError::RecoveryFailed(format!(
                    "failed to quarantine local WAL alias '{}' as '{}': {error}",
                    path.display(),
                    retained_path.display()
                ))
            });
        }
        Err(MidgeError::RecoveryFailed(format!(
            "could not allocate quarantine name for local WAL alias '{}'",
            path.display()
        )))
    }

    fn validate_sealed_recovery_wal(
        key: &str,
        data: &[u8],
        recovery_policy: RecoveryPolicy,
        opened_in_salvage_mode: &mut bool,
    ) -> MidgeResult<Option<crate::runtime::RecoveredCloudWalSegment>> {
        match crate::wal::cloud_segment::inspect_bytes(key, data) {
            Ok(readback) => Ok(Some(crate::runtime::RecoveredCloudWalSegment {
                max_sequence: readback.validation.max_sequence,
                writer_epoch: readback.validation.writer_epoch,
            })),
            Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                *opened_in_salvage_mode = true;
                tracing::warn!(%error, key, "skipping invalid sealed WAL during salvage open");
                Ok(None)
            }
            Err(error) => Err(MidgeError::RecoveryFailed(format!(
                "sealed WAL '{key}' failed validation: {error}"
            ))),
        }
    }

    fn merge_active_local_wal(
        active_path: &Path,
        staging_fs: &Arc<dyn crate::io::traits::Fs>,
        plan: &mut CloudWalRecoveryPlan,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        let bytes = match std::fs::read(active_path) {
            Ok(bytes) => bytes,
            Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                plan.opened_in_salvage_mode = true;
                tracing::warn!(
                    %error,
                    path = %active_path.display(),
                    "skipping unreadable active WAL during salvage open"
                );
                return Ok(());
            }
            Err(error) => {
                return Err(MidgeError::RecoveryFailed(format!(
                    "failed to read intact active WAL '{}': {error}",
                    active_path.display()
                )))
            }
        };
        let scan = match crate::wal::recovery::inspect_active_wal_bytes(&bytes) {
            Ok(scan) => scan,
            Err(failure) if failure.is_incomplete_tail() => {
                tracing::info!(
                    error = %failure.error(),
                    path = %active_path.display(),
                    valid_bytes = failure.verified_prefix().valid_bytes,
                    "dropping incomplete active WAL tail"
                );
                failure.verified_prefix()
            }
            Err(failure) if recovery_policy == RecoveryPolicy::Salvage => {
                plan.opened_in_salvage_mode = true;
                tracing::warn!(
                    error = %failure.error(),
                    path = %active_path.display(),
                    valid_bytes = failure.verified_prefix().valid_bytes,
                    "salvaging verified active WAL prefix"
                );
                failure.verified_prefix()
            }
            Err(failure) => {
                return Err(MidgeError::RecoveryFailed(format!(
                    "active WAL '{}' failed validation: {}",
                    active_path.display(),
                    failure.error()
                )))
            }
        };

        if scan.valid_bytes < bytes.len() {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(active_path)
                .map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to open active WAL '{}' for tail truncation: {error}",
                        active_path.display()
                    ))
                })?;
            file.set_len(u64::try_from(scan.valid_bytes).unwrap_or(u64::MAX))
                .and_then(|()| file.sync_all())
                .map_err(|error| {
                    MidgeError::RecoveryFailed(format!(
                        "failed to durably truncate active WAL '{}': {error}",
                        active_path.display()
                    ))
                })?;
        }

        if scan.record_count == 0 {
            return Ok(());
        }

        Self::stage_recovery_wal_bytes(
            staging_fs,
            crate::wal::ACTIVE_FILE_NAME,
            &bytes[..scan.valid_bytes],
        )?;
        plan.active_wal = Some(crate::runtime::RecoveredCloudActiveWal {
            max_sequence: scan.max_sequence,
            writer_epoch: scan.writer_epoch,
            record_count: scan.record_count,
            valid_bytes: scan.valid_bytes,
        });
        Ok(())
    }

    fn reset_cloud_wal_recovery_dir(db_path: &Path) -> MidgeResult<PathBuf> {
        let recovery_wal_dir = db_path.join("cloud_recovery").join("wal");
        if recovery_wal_dir.exists() {
            std::fs::remove_dir_all(&recovery_wal_dir).map_err(|error| {
                MidgeError::RecoveryFailed(format!(
                    "failed to clear cloud WAL recovery directory '{}': {error}",
                    recovery_wal_dir.display()
                ))
            })?;
        }
        std::fs::create_dir_all(&recovery_wal_dir).map_err(|error| {
            MidgeError::RecoveryFailed(format!(
                "failed to create cloud WAL recovery directory '{}': {error}",
                recovery_wal_dir.display()
            ))
        })?;
        Ok(recovery_wal_dir)
    }

    pub(super) fn stage_recovery_wal_bytes(
        staging_fs: &Arc<dyn crate::io::traits::Fs>,
        file_name: &str,
        data: &[u8],
    ) -> MidgeResult<()> {
        let target_path = crate::io::traits::FsPath::new(format!("cloud_recovery/wal/{file_name}"));
        // This directory is a disposable replay cache. Every cloud open first
        // removes it, then rebuilds it from catalog-validated authoritative
        // objects before replay starts. A crash or partial write therefore
        // cannot become recovery authority on the next open, so per-segment
        // file and directory durability barriers only add unbounded startup
        // latency when a catalog contains many small strict-write segments.
        let mut file = staging_fs
            .open(
                &target_path,
                crate::io::traits::OpenOptions {
                    mode: crate::io::traits::OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )
            .map_err(|error| {
                MidgeError::RecoveryFailed(format!(
                    "failed to open ephemeral cloud WAL recovery file {target_path:?}: {error:?}"
                ))
            })?;
        file.write_at(0, bytes::Bytes::copy_from_slice(data))
            .map_err(|error| {
                MidgeError::RecoveryFailed(format!(
                    "failed to write ephemeral cloud WAL recovery file {target_path:?}: {error:?}"
                ))
            })?;
        drop(file);
        Ok(())
    }

    /// Validate cloud authority using object metadata only. The historical name
    /// is retained for the startup boundary; this no longer populates a cache.
    pub(crate) fn ensure_local_sst_cache_from_cloud_storage(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
    ) -> MidgeResult<()> {
        let mut retained_files = Vec::with_capacity(state.manifest.files.len());
        let mut manifest_changed = false;

        for file in state.manifest.files.clone() {
            let key = crate::sst::object_key(&file.name);
            let validation = Self::blocking_cloud_head_optional(cloud, &key)
                .and_then(|metadata| {
                    metadata.ok_or_else(|| {
                        MidgeError::RecoveryFailed(format!(
                            "authoritative cloud SST '{}' is missing",
                            file.name
                        ))
                    })
                })
                .and_then(|metadata| Self::validate_manifest_sst_size(&file, metadata.size));
            if Self::retain_manifest_sst_after_metadata_validation(state, &file, validation)? {
                retained_files.push(file);
            } else {
                manifest_changed = true;
            }
        }

        if manifest_changed {
            state.manifest.files = retained_files;
            crate::metadata::ManifestPersistence::save(&state.db_path, &state.manifest)
                .map_err(MidgeError::Internal)?;
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

    fn retain_manifest_sst_after_metadata_validation(
        state: &mut RuntimeState,
        file: &crate::metadata::FileMeta,
        validation: MidgeResult<()>,
    ) -> MidgeResult<bool> {
        let Err(error) = validation else {
            state.salvaged_local_ssts.remove(&file.name);
            return Ok(true);
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
        let retain = Self::local_sst_file_matches_manifest(&state.sst_dir.join(&file.name), file);
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
        Ok(retain)
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
        let cloud_key = crate::sst::object_key(&sst_name);
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
        match Self::blocking_cloud_object_proof_optional(cloud, cloud_key) {
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
        let cloud_proof = match Self::blocking_cloud_object_proof_optional(cloud, cloud_key) {
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
        let temp_path = crate::io::traits::FsPath::new(crate::sst::temp_object_key(sst_name));
        let target_path = crate::io::traits::FsPath::new(crate::sst::object_key(sst_name));
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
