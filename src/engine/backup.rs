//! Versioned, provider-neutral backup artifacts.

use super::verification::VerificationBarrierGuard;
use super::{Engine, OpenOptions, Storage};
use crate::common::{MidgeError, MidgeResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

mod paths;

const BACKUP_VERSION: u32 = 1;
const BACKUP_MANIFEST: &str = "backup.json";

/// Storage layout recorded in a backup inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackupStorageKind {
    Local,
    CloudSimulated,
}

/// One durable file captured by [`BackupManifest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupObject {
    /// Relative path in the database tree, using `/` separators.
    pub path: String,
    /// Number of bytes captured.
    pub size_bytes: u64,
    /// CRC32C of the captured bytes.
    pub crc32c: u32,
}

/// Versioned inventory and recovery frontier for a database backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Backup inventory format version.
    pub backup_version: u32,
    /// Unique identifier for this capture.
    pub backup_id: String,
    /// UTC timestamp in RFC 3339 format.
    pub captured_at: String,
    /// Last committed sequence observed at the durable capture barrier.
    pub durability_frontier: u64,
    /// Database format marker version.
    pub database_format_version: u32,
    /// Midge package version that produced the artifact.
    pub engine_version: String,
    /// Storage layout whose files are listed in the inventory.
    pub storage_kind: BackupStorageKind,
    /// Complete inventory of the captured durable files.
    pub objects: Vec<BackupObject>,
}

struct PinnedFile {
    relative: PathBuf,
    size: u64,
    file: File,
}

impl Engine {
    /// Capture a consistent, durable database image into a new backup directory.
    ///
    /// The event loop fences mutations while it syncs the current WAL, opens
    /// durable files, and freezes the mutable manifest journal. Bulk file
    /// content is copied after the mutation barrier is released.
    ///
    /// # Errors
    ///
    /// Returns an error when capture is unsupported, the barrier cannot be
    /// acquired, durable files cannot be pinned or copied, or the artifact
    /// cannot be atomically published. The destination must be disjoint from
    /// the source database; publication never replaces an existing target.
    pub fn backup_to(
        &self,
        destination: impl AsRef<Path>,
        timeout: Duration,
    ) -> MidgeResult<BackupManifest> {
        if self.memory_mode {
            return Err(MidgeError::NotSupported(
                "backup is not supported in memory mode".to_string(),
            ));
        }
        if self.cloud_mode && !self.simulated_cloud_mode {
            return Err(MidgeError::NotSupported(
                "backup is not supported for provider-backed cloud storage".to_string(),
            ));
        }
        if !self.is_primary_lease_healthy() {
            return Err(MidgeError::Fenced(
                "backup requires a healthy primary lease".to_string(),
            ));
        }
        if timeout.is_zero() {
            return Err(MidgeError::Timeout(
                "backup capture deadline is zero".to_string(),
            ));
        }

        let source = self.backup_source.clone().ok_or_else(|| {
            MidgeError::NotSupported(
                "backup captures a database served from a host directory".to_string(),
            )
        })?;
        let destination = paths::prepare_target(&source, destination.as_ref())?;
        let parent = destination.parent().ok_or_else(|| {
            MidgeError::InvalidArgument("backup destination requires a parent".into())
        })?;

        let mut barrier =
            VerificationBarrierGuard::acquire_for_backup(&self.runtime_handle, timeout)?;
        if !matches!(barrier.health(), crate::config::EngineHealth::Healthy) {
            return Err(MidgeError::Fenced(
                "backup capture barrier reports an unhealthy engine".to_string(),
            ));
        }
        let database_format_version =
            crate::metadata::format::validate_format_marker(self.root.as_ref(), &source)?;
        let pinned = self.pin_backup_source(&source, parent, timeout, database_format_version)?;
        if !self.is_primary_lease_healthy() {
            return Err(MidgeError::Fenced(
                "primary lease became unhealthy during backup capture".to_string(),
            ));
        }
        let durability_frontier = barrier.sequence();
        let captured_at = chrono::Utc::now().to_rfc3339();
        barrier.release()?;

        let backup_id = uuid::Uuid::new_v4().to_string();
        let stage = tempfile::Builder::new()
            .prefix(".midge-backup-")
            .suffix(".tmp")
            .tempdir_in(parent)?;
        let staging = stage.path();
        let result = materialize_backup(
            pinned,
            staging,
            backup_id,
            captured_at,
            durability_frontier,
            database_format_version,
            if self.simulated_cloud_mode {
                BackupStorageKind::CloudSimulated
            } else {
                BackupStorageKind::Local
            },
        )
        .and_then(|manifest| {
            validate_backup(staging)?;
            // Offline verification may create provider lock files. Keep those
            // out of the published, checksummed artifact.
            let verification = tempfile::tempdir_in(parent)?;
            copy_verified_objects(staging, verification.path(), &manifest)?;
            if self.simulated_cloud_mode {
                crate::engine::verification::StorageVerifier::verify_simulated_cloud_path(
                    verification.path(),
                )?;
            } else {
                crate::engine::StorageVerifier::verify_path(verification.path())?;
            }
            Ok(manifest)
        });
        let manifest = result?;
        sync_directory(staging)?;
        crate::failpoints::fail_point!("midge::backup::before_backup_publish");
        paths::publish(staging, &destination)?;
        sync_directory(parent)?;
        Ok(manifest)
    }

    fn pin_backup_source(
        &self,
        source: &Path,
        scratch_dir: &Path,
        timeout: Duration,
        database_format_version: u32,
    ) -> MidgeResult<Vec<PinnedFile>> {
        if self.simulated_cloud_mode
            && database_format_version == crate::metadata::format::CURRENT_FORMAT_VERSION + 1
        {
            let lease = self.lease_state.lease.as_ref().ok_or_else(|| {
                MidgeError::Fenced("CloudSim backup has no acquired primary lease".into())
            })?;
            pin_cloud_simulated_format_10_files(
                source,
                scratch_dir,
                timeout,
                &lease.holder_id(),
                lease.epoch(),
            )
        } else {
            pin_files_for_format(
                source,
                self.simulated_cloud_mode,
                scratch_dir,
                database_format_version,
            )
        }
    }

    /// Restore a verified backup into a clean local or simulated-cloud target.
    ///
    /// The destination must not exist. Inventory validation and all object
    /// checksums complete before a staging database is created or published.
    ///
    /// # Errors
    ///
    /// Returns an error when the inventory or any object is invalid, the
    /// target is not clean, the storage kind is unsupported, or strict storage
    /// verification fails. The target must be disjoint from the artifact.
    /// Each attempt owns a unique stage, so interrupted attempts may be retried
    /// without deleting residue left by another attempt.
    pub fn restore_backup(
        artifact: impl AsRef<Path>,
        options: OpenOptions,
    ) -> MidgeResult<BackupManifest> {
        if options.injected_filesystem().is_some() || options.injected_object_store().is_some() {
            return Err(MidgeError::NotSupported(
                "restore writes a host directory, which injected storage would not serve"
                    .to_string(),
            ));
        }
        let artifact = fs::canonicalize(artifact.as_ref())?;
        let manifest = validate_backup(&artifact)?;
        if manifest.database_format_version > crate::metadata::format::CURRENT_FORMAT_VERSION + 1 {
            return Err(MidgeError::NotSupported(
                "backup restore does not support this database format".into(),
            ));
        }
        let storage = options.storage().clone();
        drop(options);
        let (target, kind) = match storage {
            Storage::Local { path } => (path, BackupStorageKind::Local),
            Storage::CloudSimulated {
                local_cache_path, ..
            } => (local_cache_path, BackupStorageKind::CloudSimulated),
            Storage::InMemory => {
                return Err(MidgeError::NotSupported(
                    "backup restore is not supported in memory mode".to_string(),
                ));
            }
            Storage::Cloud { .. } => {
                return Err(MidgeError::NotSupported(
                    "backup restore is not supported for provider-backed cloud storage".to_string(),
                ));
            }
        };
        if kind != manifest.storage_kind {
            return Err(MidgeError::InvalidArgument(format!(
                "backup storage kind {:?} cannot restore to {:?}",
                manifest.storage_kind, kind
            )));
        }
        let target = paths::prepare_target(&artifact, &target)?;
        let parent = target.parent().ok_or_else(|| {
            MidgeError::InvalidArgument("restore target requires a parent".into())
        })?;
        let stage = tempfile::Builder::new()
            .prefix(".midge-restore-")
            .suffix(".tmp")
            .tempdir_in(parent)?;
        let staging = stage.path();
        let restore_result = copy_verified_objects(&artifact, staging, &manifest).and_then(|()| {
            let verification = match kind {
                BackupStorageKind::Local => crate::engine::StorageVerifier::verify_path(staging),
                BackupStorageKind::CloudSimulated => {
                    crate::engine::verification::StorageVerifier::verify_simulated_cloud_path(
                        staging,
                    )
                }
            }?;
            let _ = verification;
            let restored_format = crate::metadata::format::validate_format_marker(
                super::startup::existing_host_root(staging)?.as_ref(),
                staging,
            )?;
            if restored_format != manifest.database_format_version {
                return Err(MidgeError::Corruption(format!(
                    "backup inventory format {} does not match restored database format {}",
                    manifest.database_format_version, restored_format
                )));
            }
            Ok(())
        });
        restore_result?;

        crate::failpoints::fail_point!("midge::backup::before_restore_publish");
        paths::publish(staging, &target)?;
        sync_directory(parent)?;
        Ok(manifest)
    }
}

fn pin_files_for_format(
    source: &Path,
    simulated_cloud: bool,
    scratch_dir: &Path,
    database_format_version: u32,
) -> MidgeResult<Vec<PinnedFile>> {
    if database_format_version > crate::metadata::format::CURRENT_FORMAT_VERSION + 1 {
        return Err(MidgeError::NotSupported(
            "backup capture does not support this database format".into(),
        ));
    }
    if database_format_version == crate::metadata::format::CURRENT_FORMAT_VERSION + 1 {
        pin_format_10_files(source, simulated_cloud, scratch_dir)
    } else {
        pin_durable_files(source, simulated_cloud, scratch_dir)
    }
}

fn pin_format_10_files(
    root: &Path,
    simulated_cloud: bool,
    scratch_dir: &Path,
) -> MidgeResult<Vec<PinnedFile>> {
    let fs_root = std::sync::Arc::new(
        crate::io::RealFs::open_existing(root).map_err(crate::io::FsError::into_midge)?,
    );
    let authority = crate::metadata::local_authority::LocalMetadataAuthority::open(fs_root)?;
    let mut relative_paths = vec![PathBuf::from("FORMAT")];
    relative_paths.extend(
        authority
            .selected_relative_paths()
            .into_iter()
            .map(PathBuf::from),
    );
    let mut paths = relative_paths
        .into_iter()
        .map(|relative| {
            validate_relative_path(&relative)?;
            let path = root.join(&relative);
            if fs::symlink_metadata(&path)?.file_type().is_symlink() {
                return Err(MidgeError::Corruption(format!(
                    "backup refuses symlink in selected FORMAT 10 state: '{}'",
                    path.display()
                )));
            }
            Ok(path)
        })
        .collect::<MidgeResult<Vec<_>>>()?;
    paths.extend(ledger_wal_paths(root)?);
    collect_files(&root.join("sst"), root, &mut paths)?;
    if simulated_cloud {
        collect_files(&root.join("cloud_store"), root, &mut paths)?;
    }
    pin_paths(root, paths, scratch_dir)
}

fn pin_cloud_simulated_format_10_files(
    root: &Path,
    scratch_dir: &Path,
    timeout: Duration,
    holder_id: &str,
    epoch: u64,
) -> MidgeResult<Vec<PinnedFile>> {
    let fs_root = std::sync::Arc::new(
        crate::io::RealFs::open_existing(root).map_err(crate::io::FsError::into_midge)?,
    );
    let location =
        crate::storage::simulated::SimulatedCloud::locate(fs_root.as_ref(), None, timeout)?;
    let store = location.open_existing()?.ok_or_else(|| {
        MidgeError::Corruption("simulated cloud FORMAT 10 object store is missing".into())
    })?;
    let cloud = crate::storage::cloud::CloudStorage::from_object_store(store, timeout);
    let deadline = crate::common::OperationDeadline::from_budget(timeout);
    let owner = Some((holder_id, epoch));
    let selected = crate::lease::cloud::read_active_format_10_backup_generation_for_owner(
        &cloud, &deadline, owner,
    )
    .map_err(|error| {
        MidgeError::Corruption(format!(
            "cannot prove active CloudSim FORMAT 10 selection: {error}"
        ))
    })?;
    let bodies = crate::runtime::hybrid_persistence::read_committed_metadata_bodies_within(
        &cloud, &selected, &deadline,
    )?;
    crate::runtime::hybrid_persistence::decode_generation_manifest(&selected, &bodies, &deadline)?;
    crate::runtime::IntentPersistence::validate_cloud_format_10_bodies(&bodies)?;
    let rechecked = crate::lease::cloud::read_active_format_10_backup_generation_for_owner(
        &cloud, &deadline, owner,
    )
    .map_err(|error| {
        MidgeError::Corruption(format!(
            "CloudSim FORMAT 10 selection changed during backup: {error}"
        ))
    })?;
    if rechecked != selected {
        return Err(MidgeError::Corruption(
            "CloudSim selected metadata pointer changed during backup capture".into(),
        ));
    }

    let mut pinned = bodies
        .iter()
        .map(|(relative, body)| pin_bytes(relative, body, scratch_dir))
        .collect::<MidgeResult<Vec<_>>>()?;
    let mut paths = ledger_wal_paths(root)?;
    collect_files(&root.join("sst"), root, &mut paths)?;
    collect_files(&root.join("cloud_store"), root, &mut paths)?;
    pinned.extend(pin_paths(root, paths, scratch_dir)?);
    Ok(pinned)
}

fn pin_bytes(relative: &str, body: &[u8], scratch_dir: &Path) -> MidgeResult<PinnedFile> {
    let relative = PathBuf::from(relative);
    validate_relative_path(&relative)?;
    let mut file = tempfile::tempfile_in(scratch_dir)?;
    file.write_all(body)?;
    file.sync_all()?;
    file.seek(SeekFrom::Start(0))?;
    Ok(PinnedFile {
        relative,
        size: body.len() as u64,
        file,
    })
}

fn pin_durable_files(
    root: &Path,
    simulated_cloud: bool,
    scratch_dir: &Path,
) -> MidgeResult<Vec<PinnedFile>> {
    let mut paths = Vec::new();
    for name in [
        "FORMAT",
        "manifest.json",
        "manifest.snapshot.json",
        "manifest.journal",
        "intent_log.json",
    ] {
        let path = root.join(name);
        if path.is_file() {
            if fs::symlink_metadata(&path)?.file_type().is_symlink() {
                return Err(MidgeError::Corruption(format!(
                    "backup refuses symlink in durable state: '{}'",
                    path.display()
                )));
            }
            paths.push(path);
        }
    }
    paths.extend(ledger_wal_paths(root)?);
    collect_files(&root.join("sst"), root, &mut paths)?;
    if simulated_cloud {
        collect_files(&root.join("cloud_store"), root, &mut paths)?;
    }
    pin_paths(root, paths, scratch_dir)
}

fn pin_paths(
    root: &Path,
    mut paths: Vec<PathBuf>,
    scratch_dir: &Path,
) -> MidgeResult<Vec<PinnedFile>> {
    paths.sort();
    paths.dedup();
    paths
        .into_iter()
        .map(|path| {
            let relative = path.strip_prefix(root).map_err(|_| {
                MidgeError::Corruption("backup path escaped database root".to_string())
            })?;
            validate_relative_path(relative)?;
            let mut file = File::open(&path)?;
            let metadata = file.metadata()?;
            if !metadata.is_file() {
                return Err(MidgeError::Corruption(format!(
                    "backup object '{}' is not a regular file",
                    path.display()
                )));
            }
            let mut size = metadata.len();
            if relative
                == Path::new("cloud_store")
                    .join(crate::cloud_layout::CloudObjectLayout::LEASE_OBJECT_KEY)
            {
                if metadata.len() > crate::lease::cloud::MAX_LEASE_DOCUMENT_BYTES as u64 {
                    return Err(MidgeError::Corruption(
                        "cloud lease document exceeds its backup size limit".to_string(),
                    ));
                }
                let capacity = usize::try_from(metadata.len()).map_err(|_| {
                    MidgeError::Corruption(
                        "cloud lease document exceeds the host address space".to_string(),
                    )
                })?;
                let mut content = Vec::with_capacity(capacity);
                file.read_to_end(&mut content)?;
                if content.len() as u64 != metadata.len() {
                    return Err(MidgeError::Corruption(
                        "cloud lease document changed during backup capture".to_string(),
                    ));
                }
                let expired = crate::lease::cloud::expired_backup_lease_document(&content)?;
                let mut frozen = tempfile::tempfile_in(scratch_dir)?;
                frozen.write_all(&expired)?;
                frozen.sync_all()?;
                frozen.seek(SeekFrom::Start(0))?;
                size = expired.len() as u64;
                file = frozen;
            }
            // Checkpointing truncates this inode in place. An open handle
            // alone cannot preserve its bytes after the capture barrier ends.
            else if relative == Path::new("manifest.journal") {
                let mut frozen = tempfile::tempfile_in(scratch_dir)?;
                let copied = std::io::copy(&mut file, &mut frozen)?;
                if copied != metadata.len() {
                    return Err(MidgeError::Corruption(
                        "manifest journal changed during backup capture".to_string(),
                    ));
                }
                frozen.seek(SeekFrom::Start(0))?;
                file = frozen;
            }
            Ok(PinnedFile {
                relative: relative.to_path_buf(),
                size,
                file,
            })
        })
        .collect()
}

/// The WAL files a backup captures (#844): the ledger and every member's
/// segment, named by the ledger itself, read inside the capture barrier.
/// Orphans, set-aside directories and retained salvage copies are not
/// history. A ledger that cannot be read fails the backup closed.
fn ledger_wal_paths(root: &Path) -> MidgeResult<Vec<PathBuf>> {
    let wal_dir = root.join(crate::wal::WAL_DIR);
    if !wal_dir.exists() {
        return Ok(Vec::new());
    }
    let wal: std::sync::Arc<dyn crate::io::ReadFs> = std::sync::Arc::new(
        crate::io::RealFs::open_existing(&wal_dir).map_err(crate::io::FsError::into_midge)?,
    );
    let Some(ledger) = crate::runtime::wal_history::WalHistory::read(&wal)? else {
        return Ok(Vec::new());
    };
    let names = std::iter::once(crate::wal::ledger_store::LEDGER_FILE.to_string())
        .chain(ledger.members().map(crate::wal::segment_file_name));
    let mut paths = Vec::new();
    for name in names {
        let path = wal_dir.join(name);
        if fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err(MidgeError::Corruption(format!(
                "backup refuses symlink in durable state: '{}'",
                path.display()
            )));
        }
        paths.push(path);
    }
    Ok(paths)
}

fn collect_files(path: &Path, root: &Path, files: &mut Vec<PathBuf>) -> MidgeResult<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(MidgeError::Corruption(format!(
            "backup refuses symlink in durable state: '{}'",
            path.display()
        )));
    }
    if metadata.is_file() {
        files.push(path.to_path_buf());
        return Ok(());
    }
    if !metadata.is_dir() {
        return Err(MidgeError::Corruption(format!(
            "backup encountered unsupported filesystem object '{}'",
            path.display()
        )));
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        collect_files(&entry.path(), root, files)?;
    }
    let _ = root;
    Ok(())
}

fn materialize_backup(
    mut pinned: Vec<PinnedFile>,
    staging: &Path,
    backup_id: String,
    captured_at: String,
    durability_frontier: u64,
    database_format_version: u32,
    storage_kind: BackupStorageKind,
) -> MidgeResult<BackupManifest> {
    let mut objects = Vec::with_capacity(pinned.len());
    let objects_root = staging.join("objects");
    fs::create_dir(&objects_root)?;
    let mut buffer = vec![0_u8; 64 * 1024];
    for mut source in pinned.drain(..) {
        let relative_text = path_to_inventory(&source.relative)?;
        let destination = objects_root.join(&source.relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut output = File::create(&destination)?;
        let mut remaining = source.size;
        let mut checksum = 0_u32;
        while remaining > 0 {
            let requested =
                usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
            let read = source.file.read(&mut buffer[..requested])?;
            if read == 0 {
                return Err(MidgeError::Corruption(format!(
                    "backup source '{}' ended before its captured length",
                    source.relative.display()
                )));
            }
            output.write_all(&buffer[..read])?;
            checksum = crc32c::crc32c_append(checksum, &buffer[..read]);
            remaining -= read as u64;
        }
        output.sync_all()?;
        crate::failpoints::fail_point!("midge::backup::after_object_copy", |_| Err(
            MidgeError::Internal("failpoint: backup interrupted during object copy".to_string())
        ));
        objects.push(BackupObject {
            path: relative_text,
            size_bytes: source.size,
            crc32c: checksum,
        });
    }
    objects.sort_by(|left, right| left.path.cmp(&right.path));
    let manifest = BackupManifest {
        backup_version: BACKUP_VERSION,
        backup_id,
        captured_at,
        durability_frontier,
        database_format_version,
        engine_version: env!("CARGO_PKG_VERSION").to_string(),
        storage_kind,
        objects,
    };
    let mut file = File::create(staging.join(BACKUP_MANIFEST))?;
    let encoded = serde_json::to_vec_pretty(&manifest).map_err(|error| {
        MidgeError::Internal(format!("failed to encode backup inventory: {error}"))
    })?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    sync_tree_directories(&staging.join("objects"))?;
    Ok(manifest)
}

fn validate_backup(artifact: &Path) -> MidgeResult<BackupManifest> {
    let artifact_metadata = fs::symlink_metadata(artifact)?;
    if !artifact_metadata.is_dir() || artifact_metadata.file_type().is_symlink() {
        return Err(MidgeError::Corruption(
            "backup artifact is not a regular directory".to_string(),
        ));
    }
    let objects_root = artifact.join("objects");
    let objects_metadata = fs::symlink_metadata(&objects_root)?;
    if !objects_metadata.is_dir() || objects_metadata.file_type().is_symlink() {
        return Err(MidgeError::Corruption(
            "backup objects root is missing or is not a regular directory".to_string(),
        ));
    }
    let manifest_path = artifact.join(BACKUP_MANIFEST);
    let manifest: BackupManifest = serde_json::from_slice(&fs::read(&manifest_path)?)
        .map_err(|error| MidgeError::Corruption(format!("invalid backup inventory: {error}")))?;
    if manifest.backup_version != BACKUP_VERSION {
        return Err(MidgeError::CompatibilityError(format!(
            "unsupported backup inventory version {}; expected {BACKUP_VERSION}",
            manifest.backup_version
        )));
    }
    if manifest.database_format_version > crate::metadata::format::CURRENT_FORMAT_VERSION + 1 {
        return Err(MidgeError::CompatibilityError(format!(
            "backup database format {} is newer than supported format {}",
            manifest.database_format_version,
            crate::metadata::format::CURRENT_FORMAT_VERSION + 1
        )));
    }
    uuid::Uuid::parse_str(&manifest.backup_id).map_err(|_| {
        MidgeError::Corruption("backup inventory has an invalid backup identifier".to_string())
    })?;
    let mut actual = BTreeSet::new();
    collect_inventory_objects(&objects_root, &objects_root, &mut actual)?;
    let mut declared = BTreeSet::new();
    for object in &manifest.objects {
        let relative = validate_inventory_path(&object.path)?;
        if !declared.insert(object.path.clone()) {
            return Err(MidgeError::Corruption(format!(
                "duplicate backup object '{}'",
                object.path
            )));
        }
        let path = objects_root.join(relative);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(MidgeError::Corruption(format!(
                "backup object '{}' is missing or not a regular file",
                object.path
            )));
        }
        if metadata.len() != object.size_bytes {
            return Err(MidgeError::Corruption(format!(
                "backup object '{}' has length {}, expected {}",
                object.path,
                metadata.len(),
                object.size_bytes
            )));
        }
        let (size, checksum) = hash_file(&path)?;
        if size != object.size_bytes || checksum != object.crc32c {
            return Err(MidgeError::Corruption(format!(
                "backup object '{}' failed size or CRC32C validation",
                object.path
            )));
        }
    }
    if actual != declared {
        return Err(MidgeError::Corruption(
            "backup inventory does not match artifact object files".to_string(),
        ));
    }
    Ok(manifest)
}

fn copy_verified_objects(
    artifact: &Path,
    staging: &Path,
    manifest: &BackupManifest,
) -> MidgeResult<()> {
    for object in &manifest.objects {
        let relative = validate_inventory_path(&object.path)?;
        let source = artifact.join("objects").join(relative);
        let destination = staging.join(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut input = File::open(source)?;
        let mut output = File::create(destination)?;
        let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
        let mut copied = 0_u64;
        let mut checksum = 0_u32;
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            output.write_all(&buffer[..read])?;
            copied = copied.saturating_add(read as u64);
            checksum = crc32c::crc32c_append(checksum, &buffer[..read]);
        }
        if copied != object.size_bytes || checksum != object.crc32c {
            return Err(MidgeError::Corruption(format!(
                "backup object '{}' changed during restore",
                object.path
            )));
        }
        output.sync_all()?;
        crate::failpoints::fail_point!("midge::backup::after_restore_object_copy", |_| Err(
            MidgeError::Internal("failpoint: restore interrupted during object copy".to_string())
        ));
    }
    sync_tree_directories(staging)?;
    Ok(())
}

fn sync_tree_directories(path: &Path) -> MidgeResult<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree_directories(&entry.path())?;
        }
    }
    sync_directory(path)
}

fn collect_inventory_objects(
    root: &Path,
    path: &Path,
    objects: &mut BTreeSet<String>,
) -> MidgeResult<()> {
    if !path.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path)?;
        if metadata.file_type().is_symlink() {
            return Err(MidgeError::Corruption(
                "backup artifact contains a symlink".to_string(),
            ));
        }
        if metadata.is_dir() {
            collect_inventory_objects(root, &entry_path, objects)?;
        } else if metadata.is_file() {
            let relative = entry_path.strip_prefix(root).map_err(|_| {
                MidgeError::Corruption("backup artifact path escaped objects directory".to_string())
            })?;
            objects.insert(path_to_inventory(relative)?);
        } else {
            return Err(MidgeError::Corruption(
                "backup artifact contains an unsupported filesystem object".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> MidgeResult<()> {
    let path_text = path.to_string_lossy();
    if path.as_os_str().is_empty()
        || path_text.contains(':')
        || (std::path::MAIN_SEPARATOR != '\\' && path_text.contains('\\'))
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(MidgeError::Corruption(format!(
            "unsafe backup object path '{}'",
            path.display()
        )));
    }
    Ok(())
}

fn validate_inventory_path(path: &str) -> MidgeResult<&Path> {
    if path.contains('\\') {
        return Err(MidgeError::Corruption(format!(
            "unsafe backup object path '{path}'"
        )));
    }
    let relative = Path::new(path);
    validate_relative_path(relative)?;
    Ok(relative)
}

fn path_to_inventory(path: &Path) -> MidgeResult<String> {
    validate_relative_path(path)?;
    path.to_str()
        .map(|path| path.replace(std::path::MAIN_SEPARATOR, "/"))
        .ok_or_else(|| MidgeError::Corruption("backup object path is not valid UTF-8".to_string()))
}

fn hash_file(path: &Path) -> MidgeResult<(u64, u32)> {
    let mut file = File::open(path)?;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut size = 0_u64;
    let mut checksum = 0_u32;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        size += read as u64;
        checksum = crc32c::crc32c_append(checksum, &buffer[..read]);
    }
    Ok((size, checksum))
}

// Keep the fallible Unix directory-sync contract uniform at call sites even
// though the standard library does not expose directory handles on Windows.
#[allow(clippy::unnecessary_wraps)]
fn sync_directory(path: &Path) -> MidgeResult<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::PrimaryLease;
    use std::sync::Arc;

    #[test]
    fn should_encode_native_nested_backup_path_with_portable_separators() {
        // Arrange
        let native_path = Path::new("wal").join("segment.wal");

        // Act
        let inventory_path = path_to_inventory(&native_path).expect("native backup path");

        // Assert
        assert_eq!(inventory_path, "wal/segment.wal");
    }

    #[test]
    fn should_pin_nested_wal_file_using_native_path_separators() {
        // Arrange
        let directory = tempfile::tempdir().expect("test directory");
        let source = directory.path().join("source");
        fs::create_dir_all(source.join("wal")).expect("create WAL directory");
        crate::runtime::wal_history::fixture::record_at(&source, &[], 1).expect("record ledger");

        // Act
        let pinned = pin_durable_files(&source, false, directory.path()).expect("pin WAL");

        // Assert
        let segment = Path::new("wal").join(crate::wal::segment_file_name(1));
        assert!(pinned.iter().any(|file| file.relative == segment));
    }

    #[test]
    fn should_restore_only_selected_format_10_history_after_source_advances() {
        // Arrange
        let directory = tempfile::tempdir().expect("test directory");
        let source = directory.path().join("source");
        fs::create_dir(&source).expect("create source");
        let fs_root: std::sync::Arc<dyn crate::io::Fs> =
            std::sync::Arc::new(crate::io::RealFs::new(&source).expect("create source filesystem"));
        let manifest = crate::metadata::codec::encode(
            crate::metadata::codec::MANIFEST,
            &crate::metadata::Manifest::default(),
        )
        .expect("encode empty manifest");
        let intents = serde_json::to_vec(&Vec::<crate::runtime::IntentLogEntry>::new())
            .expect("encode empty intents");
        let mut authority = crate::metadata::local_authority::LocalMetadataAuthority::initialize(
            std::sync::Arc::clone(&fs_root),
            "backup-generation",
            &manifest,
            &intents,
        )
        .expect("initialize FORMAT 10 metadata");
        fs::write(source.join("FORMAT"), b"midge-format-version=10\n")
            .expect("write FORMAT marker");
        let first = crate::metadata::ManifestEdit::CreateColumnFamily {
            id: 2,
            name: "captured".into(),
            created_at: 1,
        };
        let first_payload = serde_json::to_vec(&first).expect("encode first edit");
        authority
            .append(
                crate::metadata::local_authority::Stream::Manifest,
                1,
                1,
                &first_payload,
            )
            .expect("append first selected edit");

        // Act: pin the selected closure, then advance the live source.
        let pinned =
            pin_format_10_files(&source, false, directory.path()).expect("pin selected closure");
        let artifact = directory.path().join("artifact");
        fs::create_dir(&artifact).expect("create artifact stage");
        let captured = materialize_backup(
            pinned,
            &artifact,
            uuid::Uuid::new_v4().to_string(),
            chrono::Utc::now().to_rfc3339(),
            1,
            crate::metadata::format::CURRENT_FORMAT_VERSION + 1,
            BackupStorageKind::Local,
        )
        .expect("materialize selected FORMAT 10 closure");
        validate_backup(&artifact).expect("validate captured artifact");
        let second = crate::metadata::ManifestEdit::CreateColumnFamily {
            id: 3,
            name: "later".into(),
            created_at: 2,
        };
        let second_payload = serde_json::to_vec(&second).expect("encode second edit");
        authority
            .append(
                crate::metadata::local_authority::Stream::Manifest,
                2,
                2,
                &second_payload,
            )
            .expect("advance source after capture");
        let restored = directory.path().join("restored");
        fs::create_dir(&restored).expect("create restore stage");
        copy_verified_objects(&artifact, &restored, &captured).expect("restore captured objects");
        crate::engine::StorageVerifier::verify_path(&restored)
            .expect("verify restored FORMAT 10 database");
        let restored_fs: std::sync::Arc<dyn crate::io::Fs> = std::sync::Arc::new(
            crate::io::RealFs::open_existing(&restored).expect("open restored filesystem"),
        );
        let recovered = crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
            &restored_fs,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect("load restored selected manifest");

        // Assert
        assert!(recovered
            .column_families
            .iter()
            .any(|family| family.name == "captured"));
        assert!(!recovered
            .column_families
            .iter()
            .any(|family| family.name == "later"));
    }

    #[test]
    fn should_capture_only_ledger_members_when_wal_directory_holds_orphans() {
        // Arrange: the ledger names segments 1 and 2; the WAL directory also
        // holds an orphan segment, a salvage directory and a retained copy.
        let directory = tempfile::tempdir().expect("test directory");
        let source = directory.path().join("source");
        let wal = source.join("wal");
        fs::create_dir_all(&wal).expect("create WAL directory");
        crate::runtime::wal_history::fixture::record_at(&source, &[1], 2).expect("record ledger");
        fs::write(wal.join(crate::wal::segment_file_name(9)), b"orphan").expect("seed orphan");
        fs::create_dir(wal.join("salvaged-1")).expect("create salvage directory");
        fs::write(
            wal.join("salvaged-1")
                .join(crate::wal::segment_file_name(1)),
            b"set aside",
        )
        .expect("seed set-aside segment");
        fs::write(
            wal.join(format!(
                "{}.salvage-retained",
                crate::wal::segment_file_name(2)
            )),
            b"retained",
        )
        .expect("seed retained copy");

        // Act
        let pinned = pin_durable_files(&source, false, directory.path()).expect("pin WAL");

        // Assert
        let captured: BTreeSet<PathBuf> = pinned.iter().map(|file| file.relative.clone()).collect();
        assert_eq!(
            captured,
            [
                crate::wal::ledger_store::LEDGER_FILE.to_string(),
                crate::wal::segment_file_name(1),
                crate::wal::segment_file_name(2),
            ]
            .map(|name| Path::new("wal").join(name))
            .into_iter()
            .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn should_reject_backslash_in_serialized_backup_inventory_path() {
        // Arrange
        let path = "wal\\segment.wal";

        // Act
        let result = validate_inventory_path(path);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }

    #[test]
    fn should_preserve_captured_journal_when_live_journal_is_truncated() {
        // Arrange
        let directory = tempfile::tempdir().expect("test directory");
        let source = directory.path().join("source");
        let staging = directory.path().join("staging");
        fs::create_dir(&source).expect("source directory");
        fs::create_dir(&staging).expect("staging directory");
        let journal = source.join("manifest.journal");
        let captured = b"durable manifest edit";
        fs::write(&journal, captured).expect("seed journal");
        let pinned =
            pin_durable_files(&source, false, directory.path()).expect("pin captured files");

        // Act: a later manifest checkpoint truncates this inode in place.
        File::options()
            .write(true)
            .truncate(true)
            .open(&journal)
            .expect("truncate live journal");
        let result = materialize_backup(
            pinned,
            &staging,
            uuid::Uuid::new_v4().to_string(),
            chrono::Utc::now().to_rfc3339(),
            0,
            crate::metadata::format::CURRENT_FORMAT_VERSION,
            BackupStorageKind::Local,
        );

        // Assert
        result.expect("materialize captured journal");
        assert_eq!(
            fs::read(staging.join("objects/manifest.journal")).expect("copied journal"),
            captured
        );
    }

    struct CloudSimMigrationOwner {
        cloud: Arc<crate::storage::cloud::CloudStorage>,
        lease: Arc<crate::lease::CloudStorageLease>,
        _guard: crate::lease::LeaseGuard,
        store: Arc<dyn crate::lease::LeaderStore>,
        holder: String,
        epoch: u64,
        source: crate::lease::CloudMetadataGeneration,
        deadline: crate::common::OperationDeadline,
        local_root: Arc<dyn crate::io::Fs>,
    }

    fn create_cloudsim_source(
        source: &Path,
        timeout: Duration,
    ) -> MidgeResult<crate::engine::OpenOptions> {
        use crate::engine::{OpenOptions, TransactionMode, WriteOptions};
        let options = OpenOptions::cloud_simulated(source, "backup-test", "format10").build()?;
        let mut engine = Engine::open(options.clone())?;
        let default = engine.get_column_family("default").expect("default CF");
        loop {
            let result = (|| {
                let mut transaction = engine.begin_tx(default.id(), TransactionMode::ReadWrite)?;
                transaction.put(b"backup-key".to_vec(), b"backup-value".to_vec(), None)?;
                transaction.commit(WriteOptions::cloud_async())
            })();
            match result {
                Ok(()) => break,
                Err(MidgeError::Busy(_)) => std::thread::yield_now(),
                Err(error) => return Err(error),
            }
        }
        engine.flush_cf(&default)?;
        loop {
            let result = (|| {
                let mut transaction = engine.begin_tx(default.id(), TransactionMode::ReadWrite)?;
                transaction.put(b"wal-key".to_vec(), b"wal-value".to_vec(), None)?;
                transaction.commit(WriteOptions::cloud_async())
            })();
            match result {
                Ok(()) => break,
                Err(MidgeError::Busy(_)) => std::thread::yield_now(),
                Err(error) => return Err(error),
            }
        }
        let shutdown = engine.shutdown(timeout);
        drop(engine);
        expire_released_test_lease(source)?;
        if let Err(error) = shutdown {
            assert!(matches!(error, MidgeError::LeaseUnavailable(_)), "{error}");
        }
        Ok(options)
    }

    fn expire_released_test_lease(source: &Path) -> MidgeResult<()> {
        let lease_path = source
            .join("cloud_store")
            .join(crate::cloud_layout::CloudObjectLayout::LEASE_OBJECT_KEY);
        let mut lease_wire = fs::read_to_string(&lease_path)?;
        if let Some(line) = lease_wire
            .lines()
            .find(|line| line.starts_with("expires: "))
        {
            lease_wire = lease_wire.replace(line, "expires: 1970-01-01T00:00:00Z");
            fs::write(&lease_path, lease_wire)?;
        }
        Ok(())
    }

    fn acquire_cloudsim_migration_owner(
        source: &Path,
        timeout: Duration,
    ) -> MidgeResult<CloudSimMigrationOwner> {
        use crate::lease::PrimaryLease;
        use crate::storage::cloud::CloudStorage;

        // Arrange: take over the v4 CloudSim lease and select a proper local
        // candidate closure before publishing its exact remote counterpart.
        let cloud_root = source.join("cloud_store");
        let object_store = Arc::new(crate::storage::filesystem::FileSystem::new(&cloud_root)?);
        let object_store: Arc<dyn crate::storage::inventory::ObjectStore> = object_store;
        let cloud = Arc::new(CloudStorage::from_object_store(
            Arc::clone(&object_store),
            timeout,
        ));
        let lease = Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
            crate::lease::CloudLeaseConfig {
                bucket: "backup-test".into(),
                prefix: "format10".into(),
            },
            source.join("lease-cache"),
            Arc::clone(&cloud),
        ));
        let lease_guard = Arc::clone(&lease)
            .try_acquire()
            .unwrap_or_else(|error| panic!("direct CloudSim lease acquisition: {error}"));
        let store = lease.get_leader_store().expect("provider-backed store");
        let holder = lease.holder_id();
        let epoch = lease.epoch();
        let source_generation = match store.read_committed_metadata(timeout)? {
            crate::lease::CloudMetadataHead::Committed(generation) => generation,
            other => panic!("expected selected v9 generation, got {other:?}"),
        };
        let deadline = crate::common::OperationDeadline::from_budget(timeout);
        let _source_bodies =
            crate::runtime::hybrid_persistence::read_committed_metadata_bodies_within(
                &cloud,
                &source_generation,
                &deadline,
            )?;
        let local_root: Arc<dyn crate::io::Fs> = Arc::new(
            crate::io::RealFs::open_existing(&source).map_err(crate::io::FsError::into_midge)?,
        );
        Ok(CloudSimMigrationOwner {
            cloud,
            lease,
            _guard: lease_guard,
            store,
            holder,
            epoch,
            source: source_generation,
            deadline,
            local_root,
        })
    }

    fn stage_cloudsim_v10_candidate(
        owner: &CloudSimMigrationOwner,
    ) -> MidgeResult<crate::lease::CloudMetadataGeneration> {
        use crate::storage::cloud::BlockingCloud;
        let local_root = &owner.local_root;
        let manifest = crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
            &local_root,
            crate::config::RecoveryPolicy::Strict,
        )?;
        let manifest = crate::metadata::codec::encode(crate::metadata::codec::MANIFEST, &manifest)
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        let raw_intents = crate::io::read_all(
            local_root.as_ref(),
            &crate::io::FsPath::new(crate::metadata::files::INTENT_LOG),
        )
        .map_err(crate::io::FsError::into_midge)?;
        let intent_state = crate::runtime::IntentPersistence::decode_for_upgrade(
            bytes::Bytes::copy_from_slice(&raw_intents),
            &crate::common::DeadlineScope::new(owner.deadline),
        )?;
        let intent_bytes = serde_json::to_vec(&intent_state.entries)
            .map_err(|error| MidgeError::Internal(error.to_string()))?;
        crate::metadata::local_authority::LocalMetadataAuthority::stage_upgrade_candidate(
            Arc::clone(local_root),
            [9; 32],
            &manifest,
            crate::metadata::ManifestPersistence::load_with_fs_and_policy_typed(
                &local_root,
                crate::config::RecoveryPolicy::Strict,
            )?
            .edit_checkpoint_id,
            &intent_bytes,
            intent_state.sequence,
            intent_state.checksum,
        )?;
        crate::io::staging::stage_bytes_typed(
            &local_root,
            &crate::io::FsPath::new("FORMAT.tmp"),
            &crate::io::FsPath::new(crate::metadata::files::FORMAT),
            b"midge-format-version=10\n",
            || Ok(()),
        )?;

        let mut candidate_bodies = std::collections::BTreeMap::from([(
            crate::metadata::files::FORMAT.to_string(),
            b"midge-format-version=10\n".to_vec(),
        )]);
        for path in
            crate::metadata::local_authority::LocalMetadataAuthority::selected_relative_paths_from(
                local_root.as_ref(),
            )?
        {
            let bytes = crate::io::read_all(local_root.as_ref(), &crate::io::FsPath::new(&path))
                .map_err(crate::io::FsError::into_midge)?;
            candidate_bodies.insert(path, bytes.to_vec());
        }
        let manifest_horizon =
            crate::metadata::local_authority::LocalMetadataAuthority::validate_cloud_bodies(
                &candidate_bodies,
            )?;
        let generation_uuid = uuid::Uuid::new_v4().to_string();
        let candidate_generation = crate::lease::CloudMetadataGeneration {
            id: crate::lease::SharedGenerationId::new(owner.source.id.get() + 1)
                .expect("next shared generation ordinal"),
            manifest_horizon,
            objects: candidate_bodies
                .iter()
                .map(|(file_name, body)| crate::lease::CloudMetadataObject {
                    file_name: file_name.clone(),
                    object_key: format!("metadata/generations/{generation_uuid}/{file_name}"),
                    len: body.len() as u64,
                    crc32c: crc32c::crc32c(body),
                })
                .collect(),
        };
        for object in &candidate_generation.objects {
            BlockingCloud::new(&owner.cloud, &owner.deadline).put_with_precondition(
                &object.object_key,
                candidate_bodies[&object.file_name].clone(),
                &crate::storage::StoragePrecondition::IfAbsent,
            )?;
        }
        Ok(candidate_generation)
    }

    fn activate_cloudsim_v10_candidate(
        owner: &CloudSimMigrationOwner,
        candidate: &crate::lease::CloudMetadataGeneration,
        timeout: Duration,
    ) -> MidgeResult<()> {
        use crate::lease::{CloudFormatMigrationGate, CloudFormatMigrationPending};
        let migration_id = "81900000-0000-4000-8000-000000000003";
        let pending = CloudFormatMigrationPending {
            migration_id: migration_id.to_string(),
            target_format: 10,
            source: crate::lease::CloudMetadataGenerationIdentity::from_generation(&owner.source),
            candidate: crate::lease::CloudFormatMigrationCandidate {
                generation: crate::lease::CloudMetadataGenerationIdentity::from_generation(
                    candidate,
                ),
                descriptor_key: CloudFormatMigrationPending::candidate_descriptor_key(migration_id),
            },
            source_sha256: "a".repeat(64),
        };
        owner
            .store
            .install_format_10_pending(&owner.holder, owner.epoch, pending, timeout)?;
        owner.store.publish_committed_metadata(
            &owner.holder,
            owner.epoch,
            Some(&owner.source),
            candidate.clone(),
            timeout,
        )?;
        owner.store.activate_format_10_gate(
            &owner.holder,
            owner.epoch,
            migration_id,
            candidate,
            timeout,
        )?;
        assert_eq!(
            owner
                .store
                .read_format_migration_gate(&owner.holder, owner.epoch, timeout)?,
            Some(CloudFormatMigrationGate::Active { target_format: 10 })
        );
        owner.lease.release().expect("release active test lease");
        Ok(())
    }

    #[test]
    fn should_backup_and_restore_active_cloudsim_format_10_closure() -> MidgeResult<()> {
        use crate::engine::{OpenOptions, TransactionMode};
        use std::time::Duration;

        // Arrange
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        let artifact = directory.path().join("artifact");
        let target = directory.path().join("restored");
        let timeout = Duration::from_secs(60);
        let options = create_cloudsim_source(&source, timeout)?;
        let owner = acquire_cloudsim_migration_owner(&source, timeout)?;
        let candidate = stage_cloudsim_v10_candidate(&owner)?;
        activate_cloudsim_v10_candidate(&owner, &candidate, timeout)?;

        // Act: open the active-gated CloudSim database, capture it, and restore
        // through the same public backup verifier and startup paths.
        let mut active = Engine::open(options.clone())
            .unwrap_or_else(|error| panic!("open active-gated source: {error}"));
        let inventory = active.backup_to(&artifact, timeout)?;
        let restore_options =
            OpenOptions::cloud_simulated(&target, "backup-test", "format10").build()?;
        Engine::restore_backup(&artifact, restore_options.clone())?;
        let mut restored = Engine::open(restore_options)
            .unwrap_or_else(|error| panic!("open restored active-gated target: {error}"));

        // Assert: the archive carries the local and remote selected closure,
        // WAL catalog and segment, live remote SST, and expired active gate.
        let has = |suffix: &str| inventory.objects.iter().any(|object| object.path == suffix);
        assert!(has("metadata/HEAD"));
        assert!(has("cloud_store/metadata/committed-pointer.v1"));
        assert!(has("cloud_store/wal/publication-catalog.v1.json"));
        assert!(has("cloud_store/wal/publication-catalog.v1.mirror.json"));
        assert!(inventory.objects.iter().any(|object| {
            object.path.starts_with("cloud_store/metadata/generations/")
                && object.path.ends_with("/metadata/HEAD")
        }));
        assert!(inventory
            .objects
            .iter()
            .any(|object| object.path.starts_with("cloud_store/sst/")));
        assert!(inventory
            .objects
            .iter()
            .any(|object| { object.path.starts_with("wal/") && object.path.ends_with(".wal") }));
        let lease_path = artifact.join("objects/cloud_store/midge_primary_lease.json");
        let lease_bytes = fs::read(&lease_path)?;
        assert!(lease_bytes.starts_with(b"lease_version: 5\n"));
        let backup_lease = crate::lease::cloud::expired_backup_lease_document(&lease_bytes)
            .expect("active gate is retained in backup");
        assert!(String::from_utf8_lossy(&backup_lease).contains("\"phase\":\"active\""));
        let restored_default = restored.get_column_family("default").expect("restored CF");
        let read = restored.begin_tx(restored_default.id(), TransactionMode::ReadOnly)?;
        assert_eq!(
            read.get(b"backup-key")?.as_deref(),
            Some(b"backup-value".as_slice())
        );
        assert_eq!(
            read.get(b"wal-key")?.as_deref(),
            Some(b"wal-value".as_slice())
        );
        drop(read);
        restored.shutdown(timeout)?;
        active.shutdown(timeout)?;
        Ok(())
    }
}
