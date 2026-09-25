//! Versioned, provider-neutral backup artifacts.

use super::verification::VerificationBarrierGuard;
use super::{Engine, OpenOptions, Storage};
use crate::common::{MidgeError, MidgeResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

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
    /// The event loop briefly fences mutations while it syncs the current WAL
    /// and opens all files in the captured layout. File content is copied after
    /// the mutation barrier is released.
    ///
    /// # Errors
    ///
    /// Returns an error when capture is unsupported, the barrier cannot be
    /// acquired, durable files cannot be pinned or copied, or the artifact
    /// cannot be atomically published.
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

        let destination = destination.as_ref();
        if destination.exists() {
            return Err(MidgeError::InvalidArgument(format!(
                "backup destination '{}' already exists",
                destination.display()
            )));
        }
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;

        let mut barrier =
            VerificationBarrierGuard::acquire_for_backup(&self.runtime_handle, timeout)?;
        if !matches!(barrier.health(), crate::config::EngineHealth::Healthy) {
            return Err(MidgeError::Fenced(
                "backup capture barrier reports an unhealthy engine".to_string(),
            ));
        }
        let database_format_version =
            crate::metadata::format::validate_format_marker(&self.db_path)?;
        let pinned = pin_durable_files(&self.db_path, self.simulated_cloud_mode)?;
        if !self.is_primary_lease_healthy() {
            return Err(MidgeError::Fenced(
                "primary lease became unhealthy during backup capture".to_string(),
            ));
        }
        let durability_frontier = barrier.sequence();
        barrier.release()?;

        let backup_id = uuid::Uuid::new_v4().to_string();
        let staging = parent.join(format!(".midge-backup-{backup_id}.tmp"));
        fs::create_dir(&staging)?;
        let result = materialize_backup(
            pinned,
            &staging,
            backup_id,
            durability_frontier,
            database_format_version,
            if self.simulated_cloud_mode {
                BackupStorageKind::CloudSimulated
            } else {
                BackupStorageKind::Local
            },
        );
        let manifest = match result {
            Ok(manifest) => manifest,
            Err(error) => {
                let _ = fs::remove_dir_all(&staging);
                return Err(error);
            }
        };
        sync_directory(&staging)?;
        fs::rename(&staging, destination)?;
        sync_directory(parent)?;
        Ok(manifest)
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
    /// verification fails.
    pub fn restore_backup(
        artifact: impl AsRef<Path>,
        options: OpenOptions,
    ) -> MidgeResult<BackupManifest> {
        let artifact = artifact.as_ref();
        let manifest = validate_backup(artifact)?;
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
        if target.exists() {
            return Err(MidgeError::InvalidArgument(format!(
                "restore target '{}' must not exist",
                target.display()
            )));
        }
        let parent = target
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let staging = parent.join(format!(".midge-restore-{}.tmp", manifest.backup_id));
        if staging.exists() {
            return Err(MidgeError::InvalidArgument(format!(
                "restore staging path '{}' already exists",
                staging.display()
            )));
        }
        fs::create_dir(&staging)?;
        let restore_result = copy_verified_objects(artifact, &staging, &manifest).and_then(|()| {
            let verification = match kind {
                BackupStorageKind::Local => crate::engine::StorageVerifier::verify_path(&staging),
                BackupStorageKind::CloudSimulated => {
                    crate::engine::verification::StorageVerifier::verify_simulated_cloud_path(
                        &staging,
                    )
                }
            }?;
            let _ = verification;
            let restored_format = crate::metadata::format::validate_format_marker(&staging)?;
            if restored_format != manifest.database_format_version {
                return Err(MidgeError::Corruption(format!(
                    "backup inventory format {} does not match restored database format {}",
                    manifest.database_format_version, restored_format
                )));
            }
            Ok(())
        });
        if let Err(error) = restore_result {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }

        if target.exists() {
            let _ = fs::remove_dir_all(&staging);
            return Err(MidgeError::InvalidArgument(format!(
                "restore target '{}' must not exist",
                target.display()
            )));
        }
        fs::rename(&staging, &target)?;
        sync_directory(parent)?;
        Ok(manifest)
    }
}

fn pin_durable_files(root: &Path, simulated_cloud: bool) -> MidgeResult<Vec<PinnedFile>> {
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
    for name in ["wal", "sst"] {
        collect_files(&root.join(name), root, &mut paths)?;
    }
    if simulated_cloud {
        for name in ["cloud_store", "hybrid_local"] {
            collect_files(&root.join(name), root, &mut paths)?;
        }
    }
    paths.sort();
    paths.dedup();
    paths
        .into_iter()
        .map(|path| {
            let relative = path.strip_prefix(root).map_err(|_| {
                MidgeError::Corruption("backup path escaped database root".to_string())
            })?;
            validate_relative_path(relative)?;
            let file = File::open(&path)?;
            let metadata = file.metadata()?;
            if !metadata.is_file() {
                return Err(MidgeError::Corruption(format!(
                    "backup object '{}' is not a regular file",
                    path.display()
                )));
            }
            Ok(PinnedFile {
                relative: relative.to_path_buf(),
                size: metadata.len(),
                file,
            })
        })
        .collect()
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
        captured_at: chrono::Utc::now().to_rfc3339(),
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
    if manifest.database_format_version > crate::metadata::format::CURRENT_FORMAT_VERSION {
        return Err(MidgeError::CompatibilityError(format!(
            "backup database format {} is newer than supported format {}",
            manifest.database_format_version,
            crate::metadata::format::CURRENT_FORMAT_VERSION
        )));
    }
    uuid::Uuid::parse_str(&manifest.backup_id).map_err(|_| {
        MidgeError::Corruption("backup inventory has an invalid backup identifier".to_string())
    })?;
    let mut actual = BTreeSet::new();
    collect_inventory_objects(&objects_root, &objects_root, &mut actual)?;
    let mut declared = BTreeSet::new();
    for object in &manifest.objects {
        let relative = Path::new(&object.path);
        validate_relative_path(relative)?;
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
        let relative = Path::new(&object.path);
        validate_relative_path(relative)?;
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
    if path.as_os_str().is_empty()
        || path.to_string_lossy().contains('\\')
        || path.to_string_lossy().contains(':')
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
