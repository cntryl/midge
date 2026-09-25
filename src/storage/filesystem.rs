//! Filesystem storage backend implementing `StorageBackend` trait (hot path).
//!
//! Provides synchronous local filesystem storage via callback-based operations.
//! Executes immediately but conforms to the async-compatible `StorageBackend` trait.
//!
//! **On the hot path** for:
//! - Local SST cache reads/writes
//! - WAL segment fallback (before cloud upload)
//! - Test backends via `HybridStorage`
//!
//! Design is callback-driven to integrate with `CloudExecutor` and avoid blocking
//! the main engine thread.

use crate::common::MidgeResult;
use crate::storage::{
    StorageBackend, StorageCallback, StorageEvent, StorageObjectMetadata, StorageOutcome,
};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::LazyLock;

#[cfg(test)]
mod range_identity_tests;
#[cfg(windows)]
mod windows;

const CONDITIONAL_LOCK_STRIPES: usize = 64;
static MUTATION_LOCKS: LazyLock<Vec<parking_lot::Mutex<()>>> = LazyLock::new(|| {
    (0..CONDITIONAL_LOCK_STRIPES)
        .map(|_| parking_lot::Mutex::new(()))
        .collect()
});

/// Cross-process companion to the in-process stripe lock. Conditional
/// `If-Match` operations must serialize against ordinary writers too; a
/// process-local mutex alone cannot provide that guarantee.
#[cfg(unix)]
struct ProcessMutationLock {
    file: std::fs::File,
}

#[cfg(unix)]
impl Drop for ProcessMutationLock {
    fn drop(&mut self) {
        // SAFETY: `file` remains open for the lifetime of the lock and the
        // descriptor was successfully opened by `acquire_process_lock`.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[cfg(windows)]
struct ProcessMutationLock {
    file: std::fs::File,
}

#[cfg(windows)]
impl Drop for ProcessMutationLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(not(any(unix, windows)))]
struct ProcessMutationLock;

/// Filesystem-based storage backend
///
/// Implements `StorageBackend` synchronously. Suitable for local file storage.
/// All operations execute immediately and send completion events via callback.
pub struct FileSystem {
    base_path: PathBuf,
}

impl FileSystem {
    /// Create a new filesystem storage backend.
    pub fn new<P: AsRef<Path>>(base_path: P) -> MidgeResult<Self> {
        let path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?; // Ensure base dir exists
        Ok(Self {
            base_path: fs::canonicalize(path)?,
        })
    }

    /// Compute a sanitized full path for a given key.
    fn full_path(&self, key: &str) -> Result<PathBuf, crate::storage::StorageError> {
        // Prevent absolute paths or path traversal outside the base directory.
        // Treat the key as a relative, forward-slash-friendly path.
        let mut out = self.base_path.clone();
        for component in Path::new(key).components() {
            match component {
                Component::Normal(part) => out.push(part),
                Component::CurDir
                | Component::ParentDir
                | Component::RootDir
                | Component::Prefix(_) => {}
            }
        }
        let relative = out.strip_prefix(&self.base_path).map_err(|error| {
            crate::storage::StorageError::io(format!(
                "storage path escaped base directory: {error}"
            ))
        })?;
        let mut current = self.base_path.clone();
        for component in relative.components() {
            let Component::Normal(part) = component else {
                continue;
            };
            current.push(part);
            match fs::symlink_metadata(&current) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(crate::storage::StorageError::io(format!(
                        "storage path contains a symlink: {}",
                        current.display()
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => {
                    return Err(crate::storage::StorageError::io(format!(
                        "inspect storage path component {}: {error}",
                        current.display()
                    )));
                }
            }
        }
        Ok(out)
    }

    fn acquire_process_lock(
        &self,
        full_path: &Path,
    ) -> Result<ProcessMutationLock, crate::storage::StorageError> {
        #[cfg(unix)]
        {
            let lock_dir = self.base_path.join(".midge-locks");
            fs::create_dir_all(&lock_dir).map_err(|error| {
                format!("create lock directory {}: {error}", lock_dir.display())
            })?;
            let lock_name = process_lock_stripe_name(full_path);
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_dir.join(lock_name))
                .map_err(|error| {
                    crate::storage::StorageError::io(format!(
                        "open conditional mutation lock: {error}"
                    ))
                })?;
            // SAFETY: `file` is a valid open descriptor. `LOCK_EX` requests an
            // advisory exclusive lock which is released in `Drop` above.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(crate::storage::StorageError::io(format!(
                    "acquire conditional mutation lock: {}",
                    std::io::Error::last_os_error()
                )));
            }
            Ok(ProcessMutationLock { file })
        }

        #[cfg(windows)]
        {
            let lock_dir = self.base_path.join(".midge-locks");
            fs::create_dir_all(&lock_dir).map_err(|error| {
                format!("create lock directory {}: {error}", lock_dir.display())
            })?;
            let lock_name = process_lock_stripe_name(full_path);
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_dir.join(lock_name))
                .map_err(|error| {
                    crate::storage::StorageError::io(format!(
                        "open conditional mutation lock: {error}"
                    ))
                })?;
            file.lock().map_err(|error| {
                crate::storage::StorageError::io(format!(
                    "acquire conditional mutation lock: {error}"
                ))
            })?;
            Ok(ProcessMutationLock { file })
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = full_path;
            let lock_dir = self.base_path.join(".midge-locks");
            fs::create_dir_all(&lock_dir).map_err(|error| {
                format!("create lock directory {}: {error}", lock_dir.display())
            })?;
            Ok(ProcessMutationLock)
        }
    }
}

/// Prefix of in-flight object temp files. Listing skips them, so a crash
/// mid-write never exposes a partial object.
const TEMP_OBJECT_MARKER: &str = ".tmp.";

static TEMP_OBJECT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// The last modified time, in nanoseconds since the epoch, stamped on a
/// published object by this process; see [`next_object_modified_time`].
static LAST_OBJECT_MODIFIED_NANOS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Step between stamped modified times: representable on every supported
/// filesystem (NTFS stores 100 ns, the others 1 ns).
const OBJECT_MODIFIED_STEP_NANOS: u64 = 1_000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Publish {
    /// Replace any existing object.
    Replace,
    /// Fail with a precondition error if the object already exists.
    CreateNew,
}

/// Make a directory's entries durable. Windows persists them with the file
/// metadata journal and cannot open directories for syncing.
#[cfg_attr(not(unix), allow(clippy::unnecessary_wraps))]
fn sync_directory(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Create `dir` and every missing ancestor, then make each new directory
/// entry durable so a crash cannot drop the path to a durable object.
fn create_dir_all_durably(dir: &Path) -> std::io::Result<()> {
    let mut missing = Vec::new();
    let mut probe = Some(dir);
    while let Some(path) = probe {
        if path.as_os_str().is_empty() || path.exists() {
            break;
        }
        missing.push(path.to_path_buf());
        probe = path.parent();
    }
    fs::create_dir_all(dir)?;
    for created in missing.iter().rev() {
        if let Some(parent) = created
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            sync_directory(parent)?;
        }
    }
    Ok(())
}

/// The modified time to stamp on a new version of an object.
///
/// Range identities (`fs:` etags) are built from the inode and timestamps,
/// and they guard compare-and-swap on mutable control objects. Replacement
/// frees the old inode, which the filesystem may hand to the next temp
/// file, and the kernel's ctime advances on a coarse tick; so without this
/// stamp three quick writes could repeat an old identity for new content
/// (#557). The stamp is strictly later than the replaced version's modified
/// time and than every time this process stamped before, which also covers
/// a delete followed by a recreate. Other processes read the same clock, so
/// they collide only on an identical clock reading (1 ns on Linux, 1 us on
/// macOS, 100 ns on Windows) together with a delete, a recreate and inode
/// reuse. [`stamp_after_replaced`] handles filesystems that keep coarser
/// times.
fn next_object_modified_time(replaced: Option<std::time::SystemTime>) -> std::time::SystemTime {
    use std::sync::atomic::Ordering;
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = |time: SystemTime| {
        time.duration_since(UNIX_EPOCH).map_or(0, |elapsed| {
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
        })
    };
    let floor = nanos(SystemTime::now()).max(replaced.map_or(0, |time| {
        nanos(time).saturating_add(OBJECT_MODIFIED_STEP_NANOS)
    }));
    let mut stamped = floor;
    let _ = LAST_OBJECT_MODIFIED_NANOS.fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
        stamped = floor.max(last.saturating_add(OBJECT_MODIFIED_STEP_NANOS));
        Some(stamped)
    });
    UNIX_EPOCH + std::time::Duration::from_nanos(stamped)
}

/// Larger steps for filesystems that keep modified times at whole seconds
/// (HFS+, some network mounts) or two seconds (FAT, exFAT).
const COARSE_MODIFIED_STEPS: [std::time::Duration; 2] = [
    std::time::Duration::from_secs(1),
    std::time::Duration::from_secs(2),
];

/// Store `stamp` through `set`, which returns the time the filesystem kept,
/// and make sure the kept time is later than the replaced version's. A
/// filesystem coarser than [`OBJECT_MODIFIED_STEP_NANOS`] truncates the stamp
/// back to the old time, so step past it in whole seconds instead; if even
/// that cannot order the versions, fail the write rather than publish a
/// version whose identity could repeat an old one (#557).
fn stamp_after_replaced(
    replaced: Option<std::time::SystemTime>,
    stamp: std::time::SystemTime,
    mut set: impl FnMut(std::time::SystemTime) -> std::io::Result<std::time::SystemTime>,
) -> std::io::Result<()> {
    let kept = set(stamp)?;
    let Some(replaced) = replaced else {
        return Ok(());
    };
    if kept > replaced {
        return Ok(());
    }
    for step in COARSE_MODIFIED_STEPS {
        if set(replaced + step)? > replaced {
            return Ok(());
        }
    }
    Err(std::io::Error::other(
        "filesystem cannot order object versions by modified time",
    ))
}

/// Write `data` to `full_path` so readers see either the previous object or
/// the complete new one, and the result survives a crash: the bytes go to a
/// synced temp file in the same directory, which is then renamed (replace)
/// or hard-linked (create-new, atomic against an existing object) into
/// place before the directory is synced. Every version is stamped with a
/// modified time later than any earlier version's, so identity-based etags
/// change on every overwrite even when the inode is reused.
fn publish_object_atomically(full_path: &Path, data: &[u8], mode: Publish) -> StorageOutcome<()> {
    let Some(parent) = full_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return StorageOutcome::Err(
            format!("object path has no parent: {}", full_path.display()).into(),
        );
    };
    if let Err(error) = create_dir_all_durably(parent) {
        return StorageOutcome::Err(format!("mkdir {}: {error}", parent.display()).into());
    }
    let file_name = full_path
        .file_name()
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    let nonce = TEMP_OBJECT_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temp = parent.join(format!(
        ".{file_name}{TEMP_OBJECT_MARKER}{}.{nonce}",
        std::process::id()
    ));
    let replaced = fs::metadata(full_path)
        .and_then(|metadata| metadata.modified())
        .ok();
    let written = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .and_then(|mut file| {
            file.write_all(data)?;
            stamp_after_replaced(replaced, next_object_modified_time(replaced), |time| {
                file.set_modified(time)?;
                file.metadata()?.modified()
            })?;
            file.sync_all()
        });
    if let Err(error) = written {
        let _ = fs::remove_file(&temp);
        return StorageOutcome::Err(format!("write {}: {error}", full_path.display()).into());
    }
    crate::failpoints::fail_point!("midge::storage::fs_after_temp_object_write", |_| {
        StorageOutcome::Err(
            "failpoint: interrupted before publishing object"
                .to_string()
                .into(),
        )
    });
    let published = match mode {
        Publish::Replace => fs::rename(&temp, full_path),
        Publish::CreateNew => {
            let linked = fs::hard_link(&temp, full_path);
            let _ = fs::remove_file(&temp);
            linked
        }
    };
    match published {
        Ok(()) => {}
        Err(error)
            if mode == Publish::CreateNew && error.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            return StorageOutcome::Err(crate::storage::StorageError::precondition_failed(
                "object already exists",
            ));
        }
        Err(error) => {
            let _ = fs::remove_file(&temp);
            return StorageOutcome::Err(format!("publish {}: {error}", full_path.display()).into());
        }
    }
    match sync_directory(parent) {
        Ok(()) => StorageOutcome::Ok(()),
        Err(error) => {
            StorageOutcome::Err(format!("sync directory {}: {error}", parent.display()).into())
        }
    }
}

fn range_io_error(error: &std::io::Error) -> crate::storage::StorageError {
    if error.kind() == std::io::ErrorKind::NotFound {
        crate::storage::StorageError::not_found(error)
    } else {
        crate::storage::StorageError::io(error)
    }
}

#[cfg(not(windows))]
fn range_metadata(
    metadata: &fs::Metadata,
) -> Result<StorageObjectMetadata, crate::storage::StorageError> {
    if !metadata.is_file() {
        return Err("range reads require an ordinary immutable file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(StorageObjectMetadata {
            size: metadata.len(),
            etag: format!(
                "fs:{}:{}:{}:{}:{}:{}",
                metadata.dev(),
                metadata.ino(),
                metadata.mtime(),
                metadata.mtime_nsec(),
                metadata.ctime(),
                metadata.ctime_nsec()
            ),
            generation: None,
        })
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Err("filesystem backend lacks stable range identity on this platform".into())
    }
}

fn range_file_metadata(
    file: &fs::File,
) -> Result<StorageObjectMetadata, crate::storage::StorageError> {
    #[cfg(windows)]
    {
        windows::range_metadata(file)
    }
    #[cfg(not(windows))]
    {
        range_metadata(&file.metadata().map_err(|error| range_io_error(&error))?)
    }
}

fn range_path_metadata(path: &Path) -> Result<StorageObjectMetadata, crate::storage::StorageError> {
    #[cfg(windows)]
    {
        let file = fs::File::open(path).map_err(|error| range_io_error(&error))?;
        range_file_metadata(&file)
    }
    #[cfg(not(windows))]
    {
        range_metadata(&fs::metadata(path).map_err(|error| range_io_error(&error))?)
    }
}

/// Cross-process lock file for `full_path`. Keys share a fixed set of stripe
/// files, so the lock directory stays bounded no matter how many distinct
/// objects are written or deleted; a collision only serializes two keys.
/// Each operation holds at most one stripe, always after its in-process
/// mutex, so sharing a stripe cannot deadlock.
fn process_lock_stripe_name(full_path: &Path) -> String {
    let identity = full_path.to_string_lossy();
    let stripe = crc32c::crc32c(identity.as_bytes()) as usize % CONDITIONAL_LOCK_STRIPES;
    format!("stripe-{stripe:02x}.lock")
}

fn mutation_stripe(full_path: &Path) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    full_path.hash(&mut hasher);
    usize::try_from(hasher.finish()).unwrap_or(0) % CONDITIONAL_LOCK_STRIPES
}

/// Holds a mutation stripe. With failpoints compiled in it also holds the
/// failpoint read gate, taken before the stripe: a failpoint evaluated under
/// the stripe must not wait on the gate while a failpoint test that holds the
/// gate waits on the same stripe for an unrelated key.
pub(super) struct MutationGuard {
    _stripe: parking_lot::MutexGuard<'static, ()>,
    #[cfg(feature = "failpoints")]
    _failpoint_gate: Option<parking_lot::RwLockReadGuard<'static, ()>>,
}

fn mutation_lock(full_path: &Path) -> MutationGuard {
    #[cfg(feature = "failpoints")]
    let failpoint_gate = crate::failpoints::read_gate();
    MutationGuard {
        _stripe: MUTATION_LOCKS[mutation_stripe(full_path)].lock(),
        #[cfg(feature = "failpoints")]
        _failpoint_gate: failpoint_gate,
    }
}

impl StorageBackend for FileSystem {
    /// Every filesystem call completes before it returns, so the reservation
    /// only has to outlive the call: no completion thread is needed (#518).
    fn submit_write_with_reservation(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        timeout: std::time::Duration,
        reservation: std::sync::Arc<crate::common::resource_budget::ResourceReservation>,
        callback: StorageCallback,
    ) {
        self.submit_write_with_headers_and_timeout(key, data, headers, timeout, callback);
        drop(reservation);
    }

    /// See [`Self::submit_write_with_reservation`].
    fn submit_read_range_with_reservation(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        expected: StorageObjectMetadata,
        timeout: std::time::Duration,
        reservation: std::sync::Arc<crate::common::resource_budget::ResourceReservation>,
        callback: crate::storage::RangeReadCallback,
    ) {
        self.submit_read_range(key, range.start, range.end, expected, timeout, callback);
        drop(reservation);
    }

    fn submit_range_head(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: StorageCallback,
    ) {
        let result = (|| {
            if timeout.is_zero() {
                return Err(crate::storage::storage_timeout_error(
                    "range HEAD timed out",
                ));
            }
            let path = self.full_path(key)?;
            let _lock = mutation_lock(&path);
            let _process_lock = self.acquire_process_lock(&path)?;
            range_path_metadata(&path)
        })();
        let result = match result {
            Ok(metadata) => StorageOutcome::Ok(metadata),
            Err(error) => StorageOutcome::Err(error),
        };
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result,
        });
    }

    fn submit_read_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: StorageObjectMetadata,
        timeout: std::time::Duration,
        callback: crate::storage::RangeReadCallback,
    ) {
        use std::io::{Read, Seek, SeekFrom};
        let result = (|| {
            if timeout.is_zero() {
                return Err(crate::storage::storage_timeout_error(
                    "range read timed out",
                ));
            }
            if start >= end || end > expected.size {
                return Err("invalid remote SST byte range".into());
            }
            let path = self.full_path(key)?;
            let _lock = mutation_lock(&path);
            let _process_lock = self.acquire_process_lock(&path)?;
            let mut file = fs::File::open(&path).map_err(|error| range_io_error(&error))?;
            let metadata = range_file_metadata(&file)?;
            if !metadata.same_version(&expected) {
                return Err(crate::storage::StorageError::precondition_failed(
                    "remote SST version changed",
                ));
            }
            let len = usize::try_from(end - start).map_err(|error| error.to_string())?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(len)
                .map_err(|error| error.to_string())?;
            bytes.resize(len, 0);
            file.seek(SeekFrom::Start(start))
                .map_err(|error| range_io_error(&error))?;
            file.read_exact(&mut bytes)
                .map_err(|error| range_io_error(&error))?;
            let after = range_file_metadata(&file)?;
            if !after.same_version(&expected) {
                return Err(crate::storage::StorageError::precondition_failed(
                    "remote SST changed during range read",
                ));
            }
            Ok(bytes)
        })();
        let _ = callback.send(result);
    }

    fn submit_read_with_metadata(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let result = (|| {
            if timeout.is_zero() {
                return Err(crate::storage::storage_timeout_error(
                    "metadata read has no remaining budget",
                ));
            }
            let path = self.full_path(key)?;
            let _lock = mutation_lock(&path);
            let _process_lock = self.acquire_process_lock(&path)?;
            let bytes = fs::read(&path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    crate::storage::StorageError::not_found(format!(
                        "read {}: {error}",
                        path.display()
                    ))
                } else {
                    crate::storage::StorageError::io(format!("read {}: {error}", path.display()))
                }
            })?;
            let metadata = StorageObjectMetadata::content_crc(bytes.len() as u64, &bytes);
            Ok((bytes, metadata))
        })();
        let _ = callback.send(result);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        let full_path = match self.full_path(key) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let _lock = mutation_lock(&full_path);
        let _process_lock = match self.acquire_process_lock(&full_path) {
            Ok(lock) => lock,
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };

        let outcome = publish_object_atomically(&full_path, &data, Publish::Replace);

        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: outcome,
        });
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        if headers.is_empty() {
            self.submit_write(key, data, callback);
            return;
        }

        let full_path = match self.full_path(key) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let if_none_match = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-none-match"))
            .map(|(_, value)| value.trim().to_string());
        let if_match = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-match"))
            .map(|(_, value)| value.trim().trim_matches('"').to_string());

        let _lock = mutation_lock(&full_path);
        let _process_lock = match self.acquire_process_lock(&full_path) {
            Ok(lock) => lock,
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let outcome = if let Some(expected) = if_match {
            let identity = if expected.starts_with("fs:") {
                range_path_metadata(&full_path).map(|metadata| metadata.etag)
            } else {
                fs::read(&full_path)
                    .map_err(crate::storage::StorageError::from)
                    .map(|data| StorageObjectMetadata::content_crc(data.len() as u64, &data).etag)
            };
            match identity {
                Ok(current) => {
                    if current == expected {
                        publish_object_atomically(&full_path, &data, Publish::Replace)
                    } else {
                        StorageOutcome::Err(crate::storage::StorageError::precondition_failed(
                            "etag mismatch",
                        ))
                    }
                }
                Err(error) => StorageOutcome::Err(crate::storage::StorageError::new(
                    error.kind(),
                    format!("read {}: {error}", full_path.display()),
                )),
            }
        } else if if_none_match.as_deref() == Some("*") {
            publish_object_atomically(&full_path, &data, Publish::CreateNew)
        } else {
            StorageOutcome::Err(
                "conditional write requires a supported precondition"
                    .to_string()
                    .into(),
            )
        };

        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: outcome,
        });
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        let full_path = match self.full_path(key) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let _lock = mutation_lock(&full_path);
        let _process_lock = match self.acquire_process_lock(&full_path) {
            Ok(lock) => lock,
            Err(error) => {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };

        // Deleting an absent object succeeds, as on every cloud provider.
        let outcome = match fs::remove_file(&full_path) {
            Ok(()) => StorageOutcome::Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => StorageOutcome::Ok(()),
            Err(e) => StorageOutcome::Err(format!("delete {}: {e}", full_path.display()).into()),
        };

        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: outcome,
        });
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        if headers.is_empty() {
            self.submit_delete(key, callback);
            return;
        }

        let full_path = match self.full_path(key) {
            Ok(path) => path,
            Err(error) => {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let _lock = mutation_lock(&full_path);
        let _process_lock = match self.acquire_process_lock(&full_path) {
            Ok(lock) => lock,
            Err(error) => {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let outcome = if let Some((_, expected)) = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-match"))
        {
            let identity = if expected.trim_matches('"').starts_with("fs:") {
                range_path_metadata(&full_path).map(|metadata| metadata.etag)
            } else {
                fs::read(&full_path)
                    .map_err(crate::storage::StorageError::from)
                    .map(|data| StorageObjectMetadata::content_crc(data.len() as u64, &data).etag)
            };
            match identity {
                Ok(current) => {
                    if current == expected.trim_matches('"') {
                        match fs::remove_file(&full_path) {
                            Ok(()) => StorageOutcome::Ok(()),
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                                StorageOutcome::Ok(())
                            }
                            Err(error) => StorageOutcome::Err(
                                format!("delete {}: {error}", full_path.display()).into(),
                            ),
                        }
                    } else {
                        StorageOutcome::Err(crate::storage::StorageError::precondition_failed(
                            "etag mismatch",
                        ))
                    }
                }
                // The targeted version is already gone, so no other version
                // can be deleted by mistake.
                Err(error) if error.is_not_found() => StorageOutcome::Ok(()),
                Err(error) => StorageOutcome::Err(crate::storage::StorageError::new(
                    error.kind(),
                    format!("read {}: {error}", full_path.display()),
                )),
            }
        } else {
            StorageOutcome::Err(
                "conditional delete requires a supported If-Match precondition"
                    .to_string()
                    .into(),
            )
        };

        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: outcome,
        });
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        let result = self
            .full_path(key)
            .and_then(|path| {
                let _lock = mutation_lock(&path);
                let _process_lock = self.acquire_process_lock(&path)?;
                let bytes = fs::read(&path).map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        crate::storage::StorageError::not_found(format!(
                            "read {}: {error}",
                            path.display()
                        ))
                    } else {
                        crate::storage::StorageError::io(format!(
                            "read {}: {error}",
                            path.display()
                        ))
                    }
                })?;
                Ok(StorageObjectMetadata::content_crc(
                    bytes.len() as u64,
                    &bytes,
                ))
            })
            .map_or_else(StorageOutcome::Err, StorageOutcome::Ok);
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Arc, Barrier};
    use tempfile::TempDir;

    // =========== Write Tests ===========

    #[test]
    fn should_write_file_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let data = b"test data".to_vec();

        // Act
        fs.submit_write("test.txt", data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { key, result } => {
                assert_eq!(key, "test.txt");
                assert!(result.is_ok());
                // Verify file was actually written
                let content = std::fs::read(temp_dir.path().join("test.txt")).unwrap();
                assert_eq!(content, data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_empty_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("empty.txt", vec![], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("empty.txt")).unwrap();
                assert!(content.is_empty());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_large_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let large_data = vec![42u8; 1_000_000];

        // Act
        fs.submit_write("large.bin", large_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("large.bin")).unwrap();
                assert_eq!(content, large_data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_create_parent_directories() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("subdir/nested/file.txt", b"data".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("subdir/nested/file.txt").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_overwrite_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("file.txt");
        std::fs::write(&path, b"old").unwrap();

        // Act
        let (tx, rx) = mpsc::channel();
        fs.submit_write("file.txt", b"new".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(&path).unwrap();
                assert_eq!(content, b"new");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_binary_data() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let binary_data = vec![0u8, 1u8, 255u8, 254u8];

        // Act
        fs.submit_write("binary.bin", binary_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("binary.bin")).unwrap();
                assert_eq!(content, binary_data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_create_file_when_if_none_match_star_and_missing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("conditional-create.txt");
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write_with_headers(
            "conditional-create.txt",
            b"new".to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert_eq!(std::fs::read(&path).unwrap(), b"new");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_reject_create_when_if_none_match_star_and_existing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("conditional-existing.txt");
        std::fs::write(&path, b"old").unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write_with_headers(
            "conditional-existing.txt",
            b"new".to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_err());
                assert_eq!(std::fs::read(&path).unwrap(), b"old");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_allow_exactly_one_concurrent_conditional_create() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let backend = Arc::new(FileSystem::new(temp_dir.path()).unwrap());
        let contenders = 8;
        let barrier = Arc::new(Barrier::new(contenders));

        // Act
        let joins: Vec<_> = (0..contenders)
            .map(|index| {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let (tx, rx) = mpsc::channel();
                    barrier.wait();
                    backend.submit_write_with_headers(
                        "racing-create.txt",
                        format!("writer-{index}").into_bytes(),
                        vec![("If-None-Match".into(), "*".into())],
                        tx,
                    );
                    match rx.recv().expect("conditional create response") {
                        StorageEvent::WriteComplete { result, .. } => result.is_ok(),
                        other => panic!("unexpected response: {other:?}"),
                    }
                })
            })
            .collect();
        let success_count = joins
            .into_iter()
            .map(|join| usize::from(join.join().expect("create contender panicked")))
            .sum::<usize>();

        // Assert
        assert_eq!(success_count, 1);
        assert!(temp_dir.path().join("racing-create.txt").exists());
    }

    #[test]
    fn should_allow_one_winner_when_compare_swap_updates_race() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let key = "racing-cas.txt";
        let initial = b"initial".to_vec();
        std::fs::write(temp_dir.path().join(key), &initial).unwrap();
        let etag = StorageObjectMetadata::content_crc(initial.len() as u64, &initial).etag;
        let backend = Arc::new(FileSystem::new(temp_dir.path()).unwrap());
        let contenders = 8;
        let barrier = Arc::new(Barrier::new(contenders));

        // Act
        let joins: Vec<_> = (0..contenders)
            .map(|index| {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                let etag = etag.clone();
                std::thread::spawn(move || {
                    let (tx, rx) = mpsc::channel();
                    barrier.wait();
                    backend.submit_write_with_headers(
                        key,
                        format!("writer-{index}").into_bytes(),
                        vec![("If-Match".into(), etag)],
                        tx,
                    );
                    match rx.recv().expect("conditional update response") {
                        StorageEvent::WriteComplete { result, .. } => result.is_ok(),
                        other => panic!("unexpected response: {other:?}"),
                    }
                })
            })
            .collect();
        let success_count = joins
            .into_iter()
            .map(|join| usize::from(join.join().expect("CAS contender panicked")))
            .sum::<usize>();

        // Assert
        assert_eq!(success_count, 1);
        assert_ne!(std::fs::read(temp_dir.path().join(key)).unwrap(), initial);
    }

    // =========== Read Tests ===========

    #[test]
    fn should_read_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = b"hello world";
        std::fs::write(temp_dir.path().join("test.txt"), data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read_with_metadata("test.txt", std::time::Duration::from_secs(5), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            Ok((content, _metadata)) => assert_eq!(content, data),
            Err(e) => panic!("Read failed: {e}"),
        }
    }

    #[test]
    fn should_fail_reading_nonexistent_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read_with_metadata("nonexistent.txt", std::time::Duration::from_secs(5), tx);
        let event = rx.recv().unwrap();

        // Assert
        assert!(matches!(event, Err(error) if error.is_not_found()));
    }

    #[test]
    fn should_read_large_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = vec![42u8; 1_000_000];
        std::fs::write(temp_dir.path().join("large.bin"), &data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read_with_metadata("large.bin", std::time::Duration::from_secs(5), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            Ok((content, _metadata)) => assert_eq!(content, data),
            Err(e) => panic!("Read failed: {e}"),
        }
    }

    #[test]
    fn should_read_empty_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(temp_dir.path().join("empty.txt"), b"").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read_with_metadata("empty.txt", std::time::Duration::from_secs(5), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            Ok((content, _metadata)) => assert!(content.is_empty()),
            Err(e) => panic!("Read failed: {e}"),
        }
    }

    #[test]
    fn should_read_binary_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = vec![0u8, 1u8, 255u8, 254u8];
        std::fs::write(temp_dir.path().join("binary.bin"), &data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read_with_metadata("binary.bin", std::time::Duration::from_secs(5), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            Ok((content, _metadata)) => assert_eq!(content, data),
            Err(e) => panic!("Read failed: {e}"),
        }
    }

    #[test]
    fn should_complete_reserved_calls_inline_when_backend_is_filesystem() {
        // Arrange: the filesystem finishes every call before returning, so a
        // reservation needs no completion thread to outlive it (#518).
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let before = crate::storage::retained_callback::retain_calls_on_this_thread();
        let (write_tx, write_rx) = mpsc::channel();

        // Act
        fs.submit_write_with_reservation(
            "object.bin",
            b"payload".to_vec(),
            Vec::new(),
            std::time::Duration::from_secs(5),
            std::sync::Arc::new(budget.reserve(7, "reserved write").unwrap()),
            write_tx,
        );
        let write = write_rx.recv().unwrap();
        let (head_tx, head_rx) = mpsc::channel();
        fs.submit_range_head("object.bin", std::time::Duration::from_secs(5), head_tx);
        let StorageEvent::HeadComplete {
            result: StorageOutcome::Ok(metadata),
            ..
        } = head_rx.recv().unwrap()
        else {
            panic!("range HEAD failed");
        };
        let (read_tx, read_rx) = mpsc::channel();
        fs.submit_read_range_with_reservation(
            "object.bin",
            0..7,
            metadata,
            std::time::Duration::from_secs(5),
            std::sync::Arc::new(budget.reserve(7, "reserved read").unwrap()),
            read_tx,
        );
        let read = read_rx.recv().unwrap();

        // Assert
        assert!(matches!(
            write,
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }
        ));
        assert_eq!(read.unwrap(), b"payload");
        assert_eq!(
            crate::storage::retained_callback::retain_calls_on_this_thread(),
            before
        );
    }

    // =========== Delete Tests ===========

    #[test]
    fn should_delete_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("file.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete("file.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_report_success_when_deleting_nonexistent_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete("nonexistent.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok(), "an absent object is already deleted (#514)");
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_delete_nested_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("subdir")).unwrap();
        let path = temp_dir.path().join("subdir/file.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete("subdir/file.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_delete_file_when_if_match_header_matches_content() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("conditional.txt");
        let data = b"data".to_vec();
        std::fs::write(&path, &data).unwrap();
        let etag = StorageObjectMetadata::content_crc(data.len() as u64, &data).etag;
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete_with_headers("conditional.txt", vec![("If-Match".into(), etag)], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_reject_conditional_delete_when_if_match_header_mismatches() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("conditional-stale.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete_with_headers(
            "conditional-stale.txt",
            vec![("If-Match".into(), "crc32c:00000000".into())],
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_err());
                assert!(path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    // =========== List Tests ===========

    #[test]
    fn should_sanitize_path_traversal() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act - Try to use path traversal
        fs.submit_write("../escape_dir/evil.txt", b"x".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert - Should succeed but write inside base_path
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("escape_dir/evil.txt").exists());
                assert!(!temp_dir.path().join("../escape_dir/evil.txt").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_handle_absolute_paths() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act - Try absolute path
        fs.submit_write("/etc/passwd", b"x".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert - Should sanitize
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("etc/passwd").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn should_reject_write_through_symlinked_parent() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let outside_dir = TempDir::new().unwrap();
        std::os::unix::fs::symlink(outside_dir.path(), temp_dir.path().join("link")).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("link/escaped.txt", b"must stay inside".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => assert!(result.is_err()),
            _ => panic!("Expected WriteComplete"),
        }
        assert!(!outside_dir.path().join("escaped.txt").exists());
    }

    #[test]
    fn should_construct_with_custom_base_path() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("marker.txt", b"custom base path".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert: the write must land under the base path passed to `new`, not some default.
        match event {
            StorageEvent::WriteComplete { result, .. } => assert!(result.is_ok()),
            _ => panic!("Expected WriteComplete"),
        }
        assert_eq!(
            fs::read(temp_dir.path().join("marker.txt")).unwrap(),
            b"custom base path"
        );
    }

    #[test]
    fn should_create_base_directory_if_missing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let new_dir = temp_dir.path().join("new_base");
        assert!(!new_dir.exists());

        // Act
        let fs = FileSystem::new(&new_dir);

        // Assert
        assert!(fs.is_ok());
        assert!(new_dir.exists());
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod atomic_publish_tests {
    use super::*;

    fn write(
        fs: &FileSystem,
        key: &str,
        data: &[u8],
        headers: Vec<(String, String)>,
    ) -> StorageOutcome<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        fs.submit_write_with_headers(key, data.to_vec(), headers, tx);
        match rx.recv().expect("write completion") {
            StorageEvent::WriteComplete { result, .. } => result,
            other => panic!("unexpected event {other:?}"),
        }
    }

    fn read(fs: &FileSystem, key: &str) -> StorageOutcome<Vec<u8>> {
        let (tx, rx) = std::sync::mpsc::channel();
        fs.submit_read_with_metadata(key, std::time::Duration::from_secs(5), tx);
        match rx.recv().expect("read completion") {
            Ok((bytes, _metadata)) => StorageOutcome::Ok(bytes),
            Err(error) => StorageOutcome::Err(error),
        }
    }

    #[test]
    fn should_not_expose_partial_object_when_conditional_create_is_interrupted() {
        // Arrange
        let _guard = crate::failpoints::test_failpoint_guard();
        let dir = tempfile::tempdir().expect("temp dir");
        let fs = FileSystem::new(dir.path()).expect("filesystem backend");
        let create = || vec![("If-None-Match".to_string(), "*".to_string())];
        fail::cfg("midge::storage::fs_after_temp_object_write", "return").expect("failpoint");

        // Act
        let interrupted = write(&fs, "wal/1.wal", b"segment", create());
        fail::remove("midge::storage::fs_after_temp_object_write");

        // Assert
        assert!(matches!(interrupted, StorageOutcome::Err(_)));
        assert!(
            matches!(read(&fs, "wal/1.wal"), StorageOutcome::Err(error) if error.is_not_found())
        );
        assert!(matches!(
            write(&fs, "wal/1.wal", b"segment", create()),
            StorageOutcome::Ok(())
        ));
        assert!(matches!(read(&fs, "wal/1.wal"), StorageOutcome::Ok(bytes) if bytes == b"segment"));
    }

    #[test]
    fn should_keep_previous_object_when_overwrite_is_interrupted() {
        // Arrange
        let _guard = crate::failpoints::test_failpoint_guard();
        let dir = tempfile::tempdir().expect("temp dir");
        let fs = FileSystem::new(dir.path()).expect("filesystem backend");
        assert!(matches!(
            write(&fs, "meta/manifest.json", b"previous", Vec::new()),
            StorageOutcome::Ok(())
        ));
        fail::cfg("midge::storage::fs_after_temp_object_write", "return").expect("failpoint");

        // Act
        let interrupted = write(&fs, "meta/manifest.json", b"replacement-bytes", Vec::new());
        fail::remove("midge::storage::fs_after_temp_object_write");

        // Assert
        assert!(matches!(interrupted, StorageOutcome::Err(_)));
        assert!(
            matches!(read(&fs, "meta/manifest.json"), StorageOutcome::Ok(bytes) if bytes == b"previous")
        );
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod mutation_lock_order_tests {
    use super::*;

    #[test]
    fn should_take_stripe_while_failpoint_test_holds_gate_when_stripe_sharer_waits() {
        // Arrange: a failpoint test holds the gate's write side. Another
        // thread mutating a different key on the same stripe evaluates a
        // failpoint inside the stripe. It must wait for the gate before the
        // stripe, or it holds the stripe the test itself needs.
        let first = std::path::PathBuf::from("stripe-order-a");
        let stripe = mutation_stripe(&first);
        let second = (0..10_000)
            .map(|index| std::path::PathBuf::from(format!("stripe-order-{index}")))
            .find(|path| path != &first && mutation_stripe(path) == stripe)
            .expect("a second path on the same stripe");
        let test_guard = crate::failpoints::test_failpoint_guard();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let sharer = std::thread::spawn(move || {
            started_tx.send(()).expect("report start");
            let _lock = mutation_lock(&second);
            crate::failpoints::fail_point!("midge::storage::test_stripe_order");
        });
        started_rx.recv().expect("sharer started");
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Act
        let acquired = MUTATION_LOCKS[stripe].try_lock_for(std::time::Duration::from_secs(2));

        // Assert
        assert!(
            acquired.is_some(),
            "a stripe sharer must not hold the stripe while waiting on the failpoint gate"
        );
        drop(acquired);
        drop(test_guard);
        sharer.join().expect("join sharer");
    }
}

#[cfg(test)]
mod lock_stripe_tests {
    use super::*;

    #[test]
    fn should_bound_lock_files_when_deleting_many_distinct_missing_keys() {
        // Arrange: in real cloud mode this backend evicts one local copy per
        // SST ever produced, so per-key lock files grew without bound.
        let dir = tempfile::tempdir().expect("temp dir");
        let fs = FileSystem::new(dir.path()).expect("filesystem backend");

        // Act
        for index in 0..1_000 {
            let (tx, rx) = std::sync::mpsc::channel();
            fs.submit_delete(&format!("sst/{index}.sst"), tx);
            let _ = rx.recv().expect("delete completion");
        }

        // Assert
        let lock_files = std::fs::read_dir(dir.path().join(".midge-locks"))
            .expect("lock dir")
            .count();
        assert!(
            lock_files <= CONDITIONAL_LOCK_STRIPES,
            "{lock_files} lock files for 1,000 keys"
        );
    }
}
