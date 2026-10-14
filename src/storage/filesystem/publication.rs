use super::{fs, Path, StorageOutcome, Write};

/// Prefix of in-flight object temp files. Listing skips them, so a crash
/// mid-write never exposes a partial object.
pub(super) const TEMP_OBJECT_MARKER: &str = ".tmp.";

static TEMP_OBJECT_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// The last modified time, in nanoseconds since the epoch, stamped on a
/// published object by this process; see [`next_object_modified_time`].
static LAST_OBJECT_MODIFIED_NANOS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Step between stamped modified times: representable on every supported
/// filesystem (NTFS stores 100 ns, the others 1 ns).
const OBJECT_MODIFIED_STEP_NANOS: u64 = 1_000;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Publish {
    /// Replace any existing object.
    Replace,
    /// Fail with a precondition error if the object already exists.
    CreateNew,
}

/// Make a directory's entries durable after publishing into it. On Windows
/// this relies on the file metadata journal. New ancestor directories go
/// through `io::durable_dir`, which flushes directory handles on Windows the
/// same way `RealFs::sync_dir` does.
#[cfg_attr(not(unix), allow(clippy::unnecessary_wraps))]
pub(super) fn sync_directory(dir: &Path) -> std::io::Result<()> {
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
pub(super) fn next_object_modified_time(
    replaced: Option<std::time::SystemTime>,
) -> std::time::SystemTime {
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
    let _ = LAST_OBJECT_MODIFIED_NANOS.try_update(Ordering::AcqRel, Ordering::Acquire, |last| {
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
pub(super) fn stamp_after_replaced(
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
pub(super) fn publish_object_atomically(
    root: &Path,
    full_path: &Path,
    data: &[u8],
    mode: Publish,
) -> StorageOutcome<()> {
    let Some(parent) = full_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return StorageOutcome::Err(
            format!("object path has no parent: {}", full_path.display()).into(),
        );
    };
    if let Err(error) = crate::io::durable_dir::create_dir_all_durably(root, parent) {
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
