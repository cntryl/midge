use crate::common::MidgeResult;
use crate::metadata::manifest::{CloudCheckpoint, FileMeta};
use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};
use std::path::Path;

fn journal_serialize<T: ?Sized + serde::Serialize>(value: &T) -> MidgeResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| crate::common::MidgeError::Internal(e.to_string()))
}

fn journal_deserialize<T: serde::de::DeserializeOwned>(payload: &[u8]) -> MidgeResult<T> {
    serde_json::from_slice(payload).map_err(|e| crate::common::MidgeError::Internal(e.to_string()))
}

fn millis_since_epoch() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn payload_len_u32(payload: &[u8]) -> MidgeResult<u32> {
    u32::try_from(payload.len())
        .map_err(|_| crate::common::MidgeError::Internal("journal payload too large".to_string()))
}

/// TLV-encoded manifest edit record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManifestEdit {
    AddSst(FileMeta),
    RemoveSst {
        name: String,
    },
    CreateColumnFamily {
        id: u32,
        name: String,
        created_at: u64,
    },
    DropColumnFamily {
        id: u32,
    },
    BumpWalSeq {
        seq: u64,
    },
    BumpNextSstSeq {
        cf_id: u32,
        next_seq: u64,
    },
    SetCloudCheckpoint(CloudCheckpoint),
    /// Batch of edits emitted atomically as a single journal TLV record
    Batch(Vec<ManifestEdit>),
}

impl ManifestEdit {
    pub fn record_type(&self) -> u8 {
        match self {
            ManifestEdit::AddSst(_) => 1,
            ManifestEdit::RemoveSst { .. } => 2,
            ManifestEdit::CreateColumnFamily { .. } => 3,
            ManifestEdit::DropColumnFamily { .. } => 4,
            ManifestEdit::BumpWalSeq { .. } => 5,
            ManifestEdit::BumpNextSstSeq { .. } => 6,
            ManifestEdit::SetCloudCheckpoint(_) => 7,
            ManifestEdit::Batch(_) => 8,
        }
    }
}

// Special TLV record type used to mark a durable sync point in the journal.
// When replaying, only edits up to the last marker are considered durable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsyncMarker {
    pub last_persisted_sequence: u64,
    pub ts_millis: u64,
}

const BATCH_RECORD_TYPE: u8 = 8;
const FSYNC_MARKER_TYPE: u8 = 9;

/// Configurable journal sync policy (can be set via env var `MIDGE_MANIFEST_SYNC_POLICY`)
#[derive(Default, PartialEq, Eq, Debug, Clone, Copy)]
pub enum ManifestSyncPolicy {
    #[default]
    Always,
    EveryN(u64),
    TimeBased(std::time::Duration),
}

fn parse_manifest_sync_policy_from_env() -> ManifestSyncPolicy {
    let mut policy = ManifestSyncPolicy::default();
    if let Ok(s_raw) = std::env::var("MIDGE_MANIFEST_SYNC_POLICY") {
        let s = s_raw.trim().to_lowercase();
        if s == "always" {
            policy = ManifestSyncPolicy::Always;
        } else if s.starts_with("everyn:") {
            if let Some((_lhs, rhs)) = s.split_once(':') {
                if let Ok(n) = rhs.parse::<u64>() {
                    policy = ManifestSyncPolicy::EveryN(n);
                }
            }
        } else if s.starts_with("time:") {
            if let Some((_lhs, rhs)) = s.split_once(':') {
                if let Some(stripped) = rhs.strip_suffix("ms") {
                    if let Ok(v) = stripped.parse::<u64>() {
                        policy = ManifestSyncPolicy::TimeBased(std::time::Duration::from_millis(v));
                    }
                } else if let Some(stripped) = rhs.strip_suffix('s') {
                    if let Ok(v) = stripped.parse::<u64>() {
                        policy = ManifestSyncPolicy::TimeBased(std::time::Duration::from_secs(v));
                    }
                }
            }
        }
    }

    // Reset sync state when policy changes to maintain deterministic behavior in tests
    if let Some(mut state) = MANIFEST_SYNC_STATE.try_lock() {
        if state.last_policy != policy {
            state.batches_since_fsync = 0;
            state.last_policy = policy;
            state.last_fsync = std::time::Instant::now();
        }
    }

    policy
}

struct ManifestSyncState {
    batches_since_fsync: u64,
    last_fsync: std::time::Instant,
    last_policy: ManifestSyncPolicy,
}

impl Default for ManifestSyncState {
    fn default() -> Self {
        Self {
            batches_since_fsync: 0,
            last_fsync: std::time::Instant::now(),
            last_policy: ManifestSyncPolicy::default(),
        }
    }
}

use parking_lot::Mutex;
static MANIFEST_SYNC_STATE: std::sync::LazyLock<Mutex<ManifestSyncState>> =
    std::sync::LazyLock::new(|| {
        Mutex::new(ManifestSyncState {
            batches_since_fsync: 0,
            last_fsync: std::time::Instant::now(),
            last_policy: ManifestSyncPolicy::default(),
        })
    });

const JOURNAL_FILE: &str = "manifest.journal";

/// Append an edit to the manifest journal using a provided Fs (preferred).
pub fn append_edit_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    edit: &ManifestEdit,
) -> MidgeResult<()> {
    use crate::io::traits::{Durability, FsPath, OpenMode, OpenOptions};

    let payload = journal_serialize(edit)?;
    let mut hasher = Crc32::new();
    hasher.update(&payload);
    let crc = hasher.finalize();

    // Layout: [type:u8][len:u32LE][payload][crc:u32LE]
    let mut buf = Vec::with_capacity(1 + 4 + payload.len() + 4);
    buf.push(edit.record_type());
    buf.extend_from_slice(&payload_len_u32(&payload)?.to_le_bytes());
    buf.extend_from_slice(&payload);
    buf.extend_from_slice(&crc.to_le_bytes());

    let mut f = fs.open(
        &FsPath::new(JOURNAL_FILE),
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        },
    )?;

    fail::fail_point!("midge::manifest::inject_no_space_on_append_edit", |_| Err(
        crate::common::MidgeError::NoSpace(
            "failpoint: no space on manifest journal append".to_string()
        )
    ));
    let write_start = std::time::Instant::now();
    f.append(bytes::Bytes::from(buf))
        .map_err(crate::common::MidgeError::from)?;
    let write_ns = write_start.elapsed().as_nanos();

    // Honor optional bench-only skip for manifest fsync to reduce overhead.
    let skip_fsync = std::env::var("MIDGE_SKIP_MANIFEST_FSYNC").ok().as_deref() == Some("1")
        && (std::env::var("MIDGE_ALLOW_MANIFEST_SKIP_FSYNC")
            .ok()
            .as_deref()
            == Some("1"));

    // Determine sync policy
    let policy = parse_manifest_sync_policy_from_env();
    let mut should_sync = matches!(policy, ManifestSyncPolicy::Always);

    if !should_sync {
        let mut state = MANIFEST_SYNC_STATE.lock();
        match policy {
            ManifestSyncPolicy::EveryN(n) => {
                state.batches_since_fsync += 1;
                if state.batches_since_fsync >= n {
                    should_sync = true;
                    state.batches_since_fsync = 0;
                }
            }
            ManifestSyncPolicy::TimeBased(dur) if state.last_fsync.elapsed() >= dur => {
                should_sync = true;
                state.last_fsync = std::time::Instant::now();
            }
            _ => {}
        }
    }

    if skip_fsync {
        tracing::info!(
            write_ns = write_ns,
            "manifest journal append: write (fsync skipped via bench flag)"
        );
    } else if should_sync {
        let fsync_start = std::time::Instant::now();
        f.sync(Durability::Durable)
            .map_err(crate::common::MidgeError::from)?;
        let fsync_ns = fsync_start.elapsed().as_nanos();
        tracing::info!(write_ns = write_ns, fsync_ns = fsync_ns, policy = ?policy, "manifest journal append: write and fsync times (ns)");
        // Append a durable marker to indicate fsync boundary
        if let Err(e) = append_fsync_marker_with_fs(fs, 0) {
            tracing::warn!(error = ?e, "failed to append fsync marker after append_edit_with_fs");
        }
    } else {
        tracing::info!(write_ns = write_ns, policy = ?policy, "manifest journal append: write (deferred sync by policy)");
    }

    Ok(())
}

/// Convenience wrapper: append via a `RealFs` created from `db_path` (backwards compatible)
pub fn append_edit(db_path: &Path, edit: &ManifestEdit) -> MidgeResult<()> {
    let fs: std::sync::Arc<dyn crate::io::traits::Fs> =
        std::sync::Arc::new(crate::io::real::RealFs::new(db_path).map_err(|e| {
            crate::common::MidgeError::Internal(format!("failed to create RealFs: {e:?}"))
        })?);
    append_edit_with_fs(&fs, edit)
}

/// Convenience wrapper: replay journal via a `RealFs` created from `db_path` (backwards compatible)
pub fn replay_journal(db_path: &Path) -> MidgeResult<Vec<ManifestEdit>> {
    let fs: std::sync::Arc<dyn crate::io::traits::Fs> =
        std::sync::Arc::new(crate::io::real::RealFs::new(db_path).map_err(|e| {
            crate::common::MidgeError::Internal(format!("failed to create RealFs: {e:?}"))
        })?);
    replay_journal_with_fs(&fs)
}

/// Replay a journal file at `db_path`. Returns Vec<ManifestEdit> in order.
/// Stops cleanly on partial or corrupt tail record (returns edits up to that point).
pub fn replay_journal_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
) -> MidgeResult<Vec<ManifestEdit>> {
    use crate::io::traits::FsPath;

    // Open file read-only
    let file = match fs.open(
        &FsPath::new(JOURNAL_FILE),
        crate::io::traits::OpenOptions {
            mode: crate::io::traits::OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    ) {
        Ok(f) => f,
        Err(e) => match e {
            crate::io::traits::FsError::NotFound(_) => return Ok(Vec::new()),
            _ => return Err(crate::common::MidgeError::from(e)),
        },
    };

    let mut edits = Vec::new();
    let mut last_marker_edit_idx: Option<usize> = None;
    let mut fatal_prefix_error: Option<String> = None;

    // Read loop using positional reads
    let mut offset: u64 = 0;
    let file_len = file.len().map_err(crate::common::MidgeError::from)?;

    while offset < file_len {
        let record_start = offset;
        // Read header: type (1) + len (4)
        if offset + 5 > file_len {
            if edits.is_empty() && last_marker_edit_idx.is_none() && record_start == 0 {
                fatal_prefix_error = Some("manifest journal has truncated header at byte 0".into());
            }
            break; // partial header
        }
        let typ_b = file
            .read_at(offset, 1)
            .map_err(crate::common::MidgeError::from)?;
        let typ = typ_b[0];
        offset += 1;

        let len_b = file
            .read_at(offset, 4)
            .map_err(crate::common::MidgeError::from)?;
        let len = u32::from_le_bytes([len_b[0], len_b[1], len_b[2], len_b[3]]) as usize;
        offset += 4;

        // Ensure payload + crc present
        if offset + (len as u64) + 4 > file_len {
            if edits.is_empty() && last_marker_edit_idx.is_none() && record_start == 0 {
                fatal_prefix_error = Some("manifest journal has incomplete first record".into());
            }
            break; // partial payload
        }

        let payload = file
            .read_at(offset, len as u64)
            .map_err(crate::common::MidgeError::from)?;
        offset += len as u64;

        let crc_b = file
            .read_at(offset, 4)
            .map_err(crate::common::MidgeError::from)?;
        let got_crc = u32::from_le_bytes([crc_b[0], crc_b[1], crc_b[2], crc_b[3]]);
        offset += 4;

        let mut hasher = Crc32::new();
        hasher.update(&payload);
        let calc = hasher.finalize();
        if calc != got_crc {
            tracing::warn!("journal crc mismatch, stopping at tail");
            if edits.is_empty() && last_marker_edit_idx.is_none() && record_start == 0 {
                fatal_prefix_error = Some("manifest journal CRC mismatch at byte 0".into());
            }
            break;
        }

        // Dispatch
        if typ == FSYNC_MARKER_TYPE {
            match journal_deserialize::<FsyncMarker>(&payload) {
                Ok(marker) => {
                    tracing::info!(
                        last_seq = marker.last_persisted_sequence,
                        ts = marker.ts_millis,
                        "journal fsync marker encountered"
                    );
                    last_marker_edit_idx = Some(edits.len());
                }
                Err(e) => {
                    tracing::warn!("fsync marker deserialize failed: {}", e);
                    if edits.is_empty() && last_marker_edit_idx.is_none() && record_start == 0 {
                        fatal_prefix_error = Some(format!(
                            "manifest journal fsync marker deserialize failed: {e}"
                        ));
                    }
                    break;
                }
            }
        } else if typ == BATCH_RECORD_TYPE {
            match journal_deserialize::<Vec<ManifestEdit>>(&payload) {
                Ok(batch) => edits.push(ManifestEdit::Batch(batch)),
                Err(e) => {
                    tracing::warn!("journal batch deserialize failed: {}", e);
                    if edits.is_empty() && last_marker_edit_idx.is_none() && record_start == 0 {
                        fatal_prefix_error =
                            Some(format!("manifest journal batch deserialize failed: {e}"));
                    }
                    break;
                }
            }
        } else {
            match journal_deserialize::<ManifestEdit>(&payload) {
                Ok(edit) => edits.push(edit),
                Err(e) => {
                    tracing::warn!("journal record deserialize failed: {}", e);
                    if edits.is_empty() && last_marker_edit_idx.is_none() && record_start == 0 {
                        fatal_prefix_error =
                            Some(format!("manifest journal record deserialize failed: {e}"));
                    }
                    break;
                }
            }
        }
    }

    if let Some(message) = fatal_prefix_error {
        return Err(crate::common::MidgeError::Corruption(message));
    }

    if let Some(idx) = last_marker_edit_idx {
        edits.truncate(idx);
    }

    Ok(edits)
}

/// Append a batch of edits as a single TLV record using the provided Fs (preferred).
pub fn append_edit_batch_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    batch: &[ManifestEdit],
) -> MidgeResult<()> {
    use crate::io::traits::{Durability, FsPath, OpenMode, OpenOptions};

    let payload = journal_serialize(batch)?;
    let mut hasher = Crc32::new();
    hasher.update(&payload);
    let crc = hasher.finalize();

    // Layout: [type:u8][len:u32LE][payload][crc:u32LE]
    let mut buf = Vec::with_capacity(1 + 4 + payload.len() + 4);
    buf.push(BATCH_RECORD_TYPE);
    buf.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| {
                crate::common::MidgeError::Internal("journal payload too large".to_string())
            })?
            .to_le_bytes(),
    );
    buf.extend_from_slice(&payload);
    buf.extend_from_slice(&crc.to_le_bytes());

    let mut f = fs.open(
        &FsPath::new(JOURNAL_FILE),
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        },
    )?;

    fail::fail_point!(
        "midge::manifest::inject_no_space_on_append_edit_batch",
        |_| Err(crate::common::MidgeError::NoSpace(
            "failpoint: no space on manifest journal batch append".to_string()
        ))
    );
    let write_start = std::time::Instant::now();
    f.append(bytes::Bytes::from(buf))
        .map_err(crate::common::MidgeError::from)?;
    let write_ns = write_start.elapsed().as_nanos();

    // Honor optional bench-only skip for manifest fsync to reduce overhead.
    let skip_fsync = std::env::var("MIDGE_SKIP_MANIFEST_FSYNC").ok().as_deref() == Some("1")
        && (std::env::var("MIDGE_ALLOW_MANIFEST_SKIP_FSYNC")
            .ok()
            .as_deref()
            == Some("1"));

    // Determine sync policy
    let policy = parse_manifest_sync_policy_from_env();
    let mut should_sync = matches!(policy, ManifestSyncPolicy::Always);

    if !should_sync {
        let mut state = MANIFEST_SYNC_STATE.lock();
        match policy {
            ManifestSyncPolicy::EveryN(n) => {
                state.batches_since_fsync += 1;
                if state.batches_since_fsync >= n {
                    should_sync = true;
                    state.batches_since_fsync = 0;
                }
            }
            ManifestSyncPolicy::TimeBased(dur) if state.last_fsync.elapsed() >= dur => {
                should_sync = true;
                state.last_fsync = std::time::Instant::now();
            }
            _ => {}
        }
    }

    if skip_fsync {
        tracing::info!(
            batch_size = batch.len(),
            write_ns = write_ns,
            "manifest journal batch append: write (fsync skipped via bench flag)"
        );
    } else if should_sync {
        let fsync_start = std::time::Instant::now();
        f.sync(Durability::Durable)
            .map_err(crate::common::MidgeError::from)?;
        let fsync_ns = fsync_start.elapsed().as_nanos();
        tracing::info!(batch_size = batch.len(), write_ns = write_ns, fsync_ns = fsync_ns, policy = ?policy, "manifest journal batch append: write and fsync times (ns)");
        // Append a durable marker (we write a marker to denote durability boundary)
        if let Err(e) = append_fsync_marker_with_fs(fs, 0) {
            tracing::warn!(error = ?e, "failed to append fsync marker after batch append");
        }
    } else {
        tracing::info!(batch_size = batch.len(), write_ns = write_ns, policy = ?policy, "manifest journal batch append: write (deferred sync by policy)");
    }

    Ok(())
}

/// Convenience wrapper: append batch via a `RealFs` created from `db_path` (backwards compatible)
pub fn append_edit_batch(db_path: &Path, batch: &[ManifestEdit]) -> MidgeResult<()> {
    let fs: std::sync::Arc<dyn crate::io::traits::Fs> =
        std::sync::Arc::new(crate::io::real::RealFs::new(db_path).map_err(|e| {
            crate::common::MidgeError::Internal(format!("failed to create RealFs: {e:?}"))
        })?);
    append_edit_batch_with_fs(&fs, batch)
}

/// Append an FSYNC marker to indicate a durable prefix (fsynced up to now) using provided Fs.
pub fn append_fsync_marker_with_fs(
    fs: &std::sync::Arc<dyn crate::io::traits::Fs>,
    last_seq: u64,
) -> MidgeResult<()> {
    use crate::io::traits::{Durability, FsPath, OpenMode, OpenOptions};

    let marker = FsyncMarker {
        last_persisted_sequence: last_seq,
        ts_millis: millis_since_epoch(),
    };
    let payload = journal_serialize(&marker)?;
    let mut hasher = Crc32::new();
    hasher.update(&payload);
    let crc = hasher.finalize();

    let mut buf = Vec::with_capacity(1 + 4 + payload.len() + 4);
    buf.push(FSYNC_MARKER_TYPE);
    buf.extend_from_slice(&payload_len_u32(&payload)?.to_le_bytes());
    buf.extend_from_slice(&payload);
    buf.extend_from_slice(&crc.to_le_bytes());

    let mut f = fs.open(
        &FsPath::new(JOURNAL_FILE),
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: false,
        },
    )?;

    f.append(bytes::Bytes::from(buf))
        .map_err(crate::common::MidgeError::from)?;

    // Honor optional bench-only skip for manifest fsync when writing markers.
    let skip_fsync = std::env::var("MIDGE_SKIP_MANIFEST_FSYNC").ok().as_deref() == Some("1")
        && (std::env::var("MIDGE_ALLOW_MANIFEST_SKIP_FSYNC")
            .ok()
            .as_deref()
            == Some("1"));
    if skip_fsync {
        tracing::info!(
            "skip_manifest_fsync enabled: append_fsync_marker did not fsync (bench mode)"
        );
    } else {
        f.sync(Durability::Durable)
            .map_err(crate::common::MidgeError::from)?;
    }

    Ok(())
}

/// Truncate or rotate journal after snapshot using provided Fs.
pub fn truncate_journal_with_fs(fs: &std::sync::Arc<dyn crate::io::traits::Fs>) -> MidgeResult<()> {
    use crate::io::traits::{FsPath, OpenMode, OpenOptions};

    // Opening with truncate=true will set length to 0
    let mut f = fs.open(
        &FsPath::new(JOURNAL_FILE),
        OpenOptions {
            mode: OpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: true,
        },
    )?;

    // Sync file to durable state
    f.sync(crate::io::traits::Durability::Durable)
        .map_err(crate::common::MidgeError::from)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::FileMeta;
    use crate::metadata::{Manifest, ManifestPersistence};
    use proptest::prelude::*;
    use tempfile::tempdir;

    #[test]
    fn should_replay_journal_when_valid_records_exist() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();
        let sst_name = crate::sst::file_name(0, 0, 1);

        let file = FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 1024,
            cf_id: 0,
            smallest_key: Some(vec![1, 2]),
            largest_key: Some(vec![9, 9]),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        };

        // Act
        let edit = ManifestEdit::AddSst(file.clone());
        append_edit(db, &edit).expect("append_edit failed");

        // Assert
        let edits = replay_journal(db).expect("replay_journal failed");
        assert_eq!(edits.len(), 1);
        match &edits[0] {
            ManifestEdit::AddSst(m) => {
                assert_eq!(m.name, sst_name);
                assert_eq!(m.size_bytes, 1024);
            }
            _ => panic!("unexpected edit variant"),
        }
    }

    #[test]
    fn should_stop_replay_when_partial_tail() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        // create a valid edit to precede the partial record
        let file = FileMeta {
            name: "a.sst".to_string(),
            ..Default::default()
        };
        let edit = ManifestEdit::AddSst(file);
        append_edit(db, &edit).expect("append_edit failed");

        // Act: append a truncated record to simulate crash
        let path = db.join(JOURNAL_FILE);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("failed to open temp journal file");
        use std::io::Write;

        let fake = ManifestEdit::RemoveSst {
            name: "missing.sst".to_string(),
        };
        let payload = serde_json::to_vec(&fake).unwrap();
        let mut hasher = Crc32::new();
        hasher.update(&payload);
        let _crc = hasher.finalize();

        let mut buf = Vec::new();
        buf.push(fake.record_type());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload[..10.min(payload.len())]); // partial
                                                                  // do NOT write crc

        f.write_all(&buf)
            .expect("failed to write partial journal record");
        f.sync_all().expect("failed to sync partial journal file");

        // Assert: replay should only return the first valid record
        let edits = replay_journal(db).expect("replay_journal failed");
        assert_eq!(edits.len(), 1);
    }

    #[test]
    fn should_save_snapshot_to_disk_when_saved() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "one.sst".to_string(),
            level: 0,
            size_bytes: 100,
            ..Default::default()
        });

        // Act
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
            .expect("save snapshot failed");

        // Assert
        let snap = ManifestPersistence::manifest_snapshot_path(db);
        assert!(snap.exists(), "snapshot file should exist after saving");
    }

    #[test]
    fn should_truncate_journal_after_snapshot_saved() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "one.sst".to_string(),
            level: 0,
            size_bytes: 100,
            ..Default::default()
        });

        // Act
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
            .expect("save snapshot failed");

        // Assert: journal should be truncated (zero-length) if it exists
        let journal = db.join(JOURNAL_FILE);
        if journal.exists() {
            let meta = std::fs::metadata(&journal).unwrap();
            assert_eq!(
                meta.len(),
                0,
                "journal should be zero-length after snapshot/truncate"
            );
        }
    }

    #[test]
    fn should_load_snapshot_when_snapshot_exists() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        // Create snapshot
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "base.sst".to_string(),
            level: 0,
            size_bytes: 10,
            ..Default::default()
        });
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
            .expect("save snapshot failed");

        // Act
        let loaded = ManifestPersistence::load(db).expect("load failed");

        // Assert
        assert!(
            loaded.files.iter().any(|f| f.name == "base.sst"),
            "loaded manifest should include snapshot entries"
        );
    }

    #[test]
    fn should_replay_journal_on_top_of_snapshot_when_present() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        // Create snapshot
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "base.sst".to_string(),
            level: 0,
            size_bytes: 10,
            ..Default::default()
        });
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest)
            .expect("save snapshot failed");

        // Act: append an edit that should be replayed on load
        let edit = ManifestEdit::AddSst(FileMeta {
            name: "new.sst".to_string(),
            level: 1,
            size_bytes: 20,
            ..Default::default()
        });
        append_edit(db, &edit).expect("append_edit failed");

        // Assert
        let loaded = ManifestPersistence::load(db).expect("load failed");
        assert!(
            loaded.files.iter().any(|f| f.name == "base.sst"),
            "snapshot entry missing"
        );
        assert!(
            loaded.files.iter().any(|f| f.name == "new.sst"),
            "journal replay entry missing"
        );
    }

    #[test]
    fn should_replay_batch_record() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        let e1 = ManifestEdit::AddSst(FileMeta {
            name: "b1.sst".to_string(),
            level: 0,
            size_bytes: 10,
            ..Default::default()
        });
        let e2 = ManifestEdit::RemoveSst {
            name: "missing.sst".to_string(),
        };

        // Act
        append_edit_batch(db, &[e1.clone(), e2.clone()]).expect("append batch failed");

        // Assert: replay should yield a single Batch record
        let edits = replay_journal(db).expect("replay_journal failed");
        assert_eq!(edits.len(), 1);
        match &edits[0] {
            ManifestEdit::Batch(b) => {
                assert_eq!(b.len(), 2);
            }
            _ => panic!("expected batch record"),
        }
    }

    #[test]
    fn should_emit_fsync_marker_under_every_n_policy() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        // Use EveryN:1 to make the test deterministic even when tests run in parallel
        std::env::set_var("MIDGE_MANIFEST_SYNC_POLICY", "EveryN:1");
        // Reset policy state to ensure deterministic behavior in test
        {
            let mut s = MANIFEST_SYNC_STATE.lock();
            s.batches_since_fsync = 0;
            s.last_policy = ManifestSyncPolicy::default();
            s.last_fsync = std::time::Instant::now();
        }
        parse_manifest_sync_policy_from_env();

        // Act: append a single batch (policy should trigger a marker immediately)
        let e = ManifestEdit::BumpWalSeq { seq: 1 };
        append_edit_batch(db, std::slice::from_ref(&e)).expect("append batch failed");

        // Assert: journal file contains an FSYNC_MARKER_TYPE record
        let data = std::fs::read(db.join(JOURNAL_FILE)).expect("read journal");
        assert!(data.contains(&FSYNC_MARKER_TYPE));

        // Cleanup
        std::env::remove_var("MIDGE_MANIFEST_SYNC_POLICY");
    }

    proptest! {
        #[test]
        fn should_preserve_batch_order_when_replaying_bump_wal_seq_edits(
            seqs in proptest::collection::vec(0u64..10_000, 1..32)
        ) {
            // Arrange
            let td = tempdir().unwrap();
            let db = td.path();
            let batch: Vec<ManifestEdit> = seqs
                .iter()
                .copied()
                .map(|seq| ManifestEdit::BumpWalSeq { seq })
                .collect();

            // Act
            append_edit_batch(db, &batch).expect("append batch failed");

            let replayed = replay_journal(db).expect("replay_journal failed");
            prop_assert_eq!(replayed.len(), 1);

            // Assert
            match &replayed[0] {
                ManifestEdit::Batch(inner) => {
                    prop_assert_eq!(inner.len(), batch.len());
                    for (observed, expected) in inner.iter().zip(batch.iter()) {
                        match (observed, expected) {
                            (
                                ManifestEdit::BumpWalSeq { seq: observed_seq },
                                ManifestEdit::BumpWalSeq { seq: expected_seq },
                            ) => prop_assert_eq!(observed_seq, expected_seq),
                            other => prop_assert!(false, "unexpected replayed edit pair: {:?}", other),
                        }
                    }
                }
                other => prop_assert!(false, "expected batch replay, got {:?}", other),
            }

            let mut direct_manifest = Manifest::default();
            direct_manifest.apply_edit(&ManifestEdit::Batch(batch.clone()));
            let mut replayed_manifest = Manifest::default();
            replayed_manifest.apply_edit(&replayed[0]);

            prop_assert_eq!(
                replayed_manifest.last_persisted_sequence,
                direct_manifest.last_persisted_sequence
            );
        }
    }
}
