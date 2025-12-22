use crate::common::MidgeResult;
use crate::metadata::manifest::{CloudCheckpoint, FileMeta};
use bincode;
use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

/// TLV-encoded manifest edit record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManifestEdit {
    AddSst(FileMeta),
    RemoveSst { name: String },
    CreateColumnFamily { id: u32, name: String, created_at: u64 },
    DropColumnFamily { id: u32 },
    BumpWalSeq { seq: u64 },
    BumpNextSstSeq { cf_id: u32, next_seq: u64 },
    SetCloudCheckpoint(CloudCheckpoint),
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
        }
    }
}

const JOURNAL_FILE: &str = "manifest.journal";

/// Append an edit to the manifest journal and fsync the file (durable append).
pub fn append_edit(db_path: &Path, edit: &ManifestEdit) -> MidgeResult<()> {
    let path = db_path.join(JOURNAL_FILE);
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&path)
        .map_err(|e| crate::common::MidgeError::Io(e))?;

    let payload = bincode::serialize(edit).map_err(|e| crate::common::MidgeError::Internal(e.to_string()))?;
    let mut hasher = Crc32::new();
    hasher.update(&payload);
    let crc = hasher.finalize();

    // Layout: [type:u8][len:u32LE][payload][crc:u32LE]
    let mut buf = Vec::with_capacity(1 + 4 + payload.len() + 4);
    buf.push(edit.record_type());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&payload);
    buf.extend_from_slice(&crc.to_le_bytes());

    f.write_all(&buf).map_err(|e| crate::common::MidgeError::Io(e))?;
    f.sync_all().map_err(|e| crate::common::MidgeError::Io(e))?;

    Ok(())
}

/// Replay a journal file at db_path. Returns Vec<ManifestEdit> in order.
/// Stops cleanly on partial or corrupt tail record (returns edits up to that point).
pub fn replay_journal(db_path: &Path) -> MidgeResult<Vec<ManifestEdit>> {
    let path = db_path.join(JOURNAL_FILE);
    let mut file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            // No journal file -> empty
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(Vec::new());
            } else {
                return Err(crate::common::MidgeError::Io(e));
            }
        }
    };

    let mut edits = Vec::new();
    loop {
        // Read type
        let mut tbuf = [0u8; 1];
        if let Err(e) = file.read_exact(&mut tbuf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break; // clean EOF
            }
            return Err(crate::common::MidgeError::Io(e));
        }
        let _typ = tbuf[0];

        // Read len
        let mut len_buf = [0u8; 4];
        if let Err(e) = file.read_exact(&mut len_buf) {
            // partial -> stop
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(crate::common::MidgeError::Io(e));
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        // Read payload
        let mut payload = vec![0u8; len];
        if let Err(e) = file.read_exact(&mut payload) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(crate::common::MidgeError::Io(e));
        }

        // Read crc
        let mut crc_buf = [0u8; 4];
        if let Err(e) = file.read_exact(&mut crc_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(crate::common::MidgeError::Io(e));
        }
        let got_crc = u32::from_le_bytes(crc_buf);
        let mut hasher = Crc32::new();
        hasher.update(&payload);
        let calc = hasher.finalize();
        if calc != got_crc {
            // CRC mismatch -> stop replay at tail
            tracing::warn!(path = ?path, "journal crc mismatch, stopping at tail");
            break;
        }

        // Deserialize payload
        match bincode::deserialize::<ManifestEdit>(&payload) {
            Ok(edit) => edits.push(edit),
            Err(e) => {
                // Unknown/corrupt payload -> stop
                tracing::warn!(path = ?path, "journal record deserialize failed: {}", e);
                break;
            }
        }
    }

    Ok(edits)
}

/// Truncate or rotate journal after snapshot. Here we simply truncate to zero length.
pub fn truncate_journal(db_path: &Path) -> MidgeResult<()> {
    let path = db_path.join(JOURNAL_FILE);
    if path.exists() {
        let mut f = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| crate::common::MidgeError::Io(e))?;
        f.set_len(0).map_err(|e| crate::common::MidgeError::Io(e))?;
        f.sync_all().map_err(|e| crate::common::MidgeError::Io(e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Manifest, ManifestPersistence};
    use crate::metadata::FileMeta;
    use tempfile::tempdir;

    #[test]
    fn should_replay_journal_when_valid_records_exist() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        let file = FileMeta {
            name: "sst_001.sst".to_string(),
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
                assert_eq!(m.name, "sst_001.sst");
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
        let file = FileMeta { name: "a.sst".to_string(), ..Default::default() };
        let edit = ManifestEdit::AddSst(file);
        append_edit(db, &edit).expect("append_edit failed");

        // Act: append a truncated record to simulate crash
        let path = db.join(JOURNAL_FILE);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();

        let fake = ManifestEdit::RemoveSst { name: "missing.sst".to_string() };
        let payload = bincode::serialize(&fake).unwrap();
        let mut hasher = Crc32::new();
        hasher.update(&payload);
        let _crc = hasher.finalize();

        let mut buf = Vec::new();
        buf.push(fake.record_type());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload[..10.min(payload.len())]); // partial
        // do NOT write crc

        f.write_all(&buf).unwrap();
        f.sync_all().unwrap();

        // Assert: replay should only return the first valid record
        let edits = replay_journal(db).expect("replay_journal failed");
        assert_eq!(edits.len(), 1);
    }

    #[test]
    fn should_write_snapshot_and_truncate_journal_when_snapshot_saved() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta { name: "one.sst".to_string(), level: 0, size_bytes: 100, ..Default::default() });

        // Act
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest).expect("save snapshot failed");

        // Assert: snapshot exists and journal truncated
        let snap = ManifestPersistence::manifest_snapshot_path(db);
        assert!(snap.exists());

        let journal = db.join(JOURNAL_FILE);
        if journal.exists() {
            let meta = std::fs::metadata(&journal).unwrap();
            assert_eq!(meta.len(), 0);
        }
    }

    #[test]
    fn should_prefer_snapshot_and_replay_journal_when_present() {
        // Arrange
        let td = tempdir().unwrap();
        let db = td.path();

        // Create snapshot
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta { name: "base.sst".to_string(), level: 0, size_bytes: 10, ..Default::default() });
        ManifestPersistence::save_snapshot_and_truncate_journal(db, &manifest).expect("save snapshot failed");

        // Act: append an edit that should be replayed on load
        let edit = ManifestEdit::AddSst(FileMeta { name: "new.sst".to_string(), level: 1, size_bytes: 20, ..Default::default() });
        append_edit(db, &edit).expect("append_edit failed");

        // Assert: loaded manifest includes both snapshot and journal edit
        let loaded = ManifestPersistence::load(db).expect("load failed");
        assert!(loaded.files.iter().any(|f| f.name == "base.sst"));
        assert!(loaded.files.iter().any(|f| f.name == "new.sst"));
    }
}

