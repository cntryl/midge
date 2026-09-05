//! Preserve exact manifest coverage without retaining complete SST bytes.

use crate::io::{Fs, FsPath};
use crate::sst::SstFactory;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct ReplayCoverage {
    manifest: crate::metadata::Manifest,
    fs: Arc<dyn Fs>,
    // Only immutable provider identities are retained. Readers and their
    // metadata are released after each probe under a bounded allocation pool.
    verified: RefCell<HashMap<String, Option<Arc<dyn Fs>>>>,
    memory_bytes: usize,
}

impl ReplayCoverage {
    pub(super) fn new(
        manifest: crate::metadata::Manifest,
        fs: Arc<dyn Fs>,
        memory_bytes: usize,
    ) -> Self {
        Self {
            manifest,
            fs,
            verified: RefCell::new(HashMap::new()),
            memory_bytes,
        }
    }

    pub(super) fn contains(&self, record: &crate::wal::WalRecord) -> bool {
        use crate::sst::types::KeyState;
        use crate::wal::types::WalOpRole;
        // Keep the existing conservative rule: tombstones are always replayed.
        if !matches!(record.op.role(), WalOpRole::ValueWrite) {
            return false;
        }
        let mut highest: Option<KeyState> = None;
        let mut ambiguous = false;
        for file in self
            .manifest
            .files
            .iter()
            .filter(|file| candidate(file, record))
        {
            let Some(observed) = self.file_state(file, record.key.as_ref()) else {
                return false;
            };
            let Some(sequence) = state_sequence(&observed) else {
                continue;
            };
            match highest.as_ref().and_then(state_sequence) {
                Some(current) if sequence < current => {}
                Some(current) if sequence == current => {
                    ambiguous |= highest.as_ref() != Some(&observed);
                }
                _ => {
                    highest = Some(observed);
                    ambiguous = false;
                }
            }
        }
        if ambiguous {
            return false;
        }
        match highest {
            Some(KeyState::Value(value, sequence, expiration, operation)) => {
                sequence > record.seq
                    || sequence == record.seq
                        && record.value.as_ref() == Some(&value)
                        && record.expiration == expiration
                        && crate::wal::WalOpKind::from_wire_format(operation)
                            .is_ok_and(|op| matches!(op.role(), WalOpRole::ValueWrite))
            }
            Some(KeyState::Tombstone(sequence)) => sequence > record.seq,
            Some(KeyState::Absent) | None => false,
        }
    }

    fn file_state(
        &self,
        file: &crate::metadata::FileMeta,
        key: &[u8],
    ) -> Option<crate::sst::types::KeyState> {
        let path = FsPath::new(crate::sst::object_key(&file.name));
        let mut verified = self.verified.borrow_mut();
        let fs = verified.entry(file.name.clone()).or_insert_with(|| {
            let crc = file.content_crc32c?;
            let pinned = self.fs.immutable_read_view(&path).ok()??;
            super::super::streaming_wal_fs::validate_wal_source(
                pinned.as_ref(),
                &path,
                file.size_bytes,
                crc,
                self.memory_bytes,
            )
            .ok()?;
            Some(pinned)
        });
        let fs = fs.as_ref()?;
        let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(fs), self.memory_bytes);
        let budget = crate::common::resource_budget::ResourceBudget::new(self.memory_bytes);
        let reader = factory
            .open_for_compaction(std::path::Path::new(&path.0), budget)
            .ok()?;
        // Recovery compares persisted expiration metadata. A forward wall-clock
        // jump must not turn an unrelated expired value into tombstone proof.
        // Expiration zero remains conservatively replayed at equal sequence.
        reader.get_state_at_with_time(key, u64::MAX, 0).ok()
    }
}

fn state_sequence(state: &crate::sst::types::KeyState) -> Option<u64> {
    match state {
        crate::sst::types::KeyState::Value(_, sequence, _, _)
        | crate::sst::types::KeyState::Tombstone(sequence) => Some(*sequence),
        crate::sst::types::KeyState::Absent => None,
    }
}

fn candidate(file: &crate::metadata::FileMeta, record: &crate::wal::WalRecord) -> bool {
    file.cf_id == record.cf_id
        && file
            .smallest_seq
            .is_some_and(|sequence| sequence <= record.seq)
        && file
            .largest_seq
            .is_some_and(|sequence| sequence >= record.seq)
        && file
            .smallest_key
            .as_ref()
            .is_some_and(|key| key.as_slice() <= record.key.as_ref())
        && file
            .largest_key
            .as_ref()
            .is_some_and(|key| key.as_slice() >= record.key.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{WalOpKind, WalRecord};
    use bytes::Bytes;

    type PersistedEntry<'a> = (Option<&'a [u8]>, u64, Option<u64>);

    fn fixture(entries: &[PersistedEntry<'_>]) -> (tempfile::TempDir, ReplayCoverage) {
        let dir = tempfile::tempdir().expect("coverage directory");
        let fs = Arc::new(crate::io::RealFs::new(dir.path()).expect("local filesystem"));
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 4096);
        let mut manifest = crate::metadata::Manifest::default();
        std::fs::create_dir_all(dir.path().join("cloud/sst")).expect("remote SST directory");
        for (index, (value, sequence, expiration)) in entries.iter().enumerate() {
            let mut writer = factory.create().expect("SST writer");
            let operation = if value.is_some() {
                WalOpKind::Put
            } else {
                WalOpKind::Delete
            };
            writer
                .add_with_meta(
                    b"key",
                    *value,
                    *sequence,
                    operation.to_wire_format(),
                    *expiration,
                )
                .expect("SST entry");
            let bytes = writer.finish_bytes().expect("SST bytes");
            let name = crate::sst::file_name(0, 0, index as u64 + 1);
            std::fs::write(dir.path().join("cloud/sst").join(&name), &bytes).expect("remote SST");
            manifest.files.push(crate::metadata::FileMeta {
                name,
                cf_id: 0,
                level: 0,
                size_bytes: bytes.len() as u64,
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                smallest_key: Some(b"key".to_vec()),
                largest_key: Some(b"key".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(*sequence),
                ..Default::default()
            });
        }
        let cloud = Arc::new(
            crate::storage::filesystem::FileSystem::new(dir.path().join("cloud"))
                .expect("cloud filesystem"),
        );
        let remote = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
            fs,
            cloud,
            std::time::Duration::from_secs(5),
        ));
        (dir, ReplayCoverage::new(manifest, remote, 128 * 1024))
    }

    fn put(sequence: u64, expiration: Option<u64>) -> WalRecord {
        let mut record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            sequence,
            1,
        );
        record.expiration = expiration;
        record
    }

    #[test]
    fn should_replay_put_when_equal_sequence_sst_expiration_differs() {
        // Arrange
        let (_dir, coverage) = fixture(&[(Some(b"value"), 7, Some(u64::MAX))]);
        let record = put(7, None);
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            !covered,
            "equal value bytes cannot replace persisted TTL metadata"
        );
    }

    #[test]
    fn should_replay_put_when_equal_sequence_sst_contains_tombstone() {
        // Arrange
        let (_dir, coverage) = fixture(&[(None, 7, None)]);
        let record = put(7, None);
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            !covered,
            "a contradictory same-sequence tombstone is not proof of the WAL put"
        );
    }

    #[test]
    fn should_replay_put_when_verified_ssts_disagree_at_same_latest_sequence() {
        // Arrange
        let (_dir, coverage) =
            fixture(&[(Some(b"value"), 7, None), (Some(b"conflicting"), 7, None)]);
        let record = put(7, None);
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            !covered,
            "one matching file must not hide contradictory authority"
        );
    }

    #[test]
    fn should_prove_matching_expired_value_without_using_recovery_wall_clock() {
        // Arrange
        let (_dir, coverage) = fixture(&[(Some(b"value"), 7, Some(1))]);
        let record = put(7, Some(1));
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            covered,
            "raw value plus expiration remain authoritative across clock changes"
        );
    }

    #[test]
    fn should_replay_deletes_even_when_same_sequence_state_is_persisted() {
        // Arrange
        let (_dir, coverage) = fixture(&[(None, 7, None)]);
        let mut record = put(7, None);
        record.op = WalOpKind::Delete;
        record.value = None;
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(!covered, "delete replay stays conservative");
    }
    #[test]
    fn should_retain_wal_when_exact_sst_proof_cannot_fit_recovery_memory_budget() {
        // Arrange
        let (_dir, mut coverage) = fixture(&[(Some(b"value"), 7, None)]);
        coverage.memory_bytes = 32;
        let record = put(7, None);
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(!covered, "proof exhaustion must preserve replayable WAL");
    }

    #[test]
    fn should_retain_wal_when_pinned_sst_identity_changes_after_first_proof() {
        // Arrange
        let (dir, coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let record = put(7, None);
        assert!(coverage.contains(&record));
        let path = dir
            .path()
            .join("cloud/sst")
            .join(&coverage.manifest.files[0].name);
        let mut bytes = std::fs::read(&path).expect("original SST");
        bytes[0] ^= 1;
        std::fs::write(path, bytes).expect("replace immutable object");
        // Act
        let covered = coverage.contains(&record);
        // Assert
        assert!(
            !covered,
            "cached identity cannot prove replacement contents"
        );
    }
}
