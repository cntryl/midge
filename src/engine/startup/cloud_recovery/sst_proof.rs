use super::{CloudStartupRecovery, MidgeError, MidgeResult};
use crate::sst::identity::{ExpectedSst, ProofPolicy, SstIdentity};
use std::path::Path;

/// Describe a manifest proof for the shared identity checker.
///
/// A proof the manifest never recorded arrives here as `None`, which
/// [`ProofPolicy::Legacy`] reads as "unknown, skip" — including a recorded
/// size of 0, which no real SST has.
fn expected_sst(
    sst_name: &str,
    expected_size_bytes: Option<u64>,
    expected_crc32c: Option<u32>,
) -> ExpectedSst<'_> {
    ExpectedSst {
        name: sst_name,
        size_bytes: expected_size_bytes.unwrap_or(0),
        content_crc32c: expected_crc32c,
        smallest_key: None,
        largest_key: None,
        smallest_seq: None,
        largest_seq: None,
    }
}

impl CloudStartupRecovery {
    pub(super) fn validate_sst_bytes_against_proof(
        sst_name: &str,
        data: &[u8],
        expected_size_bytes: Option<u64>,
        expected_crc32c: Option<u32>,
    ) -> MidgeResult<()> {
        SstIdentity::of_bytes(data)
            .verify_against(
                expected_sst(sst_name, expected_size_bytes, expected_crc32c),
                None,
                ProofPolicy::Legacy,
            )
            .map_err(|mismatch| MidgeError::RecoveryFailed(mismatch.to_string()))
    }

    pub(super) fn local_sst_file_matches_proof(
        path: &Path,
        sst_name: &str,
        expected_size_bytes: Option<u64>,
        expected_crc32c: Option<u32>,
    ) -> bool {
        if !path.exists() {
            return false;
        }

        let Ok(identity) = SstIdentity::of_path(path) else {
            return false;
        };
        if identity
            .verify_against(
                expected_sst(sst_name, expected_size_bytes, expected_crc32c),
                None,
                ProofPolicy::Legacy,
            )
            .is_err()
        {
            return false;
        }

        let Ok(reader) = crate::sst::fs::SstFileIo::open_with_real_fs(path) else {
            return false;
        };
        // A matching whole-file CRC already proves the bytes; without one, the
        // only remaining evidence is a full block-level pass.
        expected_crc32c.is_some() || reader.verify_all_blocks().is_ok()
    }

    pub(super) fn local_sst_file_matches_manifest(
        path: &Path,
        file: &crate::metadata::FileMeta,
    ) -> bool {
        // A manifest size of 0 means "not recorded" on every other recovery
        // path, so this one no longer reads it as "the file must be empty".
        Self::local_sst_file_matches_proof(
            path,
            &file.name,
            (file.size_bytes != 0).then_some(file.size_bytes),
            file.content_crc32c,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::CloudStartupRecovery;
    use crate::metadata::FileMeta;
    use crate::types::EntryType;

    fn write_sst(directory: &tempfile::TempDir, name: &str) -> (std::path::PathBuf, Vec<u8>) {
        let fs = std::sync::Arc::new(crate::io::RealFs::new(directory.path()).expect("open fs"));
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = crate::sst::traits::SstFactory::create(&factory).expect("create writer");
        writer
            .add_with_meta(b"key", Some(b"value"), 7, EntryType::Put, None)
            .expect("add entry");
        let path = directory.path().join(name);
        crate::sst::fs::finish_writer_to_path(writer, &path).expect("finish sst");
        let bytes = std::fs::read(&path).expect("read sst");
        (path, bytes)
    }

    /// The byte-level proof and the local-file proof used to disagree about a
    /// manifest entry whose `size_bytes` is 0: the file path read it as "this
    /// file must be empty" while every other recovery path read it as "not
    /// recorded". Both now ask the one checker under the same policy.
    #[test]
    fn should_reach_the_same_verdict_for_an_unrecorded_size_on_both_recovery_proofs() {
        // Arrange
        let directory = tempfile::tempdir().expect("create directory");
        let (path, bytes) = write_sst(&directory, "000001.sst");
        let entry = FileMeta {
            name: "000001.sst".to_string(),
            size_bytes: 0,
            content_crc32c: Some(crc32c::crc32c(&bytes)),
            ..FileMeta::default()
        };

        // Act
        let from_bytes = CloudStartupRecovery::validate_sst_bytes_against_proof(
            &entry.name,
            &bytes,
            (entry.size_bytes != 0).then_some(entry.size_bytes),
            entry.content_crc32c,
        );
        let from_file = CloudStartupRecovery::local_sst_file_matches_manifest(&path, &entry);

        // Assert
        assert!(from_bytes.is_ok(), "{from_bytes:?}");
        assert!(from_file, "the local file proof must accept it too");
    }

    #[test]
    fn should_reject_a_corrupt_local_file_on_both_recovery_proofs() {
        // Arrange
        let directory = tempfile::tempdir().expect("create directory");
        let (path, bytes) = write_sst(&directory, "000002.sst");
        let entry = FileMeta {
            name: "000002.sst".to_string(),
            size_bytes: bytes.len() as u64,
            content_crc32c: Some(crc32c::crc32c(&bytes).wrapping_add(1)),
            ..FileMeta::default()
        };

        // Act
        let from_bytes = CloudStartupRecovery::validate_sst_bytes_against_proof(
            &entry.name,
            &bytes,
            Some(entry.size_bytes),
            entry.content_crc32c,
        );
        let from_file = CloudStartupRecovery::local_sst_file_matches_manifest(&path, &entry);

        // Assert
        assert!(from_bytes.is_err(), "a CRC mismatch must be rejected");
        assert!(!from_file, "the local file proof must reject it too");
    }
}
