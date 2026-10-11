use super::{CloudStartupRecovery, MidgeError, MidgeResult};
use crate::io::{Fs, FsError, FsPath};
use crate::sst::identity::{ProofPolicy, SstIdentity};
use crate::types::ExpectedSst;
use std::sync::Arc;

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
    pub(crate) fn validate_sst_bytes_against_proof(
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

    /// Whether the SST at `path` in `fs` carries the recorded proof. Any
    /// failure to read it is a "no", except an expired startup deadline.
    pub(crate) fn local_sst_file_matches_proof_within(
        fs: &Arc<dyn Fs>,
        path: &FsPath,
        sst_name: &str,
        expected_size_bytes: Option<u64>,
        expected_crc32c: Option<u32>,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> MidgeResult<bool> {
        let check = |context: &str| scope.map_or(Ok(()), |scope| scope.check(context));
        check("local recovery SST proof")?;
        let fs = scope.map_or_else(
            || Arc::clone(fs),
            |scope| crate::io::scope_fs(Arc::clone(fs), scope.clone()),
        );
        let prove = || -> MidgeResult<bool> {
            let file = fs
                .open(
                    path,
                    crate::io::OpenOptions {
                        mode: crate::io::OpenMode::ReadOnly,
                        create: false,
                        create_new: false,
                        truncate: false,
                    },
                )
                .map_err(FsError::into_midge)?;
            let size = file.len().map_err(FsError::into_midge)?;
            let identity = SstIdentity::of_file(file.as_ref(), size, None)?;
            if identity
                .verify_against(
                    expected_sst(sst_name, expected_size_bytes, expected_crc32c),
                    None,
                    ProofPolicy::Legacy,
                )
                .is_err()
            {
                return Ok(false);
            }
            check("local recovery SST checksum")?;
            let reader = crate::sst::fs::SstFileIo::open(&path.0, Arc::clone(&fs))?;
            // A matching whole-file CRC already proves the bytes; without one,
            // the only remaining evidence is a full block-level pass.
            if expected_crc32c.is_none() {
                reader.verify_all_blocks()?;
            }
            check("local recovery SST blocks")?;
            Ok(true)
        };
        match prove() {
            Err(error @ MidgeError::Timeout(_)) => Err(error),
            Err(_) => Ok(false),
            result => result,
        }
    }

    pub(crate) fn local_sst_file_matches_manifest_within(
        fs: &Arc<dyn Fs>,
        path: &FsPath,
        file: &crate::metadata::FileMeta,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> MidgeResult<bool> {
        // A manifest size of 0 means "not recorded" on every recovery path.
        Self::local_sst_file_matches_proof_within(
            fs,
            path,
            &file.name,
            (file.size_bytes != 0).then_some(file.size_bytes),
            file.content_crc32c,
            scope,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::CloudStartupRecovery;
    use crate::io::{Fs, FsPath, MockFs};
    use crate::metadata::FileMeta;
    use crate::types::EntryType;
    use std::sync::Arc;

    fn write_sst(fs: &Arc<dyn Fs>, name: &str) -> Vec<u8> {
        let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(fs), 4096);
        let mut writer = crate::sst::traits::SstFactory::create(&factory).expect("create writer");
        writer
            .add_with_meta(b"key", Some(b"value"), 7, EntryType::Put, None)
            .expect("add entry");
        crate::sst::fs::finish_writer_to_path(writer, std::path::Path::new(name))
            .expect("finish sst");
        crate::io::read_all(fs.as_ref(), &FsPath::new(name))
            .expect("read sst")
            .to_vec()
    }

    /// The byte-level proof and the local-file proof used to disagree about a
    /// manifest entry whose `size_bytes` is 0: the file path read it as "this
    /// file must be empty" while every other recovery path read it as "not
    /// recorded". Both now ask the one checker under the same policy.
    #[test]
    fn should_reach_the_same_verdict_for_an_unrecorded_size_on_both_recovery_proofs() {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
        let bytes = write_sst(&fs, "000001.sst");
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
        let from_file = CloudStartupRecovery::local_sst_file_matches_manifest_within(
            &fs,
            &FsPath::new(&entry.name),
            &entry,
            None,
        )
        .expect("local proof");

        // Assert
        assert!(from_bytes.is_ok(), "{from_bytes:?}");
        assert!(from_file, "the local file proof must accept it too");
    }

    #[test]
    fn should_reject_a_corrupt_local_file_on_both_recovery_proofs() {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
        let bytes = write_sst(&fs, "000002.sst");
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
        let from_file = CloudStartupRecovery::local_sst_file_matches_manifest_within(
            &fs,
            &FsPath::new(&entry.name),
            &entry,
            None,
        )
        .expect("local proof");

        // Assert
        assert!(from_bytes.is_err(), "a CRC mismatch must be rejected");
        assert!(!from_file, "the local file proof must reject it too");
    }
}
