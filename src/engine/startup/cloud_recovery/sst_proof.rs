use super::{CloudStartupRecovery, MidgeError, MidgeResult};
use std::path::Path;

impl CloudStartupRecovery {
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
}
