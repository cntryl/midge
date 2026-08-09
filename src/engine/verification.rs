use crate::common::{MidgeError, MidgeResult};
use crate::config::{EngineHealth, RecoveryPolicy};
use crate::io::{Fs, FsPath, OpenMode, OpenOptions};
use crate::types::StorageVerificationReport;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Storage-integrity façade for online and offline verification.
pub struct StorageVerifier {
    runtime_handle: crate::runtime::RuntimeHandle,
    db_path: std::path::PathBuf,
    memory_mode: bool,
    cloud_mode: bool,
}

impl StorageVerifier {
    pub(super) fn new(
        runtime_handle: crate::runtime::RuntimeHandle,
        db_path: std::path::PathBuf,
        memory_mode: bool,
        cloud_mode: bool,
    ) -> Self {
        Self {
            runtime_handle,
            db_path,
            memory_mode,
            cloud_mode,
        }
    }

    /// Run a non-mutating online integrity pass.
    ///
    /// # Errors
    ///
    /// Returns an error when verification is unsupported, fails, or exceeds the deadline.
    pub fn verify_storage(&self, timeout: Duration) -> MidgeResult<StorageVerificationReport> {
        self.runtime_handle.ensure_open()?;
        Self::validate_online_request(self.memory_mode, timeout)?;

        let started = Instant::now();
        let health_request_id = crate::runtime::next_request_id()?;
        let runtime_health = match self.runtime_handle.send_and_wait_timeout(
            crate::runtime::RuntimeMsg::GetRuntimeMetrics {
                request_id: health_request_id,
            },
            timeout,
        )? {
            Some(crate::runtime::RuntimeResponse::RuntimeMetricsSnapshot { snapshot, .. }) => {
                snapshot.health
            }
            Some(crate::runtime::RuntimeResponse::Error { error, .. }) => return Err(error),
            Some(other) => {
                return Err(MidgeError::Internal(format!(
                    "unexpected runtime-health response before verification: {other:?}"
                )))
            }
            None => {
                return Err(MidgeError::Timeout(
                    "runtime health capture exceeded the verification deadline".to_string(),
                ))
            }
        };
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(MidgeError::Timeout(
                "online storage verification deadline elapsed during health capture".to_string(),
            ));
        }
        verify_storage_online(
            &self.runtime_handle,
            self.db_path.clone(),
            runtime_health,
            self.cloud_mode,
            remaining,
        )
    }

    /// Verify a storage directory without opening an engine runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage contents fail strict verification.
    pub fn verify_path(
        path: impl Into<std::path::PathBuf>,
    ) -> MidgeResult<StorageVerificationReport> {
        verify_storage_path(&path.into(), None)
    }

    fn validate_online_request(memory_mode: bool, timeout: Duration) -> MidgeResult<()> {
        if memory_mode {
            return Err(MidgeError::NotSupported(
                "storage verification is not supported in memory mode".to_string(),
            ));
        }
        if timeout.is_zero() {
            return Err(MidgeError::Timeout(
                "online storage verification deadline is zero".to_string(),
            ));
        }
        Ok(())
    }
}

fn checksummed_file_crc(file: &dyn crate::io::File, file_len: u64) -> MidgeResult<u32> {
    const VERIFY_CHUNK_BYTES: u64 = 1024 * 1024;

    let mut crc = 0u32;
    let mut offset = 0u64;
    while offset < file_len {
        let chunk_len = VERIFY_CHUNK_BYTES.min(file_len - offset);
        let chunk = file.read_at(offset, chunk_len)?;
        if u64::try_from(chunk.len()).unwrap_or(u64::MAX) != chunk_len {
            return Err(MidgeError::Corruption(format!(
                "verification read returned {} bytes for requested {chunk_len}",
                chunk.len()
            )));
        }
        crc = crc32c::crc32c_append(crc, &chunk);
        offset += chunk_len;
    }
    Ok(crc)
}

fn verify_manifest_sst(
    fs: &Arc<dyn Fs>,
    file_meta: &crate::metadata::FileMeta,
) -> MidgeResult<(u64, u64)> {
    let name = crate::sst::PersistedSstName::parse(&file_meta.name)?;
    let relative = name.join_under(Path::new("sst"));
    let path = relative.to_str().ok_or_else(|| {
        MidgeError::Corruption("persisted SST path is not valid UTF-8".to_string())
    })?;
    let fs_path = FsPath::new(path);
    let actual_len = fs.metadata(&fs_path)?.len;
    if actual_len != file_meta.size_bytes {
        return Err(MidgeError::Corruption(format!(
            "SST '{}' size mismatch: manifest={}, actual={actual_len}",
            name.as_str(),
            file_meta.size_bytes
        )));
    }

    let file = fs.open(
        &fs_path,
        OpenOptions {
            mode: OpenMode::ReadOnly,
            create: false,
            create_new: false,
            truncate: false,
        },
    )?;
    let expected_crc = file_meta.content_crc32c.ok_or_else(|| {
        MidgeError::Corruption(format!(
            "SST '{}' is missing manifest content CRC",
            name.as_str()
        ))
    })?;
    let actual_crc = checksummed_file_crc(file.as_ref(), actual_len)?;
    if actual_crc != expected_crc {
        return Err(MidgeError::Corruption(format!(
            "SST '{}' content CRC mismatch: manifest={expected_crc:08x}, actual={actual_crc:08x}",
            name.as_str()
        )));
    }

    let reader = crate::sst::fs::SstFileIo::open(path, Arc::clone(fs)).map_err(|error| {
        MidgeError::Corruption(format!(
            "failed to open verified SST '{}': {error}",
            name.as_str()
        ))
    })?;
    let stats = reader.verify_all_blocks().map_err(|error| {
        MidgeError::Corruption(format!(
            "failed to verify every block in SST '{}': {error}",
            name.as_str()
        ))
    })?;
    Ok((stats.size_bytes, stats.data_blocks))
}

fn verification_stage_error(context: &str, error: MidgeError) -> MidgeError {
    match error {
        error @ (MidgeError::Io(_)
        | MidgeError::NotFound
        | MidgeError::InvalidPath
        | MidgeError::Corruption(_)
        | MidgeError::CompatibilityError(_)) => error,
        other => MidgeError::RecoveryFailed(format!("{context}: {other}")),
    }
}

pub(super) fn verify_storage_path(
    db_path: &Path,
    runtime_health: Option<EngineHealth>,
) -> MidgeResult<StorageVerificationReport> {
    crate::metadata::validate_format_marker(db_path)?;

    let fs: Arc<dyn Fs> =
        Arc::new(crate::io::RealFs::open_existing(db_path).map_err(MidgeError::from)?);
    let manifest =
        crate::metadata::ManifestPersistence::load_with_fs_and_policy(&fs, RecoveryPolicy::Strict)
            .map_err(MidgeError::RecoveryFailed)?;
    let intents =
        crate::runtime::IntentPersistence::load_with_fs_and_policy(&fs, RecoveryPolicy::Strict)
            .map_err(MidgeError::RecoveryFailed)?;

    let mut sst_bytes_verified = 0u64;
    let mut data_blocks_verified = 0u64;
    for file_meta in &manifest.files {
        let (sst_bytes, data_blocks) = verify_manifest_sst(&fs, file_meta)?;
        sst_bytes_verified = sst_bytes_verified.saturating_add(sst_bytes);
        data_blocks_verified = data_blocks_verified.saturating_add(data_blocks);
    }

    let wal_stats = crate::wal::recovery::replay_wal_with_policy(
        fs.as_ref(),
        &FsPath::new("wal"),
        &mut std::collections::HashMap::new(),
        crate::wal::recovery::ReplayPolicy::Strict,
    )
    .map_err(|error| verification_stage_error("WAL verification failed", error))?;
    let manifest_epoch = crate::metadata::journal::highest_edit_id_with_fs(&fs)
        .map_err(|error| verification_stage_error("manifest epoch failed", error))?
        .max(manifest.edit_checkpoint_id);

    let residue =
        crate::runtime::storage_residue::StorageResidueAssessment::scan_sst_dir(db_path, &manifest);
    let health = match runtime_health {
        Some(EngineHealth::Healthy) | None => {
            crate::runtime::storage_residue::classify_engine_health(
                crate::runtime::storage_residue::HealthInputs {
                    opened_in_salvage_mode: false,
                    write_stalled: false,
                    persistence_anomaly_detected: false,
                    pending_intents: intents.len(),
                    orphan_ssts: residue.orphan_ssts.len(),
                },
            )
        }
        Some(other) => other,
    };

    Ok(StorageVerificationReport {
        manifest_epoch,
        manifest_files_verified: manifest.files.len(),
        sst_files_verified: manifest.files.len(),
        bytes_verified: sst_bytes_verified.saturating_add(wal_stats.bytes),
        data_blocks_verified,
        wal_boundary: wal_stats.max_sequence,
        wal_recovery_records_replayed: wal_stats.record_count,
        wal_recovery_bytes_replayed: wal_stats.bytes,
        intent_entries_loaded: intents.len(),
        authoritative: true,
        health,
    })
}

struct VerificationBarrierGuard {
    runtime_handle: crate::runtime::RuntimeHandle,
    token: u64,
    released: bool,
}

impl VerificationBarrierGuard {
    fn pending(runtime_handle: &crate::runtime::RuntimeHandle, token: u64) -> Self {
        Self {
            runtime_handle: runtime_handle.clone(),
            token,
            released: false,
        }
    }

    fn disarm(&mut self) {
        self.released = true;
    }

    fn acquire(
        runtime_handle: &crate::runtime::RuntimeHandle,
        timeout: Duration,
    ) -> MidgeResult<Self> {
        let started = Instant::now();
        loop {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(MidgeError::Timeout(
                    "storage verification barrier was not acquired before deadline".to_string(),
                ));
            }

            let request_id = crate::runtime::next_request_id()?;
            let mut pending_barrier = Self::pending(runtime_handle, request_id);
            let response = match runtime_handle.send_and_wait_timeout(
                crate::runtime::RuntimeMsg::BeginStorageVerification { request_id },
                remaining,
            ) {
                Ok(response) => response,
                Err(error) => {
                    pending_barrier.disarm();
                    return Err(error);
                }
            };
            match response {
                Some(crate::runtime::RuntimeResponse::StorageVerificationBarrier {
                    token, ..
                }) => {
                    debug_assert_eq!(token, request_id);
                    pending_barrier.token = token;
                    return Ok(pending_barrier);
                }
                Some(crate::runtime::RuntimeResponse::Error {
                    error: MidgeError::Busy(_),
                    ..
                }) => {
                    pending_barrier.disarm();
                    std::thread::sleep(remaining.min(Duration::from_millis(1)));
                }
                Some(crate::runtime::RuntimeResponse::Error { error, .. }) => {
                    pending_barrier.disarm();
                    return Err(error);
                }
                Some(other) => {
                    return Err(MidgeError::Internal(format!(
                        "unexpected verification barrier response: {other:?}"
                    )));
                }
                None => {
                    return Err(MidgeError::Timeout(
                        "storage verification barrier request timed out".to_string(),
                    ));
                }
            }
        }
    }

    fn release(&mut self) -> MidgeResult<()> {
        if self.released {
            return Ok(());
        }
        self.runtime_handle
            .release_storage_verification_barrier(self.token)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for VerificationBarrierGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let runtime_handle = self.runtime_handle.clone();
        let token = self.token;
        let spawn_result = std::thread::Builder::new()
            .name("midge-verification-barrier-reaper".to_string())
            .spawn(move || {
                if let Err(error) = runtime_handle.release_storage_verification_barrier(token) {
                    tracing::warn!(%error, token, "verification barrier reaper could not release token");
                }
            });
        if let Err(error) = spawn_result {
            tracing::error!(%error, token, "failed to spawn verification barrier reaper");
        }
    }
}

pub(super) fn verify_storage_online(
    runtime_handle: &crate::runtime::RuntimeHandle,
    db_path: std::path::PathBuf,
    runtime_health: EngineHealth,
    cloud_mode: bool,
    timeout: Duration,
) -> MidgeResult<StorageVerificationReport> {
    let started = Instant::now();
    let mut barrier = VerificationBarrierGuard::acquire(runtime_handle, timeout)?;
    let remaining = timeout.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        // Dropping schedules Closing-safe cleanup without extending the caller's deadline.
        return Err(MidgeError::Timeout(
            "storage verification deadline elapsed after barrier acquisition".to_string(),
        ));
    }

    let (result_tx, result_rx) = crossbeam::channel::bounded(1);
    let worker = std::thread::Builder::new()
        .name("midge-storage-verification".to_string())
        .spawn(move || {
            crate::failpoints::fail_point!("midge::verification::after_barrier_acquired");
            let mut result = verify_storage_path(&db_path, Some(runtime_health));
            if let Ok(report) = &mut result {
                if cloud_mode {
                    report.authoritative = false;
                }
            }
            let release_result = barrier.release();
            if result.is_ok() {
                if let Err(error) = release_result {
                    result = Err(error);
                }
            }
            let _ = result_tx.send(result);
        })
        .map_err(MidgeError::Io)?;
    drop(worker);

    match result_rx.recv_timeout(remaining) {
        Ok(result) => result,
        Err(crossbeam::channel::RecvTimeoutError::Timeout) => Err(MidgeError::Timeout(
            "online storage verification exceeded its deadline".to_string(),
        )),
        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => Err(MidgeError::Internal(
            "storage verification worker terminated without a result".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::StorageVerifier;
    use crate::io::{Durability, File, FsError, FsResult};
    use crate::metadata::{FileMeta, Manifest, ManifestPersistence};
    use crate::sst::{FsSstFactoryIo, SstFactory};
    use bytes::Bytes;
    use std::sync::{Arc, Mutex};

    #[test]
    fn should_reject_memory_mode_without_constructing_engine() {
        // Arrange
        let timeout = std::time::Duration::from_secs(1);

        // Act
        let result = StorageVerifier::validate_online_request(true, timeout);

        // Assert
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::NotSupported(_))
        ));
    }

    struct RecordingFile {
        bytes: Vec<u8>,
        read_lengths: Mutex<Vec<u64>>,
    }

    impl RecordingFile {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                bytes,
                read_lengths: Mutex::new(Vec::new()),
            }
        }
    }

    impl File for RecordingFile {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
            self.read_lengths
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(len);
            let start = usize::try_from(offset)
                .map_err(|_| FsError::Io("test offset overflow".to_string()))?;
            let length = usize::try_from(len)
                .map_err(|_| FsError::Io("test length overflow".to_string()))?;
            let end = start
                .checked_add(length)
                .filter(|end| *end <= self.bytes.len())
                .ok_or_else(|| FsError::Io("test read out of bounds".to_string()))?;
            Ok(Bytes::copy_from_slice(&self.bytes[start..end]))
        }

        fn write_at(&mut self, _offset: u64, _data: Bytes) -> FsResult<()> {
            unreachable!("verification test file is read-only")
        }

        fn append(&mut self, _data: Bytes) -> FsResult<u64> {
            unreachable!("verification test file is read-only")
        }

        fn len(&self) -> FsResult<u64> {
            Ok(u64::try_from(self.bytes.len()).unwrap_or(u64::MAX))
        }

        fn sync(&mut self, _durability: Durability) -> FsResult<()> {
            unreachable!("verification test file is read-only")
        }

        fn close(self: Box<Self>) -> FsResult<()> {
            Ok(())
        }
    }

    struct VerificationFixture {
        _temp_dir: tempfile::TempDir,
        db_path: std::path::PathBuf,
        sst_path: std::path::PathBuf,
    }

    fn create_verification_fixture() -> VerificationFixture {
        let temp_dir = tempfile::tempdir().expect("create verification directory");
        let db_path = temp_dir.path().to_path_buf();
        crate::metadata::ensure_or_create_format_marker(&db_path).expect("create format marker");
        std::fs::create_dir_all(db_path.join("sst")).expect("create SST directory");

        let factory = FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 96);
        let mut writer = factory.create().expect("create SST writer");
        for index in 0..32_u64 {
            let key = format!("key-{index:04}");
            let value = vec![u8::try_from(index).expect("test byte"); 48];
            writer
                .add_with_meta(key.as_bytes(), Some(&value), index + 1, 0, None)
                .expect("append SST entry");
        }
        let bytes = writer.finish_bytes().expect("finish SST bytes");
        let sst_name = crate::sst::file_name(0, 0, 1);
        let sst_path = db_path.join("sst").join(&sst_name);
        std::fs::write(&sst_path, &bytes).expect("write SST fixture");

        let manifest = Manifest {
            edit_checkpoint_id: 7,
            files: vec![FileMeta {
                name: sst_name,
                level: 0,
                size_bytes: u64::try_from(bytes.len()).expect("fixture size fits in u64"),
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                cf_id: 0,
                sst_seq: 1,
                ..FileMeta::default()
            }],
            ..Manifest::default()
        };
        ManifestPersistence::save(&db_path, &manifest).expect("save manifest fixture");

        VerificationFixture {
            _temp_dir: temp_dir,
            db_path,
            sst_path,
        }
    }

    fn rewrite_manifest_crc(db_path: &std::path::Path, crc: u32) {
        let mut manifest = ManifestPersistence::load(db_path).expect("load manifest fixture");
        manifest.files[0].content_crc32c = Some(crc);
        ManifestPersistence::save(db_path, &manifest).expect("rewrite manifest fixture");
    }

    #[test]
    fn should_bound_read_memory_when_manifest_crc_is_computed() {
        // Arrange
        let bytes = vec![0xa5; 2 * 1024 * 1024 + 123];
        let file = RecordingFile::new(bytes.clone());

        // Act
        let crc = super::checksummed_file_crc(
            &file,
            u64::try_from(bytes.len()).expect("test length fits u64"),
        )
        .expect("checksum recording file");

        // Assert
        assert_eq!(crc, crc32c::crc32c(&bytes));
        let read_lengths = file
            .read_lengths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(read_lengths.as_slice(), &[1024 * 1024, 1024 * 1024, 123]);
    }

    #[test]
    fn should_report_authoritative_block_counts_when_every_sst_is_verified() {
        // Arrange
        let fixture = create_verification_fixture();

        // Act
        let report =
            StorageVerifier::verify_path(&fixture.db_path).expect("verify complete fixture");

        // Assert
        assert_eq!(report.manifest_epoch, 7);
        assert_eq!(report.manifest_files_verified, 1);
        assert_eq!(report.sst_files_verified, 1);
        assert!(report.bytes_verified > 0);
        assert!(report.data_blocks_verified > 0);
        assert_eq!(report.wal_boundary, None);
        assert!(report.authoritative);
    }

    #[test]
    fn should_reject_corrupt_data_block_when_manifest_crc_matches_corrupt_bytes() {
        // Arrange
        let fixture = create_verification_fixture();
        let mut bytes = std::fs::read(&fixture.sst_path).expect("read SST fixture");
        bytes[8] ^= 0x5a;
        std::fs::write(&fixture.sst_path, &bytes).expect("corrupt SST data block");
        rewrite_manifest_crc(&fixture.db_path, crc32c::crc32c(&bytes));

        // Act
        let error = StorageVerifier::verify_path(&fixture.db_path)
            .expect_err("block checksum corruption must fail verification");

        // Assert
        assert!(
            error.to_string().to_ascii_lowercase().contains("checksum")
                || error.to_string().to_ascii_lowercase().contains("crc"),
            "expected block checksum failure, got: {error}"
        );
    }

    #[test]
    fn should_reject_sst_length_when_manifest_size_differs() {
        // Arrange
        let fixture = create_verification_fixture();
        let mut manifest = ManifestPersistence::load(&fixture.db_path).expect("load manifest");
        manifest.files[0].size_bytes = manifest.files[0].size_bytes.saturating_add(1);
        ManifestPersistence::save(&fixture.db_path, &manifest).expect("save wrong size");

        // Act
        let error = StorageVerifier::verify_path(&fixture.db_path)
            .expect_err("manifest size mismatch must fail verification");

        // Assert
        assert!(error.to_string().contains("size"));
    }

    #[test]
    fn should_reject_sst_content_when_manifest_crc_differs() {
        // Arrange
        let fixture = create_verification_fixture();
        let manifest = ManifestPersistence::load(&fixture.db_path).expect("load manifest");
        let wrong_crc = manifest.files[0]
            .content_crc32c
            .expect("fixture has content CRC")
            .wrapping_add(1);
        rewrite_manifest_crc(&fixture.db_path, wrong_crc);

        // Act
        let error = StorageVerifier::verify_path(&fixture.db_path)
            .expect_err("manifest CRC mismatch must fail verification");

        // Assert
        assert!(error.to_string().to_ascii_lowercase().contains("crc"));
    }
}
