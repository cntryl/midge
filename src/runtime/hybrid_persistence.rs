//! Format-aware orchestration for hybrid persistence.
//!
//! `HybridStorage` deliberately knows only object keys, bytes, provider
//! identities, bounded callbacks, and conditional deletion. This runtime layer
//! owns WAL/SST decoding and manifest coverage decisions.

use crate::common::{MidgeError, MidgeResult};
use crate::metadata::{FileMeta, Manifest};
use crate::storage::cloud::CloudStorage;
use crate::storage::hybrid::backend::{GuardedObjectProof, HybridStorage, RemoteObjectProof};
use crate::storage::{StorageBackend, StorageObjectMetadata};
use crate::wal::cloud_segment::DataCoverageRecord;
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct CloudMetadataPruneProof {
    pub(crate) key: String,
    pub(crate) expected_bytes: Vec<u8>,
    pub(crate) remote: StorageObjectMetadata,
}

#[derive(Clone)]
pub(crate) struct CloudMetadataPruneGuard {
    objects: Vec<GuardedObjectProof>,
}

impl CloudMetadataPruneGuard {
    pub(crate) fn new(cloud: Arc<CloudStorage>, objects: Vec<CloudMetadataPruneProof>) -> Self {
        let backend: Arc<dyn StorageBackend> = cloud;
        let objects = objects
            .into_iter()
            .map(|proof| {
                GuardedObjectProof::exact(
                    Arc::clone(&backend),
                    proof.key,
                    proof.expected_bytes,
                    proof.remote,
                )
            })
            .collect();
        Self { objects }
    }
}

#[derive(Clone, Default)]
pub(crate) struct CloudWalPruneGuard {
    manifest: Manifest,
    metadata: Option<CloudMetadataPruneGuard>,
}

impl CloudWalPruneGuard {
    pub(crate) fn new(manifest: Manifest, metadata: Option<CloudMetadataPruneGuard>) -> Self {
        Self { manifest, metadata }
    }
}

struct ValidatedWalObject {
    proof: RemoteObjectProof,
    data_records: Vec<DataCoverageRecord>,
}

/// Runtime-owned format operations layered over raw hybrid object I/O.
pub(crate) trait HybridPersistence {
    fn enqueue_wal_segment(
        &self,
        segment_id: u64,
        local_path: &Path,
        max_sequence: u64,
    ) -> MidgeResult<()>;

    fn verify_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<(), String>;

    fn remote_wal_segment_covered_by_manifest(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        manifest: &Manifest,
    ) -> Result<bool, String>;

    fn verify_manifest_cloud_objects(&self, manifest: &Manifest) -> Result<(), String>;

    fn prune_cloud_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
    ) -> Result<(), String>;

    fn write_sst_object(&self, sst_name: &str, data: Vec<u8>) -> MidgeResult<()>;

    fn delete_sst_object_blocking(&self, sst_name: &str) -> MidgeResult<()>;
}

impl HybridPersistence for HybridStorage {
    fn enqueue_wal_segment(
        &self,
        segment_id: u64,
        local_path: &Path,
        max_sequence: u64,
    ) -> MidgeResult<()> {
        self.enqueue_object_upload(
            segment_id,
            crate::wal::cloud_segment::object_key(segment_id),
            local_path,
            max_sequence,
        )
    }

    fn verify_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<(), String> {
        validate_remote_wal(self, segment_id, expected_max_sequence).map(|_| ())
    }

    fn remote_wal_segment_covered_by_manifest(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        manifest: &Manifest,
    ) -> Result<bool, String> {
        let validated = validate_remote_wal(self, segment_id, expected_max_sequence)?;
        Ok(wal_data_records_covered_by_manifest(
            &validated.data_records,
            manifest,
        ))
    }

    fn verify_manifest_cloud_objects(&self, manifest: &Manifest) -> Result<(), String> {
        for file in &manifest.files {
            validate_remote_sst(self, file)?;
        }
        Ok(())
    }

    fn prune_cloud_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
    ) -> Result<(), String> {
        let validated = validate_remote_wal(self, segment_id, expected_max_sequence)?;
        if !wal_data_records_covered_by_manifest(&validated.data_records, &guard.manifest) {
            return Err(format!(
                "cloud WAL segment {segment_id} contains records not covered by the committed manifest"
            ));
        }

        let mut dependencies = Vec::with_capacity(
            guard.manifest.files.len().saturating_add(
                guard
                    .metadata
                    .as_ref()
                    .map_or(0, |guard| guard.objects.len()),
            ),
        );
        for file in &guard.manifest.files {
            let proof = validate_remote_sst(self, file)?;
            dependencies.push(self.remote_identity_guard(&proof));
        }
        if let Some(metadata) = guard.metadata {
            dependencies.extend(metadata.objects);
        }

        self.delete_remote_object_guarded(segment_id, validated.proof, dependencies)
    }

    fn write_sst_object(&self, sst_name: &str, data: Vec<u8>) -> MidgeResult<()> {
        let expected_size = data.len() as u64;
        let expected_crc = crc32c::crc32c(&data);
        validate_sst_object_bytes(sst_name, expected_size, None, None, &data)
            .map_err(MidgeError::Internal)?;
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_upload", |_| Err(
            MidgeError::Internal("failpoint: cloud SST upload failed".to_string())
        ));

        let key = crate::sst::object_key(sst_name);
        self.publish_immutable_object(&key, data)?;
        let proof = self
            .remote_object_proof(&key)
            .map_err(MidgeError::Internal)?;
        validate_sst_object_bytes(
            sst_name,
            expected_size,
            Some(expected_crc),
            None,
            proof.bytes(),
        )
        .map_err(MidgeError::Internal)?;
        Ok(())
    }

    fn delete_sst_object_blocking(&self, sst_name: &str) -> MidgeResult<()> {
        self.delete_immutable_object_blocking(&crate::sst::object_key(sst_name))
    }
}

fn validate_remote_wal(
    storage: &HybridStorage,
    segment_id: u64,
    expected_max_sequence: u64,
) -> Result<ValidatedWalObject, String> {
    let key = crate::wal::cloud_segment::object_key(segment_id);
    let proof = storage.remote_object_proof(&key)?;
    let readback =
        crate::wal::cloud_segment::validate_bytes(&key, proof.bytes(), expected_max_sequence)?;
    Ok(ValidatedWalObject {
        proof,
        data_records: readback.data_records,
    })
}

pub(crate) fn wal_data_records_covered_by_manifest(
    data_records: &[DataCoverageRecord],
    manifest: &Manifest,
) -> bool {
    data_records.iter().all(|record| {
        manifest
            .files
            .iter()
            .any(|file| file_covers_record(file, record))
    })
}

fn file_covers_record(file: &FileMeta, record: &DataCoverageRecord) -> bool {
    if file.cf_id != record.cf_id {
        return false;
    }

    let (Some(smallest_seq), Some(largest_seq)) = (file.smallest_seq, file.largest_seq) else {
        return false;
    };
    if record.seq < smallest_seq || record.seq > largest_seq {
        return false;
    }

    let (Some(smallest_key), Some(largest_key)) =
        (file.smallest_key.as_ref(), file.largest_key.as_ref())
    else {
        return false;
    };
    if let Some(range_end) = record.range_end.as_ref() {
        smallest_key.as_slice() <= record.key.as_slice()
            && range_end.as_slice() <= largest_key.as_slice()
    } else {
        smallest_key.as_slice() <= record.key.as_slice()
            && record.key.as_slice() <= largest_key.as_slice()
    }
}

fn validate_remote_sst(
    storage: &HybridStorage,
    file: &FileMeta,
) -> Result<RemoteObjectProof, String> {
    let key = crate::sst::object_key(&file.name);
    let proof = storage.remote_object_proof(&key)?;
    validate_sst_object_bytes(
        &file.name,
        file.size_bytes,
        file.content_crc32c,
        Some(file),
        proof.bytes(),
    )?;
    Ok(proof)
}

fn validate_sst_object_bytes(
    sst_name: &str,
    expected_size_bytes: u64,
    expected_content_crc32c: Option<u32>,
    expected_file: Option<&FileMeta>,
    data: &[u8],
) -> Result<crate::sst::fs::SstFileSummary, String> {
    if expected_size_bytes > 0 && data.len() as u64 != expected_size_bytes {
        return Err(format!(
            "cloud SST '{sst_name}' size mismatch: manifest={expected_size_bytes}, object={}",
            data.len()
        ));
    }

    let actual_content_crc32c = crc32c::crc32c(data);
    if let Some(expected_content_crc32c) = expected_content_crc32c {
        if actual_content_crc32c != expected_content_crc32c {
            return Err(format!(
                "cloud SST '{sst_name}' content crc32c {actual_content_crc32c:08x} does not match manifest {expected_content_crc32c:08x}"
            ));
        }
    }

    let mut temp = tempfile::Builder::new()
        .prefix("midge-cloud-sst-verify-")
        .suffix(".sst")
        .tempfile()
        .map_err(|error| format!("create temp SST verifier for '{sst_name}': {error}"))?;
    temp.write_all(data)
        .map_err(|error| format!("write temp SST verifier for '{sst_name}': {error}"))?;
    temp.flush()
        .map_err(|error| format!("flush temp SST verifier for '{sst_name}': {error}"))?;

    let reader = crate::sst::fs::SstFileIo::open_with_real_fs(temp.path())
        .map_err(|error| format!("cloud SST '{sst_name}' failed validation: {error}"))?;
    let summary = reader
        .summary()
        .map_err(|error| format!("cloud SST '{sst_name}' summary validation: {error}"))?;
    if let Some(expected_file) = expected_file {
        verify_sst_summary_matches_manifest(sst_name, &summary, expected_file)?;
    }
    Ok(summary)
}

fn verify_sst_summary_matches_manifest(
    sst_name: &str,
    summary: &crate::sst::fs::SstFileSummary,
    file: &FileMeta,
) -> Result<(), String> {
    if file.size_bytes > 0 && summary.size_bytes != file.size_bytes {
        return Err(format!(
            "cloud SST '{sst_name}' physical size {} does not match manifest {}",
            summary.size_bytes, file.size_bytes
        ));
    }
    if file
        .smallest_key
        .as_ref()
        .is_some_and(|key| summary.smallest_key.as_slice() != key.as_slice())
    {
        return Err(format!(
            "cloud SST '{sst_name}' smallest key does not match manifest"
        ));
    }
    if file
        .largest_key
        .as_ref()
        .is_some_and(|key| summary.largest_key.as_slice() != key.as_slice())
    {
        return Err(format!(
            "cloud SST '{sst_name}' largest key does not match manifest"
        ));
    }
    if file
        .smallest_seq
        .is_some_and(|sequence| summary.smallest_seq != sequence)
    {
        return Err(format!(
            "cloud SST '{sst_name}' smallest sequence {} does not match manifest {:?}",
            summary.smallest_seq, file.smallest_seq
        ));
    }
    if file
        .largest_seq
        .is_some_and(|sequence| summary.largest_seq != sequence)
    {
        return Err(format!(
            "cloud SST '{sst_name}' largest sequence {} does not match manifest {:?}",
            summary.largest_seq, file.largest_seq
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_require_manifest_coverage_for_wal_records() {
        // Arrange
        let manifest = Manifest {
            files: vec![FileMeta {
                cf_id: 7,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"m".to_vec()),
                smallest_seq: Some(10),
                largest_seq: Some(20),
                ..FileMeta::default()
            }],
            ..Manifest::default()
        };
        let covered = DataCoverageRecord {
            cf_id: 7,
            key: b"b".to_vec(),
            range_end: None,
            seq: 12,
        };
        let outside_key = DataCoverageRecord {
            key: b"z".to_vec(),
            ..covered.clone()
        };

        // Act
        let covered_result = wal_data_records_covered_by_manifest(&[covered], &manifest);
        let outside_result = wal_data_records_covered_by_manifest(&[outside_key], &manifest);

        // Assert
        assert!(covered_result);
        assert!(!outside_result);
    }

    #[test]
    fn should_require_full_range_coverage_for_wal_tombstones() {
        // Arrange
        let file = FileMeta {
            cf_id: 1,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"m".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(9),
            ..FileMeta::default()
        };
        let covered = DataCoverageRecord {
            cf_id: 1,
            key: b"c".to_vec(),
            range_end: Some(b"k".to_vec()),
            seq: 5,
        };
        let uncovered = DataCoverageRecord {
            range_end: Some(b"z".to_vec()),
            ..covered.clone()
        };

        // Act
        let covered_result = file_covers_record(&file, &covered);
        let uncovered_result = file_covers_record(&file, &uncovered);

        // Assert
        assert!(covered_result);
        assert!(!uncovered_result);
    }
}
