//! Tests for runtime-owned hybrid persistence.
//!
//! These exercise WAL catalog publication and takeover, manifest-coverage WAL
//! pruning, and SST publication proofs. They live beside the code that owns
//! those formats rather than inside the format-neutral storage backend.

use super::*;

use crate::sst::SstFactory;

use crate::storage::cloud::{CloudBackend, CloudStorage, MockCloudBackend};

use crate::storage::hybrid::backend::HybridStorage;

use crate::storage::hybrid::backend::HybridQueueLimits;

use crate::storage::{StorageCallback, StorageEvent, StorageOutcome};

use parking_lot::Mutex;

use bytes::Bytes;

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Barrier,
};

use crate::types::EntryType;
use std::time::{Duration, Instant};

mod resumable_prune;

mod retirement_lifetime;

/// Test-only entry points into cloud persistence.
///
/// These exist so tests can drive one segment, one manifest or one SST at a
/// time. They live here rather than on `CloudPersistence` itself so production
/// keeps exactly the entry points it calls - the batched
/// `prune_cloud_wal_segments_within` and nothing narrower.
impl CloudPersistence {
    pub(crate) fn verify_remote_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
    ) -> Result<(), String> {
        let (_, entry) = authoritative_wal_entry(self, segment_id)?;
        if entry.max_sequence != expected_max_sequence {
            return Err(format!(
                "cloud WAL catalog segment {segment_id} max sequence {} does not match expected {expected_max_sequence}",
                entry.max_sequence
            ));
        }
        validate_remote_wal(self, &entry, &crate::common::OperationDeadline::unbounded())
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn verify_manifest_cloud_objects(&self, manifest: &Manifest) -> Result<(), String> {
        self.verify_manifest_cloud_objects_within(
            manifest,
            &crate::common::OperationDeadline::unbounded(),
        )
        .map_err(|error| error.to_string())
    }

    pub(crate) fn verify_manifest_cloud_objects_within(
        &self,
        manifest: &Manifest,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        for file in &manifest.files {
            validate_remote_sst_within(self, file, deadline)?;
        }
        Ok(())
    }

    pub(crate) fn prune_cloud_wal_segment(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
    ) -> Result<(), String> {
        self.prune_cloud_wal_segment_within(
            segment_id,
            expected_max_sequence,
            guard,
            fencing_epoch,
            &crate::common::OperationDeadline::unbounded(),
        )
        .map_err(|error| error.to_string())
    }

    pub(crate) fn prune_cloud_wal_segment_within(
        &self,
        segment_id: u64,
        expected_max_sequence: u64,
        guard: CloudWalPruneGuard,
        fencing_epoch: u64,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        let mut results = self.prune_cloud_wal_segments_within(
            &[(segment_id, expected_max_sequence)],
            guard,
            fencing_epoch,
            deadline,
        )?;
        let Some((_, result)) = results.pop() else {
            // Catalog authority was retired and a storage-owned conditional
            // delete worker now owns the terminal completion event.
            return Ok(());
        };
        if result.is_ok() {
            self.queue_cloud_wal_prune_complete(segment_id, crate::storage::StorageOutcome::Ok(()));
        }
        result
    }

    pub(crate) fn write_sst_object(&self, sst_name: &str, data: Vec<u8>) -> MidgeResult<()> {
        self.write_sst_object_within(
            sst_name,
            data,
            &crate::common::OperationDeadline::unbounded(),
        )
    }

    pub(crate) fn write_sst_object_within(
        &self,
        sst_name: &str,
        data: Vec<u8>,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<()> {
        self.write_sst_object_with_proof(sst_name, data, deadline)
            .map(|_| ())
    }

    pub(crate) fn write_sst_object_with_proof(
        &self,
        sst_name: &str,
        data: Vec<u8>,
        deadline: &crate::common::OperationDeadline,
    ) -> MidgeResult<GuardedObjectProof> {
        let expected_size = data.len() as u64;
        let expected_crc = crc32c::crc32c(&data);
        validate_sst_object_bytes(sst_name, expected_size, None, None, &data)
            .map_err(MidgeError::Internal)?;
        crate::failpoints::fail_point!("midge::cloud::inject_fail_sst_upload", |_| Err(
            MidgeError::Internal("failpoint: cloud SST upload failed".to_string())
        ));

        let key = crate::cloud_layout::object_key(sst_name);
        self.publish_immutable_object_within(&key, data, deadline)?;
        let proof = self
            .remote_object_proof_within(&key, deadline)
            .map_err(|error| contextualize_cloud_error(error, "cloud SST readback failed"))?;
        validate_sst_object_bytes(
            sst_name,
            expected_size,
            Some(expected_crc),
            None,
            proof.bytes(),
        )
        .map_err(MidgeError::Internal)?;
        Ok(self.remote_identity_guard(&proof))
    }
}

#[test]
fn should_preserve_timeout_variant_when_adding_cloud_publication_context() {
    // Arrange
    let timeout = MidgeError::Timeout("remote CAS timed out".to_string());

    // Act
    let contextualized = contextualize_cloud_error(timeout, "cloud WAL catalog publication failed");

    // Assert
    assert!(matches!(
        contextualized,
        MidgeError::Timeout(message)
            if message.contains("catalog publication")
                && message.contains("remote CAS timed out")
    ));
}

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
        op: crate::wal::WalOpKind::Put,
        key: b"b".to_vec(),
        value: Some(b"value".to_vec()),
        expiration: None,
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
        op: crate::wal::WalOpKind::DeleteRange,
        key: b"c".to_vec(),
        value: None,
        expiration: None,
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

#[test]
fn should_require_exact_raw_state_when_streaming_wal_retirement() {
    use crate::wal::WalOpKind::{Delete, DeleteRange, Put};
    for (wal_op, wal_expiration, sst_value, sst_expiration, covered) in [
        (Put, None, Some(b"different".as_slice()), None, false),
        (Put, Some(1), Some(b"v".as_slice()), Some(2), false),
        (Put, Some(1), Some(b"v".as_slice()), Some(1), true),
        (Put, None, None, None, false),
        (Delete, None, None, None, true),
        (DeleteRange, None, None, None, false),
    ] {
        // Arrange
        let (_cloud, storage) = hybrid_with_mock_cloud();
        storage.enable_ephemeral_sst_cache(64 * 1024);
        let mut record = crate::wal::WalRecord::new(
            wal_op,
            Bytes::from_static(b"k"),
            Some(Bytes::from_static(b"v")),
            7,
            1,
        );
        record.expiration = wal_expiration;
        if wal_op != Put {
            record.value = None;
        }
        if wal_op == DeleteRange {
            record.range_end = Some(Bytes::from_static(b"z"));
        }
        let payload = crate::wal::encoding::encode(&record).expect("encode WAL");
        let mut wal = Vec::new();
        crate::wal::frame::append_frame(&mut wal, &payload).expect("frame WAL");
        let key = write_authoritative_cloud_wal(&storage, 1, 7, wal);
        let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
        let mut writer = factory.create().expect("SST writer");
        writer
            .add_with_meta(
                b"k",
                sst_value,
                7,
                if sst_value.is_some() {
                    crate::types::EntryType::Put
                } else {
                    crate::types::EntryType::Delete
                },
                sst_expiration,
            )
            .expect("SST raw state");
        let sst = writer.finish_bytes().expect("SST bytes");
        let manifest = manifest_covering_wal("raw-state.sst", &sst, 7, Some(crc32c::crc32c(&sst)));
        write_cloud_object(
            &storage,
            &crate::cloud_layout::object_key("raw-state.sst"),
            sst,
        );

        // Act
        let result =
            storage.prune_cloud_wal_segment(1, 7, CloudWalPruneGuard::new(manifest, None), 2);

        // Assert
        if covered {
            result.expect("matching raw state retires WAL");
            assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
        } else {
            assert!(
                result.is_err(),
                "uncovered {wal_op:?} with TTL {wal_expiration:?} must retain WAL"
            );
            assert_cloud_object_exists(&storage, &key);
        }
    }
}

/// SST range tombstones `(start, end, seq)` for one coverage case.
type TombstoneCase = &'static [(&'static [u8], &'static [u8], u64)];

#[test]
fn should_retire_cloud_wal_delete_range_only_when_sst_range_tombstones_cover_it() {
    let tombstone_cases: [(TombstoneCase, bool); 4] = [
        (&[(b"k", b"z", 7)], true),
        (&[(b"k", b"m", 7), (b"m", b"z", 7)], true),
        (&[(b"k", b"m", 7)], false),
        (&[(b"k", b"z", 6)], false),
    ];
    for (tombstones, covered) in tombstone_cases {
        // Arrange
        let (_cloud, storage) = hybrid_with_mock_cloud();
        storage.enable_ephemeral_sst_cache(64 * 1024);
        let mut record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::DeleteRange,
            Bytes::from_static(b"k"),
            None,
            7,
            1,
        );
        record.range_end = Some(Bytes::from_static(b"z"));
        let payload = crate::wal::encoding::encode(&record).expect("encode WAL");
        let mut wal = Vec::new();
        crate::wal::frame::append_frame(&mut wal, &payload).expect("frame WAL");
        let key = write_authoritative_cloud_wal(&storage, 1, 7, wal);
        let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
        let mut writer = factory.create().expect("SST writer");
        writer
            .add_with_meta(b"k", None, 7, EntryType::Delete, None)
            .expect("SST point state");
        for (start, end, seq) in tombstones {
            writer
                .add_range_tombstone(start, end, *seq)
                .expect("SST range tombstone");
        }
        let sst = writer.finish_bytes().expect("SST bytes");
        let mut manifest = manifest_covering_wal("ranges.sst", &sst, 7, Some(crc32c::crc32c(&sst)));
        manifest.files[0].largest_key = tombstones.iter().map(|(_, end, _)| end.to_vec()).max();
        manifest.files[0].smallest_seq = tombstones
            .iter()
            .map(|(_, _, seq)| *seq)
            .min()
            .map(|seq| seq.min(7));
        write_cloud_object(
            &storage,
            &crate::cloud_layout::object_key("ranges.sst"),
            sst,
        );

        // Act
        let result =
            storage.prune_cloud_wal_segment(1, 7, CloudWalPruneGuard::new(manifest, None), 2);

        // Assert
        if covered {
            result.expect("SST range tombstones covering the delete retire its WAL");
            assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
        } else {
            assert!(
                result.is_err(),
                "tombstones {tombstones:?} do not cover the delete, so WAL must be retained"
            );
            assert_cloud_object_exists(&storage, &key);
        }
    }
}

#[test]
fn should_retain_newer_wal_authority_when_streamed_oldest_segment_is_uncovered() {
    // Arrange
    let (_cloud, storage) = hybrid_with_mock_cloud();
    storage.enable_ephemeral_sst_cache(64 * 1024);
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"missing"),
        Some(Bytes::from_static(b"v")),
        1,
        1,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode WAL");
    let mut wal = Vec::new();
    crate::wal::frame::append_frame(&mut wal, &payload).expect("frame WAL");
    let first = write_authoritative_cloud_wal(&storage, 1, 1, wal);
    let second = write_authoritative_cloud_wal(&storage, 2, 2, valid_wal_bytes(2));
    let sst = valid_sst_bytes(b"k", b"v", 2);
    let manifest = manifest_covering_wal("prefix.sst", &sst, 2, Some(crc32c::crc32c(&sst)));
    write_cloud_object(
        &storage,
        &crate::cloud_layout::object_key("prefix.sst"),
        sst,
    );

    // Act
    let results = storage
        .prune_cloud_wal_segments_within(
            &[(1, 1), (2, 2)],
            CloudWalPruneGuard::new(manifest, None),
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
        .expect("prune attempt");

    // Assert
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|(_, result)| result.is_err()));
    assert_cloud_object_exists(&storage, &first);
    assert_cloud_object_exists(&storage, &second);
    let proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("retained catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(proof.bytes())
        .expect("decode catalog");
    assert_eq!(catalog.segments.len(), 2);
}

#[test]
fn should_retain_cloud_wal_authority_when_retirement_memory_is_exhausted() {
    // Arrange
    let (cloud, storage) = hybrid_with_mock_cloud();
    storage.enable_ephemeral_sst_cache(64 * 1024);
    let sequence = 8;
    let key = write_authoritative_cloud_wal(&storage, 1, sequence, valid_wal_bytes(sequence));
    cloud.clear_history();

    // Act
    let result = storage.prune_cloud_wal_segment(
        1,
        sequence,
        CloudWalPruneGuard::default().with_memory_limit(128),
        2,
    );

    // Assert
    assert!(result
        .expect_err("insufficient proof memory must retain WAL")
        .to_ascii_lowercase()
        .contains("resource limit"));
    assert!(!cloud.get_downloads().iter().any(|key| Path::new(key)
        .extension()
        .is_some_and(|extension| extension == "wal")));
    assert_cloud_object_exists(&storage, &key);
    let proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("retained catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(proof.bytes())
        .expect("decode catalog");
    assert!(catalog.segments.contains_key(&1));
}

#[test]
fn should_delete_range_verified_wal_with_filesystem_identity() {
    // Arrange
    let directory = tempfile::tempdir().expect("directory");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(directory.path().join("local"))
            .expect("local store"),
    );
    let remote = Arc::new(
        crate::storage::filesystem::FileSystem::new(directory.path().join("remote"))
            .expect("remote store"),
    );
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local,
        remote,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    storage.enable_ephemeral_sst_cache(64 * 1024);
    storage
        .fence_cloud_wal_catalog(2)
        .expect("initialize catalog");
    let sequence = 9;
    let key = write_authoritative_cloud_wal(&storage, 1, sequence, valid_wal_bytes(sequence));
    let name = "filesystem-prune.sst";
    let sst = valid_sst_bytes(b"k", b"v", sequence);
    let manifest = manifest_covering_wal(name, &sst, sequence, Some(crc32c::crc32c(&sst)));
    write_cloud_object(&storage, &crate::cloud_layout::object_key(name), sst);

    // Act
    storage
        .prune_cloud_wal_segment(1, sequence, CloudWalPruneGuard::new(manifest, None), 2)
        .expect("retire WAL");

    // Assert
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
    assert_cloud_object_missing(&storage, &key);
}

#[test]
fn should_retire_large_cloud_wal_without_whole_object_downloads() {
    // Arrange
    let (cloud, storage) = hybrid_with_mock_cloud();
    storage.enable_ephemeral_sst_cache(64 * 1024);
    let sequence = 400;
    let wal = (1..=sequence).flat_map(valid_wal_bytes).collect::<Vec<_>>();
    assert!(wal.len() > 16 * 1024);
    write_authoritative_cloud_wal(&storage, 1, sequence, wal);
    let name = "streamed-retirement.sst";
    let sst = valid_sst_bytes(b"k", b"v", sequence);
    let manifest = manifest_covering_wal(name, &sst, sequence, Some(crc32c::crc32c(&sst)));
    write_cloud_object(&storage, &crate::cloud_layout::object_key(name), sst);
    cloud.clear_history();

    // Act
    storage
        .prune_cloud_wal_segment(
            1,
            sequence,
            CloudWalPruneGuard::new(manifest, None).with_memory_limit(16 * 1024),
            2,
        )
        .expect("covered WAL retirement");

    // Assert
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
    assert!(
        !cloud
            .get_downloads()
            .iter()
            .any(|key| std::path::Path::new(key)
                .extension()
                .is_some_and(|extension| extension == "wal")),
        "retirement must never download the complete WAL backlog"
    );
}

#[test]
fn should_prune_wal_without_fetching_unrelated_ssts_when_local_cache_is_ephemeral() {
    // Arrange
    let (cloud, storage) = hybrid_with_mock_cloud();
    storage.enable_ephemeral_sst_cache(1024 * 1024);
    let sequence = 31;
    write_authoritative_cloud_wal(&storage, 1, sequence, valid_wal_bytes(sequence));
    let name = "relevant.sst";
    let bytes = valid_sst_bytes(b"k", b"v", sequence);
    let mut manifest = manifest_covering_wal(name, &bytes, sequence, Some(crc32c::crc32c(&bytes)));
    write_cloud_object(&storage, &crate::cloud_layout::object_key(name), bytes);
    manifest.files.push(crate::metadata::FileMeta {
        name: "unrelated.sst".into(),
        cf_id: 0,
        smallest_key: Some(b"z".to_vec()),
        largest_key: Some(b"zz".to_vec()),
        key_bounds_complete: true,
        ..Default::default()
    });
    cloud.clear_history();

    // Act
    storage
        .prune_cloud_wal_segment(1, sequence, CloudWalPruneGuard::new(manifest, None), 2)
        .expect("exactly covered WAL retirement");

    // Assert
    assert!(wait_for_wal_prune_result(&storage, 1).is_ok());
    assert!(
        !cloud.get_downloads().iter().any(|key| key.contains("sst/")),
        "WAL cleanup must use bounded ranges, not whole SST downloads"
    );
}

#[test]
fn should_publish_sst_without_duplicate_local_copy_when_ephemeral_cache_is_enabled() {
    // Arrange
    let (_cloud, storage) = hybrid_with_mock_cloud();
    storage.enable_ephemeral_sst_cache(20 * 1024 * 1024 * 1024);
    let name = "ephemeral.sst";
    let key = crate::cloud_layout::object_key(name);
    let bytes = valid_sst_bytes(b"key", b"value", 1);

    // Act
    storage
        .write_sst_object(name, bytes.clone())
        .expect("remote publication");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
    let exists = HybridStorage::object_exists_in_backend_within(
        &storage.local_store(),
        &key,
        storage.callback_timeout(),
        &crate::common::OperationDeadline::unbounded(),
    )
    .expect("local cache existence check");
    assert!(
        !exists,
        "remote publication must not duplicate the runtime SST staging file"
    );
}

#[test]
fn should_preserve_remote_sst_when_evicting_legacy_local_cache() {
    // Arrange
    let (_cloud, storage) = hybrid_with_mock_cloud();
    let name = "legacy-local.sst";
    let key = crate::cloud_layout::object_key(name);
    let bytes = valid_sst_bytes(b"key", b"value", 1);
    storage
        .write_sst_object(name, bytes.clone())
        .expect("publish both copies");
    storage.enable_ephemeral_sst_cache(20 * 1024 * 1024 * 1024);

    // Act
    storage
        .evict_local_object_cache(&key)
        .expect("local cache eviction");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
}

#[test]
fn should_reject_wal_catalog_publication_from_stale_epoch_after_takeover() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create stale publisher WAL dir");
    let segment_id = 1;
    let max_sequence = 11;
    let bytes = valid_wal_bytes(max_sequence);
    let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&local_path, &bytes).expect("write stale publisher local WAL");
    let object_key = crate::wal::cloud_segment_object_key(segment_id, 1);
    write_cloud_object(&storage, &object_key, bytes);
    storage
        .fence_cloud_wal_catalog(3)
        .expect("new lease fences WAL catalog");

    // Act
    let result = storage.publish_remote_wal_segment(
        segment_id,
        max_sequence,
        &local_path,
        2,
        &crate::common::OperationDeadline::unbounded(),
    );

    // Assert
    let error = result.expect_err("stale lease epoch must not publish WAL authority");
    assert!(matches!(
        error,
        crate::common::MidgeError::Fenced(message)
            if message.contains("requires fencing epoch 3")
    ));
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read winning WAL catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode winning WAL catalog");
    assert!(!catalog.segments.contains_key(&segment_id));
    assert_cloud_object_exists(&storage, &object_key);
}

#[test]
fn should_converge_catalog_copies_after_publication() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create mirrored catalog WAL dir");
    let segment_id = 1;
    let max_sequence = 11;
    let bytes = valid_wal_bytes(max_sequence);
    let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&local_path, &bytes).expect("write mirrored catalog local WAL");
    let object_key = crate::wal::cloud_segment_object_key(segment_id, 1);
    write_cloud_object(&storage, &object_key, bytes);

    // Act
    storage
        .publish_remote_wal_segment(
            segment_id,
            max_sequence,
            &local_path,
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
        .expect("publish WAL through mirrored catalog");

    // Assert
    let catalog = assert_wal_catalog_copies_match(&storage);
    assert_eq!(catalog.fencing_epoch, 2);
    assert!(catalog.segments.contains_key(&segment_id));
}

#[test]
fn should_converge_catalog_copies_after_takeover() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();

    // Act
    storage
        .fence_cloud_wal_catalog(3)
        .expect("advance mirrored catalog authority");

    // Assert
    let catalog = assert_wal_catalog_copies_match(&storage);
    assert_eq!(catalog.fencing_epoch, 3);
}

#[test]
fn should_not_publish_losing_mirror_when_initial_catalog_cas_loses() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create initial catalog race test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let winning_catalog = crate::wal::cloud_catalog::WalPublicationCatalog::empty(7)
        .expect("create winning catalog")
        .encode()
        .expect("encode winning catalog");
    let racing_cloud = Arc::new(WinningInitialCatalogCasBackend {
        inner: Arc::clone(&mock_cloud),
        winning_catalog: winning_catalog.clone(),
        inject_winner: AtomicBool::new(true),
    });
    let cloud = Arc::new(CloudStorage::new(racing_cloud, String::new()));
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));

    // Act
    let error = storage
        .fence_cloud_wal_catalog(2)
        .expect_err("losing initial catalog CAS must fail");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::Busy(_)));
    assert_eq!(
        storage
            .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
            .expect("read winning primary catalog")
            .bytes(),
        winning_catalog
    );
    assert!(
        storage
            .remote_object_proof_optional(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
            .expect("check losing mirror")
            .is_none(),
        "a draft that lost primary authority must never be published to the mirror"
    );
}

#[test]
fn should_report_fenced_when_catalog_authority_changes_before_publication() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create takeover race test dir");
    let (pausing_cloud, storage) = hybrid_with_pausing_catalog_cloud(tmp.path());

    let segment_id = 1;
    let max_sequence = 11;
    let bytes = valid_wal_bytes(max_sequence);
    let local_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&local_path, &bytes).expect("write publisher local WAL");
    let object_key = crate::wal::cloud_segment::object_key(segment_id, 1);
    write_cloud_object(&storage, &object_key, bytes);

    pausing_cloud.pause_next_catalog_compare_exchange();
    let publisher_storage = Arc::clone(&storage);
    let publisher = std::thread::spawn(move || {
        publisher_storage.publish_remote_wal_segment(
            segment_id,
            max_sequence,
            &local_path,
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
    });
    pausing_cloud.wait_for_catalog_compare_exchange();
    storage
        .fence_cloud_wal_catalog(3)
        .expect("takeover must advance catalog authority");

    // Act
    pausing_cloud.release_catalog_compare_exchange();
    let error = publisher
        .join()
        .expect("publisher thread must not panic")
        .expect_err("stale publisher must lose catalog authority");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::Fenced(_)));
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read takeover catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode takeover catalog");
    assert_eq!(catalog.fencing_epoch, 3);
    assert!(!catalog.segments.contains_key(&segment_id));
    assert_cloud_object_exists(&storage, &object_key);
}

#[test]
fn should_preserve_same_epoch_catalog_updates_when_publication_races_retirement() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create catalog mutation race test dir");
    let (pausing_cloud, storage) = hybrid_with_pausing_catalog_cloud(tmp.path());

    let retired_segment_id = 1;
    let retired_max_sequence = 11;
    write_authoritative_cloud_wal(
        &storage,
        retired_segment_id,
        retired_max_sequence,
        valid_wal_bytes(retired_max_sequence),
    );
    let sst_name = "catalog-mutation-race.sst";
    let sst_bytes = valid_sst_bytes(b"k", b"v", retired_max_sequence);
    write_cloud_object(
        &storage,
        &crate::cloud_layout::object_key(sst_name),
        sst_bytes.clone(),
    );
    let manifest = manifest_covering_wal(
        sst_name,
        &sst_bytes,
        retired_max_sequence,
        Some(crc32c::crc32c(&sst_bytes)),
    );

    let published_segment_id = 2;
    let published_max_sequence = 12;
    let published_bytes = valid_wal_bytes(published_max_sequence);
    let published_local_path = tmp
        .path()
        .join(crate::wal::segment_file_name(published_segment_id));
    std::fs::write(&published_local_path, &published_bytes)
        .expect("write concurrently published local WAL");
    let published_object_key = crate::wal::cloud_segment::object_key(published_segment_id, 1);
    write_cloud_object(&storage, &published_object_key, published_bytes);

    pausing_cloud.pause_next_catalog_compare_exchange();
    let publisher_storage = Arc::clone(&storage);
    let publisher = std::thread::spawn(move || {
        publisher_storage.publish_remote_wal_segment(
            published_segment_id,
            published_max_sequence,
            &published_local_path,
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
    });
    pausing_cloud.wait_for_catalog_compare_exchange();

    let (prune_tx, prune_rx) = std::sync::mpsc::channel();
    let pruner_storage = Arc::clone(&storage);
    let pruner = std::thread::spawn(move || {
        let result = pruner_storage.prune_cloud_wal_segment(
            retired_segment_id,
            retired_max_sequence,
            CloudWalPruneGuard::new(manifest, None),
            2,
        );
        prune_tx
            .send(result)
            .expect("send concurrent catalog retirement result");
    });
    let early_prune_result = match prune_rx.recv_timeout(Duration::from_millis(100)) {
        Ok(result) => Some(result),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("catalog retirement thread disconnected before publication release")
        }
    };

    // Act
    pausing_cloud.release_catalog_compare_exchange();
    let publication_result = publisher.join().expect("publisher thread must not panic");
    let retirement_result = early_prune_result.unwrap_or_else(|| {
        prune_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("catalog retirement must complete after publication release")
    });
    pruner
        .join()
        .expect("catalog retirement thread must not panic");

    // Assert
    publication_result.expect("same-epoch WAL publication must succeed");
    retirement_result.expect("same-epoch WAL catalog retirement must succeed");
    let delete_result = wait_for_wal_prune_result(&storage, retired_segment_id);
    assert!(
        delete_result.is_ok(),
        "retired WAL object deletion must succeed: {delete_result:?}"
    );

    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read catalog after serialized mutations");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode catalog after serialized mutations");
    assert!(!catalog.segments.contains_key(&retired_segment_id));
    assert!(catalog.segments.contains_key(&published_segment_id));
    storage
        .verify_remote_wal_segment(published_segment_id, published_max_sequence)
        .expect("newly published segment must remain authoritative");
}

#[test]
fn should_bound_wal_catalog_lock_wait_by_publication_deadline() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create bounded catalog lock test dir");
    let (pausing_cloud, storage) = hybrid_with_pausing_catalog_cloud(tmp.path());

    let holding_segment_id = 1;
    let holding_max_sequence = 11;
    let holding_bytes = valid_wal_bytes(holding_max_sequence);
    let holding_local_path = tmp
        .path()
        .join(crate::wal::segment_file_name(holding_segment_id));
    std::fs::write(&holding_local_path, &holding_bytes).expect("write lock-holding local WAL");
    write_cloud_object(
        &storage,
        &crate::wal::cloud_segment::object_key(holding_segment_id, 1),
        holding_bytes,
    );

    let waiting_segment_id = 2;
    let waiting_max_sequence = 12;
    let waiting_bytes = valid_wal_bytes(waiting_max_sequence);
    let waiting_local_path = tmp
        .path()
        .join(crate::wal::segment_file_name(waiting_segment_id));
    std::fs::write(&waiting_local_path, &waiting_bytes).expect("write lock-waiting local WAL");
    write_cloud_object(
        &storage,
        &crate::wal::cloud_segment::object_key(waiting_segment_id, 1),
        waiting_bytes,
    );

    pausing_cloud.pause_next_catalog_compare_exchange();
    let publisher_storage = Arc::clone(&storage);
    let holder = std::thread::spawn(move || {
        publisher_storage.publish_remote_wal_segment(
            holding_segment_id,
            holding_max_sequence,
            &holding_local_path,
            2,
            &crate::common::OperationDeadline::unbounded(),
        )
    });
    pausing_cloud.wait_for_catalog_compare_exchange();

    // Act
    let started = Instant::now();
    let waiting_result = storage.publish_remote_wal_segment(
        waiting_segment_id,
        waiting_max_sequence,
        &waiting_local_path,
        2,
        &crate::common::OperationDeadline::from_budget(Duration::from_millis(100)),
    );
    let elapsed = started.elapsed();
    pausing_cloud.release_catalog_compare_exchange();
    let holding_result = holder.join().expect("lock holder thread must not panic");

    // Assert
    holding_result.expect("lock-holding publication must finish after release");
    assert!(
        matches!(
            &waiting_result,
            Err(crate::common::MidgeError::Timeout(message))
                if message.contains("waiting to mutate the cloud WAL catalog")
        ),
        "bounded publication must time out waiting for the catalog lock: {waiting_result:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "catalog lock wait exceeded the shared publication deadline: {elapsed:?}"
    );
}

#[test]
fn should_reject_stale_catalog_compare_exchange_after_takeover() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let stale_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read pre-takeover WAL catalog");
    let mut takeover_catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(stale_proof.bytes())
            .expect("decode pre-takeover WAL catalog");
    takeover_catalog
        .fence_to(3)
        .expect("advance winning takeover epoch");
    storage
        .compare_exchange_remote_object(
            crate::wal::cloud_catalog::OBJECT_KEY,
            Some(stale_proof.metadata()),
            takeover_catalog.encode().expect("encode winning catalog"),
        )
        .expect("publish winning takeover catalog");
    let mut losing_catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(stale_proof.bytes())
            .expect("decode stale WAL catalog");
    losing_catalog
        .fence_to(4)
        .expect("prepare losing catalog mutation");

    // Act
    let result = storage.compare_exchange_remote_object(
        crate::wal::cloud_catalog::OBJECT_KEY,
        Some(stale_proof.metadata()),
        losing_catalog.encode().expect("encode losing catalog"),
    );

    // Assert
    assert!(matches!(result, Err(crate::common::MidgeError::Busy(_))));
    let winning_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read winning WAL catalog");
    let winning_catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(winning_proof.bytes())
            .expect("decode winning WAL catalog");
    assert_eq!(winning_catalog.fencing_epoch, 3);
}

fn hybrid_with_mock_cloud() -> (Arc<MockCloudBackend>, CloudPersistence) {
    let tmp = tempfile::tempdir().expect("create hybrid storage test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(
        mock_cloud.clone(),
        "hybrid-test".to_string(),
    ));
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    storage
        .fence_cloud_wal_catalog(2)
        .expect("initialize test WAL publication catalog");
    (mock_cloud, storage)
}

fn assert_wal_catalog_copies_match(
    storage: &HybridStorage,
) -> crate::wal::cloud_catalog::WalPublicationCatalog {
    let primary = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read primary WAL catalog");
    let mirror = storage
        .remote_object_proof(crate::wal::cloud_catalog::MIRROR_OBJECT_KEY)
        .expect("read WAL catalog mirror");
    assert_eq!(
        primary.bytes(),
        mirror.bytes(),
        "successful catalog mutations must converge both copies"
    );
    crate::wal::cloud_catalog::WalPublicationCatalog::decode(primary.bytes())
        .expect("decode converged WAL catalog")
}

fn hybrid_with_pausing_catalog_cloud(
    root: &std::path::Path,
) -> (Arc<PausingCatalogCasBackend>, Arc<CloudPersistence>) {
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(root.join("local"))
            .expect("create pausing catalog local backend"),
    );
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let pausing_cloud = Arc::new(PausingCatalogCasBackend::new(Arc::clone(&mock_cloud)));
    let cloud = Arc::new(CloudStorage::new(pausing_cloud.clone(), String::new()));
    let storage = Arc::new(CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ))));
    storage
        .fence_cloud_wal_catalog(2)
        .expect("initialize pausing WAL publication catalog");
    (pausing_cloud, storage)
}

struct PausingCatalogCasBackend {
    inner: Arc<MockCloudBackend>,
    pause_next_catalog_compare_exchange: AtomicBool,
    catalog_compare_exchange_started: Barrier,
    release_catalog_compare_exchange: Barrier,
}

struct WinningInitialCatalogCasBackend {
    inner: Arc<MockCloudBackend>,
    winning_catalog: Vec<u8>,
    inject_winner: AtomicBool,
}

impl CloudBackend for WinningInitialCatalogCasBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let is_initial_catalog_compare_exchange = key == crate::wal::cloud_catalog::OBJECT_KEY
            && headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("if-none-match") && value == "*");
        if is_initial_catalog_compare_exchange && self.inject_winner.swap(false, Ordering::SeqCst) {
            let (winner_tx, winner_rx) = std::sync::mpsc::channel();
            self.inner
                .submit_put(key, self.winning_catalog.clone(), Vec::new(), winner_tx);
            winner_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("competing initial catalog write must complete");
        }
        self.inner.submit_put(key, data, headers, callback);
    }

    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity, submit_delete, submit_list, submit_head);
}

impl PausingCatalogCasBackend {
    fn new(inner: Arc<MockCloudBackend>) -> Self {
        Self {
            inner,
            pause_next_catalog_compare_exchange: AtomicBool::new(false),
            catalog_compare_exchange_started: Barrier::new(2),
            release_catalog_compare_exchange: Barrier::new(2),
        }
    }

    fn pause_next_catalog_compare_exchange(&self) {
        self.pause_next_catalog_compare_exchange
            .store(true, Ordering::SeqCst);
    }

    fn wait_for_catalog_compare_exchange(&self) {
        self.catalog_compare_exchange_started.wait();
    }

    fn release_catalog_compare_exchange(&self) {
        self.release_catalog_compare_exchange.wait();
    }
}

impl CloudBackend for PausingCatalogCasBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let is_catalog_compare_exchange = key == crate::wal::cloud_catalog::OBJECT_KEY
            && headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("if-match"));
        if is_catalog_compare_exchange
            && self
                .pause_next_catalog_compare_exchange
                .swap(false, Ordering::SeqCst)
        {
            self.catalog_compare_exchange_started.wait();
            self.release_catalog_compare_exchange.wait();
        }
        self.inner.submit_put(key, data, headers, callback);
    }

    crate::storage::cloud::forward_cloud_backend!(inner; submit_get, submit_get_with_metadata, submit_get_range, submit_get_range_with_identity, submit_delete, submit_list, submit_head);
}

#[test]
fn should_route_each_object_class_to_its_separate_cloud_store() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create class-routing test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let wal_mock = Arc::new(MockCloudBackend::new());
    let sst_mock = Arc::new(MockCloudBackend::new());
    let control_mock = Arc::new(MockCloudBackend::new());
    let wal_cloud = Arc::new(CloudStorage::new(wal_mock.clone(), String::new()));
    let sst_cloud = Arc::new(CloudStorage::new(sst_mock.clone(), String::new()));
    let control_cloud = Arc::new(CloudStorage::new(control_mock.clone(), String::new()));
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_class_stores_policy_event_sender_and_limits(
            local,
            wal_cloud,
            sst_cloud,
            control_cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            HybridQueueLimits::default(),
        ),
    ));
    let wal_path = tmp.path().join("segment.wal");
    std::fs::write(&wal_path, valid_wal_bytes(17)).expect("write WAL segment");

    // Act
    storage
        .enqueue_wal_segment(17, &wal_path, 17)
        .expect("enqueue WAL segment");
    storage.process_uploads();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline && wal_mock.get_uploads().is_empty() {
        std::thread::yield_now();
        storage.process_uploads();
    }
    storage
        .write_sst_object("000017.sst", valid_sst_bytes(b"key", b"value", 17))
        .expect("publish SST");
    storage
        .compare_exchange_remote_object(
            crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY,
            None,
            br#"{"epoch":17}"#.to_vec(),
        )
        .expect("publish control object");

    // Assert
    let wal_size = std::fs::metadata(&wal_path)
        .expect("read WAL segment metadata")
        .len();
    assert_eq!(
        wal_mock.get_uploads(),
        vec![(crate::wal::cloud_segment::object_key(17, 1), wal_size)]
    );
    assert_eq!(
        sst_mock
            .get_uploads()
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        vec![crate::cloud_layout::object_key("000017.sst")]
    );
    assert_eq!(
        control_mock.get_uploads(),
        vec![(
            crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY.to_string(),
            br#"{"epoch":17}"#.len() as u64,
        )]
    );
}

fn valid_sst_bytes(key: &[u8], value: &[u8], seq: u64) -> Vec<u8> {
    let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
    let mut writer = factory.create().expect("create SST writer");
    writer
        .add_with_meta(key, Some(value), seq, EntryType::Put, None)
        .expect("add SST entry");
    writer.finish_bytes().expect("finish SST bytes")
}

fn valid_wal_bytes(seq: u64) -> Vec<u8> {
    let record = crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        Bytes::from_static(b"k"),
        Some(Bytes::from_static(b"v")),
        seq,
        1,
    );
    let payload = crate::wal::encoding::encode(&record).expect("encode WAL record");
    let mut bytes = Vec::new();
    crate::wal::frame::append_frame(&mut bytes, &payload).expect("append WAL frame");
    bytes
}

fn write_authoritative_cloud_wal(
    storage: &HybridStorage,
    segment_id: u64,
    catalog_max_sequence: u64,
    bytes: Vec<u8>,
) -> String {
    let publication = crate::wal::cloud_catalog::PublishedWalSegment::from_validated_bytes(
        segment_id,
        catalog_max_sequence,
        1,
        &bytes,
    );
    write_cloud_object(storage, &publication.object_key, bytes);
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read test WAL catalog");
    let mut catalog =
        crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
            .expect("decode test WAL catalog");
    catalog
        .publish(2, publication.clone())
        .expect("publish test WAL authority");
    storage
        .compare_exchange_remote_object(
            crate::wal::cloud_catalog::OBJECT_KEY,
            Some(catalog_proof.metadata()),
            catalog.encode().expect("encode test WAL catalog"),
        )
        .expect("update test WAL catalog");
    publication.object_key
}

struct AlwaysFailingWriteBackend {
    write_attempts: Arc<AtomicUsize>,
}

struct PanickingWriteBackend {
    write_attempts: Arc<AtomicUsize>,
}

impl AlwaysFailingWriteBackend {
    fn new(write_attempts: Arc<AtomicUsize>) -> Self {
        Self { write_attempts }
    }
}

impl PanickingWriteBackend {
    fn new(write_attempts: Arc<AtomicUsize>) -> Self {
        Self { write_attempts }
    }
}

impl StorageBackend for PanickingWriteBackend {
    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_delete_to_legacy(self, request, callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_write_to_legacy(self, request, data, callback);
    }

    fn submit_write(&self, _key: &str, _data: Vec<u8>, _callback: StorageCallback) {
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        panic!("injected cloud upload worker panic");
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.submit_write(key, data, callback);
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: StorageOutcome::Ok(()),
        });
    }
}

impl StorageBackend for AlwaysFailingWriteBackend {
    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_delete_to_legacy(self, request, callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_write_to_legacy(self, request, data, callback);
    }

    fn submit_write(&self, key: &str, _data: Vec<u8>, callback: StorageCallback) {
        self.write_attempts.fetch_add(1, Ordering::SeqCst);
        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err("write unavailable".to_string().into()),
        });
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.submit_write(key, data, callback);
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: StorageOutcome::Ok(()),
        });
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result: StorageOutcome::Err("head unavailable".to_string().into()),
        });
    }
}

#[derive(Default)]
struct NeverCompletesBackend {
    callbacks: Mutex<Vec<StorageCallback>>,
}

struct BudgetConsumingSstPublicationBackend {
    first_head_delay: Duration,
    head_calls: AtomicUsize,
    retained_callbacks: Mutex<Vec<StorageCallback>>,
}

impl BudgetConsumingSstPublicationBackend {
    fn new(first_head_delay: Duration) -> Self {
        Self {
            first_head_delay,
            head_calls: AtomicUsize::new(0),
            retained_callbacks: Mutex::new(Vec::new()),
        }
    }

    fn retain_callback(&self, callback: StorageCallback) {
        self.retained_callbacks.lock().push(callback);
    }
}

impl StorageBackend for BudgetConsumingSstPublicationBackend {
    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_delete_to_legacy(self, request, callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_write_to_legacy(self, request, data, callback);
    }

    fn submit_write(&self, _key: &str, _data: Vec<u8>, callback: StorageCallback) {
        self.retain_callback(callback);
    }

    fn submit_write_with_headers(
        &self,
        _key: &str,
        _data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.retain_callback(callback);
    }

    fn submit_delete(&self, _key: &str, callback: StorageCallback) {
        self.retain_callback(callback);
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        if self.head_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let delay = self.first_head_delay;
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(delay);
                let _ = callback.send(StorageEvent::HeadComplete {
                    key,
                    result: StorageOutcome::Err(crate::storage::StorageError::not_found(
                        "delayed miss",
                    )),
                });
            });
        } else {
            self.retain_callback(callback);
        }
    }
}

struct RacingReadDeleteBackend {
    object: Mutex<Option<Vec<u8>>>,
    read_started: Barrier,
    release_read: Barrier,
}

impl RacingReadDeleteBackend {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            object: Mutex::new(Some(bytes)),
            read_started: Barrier::new(2),
            release_read: Barrier::new(2),
        }
    }

    fn metadata(bytes: &[u8]) -> StorageObjectMetadata {
        StorageObjectMetadata {
            size: bytes.len() as u64,
            etag: "race-etag".to_string(),
            generation: None,
        }
    }
}

impl StorageBackend for RacingReadDeleteBackend {
    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_delete_to_legacy(self, request, callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_write_to_legacy(self, request, data, callback);
    }

    fn submit_read_with_metadata(
        &self,
        _key: &str,
        _timeout: Duration,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let snapshot = self.object.lock().clone();
        self.read_started.wait();
        self.release_read.wait();
        let result = snapshot
            .map(|bytes| {
                let metadata = Self::metadata(&bytes);
                (bytes, metadata)
            })
            .ok_or_else(|| crate::storage::StorageError::not_found("object"));
        let _ = callback.send(result);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        *self.object.lock() = Some(data);
        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: StorageOutcome::Ok(()),
        });
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        self.submit_delete_with_headers(key, Vec::new(), callback);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        let expected = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-match"))
            .map(|(_, value)| value.trim_matches('"'));
        let result = if expected.is_none_or(|value| value == "race-etag") {
            self.object.lock().take();
            StorageOutcome::Ok(())
        } else {
            StorageOutcome::Err(crate::storage::StorageError::precondition_failed(""))
        };
        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result,
        });
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        let result = self.object.lock().as_deref().map_or_else(
            || StorageOutcome::Err("object not found".to_string().into()),
            |bytes| StorageOutcome::Ok(Self::metadata(bytes)),
        );
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result,
        });
    }
}

impl NeverCompletesBackend {
    fn retain_callback(&self, callback: StorageCallback) {
        self.callbacks.lock().push(callback);
    }
}

impl StorageBackend for NeverCompletesBackend {
    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_delete_to_legacy(self, request, callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: crate::storage::StorageCallback,
    ) {
        crate::storage::test_support::forward_typed_write_to_legacy(self, request, data, callback);
    }

    fn submit_write(&self, _key: &str, _data: Vec<u8>, callback: StorageCallback) {
        self.retain_callback(callback);
    }

    fn submit_write_with_headers(
        &self,
        _key: &str,
        _data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.retain_callback(callback);
    }

    fn submit_delete(&self, _key: &str, callback: StorageCallback) {
        self.retain_callback(callback);
    }

    fn submit_delete_with_headers(
        &self,
        _key: &str,
        _headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.retain_callback(callback);
    }

    fn submit_head(&self, _key: &str, callback: StorageCallback) {
        self.retain_callback(callback);
    }
}

fn write_cloud_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.sst_store().submit_write(key, data, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::WriteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("cloud write for '{key}' failed: {other:?}"),
    }
}

fn write_local_object(storage: &HybridStorage, key: &str, data: Vec<u8>) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.local_store().submit_write(key, data, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::WriteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("local write for '{key}' failed: {other:?}"),
    }
}

fn read_local_object(storage: &HybridStorage, key: &str) -> Vec<u8> {
    let (tx, rx) = std::sync::mpsc::channel();
    storage
        .local_store()
        .submit_read_with_metadata(key, Duration::from_secs(1), tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(Ok((data, _metadata))) => data,
        other => panic!("local read for '{key}' failed: {other:?}"),
    }
}

fn read_cloud_object(storage: &HybridStorage, key: &str) -> Vec<u8> {
    let (tx, rx) = std::sync::mpsc::channel();
    storage
        .sst_store()
        .submit_read_with_metadata(key, Duration::from_secs(1), tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(Ok((data, _metadata))) => data,
        other => panic!("cloud read for '{key}' failed: {other:?}"),
    }
}

fn delete_cloud_object(storage: &HybridStorage, key: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.sst_store().submit_delete(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::DeleteComplete {
            result: StorageOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("cloud delete for '{key}' failed: {other:?}"),
    }
}

fn write_cloud_metadata_object(cloud: &CloudStorage, key: &str, data: Vec<u8>) {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(key, data, vec![], tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(crate::storage::cloud::CloudEvent::Put {
            result: crate::storage::cloud::CloudOutcome::Ok(()),
            ..
        }) => {}
        other => panic!("cloud metadata write for '{key}' failed: {other:?}"),
    }
}

fn head_cloud_metadata_object(cloud: &CloudStorage, key: &str) -> StorageObjectMetadata {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_head(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(crate::storage::cloud::CloudEvent::Head {
            result: crate::storage::cloud::CloudOutcome::Ok(metadata),
            ..
        }) => StorageObjectMetadata {
            size: metadata.size,
            etag: metadata.etag,
            generation: metadata.generation,
        },
        other => panic!("cloud metadata head for '{key}' failed: {other:?}"),
    }
}

fn assert_cloud_object_exists(storage: &HybridStorage, key: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.sst_store().submit_head(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::HeadComplete {
            result: StorageOutcome::Ok(_),
            ..
        }) => {}
        other => panic!("expected cloud object '{key}' to exist, got {other:?}"),
    }
}

fn assert_cloud_object_missing(storage: &HybridStorage, key: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    storage.sst_store().submit_head(key, tx);
    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(StorageEvent::HeadComplete {
            result: StorageOutcome::Err(_),
            ..
        }) => {}
        other => panic!("expected cloud object '{key}' to be missing, got {other:?}"),
    }
}

fn wait_for_wal_prune_result(storage: &HybridStorage, segment_id: u64) -> StorageOutcome<()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        for event in storage.process_uploads() {
            if let StorageEvent::CloudWalPruneComplete {
                segment_id: event_segment,
                result,
            } = event
            {
                if event_segment == segment_id {
                    return result;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for WAL prune result for segment {segment_id}");
}

fn manifest_for_ssts(files: &[(&str, u64)]) -> crate::metadata::Manifest {
    let files_with_crc: Vec<_> = files
        .iter()
        .map(|(name, size_bytes)| (*name, *size_bytes, None))
        .collect();
    manifest_for_ssts_with_crc(&files_with_crc)
}

fn manifest_for_ssts_with_crc(files: &[(&str, u64, Option<u32>)]) -> crate::metadata::Manifest {
    crate::metadata::Manifest {
        files: files
            .iter()
            .map(
                |(name, size_bytes, content_crc32c)| crate::metadata::FileMeta {
                    name: (*name).to_string(),
                    level: 0,
                    size_bytes: *size_bytes,
                    content_crc32c: *content_crc32c,
                    cf_id: 0,
                    smallest_key: Some(b"a".to_vec()),
                    largest_key: Some(b"a".to_vec()),
                    smallest_seq: Some(1),
                    largest_seq: Some(1),
                    ..Default::default()
                },
            )
            .collect(),
        ..Default::default()
    }
}

fn manifest_covering_wal(
    sst_name: &str,
    sst_bytes: &[u8],
    sequence: u64,
    content_crc32c: Option<u32>,
) -> crate::metadata::Manifest {
    crate::metadata::Manifest {
        files: vec![crate::metadata::FileMeta {
            name: sst_name.to_string(),
            level: 0,
            size_bytes: sst_bytes.len() as u64,
            content_crc32c,
            cf_id: 0,
            smallest_key: Some(b"k".to_vec()),
            largest_key: Some(b"k".to_vec()),
            smallest_seq: Some(sequence),
            largest_seq: Some(sequence),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn should_revalidate_manifest_ssts_on_repeated_validation() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "cached.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    write_cloud_object(
        &storage,
        &crate::cloud_layout::object_key(sst_name),
        bytes.clone(),
    );
    let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

    mock_cloud.clear_history();
    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("first manifest validation");
    let first_downloads = mock_cloud.get_downloads();
    // Act
    // Assert
    assert!(
        first_downloads
            .iter()
            .any(|key| key.ends_with("sst/cached.sst")),
        "first validation should read the cloud SST, got {first_downloads:?}"
    );

    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("second manifest validation");

    let downloads = mock_cloud.get_downloads();
    assert_eq!(
        downloads
            .iter()
            .filter(|key| key.ends_with("sst/cached.sst"))
            .count(),
        2,
        "authoritative validation must reread the immutable SST: {downloads:?}"
    );
}

#[test]
fn should_revalidate_full_manifest_after_extension() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let first_name = "first.sst";
    let second_name = "second.sst";
    let first_bytes = valid_sst_bytes(b"a", b"v1", 1);
    let second_bytes = valid_sst_bytes(b"b", b"v2", 2);
    write_cloud_object(
        &storage,
        &crate::cloud_layout::object_key(first_name),
        first_bytes.clone(),
    );
    write_cloud_object(
        &storage,
        &crate::cloud_layout::object_key(second_name),
        second_bytes.clone(),
    );

    let first_manifest = manifest_for_ssts(&[(first_name, first_bytes.len() as u64)]);
    storage
        .verify_manifest_cloud_objects(&first_manifest)
        .expect("first manifest validation");

    mock_cloud.clear_history();
    let extended_manifest = crate::metadata::Manifest {
        files: vec![
            crate::metadata::FileMeta {
                name: first_name.to_string(),
                level: 0,
                size_bytes: first_bytes.len() as u64,
                cf_id: 0,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"a".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
                ..Default::default()
            },
            crate::metadata::FileMeta {
                name: second_name.to_string(),
                level: 0,
                size_bytes: second_bytes.len() as u64,
                cf_id: 0,
                smallest_key: Some(b"b".to_vec()),
                largest_key: Some(b"b".to_vec()),
                smallest_seq: Some(2),
                largest_seq: Some(2),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    storage
        .verify_manifest_cloud_objects(&extended_manifest)
        .expect("extended manifest validation");
    let downloads = mock_cloud.get_downloads();

    // Act
    // Assert
    assert!(
        downloads.iter().any(|key| key.ends_with("sst/second.sst")),
        "extended validation should read the new SST, got {downloads:?}"
    );
    assert!(
        downloads.iter().any(|key| key.ends_with("sst/first.sst")),
        "authoritative validation should reread the existing SST, got {downloads:?}"
    );
}

#[test]
fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_deleted() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "deleted-after-proof.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::cloud_layout::object_key(sst_name);
    write_cloud_object(&storage, &key, bytes.clone());
    let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest validation");
    delete_cloud_object(&storage, &key);

    let error = storage
        .verify_manifest_cloud_objects(&manifest)
        .expect_err("deleted SST must invalidate cached proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation") || error.contains("unreadable"),
        "unexpected stale SST proof error: {error}"
    );
}

#[test]
fn should_reject_cached_manifest_sst_proof_when_cloud_object_is_overwritten() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "overwritten-after-proof.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::cloud_layout::object_key(sst_name);
    write_cloud_object(&storage, &key, bytes.clone());
    let manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);

    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest validation");
    write_cloud_object(&storage, &key, b"not a valid sst".to_vec());

    let error = storage
        .verify_manifest_cloud_objects(&manifest)
        .expect_err("overwritten SST must invalidate cached proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation") || error.contains("size mismatch"),
        "unexpected stale SST proof error: {error}"
    );
}

#[test]
fn should_reject_manifest_sst_when_content_crc_differs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "wrong-crc.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::cloud_layout::object_key(sst_name);
    let wrong_crc = crc32c::crc32c(&bytes) ^ 0xffff_ffff;
    write_cloud_object(&storage, &key, bytes.clone());
    let manifest = manifest_for_ssts_with_crc(&[(sst_name, bytes.len() as u64, Some(wrong_crc))]);

    let error = storage
        .verify_manifest_cloud_objects(&manifest)
        .expect_err("manifest SST with mismatched content CRC must not validate");

    // Act
    // Assert
    assert!(
        error.contains("crc") || error.contains("content"),
        "unexpected manifest SST CRC validation error: {error}"
    );
}

#[test]
fn should_not_reuse_size_only_sst_proof_when_manifest_later_requires_crc() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "crc-after-size-proof.sst";
    let bytes = valid_sst_bytes(b"a", b"v1", 1);
    let key = crate::cloud_layout::object_key(sst_name);
    let wrong_crc = crc32c::crc32c(&bytes) ^ 0xffff_ffff;
    write_cloud_object(&storage, &key, bytes.clone());
    let size_only_manifest = manifest_for_ssts(&[(sst_name, bytes.len() as u64)]);
    storage
        .verify_manifest_cloud_objects(&size_only_manifest)
        .expect("initial size-only manifest validation");
    let crc_manifest =
        manifest_for_ssts_with_crc(&[(sst_name, bytes.len() as u64, Some(wrong_crc))]);

    let error = storage
        .verify_manifest_cloud_objects(&crc_manifest)
        .expect_err("cached size-only SST proof must not satisfy later CRC requirement");

    // Act
    // Assert
    assert!(
        error.contains("crc") || error.contains("content"),
        "unexpected cached SST CRC validation error: {error}"
    );
}

#[test]
fn should_not_overwrite_remote_object_given_different_content_when_authoritative_upload_runs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "collision.sst";
    let key = crate::cloud_layout::object_key(sst_name);
    let existing_bytes = valid_sst_bytes(b"a", b"already-committed", 1);
    let upload_bytes = valid_sst_bytes(b"b", b"new-upload", 2);
    write_cloud_object(&storage, &key, existing_bytes.clone());

    let error = storage
        .write_sst_object(sst_name, upload_bytes)
        .expect_err("existing remote SST object must fail authoritative upload");

    // Act
    // Assert
    assert!(
        error.to_string().contains("cloud immutable upload failed")
            || error.to_string().contains("precondition failed"),
        "unexpected SST collision error: {error}"
    );
    assert_eq!(
        read_cloud_object(&storage, &key),
        existing_bytes,
        "authoritative SST upload must not overwrite an existing remote object"
    );
    let local_entry = HybridStorage::object_exists_in_backend_within(
        &storage.local_store(),
        &key,
        storage.callback_timeout(),
        &crate::common::OperationDeadline::unbounded(),
    )
    .expect("local cache existence check");
    assert!(
        !local_entry,
        "failed authoritative SST upload must not leave a local cache entry"
    );
}

#[test]
fn should_not_create_remote_sst_when_local_cache_key_already_exists() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "local-collision.sst";
    let key = crate::cloud_layout::object_key(sst_name);
    let existing_bytes = valid_sst_bytes(b"a", b"local-already-committed", 1);
    let upload_bytes = valid_sst_bytes(b"b", b"new-upload", 2);
    write_local_object(&storage, &key, existing_bytes.clone());

    let error = storage
        .write_sst_object(sst_name, upload_bytes)
        .expect_err("existing local SST cache object must fail authoritative upload");

    // Act
    // Assert
    assert!(
        error.to_string().contains("local cache already exists"),
        "unexpected local SST collision error: {error}"
    );
    assert_eq!(
        read_local_object(&storage, &key),
        existing_bytes,
        "authoritative SST upload must not overwrite an existing local cache object"
    );
    assert_cloud_object_missing(&storage, &key);
}

#[test]
fn should_resume_same_content_sst_publication_after_remote_only_success() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "remote-only-retry.sst";
    let key = crate::cloud_layout::object_key(sst_name);
    let bytes = valid_sst_bytes(b"retry", b"value", 7);
    write_cloud_object(&storage, &key, bytes.clone());
    assert_cloud_object_exists(&storage, &key);

    // Act
    storage
        .write_sst_object(sst_name, bytes.clone())
        .expect("same-content retry should finish local cache publication");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
    assert_eq!(read_local_object(&storage, &key), bytes);
}

#[test]
fn should_retry_same_content_upload_idempotently_given_remote_object_already_exists() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let sst_name = "fully-published-retry.sst";
    let key = crate::cloud_layout::object_key(sst_name);
    let bytes = valid_sst_bytes(b"retry", b"value", 8);
    storage
        .write_sst_object(sst_name, bytes.clone())
        .expect("publish both copies");

    // Act
    storage
        .write_sst_object(sst_name, bytes.clone())
        .expect("same-content retry must be idempotent");

    // Assert
    assert_eq!(read_cloud_object(&storage, &key), bytes);
    assert_eq!(read_local_object(&storage, &key), bytes);
}

#[test]
fn should_reread_remote_wal_given_repeated_authoritative_validation() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 7;
    let max_sequence = 11;
    let _key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );

    mock_cloud.clear_history();
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("first remote WAL validation");
    let first_downloads = mock_cloud.get_downloads();
    // Act
    // Assert
    assert!(
        first_downloads
            .iter()
            .any(|key| key.ends_with("wal/epochs/00000000000000000001/00000000000000000007.wal")),
        "first validation should read the cloud WAL, got {first_downloads:?}"
    );

    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("second remote WAL validation");
    let downloads = mock_cloud.get_downloads();
    assert_eq!(
        downloads
            .iter()
            .filter(|key| key.ends_with("wal/epochs/00000000000000000001/00000000000000000007.wal"))
            .count(),
        2,
        "authoritative WAL validation must reread the segment: {downloads:?}"
    );
}

#[test]
fn should_reject_cached_remote_wal_proof_when_cloud_object_is_deleted() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 8;
    let max_sequence = 12;
    let key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );

    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    delete_cloud_object(&storage, &key);

    let error = storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect_err("deleted WAL must invalidate cached proof");
    // Act
    // Assert
    assert!(
        error.contains("changed since validation") || error.contains("unreadable"),
        "unexpected stale WAL proof error: {error}"
    );
}

#[test]
fn should_reject_stale_remote_wal_target_identity_when_guarded_delete_runs() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 11;
    let max_sequence = 21;
    let key = crate::wal::cloud_segment::object_key(segment_id, 1);
    write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));
    let stale_proof = storage
        .remote_object_proof(&key)
        .expect("initial remote object proof");

    write_cloud_object(&storage, &key, valid_wal_bytes(max_sequence));
    storage
        .delete_remote_object_guarded(segment_id, stale_proof)
        .expect("schedule prune");

    let result = wait_for_wal_prune_result(&storage, segment_id);
    // Act
    // Assert
    assert!(
        result.is_err(),
        "stale WAL proof must make remote prune fail conservatively"
    );
    assert_cloud_object_exists(&storage, &key);
}

#[test]
fn should_reject_reader_proof_when_guarded_prune_deletes_wal_during_download() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create guarded-prune race test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let segment_id = 12;
    let key = crate::wal::cloud_segment::object_key(segment_id, 1);
    let bytes = valid_wal_bytes(22);
    let backend = Arc::new(RacingReadDeleteBackend::new(bytes.clone()));
    let cloud: Arc<dyn StorageBackend> = backend.clone();
    let storage = Arc::new(CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    ))));
    let target = RemoteObjectProof::exact(
        key.clone(),
        bytes.clone(),
        RacingReadDeleteBackend::metadata(&bytes),
    );
    let reader_storage = Arc::clone(&storage);
    let reader_key = key.clone();
    let reader = std::thread::spawn(move || reader_storage.remote_object_proof(&reader_key));
    backend.read_started.wait();

    // Act
    storage
        .delete_remote_object_guarded(segment_id, target)
        .expect("schedule guarded WAL prune");
    let prune_result = wait_for_wal_prune_result(&storage, segment_id);
    backend.release_read.wait();
    let reader_result = reader.join().expect("join concurrent WAL reader");

    // Assert
    assert!(
        prune_result.is_ok(),
        "guarded prune failed: {prune_result:?}"
    );
    let reader_error = reader_result.expect_err("racing read must not return a stale proof");
    assert!(
        reader_error.to_string().contains("unreadable"),
        "unexpected racing reader error: {reader_error}"
    );
    assert_cloud_object_missing(&storage, &key);
}

#[test]
fn should_not_prune_remote_wal_when_manifest_sst_disappears_after_initial_validation() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 13;
    let max_sequence = 23;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "missing-after-validation.sst";
    let sst_key = crate::cloud_layout::object_key(sst_name);
    let sst_bytes = valid_sst_bytes(b"k", b"v", max_sequence);
    let manifest = manifest_covering_wal(sst_name, &sst_bytes, max_sequence, None);

    write_cloud_object(&storage, &sst_key, sst_bytes);
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest SST validation");

    delete_cloud_object(&storage, &sst_key);
    let error = storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest.clone(), None),
            2,
        )
        .expect_err("missing manifest SST must reject prune");

    // Act
    // Assert
    assert!(
        error.contains("unreadable") || error.contains("not found"),
        "unexpected missing manifest SST error: {error}"
    );
    assert_cloud_object_exists(&storage, &wal_key);
}

#[test]
fn should_not_prune_remote_wal_given_manifest_validation_failure_when_gc_runs() {
    assert_wal_prune_rejects_manifest_crc_mismatch(false);
}

#[test]
fn should_keep_crc_proof_before_wal_retirement_when_reading_remote_sst_ranges() {
    assert_wal_prune_rejects_manifest_crc_mismatch(true);
}

fn assert_wal_prune_rejects_manifest_crc_mismatch(ephemeral: bool) {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    if ephemeral {
        storage.enable_ephemeral_sst_cache(1024 * 1024);
    }
    let segment_id = 16;
    let max_sequence = 26;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "wrong-crc-prune-guard.sst";
    let sst_key = crate::cloud_layout::object_key(sst_name);
    let sst_bytes = valid_sst_bytes(b"k", b"v", max_sequence);
    let wrong_crc = crc32c::crc32c(&sst_bytes) ^ 0xffff_ffff;
    let manifest = manifest_covering_wal(sst_name, &sst_bytes, max_sequence, Some(wrong_crc));

    write_cloud_object(&storage, &sst_key, sst_bytes);
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    let error = storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest, None),
            2,
        )
        .expect_err("incorrect manifest CRC must reject prune");

    // Act
    // Assert
    assert!(
        error.contains("crc32c"),
        "unexpected manifest CRC error: {error}"
    );
    assert_cloud_object_exists(&storage, &wal_key);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_not_prune_remote_wal_given_remote_sst_identity_change_when_gc_runs() {
    assert_sst_identity_change_retains_wal(false);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_preserve_wal_authority_when_streamed_sst_proof_identity_changes() {
    assert_sst_identity_change_retains_wal(true);
}

#[cfg(feature = "failpoints")]
fn assert_sst_identity_change_retains_wal(ephemeral: bool) {
    // Arrange
    let _test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    if ephemeral {
        storage.enable_ephemeral_sst_cache(1024 * 1024);
    }
    let segment_id = 17;
    let max_sequence = 27;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "changed-after-validation.sst";
    let sst_key = crate::cloud_layout::object_key(sst_name);
    let original = valid_sst_bytes(b"k", b"v", max_sequence);
    let replacement = valid_sst_bytes(b"k", b"v2", max_sequence);
    let manifest = manifest_covering_wal(
        sst_name,
        &original,
        max_sequence,
        Some(crc32c::crc32c(&original)),
    );
    write_cloud_object(&storage, &sst_key, original);
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    let callback_backend = Arc::clone(&mock_cloud);
    let callback_key = format!("hybrid-test/{sst_key}");
    let callback_replacement = replacement.clone();
    fail::cfg_callback(
        "midge::cloud::after_wal_prune_dependency_validation",
        move || {
            let (tx, rx) = std::sync::mpsc::channel();
            CloudBackend::submit_put(
                callback_backend.as_ref(),
                &callback_key,
                callback_replacement.clone(),
                Vec::new(),
                tx,
            );
            assert!(matches!(
                rx.recv_timeout(Duration::from_secs(1)),
                Ok(crate::storage::cloud::CloudEvent::Put {
                    result: crate::storage::cloud::CloudOutcome::Ok(()),
                    ..
                })
            ));
        },
    )
    .expect("configure SST identity replacement boundary");

    // Act
    let error = storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest, None),
            2,
        )
        .expect_err("changed SST identity must reject WAL authority retirement");
    fail::remove("midge::cloud::after_wal_prune_dependency_validation");
    scenario.teardown();

    // Assert
    assert!(
        error.contains("identity changed before conditional delete"),
        "unexpected changed SST proof error: {error}"
    );
    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read retained WAL catalog");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode retained WAL catalog");
    assert!(catalog.segments.contains_key(&segment_id));
    assert_cloud_object_exists(&storage, &wal_key);
    assert_eq!(read_cloud_object(&storage, &sst_key), replacement);
}

#[test]
fn should_not_prune_remote_wal_when_cloud_metadata_changes_after_initial_validation() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 14;
    let max_sequence = 24;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "metadata-guard.sst";
    let sst_key = crate::cloud_layout::object_key(sst_name);
    let sst_bytes = valid_sst_bytes(b"k", b"v", max_sequence);
    let manifest = manifest_covering_wal(
        sst_name,
        &sst_bytes,
        max_sequence,
        Some(crc32c::crc32c(&sst_bytes)),
    );
    let metadata_backend = Arc::new(MockCloudBackend::new());
    let metadata_cloud = Arc::new(CloudStorage::new(
        metadata_backend,
        "metadata-test".to_string(),
    ));
    let metadata_key = crate::cloud_layout::CloudObjectLayout::metadata_key("manifest.json");
    let metadata_bytes = br#"{"last_persisted_sequence":24}"#.to_vec();

    write_cloud_object(&storage, &sst_key, sst_bytes);
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    write_cloud_metadata_object(&metadata_cloud, &metadata_key, metadata_bytes.clone());
    let metadata_guard = CloudMetadataPruneGuard::new(
        metadata_cloud.clone(),
        vec![CloudMetadataPruneProof {
            key: metadata_key.clone(),
            expected_bytes: metadata_bytes,
            remote: head_cloud_metadata_object(&metadata_cloud, &metadata_key),
        }],
    );

    write_cloud_metadata_object(&metadata_cloud, &metadata_key, b"changed".to_vec());
    let error = storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest, Some(metadata_guard)),
            2,
        )
        .expect_err("changed metadata proof must reject authority retirement");

    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read catalog after prune scheduling");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode catalog after prune scheduling");
    assert!(
        catalog.segments.contains_key(&segment_id),
        "catalog authority must remain when a coverage dependency changed"
    );
    // Act
    // Assert
    assert!(
        error.contains("changed before conditional delete")
            || error.contains("identity changed before conditional delete"),
        "unexpected changed metadata proof error: {error}"
    );
    assert_cloud_object_exists(&storage, &wal_key);
}

#[test]
fn should_prune_remote_wal_when_worker_side_guard_remains_valid() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 15;
    let max_sequence = 25;
    let wal_key = write_authoritative_cloud_wal(
        &storage,
        segment_id,
        max_sequence,
        valid_wal_bytes(max_sequence),
    );
    let sst_name = "guard-valid.sst";
    let sst_key = crate::cloud_layout::object_key(sst_name);
    let sst_bytes = valid_sst_bytes(b"k", b"v", max_sequence);
    let manifest = crate::metadata::Manifest {
        files: vec![crate::metadata::FileMeta {
            name: sst_name.to_string(),
            level: 0,
            size_bytes: sst_bytes.len() as u64,
            content_crc32c: Some(crc32c::crc32c(&sst_bytes)),
            cf_id: 0,
            smallest_key: Some(b"k".to_vec()),
            largest_key: Some(b"k".to_vec()),
            smallest_seq: Some(max_sequence),
            largest_seq: Some(max_sequence),
            ..Default::default()
        }],
        ..Default::default()
    };
    let metadata_backend = Arc::new(MockCloudBackend::new());
    let metadata_cloud = Arc::new(CloudStorage::new(
        metadata_backend,
        "metadata-test".to_string(),
    ));
    let metadata_key = crate::cloud_layout::CloudObjectLayout::metadata_key("manifest.json");
    let metadata_bytes = br#"{"last_persisted_sequence":25}"#.to_vec();

    write_cloud_object(&storage, &sst_key, sst_bytes.clone());
    storage
        .verify_remote_wal_segment(segment_id, max_sequence)
        .expect("initial remote WAL validation");
    storage
        .verify_manifest_cloud_objects(&manifest)
        .expect("initial manifest SST validation");
    write_cloud_metadata_object(&metadata_cloud, &metadata_key, metadata_bytes.clone());
    let metadata_guard = CloudMetadataPruneGuard::new(
        metadata_cloud.clone(),
        vec![CloudMetadataPruneProof {
            key: metadata_key.clone(),
            expected_bytes: metadata_bytes,
            remote: head_cloud_metadata_object(&metadata_cloud, &metadata_key),
        }],
    );

    storage
        .prune_cloud_wal_segment(
            segment_id,
            max_sequence,
            CloudWalPruneGuard::new(manifest, Some(metadata_guard)),
            2,
        )
        .expect("schedule prune");

    let catalog_proof = storage
        .remote_object_proof(crate::wal::cloud_catalog::OBJECT_KEY)
        .expect("read catalog after prune scheduling");
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(catalog_proof.bytes())
        .expect("decode catalog after prune scheduling");
    assert!(
        !catalog.segments.contains_key(&segment_id),
        "catalog authority must retire before physical delete completion"
    );
    assert_wal_catalog_copies_match(&storage);

    let result = wait_for_wal_prune_result(&storage, segment_id);
    // Act
    // Assert
    assert!(
        result.is_ok(),
        "valid worker-side guard should allow conditional remote WAL deletion"
    );
    assert_cloud_object_missing(&storage, &wal_key);
}

#[test]
fn should_reject_remote_wal_segment_with_sequence_beyond_expected_max() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let segment_id = 10;
    let expected_max_sequence = 20;
    write_authoritative_cloud_wal(
        &storage,
        segment_id,
        expected_max_sequence,
        valid_wal_bytes(expected_max_sequence + 1),
    );

    let error = storage
        .verify_remote_wal_segment(segment_id, expected_max_sequence)
        .expect_err("WAL segment with records beyond expected max must be rejected");
    // Act
    // Assert
    assert!(
        error.contains("exceeds expected"),
        "unexpected WAL max-sequence error: {error}"
    );
}

#[test]
fn should_not_overwrite_existing_remote_wal_during_upload() {
    // Arrange
    let (_mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create WAL dir");
    let segment_id = 12;
    let upload_max_sequence = 22;
    let key = crate::wal::cloud_segment::object_key(segment_id, 1);
    let existing_bytes = valid_wal_bytes(upload_max_sequence + 100);
    write_cloud_object(&storage, &key, existing_bytes.clone());

    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(upload_max_sequence)).expect("write local WAL");
    storage
        .enqueue_wal_segment(segment_id, &wal_path, upload_max_sequence)
        .expect("enqueue WAL upload");
    storage.process_uploads();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut saw_fail = false;
    let mut saw_ack = false;
    while Instant::now() < deadline {
        for event in storage.process_uploads() {
            match event {
                StorageEvent::CloudFail {
                    segment_id: failed_segment,
                    ..
                } if failed_segment == segment_id => saw_fail = true,
                StorageEvent::CloudAck {
                    segment_id: acked_segment,
                    ..
                } if acked_segment == segment_id => saw_ack = true,
                _ => {}
            }
        }
        if saw_fail || saw_ack {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Act
    // Assert
    assert!(saw_fail, "existing remote WAL object must fail upload");
    assert!(
        !saw_ack,
        "existing remote WAL object must not produce a CloudAck"
    );
    assert_eq!(
        read_cloud_object(&storage, &key),
        existing_bytes,
        "upload must not overwrite an existing remote WAL object"
    );
}

#[test]
fn should_readback_remote_wal_before_upload_worker_emits_ack() {
    // Arrange
    let (mock_cloud, storage) = hybrid_with_mock_cloud();
    let tmp = tempfile::tempdir().expect("create WAL dir");
    let segment_id = 9;
    let max_sequence = 13;
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(max_sequence)).expect("write local WAL");

    mock_cloud.clear_history();
    storage
        .enqueue_wal_segment(segment_id, &wal_path, max_sequence)
        .expect("enqueue WAL upload");
    storage.process_uploads();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        events.extend(storage.process_uploads());
        if events
            .iter()
            .any(|event| matches!(event, StorageEvent::CloudAck { .. }))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Act
    // Assert
    assert!(
        events.iter().any(|event| matches!(
            event,
            StorageEvent::CloudAck {
                segment_id: 9,
                max_sequence: 13
            }
        )),
        "worker should emit CloudAck after upload and readback, got {events:?}"
    );
    assert!(
        mock_cloud
            .get_downloads()
            .iter()
            .any(|key| key.ends_with("wal/epochs/00000000000000000001/00000000000000000009.wal")),
        "upload worker must read back the remote WAL before ack"
    );
}

#[test]
fn should_stop_retrying_failed_wal_upload_after_retry_budget_exhausted() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create WAL retry test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let cloud = Arc::new(AlwaysFailingWriteBackend::new(Arc::clone(&write_attempts)));
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let segment_id = 11;
    let max_sequence = 21;
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(max_sequence)).expect("write local WAL");

    storage
        .enqueue_wal_segment(segment_id, &wal_path, max_sequence)
        .expect("enqueue WAL upload");

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut observed_terminal_failures = Vec::new();
    while Instant::now() < deadline {
        let events = storage.process_uploads();
        observed_terminal_failures.extend(events.into_iter().filter_map(|event| match event {
            StorageEvent::CloudFail {
                terminal,
                failure_kind,
                ..
            } => Some((terminal, failure_kind)),
            _ => None,
        }));
        if storage.pending_upload_count() == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Act
    // Assert
    assert_eq!(
        write_attempts.load(Ordering::SeqCst),
        3,
        "permanently failing WAL uploads should stop after the retry budget"
    );
    assert_eq!(
        observed_terminal_failures,
        vec![
            (false, crate::storage::CloudUploadFailureKind::Other),
            (false, crate::storage::CloudUploadFailureKind::Other),
            (true, crate::storage::CloudUploadFailureKind::Other),
        ],
        "only the failure that exhausts storage retry ownership may be terminal"
    );
    assert_eq!(
        storage.pending_upload_count(),
        0,
        "exhausted WAL uploads must leave the queue so cleanup and shutdown are bounded"
    );
}

#[test]
fn should_transfer_wal_retry_ownership_when_upload_worker_panics() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create WAL worker panic test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let cloud = Arc::new(PanickingWriteBackend::new(Arc::clone(&write_attempts)));
    let storage = CloudPersistence::new(Arc::new(HybridStorage::with_policy(
        local,
        cloud,
        crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
    )));
    let segment_id = 12;
    let max_sequence = 22;
    let wal_path = tmp.path().join(crate::wal::segment_file_name(segment_id));
    std::fs::write(&wal_path, valid_wal_bytes(max_sequence)).expect("write local WAL");
    storage
        .enqueue_wal_segment(segment_id, &wal_path, max_sequence)
        .expect("enqueue WAL upload");

    // Act
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut terminal_failure = false;
    while Instant::now() < deadline {
        terminal_failure |= storage.process_uploads().into_iter().any(|event| {
            matches!(
                event,
                StorageEvent::CloudFail {
                    segment_id: failed_segment,
                    terminal: true,
                    ..
                } if failed_segment == segment_id
            )
        });
        if storage.pending_upload_count() == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    assert_eq!(write_attempts.load(Ordering::SeqCst), 3);
    assert!(
        terminal_failure,
        "worker panic must transfer retry ownership"
    );
    assert_eq!(storage.pending_upload_count(), 0);
}

#[test]
fn should_reject_wal_upload_when_entry_or_byte_capacity_is_exhausted() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create bounded upload test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let cloud = Arc::new(NeverCompletesBackend::default());
    let first_path = tmp.path().join(crate::wal::segment_file_name(1));
    let second_path = tmp.path().join(crate::wal::segment_file_name(2));
    let wal_bytes = valid_wal_bytes(1);
    std::fs::write(&first_path, &wal_bytes).expect("write first WAL");
    std::fs::write(&second_path, &wal_bytes).expect("write second WAL");
    let limits = HybridQueueLimits {
        upload_entries: 1,
        upload_bytes: wal_bytes.len() as u64,
        callback_timeout: Duration::from_millis(20),
        ..HybridQueueLimits::default()
    };
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        ),
    ));
    storage
        .enqueue_wal_segment(1, &first_path, 1)
        .expect("first upload must fit");
    assert!(matches!(
        storage.ensure_wal_write_admission(),
        Err(crate::common::MidgeError::WriteStall(_))
    ));

    // Act
    let error = storage
        .enqueue_wal_segment(2, &second_path, 1)
        .expect_err("second upload must be rejected at capacity");

    // Assert
    assert!(matches!(error, crate::common::MidgeError::WriteStall(_)));
    assert!(matches!(
        storage.ensure_wal_write_admission(),
        Err(crate::common::MidgeError::WriteStall(_))
    ));
    assert_eq!(storage.pending_upload_count(), 1);
    assert_eq!(storage.pending_upload_bytes(), wal_bytes.len() as u64);
}

#[test]
fn should_restore_wal_admission_after_upload_capacity_drains() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create admission release test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let mock_cloud = Arc::new(MockCloudBackend::new());
    let cloud = Arc::new(CloudStorage::new(
        mock_cloud,
        "bounded-admission".to_string(),
    ));
    let limits = HybridQueueLimits {
        upload_entries: 1,
        ..HybridQueueLimits::default()
    };
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        ),
    ));
    let first_path = tmp.path().join(crate::wal::segment_file_name(21));
    let second_path = tmp.path().join(crate::wal::segment_file_name(22));
    std::fs::write(&first_path, valid_wal_bytes(21)).expect("write first WAL");
    std::fs::write(&second_path, valid_wal_bytes(22)).expect("write second WAL");
    storage
        .enqueue_wal_segment(21, &first_path, 21)
        .expect("enqueue first upload");
    let _ = storage
        .enqueue_wal_segment(22, &second_path, 22)
        .expect_err("capacity must reject second upload");
    assert!(storage.ensure_wal_write_admission().is_err());

    // Act
    storage.process_uploads();
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline && storage.ensure_wal_write_admission().is_err() {
        storage.process_uploads();
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    storage
        .ensure_wal_write_admission()
        .expect("completed upload must reopen admission");
    assert_eq!(storage.pending_upload_count(), 0);
}

#[test]
fn should_release_storage_reservation_given_upload_callback_is_lost_when_worker_times_out() {
    // Arrange
    let tmp = tempfile::tempdir().expect("create callback timeout test dir");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create local backend"),
    );
    let cloud = Arc::new(NeverCompletesBackend::default());
    let limits = HybridQueueLimits {
        callback_timeout: Duration::from_millis(20),
        upload_entries: 1,
        ..HybridQueueLimits::default()
    };
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        ),
    ));
    let wal_path = tmp.path().join(crate::wal::segment_file_name(3));
    std::fs::write(&wal_path, valid_wal_bytes(3)).expect("write WAL");
    storage
        .enqueue_wal_segment(3, &wal_path, 3)
        .expect("enqueue WAL upload");
    assert!(
        storage.ensure_wal_write_admission().is_err(),
        "the only queue reservation must close write admission while upload is stuck"
    );
    storage.process_uploads();

    // Act
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut timeout_failures = 0;
    while Instant::now() < deadline && storage.pending_upload_count() != 0 {
        timeout_failures += storage
            .process_uploads()
            .into_iter()
            .filter(|event| {
                matches!(
                    event,
                    StorageEvent::CloudFail {
                        error,
                        failure_kind: crate::storage::CloudUploadFailureKind::Timeout,
                        ..
                    } if error.contains("timed out")
                )
            })
            .count();
        if storage.pending_upload_count() == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // Assert
    assert_eq!(timeout_failures, 3, "every bounded retry must time out");
    assert_eq!(storage.pending_upload_count(), 0);
    storage
        .ensure_wal_write_admission()
        .expect("terminal callback timeouts must release the queue reservation");
}

#[test]
fn should_bound_sst_publication_with_shared_deadline_when_remote_preflight_consumes_budget() {
    // Arrange: remote HEAD consumes most of the shared budget, then the
    // conditional PUT never answers. A fresh per-call timeout would let this
    // attempt outlive its advertised deadline by roughly two seconds, leaving
    // a full second of scheduler headroom in the bounded assertion.
    let tmp = tempfile::tempdir().expect("create deadline publication directory");
    let local = Arc::new(
        crate::storage::filesystem::FileSystem::new(tmp.path().join("local"))
            .expect("create deadline publication local backend"),
    );
    let cloud = Arc::new(BudgetConsumingSstPublicationBackend::new(
        Duration::from_millis(300),
    ));
    let limits = HybridQueueLimits {
        callback_timeout: Duration::from_secs(2),
        ..HybridQueueLimits::default()
    };
    let storage = CloudPersistence::new(Arc::new(
        HybridStorage::with_policy_event_sender_and_limits(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
            None,
            limits,
        ),
    ));
    let deadline = crate::common::OperationDeadline::from_budget(Duration::from_millis(500));

    // Act
    let started = Instant::now();
    let result = storage.write_sst_object_within(
        "deadline-publication.sst",
        valid_sst_bytes(b"deadline", b"value", 1),
        &deadline,
    );
    let elapsed = started.elapsed();

    // Assert
    assert!(
        matches!(result, Err(crate::common::MidgeError::Timeout(_))),
        "deadline expiry must remain typed as Timeout: {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "SST publication reused a fresh callback timeout: {elapsed:?}"
    );
}

mod recovery_wal_coverage {
    use super::*;

    fn tombstone_sst(key: &[u8], seq: u64) -> Vec<u8> {
        use crate::sst::SstFactory;

        let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
        let mut writer = factory.create().expect("create test SST writer");
        writer
            .add_with_meta(key, None, seq, crate::types::EntryType::Delete, None)
            .expect("add point tombstone");
        writer.finish_bytes().expect("finish test SST bytes")
    }

    fn record(
        op: crate::wal::WalOpKind,
        value: Option<&'static [u8]>,
        seq: u64,
    ) -> crate::wal::WalRecord {
        crate::wal::WalRecord::new(
            op,
            bytes::Bytes::from_static(b"k"),
            value.map(bytes::Bytes::from_static),
            seq,
            0,
        )
    }

    /// Whether a manifest SST holding one point tombstone at `tombstone_seq`
    /// covers `record` for recovery filtering.
    fn covered(record: &crate::wal::WalRecord, tombstone_seq: u64) -> bool {
        let dir = tempfile::tempdir().expect("create SST directory");
        let name = "000001.sst";
        let bytes = tombstone_sst(&record.key, tombstone_seq);
        std::fs::write(dir.path().join(name), &bytes).expect("write SST");
        let file = crate::metadata::FileMeta {
            name: name.to_string(),
            level: 0,
            size_bytes: bytes.len() as u64,
            content_crc32c: Some(crc32c::crc32c(&bytes)),
            cf_id: 0,
            smallest_key: Some(record.key.to_vec()),
            largest_key: Some(record.key.to_vec()),
            // A flush spanning both sequences, whose newest entry for the
            // key is the tombstone.
            smallest_seq: Some(tombstone_seq.min(record.seq)),
            largest_seq: Some(tombstone_seq.max(record.seq)),
            ..Default::default()
        };
        let mut manifest = crate::metadata::Manifest::default();
        manifest.files.push(file);
        VerifiedManifestWalCoverage::open(
            Arc::new(crate::io::RealFs::new(dir.path()).expect("open SST directory")),
            "",
            &manifest,
            &mut ProvenSstIdentities::default(),
        )
        .covers_wal_record(record)
    }

    #[test]
    fn should_not_treat_a_same_sequence_tombstone_as_covering_a_value_record() {
        // Arrange: a put and a delete cannot both have been written at sequence 7,
        // so skipping the put on the strength of the delete could lose data.
        let put = record(crate::wal::WalOpKind::Put, Some(b"v"), 7);

        // Act
        let is_covered = covered(&put, 7);

        // Assert
        assert!(!is_covered);
    }

    #[test]
    fn should_treat_a_newer_tombstone_as_covering_an_older_value_record() {
        // Arrange
        let put = record(crate::wal::WalOpKind::Put, Some(b"v"), 7);

        // Act
        let is_covered = covered(&put, 9);

        // Assert
        assert!(is_covered);
    }

    #[test]
    fn should_not_treat_an_older_tombstone_as_covering_a_newer_value_record() {
        // Arrange
        let put = record(crate::wal::WalOpKind::Put, Some(b"v"), 7);

        // Act
        let is_covered = covered(&put, 5);

        // Assert
        assert!(!is_covered);
    }

    fn value_sst(key: &[u8], value: &[u8], seq: u64, expiration: Option<u64>) -> Vec<u8> {
        use crate::sst::SstFactory;

        let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
        let mut writer = factory.create().expect("create test SST writer");
        writer
            .add_with_meta(
                key,
                Some(value),
                seq,
                crate::types::EntryType::Put,
                expiration,
            )
            .expect("add value");
        writer.finish_bytes().expect("finish test SST bytes")
    }

    /// Write `bytes` as `name` and describe it as a manifest file for key
    /// `k` at `seq`; `corrupt` stores a checksum the bytes do not match.
    fn manifest_file(
        dir: &std::path::Path,
        name: &str,
        bytes: &[u8],
        seq: u64,
        corrupt: bool,
    ) -> crate::metadata::FileMeta {
        std::fs::write(dir.join(name), bytes).expect("write SST");
        let crc = crc32c::crc32c(bytes);
        crate::metadata::FileMeta {
            name: name.to_string(),
            level: 0,
            size_bytes: bytes.len() as u64,
            content_crc32c: Some(if corrupt { crc ^ 1 } else { crc }),
            cf_id: 0,
            smallest_key: Some(b"k".to_vec()),
            largest_key: Some(b"k".to_vec()),
            smallest_seq: Some(seq),
            largest_seq: Some(seq),
            ..Default::default()
        }
    }

    #[test]
    fn should_replay_wal_value_when_sst_matches_bytes_but_not_expiration() {
        // Arrange: skipping on byte equality alone would drop the WAL
        // record's TTL (#503).
        let dir = tempfile::tempdir().expect("create SST directory");
        let mut manifest = crate::metadata::Manifest::default();
        manifest.files.push(manifest_file(
            dir.path(),
            "000001.sst",
            &value_sst(b"k", b"v", 7, None),
            7,
            false,
        ));
        let mut put = record(crate::wal::WalOpKind::Put, Some(b"v"), 7);
        put.expiration = Some(4_102_444_800_000);

        // Act
        let skipped = VerifiedManifestWalCoverage::open(
            Arc::new(crate::io::RealFs::new(dir.path()).expect("open SST directory")),
            "",
            &manifest,
            &mut ProvenSstIdentities::default(),
        )
        .covers_wal_record(&put);

        // Assert
        assert!(!skipped);
    }

    #[test]
    fn should_replay_wal_value_when_one_covering_sst_is_unreadable_and_another_matches() {
        // Arrange: nothing is known about the unreadable file, so the other
        // file's evidence cannot prove the record is covered (#503).
        let dir = tempfile::tempdir().expect("create SST directory");
        let bytes = value_sst(b"k", b"v", 7, None);
        let mut manifest = crate::metadata::Manifest::default();
        manifest
            .files
            .push(manifest_file(dir.path(), "000001.sst", &bytes, 7, true));
        manifest
            .files
            .push(manifest_file(dir.path(), "000002.sst", &bytes, 7, false));
        let put = record(crate::wal::WalOpKind::Put, Some(b"v"), 7);

        // Act
        let skipped = VerifiedManifestWalCoverage::open(
            Arc::new(crate::io::RealFs::new(dir.path()).expect("open SST directory")),
            "",
            &manifest,
            &mut ProvenSstIdentities::default(),
        )
        .covers_wal_record(&put);

        // Assert
        assert!(!skipped);
    }

    #[test]
    fn should_replay_delete_record_when_sst_holds_the_same_tombstone() {
        // Arrange: an SST's contents do not prove this delete was the one
        // published, and suppressing a delete can resurrect an older value.
        let delete = record(crate::wal::WalOpKind::Delete, None, 7);

        // Act
        let is_covered = covered(&delete, 7);

        // Assert
        assert!(!is_covered);
    }

    #[test]
    fn should_skip_wal_value_when_sst_holds_it_exactly() {
        // Arrange
        let dir = tempfile::tempdir().expect("create SST directory");
        let expiration = Some(4_102_444_800_000);
        let mut manifest = crate::metadata::Manifest::default();
        manifest.files.push(manifest_file(
            dir.path(),
            "000001.sst",
            &value_sst(b"k", b"v", 7, expiration),
            7,
            false,
        ));
        let mut put = record(crate::wal::WalOpKind::Put, Some(b"v"), 7);
        put.expiration = expiration;

        // Act
        let skipped = VerifiedManifestWalCoverage::open(
            Arc::new(crate::io::RealFs::new(dir.path()).expect("open SST directory")),
            "",
            &manifest,
            &mut ProvenSstIdentities::default(),
        )
        .covers_wal_record(&put);

        // Assert
        assert!(skipped);
    }

    #[test]
    fn should_replay_wal_value_when_sst_bounds_cover_it_without_holding_it() {
        // Arrange: a concurrent flush can place unrelated entries on both
        // sides of this write without persisting the write itself.
        let dir = tempfile::tempdir().expect("create SST directory");
        let mut file = manifest_file(
            dir.path(),
            "000001.sst",
            &value_sst(b"a", b"other", 7, None),
            7,
            false,
        );
        file.smallest_key = Some(b"a".to_vec());
        file.largest_key = Some(b"z".to_vec());
        let mut manifest = crate::metadata::Manifest::default();
        manifest.files.push(file);
        let put = record(crate::wal::WalOpKind::Put, Some(b"v"), 7);

        // Act
        let skipped = VerifiedManifestWalCoverage::open(
            Arc::new(crate::io::RealFs::new(dir.path()).expect("open SST directory")),
            "",
            &manifest,
            &mut ProvenSstIdentities::default(),
        )
        .covers_wal_record(&put);

        // Assert
        assert!(!skipped);
    }

    #[test]
    fn should_replay_wal_value_when_its_sequence_is_outside_the_sst_range() {
        // Arrange: the SST holds the key only at sequence 7; the WAL write at
        // 12 is newer than anything the file covers.
        let dir = tempfile::tempdir().expect("create SST directory");
        let mut manifest = crate::metadata::Manifest::default();
        manifest.files.push(manifest_file(
            dir.path(),
            "000001.sst",
            &value_sst(b"k", b"v", 7, None),
            7,
            false,
        ));
        let put = record(crate::wal::WalOpKind::Put, Some(b"v"), 12);

        // Act
        let skipped = VerifiedManifestWalCoverage::open(
            Arc::new(crate::io::RealFs::new(dir.path()).expect("open SST directory")),
            "",
            &manifest,
            &mut ProvenSstIdentities::default(),
        )
        .covers_wal_record(&put);

        // Assert
        assert!(!skipped);
    }

    #[test]
    fn should_replay_transaction_marker_when_sst_holds_its_key() {
        // Arrange
        let dir = tempfile::tempdir().expect("create SST directory");
        let mut manifest = crate::metadata::Manifest::default();
        manifest.files.push(manifest_file(
            dir.path(),
            "000001.sst",
            &value_sst(b"k", b"v", 7, None),
            7,
            false,
        ));
        let marker = record(crate::wal::WalOpKind::TxnBatch, Some(b"v"), 7);

        // Act
        let skipped = VerifiedManifestWalCoverage::open(
            Arc::new(crate::io::RealFs::new(dir.path()).expect("open SST directory")),
            "",
            &manifest,
            &mut ProvenSstIdentities::default(),
        )
        .covers_wal_record(&marker);

        // Assert
        assert!(!skipped);
    }

    /// Counts whole-file reads through the injected filesystem. Persistent
    /// handles are refused so every SST read goes through `open`.
    struct WholeFileReadCountingFs {
        inner: crate::io::RealFs,
        whole_file_reads: Arc<std::sync::atomic::AtomicUsize>,
    }

    struct WholeFileReadCountingFile<'a> {
        inner: Box<dyn crate::io::File + 'a>,
        whole_file_reads: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl crate::io::File for WholeFileReadCountingFile<'_> {
        fn read_at(&self, offset: u64, len: u64) -> crate::io::FsResult<bytes::Bytes> {
            if offset == 0 && len == self.inner.len()? {
                self.whole_file_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.read_at(offset, len)
        }

        fn write_at(&mut self, offset: u64, data: bytes::Bytes) -> crate::io::FsResult<()> {
            self.inner.write_at(offset, data)
        }

        fn append(&mut self, data: bytes::Bytes) -> crate::io::FsResult<u64> {
            self.inner.append(data)
        }

        fn len(&self) -> crate::io::FsResult<u64> {
            self.inner.len()
        }

        fn sync(&mut self, durability: crate::io::Durability) -> crate::io::FsResult<()> {
            self.inner.sync(durability)
        }
    }

    impl crate::io::Fs for WholeFileReadCountingFs {
        fn host_addressing(&self) -> Option<crate::io::HostAddressing<'_>> {
            self.inner.host_addressing()
        }

        fn open(
            &self,
            path: &crate::io::FsPath,
            options: crate::io::OpenOptions,
        ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
            Ok(Box::new(WholeFileReadCountingFile {
                inner: self.inner.open(path, options)?,
                whole_file_reads: Arc::clone(&self.whole_file_reads),
            }))
        }

        fn open_persistent_handle(
            &self,
            _path: &crate::io::FsPath,
            _options: crate::io::OpenOptions,
        ) -> crate::io::FsResult<Box<dyn crate::io::File>> {
            Err(crate::io::FsError::Unsupported(
                "test filesystem counts path-based reads only".to_string(),
            ))
        }

        fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.remove_file(path)
        }

        fn exists(&self, path: &crate::io::FsPath) -> crate::io::FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<crate::io::traits::Metadata> {
            self.inner.metadata(path)
        }

        fn create_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.create_dir_all(path)
        }

        fn list_dir(
            &self,
            path: &crate::io::FsPath,
        ) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
            self.inner.list_dir(path)
        }

        fn remove_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
            self.inner.remove_dir_all(path)
        }

        fn sync_dir(
            &self,
            path: &crate::io::FsPath,
            durability: crate::io::Durability,
        ) -> crate::io::FsResult<()> {
            self.inner.sync_dir(path, durability)
        }

        fn rename_atomic(
            &self,
            from: &crate::io::FsPath,
            to: &crate::io::FsPath,
        ) -> crate::io::FsResult<()> {
            self.inner.rename_atomic(from, to)
        }
    }

    #[test]
    fn should_read_each_covering_sst_at_most_once_per_local_prune_pass() {
        // Arrange: the covering SST lives behind the runtime's injected
        // filesystem, under its `sst/` prefix, not in a directory the prover
        // may read directly.
        let root = tempfile::tempdir().expect("create database root");
        let sst_dir = root.path().join("sst");
        std::fs::create_dir_all(&sst_dir).expect("create SST directory");
        let bytes = value_sst(b"k", b"v", 7, None);
        let mut manifest = crate::metadata::Manifest::default();
        manifest
            .files
            .push(manifest_file(&sst_dir, "000001.sst", &bytes, 7, false));
        let whole_file_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fs: Arc<dyn crate::io::Fs> = Arc::new(WholeFileReadCountingFs {
            inner: crate::io::RealFs::new(root.path()).expect("open database root"),
            whole_file_reads: Arc::clone(&whole_file_reads),
        });
        let put = record(crate::wal::WalOpKind::Put, Some(b"v"), 7);
        let mut proven = ProvenSstIdentities::default();

        // Act: two prune passes, each with its own prover.
        let covered: Vec<bool> = (0..2)
            .map(|_| {
                VerifiedManifestWalCoverage::open(
                    Arc::clone(&fs),
                    crate::cloud_layout::CloudObjectLayout::SST_PREFIX,
                    &manifest,
                    &mut proven,
                )
                .covers_wal_record(&put)
            })
            .collect();

        // Assert: both passes prove coverage through the injected filesystem,
        // and only the first reads the whole SST to check its identity.
        assert_eq!(covered, vec![true, true]);
        assert_eq!(
            whole_file_reads.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}
