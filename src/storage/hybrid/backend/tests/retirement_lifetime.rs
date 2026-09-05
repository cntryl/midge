#![cfg(test)]

use super::*;
use crate::metadata::Manifest;
use crate::runtime::hybrid_persistence::CloudWalPruneProgress;

fn disjoint_segments(records_per_segment: u64) -> (HybridStorage, Manifest) {
    let (_cloud, storage) = hybrid_with_mock_cloud();
    storage.enable_ephemeral_sst_cache(1024 * 1024);
    let mut manifest = Manifest::default();
    for segment in 1_u64..=32 {
        let mut wal = Vec::new();
        for operation in 0..records_per_segment {
            let sequence = (segment - 1) * records_per_segment + operation + 1;
            let key = format!("key-{sequence:03}").into_bytes();
            let record = crate::wal::WalRecord::new(
                crate::wal::WalOpKind::Put,
                Bytes::copy_from_slice(&key),
                Some(Bytes::from_static(b"value")),
                sequence,
                1,
            );
            let payload = crate::wal::encoding::encode(&record).expect("WAL record");
            crate::wal::frame::append_frame(&mut wal, &payload).expect("WAL frame");
            let name = format!("disjoint-{sequence:03}.sst");
            let bytes = valid_sst_bytes(&key, b"value", sequence);
            let mut file =
                manifest_covering_wal(&name, &bytes, sequence, Some(crc32c::crc32c(&bytes)))
                    .files
                    .remove(0);
            file.smallest_key = Some(key.clone());
            file.largest_key = Some(key);
            file.key_bounds_complete = true;
            manifest.files.push(file);
            write_cloud_object(&storage, &crate::sst::object_key(&name), bytes);
        }
        write_authoritative_cloud_wal(&storage, segment, segment * records_per_segment, wal);
    }
    let catalog = read_cloud_object(&storage, crate::wal::cloud_catalog::OBJECT_KEY);
    write_cloud_object(
        &storage,
        crate::wal::cloud_catalog::MIRROR_OBJECT_KEY,
        catalog,
    );
    (storage, manifest)
}

#[test]
fn should_reclaim_completed_sst_proofs_when_many_disjoint_wal_segments_retire() {
    // Arrange
    let (storage, manifest) = disjoint_segments(1);
    let progress = CloudWalPruneProgress::default();
    // Act
    for segment in 1..=32 {
        let result = storage.prune_cloud_wal_segment_within(
            segment,
            segment,
            CloudWalPruneGuard::new(manifest.clone(), None)
                .with_memory_limit(64 * 1024)
                .with_progress(progress.clone()),
            2,
            &crate::common::OperationDeadline::from_budget(Duration::from_secs(1)),
        );
        assert!(
            result.is_ok(),
            "segment {segment} cannot inherit old proof charges: {result:?}"
        );
        assert!(wait_for_wal_prune_result(&storage, segment).is_ok());
    }
    // Assert
    assert!(assert_wal_catalog_copies_match(&storage)
        .segments
        .is_empty());
    assert_eq!(
        manifest.files.len(),
        32,
        "manifest remains unchanged throughout retirement"
    );
}

#[test]
fn should_retire_completed_batch_prefix_when_proof_memory_cannot_hold_all_candidates() {
    // Arrange
    let (storage, manifest) = disjoint_segments(3);
    let progress = CloudWalPruneProgress::default();
    let mut batches = 0;
    // Act
    loop {
        let catalog = assert_wal_catalog_copies_match(&storage);
        if catalog.segments.is_empty() {
            break;
        }
        let candidates = catalog
            .segments
            .values()
            .map(|entry| (entry.segment_id, entry.max_sequence))
            .collect::<Vec<_>>();
        let result = storage.prune_cloud_wal_segments_within(
            &candidates,
            CloudWalPruneGuard::new(manifest.clone(), None)
                .with_memory_limit(64 * 1024)
                .with_progress(progress.clone()),
            2,
            &crate::common::OperationDeadline::from_budget(Duration::from_secs(1)),
        );
        assert!(
            result.is_ok(),
            "complete prefix must survive later budget pressure: {result:?}"
        );
        let remaining = assert_wal_catalog_copies_match(&storage);
        assert!(
            remaining.segments.len() < candidates.len(),
            "each batch must retire a prefix"
        );
        wait_for_retired_segments(
            &storage,
            candidates
                .iter()
                .filter_map(|(segment, _)| {
                    (!remaining.segments.contains_key(segment)).then_some(*segment)
                })
                .collect(),
        );
        batches += 1;
    }
    // Assert
    assert!(
        batches > 1,
        "fixture must exceed one batch's proof capacity"
    );
    assert_eq!(manifest.files.len(), 96);
}

fn wait_for_retired_segments(
    storage: &HybridStorage,
    mut pending: std::collections::BTreeSet<u64>,
) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !pending.is_empty() {
        for event in storage.process_uploads() {
            if let StorageEvent::CloudWalPruneComplete { segment_id, result } = event {
                assert!(result.is_ok(), "retired segment {segment_id}: {result:?}");
                pending.remove(&segment_id);
            }
        }
        assert!(
            Instant::now() < deadline,
            "waiting for physical retirement of {pending:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
