use super::*;
use crate::common::OperationDeadline;
use crate::lease::CloudMetadataObject;
use crate::storage::cloud::CloudStorage;
use std::time::Duration;

fn deadline() -> OperationDeadline {
    OperationDeadline::from_budget(Duration::from_secs(5))
}

fn plan(publication_id: Uuid) -> Vec<CloudMetadataObject> {
    [
        ("FORMAT", b"midge-format-version=10\n".len() as u64),
        ("metadata/HEAD", 32),
        ("metadata/cut-proof", 64),
        ("metadata/generations/local-generation/manifest/base", 80),
        ("metadata/generations/local-generation/intent/base", 40),
    ]
    .into_iter()
    .map(|(file_name, len)| CloudMetadataObject {
        file_name: file_name.into(),
        object_key: format!("metadata/generations/{publication_id}/{file_name}"),
        len,
        crc32c: 7,
    })
    .collect()
}

#[test]
fn should_persist_the_reservation_lifecycle_for_a_cloud_publication() -> MidgeResult<()> {
    // Arrange
    let cloud = CloudStorage::with_mock();
    let publication_id = Uuid::new_v4();
    let planned = plan(publication_id);
    let deadline = deadline();
    let store = Format10AdmissionStore::new(&cloud, &deadline);

    // Act
    let pending = store.reserve(publication_id, 17, &planned)?;
    let slots = store.list_slots()?;
    let uploaded = store.mark_uploaded(publication_id, &planned)?;
    let finished = store.finish(publication_id)?;

    // Assert
    assert_eq!(pending.phase, AdmissionPhase::Pending);
    assert_eq!(pending.writer_epoch, 17);
    assert_eq!(slots.iter().flatten().count(), 1);
    assert_eq!(uploaded.phase, AdmissionPhase::Uploaded);
    assert_eq!(finished.phase, AdmissionPhase::Finished);
    assert_ne!(finished.identity, uploaded.identity);
    Ok(())
}

#[test]
fn should_keep_slots_occupied_until_exact_cleanup_of_a_finished_stage() -> MidgeResult<()> {
    // Arrange
    let cloud = CloudStorage::with_mock();
    let deadline = deadline();
    let store = Format10AdmissionStore::new(&cloud, &deadline);
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let third = Uuid::new_v4();
    let first_record = store.reserve(first, 1, &plan(first))?;
    store.reserve(second, 1, &plan(second))?;
    store.reserve(third, 1, &plan(third))?;
    let fourth = Uuid::new_v4();

    // Act
    let full = store.reserve(fourth, 1, &plan(fourth));
    let premature_delete = store.delete_finished(
        &first_record,
        &first_record
            .objects
            .iter()
            .map(|object| object.object_key.clone())
            .collect(),
    );

    // Assert
    assert!(matches!(full, Err(MidgeError::ResourceLimit(_))));
    assert!(matches!(premature_delete, Err(MidgeError::Fenced(_))));
    store.mark_uploaded(first, &plan(first))?;
    let finished = store.finish(first)?;
    let remaining = finished
        .objects
        .iter()
        .map(|object| object.object_key.clone())
        .collect();
    assert!(store.delete_finished(&finished, &remaining)?);
    let replacement = store.reserve(fourth, 9, &plan(fourth))?;
    assert_eq!(replacement.slot, finished.slot);
    assert_eq!(store.list_slots()?.iter().flatten().count(), 3);
    Ok(())
}

#[test]
fn should_enforce_migration_admission_budget_when_cut_proof_is_absent() {
    // Arrange
    let publication_id = Uuid::new_v4();
    let objects = plan(publication_id);
    // Act
    let duplicate_plan = canonical_plan(publication_id, &objects);
    let migration_plan = objects
        .iter()
        .filter(|object| object.file_name != "metadata/cut-proof")
        .cloned()
        .collect::<Vec<_>>();
    let migration_admitted = canonical_plan(publication_id, &migration_plan);
    let mut oversized = objects.clone();
    oversized
        .iter_mut()
        .find(|object| object.file_name.ends_with("manifest/base"))
        .unwrap()
        .len = crate::metadata::local_authority::MAX_CLOSURE_BYTES;
    let oversized_result = canonical_plan(publication_id, &oversized);
    let mut noncanonical = objects.clone();
    noncanonical[0].object_key.push_str("/unexpected");
    let noncanonical_result = canonical_plan(publication_id, &noncanonical);
    let zero_epoch = encode_wire(&AdmissionWire {
        format_version: 1,
        publication_id: publication_id.to_string(),
        writer_epoch: 0,
        phase: AdmissionPhase::Pending,
        objects: canonical_plan(publication_id, &objects).unwrap(),
    })
    .unwrap();
    let parsed_zero_epoch = decode_record(
        0,
        &zero_epoch,
        StorageObjectMetadata {
            size: zero_epoch.len() as u64,
            etag: "etag".into(),
            generation: None,
        },
    );

    // Assert
    assert!(duplicate_plan.is_ok());
    assert!(migration_admitted.is_ok());
    assert!(oversized_result.is_err());
    assert!(noncanonical_result.is_err());
    assert!(parsed_zero_epoch.is_err());
    assert!(validate_slot(u8::try_from(FORMAT10_ADMISSION_SLOT_COUNT).unwrap()).is_err());
}

#[test]
fn should_not_finish_a_slot_before_all_payload_puts_are_settled() -> MidgeResult<()> {
    // Arrange
    let cloud = CloudStorage::with_mock();
    let publication_id = Uuid::new_v4();
    let deadline = deadline();
    let store = Format10AdmissionStore::new(&cloud, &deadline);
    let _pending = store.reserve(publication_id, 7, &plan(publication_id))?;

    // Act
    let finished = store.finish(publication_id);

    // Assert
    assert!(matches!(finished, Err(MidgeError::Fenced(_))));
    assert_eq!(
        store.read_slot(0)?.expect("pending reservation").phase,
        AdmissionPhase::Pending
    );
    Ok(())
}

#[test]
fn should_reserve_different_slots_for_concurrent_publications() -> MidgeResult<()> {
    // Arrange
    let cloud = std::sync::Arc::new(CloudStorage::with_mock());
    let publications = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];

    // Act
    let threads = publications.map(|publication_id| {
        let cloud = std::sync::Arc::clone(&cloud);
        std::thread::spawn(move || {
            let deadline = deadline();
            Format10AdmissionStore::new(&cloud, &deadline).reserve(
                publication_id,
                1,
                &plan(publication_id),
            )
        })
    });
    let records = threads
        .into_iter()
        .map(|thread| thread.join().expect("reservation thread"))
        .collect::<MidgeResult<Vec<_>>>()?;

    // Assert
    let slots = records
        .iter()
        .map(|record| record.slot)
        .collect::<HashSet<_>>();
    assert_eq!(slots.len(), FORMAT10_ADMISSION_SLOT_COUNT);
    Ok(())
}

#[test]
fn should_return_only_one_new_reservation_for_a_repeated_publication_uuid() {
    // Arrange
    let cloud = std::sync::Arc::new(CloudStorage::with_mock());
    let publication_id = Uuid::new_v4();

    // Act
    let attempts = [0_u32, 1].map(|checksum_delta| {
        let cloud = std::sync::Arc::clone(&cloud);
        std::thread::spawn(move || {
            let deadline = deadline();
            let mut planned = plan(publication_id);
            planned[0].crc32c ^= checksum_delta;
            Format10AdmissionStore::new(&cloud, &deadline).reserve(publication_id, 4, &planned)
        })
    });
    let results = attempts.map(|attempt| attempt.join().expect("reservation thread"));

    // Assert
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(MidgeError::Busy(_) | MidgeError::Corruption(_))))
            .count(),
        1
    );
}
