use super::{decoded_workspace_bound, IntentLogEntry, MidgeError, OperationDeadline};
use crate::common::MidgeResult;
use crate::io::{Fs, FsError};
use crate::metadata::codec;
use crate::runtime::intent_persistence::edits::{frame_checksum, IntentEdit};
use std::mem::size_of;
use std::sync::Arc;

fn edit(append: Vec<IntentLogEntry>) -> IntentEdit {
    IntentEdit {
        sequence: 1,
        previous_checksum: 0,
        keep: 0,
        append,
    }
}

fn replay(bytes: &[u8]) -> MidgeResult<Vec<IntentLogEntry>> {
    let directory = tempfile::tempdir()?;
    let fs: Arc<dyn Fs> =
        Arc::new(crate::io::RealFs::new(directory.path()).map_err(FsError::into_midge)?);
    std::fs::write(directory.path().join("intent_log.json"), bytes)?;
    crate::runtime::intent_persistence::edits::load(&fs)
}

#[test]
fn should_charge_dense_intent_vectors_before_replaying_their_owned_append() -> MidgeResult<()> {
    // Arrange: this accepted small variant occupies the whole production enum
    // in both the append and replay vectors, not its compact encoded size.
    let entries = vec![IntentLogEntry::SeqnoAllocated { seqno: 1, cf_id: 0 }; 8192];
    let bytes = codec::encode(codec::INTENT, &edit(entries))?;
    let append: IntentEdit = codec::decode(codec::INTENT, &bytes)?;
    let append_bytes = append.append.capacity() * size_of::<IntentLogEntry>();

    // Act: use the real framed persistence reader and preflight that admission
    // calls before constructing either owned vector.
    let bound = decoded_workspace_bound(&bytes, &OperationDeadline::unbounded())?;
    let recovered = replay(&bytes)?;
    let actual_overlap = append_bytes + recovered.capacity() * size_of::<IntentLogEntry>();

    // Assert: the prior private floor missed this simultaneous ownership.
    assert_eq!(recovered.len(), 8192);
    assert_eq!(bound.entries, recovered.len());
    assert_eq!(bound.publication_entries, 0);
    assert_eq!(bound.max_single_normalization_scratch, 0);
    assert!(bound.retained_state_bytes >= recovered.capacity() * size_of::<IntentLogEntry>());
    assert!(8 * bytes.len() + 4096 < actual_overlap);
    assert!(bound.bytes >= actual_overlap);
    Ok(())
}

#[test]
fn should_charge_nested_string_descriptors_before_rejecting_invalid_sst_names() -> MidgeResult<()> {
    // Arrange: semantic SST-name checks run after decoding. Empty strings are
    // therefore a real accepted decode shape even though publication rejects it.
    let entries = vec![IntentLogEntry::CompactionPlanned {
        input_files: vec![String::new(); 8192],
        output_level: 1,
    }];
    let bytes = codec::encode(codec::INTENT, &edit(entries))?;

    // Act
    let bound = decoded_workspace_bound(&bytes, &OperationDeadline::unbounded())?;
    let recovered = replay(&bytes)?;
    let IntentLogEntry::CompactionPlanned { input_files, .. } = &recovered[0] else {
        panic!("production replay changed the persisted intent variant");
    };
    let actual_nested = input_files.capacity() * size_of::<String>();

    // Assert: no bytes-only floor can stand in for typed nested allocations.
    assert!(8 * bytes.len() + 4096 < actual_nested);
    assert!(bound.bytes >= actual_nested);
    assert_eq!(input_files.len(), 8192);
    assert_eq!(bound.entries, 1);
    assert!(crate::runtime::IntentPersistence::validate_persisted_sst_names(&recovered).is_err());
    Ok(())
}

fn runtime_file() -> crate::runtime::FileMeta {
    crate::runtime::FileMeta {
        name: crate::cloud_layout::file_name(0, 1, 1),
        level: 1,
        size_bytes: 100,
        content_crc32c: Some(42),
        cf_id: 0,
        smallest_key: Some(vec![1; 2049]),
        largest_key: Some(vec![9; 4097]),
        smallest_seq: Some(1),
        largest_seq: Some(2),
        key_bounds_complete: true,
        earliest_expiration_millis: None,
    }
}

fn nested_entries() -> Vec<IntentLogEntry> {
    use crate::runtime::PublicationPhase;
    let file = runtime_file();
    vec![
        IntentLogEntry::SeqnoAllocated { seqno: 1, cf_id: 0 },
        IntentLogEntry::FlushPlanned {
            cf_id: 0,
            seqno_range: (1, 2),
        },
        IntentLogEntry::CompactionPlanned {
            input_files: vec![file.name.clone(); 16],
            output_level: 1,
        },
        IntentLogEntry::FlushPublish {
            phase: PublicationPhase::OutputDurable,
            cf_id: 0,
            sequence: 2,
            file_meta: file.clone(),
        },
        IntentLogEntry::CompactionPublish {
            phase: PublicationPhase::ManifestPublished,
            cf_id: 0,
            removed: vec![file.name.clone(); 16],
            added: vec![file.clone(); 16],
        },
        IntentLogEntry::MoveSstsPublish {
            phase: PublicationPhase::OutputDurable,
            captured: vec![crate::metadata::FileMeta::from(&file); 16],
            target_level: 2,
        },
        IntentLogEntry::SstAdded {
            file_meta: file.clone(),
        },
        IntentLogEntry::CompactionApplied {
            removed: vec![file.name.clone(); 16],
            added: vec![file; 16],
        },
        IntentLogEntry::WalSynced {
            segment_id: 1,
            seqno: 2,
        },
        IntentLogEntry::CloudUploadComplete {
            resource: "wal/segment".repeat(100),
            seqno: 2,
        },
    ]
}

fn strings_bytes(values: &[String], capacity: usize) -> usize {
    capacity * size_of::<String>() + values.iter().map(String::capacity).sum::<usize>()
}

fn file_bytes(file: &crate::runtime::FileMeta) -> usize {
    file.name.capacity()
        + file.smallest_key.as_ref().map_or(0, Vec::capacity)
        + file.largest_key.as_ref().map_or(0, Vec::capacity)
}

fn metadata_file_bytes(file: &crate::metadata::FileMeta) -> usize {
    file.name.capacity()
        + file.smallest_key.as_ref().map_or(0, Vec::capacity)
        + file.largest_key.as_ref().map_or(0, Vec::capacity)
        + 2 * size_of::<usize>()
        + size_of::<std::sync::atomic::AtomicU64>()
}

fn entry_bytes(entry: &IntentLogEntry) -> usize {
    match entry {
        IntentLogEntry::CompactionPlanned { input_files, .. } => {
            strings_bytes(input_files, input_files.capacity())
        }
        IntentLogEntry::FlushPublish { file_meta, .. } | IntentLogEntry::SstAdded { file_meta } => {
            file_bytes(file_meta)
        }
        IntentLogEntry::CompactionPublish { removed, added, .. }
        | IntentLogEntry::CompactionApplied { removed, added } => {
            strings_bytes(removed, removed.capacity())
                + added.capacity() * size_of::<crate::runtime::FileMeta>()
                + added.iter().map(file_bytes).sum::<usize>()
        }
        IntentLogEntry::MoveSstsPublish { captured, .. } => {
            captured.capacity() * size_of::<crate::metadata::FileMeta>()
                + captured.iter().map(metadata_file_bytes).sum::<usize>()
        }
        IntentLogEntry::CloudUploadComplete { resource, .. } => resource.capacity(),
        _ => 0,
    }
}

#[test]
fn should_cover_owned_nested_fields_when_replaying_every_intent_variant() -> MidgeResult<()> {
    // Arrange
    let bytes = codec::encode(codec::INTENT, &edit(nested_entries()))?;
    let append: IntentEdit = codec::decode(codec::INTENT, &bytes)?;
    let append_vector = append.append.capacity() * size_of::<IntentLogEntry>();

    // Act
    let bound = decoded_workspace_bound(&bytes, &OperationDeadline::unbounded())?;
    let recovered = replay(&bytes)?;
    let actual = append_vector
        + recovered.capacity() * size_of::<IntentLogEntry>()
        + recovered.iter().map(entry_bytes).sum::<usize>();

    // Assert: owned names, both runtime/key-buffer encodings, nested vectors,
    // and skipped default read counters all remain inside the admitted bound.
    assert_eq!(recovered.len(), 10);
    assert_eq!(bound.entries, recovered.len());
    assert_eq!(bound.publication_entries, 3);
    assert!(bound.retained_state_bytes >= actual - append_vector);
    assert!(
        bound.bytes >= actual,
        "bound={} actual={actual}",
        bound.bytes
    );
    Ok(())
}

#[test]
fn should_retain_capacity_charge_when_an_edit_replaces_an_older_prefix() -> MidgeResult<()> {
    // Arrange
    let first = codec::encode(codec::INTENT, &edit(nested_entries()))?;
    let second = IntentEdit {
        sequence: 2,
        previous_checksum: frame_checksum(&first),
        keep: 1,
        append: vec![IntentLogEntry::SeqnoAllocated { seqno: 3, cf_id: 0 }; 32],
    };
    let second = codec::encode(codec::INTENT, &second)?;
    let mut bytes = first.clone();
    bytes.extend_from_slice(&second);

    // Act
    let bound = decoded_workspace_bound(&bytes, &OperationDeadline::unbounded())?;
    let first_bound = decoded_workspace_bound(&first, &OperationDeadline::unbounded())?;
    let recovered = replay(&bytes)?;

    // Assert: truncating old entries does not waive their preceding decode
    // peak, nor the old allocation while the later state grows.
    assert_eq!(recovered.len(), 33);
    assert_eq!(bound.entries, 42);
    assert!(bound.bytes >= first_bound.bytes);
    assert!(bound.bytes >= recovered.capacity() * size_of::<IntentLogEntry>());
    Ok(())
}

#[test]
fn should_accept_positional_edit_shape_when_the_production_decoder_accepts_it() -> MidgeResult<()> {
    // Arrange: serde accepts tuple-form structs as well as named maps.
    let entries = nested_entries();
    let bytes = codec::encode(codec::INTENT, &(1_u64, 0_u32, 0_usize, entries))?;

    // Act
    let bound = decoded_workspace_bound(&bytes, &OperationDeadline::unbounded())?;
    let recovered = replay(&bytes)?;

    // Assert
    assert_eq!(bound.entries, recovered.len());
    assert_eq!(recovered.len(), 10);
    assert!(bound.bytes >= recovered.iter().map(entry_bytes).sum::<usize>());
    Ok(())
}

#[test]
fn should_stop_at_the_same_torn_tail_when_preflighting_an_intent_history() -> MidgeResult<()> {
    // Arrange
    let first = codec::encode(codec::INTENT, &edit(nested_entries()))?;
    let second = codec::encode(
        codec::INTENT,
        &IntentEdit {
            sequence: 2,
            previous_checksum: frame_checksum(&first),
            keep: 10,
            append: vec![IntentLogEntry::WalSynced {
                segment_id: 2,
                seqno: 4,
            }],
        },
    )?;
    let expected = decoded_workspace_bound(&first, &OperationDeadline::unbounded())?;

    // Act
    for extent in [1, 8, 16, second.len() - 1] {
        let mut torn = first.clone();
        torn.extend_from_slice(&second[..extent]);
        let bound = decoded_workspace_bound(&torn, &OperationDeadline::unbounded())?;
        let recovered = replay(&torn)?;
        // Assert: admission and the production reader select the same prefix.
        assert_eq!(bound.bytes, expected.bytes);
        assert_eq!(bound.entries, recovered.len());
        assert_eq!(recovered.len(), 10);
    }
    Ok(())
}

#[test]
fn should_reject_invalid_predecessors_before_admitting_typed_replay() -> MidgeResult<()> {
    // Arrange
    let first = codec::encode(codec::INTENT, &edit(nested_entries()))?;

    // Act
    for (sequence, checksum, keep) in [
        (3, frame_checksum(&first), 0),
        (2, 0, 0),
        (2, frame_checksum(&first), 11),
    ] {
        let next = codec::encode(
            codec::INTENT,
            &IntentEdit {
                sequence,
                previous_checksum: checksum,
                keep,
                append: Vec::new(),
            },
        )?;
        let mut bytes = first.clone();
        bytes.extend_from_slice(&next);
        let preflight = decoded_workspace_bound(&bytes, &OperationDeadline::unbounded());
        let recovered = replay(&bytes);
        // Assert
        assert!(matches!(preflight, Err(MidgeError::Corruption(_))));
        assert!(matches!(recovered, Err(MidgeError::Corruption(_))));
    }
    Ok(())
}

#[test]
fn should_preserve_the_original_deadline_when_preflighting_intent_bytes() -> MidgeResult<()> {
    // Arrange
    let bytes = codec::encode(codec::INTENT, &edit(nested_entries()))?;
    let deadline = OperationDeadline::from_budget(std::time::Duration::ZERO);

    // Act
    let result = decoded_workspace_bound(&bytes, &deadline);

    // Assert: an expired caller cannot receive a fresh decoding budget.
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    Ok(())
}

#[test]
fn should_fit_an_ordinary_publication_within_the_existing_private_floor() -> MidgeResult<()> {
    // Arrange: the ordinary one-output cut keeps the same configured pool and
    // its existing minimum admission; dense histories alone need a larger bound.
    let entry = IntentLogEntry::FlushPublish {
        phase: crate::runtime::PublicationPhase::ManifestPublished,
        cf_id: 0,
        sequence: 2,
        file_meta: runtime_file(),
    };
    let bytes = codec::encode(codec::INTENT, &edit(vec![entry]))?;

    // Act
    let bound = decoded_workspace_bound(&bytes, &OperationDeadline::unbounded())?;
    let recovered = replay(&bytes)?;

    // Assert
    assert_eq!(recovered.len(), 1);
    assert!(bound.bytes <= 8 * bytes.len() + 4096);
    Ok(())
}

fn normalized_encode_scratch(entry: &IntentLogEntry) -> usize {
    let mut normalized = entry.clone();
    match &mut normalized {
        IntentLogEntry::FlushPublish { phase, .. }
        | IntentLogEntry::CompactionPublish { phase, .. }
        | IntentLogEntry::MoveSstsPublish { phase, .. } => {
            *phase = crate::runtime::PublicationPhase::OutputDurable;
        }
        _ => panic!("normalization fixture requires a publication variant"),
    }
    // The codec retains its serde payload while it allocates the framed output.
    let payload = rmp_serde::to_vec_named(&normalized).unwrap();
    let framed = codec::encode(codec::INTENT, &normalized).unwrap();
    payload.capacity() + framed.capacity()
}

#[test]
fn should_bound_actual_normalization_scratch_for_each_publication_variant() -> MidgeResult<()> {
    // Arrange: byte 255 expands to two MessagePack bytes in runtime Vec<u8>,
    // while moved metadata retains its binary key-buffer encoding.
    let mut entries = nested_entries();
    let mut scalar_maximum = runtime_file();
    scalar_maximum.name.clear();
    scalar_maximum.level = u32::MAX;
    scalar_maximum.size_bytes = u64::MAX;
    scalar_maximum.content_crc32c = Some(u32::MAX);
    scalar_maximum.cf_id = u32::MAX;
    scalar_maximum.smallest_key = Some(Vec::new());
    scalar_maximum.largest_key = Some(Vec::new());
    scalar_maximum.smallest_seq = Some(u64::MAX);
    scalar_maximum.largest_seq = Some(u64::MAX);
    scalar_maximum.earliest_expiration_millis = Some(u64::MAX);
    entries.push(IntentLogEntry::FlushPublish {
        phase: crate::runtime::PublicationPhase::ManifestPublished,
        cf_id: u32::MAX,
        sequence: u64::MAX,
        file_meta: scalar_maximum,
    });
    for entry in &mut entries {
        if let IntentLogEntry::FlushPublish { file_meta, .. } = entry {
            file_meta.smallest_key.as_mut().unwrap().fill(255);
            file_meta.largest_key.as_mut().unwrap().fill(255);
        }
    }

    // Act
    for entry in entries.into_iter().filter(|entry| {
        matches!(
            entry,
            IntentLogEntry::FlushPublish { .. }
                | IntentLogEntry::CompactionPublish { .. }
                | IntentLogEntry::MoveSstsPublish { .. }
        )
    }) {
        let actual = normalized_encode_scratch(&entry);
        let bytes = codec::encode(codec::INTENT, &edit(vec![entry]))?;
        let bound = decoded_workspace_bound(&bytes, &OperationDeadline::unbounded())?;
        // Assert: named encoding and both actual codec allocations are covered.
        assert_eq!(bound.publication_entries, 1);
        assert!(bound.max_single_normalization_scratch >= actual);
    }
    Ok(())
}

#[test]
fn should_charge_one_normalization_workspace_when_publication_history_grows() -> MidgeResult<()> {
    // Arrange
    let publications = (0..64)
        .map(|sequence| IntentLogEntry::FlushPublish {
            phase: crate::runtime::PublicationPhase::ManifestPublished,
            cf_id: 0,
            sequence,
            file_meta: runtime_file(),
        })
        .collect::<Vec<_>>();
    let one = codec::encode(codec::INTENT, &edit(vec![publications[0].clone()]))?;
    let many = codec::encode(codec::INTENT, &edit(publications))?;

    // Act
    let one = decoded_workspace_bound(&one, &OperationDeadline::unbounded())?;
    let many = decoded_workspace_bound(&many, &OperationDeadline::unbounded())?;

    // Assert: replay still owns all decoded entries, but the production
    // normalization loop serializes and drops one entry's scratch at a time.
    assert_eq!(many.publication_entries, 64);
    assert!(many.bytes > one.bytes);
    assert!(many.retained_state_bytes > one.retained_state_bytes);
    assert_eq!(
        many.max_single_normalization_scratch,
        one.max_single_normalization_scratch
    );
    Ok(())
}
