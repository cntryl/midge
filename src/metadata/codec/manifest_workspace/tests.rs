use super::*;
use crate::metadata::{FileMeta, Manifest, ManifestEdit};
use std::time::Duration;

fn file(sequence: u64, cf_id: u32) -> FileMeta {
    FileMeta {
        name: crate::cloud_layout::file_name(cf_id, 0, sequence),
        cf_id,
        smallest_key: Some(vec![1; 512]),
        largest_key: Some(vec![255; 1024]),
        ..FileMeta::default()
    }
}

fn populated_manifest() -> Manifest {
    let mut manifest = Manifest::default();
    manifest
        .apply_edit(&ManifestEdit::CreateColumnFamily {
            id: 1,
            name: "lifecycle-family".into(),
            created_at: 7,
        })
        .unwrap();
    for sequence in 1..=32 {
        manifest.files.push(file(sequence, 0));
        manifest.files.push(file(sequence, 1));
        let retired = file(sequence + 1000, 0);
        manifest.retired_ssts.insert(retired.name.clone(), retired);
    }
    manifest
        .apply_edit(&ManifestEdit::DropColumnFamilyAt {
            id: 1,
            drop_sequence: 9,
            dropped_sst_names: manifest
                .files
                .iter()
                .filter(|file| file.cf_id == 1)
                .map(|file| file.name.clone())
                .collect(),
        })
        .unwrap();
    manifest.next_sst_seqs.insert(0, 33);
    manifest.next_sst_seqs.insert(1, 33);
    manifest.validate_persisted().unwrap();
    manifest
}

fn file_payload(file: &FileMeta) -> usize {
    file.name.capacity()
        + file.smallest_key.as_ref().map_or(0, Vec::capacity)
        + file.largest_key.as_ref().map_or(0, Vec::capacity)
        + 2 * size_of::<usize>()
        + size_of::<std::sync::atomic::AtomicU64>()
}

fn retained_payload(manifest: &Manifest) -> usize {
    manifest.files.capacity() * size_of::<FileMeta>()
        + manifest.retired_ssts.capacity() * size_of::<(String, FileMeta)>()
        + manifest.next_sst_seqs.capacity() * size_of::<(u32, u64)>()
        + manifest.column_families.capacity() * size_of::<crate::metadata::ColumnFamilyMeta>()
        + manifest.files.iter().map(file_payload).sum::<usize>()
        + manifest
            .retired_ssts
            .iter()
            .map(|(name, file)| name.capacity() + file_payload(file))
            .sum::<usize>()
        + manifest
            .column_families
            .iter()
            .map(|family| {
                family.name.capacity()
                    + family.dropped_sst_names.capacity() * size_of::<String>()
                    + family
                        .dropped_sst_names
                        .iter()
                        .map(String::capacity)
                        .sum::<usize>()
            })
            .sum::<usize>()
}

#[test]
fn should_cover_owned_snapshot_payload_when_all_manifest_collections_are_populated() {
    // Arrange
    let encoded = super::super::encode(super::super::MANIFEST, &populated_manifest()).unwrap();
    let decoded: Manifest = super::super::decode(super::super::MANIFEST, &encoded).unwrap();
    let actual = retained_payload(&decoded);
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

    // Act
    let bound = decoded_workspace_bound(&encoded, &[], &deadline).unwrap();

    // Assert: compare against genuine production decoded capacities, including
    // nested keys/names and default read-count Arc payloads, rather than wire.
    assert!(bound.bytes >= actual);
    assert!(bound.authority < bound.bytes);
    assert!(
        bound.authority
            >= decoded.column_families.capacity() * size_of::<crate::metadata::ColumnFamilyMeta>()
    );
}

#[derive(serde::Serialize, Deserialize)]
struct ActualEnvelope {
    edit_id: u64,
    edit: ManifestEdit,
}

fn every_owned_edit() -> ManifestEdit {
    ManifestEdit::Batch(vec![
        ManifestEdit::AddSst(file(1, 0)),
        ManifestEdit::RemoveSst {
            name: file(2, 0).name,
        },
        ManifestEdit::MoveSsts {
            captured: vec![file(3, 0)],
            target_level: 2,
        },
        ManifestEdit::CompleteSstDelete { file: file(4, 0) },
        ManifestEdit::CreateColumnFamily {
            id: 1,
            name: "created-family".repeat(8),
            created_at: 7,
        },
        ManifestEdit::DropColumnFamilyAt {
            id: 1,
            drop_sequence: 9,
            dropped_sst_names: vec![file(1, 1).name],
        },
        ManifestEdit::ReclaimColumnFamily {
            id: 1,
            names: vec![file(1, 1).name],
        },
        ManifestEdit::AdvanceFlushFrontier {
            cf_id: 0,
            freeze_sequence: 17,
        },
        ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 18,
        },
        ManifestEdit::Batch(vec![ManifestEdit::AddSst(file(5, 0))]),
    ])
}

#[test]
fn should_cover_nested_journal_shapes_when_every_owned_edit_variant_is_decoded() {
    // Arrange: exact binary envelope and journal TLV framing, with all owned
    // strings, key buffers, file vectors and nested batch vectors represented.
    let snapshot = super::super::encode(super::super::MANIFEST, &Manifest::default()).unwrap();
    let payload = super::super::encode(
        super::super::JOURNAL,
        &ActualEnvelope {
            edit_id: 1,
            edit: every_owned_edit(),
        },
    )
    .unwrap();
    let decoded: ActualEnvelope = super::super::decode(super::super::JOURNAL, &payload).unwrap();
    let mut journal = vec![8];
    journal.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_le_bytes());
    journal.extend_from_slice(&payload);
    journal.extend_from_slice(&crc32c::crc32c(&payload).to_be_bytes());
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

    // Act
    let baseline = decoded_workspace_bound(&snapshot, &[], &deadline).unwrap();
    let bound = decoded_workspace_bound(&snapshot, &journal, &deadline).unwrap();

    // Assert: the mirror accepts the same real enum shapes and includes both
    // their retained children and newly introduced family authority.
    let ManifestEdit::Batch(edits) = decoded.edit else {
        panic!("actual batch")
    };
    assert!(bound.bytes >= edits.capacity() * size_of::<ManifestEdit>() + 4 * (512 + 1024));
    assert!(bound.bytes > baseline.bytes);
    assert!(bound.authority > baseline.authority);
}

#[test]
fn should_preserve_partial_eof_semantics_when_journal_tail_is_zero_padding() {
    // Arrange: this is an accepted partial EOF in the production replay path.
    let snapshot = super::super::encode(super::super::MANIFEST, &Manifest::default()).unwrap();
    let padded = vec![0_u8; 32];
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));

    // Act
    let decoded = crate::metadata::ManifestPersistence::decode_bodies(
        &snapshot,
        Some(&padded),
        &crate::common::DeadlineScope::new(deadline),
    )
    .unwrap();
    let baseline = decoded_workspace_bound(&snapshot, &[], &deadline).unwrap();
    let bound = decoded_workspace_bound(&snapshot, &padded, &deadline).unwrap();

    // Assert
    assert_eq!(decoded.edit_checkpoint_id, 0);
    assert_eq!(bound.bytes, baseline.bytes);
    assert_eq!(bound.authority, baseline.authority);
}
