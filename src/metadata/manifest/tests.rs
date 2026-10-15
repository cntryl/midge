use super::*;

fn flush_edit(cf_id: u32, output: &str, largest: u64, freeze: u64) -> ManifestEdit {
    ManifestEdit::Batch(vec![
        ManifestEdit::BumpNextSstSeq {
            cf_id,
            next_seq: freeze,
        },
        ManifestEdit::AddSst(FileMeta {
            name: output.into(),
            cf_id,
            largest_seq: Some(largest),
            ..Default::default()
        }),
        ManifestEdit::AdvanceFlushFrontier {
            cf_id,
            freeze_sequence: freeze,
        },
    ])
}

#[test]
fn should_leave_every_field_unchanged_when_a_nested_batch_has_an_invalid_transition() {
    // Arrange
    let mut manifest = Manifest {
        edit_checkpoint_id: 7,
        ..Manifest::default()
    };
    let before = serde_json::to_value(&manifest).unwrap();
    let edit = ManifestEdit::Batch(vec![
        flush_edit(0, "valid.sst", 3, 4),
        ManifestEdit::Batch(vec![ManifestEdit::BumpNextSstSeq {
            cf_id: 99,
            next_seq: 100,
        }]),
    ]);

    // Act
    let result = manifest.apply_edit(&edit);

    // Assert
    assert!(result.is_err());
    assert_eq!(serde_json::to_value(&manifest).unwrap(), before);
}

#[test]
fn should_publish_a_flush_when_one_batch_introduces_its_family() {
    // Arrange
    let mut manifest = Manifest::default();
    let create = ManifestEdit::CreateColumnFamily {
        id: 1,
        name: "new".into(),
        created_at: 1,
    };

    // Act
    manifest
        .apply_edit(&ManifestEdit::Batch(vec![
            create,
            flush_edit(1, "new.sst", 3, 4),
        ]))
        .unwrap();

    // Assert
    assert_eq!(
        manifest.flush_frontiers().get(1).unwrap(),
        FamilyFrontier::Live {
            through_sequence: 4
        }
    );
    assert_eq!(manifest.files.len(), 1);
    manifest.validate_persisted().unwrap();
}

#[test]
fn should_refuse_invalid_frontier_advances_when_publishing_a_flush() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest
        .apply_edit(&flush_edit(0, "first.sst", 3, 4))
        .unwrap();
    let before = serde_json::to_value(&manifest).unwrap();

    // Act
    for edit in [
        ManifestEdit::AdvanceFlushFrontier {
            cf_id: 0,
            freeze_sequence: 5,
        },
        flush_edit(0, "lower.sst", 2, 3),
        flush_edit(0, "reserved.sst", 5, u64::MAX),
        flush_edit(0, "past-freeze.sst", 8, 7),
        flush_edit(0, "first.sst", 3, 5),
        ManifestEdit::Batch(vec![
            ManifestEdit::AddSst(FileMeta {
                name: "removed.sst".into(),
                largest_seq: Some(5),
                ..Default::default()
            }),
            ManifestEdit::RemoveSst {
                name: "removed.sst".into(),
            },
            ManifestEdit::AdvanceFlushFrontier {
                cf_id: 0,
                freeze_sequence: 5,
            },
        ]),
    ] {
        let result = manifest.apply_edit(&edit);
        // Assert
        assert!(result.is_err());
        assert_eq!(serde_json::to_value(&manifest).unwrap(), before);
    }

    // Retrying the exact durable publication is still idempotent.
    manifest
        .apply_edit(&flush_edit(0, "first.sst", 3, 4))
        .unwrap();
    assert_eq!(serde_json::to_value(&manifest).unwrap(), before);
}

#[test]
fn should_refuse_unpaired_flush_frontiers_when_preparing_an_edit() {
    // Arrange: every edit below would pass the base-relative frontier checks
    // alone, so only the pairing rule can refuse it.
    let mut manifest = Manifest::default();
    manifest
        .apply_edit(&flush_edit(0, "first.sst", 3, 4))
        .unwrap();
    let before = serde_json::to_value(&manifest).unwrap();
    let add = |name: &str, largest_seq: Option<u64>| {
        ManifestEdit::AddSst(FileMeta {
            name: name.into(),
            largest_seq,
            ..Default::default()
        })
    };
    let advance = |freeze_sequence| ManifestEdit::AdvanceFlushFrontier {
        cf_id: 0,
        freeze_sequence,
    };
    let unpaired = [
        ("outside an outer batch", advance(4)),
        ("without an output", ManifestEdit::Batch(vec![advance(4)])),
        (
            "with an unbounded output",
            ManifestEdit::Batch(vec![add("unbounded.sst", None), advance(5)]),
        ),
        (
            "with an output past the freeze",
            ManifestEdit::Batch(vec![add("past.sst", Some(6)), advance(5)]),
        ),
        (
            "removing its output",
            ManifestEdit::Batch(vec![
                add("removed.sst", Some(5)),
                advance(5),
                ManifestEdit::RemoveSst {
                    name: "removed.sst".into(),
                },
            ]),
        ),
        (
            "advancing twice",
            ManifestEdit::Batch(vec![add("twice.sst", Some(5)), advance(5), advance(5)]),
        ),
    ];

    for (case, edit) in unpaired {
        // Act
        let result = manifest.prepare_edit(&edit).map(|_| ());

        // Assert
        assert!(
            matches!(result, Err(MidgeError::Corruption(_))),
            "{case}: {result:?}"
        );
        assert_eq!(serde_json::to_value(&manifest).unwrap(), before, "{case}");
    }
}

#[test]
fn should_retain_frontiers_when_compaction_or_reclamation_removes_every_sst() {
    // Arrange
    let mut manifest = Manifest::default();
    let id = manifest.create_column_family("retired".into());
    manifest
        .apply_edit(&flush_edit(id, "retired.sst", 3, 4))
        .unwrap();

    // Act
    manifest
        .apply_edit(&ManifestEdit::RemoveSst {
            name: "retired.sst".into(),
        })
        .unwrap();
    assert_eq!(
        manifest.flush_frontiers().get(id).unwrap(),
        FamilyFrontier::Live {
            through_sequence: 4
        }
    );
    manifest
        .apply_edit(&ManifestEdit::DropColumnFamilyAt {
            id,
            drop_sequence: 5,
            dropped_sst_names: Vec::new(),
        })
        .unwrap();
    manifest
        .apply_edit(&ManifestEdit::ReclaimColumnFamily {
            id,
            names: Vec::new(),
        })
        .unwrap();

    // Assert
    assert!(manifest.files.is_empty());
    assert_eq!(
        manifest.flush_frontiers().get(id).unwrap(),
        FamilyFrontier::Dropped
    );
    assert!(manifest.column_families[0].reclaimed);
    assert!(manifest
        .apply_edit(&ManifestEdit::CreateColumnFamily {
            id,
            name: "revived".into(),
            created_at: 8
        })
        .is_err());
    manifest.validate_persisted().unwrap();
}

#[test]
fn should_preserve_state_when_a_prepared_edit_is_dropped_without_commit() {
    // Arrange
    let mut manifest = Manifest::default();
    let before = serde_json::to_value(&manifest).unwrap();
    let edit = flush_edit(0, "prepared.sst", 3, 4);

    // Act
    drop(manifest.prepare_edit(&edit).unwrap());

    // Assert
    assert_eq!(serde_json::to_value(&manifest).unwrap(), before);
}

#[test]
fn should_preserve_drop_tombstone_when_create_reuses_a_family_id() {
    // Arrange
    let mut manifest = Manifest::default();
    let id = manifest.create_column_family("retired".into());
    assert!(manifest.delete_column_family(id));

    // Act
    let _ = manifest.apply_edit(&crate::metadata::ManifestEdit::CreateColumnFamily {
        id,
        name: "revived".into(),
        created_at: 99,
    });

    // Assert
    assert!(manifest.column_families[0].deleted_at.is_some());
    assert_eq!(manifest.column_families[0].name, "retired");
}

#[test]
fn should_replay_original_family_creation_without_reviving_a_dropped_family() {
    // Arrange
    let mut manifest = Manifest::default();
    let create = ManifestEdit::CreateColumnFamily {
        id: 1,
        name: "retired".into(),
        created_at: 1,
    };
    manifest.apply_edit(&create).unwrap();
    manifest
        .apply_edit(&ManifestEdit::DropColumnFamilyAt {
            id: 1,
            drop_sequence: 2,
            dropped_sst_names: Vec::new(),
        })
        .unwrap();
    manifest
        .apply_edit(&ManifestEdit::CreateColumnFamily {
            id: 2,
            name: "retired".into(),
            created_at: 3,
        })
        .unwrap();
    let before = serde_json::to_value(&manifest).unwrap();

    // Act
    let result = manifest.apply_edit(&create);

    // Assert
    assert!(
        result.is_ok(),
        "replaying the original create must be harmless"
    );
    assert_eq!(serde_json::to_value(&manifest).unwrap(), before);
    assert_eq!(
        manifest.flush_frontiers().get(1).unwrap(),
        FamilyFrontier::Dropped
    );
}

#[test]
fn should_reject_missing_frontiers_when_decoding_a_manifest() {
    // Arrange
    let mut encoded = serde_json::to_value(Manifest::default()).unwrap();
    encoded.as_object_mut().unwrap().remove("flushed_through");

    // Act
    let decoded = serde_json::from_value::<Manifest>(encoded);

    // Assert
    assert!(
        decoded.is_err(),
        "missing authority must not become default zero"
    );
}

// ========================================================================
// Manifest creation and initialization tests
// ========================================================================

#[test]
fn should_create_default_manifest() {
    // Arrange
    // (no setup)

    // Act
    let manifest = Manifest::default();

    // Assert
    assert_eq!(
        manifest.flush_frontiers().get(0).unwrap(),
        FamilyFrontier::Live {
            through_sequence: 0
        }
    );
    assert!(manifest.files.is_empty());
    assert!(manifest.column_families.is_empty());
    assert!(manifest.next_sst_seqs.is_empty());
}

// ========================================================================
// File management tests
// ========================================================================

#[test]
fn should_add_file_to_manifest() {
    // Arrange
    let mut manifest = Manifest::default();
    let sst_name = crate::cloud_layout::file_name(0, 0, 1);
    let file = FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: 1024,
        cf_id: 0,
        sst_seq: 1,
        ..Default::default()
    };

    // Act
    manifest.add_file(file.clone());

    // Assert
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(manifest.files[0].name, sst_name);
}

#[test]
fn should_treat_missing_key_bounds_completeness_as_untrusted() {
    // Arrange
    let json = r#"{
            "name":"untrusted.sst",
            "level":1,
            "size_bytes":10,
            "cf_id":0,
            "sst_seq":1,
            "smallest_key":"61",
            "largest_key":"7a"
        }"#;

    // Act
    let file: FileMeta = serde_json::from_str(json).expect("deserialize file metadata");

    // Assert
    assert!(!file.key_bounds_complete);
}

#[test]
fn should_add_multiple_files_to_manifest() {
    // Arrange
    let mut manifest = Manifest::default();
    let expected_names: Vec<String> = (0_u64..5)
        .map(|i| {
            crate::cloud_layout::file_name(0, u32::try_from(i).expect("loop index fits in u32"), i)
        })
        .collect();

    // Act
    for i in 0_u64..5 {
        let idx = usize::try_from(i).expect("loop index fits in usize");
        let file = FileMeta {
            name: expected_names[idx].clone(),
            level: u32::try_from(i).expect("loop index fits in u32"),
            size_bytes: 1000 + (i * 100),
            ..Default::default()
        };
        manifest.add_file(file);
    }

    // Assert: every file entry is present with its own distinct
    // name, level, and size (not just an overall count)
    assert_eq!(manifest.files.len(), 5);
    for i in 0_u64..5 {
        let idx = usize::try_from(i).expect("loop index fits in usize");
        let file = manifest
            .files
            .iter()
            .find(|f| f.name == expected_names[idx])
            .unwrap_or_else(|| panic!("file {idx} missing from manifest"));
        assert_eq!(
            file.level,
            u32::try_from(i).expect("loop index fits in u32")
        );
        assert_eq!(file.size_bytes, 1000 + (i * 100));
    }
}

#[test]
fn should_get_files_at_level() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.add_file(FileMeta {
        name: "l0_file1.sst".to_string(),
        level: 0,
        ..Default::default()
    });
    manifest.add_file(FileMeta {
        name: "l0_file2.sst".to_string(),
        level: 0,
        ..Default::default()
    });
    manifest.add_file(FileMeta {
        name: "l1_file1.sst".to_string(),
        level: 1,
        ..Default::default()
    });

    // Act
    let level_0 = manifest.files_at_level(0);
    let level_1 = manifest.files_at_level(1);
    let level_2 = manifest.files_at_level(2);

    // Assert
    assert_eq!(level_0.len(), 2);
    assert_eq!(level_1.len(), 1);
    assert!(level_2.is_empty());
}

#[test]
fn should_remove_file_by_name() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.add_file(FileMeta {
        name: "file_to_keep.sst".to_string(),
        ..Default::default()
    });
    manifest.add_file(FileMeta {
        name: "file_to_remove.sst".to_string(),
        ..Default::default()
    });

    // Act
    manifest.remove_file("file_to_remove.sst");

    // Assert
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(manifest.files[0].name, "file_to_keep.sst");
}

#[test]
fn should_remove_all_matching_files() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.add_file(FileMeta {
        name: "remove_me.sst".to_string(),
        ..Default::default()
    });
    manifest.add_file(FileMeta {
        name: "remove_me.sst".to_string(),
        ..Default::default()
    });

    // Act
    manifest.remove_file("remove_me.sst");

    // Assert: all matching files removed
    assert!(manifest.files.is_empty());
}

#[test]
fn should_not_remove_non_existent_file() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.add_file(FileMeta {
        name: "exists.sst".to_string(),
        ..Default::default()
    });

    // Act
    manifest.remove_file("does_not_exist.sst");

    // Assert: no change
    assert_eq!(manifest.files.len(), 1);
}

// ========================================================================
// Column family management tests
// ========================================================================

#[test]
fn should_get_next_cf_id() {
    // Arrange
    let mut manifest = Manifest::default();

    // Act
    let id1 = manifest.next_cf_id().expect("next column family id");
    manifest.create_column_family("cf1".to_string());
    let id2 = manifest.next_cf_id().expect("next column family id");

    // Assert
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);
}

#[test]
fn should_return_resource_limit_when_column_family_id_space_is_exhausted() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest
        .apply_edit(&ManifestEdit::CreateColumnFamily {
            id: u32::MAX,
            name: "last-column-family".into(),
            created_at: 0,
        })
        .unwrap();

    // Act
    let result = manifest.next_cf_id();

    // Assert
    assert!(matches!(
        result,
        Err(crate::common::MidgeError::ResourceLimit(message))
            if message.contains("column family")
    ));
}

#[test]
fn should_create_column_family() {
    // Arrange
    let mut manifest = Manifest::default();

    // Act
    let cf_id = manifest.create_column_family("my_cf".to_string());

    // Assert
    assert_eq!(cf_id, 1);
    assert_eq!(manifest.column_families.len(), 1);
    assert_eq!(manifest.column_families[0].id, 1);
    assert_eq!(manifest.column_families[0].name, "my_cf");
    assert!(manifest.column_families[0].deleted_at.is_none());
}

#[test]
fn should_auto_increment_cf_ids() {
    // Arrange
    let mut manifest = Manifest::default();

    // Act
    let id1 = manifest.create_column_family("cf1".to_string());
    let id2 = manifest.create_column_family("cf2".to_string());
    let id3 = manifest.create_column_family("cf3".to_string());

    // Assert
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);
    assert_eq!(id3, 3);
}

#[test]
fn should_get_column_family_by_name() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.create_column_family("my_cf".to_string());

    // Act
    let cf = manifest.get_column_family_by_name("my_cf");

    // Assert
    assert!(cf.is_some());
    assert_eq!(cf.unwrap().name, "my_cf");
}

#[test]
fn should_return_none_for_non_existent_cf_name() {
    // Arrange
    let manifest = Manifest::default();

    // Act
    let cf = manifest.get_column_family_by_name("does_not_exist");

    // Assert
    assert!(cf.is_none());
}

#[test]
fn should_get_column_family_by_id() {
    // Arrange
    let mut manifest = Manifest::default();
    let cf_id = manifest.create_column_family("my_cf".to_string());

    // Act
    let cf = manifest.get_column_family_by_id(cf_id);

    // Assert
    assert!(cf.is_some());
    assert_eq!(cf.unwrap().id, cf_id);
}

#[test]
fn should_return_none_for_non_existent_cf_id() {
    // Arrange
    let manifest = Manifest::default();

    // Act
    let cf = manifest.get_column_family_by_id(999);

    // Assert
    assert!(cf.is_none());
}

#[test]
fn should_return_active_column_families() {
    // Arrange
    let mut manifest = Manifest::default();
    manifest.create_column_family("cf1".to_string());
    manifest.create_column_family("cf2".to_string());

    // Act
    let active = manifest.active_column_families();

    // Assert
    assert_eq!(active.len(), 2);
}

#[test]
fn should_exclude_deleted_column_families() {
    // Arrange
    let mut manifest = Manifest::default();
    let id1 = manifest.create_column_family("cf1".to_string());
    let id2 = manifest.create_column_family("cf2".to_string());
    manifest.delete_column_family(id1);

    // Act
    let active = manifest.active_column_families();

    // Assert: only cf2 is active
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, id2);
}

#[test]
fn should_delete_column_family_by_id() {
    // Arrange
    let mut manifest = Manifest::default();
    let cf_id = manifest.create_column_family("to_delete".to_string());

    // Act: delete returns true and marks deleted_at
    let deleted = manifest.delete_column_family(cf_id);

    // Assert
    assert!(deleted);
    // After deletion, get_column_family_by_id returns None (filters out deleted)
    let cf = manifest.get_column_family_by_id(cf_id);
    assert!(cf.is_none());
    // But the CF still exists in the list with deleted_at set
    let all_cfs = &manifest.column_families;
    let deleted_cf = all_cfs.iter().find(|cf| cf.id == cf_id).unwrap();
    assert!(deleted_cf.deleted_at.is_some());
}

#[test]
fn should_set_deleted_at_timestamp() {
    // Arrange
    let mut manifest = Manifest::default();
    let cf_id = manifest.create_column_family("cf".to_string());
    let before = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .expect("timestamp fits in u64");

    // Act
    manifest.delete_column_family(cf_id);
    let after = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .expect("timestamp fits in u64");

    // Assert: deleted_at is set and within time window
    let cf = manifest
        .column_families
        .iter()
        .find(|cf| cf.id == cf_id)
        .unwrap();
    assert!(cf.deleted_at.is_some());
    let deleted_at = cf.deleted_at.unwrap();
    assert!(deleted_at >= before);
    assert!(deleted_at <= after);
}

// ========================================================================
// Integration tests
// ========================================================================

#[test]
fn should_manage_files_across_levels() {
    // Arrange
    let mut manifest = Manifest::default();

    // Act: Add files to different levels
    for level in 0..4 {
        for i in 0..3 {
            manifest.add_file(FileMeta {
                name: format!("l{level}_f{i}.sst"),
                level,
                ..Default::default()
            });
        }
    }

    // Assert
    for level in 0..4 {
        assert_eq!(manifest.files_at_level(level).len(), 3);
    }
    assert_eq!(manifest.files.len(), 12);
}

#[test]
fn should_handle_multiple_column_families_with_files() {
    // Arrange
    let mut manifest = Manifest::default();

    // Act
    let cf1_id = manifest.create_column_family("cf1".to_string());
    let cf2_id = manifest.create_column_family("cf2".to_string());

    manifest.add_file(FileMeta {
        cf_id: cf1_id,
        name: "cf1_file.sst".to_string(),
        ..Default::default()
    });
    manifest.add_file(FileMeta {
        cf_id: cf2_id,
        name: "cf2_file.sst".to_string(),
        ..Default::default()
    });

    // Assert
    assert_eq!(manifest.column_families.len(), 2);
    assert_eq!(manifest.files.len(), 2);
    assert_eq!(manifest.files[0].cf_id, cf1_id);
    assert_eq!(manifest.files[1].cf_id, cf2_id);
}

#[test]
fn should_reclaim_late_manifest_file_when_column_family_was_already_reclaimed() {
    // Arrange: model a legacy crash/race where compaction published after
    // the drop's original frozen file set had already been reclaimed.
    let mut manifest = Manifest::default();
    let cf_id = manifest.create_column_family("dropped".to_string());
    assert!(manifest.delete_column_family_with_reclamation(cf_id, 10, Vec::new()));
    assert!(manifest.mark_column_family_reclaimed(cf_id, &[]));
    let late_output = crate::cloud_layout::file_name(cf_id, 1, 99);
    manifest.files.push(FileMeta {
        name: late_output.clone(),
        cf_id,
        level: 1,
        ..Default::default()
    });

    // Act
    let candidates = manifest.reclaimable_column_family_files(None);

    // Assert
    assert_eq!(candidates, vec![(cf_id, vec![late_output])]);
}
