use super::*;

#[test]
fn should_preserve_original_delete_obligation_when_removed_manifest_is_checkpointed() {
    // Arrange
    let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());
    let mut manifest = Manifest::default();
    let original = FileMeta {
        name: crate::cloud_layout::file_name(0, 1, 7),
        cf_id: 0,
        level: 1,
        sst_seq: 7,
        size_bytes: 123,
        content_crc32c: Some(0x1234_5678),
        ..FileMeta::default()
    };
    manifest.add_file(original.clone());
    let store = crate::metadata::store::ManifestStore::new(Arc::clone(&fs));
    let edit = ManifestEdit::RemoveSst {
        name: original.name.clone(),
    };
    crate::metadata::ManifestPersistence::save_with_fs(&fs, &manifest).unwrap();

    // Act: retirement is one journal decision; a subsequent checkpoint carries
    // its obligation even after the process-local cleanup queue disappears.
    let id = store
        .append_for(crate::metadata::accounting::Origin::Administration, &edit)
        .unwrap();
    manifest.apply_edit(&edit).unwrap();
    manifest.note_applied_journal_edit(id);
    let checkpoint = store.save_snapshot(&manifest).unwrap();
    checkpoint.adopt_into(&mut manifest);
    let recovered = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
        &fs,
        crate::config::RecoveryPolicy::Strict,
    )
    .unwrap();

    // Assert
    assert!(recovered.files.is_empty());
    assert!(recovered.retired_ssts[&original.name].same_identity(&original));
    assert_eq!(
        recovered.retired_ssts[&original.name].content_crc32c,
        Some(0x1234_5678)
    );
    store
        .append_for(
            crate::metadata::accounting::Origin::Administration,
            &ManifestEdit::CompleteSstDelete { file: original },
        )
        .unwrap();
    let confirmed = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
        &fs,
        crate::config::RecoveryPolicy::Strict,
    )
    .unwrap();
    assert!(confirmed.retired_ssts.is_empty());
}

#[test]
fn should_keep_active_identity_when_delete_confirmation_targets_live_sst() {
    // Arrange
    let mut manifest = Manifest::default();
    let original = FileMeta {
        name: crate::cloud_layout::file_name(0, 1, 7),
        ..FileMeta::default()
    };
    manifest.add_file(original.clone());

    // Act
    let result = manifest.apply_edit(&ManifestEdit::CompleteSstDelete {
        file: original.clone(),
    });

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert!(manifest.files[0].same_identity(&original));
}

#[test]
fn should_reject_identity_reuse_when_retirement_and_add_share_a_batch() {
    // Arrange
    let mut manifest = Manifest::default();
    let original = FileMeta {
        name: crate::cloud_layout::file_name(0, 1, 7),
        level: 1,
        ..FileMeta::default()
    };
    manifest.add_file(original.clone());
    let before = serde_json::to_value(&manifest).unwrap();

    // Act
    let result = manifest.apply_edit(&ManifestEdit::Batch(vec![
        ManifestEdit::RemoveSst {
            name: original.name.clone(),
        },
        ManifestEdit::AddSst(original),
    ]));

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(serde_json::to_value(&manifest).unwrap(), before);
}
