use super::*;

#[test]
fn should_reopen_populated_fixture_contents_when_encoded_as_format_nine() {
    // Arrange: the current compatibility cut has binary metadata and V5 SST
    // bytes. Re-encoding it must preserve its exact logical contents.
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/compatibility/v9_populated_v5_sst_db");
    let manifest: cntryl_midge_durability::metadata::Manifest =
        cntryl_midge_durability::metadata::codec::decode(
            cntryl_midge_durability::metadata::codec::MANIFEST,
            &std::fs::read(fixture.join("manifest.snapshot.json")).unwrap(),
        )
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let fs = cntryl_midge_core::io::real::test_root(directory.path());
    cntryl_midge_durability::metadata::format::ensure_or_create_format_marker(
        &fs,
        directory.path(),
    )
    .unwrap();
    cntryl_midge_durability::metadata::ManifestPersistence::save_with_fs(&fs, &manifest).unwrap();
    cntryl_midge_durability::metadata::intent_persistence::IntentPersistence::save_with_fs(
        &fs,
        &[],
    )
    .unwrap();
    std::fs::create_dir_all(directory.path().join("sst")).unwrap();
    for file in &manifest.files {
        std::fs::copy(
            fixture.join("sst").join(&file.name),
            directory.path().join("sst").join(&file.name),
        )
        .unwrap();
    }
    let encoded = std::fs::read(directory.path().join("manifest.snapshot.json")).unwrap();
    let intents = std::fs::read(directory.path().join("intent_log.json")).unwrap();

    // Act
    let report = StorageVerifier::verify_path(directory.path()).unwrap();
    let mut engine = Engine::open(OpenOptions::local(directory.path()).build().unwrap()).unwrap();
    let cf = engine.get_column_family("default").unwrap();
    let transaction = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    let value = transaction.get(b"fixture/alpha").unwrap();
    drop(transaction);
    engine.shutdown(std::time::Duration::from_secs(5)).unwrap();

    // Assert / retain exact encoded fixture evidence for the release copy.
    assert_eq!(value.as_deref(), Some(b"value-alpha".as_slice()));
    assert_eq!(report.sst_files_verified, 1);
    assert_eq!(
        cntryl_midge_durability::metadata::codec::encode(
            cntryl_midge_durability::metadata::codec::MANIFEST,
            &cntryl_midge_durability::metadata::Manifest::default(),
        )
        .unwrap(),
        include_bytes!("../../../tests/fixtures/compatibility/v9_empty_db/manifest.snapshot.json")
    );
    println!("format9_manifest_hex={}", hex::encode(encoded));
    println!("format9_intent_hex={}", hex::encode(intents));
    println!(
        "format9_empty_manifest_hex={}",
        hex::encode(
            cntryl_midge_durability::metadata::codec::encode(
                cntryl_midge_durability::metadata::codec::MANIFEST,
                &cntryl_midge_durability::metadata::Manifest::default(),
            )
            .unwrap()
        )
    );
}
