//! Real writer output retained as the decoder corpus and compatibility cut.
use super::*;
use crate::sst::traits::DynSstWriter;
use std::path::Path;

fn seed_root() -> Option<std::path::PathBuf> {
    std::env::var_os("MIDGE_SST_V5_FIXTURE_DIR").map(Into::into)
}

fn retain_seed(target: &str, name: &str, bytes: &[u8]) {
    if let Some(root) = seed_root() {
        let directory = root.join("corpus").join(target);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(name), bytes).unwrap();
    } else {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fuzz/corpus")
            .join(target)
            .join(name);
        assert_eq!(
            std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display())),
            bytes
        );
    }
}

fn add_seed_entries(writer: &mut dyn DynSstWriter, seed: u64) -> MidgeResult<()> {
    let entries = if seed == 7 { 2 } else { 64 + seed * 7 };
    for index in 0..entries {
        let mut key = if seed == 7 {
            vec![b'p'; 70_000]
        } else {
            Vec::new()
        };
        key.extend_from_slice(format!("seed/{seed}/row/{index:04}").as_bytes());
        if seed == 0 && index == 0 {
            key.clear();
        }
        let deleted = seed == 3 && index % 3 == 0;
        let value = vec![u8::try_from(seed).unwrap(); ((seed + 1) * 13) as usize];
        writer.add_sorted_with_meta(
            &key,
            (!deleted).then_some(value.as_slice()),
            1000 + index,
            if deleted {
                EntryType::Delete
            } else if seed == 5 {
                EntryType::Insert
            } else {
                EntryType::Put
            },
            (seed == 2).then_some(u64::MAX),
        )?;
        if seed == 4 && index == 16 {
            for sequence in (900..=999).rev() {
                writer.add_sorted_with_meta(
                    &key,
                    Some(b"older-version"),
                    sequence,
                    EntryType::Put,
                    None,
                )?;
            }
        }
    }
    writer.add_range_tombstone(b"zrange/start", b"zrange/stop", seed + 1)
}

fn retain_real_sst_blocks(directory: &Path, reader: &SstFileIo, seed: u64) -> MidgeResult<()> {
    let name = format!("real-v5-{seed}");
    let footer = reader.footer.as_ref().unwrap();
    let data = reader.read_block(&reader.index_entries()?[0].1)?;
    let index = reader.read_block(&footer.index_handle)?;
    let filter = reader.read_block(&footer.block_bloom_handle.unwrap())?;
    let range = reader.read_block(&reader.range_tombstone_handle.unwrap())?;
    retain_seed("sst_data_block_decode", &name, &data);
    retain_seed("sst_index_decode", &name, &index);
    retain_seed("sst_filter_decode", &name, &filter);
    retain_seed("sst_block_decode", &format!("{name}-ranges"), &range);
    let file = std::fs::read(directory.join("seed.sst")).unwrap();
    retain_seed(
        "sst_block_decode",
        &format!("{name}-footer"),
        &file[file.len() - crate::sst::types::SST_FOOTER_SIZE..],
    );
    assert!(reader.decode_scan_block(&data)?.len() > 0);
    assert!(crate::sst::bloom::BlockBloomFilter::deserialize(&filter).is_ok());
    Ok(())
}

fn retain_real_metadata(directory: &Path, fs: &Arc<dyn Fs>, seed: u64) -> MidgeResult<()> {
    let mut manifest = crate::metadata::Manifest::default();
    manifest.next_sst_seqs.insert(0, seed + 2);
    crate::metadata::ManifestPersistence::save_with_fs(fs, &manifest)
        .map_err(MidgeError::Internal)?;
    crate::metadata::append_edit(
        directory,
        &crate::metadata::ManifestEdit::BumpNextSstSeq {
            cf_id: 0,
            next_seq: 200 + seed,
        },
    )?;
    for (selector, file) in [(0, "manifest.journal"), (1, "manifest.snapshot.json")] {
        let mut input = vec![selector];
        input.extend_from_slice(&std::fs::read(directory.join(file)).unwrap());
        retain_seed(
            "manifest_journal_replay",
            &format!("real-v9-{seed}-{selector}"),
            &input,
        );
    }
    Ok(())
}

#[test]
fn should_match_eight_real_file_seeds_for_each_sst_and_metadata_decoder() -> MidgeResult<()> {
    // Arrange and Act: each seed is a real sorted writer publication followed
    // by production footer, checksum, index, filter and data decoding.
    for seed in 0..8 {
        let directory = tempfile::tempdir()?;
        let fs = crate::io::real::test_root(directory.path());
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 64 * 1024)
            .with_compression_policy(crate::codec::CompressionPolicy::Fixed(
                crate::codec::CompressionAlgo::None,
            ));
        let mut writer = factory.create()?;
        add_seed_entries(writer.as_mut(), seed)?;
        crate::sst::fs::finish_writer_to_path(writer, &directory.path().join("seed.sst"))?;
        let reader = SstFileIo::open("seed.sst", fs.clone())?;
        // Assert: checked-in corpora equal these exact decoded production cuts.
        retain_real_sst_blocks(directory.path(), &reader, seed)?;
        retain_real_metadata(directory.path(), &fs, seed)?;
    }
    Ok(())
}

fn write_current_fixture(directory: &Path) -> MidgeResult<()> {
    let fs = crate::io::real::test_root(directory);
    std::fs::create_dir_all(directory.join("sst"))?;
    crate::metadata::ensure_or_create_format_marker(&fs, directory)?;
    let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 64 * 1024).with_compression_policy(
        crate::codec::CompressionPolicy::Fixed(crate::codec::CompressionAlgo::Lz4),
    );
    let mut writer = factory.create()?;
    writer.add_sorted_with_meta(
        b"fixture/alpha",
        Some(b"value-alpha"),
        2,
        EntryType::Put,
        None,
    )?;
    writer.add_sorted_with_meta(b"fixture/empty", Some(b""), 3, EntryType::Put, None)?;
    writer.add_sorted_with_meta(
        b"fixture/structured",
        Some(&b"account=0042|region=east|state=stable|".repeat(128)),
        4,
        EntryType::Put,
        None,
    )?;
    let name = "000000_00_00000000000000000001.sst";
    let finalized = writer
        .finish_to_path_with_summary(&directory.join("sst").join(name))?
        .unwrap();
    let summary = finalized.summary.unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/compatibility/v9_populated_v4_sst_db/manifest.snapshot.json");
    let mut manifest: crate::metadata::Manifest =
        crate::metadata::codec::decode(crate::metadata::codec::MANIFEST, &std::fs::read(source)?)?;
    let file = &mut manifest.files[0];
    file.size_bytes = finalized.size_bytes;
    file.content_crc32c = Some(finalized.content_crc32c);
    file.smallest_key = Some(summary.smallest_key);
    file.largest_key = Some(summary.largest_key);
    file.smallest_seq = Some(summary.smallest_seq);
    file.largest_seq = Some(summary.largest_seq);
    file.earliest_expiration_millis = summary.earliest_expiration_millis;
    crate::metadata::ManifestPersistence::save_with_fs(&fs, &manifest)
        .map_err(MidgeError::Internal)?;
    crate::runtime::IntentPersistence::save_with_fs(&fs, &[]).map_err(MidgeError::Internal)?;
    Ok(())
}

#[test]
fn should_reopen_format_nine_sst_v5_fixture_with_exact_finalized_identity() -> MidgeResult<()> {
    // Arrange
    let temporary = tempfile::tempdir()?;
    let directory = seed_root().map_or_else(
        || temporary.path().to_path_buf(),
        |root| root.join("compatibility/v9_populated_v5_sst_db"),
    );
    std::fs::create_dir_all(&directory)?;
    write_current_fixture(&directory)?;
    // Act
    let report = crate::StorageVerifier::verify_path(&directory)?;
    let mut engine = crate::Engine::open(crate::OpenOptions::local(&directory).build()?)?;
    let cf = engine.get_column_family("default").unwrap();
    let transaction = engine.begin_tx(cf.id(), crate::TransactionMode::ReadOnly)?;
    let value = transaction.get(b"fixture/alpha")?;
    drop(transaction);
    engine.shutdown(std::time::Duration::from_secs(5))?;
    // Assert
    assert_eq!(value.as_deref(), Some(b"value-alpha".as_slice()));
    assert_eq!(report.sst_files_verified, 1);
    if let Some(root) = seed_root() {
        std::fs::write(
            root.join("verification_healthy_v5.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )?;
    } else {
        let expected = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/compatibility/v9_populated_v5_sst_db");
        for file in [
            "FORMAT",
            "manifest.snapshot.json",
            "sst/000000_00_00000000000000000001.sst",
        ] {
            assert_eq!(
                std::fs::read(directory.join(file))?,
                std::fs::read(expected.join(file))?
            );
        }
    }
    Ok(())
}
