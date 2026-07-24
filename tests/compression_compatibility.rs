use cntryl_midge::sst::compression::{
    compress_block_with_trailer, decompress_block_with_trailer, CompressionAlgo, CompressionPolicy,
    BLOCK_TRAILER_SIZE,
};
use cntryl_midge::{
    Engine, EngineHealth, Goal, OpenOptions, Query, RecoveryPolicy, TransactionMode,
    WorkloadProfile, WriteOptions,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use xxhash_rust::xxh3::xxh3_64;

const BASELINE_FIXTURE_DATA_DIGEST: u64 = 0x0009_2c66_9deb_80b7;

fn structured_block(size: usize) -> Vec<u8> {
    let pattern = b"account=0042|region=east|status=active|segment=business|";
    pattern.iter().copied().cycle().take(size).collect()
}

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compatibility")
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let destination = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &destination)?;
        } else {
            fs::copy(entry.path(), destination)?;
        }
    }
    Ok(())
}

fn copy_compressed_fixture() -> tempfile::TempDir {
    let source = fixtures_root().join("v2_compressed_sst_db");
    let destination = tempfile::tempdir().expect("create fixture copy");
    copy_dir_recursive(&source, destination.path()).expect("copy compressed fixture");
    destination
}

fn baseline_fixture_value(index: usize) -> Vec<u8> {
    let record = format!(
        "customer={index:04}|region={}|status=active|segment={}|",
        ["east", "west", "north", "south"][index % 4],
        ["consumer", "business", "public"][index % 3],
    );
    record
        .as_bytes()
        .iter()
        .copied()
        .cycle()
        .take(8 * 1024)
        .collect()
}

fn baseline_fixture_records() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..512)
        .map(|index| {
            (
                format!("fixture:key:{index:04}").into_bytes(),
                baseline_fixture_value(index),
            )
        })
        .collect()
}

fn adaptive_records(prefix: &str, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|index| {
            let pattern = format!(
                "order={index:04}|region={}|state=committed|class={}|",
                ["apac", "emea", "amer"][index % 3],
                ["standard", "priority"][index % 2],
            );
            let value = pattern
                .as_bytes()
                .iter()
                .copied()
                .cycle()
                .take(4 * 1024)
                .collect();
            (format!("{prefix}:{index:04}").into_bytes(), value)
        })
        .collect()
}

fn canonical_data_digest<'a>(rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>) -> u64 {
    let mut canonical = Vec::new();
    for (key, value) in rows {
        canonical.extend_from_slice(
            &u32::try_from(key.len())
                .expect("test key length fits in u32")
                .to_le_bytes(),
        );
        canonical.extend_from_slice(key);
        canonical.extend_from_slice(
            &u32::try_from(value.len())
                .expect("test value length fits in u32")
                .to_le_bytes(),
        );
        canonical.extend_from_slice(value);
    }
    xxh3_64(&canonical)
}

fn local_throughput_options(path: &Path) -> cntryl_midge::OpenOptions {
    OpenOptions::local(path)
        .goal(Goal::Throughput)
        .workload(WorkloadProfile::WriteHeavy)
        .recovery_policy(RecoveryPolicy::Strict)
        .background_compaction(false)
        .build()
        .expect("build local throughput options")
}

fn write_records_and_flush(
    engine: &Engine,
    column_family: &cntryl_midge::ColumnFamilyHandle,
    records: &[(Vec<u8>, Vec<u8>)],
) {
    let mut transaction = engine
        .begin_tx(column_family.id(), TransactionMode::ReadWrite)
        .expect("begin write transaction");
    for (key, value) in records {
        transaction
            .put(key.clone(), value.clone(), None)
            .expect("write compression test record");
    }
    transaction
        .commit(WriteOptions::sync())
        .expect("commit compression test records");
    engine
        .flush_cf(column_family)
        .expect("complete compression test flush");
}

fn write_fresh_adaptive_database(path: &Path, records: &[(Vec<u8>, Vec<u8>)]) {
    let mut engine =
        Engine::open(local_throughput_options(path)).expect("open adaptive test database");
    let column_family = engine
        .create_column_family("adaptive")
        .expect("create adaptive column family");
    write_records_and_flush(&engine, &column_family, records);
    engine
        .shutdown(Duration::from_secs(10))
        .expect("clean adaptive database shutdown");
}

fn sorted_sst_files(path: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files: Vec<_> = fs::read_dir(path.join("sst"))
        .expect("read SST directory")
        .map(|entry| {
            let entry = entry.expect("read SST entry");
            (
                PathBuf::from(entry.file_name()),
                fs::read(entry.path()).expect("read SST file"),
            )
        })
        .collect();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

#[test]
fn should_preserve_exact_sst_codec_codes() {
    // Arrange
    let expected = [
        (CompressionAlgo::None, 0),
        (CompressionAlgo::Lz4, 1),
        (CompressionAlgo::Zstd3, 2),
        (CompressionAlgo::Zstd9, 3),
        (CompressionAlgo::Zlib, 4),
        (CompressionAlgo::Snappy, 5),
    ];

    // Act
    for (algorithm, code) in expected {
        // Assert
        assert_eq!(algorithm.to_u8(), code);
        assert_eq!(CompressionAlgo::from_u8(code), Some(algorithm));
    }
    assert_eq!(CompressionAlgo::from_u8(6), None);
    assert_eq!(CompressionAlgo::from_u8(u8::MAX), None);
}

#[test]
fn should_preserve_five_byte_trailer_layout_with_crc_coverage() {
    // Arrange
    let data = b"trailer-format-fixture";

    // Act
    let block = compress_block_with_trailer(data, &CompressionPolicy::None)
        .expect("build checksummed raw block");
    let algorithm_offset = block.len() - BLOCK_TRAILER_SIZE;
    let crc_offset = block.len() - size_of::<u32>();
    let stored_crc = u32::from_le_bytes(
        block[crc_offset..]
            .try_into()
            .expect("four-byte CRC32C trailer"),
    );

    // Assert
    assert_eq!(BLOCK_TRAILER_SIZE, 5);
    assert_eq!(&block[..algorithm_offset], data);
    assert_eq!(block[algorithm_offset], CompressionAlgo::None.to_u8());
    assert_eq!(stored_crc, crc32c::crc32c(&block[..crc_offset]));
}

#[test]
fn should_preserve_baseline_compressed_block_fixture() {
    // Arrange
    let data = structured_block(16 * 1024);
    let cases = [
        (
            CompressionPolicy::Fixed(CompressionAlgo::Lz4),
            CompressionAlgo::Lz4,
            0xf8ab_776d_208c_bd15_u64,
        ),
        (
            CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
            CompressionAlgo::Zstd3,
            0x4e7b_d7fc_d9a0_d5c5_u64,
        ),
        (
            CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
            CompressionAlgo::Zstd9,
            0xe2b7_653b_ded1_b28e_u64,
        ),
    ];

    // Act
    for (policy, expected_algorithm, expected_digest) in cases {
        let block =
            compress_block_with_trailer(&data, &policy).expect("compress baseline block fixture");
        let algorithm_offset = block.len() - BLOCK_TRAILER_SIZE;
        let actual_digest = xxh3_64(&block);
        eprintln!(
            "{expected_algorithm:?}: len={}, xxh3_64={actual_digest:016x}",
            block.len()
        );

        // Assert
        assert_eq!(block[algorithm_offset], expected_algorithm.to_u8());
        assert_eq!(actual_digest, expected_digest);
    }
}

#[test]
fn should_roundtrip_every_emitted_sst_block_deterministically() {
    // Arrange
    let data = structured_block(16 * 1024);
    let policies = [
        CompressionPolicy::None,
        CompressionPolicy::Fixed(CompressionAlgo::None),
        CompressionPolicy::Fixed(CompressionAlgo::Lz4),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd3),
        CompressionPolicy::Fixed(CompressionAlgo::Zstd9),
        CompressionPolicy::Adaptive {
            min_savings_bytes: 256,
            min_ratio: 0.95,
            check_algorithms: vec![
                CompressionAlgo::None,
                CompressionAlgo::Lz4,
                CompressionAlgo::Zstd3,
            ],
        },
    ];

    // Act
    for policy in policies {
        let first = compress_block_with_trailer(&data, &policy).expect("first compression");
        let second = compress_block_with_trailer(&data, &policy).expect("second compression");
        let decoded = decompress_block_with_trailer(&first).expect("roundtrip block");

        // Assert
        assert_eq!(first, second, "policy must be deterministic: {policy:?}");
        assert_eq!(decoded.as_ref(), data.as_slice());
    }
}

#[test]
fn should_reject_invalid_sst_block_trailers() {
    // Arrange
    let data = structured_block(1024);
    let valid =
        compress_block_with_trailer(&data, &CompressionPolicy::None).expect("build valid block");
    let mut corrupt = valid.to_vec();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0x01;

    let mut unknown = data.clone();
    unknown.push(u8::MAX);
    let crc = crc32c::crc32c(&unknown);
    unknown.extend_from_slice(&crc.to_le_bytes());

    // Act
    let corrupt_error = decompress_block_with_trailer(&corrupt).expect_err("corrupt CRC must fail");
    let truncated_error =
        decompress_block_with_trailer(&valid[..4]).expect_err("short trailer must fail");
    let unknown_error =
        decompress_block_with_trailer(&unknown).expect_err("unknown codec must fail");

    // Assert
    assert!(corrupt_error.to_string().contains("CRC32C mismatch"));
    assert!(truncated_error
        .to_string()
        .contains("too small for trailer"));
    assert!(unknown_error
        .to_string()
        .contains("unknown compression algorithm code"));
}

#[test]
fn should_preserve_candidate_adaptive_sst_across_strict_reopen() {
    // Arrange
    let temp = tempfile::tempdir().expect("create candidate database");
    let records = adaptive_records("adaptive:key", 384);
    let expected_digest = canonical_data_digest(
        records
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    );
    write_fresh_adaptive_database(temp.path(), &records);

    // Act
    let report = Engine::verify_path(temp.path()).expect("verify candidate adaptive database");
    let mut reopened =
        Engine::open(local_throughput_options(temp.path())).expect("strictly reopen candidate");
    let column_family = reopened
        .get_column_family("adaptive")
        .expect("reopen adaptive column family");
    let read = reopened
        .begin_tx(column_family.id(), TransactionMode::ReadOnly)
        .expect("begin candidate read");
    let first = read.get(b"adaptive:key:0000").expect("read first key");
    let middle = read.get(b"adaptive:key:0192").expect("read middle key");
    let last = read.get(b"adaptive:key:0383").expect("read last key");
    let rows = read
        .scan(&Query::new())
        .expect("scan candidate SST")
        .try_collect()
        .expect("collect candidate scan");
    drop(read);

    // Assert
    assert_eq!(report.health, EngineHealth::Healthy);
    assert!(report.authoritative);
    assert!(report.sst_files_verified >= 1);
    assert_eq!(first.as_deref(), Some(records[0].1.as_slice()));
    assert_eq!(middle.as_deref(), Some(records[192].1.as_slice()));
    assert_eq!(last.as_deref(), Some(records[383].1.as_slice()));
    assert_eq!(rows.len(), records.len());
    assert_eq!(
        canonical_data_digest(
            rows.iter()
                .map(|(key, value)| (key.as_ref(), value.as_ref()))
        ),
        expected_digest
    );
    reopened
        .shutdown(Duration::from_secs(10))
        .expect("shutdown reopened candidate");
}

#[test]
fn should_reopen_mixed_blocks_after_appending_candidate_to_baseline_fixture() {
    // Arrange
    let temp = copy_compressed_fixture();
    let new_records = adaptive_records("fixture:new", 128);
    let mut engine =
        Engine::open(local_throughput_options(temp.path())).expect("open baseline fixture copy");
    let column_family = engine
        .get_column_family("fixture")
        .expect("baseline fixture column family");
    write_records_and_flush(&engine, &column_family, &new_records);
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shutdown mixed fixture");

    let mut expected_records = baseline_fixture_records();
    expected_records.extend(new_records.clone());
    expected_records.sort_by(|left, right| left.0.cmp(&right.0));
    let expected_digest = canonical_data_digest(
        expected_records
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    );

    // Act
    let report = Engine::verify_path(temp.path()).expect("verify mixed fixture database");
    let mut reopened =
        Engine::open(local_throughput_options(temp.path())).expect("strictly reopen mixed fixture");
    let column_family = reopened
        .get_column_family("fixture")
        .expect("reopen fixture column family");
    let read = reopened
        .begin_tx(column_family.id(), TransactionMode::ReadOnly)
        .expect("begin mixed read");
    let legacy = read.get(b"fixture:key:0256").expect("read legacy key");
    let candidate = read.get(b"fixture:new:0064").expect("read candidate key");
    let rows = read
        .scan(&Query::new())
        .expect("scan mixed SSTs")
        .try_collect()
        .expect("collect mixed scan");
    drop(read);

    // Assert
    assert_eq!(report.health, EngineHealth::Healthy);
    assert!(report.sst_files_verified >= 2);
    assert_eq!(
        legacy.as_deref(),
        Some(baseline_fixture_value(256).as_slice())
    );
    assert_eq!(candidate.as_deref(), Some(new_records[64].1.as_slice()));
    assert_eq!(rows.len(), 640);
    assert_eq!(
        canonical_data_digest(
            rows.iter()
                .map(|(key, value)| (key.as_ref(), value.as_ref()))
        ),
        expected_digest
    );
    assert_eq!(
        canonical_data_digest(
            baseline_fixture_records()
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
        ),
        BASELINE_FIXTURE_DATA_DIGEST
    );
    reopened
        .shutdown(Duration::from_secs(10))
        .expect("shutdown reopened mixed fixture");
}

#[test]
fn should_strictly_reopen_completed_adaptive_compaction() {
    // Arrange
    let temp = tempfile::tempdir().expect("create compacted database");
    let first_batch = adaptive_records("compacted:a", 192);
    let second_batch = adaptive_records("compacted:b", 192);
    let mut expected_records = first_batch.clone();
    expected_records.extend(second_batch.clone());
    expected_records.sort_by(|left, right| left.0.cmp(&right.0));
    let expected_digest = canonical_data_digest(
        expected_records
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    );

    let mut engine =
        Engine::open(local_throughput_options(temp.path())).expect("open compacted database");
    let column_family = engine
        .create_column_family("adaptive")
        .expect("create compacted column family");
    write_records_and_flush(&engine, &column_family, &first_batch);
    write_records_and_flush(&engine, &column_family, &second_batch);

    // Act
    engine
        .compact_all()
        .expect("complete adaptive SST compaction");
    engine
        .shutdown(Duration::from_secs(10))
        .expect("cleanly shut down compacted database");
    let report = Engine::verify_path(temp.path()).expect("verify compacted database");
    let mut reopened =
        Engine::open(local_throughput_options(temp.path())).expect("strictly reopen compaction");
    let column_family = reopened
        .get_column_family("adaptive")
        .expect("reopen compacted column family");
    let read = reopened
        .begin_tx(column_family.id(), TransactionMode::ReadOnly)
        .expect("begin compacted read");
    let first = read.get(b"compacted:a:0000").expect("read first key");
    let middle = read.get(b"compacted:a:0191").expect("read middle key");
    let last = read.get(b"compacted:b:0191").expect("read last key");
    let rows = read
        .scan(&Query::new())
        .expect("scan compacted SST")
        .try_collect()
        .expect("collect compacted scan");
    drop(read);

    // Assert
    assert_eq!(report.health, EngineHealth::Healthy);
    assert!(report.authoritative);
    assert!(report.sst_files_verified >= 1);
    assert_eq!(first.as_deref(), Some(first_batch[0].1.as_slice()));
    assert_eq!(middle.as_deref(), Some(first_batch[191].1.as_slice()));
    assert_eq!(last.as_deref(), Some(second_batch[191].1.as_slice()));
    assert_eq!(rows.len(), expected_records.len());
    assert_eq!(
        canonical_data_digest(
            rows.iter()
                .map(|(key, value)| (key.as_ref(), value.as_ref()))
        ),
        expected_digest
    );
    reopened
        .shutdown(Duration::from_secs(10))
        .expect("shutdown reopened compaction");
}

#[test]
fn should_produce_byte_identical_sst_files_from_identical_adaptive_input() {
    // Arrange
    let first = tempfile::tempdir().expect("create first deterministic database");
    let second = tempfile::tempdir().expect("create second deterministic database");
    let records = adaptive_records("adaptive:key", 384);

    // Act
    write_fresh_adaptive_database(first.path(), &records);
    write_fresh_adaptive_database(second.path(), &records);
    let first_ssts = sorted_sst_files(first.path());
    let second_ssts = sorted_sst_files(second.path());

    // Assert
    assert!(!first_ssts.is_empty());
    assert_eq!(first_ssts, second_ssts);
}
