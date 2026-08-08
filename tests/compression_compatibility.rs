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

fn structured_block(size: usize) -> Vec<u8> {
    let pattern = b"account=0042|region=east|status=active|segment=business|";
    pattern.iter().copied().cycle().take(size).collect()
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

fn local_options(path: &Path, goal: Goal) -> cntryl_midge::OpenOptions {
    OpenOptions::local(path)
        .goal(goal)
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
        Engine::open(local_options(path, Goal::Throughput)).expect("open adaptive test database");
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
        .filter_map(|entry| {
            let entry = entry.expect("read SST entry");
            let path = entry.path();
            (path.is_file()
                && path.extension().and_then(|extension| extension.to_str()) == Some("sst"))
            .then(|| {
                (
                    PathBuf::from(entry.file_name()),
                    fs::read(path).expect("read SST file"),
                )
            })
        })
        .collect();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn sst_block_algorithms(bytes: &[u8]) -> Vec<u8> {
    let footer_start = bytes
        .len()
        .checked_sub(84)
        .expect("V4 SST has fixed footer");
    let mut cursor = 0usize;
    let mut algorithms = Vec::new();
    while cursor < footer_start {
        let length_end = cursor.checked_add(4).expect("block prefix end");
        let payload_len = usize::try_from(u32::from_le_bytes(
            bytes[cursor..length_end]
                .try_into()
                .expect("four-byte block prefix"),
        ))
        .expect("block payload length fits usize");
        let block_end = length_end
            .checked_add(payload_len)
            .expect("block extent fits usize");
        assert!(block_end <= footer_start, "block extends into V4 footer");
        algorithms.push(bytes[block_end - BLOCK_TRAILER_SIZE]);
        cursor = block_end;
    }
    assert_eq!(cursor, footer_start, "blocks exactly precede V4 footer");
    algorithms
}

#[test]
fn should_preserve_exact_sst_codec_codes() {
    // Arrange
    let expected = [
        (CompressionAlgo::None, 0),
        (CompressionAlgo::Lz4, 1),
        (CompressionAlgo::Zstd3, 2),
        (CompressionAlgo::Zstd9, 3),
    ];

    // Act
    for (algorithm, code) in expected {
        // Assert
        assert_eq!(algorithm.to_u8(), code);
        assert_eq!(CompressionAlgo::from_u8(code), Some(algorithm));
    }
    assert_eq!(CompressionAlgo::from_u8(4), None);
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
fn should_reject_nonshipping_codec_codes_without_fallback() {
    // Arrange
    let encoded = [4_u8, 5, u8::MAX].map(|code| {
        let mut block = b"payload".to_vec();
        block.push(code);
        block.extend_from_slice(&crc32c::crc32c(&block).to_le_bytes());
        block
    });

    // Act
    let errors = encoded.map(|block| {
        decompress_block_with_trailer(&block).expect_err("unknown codec must fail closed")
    });

    // Assert
    for error in errors {
        assert!(matches!(error, cntryl_midge::MidgeError::Corruption(_)));
        assert!(error
            .to_string()
            .contains("unknown compression algorithm code"));
    }
}

#[test]
fn should_reject_corrupt_compressed_payload_for_every_shipping_codec() {
    // Arrange
    let data = structured_block(16 * 1024);
    let cases = [
        CompressionAlgo::Lz4,
        CompressionAlgo::Zstd3,
        CompressionAlgo::Zstd9,
    ];

    // Act
    for algorithm in cases {
        let mut block = compress_block_with_trailer(&data, &CompressionPolicy::Fixed(algorithm))
            .expect("compress shipping codec")
            .to_vec();
        match algorithm {
            CompressionAlgo::Lz4 => {
                block[..4].copy_from_slice(&(64 * 1024 * 1024_u32 + 1).to_le_bytes());
            }
            CompressionAlgo::Zstd3 | CompressionAlgo::Zstd9 => block[0] ^= 0xff,
            CompressionAlgo::None => unreachable!(),
        }
        let crc_offset = block.len() - size_of::<u32>();
        let crc = crc32c::crc32c(&block[..crc_offset]);
        block[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        let error = decompress_block_with_trailer(&block)
            .expect_err("codec payload corruption must not fall back to raw bytes");

        // Assert
        assert!(
            matches!(error, cntryl_midge::MidgeError::Corruption(_)),
            "{algorithm:?} returned {error}"
        );
    }
}

#[test]
fn should_roundtrip_edge_case_values_when_written_through_full_sst_pipeline() {
    // Arrange
    let temp = tempfile::tempdir().expect("create database");
    let mut engine = Engine::open(local_options(temp.path(), Goal::Latency)).expect("open engine");
    let cf = engine
        .create_column_family("payloads")
        .expect("create column family");
    let incompressible = seeded_bytes(16 * 1024, 0x8f21_49da);
    let mut write = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin write");
    write
        .put(b"empty".to_vec(), Vec::new(), None)
        .expect("put empty value");
    write
        .put(b"random".to_vec(), incompressible.clone(), None)
        .expect("put incompressible value");
    write.commit(WriteOptions::sync()).expect("commit values");
    engine.flush_cf(&cf).expect("flush values");
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shutdown engine");

    // Act
    let reopened = Engine::open(local_options(temp.path(), Goal::Latency)).expect("reopen engine");
    let cf = reopened
        .get_column_family("payloads")
        .expect("reopen column family");
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin read");

    // Assert
    assert_eq!(
        read.get(b"empty").expect("read empty"),
        Some(Vec::new().into())
    );
    assert_eq!(
        read.get(b"random").expect("read incompressible").as_deref(),
        Some(incompressible.as_slice())
    );
}

fn seeded_bytes(size: usize, seed: u32) -> Vec<u8> {
    let mut state = seed;
    (0..size)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            u8::try_from(state >> 24).expect("shifted state fits u8")
        })
        .collect()
}

#[test]
fn should_preserve_data_given_deliberate_compression_policy_change_when_reopening_populated_database(
) {
    // Arrange
    let temp = tempfile::tempdir().expect("create policy-change database");
    let latency_records = adaptive_records("latency", 48);
    let economy_records = adaptive_records("economy", 48);
    let mut latency =
        Engine::open(local_options(temp.path(), Goal::Latency)).expect("open latency engine");
    let cf = latency
        .create_column_family("policies")
        .expect("create column family");
    write_records_and_flush(&latency, &cf, &latency_records);
    latency
        .shutdown(Duration::from_secs(10))
        .expect("shutdown latency engine");

    // Act
    let mut economy =
        Engine::open(local_options(temp.path(), Goal::Economy)).expect("open economy engine");
    let cf = economy
        .get_column_family("policies")
        .expect("reopen column family");
    write_records_and_flush(&economy, &cf, &economy_records);
    let read = economy
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin cross-policy read");
    let rows = read
        .scan(&Query::new())
        .expect("scan cross-policy rows")
        .try_collect()
        .expect("collect cross-policy rows");

    // Assert
    assert_eq!(rows.len(), latency_records.len() + economy_records.len());
    for (key, value) in latency_records.iter().chain(&economy_records) {
        assert_eq!(
            read.get(key).expect("read cross-policy value").as_deref(),
            Some(value.as_slice())
        );
    }
    drop(read);
    economy
        .shutdown(Duration::from_secs(10))
        .expect("shutdown economy engine");
}

#[test]
fn should_select_current_policy_for_new_blocks_while_preserving_prior_algorithm_when_compacting_after_goal_change(
) {
    // Arrange
    let temp = tempfile::tempdir().expect("create policy-change database");
    let batches = (0..4)
        .map(|batch| adaptive_records(&format!("policy-{batch}"), 48))
        .collect::<Vec<_>>();
    let mut latency =
        Engine::open(local_options(temp.path(), Goal::Latency)).expect("open latency engine");
    let cf = latency
        .create_column_family("policies")
        .expect("create column family");
    for batch in &batches[..3] {
        write_records_and_flush(&latency, &cf, batch);
    }
    latency
        .shutdown(Duration::from_secs(10))
        .expect("shutdown latency engine");
    let latency_ssts = sorted_sst_files(temp.path());
    assert!(latency_ssts
        .iter()
        .any(|(_, bytes)| { sst_block_algorithms(bytes).contains(&CompressionAlgo::Lz4.to_u8()) }));

    let mut economy =
        Engine::open(local_options(temp.path(), Goal::Economy)).expect("open economy engine");
    let cf = economy
        .get_column_family("policies")
        .expect("reopen column family");
    write_records_and_flush(&economy, &cf, &batches[3]);
    assert!(sorted_sst_files(temp.path()).iter().any(|(_, bytes)| {
        sst_block_algorithms(bytes).contains(&CompressionAlgo::Zstd9.to_u8())
    }));

    // Act
    economy.compact_all().expect("compact mixed-policy SSTs");
    economy
        .shutdown(Duration::from_secs(10))
        .expect("shutdown economy engine");
    let report = Engine::verify_path(temp.path()).expect("verify compacted database");
    let compacted_ssts = sorted_sst_files(temp.path());
    let reopened = Engine::open(local_options(temp.path(), Goal::Throughput))
        .expect("reopen compacted engine");
    let cf = reopened
        .get_column_family("policies")
        .expect("reopen compacted column family");
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin compacted read");
    let rows = read
        .scan(&Query::new())
        .expect("scan compacted rows")
        .try_collect()
        .expect("collect compacted rows");

    // Assert
    assert!(report.authoritative);
    assert!(compacted_ssts.iter().any(|(_, bytes)| {
        sst_block_algorithms(bytes).contains(&CompressionAlgo::Zstd9.to_u8())
    }));
    assert_eq!(rows.len(), batches.iter().map(Vec::len).sum::<usize>());
    for batch in &batches {
        for (key, value) in batch {
            assert_eq!(
                read.get(key).expect("read policy-change key").as_deref(),
                Some(value.as_slice())
            );
        }
    }
}

#[test]
fn should_report_meaningful_error_given_footer_corruption_when_running_explicit_verification() {
    // Arrange
    let temp = tempfile::tempdir().expect("create footer database");
    let records = adaptive_records("footer", 32);
    write_fresh_adaptive_database(temp.path(), &records);
    let sst_path = fs::read_dir(temp.path().join("sst"))
        .expect("read SST directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
        .expect("SST path");
    let mut bytes = fs::read(&sst_path).expect("read SST");
    let footer_handle_byte = bytes.len() - 84 + 8;
    bytes[footer_handle_byte] ^= 0x01;
    fs::write(&sst_path, bytes).expect("corrupt footer");

    // Act
    let error = Engine::verify_path(temp.path()).expect_err("footer corruption must fail verify");

    // Assert
    assert!(matches!(error, cntryl_midge::MidgeError::Corruption(_)));
    assert!(
        error.to_string().contains("CRC mismatch")
            || error.to_string().contains("footer CRC32C mismatch"),
        "expected checksummed verification failure, got {error}"
    );
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
    let mut reopened = Engine::open(local_options(temp.path(), Goal::Throughput))
        .expect("strictly reopen candidate");
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

    let mut engine = Engine::open(local_options(temp.path(), Goal::Throughput))
        .expect("open compacted database");
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
    let mut reopened = Engine::open(local_options(temp.path(), Goal::Throughput))
        .expect("strictly reopen compaction");
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
