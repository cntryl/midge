#[test]
fn should_keep_metric_verification_delegates_off_engine() {
    // Arrange
    let source = include_str!("../src/engine/mod.rs");

    // Act
    let forbidden = [
        "pub fn get_read_amp_metrics(",
        "pub fn get_recovery_metrics(",
        "pub fn get_runtime_metrics(",
        "pub fn get_runtime_metrics_with_timeout(",
        "pub fn verify_storage(",
        "pub fn verify_path(",
        "pub fn get_storage_layout(",
    ];

    // Assert
    for signature in forbidden {
        assert!(
            !source.contains(signature),
            "Engine still exposes {signature}"
        );
    }
}

#[test]
fn should_keep_test_only_ingest_and_idempotency_protocols_out_of_runtime() {
    // Arrange
    let state = include_str!("../src/runtime/state.rs");
    let protocol = include_str!("../src/runtime/protocol.rs");
    let durability = include_str!("../src/runtime/durability.rs");
    let engine = include_str!("../src/engine/mod.rs");

    // Act / Assert
    for (name, source) in [
        ("state", state),
        ("protocol", protocol),
        ("durability", durability),
        ("engine", engine),
    ] {
        for forbidden in [
            "ingest_active",
            "ingest_epoch",
            "BeginIngest",
            "EndIngest",
            "idempotency_cache",
            "ConfirmWalAppend",
        ] {
            assert!(!source.contains(forbidden), "{name} retains {forbidden}");
        }
    }
    assert!(state.contains("HashMap<u64, CompactionWait>"));
    assert!(!state.contains("HashMap<u64, String>"));
}

#[test]
fn should_require_explicit_sst_tombstone_and_filesystem_behavior() {
    // Arrange
    let traits = include_str!("../src/sst/traits.rs");

    // Act / Assert
    let reader_contract = traits
        .split("/// Materializing cursor for explicit test doubles.")
        .next()
        .expect("reader contract");
    assert!(!reader_contract.contains("fn range_tombstones(&self) -> Vec<RangeTombstone> {"));
    assert!(!traits.contains("persist_sst_bytes_with_host_fs"));
    assert!(traits.contains("fn finish_to_path(self: Box<Self>, path: &Path) -> MidgeResult<()>;"));
}

#[test]
fn should_decode_sst_frames_and_prefix_keys_in_one_reader_path() {
    // Arrange
    let reader = include_str!("../src/sst/fs/reader_io/mod.rs");
    let io = include_str!("../src/sst/fs/reader_io/io.rs");
    let recovery = include_str!("../src/sst/fs/reader_io/recovery.rs");
    let state = include_str!("../src/sst/fs/reader_io/state.rs");
    let scan = include_str!("../src/sst/fs/reader_io/scan.rs");

    // Act / Assert
    let sources = [reader, io, recovery, state, scan];
    assert_eq!(
        sources
            .iter()
            .map(|source| source.matches("u32::from_le_bytes").count())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        sources
            .iter()
            .map(|source| source.matches("Invalid shared prefix length").count())
            .sum::<usize>(),
        1
    );
    assert!(io.contains("fn read_framed_block"));
    assert!(reader.contains("struct BlockEntryDecoder"));
    assert!(!state.contains("encoding::decode_with_format("));
    assert!(scan.matches("self.block_span(").count() >= 2);
}

#[test]
fn should_keep_dead_sst_and_skiplist_interfaces_out_of_production() {
    // Arrange
    let types = include_str!("../src/sst/types.rs");
    let traits = include_str!("../src/sst/traits.rs");
    let reader = include_str!("../src/sst/fs/reader_io/mod.rs");
    let skiplist = include_str!("../src/memtable/skiplist.rs");

    // Act / Assert
    assert!(!types.contains("enum SstBlockType"));
    assert!(!traits.contains("pub trait SstReader: "));
    assert!(!reader.contains("    bloom_metrics: BloomMetrics"));
    assert!(!reader.contains("    read_amp_metrics: ReadAmpMetrics"));
    for signature in [
        "pub fn get(&self, key: &[u8], snapshot_seq: u64)",
        "pub fn get_all_keys(&self)",
        "pub fn tombstones_range_visible(",
    ] {
        let position = skiplist.find(signature).expect("test reader remains gated");
        assert!(skiplist[..position].ends_with("#[cfg(test)]\n    "));
    }
}

#[test]
fn should_route_compaction_output_checks_through_injected_fs() {
    // Arrange
    let executor = include_str!("../src/compaction/executor.rs");
    let actor = include_str!("../src/runtime/actors/compaction.rs");
    let flush = include_str!("../src/runtime/actors/flush/build.rs");
    let reader = include_str!("../src/sst/fs/reader_io/mod.rs");

    // Act / Assert
    let output_code = executor.split("#[cfg(test)]\nmod tests").next().unwrap();
    assert!(!output_code.contains("std::fs::metadata("));
    assert!(!output_code.contains("std::fs::remove_file("));
    assert!(!actor.contains("std::fs::remove_file("));
    assert!(!actor.contains("std::fs::read_dir("));
    assert!(!flush.contains("std::fs::create_dir_all("));
    assert!(!flush.contains("file_identity(&task.staging_path)"));
    assert!(!reader.contains("summarize_with_real_fs_for_compaction"));
}

#[test]
fn should_include_range_tombstones_in_skippable_sst_sequence_bounds() {
    // Arrange
    let flush = include_str!("../src/runtime/actors/flush/build.rs");
    let summary = include_str!("../src/sst/fs/reader_io/mod.rs");
    let compaction = include_str!("../src/runtime/actors/compaction.rs");
    let backfill = include_str!("../src/runtime/event_loop/read_path.rs");
    let snapshot = include_str!("../src/runtime/read_snapshot.rs");

    // Act / Assert
    assert!(flush.contains("largest_seq = largest_seq.max(range.seq)"));
    assert!(summary.contains("accumulator.observe(size_bytes, &range.start, range.seq"));
    assert!(summary.contains("accumulator.observe(size_bytes, &range.end, range.seq"));
    assert!(compaction.contains("largest_seq: Some(summary.largest_seq)"));
    assert!(backfill.contains("updated.largest_seq = Some(summary.largest_seq)"));
    let conflict_check = snapshot
        .split("pub fn any_sequence_after_in_range(")
        .nth(1)
        .unwrap()
        .split("/// Perform a range scan")
        .next()
        .unwrap();
    assert!(conflict_check.contains("reader.raw_state_scan("));
    assert!(!conflict_check.contains("scan_range_state_with_time("));
}

#[test]
fn should_copy_file_meta_proofs_only_in_central_conversions() {
    // Arrange
    let producers = [
        include_str!("../src/runtime/actors/manifest.rs"),
        include_str!("../src/runtime/state/manifest.rs"),
        include_str!("../src/runtime/actors/flush.rs"),
        include_str!("../src/runtime/event_loop/flush_pipeline.rs"),
        include_str!("../src/engine/startup/storage.rs"),
    ];

    // Act / Assert
    for source in producers {
        assert!(source.lines().all(|line| {
            !(line.contains("key_bounds_complete:") && line.contains(".key_bounds_complete"))
        }));
    }
}
