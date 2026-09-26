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
    assert!(scan.matches("self.block_span(").count() >= 3);
}
