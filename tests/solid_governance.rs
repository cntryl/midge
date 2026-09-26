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
