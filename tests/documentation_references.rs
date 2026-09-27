use std::path::Path;

#[test]
fn should_reference_existing_test_sources_in_current_storage_guides() {
    // Arrange
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let guides = [
        (
            "docs/development/storage-invariants.md",
            include_str!("../docs/development/storage-invariants.md"),
        ),
        (
            "docs/development/testing.md",
            include_str!("../docs/development/testing.md"),
        ),
    ];

    // Act
    let missing: Vec<_> = guides
        .into_iter()
        .flat_map(|(guide, body)| {
            body.split('`')
                .skip(1)
                .step_by(2)
                .filter(|reference| {
                    (reference.starts_with("src/") || reference.starts_with("tests/"))
                        && Path::new(reference)
                            .extension()
                            .is_some_and(|extension| extension.eq_ignore_ascii_case("rs"))
                })
                .filter(move |reference| !root.join(reference).is_file())
                .map(move |reference| format!("{guide}: {reference}"))
        })
        .collect();

    // Assert
    assert!(missing.is_empty(), "missing source references: {missing:?}");
}
