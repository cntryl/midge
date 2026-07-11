use std::path::{Path, PathBuf};

fn rust_sources_under(relative: &str) -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    let mut pending = vec![root];
    let mut sources = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path).expect("read source directory") {
            let path = entry.expect("read source entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }
    sources
}

#[test]
fn should_keep_common_independent_from_higher_subsystems() {
    // Arrange
    let forbidden = [
        "crate::engine",
        "crate::lease",
        "crate::metadata",
        "crate::runtime",
        "crate::sst",
        "crate::storage",
        "crate::wal",
    ];

    // Act
    let violations: Vec<_> = rust_sources_under("src/common")
        .into_iter()
        .flat_map(|path| {
            let source = std::fs::read_to_string(&path).expect("read common source");
            forbidden
                .iter()
                .filter(move |edge| source.contains(**edge))
                .map(move |edge| format!("{} imports {edge}", path.display()))
        })
        .collect();

    // Assert
    assert!(
        violations.is_empty(),
        "common must remain the bottom dependency layer: {violations:#?}"
    );
}
