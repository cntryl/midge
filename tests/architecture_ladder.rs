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

fn production_source(path: &Path) -> String {
    let source = std::fs::read_to_string(path).expect("read Rust source");
    source
        .split("\n#[cfg(test)]\nmod tests")
        .next()
        .unwrap_or(&source)
        .to_string()
}

fn prohibited_edges_under(relative: &str, forbidden: &[&str]) -> Vec<String> {
    rust_sources_under(relative)
        .into_iter()
        .flat_map(|path| {
            let source = production_source(&path);
            forbidden
                .iter()
                .filter(move |edge| source.contains(**edge))
                .map(move |edge| format!("{} imports {edge}", path.display()))
        })
        .collect()
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

#[test]
fn should_keep_provider_configuration_owned_by_config_layer() {
    // Arrange
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = vec![manifest_dir.join("src/config.rs")];
    let config_dir = manifest_dir.join("src/config");
    if config_dir.exists() {
        sources.extend(rust_sources_under("src/config"));
    }

    // Act
    let violations: Vec<_> = sources
        .into_iter()
        .filter_map(|path| {
            let source = std::fs::read_to_string(&path).expect("read config source");
            source
                .contains("crate::storage")
                .then(|| format!("{} imports crate::storage", path.display()))
        })
        .collect();

    // Assert
    assert!(
        violations.is_empty(),
        "configuration DTOs must not be owned or re-exported by storage: {violations:#?}"
    );
}

#[test]
fn should_keep_storage_as_raw_object_io() {
    // Arrange
    let forbidden = [
        "crate::engine",
        "crate::metadata",
        "crate::runtime",
        "crate::sst",
        "crate::wal",
    ];

    // Act
    let violations = prohibited_edges_under("src/storage", &forbidden);

    // Assert
    assert!(
        violations.is_empty(),
        "storage must provide raw bounded object I/O without format or runtime ownership: {violations:#?}"
    );
}

#[test]
fn should_keep_sst_below_read_layers() {
    // Arrange
    let forbidden = [
        "crate::engine",
        "crate::iterators",
        "crate::metadata",
        "crate::runtime",
        "crate::storage",
        "crate::wal",
    ];

    // Act
    let violations = prohibited_edges_under("src/sst", &forbidden);

    // Assert
    assert!(
        violations.is_empty(),
        "SST format and readers must not depend on iterator facades or orchestration: {violations:#?}"
    );
}

#[test]
fn should_keep_iterator_contracts_in_lower_layer() {
    // Arrange
    let forbidden = [
        "crate::engine",
        "crate::metadata",
        "crate::runtime",
        "crate::sst",
        "crate::storage",
        "crate::wal",
    ];

    // Act
    let violations = prohibited_edges_under("src/iterators", &forbidden);

    // Assert
    assert!(
        violations.is_empty(),
        "shared iterator contracts must be owned below SST and orchestration: {violations:#?}"
    );
}

#[test]
fn should_keep_persistence_formats_below_storage_orchestration() {
    // Arrange
    let forbidden = ["crate::engine", "crate::runtime", "crate::storage"];

    // Act
    let mut violations = prohibited_edges_under("src/wal", &forbidden);
    violations.extend(prohibited_edges_under("src/metadata", &forbidden));

    // Assert
    assert!(
        violations.is_empty(),
        "WAL and metadata formats must not depend on storage or runtime orchestration: {violations:#?}"
    );
}
