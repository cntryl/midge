//! Backup/restore root isolation and retry regressions.
use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use std::path::Path;
use std::time::Duration;

fn open(path: &Path) -> Engine {
    Engine::open(OpenOptions::local(path.to_path_buf()).build().unwrap()).unwrap()
}

fn inventory(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut rows = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type().unwrap().is_dir() {
            for (child, bytes) in inventory(&entry.path()) {
                rows.push((format!("{name}/{child}"), bytes));
            }
        } else {
            rows.push((name, std::fs::read(entry.path()).unwrap()));
        }
    }
    rows.sort();
    rows
}

#[test]
fn should_reject_backup_overlap_before_mutation() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let mut engine = open(&source);
    let cf = engine.get_column_family("default").unwrap();
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
    tx.commit(WriteOptions::sync()).unwrap();
    // Keep the tiny transaction resident: flush completion precedes WAL retirement,
    // which would make a whole-directory comparison race background maintenance.
    let before = inventory(&source);

    // Act
    let result = engine.backup_to(source.join("new-parent/backup"), Duration::from_secs(10));

    // Assert
    assert!(
        matches!(result, Err(MidgeError::InvalidArgument(_))),
        "{result:?}"
    );
    assert_eq!(before, inventory(&source));
    assert!(engine
        .backup_to(directory.path().join("sibling"), Duration::from_secs(10))
        .is_ok());
    engine.shutdown(Duration::from_secs(10)).unwrap();
}

#[test]
fn should_reject_restore_overlap_without_changing_artifact() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let mut engine = open(&directory.path().join("source"));
    let artifact = directory.path().join("backup");
    engine
        .backup_to(&artifact, Duration::from_secs(10))
        .unwrap();
    engine.shutdown(Duration::from_secs(10)).unwrap();
    let before = inventory(&artifact);

    // Act
    let result = Engine::restore_backup(
        &artifact,
        OpenOptions::local(artifact.join("new-parent/restored"))
            .build()
            .unwrap(),
    );

    // Assert
    assert!(
        matches!(result, Err(MidgeError::InvalidArgument(_))),
        "{result:?}"
    );
    assert_eq!(before, inventory(&artifact));
}

#[test]
fn should_preserve_foreign_stage_when_retrying_restore() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let mut engine = open(&directory.path().join("source"));
    let artifact = directory.path().join("backup");
    let manifest = engine
        .backup_to(&artifact, Duration::from_secs(10))
        .unwrap();
    engine.shutdown(Duration::from_secs(10)).unwrap();
    let foreign = directory
        .path()
        .join(format!(".midge-restore-{}.tmp", manifest.backup_id));
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("owned-by-someone-else"), b"retain").unwrap();

    // Act
    let result = Engine::restore_backup(
        &artifact,
        OpenOptions::local(directory.path().join("restored"))
            .build()
            .unwrap(),
    );

    // Assert
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        std::fs::read(foreign.join("owned-by-someone-else")).unwrap(),
        b"retain"
    );
}

#[cfg(unix)]
#[test]
fn should_reject_symlink_alias_and_dangling_link_before_mutation() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let mut engine = open(&source);
    let alias = directory.path().join("alias");
    std::os::unix::fs::symlink(&source, &alias).unwrap();
    let dangling = directory.path().join("dangling");
    std::os::unix::fs::symlink(source.join("missing"), &dangling).unwrap();
    let before = inventory(&source);

    // Act
    let alias_result = engine.backup_to(alias.join("new-parent/backup"), Duration::from_secs(10));
    let dangling_result = engine.backup_to(dangling.join("backup"), Duration::from_secs(10));

    // Assert
    assert!(matches!(alias_result, Err(MidgeError::InvalidArgument(_))));
    assert!(dangling_result.is_err());
    assert_eq!(before, inventory(&source));
    engine.shutdown(Duration::from_secs(10)).unwrap();
}

#[test]
fn should_publish_only_one_restore_when_attempts_share_target() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let mut engine = open(&directory.path().join("source"));
    let artifact = directory.path().join("backup");
    engine
        .backup_to(&artifact, Duration::from_secs(10))
        .unwrap();
    engine.shutdown(Duration::from_secs(10)).unwrap();
    let target = directory.path().join("restored");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let attempts: Vec<_> = (0..2)
        .map(|_| {
            let artifact = artifact.clone();
            let target = target.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                Engine::restore_backup(artifact, OpenOptions::local(target).build().unwrap())
            })
        })
        .collect();

    // Act
    let results: Vec<_> = attempts
        .into_iter()
        .map(|attempt| attempt.join().unwrap())
        .collect();

    // Assert
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(MidgeError::InvalidArgument(_))))
            .count(),
        1
    );
    let mut restored = open(&target);
    restored.shutdown(Duration::from_secs(10)).unwrap();
    assert!(directory.path().read_dir().unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".midge-restore-")));
}

#[cfg(windows)]
#[test]
fn should_reject_windows_case_alias_before_mutation() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("MixedCase");
    let mut engine = open(&source);
    let before = inventory(&source);

    // Act
    let result = engine.backup_to(
        directory.path().join("mixedcase/new-parent/backup"),
        Duration::from_secs(10),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    assert_eq!(before, inventory(&source));
    engine.shutdown(Duration::from_secs(10)).unwrap();
}

#[test]
fn should_reject_relative_cloud_simulated_overlap_and_allow_siblings() {
    // Arrange
    let cwd = std::env::current_dir().unwrap();
    let directory = tempfile::tempdir_in(cwd.join("target")).unwrap();
    let source = directory.path().join("source");
    let mut engine = Engine::open(
        OpenOptions::cloud_simulated(&source, "bucket", "prefix")
            .build()
            .unwrap(),
    )
    .unwrap();
    let relative = source.strip_prefix(&cwd).unwrap().join("new-parent/backup");
    let before = inventory(&source);

    // Act
    let backup_result = engine.backup_to(relative, Duration::from_secs(10));

    // Assert
    assert!(matches!(backup_result, Err(MidgeError::InvalidArgument(_))));
    assert_eq!(before, inventory(&source));
    let artifact = directory.path().join("backup");
    engine
        .backup_to(&artifact, Duration::from_secs(10))
        .unwrap();
    engine.shutdown(Duration::from_secs(10)).unwrap();
    let before = inventory(&artifact);
    let result = Engine::restore_backup(
        &artifact,
        OpenOptions::cloud_simulated(artifact.join("new-parent/restored"), "bucket", "prefix")
            .build()
            .unwrap(),
    );
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    assert_eq!(before, inventory(&artifact));
    Engine::restore_backup(
        &artifact,
        OpenOptions::cloud_simulated(directory.path().join("restored"), "bucket", "prefix")
            .build()
            .unwrap(),
    )
    .unwrap();
}

#[cfg(windows)]
#[test]
fn should_reject_drive_relative_overlap_before_creating_parent() {
    // Arrange
    let cwd = std::env::current_dir().unwrap();
    let directory = tempfile::tempdir_in(cwd.join("target")).unwrap();
    let source = directory.path().join("source");
    let mut engine = open(&source);
    let relative = source.strip_prefix(&cwd).unwrap().join("new-parent/backup");
    let drive = match cwd.components().next().unwrap() {
        std::path::Component::Prefix(prefix) => match prefix.kind() {
            std::path::Prefix::Disk(drive) | std::path::Prefix::VerbatimDisk(drive) => {
                char::from(drive)
            }
            _ => panic!("test requires a disk path"),
        },
        _ => panic!("test requires a Windows drive"),
    };
    let before = inventory(&source);

    // Act
    let result = engine.backup_to(
        format!("{drive}:{}", relative.display()),
        Duration::from_secs(10),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    assert!(!source.join("new-parent").exists());
    assert_eq!(before, inventory(&source));
    engine.shutdown(Duration::from_secs(10)).unwrap();
}

#[test]
fn should_reject_parent_traversal_through_missing_or_regular_ancestor() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let mut engine = open(&directory.path().join("source"));
    std::fs::write(directory.path().join("regular-file"), b"retain").unwrap();
    let before = inventory(directory.path());

    // Act
    for ancestor in ["missing", "regular-file"] {
        let result = engine.backup_to(
            directory.path().join(ancestor).join("..").join("backup"),
            Duration::from_secs(10),
        );

        // Assert
        assert!(
            matches!(result, Err(MidgeError::InvalidArgument(_))),
            "{result:?}"
        );
        assert_eq!(before, inventory(directory.path()));
    }
    engine.shutdown(Duration::from_secs(10)).unwrap();
}
