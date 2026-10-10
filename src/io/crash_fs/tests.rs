use super::*;
use crate::io::traits::OpenMode;

const CREATE: OpenOptions = OpenOptions {
    mode: OpenMode::ReadWrite,
    create: true,
    create_new: false,
    truncate: false,
};
const READ: OpenOptions = OpenOptions {
    mode: OpenMode::ReadOnly,
    create: false,
    create_new: false,
    truncate: false,
};

fn bytes(fs: &CrashFs, name: &str) -> Vec<u8> {
    let file = fs.open(&FsPath::new(name), READ).expect("open");
    file.read_at(0, file.len().expect("length"))
        .expect("read")
        .to_vec()
}

#[test]
fn should_lose_unsynced_append_when_power_loss_discards_volatile_bytes() {
    // Arrange
    let fs = CrashFs::new();
    let mut file = fs.open(&FsPath::new("wal"), CREATE).unwrap();
    file.append(Bytes::from_static(b"durable")).unwrap();
    file.sync(Durability::Durable).unwrap();
    fs.sync_dir(&FsPath::new(""), Durability::Durable).unwrap();
    file.append(Bytes::from_static(b"volatile")).unwrap();
    // Act
    fs.crash(CrashMode::DiscardVolatile);
    // Assert
    assert_eq!(bytes(&fs, "wal"), b"durable");
    assert!(file.append(Bytes::from_static(b"late")).is_err());
}

#[test]
fn should_lose_created_name_when_file_sync_precedes_directory_barrier() {
    // Arrange
    let fs = CrashFs::new();
    let mut file = fs.open(&FsPath::new("new"), CREATE).unwrap();
    file.append(Bytes::from_static(b"synced bytes")).unwrap();
    file.sync(Durability::Durable).unwrap();
    // Act
    fs.crash(CrashMode::DiscardVolatile);
    // Assert
    assert!(!fs.exists(&FsPath::new("new")).unwrap());
}

#[test]
fn should_restore_unlinked_file_when_parent_directory_was_not_synced() {
    // Arrange
    let fs = CrashFs::new();
    let mut file = fs.open(&FsPath::new("old"), CREATE).unwrap();
    file.append(Bytes::from_static(b"history")).unwrap();
    file.sync(Durability::Durable).unwrap();
    fs.sync_dir(&FsPath::new("."), Durability::Durable).unwrap();
    fs.remove_file(&FsPath::new("old")).unwrap();
    // Act
    fs.crash(CrashMode::DiscardVolatile);
    // Assert
    assert_eq!(bytes(&fs, "old"), b"history");
}

#[test]
fn should_preserve_renamed_file_when_both_barriers_complete() {
    // Arrange
    let fs = CrashFs::new();
    let mut file = fs.open(&FsPath::new("active"), CREATE).unwrap();
    file.append(Bytes::from_static(b"sealed")).unwrap();
    file.sync(Durability::Durable).unwrap();
    fs.rename_atomic(&FsPath::new("active"), &FsPath::new("sealed"))
        .unwrap();
    fs.sync_dir(&FsPath::new("."), Durability::Durable).unwrap();
    // Act
    fs.crash(CrashMode::DiscardVolatile);
    // Assert
    assert!(!fs.exists(&FsPath::new("active")).unwrap());
    assert_eq!(bytes(&fs, "sealed"), b"sealed");
}

#[test]
fn should_keep_handles_bound_to_inode_when_path_is_replaced() {
    // Arrange
    let fs = CrashFs::new();
    let mut old = fs.open(&FsPath::new("old"), CREATE).unwrap();
    old.append(Bytes::from_static(b"old")).unwrap();
    let mut new = fs.open(&FsPath::new("new"), CREATE).unwrap();
    new.append(Bytes::from_static(b"new")).unwrap();
    // Act
    fs.rename_atomic(&FsPath::new("new"), &FsPath::new("old"))
        .unwrap();
    old.append(Bytes::from_static(b" tail")).unwrap();
    // Assert
    assert_eq!(bytes(&fs, "old"), b"new");
    assert_eq!(old.read_at(0, 8).unwrap().as_ref(), b"old tail");
}

#[test]
fn should_preserve_synced_prefix_when_unsynced_tail_is_torn() {
    for seed in 0..64 {
        // Arrange
        let fs = CrashFs::new();
        let mut file = fs.open(&FsPath::new("wal"), CREATE).unwrap();
        file.append(Bytes::from_static(b"stable")).unwrap();
        file.sync(Durability::Durable).unwrap();
        fs.sync_dir(&FsPath::new(""), Durability::Durable).unwrap();
        file.append(Bytes::from_static(b"tail")).unwrap();
        // Act
        fs.crash(CrashMode::Torn { seed });
        // Assert
        let recovered = bytes(&fs, "wal");
        assert!(recovered.starts_with(b"stable"));
        assert!(b"stabletail".starts_with(&recovered));
    }
}

#[test]
fn should_require_ancestor_barriers_when_creating_nested_directories() {
    // Arrange
    let fs = CrashFs::new();
    fs.create_dir_all(&FsPath::new("wal/nested")).unwrap();
    let mut file = fs.open(&FsPath::new("wal/nested/segment"), CREATE).unwrap();
    file.append(Bytes::from_static(b"history")).unwrap();
    file.sync(Durability::Durable).unwrap();
    fs.sync_dir(&FsPath::new("wal/nested"), Durability::Durable)
        .unwrap();
    // Act
    fs.crash(CrashMode::DiscardVolatile);
    // Assert
    assert!(!fs.exists(&FsPath::new("wal/nested/segment")).unwrap());
}

#[test]
fn should_reclaim_unlinked_bytes_when_directory_barrier_is_complete_at_last_handle_close() {
    // Arrange
    let fs = CrashFs::new();
    let mut file = fs.open(&FsPath::new("old"), CREATE).unwrap();
    file.append(Bytes::from_static(b"history")).unwrap();
    file.sync(Durability::Durable).unwrap();
    fs.sync_dir(&FsPath::new(""), Durability::Durable).unwrap();
    // Act
    fs.remove_file(&FsPath::new("old")).unwrap();
    assert_eq!(fs.physical_bytes(), 7);
    fs.sync_dir(&FsPath::new(""), Durability::Durable).unwrap();
    assert_eq!(fs.physical_bytes(), 7);
    drop(file);
    // Assert
    assert_eq!(fs.physical_bytes(), 0);
}

#[test]
fn should_retain_unbarriered_bytes_when_only_process_fails() {
    // Arrange
    let root = std::env::current_dir()
        .unwrap()
        .join("unmaterialized/model-database");
    let fs = CrashFs::for_root(root.clone());
    let mut file = fs
        .open(&FsPath::new(root.join("wal").to_string_lossy()), CREATE)
        .unwrap();
    file.append(Bytes::from_static(b"buffered")).unwrap();
    // Act
    fs.crash(CrashMode::PreserveVolatile);
    // Assert
    assert_eq!(bytes(&fs, "wal"), b"buffered");
    assert!(fs
        .exists(&FsPath::new(
            root.parent().unwrap().join("outside/wal").to_string_lossy()
        ))
        .is_err());
}
