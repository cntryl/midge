use super::*;

#[test]
fn should_leave_wal_history_unchanged_when_salvage_lacks_family_authority() {
    // Arrange
    let wal: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    let history = crate::runtime::wal_history::WalHistory::open(Arc::clone(&wal), 1, 1).unwrap();
    let ledger = crate::io::FsPath::new(crate::wal::ledger_store::LEDGER_FILE);
    let before = crate::io::read_all(wal.as_ref(), &ledger).unwrap();
    let root: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    let storage = crate::wal::FsWalFactoryIo::new(Arc::clone(&wal));
    let writer = storage.open_segment(1).unwrap();
    let record = crate::wal::WalRecord::new_cf(
        7,
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from_static(b"key"),
        Some(bytes::Bytes::from_static(b"value")),
        1,
    );
    writer.append_record(&record).unwrap();
    writer.sync().unwrap();

    // Act
    let result = RuntimeState::replay_wal_storage_within(
        std::path::Path::new("wal"),
        &root,
        crate::config::RecoveryPolicy::Salvage,
        &Manifest::default(),
        HashMap::new(),
        None,
        wal.as_ref(),
        history,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(crate::io::read_all(wal.as_ref(), &ledger).unwrap(), before);
}
