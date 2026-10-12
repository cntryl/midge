use super::*;

#[test]
fn should_leave_wal_history_unchanged_when_salvage_lacks_family_authority() {
    // Arrange
    let wal: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    let history = crate::runtime::wal_history::WalHistory::open(Arc::clone(&wal), 1, 1).unwrap();
    let ledger = crate::io::FsPath::new(crate::wal::ledger_store::LEDGER_FILE);
    let before = crate::io::read_all(wal.as_ref(), &ledger).unwrap();
    let failure = MidgeError::Corruption("flush frontier is missing for family 7".into());

    // Act
    let result = RuntimeState::handle_local_wal_replay_failure(
        history,
        crate::config::RecoveryPolicy::Salvage,
        failure,
        HashMap::new(),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(crate::io::read_all(wal.as_ref(), &ledger).unwrap(), before);
}
