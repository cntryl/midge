#[test]
fn should_preserve_empty_key_through_engine_flush_with_deep_trie() {
    use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .unwrap(),
    )
    .unwrap();
    let cf = engine.create_column_family("probe").unwrap();
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    let value = vec![b'v'; 32 * 1024];
    tx.put(Vec::new(), value.clone(), None).unwrap();
    for n in (1..=300).rev() {
        let mut key = vec![b'a'; n];
        key.push(b'b');
        tx.put(key, value.clone(), None).unwrap();
    }
    tx.commit(WriteOptions::sync()).unwrap();
    assert!(engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap()
        .get(b"")
        .unwrap()
        .is_some());
    // Act
    engine.flush_cf(&cf).unwrap();
    let result = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap()
        .get(b"")
        .unwrap();
    let verification = engine
        .verify_storage(std::time::Duration::from_secs(30))
        .unwrap();
    assert!(verification.authoritative);
    engine.shutdown(std::time::Duration::from_secs(30)).unwrap();
    let reopened = Engine::open(
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .unwrap(),
    )
    .unwrap();
    let cf = reopened.get_column_family("probe").unwrap();
    let after_reopen = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap()
        .get(b"")
        .unwrap();

    // Assert
    assert_eq!(after_reopen.as_deref().map(<[u8]>::len), Some(value.len()));
    assert_eq!(result.as_deref().map(<[u8]>::len), Some(value.len()));
}

#[test]
fn should_reject_oversized_value_before_transaction_stages_it() {
    use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(OpenOptions::local(dir.path()).build().unwrap()).unwrap();
    let cf = engine.create_column_family("probe").unwrap();
    for insert in [false, true] {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        let value = vec![b'v'; 64 * 1024 * 1024];
        // Act
        let result = if insert {
            tx.insert(b"key".to_vec(), value, Some(60))
        } else {
            tx.put(b"key".to_vec(), value, None)
        };
        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        tx.put(b"valid".to_vec(), b"value".to_vec(), None).unwrap();
        tx.commit(WriteOptions::sync()).unwrap();
        assert!(engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap()
            .get(b"key")
            .unwrap()
            .is_none());
    }
    engine.flush_cf(&cf).unwrap();
    assert!(engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap()
        .get(b"valid")
        .unwrap()
        .is_some());
}
