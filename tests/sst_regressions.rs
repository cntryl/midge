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

#[test]
fn should_preserve_transaction_when_oversized_range_delete_is_rejected() {
    use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(OpenOptions::local(dir.path()).build().unwrap()).unwrap();
    let cf = engine.create_column_family("range-admission").unwrap();
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"middle".to_vec(), b"value".to_vec(), None).unwrap();
    // Act
    let result = tx.delete_range(b"a".to_vec(), vec![b'z'; 64 * 1024 * 1024]);
    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    assert_eq!(
        tx.get(b"middle").unwrap().as_deref(),
        Some(b"value".as_slice())
    );
    tx.commit(WriteOptions::sync()).unwrap();
    engine.flush_cf(&cf).unwrap();
    assert_eq!(
        engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap()
            .get(b"middle")
            .unwrap()
            .as_deref(),
        Some(b"value".as_slice())
    );
}

#[test]
fn should_continue_writing_across_l0_ceiling_when_background_compaction_is_disabled() {
    use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
    use std::time::Duration;
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .unwrap(),
    )
    .unwrap();
    let cf = engine.create_column_family("l0-pressure").unwrap();
    // Act: 32 separate flushes exceed the default hard L0 ceiling. Only the
    // production pressure-recovery path can restore admission in this process.
    for index in 0_u32..32 {
        let commit = || {
            let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
            tx.put(index.to_be_bytes().to_vec(), b"value".to_vec(), None)?;
            tx.commit(WriteOptions::sync())
        };
        match commit() {
            Ok(()) => {}
            Err(MidgeError::WriteStall(_)) => {
                assert!(engine
                    .wait_for_write_stall_clear(cf.id(), Duration::from_secs(10))
                    .unwrap());
                commit().unwrap();
            }
            Err(error) => panic!("unexpected write failure: {error}"),
        }
        engine.flush_cf(&cf).unwrap();
    }
    engine.shutdown(Duration::from_secs(30)).unwrap();
    let reopened = Engine::open(
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .unwrap(),
    )
    .unwrap();
    let cf = reopened.get_column_family("l0-pressure").unwrap();
    let tx = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    // Assert
    for index in 0_u32..32 {
        assert_eq!(
            tx.get(&index.to_be_bytes()).unwrap().as_deref(),
            Some(b"value".as_slice())
        );
    }
}
