use super::*;

#[test]
fn should_keep_owned_large_spill_tail_charged_to_original_transaction_pool() -> MidgeResult<()> {
    // Arrange: the first two accepted operations create real spill runs. The
    // last stays in its original9MiB pool and exceeds the validation8MiB pool.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::super::super::tests::create_test_local_event_loop()?;
    seed_spanning_sst(&mut event_loop);
    let pool =
        Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(9 * 1024 * 1024));
    let mut intents = crate::runtime::transaction_spill::TransactionWriteSet::new(
        Arc::clone(&pool),
        &event_loop.state.fs,
        false,
        946_080,
    );
    intents.push(request(0, b"l", false).ops.remove(0))?;
    intents.push(TransactionOp::Put {
        cf_id: 0,
        key: Bytes::from_static(b"m"),
        value: Bytes::from(vec![4; 9 * 1024 * 1024]),
        ttl_seconds: None,
        insert_only: true,
    })?;
    let tail = Bytes::from(vec![5; 8 * 1024 * 1024 + 1]);
    let checksum = crc32c::crc32c(&tail);
    intents.push(TransactionOp::Put {
        cf_id: 0,
        key: Bytes::from_static(b"n"),
        value: tail,
        ttl_seconds: None,
        insert_only: true,
    })?;
    let source = intents.take_source();
    assert!(source.retained_memory_bytes() > 8 * 1024 * 1024);
    let checked = SpilledTransactionRequest {
        request_id: 946_080,
        source,
        assertions: Vec::new(),
        durability_policy: Some(crate::wal::DurabilityPolicy::Strict),
        start_sequence: 4,
        conflict_policy: ConflictPolicy::AbortOnWriteConflict,
        validation: None,
    };
    let response = event_loop
        .routing
        .router
        .register(946_080, "original source ownership");
    let held = HeldCallback::first("midge::validation::after_spill_snapshot_reads");

    // Act: moving ownership keeps the original charge alive through validation
    // and the drained WAL/application path, without copying the resident tail.
    WalCoordinator::apply_spilled_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        checked,
        None,
    );
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("large source remains owned");
    assert!(!pool.try_reserve(2 * 1024 * 1024));
    assert!(event_loop.pipeline.validation.memory.used() < 8 * 1024 * 1024);
    held.release.send(()).expect("complete retained source");
    let result = settle_validation_request(&mut event_loop, &response);

    // Assert: no new8MiB input limit rejects this previously supported source.
    assert!(matches!(
        result,
        RuntimeResponse::TransactionApplied {
            last_sequence: 9,
            ..
        }
    ));
    assert_eq!(
        event_loop.pipeline.validation.memory.limit(),
        8 * 1024 * 1024
    );
    assert_eq!(event_loop.pipeline.validation.memory.used(), 0);
    let snapshot = event_loop.create_read_snapshot(0).unwrap();
    let stored = snapshot.get_bytes(b"n", 9)?.expect("whole resident tail");
    assert_eq!(stored.len(), 8 * 1024 * 1024 + 1);
    assert_eq!(crc32c::crc32c(&stored), checksum);
    assert!(pool.try_reserve(9 * 1024 * 1024));
    pool.release(9 * 1024 * 1024);
    Ok(())
}
