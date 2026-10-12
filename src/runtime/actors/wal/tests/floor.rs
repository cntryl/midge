use super::*;

#[test]
fn should_protect_spilled_transaction_participants_until_all_generations_publish() -> MidgeResult<()>
{
    // Arrange
    let directory = tempfile::tempdir()?;
    let setup =
        crate::storage::simulated::host_simulated_cloud(directory.path(), Some(1024 * 1024))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(1024 * 1024);
    let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
    crate::runtime::wal_history::fixture::rotate_to(&mut state.wal.history, 7)?;
    let secondary = state.create_cf("spilled-floor".into())?;
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    actor.set_storage_budget(setup.hybrid_storage.clone());
    // A transaction is single-family (#843): one spilled transaction per
    // family, both in segment 7.
    let mut sources = Vec::new();
    for (txn_id, cf_id) in (1..).zip([0, secondary]) {
        let mut writes = crate::runtime::transaction_spill::TransactionWriteSet::new(
            Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(0)),
            &crate::io::real::test_root(directory.path()),
            false,
            txn_id,
        )
        .with_storage_budget(Some(setup.hybrid_storage.clone()));
        writes.push(crate::runtime::TransactionOp::Put {
            cf_id,
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            ttl_seconds: None,
            insert_only: false,
        })?;
        sources.push(writes.take_source());
    }
    assert!(
        setup
            .hybrid_storage
            .budget_snapshot()
            .usage
            .transaction_spill_bytes
            > 0
    );
    let wal_path = directory
        .path()
        .join("wal")
        .join(crate::wal::segment_file_name(7));
    assert_eq!(
        std::fs::metadata(&wal_path)?.len(),
        crate::wal::format::HEADER_LEN as u64,
        "an open caller transaction has not emitted WAL frames"
    );

    // Act
    for (request_id, source) in (1..).zip(&sources) {
        actor.append_spilled_transaction(
            &mut state,
            source,
            SpilledTransactionAppendParams {
                request_id,
                assertions: Vec::new(),
                durability_policy: Some(DurabilityPolicy::CloudAsync),
                start_sequence: 0,
                conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
            },
        )?;
    }
    let observed = crate::wal::format::segment::records(&std::fs::read(wal_path)?)?
        .into_iter()
        .filter(|record| record.op == crate::wal::WalOpKind::Put)
        .count();
    crate::runtime::wal_history::fixture::rotate_to(&mut state.wal.history, 9)?;
    let sequence = state.sequence;
    let mut frozen = Vec::new();
    for cf_id in [0, secondary] {
        let table = state.get_cf(cf_id).unwrap().memtable.clone();
        state
            .track_new_immutable_flush(cf_id, table.clone(), sequence)
            .unwrap();
        state.get_cf_mut(cf_id).unwrap().memtable =
            Arc::new(crate::memtable::SkipListMemtable::new());
        frozen.push((cf_id, table));
    }
    let before_publication = state.wal_recovery_floor_segment();
    state
        .complete_immutable_flush(frozen[0].0, &frozen[0].1)
        .unwrap();
    let after_first_family = state.wal_recovery_floor_segment();
    state
        .complete_immutable_flush(frozen[1].0, &frozen[1].1)
        .unwrap();

    // Assert
    assert_eq!(observed, 2);
    assert_eq!(before_publication, Some(7));
    assert_eq!(after_first_family, Some(7));
    assert_eq!(state.wal_recovery_floor_segment(), Some(9));
    Ok(())
}

#[test]
fn should_keep_conservative_generation_floor_when_reconstructing_startup_memtables(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::CloudAsync,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let prepared = prepare_put_transaction(
        &mut actor,
        &mut state,
        1,
        b"recovered",
        b"value",
        DurabilityPolicy::CloudAsync,
    )?;
    actor.append_prepared_transactions(&mut state, vec![prepared])?;
    drop(actor);
    drop(state);
    // Move segment 1's records to segment 7: the fixture gives them
    // segment 7's header.
    let wal_directory = directory.path().join("wal");
    let first = wal_directory.join(crate::wal::segment_file_name(1));
    let records = std::fs::read(&first)?[crate::wal::format::HEADER_LEN..].to_vec();
    std::fs::remove_file(&first)?;
    std::fs::write(
        wal_directory.join(crate::wal::segment_file_name(7)),
        records,
    )?;
    let wal: Arc<dyn crate::io::Fs> =
        Arc::new(crate::io::RealFs::new(&wal_directory).map_err(crate::io::FsError::into_midge)?);
    crate::runtime::wal_history::fixture::replace(&wal, &[7], 8)?;

    // Act
    let mut recovered = RuntimeState::try_new(
        directory.path(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )?;
    assert_eq!(recovered.wal.open_segment(), 8);
    recovered.wal.history = crate::runtime::wal_history::WalHistory::in_memory(0);
    crate::runtime::wal_history::fixture::rotate_to(&mut recovered.wal.history, 50)?;
    let table = recovered.get_cf(0).unwrap().memtable.clone();
    let generation = recovered
        .track_new_immutable_flush(0, table, recovered.sequence)
        .unwrap();

    // Assert
    assert!(recovered.recovery_stats.wal_recovery_records_replayed > 0);
    assert_eq!(generation.first_wal_segment, Some(1));
    assert_eq!(recovered.wal_recovery_floor_segment(), Some(1));
    Ok(())
}
