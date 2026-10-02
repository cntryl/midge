use super::*;

#[test]
fn should_reject_spill_before_creating_files_when_shared_disk_budget_is_exhausted(
) -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let setup = crate::storage::simulated::build_simulated_cloud_stores(dir.path(), Some(128))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(128);
    let mut writes = TransactionWriteSet::new(
        Arc::new(TransactionMemoryPool::new(0)),
        dir.path(),
        false,
        1,
    )
    .with_storage_budget(Some(Arc::clone(&setup.hybrid_storage)));

    // Act
    let result = writes.push(put(b"key", &[0x5A; 256]));

    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))), "{result:?}");
    assert!(writes.is_empty());
    assert!(!dir.path().join("txn").exists());
    assert_eq!(
        setup.hybrid_storage.budget_snapshot().total_committed_bytes,
        0
    );
    Ok(())
}

#[test]
fn should_hold_spill_disk_charge_through_commit_source_until_files_are_removed() -> MidgeResult<()>
{
    // Arrange
    let dir = tempfile::tempdir()?;
    let setup = crate::storage::simulated::build_simulated_cloud_stores(dir.path(), Some(4096))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(4096);
    let storage = &setup.hybrid_storage;
    let mut writes = TransactionWriteSet::new(
        Arc::new(TransactionMemoryPool::new(0)),
        dir.path(),
        false,
        1,
    )
    .with_storage_budget(Some(Arc::clone(storage)));
    writes.push(put(b"key", &[0x5A; 256]))?;
    let charged = storage.budget_snapshot().total_committed_bytes;

    // Act
    let source = writes.take_source();
    drop(writes);
    let wal_admission = storage.admit_local_wal_bytes(4097 - charged);

    // Assert
    assert!(charged > 0);
    assert_eq!(storage.budget_snapshot().total_committed_bytes, charged);
    assert!(matches!(wal_admission, Err(MidgeError::NoSpace(_))));
    assert_eq!(fs::read_dir(dir.path().join("txn"))?.count(), 2);
    drop(source);
    assert_eq!(fs::read_dir(dir.path().join("txn"))?.count(), 0);
    assert_eq!(storage.budget_snapshot().total_committed_bytes, 0);
    storage.admit_local_wal_bytes(4096)?;
    Ok(())
}

#[test]
fn should_reject_oversized_range_before_staging_or_spilling() -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let limit = crate::codec::MAX_DECOMPRESSED_BLOCK_SIZE;
    for (start_len, end_len) in [(limit, 1), (1, limit), (limit / 2, limit / 2)] {
        let pool = Arc::new(TransactionMemoryPool::new(1024));
        let mut writes = TransactionWriteSet::new(pool.clone(), dir.path(), false, 1);
        writes.push(put(b"safe", b"value"))?;
        let reserved = pool.resident.load(Ordering::Acquire);
        // Act
        let result = writes.push(TransactionOp::DeleteRange {
            cf_id: 0,
            start_key: Bytes::from(vec![b'a'; start_len]),
            end_key: Bytes::from(vec![b'z'; end_len]),
        });
        // Assert
        assert!(
            matches!(result, Err(MidgeError::ResourceLimit(_))),
            "oversized range was admitted: {result:?}"
        );
        assert_eq!(writes.next_ordinal, 1);
        assert!(!writes.has_spills());
        assert_eq!(pool.resident.load(Ordering::Acquire), reserved);
        assert!(
            matches!(writes.latest_for_key(b"safe")?, Some(IntentLookup::Present(value)) if value.as_ref() == b"value")
        );
    }
    Ok(())
}

#[test]
fn should_cover_range_tree_endpoint_duplication_with_spill_disk_reservation() -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let setup =
        crate::storage::simulated::build_simulated_cloud_stores(dir.path(), Some(1024 * 1024))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(1024 * 1024);
    let budget = SpillDiskBudget(Arc::clone(&setup.hybrid_storage));
    let mut ops = (0_u64..64)
        .map(|ordinal| OrdinalOp {
            ordinal,
            op: delete_range(
                format!("a{ordinal:02}").as_bytes(),
                if ordinal == 63 { &[b'z'; 4096] } else { b"m" },
            ),
        })
        .collect::<Vec<_>>();

    // Act
    let pool = Arc::new(TransactionMemoryPool::new(usize::MAX));
    let run = write_run_with_budget(
        &dir.path().join("txn"),
        1,
        0,
        &mut ops,
        Some(&budget),
        &pool,
    )?;
    let bytes = fs::metadata(&run.path)?.len() + fs::metadata(&run.range_path)?.len();

    // Assert
    assert!(setup.hybrid_storage.budget_snapshot().total_committed_bytes >= bytes);
    drop(run);
    assert_eq!(
        setup.hybrid_storage.budget_snapshot().total_committed_bytes,
        0
    );
    Ok(())
}

#[test]
fn should_retain_spill_disk_charge_when_run_cleanup_fails() -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let setup = crate::storage::simulated::build_simulated_cloud_stores(dir.path(), Some(4096))?;
    setup.hybrid_storage.enable_ephemeral_sst_cache(4096);
    let mut writes = TransactionWriteSet::new(
        Arc::new(TransactionMemoryPool::new(0)),
        dir.path(),
        false,
        1,
    )
    .with_storage_budget(Some(Arc::clone(&setup.hybrid_storage)));
    writes.push(put(b"key", b"value"))?;
    let run_path = writes.runs[0].path.clone();
    let charged = setup.hybrid_storage.budget_snapshot().total_committed_bytes;
    fs::remove_file(&run_path)?;
    fs::create_dir(&run_path)?;

    // Act
    drop(writes);

    // Assert
    assert!(
        run_path.is_dir(),
        "failed cleanup residue remains accounted for"
    );
    assert_eq!(
        setup.hybrid_storage.budget_snapshot().total_committed_bytes,
        charged
    );
    Ok(())
}

fn put(key: &[u8], value: &[u8]) -> TransactionOp {
    TransactionOp::Put {
        cf_id: 0,
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
        ttl_seconds: None,
        insert_only: false,
    }
}

fn delete_range(start: &[u8], end: &[u8]) -> TransactionOp {
    TransactionOp::DeleteRange {
        cf_id: 0,
        start_key: Bytes::copy_from_slice(start),
        end_key: Bytes::copy_from_slice(end),
    }
}

#[test]
fn should_distinguish_absent_ttl_from_maximum_ttl_when_spilling_transaction_ops() -> MidgeResult<()>
{
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![
        OrdinalOp {
            ordinal: 0,
            op: put(b"no-ttl", b"value"),
        },
        OrdinalOp {
            ordinal: 1,
            op: TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"max-ttl"),
                value: Bytes::from_static(b"value"),
                ttl_seconds: Some(u64::MAX),
                insert_only: false,
            },
        },
    ];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;

    // Act
    let mut decoded = Vec::new();
    for_each_run_ordinal(&run, |ordinal_op| {
        if let TransactionOp::Put { ttl_seconds, .. } = ordinal_op.op {
            decoded.push(ttl_seconds);
        }
        Ok(())
    })?;

    // Assert
    assert_eq!(decoded, vec![None, Some(u64::MAX)]);
    Ok(())
}

#[test]
fn should_reject_corrupt_data_frame_when_reading_spill_run() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![OrdinalOp {
        ordinal: 0,
        op: put(b"key", b"value"),
    }];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut bytes = fs::read(&run.path)?;
    bytes[RUN_HEADER_LEN + 8] ^= 0x55;
    fs::write(&run.path, bytes)?;

    // Act
    let result = for_each_run_ordinal(&run, |_| Ok(()));

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    Ok(())
}

#[test]
fn should_reject_corrupt_sparse_index_when_reading_spill_run() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![OrdinalOp {
        ordinal: 0,
        op: put(b"key", b"value"),
    }];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut file = RunFile::open(&run.path)?;
    let header = read_header(&mut file)?;
    drop(file);
    let mut bytes = fs::read(&run.path)?;
    let corrupt_at = usize::try_from(header.sparse_index_offset)
        .map_err(|_| MidgeError::Corruption("index offset exceeds usize".to_string()))?
        + 8;
    bytes[corrupt_at] ^= 0x55;
    fs::write(&run.path, bytes)?;

    // Act
    let result = for_each_run_ordinal(&run, |_| Ok(()));

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    Ok(())
}

#[test]
fn should_resolve_range_tombstone_before_ordinal_ceiling() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![
        OrdinalOp {
            ordinal: 0,
            op: put(b"middle", b"before"),
        },
        OrdinalOp {
            ordinal: 1,
            op: delete_range(b"alpha", b"omega"),
        },
        OrdinalOp {
            ordinal: 2,
            op: put(b"middle", b"after"),
        },
    ];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;

    // Act
    let mut before_final_put = None;
    lookup_run_key(&run, b"middle", 2, &mut before_final_put)?;
    let mut after_final_put = None;
    lookup_run_key(&run, b"middle", 3, &mut after_final_put)?;

    // Assert
    assert!(matches!(before_final_put, Some((1, IntentLookup::Deleted))));
    assert!(matches!(
        after_final_put,
        Some((2, IntentLookup::Present(value))) if value == Bytes::from_static(b"after")
    ));
    Ok(())
}

#[test]
fn should_reject_corrupt_range_index_when_reading_spill_run() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![OrdinalOp {
        ordinal: 0,
        op: delete_range(b"alpha", b"omega"),
    }];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut file = RunFile::open(&run.range_path)?;
    let header = read_range_header(&mut file)?;
    drop(file);
    let mut bytes = fs::read(&run.range_path)?;
    let corrupt_at = usize::try_from(header.node_section_offset)
        .map_err(|_| MidgeError::Corruption("range offset exceeds usize".to_string()))?
        + 8;
    bytes[corrupt_at] ^= 0x55;
    fs::write(&run.range_path, bytes)?;

    // Act
    let result = lookup_run_key(&run, b"middle", u64::MAX, &mut None);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    Ok(())
}

#[test]
fn should_stream_large_spill_record_with_wal_sized_bound() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let value = vec![b'x'; 2 * 1024 * 1024];
    let mut ops = vec![OrdinalOp {
        ordinal: 0,
        op: put(b"large", &value),
    }];

    // Act
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut read_value = None;
    for_each_run_ordinal(&run, |ordinal_op| {
        if let TransactionOp::Put { value, .. } = ordinal_op.op {
            read_value = Some(value);
        }
        Ok(())
    })?;

    // Assert
    assert_eq!(read_value.as_ref().map(Bytes::len), Some(value.len()));
    Ok(())
}

#[test]
fn should_scan_spill_keys_in_both_directions_across_sparse_chunks() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = (0_u64..40)
        .map(|ordinal| OrdinalOp {
            ordinal,
            op: put(format!("key-{ordinal:02}").as_bytes(), b"value"),
        })
        .collect::<Vec<_>>();
    let run = write_run(temp.path(), 1, 0, &mut ops)?;

    // Act
    let forward = RunKeyCursor::new(&run, Some(b"key-10"), Some(b"key-20"), false)?
        .collect::<MidgeResult<Vec<_>>>()?;
    let reverse = RunKeyCursor::new(&run, Some(b"key-10"), Some(b"key-20"), true)?
        .collect::<MidgeResult<Vec<_>>>()?;

    // Assert
    let expected = (10_u64..20)
        .map(|ordinal| Bytes::from(format!("key-{ordinal:02}")))
        .collect::<Vec<_>>();
    assert_eq!(forward, expected);
    assert_eq!(reverse, expected.into_iter().rev().collect::<Vec<_>>());
    Ok(())
}

#[test]
fn should_surface_late_spill_corruption_from_key_cursor_item() -> MidgeResult<()> {
    // Arrange
    let temp = tempfile::tempdir()?;
    let mut ops = vec![
        OrdinalOp {
            ordinal: 0,
            op: put(b"alpha", b"one"),
        },
        OrdinalOp {
            ordinal: 1,
            op: put(b"bravo", b"two"),
        },
    ];
    let run = write_run(temp.path(), 1, 0, &mut ops)?;
    let mut file = RunFile::open(&run.path)?;
    file.seek_to(RUN_HEADER_LEN as u64)?;
    let (_, second_offset) = read_op_frame(&mut file)?;
    drop(file);
    let mut bytes = fs::read(&run.path)?;
    let corrupt_at = u64_to_usize(second_offset)?.saturating_add(8);
    bytes[corrupt_at] ^= 0x55;
    fs::write(&run.path, bytes)?;
    let mut scan = RunKeyCursor::new(&run, None, None, false)?;

    // Act
    let first = scan.next().transpose()?;
    let late = scan.next().expect("second item must report corruption");

    // Assert
    assert_eq!(first, Some(Bytes::from_static(b"alpha")));
    assert!(matches!(late, Err(MidgeError::Corruption(_))));
    Ok(())
}

#[test]
fn should_find_earliest_same_key_intent_when_key_spans_many_sparse_index_strides() -> MidgeResult<()>
{
    // Arrange: one run holds 40 writes to the same key, far more than one
    // 16-record sparse-index stride, so the earliest ones sit well before the
    // last index entry for that key.
    let dir = tempfile::tempdir()?;
    let mut writes = TransactionWriteSet::new(
        Arc::new(TransactionMemoryPool::new(64 * 1024)),
        dir.path(),
        false,
        1,
    );
    for index in 0_u8..40 {
        writes.push(put(b"k", &[index; 8]))?;
    }
    writes.push(put(b"spill-trigger", &vec![0x5A; 128 * 1024]))?;
    assert!(
        writes.has_spills(),
        "the resident writes must have spilled to a run"
    );

    let source = writes.take_source();

    // Act
    let before_second = source.latest_before(1, b"k")?;
    let before_twentieth = source.latest_before(20, b"k")?;

    // Assert
    assert!(
        before_second.is_some(),
        "the first write to the key (ordinal 0) must be visible before ordinal 1"
    );
    assert!(before_twentieth.is_some());
    Ok(())
}

#[test]
fn should_bound_live_file_handles_when_scanning_spilled_runs() -> MidgeResult<()> {
    // Arrange: another transaction holds the pool, forcing every intent into
    // its own run. An eager scan cursor used to retain one additional handle
    // per run on top of the run's cached data and range handles.
    let dir = tempfile::tempdir()?;
    let pool = Arc::new(TransactionMemoryPool::new(4096));
    assert!(pool.try_reserve(4096));
    let mut writes = TransactionWriteSet::new(Arc::clone(&pool), dir.path(), false, 1);
    for index in 0_u32..32 {
        writes.push(put(format!("key-{index:04}").as_bytes(), b"value"))?;
    }
    let runs = writes.runs.len();
    assert_eq!(runs, 32, "each refused intent must become its own run");
    reset_peak_run_files();
    reset_sparse_index_decodes();

    // Act
    let keys = resolved_scan(&writes, None, None, true)?;
    let peak = peak_run_files();

    // Assert
    assert_eq!(keys.len(), 32);
    assert!(
        peak <= MAX_CACHED_SPILL_READERS * 2 + 1,
        "{peak} spill handles were live for a cache limit of {MAX_CACHED_SPILL_READERS}"
    );
    assert_eq!(sparse_index_decodes(), 0);
    drop(writes);
    pool.release(4096);
    Ok(())
}

#[test]
fn should_hold_no_uncharged_intents_in_memory_when_other_transactions_hold_the_pool(
) -> MidgeResult<()> {
    // Arrange: another transaction holds the whole pool, so every push below is
    // refused admission. The pool is what bounds resident bytes across all
    // transactions, so a refused write must not stay in memory outside it.
    let dir = tempfile::tempdir()?;
    let pool = Arc::new(TransactionMemoryPool::new(64 * 1024));
    assert!(pool.try_reserve(64 * 1024));
    let mut writes = TransactionWriteSet::new(Arc::clone(&pool), dir.path(), false, 2);

    // Act
    for index in 0_u32..8 {
        writes.push(put(format!("key-{index:04}").as_bytes(), b"value"))?;
    }

    // Assert
    assert!(
        writes.resident.is_empty(),
        "{} refused intents were held in memory outside the pool",
        writes.resident.len()
    );
    for index in 0_u32..8 {
        let key = format!("key-{index:04}");
        assert!(
            matches!(
                writes.latest_for_key(key.as_bytes())?,
                Some(IntentLookup::Present(value)) if value.as_ref() == b"value"
            ),
            "{key} was lost while spilling under pool pressure"
        );
    }
    drop(writes);
    pool.release(64 * 1024);
    Ok(())
}

#[test]
fn should_look_up_spilled_key_when_pool_cannot_charge_sparse_index() -> MidgeResult<()> {
    // Arrange: a pool with no capacity refuses the cached index, so the reader
    // must fall back to walking the index in the file.
    let temp = tempfile::tempdir()?;
    let pool = Arc::new(TransactionMemoryPool::new(0));
    let mut ops = (0_u64..40)
        .map(|ordinal| OrdinalOp {
            ordinal,
            op: put(format!("key-{ordinal:02}").as_bytes(), b"value"),
        })
        .collect::<Vec<_>>();
    let run = write_run_with_budget(temp.path(), 1, 0, &mut ops, None, &pool)?;
    reset_sparse_index_decodes();

    // Act
    let mut latest = None;
    lookup_run_key(&run, b"key-37", u64::MAX, &mut latest)?;

    // Assert
    assert!(
        matches!(latest, Some((37, IntentLookup::Present(value))) if value.as_ref() == b"value")
    );
    assert_eq!(pool.resident.load(Ordering::Acquire), 0);
    assert_eq!(
        sparse_index_decodes(),
        0,
        "a rejected reservation must fall back before decoding the sparse index"
    );
    Ok(())
}

/// Drives the intent scan the way a transaction scan does and returns each
/// distinct intent key with its resolved newest intent.
fn resolved_scan(
    writes: &TransactionWriteSet,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    reverse: bool,
) -> MidgeResult<Vec<(Bytes, Option<IntentLookup>)>> {
    let mut scan = writes.key_scan(start, end, reverse)?;
    let mut rows = Vec::new();
    while let Some(entry) = scan.next_entry(writes) {
        let entry = entry?;
        let lookup = scan.resolve(writes, &entry.key, entry.point)?;
        rows.push((entry.key, lookup));
    }
    Ok(rows)
}

/// Resolves every intent key through the scan path and returns the lookup
/// work it took.
fn scan_lookup_work(writes: &TransactionWriteSet) -> MidgeResult<u64> {
    let before = writes.lookup_work();
    resolved_scan(writes, None, None, false)?;
    Ok(writes.lookup_work() - before)
}

fn write_keys(
    pool_bytes: usize,
    dir: &Path,
    count: u32,
) -> MidgeResult<(TransactionWriteSet, Arc<TransactionMemoryPool>)> {
    let pool = Arc::new(TransactionMemoryPool::new(pool_bytes));
    let mut writes = TransactionWriteSet::new(Arc::clone(&pool), dir, false, 1);
    for index in 0..count {
        // Interleave key order so runs overlap in key space.
        let scrambled = index.wrapping_mul(7919) % count;
        writes.push(put(format!("key-{scrambled:06}").as_bytes(), b"value"))?;
    }
    Ok((writes, pool))
}

#[test]
fn should_not_quadruple_resident_lookup_work_when_operation_count_doubles() -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let (small, _small_pool) = write_keys(usize::MAX / 2, dir.path(), 500)?;
    let (large, _large_pool) = write_keys(usize::MAX / 2, dir.path(), 1000)?;
    assert!(!small.has_spills() && !large.has_spills());

    // Act
    let small_work = scan_lookup_work(&small)?;
    let large_work = scan_lookup_work(&large)?;

    // Assert: linear growth doubles the work (log factors stay under 2.4x);
    // quadratic growth quadruples it.
    assert!(
        large_work * 5 < small_work * 12,
        "work grew from {small_work} to {large_work}"
    );
    Ok(())
}

#[test]
fn should_not_quadruple_spilled_lookup_work_when_operation_count_doubles() -> MidgeResult<()> {
    // Arrange
    let dir = tempfile::tempdir()?;
    let (small, _small_pool) = write_keys(8 * 1024, dir.path(), 400)?;
    let large_dir = tempfile::tempdir()?;
    let (large, _large_pool) = write_keys(8 * 1024, large_dir.path(), 800)?;
    assert!(small.has_spills() && large.has_spills());

    // Act
    let small_work = scan_lookup_work(&small)?;
    let large_work = scan_lookup_work(&large)?;

    // Assert
    assert!(
        large_work * 5 < small_work * 12,
        "work grew from {small_work} to {large_work}"
    );
    Ok(())
}

fn delete(key: &[u8]) -> TransactionOp {
    TransactionOp::Delete {
        cf_id: 0,
        key: Bytes::copy_from_slice(key),
    }
}

fn model_key(n: u64) -> Vec<u8> {
    format!("k{n:02}").into_bytes()
}

#[derive(Debug, Clone, PartialEq)]
enum LookupView {
    Untouched,
    Deleted,
    Present(Vec<u8>),
}

fn lookup_view(lookup: Option<IntentLookup>) -> LookupView {
    match lookup {
        None => LookupView::Untouched,
        Some(IntentLookup::Deleted) => LookupView::Deleted,
        Some(IntentLookup::Present(value)) => LookupView::Present(value.to_vec()),
    }
}

#[test]
fn should_match_model_when_point_and_scan_lookups_follow_mixed_ops_in_every_residency(
) -> MidgeResult<()> {
    let mut state = 0x1234_5678_u64;
    let mut next = move |bound: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % bound
    };
    // Resident only, mixed resident and spilled, and one run per operation.
    for pool_bytes in [usize::MAX / 2, 3 * 1024, 0] {
        for round in 0..3 {
            // Arrange
            let dir = tempfile::tempdir()?;
            let pool = Arc::new(TransactionMemoryPool::new(pool_bytes));
            let mut writes = TransactionWriteSet::new(Arc::clone(&pool), dir.path(), false, 1);
            let mut model: std::collections::BTreeMap<Vec<u8>, LookupView> =
                std::collections::BTreeMap::new();
            for op_index in 0..50_u64 {
                match next(4) {
                    0 | 1 => {
                        let key = model_key(next(40));
                        let value = format!("v{round}-{op_index}").into_bytes();
                        writes.push(put(&key, &value))?;
                        model.insert(key, LookupView::Present(value));
                    }
                    2 => {
                        let key = model_key(next(40));
                        writes.push(delete(&key))?;
                        model.insert(key, LookupView::Deleted);
                    }
                    _ => {
                        let (a, b) = (next(40), next(40));
                        let (low, high) = (a.min(b), a.max(b));
                        writes.push(delete_range(&model_key(low), &model_key(high + 1)))?;
                        for n in low..=high {
                            model.insert(model_key(n), LookupView::Deleted);
                        }
                    }
                }
            }
            if pool_bytes != usize::MAX / 2 {
                assert!(writes.has_spills(), "pool {pool_bytes} did not spill");
            }

            // Act
            let mut point_views = Vec::new();
            for n in 0..42 {
                point_views.push(lookup_view(writes.latest_for_key(&model_key(n))?));
            }

            // Assert: point lookups match the model.
            for (n, view) in point_views.into_iter().enumerate() {
                assert_eq!(
                    view,
                    model
                        .get(&model_key(n as u64))
                        .cloned()
                        .unwrap_or(LookupView::Untouched),
                    "pool {pool_bytes} round {round} get {n}"
                );
            }

            // Assert: scans walk every key, including keys only a
            // snapshot would hold, in each direction and with bounds.
            for (start, end) in [(None, None), (Some(model_key(10)), Some(model_key(30)))] {
                for reverse in [false, true] {
                    let mut stream: Vec<Vec<u8>> = (0..42)
                        .map(model_key)
                        .filter(|key| {
                            start.as_ref().is_none_or(|start| key >= start)
                                && end.as_ref().is_none_or(|end| key < end)
                        })
                        .collect();
                    if reverse {
                        stream.reverse();
                    }
                    let mut scan = writes.key_scan(start.as_deref(), end.as_deref(), reverse)?;
                    let mut head = scan.next_entry(&writes).transpose()?;
                    for key in stream {
                        let point = if head.as_ref().is_some_and(|entry| entry.key == key) {
                            let entry = head.take().expect("matched head");
                            head = scan.next_entry(&writes).transpose()?;
                            entry.point
                        } else {
                            None
                        };
                        let resolved = lookup_view(scan.resolve(&writes, &key, point)?);
                        assert_eq!(
                            resolved,
                            model.get(&key).cloned().unwrap_or(LookupView::Untouched),
                            "pool {pool_bytes} round {round} reverse {reverse} scan {key:?}"
                        );
                    }
                    assert!(head.is_none(), "scan produced keys outside the stream");
                }
            }
        }
    }
    Ok(())
}

#[test]
fn should_not_quadruple_range_delete_lookup_work_when_spilled_operation_count_doubles(
) -> MidgeResult<()> {
    // Arrange: puts interleaved with narrow range deletes, so most spill runs
    // hold a range index that a naive scan would stab once per key.
    let build = |count: u32, dir: &Path| -> MidgeResult<TransactionWriteSet> {
        let pool = Arc::new(TransactionMemoryPool::new(8 * 1024));
        let mut writes = TransactionWriteSet::new(pool, dir, false, 1);
        for index in 0..count {
            let scrambled = index.wrapping_mul(7919) % count;
            writes.push(put(format!("key-{scrambled:06}").as_bytes(), b"value"))?;
            if index % 5 == 0 {
                let start = format!("key-{scrambled:06}x");
                let end = format!("key-{scrambled:06}y");
                writes.push(delete_range(start.as_bytes(), end.as_bytes()))?;
            }
        }
        Ok(writes)
    };
    let small_dir = tempfile::tempdir()?;
    let large_dir = tempfile::tempdir()?;
    let small = build(400, small_dir.path())?;
    let large = build(800, large_dir.path())?;
    assert!(small.runs.iter().any(|run| run.range_count != 0));

    // Act
    let small_work = scan_lookup_work(&small)?;
    let large_work = scan_lookup_work(&large)?;

    // Assert
    assert!(
        large_work * 5 < small_work * 12,
        "work grew from {small_work} to {large_work}"
    );
    Ok(())
}
