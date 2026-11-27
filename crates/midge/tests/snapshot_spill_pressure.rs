use cntryl_midge::{
    Clock, ColumnFamilyHandle, Engine, MemoryBudget, MidgeError, OpenOptions, Query, Transaction,
    TransactionMode, WriteOptions,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const LOCAL_STORAGE_BYTES: u64 = 2 * 1024 * 1024;
const TRANSACTION_POOL_BYTES: usize = 128 * 1024;

struct ControlledClock(AtomicU64);
impl Clock for ControlledClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}
fn inventory(tx: &cntryl_midge::Transaction) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let rows = tx.scan(&Query::new()).unwrap().try_collect().unwrap();
    assert!(
        rows.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "scan keys must be unique and ordered"
    );
    rows.into_iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect()
}
fn payload(mut seed: u32, len: usize) -> Vec<u8> {
    (0..len)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed.to_le_bytes()[0]
        })
        .collect()
}
fn assert_exact(tx: &cntryl_midge::Transaction, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    assert_eq!(&inventory(tx), expected);
    for i in 0..32 {
        let key = format!("row-{i:03}").into_bytes();
        assert_eq!(
            tx.get(&key).unwrap().map(|v| v.to_vec()),
            expected.get(&key).cloned()
        );
    }
    for i in 0..128 {
        assert_eq!(
            tx.get(format!("uncommitted-{i:03}").as_bytes()).unwrap(),
            None
        );
    }
    assert_eq!(
        tx.get(b"released").unwrap().map(|v| v.to_vec()),
        expected.get(b"released".as_slice()).cloned()
    );
}
fn seed_rows(engine: &Engine, cf: &ColumnFamilyHandle) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut initial = BTreeMap::new();
    for i in 0..32 {
        let key = format!("row-{i:03}").into_bytes();
        let value = payload(u32::try_from(i + 1).unwrap(), 8192);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(
            key.clone(),
            value.clone(),
            if i < 8 { Some(1) } else { None },
        )
        .unwrap();
        tx.commit(WriteOptions::cloud_strict()).unwrap();
        initial.insert(key, value);
    }
    engine.flush_cf(cf).unwrap();
    initial
}

fn sst_files(engine: &Engine) -> Vec<String> {
    engine
        .metrics()
        .get_storage_layout()
        .unwrap()
        .levels
        .into_iter()
        .flat_map(|level| level.files)
        .map(|file| file.name)
        .collect()
}

fn assert_only_flushed_wal_blocks_staging(
    metrics: &cntryl_midge::RuntimeMetricsSnapshot,
    cf: &ColumnFamilyHandle,
) -> cntryl_midge::StorageAdmissionBlock {
    let local = metrics.local_storage.expect("cloud disk ledger");
    let blocked = local
        .blocked_admission
        .expect("staging admission rejection");
    assert_eq!(
        blocked.operation,
        cntryl_midge::StorageAdmissionKind::Compaction
    );
    assert_eq!(
        blocked.reason,
        cntryl_midge::StorageAdmissionReason::LocalCapacity
    );
    assert!(blocked.requested_bytes > blocked.free_bytes_at_rejection);
    assert_eq!(
        local.total_committed_bytes, local.usage.wal_bytes,
        "{local:?}"
    );
    assert_eq!(
        local.total_committed_bytes + local.free_bytes,
        local.max_local_bytes
    );
    assert_eq!(
        [
            local.usage.transaction_spill_bytes,
            local.usage.resident_sst_bytes,
            local.usage.startup_residue_bytes,
            local.usage.flush_staging_reserved_bytes,
            local.usage.flush_headroom_reserved_bytes,
            local.usage.compaction_staging_reserved_bytes,
            local.usage.wal_headroom_reserved_bytes,
        ],
        [0; 7],
        "only acknowledged WAL may delay staging: {local:?}"
    );
    assert_eq!(metrics.wal_pending_writes, 0);
    assert_eq!(metrics.total_memtable_bytes, 0);
    assert_eq!(metrics.active_compactions, 0);
    assert_eq!(metrics.flush_inflight, 0);
    assert!(metrics.wal_cloud_durable_seq >= metrics.current_sequence);
    assert!(metrics.manifest_flush_frontiers.get(&cf.id()).is_some_and(
        |frontier| frontier.is_some_and(|through| through >= metrics.current_sequence)
    ));
    blocked
}

fn compact_after_flushed_wal_retires(engine: &Engine, cf: &ColumnFamilyHandle, round: usize) {
    let Err(error) = engine.compact_all() else {
        return;
    };
    assert!(
        matches!(&error, MidgeError::WriteStall(message)
            if message == "local compaction staging budget unavailable: RejectNoSpace"),
        "snapshot rewrite round {round}: {error}"
    );
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("retirement metrics");
    let blocked = assert_only_flushed_wal_blocks_staging(&before, cf);
    let initial = before.local_storage.unwrap();
    // Retirement may finish between the rejection and its metrics response.
    // The rejection records the occupied bytes at that exact admission point.
    let blocked_wal = initial.max_local_bytes - blocked.free_bytes_at_rejection;
    assert!(initial.usage.wal_bytes <= blocked_wal);
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        // Metrics requests leave ordinary runtime fairness/retirement active.
        // Retry only after the measured blocking WAL charge actually decreases.
        let current = engine
            .metrics()
            .get_runtime_metrics()
            .expect("retirement progress");
        let local = current.local_storage.expect("cloud disk ledger");
        assert_eq!(current.current_sequence, before.current_sequence);
        assert_eq!(local.total_committed_bytes, local.usage.wal_bytes);
        if local.usage.wal_bytes < blocked_wal && local.free_bytes >= blocked.requested_bytes {
            break;
        }
        assert!(Instant::now() < until, "WAL did not retire: {local:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
    engine
        .compact_all()
        .expect("one retry after verified WAL retirement");
}

fn rewrite_rows_under_snapshot(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    old: &Transaction,
    initial: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut expected = BTreeMap::new();
    for round in 0..16 {
        let mut candidate = BTreeMap::new();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"row-008".to_vec(), b"row-024".to_vec())
            .unwrap();
        for i in 0..32 {
            if (8..24).contains(&i) {
                continue;
            }
            let key = format!("row-{i:03}").into_bytes();
            let value = payload(u32::try_from(1000 + round * 32 + i).unwrap(), 8192);
            tx.put(key.clone(), value.clone(), None).unwrap();
            candidate.insert(key, value);
        }
        tx.commit(WriteOptions::cloud_strict())
            .expect("bounded mixed transaction fits the cloud staging window");
        expected = candidate;
        engine.flush_cf(cf).unwrap();
        compact_after_flushed_wal_retires(engine, cf, round);
        assert_exact(old, initial);
        let current = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_exact(&current, &expected);
    }
    expected
}

fn exhaust_spill_capacity(
    engine: &Engine,
    cf: &ColumnFamilyHandle,
    directory: &Path,
) -> (Transaction, u64) {
    let mut hog = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    let mut spill_boundary = false;
    for i in 0..128 {
        match hog.put(
            format!("uncommitted-{i:03}").into_bytes(),
            vec![9; 32768],
            None,
        ) {
            Ok(()) => (),
            Err(MidgeError::NoSpace(message)) => {
                eprintln!("spill boundary after {i} operations: {message}");
                spill_boundary = true;
                break;
            }
            Err(error) => panic!("unexpected spill error: {error}"),
        }
    }
    assert!(spill_boundary, "must exhaust actual spill capacity");
    let spill_charge = assert_spill_budget_pressure(engine, directory);
    (hog, spill_charge)
}

fn assert_spill_budget_pressure(engine: &Engine, directory: &Path) -> u64 {
    let spill_bytes = std::fs::read_dir(directory.join("txn"))
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum::<u64>();
    let local = engine
        .metrics()
        .get_runtime_metrics()
        .unwrap()
        .local_storage
        .expect("cloud disk ledger at spill rejection");
    eprintln!("measured spill bytes={spill_bytes}; local storage={local:?}");
    assert!(
        spill_bytes > u64::try_from(TRANSACTION_POOL_BYTES).unwrap(),
        "real spill files must exceed the resident transaction pool: {spill_bytes}"
    );
    let blocked = local
        .blocked_admission
        .expect("actual spill admission failure");
    assert_eq!(
        blocked.operation,
        cntryl_midge::StorageAdmissionKind::TransactionSpill
    );
    assert_eq!(
        blocked.reason,
        cntryl_midge::StorageAdmissionReason::LocalCapacity
    );
    assert!(blocked.requested_bytes > blocked.free_bytes_at_rejection);
    assert!(blocked.requested_bytes <= local.max_local_bytes);
    assert_eq!(local.max_local_bytes, LOCAL_STORAGE_BYTES);
    let usage = local.usage;
    assert!(usage.transaction_spill_bytes >= spill_bytes);
    assert_eq!(
        local.total_committed_bytes,
        [
            usage.wal_bytes,
            usage.transaction_spill_bytes,
            usage.resident_sst_bytes,
            usage.startup_residue_bytes,
            usage.flush_staging_reserved_bytes,
            usage.flush_headroom_reserved_bytes,
            usage.compaction_staging_reserved_bytes,
            usage.wal_headroom_reserved_bytes,
        ]
        .into_iter()
        .sum::<u64>(),
        "every shared disk charge is accounted: {local:?}"
    );
    assert_eq!(
        local.total_committed_bytes + local.free_bytes,
        LOCAL_STORAGE_BYTES
    );
    usage.transaction_spill_bytes
}

fn assert_spill_capacity_released(engine: &Engine, directory: &Path, released: u64) {
    assert_eq!(std::fs::read_dir(directory.join("txn")).unwrap().count(), 0);
    let local = engine
        .metrics()
        .get_runtime_metrics()
        .unwrap()
        .local_storage
        .expect("cloud disk ledger after spill owner drops");
    assert_eq!(local.usage.transaction_spill_bytes, 0, "{local:?}");
    assert!(
        local.free_bytes >= released,
        "released spill capacity returns: {local:?}"
    );
}

fn assert_rejects_write_under_pressure(engine: &Engine, cf: &ColumnFamilyHandle) {
    let mut blocked = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    let blocked_result = match blocked.put(b"released".to_vec(), vec![7; 65536], None) {
        Ok(()) => blocked.commit(WriteOptions::cloud_strict()),
        Err(error) => {
            drop(blocked);
            Err(error)
        }
    };
    assert!(
        matches!(
            blocked_result,
            Err(MidgeError::NoSpace(_) | MidgeError::WriteStall(_) | MidgeError::Busy(_))
        ),
        "capacity exhaustion must reject before acceptance: {blocked_result:?}"
    );
}

fn retained_snapshot_inputs(
    engine: &Engine,
    directory: &Path,
    original_files: Vec<String>,
) -> Vec<PathBuf> {
    let live_files = sst_files(engine);
    let retired: Vec<_> = original_files
        .into_iter()
        .filter(|file| !live_files.contains(file))
        .map(|file| directory.join("cloud_store/sst").join(file))
        .collect();
    assert!(
        !retired.is_empty(),
        "actual compaction must retire pinned input files"
    );
    assert!(
        retired.iter().all(|file| file.exists()),
        "snapshot inputs must remain physically retained"
    );
    retired
}

fn assert_reclaims_snapshot_inputs(retired: &[PathBuf]) {
    let reclaim_deadline = Instant::now() + Duration::from_secs(10);
    while retired.iter().any(|file| file.exists()) {
        assert!(
            Instant::now() < reclaim_deadline,
            "all released snapshot inputs must be physically reclaimed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn assert_resumes_compaction(engine: &Engine, cf: &ColumnFamilyHandle) {
    let deadline = Instant::now() + Duration::from_secs(10);
    engine.compact_all().unwrap();
    assert!(
        Instant::now() < deadline,
        "compaction must resume after releasing the pin"
    );
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"released".to_vec(), b"progress".to_vec(), None)
        .unwrap();
    tx.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(cf).unwrap();
}

fn assert_exact_after_reopen(
    mut engine: Engine,
    options: OpenOptions,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
) {
    engine.shutdown(Duration::from_secs(15)).unwrap();
    drop(engine);
    let mut reopened = Engine::open(options).unwrap();
    let cf = reopened.get_column_family("data").unwrap();
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    assert_exact(&read, expected);
    drop(read);
    reopened.shutdown(Duration::from_secs(15)).unwrap();
}

#[test]
fn should_preserve_snapshot_isolation_when_spill_pressure_is_released() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(ControlledClock(AtomicU64::new(10_000)));
    let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "snapshot-pressure")
        .memory_budget(MemoryBudget::Bytes(16 * 1024 * 1024))
        .local_storage_budget(LOCAL_STORAGE_BYTES)
        .with_memtable_size_limit(256 * 1024)
        .with_memtable_flush_threshold(256 * 1024)
        .transaction_memory_pool_size(TRANSACTION_POOL_BYTES)
        .ttl_clock(clock.clone())
        .background_compaction(false)
        .storage_io_timeout(Duration::from_secs(2))
        .runtime_response_timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let engine = Engine::open(options.clone()).unwrap();
    let cf = engine.create_column_family("data").unwrap();
    let initial = seed_rows(&engine, &cf);
    let original_files = sst_files(&engine);
    assert!(
        !original_files.is_empty(),
        "the frozen snapshot must pin actual persisted SSTs"
    );
    let old = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    assert_exact(&old, &initial);
    clock.0.store(12_000, Ordering::Release);
    let expired_expected = initial
        .iter()
        .filter(|(key, _)| key.as_slice() >= b"row-008".as_slice())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let expired = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    assert_exact(&expired, &expired_expected);
    drop(expired);

    // Act: consume real caller-owned spill capacity while the old snapshot lives.
    let mut expected = rewrite_rows_under_snapshot(&engine, &cf, &old, &initial);
    let (hog, spill_charge) = exhaust_spill_capacity(&engine, &cf, directory.path());
    assert_rejects_write_under_pressure(&engine, &cf);
    assert_exact(&old, &initial);
    let current = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    assert_exact(&current, &expected);
    drop(current);
    drop(hog);
    assert_spill_capacity_released(&engine, directory.path(), spill_charge);
    let mut resumed = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    resumed
        .put(b"released".to_vec(), vec![7; 65536], None)
        .unwrap();
    resumed.commit(WriteOptions::cloud_strict()).unwrap();
    expected.insert(b"released".to_vec(), vec![7; 65536]);
    assert_eq!(
        inventory(&old),
        initial,
        "releasing spill pressure must preserve the older snapshot"
    );

    // Assert
    assert_exact(&old, &initial);
    let current = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    assert_exact(&current, &expected);
    drop(current);
    let retired = retained_snapshot_inputs(&engine, directory.path(), original_files);
    drop(old);
    assert_reclaims_snapshot_inputs(&retired);
    assert_resumes_compaction(&engine, &cf);
    expected.insert(b"released".to_vec(), b"progress".to_vec());
    assert_exact_after_reopen(engine, options, &expected);
}
