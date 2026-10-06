use cntryl_midge::{
    Clock, Engine, MemoryBudget, MidgeError, OpenOptions, Query, TransactionMode, WriteOptions,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
struct ControlledClock(AtomicU64);
impl Clock for ControlledClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}
fn inventory(tx: &cntryl_midge::Transaction) -> BTreeMap<Vec<u8>, Vec<u8>> {
    tx.scan(&Query::new())
        .unwrap()
        .try_collect()
        .unwrap()
        .into_iter()
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
#[test]
// Keep the pressure/release order visible beside its exact-state assertions.
#[allow(clippy::too_many_lines)]
fn should_retain_frozen_history_and_resume_writes_when_spill_capacity_is_exhausted() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(ControlledClock(AtomicU64::new(10_000)));
    let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "snapshot-pressure")
        .memory_budget(MemoryBudget::Bytes(16 * 1024 * 1024))
        .local_storage_budget(2 * 1024 * 1024)
        .with_memtable_size_limit(256 * 1024)
        .with_memtable_flush_threshold(256 * 1024)
        .transaction_memory_pool_size(128 * 1024)
        .ttl_clock(clock.clone())
        .background_compaction(false)
        .storage_io_timeout(Duration::from_secs(2))
        .runtime_response_timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let mut engine = Engine::open(options.clone()).unwrap();
    let cf = engine.create_column_family("data").unwrap();
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
    engine.flush_cf(&cf).unwrap();
    let original_files: Vec<_> = engine
        .metrics()
        .get_storage_layout()
        .unwrap()
        .levels
        .into_iter()
        .flat_map(|level| level.files)
        .map(|file| file.name)
        .collect();
    assert!(
        !original_files.is_empty(),
        "the frozen snapshot must pin actual persisted SSTs"
    );
    let old = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    assert_exact(&old, &initial);
    clock.0.store(12_000, Ordering::Release);
    let mut expected = initial
        .iter()
        .filter(|(key, _)| key.as_slice() >= b"row-008".as_slice())
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let expired = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    assert_exact(&expired, &expected);
    drop(expired);
    // Act

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
        engine.flush_cf(&cf).unwrap();
        engine.compact_all().unwrap();
        assert_exact(&old, &initial);
        let current = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
        assert_exact(&current, &expected);
    }
    // Act: consume real caller-owned spill capacity while the old snapshot lives.
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
    let spill_bytes = std::fs::read_dir(directory.path().join("txn"))
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum::<u64>();
    assert!(
        spill_bytes > 1536 * 1024,
        "actual caller-owned spill files must consume most of the 2 MiB local budget"
    );
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
    assert_exact(&old, &initial);
    let current = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    assert_exact(&current, &expected);
    drop(current);
    drop(hog);
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
    let live_files: Vec<_> = engine
        .metrics()
        .get_storage_layout()
        .unwrap()
        .levels
        .into_iter()
        .flat_map(|level| level.files)
        .map(|file| file.name)
        .collect();
    let retired: Vec<_> = original_files
        .into_iter()
        .filter(|file| !live_files.contains(file))
        .map(|file| directory.path().join("cloud_store/sst").join(file))
        .collect();
    assert!(
        !retired.is_empty(),
        "actual compaction must retire pinned input files"
    );
    assert!(
        retired.iter().all(|file| file.exists()),
        "snapshot inputs must remain physically retained"
    );
    drop(old);
    let reclaim_deadline = Instant::now() + Duration::from_secs(10);
    while retired.iter().any(|file| file.exists()) {
        assert!(
            Instant::now() < reclaim_deadline,
            "all released snapshot inputs must be physically reclaimed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
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
    expected.insert(b"released".to_vec(), b"progress".to_vec());
    engine.flush_cf(&cf).unwrap();
    engine.shutdown(Duration::from_secs(15)).unwrap();
    drop(engine);
    let mut reopened = Engine::open(options).unwrap();
    let cf = reopened.get_column_family("data").unwrap();
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    assert_exact(&read, &expected);
    drop(read);
    reopened.shutdown(Duration::from_secs(15)).unwrap();
}
