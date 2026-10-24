//! Remote SST blocks are cached on local disk (#938): a working set larger than
//! the block cache stops paying a provider range read per miss once warm, and a
//! cold reader open costs one HEAD plus one tail read instead of one request
//! per metadata block.

use cntryl_midge::__internal::simulation::{Operation, Schedule, SimulatedObjectStore};
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, Engine, EngineHealth, MemoryBudget, OpenOptions,
    RecoveryPolicy, TransactionMode, WriteOptions,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const VALUE_BYTES: usize = 4096;
const KEYS_PER_SST: usize = 256;
const SST_COUNT: usize = 4;
/// Commits stay under the WAL replay bound of the small memory budget.
const KEYS_PER_COMMIT: usize = 32;

fn key(index: usize) -> Vec<u8> {
    format!("key:{index:06}").into_bytes()
}

/// Incompressible, so each block holds a few values and the SSTs really span
/// four times the block cache; a constant fill would compress to nothing.
fn value(index: usize) -> Vec<u8> {
    let mut state = u64::try_from(index).expect("index fits") ^ 0x9e37_79b9_7f4a_7c15;
    (0..VALUE_BYTES)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            u8::try_from(state & 0xff).expect("one byte")
        })
        .collect()
}

fn provider_options(cache: &Path, store: &SimulatedObjectStore) -> OpenOptions {
    OpenOptions::cloud(
        cache,
        CloudStorageLocation::new(
            CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
            "block-cache/",
        ),
    )
    .object_store_for_testing(Arc::new(store.process()))
    // About 1 MiB of block cache under 4 MiB of published data.
    .memory_budget(MemoryBudget::Bytes(8 * 1024 * 1024))
    .background_compaction(false)
    .build()
    .expect("provider options")
}

/// Publish four SSTs to the simulated provider and reopen cold, so every block
/// read goes to the provider.
fn cold_engine(dir: &Path, store: &SimulatedObjectStore) -> Engine {
    let mut engine = Engine::open(provider_options(dir, store)).expect("seed engine");
    let cf = engine.create_column_family("data").expect("create CF");
    for sst in 0..SST_COUNT {
        for chunk in 0..KEYS_PER_SST / KEYS_PER_COMMIT {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("write transaction");
            for index in 0..KEYS_PER_COMMIT {
                let global = sst * KEYS_PER_SST + chunk * KEYS_PER_COMMIT + index;
                tx.put(key(global), value(global), None).expect("put");
            }
            tx.commit(WriteOptions::cloud_strict())
                .expect("cloud commit");
        }
        engine.flush_cf(&cf).expect("flush");
    }
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
    drop(engine);
    Engine::open(provider_options(dir, store)).expect("cold engine")
}

fn range_reads(store: &SimulatedObjectStore) -> u64 {
    store
        .request_counts()
        .get(&Operation::RangeRead)
        .copied()
        .unwrap_or(0)
}

fn requests(store: &SimulatedObjectStore) -> BTreeMap<Operation, u64> {
    store.request_counts()
}

fn read_working_set(engine: &Engine, cf_id: u32) {
    let tx = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("read transaction");
    for step in 0..1_024_usize {
        let index = (step * 97) % (SST_COUNT * KEYS_PER_SST);
        let value = tx.get(&key(index)).expect("get").expect("key present");
        assert_eq!(value.len(), VALUE_BYTES);
    }
}

#[test]
fn should_cut_provider_range_reads_tenfold_given_working_set_four_times_block_cache_when_warm() {
    // Arrange: a cold engine whose working set is four times its block cache.
    let dir = tempfile::tempdir().expect("database directory");
    let store = SimulatedObjectStore::eager();
    let mut engine = cold_engine(dir.path(), &store);
    let cf = engine.get_column_family("data").expect("recovered CF");
    let before_cold = range_reads(&store);
    read_working_set(&engine, cf.id());
    let cold_reads = range_reads(&store) - before_cold;

    // Act: the same reads again, now with the disk cache warm.
    let before_warm = range_reads(&store);
    read_working_set(&engine, cf.id());
    let warm_reads = range_reads(&store) - before_warm;

    // Assert: four 1 MiB SSTs of 64 KiB blocks, so the cold pass fetched most
    // of their blocks and the warm pass almost none.
    assert!(
        cold_reads >= 48,
        "cold pass issued {cold_reads} range reads"
    );
    assert!(
        warm_reads * 10 <= cold_reads,
        "warm pass issued {warm_reads} range reads against {cold_reads} cold"
    );
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
}

#[test]
fn should_open_cold_reader_with_at_most_two_provider_requests_given_published_sst_when_read() {
    // Arrange
    let dir = tempfile::tempdir().expect("database directory");
    let store = SimulatedObjectStore::eager();
    let mut engine = cold_engine(dir.path(), &store);
    let cf = engine.get_column_family("data").expect("recovered CF");
    let before = requests(&store);

    // Act: the first read of one SST opens its reader and fetches one block.
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read transaction");
    assert!(tx.get(&key(0)).expect("get").is_some());
    let after = requests(&store);

    // Assert: one HEAD, one tail read for the metadata, one block read.
    let delta = |operation: Operation| {
        after.get(&operation).copied().unwrap_or(0) - before.get(&operation).copied().unwrap_or(0)
    };
    assert_eq!(delta(Operation::Head), 1, "HEAD requests");
    assert!(
        delta(Operation::RangeRead) <= 2,
        "{} range reads for a cold open plus one block",
        delta(Operation::RangeRead)
    );
    drop(tx);
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
}

fn cache_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let cache_dir = dir.join("sst-cache");
    match std::fs::read_dir(&cache_dir) {
        Ok(entries) => entries
            .map(|entry| entry.expect("cache entry").path())
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("list {}: {error}", cache_dir.display()),
    }
}

#[test]
fn should_fall_through_to_provider_given_corrupted_cache_files_when_read() {
    // Arrange: a warm cache, then every cached entry is corrupted on disk.
    let dir = tempfile::tempdir().expect("database directory");
    let store = SimulatedObjectStore::eager();
    let mut engine = cold_engine(dir.path(), &store);
    let cf = engine.get_column_family("data").expect("recovered CF");
    read_working_set(&engine, cf.id());
    let files = cache_files(dir.path());
    assert!(!files.is_empty(), "the warm pass left cache entries");
    for path in &files {
        let mut bytes = std::fs::read(path).expect("read cache entry");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(path, bytes).expect("corrupt cache entry");
    }

    // Act: reads verify every value; corrupt entries fall through and are
    // replaced, so a further pass is cheap again.
    let before_corrupt = range_reads(&store);
    read_working_set(&engine, cf.id());
    let corrupt_pass = range_reads(&store) - before_corrupt;
    let before_repaired = range_reads(&store);
    read_working_set(&engine, cf.id());
    let repaired_pass = range_reads(&store) - before_repaired;

    // Assert
    assert!(
        corrupt_pass >= 48,
        "corrupt pass fell through {corrupt_pass} times"
    );
    assert!(
        repaired_pass * 10 <= corrupt_pass,
        "repaired pass issued {repaired_pass} range reads against {corrupt_pass}"
    );
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
}

#[test]
fn should_bound_cache_bytes_to_its_share_given_small_local_budget_when_working_set_exceeds_it() {
    // Arrange: a 32 MiB local budget gives the cache an 8 MiB share under 12 MiB
    // of published data (twelve 1 MiB SSTs).
    const LOCAL_BUDGET: u64 = 32 * 1024 * 1024;
    const CACHE_SHARE: u64 = LOCAL_BUDGET / 4;
    const SSTS: usize = 12;
    // Background compaction may have one insert in flight, whose 64 KiB block
    // is on disk before the entries it evicts are removed.
    const IN_FLIGHT_SLACK: u64 = 2 * 64 * 1024;
    let dir = tempfile::tempdir().expect("database directory");
    let store = SimulatedObjectStore::eager();
    let options = |cache: &Path| {
        OpenOptions::cloud(
            cache,
            CloudStorageLocation::new(
                CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
                "block-cache-budget/",
            ),
        )
        .object_store_for_testing(Arc::new(store.process()))
        .memory_budget(MemoryBudget::Bytes(8 * 1024 * 1024))
        .local_storage_budget(LOCAL_BUDGET)
        // Twelve L0 files plus their frozen generations would hit the L0
        // ceiling without compaction; the cache bound does not depend on it.
        .background_compaction(true)
        .build()
        .expect("provider options")
    };
    let mut engine = Engine::open(options(dir.path())).expect("seed engine");
    let cf = engine.create_column_family("data").expect("create CF");
    for sst in 0..SSTS {
        for chunk in 0..KEYS_PER_SST / 32 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("write transaction");
            for index in 0..32 {
                let global = sst * KEYS_PER_SST + chunk * 32 + index;
                tx.put(key(global), value(global), None).expect("put");
            }
            tx.commit(WriteOptions::cloud_strict())
                .expect("cloud commit");
        }
        engine.flush_cf(&cf).expect("flush");
    }
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
    drop(engine);
    let mut engine = Engine::open(options(dir.path())).expect("cold engine");
    let cf = engine.get_column_family("data").expect("recovered CF");

    // Act: read every key once, then publish one more SST.
    {
        let tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        for index in 0..SSTS * KEYS_PER_SST {
            assert!(tx.get(&key(index)).expect("get").is_some());
        }
    }
    let cached_bytes: u64 = cache_files(dir.path())
        .iter()
        .map(|path| std::fs::metadata(path).expect("entry metadata").len())
        .sum();
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("write transaction");
    tx.put(key(usize::MAX / 2), value(7), None).expect("put");
    tx.commit(WriteOptions::cloud_strict())
        .expect("commit with a full cache");
    engine.flush_cf(&cf).expect("flush with a full cache");

    // Assert: the cache stayed within its quarter and never starved the flush.
    assert!(
        cached_bytes > CACHE_SHARE / 2,
        "cache holds only {cached_bytes} bytes"
    );
    assert!(
        cached_bytes <= CACHE_SHARE + IN_FLIGHT_SLACK,
        "cache holds {cached_bytes} bytes against a {CACHE_SHARE}-byte share"
    );
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
}

#[test]
fn should_keep_disk_cache_given_salvaged_local_sst_when_reading_published_sst() {
    // Arrange: four published SSTs; the remote copy of one is corrupted while
    // a verified local copy survives, so a salvage open retains it and
    // rebuilds its SST read view around that override.
    let dir = tempfile::tempdir().expect("database directory");
    let seed = SimulatedObjectStore::eager();
    let mut seeded = cold_engine(dir.path(), &seed);
    seeded
        .shutdown(Duration::from_secs(30))
        .expect("release the seed lease");
    drop(seeded);
    let mut image = seed.image();
    let salvaged = image
        .objects
        .keys()
        .find(|key| {
            std::path::Path::new(key)
                .extension()
                .is_some_and(|extension| extension == "sst")
        })
        .cloned()
        .expect("a published SST");
    let basename = salvaged.rsplit('/').next().expect("object basename");
    let local = dir.path().join("sst").join(basename);
    std::fs::create_dir_all(local.parent().expect("sst dir")).expect("local sst dir");
    std::fs::write(&local, &image.objects[&salvaged]).expect("preserve the local copy");
    image.objects.get_mut(&salvaged).expect("remote copy")[0] ^= 1;
    let store = SimulatedObjectStore::from_image(Schedule::Eager, &image);
    let options = OpenOptions::cloud(
        dir.path(),
        CloudStorageLocation::new(
            CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
            "block-cache/",
        ),
    )
    .object_store_for_testing(Arc::new(store.process()))
    .memory_budget(MemoryBudget::Bytes(8 * 1024 * 1024))
    .background_compaction(false)
    .recovery_policy(RecoveryPolicy::Salvage)
    .build()
    .expect("salvage options");

    // Act: a salvage open, then two passes over every SST.
    let mut engine = Engine::open(options).expect("salvage reopen");
    let cf = engine.get_column_family("data").expect("recovered CF");
    assert!(
        local.exists(),
        "salvage must preserve the only verified SST copy"
    );
    assert_eq!(
        engine
            .metrics()
            .get_runtime_metrics()
            .expect("metrics")
            .health,
        EngineHealth::SalvageMode,
        "the open retained the local copy and rebuilt its SST read view"
    );
    read_working_set(&engine, cf.id());
    let before_warm = range_reads(&store);
    read_working_set(&engine, cf.id());
    let warm_reads = range_reads(&store) - before_warm;

    // Assert: the published SSTs are served from the disk cache, so the warm
    // pass reaches the provider for nothing.
    assert_eq!(
        warm_reads, 0,
        "salvage recovery dropped the remote block cache: {warm_reads} range reads warm"
    );
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
}
