//! The cloud-durable frontier API (#936): `Transaction::commit_with_sequence`,
//! `Engine::cloud_durable_sequence`, and `Engine::wait_cloud_durable`.
//!
//! The crash matrix in `docs/user-guides/transaction-durability-contract.md`
//! is exercised with the in-memory object-store simulator: a `cloud_async()`
//! write is visible at once, becomes cloud durable only when its upload lands,
//! and survives loss of the local cache only after that.
use cntryl_midge::__internal::simulation::{
    Fate, Operation, Schedule, SimulatedClock, SimulatedObjectStore,
};
use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, Engine, MidgeError, OpenOptions, TransactionMode,
    WriteOptions,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const LEASE_TTL: Duration = Duration::from_secs(18);

fn provider_options(
    cache: &Path,
    store: &SimulatedObjectStore,
    clock: &SimulatedClock,
) -> OpenOptions {
    OpenOptions::cloud(
        cache,
        CloudStorageLocation::new(
            CloudProviderConfig::aws_s3_static("bucket", "us-east-1", "access", "secret"),
            "durable-wait/",
        ),
    )
    .object_store_for_testing(Arc::new(store.process()))
    .lease_clock_for_testing(Arc::new(clock.clone()))
    .lease_ttl(LEASE_TTL)
    .background_compaction(false)
    .build()
    .expect("provider options")
}

/// Every WAL object write lands only when the test lands it; everything else
/// is eager, so the engine opens and reads normally while uploads hang.
fn late_wal_uploads() -> Schedule {
    Schedule::Seeded {
        seed: 7,
        fates: vec![Fate::Late],
        scope: vec!["durable-wait/wal/".to_string()],
        operations: vec![Operation::Put],
    }
}

fn commit_one(engine: &Engine, cf_id: u32, key: &[u8], value: &[u8]) -> u64 {
    let mut tx = engine
        .begin_tx(cf_id, TransactionMode::ReadWrite)
        .expect("begin");
    tx.put(key.to_vec(), value.to_vec(), None).expect("put");
    tx.commit_with_sequence(WriteOptions::cloud_async())
        .expect("cloud_async commit")
}

fn read_one(engine: &Engine, cf_id: u32, key: &[u8]) -> Option<bytes::Bytes> {
    engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("begin read")
        .get(key)
        .expect("get")
}

#[test]
fn should_cover_commit_sequence_given_cloud_async_write_when_its_upload_lands() {
    // Arrange
    let cache = tempfile::tempdir().expect("cache");
    let store = SimulatedObjectStore::new(Schedule::Eager);
    let clock = SimulatedClock::new();
    let mut engine = Engine::open(provider_options(cache.path(), &store, &clock)).expect("open");
    let cf_id = engine.get_column_family("default").expect("default").id();

    // Act
    let sequence = commit_one(&engine, cf_id, b"k", b"v");
    let covered = engine
        .wait_cloud_durable(sequence, Duration::from_secs(10))
        .expect("wait");

    // Assert: the wait returns once the frontier covers the commit, the
    // frontier query agrees, and the write was visible all along.
    assert!(covered, "eager uploads must cover sequence {sequence}");
    let frontier = engine.cloud_durable_sequence().expect("frontier");
    assert!(
        frontier >= sequence,
        "frontier {frontier} must cover the committed sequence {sequence}"
    );
    assert_eq!(
        read_one(&engine, cf_id, b"k"),
        Some(bytes::Bytes::from_static(b"v"))
    );
    engine.shutdown(Duration::from_secs(10)).expect("shutdown");
}

#[test]
fn should_report_not_covered_given_late_wal_uploads_when_wait_times_out() {
    // Arrange: open eagerly (startup publishes the WAL catalog), then hang
    // every later WAL object write.
    let cache = tempfile::tempdir().expect("cache");
    let store = SimulatedObjectStore::new(Schedule::Eager);
    let clock = SimulatedClock::new();
    let engine = Engine::open(provider_options(cache.path(), &store, &clock)).expect("open");
    let cf_id = engine.get_column_family("default").expect("default").id();
    store.set_schedule(late_wal_uploads());

    // Act
    let sequence = commit_one(&engine, cf_id, b"k", b"v");
    let visible = read_one(&engine, cf_id, b"k");
    let covered = engine
        .wait_cloud_durable(sequence, Duration::from_millis(500))
        .expect("wait");
    let frontier = engine.cloud_durable_sequence().expect("frontier");

    // Assert: visible at once, not cloud durable while the upload hangs, and
    // the wait says so instead of pretending.
    assert_eq!(visible, Some(bytes::Bytes::from_static(b"v")));
    assert!(!covered, "a hung upload must not report cloud durability");
    assert!(
        frontier < sequence,
        "frontier {frontier} must stay below the unacknowledged sequence {sequence}"
    );
    drop(engine);
}

#[test]
fn should_lose_unacknowledged_cloud_async_write_given_cache_loss_when_successor_opens() {
    // Arrange: the write is visible and locally durable, its upload is hung,
    // and `wait_cloud_durable` has said it is not covered.
    let cache = tempfile::tempdir().expect("cache");
    let fresh_cache = tempfile::tempdir().expect("fresh cache");
    let store = SimulatedObjectStore::new(Schedule::Eager);
    let clock = SimulatedClock::new();
    let engine = Engine::open(provider_options(cache.path(), &store, &clock)).expect("open");
    let cf_id = engine.get_column_family("default").expect("default").id();
    // A covered sentinel proves the successor really opens the predecessor's
    // published state, not an empty store.
    let sentinel = commit_one(&engine, cf_id, b"sentinel", b"published");
    assert!(engine
        .wait_cloud_durable(sentinel, Duration::from_secs(10))
        .expect("sentinel wait"));
    store.set_schedule(late_wal_uploads());
    let sequence = commit_one(&engine, cf_id, b"k", b"v");
    assert!(!engine
        .wait_cloud_durable(sequence, Duration::from_millis(500))
        .expect("wait"));

    // Act: the writer's cache is lost; a successor opens from what the cloud
    // actually holds, after the predecessor's lease could have lapsed.
    let published = SimulatedObjectStore::from_image(Schedule::Eager, &store.image());
    drop(engine);
    drop(cache);
    clock.advance(LEASE_TTL * 2);
    let mut successor = Engine::open(provider_options(fresh_cache.path(), &published, &clock))
        .expect("successor opens from the published cloud state");

    // Assert: the write that was never acknowledged is not promised to the
    // successor, exactly as the contract states for `cloud_async()`.
    let cf_id = successor
        .get_column_family("default")
        .expect("default")
        .id();
    assert_eq!(
        read_one(&successor, cf_id, b"sentinel"),
        Some(bytes::Bytes::from_static(b"published")),
        "the successor must see the predecessor's covered write"
    );
    assert_eq!(read_one(&successor, cf_id, b"k"), None);
    let successor_frontier = successor.cloud_durable_sequence().expect("frontier");
    assert!(
        (sentinel..sequence).contains(&successor_frontier),
        "successor frontier {successor_frontier} must cover the sentinel {sentinel} but not the unacknowledged sequence {sequence}"
    );
    successor
        .shutdown(Duration::from_secs(10))
        .expect("shutdown successor");
}

#[test]
fn should_keep_acknowledged_cloud_async_write_given_cache_loss_when_wait_returned_covered() {
    // Arrange
    let cache = tempfile::tempdir().expect("cache");
    let fresh_cache = tempfile::tempdir().expect("fresh cache");
    let store = SimulatedObjectStore::new(Schedule::Eager);
    let clock = SimulatedClock::new();
    let engine = Engine::open(provider_options(cache.path(), &store, &clock)).expect("open");
    let cf_id = engine.get_column_family("default").expect("default").id();
    let sequence = commit_one(&engine, cf_id, b"k", b"v");
    assert!(engine
        .wait_cloud_durable(sequence, Duration::from_secs(10))
        .expect("wait"));

    // Act
    let published = SimulatedObjectStore::from_image(Schedule::Eager, &store.image());
    drop(engine);
    drop(cache);
    clock.advance(LEASE_TTL * 2);
    let mut successor = Engine::open(provider_options(fresh_cache.path(), &published, &clock))
        .expect("successor opens from the published cloud state");

    // Assert: a covered sequence survives the cache.
    let cf_id = successor
        .get_column_family("default")
        .expect("default")
        .id();
    assert_eq!(
        read_one(&successor, cf_id, b"k"),
        Some(bytes::Bytes::from_static(b"v"))
    );
    successor
        .shutdown(Duration::from_secs(10))
        .expect("shutdown successor");
}

#[test]
fn should_reject_cloud_frontier_queries_given_local_storage_when_called() {
    // Arrange
    let dir = tempfile::tempdir().expect("dir");
    let engine = Engine::open(
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .expect("options"),
    )
    .expect("open");

    // Act
    let frontier = engine.cloud_durable_sequence();
    let wait = engine.wait_cloud_durable(1, Duration::from_millis(10));

    // Assert
    assert!(
        matches!(frontier, Err(MidgeError::InvalidArgument(_))),
        "{frontier:?}"
    );
    assert!(
        matches!(wait, Err(MidgeError::InvalidArgument(_))),
        "{wait:?}"
    );
}

#[test]
fn should_return_increasing_assigned_sequences_given_local_writes_when_committed() {
    // Arrange
    let dir = tempfile::tempdir().expect("dir");
    let engine = Engine::open(
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .expect("options"),
    )
    .expect("open");
    let cf_id = engine.create_column_family("seq").expect("cf").id();

    // Act
    let mut sequences = Vec::new();
    for i in 0_u32..5 {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(i.to_be_bytes().to_vec(), b"v".to_vec(), None)
            .expect("put");
        sequences.push(
            tx.commit_with_sequence(WriteOptions::sync())
                .expect("sync commit"),
        );
    }
    let read_only = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("begin read-only")
        .commit_with_sequence(WriteOptions::sync())
        .expect("read-only commit");

    // Assert: each write got a later sequence, none exceeds the committed
    // sequence, and a commit without writes reports a sequence at or below it.
    let committed = engine
        .metrics()
        .get_runtime_metrics()
        .expect("metrics")
        .current_sequence;
    for pair in sequences.windows(2) {
        assert!(pair[0] < pair[1], "sequences must increase: {sequences:?}");
    }
    assert!(sequences[4] <= committed);
    assert!(read_only <= committed);
}
