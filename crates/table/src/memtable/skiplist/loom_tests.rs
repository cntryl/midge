//! Loom models of the lock-free skiplist protocol (#931).
//!
//! Each model runs every interleaving of its threads up to the preemption
//! bound, with the list's atomics replaced by loom's. Loom tracks causality
//! through those atomics only: the models catch lost updates, missed links,
//! wrong chain order and leaked nodes, while the ordering of the plain payload
//! fields behind each publication is miri's job. Run the models alone, since
//! the ordinary tests construct lists outside a model:
//!
//! ```text
//! RUSTFLAGS="--cfg midge_loom" cargo test --lib --release memtable::skiplist::loom_tests
//! ```
//!
//! `LOOM_MAX_PREEMPTIONS` and `LOOM_MAX_BRANCHES` override the bounds below
//! for a deeper run.

use super::{OpType, SkipList, AO, PROBE_LIVE_VERSIONS, PROBE_SEQ_BASE, PROBE_SPLICE_RETRIES};
use bytes::Bytes;
use loom::sync::Arc;
use loom::thread;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Context switches loom may force per execution. Three is the conventional
/// loom budget; every CAS retry in these protocols needs one competing write
/// between a thread's search and its CAS, so two writers with a few retries
/// fit inside it.
const PREEMPTION_BOUND: usize = 3;
/// Branch points per execution before loom gives up on a model; the default
/// of one thousand is below what a few searches through the towers cost.
const MAX_BRANCHES: usize = 20_000;
/// Tower heights are a function of the seed, so the keys below have the
/// heights their comments state in every run.
const SEED: u64 = 0x5eed_0931;

/// Models that read the probe counters hold this, since the counters are
/// process-wide and the test harness runs models in parallel.
static PROBE_COUNTERS: Mutex<()> = Mutex::new(());

/// Explore every schedule of `f` up to the preemption bound, and insist the
/// model had schedules to explore: a model with one execution checks nothing.
fn model(f: impl Fn() + Sync + Send + 'static) {
    let executions = std::sync::Arc::new(AtomicUsize::new(0));
    let counted = std::sync::Arc::clone(&executions);
    let mut builder = loom::model::Builder::new();
    if std::env::var_os("LOOM_MAX_PREEMPTIONS").is_none() {
        builder.preemption_bound = Some(PREEMPTION_BOUND);
    }
    if std::env::var_os("LOOM_MAX_BRANCHES").is_none() {
        builder.max_branches = MAX_BRANCHES;
    }
    builder.check(move || {
        counted.fetch_add(1, Ordering::Relaxed);
        f();
    });
    let explored = executions.load(Ordering::Relaxed);
    eprintln!("loom explored {explored} executions");
    assert!(explored > 1, "the model has a single schedule");
}

fn put(list: &SkipList, key: &[u8], value: &'static [u8], seq: u64) -> bool {
    list.upsert_exp(
        Bytes::copy_from_slice(key),
        Some(Bytes::from_static(value)),
        seq,
        None,
        OpType::Put,
    )
}

fn sequences(list: &SkipList, key: &[u8]) -> Vec<u64> {
    list.drain_with_meta_with_exp()
        .into_iter()
        .filter(|entry| entry.0 == key)
        .map(|entry| entry.2)
        .collect()
}

/// The first `count` keys of the form `k<n>` whose tower is at least
/// `min_level` high, in key order.
fn keys_of_height(list: &SkipList, min_level: usize, count: usize) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = (0..256_u32)
        .map(|index| format!("k{index:03}").into_bytes())
        .filter(|key| list.level_for(key) >= min_level)
        .take(count)
        .collect();
    keys.sort();
    assert_eq!(keys.len(), count, "seed {SEED:#x} yields too few tall keys");
    keys
}

/// The keys linked at `level`, walking the tower chain from the sentinel.
fn keys_at_level(list: &SkipList, level: usize) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    let mut current = list.head.link(level).load(AO::Acquire);
    // SAFETY: published nodes live until the list is dropped, and the model
    // holds the list.
    while let Some(node) = unsafe { current.as_ref() } {
        keys.push(node.key.to_vec());
        current = node.link(level).load(AO::Acquire);
    }
    keys
}

#[test]
fn should_reclaim_losing_node_given_concurrent_inserts_when_loom_explores_all_schedules() {
    // Arrange: the probe counters are process-wide.
    let _counters = PROBE_COUNTERS.lock().expect("probe counters");
    PROBE_SPLICE_RETRIES.store(0, Ordering::SeqCst);

    // Act
    model(|| {
        // Two writers race to create the same absent key.
        PROBE_LIVE_VERSIONS.store(0, Ordering::SeqCst);
        let list = Arc::new(SkipList::with_seed(SEED));
        let key = keys_of_height(&list, 2, 1).remove(0);
        let writers: Vec<_> = [1_u64, 2]
            .into_iter()
            .map(|seq| {
                let list = Arc::clone(&list);
                let key = key.clone();
                thread::spawn(move || assert!(put(&list, &key, b"v", PROBE_SEQ_BASE + seq)))
            })
            .collect();
        for writer in writers {
            writer.join().expect("writer");
        }

        // Assert: one node, both versions newest first, and nothing leaks from
        // the node that lost the splice.
        assert_eq!(list.get_all_keys().len(), 1);
        assert_eq!(
            sequences(&list, &key),
            vec![PROBE_SEQ_BASE + 2, PROBE_SEQ_BASE + 1]
        );
        drop(list);
        assert_eq!(PROBE_LIVE_VERSIONS.load(Ordering::SeqCst), 0);
    });

    // Assert: some schedule made a writer lose the splice, so the reclaim
    // path was exercised rather than vacuously clean.
    assert!(
        PROBE_SPLICE_RETRIES.load(Ordering::SeqCst) > 0,
        "no schedule reached the lost-splice path"
    );
}

#[test]
fn should_link_every_key_at_every_level_given_concurrent_inserts_of_distinct_keys_when_loom_explores_all_schedules(
) {
    model(|| {
        // Arrange: one tall key already present, so the racing writers'
        // predecessors are real nodes rather than the sentinel, then two more
        // adjacent tall keys race through the level-zero splice and the
        // best-effort upper links. (A third racing writer multiplies the
        // schedules into a fourteen-minute run.)
        let list = Arc::new(SkipList::with_seed(SEED));
        let keys = keys_of_height(&list, 2, 3);
        assert!(put(&list, &keys[0], b"v", 1));

        // Act
        let writers: Vec<_> = keys[1..]
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, key)| {
                let list = Arc::clone(&list);
                thread::spawn(move || {
                    assert!(put(&list, &key, b"v", index as u64 + 2));
                })
            })
            .collect();
        for writer in writers {
            writer.join().expect("writer");
        }

        // Assert: every level holds exactly the keys whose towers reach it,
        // in order, and every key is found by point search.
        let top = keys
            .iter()
            .map(|key| list.level_for(key))
            .max()
            .expect("keys");
        for level in 0..top {
            let expected: Vec<Vec<u8>> = keys
                .iter()
                .filter(|key| list.level_for(key) > level)
                .cloned()
                .collect();
            assert_eq!(keys_at_level(&list, level), expected, "level {level}");
        }
        for key in &keys {
            assert_eq!(list.get(key, u64::MAX).as_deref(), Some(b"v".as_slice()));
        }
    });
}

#[test]
fn should_read_nothing_or_the_published_version_given_concurrent_insert_when_loom_explores_all_schedules(
) {
    model(|| {
        // Arrange
        let list = Arc::new(SkipList::with_seed(SEED));
        let key = keys_of_height(&list, 2, 1).remove(0);

        // Act: a reader races the first insert of the key.
        let writer = {
            let list = Arc::clone(&list);
            let key = key.clone();
            thread::spawn(move || assert!(put(&list, &key, b"v1", 1)))
        };
        let reader = {
            let list = Arc::clone(&list);
            let key = key.clone();
            thread::spawn(move || list.get(&key, 1))
        };
        writer.join().expect("writer");
        let observed = reader.join().expect("reader");

        // Assert: the reader found the key absent or found the published
        // version, and the version is visible once the writer returned.
        assert!(
            observed.is_none() || observed.as_deref() == Some(b"v1".as_slice()),
            "reader observed {observed:?}"
        );
        assert_eq!(list.get(&key, 1).as_deref(), Some(b"v1".as_slice()));
    });
}

#[test]
fn should_keep_versions_descending_given_out_of_order_appends_when_loom_explores_all_schedules() {
    // Arrange: the probe counters are process-wide.
    let _counters = PROBE_COUNTERS.lock().expect("probe counters");

    // Act
    model(|| {
        // The key exists at sequence 1; sequence 3 arrives before sequence 2,
        // so the later arrival splices mid-chain while a snapshot at
        // sequence 2 reads. Probe-range sequences count live versions, so a
        // version built for a lost append CAS must be reclaimed.
        PROBE_LIVE_VERSIONS.store(0, Ordering::SeqCst);
        let list = Arc::new(SkipList::with_seed(SEED));
        let key = keys_of_height(&list, 1, 1).remove(0);
        assert!(put(&list, &key, b"v1", PROBE_SEQ_BASE + 1));
        let newest = {
            let list = Arc::clone(&list);
            let key = key.clone();
            thread::spawn(move || assert!(put(&list, &key, b"v3", PROBE_SEQ_BASE + 3)))
        };
        let middle = {
            let list = Arc::clone(&list);
            let key = key.clone();
            thread::spawn(move || assert!(put(&list, &key, b"v2", PROBE_SEQ_BASE + 2)))
        };
        let reader = {
            let list = Arc::clone(&list);
            let key = key.clone();
            thread::spawn(move || list.get(&key, PROBE_SEQ_BASE + 2))
        };
        newest.join().expect("newest writer");
        middle.join().expect("middle writer");
        let observed = reader.join().expect("reader");

        // Assert: the snapshot never sees sequence 3; the chain ends up ordered
        // newest first with every version present; nothing leaks.
        assert!(
            matches!(observed.as_deref(), Some(b"v1" | b"v2")),
            "snapshot 2 observed {observed:?}"
        );
        assert_eq!(
            sequences(&list, &key),
            vec![PROBE_SEQ_BASE + 3, PROBE_SEQ_BASE + 2, PROBE_SEQ_BASE + 1]
        );
        assert_eq!(
            list.get(&key, PROBE_SEQ_BASE + 2).as_deref(),
            Some(b"v2".as_slice())
        );
        assert_eq!(list.get(&key, u64::MAX).as_deref(), Some(b"v3".as_slice()));
        drop(list);
        assert_eq!(PROBE_LIVE_VERSIONS.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn should_accept_exactly_one_writer_given_concurrent_appends_of_the_same_sequence_when_loom_explores_all_schedules(
) {
    model(|| {
        // Arrange: the key exists at sequence 1 and two writers both claim
        // sequence 2.
        let list = Arc::new(SkipList::with_seed(SEED));
        let key = keys_of_height(&list, 1, 1).remove(0);
        assert!(put(&list, &key, b"v1", 1));

        // Act
        let writers: Vec<_> = [b"a".as_slice(), b"b".as_slice()]
            .into_iter()
            .map(|value| {
                let list = Arc::clone(&list);
                let key = key.clone();
                thread::spawn(move || put(&list, &key, value, 2))
            })
            .collect();
        let accepted = writers
            .into_iter()
            .map(|writer| writer.join().expect("writer"))
            .filter(|accepted| *accepted)
            .count();

        // Assert: one append wins, the other is refused, and the chain holds
        // the sequence once.
        assert_eq!(accepted, 1, "writers accepted");
        assert_eq!(sequences(&list, &key), vec![2, 1]);
        let value = list.get(&key, 2);
        assert!(
            matches!(value.as_deref(), Some(b"a" | b"b")),
            "sequence 2 holds {value:?}"
        );
    });
}
