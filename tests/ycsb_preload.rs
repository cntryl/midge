//! Real public-engine setup controls; best-effort preload is not a strict ACK claim.

#![cfg(feature = "internal-testing")]

#[path = "../benches/bench_support/config.rs"]
mod config;
#[path = "../benches/bench_support/ycsb.rs"]
mod ycsb;

use cntryl_midge::{Engine, MidgeError, MidgeResult, Query, TransactionMode, WriteOptions};
use config::{MidgeOptions, StorageMode};
use std::time::Duration;

const LOCAL_BYTES: u64 = 16 * 1024 * 1024;
const INITIAL_KEYS: usize = 50_000;
const WORKERS: usize = 4;
const VALUE_BYTES: usize = 128;

fn hybrid_options(path: &std::path::Path) -> cntryl_midge::OpenOptions {
    MidgeOptions {
        storage_mode: StorageMode::CloudBacked {
            local_cache_path: path.to_path_buf(),
        },
        wal_sync: true,
        memtable_size: ycsb::TIER4_MEMTABLE_SIZE_BYTES,
        enable_compaction: true,
        ..MidgeOptions::default()
    }
    .with_simulated_cloud_local_storage_budget(LOCAL_BYTES)
    .to_open_options()
}

fn dataset_matches(engine: &Engine, count: usize) -> MidgeResult<bool> {
    dataset_matches_with_value_size(engine, count, VALUE_BYTES)
}

fn dataset_matches_with_value_size(
    engine: &Engine,
    count: usize,
    value_size: usize,
) -> MidgeResult<bool> {
    let family = engine.get_column_family("cf1").expect("dataset family");
    let tx = engine.begin_tx(family.id(), TransactionMode::ReadOnly)?;
    let rows = tx.scan(&Query::new())?.collect::<Result<Vec<_>, _>>()?;
    Ok(rows.len() == count
        && rows.iter().enumerate().all(|(index, (key, value))| {
            let id = u64::try_from(index).expect("dataset index fits u64");
            key.as_ref() == ycsb::make_key(id)
                && value.as_ref() == vec![u8::try_from(id % 251).unwrap(); value_size]
        }))
}

#[test]
fn should_match_engine_admission_when_batch_reaches_exact_staging_boundary() {
    // Arrange: the shared bound predicts 6,528 points exactly fill the staging window.
    use cntryl_midge::__internal::memtable::bench::point_flush_staging_bytes;
    let directory = tempfile::tempdir().expect("dataset directory");
    let options = hybrid_options(directory.path());
    let mut engine = Engine::open(options.clone()).expect("open dataset engine");
    let family = engine.create_column_family("cf1").expect("dataset family");
    let accepted_keys = 6_528;
    let commit = |count: usize| -> MidgeResult<()> {
        let mut tx = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
        for index in 0..count {
            let id = u64::try_from(index).unwrap();
            tx.put(
                ycsb::make_key(id).to_vec(),
                vec![u8::try_from(id % 251).unwrap(); VALUE_BYTES],
                None,
            )?;
        }
        tx.commit(WriteOptions::best_effort())
    };

    // Act: admit the exact boundary, flush it, then reject one larger atomic batch.
    let accepted = commit(accepted_keys);
    let flushed = accepted.as_ref().ok().map(|()| engine.flush_cf(&family));
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("before reject");
    let rejected = commit(accepted_keys + 1);
    let after = engine
        .metrics()
        .get_runtime_metrics()
        .expect("after reject");
    let rows = dataset_matches(&engine, accepted_keys);
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
    drop(engine);
    let mut reopened = Engine::open(options).expect("reopen dataset engine");
    let recovered = dataset_matches(&reopened, accepted_keys);
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("shutdown");

    // Assert: real admission agrees with the bound, including no rejected WAL/sequence movement.
    assert_eq!(
        point_flush_staging_bytes(accepted_keys, 16, VALUE_BYTES),
        LOCAL_BYTES / 2
    );
    assert!(point_flush_staging_bytes(accepted_keys + 1, 16, VALUE_BYTES) > LOCAL_BYTES / 2);
    assert!(accepted.is_ok(), "{accepted:?}");
    assert!(matches!(flushed, Some(Ok(()))), "{flushed:?}");
    assert!(
        matches!(rejected, Err(MidgeError::NoSpace(_))),
        "{rejected:?}"
    );
    assert_eq!(before.current_sequence, after.current_sequence);
    assert_eq!(before.wal_append_count, after.wal_append_count);
    assert!(matches!(rows, Ok(true)), "{rows:?}");
    assert!(matches!(recovered, Ok(true)), "{recovered:?}");
}

#[test]
fn should_return_real_no_space_when_one_preload_point_cannot_fit() {
    // Arrange: even an indivisible point exceeds the real cloud staging window.
    let directory = tempfile::tempdir().expect("dataset directory");
    let mut engine = Engine::open(hybrid_options(directory.path())).expect("open engine");
    let family = engine.create_column_family("cf1").expect("dataset family");
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("before reject");

    // Act: batch splitting cannot admit this point, and the loader must preserve its error.
    let result = ycsb::load_initial_dataset_with_workers(&engine, &family, 1, 1, 2 * 1024 * 1024);
    let after = engine
        .metrics()
        .get_runtime_metrics()
        .expect("after reject");
    let empty = dataset_matches(&engine, 0);
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");

    // Assert
    assert!(matches!(result, Err(MidgeError::NoSpace(_))), "{result:?}");
    assert_eq!(before.current_sequence, after.current_sequence);
    assert_eq!(before.wal_append_count, after.wal_append_count);
    assert!(matches!(empty, Ok(true)), "{empty:?}");
}

#[test]
fn should_preserve_dataset_when_worker_value_size_and_storage_profiles_change() {
    // Arrange: single-worker hybrid, larger-value hybrid, and real local-disk setup.
    for (workers, value_size, local) in [
        (1, VALUE_BYTES, false),
        (WORKERS, 512, false),
        (WORKERS, VALUE_BYTES, true),
    ] {
        let directory = tempfile::tempdir().expect("dataset directory");
        let options = if local {
            MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: directory.path().to_path_buf(),
                },
                wal_sync: true,
                memtable_size: ycsb::TIER4_MEMTABLE_SIZE_BYTES,
                ..MidgeOptions::default()
            }
            .to_open_options()
        } else {
            hybrid_options(directory.path())
        };
        let mut engine = Engine::open(options.clone()).expect("open engine");
        let family = engine.create_column_family("cf1").expect("dataset family");

        // Act: use the same real loader, then verify exact rows before and after owned reopen.
        let result = ycsb::load_initial_dataset_with_workers(
            &engine,
            &family,
            INITIAL_KEYS,
            workers,
            value_size,
        );
        let before = dataset_matches_with_value_size(&engine, INITIAL_KEYS, value_size);
        engine.shutdown(Duration::from_secs(30)).expect("shutdown");
        drop(engine);
        let mut reopened = Engine::open(options).expect("reopen engine");
        let after = dataset_matches_with_value_size(&reopened, INITIAL_KEYS, value_size);
        reopened
            .shutdown(Duration::from_secs(30))
            .expect("shutdown");

        // Assert
        assert!(
            result.is_ok(),
            "workers={workers} value_size={value_size} local={local}: {result:?}"
        );
        assert!(matches!(before, Ok(true)), "{before:?}");
        assert!(matches!(after, Ok(true)), "{after:?}");
    }
}

#[test]
fn should_rebuild_real_transaction_when_scripted_stall_clears() {
    // Arrange: the rejection and waits are constructed; committed rows use a real local engine.
    let directory = tempfile::tempdir().expect("dataset directory");
    let options = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: directory.path().to_path_buf(),
        },
        wal_sync: true,
        ..MidgeOptions::default()
    }
    .to_open_options();
    let mut engine = Engine::open(options.clone()).expect("open engine");
    let family = engine.create_column_family("cf1").expect("dataset family");
    let operations = std::cell::Cell::new(0);
    let waits = std::cell::Cell::new(0);
    let keys = 32;

    // Act: false and transient stalled waits do not credit a successful commit.
    let result = ycsb::retry_preload_write_stall(
        || {
            operations.set(operations.get() + 1);
            let mut tx = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
            for id in 0..u64::try_from(keys).unwrap() {
                tx.put(
                    ycsb::make_key(id).to_vec(),
                    vec![u8::try_from(id % 251).unwrap(); VALUE_BYTES],
                    None,
                )?;
            }
            if operations.get() == 1 {
                Err(MidgeError::WriteStall("constructed".into()))
            } else {
                tx.commit(WriteOptions::best_effort())
            }
        },
        |_| {
            waits.set(waits.get() + 1);
            match waits.get() {
                1 => Ok(false),
                2 => Err(MidgeError::WriteStall("constructed".into())),
                _ => Ok(true),
            }
        },
        Duration::from_secs(1),
    );
    let flushed = engine.flush_cf(&family);
    let before = dataset_matches(&engine, keys);
    engine.shutdown(Duration::from_secs(30)).expect("shutdown");
    drop(engine);
    let mut reopened = Engine::open(options).expect("reopen engine");
    let after = dataset_matches(&reopened, keys);
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("shutdown");

    // Assert
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(operations.get(), 2);
    assert_eq!(waits.get(), 3);
    assert!(flushed.is_ok(), "{flushed:?}");
    assert!(matches!(before, Ok(true)), "{before:?}");
    assert!(matches!(after, Ok(true)), "{after:?}");
}

#[test]
fn should_preserve_terminal_error_when_scripted_preload_operation_is_rejected() {
    // Arrange: these constructed errors must never trigger transaction rebuilding.
    for error in [
        MidgeError::NoSpace("limit".into()),
        MidgeError::Fenced("epoch".into()),
        MidgeError::Corruption("crc".into()),
        MidgeError::Busy("owner".into()),
        MidgeError::Timeout("request".into()),
    ] {
        let expected = error.to_string();
        let mut error = Some(error);
        let mut operations = 0;
        let mut waits = 0;

        // Act
        let result = ycsb::retry_preload_write_stall(
            || {
                operations += 1;
                Err(error.take().expect("only one attempt"))
            },
            |_| {
                waits += 1;
                Ok(true)
            },
            Duration::from_secs(1),
        );

        // Assert
        assert_eq!(result.err().map(|error| error.to_string()), Some(expected));
        assert_eq!(operations, 1);
        assert_eq!(waits, 0);
    }
}

#[test]
fn should_exhaust_original_retry_budget_when_scripted_stall_cannot_be_waited() {
    // Arrange: a zero retry budget still allows the original attempted operation.
    let mut operations = 0;
    let mut waits = 0;

    // Act
    let result = ycsb::retry_preload_write_stall(
        || {
            operations += 1;
            Err(MidgeError::WriteStall("constructed".into()))
        },
        |_| {
            waits += 1;
            Ok(true)
        },
        Duration::ZERO,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
    assert_eq!(operations, 1);
    assert_eq!(waits, 0);
}

#[test]
fn should_refuse_retry_when_scripted_clear_response_arrives_after_original_budget() {
    // Arrange: a constructed slow waiter returns clear after the original budget expires.
    let timeout = Duration::from_secs(1);
    let mut operations = 0;

    // Act: a late clear result cannot authorize another commit attempt.
    let result = ycsb::retry_preload_write_stall(
        || {
            operations += 1;
            if operations == 1 {
                Err(MidgeError::WriteStall("constructed".into()))
            } else {
                Ok(())
            }
        },
        |_| {
            std::thread::sleep(timeout + Duration::from_millis(10));
            Ok(true)
        },
        timeout,
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Timeout(_))), "{result:?}");
    assert_eq!(operations, 1);
}

#[test]
fn should_load_original_dataset_when_four_workers_share_hybrid_staging_window() {
    // Arrange: actual Tier-4 defaults, with workers fixed independently of host CPU count.
    let directory = tempfile::tempdir().expect("dataset directory");
    let options = hybrid_options(directory.path());
    let mut engine = Engine::open(options.clone()).expect("open dataset engine");
    let family = engine.create_column_family("cf1").expect("dataset family");
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("runtime budget");
    assert_eq!(before.hybrid_max_local_bytes, LOCAL_BYTES);
    assert_eq!(before.memtable_size_limit, ycsb::TIER4_MEMTABLE_SIZE_BYTES);

    // Act: the actual shared loader spawns four workers, commits and flushes real rows.
    let result = ycsb::load_initial_dataset_with_workers(
        &engine,
        &family,
        INITIAL_KEYS,
        WORKERS,
        VALUE_BYTES,
    );
    let matches_before = result
        .as_ref()
        .ok()
        .map(|()| dataset_matches(&engine, INITIAL_KEYS));
    engine
        .shutdown(Duration::from_secs(30))
        .expect("shutdown original engine");
    drop(engine);
    let mut reopened = Engine::open(options).expect("reopen dataset engine");
    let matches_after = result
        .as_ref()
        .ok()
        .map(|()| dataset_matches(&reopened, INITIAL_KEYS));
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("shutdown recovered engine");

    // Assert: on the old loader only this typed result fails, after owned cleanup.
    assert!(
        result.is_ok(),
        "real four-worker preload returned {result:?}"
    );
    assert!(
        matches!(matches_before, Some(Ok(true))),
        "before: {matches_before:?}"
    );
    assert!(
        matches!(matches_after, Some(Ok(true))),
        "reopened: {matches_after:?}"
    );
}

#[test]
fn should_preserve_exact_dataset_when_four_worker_transactions_fit_local_window() {
    // Arrange
    let directory = tempfile::tempdir().expect("dataset directory");
    let options = hybrid_options(directory.path());
    let mut engine = Engine::open(options.clone()).expect("open dataset engine");
    let family = engine.create_column_family("cf1").expect("dataset family");
    let keys = 512;

    // Act: same real helper and four workers, with individually admissible transactions.
    let result =
        ycsb::load_initial_dataset_with_workers(&engine, &family, keys, WORKERS, VALUE_BYTES);
    let matches_before = result
        .as_ref()
        .ok()
        .map(|()| dataset_matches(&engine, keys));
    engine
        .shutdown(Duration::from_secs(30))
        .expect("shutdown original engine");
    drop(engine);
    let mut reopened = Engine::open(options).expect("reopen dataset engine");
    let matches_after = result
        .as_ref()
        .ok()
        .map(|()| dataset_matches(&reopened, keys));
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("shutdown recovered engine");

    // Assert
    assert!(result.is_ok(), "admissible preload returned {result:?}");
    assert!(
        matches!(matches_before, Some(Ok(true))),
        "before: {matches_before:?}"
    );
    assert!(
        matches!(matches_after, Some(Ok(true))),
        "reopened: {matches_after:?}"
    );
}

#[test]
fn should_reject_indivisible_transaction_when_staging_bound_exceeds_local_window() {
    // Arrange: one actual oversized transaction, independent of the loader policy.
    let directory = tempfile::tempdir().expect("dataset directory");
    let mut engine = Engine::open(hybrid_options(directory.path())).expect("open dataset engine");
    let family = engine.create_column_family("cf1").expect("dataset family");
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("before admission");
    let mut tx = engine
        .begin_tx(family.id(), TransactionMode::ReadWrite)
        .expect("begin transaction");
    for id in 0..u64::try_from(INITIAL_KEYS / WORKERS).unwrap() {
        tx.put(ycsb::make_key(id).to_vec(), vec![0; VALUE_BYTES], None)
            .expect("stage row");
    }

    // Act: strict commit must reject before WAL append or row visibility.
    let result = tx.commit(WriteOptions::cloud_strict());
    let after = engine
        .metrics()
        .get_runtime_metrics()
        .expect("after admission");
    let empty = dataset_matches(&engine, 0);
    engine
        .shutdown(Duration::from_secs(30))
        .expect("shutdown rejected transaction owner");

    // Assert: this genuine resource error remains terminal, without retry or parsing its text.
    assert!(matches!(result, Err(MidgeError::NoSpace(_))), "{result:?}");
    assert_eq!(after.current_sequence, before.current_sequence);
    assert_eq!(after.wal_append_count, before.wal_append_count);
    assert!(
        matches!(empty, Ok(true)),
        "unexpected visible rows: {empty:?}"
    );
}
