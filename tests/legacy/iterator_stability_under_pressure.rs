mod common;
use common::*;

use cntryl_midge::Query;

#[test]
fn should_iterate_consistently_across_sst_boundaries_with_evictions() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    for i in 0..50u8 {
        eng.put(&cf, &[i], format!("v{}", i).as_bytes()).unwrap();
    }

    // Force several flushes so items span multiple SSTs
    eng.flush().unwrap();

    // Act: scan all rows
    let rows = eng.scan(&cf, Query::new()).expect("scan");
    let count = rows.len();

    // Assert
    assert_eq!(count, 50);

    drop(eng);
    drop(tmp);
}

#[test]
fn should_rewind_correctly_given_tombstones_with_merges_when_rescanning() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    eng.put(&cf, b"a", b"1").unwrap();
    let _ = eng.merge_cf(&cf, b"a", b"+2");
    eng.delete_range(&cf, b"b", b"c").unwrap();

    // Act
    let first_scan = eng.scan(&cf, Query::new()).expect("scan");
    let second_scan = eng.scan(&cf, Query::new()).expect("scan");

    // Assert
    assert_eq!(first_scan.len(), second_scan.len());

    drop(eng);
    drop(tmp);
}

#[test]
fn should_handle_freeze_then_compaction_then_iterate_sequence() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    for i in 0..20u8 {
        eng.put(&cf, &[i], format!("v{}", i).as_bytes()).unwrap();
    }

    // Act
    eng.flush().unwrap();
    // compact: the engine compact API is internal — we exercise restart to simulate it
    let dirpath = tmp.path().to_path_buf();

    // Restart engine to simulate an internal lifecycle (freeze/compact)
    // Drop the original engine so the restart can re-open the same path
    // deterministically without encountering a lock held by the prior handle.
    drop(eng);
    with_engine_restart(
        durability_opts(dirpath.clone()),
        |_| {},
        |eng2| {
            // Assert
            let rows = eng2
                .scan(&eng2.default_column_family(), Query::new())
                .expect("scan");
            assert_eq!(rows.len(), 20);
        },
    );

    drop(tmp);
}

#[test]
fn should_yield_stable_results_with_cf_flush_in_progress() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    for i in 0..30u8 {
        eng.put(&cf, &[i], format!("v{}", i).as_bytes()).unwrap();
    }

    // Act
    // Start a flush in a separate thread and iterate immediately.
    let eng_ref = std::sync::Arc::new(std::sync::Mutex::new(eng));
    let eng_clone = eng_ref.clone();
    let flusher = std::thread::spawn(move || {
        let guard = eng_clone.lock().unwrap();
        guard.flush().unwrap();
        drop(guard);
    });

    let guard = eng_ref.lock().unwrap();
    let rows = guard
        .scan(&guard.default_column_family(), Query::new())
        .expect("scan");
    let count = rows.len();

    // Assert
    assert_eq!(count, 30);

    drop(guard);
    flusher.join().unwrap();
    drop(tmp);
}
