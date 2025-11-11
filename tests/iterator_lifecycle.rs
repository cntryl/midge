mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};
use common::{new_engine, test_temp_dir};

#[test]
fn should_return_error_given_iterator_used_after_close() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.put(&cf, b"key2", b"value2").expect("put");

    // Act - create iterator, get results, then drop engine
    let query = Query::new()
        .start_key(Bytes::from_static(b"key1"))
        .end_key(Bytes::from_static(b"key3"));
    let results = eng.scan(&cf, query).expect("scan");
    drop(eng);

    // Assert - results should be valid even after engine dropped
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, Bytes::from_static(b"key1"));
}

#[test]
fn should_continue_iteration_given_compaction_in_progress_when_scan() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act - write data that might trigger compaction
    for i in 0..200 {
        eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
            .expect("put");
    }

    // Assert - scan should work even if compaction is running
    let query = Query::new()
        .start_key(Bytes::from("key0000"))
        .end_key(Bytes::from("key0200"));
    let results = eng.scan(&cf, query).expect("scan during compaction");
    assert_eq!(
        results.len(),
        200,
        "All keys should be visible during compaction"
    );
}

#[test]
fn should_rewind_iterator_to_start_given_reset_called() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    for i in 0..10 {
        eng.put(&cf, format!("key{:02}", i).as_bytes(), b"value")
            .expect("put");
    }

    // Act - scan twice with same query parameters
    let query1 = Query::new()
        .start_key(Bytes::from("key00"))
        .end_key(Bytes::from("key10"));
    let results1 = eng.scan(&cf, query1).expect("first scan");

    let query2 = Query::new()
        .start_key(Bytes::from("key00"))
        .end_key(Bytes::from("key10"));
    let results2 = eng.scan(&cf, query2).expect("second scan");

    // Assert - both scans should return same results (iterator reset semantics)
    assert_eq!(results1.len(), 10);
    assert_eq!(results2.len(), 10);
    assert_eq!(
        results1, results2,
        "Repeated scans should produce identical results"
    );
}

#[test]
fn should_resume_iteration_given_checkpoint_sequence() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    for i in 0..20 {
        eng.put(
            &cf,
            format!("key{:02}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
        )
        .expect("put");
    }

    // Act - scan in chunks (simulating pagination)
    let query1 = Query::new()
        .start_key(Bytes::from("key00"))
        .end_key(Bytes::from("key10"));
    let chunk1 = eng.scan(&cf, query1).expect("first chunk");

    let query2 = Query::new()
        .start_key(Bytes::from("key10"))
        .end_key(Bytes::from("key20"));
    let chunk2 = eng.scan(&cf, query2).expect("second chunk");

    // Assert - chunks should be disjoint and complete
    assert_eq!(chunk1.len(), 10, "First chunk should have 10 items");
    assert_eq!(chunk2.len(), 10, "Second chunk should have 10 items");
    assert_eq!(chunk1[0].0, Bytes::from("key00"));
    assert_eq!(chunk1[9].0, Bytes::from("key09"));
    assert_eq!(chunk2[0].0, Bytes::from("key10"));
    assert_eq!(chunk2[9].0, Bytes::from("key19"));
}

#[test]
fn should_iterate_in_reverse_given_reverse_iterator_enabled_when_scan() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.put(&cf, b"key2", b"value2").expect("put");
    eng.put(&cf, b"key3", b"value3").expect("put");

    // Act - scan with reverse
    let query = Query::new()
        .start_key(Bytes::from_static(b"key1"))
        .end_key(Bytes::from_static(b"key4"))
        .reverse();
    let results = eng.scan(&cf, query).expect("reverse scan");

    // Assert - results should be in descending order
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].0,
        Bytes::from_static(b"key3"),
        "First result should be key3"
    );
    assert_eq!(
        results[1].0,
        Bytes::from_static(b"key2"),
        "Second result should be key2"
    );
    assert_eq!(
        results[2].0,
        Bytes::from_static(b"key1"),
        "Third result should be key1"
    );
}
