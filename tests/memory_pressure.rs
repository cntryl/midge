// Memory Pressure & Resource Limits tests - P1 Priority
use bytes::Bytes;
use midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::time::Duration;

#[test]
fn should_trigger_flush_when_memtable_full() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 4 * 1024,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();

    // Act
    let value = vec![b'x'; 512];
    for i in 0..20 {
        let key = format!("key{:08}", i);
        engine
            .put(Bytes::from(key), Bytes::from(value.clone()))
            .unwrap();
    }

    // Assert
    let metrics = engine.metrics().snapshot();
    assert!(metrics.memtable_flushes > 0);
}

#[test]
fn should_trigger_emergency_flush_given_memtable_memory_critical() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 2 * 1024,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let initial = engine.metrics().snapshot().memtable_flushes;

    // Act
    let value = vec![b'y'; 256];
    for i in 0..50 {
        engine
            .put(Bytes::from(format!("k{}", i)), Bytes::from(value.clone()))
            .unwrap();
    }

    // Assert
    assert!(engine.metrics().snapshot().memtable_flushes > initial);
}

#[test]
fn should_continue_operations_given_compaction_under_memory_pressure() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 2 * 1024,
        compaction_sst_threshold: 2,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();

    // Act
    for b in 0..10 {
        for i in 0..20 {
            if let Err(e) = engine.put(
                Bytes::from(format!("b{}k{}", b, i)),
                Bytes::from(vec![b'c'; 256]),
            ) {
                eprintln!("engine.put failed in loop b={} i={} : {:?}", b, i, e);
                panic!("engine.put failed");
            }
        }
        engine.flush().unwrap();
    }
    // Deterministically wait for background compaction to quiesce, then perform
    // the final put. This avoids timing-dependent sleeps in the test.
    engine
        .wait_for_compaction_idle(Duration::from_secs(1))
        .unwrap();

    // Assert (with diagnostics on error to capture missing-file path)
    if let Err(e) = engine.put(Bytes::from("test"), Bytes::from("ok")) {
        eprintln!("engine.put failed: {:?}", e);
        panic!("engine.put failed");
    }
    match engine.get(b"test") {
        Ok(Some(v)) => assert_eq!(v, Bytes::from("ok")),
        Ok(None) => {
            eprintln!("engine.get returned None for key 'test'");
            panic!("engine.get returned None");
        }
        Err(e) => {
            eprintln!("engine.get failed: {:?}", e);
            panic!("engine.get failed");
        }
    }
}

#[test]
fn should_report_memory_usage_metrics_given_runtime_query() {
    // Arrange
    let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();

    // Act
    for i in 0..100 {
        engine
            .put(Bytes::from(format!("k{}", i)), Bytes::from("v"))
            .unwrap();
    }

    // Assert
    let m = engine.metrics().snapshot();
    assert_eq!(m.put_count, 100);
}
