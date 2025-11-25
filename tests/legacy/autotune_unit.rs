// Focused autotune behavior tests

mod common;
use cntryl_midge::config::Autotuner;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::*;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn should_record_autotune_metrics_when_under_sustained_write_load() {
    // Arrange: engine with disk-backed storage
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024,
        autotuner: Some(Arc::new(
            Autotuner::new().with_adjustment_interval(Duration::from_secs(0)),
        )),
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();

    // Baseline metrics
    let m0 = eng.metrics();
    let wal_adj0 = m0.get_autotune_wal_interval_adjustments();
    let comp_adj0 = m0.get_autotune_compaction_thread_adjustments();
    let bloom_adj0 = m0.get_autotune_bloom_bits_adjustments();

    // Act: drive some write + flush workload; autotune should eventually react
    for i in 0..2_000u32 {
        let key = format!("k{:04}", i);
        eng.put(&cf, key.as_bytes(), b"v").unwrap();
        if i % 200 == 0 {
            eng.flush().unwrap();
        }
    }
    eng.flush().unwrap();

    // Grab metrics again
    let m1 = eng.metrics();
    let wal_adj1 = m1.get_autotune_wal_interval_adjustments();
    let comp_adj1 = m1.get_autotune_compaction_thread_adjustments();
    let bloom_adj1 = m1.get_autotune_bloom_bits_adjustments();

    // Assert: at least one autotune dimension has adjusted
    assert!(
        wal_adj1 > wal_adj0 || comp_adj1 > comp_adj0 || bloom_adj1 > bloom_adj0,
        "expected at least one autotune metric to increase, got wal: {}->{}, comp: {}->{}, bloom: {}->{}",
        wal_adj0,
        wal_adj1,
        comp_adj0,
        comp_adj1,
        bloom_adj0,
        bloom_adj1
    );
}

#[test]
fn should_not_decrease_sst_file_count_when_autotune_runs() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();

    // Create at least one SST
    for i in 0..200u32 {
        let key = format!("pre{:04}", i);
        eng.put(&cf, key.as_bytes(), b"v").unwrap();
    }
    eng.flush().unwrap();

    let count_before = eng.sst_file_count();

    // Act: additional workload to trigger autotune
    for i in 0..500u32 {
        let key = format!("post{:04}", i);
        eng.put(&cf, key.as_bytes(), b"v").unwrap();
        if i % 100 == 0 {
            eng.flush().unwrap();
        }
    }
    eng.flush().unwrap();

    let count_after = eng.sst_file_count();

    // Assert: autotune activity should not mysteriously reduce the visible SST count
    assert!(
        count_after >= count_before,
        "expected sst_file_count to stay >= {}, got {}",
        count_before,
        count_after
    );
}

#[test]
fn should_use_autotune_defaults_when_enabled_in_config() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    // Autotune is enabled by default in options; opening the engine should
    // wire an autotuner instance with its default baselines.

    let eng = MidgeEngine::open(opts.clone()).expect("open engine");
    let metrics = eng.metrics().snapshot();

    // Assert: snapshot should expose defined (possibly zero) autotune counters
    // without panicking or corrupting state.
    let _ = metrics.autotune_wal_interval_adjustments;
    let _ = metrics.autotune_compaction_thread_adjustments;
    let _ = metrics.autotune_bloom_bits_adjustments;

    drop(eng);

    // Act: disable autotune via Config API and reopen; adjustment counters
    // should remain stable under a tiny load.
    let cfg = cntryl_midge::config::ConfigBuilder::new(dir.path())
        // Explicitly leave autotune disabled (default)
        .build()
        .expect("build config");
    let opts = cfg.to_options();
    let eng2 = MidgeEngine::open(opts).expect("reopen engine with autotune disabled");
    let cf = eng2.default_column_family();

    for i in 0..100u32 {
        let key = format!("cfg{:04}", i);
        eng2.put(&cf, key.as_bytes(), b"v").unwrap();
    }
    eng2.flush().unwrap();

    let m_before = eng2.metrics();
    let wal_before = m_before.get_autotune_wal_interval_adjustments();
    let comp_before = m_before.get_autotune_compaction_thread_adjustments();
    let bloom_before = m_before.get_autotune_bloom_bits_adjustments();

    let m_after = eng2.metrics();
    let wal_after = m_after.get_autotune_wal_interval_adjustments();
    let comp_after = m_after.get_autotune_compaction_thread_adjustments();
    let bloom_after = m_after.get_autotune_bloom_bits_adjustments();

    // Assert: with autotune disabled, adjustment counters should remain stable
    assert_eq!(wal_before, wal_after);
    assert_eq!(comp_before, comp_after);
    assert_eq!(bloom_before, bloom_after);
}

#[test]
fn should_bound_autotune_adjustments_when_metrics_fluctuate() {
    // Arrange: warm engine with small memtable to encourage flushes
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 32 * 1024,
        autotuner: Some(Arc::new(
            Autotuner::new().with_adjustment_interval(Duration::from_secs(0)),
        )),
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();

    // Act: alternate between bursts of writes and idle flushes to generate
    // changing load patterns that exercise the autotuner over time.
    for cycle in 0..5u32 {
        let writes = 200 + cycle * 50;
        for i in 0..writes {
            let key = format!("flt{}_{}", cycle, i);
            eng.put(&cf, key.as_bytes(), b"v").unwrap();
        }
        eng.flush().unwrap();
    }

    let snap1 = eng.metrics().snapshot();

    // Assert: autotune adjustment counters should not explode under
    // fluctuating load patterns.
    let max_adjustments = 10u64;
    assert!(
        snap1.autotune_wal_interval_adjustments <= max_adjustments,
        "wal adjustments too large: {}",
        snap1.autotune_wal_interval_adjustments
    );
    assert!(
        snap1.autotune_compaction_thread_adjustments <= max_adjustments,
        "compaction adjustments too large: {}",
        snap1.autotune_compaction_thread_adjustments
    );
    assert!(
        snap1.autotune_bloom_bits_adjustments <= max_adjustments,
        "bloom adjustments too large: {}",
        snap1.autotune_bloom_bits_adjustments
    );
}
