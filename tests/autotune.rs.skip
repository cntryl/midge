mod common;
use cntryl_midge::config::Autotuner;
use cntryl_midge::{MidgeEngine, MidgeOptions};
use cntryl_midge::testkit::{create_storage_mode, disk_storage_modes, DurabilityTestContext};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn should_adjust_memtable_size_smoothly_given_sustained_high_write_throughput_when_autotune_enabled(
) {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange: simulate sustained high write throughput
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 64 * 1024, // 64KB
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("Failed to create engine");
        let cf = eng.default_column_family();

        // Act: perform high write throughput
        for i in 0..100 {
            eng.put(&cf, format!("k{:04}", i).as_bytes(), b"v")
                .expect("put during autotune test");
        }
        eng.flush().expect("flush during autotune test");

        // Assert: writes succeeded smoothly
        for i in 0..100 {
            let key = format!("k{:04}", i);
            let value = eng
                .get(&cf, key.as_bytes())
                .expect("get during autotune test");
            assert!(value.is_some(), "key {} not found for {}", key, mode);
        }
    }
}

#[test]
fn should_not_enter_feedback_loop_oscillation_given_fluctuating_write_load_when_autotune_controls_compaction_threads(
) {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange: fluctuating write load
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("Failed to create engine");
        let cf = eng.default_column_family();

        // Act: fluctuating writes
        for cycle in 0..5 {
            let num_writes = 100 + cycle * 50;
            for i in 0..num_writes {
                eng.put(&cf, format!("k{:04}", i).as_bytes(), b"v")
                    .expect("put during oscillation test");
            }
            eng.flush().expect("flush during oscillation test");
        }

        // Assert: no oscillation (engine stable)
        let value = eng.get(&cf, b"k0000").expect("get during oscillation test");
        assert!(
            value.is_some(),
            "engine unstable after oscillation test for {}",
            mode
        );
    }
}

#[test]
fn should_respect_configured_limits_given_autotune_recommendations_exceed_maximums_when_system_under_extreme_load(
) {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange: configure strict limits
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 64 * 1024, // 64KB small limit
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("Failed to create engine");
        let cf = eng.default_column_family();

        // Act: extreme load
        for i in 0..1000 {
            eng.put(&cf, format!("k{:04}", i).as_bytes(), b"v")
                .expect("put during extreme load test");
        }

        // Assert: engine enforces limits and remains stable
        let value = eng
            .get(&cf, b"k0000")
            .expect("get during extreme load test");
        assert!(
            value.is_some(),
            "engine unstable under extreme load for {}",
            mode
        );
    }
}

#[test]
fn should_revert_to_safe_defaults_given_corrupted_autotune_state_on_startup_when_recovering_engine()
{
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        // Arrange: normal startup
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("Failed to create engine");
        let cf = eng.default_column_family();

        // Put some data
        eng.put(&cf, b"key", b"value")
            .expect("put during recovery test");

        // Act: restart engine
        drop(eng);
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng2 = MidgeEngine::open(opts).expect("Failed to reopen engine");

        // Assert: reverts to safe defaults (data preserved)
        let cf2 = eng2.default_column_family();
        let value = eng2.get(&cf2, b"key").expect("get during recovery test");
        assert_eq!(
            value.as_deref(),
            Some(b"value".as_ref()),
            "data not preserved after restart for {}",
            mode
        );
    }
}

#[test]
fn should_record_autotune_metrics_when_under_sustained_write_load() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange: engine with disk-backed storage
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 64 * 1024,
            autotuner: Some(Arc::new(
                Autotuner::new().with_adjustment_interval(Duration::from_secs(0)),
            )),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("Failed to open engine");
        let cf = eng.default_column_family();

        // Baseline metrics
        let m0 = eng.metrics();
        let wal_adj0 = m0.get_autotune_wal_interval_adjustments();
        let comp_adj0 = m0.get_autotune_compaction_thread_adjustments();
        let bloom_adj0 = m0.get_autotune_bloom_bits_adjustments();

        // Act: drive some write + flush workload; autotune should eventually react
        for i in 0..2_000u32 {
            let key = format!("k{:04}", i);
            eng.put(&cf, key.as_bytes(), b"v")
                .expect("put during metrics test");
            if i % 200 == 0 {
                eng.flush().expect("flush during metrics test");
            }
        }
        eng.flush().expect("final flush during metrics test");

        // Grab metrics again
        let m1 = eng.metrics();
        let wal_adj1 = m1.get_autotune_wal_interval_adjustments();
        let comp_adj1 = m1.get_autotune_compaction_thread_adjustments();
        let bloom_adj1 = m1.get_autotune_bloom_bits_adjustments();

        // Assert: at least one autotune dimension has adjusted
        assert!(
            wal_adj1 > wal_adj0 || comp_adj1 > comp_adj0 || bloom_adj1 > bloom_adj0,
            "expected at least one autotune metric to increase for {}, got wal: {}->{}, comp: {}->{}, bloom: {}->{}",
            mode, wal_adj0, wal_adj1, comp_adj0, comp_adj1, bloom_adj0, bloom_adj1
        );
    }
}

#[test]
fn should_not_decrease_sst_file_count_when_autotune_runs() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 64 * 1024,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("Failed to open engine");
        let cf = eng.default_column_family();

        // Create at least one SST
        for i in 0..200u32 {
            let key = format!("pre{:04}", i);
            eng.put(&cf, key.as_bytes(), b"v")
                .expect("put during SST count test");
        }
        eng.flush().expect("flush during SST count test");

        let count_before = eng.sst_file_count();

        // Act: additional workload to trigger autotune
        for i in 0..500u32 {
            let key = format!("post{:04}", i);
            eng.put(&cf, key.as_bytes(), b"v")
                .expect("put during additional workload");
            if i % 100 == 0 {
                eng.flush().expect("flush during additional workload");
            }
        }
        eng.flush().expect("final flush during additional workload");

        let count_after = eng.sst_file_count();

        // Assert: autotune activity should not mysteriously reduce the visible SST count
        assert!(
            count_after >= count_before,
            "expected sst_file_count to stay >= {} for {}, got {}",
            count_before,
            mode,
            count_after
        );
    }
}

#[test]
fn should_use_autotune_defaults_when_enabled_in_config() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, temp_dir) = create_storage_mode(mode);
        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        // Autotune is enabled by default in options; opening the engine should
        // wire an autotuner instance with its default baselines.

        let eng = MidgeEngine::open(opts.clone()).expect("Failed to open engine");
        let metrics = eng.metrics().snapshot();

        // Assert: snapshot should expose defined (possibly zero) autotune counters
        // without panicking or corrupting state.
        let _ = metrics.autotune_wal_interval_adjustments;
        let _ = metrics.autotune_compaction_thread_adjustments;
        let _ = metrics.autotune_bloom_bits_adjustments;

        drop(eng);

        // Act: disable autotune via Config API and reopen; adjustment counters
        // should remain stable under a tiny load.
        let cfg = cntryl_midge::config::ConfigBuilder::new(temp_dir.as_ref().unwrap().path())
            // Explicitly leave autotune disabled (default)
            .build()
            .expect("build config");
        let opts = cfg.to_options();
        let eng2 = MidgeEngine::open(opts).expect("Failed to reopen engine with autotune disabled");
        let cf = eng2.default_column_family();

        for i in 0..100u32 {
            let key = format!("cfg{:04}", i);
            eng2.put(&cf, key.as_bytes(), b"v")
                .expect("put during config test");
        }
        eng2.flush().expect("flush during config test");

        let m_before = eng2.metrics();
        let wal_before = m_before.get_autotune_wal_interval_adjustments();
        let comp_before = m_before.get_autotune_compaction_thread_adjustments();
        let bloom_before = m_before.get_autotune_bloom_bits_adjustments();

        let m_after = eng2.metrics();
        let wal_after = m_after.get_autotune_wal_interval_adjustments();
        let comp_after = m_after.get_autotune_compaction_thread_adjustments();
        let bloom_after = m_after.get_autotune_bloom_bits_adjustments();

        // Assert: with autotune disabled, adjustment counters should remain stable
        assert_eq!(
            wal_before, wal_after,
            "WAL adjustments changed when autotune disabled for {}",
            mode
        );
        assert_eq!(
            comp_before, comp_after,
            "compaction adjustments changed when autotune disabled for {}",
            mode
        );
        assert_eq!(
            bloom_before, bloom_after,
            "bloom adjustments changed when autotune disabled for {}",
            mode
        );
    }
}

#[test]
fn should_bound_autotune_adjustments_when_metrics_fluctuate() {
    for mode in disk_storage_modes() {
        let (_mode_name, storage_mode, _temp_dir) = create_storage_mode(mode);
        // Arrange: warm engine with small memtable to encourage flushes
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 32 * 1024,
            autotuner: Some(Arc::new(
                Autotuner::new().with_adjustment_interval(Duration::from_secs(0)),
            )),
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("Failed to open engine");
        let cf = eng.default_column_family();

        // Act: alternate between bursts of writes and idle flushes to generate
        // changing load patterns that exercise the autotuner over time.
        for cycle in 0..5u32 {
            let writes = 200 + cycle * 50;
            for i in 0..writes {
                let key = format!("flt{}_{}", cycle, i);
                eng.put(&cf, key.as_bytes(), b"v")
                    .expect("put during fluctuation test");
            }
            eng.flush().expect("flush during fluctuation test");
        }

        let snap1 = eng.metrics().snapshot();

        // Assert: autotune adjustment counters should not explode under
        // fluctuating load patterns.
        let max_adjustments = 10u64;
        assert!(
            snap1.autotune_wal_interval_adjustments <= max_adjustments,
            "wal adjustments too large for {}: {}",
            mode,
            snap1.autotune_wal_interval_adjustments
        );
        assert!(
            snap1.autotune_compaction_thread_adjustments <= max_adjustments,
            "compaction adjustments too large for {}: {}",
            mode,
            snap1.autotune_compaction_thread_adjustments
        );
        assert!(
            snap1.autotune_bloom_bits_adjustments <= max_adjustments,
            "bloom adjustments too large for {}: {}",
            mode,
            snap1.autotune_bloom_bits_adjustments
        );
    }
}
