//! Tier 3 — Scan seek behavior (iterator construction + first advance)
//!
//! Measures: cost of seeking and advancing once across different storage layouts.
//! Value size is IRRELEVANT to the measured primitive (seek behavior independent of payload).
//! This test only answers: "How fast can we seek and start iteration?"
//!
//! **Important:** This benchmark measures iterator setup cost plus first element only.
//! It does NOT measure full scan throughput. For that, see `tier4_ycsb_workload_e.rs`
//! or `tier2_subsystem` benchmarks that consume entire iterator results.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use cntryl_midge::{Key, MidgeEngine, Query};
use stress_config::MidgeOptions;
const VALUE_SIZE: usize = 64; // Irrelevant to measured primitive; used only in setup
const TARGET_BATCH: usize = 10_000;
const SCAN_SEEK_BATCH_SIZE: usize = 64;
const ROTATING_PREFIX_COUNT: usize = 4;

fn rotating_prefixes(first_prefix: u8) -> Vec<u8> {
    (0..ROTATING_PREFIX_COUNT)
        .map(|offset| first_prefix.wrapping_add(u8::try_from(offset).expect("prefix offset fits")))
        .collect()
}

fn write_prefixed_keys(engine: &MidgeEngine, num_keys: usize, prefix: u8) {
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();
    // Use best_effort for fast setup load (benchmarks measure seek cost, not WAL cost)
    let write_opts = cntryl_midge::WriteOptions::best_effort();
    for start in (0..num_keys).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(num_keys);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for i in start..end {
            let k = stress_config::bench_stress::key16_prefix_u64_be(prefix, i as u64);
            let v = vec![u8::try_from(i % 251).expect("value byte fits in u8"); VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap(); // Ensure durability before measurement
}

fn run_scan_query_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: MidgeOptions,
    setup: impl FnOnce(&MidgeEngine),
    first_prefix: u8,
) {
    ctx.parameter("logical_batch_size", SCAN_SEEK_BATCH_SIZE);
    ctx.parameter("logical_unit", "scan_seek");
    ctx.parameter("operation_surface", "scan_seek_first_row");
    ctx.parameter("begin_tx_included", "false");
    ctx.parameter("rotating_prefix_count", ROTATING_PREFIX_COUNT);

    let engine = stress_config::bench_stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup done outside measurement
    setup(&engine);

    let cf_id = cf.id();
    let tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");
    let prefixes: Vec<Key> = rotating_prefixes(first_prefix)
        .into_iter()
        .map(|prefix| Key::copy_from_slice(&[prefix]))
        .collect();
    let mut prefix_index = 0usize;

    let _ = ctx.measure_batch(scenario, SCAN_SEEK_BATCH_SIZE as u64, || {
        for _ in 0..SCAN_SEEK_BATCH_SIZE {
            let prefix = prefixes[prefix_index % prefixes.len()].clone();
            prefix_index = prefix_index.wrapping_add(1);
            let query = Query::new().prefix(prefix);
            let mut it = tx.scan(&query).expect("scan failed");
            let _ = it.next();
        }
    });

    drop(engine);
}

#[stress(tier = 3)]
fn tier3_scan_seek_memtable_only_mem(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("memory");

    run_scan_query_case(
        ctx,
        "tier3_scan_seek_memtable_only_mem",
        opts,
        |e| {
            for prefix in rotating_prefixes(0xAA) {
                write_prefixed_keys(e, 5_000 / ROTATING_PREFIX_COUNT, prefix);
            }
        },
        0xAA,
    );
}

#[stress(tier = 3)]
fn tier3_scan_seek_l0_only_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");

    run_scan_query_case(
        ctx,
        "tier3_scan_seek_l0_only_local",
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            for prefix in rotating_prefixes(0xAB) {
                write_prefixed_keys(e, 5_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
        },
        0xAB,
    );
}

#[stress(tier = 3)]
fn tier3_scan_seek_l0_only_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");

    run_scan_query_case(
        ctx,
        "tier3_scan_seek_l0_only_cloud",
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            for prefix in rotating_prefixes(0xAC) {
                write_prefixed_keys(e, 5_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
        },
        0xAC,
    );
}

#[stress(tier = 3)]
fn tier3_scan_seek_multi_level_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");

    run_scan_query_case(
        ctx,
        "tier3_scan_seek_multi_level_local",
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            // Build L1 via compact_all, then add a fresh L0.
            for prefix in rotating_prefixes(0xAD) {
                write_prefixed_keys(e, 3_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
            for prefix in rotating_prefixes(0xAD) {
                write_prefixed_keys(e, 3_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
            e.compact_all().unwrap();
            for prefix in rotating_prefixes(0xAD) {
                write_prefixed_keys(e, 1_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
        },
        0xAD,
    );
}

#[stress(tier = 3)]
fn tier3_scan_seek_multi_level_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");

    run_scan_query_case(
        ctx,
        "tier3_scan_seek_multi_level_cloud",
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            for prefix in rotating_prefixes(0xAE) {
                write_prefixed_keys(e, 3_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
            for prefix in rotating_prefixes(0xAE) {
                write_prefixed_keys(e, 3_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
            e.compact_all().unwrap();
            for prefix in rotating_prefixes(0xAE) {
                write_prefixed_keys(e, 1_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
        },
        0xAE,
    );
}

#[stress(tier = 3)]
fn tier3_scan_seek_after_compaction_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");

    run_scan_query_case(
        ctx,
        "tier3_scan_seek_after_compaction_local",
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            for prefix in rotating_prefixes(0xAF) {
                write_prefixed_keys(e, 5_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
            for prefix in rotating_prefixes(0xAF) {
                write_prefixed_keys(e, 5_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
            e.compact_all().unwrap();
        },
        0xAF,
    );
}

#[stress(tier = 3)]
fn tier3_scan_seek_after_compaction_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");

    run_scan_query_case(
        ctx,
        "tier3_scan_seek_after_compaction_cloud",
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            for prefix in rotating_prefixes(0xB0) {
                write_prefixed_keys(e, 5_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
            for prefix in rotating_prefixes(0xB0) {
                write_prefixed_keys(e, 5_000 / ROTATING_PREFIX_COUNT, prefix);
            }
            e.flush_cf(&cf).unwrap();
            e.compact_all().unwrap();
        },
        0xB0,
    );
}

stress_main!();
