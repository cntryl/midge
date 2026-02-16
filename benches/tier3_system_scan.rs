//! Tier 3 — Scan seek behavior (iterator construction + first advance)
//!
//! Measures: cost of seeking and advancing once across different storage layouts.
//! Value size is IRRELEVANT to the measured primitive (seek behavior independent of payload).
//! This test only answers: "How fast can we seek and start iteration?"
//!
//! **Important:** This benchmark measures iterator setup cost plus first element only.
//! It does NOT measure full scan throughput. For that, see tier4_ycsb_workload_e.rs
//! or tier2_subsystem benchmarks that consume entire iterator results.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{Key, MidgeEngine, testkit::MidgeOptions, Query};
const VALUE_SIZE: usize = 64; // Irrelevant to measured primitive; used only in setup
const TARGET_BATCH: usize = 10_000;

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
            let k = cntryl_midge::testkit::stress::key16_prefix_u64_be(prefix, i as u64);
            let v = vec![(i % 251) as u8; VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        engine.commit(tx, write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap(); // Ensure durability before measurement
}

fn run_scan_query_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    setup: impl FnOnce(&MidgeEngine),
    query: Query,
) {
    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup done outside measurement
    setup(&engine);

    let cf_id = cf.id();
    let tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");

    // Measure ONLY iterator construction and first advance
    ctx.measure(|| {
        let mut it = tx.scan(&query).expect("scan failed");
        let _ = it.next();
    });

    drop(engine);
}

#[stress_test]
fn tier3_scan_seek_memtable_only_mem(ctx: &mut StressContext) {
    ctx.set_elements(5_000);
    let opts = cntryl_midge::testkit::opts_for_mode("memory");

    let prefix = Key::copy_from_slice(&[0xAA]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            write_prefixed_keys(e, 5_000, 0xAA);
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_seek_l0_only_local(ctx: &mut StressContext) {
    ctx.set_elements(5_000);
    let opts = cntryl_midge::testkit::opts_for_mode("local");

    let prefix = Key::copy_from_slice(&[0xAB]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            write_prefixed_keys(e, 5_000, 0xAB);
            e.flush_cf(&cf).unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_seek_l0_only_cloud(ctx: &mut StressContext) {
    ctx.set_elements(5_000);
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");

    let prefix = Key::copy_from_slice(&[0xAC]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            write_prefixed_keys(e, 5_000, 0xAC);
            e.flush_cf(&cf).unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_seek_multi_level_local(ctx: &mut StressContext) {
    ctx.set_elements(5_000);
    let opts = cntryl_midge::testkit::opts_for_mode("local");

    let prefix = Key::copy_from_slice(&[0xAD]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            // Build L1 via compact_all, then add a fresh L0.
            write_prefixed_keys(e, 3_000, 0xAD);
            e.flush_cf(&cf).unwrap();
            write_prefixed_keys(e, 3_000, 0xAD);
            e.flush_cf(&cf).unwrap();
            e.compact_all().unwrap();
            write_prefixed_keys(e, 1_000, 0xAD);
            e.flush_cf(&cf).unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_seek_multi_level_cloud(ctx: &mut StressContext) {
    ctx.set_elements(5_000);
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");

    let prefix = Key::copy_from_slice(&[0xAE]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            write_prefixed_keys(e, 3_000, 0xAE);
            e.flush_cf(&cf).unwrap();
            write_prefixed_keys(e, 3_000, 0xAE);
            e.flush_cf(&cf).unwrap();
            e.compact_all().unwrap();
            write_prefixed_keys(e, 1_000, 0xAE);
            e.flush_cf(&cf).unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_seek_after_compaction_local(ctx: &mut StressContext) {
    ctx.set_elements(5_000);
    let opts = cntryl_midge::testkit::opts_for_mode("local");

    let prefix = Key::copy_from_slice(&[0xAF]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            write_prefixed_keys(e, 5_000, 0xAF);
            e.flush_cf(&cf).unwrap();
            write_prefixed_keys(e, 5_000, 0xAF);
            e.flush_cf(&cf).unwrap();
            e.compact_all().unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_seek_after_compaction_cloud(ctx: &mut StressContext) {
    ctx.set_elements(5_000);
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");

    let prefix = Key::copy_from_slice(&[0xB0]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            let cf = e.create_column_family("cf1").unwrap();
            write_prefixed_keys(e, 5_000, 0xB0);
            e.flush_cf(&cf).unwrap();
            write_prefixed_keys(e, 5_000, 0xB0);
            e.flush_cf(&cf).unwrap();
            e.compact_all().unwrap();
        },
        query,
    );
}

stress_main!();
