//! Tier 3 — Scan behavior scenarios (stress harness)

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{Key, MidgeEngine, MidgeOptions, Query};
const VALUE_SIZE: usize = 64;

fn write_prefixed_keys(engine: &MidgeEngine, num_keys: usize, prefix: u8) {
    let cf = engine.default_column_family();
    let cf_id = cf.id();
    for i in 0..num_keys {
        let k = cntryl_midge::testkit::stress::key16_prefix_u64_be(prefix, i as u64);
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        let mut tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).expect("begin");
        tx.put(k.to_vec(), v, None).unwrap();
        engine.commit(tx, cntryl_midge::WriteOptions::buffered()).unwrap();
    }
}

fn run_scan_query_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    setup: impl FnOnce(&MidgeEngine),
    query: Query,
) {
    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.default_column_family();

    // Setup (not measured)
    setup(&engine);

    // Extract query bounds (precompute outside measurement)
    let start = query.effective_start().expect("query must have start");
    let end_vec = query.effective_end().expect("query must have end");

    // Measure exactly one scan
    let cf_id = cf.id();
    ctx.measure_ref(&engine, |e| {
        let tx = e.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).expect("begin");
        let results = tx.scan(start, &end_vec).expect("scan failed");
        results.len()
    });

    drop(engine);
}

#[stress_test]
fn tier3_scan_memtable_only_mem(ctx: &mut StressContext) {
    ctx.set_elements(1);
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
fn tier3_scan_l0_only_local(ctx: &mut StressContext) {
    ctx.set_elements(1);
    let opts = cntryl_midge::testkit::opts_for_mode("local");

    let prefix = Key::copy_from_slice(&[0xAB]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            write_prefixed_keys(e, 5_000, 0xAB);
            e.flush().unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_l0_only_cloud(ctx: &mut StressContext) {
    ctx.set_elements(1);
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");

    let prefix = Key::copy_from_slice(&[0xAC]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            write_prefixed_keys(e, 5_000, 0xAC);
            e.flush().unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_multi_level_local(ctx: &mut StressContext) {
    ctx.set_elements(1);
    let opts = cntryl_midge::testkit::opts_for_mode("local");

    let prefix = Key::copy_from_slice(&[0xAD]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            // Build L1 via compact_all, then add a fresh L0.
            write_prefixed_keys(e, 3_000, 0xAD);
            e.flush().unwrap();
            write_prefixed_keys(e, 3_000, 0xAD);
            e.flush().unwrap();
            e.compact_all().unwrap();
            write_prefixed_keys(e, 1_000, 0xAD);
            e.flush().unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_multi_level_cloud(ctx: &mut StressContext) {
    ctx.set_elements(1);
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");

    let prefix = Key::copy_from_slice(&[0xAE]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            write_prefixed_keys(e, 3_000, 0xAE);
            e.flush().unwrap();
            write_prefixed_keys(e, 3_000, 0xAE);
            e.flush().unwrap();
            e.compact_all().unwrap();
            write_prefixed_keys(e, 1_000, 0xAE);
            e.flush().unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_after_compaction_local(ctx: &mut StressContext) {
    ctx.set_elements(1);
    let opts = cntryl_midge::testkit::opts_for_mode("local");

    let prefix = Key::copy_from_slice(&[0xAF]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            write_prefixed_keys(e, 5_000, 0xAF);
            e.flush().unwrap();
            write_prefixed_keys(e, 5_000, 0xAF);
            e.flush().unwrap();
            e.compact_all().unwrap();
        },
        query,
    );
}

#[stress_test]
fn tier3_scan_after_compaction_cloud(ctx: &mut StressContext) {
    ctx.set_elements(1);
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");

    let prefix = Key::copy_from_slice(&[0xB0]);
    let query = Query::new().prefix(prefix);

    run_scan_query_case(
        ctx,
        opts,
        |e| {
            write_prefixed_keys(e, 5_000, 0xB0);
            e.flush().unwrap();
            write_prefixed_keys(e, 5_000, 0xB0);
            e.flush().unwrap();
            e.compact_all().unwrap();
        },
        query,
    );
}

stress_main!();
