//! Tier 3 — durable-layout prefix scans.
//!
//! Measures the first row returned by a prefix scan through real local and
//! simulated-cloud SST layouts. Layout creation and validation stay outside
//! the timing window except for the one query/first-row operation being timed.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::{Bytes, MidgeEngine, Query, TransactionMode, WriteOptions};
use cntryl_stress::{stress, stress_main, StressContext};

const KEYS_PER_FLUSH: usize = 256;
const VALUE_SIZE: usize = 64;

#[derive(Clone, Copy)]
enum Layout {
    L0Only,
    L0PlusL1,
    FullyCompacted,
}

impl Layout {
    const fn name(self) -> &'static str {
        match self {
            Self::L0Only => "l0_only",
            Self::L0PlusL1 => "l0_plus_l1",
            Self::FullyCompacted => "fully_compacted",
        }
    }

    const fn flushes(self) -> usize {
        match self {
            Self::L0Only => 1,
            Self::L0PlusL1 => 2,
            Self::FullyCompacted => 3,
        }
    }
}

fn write_flush(
    engine: &MidgeEngine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    batch: usize,
    mode: &str,
) {
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("begin layout write");
    for offset in 0..KEYS_PER_FLUSH {
        let ordinal = batch * KEYS_PER_FLUSH + offset;
        let key = stress_config::bench_stress::key16_prefix_u64_be(0x7a, ordinal as u64);
        tx.put(
            key.to_vec(),
            vec![u8::try_from(ordinal % 251).expect("value byte fits"); VALUE_SIZE],
            None,
        )
        .expect("stage layout value");
    }
    let write_options = if mode == "cloud" {
        WriteOptions::cloud_async()
    } else {
        WriteOptions::buffered()
    };
    tx.commit(write_options).expect("commit layout batch");
    engine.flush_cf(cf).expect("flush layout batch");
}

fn prepare_layout(
    engine: &MidgeEngine,
    layout: Layout,
    mode: &str,
) -> cntryl_midge::ColumnFamilyHandle {
    let cf = engine.create_column_family("scan").expect("create scan CF");
    write_flush(engine, &cf, 0, mode);
    if matches!(layout, Layout::L0PlusL1 | Layout::FullyCompacted) {
        engine.compact_all().expect("compact L1 base");
    }
    for batch in 1..layout.flushes() {
        write_flush(engine, &cf, batch, mode);
    }
    if matches!(layout, Layout::FullyCompacted) {
        engine.compact_all().expect("fully compact layout");
    }
    cf
}

fn run_scan_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    mode: &'static str,
    layout: Layout,
) {
    ctx.parameter("logical_batch_size", 1);
    ctx.parameter("logical_unit", "scan_seek_first_row");
    ctx.parameter("operation_surface", "prefix_scan_first_row");
    ctx.parameter("storage_layout", layout.name());
    ctx.parameter("storage_mode", mode);
    ctx.metadata("diagnostic_reason", "pending_three_clean_baselines");
    ctx.parameter("local_gate_rsd_limit_pct", 5);

    let engine =
        stress_config::bench_stress::open_engine_no_compaction(stress_config::opts_for_mode(mode));
    let cf = prepare_layout(&engine, layout, mode);
    let snapshot = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin scan snapshot");
    let expected_key = stress_config::bench_stress::key16_prefix_u64_be(0x7a, 0);
    let expected_value = vec![0; VALUE_SIZE];
    let read_path_before = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let mut validation_failures = 0_u64;

    let _ = ctx.measure_batch(scenario, 1, || {
        let query = Query::new().prefix(Bytes::from_static(&[0x7a]));
        match snapshot.scan(&query).ok().and_then(|mut rows| rows.next()) {
            Some(Ok((key, value)))
                if key.as_ref() == expected_key.as_slice()
                    && value.as_ref() == expected_value.as_slice() => {}
            _ => validation_failures += 1,
        }
    });

    let read_path_after = engine.read_path_diagnostics_snapshot_for_benchmarks();
    assert_eq!(
        validation_failures, 0,
        "each measured scan must return its expected first row"
    );
    assert!(
        read_path_after.candidate_sst_files_checked > read_path_before.candidate_sst_files_checked
            && read_path_after.candidate_blocks_checked > read_path_before.candidate_blocks_checked
            && read_path_after.data_blocks_read > read_path_before.data_blocks_read,
        "scan construction plus first row must exercise candidate SST and data-block work"
    );
    drop(snapshot);
    drop(engine);
}

macro_rules! scan_rows {
    ($(($name:ident, $scenario:literal, $mode:literal, $layout:expr)),+ $(,)?) => {
        $(
            #[stress(tier = 3, role = "diagnostic")]
            fn $name(ctx: &mut StressContext) {
                run_scan_case(ctx, $scenario, $mode, $layout);
            }
        )+
    };
}

scan_rows!(
    (
        tier3_scan_l0_only_local,
        "tier3_scan_l0_only_local",
        "local",
        Layout::L0Only
    ),
    (
        tier3_scan_l0_only_cloud,
        "tier3_scan_l0_only_cloud",
        "cloud",
        Layout::L0Only
    ),
    (
        tier3_scan_l0_plus_l1_local,
        "tier3_scan_l0_plus_l1_local",
        "local",
        Layout::L0PlusL1
    ),
    (
        tier3_scan_l0_plus_l1_cloud,
        "tier3_scan_l0_plus_l1_cloud",
        "cloud",
        Layout::L0PlusL1
    ),
    (
        tier3_scan_fully_compacted_local,
        "tier3_scan_fully_compacted_local",
        "local",
        Layout::FullyCompacted
    ),
    (
        tier3_scan_fully_compacted_cloud,
        "tier3_scan_fully_compacted_cloud",
        "cloud",
        Layout::FullyCompacted
    ),
);

stress_main!();
