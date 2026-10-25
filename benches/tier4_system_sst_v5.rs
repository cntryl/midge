//! Tier 4 — SST V5 decode work and physical footprint.
//!
//! Records production point-lookup decoder work and the flushed SST footprint
//! for small key/value pairs. Compare this row across the preregistered base
//! and candidate source revisions; it does not infer a format delta in-run.

use cntryl_midge::{
    __internal::sst::PointLookupReaderForBenchmarks, Engine, OpenOptions, TransactionMode,
    WriteOptions,
};
use cntryl_stress::{stress, stress_main, ObservationDirection, ObservationUnit, StressContext};
use std::time::Duration;
use tempfile::TempDir;

const UNIQUE_KEYS: u64 = 8_192;
const REPEATED_VERSIONS: usize = 128;
const VALUE_BYTES: usize = 32;

fn key(index: u64) -> [u8; 16] {
    let mut key = *b"sst-v5-bench0000";
    key[8..].copy_from_slice(&index.to_be_bytes());
    key
}

fn value(seed: u64) -> [u8; VALUE_BYTES] {
    let mut state = seed ^ 0x9180_2026_1006;
    let mut value = [0; VALUE_BYTES];
    for chunk in value.chunks_exact_mut(8) {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut word = state;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        chunk.copy_from_slice(&(word ^ (word >> 31)).to_le_bytes());
    }
    value
}

fn put(engine: &Engine, cf_id: u32, key: &[u8], value: &[u8]) {
    let mut tx = engine
        .begin_tx(cf_id, TransactionMode::ReadWrite)
        .expect("begin SST V5 fixture write");
    tx.put(key.to_vec(), value.to_vec(), None)
        .expect("stage SST V5 fixture value");
    tx.commit(WriteOptions::best_effort())
        .expect("commit SST V5 fixture value");
}

fn run(ctx: &mut StressContext) {
    let directory = TempDir::new().expect("create SST V5 database");
    let mut engine = Engine::open(
        OpenOptions::local(directory.path())
            .build()
            .expect("build SST V5 benchmark options"),
    )
    .expect("open SST V5 benchmark database");
    let cf = engine
        .create_column_family("sst-v5-qualification")
        .expect("create SST V5 benchmark column family");

    for index in 0..UNIQUE_KEYS {
        put(&engine, cf.id(), &key(index), &value(index));
    }
    let repeated_key = key(UNIQUE_KEYS);
    for version in 0..REPEATED_VERSIONS {
        put(
            &engine,
            cf.id(),
            &repeated_key,
            &value(UNIQUE_KEYS + u64::try_from(version).expect("version fits in u64")),
        );
    }
    engine.flush_cf(&cf).expect("flush SST V5 fixture");

    let layout = engine
        .metrics()
        .get_storage_layout()
        .expect("capture SST V5 fixture layout");
    let files: Vec<_> = layout
        .levels
        .iter()
        .flat_map(|level| level.files.iter())
        .filter(|file| file.cf_id == cf.id())
        .collect();
    assert!(!files.is_empty(), "fixture must publish an SST");
    let total_sst_bytes: u64 = files.iter().map(|file| file.size_bytes).sum();
    let target = key(UNIQUE_KEYS / 2);
    let target_file = files
        .iter()
        .find(|file| {
            file.smallest_key
                .as_deref()
                .is_some_and(|smallest| smallest <= target.as_slice())
                && file
                    .largest_key
                    .as_deref()
                    .is_some_and(|largest| largest >= target.as_slice())
        })
        .expect("find SST containing unique-key probe");
    let repeated_file = files
        .iter()
        .find(|file| {
            file.smallest_key
                .as_deref()
                .is_some_and(|smallest| smallest <= repeated_key.as_slice())
                && file
                    .largest_key
                    .as_deref()
                    .is_some_and(|largest| largest >= repeated_key.as_slice())
        })
        .expect("find SST containing repeated-version probe");
    let unique_reader = PointLookupReaderForBenchmarks::open(directory.path(), &target_file.name)
        .expect("open unique-key SST reader");
    let repeated_reader =
        PointLookupReaderForBenchmarks::open(directory.path(), &repeated_file.name)
            .expect("open repeated-version SST reader");
    let latest_sequence = engine
        .metrics()
        .get_runtime_metrics()
        .expect("read latest sequence after flush")
        .current_sequence;
    let old_snapshot = latest_sequence
        .checked_sub(u64::try_from(REPEATED_VERSIONS / 2).expect("version count fits in u64"))
        .expect("old snapshot remains nonnegative");
    let unique_decoded = ctx.measure("unique_key_point_lookup", || {
        unique_reader
            .entries_decoded(&target, u64::MAX)
            .expect("measure unique-key decoder work")
    });
    let repeated_decoded = ctx.measure("repeated_version_point_lookup", || {
        repeated_reader
            .entries_decoded(&repeated_key, u64::MAX)
            .expect("measure repeated-version decoder work")
    });
    let repeated_old_snapshot_decoded =
        ctx.measure("repeated_version_old_snapshot_point_lookup", || {
            repeated_reader
                .entries_decoded(&repeated_key, old_snapshot)
                .expect("measure old-snapshot decoder work")
        });
    assert!(
        unique_decoded <= 23,
        "unique lookup decoded {unique_decoded} entries"
    );
    assert!(
        repeated_decoded <= 150,
        "repeated-version lookup decoded {repeated_decoded} entries"
    );
    assert!(
        repeated_old_snapshot_decoded <= 150,
        "old-snapshot lookup decoded {repeated_old_snapshot_decoded} entries"
    );

    ctx.parameter("format", "SST_V5");
    ctx.parameter("unique_keys", UNIQUE_KEYS);
    ctx.parameter("repeated_versions", REPEATED_VERSIONS);
    ctx.parameter("key_bytes", 16);
    ctx.parameter("value_bytes", VALUE_BYTES);
    ctx.record_observation(
        "sst_files",
        files.len() as f64,
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "sst_footprint_bytes",
        total_sst_bytes as f64,
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "unique_key_entries_decoded",
        unique_decoded as f64,
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "repeated_version_entries_decoded",
        repeated_decoded as f64,
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "repeated_old_snapshot_entries_decoded",
        repeated_old_snapshot_decoded as f64,
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );

    engine
        .shutdown(Duration::from_secs(30))
        .expect("shutdown SST V5 benchmark database");
}

#[stress(
    tier = 4,
    role = "diagnostic",
    metadata(component = "sst_format", scenario = "v5_decode_and_footprint")
)]
fn tier4_system_sst_v5(ctx: &mut StressContext) {
    run(ctx);
}

stress_main!();
