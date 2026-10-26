//! Tier 4 — SST footprint for small key/value pairs.
//!
//! This public-API-only fixture is compatible with the V4 baseline and V5
//! candidate, so identical workloads can report SST bytes across both formats.

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use cntryl_stress::{stress, stress_main, ObservationDirection, ObservationUnit, StressContext};
use std::time::Duration;
use tempfile::TempDir;

const UNIQUE_KEYS: u64 = 8_192;
const REPEATED_VERSIONS: usize = 128;
const VALUE_BYTES: usize = 32;

struct PreparedFixture {
    _directory: TempDir,
    engine: Engine,
    files_count: usize,
    total_sst_bytes: u64,
}

fn key(index: u64) -> [u8; 16] {
    let mut key = *b"sst-v5-bench0000";
    key[8..].copy_from_slice(&index.to_be_bytes());
    key
}

fn value(seed: u64) -> [u8; VALUE_BYTES] {
    let mut state = seed ^ 0x9180_2026_1006;
    let mut value = [0; VALUE_BYTES];
    for chunk in value.as_chunks_mut::<8>().0 {
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
        .expect("begin SST footprint fixture write");
    tx.put(key.to_vec(), value.to_vec(), None)
        .expect("stage SST footprint fixture value");
    tx.commit(WriteOptions::best_effort())
        .expect("commit SST footprint fixture value");
}

fn prepare_fixture() -> PreparedFixture {
    let directory = TempDir::new().expect("create SST footprint database");
    let engine = Engine::open(
        OpenOptions::local(directory.path())
            .build()
            .expect("build SST footprint benchmark options"),
    )
    .expect("open SST footprint benchmark database");
    let cf = engine
        .create_column_family("sst-v5-footprint")
        .expect("create SST footprint benchmark column family");
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
    engine.flush_cf(&cf).expect("flush SST footprint fixture");

    let layout = engine
        .metrics()
        .get_storage_layout()
        .expect("capture SST footprint layout");
    let files: Vec<_> = layout
        .levels
        .iter()
        .flat_map(|level| level.files.iter())
        .filter(|file| file.cf_id == cf.id())
        .collect();
    assert!(!files.is_empty(), "fixture must publish an SST");
    let total_sst_bytes = files.iter().map(|file| file.size_bytes).sum();
    PreparedFixture {
        _directory: directory,
        engine,
        files_count: files.len(),
        total_sst_bytes,
    }
}

fn exact_metric_value(value: u64) -> f64 {
    const MAX_EXACT_F64_INTEGER: u64 = 1_u64 << 53;
    assert!(
        value <= MAX_EXACT_F64_INTEGER,
        "metric exceeds exact f64 range"
    );
    let high = u32::try_from(value >> 32).expect("high metric word fits in u32");
    let low = u32::try_from(value & u64::from(u32::MAX)).expect("low metric word fits in u32");
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

fn record_metrics(ctx: &mut StressContext, files_count: usize, total_sst_bytes: u64) {
    ctx.parameter("format", "small_pair_sst");
    ctx.parameter("unique_keys", UNIQUE_KEYS);
    ctx.parameter("repeated_versions", REPEATED_VERSIONS);
    ctx.parameter("key_bytes", 16);
    ctx.parameter("value_bytes", VALUE_BYTES);
    ctx.record_observation(
        "sst_files",
        exact_metric_value(u64::try_from(files_count).expect("file count fits in u64")),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "sst_footprint_bytes",
        exact_metric_value(total_sst_bytes),
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    );
}

fn run(ctx: &mut StressContext) {
    let mut footprints = Vec::with_capacity(6);
    ctx.benchmark("fixture_build_and_flush")
        .samples(5)
        .warmup(1)
        .operations_per_sample(1)
        .measure(|| {
            let mut fixture = prepare_fixture();
            footprints.push((fixture.files_count, fixture.total_sst_bytes));
            fixture
                .engine
                .shutdown(Duration::from_secs(30))
                .expect("shutdown SST footprint benchmark database");
        });
    let (files_count, total_sst_bytes) = *footprints
        .last()
        .expect("benchmark records at least one fixture");
    record_metrics(ctx, files_count, total_sst_bytes);
}

#[stress(
    tier = 4,
    role = "diagnostic",
    metadata(component = "sst_format", scenario = "small_pair_footprint")
)]
fn tier4_system_sst_footprint(ctx: &mut StressContext) {
    run(ctx);
}

stress_main!();
