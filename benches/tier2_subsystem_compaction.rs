//! Tier 2 - production compaction CPU per logical input MiB.
//!
//! The fixture writes eight independently flushed SSTs. Fixture creation is
//! outside the measurement; the measured phase is one production `compact_all`
//! call. Process CPU time includes the compaction worker threads.

use cntryl_midge::{ColumnFamilyHandle, Engine, OpenOptions, TransactionMode, WriteOptions};
use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const BENCHMARK: &str = "tier2_subsystem_compaction";
const SST_COUNT: usize = 8;
const ROWS_PER_SST: usize = 100_000;
const KEY_BYTES: usize = 16;
const VALUE_BYTES: usize = 128;
const LOGICAL_BYTES_PER_ROW: u64 = (KEY_BYTES + VALUE_BYTES) as u64;
const WRITE_BATCH: usize = 1_000;

fn data_path() -> PathBuf {
    let root = PathBuf::from("target/tmp");
    std::fs::create_dir_all(&root).expect("create benchmark temp directory");
    root.join(format!("{BENCHMARK}-{}", std::process::id()))
}

fn key(id: u64) -> [u8; KEY_BYTES] {
    let mut key = [0; KEY_BYTES];
    key[..8].copy_from_slice(b"midge-cp");
    key[8..].copy_from_slice(&id.to_be_bytes());
    key
}

fn value(id: u64) -> [u8; VALUE_BYTES] {
    let mut value = [0; VALUE_BYTES];
    let mut state = id ^ 0x9490_2026_1025;
    for chunk in value.as_chunks_mut::<8>().0 {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut word = state;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        *chunk = (word ^ (word >> 31)).to_le_bytes();
    }
    value
}

fn process_cpu_time() -> Duration {
    // SAFETY: `usage` points to a valid, initialized writable rusage value.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `getrusage` writes to the valid pointer above and does not retain it.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) };
    assert_eq!(result, 0, "read process CPU time");
    let user = timeval_duration(usage.ru_utime);
    let system = timeval_duration(usage.ru_stime);
    user.saturating_add(system)
}

fn u64_as_f64(value: u64) -> f64 {
    let upper = u32::try_from(value >> u32::BITS).expect("upper half fits in u32");
    let lower = u32::try_from(value & u64::from(u32::MAX)).expect("lower half fits in u32");
    f64::from(upper) * 4_294_967_296.0 + f64::from(lower)
}

fn timeval_duration(value: libc::timeval) -> Duration {
    Duration::new(
        u64::try_from(value.tv_sec).expect("process CPU seconds are nonnegative"),
        u32::try_from(value.tv_usec)
            .expect("process CPU microseconds are nonnegative")
            .saturating_mul(1_000),
    )
}

fn write_sst(engine: &Engine, cf: &ColumnFamilyHandle, first_id: u64) {
    for offset in (0..ROWS_PER_SST).step_by(WRITE_BATCH) {
        let mut transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin compaction fixture transaction");
        for row in offset..offset + WRITE_BATCH {
            let id = first_id + u64::try_from(row).expect("row index fits in u64");
            transaction
                .put(key(id).to_vec(), value(id).to_vec(), None)
                .expect("stage compaction fixture row");
        }
        transaction
            .commit(WriteOptions::buffered())
            .expect("commit compaction fixture transaction");
    }
    engine.flush_cf(cf).expect("flush compaction fixture SST");
}

fn fixture(engine: &Engine, cf: &ColumnFamilyHandle) {
    for sst in 0..SST_COUNT {
        let first_id = u64::try_from(sst * ROWS_PER_SST).expect("fixture id fits in u64");
        write_sst(engine, cf, first_id);
    }
}

fn sst_layout(engine: &Engine, cf: &ColumnFamilyHandle) -> Vec<(u32, String, u64)> {
    engine
        .metrics()
        .get_storage_layout()
        .expect("read benchmark storage layout")
        .levels
        .into_iter()
        .flat_map(|level| {
            level
                .files
                .into_iter()
                .filter(|file| file.cf_id == cf.id())
                .map(move |file| (level.level, file.name, file.size_bytes))
        })
        .collect()
}

fn run(ctx: &mut StressContext) {
    let path = data_path();
    if path.exists() {
        std::fs::remove_dir_all(&path).expect("remove previous benchmark database");
    }
    let mut engine = Engine::open(
        OpenOptions::local(&path)
            .background_compaction(false)
            .build()
            .expect("build local benchmark options"),
    )
    .expect("open local benchmark engine");
    let cf = engine
        .create_column_family("compaction-cpu")
        .expect("create benchmark column family");
    fixture(&engine, &cf);

    let rows = u64::try_from(SST_COUNT * ROWS_PER_SST).expect("row count fits in u64");
    let logical_bytes = rows.saturating_mul(LOGICAL_BYTES_PER_ROW);
    let logical_mib = u64_as_f64(logical_bytes) / (1024.0 * 1024.0);
    let inputs_before = sst_layout(&engine, &cf);
    assert_eq!(
        inputs_before.len(),
        SST_COUNT,
        "fixture must contain eight SSTs"
    );
    let cpu_before = process_cpu_time();
    let wall_started = Instant::now();
    engine.compact_all().expect("compact benchmark fixture");
    let wall_elapsed = wall_started.elapsed();
    let cpu_elapsed = process_cpu_time().saturating_sub(cpu_before);
    assert!(
        sst_layout(&engine, &cf) != inputs_before,
        "fixture must produce real compaction work: before={inputs_before:?}, after={:?}",
        sst_layout(&engine, &cf)
    );
    let compaction_bytes = sst_layout(&engine, &cf)
        .iter()
        .map(|(_, _, size)| size)
        .copied()
        .sum::<u64>();

    ctx.parameter("fixture_sst_count", SST_COUNT);
    ctx.parameter("rows_per_sst", ROWS_PER_SST);
    ctx.parameter("key_bytes", KEY_BYTES);
    ctx.parameter("value_bytes", VALUE_BYTES);
    ctx.parameter("logical_input_bytes", logical_bytes);
    ctx.record_external_outcome(
        "compaction_cpu_per_logical_input_byte",
        cpu_elapsed,
        LogicalUnit::new("logical_input_byte"),
        OperationOutcome::success(logical_bytes),
    );
    ctx.record_observation(
        "compaction_cpu_ns_per_logical_mib",
        u64_as_f64(
            u64::try_from(cpu_elapsed.as_nanos()).expect("CPU time fits in u64 nanoseconds"),
        ) / logical_mib,
        ObservationUnit::Nanoseconds,
        ObservationDirection::LowerIsBetter,
    );
    ctx.record_observation(
        "compaction_wall_ns_per_logical_mib",
        u64_as_f64(
            u64::try_from(wall_elapsed.as_nanos()).expect("wall time fits in u64 nanoseconds"),
        ) / logical_mib,
        ObservationUnit::Nanoseconds,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "compaction_output_bytes",
        u64_as_f64(compaction_bytes),
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    );

    engine
        .shutdown(Duration::from_secs(60))
        .expect("shutdown benchmark engine");
    drop(engine);
    std::fs::remove_dir_all(path).expect("remove benchmark database");
}

#[stress(
    tier = 2,
    metadata(component = "compaction", scenario = "cpu_per_logical_mib")
)]
fn tier2_compaction_cpu_per_logical_mib(ctx: &mut StressContext) {
    run(ctx);
}

stress_main!();
