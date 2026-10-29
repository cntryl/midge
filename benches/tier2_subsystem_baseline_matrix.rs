//! Tier 2 - Baseline matrix
//!
//! The rows every 0.5.0 performance change is judged against, shaped like the
//! `db_bench` workloads they will be compared with: 16-byte keys, 100-byte
//! values, one operation per transaction, on local disk with production
//! defaults. `fillseq` and `fillrandom` write; `readrandom` reads a
//! memtable-resident set (hot) and a freshly reopened engine's SSTs (cold);
//! `seekrandom` scans ten rows from a random key. The procedure that turns
//! these rows into committed evidence is `docs/development/baseline-procedure.md`.

use cntryl_midge::{
    ColumnFamilyId, Engine, OpenOptions, Query, RuntimeMetricsSnapshot, TransactionMode,
    WriteOptions,
};
use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};
use std::time::{Duration, Instant};

const OPERATIONS: usize = 100_000;
/// Each `sync()` commit waits for an fsync, so the strict fill row uses fewer
/// operations to stay within a Tier 2 window.
const SYNC_OPERATIONS: usize = 10_000;
const SEEKS: usize = 20_000;
const SEEK_LIMIT: usize = 10;
const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 100;
/// Reads refresh their snapshot this often so one long-lived snapshot does
/// not pin the whole run while the per-read cost stays the read itself.
const READS_PER_SNAPSHOT: usize = 1_000;
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// Deterministic xorshift64* so every repeat issues the same keys.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(bound).expect("bound fits in u64"))
            .expect("index fits in usize")
    }
}

fn key(index: usize) -> Vec<u8> {
    format!("{index:0KEY_SIZE$}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    vec![u8::try_from(index % 251).expect("value byte fits in u8"); VALUE_SIZE]
}

/// One row's engine on a fresh directory with production defaults, so rows
/// never see each other's data and the numbers describe the shipped
/// configuration rather than a benchmark-only one.
struct Baseline {
    _dir: tempfile::TempDir,
    opts: OpenOptions,
    engine: Engine,
    cf_id: ColumnFamilyId,
}

impl Baseline {
    fn open() -> Self {
        let dir = tempfile::tempdir().expect("baseline directory");
        let opts = OpenOptions::local(dir.path())
            .build()
            .expect("baseline options");
        let engine = Engine::open(opts.clone()).expect("open baseline engine");
        let cf_id = engine
            .create_column_family("baseline")
            .expect("create baseline column family")
            .id();
        Self {
            _dir: dir,
            opts,
            engine,
            cf_id,
        }
    }

    /// Shut down and reopen on the same directory so the block cache and
    /// memtables start empty and reads come from SSTs.
    fn reopen_cold(&mut self) {
        self.engine
            .shutdown(Duration::from_secs(60))
            .expect("shut down before cold reopen");
        self.engine = Engine::open(self.opts.clone()).expect("reopen baseline engine");
        self.cf_id = self
            .engine
            .get_column_family("baseline")
            .expect("baseline column family survives reopen")
            .id();
    }

    fn metrics(&self) -> RuntimeMetricsSnapshot {
        self.engine
            .metrics()
            .get_runtime_metrics()
            .expect("baseline runtime metrics")
    }

    fn close(mut self) {
        self.engine
            .shutdown(Duration::from_secs(60))
            .expect("shut down baseline engine");
    }
}

struct Timed {
    elapsed: Duration,
    latencies_ns: Vec<u64>,
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn u64_to_f64(value: u64) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    {
        value as f64
    }
}

/// Write `count` single-put transactions, sequential or random keys.
fn fill(base: &Baseline, count: usize, random: bool, write: WriteOptions) -> Timed {
    let mut rng = Rng(SEED);
    let mut latencies_ns = Vec::with_capacity(count);
    let started = Instant::now();
    for ordinal in 0..count {
        let index = if random { rng.below(count) } else { ordinal };
        let op_started = Instant::now();
        let mut tx = base
            .engine
            .begin_tx(base.cf_id, TransactionMode::ReadWrite)
            .expect("begin fill transaction");
        tx.put(key(index), value(index), None)
            .expect("put fill value");
        tx.commit(write).expect("commit fill value");
        latencies_ns.push(duration_nanos(op_started.elapsed()));
    }
    Timed {
        elapsed: started.elapsed(),
        latencies_ns,
    }
}

/// Point-read `count` random keys from the `fillrandom` key space.
fn read_random(base: &Baseline, count: usize) -> Timed {
    let mut rng = Rng(SEED.rotate_left(17));
    let mut latencies_ns = Vec::with_capacity(count);
    let started = Instant::now();
    let mut remaining = 0;
    let mut snapshot = None;
    for _ in 0..count {
        if remaining == 0 {
            snapshot = Some(
                base.engine
                    .begin_tx(base.cf_id, TransactionMode::ReadOnly)
                    .expect("begin read snapshot"),
            );
            remaining = READS_PER_SNAPSHOT;
        }
        remaining -= 1;
        let target = key(rng.below(OPERATIONS));
        let op_started = Instant::now();
        let _ = snapshot
            .as_ref()
            .expect("read snapshot is open")
            .get(&target)
            .expect("point read");
        latencies_ns.push(duration_nanos(op_started.elapsed()));
    }
    Timed {
        elapsed: started.elapsed(),
        latencies_ns,
    }
}

/// Scan `SEEK_LIMIT` rows from `count` random start keys.
fn seek_random(base: &Baseline, count: usize) -> (Timed, u64) {
    let mut rng = Rng(SEED.rotate_left(29));
    let mut latencies_ns = Vec::with_capacity(count);
    let mut rows = 0_u64;
    let started = Instant::now();
    let mut remaining = 0;
    let mut snapshot = None;
    for _ in 0..count {
        if remaining == 0 {
            snapshot = Some(
                base.engine
                    .begin_tx(base.cf_id, TransactionMode::ReadOnly)
                    .expect("begin seek snapshot"),
            );
            remaining = READS_PER_SNAPSHOT;
        }
        remaining -= 1;
        let start = key(rng.below(OPERATIONS));
        let query = Query::new()
            .start_key(bytes::Bytes::from(start))
            .limit(SEEK_LIMIT);
        let op_started = Instant::now();
        let scanned = snapshot
            .as_ref()
            .expect("seek snapshot is open")
            .scan(&query)
            .expect("seek scan")
            .try_collect()
            .expect("seek rows")
            .len();
        latencies_ns.push(duration_nanos(op_started.elapsed()));
        rows += u64::try_from(scanned).expect("row count fits in u64");
    }
    (
        Timed {
            elapsed: started.elapsed(),
            latencies_ns,
        },
        rows,
    )
}

fn record(ctx: &mut StressContext, scenario: &'static str, unit: &'static str, timed: &Timed) {
    ctx.record_external_outcome(
        scenario,
        timed.elapsed,
        LogicalUnit::new(unit),
        OperationOutcome::success(
            u64::try_from(timed.latencies_ns.len()).expect("operation count fits in u64"),
        ),
    );
    for latency_ns in &timed.latencies_ns {
        ctx.record_latency(Duration::from_nanos((*latency_ns).max(1)));
    }
}

fn record_write_deltas(
    ctx: &mut StressContext,
    before: &RuntimeMetricsSnapshot,
    after: &RuntimeMetricsSnapshot,
) {
    ctx.record_observation(
        "wal_appends",
        u64_to_f64(
            after
                .wal_append_count
                .saturating_sub(before.wal_append_count),
        ),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "wal_fsyncs",
        u64_to_f64(after.wal_fsync_count.saturating_sub(before.wal_fsync_count)),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
}

fn record_read_deltas(
    ctx: &mut StressContext,
    before: &RuntimeMetricsSnapshot,
    after: &RuntimeMetricsSnapshot,
) {
    ctx.record_observation(
        "block_cache_hits",
        u64_to_f64(after.cache_hits.saturating_sub(before.cache_hits)),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "block_cache_misses",
        u64_to_f64(after.cache_misses.saturating_sub(before.cache_misses)),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "sst_data_blocks_read",
        u64_to_f64(
            after
                .sst_data_blocks_read_total
                .saturating_sub(before.sst_data_blocks_read_total),
        ),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
}

fn common_parameters(ctx: &mut StressContext, operations: usize) {
    ctx.parameter("logical_batch_size", operations);
    ctx.parameter("storage_profile", "local");
    ctx.parameter("key_size_bytes", KEY_SIZE);
    ctx.parameter("value_size_bytes", VALUE_SIZE);
    ctx.parameter("operations_per_transaction", 1);
    ctx.parameter("writers", 1);
}

fn run_fill(ctx: &mut StressContext, scenario: &'static str, random: bool, write: WriteOptions) {
    let count = if write.is_sync() {
        SYNC_OPERATIONS
    } else {
        OPERATIONS
    };
    common_parameters(ctx, count);
    ctx.parameter("logical_unit", "put");
    ctx.parameter("key_order", if random { "random" } else { "sequential" });
    ctx.parameter(
        "commit_mode",
        if write.is_sync() { "sync" } else { "buffered" },
    );
    let base = Baseline::open();
    let before = base.metrics();
    let timed = fill(&base, count, random, write);
    let after = base.metrics();
    base.close();
    record(ctx, scenario, "put", &timed);
    record_write_deltas(ctx, &before, &after);
}

#[stress(tier = 2)]
fn tier2_baseline_fillseq_local_buffered(ctx: &mut StressContext) {
    run_fill(
        ctx,
        "tier2_baseline_fillseq_local_buffered",
        false,
        WriteOptions::buffered(),
    );
}

#[stress(tier = 2)]
fn tier2_baseline_fillrandom_local_buffered(ctx: &mut StressContext) {
    run_fill(
        ctx,
        "tier2_baseline_fillrandom_local_buffered",
        true,
        WriteOptions::buffered(),
    );
}

#[stress(tier = 2)]
fn tier2_baseline_fillrandom_local_sync(ctx: &mut StressContext) {
    run_fill(
        ctx,
        "tier2_baseline_fillrandom_local_sync",
        true,
        WriteOptions::sync(),
    );
}

#[stress(tier = 2)]
fn tier2_baseline_readrandom_local_hot(ctx: &mut StressContext) {
    common_parameters(ctx, OPERATIONS);
    ctx.parameter("logical_unit", "get");
    ctx.parameter("working_set", "memtable_resident");
    let base = Baseline::open();
    fill(&base, OPERATIONS, true, WriteOptions::buffered());
    let before = base.metrics();
    let timed = read_random(&base, OPERATIONS);
    let after = base.metrics();
    base.close();
    record(ctx, "tier2_baseline_readrandom_local_hot", "get", &timed);
    record_read_deltas(ctx, &before, &after);
}

#[stress(tier = 2)]
fn tier2_baseline_readrandom_local_cold(ctx: &mut StressContext) {
    common_parameters(ctx, OPERATIONS);
    ctx.parameter("logical_unit", "get");
    ctx.parameter("working_set", "sst_after_cold_reopen");
    let mut base = Baseline::open();
    fill(&base, OPERATIONS, true, WriteOptions::buffered());
    let cf = base
        .engine
        .get_column_family("baseline")
        .expect("baseline column family");
    base.engine.flush_cf(&cf).expect("flush before cold reads");
    base.reopen_cold();
    let before = base.metrics();
    let timed = read_random(&base, OPERATIONS);
    let after = base.metrics();
    base.close();
    record(ctx, "tier2_baseline_readrandom_local_cold", "get", &timed);
    record_read_deltas(ctx, &before, &after);
}

#[stress(tier = 2)]
fn tier2_baseline_seekrandom_local(ctx: &mut StressContext) {
    common_parameters(ctx, SEEKS);
    ctx.parameter("logical_unit", "scan");
    ctx.parameter("scan_limit", SEEK_LIMIT);
    let base = Baseline::open();
    fill(&base, OPERATIONS, true, WriteOptions::buffered());
    let cf = base
        .engine
        .get_column_family("baseline")
        .expect("baseline column family");
    base.engine.flush_cf(&cf).expect("flush before seeks");
    let before = base.metrics();
    let (timed, rows) = seek_random(&base, SEEKS);
    let after = base.metrics();
    base.close();
    record(ctx, "tier2_baseline_seekrandom_local", "scan", &timed);
    ctx.record_observation(
        "rows_returned",
        u64_to_f64(rows),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    record_read_deltas(ctx, &before, &after);
}

stress_main!();
