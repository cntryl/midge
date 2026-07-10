//! Tier 2 - Transaction Latency and Coalescing Guard
//!
//! Measures: public transaction lifecycle latency and buffered write coalescing
//! Purpose: Catch regressions in actor-faithful transaction submission paths
//!
//! This benchmark uses only public `Engine::begin_tx`, transaction `commit`,
//! and runtime metrics. It does not call runtime internals directly.

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::diagnostics::TransactionCommitTimingSample;
use cntryl_midge::{ColumnFamilyId, Engine, RuntimeMetricsSnapshot, TransactionMode, WriteOptions};
use cntryl_stress::{stress, stress_main, StressContext};
use hdrhistogram::Histogram;
use stress_config::init_benchmark_telemetry;

const COALESCING_CLIENTS: usize = 16;
const COALESCING_TXNS_PER_CLIENT: usize = 128;
const COALESCING_VALUE_SIZE: usize = 64;
const LATENCY_SEQUENTIAL_TXNS: usize = 1024;
const LATENCY_READ_ONLY_TXNS: usize = 4096;
const LATENCY_SAMPLE_REPEATS: usize = 16;
const READ_ONLY_BEGIN_TX_SAMPLE_REPEATS: usize = 64;
const READ_ONLY_BEGIN_TX_SAMPLE_COUNT: usize = 12;
const READ_ONLY_BEGIN_TX_WARMUP_SAMPLES: usize = 8;
const MAX_DIRECT_SUBMIT_FOLLOWER_WAIT_US: f64 = 1.0;
const MIN_AVG_TXN_RECORDS_PER_APPEND: f64 = 7.0;

type LatencyClientWorkload = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Clone, Copy, Debug)]
struct TransactionCoalescingSignal {
    logical_txn_records: u64,
    physical_wal_appends: u64,
    avg_txn_records_per_append: f64,
    avg_wal_append_us: f64,
}

#[derive(Clone, Copy, Debug)]
enum LatencyWorkloadKind {
    SequentialBufferedSingleOp,
    ConcurrentBufferedSingleOp,
    ReadOnlyBeginTx,
}

impl LatencyWorkloadKind {
    fn label(self) -> &'static str {
        match self {
            Self::SequentialBufferedSingleOp => "buffered_sequential_single_op_transactions",
            Self::ConcurrentBufferedSingleOp => "buffered_concurrent_single_op_transactions",
            Self::ReadOnlyBeginTx => "read_only_begin_tx_baseline",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct LatencyClientTotals {
    transactions: u64,
    begin_tx_ns: u64,
    put_ns: u64,
    commit_ns: u64,
}

impl LatencyClientTotals {
    fn record_begin_tx(&mut self, duration: Duration) {
        self.begin_tx_ns = self.begin_tx_ns.saturating_add(duration_to_ns(duration));
    }

    fn record_put(&mut self, duration: Duration) {
        self.put_ns = self.put_ns.saturating_add(duration_to_ns(duration));
    }

    fn record_commit(&mut self, duration: Duration) {
        self.commit_ns = self.commit_ns.saturating_add(duration_to_ns(duration));
    }

    fn record_transaction(&mut self) {
        self.transactions = self.transactions.saturating_add(1);
    }

    fn merge(&mut self, other: Self) {
        self.transactions = self.transactions.saturating_add(other.transactions);
        self.begin_tx_ns = self.begin_tx_ns.saturating_add(other.begin_tx_ns);
        self.put_ns = self.put_ns.saturating_add(other.put_ns);
        self.commit_ns = self.commit_ns.saturating_add(other.commit_ns);
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct CommitLatencyDistribution {
    samples: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

impl CommitLatencyDistribution {
    fn from_histogram(histogram: &Histogram<u64>) -> Self {
        let samples = histogram.len();
        if samples == 0 {
            return Self::default();
        }

        Self {
            samples,
            p50_us: histogram.value_at_quantile(0.50),
            p95_us: histogram.value_at_quantile(0.95),
            p99_us: histogram.value_at_quantile(0.99),
            max_us: histogram.max(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct CommitTimingTotals {
    samples: u64,
    succeeded: u64,
    commit_total_ns: u64,
    submit_apply_transaction_ns: u64,
    write_group_leader_collect_ns: u64,
    write_group_runtime_apply_ns: u64,
    write_group_follower_wait_ns: u64,
    durability_finalize_ns: u64,
    unregister_snapshot_ns: u64,
    commit_latency_us: CommitLatencyDistribution,
}

impl CommitTimingTotals {
    fn from_samples(samples: &[TransactionCommitTimingSample]) -> Self {
        let mut commit_latency_us =
            Histogram::<u64>::new(3).expect("create commit latency histogram");

        let mut totals = Self::default();
        for sample in samples {
            totals.samples = totals.samples.saturating_add(1);
            totals.succeeded = totals.succeeded.saturating_add(u64::from(sample.succeeded));
            totals.commit_total_ns = totals
                .commit_total_ns
                .saturating_add(sample.commit_total_ns);
            totals.submit_apply_transaction_ns = totals
                .submit_apply_transaction_ns
                .saturating_add(sample.submit_apply_transaction_ns);
            totals.write_group_leader_collect_ns = totals
                .write_group_leader_collect_ns
                .saturating_add(sample.write_group_leader_collect_ns);
            totals.write_group_runtime_apply_ns = totals
                .write_group_runtime_apply_ns
                .saturating_add(sample.write_group_runtime_apply_ns);
            totals.write_group_follower_wait_ns = totals
                .write_group_follower_wait_ns
                .saturating_add(sample.write_group_follower_wait_ns);
            totals.durability_finalize_ns = totals
                .durability_finalize_ns
                .saturating_add(sample.durability_finalize_ns);
            totals.unregister_snapshot_ns = totals
                .unregister_snapshot_ns
                .saturating_add(sample.unregister_snapshot_ns);
            commit_latency_us
                .record(ns_to_us_ceil(sample.commit_total_ns))
                .expect("record commit latency sample");
        }

        totals.commit_latency_us = CommitLatencyDistribution::from_histogram(&commit_latency_us);
        totals
    }
}

#[derive(Clone, Copy, Debug)]
struct TransactionLatencyBreakdown {
    kind: LatencyWorkloadKind,
    clients: usize,
    transactions: u64,
    logical_txn_records: u64,
    begin_tx_us: f64,
    put_us: f64,
    commit_total_us: f64,
    commit_samples: u64,
    commit_p50_us: u64,
    commit_p95_us: u64,
    commit_p99_us: u64,
    commit_max_us: u64,
    submit_apply_transaction_us: f64,
    write_group_leader_collect_us: f64,
    write_group_runtime_apply_us: f64,
    write_group_follower_wait_us: f64,
    submit_apply_other_us: f64,
    durability_finalize_us: f64,
    unregister_snapshot_us: f64,
    runtime_submit_ack_non_wal_us: f64,
    physical_wal_appends: u64,
    avg_wal_append_us: f64,
    avg_txn_records_per_append: f64,
}

impl TransactionLatencyBreakdown {
    fn dominant_phase(self) -> &'static str {
        let mut dominant = ("begin_tx", self.begin_tx_us);
        for candidate in [
            ("put", self.put_us),
            (
                "write_group_leader_collect",
                self.write_group_leader_collect_us,
            ),
            (
                "write_group_runtime_apply",
                self.write_group_runtime_apply_us,
            ),
            (
                "write_group_follower_wait",
                self.write_group_follower_wait_us,
            ),
            ("submit_apply_other", self.submit_apply_other_us),
            ("durability_finalize", self.durability_finalize_us),
            ("unregister_snapshot", self.unregister_snapshot_us),
        ] {
            if candidate.1 > dominant.1 {
                dominant = candidate;
            }
        }
        dominant.0
    }
}

impl TransactionCoalescingSignal {
    fn from_snapshots(
        logical_txn_records: u64,
        start: &RuntimeMetricsSnapshot,
        end: &RuntimeMetricsSnapshot,
    ) -> Self {
        let physical_wal_appends = end.wal_append_count.saturating_sub(start.wal_append_count);
        let wal_append_ns_total = end
            .wal_append_ns_total
            .saturating_sub(start.wal_append_ns_total);

        Self {
            logical_txn_records,
            physical_wal_appends,
            avg_txn_records_per_append: average(logical_txn_records, physical_wal_appends),
            avg_wal_append_us: average(wal_append_ns_total, physical_wal_appends * 1_000),
        }
    }
}

fn u64_to_f64(value: u64) -> f64 {
    let upper = u32::try_from(value >> 32).expect("upper half fits in u32");
    let lower = u32::try_from(value & u64::from(u32::MAX)).expect("lower half fits in u32");
    f64::from(upper) * 4_294_967_296.0 + f64::from(lower)
}

fn average(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        u64_to_f64(numerator) / u64_to_f64(denominator)
    }
}

fn duration_to_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn ns_to_avg_us(total_ns: u64, count: u64) -> f64 {
    average(total_ns, count.saturating_mul(1_000))
}

fn ns_to_us_ceil(ns: u64) -> u64 {
    ns.saturating_add(999) / 1_000
}

fn logical_coalescing_txn_records() -> u64 {
    u64::try_from(COALESCING_CLIENTS * COALESCING_TXNS_PER_CLIENT)
        .expect("logical transaction count fits in u64")
}

fn make_latency_client_workloads(
    clients: usize,
    txns_per_client: usize,
    key_prefix: &str,
) -> Vec<LatencyClientWorkload> {
    (0..clients)
        .map(|client_id| {
            (0..txns_per_client)
                .map(|txn_id| {
                    let key = format!("{key_prefix}_{client_id:02}_{txn_id:04}").into_bytes();
                    let value_byte =
                        u8::try_from((client_id + txn_id) % 251).expect("value byte fits in u8");
                    (key, vec![value_byte; COALESCING_VALUE_SIZE])
                })
                .collect()
        })
        .collect()
}

fn run_transaction_coalescing_signal(
    engine: &Arc<Engine>,
    cf_id: ColumnFamilyId,
) -> TransactionCoalescingSignal {
    let start = engine
        .get_runtime_metrics()
        .expect("get starting runtime metrics");
    // The signal measures WAL submission coalescing, not snapshot-acquisition
    // scheduling. Coordinate clients immediately before commit so every
    // transaction is already prepared when the write-group collector runs.
    let commit_barrier = Arc::new(Barrier::new(COALESCING_CLIENTS));
    let mut handles = Vec::with_capacity(COALESCING_CLIENTS);

    for client_id in 0..COALESCING_CLIENTS {
        let engine_clone = Arc::clone(engine);
        let commit_barrier_clone = Arc::clone(&commit_barrier);

        handles.push(std::thread::spawn(move || {
            for txn_id in 0..COALESCING_TXNS_PER_CLIENT {
                let mut tx = engine_clone
                    .begin_tx(cf_id, TransactionMode::ReadWrite)
                    .expect("begin transaction");
                let key = format!("coalesce_{client_id:02}_{txn_id:04}").into_bytes();
                let value_byte =
                    u8::try_from((client_id + txn_id) % 251).expect("value byte fits in u8");
                tx.put(key, vec![value_byte; COALESCING_VALUE_SIZE], None)
                    .expect("put coalescing value");
                commit_barrier_clone.wait();
                tx.commit(WriteOptions::buffered())
                    .expect("commit coalescing transaction");
            }
        }));
    }

    for handle in handles {
        handle.join().expect("coalescing worker should finish");
    }

    let end = engine
        .get_runtime_metrics()
        .expect("get ending runtime metrics");
    TransactionCoalescingSignal::from_snapshots(logical_coalescing_txn_records(), &start, &end)
}

fn run_buffered_latency_client(
    engine: &Engine,
    cf_id: ColumnFamilyId,
    workload: LatencyClientWorkload,
) -> LatencyClientTotals {
    let mut totals = LatencyClientTotals::default();
    let write_opts = WriteOptions::buffered();

    for (key, value) in workload {
        let begin_started_at = Instant::now();
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin latency transaction");
        totals.record_begin_tx(begin_started_at.elapsed());

        let put_started_at = Instant::now();
        tx.put(key, value, None).expect("put latency value");
        totals.record_put(put_started_at.elapsed());

        let commit_started_at = Instant::now();
        tx.commit(write_opts).expect("commit latency transaction");
        totals.record_commit(commit_started_at.elapsed());
        totals.record_transaction();
    }

    totals
}

fn run_buffered_transaction_latency_breakdown(
    engine: &Arc<Engine>,
    cf_id: ColumnFamilyId,
    kind: LatencyWorkloadKind,
    workloads: Vec<LatencyClientWorkload>,
) -> TransactionLatencyBreakdown {
    let clients = workloads.len();
    let logical_txn_records = workloads
        .iter()
        .map(Vec::len)
        .try_fold(0_u64, |accumulator, len| {
            u64::try_from(len).map(|len| accumulator.saturating_add(len))
        })
        .expect("transaction count fits in u64");
    let start = engine
        .get_runtime_metrics()
        .expect("get starting latency runtime metrics");
    cntryl_midge::diagnostics::enable_transaction_commit_timing_for_benchmarks();
    let mut client_totals = LatencyClientTotals::default();
    if clients == 1 {
        let workload = workloads
            .into_iter()
            .next()
            .expect("sequential workload should exist");
        client_totals.merge(run_buffered_latency_client(engine, cf_id, workload));
    } else {
        let barrier = Arc::new(Barrier::new(clients));
        let mut handles = Vec::with_capacity(clients);

        for workload in workloads {
            let engine_clone = Arc::clone(engine);
            let barrier_clone = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier_clone.wait();
                run_buffered_latency_client(&engine_clone, cf_id, workload)
            }));
        }

        for handle in handles {
            client_totals.merge(handle.join().expect("latency worker should finish"));
        }
    }

    let end = engine
        .get_runtime_metrics()
        .expect("get ending latency runtime metrics");
    let timing_samples =
        cntryl_midge::diagnostics::drain_transaction_commit_timings_for_benchmarks();
    cntryl_midge::diagnostics::disable_transaction_commit_timing_for_benchmarks();
    let commit_totals = CommitTimingTotals::from_samples(&timing_samples);
    assert_eq!(commit_totals.samples, logical_txn_records);
    assert_eq!(commit_totals.succeeded, commit_totals.samples);
    assert_eq!(
        commit_totals.commit_latency_us.samples,
        commit_totals.samples
    );

    latency_breakdown_from_totals(
        kind,
        clients,
        logical_txn_records,
        client_totals,
        commit_totals,
        &start,
        &end,
    )
}

fn run_read_only_begin_tx_sample(engine: &Engine, cf_id: ColumnFamilyId) -> LatencyClientTotals {
    let mut client_totals = LatencyClientTotals::default();

    for _ in 0..LATENCY_READ_ONLY_TXNS {
        let begin_started_at = Instant::now();
        let tx = engine
            .begin_tx(cf_id, TransactionMode::ReadOnly)
            .expect("begin read-only transaction");
        client_totals.record_begin_tx(begin_started_at.elapsed());
        tx.rollback().expect("rollback read-only transaction");
        client_totals.record_transaction();
    }

    client_totals
}

fn latency_breakdown_from_totals(
    kind: LatencyWorkloadKind,
    clients: usize,
    logical_txn_records: u64,
    client_totals: LatencyClientTotals,
    commit_totals: CommitTimingTotals,
    start: &RuntimeMetricsSnapshot,
    end: &RuntimeMetricsSnapshot,
) -> TransactionLatencyBreakdown {
    let physical_wal_appends = end.wal_append_count.saturating_sub(start.wal_append_count);
    let wal_append_ns_total = end
        .wal_append_ns_total
        .saturating_sub(start.wal_append_ns_total);
    let submit_apply_transaction_us = ns_to_avg_us(
        commit_totals.submit_apply_transaction_ns,
        commit_totals.samples,
    );
    let write_group_leader_collect_us = ns_to_avg_us(
        commit_totals.write_group_leader_collect_ns,
        commit_totals.samples,
    );
    let write_group_runtime_apply_us = ns_to_avg_us(
        commit_totals.write_group_runtime_apply_ns,
        commit_totals.samples,
    );
    let write_group_follower_wait_us = ns_to_avg_us(
        commit_totals.write_group_follower_wait_ns,
        commit_totals.samples,
    );
    let submit_apply_other_us = (submit_apply_transaction_us
        - write_group_leader_collect_us
        - write_group_runtime_apply_us
        - write_group_follower_wait_us)
        .max(0.0);
    let avg_wal_append_us = ns_to_avg_us(wal_append_ns_total, physical_wal_appends);

    TransactionLatencyBreakdown {
        kind,
        clients,
        transactions: client_totals.transactions,
        logical_txn_records,
        begin_tx_us: ns_to_avg_us(client_totals.begin_tx_ns, client_totals.transactions),
        put_us: ns_to_avg_us(client_totals.put_ns, client_totals.transactions),
        commit_total_us: ns_to_avg_us(commit_totals.commit_total_ns, commit_totals.samples),
        commit_samples: commit_totals.samples,
        commit_p50_us: commit_totals.commit_latency_us.p50_us,
        commit_p95_us: commit_totals.commit_latency_us.p95_us,
        commit_p99_us: commit_totals.commit_latency_us.p99_us,
        commit_max_us: commit_totals.commit_latency_us.max_us,
        submit_apply_transaction_us,
        write_group_leader_collect_us,
        write_group_runtime_apply_us,
        write_group_follower_wait_us,
        submit_apply_other_us,
        durability_finalize_us: ns_to_avg_us(
            commit_totals.durability_finalize_ns,
            commit_totals.samples,
        ),
        unregister_snapshot_us: ns_to_avg_us(
            commit_totals.unregister_snapshot_ns,
            commit_totals.samples,
        ),
        runtime_submit_ack_non_wal_us: (submit_apply_transaction_us - avg_wal_append_us).max(0.0),
        physical_wal_appends,
        avg_wal_append_us,
        avg_txn_records_per_append: average(logical_txn_records, physical_wal_appends),
    }
}

fn assert_transaction_latency_guardrails(breakdown: TransactionLatencyBreakdown) {
    match breakdown.kind {
        LatencyWorkloadKind::SequentialBufferedSingleOp
        | LatencyWorkloadKind::ConcurrentBufferedSingleOp => {
            assert_eq!(breakdown.commit_samples, breakdown.logical_txn_records);
        }
        LatencyWorkloadKind::ReadOnlyBeginTx => {
            assert_eq!(breakdown.commit_samples, 0);
        }
    }

    if matches!(
        breakdown.kind,
        LatencyWorkloadKind::SequentialBufferedSingleOp
    ) {
        assert!(
            breakdown.write_group_follower_wait_us <= MAX_DIRECT_SUBMIT_FOLLOWER_WAIT_US,
            "buffered direct-submit follower wait average exceeded guardrail: {:.2}us > {:.2}us",
            breakdown.write_group_follower_wait_us,
            MAX_DIRECT_SUBMIT_FOLLOWER_WAIT_US
        );
    }
}

fn assert_transaction_coalescing_guardrail(signal: TransactionCoalescingSignal) {
    assert_eq!(signal.logical_txn_records, logical_coalescing_txn_records());
    assert!(
        signal.avg_txn_records_per_append >= MIN_AVG_TXN_RECORDS_PER_APPEND,
        "buffered transaction coalescing fell below guardrail: {:.2} < {:.2} txns/append",
        signal.avg_txn_records_per_append,
        MIN_AVG_TXN_RECORDS_PER_APPEND
    );
}

fn open_local_engine_with_cf(cf_name: &str) -> (Arc<Engine>, ColumnFamilyId) {
    let opts = stress_config::write_coordination_opts_for_mode("local");
    let engine = Arc::new(Engine::open(opts.to_open_options()).expect("open local engine"));
    let cf = engine
        .create_column_family(cf_name)
        .expect("create benchmark column family");
    (engine, cf.id())
}

fn tag_transaction_latency_breakdown(
    ctx: &mut StressContext,
    breakdown: TransactionLatencyBreakdown,
) {
    ctx.parameter("workload", breakdown.kind.label());
    ctx.parameter("clients", breakdown.clients);
    ctx.parameter("transactions", breakdown.transactions);
    ctx.parameter("begin_tx_us", format!("{:.2}", breakdown.begin_tx_us));
    ctx.parameter("put_us", format!("{:.2}", breakdown.put_us));
    ctx.parameter(
        "commit_total_us",
        format!("{:.2}", breakdown.commit_total_us),
    );
    ctx.parameter("commit_samples", breakdown.commit_samples);
    ctx.parameter("commit_p50_us", breakdown.commit_p50_us);
    ctx.parameter("commit_p95_us", breakdown.commit_p95_us);
    ctx.parameter("commit_p99_us", breakdown.commit_p99_us);
    ctx.parameter("commit_max_us", breakdown.commit_max_us);
    ctx.parameter(
        "submit_apply_transaction_us",
        format!("{:.2}", breakdown.submit_apply_transaction_us),
    );
    ctx.parameter(
        "write_group_leader_collect_us",
        format!("{:.2}", breakdown.write_group_leader_collect_us),
    );
    ctx.parameter(
        "write_group_runtime_apply_us",
        format!("{:.2}", breakdown.write_group_runtime_apply_us),
    );
    ctx.parameter(
        "write_group_follower_wait_us",
        format!("{:.2}", breakdown.write_group_follower_wait_us),
    );
    ctx.parameter(
        "submit_apply_other_us",
        format!("{:.2}", breakdown.submit_apply_other_us),
    );
    ctx.parameter(
        "durability_finalize_us",
        format!("{:.2}", breakdown.durability_finalize_us),
    );
    ctx.parameter(
        "unregister_snapshot_us",
        format!("{:.2}", breakdown.unregister_snapshot_us),
    );
    ctx.parameter(
        "runtime_submit_ack_non_wal_us",
        format!("{:.2}", breakdown.runtime_submit_ack_non_wal_us),
    );
    ctx.parameter("logical_txn_records", breakdown.logical_txn_records);
    ctx.parameter("physical_wal_appends", breakdown.physical_wal_appends);
    ctx.parameter(
        "avg_txn_records_per_append",
        format!("{:.2}", breakdown.avg_txn_records_per_append),
    );
    ctx.parameter(
        "avg_wal_append_us",
        format!("{:.2}", breakdown.avg_wal_append_us),
    );
    ctx.parameter("dominant_phase", breakdown.dominant_phase());
}

fn tag_transaction_coalescing_signal(ctx: &mut StressContext, signal: TransactionCoalescingSignal) {
    ctx.parameter("logical_txn_records", signal.logical_txn_records);
    ctx.parameter("physical_wal_appends", signal.physical_wal_appends);
    ctx.parameter(
        "avg_txn_records_per_append",
        format!("{:.2}", signal.avg_txn_records_per_append),
    );
    ctx.parameter(
        "avg_wal_append_us",
        format!("{:.2}", signal.avg_wal_append_us),
    );
}

fn run_latency_breakdown_case(
    ctx: &mut StressContext,
    kind: LatencyWorkloadKind,
    clients: usize,
    txns_per_client: usize,
    cf_name: &'static str,
    key_prefix: &'static str,
) {
    init_benchmark_telemetry().expect("initialize benchmark telemetry");
    let (engine, cf_id) = open_local_engine_with_cf(cf_name);
    let workloads = make_latency_client_workloads(clients, txns_per_client, key_prefix);
    ctx.parameter("workload", kind.label());
    ctx.parameter("clients", clients);
    ctx.parameter("txns_per_client", txns_per_client);
    ctx.parameter("sample_repeats", LATENCY_SAMPLE_REPEATS);
    ctx.parameter("logical_unit", "transaction");

    let mut observed = None;
    let logical_ops = (clients * txns_per_client * LATENCY_SAMPLE_REPEATS) as u64;
    let measurement_name = match kind {
        LatencyWorkloadKind::SequentialBufferedSingleOp => "sequential_buffered_single_op",
        LatencyWorkloadKind::ConcurrentBufferedSingleOp => "concurrent_buffered_single_op",
        LatencyWorkloadKind::ReadOnlyBeginTx => "read_only_begin_tx",
    };
    let _completed = ctx.measure_batch(measurement_name, logical_ops, || {
        let mut completed = 0u64;
        for _ in 0..LATENCY_SAMPLE_REPEATS {
            let breakdown =
                run_buffered_transaction_latency_breakdown(&engine, cf_id, kind, workloads.clone());
            assert_transaction_latency_guardrails(breakdown);
            completed = completed.saturating_add(breakdown.transactions);
            observed = Some(breakdown);
        }
        assert_eq!(completed, logical_ops);
        black_box(completed);
    });

    tag_transaction_latency_breakdown(ctx, observed.expect("latency breakdown recorded"));
}

#[stress(
    tier = 2,
    metadata(
        component = "transaction_latency",
        scenario = "sequential_buffered_single_op"
    )
)]
fn sequential_buffered_single_op(ctx: &mut StressContext) {
    run_latency_breakdown_case(
        ctx,
        LatencyWorkloadKind::SequentialBufferedSingleOp,
        1,
        LATENCY_SEQUENTIAL_TXNS,
        "latency_seq",
        "latency_seq",
    );
}

#[stress(
    tier = 2,
    metadata(
        component = "transaction_latency",
        scenario = "concurrent_buffered_single_op"
    )
)]
fn concurrent_buffered_single_op(ctx: &mut StressContext) {
    run_latency_breakdown_case(
        ctx,
        LatencyWorkloadKind::ConcurrentBufferedSingleOp,
        COALESCING_CLIENTS,
        COALESCING_TXNS_PER_CLIENT,
        "latency_concurrent",
        "latency_concurrent",
    );
}

#[stress(
    tier = 2,
    metadata(component = "transaction_latency", scenario = "read_only_begin_tx")
)]
fn read_only_begin_tx(ctx: &mut StressContext) {
    init_benchmark_telemetry().expect("initialize benchmark telemetry");
    let (engine, cf_id) = open_local_engine_with_cf("latency_read_only");
    ctx.parameter("workload", LatencyWorkloadKind::ReadOnlyBeginTx.label());
    ctx.parameter("transactions", LATENCY_READ_ONLY_TXNS);
    ctx.parameter("sample_repeats", READ_ONLY_BEGIN_TX_SAMPLE_REPEATS);
    ctx.parameter("logical_unit", "transaction");

    let mut observed = LatencyClientTotals::default();
    let logical_ops = (LATENCY_READ_ONLY_TXNS * READ_ONLY_BEGIN_TX_SAMPLE_REPEATS) as u64;
    let _completed = ctx
        .benchmark("read_only_begin_tx")
        .samples(READ_ONLY_BEGIN_TX_SAMPLE_COUNT)
        .warmup(READ_ONLY_BEGIN_TX_WARMUP_SAMPLES)
        .measure_batch(logical_ops, || {
            let mut completed = 0u64;
            let mut begin_tx_ns = 0u64;
            for _ in 0..READ_ONLY_BEGIN_TX_SAMPLE_REPEATS {
                let totals = run_read_only_begin_tx_sample(&engine, cf_id);
                completed = completed.saturating_add(totals.transactions);
                begin_tx_ns = begin_tx_ns.saturating_add(totals.begin_tx_ns);
            }
            observed.transactions = completed;
            observed.begin_tx_ns = begin_tx_ns;
            assert_eq!(completed, logical_ops);
            black_box(completed);
        });

    let breakdown = TransactionLatencyBreakdown {
        kind: LatencyWorkloadKind::ReadOnlyBeginTx,
        clients: 1,
        transactions: observed.transactions,
        logical_txn_records: 0,
        begin_tx_us: ns_to_avg_us(observed.begin_tx_ns, observed.transactions),
        put_us: 0.0,
        commit_total_us: 0.0,
        commit_samples: 0,
        commit_p50_us: 0,
        commit_p95_us: 0,
        commit_p99_us: 0,
        commit_max_us: 0,
        submit_apply_transaction_us: 0.0,
        write_group_leader_collect_us: 0.0,
        write_group_runtime_apply_us: 0.0,
        write_group_follower_wait_us: 0.0,
        submit_apply_other_us: 0.0,
        durability_finalize_us: 0.0,
        unregister_snapshot_us: 0.0,
        runtime_submit_ack_non_wal_us: 0.0,
        physical_wal_appends: 0,
        avg_wal_append_us: 0.0,
        avg_txn_records_per_append: 0.0,
    };
    assert_transaction_latency_guardrails(breakdown);
    tag_transaction_latency_breakdown(ctx, breakdown);
}

#[stress(
    tier = 2,
    metadata(component = "transaction_latency", scenario = "coalescing_signal")
)]
fn coalescing_signal(ctx: &mut StressContext) {
    init_benchmark_telemetry().expect("initialize benchmark telemetry");
    let (engine, cf_id) = open_local_engine_with_cf("coalescing");
    ctx.parameter("clients", COALESCING_CLIENTS);
    ctx.parameter("txns_per_client", COALESCING_TXNS_PER_CLIENT);
    ctx.parameter("sample_repeats", LATENCY_SAMPLE_REPEATS);
    ctx.parameter("logical_unit", "transaction_record");

    let mut observed = None;
    let logical_ops =
        logical_coalescing_txn_records().saturating_mul(LATENCY_SAMPLE_REPEATS as u64);
    let _completed = ctx.measure_batch("coalescing_signal", logical_ops, || {
        let mut completed = 0u64;
        for _ in 0..LATENCY_SAMPLE_REPEATS {
            let signal = run_transaction_coalescing_signal(&engine, cf_id);
            assert_transaction_coalescing_guardrail(signal);
            black_box((
                signal.physical_wal_appends,
                signal.avg_txn_records_per_append,
                signal.avg_wal_append_us,
            ));
            completed = completed.saturating_add(signal.logical_txn_records);
            observed = Some(signal);
        }
        assert_eq!(completed, logical_ops);
        black_box(completed);
    });

    tag_transaction_coalescing_signal(ctx, observed.expect("coalescing signal recorded"));
}

stress_main!();
