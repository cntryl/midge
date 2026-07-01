//! Tier 2 â€” Local Durability Throughput Regression Guard
//!
//! Measures: batched write throughput for memory, local, cloud, and hybrid modes
//! Purpose: Catch unintended local throughput collapse (regression guard)
//!
//! This benchmark ensures that local mode with batched durability doesn't
//! drop below 50% of memory throughput for the same workload.
//!
//! If this fails, it indicates a regression in local WAL batching,
//! memtable configuration, or durability path performance.

#[path = "./criterion_config.rs"]
mod criterion_config;

use std::time::{Duration, Instant};

use cntryl_midge::testkit::{
    bench::{
        init_benchmark_telemetry, TransactionCommitTimingGuard, TransactionCommitTimingSample,
    },
    opts_for_mode,
};
use cntryl_midge::{ColumnFamilyId, Engine, RuntimeMetricsSnapshot, TransactionMode, WriteOptions};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_config::criterion_config_for_tier2;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

const NUM_OPS_PER_BATCH: usize = 100;
const VALUE_SIZE: usize = 128;
const BATCH_ITERATIONS: usize = 100;
const COALESCING_CLIENTS: usize = 8;
const COALESCING_TXNS_PER_CLIENT: usize = 32;
const COALESCING_VALUE_SIZE: usize = 64;
const LATENCY_SEQUENTIAL_TXNS: usize = 256;
const LATENCY_READ_ONLY_TXNS: usize = 256;
static COALESCING_SIGNAL_REPORTED: AtomicBool = AtomicBool::new(false);
static LATENCY_BREAKDOWN_REPORTED_MASK: AtomicU64 = AtomicU64::new(0);

type KeyValueBatch = Vec<(Vec<u8>, Vec<u8>)>;
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

    fn report_mask(self) -> u64 {
        match self {
            Self::SequentialBufferedSingleOp => 1,
            Self::ConcurrentBufferedSingleOp => 2,
            Self::ReadOnlyBeginTx => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct LatencyClientTotals {
    transactions: u64,
    begin_tx_ns: u64,
    put_ns: u64,
}

impl LatencyClientTotals {
    fn record_begin_tx(&mut self, duration: Duration) {
        self.begin_tx_ns = self.begin_tx_ns.saturating_add(duration_to_ns(duration));
    }

    fn record_put(&mut self, duration: Duration) {
        self.put_ns = self.put_ns.saturating_add(duration_to_ns(duration));
    }

    fn record_transaction(&mut self) {
        self.transactions = self.transactions.saturating_add(1);
    }

    fn merge(&mut self, other: Self) {
        self.transactions = self.transactions.saturating_add(other.transactions);
        self.begin_tx_ns = self.begin_tx_ns.saturating_add(other.begin_tx_ns);
        self.put_ns = self.put_ns.saturating_add(other.put_ns);
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
}

impl CommitTimingTotals {
    fn from_samples(samples: &[TransactionCommitTimingSample]) -> Self {
        let mut totals = Self {
            samples: u64::try_from(samples.len()).expect("sample count fits in u64"),
            ..Self::default()
        };

        for sample in samples {
            if sample.succeeded {
                totals.succeeded = totals.succeeded.saturating_add(1);
            }
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
        }

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

fn make_key_value_batch() -> KeyValueBatch {
    (0..NUM_OPS_PER_BATCH)
        .map(|i| {
            let key = format!("key_{i:016}").into_bytes();
            let value_byte = u8::try_from(i % 251).expect("value byte fits in u8");
            let value = vec![value_byte; VALUE_SIZE];
            (key, value)
        })
        .collect()
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

fn logical_coalescing_txn_records() -> u64 {
    u64::try_from(COALESCING_CLIENTS * COALESCING_TXNS_PER_CLIENT)
        .expect("logical transaction count fits in u64")
}

fn latency_workload_records(clients: usize, txns_per_client: usize) -> u64 {
    u64::try_from(clients * txns_per_client).expect("transaction count fits in u64")
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
    let barrier = Arc::new(Barrier::new(COALESCING_CLIENTS));
    let mut handles = Vec::with_capacity(COALESCING_CLIENTS);

    for client_id in 0..COALESCING_CLIENTS {
        let engine_clone = Arc::clone(engine);
        let barrier_clone = Arc::clone(&barrier);

        handles.push(std::thread::spawn(move || {
            barrier_clone.wait();
            for txn_id in 0..COALESCING_TXNS_PER_CLIENT {
                let mut tx = engine_clone
                    .begin_tx(cf_id, TransactionMode::ReadWrite)
                    .expect("begin transaction");
                let key = format!("coalesce_{client_id:02}_{txn_id:04}").into_bytes();
                let value_byte =
                    u8::try_from((client_id + txn_id) % 251).expect("value byte fits in u8");
                tx.put(key, vec![value_byte; COALESCING_VALUE_SIZE], None)
                    .expect("put coalescing value");
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

        tx.commit(write_opts).expect("commit latency transaction");
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
    let timing_guard = TransactionCommitTimingGuard::start();

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

    let commit_samples = timing_guard.drain();
    drop(timing_guard);
    let end = engine
        .get_runtime_metrics()
        .expect("get ending latency runtime metrics");
    let commit_totals = CommitTimingTotals::from_samples(&commit_samples);
    assert_eq!(commit_totals.samples, logical_txn_records);
    assert_eq!(commit_totals.succeeded, commit_totals.samples);

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

fn run_read_only_begin_tx_latency_baseline(
    engine: &Engine,
    cf_id: ColumnFamilyId,
) -> TransactionLatencyBreakdown {
    let start = engine
        .get_runtime_metrics()
        .expect("get starting read-only latency runtime metrics");
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

    let end = engine
        .get_runtime_metrics()
        .expect("get ending read-only latency runtime metrics");

    latency_breakdown_from_totals(
        LatencyWorkloadKind::ReadOnlyBeginTx,
        1,
        0,
        client_totals,
        CommitTimingTotals::default(),
        &start,
        &end,
    )
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

fn report_transaction_coalescing_signal(signal: TransactionCoalescingSignal) {
    if COALESCING_SIGNAL_REPORTED.swap(true, Ordering::Relaxed) {
        return;
    }

    eprintln!(
        "transaction_coalescing_signal logical_txn_records={} physical_wal_appends={} avg_txn_records_per_append={:.2} avg_wal_append_us={:.2}",
        signal.logical_txn_records,
        signal.physical_wal_appends,
        signal.avg_txn_records_per_append,
        signal.avg_wal_append_us
    );
}

fn report_transaction_latency_breakdown(breakdown: TransactionLatencyBreakdown) {
    let report_mask = breakdown.kind.report_mask();
    if LATENCY_BREAKDOWN_REPORTED_MASK.fetch_or(report_mask, Ordering::Relaxed) & report_mask != 0 {
        return;
    }

    eprintln!(
        "transaction_latency_breakdown workload={} clients={} transactions={} begin_tx_us={:.2} put_us={:.2} commit_total_us={:.2} submit_apply_transaction_us={:.2} write_group_leader_collect_us={:.2} write_group_runtime_apply_us={:.2} write_group_follower_wait_us={:.2} submit_apply_other_us={:.2} durability_finalize_us={:.2} unregister_snapshot_us={:.2} runtime_submit_ack_non_wal_us={:.2} logical_txn_records={} physical_wal_appends={} avg_txn_records_per_append={:.2} avg_wal_append_us={:.2} dominant_phase={}",
        breakdown.kind.label(),
        breakdown.clients,
        breakdown.transactions,
        breakdown.begin_tx_us,
        breakdown.put_us,
        breakdown.commit_total_us,
        breakdown.submit_apply_transaction_us,
        breakdown.write_group_leader_collect_us,
        breakdown.write_group_runtime_apply_us,
        breakdown.write_group_follower_wait_us,
        breakdown.submit_apply_other_us,
        breakdown.durability_finalize_us,
        breakdown.unregister_snapshot_us,
        breakdown.runtime_submit_ack_non_wal_us,
        breakdown.logical_txn_records,
        breakdown.physical_wal_appends,
        breakdown.avg_txn_records_per_append,
        breakdown.avg_wal_append_us,
        breakdown.dominant_phase()
    );
}

fn open_local_engine_with_cf(cf_name: &str) -> (Arc<Engine>, ColumnFamilyId) {
    let opts = opts_for_mode("local");
    let engine = Arc::new(Engine::open_with_options(&opts).expect("open local engine"));
    let cf = engine
        .create_column_family(cf_name)
        .expect("create benchmark column family");
    (engine, cf.id())
}

fn benchmark_batched_writes(c: &mut Criterion) {
    init_benchmark_telemetry().expect("initialize benchmark telemetry");

    let mut group = c.benchmark_group("tier2_local_throughput_regression");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    // Measure all benchmark storage profiles plus memory for a fast ceiling.
    for mode in &["memory", "local", "cloud", "hybrid"] {
        let opts = opts_for_mode(mode);

        group.throughput(Throughput::Bytes(
            (NUM_OPS_PER_BATCH * VALUE_SIZE * BATCH_ITERATIONS) as u64,
        ));

        group.bench_with_input(BenchmarkId::from_parameter(mode), mode, |b, _mode| {
            b.iter_batched(
                || {
                    // Setup: create engine and column family
                    let engine = Engine::open_with_options(&opts).expect("failed to open engine");
                    let cf = engine
                        .create_column_family("test")
                        .expect("failed to create column family");
                    let keys_vals = make_key_value_batch();
                    (engine, cf, keys_vals)
                },
                |(engine, cf, keys_vals)| {
                    let cf_id = cf.id();

                    // Run batches
                    for _ in 0..BATCH_ITERATIONS {
                        let mut tx = engine
                            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                            .expect("begin");

                        for (k, v) in &keys_vals {
                            tx.put(k.clone(), v.clone(), None).expect("put");
                        }

                        tx.commit(WriteOptions::buffered()).expect("commit");
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn benchmark_transaction_latency_breakdown(c: &mut Criterion) {
    init_benchmark_telemetry().expect("initialize benchmark telemetry");

    let mut group = c.benchmark_group("tier2_transaction_latency_breakdown");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));

    group.throughput(Throughput::Elements(latency_workload_records(
        1,
        LATENCY_SEQUENTIAL_TXNS,
    )));
    group.bench_function(
        LatencyWorkloadKind::SequentialBufferedSingleOp.label(),
        |b| {
            b.iter_batched(
                || {
                    let (engine, cf_id) = open_local_engine_with_cf("latency_seq");
                    let workloads =
                        make_latency_client_workloads(1, LATENCY_SEQUENTIAL_TXNS, "latency_seq");
                    (engine, cf_id, workloads)
                },
                |(engine, cf_id, workloads)| {
                    let breakdown = run_buffered_transaction_latency_breakdown(
                        &engine,
                        cf_id,
                        LatencyWorkloadKind::SequentialBufferedSingleOp,
                        workloads,
                    );
                    report_transaction_latency_breakdown(breakdown);
                    black_box(breakdown);
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.throughput(Throughput::Elements(logical_coalescing_txn_records()));
    group.bench_function(
        LatencyWorkloadKind::ConcurrentBufferedSingleOp.label(),
        |b| {
            b.iter_batched(
                || {
                    let (engine, cf_id) = open_local_engine_with_cf("latency_concurrent");
                    let workloads = make_latency_client_workloads(
                        COALESCING_CLIENTS,
                        COALESCING_TXNS_PER_CLIENT,
                        "latency_concurrent",
                    );
                    (engine, cf_id, workloads)
                },
                |(engine, cf_id, workloads)| {
                    let breakdown = run_buffered_transaction_latency_breakdown(
                        &engine,
                        cf_id,
                        LatencyWorkloadKind::ConcurrentBufferedSingleOp,
                        workloads,
                    );
                    report_transaction_latency_breakdown(breakdown);
                    black_box(breakdown);
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.throughput(Throughput::Elements(
        u64::try_from(LATENCY_READ_ONLY_TXNS).expect("read-only transaction count fits in u64"),
    ));
    group.bench_function(LatencyWorkloadKind::ReadOnlyBeginTx.label(), |b| {
        b.iter_batched(
            || open_local_engine_with_cf("latency_read_only"),
            |(engine, cf_id)| {
                let breakdown = run_read_only_begin_tx_latency_baseline(&engine, cf_id);
                report_transaction_latency_breakdown(breakdown);
                black_box(breakdown);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn benchmark_transaction_coalescing_signal(c: &mut Criterion) {
    init_benchmark_telemetry().expect("initialize benchmark telemetry");

    let mut group = c.benchmark_group("tier2_transaction_coalescing_signal");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(logical_coalescing_txn_records()));

    group.bench_function("buffered_concurrent_single_op_transactions", |b| {
        b.iter_batched(
            || {
                let opts = opts_for_mode("local");
                let engine = Arc::new(Engine::open_with_options(&opts).expect("open engine"));
                let cf = engine
                    .create_column_family("coalescing")
                    .expect("create column family");
                (engine, cf.id())
            },
            |(engine, cf_id)| {
                let signal = run_transaction_coalescing_signal(&engine, cf_id);
                assert_eq!(signal.logical_txn_records, logical_coalescing_txn_records());
                report_transaction_coalescing_signal(signal);
                black_box((
                    signal.physical_wal_appends,
                    signal.avg_txn_records_per_append,
                    signal.avg_wal_append_us,
                ));
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn verify_local_throughput_minimum(c: &mut Criterion) {
    let mut group = c.benchmark_group("tier2_local_throughput_threshold");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for (name, opts) in [
        ("memory_baseline", opts_for_mode("memory")),
        ("local_throughput", opts_for_mode("local")),
        ("cloud_throughput", opts_for_mode("cloud")),
        ("hybrid_throughput", opts_for_mode("hybrid")),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let engine = Engine::open_with_options(&opts).unwrap();
                    let cf = engine.create_column_family("test").unwrap();
                    let keys_vals = make_key_value_batch();
                    (engine, cf, keys_vals)
                },
                |(engine, cf, keys_vals)| {
                    let cf_id = cf.id();

                    for _ in 0..BATCH_ITERATIONS {
                        let mut tx = engine
                            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                            .expect("begin");
                        for (k, v) in &keys_vals {
                            tx.put(k.clone(), v.clone(), None).expect("put");
                        }
                        tx.commit(WriteOptions::buffered()).expect("commit");
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config_for_tier2();
    targets = benchmark_batched_writes, benchmark_transaction_latency_breakdown, benchmark_transaction_coalescing_signal, verify_local_throughput_minimum
}
criterion_main!(benches);
