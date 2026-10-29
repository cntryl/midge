// Differential driver for the public engine API (#951): a byte string decodes
// into a bounded history of multi-key transactions, reads, held snapshots,
// clock advances, flushes, compactions, reopens and power-loss crashes; every
// read is checked against a `BTreeMap` model evaluated at the read's snapshot.
// Included by `tests/engine_differential.rs` (proptest) and by the libFuzzer
// target `fuzz/fuzz_targets/engine_differential.rs`, so this file carries no
// inner doc comment.

use bytes::Bytes;
use cntryl_midge::__internal::io::Fs;
use cntryl_midge::__internal::simulation::{CrashDevice, SimulatedClock};
use cntryl_midge::{
    Clock, Engine, MidgeError, OpenOptions, Query, Transaction, TransactionMode, WriteOptions,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Steps decoded from one input at most, so a history stays bounded.
pub const MAX_STEPS: usize = 96;
/// Distinct keys: 24 two-byte keys under three prefixes plus 8 three-byte
/// children, so prefix scans and range deletes have structure to hit.
pub const KEY_SPACE: u8 = 32;
/// Snapshots held at once; opening another replaces the oldest.
const SNAPSHOT_SLOTS: usize = 2;
/// The lease clock is virtual, so a long TTL costs nothing and keeps a slow
/// history from losing its lease; a crash advances the clock past it.
const LEASE_TTL: Duration = Duration::from_secs(60);

/// A TTL clock that moves only when the history says so, so the model and the
/// engine compute the same expiry for every write and the same reading for
/// every read.
#[derive(Debug)]
pub struct FrozenClock {
    millis: AtomicU64,
}

impl FrozenClock {
    /// Starts well after the epoch so every expiry lies in the future.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            millis: AtomicU64::new(1_000_000),
        })
    }

    pub fn advance(&self, by: Duration) {
        self.millis.fetch_add(
            u64::try_from(by.as_millis()).expect("millis fit"),
            Ordering::SeqCst,
        );
    }
}

impl Clock for FrozenClock {
    fn now_millis(&self) -> u64 {
        self.millis.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    },
    Delete {
        key: Vec<u8>,
    },
    DeleteRange {
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanSpec {
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub prefix: Option<Vec<u8>>,
    pub limit: Option<usize>,
    pub reverse: bool,
}

impl ScanSpec {
    pub const EVERYTHING: Self = Self {
        start: None,
        end: None,
        prefix: None,
        limit: None,
        reverse: false,
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// One commit of several operations, applied in order. A synced commit
    /// survives a power loss; a buffered one may be lost with every later
    /// commit, but never torn and never without its successors.
    Commit {
        ops: Vec<WriteOp>,
        sync: bool,
    },
    Get(Vec<u8>),
    Scan(ScanSpec),
    /// Open a read-only transaction and keep it in a slot.
    Snapshot,
    SnapshotGet {
        slot: usize,
        key: Vec<u8>,
    },
    SnapshotScan {
        slot: usize,
        spec: ScanSpec,
    },
    AdvanceClock(Duration),
    Flush,
    Compact,
    Reopen,
    /// Process crash, then power loss with the given seed, then reopen.
    Crash(u64),
}

/// The key for a byte of input.
pub fn key(index: u8) -> Vec<u8> {
    let index = index % KEY_SPACE;
    let prefix = b"abc"[usize::from(index / 8) % 3];
    let digit = b'0' + index % 8;
    if index < 24 {
        vec![prefix, digit]
    } else {
        vec![prefix, digit, b'x']
    }
}

struct Input<'a> {
    data: &'a [u8],
    position: usize,
}

impl Input<'_> {
    fn exhausted(&self) -> bool {
        self.position >= self.data.len()
    }

    fn byte(&mut self) -> u8 {
        let byte = self.data.get(self.position).copied().unwrap_or(0);
        self.position += 1;
        byte
    }

    fn maybe_key(&mut self) -> Option<Vec<u8>> {
        let byte = self.byte();
        (!byte.is_multiple_of(4)).then(|| key(byte / 4))
    }

    fn scan(&mut self) -> ScanSpec {
        let mut start = self.maybe_key();
        let mut end = self.maybe_key();
        // The engine rejects a start above its end (a separate test pins that
        // contract); the model has nothing to say about rejected queries.
        if let (Some(first), Some(second)) = (&start, &end) {
            if first > second {
                std::mem::swap(&mut start, &mut end);
            }
        }
        let prefix = {
            let byte = self.byte();
            match byte % 4 {
                0 => None,
                1 => Some(vec![b"abc"[usize::from(byte / 4) % 3]]),
                _ => Some(key(byte / 4)[..2].to_vec()),
            }
        };
        let limit = {
            let byte = self.byte();
            (!byte.is_multiple_of(3)).then(|| 1 + usize::from(byte / 3) % 6)
        };
        let reverse = self.byte() % 2 == 1;
        ScanSpec {
            start,
            end,
            prefix,
            limit,
            reverse,
        }
    }

    /// Operations of one commit. Every operation in a transaction gets its
    /// own sequence in write order, so repeated keys and range deletes mixed
    /// with point writes are applied in order, as the model does.
    fn commit(&mut self, index: usize) -> Vec<WriteOp> {
        let count = 1 + usize::from(self.byte() % 4);
        (0..count)
            .map(|op_index| {
                let kind = self.byte();
                let subject = key(self.byte());
                match kind % 8 {
                    5 => WriteOp::Delete { key: subject },
                    6 => {
                        let other = key(self.byte());
                        let (start, end) = match subject.cmp(&other) {
                            std::cmp::Ordering::Less => (subject, other),
                            std::cmp::Ordering::Greater => (other, subject),
                            std::cmp::Ordering::Equal => {
                                let mut end = subject.clone();
                                end.push(0);
                                (subject, end)
                            }
                        };
                        WriteOp::DeleteRange { start, end }
                    }
                    4 => WriteOp::Put {
                        key: subject,
                        value: value(index, op_index),
                        ttl_seconds: Some(1 + u64::from(self.byte() % 3)),
                    },
                    _ => WriteOp::Put {
                        key: subject,
                        value: value(index, op_index),
                        ttl_seconds: None,
                    },
                }
            })
            .collect()
    }
}

fn value(commit: usize, op: usize) -> Vec<u8> {
    vec![
        b'v',
        u8::try_from(commit % 256).expect("byte"),
        u8::try_from(op).expect("byte"),
    ]
}

/// Decode a history from `data`. Five of fourteen steps are commits and one
/// is a crash, so a history writes more than it recovers.
pub fn decode(data: &[u8]) -> Vec<Step> {
    let mut input = Input { data, position: 0 };
    let mut steps = Vec::new();
    let mut commits = 0;
    while !input.exhausted() && steps.len() < MAX_STEPS {
        let step = match input.byte() % 14 {
            0..=4 => {
                commits += 1;
                let sync = !input.byte().is_multiple_of(3);
                Step::Commit {
                    ops: input.commit(commits),
                    sync,
                }
            }
            5 => Step::Get(key(input.byte())),
            6 => Step::Scan(input.scan()),
            7 => Step::Snapshot,
            8 => Step::SnapshotGet {
                slot: usize::from(input.byte()) % SNAPSHOT_SLOTS,
                key: key(input.byte()),
            },
            9 => Step::SnapshotScan {
                slot: usize::from(input.byte()) % SNAPSHOT_SLOTS,
                spec: input.scan(),
            },
            10 => Step::AdvanceClock(Duration::from_millis(
                500 * (1 + u64::from(input.byte() % 6)),
            )),
            11 => Step::Flush,
            12 => {
                if input.byte().is_multiple_of(2) {
                    Step::Compact
                } else {
                    Step::Reopen
                }
            }
            _ => Step::Crash(u64::from(input.byte())),
        };
        steps.push(step);
    }
    steps
}

/// The reference: every commit with the TTL clock reading it was applied at,
/// and how many of them are known to survive a power loss.
#[derive(Default)]
pub struct Model {
    commits: Vec<(u64, Vec<WriteOp>)>,
    durable: usize,
}

impl Model {
    pub fn commit(&mut self, clock_millis: u64, ops: Vec<WriteOp>, sync: bool) {
        self.commits.push((clock_millis, ops));
        if sync {
            self.durable = self.commits.len();
        }
    }

    pub fn commits(&self) -> usize {
        self.commits.len()
    }

    /// Every commit so far is durable: a flush published them or a graceful
    /// shutdown synced them.
    pub fn mark_durable(&mut self) {
        self.durable = self.commits.len();
    }

    /// The visible state after the first `commits` commits for a transaction
    /// begun when the TTL clock said `now_millis`: the engine evaluates expiry
    /// at the transaction's begin.
    pub fn state_at(&self, commits: usize, now_millis: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut state: BTreeMap<Vec<u8>, (Vec<u8>, Option<u64>)> = BTreeMap::new();
        for (clock, ops) in &self.commits[..commits] {
            for op in ops {
                match op {
                    WriteOp::Put {
                        key,
                        value,
                        ttl_seconds,
                    } => {
                        let expires_at = ttl_seconds
                            .filter(|ttl| *ttl > 0)
                            .map(|ttl| clock.saturating_add(ttl.saturating_mul(1_000)));
                        state.insert(key.clone(), (value.clone(), expires_at));
                    }
                    WriteOp::Delete { key } => {
                        state.remove(key);
                    }
                    WriteOp::DeleteRange { start, end } => {
                        state.retain(|key, _| !(key >= start && key < end));
                    }
                }
            }
        }
        state
            .into_iter()
            .filter(|(_, (_, expires_at))| !expires_at.is_some_and(|at| at <= now_millis))
            .map(|(key, (value, _))| (key, value))
            .collect()
    }

    /// What a scan returns over `state`: the keys inside `[start, end)` that
    /// carry the prefix, in the requested direction, cut to the limit. This
    /// is the contract, not the engine's bound arithmetic.
    pub fn scan(state: &BTreeMap<Vec<u8>, Vec<u8>>, spec: &ScanSpec) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = state
            .iter()
            .filter(|(key, _)| spec.start.as_ref().is_none_or(|start| *key >= start))
            .filter(|(key, _)| spec.end.as_ref().is_none_or(|end| *key < end))
            .filter(|(key, _)| {
                spec.prefix
                    .as_ref()
                    .is_none_or(|prefix| key.starts_with(prefix))
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if spec.reverse {
            rows.reverse();
        }
        if let Some(limit) = spec.limit {
            rows.truncate(limit);
        }
        rows
    }

    /// After a power loss the engine must hold the state after some commit
    /// prefix no shorter than the durable one: no acknowledged synced commit
    /// lost, no buffered commit torn, no later commit kept without its
    /// predecessors. Returns the prefix length the recovered state matched
    /// and forgets the commits after it.
    pub fn reconcile_after_crash(
        &mut self,
        recovered: &[(Vec<u8>, Vec<u8>)],
        now_millis: u64,
    ) -> Result<usize, String> {
        let matched = (self.durable..=self.commits.len()).rev().find(|&commits| {
            Model::scan(&self.state_at(commits, now_millis), &ScanSpec::EVERYTHING) == recovered
        });
        match matched {
            Some(commits) => {
                self.commits.truncate(commits);
                self.durable = commits;
                Ok(commits)
            }
            None => Err(format!(
                "recovered state matches no commit prefix in {}..={} (durable..=all): {recovered:?}",
                self.durable,
                self.commits.len()
            )),
        }
    }
}

fn query(spec: &ScanSpec) -> Query {
    let mut query = Query::new();
    if let Some(start) = &spec.start {
        query = query.start_key(Bytes::copy_from_slice(start));
    }
    if let Some(end) = &spec.end {
        query = query.end_key(Bytes::copy_from_slice(end));
    }
    if let Some(prefix) = &spec.prefix {
        query = query.prefix(Bytes::copy_from_slice(prefix));
    }
    if let Some(limit) = spec.limit {
        query = query.limit(limit);
    }
    if spec.reverse {
        query = query.reverse();
    }
    query
}

/// Every row `tx` can see in the requested order.
pub fn rows(tx: &Transaction, spec: &ScanSpec) -> Vec<(Vec<u8>, Vec<u8>)> {
    tx.scan(&query(spec))
        .expect("scan")
        .try_collect()
        .expect("scan rows")
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect()
}

/// One simulated process over a crash device: the engine, the model, the two
/// clocks and the held snapshots. Snapshots are declared before the engine so
/// they drop first when a finding panics.
pub struct Harness {
    directory: tempfile::TempDir,
    device: CrashDevice,
    lease_clock: SimulatedClock,
    ttl_clock: Arc<FrozenClock>,
    /// Held read-only transactions with the commit count and the TTL clock
    /// reading at their start: expiry is evaluated at the transaction's begin,
    /// so a snapshot keeps seeing values that expire later.
    snapshots: Vec<Option<(Transaction, usize, u64)>>,
    next_slot: usize,
    engine: Option<Engine>,
    cf_id: u32,
    model: Model,
}

impl Harness {
    pub fn open() -> Self {
        let directory = tempfile::tempdir().expect("database directory");
        let device = CrashDevice::new();
        let lease_clock = SimulatedClock::new();
        let ttl_clock = FrozenClock::new();
        let mut harness = Self {
            directory,
            device,
            lease_clock,
            ttl_clock,
            snapshots: (0..SNAPSHOT_SLOTS).map(|_| None).collect(),
            next_slot: 0,
            engine: None,
            cf_id: 0,
            model: Model::default(),
        };
        harness.start();
        harness
    }

    fn options(&self) -> OpenOptions {
        let root: Arc<dyn Fs> = Arc::new(self.device.fs());
        OpenOptions::local(self.directory.path())
            .background_compaction(false)
            .lease_ttl(LEASE_TTL)
            .lease_clock_skew_tolerance(Duration::ZERO)
            .lease_clock_for_testing(Arc::new(self.lease_clock.clone()))
            .ttl_clock(Arc::clone(&self.ttl_clock) as Arc<dyn Clock>)
            .filesystem_for_testing(root)
            .build()
            .expect("crash-device options")
    }

    fn start(&mut self) {
        let engine = Engine::open(self.options()).expect("open engine");
        self.cf_id = engine
            .get_column_family("default")
            .expect("default family")
            .id();
        self.engine = Some(engine);
    }

    fn engine(&self) -> &Engine {
        self.engine.as_ref().expect("engine is open")
    }

    fn now_millis(&self) -> u64 {
        self.ttl_clock.now_millis()
    }

    fn read_tx(&self) -> Transaction {
        self.engine()
            .begin_tx(self.cf_id, TransactionMode::ReadOnly)
            .expect("read transaction")
    }

    fn drop_snapshots(&mut self) {
        for slot in &mut self.snapshots {
            *slot = None;
        }
    }

    fn check_get(tx: &Transaction, key: &[u8], expected: Option<&Vec<u8>>, context: &str) {
        let actual = tx.get(key).expect("get");
        assert_eq!(
            actual.as_deref(),
            expected.map(Vec::as_slice),
            "{context}: get {key:?}"
        );
    }

    fn check_scan(
        tx: &Transaction,
        spec: &ScanSpec,
        state: &BTreeMap<Vec<u8>, Vec<u8>>,
        context: &str,
    ) {
        assert_eq!(
            rows(tx, spec),
            Model::scan(state, spec),
            "{context}: scan {spec:?}"
        );
    }

    fn commit(&mut self, ops: &[WriteOp], sync: bool) {
        let mut tx = self
            .engine()
            .begin_tx(self.cf_id, TransactionMode::ReadWrite)
            .expect("write transaction");
        for op in ops {
            match op {
                WriteOp::Put {
                    key,
                    value,
                    ttl_seconds,
                } => tx
                    .put(key.clone(), value.clone(), *ttl_seconds)
                    .expect("put"),
                WriteOp::Delete { key } => tx.delete(key.clone()).expect("delete"),
                WriteOp::DeleteRange { start, end } => tx
                    .delete_range(start.clone(), end.clone())
                    .expect("delete_range"),
            }
        }
        let clock = self.now_millis();
        let options = if sync {
            WriteOptions::sync()
        } else {
            WriteOptions::buffered()
        };
        match tx.commit(options) {
            Ok(()) => self.model.commit(clock, ops.to_vec(), sync),
            Err(MidgeError::WriteStall(_)) => {
                // Local write stalls reject admission before the transaction is applied.
                // Compare immediately so the model also detects an accidental partial write.
                self.check_everything("after rejected write stall");
            }
            Err(error) => panic!("commit: {error}"),
        }
    }

    /// Everything the engine holds equals the model after every commit.
    fn check_everything(&self, context: &str) {
        let state = self.model.state_at(self.model.commits(), self.now_millis());
        Self::check_scan(&self.read_tx(), &ScanSpec::EVERYTHING, &state, context);
    }

    /// Apply one step to the engine and the model, checking any read.
    pub fn step(&mut self, step: &Step) {
        match step {
            Step::Commit { ops, sync } => self.commit(ops, *sync),
            Step::Get(key) => {
                let state = self.model.state_at(self.model.commits(), self.now_millis());
                Self::check_get(&self.read_tx(), key, state.get(key), "live");
            }
            Step::Scan(spec) => {
                let state = self.model.state_at(self.model.commits(), self.now_millis());
                Self::check_scan(&self.read_tx(), spec, &state, "live");
            }
            Step::Snapshot => {
                let tx = self.read_tx();
                let slot = self.next_slot;
                self.next_slot = (slot + 1) % SNAPSHOT_SLOTS;
                self.snapshots[slot] = Some((tx, self.model.commits(), self.now_millis()));
            }
            Step::SnapshotGet { slot, key } => {
                if let Some((tx, commits, began_at)) = &self.snapshots[*slot] {
                    let state = self.model.state_at(*commits, *began_at);
                    let context = format!("snapshot at commit {commits} begun at {began_at}");
                    Self::check_get(tx, key, state.get(key), &context);
                }
            }
            Step::SnapshotScan { slot, spec } => {
                if let Some((tx, commits, began_at)) = &self.snapshots[*slot] {
                    let state = self.model.state_at(*commits, *began_at);
                    let context = format!("snapshot at commit {commits} begun at {began_at}");
                    Self::check_scan(tx, spec, &state, &context);
                }
            }
            Step::AdvanceClock(by) => self.ttl_clock.advance(*by),
            Step::Flush => {
                let cf = self
                    .engine()
                    .get_column_family("default")
                    .expect("default family");
                self.engine().flush_cf(&cf).expect("flush");
                // A published SST holds every buffered commit before it.
                self.model.mark_durable();
            }
            Step::Compact => self.engine().compact_all().expect("compact"),
            Step::Reopen => {
                self.drop_snapshots();
                let mut engine = self.engine.take().expect("engine is open");
                engine.shutdown(Duration::from_secs(30)).expect("shutdown");
                drop(engine);
                self.model.mark_durable();
                self.start();
                self.check_everything("after reopen");
            }
            Step::Crash(seed) => {
                self.drop_snapshots();
                self.device.process_crash();
                drop(self.engine.take());
                self.device.power_loss(*seed);
                self.lease_clock.advance(LEASE_TTL * 2);
                self.start();
                let recovered = rows(&self.read_tx(), &ScanSpec::EVERYTHING);
                let now = self.now_millis();
                if let Err(message) = self.model.reconcile_after_crash(&recovered, now) {
                    panic!("crash {seed}: {message}");
                }
            }
        }
    }

    /// The final state equals the model, before and after a clean restart.
    pub fn check_final_state(&mut self) {
        self.check_everything("final");
        self.step(&Step::Reopen);
    }

    pub fn close(mut self) {
        self.drop_snapshots();
        if let Some(mut engine) = self.engine.take() {
            engine.shutdown(Duration::from_secs(30)).expect("shutdown");
        }
    }
}

/// Run one decoded history to the end.
pub fn run_history(data: &[u8]) {
    let steps = decode(data);
    let trace = std::env::var_os("DIFFERENTIAL_TRACE").is_some();
    let mut harness = Harness::open();
    for (index, step) in steps.iter().enumerate() {
        if trace {
            eprintln!("step {index}: {step:?}");
        }
        harness.step(step);
    }
    harness.check_final_state();
    harness.close();
}
