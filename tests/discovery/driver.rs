use super::histories::{History, SEED};
use super::model::{self, Action, Intent, Model, Observation, Scan};
use cntryl_midge::{
    ColumnFamilyHandle, Engine, MemoryBudget, MidgeError, MidgeResult, OpenOptions, Query,
    Transaction, TransactionMode, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum Backend {
    Local,
    CloudSimulated,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum ReadPath {
    Resident,
    Spilled,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Fixture {
    pub backend: Backend,
    pub read_path: ReadPath,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Counters {
    operations: usize,
    reads: usize,
    scans: usize,
    commits: usize,
    restarts: usize,
    maintenance: usize,
    peak_spill_files: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub enum FailureKind {
    Api,
    Read,
    Scan,
    FinalState,
    Harness,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Failure {
    step: usize,
    kind: FailureKind,
    detail: String,
    expected: Option<Box<Observation>>,
    actual: Option<Box<Observation>>,
    counters: Box<Counters>,
    api_signature: Option<String>,
}

struct Driver {
    // Drop explicitly settles transactions before closing the runtime.
    transactions: [Option<Transaction>; 2],
    engine: Option<Engine>,
    options: OpenOptions,
    cf: ColumnFamilyHandle,
}

fn options(root: &Path, fixture: Fixture) -> MidgeResult<OpenOptions> {
    let builder = match fixture.backend {
        Backend::Local => OpenOptions::local(root),
        Backend::CloudSimulated => OpenOptions::cloud_simulated(root, "discovery", "model/"),
    };
    builder
        .memory_budget(MemoryBudget::Bytes(64 * 1024 * 1024))
        .transaction_memory_pool_size(match fixture.read_path {
            ReadPath::Resident => 2 * 1024 * 1024,
            ReadPath::Spilled => 8 * 1024,
        })
        .with_memtable_size_limit(64 * 1024)
        .with_memtable_flush_threshold(64 * 1024)
        .background_compaction(false)
        .build()
}

impl Driver {
    fn new(root: &Path, fixture: Fixture) -> MidgeResult<Self> {
        let options = options(root, fixture)?;
        let engine = Engine::open(options.clone())?;
        let cf = engine.create_column_family("discovery")?;
        Ok(Self {
            transactions: [None, None],
            engine: Some(engine),
            options,
            cf,
        })
    }

    fn engine(&self) -> &Engine {
        self.engine.as_ref().expect("live fixture engine")
    }

    fn transaction(&self, id: u8) -> &Transaction {
        self.transactions[usize::from(id)]
            .as_ref()
            .expect("legal active slot")
    }

    fn query(scan: &Scan) -> Query {
        let mut query = Query::new();
        if let Some(start) = &scan.start {
            query = query.start_key(start.clone().into());
        }
        if let Some(end) = &scan.end {
            query = query.end_key(end.clone().into());
        }
        if let Some(prefix) = &scan.prefix {
            query = query.prefix(prefix.clone().into());
        }
        if let Some(limit) = scan.limit {
            query = query.limit(limit);
        }
        if scan.reverse {
            query = query.reverse();
        }
        query
    }

    fn scan(tx: &Transaction, scan: &Scan) -> MidgeResult<Observation> {
        let mut iterator = tx.scan(&Self::query(scan))?;
        let rows: MidgeResult<model::Rows> = iterator
            .by_ref()
            .map(|row| row.map(|(key, value)| (key.to_vec(), value.to_vec())))
            .collect();
        let rows = rows?;
        if !iterator.exhausted() || iterator.failed() {
            return Err(MidgeError::Internal(
                "scan terminated without successful exhaustion".into(),
            ));
        }
        Ok(Observation::Rows(rows))
    }

    fn restart(&mut self) -> MidgeResult<()> {
        self.transactions = [None, None];
        let mut old = self.engine.take().expect("live fixture engine");
        old.shutdown(Duration::from_secs(10))?;
        drop(old);
        let engine = Engine::open(self.options.clone())?;
        self.cf = engine.get_column_family("discovery").ok_or_else(|| {
            MidgeError::Internal("column family disappeared after clean restart".into())
        })?;
        self.engine = Some(engine);
        Ok(())
    }

    fn apply(&mut self, action: &Action, fixture: Fixture) -> MidgeResult<Observation> {
        match action {
            Action::Begin(id) => {
                self.transactions[usize::from(*id)] = Some(
                    self.engine()
                        .begin_tx(self.cf.id(), TransactionMode::ReadWrite)?,
                );
            }
            Action::Write(id, intent) => {
                let tx = self.transactions[usize::from(*id)]
                    .as_mut()
                    .expect("legal active slot");
                match intent {
                    Intent::Put { key, byte, length } => {
                        tx.put(key.clone(), vec![*byte; usize::from(*length)], None)?;
                    }
                    Intent::Delete { key } => tx.delete(key.clone())?,
                    Intent::DeleteRange { start, end } => {
                        tx.delete_range(start.clone(), end.clone())?;
                    }
                }
            }
            Action::Read(id, key) => {
                return self
                    .transaction(*id)
                    .get(key)
                    .map(|value| Observation::Value(value.map(|value| value.to_vec())));
            }
            Action::Scan(id, scan) => return Self::scan(self.transaction(*id), scan),
            Action::Commit(id) => {
                let tx = self.transactions[usize::from(*id)]
                    .take()
                    .expect("legal active slot");
                tx.commit(match fixture.backend {
                    Backend::Local => WriteOptions::sync(),
                    Backend::CloudSimulated => WriteOptions::cloud_strict(),
                })?;
            }
            Action::Rollback(id) => self.transactions[usize::from(*id)]
                .take()
                .expect("legal active slot")
                .rollback()?,
            Action::Flush => self.engine().flush_cf(&self.cf)?,
            Action::Compact => self.engine().compact_all()?,
            Action::Restart => self.restart()?,
        }
        Ok(Observation::None)
    }

    fn current(&self) -> MidgeResult<Observation> {
        let tx = self
            .engine()
            .begin_tx(self.cf.id(), TransactionMode::ReadOnly)?;
        Self::scan(&tx, &Scan::default())
    }

    fn close(&mut self) -> MidgeResult<()> {
        self.transactions = [None, None];
        if let Some(mut engine) = self.engine.take() {
            engine.shutdown(Duration::from_secs(10))?;
        }
        Ok(())
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn fail(step: usize, kind: FailureKind, detail: String, counters: &Counters) -> Failure {
    Failure {
        step,
        kind,
        detail,
        expected: None,
        actual: None,
        counters: Box::new(counters.clone()),
        api_signature: None,
    }
}

fn api_fail(step: usize, action: &'static str, error: &MidgeError, counters: &Counters) -> Failure {
    let mut failure = fail(step, FailureKind::Api, error.to_string(), counters);
    let debug = format!("{error:?}");
    let variant = debug.split(['(', '{']).next().expect("error variant");
    failure.api_signature = Some(format!("{action}:{variant}"));
    failure
}

fn action_name(action: &Action) -> &'static str {
    match action {
        Action::Begin(_) => "begin",
        Action::Write(_, Intent::Put { .. }) => "put",
        Action::Write(_, Intent::Delete { .. }) => "delete",
        Action::Write(_, Intent::DeleteRange { .. }) => "delete_range",
        Action::Read(..) => "get",
        Action::Scan(..) => "scan",
        Action::Commit(_) => "commit",
        Action::Rollback(_) => "rollback",
        Action::Flush => "flush",
        Action::Compact => "compact",
        Action::Restart => "restart",
    }
}

fn spill_files(root: &Path) -> std::io::Result<usize> {
    let entries = match std::fs::read_dir(root.join("txn")) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut count = 0;
    for entry in entries {
        if entry?.file_type()?.is_file() {
            count += 1;
        }
    }
    Ok(count)
}

pub fn execute(
    history: &History,
    fixture: Fixture,
    require_spill: bool,
) -> Result<Counters, Failure> {
    let mut counters = Counters::default();
    if !model::is_legal(&history.actions) {
        return Err(fail(
            0,
            FailureKind::Harness,
            "illegal concrete history".into(),
            &counters,
        ));
    }
    let directory = tempfile::tempdir()
        .map_err(|error| fail(0, FailureKind::Harness, error.to_string(), &counters))?;
    let mut driver = Driver::new(directory.path(), fixture)
        .map_err(|error| api_fail(0, "open", &error, &counters))?;
    let mut model = Model::default();
    for (step, action) in history.actions.iter().enumerate() {
        counters.operations += 1;
        match action {
            Action::Read(..) => counters.reads += 1,
            Action::Scan(..) => counters.scans += 1,
            Action::Commit(..) => counters.commits += 1,
            Action::Restart => counters.restarts += 1,
            Action::Flush | Action::Compact => counters.maintenance += 1,
            _ => {}
        }
        let actual = driver
            .apply(action, fixture)
            .map_err(|error| api_fail(step, action_name(action), &error, &counters))?;
        // Only successful accepted operations advance the oracle. An unexpected
        // API error fails the probe; it is not assumed to imply nonapplication.
        let expected = model.apply(action).expect("validated legal history");
        if actual != expected {
            let kind = if matches!(action, Action::Read(..)) {
                FailureKind::Read
            } else {
                FailureKind::Scan
            };
            let mut failure = fail(
                step,
                kind,
                "public observation differs from oracle".into(),
                &counters,
            );
            failure.expected = Some(Box::new(expected));
            failure.actual = Some(Box::new(actual));
            return Err(failure);
        }
        let files = spill_files(directory.path())
            .map_err(|error| fail(step, FailureKind::Harness, error.to_string(), &counters))?;
        counters.peak_spill_files = counters.peak_spill_files.max(files);
    }
    let step = history.actions.len();
    let actual = driver
        .current()
        .map_err(|error| api_fail(step, "final_scan", &error, &counters))?;
    let expected = Observation::Rows(model.committed.into_iter().collect());
    if actual != expected {
        let mut failure = fail(
            step,
            FailureKind::FinalState,
            "final committed state differs".into(),
            &counters,
        );
        failure.actual = Some(Box::new(actual));
        failure.expected = Some(Box::new(expected));
        return Err(failure);
    }
    driver
        .close()
        .map_err(|error| api_fail(step, "shutdown", &error, &counters))?;
    if require_spill
        && matches!(fixture.read_path, ReadPath::Spilled)
        && counters.peak_spill_files == 0
    {
        return Err(fail(
            step,
            FailureKind::Harness,
            "spilled fixture never produced a spill file".into(),
            &counters,
        ));
    }
    Ok(counters)
}

/// Structurally remove legal chunks while preserving the failure class. Never
/// turn a missing Begin into a fake engine defect. The deterministic replay cap
/// bounds minimization separately from the campaign's operation budget.
fn minimize(
    history: &History,
    fixture: Fixture,
    failure: &Failure,
) -> (History, Failure, usize, bool) {
    minimize_with(history, failure, |candidate| {
        execute(candidate, fixture, false)
    })
}

fn minimize_with(
    history: &History,
    failure: &Failure,
    mut replay: impl FnMut(&History) -> Result<Counters, Failure>,
) -> (History, Failure, usize, bool) {
    const MAX_REPLAYS: usize = 128;
    let mut current = history.clone();
    let mut current_failure = failure.clone();
    let mut granularity = 2;
    let mut replays = 0;
    while current.actions.len() >= 2 && replays < MAX_REPLAYS {
        let chunk = current.actions.len().div_ceil(granularity);
        let mut reduced = false;
        for start in (0..current.actions.len()).step_by(chunk) {
            if replays == MAX_REPLAYS {
                break;
            }
            let mut candidate = current.clone();
            candidate
                .actions
                .drain(start..(start + chunk).min(current.actions.len()));
            if !model::is_legal(&candidate.actions) {
                continue;
            }
            replays += 1;
            if let Err(next) = replay(&candidate) {
                if next.kind == failure.kind && next.api_signature == failure.api_signature {
                    current = candidate;
                    current_failure = next;
                    granularity = granularity.saturating_sub(1).max(2);
                    reduced = true;
                    break;
                }
            }
        }
        if !reduced {
            if granularity >= current.actions.len() {
                break;
            }
            granularity = (granularity * 2).min(current.actions.len());
        }
    }
    (current, current_failure, replays, replays < MAX_REPLAYS)
}

#[derive(Serialize)]
struct Revision {
    head: String,
    dirty: bool,
}

fn revision() -> Revision {
    let sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("read revision");
    assert!(
        sha.status.success(),
        "discovery requires repository revision metadata"
    );
    let status = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .expect("read working state");
    assert!(status.status.success());
    Revision {
        head: String::from_utf8(sha.stdout)
            .expect("ASCII SHA")
            .trim()
            .to_string(),
        dirty: !status.stdout.is_empty(),
    }
}

#[derive(Serialize)]
struct Counterexample<'a> {
    revision: &'a Revision,
    seed: u64,
    fixture: Fixture,
    durability: &'static str,
    failpoint_ordinal: Option<usize>,
    original: &'a History,
    original_failure: &'a Failure,
    minimized: History,
    minimized_failure: Failure,
    minimization_replays: usize,
    minimization_complete: bool,
}

fn write_json(path: &Path, value: &impl Serialize) {
    let bytes = serde_json::to_vec_pretty(value).expect("serialize discovery artifact");
    std::fs::write(path, bytes).expect("persist discovery artifact");
}

/// Replaying uses concrete actions, never regenerated random inputs. The other
/// artifact fields remain provenance rather than executable instructions.
pub fn replay(path: &Path) {
    #[derive(Deserialize)]
    struct SavedCounterexample {
        fixture: Fixture,
        minimized: History,
    }
    let bytes = std::fs::read(path).expect("read counterexample artifact");
    let saved: SavedCounterexample =
        serde_json::from_slice(&bytes).expect("decode counterexample artifact");
    if let Err(failure) = execute(&saved.minimized, saved.fixture, false) {
        panic!("replayed mismatch in {}: {failure:?}", path.display());
    }
}

pub fn campaign(profile: &str, histories: &[History], fixtures: &[Fixture]) {
    let revision = revision();
    if matches!(profile, "discovery" | "release") {
        assert!(
            !revision.dirty,
            "full discovery/release evidence requires a clean committed revision"
        );
    }
    let root = std::env::var_os("MIDGE_DISCOVERY_ARTIFACT_DIR")
        .map_or_else(|| PathBuf::from("target/discovery-031"), PathBuf::from);
    std::fs::create_dir_all(&root).expect("artifact directory");
    let root = root.join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir(&root).expect("unique attempt-owned artifact directory");
    // Persist the entire corpus before any backend executes it.
    write_json(&root.join("corpus.json"), &histories);
    write_json(&root.join("revision.json"), &revision);
    write_json(
        &root.join("run.json"),
        &serde_json::json!({
            "revision": revision, "seed": SEED, "profile": profile,
            "fixtures": fixtures, "histories": histories.len(),
            "max_operations": histories.iter().map(|history| history.actions.len()).max(),
            "scope": "logical histories only; crash, race, retry, TTL, and conflict-policy probes are separate",
        }),
    );
    let mut failures = 0;
    let mut successes = Vec::new();
    for fixture in fixtures {
        for history in histories {
            match execute(history, *fixture, true) {
                Ok(counters) => {
                    successes.push((history.ordinal, history.template, fixture, counters));
                }
                Err(failure) => {
                    failures += 1;
                    let mut artifact = Counterexample {
                        revision: &revision,
                        seed: SEED,
                        fixture: *fixture,
                        durability: match fixture.backend {
                            Backend::Local => "sync",
                            Backend::CloudSimulated => "cloud_strict",
                        },
                        failpoint_ordinal: None,
                        original: history,
                        original_failure: &failure,
                        minimized: history.clone(),
                        minimized_failure: failure.clone(),
                        minimization_replays: 0,
                        minimization_complete: false,
                    };
                    let path = root.join(format!(
                        "failure-{}-{:?}-{:?}.json",
                        history.ordinal, fixture.backend, fixture.read_path
                    ));
                    // Retain a replayable failure even if a watchdog interrupts
                    // the subsequent structural minimization.
                    write_json(&path, &artifact);
                    let (minimized, minimized_failure, minimization_replays, minimization_complete) =
                        minimize(history, *fixture, &failure);
                    artifact.minimized = minimized;
                    artifact.minimized_failure = minimized_failure;
                    artifact.minimization_replays = minimization_replays;
                    artifact.minimization_complete = minimization_complete;
                    write_json(&path, &artifact);
                    eprintln!(
                        "discovery mismatch: {}: {:?} at step {}: {}",
                        path.display(),
                        failure.kind,
                        failure.step,
                        failure.detail
                    );
                }
            }
        }
    }
    write_json(&root.join("successes.json"), &successes);
    eprintln!("discovery artifacts: {}", root.display());
    assert_eq!(
        failures,
        0,
        "discovery mismatches; replay artifacts in {}",
        root.display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_illegal_shrink_before_opening_an_engine() {
        // Arrange
        let history = History {
            ordinal: 0,
            template: super::super::histories::Template::HeldSnapshot,
            actions: vec![Action::Commit(0)],
        };
        let fixture = Fixture {
            backend: Backend::Local,
            read_path: ReadPath::Resident,
        };

        // Act
        let failure = execute(&history, fixture, false).unwrap_err();

        // Assert
        assert_eq!(failure.kind, FailureKind::Harness);
        assert_eq!(failure.counters.operations, 0);
    }

    #[test]
    fn should_preserve_failure_provenance_when_minimizing_legal_histories() {
        // Arrange
        let history = History {
            ordinal: 0,
            template: super::super::histories::Template::HeldSnapshot,
            actions: vec![
                Action::Begin(0),
                Action::Read(0, b"keep".to_vec()),
                Action::Read(0, b"other".to_vec()),
                Action::Rollback(0),
                Action::Restart,
            ],
        };
        let counters = Counters::default();
        let expected = api_fail(1, "get", &MidgeError::Internal("target".into()), &counters);
        let replay = |candidate: &History| {
            assert!(model::is_legal(&candidate.actions));
            for (step, action) in candidate.actions.iter().enumerate() {
                if let Action::Read(_, key) = action {
                    let error = if key == b"keep" {
                        MidgeError::Internal("target".into())
                    } else {
                        MidgeError::NotSupported("unrelated".into())
                    };
                    return Err(api_fail(step, "get", &error, &counters));
                }
            }
            Ok(counters.clone())
        };

        // Act
        let (minimal, failure, replays, complete) = minimize_with(&history, &expected, replay);

        // Assert
        assert!(complete);
        assert!(replays > 0 && replays <= 128);
        assert_eq!(failure.api_signature, expected.api_signature);
        assert_eq!(minimal.actions.len(), 2);
        assert!(matches!(&minimal.actions[0], Action::Begin(0)));
        assert!(matches!(&minimal.actions[1], Action::Read(0, key) if key == b"keep"));
    }
}
