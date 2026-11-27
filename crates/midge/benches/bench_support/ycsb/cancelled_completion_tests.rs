//! #745 controls use the exact duration worker loop and a real local Engine.
//! Stop and rejection are constructed; they do not identify the native 525s outlier.

use super::{Arc, ColumnFamilyHandle, Engine};

struct LocalFixture {
    _directory: tempfile::TempDir,
    options: cntryl_midge::OpenOptions,
    engine: Arc<Engine>,
    family: ColumnFamilyHandle,
}

impl LocalFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("local dataset directory");
        let options = cntryl_midge::OpenOptions::local(directory.path())
            .build()
            .expect("local options");
        let engine = Arc::new(Engine::open(options.clone()).expect("local engine"));
        let family = engine.create_column_family("cf1").expect("dataset family");
        Self {
            _directory: directory,
            options,
            engine,
            family,
        }
    }

    fn sequence(&self) -> u64 {
        self.engine
            .metrics()
            .get_runtime_metrics()
            .expect("runtime metrics")
            .current_sequence
    }

    fn row(&self) -> Option<cntryl_midge::Bytes> {
        read_row(self.engine.as_ref(), self.family.id())
    }

    fn finish(self) -> Option<cntryl_midge::Bytes> {
        let mut engine = Arc::try_unwrap(self.engine)
            .unwrap_or_else(|_| panic!("all shared client handles must be released"));
        engine
            .shutdown(std::time::Duration::from_secs(30))
            .expect("shutdown actual engine");
        drop(engine);
        let mut reopened = Engine::open(self.options).expect("same-path reopen");
        let family = reopened.get_column_family("cf1").expect("recovered family");
        let row = read_row(&reopened, family.id());
        reopened
            .shutdown(std::time::Duration::from_secs(30))
            .expect("shutdown recovered engine");
        row
    }
}

fn read_row(engine: &Engine, cf_id: cntryl_midge::ColumnFamilyId) -> Option<cntryl_midge::Bytes> {
    let tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("actual read transaction");
    tx.get(b"key").expect("actual row read")
}

fn commit_row(engine: &Engine, family: &ColumnFamilyHandle) -> cntryl_midge::MidgeResult<()> {
    let mut tx = engine.begin_tx(family.id(), cntryl_midge::TransactionMode::ReadWrite)?;
    tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
    tx.commit(cntryl_midge::WriteOptions::sync())
}

#[test]
fn should_exclude_cancelled_write_when_stop_precedes_actual_commit() {
    use super::{
        retry_write_stall_observed, run_observed_client_loop, AtomicBool, AtomicU64, Ordering,
    };
    // Arrange: the actual shared engine has no row and no stop initially.
    let fixture = LocalFixture::new();
    let before = fixture.sequence();
    let stop = AtomicBool::new(false);
    let heartbeat = AtomicU64::new(0);
    let mut steps = 0;
    let mut commit_calls = 0;

    // Act: enter the real worker loop, then cancel before its real commit callback.
    let stats = run_observed_client_loop(
        fixture.engine.as_ref(),
        &fixture.family,
        &stop,
        &heartbeat,
        0,
        |engine, family, _| {
            steps += 1;
            stop.store(true, Ordering::Release);
            retry_write_stall_observed(engine, family.id(), &stop, || {
                commit_calls += 1;
                commit_row(engine, family)
            })
            .expect("observed cancellation")
        },
    );
    let after = fixture.sequence();
    let row = fixture.row();
    let recovered = fixture.finish();

    // Assert: genuine absence/admission evidence precedes the intended accounting gate.
    assert_eq!(steps, 1);
    assert_eq!(commit_calls, 0);
    assert_eq!(before, after);
    assert_eq!(row, None);
    assert_eq!(recovered, None);
    assert_eq!(stats.operations, 0);
    assert_eq!(stats.latency_us.len(), 0);
    assert_eq!(heartbeat.load(Ordering::Acquire), 0);
}

#[test]
fn should_exclude_rejected_write_when_stop_interrupts_retry() {
    use super::{
        retry_write_stall_observed, run_observed_client_loop, AtomicBool, AtomicU64, Ordering,
    };
    // Arrange: rejection is scripted, while the actual engine remains empty.
    let fixture = LocalFixture::new();
    let before = fixture.sequence();
    let stop = AtomicBool::new(false);
    let heartbeat = AtomicU64::new(0);
    let mut attempts = 0;

    // Act: a rejected operation stops before the retry can admit real work.
    let stats = run_observed_client_loop(
        fixture.engine.as_ref(),
        &fixture.family,
        &stop,
        &heartbeat,
        0,
        |engine, family, _| {
            retry_write_stall_observed(engine, family.id(), &stop, || {
                attempts += 1;
                stop.store(true, Ordering::Release);
                Err(cntryl_midge::MidgeError::WriteStall(
                    "constructed rejection".into(),
                ))
            })
            .expect("observed rejected retry")
        },
    );
    let after = fixture.sequence();
    let row = fixture.row();
    let recovered = fixture.finish();

    // Assert: one constructed rejection is neither an actual commit nor completion.
    assert_eq!(attempts, 1);
    assert_eq!(before, after);
    assert_eq!(row, None);
    assert_eq!(recovered, None);
    assert_eq!(stats.operations, 0);
    assert_eq!(stats.latency_us.len(), 0);
    assert_eq!(heartbeat.load(Ordering::Acquire), 0);
}

#[test]
fn should_exclude_partial_rmw_when_stop_precedes_its_write() {
    use super::{
        retry_write_stall_observed, run_observed_client_loop, AtomicBool, AtomicU64, Ordering,
    };
    // Arrange: the existing row was committed by the actual public local engine.
    let fixture = LocalFixture::new();
    commit_row(fixture.engine.as_ref(), &fixture.family).expect("real seed commit");
    let before = fixture.sequence();
    let stop = AtomicBool::new(false);
    let heartbeat = AtomicU64::new(0);
    let mut reads = 0;
    let mut commit_calls = 0;

    // Act: complete the real read but cancel the write portion of the same logical operation.
    let stats = run_observed_client_loop(
        fixture.engine.as_ref(),
        &fixture.family,
        &stop,
        &heartbeat,
        0,
        |engine, family, _| {
            let row = read_row(engine, family.id());
            reads += usize::from(row.as_deref() == Some(b"value".as_slice()));
            stop.store(true, Ordering::Release);
            retry_write_stall_observed(engine, family.id(), &stop, || {
                commit_calls += 1;
                commit_row(engine, family)
            })
            .expect("observed partial operation")
        },
    );
    let after = fixture.sequence();
    let row = fixture.row();
    let recovered = fixture.finish();

    // Assert: real read success alone cannot satisfy an RMW completion.
    assert_eq!(reads, 1);
    assert_eq!(commit_calls, 0);
    assert_eq!(before, after);
    assert_eq!(row.as_deref(), Some(b"value".as_slice()));
    assert_eq!(recovered, row);
    assert_eq!(stats.operations, 0);
    assert_eq!(stats.latency_us.len(), 0);
    assert_eq!(heartbeat.load(Ordering::Acquire), 0);
}

#[test]
fn should_count_real_write_when_stop_follows_successful_commit() {
    use super::{
        retry_write_stall_observed, run_observed_client_loop, AtomicBool, AtomicU64, Ordering,
    };
    // Arrange: a real successful callback may race the window's stop signal.
    let fixture = LocalFixture::new();
    let before = fixture.sequence();
    let stop = AtomicBool::new(false);
    let heartbeat = AtomicU64::new(0);
    let mut commit_calls = 0;

    // Act: commit actual data, then set stop before the callback returns its success.
    let stats = run_observed_client_loop(
        fixture.engine.as_ref(),
        &fixture.family,
        &stop,
        &heartbeat,
        0,
        |engine, family, _| {
            retry_write_stall_observed(engine, family.id(), &stop, || {
                commit_calls += 1;
                commit_row(engine, family)?;
                stop.store(true, Ordering::Release);
                Ok(())
            })
            .expect("actual completed operation")
        },
    );
    let after = fixture.sequence();
    let row = fixture.row();
    let recovered = fixture.finish();

    // Assert: actual ACK, exact row and reopen remain counted after the stop race.
    assert_eq!(commit_calls, 1);
    assert!(after > before);
    assert_eq!(row.as_deref(), Some(b"value".as_slice()));
    assert_eq!(recovered, row);
    assert_eq!(stats.operations, 1);
    assert_eq!(stats.latency_us.len(), 1);
    assert!(heartbeat.load(Ordering::Acquire) > 0);
}
