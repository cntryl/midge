use super::*;
use crate::lease::{ActiveWriterEffect, WriterEffectGroup};
use crate::wal::writer_authority::{WriterAuthority, WriterGroupAuthority};

struct NativeAuthority {
    store: Arc<dyn LeaderStore>,
    epoch: u64,
    reject_after_run: std::sync::atomic::AtomicBool,
    panic_after_run: std::sync::atomic::AtomicBool,
}

impl WriterAuthority for NativeAuthority {
    fn run(&self, operation: &mut dyn FnMut()) -> MidgeResult<()> {
        self.store
            .with_writer_epoch("writer", self.epoch, Duration::from_secs(2), operation)
            .map_err(|error| error.into_writer_effect_error("test WAL authority"))
    }
    fn groups(&self) -> Option<&dyn WriterGroupAuthority> {
        Some(self)
    }
}

impl WriterGroupAuthority for NativeAuthority {
    fn begin_group(&self, timeout: Duration) -> MidgeResult<WriterEffectGroup> {
        self.store
            .writer_effect_groups()
            .unwrap()
            .begin_group("writer", self.epoch, timeout)
            .map_err(|error| error.into_writer_effect_error("test WAL group"))
    }
    fn run_active(
        &self,
        active: &ActiveWriterEffect,
        timeout: Duration,
        operation: &mut dyn FnMut(),
    ) -> MidgeResult<()> {
        self.store
            .writer_effect_groups()
            .unwrap()
            .run_active("writer", self.epoch, active, timeout, operation)
            .map_err(|error| error.into_writer_effect_error("test WAL effect"))?;
        assert!(
            !self.panic_after_run.swap(false, Ordering::SeqCst),
            "injected native worker panic"
        );
        if self.reject_after_run.swap(false, Ordering::SeqCst) {
            return Err(crate::common::MidgeError::LeaseUnavailable(
                "scope cancelled after validation callback".into(),
            ));
        }
        Ok(())
    }
}

fn native_writer(
    fs: Arc<dyn Fs>,
    timeout: Duration,
) -> MidgeResult<(Arc<FsWalWriterIo>, Arc<dyn LeaderStore>)> {
    create_test_segment(fs.as_ref(), "wal.log")?;
    let store: Arc<dyn LeaderStore> = Arc::new(crate::lease::fs_leader_store::FsLeaderStore::new(
        fs.clone(),
        crate::lease::LeaseClock::default(),
    ));
    let lease = store.acquire_leadership("writer").unwrap();
    let writer = FsWalWriterIo::new_with_authority(
        "wal.log",
        fs,
        timeout,
        crate::telemetry::CounterSink::default(),
        Some(Arc::new(NativeAuthority {
            store: store.clone(),
            epoch: lease.epoch,
            reject_after_run: std::sync::atomic::AtomicBool::new(false),
            panic_after_run: std::sync::atomic::AtomicBool::new(false),
        })),
    )?;
    Ok((Arc::new(writer), store))
}

fn record(sequence: u64) -> WalRecord {
    WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(Bytes::from_static(b"value")),
        sequence,
    )
}

fn wait_until(condition: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(
            std::time::Instant::now() < deadline,
            "request did not reach its queue"
        );
        std::thread::yield_now();
    }
}

struct ReleaseGate(Option<std::sync::mpsc::Sender<()>>);

impl ReleaseGate {
    fn release(mut self) {
        let _ = self.0.take().unwrap().send(());
    }
}

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[derive(Clone, Copy)]
enum TimedRequest {
    Append,
    Sync,
}

#[test]
fn should_charge_native_admission_against_append_acknowledgement_budget() -> MidgeResult<()> {
    // Arrange
    let request = TimedRequest::Append;
    // Act
    let result = assert_native_acknowledgement_budget(request);
    // Assert
    assert!(result.is_ok(), "{result:?}");
    result
}

#[test]
fn should_charge_native_admission_against_sync_acknowledgement_budget() -> MidgeResult<()> {
    // Arrange
    let request = TimedRequest::Sync;
    // Act
    let result = assert_native_acknowledgement_budget(request);
    // Assert
    assert!(result.is_ok(), "{result:?}");
    result
}

fn assert_native_acknowledgement_budget(request: TimedRequest) -> MidgeResult<()> {
    let budget = Duration::from_millis(400);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let fs = Arc::new(SyncCountingFs {
        open_gate: Some(Arc::new((entered_tx, std::sync::Mutex::new(release_rx)))),
        ..SyncCountingFs::default()
    });
    let (writer, store) = native_writer(fs.clone(), budget)?;
    let worker_gate = ReleaseGate(Some(release_tx));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (unpin, pin) = pin_closed_grant(&store);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    writer.set_write_authority(Some(Arc::new(DelayedAdmissionAuthority::new(
        store.clone(),
        Duration::ZERO,
        Some(started_tx),
    ))));
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let caller = {
        let writer = writer.clone();
        std::thread::spawn(move || {
            let result = match request {
                TimedRequest::Append => writer
                    .append_record_accounted(&record(1))
                    .map(|_| ())
                    .map_err(|failure| failure.error),
                TimedRequest::Sync => writer.sync_with_timeout(budget),
            };
            let _ = result_tx.send(result);
        })
    };
    let started = started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    std::thread::sleep(Duration::from_millis(250));
    let waiting_for_admission =
        writer.queue.lock().is_empty() && writer.sync_state.lock().requests.is_empty();
    unpin.release();
    pin.join().unwrap();
    let limit = budget + Duration::from_millis(100);
    let response = result_rx.recv_timeout(limit.saturating_sub(started.elapsed()));
    let elapsed = started.elapsed();
    worker_gate.release();
    caller.join().unwrap();
    drop(writer);
    assert!(
        waiting_for_admission,
        "request bypassed the closed native grant"
    );
    assert!(
        matches!(response, Ok(Err(crate::common::MidgeError::Timeout(_)))),
        "{response:?}"
    );
    assert!(
        elapsed < limit,
        "native admission restarted the acknowledgement window: {elapsed:?}"
    );
    assert_eq!(
        fs.metadata(&FsPath::new("wal.log")).unwrap().len,
        HEADER_LEN as u64
    );
    assert!(store.acquire_leadership("successor").unwrap().epoch > 1);
    Ok(())
}

fn pin_closed_grant(store: &Arc<dyn LeaderStore>) -> (ReleaseGate, std::thread::JoinHandle<()>) {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (unpin_tx, unpin_rx) = std::sync::mpsc::channel();
    let pin = {
        let store = store.clone();
        std::thread::spawn(move || {
            let root = store
                .writer_effect_groups()
                .unwrap()
                .begin_group("writer", 1, Duration::from_secs(2))
                .unwrap();
            let ticket = root.reserve_ticket(Duration::from_secs(2)).unwrap();
            drop(root);
            ready_tx.send(()).unwrap();
            let _ = unpin_rx.recv();
            drop(ticket);
        })
    };
    let unpin = ReleaseGate(Some(unpin_tx));
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    (unpin, pin)
}

struct DelayedAdmissionAuthority {
    inner: NativeAuthority,
    delay: Duration,
    started: Option<std::sync::mpsc::Sender<std::time::Instant>>,
}

impl DelayedAdmissionAuthority {
    fn new(
        store: Arc<dyn LeaderStore>,
        delay: Duration,
        started: Option<std::sync::mpsc::Sender<std::time::Instant>>,
    ) -> Self {
        Self {
            inner: NativeAuthority {
                store,
                epoch: 1,
                reject_after_run: std::sync::atomic::AtomicBool::new(false),
                panic_after_run: std::sync::atomic::AtomicBool::new(false),
            },
            delay,
            started,
        }
    }
}

impl WriterAuthority for DelayedAdmissionAuthority {
    fn run(&self, operation: &mut dyn FnMut()) -> MidgeResult<()> {
        self.inner.run(operation)
    }
    fn groups(&self) -> Option<&dyn WriterGroupAuthority> {
        Some(self)
    }
}

impl WriterGroupAuthority for DelayedAdmissionAuthority {
    fn begin_group(&self, timeout: Duration) -> MidgeResult<WriterEffectGroup> {
        if let Some(started) = &self.started {
            started.send(std::time::Instant::now()).unwrap();
        }
        let root = self.inner.begin_group(timeout)?;
        std::thread::sleep(self.delay);
        Ok(root)
    }
    fn run_active(
        &self,
        active: &ActiveWriterEffect,
        timeout: Duration,
        operation: &mut dyn FnMut(),
    ) -> MidgeResult<()> {
        self.inner.run_active(active, timeout, operation)
    }
}

#[test]
fn should_reject_native_append_unchanged_when_budget_expires_before_queue_submission(
) -> MidgeResult<()> {
    // Arrange
    let request = TimedRequest::Append;
    // Act
    let result = assert_expired_native_submission(request);
    // Assert
    assert!(result.is_ok(), "{result:?}");
    result
}

#[test]
fn should_keep_native_sync_retryable_when_budget_expires_before_queue_submission() -> MidgeResult<()>
{
    // Arrange
    let request = TimedRequest::Sync;
    // Act
    let result = assert_expired_native_submission(request);
    // Assert
    assert!(result.is_ok(), "{result:?}");
    result
}

fn assert_expired_native_submission(request: TimedRequest) -> MidgeResult<()> {
    // Arrange: deschedule admission after acquiring its native grant.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let fs = Arc::new(SyncCountingFs {
        open_gate: Some(Arc::new((entered_tx, std::sync::Mutex::new(release_rx)))),
        ..SyncCountingFs::default()
    });
    let (writer, store) = native_writer(fs.clone(), Duration::from_millis(10))?;
    let worker_gate = ReleaseGate(Some(release_tx));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    writer.set_write_authority(Some(Arc::new(DelayedAdmissionAuthority::new(
        store.clone(),
        Duration::from_millis(30),
        None,
    ))));

    // Act
    let mut unchanged = None;
    let result = match request {
        TimedRequest::Append => writer
            .append_record_accounted(&record(1))
            .map(|_| ())
            .map_err(|failure| {
                unchanged = Some(failure.unchanged);
                failure.error
            }),
        TimedRequest::Sync => writer.sync_with_timeout(Duration::from_millis(10)),
    };
    let queued = writer.queue.lock().len();
    let sync_requests = writer.sync_state.lock().requests.len();
    let sticky = writer.writer_failure();
    worker_gate.release();
    drop(writer);

    // Assert: an unsubmitted request has a definite unchanged outcome and
    // leaves the writer retryable rather than poisoning it with unknown I/O.
    assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
    if let Some(unchanged) = unchanged {
        assert!(unchanged);
    }
    assert_eq!(queued, 0);
    assert_eq!(sync_requests, 0);
    assert!(sticky.is_none());
    assert_eq!(
        fs.metadata(&FsPath::new("wal.log")).unwrap().len,
        HEADER_LEN as u64
    );
    assert!(store.acquire_leadership("successor").unwrap().epoch > 1);
    Ok(())
}

#[test]
fn should_continue_explicit_writer_group_when_an_independent_creator_has_left() -> MidgeResult<()> {
    // Arrange: the strict root joins an earlier independent native cohort.
    let fs = Arc::new(crate::io::MockFs::new());
    let (writer, store) = native_writer(fs.clone(), Duration::from_secs(2))?;
    let creator = store
        .writer_effect_groups()
        .unwrap()
        .begin_group("writer", 1, Duration::from_secs(2))
        .unwrap();
    let group = writer
        .writer_group_capability()
        .unwrap()
        .begin_group(Duration::from_secs(2))?;
    drop(creator);

    // Act: explicit jobs continue after the original creator closes admission.
    writer
        .writer_group_capability()
        .unwrap()
        .append_batch_in_group(&[record(1)], &group)
        .map_err(|failure| failure.error)?;
    writer
        .writer_group_capability()
        .unwrap()
        .sync_in_group(Duration::from_secs(2), &group)?;
    drop(group);

    // Assert
    let bytes =
        crate::io::read_all(fs.as_ref(), &FsPath::new("wal.log")).map_err(FsError::into_midge)?;
    assert_eq!(crate::wal::format::segment::records(&bytes)?.len(), 1);
    assert!(store.acquire_leadership("successor").unwrap().epoch > 1);
    Ok(())
}

#[test]
fn should_process_raw_append_ahead_of_explicit_group_without_waiting_on_its_native_grant(
) -> MidgeResult<()> {
    // Arrange: hold the worker before it can drain either submitted job.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let fs = Arc::new(SyncCountingFs {
        open_gate: Some(Arc::new((entered_tx, std::sync::Mutex::new(release_rx)))),
        ..SyncCountingFs::default()
    });
    let (writer, store) = native_writer(fs.clone(), Duration::from_secs(2))?;
    let release_gate = ReleaseGate(Some(release_tx));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let group = writer
        .writer_group_capability()
        .unwrap()
        .begin_group(Duration::from_secs(2))?;
    let raw = {
        let writer = writer.clone();
        std::thread::spawn(move || writer.append_record_accounted(&record(1)))
    };
    wait_until(|| writer.queue.lock().len() == 1);
    let release = {
        let writer = writer.clone();
        std::thread::spawn(move || {
            wait_until(|| writer.queue.lock().len() == 2);
            release_gate.release();
        })
    };

    // Act: both jobs must own native child tickets before either can execute.
    let explicit = writer
        .writer_group_capability()
        .unwrap()
        .append_batch_in_group(&[record(2)], &group);
    let first = raw.join().unwrap();
    release.join().unwrap();
    let first = first.map_err(|failure| failure.error)?;
    let second = explicit.map_err(|failure| failure.error)?;
    writer
        .writer_group_capability()
        .unwrap()
        .sync_in_group(Duration::from_secs(2), &group)?;
    drop(group);

    // Assert
    assert!(second > first);
    let bytes =
        crate::io::read_all(fs.as_ref(), &FsPath::new("wal.log")).map_err(FsError::into_midge)?;
    let records = crate::wal::format::segment::records(&bytes)?;
    assert_eq!(
        records.iter().map(|record| record.seq).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(store.acquire_leadership("successor").unwrap().epoch > 1);
    Ok(())
}

#[test]
fn should_keep_successful_prefix_when_later_queued_authority_rejects_its_partition(
) -> MidgeResult<()> {
    // Arrange: two queued jobs capture different authority identities.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let fs = Arc::new(SyncCountingFs {
        open_gate: Some(Arc::new((entered_tx, std::sync::Mutex::new(release_rx)))),
        ..SyncCountingFs::default()
    });
    let writer = Arc::new(FsWalWriterIo::new_with_timeout(
        "wal.log",
        fs.clone(),
        Duration::from_secs(2),
    )?);
    let release_gate = ReleaseGate(Some(release_tx));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    writer.set_write_authority(Some(Arc::new(|operation: &mut dyn FnMut()| {
        operation();
        Ok(())
    })));
    let first = {
        let writer = writer.clone();
        std::thread::spawn(move || writer.append_record_accounted(&record(1)))
    };
    wait_until(|| writer.queue.lock().len() == 1);
    writer.set_write_authority(Some(Arc::new(|_: &mut dyn FnMut()| {
        Err(crate::common::MidgeError::Fenced(
            "replacement rejects work".into(),
        ))
    })));
    let second = {
        let writer = writer.clone();
        std::thread::spawn(move || writer.append_record_accounted(&record(2)))
    };
    wait_until(|| writer.queue.lock().len() == 2);
    let sync = {
        let writer = writer.clone();
        std::thread::spawn(move || writer.sync_with_timeout(writer.io_timeout))
    };
    wait_until(|| writer.sync_state.lock().pending_fsyncs != 0);
    let syncs_before = fs.durable_sync_count.load(Ordering::SeqCst);

    // Act
    release_gate.release();
    let accepted = first.join().unwrap().map_err(|failure| failure.error)?;
    let rejected = second.join().unwrap().unwrap_err();
    let synced = sync.join().unwrap();

    // Assert: an unfinished suffix cannot complete the sync snapshot, and its
    // rejection cannot relabel or erase the prefix that was already written.
    assert_eq!(accepted, HEADER_LEN as u64);
    assert!(rejected.unchanged);
    assert!(matches!(
        rejected.error,
        crate::common::MidgeError::Fenced(_)
    ));
    assert!(matches!(synced, Err(crate::common::MidgeError::Fenced(_))));
    assert_eq!(fs.durable_sync_count.load(Ordering::SeqCst), syncs_before);
    let bytes =
        crate::io::read_all(fs.as_ref(), &FsPath::new("wal.log")).map_err(FsError::into_midge)?;
    let records = crate::wal::format::segment::records(&bytes)?;
    assert_eq!(
        records.iter().map(|record| record.seq).collect::<Vec<_>>(),
        vec![1]
    );
    Ok(())
}

#[test]
fn should_release_queued_request_tickets_when_native_worker_fails() -> MidgeResult<()> {
    // Arrange
    let panic = false;
    // Act
    let result = assert_terminal_ticket_drain(panic);
    // Assert
    assert!(result.is_ok(), "{result:?}");
    result
}

#[test]
fn should_release_queued_native_tickets_when_worker_panics() -> MidgeResult<()> {
    // Arrange
    let panic = true;
    // Act
    let result = assert_terminal_ticket_drain(panic);
    // Assert
    assert!(result.is_ok(), "{result:?}");
    result
}

fn assert_terminal_ticket_drain(panic: bool) -> MidgeResult<()> {
    // Arrange: first write is running; a second append and sync are queued.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let fs = Arc::new(GatedNoSpaceFs {
        inner: crate::io::MockFs::new(),
        entered: std::sync::Mutex::new(entered_tx),
        release: Arc::new(std::sync::Mutex::new(release_rx)),
    });
    let (writer, store) = native_writer(fs.clone(), Duration::from_secs(5))?;
    let release_gate = ReleaseGate(Some(release_tx));
    writer.set_write_authority(Some(Arc::new(NativeAuthority {
        store: store.clone(),
        epoch: 1,
        reject_after_run: std::sync::atomic::AtomicBool::new(false),
        panic_after_run: std::sync::atomic::AtomicBool::new(panic),
    })));
    let first = {
        let writer = writer.clone();
        std::thread::spawn(move || writer.append_record_accounted(&record(1)))
    };
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let second = {
        let writer = writer.clone();
        std::thread::spawn(move || writer.append_record_accounted(&record(2)))
    };
    wait_until(|| !writer.queue.lock().is_empty());
    let sync = {
        let writer = writer.clone();
        std::thread::spawn(move || writer.sync_with_timeout(writer.io_timeout))
    };
    wait_until(|| !writer.sync_state.lock().requests.is_empty());

    // Act
    release_gate.release();
    let failed = first.join().unwrap().unwrap_err();
    let skipped = second.join().unwrap().unwrap_err();
    let sync_result = sync.join().unwrap();

    // Assert: the writer Arc remains alive while all native tickets drain.
    if panic {
        assert!(matches!(
            failed.error,
            crate::common::MidgeError::Internal(_)
        ));
    } else {
        assert!(matches!(
            failed.error,
            crate::common::MidgeError::NoSpace(_)
        ));
    }
    assert!(skipped.unchanged);
    if panic {
        assert!(matches!(
            sync_result,
            Err(crate::common::MidgeError::Internal(_))
        ));
    } else {
        assert!(matches!(
            sync_result,
            Err(crate::common::MidgeError::NoSpace(_))
        ));
    }
    assert!(writer.queue.lock().is_empty());
    assert!(writer.sync_state.lock().requests.is_empty());
    assert!(store.acquire_leadership("successor").unwrap().epoch > 1);
    assert_eq!(
        fs.metadata(&FsPath::new("wal.log")).unwrap().len,
        HEADER_LEN as u64
    );
    Ok(())
}

#[test]
fn should_report_unchanged_batch_when_a_child_validation_fails_after_its_noop_callback(
) -> MidgeResult<()> {
    // Arrange: both requests share one authority and native grant.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let fs = Arc::new(SyncCountingFs {
        open_gate: Some(Arc::new((entered_tx, std::sync::Mutex::new(release_rx)))),
        ..SyncCountingFs::default()
    });
    let (writer, store) = native_writer(fs.clone(), Duration::from_secs(2))?;
    let release_gate = ReleaseGate(Some(release_tx));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    writer.set_write_authority(Some(Arc::new(NativeAuthority {
        store: store.clone(),
        epoch: 1,
        reject_after_run: std::sync::atomic::AtomicBool::new(true),
        panic_after_run: std::sync::atomic::AtomicBool::new(false),
    })));
    let first = {
        let writer = writer.clone();
        std::thread::spawn(move || writer.append_record_accounted(&record(1)))
    };
    wait_until(|| writer.queue.lock().len() == 1);
    let second = {
        let writer = writer.clone();
        std::thread::spawn(move || writer.append_record_accounted(&record(2)))
    };
    wait_until(|| writer.queue.lock().len() == 2);

    // Act: the extra ticket's validation runs a no-op, then reports failure.
    release_gate.release();
    let first = first.join().unwrap().unwrap_err();
    let second = second.join().unwrap().unwrap_err();

    // Assert: no real write callback began for either coalesced request.
    assert!(first.unchanged && second.unchanged);
    assert!(matches!(
        first.error,
        crate::common::MidgeError::LeaseUnavailable(_)
    ));
    assert_eq!(writer.current_pos(), HEADER_LEN as u64);
    assert_eq!(
        fs.metadata(&FsPath::new("wal.log")).unwrap().len,
        HEADER_LEN as u64
    );
    assert!(store.acquire_leadership("successor").unwrap().epoch > 1);
    Ok(())
}
