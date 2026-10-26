use super::*;
use crate::runtime::event_loop::flush::FlushCoordinator;
use crate::sst::read_path_metrics::SstReadObserver as _;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

mod spilled_tests;

struct HeldCallback {
    release: Sender<()>,
    reached: Receiver<()>,
    calls: Arc<AtomicUsize>,
    worker_thread: Arc<std::sync::Mutex<Option<std::thread::ThreadId>>>,
    hook: &'static str,
}

impl HeldCallback {
    fn first(hook: &'static str) -> Self {
        let (entered, reached) = crossbeam::channel::bounded(1);
        let (release, released) = crossbeam::channel::unbounded();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let worker_thread = Arc::new(std::sync::Mutex::new(None));
        let observed_thread = Arc::clone(&worker_thread);
        fail::cfg_callback(hook, move || {
            if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                *observed_thread
                    .lock()
                    .expect("validation worker thread lock") = Some(std::thread::current().id());
                let _ = entered.send(());
                released
                    .recv_timeout(Duration::from_secs(30))
                    .expect("release held validation test stage");
            }
        })
        .expect("hold first validation test stage");
        Self {
            release,
            reached,
            calls,
            worker_thread,
            hook,
        }
    }

    fn worker_thread(&self) -> std::thread::ThreadId {
        self.worker_thread
            .lock()
            .expect("validation worker thread lock")
            .expect("validation failpoint records its worker thread")
    }
}

impl Drop for HeldCallback {
    fn drop(&mut self) {
        let _ = self.release.send(());
        fail::remove(self.hook);
    }
}

fn request(request_id: u64, key: &'static [u8], checked: bool) -> ApplyTransactionRequest {
    ApplyTransactionRequest {
        request_id,
        ops: vec![TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(key),
            value: Bytes::from_static(b"value"),
            ttl_seconds: None,
            insert_only: false,
        }],
        assertions: Vec::new(),
        durability_policy: Some(crate::wal::DurabilityPolicy::Strict),
        start_sequence: checked.then_some(0),
        conflict_policy: if checked {
            ConflictPolicy::AbortOnWriteConflict
        } else {
            ConflictPolicy::LastWriteWins
        },
        validation: None,
    }
}

fn apply_and_flush(event_loop: &mut EventLoop, request: ApplyTransactionRequest) {
    let request_id = request.request_id;
    let response = event_loop
        .routing
        .router
        .register(request_id, "validation fixture write");
    WalCoordinator::apply_transaction(event_loop, &crossbeam::channel::never(), request, None);
    assert!(matches!(
        response.recv_timeout(Duration::from_secs(10)),
        Ok(RuntimeResponse::TransactionApplied { .. })
    ));
    let flush = event_loop
        .routing
        .router
        .register(request_id + 100, "validation fixture flush");
    FlushCoordinator::flush_memtable(event_loop, request_id + 100, 0);
    let flush_result = flush.recv_timeout(Duration::from_secs(10));
    assert!(
        matches!(flush_result, Ok(RuntimeResponse::Ok { .. })),
        "unexpected validation fixture flush result: {flush_result:?}"
    );
    assert!(!event_loop.state.manifest.files.is_empty());
}

struct SstReadOpenCounter {
    inner: crate::io::RealFs,
    event_loop_thread: std::thread::ThreadId,
    validation_worker_thread: std::sync::Mutex<Option<std::thread::ThreadId>>,
    event_loop_opens: AtomicUsize,
    worker_opens: AtomicUsize,
    unrelated_opens: AtomicUsize,
}

impl crate::io::ReadFs for SstReadOpenCounter {
    fn open_read(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<Box<dyn crate::io::ReadFile + '_>> {
        crate::io::Fs::open(self, path, crate::io::OpenOptions::READ_ONLY)
            .map(|file| file as Box<dyn crate::io::ReadFile>)
    }

    fn exists(&self, path: &crate::io::FsPath) -> crate::io::FsResult<bool> {
        self.inner.exists(path)
    }

    fn metadata(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<crate::io::traits::Metadata> {
        self.inner.metadata(path)
    }

    fn list_dir(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<Vec<crate::io::traits::DirEntry>> {
        self.inner.list_dir(path)
    }
}

impl SstReadOpenCounter {
    fn record_read_open(&self, path: &crate::io::FsPath, options: crate::io::OpenOptions) {
        if options.mode == crate::io::OpenMode::ReadOnly
            && std::path::Path::new(&path.0)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("sst"))
        {
            let current_thread = std::thread::current().id();
            let opens = if current_thread == self.event_loop_thread {
                &self.event_loop_opens
            } else if self
                .validation_worker_thread
                .lock()
                .expect("validation worker thread lock")
                .is_some_and(|worker| worker == current_thread)
            {
                &self.worker_opens
            } else {
                &self.unrelated_opens
            };
            opens.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl crate::io::Fs for SstReadOpenCounter {
    fn persistent_handle_capability(&self) -> Option<&dyn crate::io::PersistentHandleFs> {
        Some(self)
    }

    fn try_lock_exclusive(
        &self,
        path: &crate::io::FsPath,
    ) -> crate::io::FsResult<Box<dyn crate::io::FsLock>> {
        self.inner.try_lock_exclusive(path)
    }

    fn open(
        &self,
        path: &crate::io::FsPath,
        options: crate::io::OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File + '_>> {
        self.record_read_open(path, options);
        self.inner.open(path, options)
    }

    fn remove_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.inner.remove_file(path)
    }

    fn create_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.inner.create_dir_all(path)
    }

    fn remove_dir_all(&self, path: &crate::io::FsPath) -> crate::io::FsResult<()> {
        self.inner.remove_dir_all(path)
    }

    fn sync_dir(
        &self,
        path: &crate::io::FsPath,
        durability: crate::io::Durability,
    ) -> crate::io::FsResult<()> {
        self.inner.sync_dir(path, durability)
    }

    fn rename_atomic(
        &self,
        from: &crate::io::FsPath,
        to: &crate::io::FsPath,
    ) -> crate::io::FsResult<()> {
        self.inner.rename_atomic(from, to)
    }
}

impl crate::io::PersistentHandleFs for SstReadOpenCounter {
    fn open_persistent(
        &self,
        path: &crate::io::FsPath,
        options: crate::io::OpenOptions,
    ) -> crate::io::FsResult<Box<dyn crate::io::File>> {
        self.record_read_open(path, options);
        crate::io::PersistentHandleFs::open_persistent(&self.inner, path, options)
    }
}

fn create_counted_event_loop(
) -> crate::common::MidgeResult<(tempfile::TempDir, EventLoop, Arc<SstReadOpenCounter>)> {
    let directory = tempfile::tempdir()?;
    let fs = Arc::new(SstReadOpenCounter {
        inner: crate::io::RealFs::new(directory.path()).map_err(crate::io::FsError::into_midge)?,
        event_loop_thread: std::thread::current().id(),
        validation_worker_thread: std::sync::Mutex::new(None),
        event_loop_opens: AtomicUsize::new(0),
        worker_opens: AtomicUsize::new(0),
        unrelated_opens: AtomicUsize::new(0),
    });
    let mut state = crate::runtime::RuntimeState::new(directory.path().to_path_buf(), false);
    state.fs = fs.clone();
    let event_loop = EventLoop::new(
        state,
        false,
        Arc::new(crate::runtime::ResponseRouter::new()),
        crate::runtime::RuntimeConfig {
            retry_policy: crate::runtime::RuntimeRetryPolicy::fast_for_testing(),
            ..crate::runtime::RuntimeConfig::default()
        },
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    Ok((directory, event_loop, fs))
}

#[test]
fn should_retry_detached_validation_when_intervening_conflicting_write_is_flushed(
) -> MidgeResult<()> {
    // Arrange
    let checked = request(94_601, b"conflict", true);
    // Act
    assert_intervening_flushed_write_is_seen(checked, |error| {
        matches!(error, MidgeError::WriteConflict(_))
    })?;
    // Assert: the shared schedule requires the original typed conflict.
    Ok(())
}

#[test]
fn should_select_detached_validation_completion_without_event_loop_sst_reads() -> MidgeResult<()> {
    // Arrange
    let (_directory, mut event_loop, sst_opens) = create_counted_event_loop()?;
    apply_and_flush(&mut event_loop, request(94_600, b"seed", false));
    sst_opens.event_loop_opens.store(0, Ordering::SeqCst);
    let mut checked = request(94_601, b"new-key", true);
    checked.start_sequence = Some(event_loop.state.applied_sequence);
    checked.durability_policy = Some(crate::wal::DurabilityPolicy::BestEffort);
    let router = Arc::clone(&event_loop.routing.router);
    let client_router = Arc::clone(&router);
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let (worker_msg_tx, worker_msg_rx) = crossbeam::channel::unbounded();
    event_loop.pipeline.worker_msg_tx = Some(worker_msg_tx);
    let client_thread = std::thread::spawn(move || {
        let response = client_router.register(94_601, "detached validation completion select");
        msg_tx
            .send(RuntimeMsg::ApplyTransaction {
                request_id: checked.request_id,
                ops: checked.ops,
                assertions: checked.assertions,
                durability_policy: checked.durability_policy,
                start_sequence: checked.start_sequence,
                conflict_policy: checked.conflict_policy,
                response_tx: None,
            })
            .expect("submit checked transaction to event loop");
        let result = response.recv_timeout(Duration::from_secs(10));
        msg_tx
            .send(RuntimeMsg::Shutdown)
            .expect("request event loop shutdown after validation completion");
        result
    });

    // Act
    event_loop.run(&msg_rx, &worker_msg_rx);
    let result = client_thread
        .join()
        .expect("validation client exits after receiving the response");

    // Assert
    assert!(matches!(
        result,
        Ok(RuntimeResponse::TransactionApplied { .. })
    ));
    assert_eq!(router.pending_len(), 0);
    assert_eq!(sst_opens.event_loop_opens.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn should_keep_resident_validation_worker_running_between_completed_jobs() -> MidgeResult<()> {
    // Arrange
    let (_directory, mut event_loop, _) = create_counted_event_loop()?;
    apply_and_flush(&mut event_loop, request(94_619, b"seed", false));
    let first_reservation = event_loop
        .pipeline
        .validation
        .memory
        .reserve(1024, "resident validation worker test")?;
    let first = event_loop
        .capture_validation_job(request(94_620, b"first-key", true), first_reservation)?;
    let second_reservation = event_loop
        .pipeline
        .validation
        .memory
        .reserve(1024, "resident validation worker test")?;
    let second = event_loop
        .capture_validation_job(request(94_621, b"second-key", true), second_reservation)?;
    let worker = ValidationWorker::new()?;
    let requests = worker.requests.as_ref().expect("validation worker open");
    requests
        .send(ValidationTask::Resident(first))
        .expect("send first resident validation");
    requests
        .send(ValidationTask::Resident(second))
        .expect("send second resident validation");

    // Act: leave the first completion unread while the worker processes the second job.
    let first = worker
        .completions
        .recv_timeout(Duration::from_secs(5))
        .expect("first resident validation completion");
    let second = worker
        .completions
        .recv_timeout(Duration::from_secs(5))
        .expect("resident worker continues without a completion acknowledgement");

    // Assert
    assert_eq!(first.request_id(), 94_620);
    assert_eq!(second.request_id(), 94_621);
    Ok(())
}

#[test]
fn should_retry_detached_insert_only_validation_when_intervening_write_is_flushed(
) -> MidgeResult<()> {
    // Arrange
    let mut insert = request(94_601, b"conflict", false);
    if let TransactionOp::Put { insert_only, .. } = &mut insert.ops[0] {
        *insert_only = true;
    }
    // Act
    assert_intervening_flushed_write_is_seen(
        insert,
        |error| matches!(error, MidgeError::InvalidArgument(message) if message == "key already exists"),
    )?;
    // Assert: the shared schedule requires the existing duplicate outcome.
    Ok(())
}

#[test]
fn should_retry_detached_assertion_validation_when_intervening_write_is_flushed() -> MidgeResult<()>
{
    // Arrange
    let mut asserting = request(94_601, b"unrelated", false);
    asserting.assertions.push(crate::runtime::KeyAssertion {
        cf_id: 0,
        key: Bytes::from_static(b"conflict"),
    });
    // Act
    assert_intervening_flushed_write_is_seen(asserting, |error| {
        matches!(error, MidgeError::WriteConflict(_))
    })?;
    // Assert: the shared schedule rejects the stale assertion before allocation.
    Ok(())
}

#[test]
fn should_retry_detached_range_validation_when_intervening_write_is_flushed() -> MidgeResult<()> {
    // Arrange
    let mut deleting = request(94_601, b"unused", true);
    deleting.ops = vec![TransactionOp::DeleteRange {
        cf_id: 0,
        start_key: Bytes::from_static(b"a"),
        end_key: Bytes::from_static(b"z"),
    }];
    // Act
    assert_intervening_flushed_write_is_seen(deleting, |error| {
        matches!(error, MidgeError::WriteConflict(_))
    })?;
    // Assert: the shared schedule detects the whole intervening range conflict.
    Ok(())
}

fn assert_intervening_flushed_write_is_seen(
    mut checked: ApplyTransactionRequest,
    expected_error: fn(&MidgeError) -> bool,
) -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let (_directory, mut event_loop, sst_opens) = create_counted_event_loop()?;
    apply_and_flush(&mut event_loop, request(94_600, b"seed", false));
    checked.start_sequence = Some(event_loop.state.applied_sequence);
    sst_opens.event_loop_opens.store(0, Ordering::SeqCst);
    sst_opens.worker_opens.store(0, Ordering::SeqCst);
    let diagnostics = Arc::clone(&event_loop.state.diagnostics);
    let initial_opens = diagnostics.sst_metrics().reader_cache_misses();
    let held = HeldCallback::first("midge::validation::before_snapshot_reads");
    let response = event_loop
        .routing
        .router
        .register(94_601, "conflict checked commit");

    // Act: no SST opens on the loop while the read worker is held. A blind
    // write and its flush progress, removing the intervening active memtable.
    WalCoordinator::apply_transaction(&mut event_loop, &crossbeam::channel::never(), checked, None);
    held.reached
        .recv_timeout(Duration::from_secs(10))
        .expect("read worker reached hold");
    *sst_opens
        .validation_worker_thread
        .lock()
        .expect("validation worker thread lock") = Some(held.worker_thread());
    assert_eq!(
        sst_opens.event_loop_opens.load(Ordering::SeqCst),
        0,
        "conflict-checked dispatch must not open an SST on the event loop"
    );
    assert_eq!(
        sst_opens.worker_opens.load(Ordering::SeqCst),
        0,
        "the held validation worker has not started SST reads"
    );
    apply_and_flush(&mut event_loop, request(94_602, b"conflict", false));
    assert_eq!(
        diagnostics.sst_metrics().reader_cache_misses(),
        initial_opens
    );
    held.release.send(()).expect("release first read pass");
    let deadline = Instant::now() + Duration::from_secs(10);
    let result = loop {
        event_loop.drain_validation_results(&crossbeam::channel::never());
        if let Ok(response) = response.try_recv() {
            break response;
        }
        assert!(Instant::now() < deadline, "validation did not settle");
        std::thread::sleep(Duration::from_millis(1));
    };

    // Assert: the stale negative read cannot authorize the write. The new
    // source generation detects the conflicting version after its flush.
    assert!(
        matches!(&result, RuntimeResponse::Error { error, .. } if expected_error(error)),
        "unexpected validation result: {result:?}"
    );
    assert!(held.calls.load(Ordering::SeqCst) >= 2);
    assert!(diagnostics.sst_metrics().reader_cache_misses() > initial_opens);
    assert!(
        sst_opens.worker_opens.load(Ordering::SeqCst) > 0,
        "the detached read worker should open the SST after release"
    );
    assert_eq!(event_loop.state.applied_sequence, 6);
    assert_eq!(event_loop.state.sequence, 6);
    let snapshot = event_loop.create_read_snapshot(0).expect("current family");
    assert_eq!(snapshot.get(b"unrelated", 6)?, None);
    assert_eq!(snapshot.get(b"seed", 6)?, Some(b"value".to_vec()));
    assert_eq!(snapshot.get(b"conflict", 6)?, Some(b"value".to_vec()));
    Ok(())
}

#[test]
fn should_retry_detached_validation_after_earlier_pending_group_becomes_visible() -> MidgeResult<()>
{
    // Arrange
    let checked = request(94_601, b"conflict", true);

    // Act
    // Assert: the shared helper checks that the earlier pending write wins.
    assert_pending_group_validation_rejects(checked, |error| {
        matches!(error, MidgeError::WriteConflict(_))
    })
}

#[test]
fn should_retry_detached_insert_only_check_after_earlier_pending_group_becomes_visible(
) -> MidgeResult<()> {
    // Arrange
    let mut checked = request(94_601, b"conflict", false);
    if let TransactionOp::Put { insert_only, .. } = &mut checked.ops[0] {
        *insert_only = true;
    }

    // Act
    // Assert: the shared helper checks insert-only behavior after visibility.
    assert_pending_group_validation_rejects(
        checked,
        |error| matches!(error, MidgeError::InvalidArgument(message) if message == "key already exists"),
    )
}

#[test]
fn should_retry_detached_assertion_after_earlier_pending_group_becomes_visible() -> MidgeResult<()>
{
    // Arrange
    let mut checked = request(94_601, b"unrelated", false);
    checked.assertions.push(crate::runtime::KeyAssertion {
        cf_id: 0,
        key: Bytes::from_static(b"conflict"),
    });

    // Act
    // Assert: the shared helper checks the detached assertion after visibility.
    assert_pending_group_validation_rejects(checked, |error| {
        matches!(error, MidgeError::WriteConflict(_))
    })
}

fn assert_pending_group_validation_rejects(
    mut checked: ApplyTransactionRequest,
    expected_error: fn(&MidgeError) -> bool,
) -> MidgeResult<()> {
    // Arrange: a caller's detached read starts at the fully applied seed cut.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    let checked_id = checked.request_id;
    apply_and_flush(&mut event_loop, request(checked_id - 1, b"seed", false));
    event_loop.pipeline.inline_flush_worker = false;
    let validation = HeldCallback::first("midge::validation::before_snapshot_reads");
    let sync = HeldCallback::first("midge::wal::before_physical_sync");
    let checked_response = event_loop
        .routing
        .router
        .register(checked_id, "pending checked transaction");
    checked.start_sequence = Some(3);
    WalCoordinator::apply_transaction(&mut event_loop, &crossbeam::channel::never(), checked, None);
    validation
        .reached
        .recv_timeout(Duration::from_secs(10))
        .expect("held validation");
    let writer_response = event_loop
        .routing
        .router
        .register(checked_id + 1, "earlier strict group");

    // Act: the blind writer allocates the earlier WAL unit and waits in sync.
    // The completed old proof must queue without acknowledging the caller.
    WalCoordinator::apply_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        request(checked_id + 1, b"conflict", false),
        None,
    );
    wait_for_stage(&mut event_loop, &sync.reached);
    validation.release.send(()).expect("release old validation");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !event_loop.pipeline.validation.has_ready_result() {
        assert!(Instant::now() < deadline, "validation read did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
    event_loop.drain_validation_results(&crossbeam::channel::never());
    assert!(checked_response.try_recv().is_err());
    assert!(writer_response.try_recv().is_err());
    assert_eq!(event_loop.state.applied_sequence, 3);
    let old = event_loop.create_read_snapshot(0).expect("old family view");
    assert_eq!(old.get(b"conflict", 3)?, None);
    sync.release.send(()).expect("release earlier strict unit");
    let writer = settle_validation_request(&mut event_loop, &writer_response);
    let checked = settle_validation_request(&mut event_loop, &checked_response);

    // Assert: the earlier group is whole before retry; rejection allocates no
    // later sequence and preserves the typed outcome for this check.
    assert!(matches!(
        writer,
        RuntimeResponse::TransactionApplied {
            last_sequence: 6,
            ..
        }
    ));
    assert!(
        matches!(&checked, RuntimeResponse::Error { error, .. } if expected_error(error)),
        "unexpected checked transaction response: {checked:?}"
    );
    assert!(validation.calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(event_loop.state.applied_sequence, 6);
    assert_eq!(event_loop.state.sequence, 6);
    assert_eq!(old.get(b"conflict", 3)?, None);
    assert_eq!(old.get(b"conflict", 6)?, Some(b"value".to_vec()));
    Ok(())
}

#[test]
fn should_retry_detached_range_validation_after_earlier_pending_group_becomes_visible(
) -> MidgeResult<()> {
    // Arrange: the range proof starts at the applied seed cut and sees no later key.
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    apply_and_flush(&mut event_loop, request(94_610, b"seed", false));
    event_loop.pipeline.inline_flush_worker = false;
    let validation = HeldCallback::first("midge::validation::before_snapshot_reads");
    let sync = HeldCallback::first("midge::wal::before_physical_sync");
    let checked_response = event_loop
        .routing
        .router
        .register(94_611, "pending range check");
    let mut checked = request(94_611, b"unused", true);
    checked.start_sequence = Some(3);
    checked.ops = vec![TransactionOp::DeleteRange {
        cf_id: 0,
        start_key: Bytes::from_static(b"a"),
        end_key: Bytes::from_static(b"z"),
    }];
    WalCoordinator::apply_transaction(&mut event_loop, &crossbeam::channel::never(), checked, None);
    validation
        .reached
        .recv_timeout(Duration::from_secs(10))
        .expect("held range validation");
    let writer_response = event_loop
        .routing
        .router
        .register(94_612, "earlier in-range write");

    // Act: allocate the earlier conflicting write, hold its sync, then finish
    // the stale empty-range proof against the still-applied seed cut.
    WalCoordinator::apply_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        request(94_612, b"inside", false),
        None,
    );
    wait_for_stage(&mut event_loop, &sync.reached);
    validation
        .release
        .send(())
        .expect("release old range proof");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !event_loop.pipeline.validation.has_ready_result() {
        assert!(Instant::now() < deadline, "range proof did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
    event_loop.drain_validation_results(&crossbeam::channel::never());
    assert!(checked_response.try_recv().is_err());
    assert!(writer_response.try_recv().is_err());
    assert_eq!(event_loop.state.applied_sequence, 3);
    let old = event_loop.create_read_snapshot(0).expect("old family view");
    assert_eq!(old.get(b"inside", 3)?, None);

    sync.release.send(()).expect("release earlier strict group");
    let writer = settle_validation_request(&mut event_loop, &writer_response);
    let checked = settle_validation_request(&mut event_loop, &checked_response);

    // Assert: refreshed range validation rejects the whole delete without
    // allocating any sequence after the earlier write.
    assert!(matches!(
        writer,
        RuntimeResponse::TransactionApplied {
            last_sequence: 6,
            ..
        }
    ));
    assert!(matches!(
        checked,
        RuntimeResponse::Error {
            error: MidgeError::WriteConflict(_),
            ..
        }
    ));
    assert!(validation.calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(event_loop.state.applied_sequence, 6);
    assert_eq!(event_loop.state.sequence, 6);
    assert_eq!(old.get(b"inside", 3)?, None);
    let current = event_loop
        .create_read_snapshot(0)
        .expect("current family view");
    assert_eq!(current.get(b"inside", 6)?, Some(b"value".to_vec()));
    assert_eq!(current.get(b"seed", 6)?, Some(b"value".to_vec()));
    Ok(())
}

fn wait_for_stage(event_loop: &mut EventLoop, reached: &Receiver<()>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        event_loop.progress_wal_pipeline();
        if reached.try_recv().is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "WAL did not reach held stage");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn settle_validation_request(
    event_loop: &mut EventLoop,
    response: &Receiver<RuntimeResponse>,
) -> RuntimeResponse {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        event_loop.progress_wal_pipeline();
        event_loop.drain_validation_results(&crossbeam::channel::never());
        event_loop.process_pending_message(&crossbeam::channel::never());
        if let Ok(response) = response.try_recv() {
            return response;
        }
        assert!(Instant::now() < deadline, "validation did not settle");
        std::thread::sleep(Duration::from_millis(1));
    }
}
