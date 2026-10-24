use super::*;
use crate::runtime::event_loop::flush::FlushCoordinator;
use crate::sst::read_path_metrics::SstReadObserver as _;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

struct ReleaseRead(Sender<()>);

impl Drop for ReleaseRead {
    fn drop(&mut self) {
        let _ = self.0.send(());
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
    assert!(matches!(
        flush.recv_timeout(Duration::from_secs(10)),
        Ok(RuntimeResponse::Ok { .. })
    ));
    assert!(!event_loop.state.manifest.files.is_empty());
}

#[test]
fn should_retry_detached_validation_when_intervening_conflicting_write_is_flushed(
) -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    apply_and_flush(&mut event_loop, request(94_600, b"seed", false));
    let diagnostics = Arc::clone(&event_loop.state.diagnostics);
    let initial_opens = diagnostics.sst_metrics().reader_cache_misses();
    let (entered, reached) = crossbeam::channel::bounded(1);
    let (release, released) = crossbeam::channel::unbounded();
    let release = ReleaseRead(release);
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&reads);
    fail::cfg_callback("midge::validation::before_snapshot_reads", move || {
        if observed.fetch_add(1, Ordering::SeqCst) == 0 {
            let _ = entered.send(());
            released
                .recv_timeout(Duration::from_secs(30))
                .expect("release held validation");
        }
    })
    .expect("pause first read job");
    let response = event_loop
        .routing
        .router
        .register(94_601, "conflict checked commit");

    // Act: no SST opens on the loop while the read worker is held. A blind
    // write and its flush progress, removing the intervening active memtable.
    WalCoordinator::apply_transaction(
        &mut event_loop,
        &crossbeam::channel::never(),
        request(94_601, b"conflict", true),
        None,
    );
    reached
        .recv_timeout(Duration::from_secs(10))
        .expect("read worker reached hold");
    apply_and_flush(&mut event_loop, request(94_602, b"conflict", false));
    assert_eq!(
        diagnostics.sst_metrics().reader_cache_misses(),
        initial_opens
    );
    release.0.send(()).expect("release first read pass");
    let deadline = Instant::now() + Duration::from_secs(10);
    let result = loop {
        event_loop.drain_validation_results(&crossbeam::channel::never());
        if let Ok(response) = response.try_recv() {
            break response;
        }
        assert!(Instant::now() < deadline, "validation did not settle");
        std::thread::sleep(Duration::from_millis(1));
    };
    fail::remove("midge::validation::before_snapshot_reads");

    // Assert: the stale negative read cannot authorize the write. The new
    // source generation detects the conflicting version after its flush.
    assert!(matches!(
        result,
        RuntimeResponse::Error {
            error: MidgeError::WriteConflict(_),
            ..
        }
    ));
    assert!(reads.load(Ordering::SeqCst) >= 2);
    assert!(diagnostics.sst_metrics().reader_cache_misses() > initial_opens);
    assert_eq!(event_loop.state.applied_sequence, 6);
    Ok(())
}
