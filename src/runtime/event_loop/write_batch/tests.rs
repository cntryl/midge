use super::*;
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::event_loop::EventLoop;
use crate::runtime::state::RuntimeState;
use crate::runtime::{ConflictPolicy, ResponseRouter, RuntimeConfig};
use crate::wal::DurabilityPolicy;
use bytes::Bytes;
use std::sync::Arc;
#[cfg(feature = "failpoints")]
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tempfile::TempDir;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);
const NO_RESPONSE_TIMEOUT: Duration = Duration::from_millis(25);

#[cfg(feature = "failpoints")]
static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

struct EventLoopFixture {
    _temp_dir: TempDir,
    event_loop: EventLoop,
    router: Arc<ResponseRouter>,
}

impl EventLoopFixture {
    fn batched() -> MidgeResult<Self> {
        Self::with_policy(DurabilityPolicy::Batched)
    }

    fn with_policy(policy: DurabilityPolicy) -> MidgeResult<Self> {
        let temp_dir = tempfile::tempdir().map_err(MidgeError::Io)?;
        let state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
        let router = Arc::new(ResponseRouter::new());
        let event_loop = EventLoop::new(
            state,
            false,
            Arc::clone(&router),
            RuntimeConfig {
                wal_durability_policy: policy,
                ..RuntimeConfig::default()
            },
            None,
        )?;

        Ok(Self {
            _temp_dir: temp_dir,
            event_loop,
            router,
        })
    }

    fn register(&self, request_id: u64) -> crossbeam::channel::Receiver<RuntimeResponse> {
        self.router.register(request_id)
    }
}

#[cfg(feature = "failpoints")]
fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(feature = "failpoints")]
struct TxnAppendBatchNoSpaceFailpointGuard {
    _test_guard: crate::failpoints::TestFailpointGuard,
}

#[cfg(feature = "failpoints")]
impl TxnAppendBatchNoSpaceFailpointGuard {
    fn setup(request_id: u64) -> Self {
        let test_guard = crate::failpoints::test_failpoint_guard();
        crate::runtime::actors::wal::set_txn_append_batch_no_space_failpoint_request_id(Some(
            request_id,
        ));
        fail::cfg("midge::wal::inject_no_space_on_txn_append_batch", "return")
            .expect("configure txn append batch no-space failpoint");
        Self {
            _test_guard: test_guard,
        }
    }
}

#[cfg(feature = "failpoints")]
impl Drop for TxnAppendBatchNoSpaceFailpointGuard {
    fn drop(&mut self) {
        fail::remove("midge::wal::inject_no_space_on_txn_append_batch");
        crate::runtime::actors::wal::set_txn_append_batch_no_space_failpoint_request_id(None);
    }
}

fn txn_request(
    request_id: u64,
    ops: Vec<TransactionOp>,
    durability_policy: Option<DurabilityPolicy>,
) -> ApplyTransactionRequest {
    ApplyTransactionRequest {
        request_id,
        ops,
        durability_policy,
        start_sequence: None,
        conflict_policy: ConflictPolicy::LastWriteWins,
    }
}

fn txn_msg(
    request_id: u64,
    ops: Vec<TransactionOp>,
    durability_policy: Option<DurabilityPolicy>,
) -> RuntimeMsg {
    RuntimeMsg::ApplyTransaction {
        request_id,
        ops,
        durability_policy,
        start_sequence: None,
        conflict_policy: ConflictPolicy::LastWriteWins,
        response_tx: None,
    }
}

fn inline_txn_msg(
    request_id: u64,
    ops: Vec<TransactionOp>,
    durability_policy: Option<DurabilityPolicy>,
) -> (RuntimeMsg, crossbeam::channel::Receiver<RuntimeResponse>) {
    let (response_tx, response_rx) = crossbeam::channel::bounded(1);
    (
        RuntimeMsg::ApplyTransaction {
            request_id,
            ops,
            durability_policy,
            start_sequence: None,
            conflict_policy: ConflictPolicy::LastWriteWins,
            response_tx: Some(response_tx),
        },
        response_rx,
    )
}

fn put_op(
    cf_id: crate::types::ColumnFamilyId,
    key: &'static [u8],
    value: &'static [u8],
) -> TransactionOp {
    TransactionOp::Put {
        cf_id,
        key: Bytes::from_static(key),
        value: Bytes::from_static(value),
        ttl_seconds: None,
        insert_only: false,
    }
}

fn insert_only_put_op(
    cf_id: crate::types::ColumnFamilyId,
    key: &'static [u8],
    value: &'static [u8],
) -> TransactionOp {
    TransactionOp::Put {
        cf_id,
        key: Bytes::from_static(key),
        value: Bytes::from_static(value),
        ttl_seconds: None,
        insert_only: true,
    }
}

fn ttl_put_op(
    cf_id: crate::types::ColumnFamilyId,
    key: &'static [u8],
    value: &'static [u8],
    ttl_seconds: u64,
) -> TransactionOp {
    TransactionOp::Put {
        cf_id,
        key: Bytes::from_static(key),
        value: Bytes::from_static(value),
        ttl_seconds: Some(ttl_seconds),
        insert_only: false,
    }
}

fn delete_op(cf_id: crate::types::ColumnFamilyId, key: &'static [u8]) -> TransactionOp {
    TransactionOp::Delete {
        cf_id,
        key: Bytes::from_static(key),
    }
}

fn delete_range_op(
    cf_id: crate::types::ColumnFamilyId,
    start_key: &'static [u8],
    end_key: &'static [u8],
) -> TransactionOp {
    TransactionOp::DeleteRange {
        cf_id,
        start_key: Bytes::from_static(start_key),
        end_key: Bytes::from_static(end_key),
    }
}

fn recv_response(rx: &crossbeam::channel::Receiver<RuntimeResponse>) -> RuntimeResponse {
    rx.recv_timeout(RESPONSE_TIMEOUT)
        .expect("response should arrive before timeout")
}

fn expect_transaction_applied(
    rx: &crossbeam::channel::Receiver<RuntimeResponse>,
    expected_request_id: u64,
) -> (u64, usize) {
    match recv_response(rx) {
        RuntimeResponse::TransactionApplied {
            request_id,
            last_sequence,
            op_count,
            ..
        } => {
            assert_eq!(request_id, expected_request_id);
            (last_sequence, op_count)
        }
        other => panic!("unexpected response for {expected_request_id}: {other:?}"),
    }
}

fn expect_ok(rx: &crossbeam::channel::Receiver<RuntimeResponse>, expected_request_id: u64) {
    match recv_response(rx) {
        RuntimeResponse::Ok { request_id } => {
            assert_eq!(request_id, expected_request_id);
        }
        other => panic!("unexpected response for {expected_request_id}: {other:?}"),
    }
}

fn expect_error<F>(
    rx: &crossbeam::channel::Receiver<RuntimeResponse>,
    expected_request_id: u64,
    predicate: F,
) where
    F: FnOnce(&MidgeError) -> bool,
{
    match recv_response(rx) {
        RuntimeResponse::Error { request_id, error } => {
            assert_eq!(request_id, expected_request_id);
            assert!(
                predicate(&error),
                "unexpected error for {expected_request_id}: {error:?}"
            );
        }
        other => panic!("unexpected response for {expected_request_id}: {other:?}"),
    }
}

fn assert_no_response(rx: &crossbeam::channel::Receiver<RuntimeResponse>) {
    match rx.recv_timeout(NO_RESPONSE_TIMEOUT) {
        Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
            panic!("response channel disconnected before timeout")
        }
        Ok(response) => panic!("unexpected response: {response:?}"),
    }
}

fn assert_no_additional_response(rx: &crossbeam::channel::Receiver<RuntimeResponse>) {
    match rx.recv_timeout(NO_RESPONSE_TIMEOUT) {
        Err(
            crossbeam::channel::RecvTimeoutError::Timeout
            | crossbeam::channel::RecvTimeoutError::Disconnected,
        ) => {}
        Ok(response) => panic!("unexpected additional response: {response:?}"),
    }
}

#[test]
fn should_fence_runtime_when_storage_acknowledgement_times_out() -> MidgeResult<()> {
    // Arrange
    let temp_dir = tempfile::tempdir().map_err(MidgeError::Io)?;
    let state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
    let router = Arc::new(ResponseRouter::new());
    let lease_healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut event_loop = EventLoop::new(
        state,
        false,
        Arc::clone(&router),
        RuntimeConfig {
            lease_healthy: Some(Arc::clone(&lease_healthy)),
            ..RuntimeConfig::default()
        },
        None,
    )?;
    let response = router.register(1);

    // Act
    event_loop.handle_write_error(
        1,
        MidgeError::Timeout("injected storage timeout".to_string()),
    );

    // Assert
    expect_error(&response, 1, |error| {
        matches!(error, MidgeError::Timeout(_))
    });
    assert!(event_loop.state.persistence_anomaly_detected());
    assert!(!lease_healthy.load(std::sync::atomic::Ordering::Acquire));
    assert!(matches!(
        event_loop.check_lease_health(),
        Err(MidgeError::Fenced(_))
    ));
    Ok(())
}

fn assert_memtable_value(
    event_loop: &EventLoop,
    cf_id: crate::types::ColumnFamilyId,
    key: &[u8],
    expected: Option<&[u8]>,
) {
    let snapshot = event_loop
        .create_read_snapshot(cf_id)
        .expect("column family should exist");
    let actual = snapshot
        .get(key, u64::MAX)
        .expect("snapshot lookup should succeed");
    assert_eq!(actual.as_deref(), expected);
}

fn seed_memtable_value(
    event_loop: &mut EventLoop,
    cf_id: crate::types::ColumnFamilyId,
    key: &[u8],
    value: &[u8],
    sequence: u64,
) -> MidgeResult<()> {
    event_loop
        .state
        .get_cf(cf_id)
        .expect("column family should exist")
        .memtable
        .put_with_seq(key.to_vec(), value.to_vec(), sequence, None)
}

#[test]
fn should_coalesce_independent_buffered_transactions_into_one_wal_append() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(10);
    let second_rx = fixture.register(11);
    let third_rx = fixture.register(12);

    msg_tx
        .send(txn_msg(
            11,
            vec![put_op(0, b"coalesce-b", b"value-b")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue second transaction");
    msg_tx
        .send(txn_msg(
            12,
            vec![put_op(0, b"coalesce-c", b"value-c")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue third transaction");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            10,
            vec![put_op(0, b"coalesce-a", b"value-a")],
            Some(DurabilityPolicy::Batched),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 3);
    assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
    assert_eq!(fixture.event_loop.state.wal.pending_writes, 3);
    assert_eq!(fixture.event_loop.wal_actor.pending_sync_count(), 3);
    assert_eq!(fixture.event_loop.state.pending_txn_min_seq, Some(1));

    assert_eq!(expect_transaction_applied(&first_rx, 10), (3, 1));
    assert_eq!(expect_transaction_applied(&second_rx, 11), (6, 1));
    assert_eq!(expect_transaction_applied(&third_rx, 12), (9, 1));

    assert_memtable_value(&fixture.event_loop, 0, b"coalesce-a", Some(b"value-a"));
    assert_memtable_value(&fixture.event_loop, 0, b"coalesce-b", Some(b"value-b"));
    assert_memtable_value(&fixture.event_loop, 0, b"coalesce-c", Some(b"value-c"));

    Ok(())
}

#[test]
fn should_coalesce_mixed_transaction_contents_across_column_families() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let secondary_cf = fixture
        .event_loop
        .state
        .create_cf("secondary".to_string())?;
    seed_memtable_value(&mut fixture.event_loop, 0, b"delete-me", b"old", 1)?;
    seed_memtable_value(&mut fixture.event_loop, secondary_cf, b"range-m", b"old", 2)?;
    seed_memtable_value(
        &mut fixture.event_loop,
        secondary_cf,
        b"range-z",
        b"keep",
        3,
    )?;
    fixture.event_loop.state.sequence = 10;

    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(20);
    let second_rx = fixture.register(21);
    let third_rx = fixture.register(22);
    let fourth_rx = fixture.register(23);

    msg_tx
        .send(txn_msg(
            21,
            vec![insert_only_put_op(secondary_cf, b"insert-key", b"inserted")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue insert-only transaction");
    msg_tx
        .send(txn_msg(
            22,
            vec![delete_op(0, b"delete-me")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue delete transaction");
    msg_tx
        .send(txn_msg(
            23,
            vec![delete_range_op(secondary_cf, b"range-a", b"range-z")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue delete-range transaction");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            20,
            vec![ttl_put_op(0, b"ttl-key", b"ttl-value", 600)],
            Some(DurabilityPolicy::Batched),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 4);
    assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
    assert_eq!(fixture.event_loop.state.wal.pending_writes, 4);

    assert_eq!(expect_transaction_applied(&first_rx, 20).1, 1);
    assert_eq!(expect_transaction_applied(&second_rx, 21).1, 1);
    assert_eq!(expect_transaction_applied(&third_rx, 22).1, 1);
    assert_eq!(expect_transaction_applied(&fourth_rx, 23).1, 1);

    assert_memtable_value(&fixture.event_loop, 0, b"ttl-key", Some(b"ttl-value"));
    assert_memtable_value(
        &fixture.event_loop,
        secondary_cf,
        b"insert-key",
        Some(b"inserted"),
    );
    assert_memtable_value(&fixture.event_loop, 0, b"delete-me", None);
    assert_memtable_value(&fixture.event_loop, secondary_cf, b"range-m", None);
    assert_memtable_value(&fixture.event_loop, secondary_cf, b"range-z", Some(b"keep"));

    Ok(())
}

#[test]
fn should_fall_back_from_coalescing_when_point_range_overlap() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(30);
    let fallback_rx = fixture.register(31);

    msg_tx
        .send(txn_msg(
            31,
            vec![delete_range_op(0, b"overlap-a", b"overlap-z")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue overlapping delete range");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            30,
            vec![put_op(0, b"overlap-m", b"value")],
            Some(DurabilityPolicy::Batched),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(fixture.event_loop.wal_actor.append_calls(), 2);
    assert_eq!(expect_transaction_applied(&first_rx, 30).1, 1);
    assert_eq!(expect_transaction_applied(&fallback_rx, 31).1, 1);
    assert_memtable_value(&fixture.event_loop, 0, b"overlap-m", None);

    Ok(())
}

#[test]
fn should_return_inline_responses_when_same_key_lww_transaction_falls_back_from_coalescing(
) -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let (first_msg, first_rx) = inline_txn_msg(
        34,
        vec![put_op(0, b"same-key-inline", b"first")],
        Some(DurabilityPolicy::Batched),
    );
    let (fallback_msg, fallback_rx) = inline_txn_msg(
        35,
        vec![put_op(0, b"same-key-inline", b"second")],
        Some(DurabilityPolicy::Batched),
    );

    msg_tx.send(first_msg).expect("queue first transaction");
    msg_tx
        .send(fallback_msg)
        .expect("queue same-key fallback transaction");

    // Act
    let handled = fixture.event_loop.drain_pending_writes(&msg_rx, 1024);

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(fixture.event_loop.wal_actor.append_calls(), 2);
    assert_eq!(expect_transaction_applied(&first_rx, 34).1, 1);
    assert_eq!(expect_transaction_applied(&fallback_rx, 35).1, 1);
    assert_no_additional_response(&first_rx);
    assert_no_additional_response(&fallback_rx);
    assert_memtable_value(&fixture.event_loop, 0, b"same-key-inline", Some(b"second"));

    Ok(())
}

#[test]
fn should_return_inline_error_when_same_key_insert_only_fallback_rejects_from_coalescing(
) -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let (first_msg, first_rx) = inline_txn_msg(
        36,
        vec![put_op(0, b"same-key-insert-inline", b"first")],
        Some(DurabilityPolicy::Batched),
    );
    let (fallback_msg, fallback_rx) = inline_txn_msg(
        37,
        vec![insert_only_put_op(0, b"same-key-insert-inline", b"second")],
        Some(DurabilityPolicy::Batched),
    );

    msg_tx.send(first_msg).expect("queue first transaction");
    msg_tx
        .send(fallback_msg)
        .expect("queue same-key insert-only fallback transaction");

    // Act
    let handled = fixture.event_loop.drain_pending_writes(&msg_rx, 1024);

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
    assert_eq!(expect_transaction_applied(&first_rx, 36).1, 1);
    expect_error(
        &fallback_rx,
        37,
        |error| matches!(error, MidgeError::InvalidArgument(message) if message == "key already exists"),
    );
    assert_no_additional_response(&first_rx);
    assert_no_additional_response(&fallback_rx);
    assert_memtable_value(
        &fixture.event_loop,
        0,
        b"same-key-insert-inline",
        Some(b"first"),
    );

    Ok(())
}

#[test]
fn should_coalesce_across_snapshot_bookkeeping_messages() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(42);
    let register_rx = fixture.register(43);
    let second_rx = fixture.register(44);

    msg_tx
        .send(RuntimeMsg::RegisterSnapshot {
            request_id: 43,
            snapshot_id: 900,
            sequence: 0,
            pinned_sst_names: Vec::new(),
        })
        .expect("queue snapshot registration");
    msg_tx
        .send(txn_msg(
            44,
            vec![put_op(0, b"after-register", b"value-b")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue second transaction");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            42,
            vec![put_op(0, b"before-register", b"value-a")],
            Some(DurabilityPolicy::Batched),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
    assert_eq!(expect_transaction_applied(&first_rx, 42).1, 1);
    expect_ok(&register_rx, 43);
    assert_eq!(expect_transaction_applied(&second_rx, 44).1, 1);
    assert_memtable_value(&fixture.event_loop, 0, b"before-register", Some(b"value-a"));
    assert_memtable_value(&fixture.event_loop, 0, b"after-register", Some(b"value-b"));

    Ok(())
}

#[test]
fn should_not_drop_non_write_after_coalescing_stashes_pending_message() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let (txn_msg, txn_rx) = inline_txn_msg(
        38,
        vec![put_op(0, b"coalesce-before-non-write", b"value")],
        Some(DurabilityPolicy::Batched),
    );

    msg_tx.send(txn_msg).expect("queue transaction");
    msg_tx
        .send(RuntimeMsg::Noop { request_id: 39 })
        .expect("queue first non-write");
    msg_tx
        .send(RuntimeMsg::Noop { request_id: 40 })
        .expect("queue second non-write");

    // Act
    let handled = fixture.event_loop.drain_pending_writes(&msg_rx, 1024);

    // Assert
    assert_eq!(handled, 1);
    assert_eq!(expect_transaction_applied(&txn_rx, 38).1, 1);
    match fixture.event_loop.pending_msg.take() {
        Some(RuntimeMsg::Noop { request_id }) => assert_eq!(request_id, 39),
        other => panic!("expected first non-write to be stashed, got {other:?}"),
    }
    match msg_rx.try_recv() {
        Ok(RuntimeMsg::Noop { request_id }) => assert_eq!(request_id, 40),
        other => panic!("expected second non-write to remain queued, got {other:?}"),
    }

    Ok(())
}

#[test]
fn should_error_insert_only_fallback_after_first_transaction_publishes() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(40);
    let fallback_rx = fixture.register(41);

    msg_tx
        .send(txn_msg(
            41,
            vec![insert_only_put_op(0, b"insert-conflict", b"second")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue overlapping insert-only transaction");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            40,
            vec![put_op(0, b"insert-conflict", b"first")],
            Some(DurabilityPolicy::Batched),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
    assert_eq!(expect_transaction_applied(&first_rx, 40).1, 1);
    expect_error(
        &fallback_rx,
        41,
        |error| matches!(error, MidgeError::InvalidArgument(message) if message == "key already exists"),
    );
    assert_memtable_value(&fixture.event_loop, 0, b"insert-conflict", Some(b"first"));

    Ok(())
}

#[test]
fn should_not_enter_coalesced_path_for_non_buffered_durability() -> MidgeResult<()> {
    for (policy_name, durability_policy, expected_append_calls) in [
        ("strict", DurabilityPolicy::Strict, 1),
        ("best_effort", DurabilityPolicy::BestEffort, 0),
        ("cloud_effective", DurabilityPolicy::CloudAsync, 1),
    ] {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let first_rx = fixture.register(50);
        let queued_rx = fixture.register(51);
        msg_tx
            .send(txn_msg(
                51,
                vec![put_op(0, b"queued-buffered", b"value")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue transaction that must not be drained");

        // Act
        let handled = fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                50,
                vec![put_op(0, b"non-coalesced", b"value")],
                Some(durability_policy),
            ),
            1024,
        );

        // Assert
        assert_eq!(
            handled, 1,
            "{policy_name} should handle only the initial request"
        );
        assert_eq!(
            fixture.event_loop.wal_actor.append_calls(),
            expected_append_calls,
            "{policy_name} should not batch with the queued request"
        );
        assert_eq!(expect_transaction_applied(&first_rx, 50).1, 1);
        assert_no_response(&queued_rx);
    }

    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fail_all_event_loop_buffered_transactions_when_append_hits_no_space() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(60);
    let second_rx = fixture.register(61);

    msg_tx
        .send(txn_msg(
            61,
            vec![put_op(0, b"failed-b", b"value-b")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue second transaction");

    {
        let scenario = fail::FailScenario::setup();
        let failpoint_guard = TxnAppendBatchNoSpaceFailpointGuard::setup(60);

        // Act
        let handled = fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                60,
                vec![put_op(0, b"failed-a", b"value-a")],
                Some(DurabilityPolicy::Batched),
            ),
            1024,
        );

        // Assert
        assert_eq!(handled, 2);
        expect_error(&first_rx, 60, |error| {
            matches!(error, MidgeError::NoSpace(_))
        });
        expect_error(&second_rx, 61, |error| {
            matches!(error, MidgeError::NoSpace(_))
        });
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 0);
        assert_eq!(fixture.event_loop.state.wal.pending_writes, 0);
        assert_eq!(fixture.event_loop.wal_actor.pending_sync_count(), 0);
        assert_eq!(fixture.event_loop.wal_actor.bytes_since_sync(), 0);
        assert_eq!(fixture.event_loop.state.pending_txn_min_seq, None);
        assert!(fixture
            .event_loop
            .state
            .get_cf(0)
            .expect("default column family should exist")
            .memtable
            .iter_all(u64::MAX)
            .is_empty());
        assert!(
            fixture.event_loop.state.sequence > 0,
            "failed preparation may leave sequence gaps"
        );

        drop(failpoint_guard);
        scenario.teardown();
    }

    let recovery_rx = fixture.register(62);
    let (_unused_tx, empty_rx) = crossbeam::channel::unbounded();
    let recovery_handled = fixture.event_loop.apply_transaction_with_coalescing(
        &empty_rx,
        txn_request(
            62,
            vec![put_op(0, b"recovered", b"value")],
            Some(DurabilityPolicy::Batched),
        ),
        1024,
    );

    assert_eq!(recovery_handled, 1);
    assert_eq!(expect_transaction_applied(&recovery_rx, 62).1, 1);
    assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
    assert_memtable_value(&fixture.event_loop, 0, b"recovered", Some(b"value"));

    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fail_same_key_fallback_when_coalesced_prefix_append_fails() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let (first_msg, first_rx) = inline_txn_msg(
        80,
        vec![put_op(0, b"failed-same-key", b"first")],
        Some(DurabilityPolicy::Batched),
    );
    let (fallback_msg, fallback_rx) = inline_txn_msg(
        81,
        vec![put_op(0, b"failed-same-key", b"second")],
        Some(DurabilityPolicy::Batched),
    );

    msg_tx.send(first_msg).expect("queue first transaction");
    msg_tx
        .send(fallback_msg)
        .expect("queue same-key fallback transaction");

    {
        let scenario = fail::FailScenario::setup();
        let failpoint_guard = TxnAppendBatchNoSpaceFailpointGuard::setup(80);

        // Act
        let handled = fixture.event_loop.drain_pending_writes(&msg_rx, 1024);

        // Assert
        assert_eq!(handled, 2);
        expect_error(&first_rx, 80, |error| {
            matches!(error, MidgeError::NoSpace(_))
        });
        expect_error(&fallback_rx, 81, |error| {
            matches!(error, MidgeError::NoSpace(_))
        });
        assert_no_additional_response(&first_rx);
        assert_no_additional_response(&fallback_rx);
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 0);
        assert_eq!(fixture.event_loop.state.wal.pending_writes, 0);
        assert_memtable_value(&fixture.event_loop, 0, b"failed-same-key", None);

        drop(failpoint_guard);
        scenario.teardown();
    }

    Ok(())
}

#[test]
fn should_detect_overlapping_point_range_touches_when_staging_transactions() {
    // Arrange
    let mut touches = StagedTransactionTouches::default();
    touches.record_ops(&[
        TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(b"point"),
            value: Bytes::from_static(b"value"),
            ttl_seconds: None,
            insert_only: false,
        },
        TransactionOp::DeleteRange {
            cf_id: 0,
            start_key: Bytes::from_static(b"range-a"),
            end_key: Bytes::from_static(b"range-z"),
        },
    ]);

    // Act
    // Assert
    assert!(touches.touches_ops(&[TransactionOp::Delete {
        cf_id: 0,
        key: Bytes::from_static(b"point"),
    }]));
    assert!(touches.touches_ops(&[TransactionOp::Put {
        cf_id: 0,
        key: Bytes::from_static(b"range-m"),
        value: Bytes::from_static(b"value"),
        ttl_seconds: None,
        insert_only: true,
    }]));
    assert!(touches.touches_ops(&[TransactionOp::DeleteRange {
        cf_id: 0,
        start_key: Bytes::from_static(b"range-y"),
        end_key: Bytes::from_static(b"zz"),
    }]));
    assert!(!touches.touches_ops(&[TransactionOp::Put {
        cf_id: 1,
        key: Bytes::from_static(b"point"),
        value: Bytes::from_static(b"value"),
        ttl_seconds: None,
        insert_only: false,
    }]));
    assert!(!touches.touches_ops(&[TransactionOp::DeleteRange {
        cf_id: 0,
        start_key: Bytes::from_static(b"range-z"),
        end_key: Bytes::from_static(b"zz"),
    }]));
}

#[test]
fn should_remove_cancelled_write_stall_waiter_from_all_indexes() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    fixture.event_loop.state.set_write_stalled(true);
    fixture.event_loop.handle_wait_for_write_stall_clear(900, 0);
    assert_eq!(fixture.event_loop.write_stall_waiters.get(&900), Some(&0));
    assert_eq!(
        fixture
            .event_loop
            .write_stall_waiter_queues
            .get(&0)
            .map(std::collections::VecDeque::len),
        Some(1)
    );

    // Act
    fixture
        .event_loop
        .handle_cancel_wait_for_write_stall_clear(900);

    // Assert
    assert!(!fixture.event_loop.write_stall_waiters.contains_key(&900));
    assert!(
        fixture
            .event_loop
            .write_stall_waiter_queues
            .get(&0)
            .is_none_or(std::collections::VecDeque::is_empty),
        "cancellation must remove the request from both waiter indexes"
    );
    Ok(())
}

#[test]
fn should_report_stall_hint_for_transaction_actual_column_family() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let secondary_cf = fixture
        .event_loop
        .state
        .create_cf("stalled-secondary".to_string())?;
    fixture.event_loop.state.max_immutable_memtables = 1;
    fixture
        .event_loop
        .state
        .get_cf_mut(secondary_cf)
        .expect("secondary column family")
        .immutable_memtables
        .push(Arc::new(crate::sst::SkipListMemtable::new()));
    assert!(!fixture.event_loop.state.should_stall_writes(0));
    assert!(fixture.event_loop.state.should_stall_writes(secondary_cf));
    let response_rx = fixture.register(901);
    let (_tx, empty_rx) = crossbeam::channel::unbounded();

    // Act
    fixture.event_loop.apply_transaction_with_coalescing(
        &empty_rx,
        txn_request(
            901,
            vec![put_op(secondary_cf, b"secondary-key", b"value")],
            Some(DurabilityPolicy::Batched),
        ),
        1,
    );

    // Assert
    match recv_response(&response_rx) {
        RuntimeResponse::TransactionApplied {
            write_stall_hint, ..
        } => assert!(
            write_stall_hint,
            "response must compute pressure from the transaction's CF, not CF 0"
        ),
        other => panic!("unexpected response: {other:?}"),
    }
    Ok(())
}
