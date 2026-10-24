use super::*;
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::event_loop::EventLoop;
use crate::runtime::state::RuntimeState;
use crate::runtime::TestRuntimeMsg;
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
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;

        Ok(Self {
            _temp_dir: temp_dir,
            event_loop,
        })
    }

    fn register(&self, request_id: u64) -> crossbeam::channel::Receiver<RuntimeResponse> {
        self.event_loop
            .routing
            .router
            .register(request_id, "TestRequest")
    }
}

#[cfg(feature = "failpoints")]
fn failpoint_test_lock() -> &'static Mutex<()> {
    FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(feature = "failpoints")]
struct TxnAppendBatchNoSpaceFailpointGuard {
    // Drop order: the scenario is torn down in `drop` before the gate field
    // is released.
    scenario: Option<fail::FailScenario<'static>>,
    _test_guard: crate::failpoints::TestFailpointGuard,
}

#[cfg(feature = "failpoints")]
impl TxnAppendBatchNoSpaceFailpointGuard {
    /// Take the failpoint gate before the `fail` scenario lock, matching
    /// every other failpoint test; the reverse order deadlocks against them.
    fn setup(request_id: u64) -> Self {
        let test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        crate::runtime::actors::wal::set_txn_append_batch_no_space_failpoint_request_id(Some(
            request_id,
        ));
        fail::cfg("midge::wal::inject_no_space_on_txn_append_batch", "return")
            .expect("configure txn append batch no-space failpoint");
        Self {
            scenario: Some(scenario),
            _test_guard: test_guard,
        }
    }
}

#[cfg(feature = "failpoints")]
impl Drop for TxnAppendBatchNoSpaceFailpointGuard {
    fn drop(&mut self) {
        fail::remove("midge::wal::inject_no_space_on_txn_append_batch");
        crate::runtime::actors::wal::set_txn_append_batch_no_space_failpoint_request_id(None);
        if let Some(scenario) = self.scenario.take() {
            scenario.teardown();
        }
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
        assertions: Vec::new(),
        durability_policy,
        start_sequence: None,
        conflict_policy: ConflictPolicy::LastWriteWins,
    }
}

fn ordered_txn_request(
    request_id: u64,
    ops: Vec<TransactionOp>,
    durability_policy: DurabilityPolicy,
    start_sequence: u64,
    conflict_policy: ConflictPolicy,
) -> ApplyTransactionRequest {
    ApplyTransactionRequest {
        request_id,
        ops,
        assertions: Vec::new(),
        durability_policy: Some(durability_policy),
        start_sequence: Some(start_sequence),
        conflict_policy,
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
        assertions: Vec::new(),
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
            assertions: Vec::new(),
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
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    let response = router.register(1, "TestRequest");

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
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
    assert_eq!(fixture.event_loop.state.wal.pending_writes, 3);
    assert_eq!(fixture.event_loop.actors.wal_actor.pending_sync_count(), 3);
    assert!(fixture.event_loop.state.pending_transaction_started());

    assert_eq!(expect_transaction_applied(&first_rx, 10), (3, 1));
    assert_eq!(expect_transaction_applied(&second_rx, 11), (6, 1));
    assert_eq!(expect_transaction_applied(&third_rx, 12), (9, 1));

    assert_memtable_value(&fixture.event_loop, 0, b"coalesce-a", Some(b"value-a"));
    assert_memtable_value(&fixture.event_loop, 0, b"coalesce-b", Some(b"value-b"));
    assert_memtable_value(&fixture.event_loop, 0, b"coalesce-c", Some(b"value-c"));

    Ok(())
}

#[test]
fn should_coalesce_independent_strict_transactions_with_one_durable_append() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(13);
    let second_rx = fixture.register(14);
    let third_rx = fixture.register(15);
    msg_tx
        .send(txn_msg(
            14,
            vec![insert_only_put_op(0, b"strict-b", b"value-b")],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue second strict transaction");
    msg_tx
        .send(txn_msg(
            15,
            vec![delete_range_op(0, b"strict-range-a", b"strict-range-z")],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue third strict transaction");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            13,
            vec![put_op(0, b"strict-a", b"value-a")],
            Some(DurabilityPolicy::Strict),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 3);
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
    assert_eq!(fixture.event_loop.actors.wal_actor.sync_calls(), 1);
    assert_eq!(fixture.event_loop.state.wal.pending_writes, 0);
    assert_eq!(
        fixture.event_loop.state.wal.frontiers.local_durable(),
        fixture.event_loop.state.sequence
    );
    assert_eq!(expect_transaction_applied(&first_rx, 13), (3, 1));
    assert_eq!(expect_transaction_applied(&second_rx, 14), (6, 1));
    assert_eq!(expect_transaction_applied(&third_rx, 15), (9, 1));
    assert_memtable_value(&fixture.event_loop, 0, b"strict-a", Some(b"value-a"));
    assert_memtable_value(&fixture.event_loop, 0, b"strict-b", Some(b"value-b"));

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
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
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
fn should_coalesce_blind_transactions_when_point_range_overlap() -> MidgeResult<()> {
    for (policy, expected_syncs) in [
        (DurabilityPolicy::Batched, 0),
        (DurabilityPolicy::Strict, 1),
    ] {
        // Arrange
        let mut fixture = EventLoopFixture::with_policy(policy)?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let first_rx = fixture.register(30);
        let fallback_rx = fixture.register(31);
        msg_tx
            .send(txn_msg(
                31,
                vec![delete_range_op(0, b"overlap-a", b"overlap-z")],
                Some(policy),
            ))
            .expect("queue overlapping delete range");

        // Act
        let handled = fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(30, vec![put_op(0, b"overlap-m", b"value")], Some(policy)),
            1024,
        );

        // Assert
        assert_eq!(handled, 2);
        assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
        assert_eq!(
            fixture.event_loop.actors.wal_actor.sync_calls(),
            expected_syncs
        );
        assert_eq!(expect_transaction_applied(&first_rx, 30).1, 1);
        assert_eq!(expect_transaction_applied(&fallback_rx, 31).1, 1);
        assert_memtable_value(&fixture.event_loop, 0, b"overlap-m", None);
    }

    Ok(())
}

#[test]
fn should_return_inline_responses_when_same_key_lww_transactions_share_a_group() -> MidgeResult<()>
{
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
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
    assert_eq!(expect_transaction_applied(&first_rx, 34).1, 1);
    assert_eq!(expect_transaction_applied(&fallback_rx, 35).1, 1);
    assert_no_additional_response(&first_rx);
    assert_no_additional_response(&fallback_rx);
    assert_memtable_value(&fixture.event_loop, 0, b"same-key-inline", Some(b"second"));

    Ok(())
}

#[test]
fn should_return_inline_error_when_same_key_insert_only_is_rejected_by_staged_writes(
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
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
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
fn should_stash_snapshot_bookkeeping_instead_of_coalescing_past_it() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(42);

    msg_tx
        .send(RuntimeMsg::Test(TestRuntimeMsg::RegisterSnapshot {
            request_id: 43,
            snapshot_id: 900,
            sequence: 0,
            pinned_sst_names: Vec::new(),
        }))
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
    // The coalescing loop must not run any message itself: everything that is
    // not another `ApplyTransaction` is stashed for the main loop, which runs it
    // through `RuntimeDispatcher::handle` and therefore through both gates.
    assert_eq!(handled, 1);
    assert_eq!(expect_transaction_applied(&first_rx, 42).1, 1);
    assert!(
        matches!(
            fixture.event_loop.routing.pending_msg.take(),
            Some(RuntimeMsg::Test(TestRuntimeMsg::RegisterSnapshot {
                request_id: 43,
                ..
            }))
        ),
        "snapshot bookkeeping must be stashed for the dispatcher, not handled inline"
    );
    assert_memtable_value(&fixture.event_loop, 0, b"before-register", Some(b"value-a"));
    assert_memtable_value(&fixture.event_loop, 0, b"after-register", None);

    Ok(())
}

#[test]
fn should_not_drop_non_write_after_coalescing_stashes_pending_message() -> MidgeResult<()> {
    for (policy, expected_syncs) in [
        (DurabilityPolicy::Batched, 0),
        (DurabilityPolicy::Strict, 1),
    ] {
        // Arrange
        let mut fixture = EventLoopFixture::with_policy(policy)?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let (txn_msg, txn_rx) = inline_txn_msg(
            38,
            vec![put_op(0, b"coalesce-before-non-write", b"value")],
            Some(policy),
        );
        msg_tx.send(txn_msg).expect("queue transaction");
        msg_tx
            .send(RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 39 }))
            .expect("queue first non-write");
        msg_tx
            .send(RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 40 }))
            .expect("queue second non-write");

        // Act
        let handled = fixture.event_loop.drain_pending_writes(&msg_rx, 1024);

        // Assert
        assert_eq!(handled, 1);
        assert_eq!(expect_transaction_applied(&txn_rx, 38).1, 1);
        assert_eq!(
            fixture.event_loop.actors.wal_actor.sync_calls(),
            expected_syncs
        );
        match fixture.event_loop.routing.pending_msg.take() {
            Some(RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id })) => {
                assert_eq!(request_id, 39);
            }
            other => panic!("expected first non-write to be stashed, got {other:?}"),
        }
        match msg_rx.try_recv() {
            Ok(RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id })) => assert_eq!(request_id, 40),
            other => panic!("expected second non-write to remain queued, got {other:?}"),
        }
    }

    Ok(())
}

#[test]
fn should_leave_control_message_queued_when_pending_slot_is_occupied_during_coalescing(
) -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let transaction_rx = fixture.register(41);
    fixture.event_loop.routing.pending_msg =
        Some(RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 42 }));
    msg_tx
        .send(RuntimeMsg::FlushMemtable {
            request_id: 43,
            cf_id: 0,
        })
        .expect("queue control request behind occupied pending slot");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            41,
            vec![put_op(0, b"pending-slot", b"value")],
            Some(DurabilityPolicy::Batched),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 1);
    assert_eq!(expect_transaction_applied(&transaction_rx, 41).1, 1);
    assert!(matches!(
        fixture.event_loop.routing.pending_msg,
        Some(RuntimeMsg::Test(TestRuntimeMsg::Noop { request_id: 42 }))
    ));
    assert!(matches!(
        msg_rx.try_recv(),
        Ok(RuntimeMsg::FlushMemtable {
            request_id: 43,
            cf_id: 0,
        })
    ));

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
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
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
fn should_not_enter_coalesced_path_for_non_local_strict_or_buffered_durability() -> MidgeResult<()>
{
    for (policy_name, durability_policy, expected_append_calls) in [
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
            fixture.event_loop.actors.wal_actor.append_calls(),
            expected_append_calls,
            "{policy_name} should not batch with the queued request"
        );
        assert_eq!(expect_transaction_applied(&first_rx, 50).1, 1);
        assert_no_response(&queued_rx);
    }

    Ok(())
}

#[test]
fn should_order_mixed_policy_transactions_on_fallback() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let strict_rx = fixture.register(52);
    let buffered_rx = fixture.register(53);
    msg_tx
        .send(txn_msg(
            53,
            vec![put_op(0, b"mixed-buffered", b"buffered")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue buffered fallback");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            52,
            vec![put_op(0, b"mixed-strict", b"strict")],
            Some(DurabilityPolicy::Strict),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 2);
    assert_eq!(fixture.event_loop.actors.wal_actor.sync_calls(), 1);
    assert_eq!(expect_transaction_applied(&strict_rx, 52), (3, 1));
    assert_eq!(expect_transaction_applied(&buffered_rx, 53), (6, 1));
    assert_memtable_value(&fixture.event_loop, 0, b"mixed-strict", Some(b"strict"));
    assert_memtable_value(&fixture.event_loop, 0, b"mixed-buffered", Some(b"buffered"));

    Ok(())
}

#[test]
fn should_reject_assertion_when_earlier_batch_member_writes_the_asserted_key() -> MidgeResult<()> {
    // Arrange: A (initial, prepared first) writes a key; B (queued, prepared
    // second) asserts that same key and also carries an unrelated write so
    // it doesn't already fall back via the empty-ops guard alone.
    //
    // Preparation allocates a sequence immediately, but memtable apply for
    // the whole batch is deferred until it flushes — so without the shared
    // sequenced-write overlay, B's assertion would be
    // checked against a memtable that does not yet reflect A's write (whose
    // sequence is nonetheless already allocated) and would wrongly pass.
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let writer_rx = fixture.register(60);
    let asserting_rx = fixture.register(61);
    msg_tx
        .send(RuntimeMsg::ApplyTransaction {
            request_id: 61,
            ops: vec![put_op(0, b"assert-coalesce-unrelated", b"value-b")],
            assertions: vec![crate::runtime::KeyAssertion {
                cf_id: 0,
                key: Bytes::from_static(b"assert-coalesce-key"),
            }],
            durability_policy: Some(DurabilityPolicy::Strict),
            start_sequence: Some(0),
            conflict_policy: ConflictPolicy::LastWriteWins,
            response_tx: None,
        })
        .expect("queue asserting transaction");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        ordered_txn_request(
            60,
            vec![put_op(0, b"assert-coalesce-key", b"value-a")],
            DurabilityPolicy::Strict,
            0,
            ConflictPolicy::LastWriteWins,
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(expect_transaction_applied(&writer_rx, 60), (3, 1));
    assert!(
        matches!(
            recv_response(&asserting_rx),
            RuntimeResponse::Error {
                error: MidgeError::WriteConflict(_),
                ..
            }
        ),
        "expected the coalescing-aware assertion check to reject the stale assertion"
    );

    Ok(())
}

#[test]
fn should_fall_back_when_strict_transactions_have_different_ordering_contracts() -> MidgeResult<()>
{
    // Arrange
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(54);
    let second_rx = fixture.register(55);
    msg_tx
        .send(RuntimeMsg::ApplyTransaction {
            request_id: 55,
            ops: vec![put_op(0, b"ordered-b", b"value-b")],
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::Strict),
            start_sequence: Some(0),
            conflict_policy: ConflictPolicy::AbortOnWriteConflict,
            response_tx: None,
        })
        .expect("queue differently ordered strict transaction");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        ordered_txn_request(
            54,
            vec![put_op(0, b"ordered-a", b"value-a")],
            DurabilityPolicy::Strict,
            0,
            ConflictPolicy::LastWriteWins,
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 2);
    assert_eq!(fixture.event_loop.actors.wal_actor.sync_calls(), 2);
    assert_eq!(expect_transaction_applied(&first_rx, 54), (3, 1));
    assert_eq!(expect_transaction_applied(&second_rx, 55), (6, 1));

    Ok(())
}

#[test]
fn should_exclude_empty_strict_request_without_draining_following_commit() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let empty_rx = fixture.register(56);
    let queued_rx = fixture.register(57);
    msg_tx
        .send(txn_msg(
            57,
            vec![put_op(0, b"after-empty", b"value")],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue strict transaction after empty request");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(56, Vec::new(), Some(DurabilityPolicy::Strict)),
        1024,
    );

    // Assert
    assert_eq!(handled, 1);
    assert_eq!(expect_transaction_applied(&empty_rx, 56), (0, 0));
    assert_no_response(&queued_rx);
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 0);
    assert_eq!(fixture.event_loop.actors.wal_actor.sync_calls(), 0);

    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fail_all_event_loop_buffered_transactions_when_append_hits_no_space() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 0);
        assert_eq!(fixture.event_loop.state.wal.pending_writes, 0);
        assert_eq!(fixture.event_loop.actors.wal_actor.pending_sync_count(), 0);
        assert_eq!(fixture.event_loop.actors.wal_actor.bytes_since_sync(), 0);
        assert!(!fixture.event_loop.state.pending_transaction_started());
        assert_eq!(
            fixture
                .event_loop
                .state
                .get_cf(0)
                .expect("default column family should exist")
                .memtable
                .iter_all()
                .len(),
            0
        );
        assert!(
            fixture.event_loop.state.sequence > 0,
            "failed preparation may leave sequence gaps"
        );

        drop(failpoint_guard);
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
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
    assert_memtable_value(&fixture.event_loop, 0, b"recovered", Some(b"value"));

    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fail_all_event_loop_strict_transactions_when_shared_append_fails() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(63);
    let second_rx = fixture.register(64);
    msg_tx
        .send(txn_msg(
            64,
            vec![put_op(0, b"strict-append-failed-b", b"value-b")],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue second strict transaction");

    {
        let failpoint_guard = TxnAppendBatchNoSpaceFailpointGuard::setup(63);

        // Act
        let handled = fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                63,
                vec![put_op(0, b"strict-append-failed-a", b"value-a")],
                Some(DurabilityPolicy::Strict),
            ),
            1024,
        );

        // Assert
        assert_eq!(handled, 2);
        expect_error(&first_rx, 63, |error| {
            matches!(error, MidgeError::NoSpace(_))
        });
        expect_error(&second_rx, 64, |error| {
            matches!(error, MidgeError::NoSpace(_))
        });
        assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 0);
        assert_eq!(fixture.event_loop.actors.wal_actor.sync_calls(), 0);
        assert_eq!(
            fixture
                .event_loop
                .state
                .get_cf(0)
                .expect("default column family")
                .memtable
                .iter_all()
                .len(),
            0
        );

        drop(failpoint_guard);
    }

    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fail_all_event_loop_strict_transactions_when_shared_sync_fails() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let lease_healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));
    fixture.event_loop.authority.fencing.lease_healthy = Some(Arc::clone(&lease_healthy));
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(65);
    let second_rx = fixture.register(66);
    msg_tx
        .send(txn_msg(
            66,
            vec![put_op(0, b"strict-sync-failed-b", b"value-b")],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue second strict transaction");
    fail::cfg("midge::wal::inject_no_space_on_sync", "return").expect("configure WAL sync failure");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            65,
            vec![put_op(0, b"strict-sync-failed-a", b"value-a")],
            Some(DurabilityPolicy::Strict),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 2);
    expect_error(&first_rx, 65, |error| {
        matches!(error, MidgeError::OutcomeUnknown(_))
    });
    expect_error(&second_rx, 66, |error| {
        matches!(error, MidgeError::OutcomeUnknown(_))
    });
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
    assert_eq!(fixture.event_loop.actors.wal_actor.sync_calls(), 0);
    assert!(fixture.event_loop.state.persistence_anomaly_detected());
    assert!(!lease_healthy.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        fixture
            .event_loop
            .state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .iter_all()
            .len(),
        0
    );

    fail::remove("midge::wal::inject_no_space_on_sync");
    scenario.teardown();
    drop(test_guard);
    Ok(())
}

#[cfg(feature = "failpoints")]
fn memtable_keys(fixture: &EventLoopFixture, cf_id: crate::types::ColumnFamilyId) -> Vec<Vec<u8>> {
    fixture
        .event_loop
        .state
        .get_cf(cf_id)
        .expect("column family")
        .memtable
        .iter_all()
        .into_iter()
        .map(|(key, _, _)| key)
        .collect()
}

/// Give the fixture's WAL actor a lease that lapses at the `lapse_at`-th
/// (zero-based) hit of `failpoint`.
#[cfg(feature = "failpoints")]
fn lapse_lease_at(fixture: &mut EventLoopFixture, failpoint: &str, lapse_at: usize) {
    let validity = Arc::new(crate::lease::LeaseValidity::new());
    validity
        .activate(
            fixture.event_loop.authority.fencing.writer_epoch,
            std::time::Instant::now() + Duration::from_mins(1),
        )
        .expect("activate the lease");
    fixture
        .event_loop
        .actors
        .wal_actor
        .set_lease_validity(Some(Arc::clone(&validity)));
    let hits = std::sync::atomic::AtomicUsize::new(0);
    fail::cfg_callback(failpoint, move || {
        if hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == lapse_at {
            validity.expire_for_test();
        }
    })
    .expect("configure the lease lapse");
}

/// Leave family 0's memtable non-empty with a best-effort write, so a
/// freeze has something to publish.
#[cfg(feature = "failpoints")]
fn seed_best_effort_write(fixture: &mut EventLoopFixture) -> MidgeResult<()> {
    fixture.event_loop.actors.wal_actor.append_transaction(
        &mut fixture.event_loop.state,
        crate::runtime::actors::wal::TransactionAppendParams {
            request_id: 70,
            ops: vec![put_op(0, b"best-effort", b"value")],
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::BestEffort),
            start_sequence: None,
            conflict_policy: ConflictPolicy::LastWriteWins,
        },
    )?;
    Ok(())
}

/// Commit a strict spilled transaction of one put to family 0.
#[cfg(feature = "failpoints")]
fn commit_strict_spill(
    fixture: &mut EventLoopFixture,
    spill_dir: &std::path::Path,
) -> MidgeResult<(u64, usize, bool)> {
    let pool = Arc::new(crate::runtime::transaction_spill::TransactionMemoryPool::new(16));
    let mut write_set = crate::runtime::transaction_spill::TransactionWriteSet::new(
        pool,
        &crate::io::real::test_root(spill_dir),
        false,
        1,
    );
    write_set.push(put_op(0, b"spilled", b"value"))?;
    let source = write_set.take_source();
    fixture
        .event_loop
        .actors
        .wal_actor
        .append_spilled_transaction(
            &mut fixture.event_loop.state,
            &source,
            crate::runtime::actors::wal::SpilledTransactionAppendParams {
                request_id: 71,
                assertions: Vec::new(),
                durability_policy: Some(DurabilityPolicy::Strict),
                start_sequence: 0,
                conflict_policy: ConflictPolicy::LastWriteWins,
            },
        )
}

#[cfg(feature = "failpoints")]
#[test]
fn should_freeze_without_spilled_unit_when_lease_lapses_before_its_apply() -> MidgeResult<()> {
    // Arrange: a best-effort write leaves the family's memtable non-empty.
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let temp = tempfile::tempdir().map_err(MidgeError::Io)?;
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    seed_best_effort_write(&mut fixture)?;
    lapse_lease_at(&mut fixture, "midge::wal::txn_after_sync_before_ack", 0);
    let result = commit_strict_spill(&mut fixture, temp.path());
    fail::remove("midge::wal::txn_after_sync_before_ack");
    let keys = memtable_keys(&fixture, 0);

    // Act
    let frozen = fixture.event_loop.freeze_active_memtable(0);

    // Assert: the strict unit is durable but none of it is applied, so the
    // flush publishes none of it and replay keeps or drops it whole; the
    // best-effort write is flushed.
    assert!(
        matches!(&result, Err(MidgeError::OutcomeUnknown(message)) if message.contains("may be applied on restart")),
        "{result:?}"
    );
    assert!(fixture.event_loop.actors.wal_actor.is_fenced());
    assert_eq!(keys, vec![b"best-effort".to_vec()]);
    assert!(matches!(frozen, Ok(Some(_))), "{frozen:?}");

    scenario.teardown();
    drop(test_guard);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_freeze_without_mark_when_spilled_apply_fails_before_its_first_op() -> MidgeResult<()> {
    // Arrange: the apply fails before any op reaches the memtable, the
    // `applied_ops == 0` boundary between a fence and a mark.
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let temp = tempfile::tempdir().map_err(MidgeError::Io)?;
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    seed_best_effort_write(&mut fixture)?;
    fail::cfg("midge::wal::spilled_apply_op", "return").expect("fail the first apply");
    let result = commit_strict_spill(&mut fixture, temp.path());
    fail::remove("midge::wal::spilled_apply_op");
    let keys = memtable_keys(&fixture, 0);
    let marked = fixture
        .event_loop
        .state
        .get_cf(0)
        .and_then(crate::runtime::state::ColumnFamilyState::partially_applied_commit);

    // Act
    let frozen = fixture.event_loop.freeze_active_memtable(0);

    // Assert: nothing of the unit is in the memtable, so the family is not
    // marked and the flush publishes only the best-effort write.
    assert!(
        matches!(&result, Err(MidgeError::OutcomeUnknown(message)) if message.contains("may be applied on restart")),
        "{result:?}"
    );
    assert!(fixture.event_loop.actors.wal_actor.is_fenced());
    assert_eq!(keys, vec![b"best-effort".to_vec()]);
    assert_eq!(marked, None);
    assert!(matches!(frozen, Ok(Some(_))), "{frozen:?}");

    scenario.teardown();
    drop(test_guard);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_freeze_only_applied_units_when_lease_lapses_inside_coalesced_group() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let _first_rx = fixture.register(72);
    let _second_rx = fixture.register(73);
    msg_tx
        .send(txn_msg(
            73,
            vec![put_op(0, b"group-second", b"value")],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue second strict transaction");
    lapse_lease_at(
        &mut fixture,
        "midge::wal::before_prepared_transaction_apply",
        1,
    );
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            72,
            vec![put_op(0, b"group-first", b"value")],
            Some(DurabilityPolicy::Strict),
        ),
        1024,
    );
    fail::remove("midge::wal::before_prepared_transaction_apply");
    let keys = memtable_keys(&fixture, 0);

    // Act
    let frozen = fixture.event_loop.freeze_active_memtable(0);

    // Assert: both units are durable; the first is applied whole and the
    // second not at all, so the flush publishes only the first and replay
    // keeps or drops the second whole.
    assert_eq!(handled, 2);
    assert!(fixture.event_loop.actors.wal_actor.is_fenced());
    assert_eq!(keys, vec![b"group-first".to_vec()]);
    assert!(matches!(frozen, Ok(Some(_))), "{frozen:?}");

    scenario.teardown();
    drop(test_guard);
    Ok(())
}

/// Close the group's runtime, reopen its directory as an engine and report
/// which of `keys` family 0 holds after recovery. Call it without the
/// failpoint gate: the engine's workers wait on it.
#[cfg(feature = "failpoints")]
fn reopen_and_find(fixture: EventLoopFixture, keys: [&[u8]; 2]) -> MidgeResult<[bool; 2]> {
    let [first, second] = reopen_and_read(fixture, keys)?;
    Ok([first.is_some(), second.is_some()])
}

/// Like `reopen_and_find`, returning the recovered values.
fn reopen_and_read(
    fixture: EventLoopFixture,
    keys: [&[u8]; 2],
) -> MidgeResult<[Option<Vec<u8>>; 2]> {
    let EventLoopFixture {
        _temp_dir: directory,
        event_loop,
    } = fixture;
    drop(event_loop);
    let options = crate::OpenOptions::local(directory.path())
        .background_compaction(false)
        .build()?;
    let mut engine = crate::Engine::open(options)?;
    let found = {
        let tx = engine.begin_tx(0, crate::TransactionMode::ReadOnly)?;
        [
            tx.get(keys[0])?.map(|value| value.to_vec()),
            tx.get(keys[1])?.map(|value| value.to_vec()),
        ]
    };
    engine.shutdown(Duration::from_secs(30))?;
    Ok(found)
}

#[cfg(feature = "failpoints")]
#[test]
fn should_answer_outcome_unknown_when_group_append_is_partly_written_and_authority_lost(
) -> MidgeResult<()> {
    // Arrange: one positional write carries both units' frames. It writes
    // half of them and fails, and the authority check that follows fails
    // too, so the written prefix stays in the file.
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let first_keys: [&[u8]; 2] = [b"partial-a1", b"partial-a2"];
    let (fixture, first, second) = {
        let test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let first_rx = fixture.register(82);
        let second_rx = fixture.register(83);
        msg_tx
            .send(txn_msg(
                83,
                vec![put_op(0, b"partial-b", b"value")],
                Some(DurabilityPolicy::Strict),
            ))
            .expect("queue second strict transaction");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        fixture
            .event_loop
            .actors
            .wal_actor
            .set_write_authority_for_test(Arc::new(move |operation: &mut dyn FnMut()| {
                if counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
                    Err(MidgeError::Fenced("test writer authority lost".to_string()))
                } else {
                    operation();
                    Ok(())
                }
            }));
        fail::cfg("midge::wal::partial_write_then_no_space", "return")
            .expect("configure partial WAL write");

        // Act
        fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                82,
                vec![
                    put_op(0, first_keys[0], b"value"),
                    put_op(0, first_keys[1], b"value"),
                ],
                Some(DurabilityPolicy::Strict),
            ),
            1024,
        );
        fail::remove("midge::wal::partial_write_then_no_space");
        let responses = (recv_response(&first_rx), recv_response(&second_rx));
        scenario.teardown();
        drop(test_guard);
        (fixture, responses.0, responses.1)
    };
    let recovered = reopen_and_find(fixture, first_keys)?;

    // Assert: a written frame may replay, so neither caller may be told its
    // unit is not applied; the first unit comes back whole or not at all.
    assert!(is_outcome_unknown(&first, 82), "{first:?}");
    assert!(is_outcome_unknown(&second, 83), "{second:?}");
    assert_eq!(recovered[0], recovered[1], "{recovered:?}");
    Ok(())
}

/// Run `run_group_with_lease_lapse` under the failpoint gate, releasing the
/// gate before returning, and capture the family's memtable keys.
#[cfg(feature = "failpoints")]
fn lapsed_group_outside_gate(
    policy: DurabilityPolicy,
    request_ids: [u64; 2],
    keys: [&'static [u8]; 2],
) -> MidgeResult<(LapsedGroup, Vec<Vec<u8>>)> {
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let group = run_group_with_lease_lapse(policy, request_ids, keys)?;
    let memtable = memtable_keys(&group.fixture, 0);
    scenario.teardown();
    drop(test_guard);
    Ok((group, memtable))
}

#[cfg(feature = "failpoints")]
#[test]
fn should_acknowledge_applied_unit_when_lease_lapses_inside_coalesced_group() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let keys: [&[u8]; 2] = [b"acked-first", b"acked-second"];

    // Act
    let (
        LapsedGroup {
            fixture,
            first,
            second,
        },
        memtable,
    ) = lapsed_group_outside_gate(DurabilityPolicy::Strict, [74, 75], keys)?;
    let recovered = reopen_and_find(fixture, keys)?;

    // Assert: the first unit is synced and applied, so it is acknowledged
    // and survives restart. The second is synced but not applied, so its
    // caller learns nothing definite, and replay applies it.
    assert!(
        matches!(
            first,
            RuntimeResponse::TransactionApplied { request_id: 74, .. }
        ),
        "{first:?}"
    );
    assert!(is_outcome_unknown(&second, 75), "{second:?}");
    assert_eq!(memtable, vec![keys[0].to_vec()]);
    assert_eq!(recovered, [true, true]);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_promise_nothing_for_buffered_units_when_lease_lapses_inside_coalesced_group(
) -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let keys: [&[u8]; 2] = [b"buffered-first", b"buffered-second"];

    // Act
    let (
        LapsedGroup {
            fixture,
            first,
            second,
        },
        memtable,
    ) = lapsed_group_outside_gate(DurabilityPolicy::Batched, [80, 81], keys)?;
    let recovered = reopen_and_find(fixture, keys)?;

    // Assert: neither frame is synced, so neither caller is told
    // its unit is applied or not. The first unit is applied and the second
    // is not. An append returns only once the writer has written the
    // frames, so both are in the file, unsynced; a restart without a crash
    // replays both, including the one that was never applied. Fenced for
    // either unit would have been wrong.
    assert!(
        matches!(
            &first,
            RuntimeResponse::Error {
                request_id: 80,
                error: MidgeError::OutcomeUnknown(message),
            } if message.contains("is applied but") && message.contains("may be lost")
        ),
        "{first:?}"
    );
    assert!(is_outcome_unknown(&second, 81), "{second:?}");
    assert_eq!(memtable, vec![keys[0].to_vec()]);
    assert_eq!(recovered, [true, true]);
    Ok(())
}

/// Run a coalesced group of two one-put units under `policy`, with the
/// writer lease lapsing just before the second unit is applied.
#[cfg(feature = "failpoints")]
struct LapsedGroup {
    fixture: EventLoopFixture,
    first: RuntimeResponse,
    second: RuntimeResponse,
}

#[cfg(feature = "failpoints")]
fn run_group_with_lease_lapse(
    policy: DurabilityPolicy,
    request_ids: [u64; 2],
    keys: [&'static [u8]; 2],
) -> MidgeResult<LapsedGroup> {
    let group = run_group(policy, request_ids, keys, |fixture| {
        lapse_lease_at(fixture, "midge::wal::before_prepared_transaction_apply", 1);
    });
    fail::remove("midge::wal::before_prepared_transaction_apply");
    group
}

/// Run a coalesced group of two one-put units under `policy`, after `arm`
/// has prepared the fixture's failure. The caller disarms it.
#[cfg(feature = "failpoints")]
fn run_group(
    policy: DurabilityPolicy,
    request_ids: [u64; 2],
    keys: [&'static [u8]; 2],
    arm: impl FnOnce(&mut EventLoopFixture),
) -> MidgeResult<LapsedGroup> {
    let mut fixture = EventLoopFixture::with_policy(policy)?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(request_ids[0]);
    let second_rx = fixture.register(request_ids[1]);
    msg_tx
        .send(txn_msg(
            request_ids[1],
            vec![put_op(0, keys[1], b"value")],
            Some(policy),
        ))
        .expect("queue second transaction");
    arm(&mut fixture);
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            request_ids[0],
            vec![put_op(0, keys[0], b"value")],
            Some(policy),
        ),
        1024,
    );
    assert_eq!(handled, 2, "both units must share one group");
    Ok(LapsedGroup {
        fixture,
        first: recv_response(&first_rx),
        second: recv_response(&second_rx),
    })
}

#[cfg(feature = "failpoints")]
fn is_outcome_unknown(response: &RuntimeResponse, expected_request_id: u64) -> bool {
    matches!(
        response,
        RuntimeResponse::Error {
            request_id,
            error: MidgeError::OutcomeUnknown(message),
        } if *request_id == expected_request_id && message.contains("may be applied on restart")
    )
}

#[cfg(feature = "failpoints")]
#[test]
fn should_freeze_without_failed_unit_when_strict_sync_fails_after_wal_append() -> MidgeResult<()> {
    // Arrange: an earlier write leaves the family's memtable non-empty, so
    // a freeze would publish a frontier at `state.sequence`.
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let _applied_rx = fixture.register(67);
    let failed_rx = fixture.register(68);
    fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            67,
            vec![put_op(0, b"strict-applied", b"value")],
            Some(DurabilityPolicy::Strict),
        ),
        1,
    );
    fail::cfg("midge::wal::inject_no_space_on_sync", "return").expect("configure WAL sync failure");
    fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            68,
            vec![put_op(0, b"strict-sync-failed", b"value")],
            Some(DurabilityPolicy::Strict),
        ),
        1,
    );
    fail::remove("midge::wal::inject_no_space_on_sync");
    expect_error(&failed_rx, 68, |error| {
        matches!(error, MidgeError::OutcomeUnknown(_))
    });

    let keys = memtable_keys(&fixture, 0);

    // Act
    let frozen = fixture.event_loop.freeze_active_memtable(0);

    // Assert: replay keeps a unit iff its commit is above the family's
    // frontier. None of the failed unit is in the frozen memtable, so the
    // flush publishes none of it and replay keeps or drops it whole.
    assert_eq!(keys, vec![b"strict-applied".to_vec()]);
    assert!(matches!(frozen, Ok(Some(_))), "{frozen:?}");

    scenario.teardown();
    drop(test_guard);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_fail_overlapping_group_when_coalesced_append_fails() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 0);
        assert_eq!(fixture.event_loop.state.wal.pending_writes, 0);
        assert_memtable_value(&fixture.event_loop, 0, b"failed-same-key", None);

        drop(failpoint_guard);
    }

    Ok(())
}

#[test]
fn should_select_strict_group_commit_window_given_batch_capacity() {
    // Arrange
    let at_capacity = true;
    let below_capacity = false;

    // Act
    let capped_window = local_strict_group_commit_collect_window(at_capacity);
    let available_window = local_strict_group_commit_collect_window(below_capacity);

    // Assert - the real production function, not a re-statement of the
    // internal candidate-window constants it was tuned from.
    assert_eq!(capped_window, None);
    assert_eq!(available_window, Some(LOCAL_STRICT_GROUP_COMMIT_WINDOW));
}

#[test]
fn should_stop_strict_group_at_existing_batch_cap() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(910);
    let second_rx = fixture.register(911);
    let third_rx = fixture.register(912);
    msg_tx
        .send(txn_msg(
            911,
            vec![put_op(0, b"cap-b", b"value-b")],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue second strict transaction");
    msg_tx
        .send(txn_msg(
            912,
            vec![put_op(0, b"cap-c", b"value-c")],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue strict transaction beyond cap");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            910,
            vec![put_op(0, b"cap-a", b"value-a")],
            Some(DurabilityPolicy::Strict),
        ),
        2,
    );

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
    assert_eq!(fixture.event_loop.actors.wal_actor.sync_calls(), 1);
    assert_eq!(expect_transaction_applied(&first_rx, 910), (3, 1));
    assert_eq!(expect_transaction_applied(&second_rx, 911), (6, 1));
    assert_no_response(&third_rx);
    assert!(matches!(
        msg_rx.try_recv(),
        Ok(RuntimeMsg::ApplyTransaction {
            request_id: 912,
            ..
        })
    ));

    Ok(())
}

fn fence_stalled_writer(fixture: &mut EventLoopFixture, expired: bool) {
    fixture.event_loop.state.set_write_stalled(true);
    if expired {
        let validity = Arc::new(crate::lease::LeaseValidity::new());
        validity
            .activate(1, std::time::Instant::now() + Duration::from_mins(1))
            .expect("activate test writer authority");
        validity.expire_for_test();
        fixture.event_loop.authority.fencing.writer_epoch = 1;
        fixture.event_loop.authority.fencing.lease_validity = Some(validity);
        fixture.event_loop.authority.fencing.lease_healthy =
            Some(Arc::new(std::sync::atomic::AtomicBool::new(true)));
    } else {
        fixture.event_loop.authority.fencing.lease_healthy =
            Some(Arc::new(std::sync::atomic::AtomicBool::new(false)));
    }
}

#[test]
fn should_report_fenced_when_stall_probe_observes_lost_authority() -> MidgeResult<()> {
    for expired in [false, true] {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        fence_stalled_writer(&mut fixture, expired);
        let response_rx = fixture.register(920);
        let (_tx, msg_rx) = crossbeam::channel::unbounded();
        let sequence_before = fixture.event_loop.state.sequence;
        let wal_appends_before = fixture.event_loop.actors.wal_actor.append_calls();

        // Act
        crate::runtime::event_loop::dispatch::RuntimeDispatcher::handle(
            &mut fixture.event_loop,
            RuntimeMsg::CheckWriteStall {
                request_id: 920,
                cf_id: 0,
            },
            &msg_rx,
        );

        // Assert
        expect_error(&response_rx, 920, |error| {
            matches!(error, MidgeError::Fenced(_))
        });
        assert_eq!(fixture.event_loop.state.sequence, sequence_before);
        assert_eq!(
            fixture.event_loop.actors.wal_actor.append_calls(),
            wal_appends_before
        );
    }
    Ok(())
}

#[test]
fn should_reject_new_stall_waiter_when_writer_authority_is_lost() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    fence_stalled_writer(&mut fixture, false);
    let response_rx = fixture.register(921);

    // Act
    fixture.event_loop.handle_wait_for_write_stall_clear(921, 0);

    // Assert
    expect_error(&response_rx, 921, |error| {
        matches!(error, MidgeError::Fenced(_))
    });
    assert!(!fixture
        .event_loop
        .pipeline
        .write_stall_waiters
        .contains(921));
    Ok(())
}

#[test]
fn should_fail_existing_stall_waiters_when_authority_expires_under_pressure() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    fixture.event_loop.state.set_write_stalled(true);
    let first_rx = fixture.register(922);
    let second_rx = fixture.register(923);
    fixture.event_loop.handle_wait_for_write_stall_clear(922, 0);
    fixture.event_loop.handle_wait_for_write_stall_clear(923, 0);
    assert!(fixture
        .event_loop
        .pipeline
        .write_stall_waiters
        .contains(922));
    assert!(fixture
        .event_loop
        .pipeline
        .write_stall_waiters
        .contains(923));
    assert!(fixture
        .event_loop
        .idle_progress_timeout()
        .is_some_and(|wait| wait <= Duration::from_millis(100)));
    fence_stalled_writer(&mut fixture, true);

    // Act
    fixture.event_loop.background_progress(None);

    // Assert
    expect_error(&first_rx, 922, |error| {
        matches!(error, MidgeError::Fenced(_))
    });
    expect_error(&second_rx, 923, |error| {
        matches!(error, MidgeError::Fenced(_))
    });
    assert!(!fixture
        .event_loop
        .pipeline
        .write_stall_waiters
        .contains(922));
    assert!(!fixture
        .event_loop
        .pipeline
        .write_stall_waiters
        .contains(923));
    assert!(fixture
        .event_loop
        .pipeline
        .write_stall_waiters
        .column_family_queue(0)
        .is_none());
    Ok(())
}

#[test]
fn should_fail_stall_waiter_when_verification_control_requests_keep_runtime_busy() -> MidgeResult<()>
{
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    fixture.event_loop.state.set_write_stalled(true);
    let stall_rx = fixture.register(924);
    let metrics_rx = fixture.register(925);
    fixture.event_loop.handle_wait_for_write_stall_clear(924, 0);
    fixture.event_loop.publication.verification_barrier.token = Some(1);
    fence_stalled_writer(&mut fixture, true);
    let (_tx, msg_rx) = crossbeam::channel::unbounded();

    // Act
    fixture
        .event_loop
        .process_one(RuntimeMsg::GetRuntimeMetrics { request_id: 925 }, &msg_rx);

    // Assert
    assert!(matches!(
        recv_response(&metrics_rx),
        RuntimeResponse::RuntimeMetricsSnapshot { .. }
    ));
    expect_error(&stall_rx, 924, |error| {
        matches!(error, MidgeError::Fenced(_))
    });
    assert_eq!(
        fixture.event_loop.publication.verification_barrier.token,
        Some(1)
    );
    assert!(!fixture
        .event_loop
        .pipeline
        .write_stall_waiters
        .contains(924));
    Ok(())
}

#[test]
fn should_remove_cancelled_write_stall_waiter_from_all_indexes() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    fixture.event_loop.state.set_write_stalled(true);
    fixture.event_loop.handle_wait_for_write_stall_clear(900, 0);
    assert_eq!(
        fixture
            .event_loop
            .pipeline
            .write_stall_waiters
            .column_family_for(900),
        Some(0)
    );
    assert_eq!(
        fixture
            .event_loop
            .pipeline
            .write_stall_waiters
            .column_family_queue(0)
            .map(std::collections::VecDeque::len),
        Some(1)
    );

    // Act
    fixture
        .event_loop
        .handle_cancel_wait_for_write_stall_clear(900);

    // Assert
    assert!(!fixture
        .event_loop
        .pipeline
        .write_stall_waiters
        .contains(900));
    assert!(
        fixture
            .event_loop
            .pipeline
            .write_stall_waiters
            .column_family_queue(0)
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
    fixture.event_loop.state.limits.max_immutable_memtables = 1;
    fixture
        .event_loop
        .state
        .get_cf_mut(secondary_cf)
        .expect("secondary column family")
        .immutable_memtables
        .push(Arc::new(crate::memtable::SkipListMemtable::new()));
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

#[test]
fn should_stall_before_wal_when_transaction_would_exceed_hard_l0_ceiling() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    fixture.event_loop.state.limits.l0_compaction_trigger = 1;
    fixture.event_loop.state.limits.max_immutable_memtables = 1;
    fixture.event_loop.state.limits.memtable_flush_threshold = 1;
    fixture
        .event_loop
        .state
        .manifest
        .test_mut()
        .files
        .extend((0..2).map(|index| crate::metadata::FileMeta {
            name: format!("l0-{index}.sst"),
            level: 0,
            cf_id: 0,
            ..Default::default()
        }));
    let first_rx = fixture.register(902);
    let second_rx = fixture.register(903);
    let (_tx, empty_rx) = crossbeam::channel::unbounded();
    fixture.event_loop.apply_transaction_with_coalescing(
        &empty_rx,
        txn_request(
            902,
            vec![put_op(0, b"fills-last-slot", b"value")],
            Some(DurabilityPolicy::Batched),
        ),
        1,
    );
    assert!(matches!(
        recv_response(&first_rx),
        RuntimeResponse::TransactionApplied { .. }
    ));
    let sequence_after_first = fixture.event_loop.state.sequence;
    let wal_appends_after_first = fixture.event_loop.actors.wal_actor.append_calls();
    assert_eq!(
        fixture.event_loop.state.l0_slot_usage(0),
        fixture.event_loop.state.l0_hard_ceiling()
    );

    // Act
    fixture.event_loop.apply_transaction_with_coalescing(
        &empty_rx,
        txn_request(
            903,
            vec![put_op(0, b"must-stall", b"value")],
            Some(DurabilityPolicy::Batched),
        ),
        1,
    );

    // Assert
    assert!(matches!(
        recv_response(&second_rx),
        RuntimeResponse::Error {
            error: MidgeError::WriteStall(_),
            ..
        }
    ));
    assert_eq!(fixture.event_loop.state.sequence, sequence_after_first);
    assert_eq!(
        fixture.event_loop.actors.wal_actor.append_calls(),
        wal_appends_after_first,
        "a stalled transaction must not mutate the WAL"
    );
    assert_eq!(
        fixture
            .event_loop
            .state
            .diagnostics
            .write_admission_snapshot()
            .l0_total,
        1,
        "count the rejected transaction once, independently of stall transitions"
    );
    Ok(())
}

#[test]
fn should_treat_memtable_limit_as_advisory_until_l0_slots_are_exhausted() -> MidgeResult<()> {
    // Arrange: exceed the memory hint without consuming every L0 slot.
    let mut fixture = EventLoopFixture::batched()?;
    fixture.event_loop.state.limits.memtable_flush_threshold = 1;
    fixture.event_loop.state.limits.l0_compaction_trigger = 1;
    fixture.event_loop.state.limits.max_immutable_memtables = 1;
    let (_messages, message_rx) = crossbeam::channel::unbounded();
    let first = fixture.register(930);
    fixture.event_loop.apply_transaction_with_coalescing(
        &message_rx,
        txn_request(
            930,
            vec![put_op(0, b"seed", b"over the memory hint")],
            Some(DurabilityPolicy::Batched),
        ),
        1,
    );
    assert_eq!(expect_transaction_applied(&first, 930).1, 1);
    assert!(fixture
        .event_loop
        .state
        .is_total_memtable_hard_limit_exceeded());
    assert!(fixture.event_loop.state.l0_slot_usage(0) < fixture.event_loop.state.l0_hard_ceiling());
    let second = fixture.register(931);

    // Act: another accepted write can use the existing active generation.
    fixture.event_loop.apply_transaction_with_coalescing(
        &message_rx,
        txn_request(
            931,
            vec![put_op(0, b"accepted", b"value")],
            Some(DurabilityPolicy::Batched),
        ),
        1,
    );

    // Assert: memory remains a hint; exhausting L0 slots is an enforced bound.
    assert_eq!(expect_transaction_applied(&second, 931).1, 1);
    fixture
        .event_loop
        .state
        .manifest
        .test_mut()
        .files
        .extend((0..2).map(|index| crate::metadata::FileMeta {
            name: format!("advisory-l0-{index}.sst"),
            level: 0,
            cf_id: 0,
            ..Default::default()
        }));
    let sequence = fixture.event_loop.state.sequence;
    let appends = fixture.event_loop.actors.wal_actor.append_calls();
    let third = fixture.register(932);
    fixture.event_loop.apply_transaction_with_coalescing(
        &message_rx,
        txn_request(
            932,
            vec![put_op(0, b"rejected", b"value")],
            Some(DurabilityPolicy::Batched),
        ),
        1,
    );
    expect_error(&third, 932, |error| {
        matches!(error, MidgeError::WriteStall(_))
    });
    assert_eq!(fixture.event_loop.state.sequence, sequence);
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), appends);
    assert_eq!(
        fixture
            .event_loop
            .state
            .get_cf(0)
            .unwrap()
            .memtable
            .get(b"rejected")?,
        None
    );
    Ok(())
}

#[test]
fn should_not_fence_runtime_when_no_space_comes_from_outside_the_wal_writer() -> MidgeResult<()> {
    // Arrange: storage admission pressure is backpressure, not lost
    // durability, even when its message mentions the wal writer.
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
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    let response = router.register(2, "TestRequest");
    assert!(!event_loop.actors.wal_actor.is_fenced());

    // Act
    event_loop.handle_write_error(
        2,
        MidgeError::NoSpace("upload queue full for wal writer backlog".to_string()),
    );

    // Assert
    expect_error(&response, 2, |error| {
        matches!(error, MidgeError::NoSpace(_))
    });
    assert!(
        lease_healthy.load(std::sync::atomic::Ordering::Acquire),
        "message text must not decide fencing"
    );
    assert!(!event_loop.state.persistence_anomaly_detected());
    Ok(())
}

#[test]
fn should_fence_runtime_when_the_wal_actor_reports_a_fenced_writer() -> MidgeResult<()> {
    // Arrange: the WAL actor fences itself when an append fails, whatever the
    // error's wording.
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
        crate::runtime::event_loop::FlushWorkerMode::Inline,
    )?;
    let response = router.register(3, "TestRequest");
    event_loop
        .actors
        .wal_actor
        .fence_transition(&mut event_loop.state, "injected append failure".to_string());

    // Act
    event_loop.handle_write_error(3, MidgeError::NoSpace("device is full".to_string()));

    // Assert
    expect_error(&response, 3, |error| {
        matches!(error, MidgeError::NoSpace(_))
    });
    assert!(!lease_healthy.load(std::sync::atomic::Ordering::Acquire));
    Ok(())
}

#[test]
fn should_leave_queued_writes_undrained_when_lease_is_unhealthy() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    fixture.event_loop.authority.fencing.lease_healthy =
        Some(Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    for request_id in [71, 72] {
        fixture.register(request_id);
        msg_tx
            .send(txn_msg(
                request_id,
                vec![put_op(0, b"fenced-drain", b"value")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue batched transaction");
    }

    // Act
    let drained = fixture.event_loop.drain_pending_writes(&msg_rx, 64);

    // Assert: normal dispatch rejects them one by one instead.
    assert_eq!(drained, 0);
    assert_eq!(msg_rx.len(), 2);
    assert_eq!(fixture.event_loop.state.sequence, 0);
    Ok(())
}

#[test]
fn should_not_apply_coalesced_transactions_when_lease_is_unhealthy() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::batched()?;
    fixture.event_loop.authority.fencing.lease_healthy =
        Some(Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let first_rx = fixture.register(73);
    let second_rx = fixture.register(74);
    msg_tx
        .send(txn_msg(
            74,
            vec![put_op(0, b"fenced-b", b"value-b")],
            Some(DurabilityPolicy::Batched),
        ))
        .expect("queue second transaction");

    // Act
    fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            73,
            vec![put_op(0, b"fenced-a", b"value-a")],
            Some(DurabilityPolicy::Batched),
        ),
        1024,
    );

    // Assert
    expect_error(&first_rx, 73, |error| {
        matches!(error, MidgeError::Fenced(_))
    });
    assert!(
        second_rx.try_recv().is_err() && msg_rx.len() == 1,
        "the second write stays queued for dispatch to fence"
    );
    assert_eq!(fixture.event_loop.state.sequence, 0);
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 0);
    Ok(())
}

#[test]
fn should_sync_batched_wal_when_fairness_slot_runs_past_batch_window() -> MidgeResult<()> {
    // Arrange: a Batched write is buffered and its batch window has passed,
    // but the request queue never empties, so only the fairness slot runs.
    let mut fixture = EventLoopFixture::batched()?;
    let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();
    let _response = fixture.register(81);
    fixture.event_loop.apply_transaction_with_coalescing(
        &msg_rx,
        txn_request(
            81,
            vec![put_op(0, b"fairness-sync", b"value")],
            Some(DurabilityPolicy::Batched),
        ),
        1,
    );
    let window = fixture
        .event_loop
        .actors
        .wal_actor
        .batch_config()
        .max_delay_ms;
    std::thread::sleep(Duration::from_millis(window + 20));
    let syncs_before = fixture.event_loop.actors.wal_actor.sync_calls();

    // Act
    fixture.event_loop.run_request_fairness_slot();

    // Assert
    assert!(fixture.event_loop.actors.wal_actor.sync_calls() > syncs_before);
    Ok(())
}

/// Fail the WAL actor's sync-time epoch check with an unreachable store.
#[cfg(feature = "failpoints")]
struct UnreachableLeaderStore {
    writer_effects: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "failpoints")]
impl crate::lease::LeaderStore for UnreachableLeaderStore {
    fn acquire_leadership(
        &self,
        _holder_id: &str,
    ) -> Result<crate::lease::LeaderRecord, crate::lease::LeaseError> {
        unreachable!("validation fixture")
    }

    fn read_current(&self) -> Result<Option<crate::lease::LeaderRecord>, crate::lease::LeaseError> {
        Err(crate::lease::LeaseError::IoError(
            "test leader store is unreachable".to_string(),
        ))
    }

    fn validate_write_admission(
        &self,
        _expected_holder_id: &str,
        _expected_epoch: u64,
        _timeout: Duration,
    ) -> Result<(), crate::lease::LeaseError> {
        Ok(())
    }

    fn with_writer_epoch(
        &self,
        _expected_holder_id: &str,
        _expected_epoch: u64,
        _timeout: Duration,
        operation: &mut dyn FnMut(),
    ) -> Result<(), crate::lease::LeaseError> {
        if self
            .writer_effects
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            operation();
            Ok(())
        } else {
            Err(crate::lease::LeaseError::IoError(
                "test leader store became unreachable at sync".to_string(),
            ))
        }
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn should_answer_outcome_unknown_when_lease_lapses_before_group_sync() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();

    // Act: both frames are appended; the lease lapses before their sync.
    let group = run_group(
        DurabilityPolicy::Strict,
        [84, 85],
        [b"sync-lapse-a", b"sync-lapse-b"],
        |fixture| {
            lapse_lease_at(
                fixture,
                "midge::wal::txn_after_commit_append_before_sync",
                0,
            );
        },
    );
    fail::remove("midge::wal::txn_after_commit_append_before_sync");
    let group = group?;

    // Assert: the appended frames may replay, so no caller hears "not
    // applied", which a lease error would tell it.
    assert!(is_outcome_unknown(&group.first, 84), "{:?}", group.first);
    assert!(is_outcome_unknown(&group.second, 85), "{:?}", group.second);

    scenario.teardown();
    drop(test_guard);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_answer_outcome_unknown_when_leader_store_is_unreachable_at_group_sync() -> MidgeResult<()>
{
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();

    // Act: the grouped append is admitted, then the sync's authority check
    // cannot reach the leader store.
    let group = run_group(
        DurabilityPolicy::Strict,
        [86, 87],
        [b"store-down-a", b"store-down-b"],
        |fixture| {
            fixture.event_loop.actors.wal_actor.set_leader_store(
                Arc::new(UnreachableLeaderStore {
                    writer_effects: std::sync::atomic::AtomicUsize::new(0),
                }),
                "writer".to_string(),
            );
        },
    )?;

    // Assert
    assert!(is_outcome_unknown(&group.first, 86), "{:?}", group.first);
    assert!(is_outcome_unknown(&group.second, 87), "{:?}", group.second);

    scenario.teardown();
    drop(test_guard);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_answer_outcome_unknown_when_lease_lapses_before_spilled_sync() -> MidgeResult<()> {
    // Arrange
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let test_guard = crate::failpoints::test_failpoint_guard();
    let scenario = fail::FailScenario::setup();
    let temp = tempfile::tempdir().map_err(MidgeError::Io)?;
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    lapse_lease_at(
        &mut fixture,
        "midge::wal::txn_after_commit_append_before_sync",
        0,
    );

    // Act
    let result = commit_strict_spill(&mut fixture, temp.path());
    fail::remove("midge::wal::txn_after_commit_append_before_sync");

    // Assert
    assert!(
        matches!(&result, Err(MidgeError::OutcomeUnknown(message)) if message.contains("may be applied on restart")),
        "{result:?}"
    );

    scenario.teardown();
    drop(test_guard);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_answer_not_applied_to_fallback_request_when_its_group_fails() -> MidgeResult<()> {
    // Arrange: request 75 requires a different ordering contract, so it
    // cannot join the group and waits as the fallback, never prepared.
    let _guard = failpoint_test_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let (fixture, grouped, fallback) = {
        let test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let grouped_rx = fixture.register(74);
        let fallback_rx = fixture.register(75);
        msg_tx
            .send(RuntimeMsg::ApplyTransaction {
                request_id: 75,
                ops: vec![put_op(0, b"shared", b"fallback-value")],
                assertions: Vec::new(),
                durability_policy: Some(DurabilityPolicy::Strict),
                start_sequence: Some(0),
                conflict_policy: ConflictPolicy::AbortOnWriteConflict,
                response_tx: None,
            })
            .expect("queue fallback transaction");
        lapse_lease_at(
            &mut fixture,
            "midge::wal::before_prepared_transaction_apply",
            0,
        );

        // Act
        fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                74,
                vec![put_op(0, b"shared", b"grouped-value")],
                Some(DurabilityPolicy::Strict),
            ),
            1024,
        );
        fail::remove("midge::wal::before_prepared_transaction_apply");
        let responses = (recv_response(&grouped_rx), recv_response(&fallback_rx));
        scenario.teardown();
        drop(test_guard);
        (fixture, responses.0, responses.1)
    };
    let [recovered, _] = reopen_and_read(fixture, [b"shared", b"shared"])?;

    // Assert: the grouped unit is synced but not applied, so its outcome is
    // unknown. The fallback was never logged or applied, so it is told so.
    assert!(is_outcome_unknown(&grouped, 74), "{grouped:?}");
    assert!(
        matches!(
            fallback,
            RuntimeResponse::Error {
                request_id: 75,
                error: MidgeError::Fenced(_),
            }
        ),
        "{fallback:?}"
    );
    assert_eq!(recovered.as_deref(), Some(b"grouped-value".as_slice()));
    Ok(())
}

#[test]
fn should_recover_later_overlapping_strict_unit_when_both_share_one_append() -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    let (sender, receiver) = crossbeam::channel::unbounded();
    let first = fixture.register(9_905);
    let second = fixture.register(9_906);
    sender
        .send(txn_msg(
            9_906,
            vec![
                put_op(0, b"hot-key", b"second"),
                put_op(0, b"second-unit", b"whole"),
            ],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue later unit");

    // Act
    let handled = fixture.event_loop.apply_transaction_with_coalescing(
        &receiver,
        txn_request(
            9_905,
            vec![put_op(0, b"hot-key", b"first")],
            Some(DurabilityPolicy::Strict),
        ),
        1024,
    );

    // Assert
    assert_eq!(handled, 2);
    assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
    assert_eq!(fixture.event_loop.actors.wal_actor.sync_calls(), 1);
    assert_eq!(expect_transaction_applied(&first, 9_905).1, 1);
    assert_eq!(expect_transaction_applied(&second, 9_906).1, 2);
    let [key, second_unit] = reopen_and_read(fixture, [b"hot-key", b"second-unit"])?;
    assert_eq!(key.as_deref(), Some(b"second".as_slice()));
    assert_eq!(second_unit.as_deref(), Some(b"whole".as_slice()));
    Ok(())
}

#[test]
fn should_reject_conflicting_follower_when_staged_point_or_range_intersects_its_writes(
) -> MidgeResult<()> {
    for (first_ops, second_ops) in [
        (
            vec![put_op(0, b"middle", b"first")],
            vec![put_op(0, b"middle", b"second")],
        ),
        (
            vec![put_op(0, b"middle", b"first")],
            vec![delete_range_op(0, b"a", b"z")],
        ),
        (
            vec![delete_range_op(0, b"a", b"z")],
            vec![put_op(0, b"middle", b"second")],
        ),
        (
            vec![delete_range_op(0, b"a", b"z")],
            vec![delete_range_op(0, b"b", b"y")],
        ),
    ] {
        // Arrange
        let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
        let (sender, receiver) = crossbeam::channel::unbounded();
        let first = fixture.register(9_946);
        let second = fixture.register(9_947);
        sender
            .send(RuntimeMsg::ApplyTransaction {
                request_id: 9_947,
                ops: second_ops,
                assertions: Vec::new(),
                durability_policy: Some(DurabilityPolicy::Strict),
                start_sequence: Some(0),
                conflict_policy: ConflictPolicy::AbortOnWriteConflict,
                response_tx: None,
            })
            .expect("queue conflicting unit");

        // Act
        fixture.event_loop.apply_transaction_with_coalescing(
            &receiver,
            ordered_txn_request(
                9_946,
                first_ops,
                DurabilityPolicy::Strict,
                0,
                ConflictPolicy::AbortOnWriteConflict,
            ),
            1024,
        );

        // Assert
        assert_eq!(expect_transaction_applied(&first, 9_946).1, 1);
        expect_error(&second, 9_947, |error| {
            matches!(error, MidgeError::WriteConflict(_))
        });
        assert_eq!(fixture.event_loop.actors.wal_actor.append_calls(), 1);
        assert_eq!(fixture.event_loop.state.applied_sequence, 3);
    }
    Ok(())
}

#[test]
fn should_allow_insert_only_after_staged_range_delete_when_existing_value_is_masked(
) -> MidgeResult<()> {
    // Arrange
    let mut fixture = EventLoopFixture::with_policy(DurabilityPolicy::Strict)?;
    fixture.event_loop.actors.wal_actor.append_transaction(
        &mut fixture.event_loop.state,
        crate::runtime::actors::wal::TransactionAppendParams {
            request_id: 9_950,
            ops: vec![put_op(0, b"middle", b"old")],
            assertions: Vec::new(),
            durability_policy: Some(DurabilityPolicy::Strict),
            start_sequence: None,
            conflict_policy: ConflictPolicy::LastWriteWins,
        },
    )?;
    let appends_before = fixture.event_loop.actors.wal_actor.append_calls();
    let (sender, receiver) = crossbeam::channel::unbounded();
    let first = fixture.register(9_951);
    let second = fixture.register(9_952);
    sender
        .send(txn_msg(
            9_952,
            vec![insert_only_put_op(0, b"middle", b"new")],
            Some(DurabilityPolicy::Strict),
        ))
        .expect("queue insert");

    // Act
    fixture.event_loop.apply_transaction_with_coalescing(
        &receiver,
        txn_request(
            9_951,
            vec![delete_range_op(0, b"a", b"z")],
            Some(DurabilityPolicy::Strict),
        ),
        1024,
    );

    // Assert
    assert_eq!(expect_transaction_applied(&first, 9_951).1, 1);
    assert_eq!(expect_transaction_applied(&second, 9_952).1, 1);
    assert_eq!(
        fixture.event_loop.actors.wal_actor.append_calls() - appends_before,
        1
    );
    assert_memtable_value(&fixture.event_loop, 0, b"middle", Some(b"new"));
    Ok(())
}
