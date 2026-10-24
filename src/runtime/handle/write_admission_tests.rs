use super::*;

#[test]
fn should_never_submit_after_deadline_expires_during_caller_slowdown() {
    // Arrange
    let (_runtime, mut handle) =
        crate::runtime::Runtime::new_with_response_timeout(Duration::from_millis(2));
    let (tx, rx) = crossbeam::channel::bounded(1);
    handle.msg_tx = tx;
    handle
        .write_admission
        .configure(&crate::config::WriteBackpressureConfig {
            l0_soft_limit: Some(1),
            max_admission_delay: Duration::from_secs(1),
            ..Default::default()
        });
    handle.write_admission.update_family(0, 1, 0);
    let submission = TransactionSubmission {
        ops: vec![crate::runtime::TransactionOp::Put {
            cf_id: 0,
            key: bytes::Bytes::from_static(b"k"),
            value: bytes::Bytes::from_static(b"v"),
            ttl_seconds: None,
            insert_only: false,
        }],
        assertions: vec![],
        durability_policy: None,
        start_sequence: None,
        conflict_policy: crate::types::ConflictPolicy::LastWriteWins,
    };

    // Act
    let result = handle.send_apply_transaction_and_wait(123, submission);

    // Assert: timeout is proven unchanged, with no registered route or queued
    // message that could become a delayed mutation after this caller returns.
    assert!(matches!(result, Err(MidgeError::Timeout(_))));
    assert!(rx.is_empty());
    assert_eq!(handle.router.pending_len(), 0);
    assert_eq!(
        handle
            .write_admission
            .slowdown_count
            .load(Ordering::Relaxed),
        1
    );
}
