use super::*;
use crate::runtime::event_loop::wal_pipeline::tests::{begin_snapshot, settle, write};
use crate::runtime::{RuntimeMsg, RuntimeResponse};
use crossbeam::channel::Sender;
use std::time::{Duration, Instant};

struct HeldApply {
    first: Receiver<()>,
    later: Receiver<()>,
    release: Sender<()>,
}

impl HeldApply {
    fn new() -> Self {
        let (entered, first) = crossbeam::channel::bounded(1);
        let (finished, later) = crossbeam::channel::bounded(1);
        let (release, released) = crossbeam::channel::unbounded();
        fail::cfg_callback("midge::apply::before_first_unit", move || {
            let _ = entered.try_send(());
            released
                .recv_timeout(Duration::from_secs(30))
                .expect("release first unit");
        })
        .expect("hold first apply unit");
        fail::cfg_callback("midge::apply::after_second_unit", move || {
            let _ = finished.try_send(());
        })
        .expect("observe later unit completion");
        Self {
            first,
            later,
            release,
        }
    }

    fn wait(&self, event_loop: &mut EventLoop) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut first = false;
        let mut later = false;
        while !first || !later {
            event_loop.progress_wal_pipeline();
            first |= self.first.try_recv().is_ok();
            later |= self.later.try_recv().is_ok();
            assert!(
                Instant::now() < deadline,
                "workers did not overlap held and later apply"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        event_loop.progress_wal_pipeline();
    }
}

impl Drop for HeldApply {
    fn drop(&mut self) {
        let _ = self.release.send(());
        fail::remove("midge::apply::before_first_unit");
        fail::remove("midge::apply::after_second_unit");
    }
}

fn start_group(
    event_loop: &mut EventLoop,
) -> (Receiver<RuntimeResponse>, Receiver<RuntimeResponse>) {
    let first = event_loop
        .routing
        .router
        .register(90_600, "first concurrent apply");
    let later = event_loop
        .routing
        .router
        .register(90_601, "later concurrent apply");
    let (queued, incoming) = crossbeam::channel::unbounded();
    queued
        .send(write(90_601, &[b"later-one", b"later-two"], true))
        .unwrap();
    event_loop.process_one(
        write(90_600, &[b"first-one", b"first-two"], true),
        &incoming,
    );
    (first, later)
}

#[test]
fn should_publish_one_ordered_cut_when_later_apply_finishes_before_first_unit(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    let held = HeldApply::new();
    let (first, later) = start_group(&mut event_loop);
    held.wait(&mut event_loop);
    let begin = event_loop
        .routing
        .router
        .register(90_602, "old cut during apply");

    // Act: unit N+1 is physically present, while N has not mutated a key.
    event_loop.process_one(
        RuntimeMsg::BeginTransaction {
            request_id: 90_602,
            cf_id: 0,
        },
        &crossbeam::channel::never(),
    );
    let (cut, old) = begin_snapshot(&begin);

    // Assert: source reads at the old cut cannot see N+1 or part of N.
    assert_eq!(cut, 0);
    assert_eq!(old.get(b"later-one", u64::MAX)?, Some(b"value".to_vec()));
    assert_eq!(old.get(b"first-one", u64::MAX)?, None);
    assert_eq!(old.get(b"later-one", cut)?, None);
    assert_eq!(old.get(b"later-two", cut)?, None);
    assert!(first.try_recv().is_err());
    assert!(later.try_recv().is_err());
    assert_eq!(event_loop.state.total_memtable_bytes, 0);
    held.release.send(()).expect("release lower unit");
    assert!(matches!(
        settle(&mut event_loop, &first),
        RuntimeResponse::TransactionApplied { .. }
    ));
    assert!(matches!(
        settle(&mut event_loop, &later),
        RuntimeResponse::TransactionApplied { .. }
    ));
    assert_eq!(event_loop.state.applied_sequence, event_loop.state.sequence);
    let accounted = event_loop.state.total_memtable_bytes;
    assert!(accounted > 0);
    event_loop.state.recompute_total_memtable_bytes();
    assert_eq!(event_loop.state.total_memtable_bytes, accounted);
    let current = event_loop
        .create_read_snapshot(0)
        .expect("complete group source");
    for key in [
        b"first-one".as_slice(),
        b"first-two",
        b"later-one",
        b"later-two",
    ] {
        assert_eq!(
            current.get(key, event_loop.state.applied_sequence)?,
            Some(b"value".to_vec())
        );
        assert_eq!(old.get(key, cut)?, None);
    }
    Ok(())
}

#[test]
fn should_preserve_group_authority_when_authority_is_lost_during_apply(
) -> crate::common::MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let mut event_loop = super::super::tests::create_test_local_event_loop()?;
    event_loop.pipeline.inline_flush_worker = false;
    event_loop.state.column_families.insert(
        1,
        crate::runtime::state::ColumnFamilyState::new(1, "other".into()),
    );
    let healthy = Arc::new(AtomicBool::new(true));
    event_loop.authority.fencing.lease_healthy = Some(Arc::clone(&healthy));
    let path = event_loop.state.db_path.clone();
    let held = HeldApply::new();
    let (first, later) = start_group(&mut event_loop);
    held.wait(&mut event_loop);

    // Act
    healthy.store(false, Ordering::Release);
    held.release.send(()).expect("release after authority loss");
    assert!(matches!(
        settle(&mut event_loop, &first),
        RuntimeResponse::Error {
            error: MidgeError::OutcomeUnknown(_),
            ..
        }
    ));
    assert!(matches!(
        settle(&mut event_loop, &later),
        RuntimeResponse::Error {
            error: MidgeError::OutcomeUnknown(_),
            ..
        }
    ));
    let deadline = Instant::now() + Duration::from_secs(10);
    while event_loop.wal_group_in_flight() {
        event_loop.progress_wal_pipeline();
        assert!(
            Instant::now() < deadline,
            "admitted apply effects did not drain"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    healthy.store(true, Ordering::Release);
    let response = event_loop
        .routing
        .router
        .register(90_603, "cross-family best effort after failure");
    let mut message = write(90_603, &[b"other-family"], false);
    if let RuntimeMsg::ApplyTransaction { ops, .. } = &mut message {
        if let crate::runtime::TransactionOp::Put { cf_id, .. } = &mut ops[0] {
            *cf_id = 1;
        }
    }
    event_loop.process_one(message, &crossbeam::channel::never());

    // Assert: restoring a health flag cannot reopen the unknown global cut.
    assert!(matches!(
        response.try_recv(),
        Ok(RuntimeResponse::Error {
            error: MidgeError::Fenced(_),
            ..
        })
    ));
    assert_eq!(event_loop.state.applied_sequence, 0);
    assert_eq!(event_loop.state.get_cf(1).unwrap().memtable.size_bytes(), 0);
    drop(held);
    drop(event_loop);
    let recovered = crate::runtime::state::RuntimeState::try_new(
        path,
        false,
        crate::config::RecoveryPolicy::Strict,
    )?;
    for key in [
        b"first-one".as_slice(),
        b"first-two",
        b"later-one",
        b"later-two",
    ] {
        assert_eq!(
            recovered
                .get_cf(0)
                .unwrap()
                .memtable
                .get_at_seq(key, u64::MAX)?,
            Some(b"value".to_vec())
        );
    }
    Ok(())
}
