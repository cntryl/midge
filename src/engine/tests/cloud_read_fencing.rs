use super::*;
use crate::engine::api::{IteratorState, Query};

#[test]
fn should_invalidate_cloud_reads_when_monotonic_lease_expires_before_notification(
) -> MidgeResult<()> {
    // Arrange: the same expired local lease remains a readable control.
    for cloud in [false, true] {
        let directory = tempfile::tempdir()?;
        let options = if cloud {
            OpenOptions::cloud_simulated(directory.path(), "read-fence", "db").build()?
        } else {
            OpenOptions::local(directory.path()).build()?
        };
        let engine = Engine::open(options)?;
        let cf = engine.get_column_family("default").unwrap();
        let mut seed = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        seed.put(b"a".to_vec(), b"one".to_vec(), None)?;
        seed.put(b"b".to_vec(), b"two".to_vec(), None)?;
        seed.commit(if cloud {
            WriteOptions::cloud_strict()
        } else {
            WriteOptions::sync()
        })?;
        engine.flush_cf(&cf)?;
        let frozen = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        let mut own = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        own.put(b"own".to_vec(), b"uncommitted".to_vec(), None)?;
        let mut forward = frozen.scan(&Query::new())?;
        let mut reverse = frozen.scan(&Query::new().reverse())?;
        let mut empty = frozen.scan(&Query::new().limit(0))?;
        assert_eq!(frozen.get(b"a")?.as_deref(), Some(b"one".as_slice()));
        assert_eq!(forward.next().unwrap()?.0.as_ref(), b"a");

        // Act: stop notification and expire only the shared monotonic authority.
        let mut heartbeat = engine
            .lease_state
            .heartbeat
            .as_ref()
            .unwrap()
            .lock()
            .unwrap();
        heartbeat.stop();
        heartbeat.validity_for_test().unwrap().expire_for_test();
        assert!(
            heartbeat.is_healthy(),
            "no watchdog notification is involved"
        );
        drop(heartbeat);

        // Assert: cached, own-write, missing and pre-created lazy paths agree.
        if cloud {
            for result in [frozen.get(b"a"), frozen.get(b"absent"), own.get(b"own")] {
                assert!(matches!(result, Err(MidgeError::Fenced(_))), "{result:?}");
            }
            assert!(matches!(
                frozen.scan(&Query::new()),
                Err(MidgeError::Fenced(_))
            ));
            for scan in [&mut forward, &mut reverse, &mut empty] {
                for _ in 0..2 {
                    assert!(matches!(scan.next(), Some(Err(MidgeError::Fenced(_)))));
                    assert_eq!(scan.state(), IteratorState::Failed);
                }
            }
            for mode in [TransactionMode::ReadOnly, TransactionMode::ReadWrite] {
                assert!(matches!(
                    engine.begin_tx(cf.id(), mode),
                    Err(MidgeError::Fenced(_))
                ));
            }
        } else {
            assert_eq!(frozen.get(b"a")?.as_deref(), Some(b"one".as_slice()));
            assert_eq!(frozen.get(b"absent")?, None);
            assert_eq!(own.get(b"own")?.as_deref(), Some(b"uncommitted".as_slice()));
            assert_eq!(forward.next().unwrap()?.0.as_ref(), b"b");
            assert_eq!(reverse.next().unwrap()?.0.as_ref(), b"b");
            assert!(empty.next().is_none());
            assert_eq!(
                engine
                    .begin_tx(cf.id(), TransactionMode::ReadOnly)?
                    .get(b"a")?
                    .as_deref(),
                Some(b"one".as_slice())
            );
        }
        // Diagnostic requests retain their control path in either mode, even
        // when cloud data reads correctly reject the predecessor's authority.
        let diagnostics = engine.metrics().get_runtime_metrics()?;
        assert!(diagnostics.current_sequence > 0);
    }
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_preserve_read_authority_after_wal_io_fence_with_live_lease() -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    for cloud in [false, true] {
        let directory = tempfile::tempdir()?;
        let options = if cloud {
            OpenOptions::cloud_simulated(directory.path(), "read-wal-fence", "db").build()?
        } else {
            OpenOptions::local(directory.path()).build()?
        };
        let engine = Engine::open(options)?;
        let family = engine.get_column_family("default").unwrap();
        let policy = if cloud {
            WriteOptions::cloud_strict()
        } else {
            WriteOptions::sync()
        };
        let mut seed = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
        seed.put(b"retained".to_vec(), b"value".to_vec(), None)?;
        seed.commit(policy)?;
        engine.flush_cf(&family)?;
        let frozen = engine.begin_tx(family.id(), TransactionMode::ReadOnly)?;
        let mut failing = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
        failing.put(b"failed".to_vec(), b"unconfirmed".to_vec(), None)?;
        fail::cfg("midge::wal::inject_no_space_on_sync", "return").expect("WAL sync failure");

        // Act
        let result = failing.commit(policy);
        fail::remove("midge::wal::inject_no_space_on_sync");

        // Assert: storage failure does not revoke a live read lease or its
        // retained source, while new durable writes remain fenced.
        assert!(
            matches!(result, Err(MidgeError::OutcomeUnknown(_))),
            "{result:?}"
        );
        assert_eq!(
            frozen.get(b"retained")?.as_deref(),
            Some(b"value".as_slice())
        );
        assert_eq!(frozen.get(b"absent")?, None);
        let mut scan = frozen.scan(&Query::new())?;
        assert_eq!(scan.next().unwrap()?.0.as_ref(), b"retained");
        assert!(scan.next().is_none());
        assert_eq!(
            engine
                .begin_tx(family.id(), TransactionMode::ReadOnly)?
                .get(b"retained")?
                .as_deref(),
            Some(b"value".as_slice())
        );
        let diagnostics = engine.metrics().get_runtime_metrics()?;
        assert_eq!(diagnostics.health, crate::EngineHealth::Degraded);
        assert!(diagnostics.current_sequence > 0);
    }
    Ok(())
}
