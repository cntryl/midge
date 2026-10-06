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
    }
    Ok(())
}
